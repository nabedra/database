use super::{split::SplitResult, *};
use crate::{
    oltp::{failure, mvcc::store::split_usage::ShardEpoch},
    oltp_replication::{
        authority::{
            control::{GroupRecord, InventoryPage, ReplicaMove},
            timestamp::split::{
                AUTOMATIC_ATTEMPTS, AutomaticSplitOrigin, SplitAttempt, SplitIntent, SplitPlacement,
                SplitSourcePlacement,
            },
        },
        manager::shard_split::{
            AutomaticSplitPolicy, SplitCapacity, SplitClient, SplitPoolDeficit, Usage, UsageResume,
        },
    },
};
use serde::Serialize;
use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

const OBSERVATION_INTERVAL: Duration = Duration::from_millis(500);
const STEP_DEADLINE: Duration = Duration::from_secs(75);
const MAX_CONSECUTIVE_FAILURES: u8 = 4;
const MIN_IMAGE_PAGE_BYTES: usize = 256 << 10;
const DEFICIT_TTL: Duration = Duration::from_secs(10);

#[derive(Default, Serialize)]
pub(super) struct Observation {
    target_bytes: Option<u64>,
    sample: Option<Sample>,
    transition: Option<Transition>,
    #[serde(skip)]
    deficit: Option<SplitDeficit>,
}
#[derive(Clone, Serialize)]
pub(super) struct SplitDeficit {
    ready: ReadyMembership,
    source: SplitSourcePlacement,
    epoch: ShardEpoch,
    observed_committed_bytes: u64,
    required_bytes: u64,
    pools: Page<SplitPoolDeficit>,
    #[serde(skip)]
    recorded_at: Instant,
}
#[derive(Serialize)]
struct Sample {
    source: Uuid,
    epoch: ShardEpoch,
    observed_committed_bytes: u64,
}
#[derive(Serialize)]
struct Transition {
    source: Uuid,
    attempt: Option<Uuid>,
    state: &'static str,
    error: Option<ErrorCode>,
}

#[derive(Default)]
pub(super) struct Sweep {
    revision: Option<u64>,
    after: Option<Uuid>,
    page: VecDeque<GroupRecord>,
    page_owner: Option<ScratchLease>,
    done: bool,
}
impl Sweep {
    pub(super) fn is_exhausted(&self) -> bool {
        self.done && self.page.is_empty()
    }

    pub(super) async fn next(
        &mut self,
        runtime: &WorkerRuntime,
    ) -> Result<Option<GroupRecord>, ProtocolError> {
        if let Some(record) = self.page.pop_front() {
            return Ok(Some(record));
        }
        self.page_owner = None;
        if self.done {
            *self = Self::default();
            return Ok(None);
        }
        let revision = match self.revision {
            Some(revision) => revision,
            None => {
                runtime
                    .services
                    .control
                    .inspect()
                    .await?
                    .ok_or_else(unavailable)?
                    .group_inventory_revision
            }
        };
        let raw = runtime
            .services
            .control
            .groups(revision, self.after)
            .await?;
        let owner = runtime.services.shared.reserve_scratch(
            ByteClass::Data,
            crate::oltp::mvcc::json_decode_budget(raw.as_slice())?,
        )?;
        let page: InventoryPage<GroupRecord> =
            serde_json::from_slice(raw.as_slice()).map_err(|_| unknown())?;
        if page.revision != revision || (!page.done && page.after <= self.after) {
            return Err(unknown());
        }
        self.after = page.after;
        self.revision = Some(revision);
        self.done = page.done;
        self.page = page
            .items
            .into_inner()
            .into_iter()
            .filter(|record| matches!(record.identity.kind, GroupKind::Data { .. }))
            .collect();
        self.page_owner = Some(owner);
        Ok(self.page.pop_front())
    }
}

struct Work {
    intent: SplitIntent,
    placement: Option<SplitPlacement>,
    failures: u8,
    cancel: bool,
}
#[derive(Default)]
struct OwnerLoop {
    sweep: Sweep,
    scan: Option<(GroupRecord, UsageResume)>,
    active: Option<Work>,
    blocked: BTreeSet<Uuid>,
    /// RANGES-2: observations since the last listing of open attempts, and
    /// the listing's cursor.
    since_open_check: u32,
    open_after: Option<Uuid>,
}
/// RANGES-2: list the open split attempts every this many observations
/// (about 5 s), independently of the group sweep.
const OPEN_CHECK_EVERY: u32 = 10;

impl WorkerRuntime {
    pub(in crate::oltp_runtime) fn spawn_automatic_split(
        self: &Arc<Self>,
        policy: Option<AutomaticSplitPolicy>,
    ) -> Result<Option<tokio::task::JoinHandle<()>>, ProtocolError> {
        if let Some(policy) = policy {
            policy.validate()?;
        }
        let Some(client) = self.services.split.clone() else {
            // Without registered placement there is no split at all.
            return if policy.is_some() { Err(unavailable()) } else { Ok(None) };
        };
        let owner = self
            .services
            .shared
            .reserve_scratch(ByteClass::Data, 128 << 10)?;
        if let Some(policy) = policy {
            self.automatic_split
                .lock()
                .map_err(|_| unknown())?
                .target_bytes = Some(policy.shard_target_bytes);
        }
        let weak = Arc::downgrade(self);
        Ok(Some(nabedra_rt::task::spawn(async move {
            let _owner = owner;
            let mut authority = None;
            let mut state = OwnerLoop::default();
            loop {
                nabedra_rt::time::sleep(OBSERVATION_INTERVAL).await;
                let Some(runtime) = weak.upgrade() else { break };
                let local_owner = runtime
                    .services
                    .replica
                    .lock()
                    .ok()
                    .is_some_and(|replica| replica.authority().owner == runtime.services.local);
                if !local_owner {
                    if !runtime.accepting.load(Ordering::Acquire) {
                        break;
                    }
                    state = OwnerLoop::default();
                    authority = None;
                    continue;
                }
                if state.active.is_none() {
                    state.since_open_check = state.since_open_check.saturating_add(1);
                    // RANGES-2: without a policy the owner sweeps no group and
                    // holds no sweep state. It only resumes open attempts from
                    // the Timestamps index every OPEN_CHECK_EVERY observations.
                    if policy.is_none() && state.since_open_check < OPEN_CHECK_EVERY {
                        if !runtime.accepting.load(Ordering::Acquire) {
                            break;
                        }
                        continue;
                    }
                }
                let current = nabedra_rt::time::timeout(
                    Duration::from_secs(5),
                    runtime.services.control.current(&runtime.services.scope),
                )
                .await
                .ok()
                .and_then(Result::ok)
                .filter(|current| current.owner == runtime.services.local);
                if !runtime.accepting.load(Ordering::Acquire) {
                    if let (Some(current), Some(work)) = (current, state.active.take()) {
                        let _ = settle(
                            &runtime,
                            &client,
                            &work.intent,
                            current.ready.view.authority(),
                        )
                        .await;
                    }
                    break;
                }
                let Some(current) = current else { continue };
                let next = current.ready.view.authority();
                if authority != Some(next) {
                    state = OwnerLoop::default();
                    authority = Some(next);
                }
                if let Some(work) = state.active.take() {
                    state.drive(&runtime, &client, work, next).await;
                } else {
                    let result = nabedra_rt::time::timeout(
                        Duration::from_secs(10),
                        state.observe(&runtime, &client, policy, current.ready),
                    )
                    .await;
                    if !matches!(result, Ok(Ok(()))) {
                        state.scan = None;
                    }
                }
            }
        })))
    }

    pub(in crate::oltp_runtime) fn automatic_split_status(
        &self,
        id: Uuid,
    ) -> Result<EncodedResponse, ProtocolError> {
        Ok(response::encode_success(
            id,
            &*self.automatic_split.lock().map_err(|_| unknown())?,
            false,
        ))
    }

    pub(super) async fn split_growth_observations(
        &self,
        ready: ReadyMembership,
    ) -> Result<(Option<SplitDeficit>, Option<ReplicaMove>), ProtocolError> {
        let deficit = {
            let observation = self.automatic_split.lock().map_err(|_| unknown())?;
            if observation.target_bytes.is_none() || !self.accepting.load(Ordering::Acquire) {
                return Ok((None, None));
            }
            observation.deficit.clone().filter(|sample| {
                sample.ready == ready && nabedra_rt::time::elapsed(sample.recorded_at) < DEFICIT_TTL
            })
        };
        let current = self.services.control.current(&self.services.scope).await?;
        if current.owner != self.services.local || current.ready != ready {
            return Ok((None, None));
        }
        if let Some(active) = self.services.control.active_replica_move().await? {
            return Ok((None, Some(active)));
        }
        let Some(deficit) = deficit else {
            return Ok((None, None));
        };
        let client = self.services.split.as_ref().ok_or_else(unavailable)?;
        if client
            .source_placement(&deficit.source.record, ready.view.authority())
            .await?
            != deficit.source
        {
            return Ok((None, None));
        }
        let current = self
            .automatic_split
            .lock()
            .map_err(|_| unknown())?
            .deficit
            .as_ref()
            .filter(|sample| {
                sample.recorded_at == deficit.recorded_at
                    && nabedra_rt::time::elapsed(sample.recorded_at) < DEFICIT_TTL
            })
            .map(|_| deficit);
        Ok((current, None))
    }

    fn clear_split_deficit(&self, source: Uuid) {
        if let Ok(mut observation) = self.automatic_split.lock()
            && observation
                .deficit
                .as_ref()
                .is_some_and(|sample| sample.source.record.identity.group_id == source)
        {
            observation.deficit = None;
        }
    }

    fn split_observation(
        &self,
        source: Uuid,
        attempt: Option<Uuid>,
        state: &'static str,
        error: Option<ErrorCode>,
    ) {
        if let Ok(mut observation) = self.automatic_split.lock() {
            observation.transition = Some(Transition {
                source,
                attempt,
                state,
                error,
            });
        }
    }
}

impl OwnerLoop {
    async fn observe(
        &mut self,
        runtime: &WorkerRuntime,
        client: &SplitClient,
        policy: Option<AutomaticSplitPolicy>,
        ready: ReadyMembership,
    ) -> Result<(), ProtocolError> {
        let authority = ready.view.authority();
        if self.since_open_check >= OPEN_CHECK_EVERY {
            self.since_open_check = 0;
            if self.resume_open(client, authority).await? {
                return Ok(());
            }
        }
        // Without a policy no group is swept: every attempt that can hold a
        // source, manual or bound automatic, is in the open index.
        let Some(policy) = policy else {
            return Ok(());
        };
        if self.blocked.len() >= runtime.services.shared.config().max_resident_groups {
            return Ok(());
        }
        let (record, previous) = match self.scan.take() {
            Some((record, scan)) => (record, Some(scan)),
            None => match self.sweep.next(runtime).await {
                Ok(Some(record)) => (record, None),
                Ok(None) => return Ok(()),
                Err(error) => {
                    self.sweep = Sweep::default();
                    return Err(error);
                }
            },
        };
        let source = &record.identity;
        if self.blocked.contains(&source.group_id) {
            return Ok(());
        }
        let placement = match client.source_placement(&record, authority).await {
            Ok(placement) => placement,
            Err(error) => {
                runtime.split_observation(
                    source.group_id,
                    None,
                    "capacity_pending",
                    Some(error.code),
                );
                return Ok(());
            }
        };
        match client
            .observe_usage(&placement, authority, policy.shard_target_bytes, previous)
            .await?
        {
            Usage::Inactive => runtime.clear_split_deficit(source.group_id),
            Usage::Scanning(scan) => self.scan = Some((record, scan)),
            Usage::Frozen { epoch, split_id } => {
                // RANGES-2: a source frozen, or holding the direct barrier, by
                // an open attempt is resumed here, whatever started it, until
                // the attempt publishes or aborts.
                runtime.clear_split_deficit(source.group_id);
                let automatic = (1..=AUTOMATIC_ATTEMPTS)
                    .any(|slot| AutomaticSplitOrigin { epoch, slot }.id(source) == split_id);
                if let Some(attempt) = client.job(split_id).await?
                    && attempt.closed.is_none()
                    && attempt.intent.source == *source
                    && attempt.intent.automatic.is_some() == automatic
                {
                    self.active = Some(Work {
                        intent: attempt.intent.clone(),
                        placement: attempt.placement().cloned(),
                        failures: 0,
                        // A manual attempt of an older owner term is settled by
                        // the split itself (interrupted by worker recovery).
                        cancel: automatic && attempt.authority != authority,
                    });
                }
            }
            Usage::Observed {
                epoch,
                bytes,
                largest_image,
                pivot,
            } => {
                runtime
                    .automatic_split
                    .lock()
                    .map_err(|_| unknown())?
                    .sample = Some(Sample {
                    source: source.group_id,
                    epoch,
                    observed_committed_bytes: bytes,
                });
                if threshold_crossed(bytes, policy) {
                    self.active = select_work(
                        runtime,
                        client,
                        policy,
                        ready,
                        &placement,
                        epoch,
                        bytes,
                        largest_image,
                        pivot,
                    )
                    .await?;
                } else {
                    runtime.clear_split_deficit(source.group_id);
                }
            }
        }
        Ok(())
    }

    /// RANGES-2: resume the next open attempt from Timestamps' index of open
    /// attempts, whatever started it and whether or not it holds its source
    /// yet. Recovery of a lost coordinator then takes about
    /// `OPEN_CHECK_EVERY` observations per open attempt, not one sweep of
    /// every group. Returns whether it found one.
    async fn resume_open(
        &mut self,
        client: &SplitClient,
        authority: Authority,
    ) -> Result<bool, ProtocolError> {
        let ids = client.open_jobs(self.open_after).await?;
        let Some(&id) = ids.first() else {
            self.open_after = None;
            return Ok(false);
        };
        self.open_after = Some(id);
        let Some(attempt) = client.job(id).await? else {
            return Ok(false);
        };
        if attempt.closed.is_some()
            || self.blocked.contains(&attempt.intent.source.group_id)
            || leave_to_select_work(&attempt, authority)
        {
            return Ok(false);
        }
        let automatic = attempt.intent.automatic.is_some();
        self.active = Some(Work {
            intent: attempt.intent.clone(),
            placement: attempt.placement().cloned(),
            failures: 0,
            cancel: automatic && attempt.authority != authority,
        });
        Ok(true)
    }

    async fn drive(
        &mut self,
        runtime: &WorkerRuntime,
        client: &SplitClient,
        mut work: Work,
        authority: Authority,
    ) {
        let source = work.intent.source.group_id;
        let id = Some(work.intent.split_id);
        if work.cancel {
            match settle(runtime, client, &work.intent, authority).await {
                Ok(Some(SplitResult::Published { .. })) => {
                    runtime.split_observation(source, id, "published", None)
                }
                Ok(_) => runtime.split_observation(source, id, "aborted", None),
                Err(error) => {
                    self.blocked.insert(source);
                    runtime.split_observation(source, id, "settlement_pending", Some(error.code));
                }
            }
            return;
        }
        runtime.split_observation(source, id, "splitting", None);
        let error = match nabedra_rt::time::timeout(
            STEP_DEADLINE,
            runtime.split_with_placement(work.intent.clone(), work.placement.as_ref()),
        )
        .await
        {
            Ok(Ok(SplitResult::Published { .. })) => {
                runtime.split_observation(source, id, "published", None);
                return;
            }
            Ok(Ok(SplitResult::Importing { .. })) => {
                work.failures = 0;
                self.active = Some(work);
                return;
            }
            Ok(Err(error)) => error,
            Err(_) => unknown(),
        };
        if work.intent.automatic.is_none() {
            // A manual attempt is never cancelled here: the next sweep that
            // finds its source held resumes it again, and an operator retry
            // of the same id still works.
            runtime.split_observation(source, id, "retry_pending", Some(error.code));
            return;
        }
        work.failures += 1;
        work.cancel = work.failures >= MAX_CONSECUTIVE_FAILURES
            || !matches!(
                error.code,
                ErrorCode::StorageUnavailable | ErrorCode::Overloaded | ErrorCode::OutcomeUnknown
            );
        runtime.split_observation(source, id, "retry_pending", Some(error.code));
        self.active = Some(work);
    }
}

async fn select_work(
    runtime: &WorkerRuntime,
    client: &SplitClient,
    policy: AutomaticSplitPolicy,
    ready: ReadyMembership,
    placement: &SplitSourcePlacement,
    epoch: ShardEpoch,
    bytes: u64,
    largest_image: usize,
    pivot: Option<protocol::Key>,
) -> Result<Option<Work>, ProtocolError> {
    let authority = ready.view.authority();
    let source = &placement.record.identity;
    for slot in 1..=AUTOMATIC_ATTEMPTS {
        let origin = AutomaticSplitOrigin { epoch, slot };
        let split_id = origin.id(source);
        if let Some(attempt) = client.job(split_id).await? {
            if attempt.intent.automatic != Some(origin) || attempt.intent.source != *source {
                continue;
            }
            match attempt.closed {
                None => {
                    runtime.clear_split_deficit(source.group_id);
                    let selected = if attempt.children().is_ok() || attempt.authority != authority {
                        attempt.placement().cloned()
                    } else {
                        match client
                            .check_capacity(
                                placement,
                                authority,
                                policy,
                                bytes,
                                attempt.intent.page_bytes,
                            )
                            .await
                        {
                            Ok(SplitCapacity::Ready(selected)) => Some(selected),
                            Ok(SplitCapacity::Disk { .. }) => {
                                runtime.split_observation(
                                    source.group_id,
                                    Some(split_id),
                                    "capacity_pending",
                                    Some(ErrorCode::LimitExceeded),
                                );
                                return Ok(None);
                            }
                            Err(error) => {
                                runtime.split_observation(
                                    source.group_id,
                                    Some(split_id),
                                    "capacity_pending",
                                    Some(error.code),
                                );
                                return Ok(None);
                            }
                        }
                    };
                    return Ok(Some(Work {
                        intent: attempt.intent.clone(),
                        placement: selected,
                        failures: 0,
                        cancel: attempt.authority != authority,
                    }));
                }
                Some(TxnDecision::Commit { .. }) => return Ok(None),
                Some(TxnDecision::Abort { .. }) => continue,
            }
        }
        let Some(boundary) = pivot.clone() else {
            runtime.clear_split_deficit(source.group_id);
            runtime.split_observation(source.group_id, None, "no_interior_pivot", None);
            return Ok(None);
        };
        let page_bytes = largest_image.max(MIN_IMAGE_PAGE_BYTES);
        if page_bytes > crate::oltp::mvcc::MAX_FIXED_COMMAND_BYTES - (64 << 10) {
            runtime.clear_split_deficit(source.group_id);
            runtime.split_observation(
                source.group_id,
                None,
                "capacity_pending",
                Some(ErrorCode::LimitExceeded),
            );
            return Ok(None);
        }
        let intent = SplitIntent {
            split_id,
            source: source.clone(),
            boundary,
            page_bytes,
            automatic: Some(origin),
        };
        intent.validate()?;
        if let Some(identity) = runtime.durable_lookup(split_id).await? {
            // A lost reservation reply must never bind this exact UUID to a
            // different pivot, including after a full process restart.
            if !matches!(identity, IdentityLookup::Reserved(reserved)
                if reserved.original_run == authority.run_id && reserved.digest == intent.digest()?)
            {
                continue;
            }
        }
        let selected = match client
            .check_capacity(placement, authority, policy, bytes, page_bytes)
            .await
        {
            Ok(SplitCapacity::Ready(selected)) => {
                runtime.clear_split_deficit(source.group_id);
                selected
            }
            Ok(SplitCapacity::Disk {
                required_bytes,
                pools,
            }) => {
                if placement.voters.len() == 3 {
                    runtime
                        .automatic_split
                        .lock()
                        .map_err(|_| unknown())?
                        .deficit = Some(SplitDeficit {
                        ready,
                        source: placement.clone(),
                        epoch,
                        observed_committed_bytes: bytes,
                        required_bytes,
                        pools,
                        recorded_at: nabedra_rt::time::now_std(),
                    });
                }
                runtime.split_observation(
                    source.group_id,
                    None,
                    "capacity_pending",
                    Some(ErrorCode::LimitExceeded),
                );
                return Ok(None);
            }
            Err(error) => {
                runtime.clear_split_deficit(source.group_id);
                runtime.split_observation(
                    source.group_id,
                    None,
                    "capacity_pending",
                    Some(error.code),
                );
                return Ok(None);
            }
        };
        return Ok(Some(Work {
            intent,
            placement: Some(selected),
            failures: 0,
            cancel: false,
        }));
    }
    runtime.split_observation(
        source.group_id,
        None,
        "attempts_exhausted",
        Some(ErrorCode::Conflict),
    );
    Ok(None)
}

/// Review F4: an automatic attempt of this owner term without bound children
/// still needs select_work's capacity check, which fixes its placement before
/// Bind; resuming it here with no placement would skip that check. Its source
/// is still observed (a paused source reports its usage), so select_work
/// resumes it when the threshold holds.
fn leave_to_select_work(attempt: &SplitAttempt, authority: Authority) -> bool {
    attempt.intent.automatic.is_some()
        && attempt.authority == authority
        && attempt.children().is_err()
}

fn threshold_crossed(bytes: u64, policy: AutomaticSplitPolicy) -> bool {
    bytes > policy.shard_target_bytes
}

#[cfg(test)]
#[test]
fn o16_o21_o28_c03_c04_m01_a01_split_threshold_and_attempt_identity() {
    let policy = AutomaticSplitPolicy {
        shard_target_bytes: 100,
        minimum_free_bytes: 1,
    };
    assert!(policy.validate().is_ok());
    assert!(!threshold_crossed(99, policy));
    assert!(!threshold_crossed(100, policy));
    assert!(threshold_crossed(101, policy));
    assert!(
        AutomaticSplitPolicy {
            shard_target_bytes: u64::MAX,
            ..policy
        }
        .validate()
        .is_err()
    );
    let source = GroupIdentity {
        cluster_id: Uuid::from_u128(1),
        database: Text::new("split".to_owned()).unwrap(),
        kind: GroupKind::Data {
            table_id: Uuid::from_u128(2),
            shard_id: Uuid::from_u128(3),
        },
        group_id: Uuid::from_u128(4),
        group_format_version: 1,
    };
    let epoch = ShardEpoch {
        schema: 7,
        routing: 11,
    };
    let origin = AutomaticSplitOrigin { epoch, slot: 1 };
    let intent = SplitIntent {
        split_id: origin.id(&source),
        source: source.clone(),
        boundary: protocol::Key::Int64(2),
        page_bytes: MIN_IMAGE_PAGE_BYTES,
        automatic: Some(origin),
    };
    assert!(intent.validate().is_ok());
    for changed in [
        AutomaticSplitOrigin { slot: 2, ..origin },
        AutomaticSplitOrigin {
            epoch: ShardEpoch {
                routing: 12,
                ..epoch
            },
            ..origin
        },
    ] {
        assert_ne!(changed.id(&source), intent.split_id);
        assert!(
            SplitIntent {
                automatic: Some(changed),
                ..intent.clone()
            }
            .validate()
            .is_err()
        );
    }
}

async fn settle(
    runtime: &WorkerRuntime,
    client: &SplitClient,
    intent: &SplitIntent,
    authority: Authority,
) -> Result<Option<SplitResult>, ProtocolError> {
    nabedra_rt::time::timeout(STEP_DEADLINE, async {
        let gate = runtime.gate(intent.split_id)?;
        let _actor = gate.lock().await;
        let current = runtime
            .services
            .control
            .current(&runtime.services.scope)
            .await?;
        if current.owner != runtime.services.local || current.ready.view.authority() != authority {
            return Err(unavailable());
        }
        let Some(attempt) = client.job(intent.split_id).await? else {
            return Ok(None);
        };
        if attempt.intent != *intent || attempt.intent.automatic.is_none() {
            return Err(unknown());
        }
        if let Some(closed) = attempt.closed {
            return match closed {
                TxnDecision::Commit { commit_ts } => Ok(Some(SplitResult::Published {
                    at: commit_ts,
                    children: attempt.children()?.clone().map(|child| child.target),
                })),
                TxnDecision::Abort { .. } => Ok(None),
            };
        }
        let error = failure(
            ErrorCode::StorageUnavailable,
            "automatic shard split stopped before publication",
        );
        let decision = runtime.transactions.recover(authority, attempt.txn).await?;
        let result = if let Some(decision) = decision {
            runtime
                .settle_split(client, &attempt, authority, decision)
                .await
        } else if runtime
            .services
            .status
            .inspect_transaction(attempt.txn)
            .await?
            .is_some()
        {
            let decision = runtime
                .services
                .status
                .abort(authority, attempt.txn, AbortReason::ResourceLimit, error)
                .await?;
            runtime
                .settle_split(client, &attempt, authority, decision)
                .await
        } else {
            runtime
                .services
                .status
                .terminal_failure(authority, attempt.request, error)
                .await?;
            let proof = runtime
                .services
                .status
                .failure_settled_proof(attempt.txn, attempt.request)
                .await?;
            runtime
                .settle_split_proof(client, &attempt, authority, proof)
                .await
        };
        match result {
            Ok(result) => Ok(Some(result)),
            Err(error) => {
                let closed = client
                    .job(intent.split_id)
                    .await?
                    .and_then(|attempt| attempt.closed);
                if matches!(closed, Some(TxnDecision::Abort { .. })) {
                    Ok(None)
                } else {
                    Err(error)
                }
            }
        }
    })
    .await
    .map_err(|_| unknown())?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oltp::mvcc::store::{
        KeySpan,
        lifecycle::{GuardMode, LifecycleBinding, VerifiedLifecycleGuard},
        lineage::SplitBinding,
    };
    use crate::oltp_replication::authority::timestamp::split::ImageCut;

    const EPOCH: ShardEpoch = ShardEpoch {
        schema: 7,
        routing: 11,
    };

    fn source() -> GroupIdentity {
        GroupIdentity {
            cluster_id: Uuid::from_u128(1),
            database: Text::new("split".to_owned()).unwrap(),
            kind: GroupKind::Data {
                table_id: Uuid::from_u128(2),
                shard_id: Uuid::from_u128(3),
            },
            group_id: Uuid::from_u128(4),
            group_format_version: 1,
        }
    }

    fn authority(owner_term: u64) -> Authority {
        Authority {
            run_id: Uuid::from_u128(5),
            owner_term,
        }
    }

    /// An open attempt on `source()` opened under `opened_by`: automatic in
    /// `slot` of `EPOCH`, or manual for `None`, with its children bound or not.
    fn attempt(opened_by: Authority, slot: Option<u8>, bound: bool) -> SplitAttempt {
        let source = source();
        let automatic = slot.map(|slot| AutomaticSplitOrigin { epoch: EPOCH, slot });
        let split_id = automatic.map_or(Uuid::from_u128(6), |origin| origin.id(&source));
        let intent = SplitIntent {
            split_id,
            source: source.clone(),
            boundary: protocol::Key::Int64(2),
            page_bytes: MIN_IMAGE_PAGE_BYTES,
            automatic,
        };
        let request = RequestBinding {
            run_id: opened_by.run_id,
            request_id: split_id,
            digest: intent.digest().unwrap(),
            accepted_context: Digest::from_bytes([1; 32]),
        };
        let txn = TxnId {
            request_id: split_id,
            attempt: 1,
        };
        let read_ts = Timestamp::new(10).unwrap();
        let pin = StoragePin {
            pin_id: Uuid::from_u128(7),
            read_ts,
            allocator_epoch: 1,
        };
        let catalog_group_id = Uuid::from_u128(8);
        let image = bound.then(|| {
            let guard = VerifiedLifecycleGuard::model_for_test(
                LifecycleBinding {
                    guard_id: Uuid::from_u128(9),
                    catalog_group_id,
                    table_id: Uuid::from_u128(2),
                    lifecycle_version: None,
                    schema_epoch: EPOCH.schema,
                    txn,
                    request,
                    read_ts,
                    authority: opened_by,
                },
                GuardMode::Exclusive,
            )
            .into_sealed();
            let children = std::array::from_fn(|index| SplitBinding {
                split_id,
                source: source.clone(),
                source_schema_epoch: EPOCH.schema,
                source_routing_epoch: EPOCH.routing,
                source_span: KeySpan {
                    start: None,
                    end: None,
                },
                target: GroupIdentity {
                    group_id: Uuid::from_u128(10 + index as u128),
                    ..source.clone()
                },
                target_span: if index == 0 {
                    KeySpan {
                        start: None,
                        end: Some(intent.boundary.clone()),
                    }
                } else {
                    KeySpan {
                        start: Some(intent.boundary.clone()),
                        end: None,
                    }
                },
                read_ts,
                guard: guard.clone(),
            });
            ImageCut {
                pin,
                guard,
                children: Some(children),
                placement: None,
            }
        });
        SplitAttempt {
            intent,
            authority: opened_by,
            catalog_group_id,
            request,
            txn,
            snapshot: pin,
            image,
            closed: None,
        }
    }

    #[test]
    fn o16_o21_review_f4_only_unbound_automatic_attempts_of_this_term_are_left_to_select_work() {
        let current = authority(3);
        let older = authority(2);
        let other_run = Authority {
            run_id: Uuid::from_u128(99),
            ..current
        };
        // Unbound and automatic in this owner term: select_work's capacity
        // check fixes its placement before Bind.
        assert!(leave_to_select_work(&attempt(current, Some(1), false), current));
        // Cut (the source may be paused) but not bound: still unbound.
        let mut cut = attempt(current, Some(3), true);
        cut.image.as_mut().unwrap().children = None;
        assert!(cut.children().is_err() && cut.image.is_some());
        assert!(leave_to_select_work(&cut, current));
        // Bound: its placement is fixed, resume_open resumes it with it.
        assert!(!leave_to_select_work(&attempt(current, Some(1), true), current));
        // Another owner term or run: resume_open cancels it.
        for opened_by in [older, other_run] {
            for bound in [false, true] {
                assert!(!leave_to_select_work(&attempt(opened_by, Some(2), bound), current));
            }
        }
        // Manual: select_work never selects it, resume_open resumes it.
        for opened_by in [current, older] {
            for bound in [false, true] {
                assert!(!leave_to_select_work(&attempt(opened_by, None, bound), current));
            }
        }
    }
}
