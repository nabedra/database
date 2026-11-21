use super::{split::SplitResult, *};
use crate::{
    oltp::{failure, mvcc::store::split_usage::ShardEpoch},
    oltp_replication::{
        authority::{
            control::{GroupRecord, InventoryPage, ReplicaMove},
            timestamp::split::{
                AUTOMATIC_ATTEMPTS, AutomaticSplitOrigin, SplitIntent, SplitPlacement,
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
}

impl WorkerRuntime {
    pub(in crate::oltp_runtime) fn spawn_automatic_split(
        self: &Arc<Self>,
        policy: AutomaticSplitPolicy,
    ) -> Result<tokio::task::JoinHandle<()>, ProtocolError> {
        policy.validate()?;
        let client = self.services.split.clone().ok_or_else(unavailable)?;
        let owner = self
            .services
            .shared
            .reserve_scratch(ByteClass::Data, 128 << 10)?;
        self.automatic_split
            .lock()
            .map_err(|_| unknown())?
            .target_bytes = Some(policy.shard_target_bytes);
        let weak = Arc::downgrade(self);
        Ok(tokio::spawn(async move {
            let _owner = owner;
            let mut authority = None;
            let mut state = OwnerLoop::default();
            loop {
                tokio::time::sleep(OBSERVATION_INTERVAL).await;
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
                let current = tokio::time::timeout(
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
                    let result = tokio::time::timeout(
                        Duration::from_secs(10),
                        state.observe(&runtime, &client, policy, current.ready),
                    )
                    .await;
                    if !matches!(result, Ok(Ok(()))) {
                        state.scan = None;
                    }
                }
            }
        }))
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
                sample.ready == ready && sample.recorded_at.elapsed() < DEFICIT_TTL
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
                    && sample.recorded_at.elapsed() < DEFICIT_TTL
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
        policy: AutomaticSplitPolicy,
        ready: ReadyMembership,
    ) -> Result<(), ProtocolError> {
        let authority = ready.view.authority();
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
                runtime.clear_split_deficit(source.group_id);
                if (1..=AUTOMATIC_ATTEMPTS)
                    .any(|slot| AutomaticSplitOrigin { epoch, slot }.id(source) == split_id)
                {
                    let attempt = client.job(split_id).await?.ok_or_else(unknown)?;
                    if attempt.closed.is_none()
                        && attempt.intent.source == *source
                        && attempt.intent.automatic.is_some()
                    {
                        self.active = Some(Work {
                            intent: attempt.intent.clone(),
                            placement: attempt.placement().cloned(),
                            failures: 0,
                            cancel: attempt.authority != authority,
                        });
                    }
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
        let error = match tokio::time::timeout(
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
                        recorded_at: Instant::now(),
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
    tokio::time::timeout(STEP_DEADLINE, async {
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
