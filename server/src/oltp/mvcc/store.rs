//! Fixed participant transitions. No executable request is persisted or rerun.
//! This private adapter is not a production coordinator: catalog lifecycle
//! capabilities and authenticated status/TSO services must precede admission.
use super::{Change, MvccKey, PreparedWrite};
use crate::{
    oltp::{
        failure,
        pool::{
            ApplyMeta, ChargedBytes, GroupStore, GroupTransaction, TransactionBudget, Transition,
        },
        recovery_unknown,
    },
    oltp_replication::{
        authority::status::{SealedStatusDecision, TimestampDecisionLocator},
        owned::{OwnedCommand, OwnedPayload},
        types::{
            AppliedPosition, Authority, CommitTs, Digest, GroupIdentity, GroupKind, ManifestRef,
            Page, ParticipantPlan, PreparedParticipant, ReadTs, RequestBinding, TxnDecision, TxnId,
        },
    },
    protocol::{ErrorCode, Key, ProtocolError},
};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use sha2::{Digest as _, Sha256};
use std::{cmp::Ordering, io, sync::Arc};
use uuid::Uuid;

pub(crate) mod archive;
pub(crate) mod direct;
mod handle;
pub(crate) mod image_archive;
pub(crate) mod index;
pub(crate) mod lifecycle;
pub(crate) mod lineage;
pub(crate) mod point;
pub(crate) mod restore;
pub(crate) mod scan;
pub(crate) mod scoped_queue;
pub(crate) mod split_closure;
pub(crate) mod split_usage;
pub(crate) mod timestamp_resolution;
pub(crate) use handle::{
    LatestRead, ParticipantEpochs, ParticipantFuture, ParticipantHandle, PointEvidenceRead,
    RemoteParticipant, ScheduledReadFuture, VerifiedParticipantResolution, VerifiedParticipantSeal,
};
const CONFIG: &[u8] = b"mvcc/config/v1";
const HEAD: u8 = b'h';
const VALUE: u8 = b'v';
const RESOLUTION_WITNESS: u8 = b'q';
pub(in crate::oltp) const INTENT: u8 = b'i';
const APPLY: TransactionBudget = TransactionBudget {
    scratch_bytes: (32 << 20) + (128 << 10),
    batch_bytes: (32 << 20) + (128 << 10),
    output_bytes: 64 << 10,
};
const METADATA_BYTES: usize = 8192;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct KeySpan {
    pub start: Option<Key>,
    pub end: Option<Key>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    catalog_group_id: Uuid,
    authority: Authority,
    schema_epoch: u64,
    routing_epoch: u64,
    span: KeySpan,
    gc_floor: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    split_source: Option<lineage::SplitSourceState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    split_child: Option<lineage::SplitChildVisibility>,
    /// The split holding the replicated direct-commit barrier, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    direct_pause: Option<Uuid>,
    /// Set once `data.direct.v1` reached this group; see
    /// `direct::DirectSchema`. A replica of an older version cannot read a
    /// config that carries it, which is why downgrade is unsupported then.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    direct_schema: Option<direct::DirectSchema>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ParticipantCommand {
    Scoped {
        scope: crate::oltp_replication::authority::control::ScopedCatalogRead,
        command: OwnedCommand<ParticipantCommand>,
    },
    FenceScopedQueue(scoped_queue::QueueFence),
    Fence {
        transition_id: Uuid,
        expected: Option<Authority>,
        next: Authority,
        schema_epoch: u64,
        routing_epoch: u64,
        span: KeySpan,
        catalog_group_id: Uuid,
    },
    Lifecycle(lifecycle::LifecycleCommand),
    SplitFreeze {
        authority: Authority,
        children: Box<[lineage::SplitBinding; 2]>,
    },
    SplitFinishSource {
        authority: Authority,
        split_id: Uuid,
        settled: crate::oltp_replication::authority::status::SealedSettlement,
    },
    SplitStage {
        authority: Authority,
        binding: Box<lineage::SplitBinding>,
    },
    SplitImportPage {
        authority: Authority,
        page: Box<lineage::SplitPage>,
    },
    PreparePage {
        authority: Authority,
        txn: TxnId,
        request: RequestBinding,
        manifest: ManifestRef,
        plan: ParticipantPlan,
        page_index: u64,
        previous_root: Digest,
        writes: Page<PreparedWrite>,
        guards: Page<lifecycle::SealedLifecycleGuard>,
    },
    Seal {
        authority: Authority,
        txn: TxnId,
        manifest: ManifestRef,
        result_digest: Digest,
    },
    PrepareTimestampPoint {
        locator: Box<TimestampDecisionLocator>,
        page: OwnedCommand<ParticipantCommand>,
    },
    SealTimestampPoint {
        authority: Authority,
        locator: Box<TimestampDecisionLocator>,
    },
    RejectTimestampPoint {
        locator: Box<TimestampDecisionLocator>,
        page: OwnedCommand<ParticipantCommand>,
    },
    ResolveTimestampPoint {
        authority: Authority,
        proof: Box<timestamp_resolution::SealedTimestampPointDecision>,
    },
    /// Resolve is paged. The immutable status proof authorizes every page; a
    /// partially resolved transaction remains discoverable through its heads.
    Resolve {
        authority: Authority,
        proof: SealedStatusDecision,
        max_items: u16,
    },
    /// One-entry direct commit. Proposed only after `data.direct.v1` is
    /// active: an older replica fails to decode this variant (fail closed).
    Direct(direct::DirectCommit),
    /// GC2: several requests' direct commits to one range in one entry.
    /// Proposed only while Control's `data.direct.batch` cluster feature is
    /// enabled: a replica of an older build cannot decode the variant.
    DirectBatch(direct::DirectBatch),
    /// Deletes dedup records older than a cutoff the proposer computed, so
    /// every replica deletes the same records. Gated like `Direct`.
    DirectExpire(direct::DirectExpire),
    /// Settles a request id at its anchor before it takes the queued path.
    /// Gated like `Direct`.
    DirectForward(direct::DirectForward),
    /// The replicated split barrier for direct commits. Gated like `Direct`.
    DirectPause(direct::DirectPause),
    /// The source's publication timestamp for a complete split child.
    /// Gated like `Direct`.
    ChildPublished(direct::ChildPublished),
    /// Activates direct commits on a Data group, or fences them before a
    /// DDL publishes (plan section 7). Gated like `Direct`.
    DirectSchema(direct::DirectSchemaFence),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ParticipantReply {
    ScopedQueueFenced(scoped_queue::QueueFenceReceipt),
    Fenced(crate::oltp_replication::types::GroupFenceReceipt),
    Lifecycle(lifecycle::LifecycleReply),
    SplitSourceFrozen(lineage::SplitSourceStatement),
    SplitSourceFinished(TxnDecision),
    SplitStaged,
    PreparedPage {
        next_page: u64,
        writes: u64,
    },
    Sealed(PreparedParticipant),
    Resolved {
        next_ordinal: u64,
        complete: bool,
    },
    SplitImported {
        pages: u64,
        images: u64,
        source_images: u64,
        root: Digest,
        complete: bool,
    },
    TimestampPointRejected(AppliedPosition),
    TimestampPointResolved {
        evidence: ResolutionEvidence,
        writes: u64,
    },
    Rejected(Fault),
    Direct(direct::DirectReply),
    DirectExpired {
        deleted: u64,
        complete: bool,
    },
    DirectPaused {
        split_id: Uuid,
        paused: bool,
    },
    ChildPublished {
        published_at: CommitTs,
    },
    DirectSchema(direct::DirectSchema),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Fault {
    NotInitialized,
    StaleAuthority,
    InvalidBinding,
    Conflict,
    RolledBack,
    Incomplete,
    AlreadyDecided,
    ResourceLimit,
    /// The schema or routing epoch differs from the group's: refresh the
    /// route. Emitted only for direct commits, which are gated by
    /// `data.direct.v1`.
    StaleEpoch,
    /// A split holds the range (its direct barrier, a frozen source or an
    /// unimported child). The executor falls back to the queued path or
    /// waits; a route refresh would return the same route. Direct only.
    SplitInProgress,
    /// A split freeze whose cut is below a direct commit that already
    /// applied: take the direct barrier and allocate a newer cut. Emitted
    /// only once direct commits exist.
    DirectCommitAboveCut,
    /// A direct-family command reached a group that `data.direct.v1` has not
    /// activated (the store-side gate). Emitted only to proposers that
    /// already speak the direct commands.
    DirectInactive,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Txn {
    /// Earlier records remain readable, but cannot claim complete archive
    /// coverage for effects whose preparation did not persist an audit.
    #[serde(default)]
    archive_audit_version: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    scoped_queue: Option<scoped_queue::QueueBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    point_home: Option<Digest>,
    request: RequestBinding,
    manifest: ManifestRef,
    plan: ParticipantPlan,
    root: Digest,
    next_page: u64,
    writes: u64,
    sealed: Option<PreparedParticipant>,
    decision: Option<TxnDecision>,
    resolved: u64,
}
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Head {
    version: Option<CommitTs>,
    intent: Option<TxnId>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Intent<'a> {
    read_ts: ReadTs,
    txn: TxnId,
    manifest: ManifestRef,
    change: IntentChange,
    #[serde(borrow)]
    value: Option<&'a RawValue>,
}
#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum IntentChange {
    Keep,
    Put,
    Delete,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum ReadReply<'a> {
    Value {
        version: Option<CommitTs>,
        value: Option<&'a RawValue>,
    },
    Blocked {
        txn: TxnId,
        manifest: ManifestRef,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResolutionEvidence {
    pub group: GroupIdentity,
    pub txn: TxnId,
    pub request: RequestBinding,
    pub manifest: ManifestRef,
    pub decision: TxnDecision,
    pub completed_at: AppliedPosition,
}
#[derive(Clone)]
pub(crate) struct ParticipantMachine {
    store: GroupStore,
    clock: Arc<direct::RangeClock>,
}
impl ParticipantMachine {
    pub(crate) fn new(store: GroupStore) -> Result<Self, ProtocolError> {
        if !matches!(
            store.identity().kind,
            GroupKind::Catalog | GroupKind::Data { .. }
        ) {
            return Err(invalid());
        }
        let config = store.resources().config();
        if config.apply_bytes_per_slot
            < APPLY.scratch_bytes + APPLY.batch_bytes + APPLY.output_bytes
            || config.query_reply_bytes < (8 << 20)
            || config.control_reply_bytes < APPLY.output_bytes
        {
            return Err(failure(
                ErrorCode::LimitExceeded,
                "participant apply profile is too small",
            ));
        }
        let clock = direct::RangeClock::for_store(&store);
        Ok(Self { store, clock })
    }
    pub(crate) fn store(&self) -> &GroupStore {
        &self.store
    }
    /// The range leader's read/commit clock, shared by every machine over
    /// this group store.
    pub(crate) fn range_clock(&self) -> &Arc<direct::RangeClock> {
        &self.clock
    }
    /// Rule 2 of the range clock for a snapshot read at `read_ts`. Catalog
    /// groups take no direct commits and skip it.
    pub(super) async fn observe_snapshot(&self, read_ts: ReadTs) -> Result<(), ProtocolError> {
        if matches!(self.store.identity().kind, GroupKind::Data { .. }) {
            self.clock.observe_snapshot(read_ts).await?;
        }
        Ok(())
    }
    pub(crate) async fn apply(
        &self,
        meta: ApplyMeta,
        command: OwnedCommand<ParticipantCommand>,
    ) -> Result<ChargedBytes, ProtocolError> {
        command.ensure_charged()?;
        let identity = self.store.identity().clone();
        let position = meta.position;
        let command_digest = meta.command_digest;
        let direct: Vec<CommitTs> = match command.payload()? {
            ParticipantCommand::Direct(direct) => vec![direct.commit_ts],
            ParticipantCommand::DirectBatch(batch) => batch
                .members
                .iter()
                .map(|member| member.commit_ts)
                .collect(),
            _ => Vec::new(),
        };
        let applied = self
            .store
            .transaction(meta, APPLY, move |tx| {
                apply(tx, &identity, position, command_digest, command.payload()?)
            })
            .await?;
        // The entry applied (or was rejected) durably: its pending entry, if
        // this node proposed it, leaves the range clock.
        self.clock.observe_applied(position);
        for commit_ts in direct {
            self.clock.settle(commit_ts);
        }
        Ok(applied)
    }
    /// The dedup record or forward marker for `request_id` at `anchor`
    /// (plan addendum 2026-11-19). Callers go through
    /// `ParticipantHandle::direct_dedup`, which takes the linearizable
    /// barrier first.
    pub(super) async fn direct_dedup(
        &self,
        table_id: Uuid,
        anchor: Key,
        request_id: Uuid,
    ) -> Result<Option<direct::DedupLookup>, ProtocolError> {
        let group = self.store.identity().clone();
        let raw = self
            .store
            .read(
                TransactionBudget {
                    scratch_bytes: 1 << 20,
                    batch_bytes: 0,
                    output_bytes: 64 << 10,
                },
                move |view| direct::read_dedup(view, &group, table_id, anchor, request_id),
            )
            .await?;
        serde_json::from_slice(raw.as_slice()).map_err(|_| recovery_unknown())
    }
    /// A read never guesses the status of an unresolved intent. The caller
    /// releases native capacity before consulting the current status leader.
    pub(crate) async fn read_at(
        &self,
        read_ts: ReadTs,
        key: MvccKey,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.observe_snapshot(read_ts).await?;
        let group = self.store.identity().clone();
        self.store
            .read(
                TransactionBudget {
                    scratch_bytes: 32 << 20,
                    batch_bytes: 0,
                    output_bytes: 8 << 20,
                },
                move |view| {
                    let raw = view.pinned(CONFIG)?.ok_or_else(invalid)?;
                    if raw.len() > METADATA_BYTES {
                        return Err(recovery_unknown());
                    }
                    let config: Config =
                        serde_json::from_slice(&raw).map_err(|_| recovery_unknown())?;
                    lineage::validate_read(&config, read_ts)?;
                    if !owns(&group, &config.span, &key) {
                        return Err(invalid());
                    }
                    let length = key_length(&key)?;
                    view.with_scratch_buffer(length + 9, |view, buffer| {
                        encode_key(&key, &mut buffer[1..1 + length]);
                        let end = length + 1;
                        buffer[0] = HEAD;
                        if let Some(raw) = view.pinned(&buffer[..end])? {
                            let head: Head =
                                serde_json::from_slice(&raw).map_err(|_| recovery_unknown())?;
                            if let Some(txn) = head.intent {
                                buffer[0] = INTENT;
                                let raw =
                                    view.pinned(&buffer[..end])?.ok_or_else(recovery_unknown)?;
                                let intent: Intent<'_> =
                                    serde_json::from_slice(&raw).map_err(|_| recovery_unknown())?;
                                if intent.txn != txn {
                                    return Err(recovery_unknown());
                                }
                                if intent.read_ts < read_ts {
                                    return view.encode(&ReadReply::Blocked {
                                        txn,
                                        manifest: intent.manifest,
                                    });
                                }
                            }
                        }
                        buffer[0] = VALUE;
                        buffer[end..].copy_from_slice(&(!read_ts.get()).to_be_bytes());
                        let Some(record) = view.first_from(&buffer[..end], buffer)? else {
                            return view.encode(&ReadReply::Value {
                                version: None,
                                value: None,
                            });
                        };
                        if record.key().len() != buffer.len() {
                            return Err(recovery_unknown());
                        }
                        let version = CommitTs::new(!u64::from_be_bytes(
                            record.key()[end..]
                                .try_into()
                                .map_err(|_| recovery_unknown())?,
                        ))
                        .map_err(|_| recovery_unknown())?;
                        if version > read_ts {
                            return Err(recovery_unknown());
                        }
                        #[derive(Deserialize)]
                        #[serde(deny_unknown_fields)]
                        struct Version<'a> {
                            #[serde(borrow)]
                            value: Option<&'a RawValue>,
                        }
                        let value: Version<'_> = serde_json::from_slice(record.value())
                            .map_err(|_| recovery_unknown())?;
                        view.encode(&ReadReply::Value {
                            version: Some(version),
                            value: value.value,
                        })
                    })
                },
            )
            .await
    }
}

fn apply(
    tx: &mut GroupTransaction<'_>,
    group: &GroupIdentity,
    position: AppliedPosition,
    command_digest: Digest,
    command: &ParticipantCommand,
) -> Result<Transition, ProtocolError> {
    let config = load::<Config>(tx, CONFIG)?;
    if let ParticipantCommand::Fence {
        transition_id,
        expected,
        next,
        schema_epoch,
        routing_epoch,
        span,
        catalog_group_id,
    } = command
    {
        let mut fence_key = [0; 17];
        fence_key[0] = b'F';
        fence_key[1..].copy_from_slice(transition_id.as_bytes());
        if let Some(receipt) =
            load::<crate::oltp_replication::types::GroupFenceReceipt>(tx, &fence_key)?
        {
            return if receipt.command_digest == command_digest {
                commit(tx, &ParticipantReply::Fenced(receipt))
            } else {
                reject(tx, Fault::InvalidBinding)
            };
        }
        if transition_id.is_nil() {
            return reject(tx, Fault::InvalidBinding);
        }
        if catalog_group_id.is_nil()
            || (matches!(group.kind, GroupKind::Catalog) && *catalog_group_id != group.group_id)
        {
            return reject(tx, Fault::InvalidBinding);
        }
        if next.validate().is_err()
            || *schema_epoch == 0
            || *routing_epoch == 0
            || !valid_span(span)
        {
            return reject(tx, Fault::InvalidBinding);
        }
        if let Some(old) = &config {
            if Some(old.authority) != *expected
                || old.authority.owner_term.checked_add(1) != Some(next.owner_term)
                || old.schema_epoch != *schema_epoch
                || old.routing_epoch != *routing_epoch
                || old.span != *span
                || old.catalog_group_id != *catalog_group_id
            {
                return reject(tx, Fault::StaleAuthority);
            }
        } else if expected.is_some() {
            return reject(tx, Fault::StaleAuthority);
        }
        let imported_floor = if config.is_none() {
            restore::validate_initial_fence(
                tx,
                group,
                catalog_group_id,
                schema_epoch,
                routing_epoch,
                span,
            )?
        } else {
            0
        };
        let next_config = Config {
            catalog_group_id: *catalog_group_id,
            authority: *next,
            schema_epoch: *schema_epoch,
            routing_epoch: *routing_epoch,
            span: span.clone(),
            gc_floor: config.as_ref().map_or(imported_floor, |c| c.gc_floor),
            split_source: config.as_ref().and_then(|c| c.split_source.clone()),
            split_child: config.as_ref().and_then(|c| c.split_child),
            // A fence keeps the split barrier: the split outlives the run.
            direct_pause: config.as_ref().and_then(|c| c.direct_pause),
            // A run fence keeps direct activation and the DDL schema fence.
            direct_schema: config.as_ref().and_then(|c| c.direct_schema),
        };
        if encoded_length(&next_config, METADATA_BYTES).is_err() {
            return reject(tx, Fault::ResourceLimit);
        }
        tx.put_serialized(CONFIG, &next_config)?;
        let receipt = crate::oltp_replication::types::GroupFenceReceipt {
            group: group.clone(),
            transition_id: *transition_id,
            expected: *expected,
            next: *next,
            applied_at: position,
            command_digest,
        };
        tx.put_serialized(&fence_key, &receipt)?;
        return commit(tx, &ParticipantReply::Fenced(receipt));
    }
    match command {
        ParticipantCommand::Direct(direct) => {
            return direct::apply(tx, group, config.as_ref(), direct);
        }
        ParticipantCommand::DirectBatch(batch) => {
            return direct::apply_batch(tx, group, config.as_ref(), batch);
        }
        ParticipantCommand::DirectExpire(expire) => {
            return direct::expire(tx, group, config.as_ref(), expire);
        }
        ParticipantCommand::DirectForward(forward) => {
            return direct::forward(tx, group, config.as_ref(), forward);
        }
        ParticipantCommand::DirectPause(pause) => {
            return direct::pause(tx, group, config, pause);
        }
        ParticipantCommand::ChildPublished(published) => {
            return direct::child_published(tx, group, config, published);
        }
        ParticipantCommand::DirectSchema(fence) => {
            return direct::schema(tx, group, config, fence);
        }
        _ => {}
    }
    let Some(config) = config else {
        return reject(tx, Fault::NotInitialized);
    };
    if let ParticipantCommand::FenceScopedQueue(fence) = command {
        return scoped_queue::apply_fence(tx, group, &config, position, *fence);
    }
    let (scope, command) = match command {
        ParticipantCommand::Scoped { scope, command } => {
            let command = command.payload()?;
            if command.is_direct() {
                return reject(tx, Fault::InvalidBinding);
            }
            if let Err(fault) = scoped_queue::check(tx, group, &config, *scope, command)? {
                return reject(tx, fault);
            }
            (Some(*scope), command)
        }
        _ => (None, command),
    };
    let queue_binding = scope.map(scoped_queue::QueueBinding::from_scope);
    let (point, rejecting, command) = match command {
        ParticipantCommand::PrepareTimestampPoint { locator, page }
        | ParticipantCommand::RejectTimestampPoint { locator, page } => {
            let page = page.payload()?;
            let Some(scope) = scope else {
                return reject(tx, Fault::InvalidBinding);
            };
            if !timestamp_resolution::matches_page_binding(locator, page)?
                || !timestamp_resolution::matches_scope(locator, scope)
            {
                return reject(tx, Fault::InvalidBinding);
            }
            (
                Some(locator.as_ref()),
                matches!(command, ParticipantCommand::RejectTimestampPoint { .. }),
                page,
            )
        }
        _ => (None, false, command),
    };
    if command.is_direct() {
        return reject(tx, Fault::InvalidBinding);
    }
    if let ParticipantCommand::Lifecycle(command) = command {
        return lifecycle::apply(tx, group, &config, position, command, queue_binding);
    }
    let authority = match command {
        ParticipantCommand::PreparePage { authority, .. }
        | ParticipantCommand::Seal { authority, .. }
        | ParticipantCommand::SealTimestampPoint { authority, .. }
        | ParticipantCommand::ResolveTimestampPoint { authority, .. }
        | ParticipantCommand::Resolve { authority, .. }
        | ParticipantCommand::SplitFreeze { authority, .. }
        | ParticipantCommand::SplitFinishSource { authority, .. }
        | ParticipantCommand::SplitStage { authority, .. }
        | ParticipantCommand::SplitImportPage { authority, .. } => *authority,
        _ => unreachable!(),
    };
    if config.authority != authority {
        return reject(tx, Fault::StaleAuthority);
    }
    match command {
        ParticipantCommand::SplitFreeze { children, .. } => {
            lineage::freeze_source(tx, group, &config, position, children)
        }
        ParticipantCommand::SplitFinishSource {
            split_id, settled, ..
        } => lineage::finish_source(tx, config, *split_id, settled),
        ParticipantCommand::SplitStage { binding, .. } => {
            lineage::stage_child(tx, group, config, binding)
        }
        ParticipantCommand::SplitImportPage { page, .. } => {
            lineage::apply(tx, group, config, position, command_digest, page)
        }
        ParticipantCommand::PreparePage {
            txn,
            request,
            manifest,
            plan,
            page_index,
            previous_root,
            writes,
            guards,
            ..
        } => {
            if config.split_source.is_some()
                || config
                    .split_child
                    .is_some_and(|child| !child.complete || plan.read_ts < child.read_ts)
            {
                return reject(tx, Fault::Incomplete);
            }
            if plan.validate().is_err()
                || &plan.group != group
                || plan.schema_epoch != config.schema_epoch
                || plan.routing_epoch != config.routing_epoch
                || plan.read_ts.get() < config.gc_floor
                || request.run_id != authority.run_id
                || request.request_id != txn.request_id
                || manifest.status_group_id.is_nil()
                || manifest.participant_count == 0
                || *page_index >= plan.page_count
                || writes.is_empty()
            {
                return reject(tx, Fault::InvalidBinding);
            }
            if !lifecycle::validate_guards(&config, group, *txn, request, plan, writes, guards)? {
                return reject(tx, Fault::InvalidBinding);
            }
            let root = page_root(*previous_root, *page_index, writes)?;
            if let Some(locator) = point {
                if root != plan.write_root
                    || !timestamp_resolution::eligible_config(&config, locator)
                    || !locator.binding.accepts_authority(authority)
                    || !owns(group, &config.span, &writes[0].key)
                {
                    return reject(tx, Fault::InvalidBinding);
                }
                if rejecting {
                    if !page_fits(txn, manifest, writes)? {
                        return reject(tx, Fault::ResourceLimit);
                    }
                    return timestamp_resolution::apply_rejection(
                        tx, locator, authority, position, &writes[0],
                    );
                }
            }
            if timestamp_resolution::completion_exists(tx, *txn)?
                || tx.pinned(&txn_key(b'b', *txn))?.is_some()
                || timestamp_resolution::rejection_exists(tx, *txn)?
            {
                return reject(tx, Fault::RolledBack);
            }
            let point_home = point.map(TimestampDecisionLocator::digest).transpose()?;
            let key = txn_key(b't', *txn);
            let old = load::<Txn>(tx, &key)?;
            let mut state = match old {
                Some(state)
                    if state.request == *request
                        && state.manifest == *manifest
                        && state.plan == *plan
                        && state.scoped_queue == queue_binding
                        && state.point_home == point_home =>
                {
                    state
                }
                Some(_) => return reject(tx, Fault::InvalidBinding),
                None => Txn {
                    archive_audit_version: Some(1),
                    scoped_queue: queue_binding,
                    point_home,
                    request: *request,
                    manifest: *manifest,
                    plan: plan.clone(),
                    root: initial_root(*txn, plan)?,
                    next_page: 0,
                    writes: 0,
                    sealed: None,
                    decision: None,
                    resolved: 0,
                },
            };
            if *page_index < state.next_page {
                return if tx
                    .pinned(&ordinal_key(b'p', *txn, *page_index))?
                    .as_ref()
                    .map(|v| v.as_ref())
                    == Some(root.as_bytes())
                {
                    commit(
                        tx,
                        &ParticipantReply::PreparedPage {
                            next_page: state.next_page,
                            writes: state.writes,
                        },
                    )
                } else {
                    reject(tx, Fault::InvalidBinding)
                };
            }
            if state.sealed.is_some() || state.decision.is_some() {
                return reject(tx, Fault::AlreadyDecided);
            }
            if *page_index != state.next_page
                || *previous_root != state.root
                || state
                    .writes
                    .checked_add(writes.len() as u64)
                    .is_none_or(|n| n > plan.write_count)
            {
                return reject(tx, Fault::InvalidBinding);
            }
            if !page_fits(txn, manifest, writes)? {
                return reject(tx, Fault::ResourceLimit);
            }
            for (offset, write) in writes.iter().enumerate() {
                if !owns(group, &config.span, &write.key)
                    || (offset != 0
                        && compare_keys(&writes[offset - 1].key, &write.key) != Ordering::Less)
                {
                    return reject(tx, Fault::InvalidBinding);
                }
                let length = key_length(&write.key)?;
                let fault = tx.with_scratch_buffer(length + 9, |tx, buffer| {
                    encode_key(&write.key, &mut buffer[1..1 + length]);
                    let end = length + 1;
                    if offset == 0 && state.writes != 0 {
                        let last = tx
                            .pinned(&ordinal_key(b'w', *txn, state.writes - 1))?
                            .ok_or_else(recovery_unknown)?;
                        if last.as_ref() >= &buffer[1..end] {
                            return Ok(Some(Fault::InvalidBinding));
                        }
                    }
                    buffer[0] = HEAD;
                    let head = load::<Head>(tx, &buffer[..end])?.unwrap_or_default();
                    if head.intent.is_some()
                        || head.version != write.expected_version
                        || head.version.is_some_and(|ts| ts > plan.read_ts)
                    {
                        return Ok(Some(Fault::Conflict));
                    }
                    buffer[0] = INTENT;
                    let value = match &write.change {
                        Change::Put(value) => Some(
                            serde_json::from_str::<&RawValue>(value.as_str())
                                .map_err(|_| invalid())?,
                        ),
                        _ => None,
                    };
                    let change = match write.change {
                        Change::Keep => IntentChange::Keep,
                        Change::Put(_) => IntentChange::Put,
                        Change::Delete => IntentChange::Delete,
                    };
                    tx.put_serialized(
                        &buffer[..end],
                        &Intent {
                            read_ts: plan.read_ts,
                            txn: *txn,
                            manifest: *manifest,
                            change,
                            value,
                        },
                    )?;
                    buffer[0] = HEAD;
                    tx.put_serialized(
                        &buffer[..end],
                        &Head {
                            version: head.version,
                            intent: Some(*txn),
                        },
                    )?;
                    tx.put(
                        &ordinal_key(b'w', *txn, state.writes + offset as u64),
                        &buffer[1..end],
                    )?;
                    #[derive(Serialize)]
                    struct Audit {
                        change: IntentChange,
                        expected_version: Option<CommitTs>,
                        value_digest: Option<Digest>,
                    }
                    tx.put_serialized(
                        &ordinal_key(b'a', *txn, state.writes + offset as u64),
                        &Audit {
                            change,
                            expected_version: write.expected_version,
                            value_digest: value.map(|value| {
                                Digest::from_bytes(Sha256::digest(value.get().as_bytes()).into())
                            }),
                        },
                    )?;
                    Ok(None)
                })?;
                if let Some(fault) = fault {
                    return reject(tx, fault);
                }
            }
            state.next_page += 1;
            state.writes += writes.len() as u64;
            state.root = root;
            tx.put(&ordinal_key(b'p', *txn, *page_index), root.as_bytes())?;
            tx.put_serialized(&key, &state)?;
            commit(
                tx,
                &ParticipantReply::PreparedPage {
                    next_page: state.next_page,
                    writes: state.writes,
                },
            )
        }
        ParticipantCommand::Seal { txn, manifest, result_digest, .. } => {
            seal(tx, *txn, *manifest, *result_digest, authority, position, scope, None)
        }
        ParticipantCommand::SealTimestampPoint { locator, .. } => {
            let Some(scope) = scope else {
                return reject(tx, Fault::InvalidBinding);
            };
            if !timestamp_resolution::matches_scope(locator, scope)
                || !locator.binding.accepts_authority(authority)
                || !timestamp_resolution::eligible_config(&config, locator)
            {
                return reject(tx, Fault::InvalidBinding);
            }
            seal(
                tx, locator.binding.txn, locator.manifest, locator.binding.result_digest,
                authority, position, Some(scope), Some(locator.digest()?),
            )
        }
        ParticipantCommand::ResolveTimestampPoint { proof, .. } => {
            timestamp_resolution::apply_resolution(tx, group, &config, position, scope, proof)
        }
        ParticipantCommand::Resolve {
            proof, max_items, ..
        } => {
            if *max_items == 0 || *max_items > 128 {
                return reject(tx, Fault::InvalidBinding);
            }
            let statement = proof.statement();
            // Validation binds the sealed status DTO even on an early ABORT.
            let txn = statement.txn;
            if timestamp_resolution::completion_exists(tx, txn)?
                || timestamp_resolution::rejection_exists(tx, txn)?
            {
                return reject(tx, Fault::InvalidBinding);
            }
            let key = txn_key(b't', txn);
            let witness_key = txn_key(RESOLUTION_WITNESS, txn);
            let known_digest = tx
                .pinned(&witness_key)?
                .map(|value| decode_resolution_witness(&value))
                .transpose()?;
            if known_digest.is_some_and(|digest| digest != proof.digest()) {
                return reject(tx, Fault::InvalidBinding);
            }
            let old = load::<Txn>(tx, &key)?;
            let Some(mut state) = old else {
                if known_digest.is_some() {
                    return Err(recovery_unknown());
                }
                proof.validate_for(txn, &statement.manifest, &statement.request)?;
                if matches!(statement.decision, TxnDecision::Commit { .. }) {
                    return reject(tx, Fault::Incomplete);
                }
                // Do not lose this marker when cleanup beats an in-flight page.
                let rollback = txn_key(b'b', txn);
                if let Some(existing) =
                    load::<crate::oltp_replication::types::DecisionStatement>(tx, &rollback)?
                {
                    if existing != *statement {
                        return reject(tx, Fault::InvalidBinding);
                    }
                } else {
                    tx.put_serialized(&rollback, statement)?;
                }
                if tx.pinned(&txn_key(b'd', txn))?.is_none() {
                    tx.put_serialized(
                        &txn_key(b'd', txn),
                        &ResolutionEvidence {
                            group: group.clone(),
                            txn,
                            request: statement.request,
                            manifest: statement.manifest,
                            decision: statement.decision,
                            completed_at: position,
                        },
                    )?;
                }
                return commit(
                    tx,
                    &ParticipantReply::Resolved {
                        next_ordinal: 0,
                        complete: true,
                    },
                );
            };
            if state.point_home.is_some() {
                return reject(tx, Fault::InvalidBinding);
            }
            // Cold recovery still uses the exact Status decision for a
            // transaction whose queue and in-memory request are gone.
            if (state.request.run_id == authority.run_id && state.scoped_queue != queue_binding)
                || scope.is_some_and(|scope| state.plan.read_ts != scope.read_ts)
            {
                return reject(tx, Fault::InvalidBinding);
            }
            proof.validate_for(txn, &state.manifest, &state.request)?;
            if known_digest.is_some()
                && (state.decision.is_none() || state.resolved == 0 || state.resolved > state.writes)
            {
                return Err(recovery_unknown());
            }
            if let Some(decision) = state.decision {
                if decision != statement.decision {
                    return reject(tx, Fault::InvalidBinding);
                }
            }
            if let TxnDecision::Commit { commit_ts } = statement.decision {
                if state.sealed.is_none() || commit_ts <= state.plan.read_ts {
                    return reject(tx, Fault::Incomplete);
                }
            } else {
                tx.put_serialized(&txn_key(b'b', txn), statement)?;
            }
            let until = state
                .resolved
                .saturating_add(*max_items as u64)
                .min(state.writes);
            resolve_writes(tx, &state, txn, until, statement.decision)?;
            if until == state.writes && tx.pinned(&txn_key(b'd', txn))?.is_none() {
                tx.put_serialized(
                    &txn_key(b'd', txn),
                    &ResolutionEvidence {
                        group: group.clone(),
                        txn,
                        request: statement.request,
                        manifest: statement.manifest,
                        decision: statement.decision,
                        completed_at: position,
                    },
                )?;
            }
            state.resolved = until;
            state.decision = Some(statement.decision);
            // Record the exact Status proof only while the participant has
            // more pages. It becomes visible atomically with this progress.
            if until == state.writes {
                if known_digest.is_some() {
                    tx.delete(&witness_key)?;
                }
            } else if known_digest.is_none() {
                tx.put(&witness_key, proof.digest().as_bytes())?;
            }
            tx.put_serialized(&key, &state)?;
            commit(
                tx,
                &ParticipantReply::Resolved {
                    next_ordinal: until,
                    complete: until == state.writes,
                },
            )
        }
        ParticipantCommand::Fence { .. }
        | ParticipantCommand::FenceScopedQueue(_)
        | ParticipantCommand::Scoped { .. }
        | ParticipantCommand::PrepareTimestampPoint { .. }
        | ParticipantCommand::RejectTimestampPoint { .. }
        | ParticipantCommand::Lifecycle(_)
        | ParticipantCommand::Direct(_)
        | ParticipantCommand::DirectBatch(_)
        | ParticipantCommand::DirectExpire(_)
        | ParticipantCommand::DirectForward(_)
        | ParticipantCommand::DirectPause(_)
        | ParticipantCommand::ChildPublished(_)
        | ParticipantCommand::DirectSchema(_) => unreachable!(),
    }
}

/// The page producer uses this same deterministic bound before proposing. The
/// state machine repeats it before candidate growth, returning a stable page
/// rejection instead of making a committed entry depend on replica memory.
fn seal(
    tx: &mut GroupTransaction<'_>,
    txn: TxnId,
    manifest: ManifestRef,
    result_digest: Digest,
    authority: Authority,
    position: AppliedPosition,
    scope: Option<crate::oltp_replication::authority::control::ScopedCatalogRead>,
    point_home: Option<Digest>,
) -> Result<Transition, ProtocolError> {
    if tx.pinned(&txn_key(b'b', txn))?.is_some()
        || timestamp_resolution::rejection_exists(tx, txn)?
    {
        return reject(tx, Fault::RolledBack);
    }
    let key = txn_key(b't', txn);
    let Some(mut state) = load::<Txn>(tx, &key)? else {
        return reject(tx, Fault::Incomplete);
    };
    if state.manifest != manifest
        || state.point_home != point_home
        || state.scoped_queue != scope.map(scoped_queue::QueueBinding::from_scope)
        || scope.is_some_and(|scope| {
            state.request != scope.request || state.plan.read_ts != scope.read_ts
        })
    {
        return reject(tx, Fault::InvalidBinding);
    }
    if let Some(sealed) = state.sealed {
        return if sealed.result_digest == result_digest {
            commit(tx, &ParticipantReply::Sealed(sealed))
        } else {
            reject(tx, Fault::InvalidBinding)
        };
    }
    if state.decision.is_some()
        || state.next_page != state.plan.page_count
        || state.writes != state.plan.write_count
        || state.root != state.plan.write_root
    {
        return reject(tx, Fault::Incomplete);
    }
    let sealed = PreparedParticipant {
        txn,
        manifest,
        participant: state.plan.clone(),
        authority,
        sealed_at: position,
        result_digest,
    };
    state.sealed = Some(sealed.clone());
    tx.put_serialized(&key, &state)?;
    commit(tx, &ParticipantReply::Sealed(sealed))
}

fn resolve_writes(
    tx: &mut GroupTransaction<'_>,
    state: &Txn,
    txn: TxnId,
    until: u64,
    decision: TxnDecision,
) -> Result<(), ProtocolError> {
    for ordinal in state.resolved..until {
        let logical = tx
            .pinned(&ordinal_key(b'w', txn, ordinal))?
            .ok_or_else(recovery_unknown)?;
        let length = logical.len();
        tx.with_scratch_buffer(length + 9, |tx, buffer| {
            buffer[1..1 + length].copy_from_slice(&logical);
            let end = length + 1;
            buffer[0] = HEAD;
            let head = load::<Head>(tx, &buffer[..end])?.ok_or_else(recovery_unknown)?;
            if head.intent != Some(txn) {
                return Err(recovery_unknown());
            }
            buffer[0] = INTENT;
            let encoded = tx.pinned(&buffer[..end])?.ok_or_else(recovery_unknown)?;
            if encoded.len() > super::MAX_FIXED_COMMAND_BYTES {
                return Err(recovery_unknown());
            }
            // Borrow row JSON from the pinned intent; only fixed
            // metadata is decoded, never the application row tree.
            let intent: Intent<'_> =
                serde_json::from_slice(&encoded).map_err(|_| recovery_unknown())?;
            if intent.txn != txn || intent.manifest != state.manifest {
                return Err(recovery_unknown());
            }
            let mut version = head.version;
            if let TxnDecision::Commit { commit_ts } = decision {
                if !matches!(intent.change, IntentChange::Keep) {
                    buffer[0] = VALUE;
                    buffer[end..].copy_from_slice(&(!commit_ts.get()).to_be_bytes());
                    #[derive(Serialize)]
                    struct Version<'a> {
                        value: Option<&'a RawValue>,
                    }
                    let value = match intent.change {
                        IntentChange::Put => {
                            Some(intent.value.ok_or_else(recovery_unknown)?)
                        }
                        IntentChange::Delete => None,
                        _ => unreachable!(),
                    };
                    tx.put_serialized(buffer, &Version { value })?;
                    version = Some(commit_ts);
                }
            }
            buffer[0] = INTENT;
            tx.delete(&buffer[..end])?;
            buffer[0] = HEAD;
            if version.is_none() {
                tx.delete(&buffer[..end])?
            } else {
                tx.put_serialized(
                    &buffer[..end],
                    &Head {
                        version,
                        intent: None,
                    },
                )?;
            }
            Ok(())
        })?;
    }
    Ok(())
}

pub(crate) fn page_fits(
    txn: &TxnId,
    manifest: &ManifestRef,
    writes: &[PreparedWrite],
) -> Result<bool, ProtocolError> {
    let mut budget = PreparePageBudget::new(txn, manifest);
    for write in writes {
        if !budget.try_add(write)? {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) struct PreparePageBudget<'a> {
    txn: &'a TxnId,
    manifest: &'a ManifestRef,
    batch_bytes: usize,
}
impl<'a> PreparePageBudget<'a> {
    pub(crate) fn new(txn: &'a TxnId, manifest: &'a ManifestRef) -> Self {
        Self {
            txn,
            manifest,
            // Transaction/page metadata, response and framing.
            batch_bytes: 16 << 10,
        }
    }

    pub(crate) fn try_add(&mut self, write: &PreparedWrite) -> Result<bool, ProtocolError> {
        let length = key_length(&write.key)?;
        let value = match &write.change {
            Change::Put(v) => {
                Some(serde_json::from_str::<&RawValue>(v.as_str()).map_err(|_| invalid())?)
            }
            _ => None,
        };
        let change = match write.change {
            Change::Keep => IntentChange::Keep,
            Change::Put(_) => IntentChange::Put,
            Change::Delete => IntentChange::Delete,
        };
        let intent = encoded_length(
            &Intent {
                read_ts: ReadTs::new(u64::MAX).map_err(|_| invalid())?,
                txn: *self.txn,
                manifest: *self.manifest,
                change,
                value,
            },
            super::MAX_FIXED_COMMAND_BYTES,
        )?;
        // Intent key, latest-head key, ordinal's logical-key value. Head
        // metadata, three scoped native prefixes and record framing fit512B.
        let batch = self
            .batch_bytes
            .checked_add(length.checked_mul(3).ok_or_else(invalid)?)
            .and_then(|n| n.checked_add(intent))
            .and_then(|n| n.checked_add(1024))
            .ok_or_else(invalid)?;
        // One temporary logical key, the reused native key, and one temporary
        // intent encoding must also coexist. Metadata is bounded separately.
        let scratch = length
            .checked_mul(2)
            .and_then(|n| n.checked_add(intent))
            .and_then(|n| n.checked_add(256 << 10))
            .ok_or_else(invalid)?;
        if batch > APPLY.batch_bytes || scratch > APPLY.scratch_bytes {
            return Ok(false);
        }
        self.batch_bytes = batch;
        Ok(true)
    }
}
pub(crate) fn encoded_length(value: &impl Serialize, limit: usize) -> Result<usize, ProtocolError> {
    struct Count {
        bytes: usize,
        limit: usize,
    }
    impl io::Write for Count {
        fn write(&mut self, b: &[u8]) -> io::Result<usize> {
            self.bytes = self
                .bytes
                .checked_add(b.len())
                .filter(|n| *n <= self.limit)
                .ok_or_else(|| io::Error::other("fixed metadata bound"))?;
            Ok(b.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count { bytes: 0, limit };
    serde_json::to_writer(&mut count, value).map_err(|_| invalid())?;
    Ok(count.bytes)
}
impl std::fmt::Display for ParticipantCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Scoped { .. } => "participant-scoped",
            Self::FenceScopedQueue(_) => "participant-scoped-queue-fence",
            Self::Fence { .. } => "participant-fence",
            Self::Lifecycle(_) => "catalog-lifecycle",
            Self::SplitFreeze { .. } => "participant-split-freeze",
            Self::SplitFinishSource { .. } => "participant-split-finish-source",
            Self::SplitStage { .. } => "participant-split-stage",
            Self::SplitImportPage { .. } => "participant-split-image-page",
            Self::PreparePage { .. } => "participant-prepare-page",
            Self::Seal { .. } => "participant-seal",
            Self::PrepareTimestampPoint { .. } => "participant-prepare-timestamp-point",
            Self::SealTimestampPoint { .. } => "participant-seal-timestamp-point",
            Self::RejectTimestampPoint { .. } => "participant-reject-timestamp-point",
            Self::ResolveTimestampPoint { .. } => "participant-resolve-timestamp-point",
            Self::Resolve { .. } => "participant-resolve",
            Self::Direct(_) => "participant-direct",
            Self::DirectBatch(_) => "participant-direct-batch",
            Self::DirectExpire(_) => "participant-direct-expire",
            Self::DirectForward(_) => "participant-direct-forward",
            Self::DirectPause(_) => "participant-direct-pause",
            Self::ChildPublished(_) => "participant-child-published",
            Self::DirectSchema(_) => "participant-direct-schema",
        })
    }
}

pub(crate) fn initial_root(txn: TxnId, plan: &ParticipantPlan) -> Result<Digest, ProtocolError> {
    #[derive(Serialize)]
    struct Initial<'a> {
        group: &'a GroupIdentity,
        txn: TxnId,
        read_ts: ReadTs,
        schema_epoch: u64,
        routing_epoch: u64,
    }
    hash(
        b"nabedra/mvcc-write-root/v1\0",
        &Initial {
            group: &plan.group,
            txn,
            read_ts: plan.read_ts,
            schema_epoch: plan.schema_epoch,
            routing_epoch: plan.routing_epoch,
        },
    )
}
pub(crate) fn page_root(
    previous: Digest,
    index: u64,
    writes: &[PreparedWrite],
) -> Result<Digest, ProtocolError> {
    #[derive(Serialize)]
    struct Part<'a> {
        previous: Digest,
        index: u64,
        writes: &'a [PreparedWrite],
    }
    hash(
        b"nabedra/mvcc-write-page/v1\0",
        &Part {
            previous,
            index,
            writes,
        },
    )
}
fn hash(domain: &[u8], value: &impl Serialize) -> Result<Digest, ProtocolError> {
    struct Writer(Sha256);
    impl io::Write for Writer {
        fn write(&mut self, b: &[u8]) -> io::Result<usize> {
            self.0.update(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer(Sha256::new());
    writer.0.update(domain);
    serde_json::to_writer(&mut writer, value).map_err(|_| invalid())?;
    Ok(Digest::from_bytes(writer.0.finalize().into()))
}
fn load<T: serde::de::DeserializeOwned>(
    tx: &mut GroupTransaction<'_>,
    key: &[u8],
) -> Result<Option<T>, ProtocolError> {
    tx.pinned(key)?
        .map(|bytes| {
            if bytes.len() > METADATA_BYTES {
                return Err(recovery_unknown());
            }
            serde_json::from_slice(&bytes).map_err(|_| recovery_unknown())
        })
        .transpose()
}
fn commit(
    tx: &mut GroupTransaction<'_>,
    reply: &ParticipantReply,
) -> Result<Transition, ProtocolError> {
    Ok(Transition::Commit(tx.encode(reply)?))
}
fn reject(tx: &mut GroupTransaction<'_>, fault: Fault) -> Result<Transition, ProtocolError> {
    Ok(Transition::Reject(
        tx.encode(&ParticipantReply::Rejected(fault))?,
    ))
}
fn invalid() -> ProtocolError {
    failure(
        ErrorCode::ValidationError,
        "invalid fixed MVCC participant binding",
    )
}
fn decode_resolution_witness(bytes: &[u8]) -> Result<Digest, ProtocolError> {
    bytes
        .try_into()
        .map(Digest::from_bytes)
        .map_err(|_| recovery_unknown())
}
fn txn_key(tag: u8, txn: TxnId) -> [u8; 25] {
    let mut key = [0; 25];
    key[0] = tag;
    key[1..17].copy_from_slice(txn.request_id.as_bytes());
    key[17..].copy_from_slice(&txn.attempt.to_be_bytes());
    key
}
fn ordinal_key(tag: u8, txn: TxnId, ordinal: u64) -> [u8; 33] {
    let mut key = [0; 33];
    key[..25].copy_from_slice(&txn_key(tag, txn));
    key[25..].copy_from_slice(&ordinal.to_be_bytes());
    key
}
fn compare_primary(a: &Key, b: &Key) -> Ordering {
    match (a, b) {
        (Key::Int64(a), Key::Int64(b)) => a.cmp(b),
        (Key::Utf8(a), Key::Utf8(b)) => a.cmp(b),
        (Key::Int64(_), Key::Utf8(_)) => Ordering::Less,
        _ => Ordering::Greater,
    }
}
fn valid_span(span: &KeySpan) -> bool {
    match (&span.start, &span.end) {
        (Some(a), Some(b)) => compare_primary(a, b) == Ordering::Less,
        _ => true,
    }
}
fn owns(group: &GroupIdentity, span: &KeySpan, key: &MvccKey) -> bool {
    if let MvccKey::IndexEntry { values, .. } = key {
        if values.iter().any(|v| !v.valid()) {
            return false;
        }
    }

    match (&group.kind, key) {
        (
            GroupKind::Data { table_id, .. },
            MvccKey::Row {
                table_id: table,
                primary_key,
            }
            | MvccKey::IndexEntry {
                table_id: table,
                primary_key,
                ..
            },
        ) => {
            table == table_id
                && span
                    .start
                    .as_ref()
                    .is_none_or(|k| compare_primary(k, primary_key) != Ordering::Greater)
                && span
                    .end
                    .as_ref()
                    .is_none_or(|k| compare_primary(primary_key, k) == Ordering::Less)
        }
        (
            GroupKind::Catalog,
            MvccKey::Row { .. } | MvccKey::IndexEntry { .. } | MvccKey::DirectDedup { .. },
        ) => false,
        (GroupKind::Catalog, _) => true,
        _ => false,
    }
}
/// `owns` for committed images (split pages, backups, restore). A dedup
/// record is never read, prepared or written through the generic key paths,
/// but its image travels with its anchor row.
fn owns_image(group: &GroupIdentity, span: &KeySpan, key: &MvccKey) -> bool {
    match key {
        MvccKey::DirectDedup {
            table_id,
            anchor,
            request_id,
        } => {
            !request_id.is_nil()
                && owns(
                    group,
                    span,
                    &MvccKey::Row {
                        table_id: *table_id,
                        primary_key: anchor.clone(),
                    },
                )
        }
        _ => owns(group, span, key),
    }
}
fn components(key: &MvccKey) -> (u8, Uuid, Uuid, Option<&str>, Option<&Key>) {
    match key {
        MvccKey::Row {
            table_id,
            primary_key,
        } => (1, *table_id, Uuid::nil(), None, Some(primary_key)),
        MvccKey::CatalogName { name } => (2, Uuid::nil(), Uuid::nil(), Some(name.as_str()), None),
        MvccKey::CatalogTable { table_id } => (3, *table_id, Uuid::nil(), None, None),
        MvccKey::CatalogIndex { table_id, index_id } => (4, *table_id, *index_id, None, None),
        MvccKey::CatalogIndexName { table_id, name } => {
            (5, *table_id, Uuid::nil(), Some(name.as_str()), None)
        }
        MvccKey::CatalogRoutingRoot { table_id } => (6, *table_id, Uuid::nil(), None, None),
        MvccKey::CatalogRoutingPage { table_id, page_id } => (7, *table_id, *page_id, None, None),
        MvccKey::CatalogIndexesRoot { table_id } => (8, *table_id, Uuid::nil(), None, None),
        MvccKey::CatalogIndexesPage { table_id, page_id } => (9, *table_id, *page_id, None, None),
        MvccKey::IndexEntry {
            table_id,
            index_id,
            primary_key,
            ..
        } => (10, *table_id, *index_id, None, Some(primary_key)),
        MvccKey::CatalogIndexProgress {
            table_id,
            progress_id,
        } => (11, *table_id, *progress_id, None, None),
        MvccKey::CatalogIndexClosed {
            table_id,
            closure_id,
        } => (12, *table_id, *closure_id, None, None),
        MvccKey::CatalogSplitClosed { table_id, split_id } => {
            (13, *table_id, *split_id, None, None)
        }
        MvccKey::DirectDedup {
            table_id, anchor, ..
        } => (direct::DEDUP_TAG, *table_id, Uuid::nil(), None, Some(anchor)),
    }
}
pub(crate) fn compare_keys(a: &MvccKey, b: &MvccKey) -> Ordering {
    let index_order = match (a, b) {
        (MvccKey::IndexEntry { values: a, .. }, MvccKey::IndexEntry { values: b, .. }) => {
            a[..].cmp(&b[..])
        }
        _ => Ordering::Equal,
    };
    let request_order = match (a, b) {
        (
            MvccKey::DirectDedup { request_id: a, .. },
            MvccKey::DirectDedup { request_id: b, .. },
        ) => a.cmp(b),
        _ => Ordering::Equal,
    };
    let a = components(a);
    let b = components(b);
    a.0.cmp(&b.0)
        .then(a.1.cmp(&b.1))
        .then(a.2.cmp(&b.2))
        .then(a.3.cmp(&b.3))
        .then(index_order)
        .then_with(|| match (a.4, b.4) {
            (Some(a), Some(b)) => compare_primary(a, b),
            _ => Ordering::Equal,
        })
        .then(request_order)
}
fn key_length(key: &MvccKey) -> Result<usize, ProtocolError> {
    let (_, _, _, text, primary) = components(key);
    let variable = match primary {
        Some(Key::Int64(_)) => 9,
        Some(Key::Utf8(s)) => 1 + s.len() + s.bytes().filter(|b| *b == 0).count() + 2,
        None => text.map_or(0, |s| s.len() + s.bytes().filter(|b| *b == 0).count() + 2),
    };
    let tuple = match key {
        MvccKey::IndexEntry { values, .. } => values
            .iter()
            .try_fold(1usize, |n, v| n.checked_add(v.encoded_len()))
            .ok_or_else(invalid)?,
        // The request id follows the prefix-free anchor encoding.
        MvccKey::DirectDedup { .. } => 16,
        _ => 0,
    };
    33usize
        .checked_add(tuple)
        .ok_or_else(invalid)?
        .checked_add(variable)
        .filter(|n| *n <= 2 * crate::protocol::MAX_BODY_BYTES + 128)
        .ok_or_else(invalid)
}
fn encode_key(key: &MvccKey, out: &mut [u8]) {
    let (tag, table, id, text, primary) = components(key);
    out[0] = tag;
    out[1..17].copy_from_slice(table.as_bytes());
    out[17..33].copy_from_slice(id.as_bytes());
    let mut cursor = 33;
    if let MvccKey::IndexEntry { values, .. } = key {
        for value in values.iter() {
            cursor += value.encode(&mut out[cursor..]);
        }
        out[cursor] = 0;
        cursor += 1;
    }
    let string = match primary {
        Some(Key::Int64(value)) => {
            out[cursor] = 1;
            out[cursor + 1..cursor + 9]
                .copy_from_slice(&((*value as u64) ^ (1 << 63)).to_be_bytes());
            cursor += 9;
            None
        }
        Some(Key::Utf8(s)) => {
            out[cursor] = 2;
            cursor += 1;
            Some(s.as_str())
        }
        None => text,
    };
    if let Some(s) = string {
        for b in s.bytes() {
            out[cursor] = b;
            cursor += 1;
            if b == 0 {
                out[cursor] = 255;
                cursor += 1;
            }
        }
        out[cursor..cursor + 2].copy_from_slice(&[0, 0]);
        cursor += 2;
    }
    if let MvccKey::DirectDedup { request_id, .. } = key {
        out[cursor..cursor + 16].copy_from_slice(request_id.as_bytes());
    }
}

impl ParticipantCommand {
    /// Commands only a range leader with `data.direct.v1` proposes. They are
    /// never nested inside a scoped or timestamp-point envelope.
    pub(crate) fn is_direct(&self) -> bool {
        matches!(
            self,
            Self::Direct(_)
                | Self::DirectBatch(_)
                | Self::DirectExpire(_)
                | Self::DirectForward(_)
                | Self::DirectPause(_)
                | Self::ChildPublished(_)
                | Self::DirectSchema(_)
        )
    }
    pub(crate) fn uses_prepare_lane(&self) -> bool {
        matches!(
            self,
            Self::PreparePage { .. }
                | Self::PrepareTimestampPoint { .. }
                | Self::RejectTimestampPoint { .. }
                | Self::SplitImportPage { .. }
        )
    }
}

impl OwnedPayload for ParticipantCommand {
    fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        let mut bytes = 0usize;
        let mut add = |n: usize| -> Result<(), ProtocolError> {
            bytes = bytes.checked_add(n).ok_or_else(invalid)?;
            Ok(())
        };
        match self {
            Self::Scoped { command, .. } => {
                add(command.uncharged_heap_bytes()?)?;
            }
            Self::PrepareTimestampPoint { locator, page }
            | Self::RejectTimestampPoint { locator, page } => {
                add(timestamp_resolution::locator_heap_bytes(locator))?;
                add(page.uncharged_heap_bytes()?)?;
            }
            Self::SealTimestampPoint { locator, .. } => {
                add(timestamp_resolution::locator_heap_bytes(locator))?;
            }
            Self::ResolveTimestampPoint { proof, .. } => add(proof.heap_bytes())?,
            Self::SplitFreeze { children, .. } => {
                add(std::mem::size_of_val(children.as_ref()))?;
                for child in children.iter() {
                    add(child.heap_bytes()?)?;
                }
            }
            Self::SplitStage { binding, .. } => {
                add(std::mem::size_of_val(binding.as_ref()))?;
                add(binding.heap_bytes()?)?;
            }
            Self::SplitImportPage { page, .. } => {
                add(std::mem::size_of::<lineage::SplitPage>())?;
                add(page.heap_bytes()?)?;
            }
            Self::Fence { span, .. } => {
                for key in [&span.start, &span.end].into_iter().flatten() {
                    if let Key::Utf8(s) = key {
                        add(s.capacity())?
                    }
                }
            }
            Self::Direct(direct) => add(direct.heap_bytes()?)?,
            Self::DirectBatch(batch) => add(batch.heap_bytes()?)?,
            Self::DirectForward(forward) => add(forward.heap_bytes())?,
            Self::PreparePage {
                plan,
                writes,
                guards,
                ..
            } => {
                add(guards.capacity() * std::mem::size_of::<lifecycle::SealedLifecycleGuard>())?;
                add(plan.group.database.capacity_bytes())?;
                add(writes.capacity() * std::mem::size_of::<PreparedWrite>())?;
                for write in writes.iter() {
                    add(write.heap_bytes()?)?
                }
            }
            _ => {}
        }
        Ok(bytes)
    }
    fn attach_fields(
        &self,
        owner: Arc<crate::oltp::pool::ScratchLease>,
    ) -> Result<(), ProtocolError> {
        match self {
            Self::Scoped { command, .. }
            | Self::PrepareTimestampPoint { page: command, .. }
            | Self::RejectTimestampPoint { page: command, .. } => {
                return command.attach_from_codec(owner);
            }
            _ => {}
        }
        if let Self::SplitImportPage { page, .. } = self {
            page.attach_fields(owner.clone())?;
        }
        if let Self::Direct(direct) = self {
            direct.attach_fields(owner.clone())?;
        }
        if let Self::DirectBatch(batch) = self {
            batch.attach_fields(owner.clone())?;
        }
        if let Self::PreparePage { writes, .. } = self {
            for write in writes.iter() {
                write.attach_charge(owner.clone())?;
            }
        }
        Ok(())
    }
    fn fields_attached(&self) -> bool {
        match self {
            Self::Scoped { command, .. }
            | Self::PrepareTimestampPoint { page: command, .. }
            | Self::RejectTimestampPoint { page: command, .. } => command.is_attached(),
            Self::SplitImportPage { page, .. } => page.fields_attached(),
            Self::Direct(direct) => direct.fields_attached(),
            Self::DirectBatch(batch) => batch.fields_attached(),
            Self::PreparePage { writes, .. } => writes.iter().all(PreparedWrite::fields_attached),
            _ => true,
        }
    }
}

#[cfg(test)]
mod tests;
