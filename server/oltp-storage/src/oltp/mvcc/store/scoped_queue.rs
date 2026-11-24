//! The queue owner can change independently of the participant's Raft owner.
use super::*;
use crate::oltp::pool::ByteClass;
use nabedra_oltp_types::{
    authority::{ScopedCatalogRead, ScopedQueueInventory, ScopedQueuePlacement, ScopedQueueTransition},
    types::AuthorityView,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct QueueBinding {
    group: Uuid,
    generation: Uuid,
}

impl QueueBinding {
    pub(super) fn from_scope(scope: ScopedCatalogRead) -> Self {
        Self {
            group: scope.queue_group_id,
            generation: scope.queue_generation,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct QueueEpoch {
    binding: QueueBinding,
    data_generation: Uuid,
    table_id: Uuid,
    view: AuthorityView,
}

impl QueueEpoch {
    fn from_scope(scope: ScopedCatalogRead) -> Self {
        Self {
            binding: QueueBinding::from_scope(scope),
            data_generation: scope.data_generation,
            table_id: scope.table_id,
            view: scope.queue_view,
        }
    }

    fn from_placement(placement: &ScopedQueuePlacement) -> Result<Self, ProtocolError> {
        let GroupKind::Data { table_id, .. } = placement.group.kind else {
            return Err(invalid());
        };
        Ok(Self {
            binding: QueueBinding {
                group: placement.group.group_id,
                generation: placement.queue_generation,
            },
            data_generation: placement.data_generation,
            table_id,
            view: placement.view,
        })
    }

    fn key(self) -> [u8; 34] {
        queue_key(self.binding.group, self.view.run_id)
    }

    fn valid(self, target: &GroupIdentity, authority: Authority) -> bool {
        !self.binding.group.is_nil()
            && !self.binding.generation.is_nil()
            && !self.data_generation.is_nil()
            && !self.table_id.is_nil()
            && self.view.validate().is_ok()
            && self.view.run_id == authority.run_id
            && match target.kind {
                GroupKind::Catalog => true,
                GroupKind::Data { table_id, .. } => {
                    target.group_id == self.binding.group && table_id == self.table_id
                }
                _ => false,
            }
    }

    fn same_queue(self, other: Self) -> bool {
        self.binding == other.binding
            && self.table_id == other.table_id
            && self.view.run_id == other.view.run_id
    }
}

fn queue_key(group: Uuid, run: Uuid) -> [u8; 34] {
    let mut key = [0; 34];
    key[..2].copy_from_slice(b"qe");
    key[2..18].copy_from_slice(group.as_bytes());
    key[18..].copy_from_slice(run.as_bytes());
    key
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueFence {
    transition_id: Uuid,
    basis: Digest,
    installed: ScopedQueueInventory,
    previous: QueueEpoch,
    next: QueueEpoch,
}

impl QueueFence {
    pub fn for_transition(
        target: &GroupIdentity,
        transfer: &ScopedQueueTransition,
    ) -> Result<Self, ProtocolError> {
        let group = &transfer.previous.group;
        transfer.previous.validate(group)?;
        transfer.next.validate(group)?;
        if target.cluster_id != group.cluster_id
            || target.database != group.database
            || target.kind != GroupKind::Catalog && target != group
        {
            return Err(invalid());
        }
        let owner_cut = transfer
            .next
            .members
            .iter()
            .zip(transfer.installed)
            .find_map(|(worker, cut)| (*worker == transfer.next.owner).then_some(cut).flatten())
            .ok_or_else(recovery_unknown)?;
        if transfer
            .installed
            .iter()
            .filter(|cut| **cut == Some(owner_cut))
            .count()
            < 2
        {
            return Err(recovery_unknown());
        }
        let fence = Self {
            transition_id: transfer.id,
            basis: transfer.basis()?.ok_or_else(recovery_unknown)?,
            installed: owner_cut,
            previous: QueueEpoch::from_placement(&transfer.previous)?,
            next: QueueEpoch::from_placement(&transfer.next)?,
        };
        if !fence.valid(target, transfer.next.view.authority()) {
            return Err(invalid());
        }
        Ok(fence)
    }

    fn valid(self, group: &GroupIdentity, authority: Authority) -> bool {
        !self.transition_id.is_nil()
            && self.previous.valid(group, authority)
            && self.next.valid(group, authority)
            && self.previous.same_queue(self.next)
            && self.previous.view.owner_term < self.next.view.owner_term
            && self.previous.view.membership_revision <= self.next.view.membership_revision
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueFenceReceipt {
    fence: QueueFence,
    applied_at: AppliedPosition,
}

impl QueueFenceReceipt {
    pub fn verifies(self, fence: QueueFence) -> bool {
        self.fence == fence
            && self.applied_at.term != 0
            && self.applied_at.node_id != 0
            && self.applied_at.index != 0
    }
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct QueueState {
    epoch: QueueEpoch,
    fence: Option<QueueFenceReceipt>,
}

impl QueueState {
    fn matches(self, epoch: QueueEpoch, require_fence: bool) -> bool {
        self.epoch == epoch
            && (!require_fence || self.fence.is_some_and(|receipt| receipt.fence.next == epoch))
    }
}

pub(super) fn apply_fence(
    tx: &mut GroupTransaction<'_>,
    group: &GroupIdentity,
    config: &Config,
    position: AppliedPosition,
    fence: QueueFence,
) -> Result<Transition, ProtocolError> {
    if !fence.valid(group, config.authority) {
        return reject(tx, Fault::InvalidBinding);
    }
    let key = fence.next.key();
    if let Some(current) = load::<QueueState>(tx, &key)? {
        if current.epoch == fence.next {
            return match current.fence {
                Some(receipt) if receipt.verifies(fence) => {
                    commit(tx, &ParticipantReply::ScopedQueueFenced(receipt))
                }
                _ => reject(tx, Fault::StaleAuthority),
            };
        }
        // A replacement transfer can advance over a partially installed fence
        // for an earlier successor while Control still names the predecessor.
        let superseded = current.fence.is_some()
            && current.epoch.same_queue(fence.previous)
            && current.epoch.view.owner_term > fence.previous.view.owner_term
            && current.epoch.view.owner_term < fence.next.view.owner_term;
        if current.epoch != fence.previous && !superseded {
            return reject(tx, Fault::StaleAuthority);
        }
    }
    let receipt = QueueFenceReceipt {
        fence,
        applied_at: position,
    };
    tx.put_serialized(
        &key,
        &QueueState {
            epoch: fence.next,
            fence: Some(receipt),
        },
    )?;
    commit(tx, &ParticipantReply::ScopedQueueFenced(receipt))
}

pub(super) fn check(
    tx: &mut GroupTransaction<'_>,
    group: &GroupIdentity,
    config: &Config,
    scope: ScopedCatalogRead,
    command: &ParticipantCommand,
) -> Result<Result<(), Fault>, ProtocolError> {
    let epoch = QueueEpoch::from_scope(scope);
    if !epoch.valid(group, config.authority) {
        return Ok(Err(Fault::StaleAuthority));
    }
    if scope.request.run_id != epoch.view.run_id
        || scope.request.request_id.is_nil()
        || !bound_to_scope(group, scope, command)
    {
        return Ok(Err(Fault::InvalidBinding));
    }
    // An immutable point home can outlive its original Data placement. A
    // replacement generation must use the fence installed by its transfer.
    let locator = match command {
        ParticipantCommand::PrepareTimestampPoint { locator, .. }
        | ParticipantCommand::RejectTimestampPoint { locator, .. }
        | ParticipantCommand::SealTimestampPoint { locator, .. } => Some(locator.as_ref()),
        ParticipantCommand::ResolveTimestampPoint { proof, .. } => Some(proof.locator()),
        _ => None,
    };
    let require_fence = locator.is_some_and(|locator| {
        scope.data_generation != locator.binding.original_scope.data_generation
    });
    let key = epoch.key();
    match load::<QueueState>(tx, &key)? {
        Some(current) if current.matches(epoch, require_fence) => {}
        None if !require_fence => {
            tx.put_serialized(&key, &QueueState { epoch, fence: None })?;
        }
        _ => return Ok(Err(Fault::StaleAuthority)),
    }
    Ok(Ok(()))
}

pub fn bound_to_scope(
    group: &GroupIdentity,
    scope: ScopedCatalogRead,
    command: &ParticipantCommand,
) -> bool {
    use lifecycle::{GuardMode, LifecycleBinding, LifecycleCommand};

    let data = group.group_id == scope.queue_group_id
        && matches!(group.kind, GroupKind::Data { table_id, .. } if table_id == scope.table_id);
    let txn_matches = |txn: TxnId| txn.attempt == 1 && txn.request_id == scope.request.request_id;
    let guard_matches = |binding: &LifecycleBinding, mode| {
        mode == GuardMode::Shared
            && binding.table_id == scope.table_id
            && txn_matches(binding.txn)
            && binding.request == scope.request
            && binding.read_ts == scope.read_ts
    };

    match command {
        ParticipantCommand::PrepareTimestampPoint { locator, page }
        | ParticipantCommand::RejectTimestampPoint { locator, page } => {
            data && timestamp_resolution::matches_scope(locator, scope)
                && page.payload().is_ok_and(|page| {
                    matches!(page, ParticipantCommand::PreparePage { .. })
                        && bound_to_scope(group, scope, page)
                })
        }
        ParticipantCommand::SealTimestampPoint { locator, .. } => {
            data && timestamp_resolution::matches_scope(locator, scope)
        }
        ParticipantCommand::ResolveTimestampPoint { proof, .. } => {
            data && timestamp_resolution::matches_scope(proof.locator(), scope)
        }
        ParticipantCommand::PreparePage {
            txn,
            request,
            plan,
            page_index,
            writes,
            guards,
            ..
        } => {
            data && txn_matches(*txn)
                && *request == scope.request
                && plan.group == *group
                && plan.read_ts == scope.read_ts
                && *page_index < plan.page_count
                && !writes.is_empty()
                && writes.iter().all(|write| {
                    matches!(
                        &write.key,
                        MvccKey::Row { table_id, .. } | MvccKey::IndexEntry { table_id, .. }
                            if *table_id == scope.table_id
                    )
                })
                && guards.len() == 1
                && guards
                    .iter()
                    .all(|guard| guard_matches(guard.binding(), guard.mode()))
        }
        ParticipantCommand::Seal { txn, .. } => data && txn_matches(*txn),
        ParticipantCommand::Resolve { proof, .. } => {
            data && txn_matches(proof.statement().txn) && proof.statement().request == scope.request
        }
        ParticipantCommand::Lifecycle(LifecycleCommand::Acquire { binding, mode, .. }) => {
            group.kind == GroupKind::Catalog && guard_matches(binding, *mode)
        }
        ParticipantCommand::Lifecycle(LifecycleCommand::Release { guard, .. }) => {
            group.kind == GroupKind::Catalog && guard_matches(guard.binding(), guard.mode())
        }
        _ => false,
    }
}

pub(super) fn read_current(
    view: &mut crate::oltp::pool::GroupRead<'_>,
    scope: ScopedCatalogRead,
    group: &GroupIdentity,
    authority: Authority,
    locator: &TimestampDecisionLocator,
) -> Result<bool, ProtocolError> {
    let epoch = QueueEpoch::from_scope(scope);
    let current: QueueState = timestamp_resolution::read_record(view, &epoch.key())?;
    let require_fence = scope.data_generation != locator.binding.original_scope.data_generation;
    Ok(epoch.valid(group, authority) && current.matches(epoch, require_fence))
}

impl ParticipantHandle {
    pub async fn fence_scoped_queue(
        &self,
        transfer: &ScopedQueueTransition,
    ) -> Result<(), ProtocolError> {
        if let Some(remote) = &self.remote {
            return remote.fence_scoped_queue(transfer).await;
        }
        let fence = QueueFence::for_transition(self.identity(), transfer)?;
        self.apply_scoped_queue_fence(fence).await.map(|_| ())
    }

    pub async fn apply_scoped_queue_fence(
        &self,
        fence: QueueFence,
    ) -> Result<QueueFenceReceipt, ProtocolError> {
        let applied = self
            .propose(OwnedCommand::new_owned(
                ParticipantCommand::FenceScopedQueue(fence),
                self.resources(),
                ByteClass::Progress,
            )?)
            .await?;
        match applied.reply {
            ParticipantReply::ScopedQueueFenced(receipt) if receipt.verifies(fence) => Ok(receipt),
            _ => Err(recovery_unknown()),
        }
    }

    pub async fn scoped_queue_fence(
        &self,
        queue_group_id: Uuid,
        transition_id: Uuid,
        expected: Authority,
        next: Authority,
    ) -> Result<QueueFenceReceipt, ProtocolError> {
        self.local_consensus().ensure_linearizable().await?;
        let group = self.identity().clone();
        let raw = self
            .local_machine()
            .store
            .read_metadata(
                TransactionBudget {
                    scratch_bytes: 64 << 10,
                    batch_bytes: 0,
                    output_bytes: 16 << 10,
                },
                move |view| {
                    let raw = view.pinned(CONFIG)?.ok_or_else(recovery_unknown)?;
                    if raw.len() > METADATA_BYTES {
                        return Err(recovery_unknown());
                    }
                    let config: Config =
                        serde_json::from_slice(&raw).map_err(|_| recovery_unknown())?;
                    if next.run_id != config.authority.run_id {
                        return Err(recovery_unknown());
                    }
                    let raw = view
                        .pinned(&queue_key(queue_group_id, next.run_id))?
                        .ok_or_else(recovery_unknown)?;
                    if raw.len() > METADATA_BYTES {
                        return Err(recovery_unknown());
                    }
                    let current: QueueState =
                        serde_json::from_slice(&raw).map_err(|_| recovery_unknown())?;
                    let receipt = current
                        .fence
                        .filter(|receipt| {
                            let fence = receipt.fence;
                            fence.transition_id == transition_id
                                && fence.previous.view.authority() == expected
                                && fence.next.view.authority() == next
                                && current.epoch == fence.next
                                && fence.valid(&group, config.authority)
                                && receipt.verifies(fence)
                        })
                        .ok_or_else(recovery_unknown)?;
                    view.encode(&receipt)
                },
            )
            .await?;
        serde_json::from_slice(raw.as_slice()).map_err(|_| recovery_unknown())
    }
}
