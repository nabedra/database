use super::model::*;
use crate::{
    oltp::pool::{
        ApplyMeta, ChargedBytes, GroupStore, GroupTransaction, TransactionBudget, Transition,
    },
    oltp_replication::{ReadySetHasher, owned::OwnedCommand, types::*},
    protocol::ProtocolError,
};
use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeSet;
use uuid::Uuid;

pub(super) const STATE: &[u8] = b"ct/state";
pub(super) const WORKERS: &[u8] = b"ct/worker/";
pub(super) const GROUPS: &[u8] = b"ct/group/";
pub(super) const BUDGET: TransactionBudget = TransactionBudget {
    scratch_bytes: 2 << 20,
    batch_bytes: 2 << 20,
    output_bytes: 256 << 10,
};
#[derive(Clone)]
pub(crate) struct ControlMachine {
    store: GroupStore,
}
impl ControlMachine {
    pub(crate) fn new(store: GroupStore) -> Result<Self, ProtocolError> {
        if store.identity().kind != GroupKind::Control {
            return Err(unavailable("wrong Control group kind"));
        }
        Ok(Self { store })
    }
    pub(crate) fn store(&self) -> &GroupStore {
        &self.store
    }
    pub(crate) async fn apply(
        &self,
        meta: ApplyMeta,
        command: OwnedCommand<ControlCommand>,
    ) -> Result<ChargedBytes, ProtocolError> {
        command.ensure_charged()?;
        let scope = self.store.identity().clone();
        let position = meta.position;
        let digest = meta.command_digest;
        self.store
            .transaction(meta, BUDGET, move |tx| {
                if crate::oltp_replication::owned::serialized_digest(&command)? != digest {
                    return Err(unavailable("Control command digest differs"));
                }
                apply(tx, &scope, position, digest, command.payload()?)
            })
            .await
    }
}

#[derive(Serialize, serde::Deserialize)]
struct Receipt {
    digest: Digest,
    reply: ControlReply,
}
#[derive(Serialize, serde::Deserialize, PartialEq, Eq)]
struct FenceAck {
    group: GroupIdentity,
    expected: Authority,
    next: Authority,
    applied_at: AppliedPosition,
    command_digest: Digest,
}

fn apply(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    position: AppliedPosition,
    digest: Digest,
    command: &ControlCommand,
) -> Result<Transition, ProtocolError> {
    let op = command.operation_id();
    if op.is_nil() {
        return reject(tx, Fault::Invalid);
    }
    let receipt_key = key(b"ct/op/", op);
    if let Some(old) = load::<Receipt>(tx, &receipt_key)? {
        return if old.digest == digest {
            commit(tx, &old.reply)
        } else {
            reject(tx, Fault::Conflict)
        };
    }
    let state = load::<State>(tx, STATE)?;
    let reply = match (state, command) {
        (
            None,
            ControlCommand::Initialize {
                run_id,
                authorization,
                ..
            },
        ) => {
            if run_id.is_nil() || !stamp(authorization, authorization.policy) {
                ControlReply::Rejected(Fault::Invalid)
            } else {
                let ready = ReadyMembership {
                    view: AuthorityView {
                        run_id: *run_id,
                        owner_term: 1,
                        membership_revision: 1,
                    },
                    ready_root: ReadySetHasher::default().finish(),
                    ready_count: 0,
                };
                let state = State {
                    policy: authorization.policy,
                    ready,
                    owner: None,
                    ready_generation: op,
                    enrollment_revision: 0,
                    group_inventory_revision: 1,
                    group_count: 1,
                    invalid_ready: 0,
                    pending: None,
                };
                save(tx, STATE, &state)?;
                save(
                    tx,
                    &key(GROUPS, scope.group_id),
                    &GroupRecord {
                        identity: scope.clone(),
                        generation: op,
                        replica_count: 0,
                        replica_root: group_root(0, group_seed()),
                        membership_position: position,
                    },
                )?;
                ControlReply::Initialized { ready }
            }
        }
        (None, _) => ControlReply::Rejected(Fault::NotInitialized),
        (Some(_), ControlCommand::Initialize { .. }) => ControlReply::Rejected(Fault::Conflict),
        (Some(mut state), _) => {
            let reply = apply_initialized(tx, scope, position, digest, command, &mut state)?;
            if !matches!(reply, ControlReply::Rejected(_)) {
                save(tx, STATE, &state)?;
            }
            reply
        }
    };
    if matches!(reply, ControlReply::Rejected(_)) {
        // GroupTransaction cannot expose candidate writes after a rejection.
        // Semantic paths below preflight their page before writing anything.
        return reject(
            tx,
            match reply {
                ControlReply::Rejected(fault) => fault,
                _ => unreachable!(),
            },
        );
    }
    save(
        tx,
        &receipt_key,
        &Receipt {
            digest,
            reply: reply.clone(),
        },
    )?;
    commit(tx, &reply)
}

fn apply_initialized(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    position: AppliedPosition,
    command_digest: Digest,
    command: &ControlCommand,
    state: &mut State,
) -> Result<ControlReply, ProtocolError> {
    use ControlCommand::*;
    match command {
        Initialize { .. } => Ok(ControlReply::Rejected(Fault::Conflict)),
        Enroll {
            expected,
            next,
            authorization,
            previous_process,
            ..
        } => {
            if matches!(state.pending, Some(Pending::Ready(_))) {
                return fault(Fault::Busy);
            }
            if !stamp(authorization, state.policy) || next.validate().is_err() {
                return fault(Fault::Unauthorized);
            }
            let old = load::<WorkerRecord>(tx, &key(WORKERS, next.worker_id))?;
            if old.as_ref().map(|v| WorkerIdentity::from(&v.enrollment)) != *expected {
                return fault(Fault::Stale);
            }
            let revision = old.as_ref().map_or(Ok(1), |v| {
                v.enrollment
                    .enrollment_revision
                    .checked_add(1)
                    .ok_or_else(unknown)
            })?;
            if next.enrollment_revision < revision {
                return fault(Fault::Invalid);
            }
            if let Some(old) = &old {
                let identity = WorkerIdentity::from(&old.enrollment);
                if next.incarnation == identity.incarnation
                    || !previous_process.as_ref().is_some_and(|proof| {
                        proof.worker == identity && exclusion(proof, state.policy)
                    })
                {
                    return fault(Fault::Unauthorized);
                }
                if load::<WorkerIdentity>(tx, &ready_key(state.ready_generation, next.worker_id))?
                    == Some(identity)
                {
                    state.invalid_ready = state.invalid_ready.checked_add(1).ok_or_else(unknown)?;
                }
            } else if previous_process.is_some() {
                return fault(Fault::Invalid);
            }
            state.enrollment_revision = state
                .enrollment_revision
                .checked_add(1)
                .ok_or_else(unknown)?;
            save(
                tx,
                &key(WORKERS, next.worker_id),
                &WorkerRecord {
                    enrollment: next.clone(),
                    previous_process: previous_process.clone(),
                },
            )?;
            Ok(ControlReply::Enrolled(WorkerIdentity::from(next)))
        }
        BeginGroup {
            generation,
            identity,
            authorization,
            ..
        } => {
            if state.pending.is_some() {
                return fault(Fault::Busy);
            }
            if !stamp(authorization, state.policy)
                || !same_scope(scope, identity)
                || identity.validate().is_err()
                || generation.is_nil()
                || identity.kind == GroupKind::Control && identity.group_id != scope.group_id
            {
                return fault(Fault::Unauthorized);
            }
            if let Some(existing) = load::<GroupBuild>(tx, &key(b"ct/group-build/", *generation))? {
                return if existing.identity == *identity {
                    Ok(ControlReply::GroupBuilding {
                        generation: *generation,
                        count: existing.count,
                    })
                } else {
                    fault(Fault::Conflict)
                };
            }
            if load::<GroupRecord>(tx, &key(GROUPS, identity.group_id))?
                .is_some_and(|g| g.replica_count != 0)
            {
                return fault(Fault::Conflict);
            }
            save(
                tx,
                &key(b"ct/group-build/", *generation),
                &GroupBuild {
                    generation: *generation,
                    identity: identity.clone(),
                    count: 0,
                    last_node: None,
                    chain: group_seed(),
                },
            )?;
            Ok(ControlReply::GroupBuilding {
                generation: *generation,
                count: 0,
            })
        }
        GroupPage {
            generation,
            replicas,
            ..
        } => {
            if state.pending.is_some() {
                return fault(Fault::Busy);
            }
            let Some(mut build) = load::<GroupBuild>(tx, &key(b"ct/group-build/", *generation))?
            else {
                return fault(Fault::NotFound);
            };
            let mut within = BTreeSet::new();
            for member in replicas.iter() {
                if member.validate().is_err()
                    || build.last_node.is_some_and(|old| member.node_id <= old)
                    || !within.insert(member.worker_id)
                    || load::<WorkerRecord>(tx, &key(WORKERS, member.worker_id))?.is_none()
                    || tx
                        .get(&member_worker_key(*generation, member.worker_id))?
                        .is_some()
                {
                    return fault(Fault::Invalid);
                }
                build.last_node = Some(member.node_id);
                build.count = build.count.checked_add(1).ok_or_else(unknown)?;
                build.chain = group_step(build.chain, member);
            }
            for member in replicas.iter() {
                save(tx, &member_key(*generation, member.node_id), member)?;
                save(
                    tx,
                    &member_worker_key(*generation, member.worker_id),
                    member,
                )?;
            }
            save(tx, &key(b"ct/group-build/", *generation), &build)?;
            Ok(ControlReply::GroupBuilding {
                generation: *generation,
                count: build.count,
            })
        }
        SealGroup {
            generation, proof, ..
        } => {
            if state.pending.is_some() {
                return fault(Fault::Busy);
            }
            let Some(build) = load::<GroupBuild>(tx, &key(b"ct/group-build/", *generation))? else {
                return fault(Fault::NotFound);
            };
            if build.count == 0
                || proof.group != build.identity
                || proof.generation != *generation
                || proof.replica_count != build.count
                || proof.replica_root != group_root(build.count, build.chain)
                || proof.authority != state.ready.view.authority()
                || !position_valid(proof.applied_at)
            {
                return fault(Fault::Invalid);
            }
            let old = load::<GroupRecord>(tx, &key(GROUPS, build.identity.group_id))?;
            if old.as_ref().is_some_and(|g| g.replica_count != 0) {
                return fault(Fault::Conflict);
            }
            save(
                tx,
                &key(GROUPS, build.identity.group_id),
                &GroupRecord {
                    identity: build.identity.clone(),
                    generation: *generation,
                    replica_count: build.count,
                    replica_root: proof.replica_root,
                    membership_position: proof.applied_at,
                },
            )?;
            if old.is_none() {
                state.group_count = state.group_count.checked_add(1).ok_or_else(unknown)?;
            }
            state.group_inventory_revision = state
                .group_inventory_revision
                .checked_add(1)
                .ok_or_else(unknown)?;
            Ok(ControlReply::GroupRegistered {
                group_id: build.identity.group_id,
                count: build.count,
                root: proof.replica_root,
            })
        }
        BeginJoin {
            operation_id,
            expected,
            target,
        } => begin_ready(
            tx,
            state,
            *operation_id,
            *expected,
            *target,
            ReadyChange::Join,
            None,
        ),
        BeginRemoval {
            operation_id,
            expected,
            target,
            exclusion: proof,
        } => begin_ready(
            tx,
            state,
            *operation_id,
            *expected,
            *target,
            ReadyChange::Remove,
            Some(proof),
        ),
        ReadyPage {
            transition_id,
            members,
            ..
        } => ready_page(tx, state, *transition_id, members),
        SealReady { transition_id, .. } => seal_ready(tx, state, *transition_id),
        TimestampFenced {
            transition_id,
            receipt,
            ..
        } => {
            let Some(Pending::Ready(pending)) = &mut state.pending else {
                return fault(Fault::NotFound);
            };
            if pending.transition_id != *transition_id
                || pending.phase != ReadyPhase::AwaitingTimestampFence
                || receipt.transition_id != *transition_id
                || receipt.expected != pending.expected
                || Some(receipt.next) != pending.next
                || !position_valid(receipt.applied_at)
            {
                return fault(Fault::Stale);
            }
            pending.timestamp_fence = Some(receipt.clone());
            pending.phase = ReadyPhase::AwaitingQueueProof;
            Ok(ControlReply::TimestampFenced {
                transition_id: *transition_id,
                next: receipt.next,
            })
        }
        PublishReady {
            transition_id,
            proof,
            ..
        } => publish_ready(state, *transition_id, proof),
        BeginOwnerFence {
            operation_id,
            expected,
            next_owner,
            authorization,
        } => {
            if state.pending.is_some() {
                return fault(Fault::Busy);
            }
            if *expected != state.ready
                || !stamp(authorization, state.policy)
                || !current_ready(tx, state, *next_owner)?
            {
                return fault(Fault::Stale);
            }
            let mut next = *expected;
            next.view.owner_term = next.view.owner_term.checked_add(1).ok_or_else(unknown)?;
            begin_fence(
                tx,
                scope,
                state,
                *operation_id,
                next,
                Some(*next_owner),
                false,
                None,
                authorization.clone(),
                position,
                command_digest,
            )
        }
        BeginColdFence {
            operation_id,
            expected,
            next_run,
            authorization,
        } => {
            // An explicitly authorized cold fence supersedes an interrupted
            // join only after the target is also positively excluded. The TSO
            // and every data/status group still fence the old authority before
            // Control publishes the new run.
            let extra_exclusion = match &state.pending {
                Some(Pending::Fence(_)) => return fault(Fault::Busy),
                Some(Pending::Ready(pending)) if pending.change == ReadyChange::Join => {
                    Some(pending.target)
                }
                _ => None,
            };
            if *expected != state.ready
                || next_run.is_nil()
                || *next_run == state.ready.view.run_id
                || !stamp(authorization, state.policy)
            {
                return fault(Fault::Unauthorized);
            }
            let next = ReadyMembership {
                view: AuthorityView {
                    run_id: *next_run,
                    owner_term: state
                        .ready
                        .view
                        .owner_term
                        .checked_add(1)
                        .ok_or_else(unknown)?,
                    membership_revision: state
                        .ready
                        .view
                        .membership_revision
                        .checked_add(1)
                        .ok_or_else(unknown)?,
                },
                ready_root: ReadySetHasher::default().finish(),
                ready_count: 0,
            };
            begin_fence(
                tx,
                scope,
                state,
                *operation_id,
                next,
                None,
                true,
                extra_exclusion,
                authorization.clone(),
                position,
                command_digest,
            )
        }
        ExcludedPage {
            transition_id,
            workers,
            ..
        } => {
            let Some(Pending::Fence(pending)) = &mut state.pending else {
                return fault(Fault::NotFound);
            };
            if pending.transition_id != *transition_id || !pending.cold {
                return fault(Fault::Stale);
            }
            let mut within = BTreeSet::new();
            let mut additions = Vec::new();
            for proof in workers.iter() {
                if !within.insert(proof.worker.worker_id)
                    || !exclusion(proof, state.policy)
                    || load::<WorkerIdentity>(
                        tx,
                        &ready_key(state.ready_generation, proof.worker.worker_id),
                    )? != Some(proof.worker)
                        && pending.extra_exclusion != Some(proof.worker)
                {
                    return fault(Fault::Unauthorized);
                }
                let target = exclusion_key(*transition_id, proof.worker.worker_id);
                match load::<ExclusionStamp>(tx, &target)? {
                    Some(old) if old != *proof => return fault(Fault::Conflict),
                    Some(_) => {}
                    None => additions.push((target, proof)),
                }
            }
            for (target, proof) in additions {
                save(tx, &target, proof)?;
                pending.excluded_workers = pending
                    .excluded_workers
                    .checked_add(1)
                    .ok_or_else(unknown)?;
            }
            Ok(ControlReply::FenceProgress {
                transition_id: *transition_id,
                fenced_groups: pending.fenced_groups,
                excluded_workers: pending.excluded_workers,
            })
        }
        GroupFenced { receipt, .. } => {
            let Some(Pending::Fence(pending)) = &mut state.pending else {
                return fault(Fault::NotFound);
            };
            if pending.transition_id != receipt.transition_id
                || receipt.expected != Some(pending.expected.view.authority())
                || receipt.next != pending.next.view.authority()
                || !same_scope(scope, &receipt.group)
                || !position_valid(receipt.applied_at)
                || !nonzero(receipt.command_digest)
                || load::<GroupRecord>(tx, &key(GROUPS, receipt.group.group_id))?
                    .is_none_or(|g| g.identity != receipt.group)
            {
                return fault(Fault::Invalid);
            }
            let target = fence_key(receipt.transition_id, receipt.group.group_id);
            let ack = FenceAck {
                group: receipt.group.clone(),
                expected: pending.expected.view.authority(),
                next: receipt.next,
                applied_at: receipt.applied_at,
                command_digest: receipt.command_digest,
            };
            match load::<FenceAck>(tx, &target)? {
                Some(old) if old != ack => return fault(Fault::Conflict),
                Some(_) => {}
                None => {
                    save(tx, &target, &ack)?;
                    pending.fenced_groups =
                        pending.fenced_groups.checked_add(1).ok_or_else(unknown)?;
                }
            }
            Ok(ControlReply::FenceProgress {
                transition_id: receipt.transition_id,
                fenced_groups: pending.fenced_groups,
                excluded_workers: pending.excluded_workers,
            })
        }
        PublishFence { transition_id, .. } => {
            let Some(Pending::Fence(pending)) = &state.pending else {
                return fault(Fault::NotFound);
            };
            if pending.transition_id != *transition_id
                || pending.group_inventory_revision != state.group_inventory_revision
                || pending.fenced_groups != pending.required_groups
                || pending.cold
                    && pending.excluded_workers
                        != state
                            .ready
                            .ready_count
                            .checked_add(u64::from(pending.extra_exclusion.is_some()))
                            .ok_or_else(unknown)?
            {
                return fault(Fault::Incomplete);
            }
            let pending = pending.clone();
            state.ready = pending.next;
            state.owner = pending.next_owner;
            if pending.cold {
                state.ready_generation = *transition_id;
                state.invalid_ready = 0;
            }
            state.pending = None;
            Ok(ControlReply::Published {
                ready: state.ready,
                owner: state.owner,
            })
        }
    }
}

fn begin_ready(
    tx: &mut GroupTransaction<'_>,
    state: &mut State,
    id: Uuid,
    expected: AuthorityView,
    target: WorkerIdentity,
    change: ReadyChange,
    removed: Option<&ExclusionStamp>,
) -> Result<ControlReply, ProtocolError> {
    if state.pending.is_some() {
        return fault(Fault::Busy);
    }
    if state.ready.view != expected || target.validate().is_err() {
        return fault(Fault::Stale);
    }
    let enrolled = load::<WorkerRecord>(tx, &key(WORKERS, target.worker_id))?;
    if enrolled
        .as_ref()
        .map(|w| WorkerIdentity::from(&w.enrollment))
        != Some(target)
    {
        return fault(Fault::Unauthorized);
    }
    let existing =
        load::<WorkerIdentity>(tx, &ready_key(state.ready_generation, target.worker_id))?;
    match change {
        ReadyChange::Join if existing.is_some() => return fault(Fault::Conflict),
        ReadyChange::Remove
            if existing != Some(target)
                || state.ready.ready_count <= 1
                || state.owner == Some(target)
                || !removed.is_some_and(|proof| {
                    proof.worker == target && exclusion(proof, state.policy)
                }) =>
        {
            return fault(Fault::Unauthorized);
        }
        _ => {}
    }
    state.pending = Some(Pending::Ready(ReadyPending {
        transition_id: id,
        expected: state.ready,
        target,
        change,
        generation: id,
        source_after: None,
        target_seen: false,
        root: ReadySetHasher::default(),
        next: None,
        phase: ReadyPhase::Building,
        timestamp_fence: None,
    }));
    Ok(ControlReply::JoinBegun {
        ticket: JoinTicket {
            ticket_id: id,
            target,
            source_ready: state.ready,
        },
        bootstrap: state.ready.ready_count == 0,
    })
}

fn ready_page(
    tx: &mut GroupTransaction<'_>,
    state: &mut State,
    id: Uuid,
    members: &Page<WorkerIdentity>,
) -> Result<ControlReply, ProtocolError> {
    let Some(Pending::Ready(pending)) = &mut state.pending else {
        return fault(Fault::NotFound);
    };
    if pending.transition_id != id || pending.phase != ReadyPhase::Building {
        return fault(Fault::Stale);
    }
    let mut next = pending.clone();
    for member in members.iter() {
        let enrolled = load::<WorkerRecord>(tx, &key(WORKERS, member.worker_id))?;
        if enrolled
            .as_ref()
            .map(|w| WorkerIdentity::from(&w.enrollment))
            != Some(*member)
            || next.root.push(*member).is_err()
        {
            return fault(Fault::Invalid);
        }
        let mut old = next_ready(tx, state.ready_generation, next.source_after)?;
        if next.change == ReadyChange::Remove && old == Some(next.target) {
            next.source_after = Some(next.target.worker_id);
            old = next_ready(tx, state.ready_generation, next.source_after)?;
            next.target_seen = true;
        }
        if next.change == ReadyChange::Join && *member == next.target {
            if next.target_seen {
                return fault(Fault::Conflict);
            }
            next.target_seen = true;
        } else {
            if old != Some(*member) {
                return fault(Fault::Invalid);
            }
            next.source_after = Some(member.worker_id);
        }
    }
    for member in members.iter() {
        save(tx, &ready_key(next.generation, member.worker_id), member)?;
    }
    *pending = next;
    Ok(ControlReply::ReadyBuilding {
        transition_id: id,
        count: pending.root.count(),
    })
}
fn seal_ready(
    tx: &mut GroupTransaction<'_>,
    state: &mut State,
    id: Uuid,
) -> Result<ControlReply, ProtocolError> {
    let Some(Pending::Ready(pending)) = &mut state.pending else {
        return fault(Fault::NotFound);
    };
    if pending.transition_id != id || pending.phase != ReadyPhase::Building {
        return fault(Fault::Stale);
    }
    let mut old = next_ready(tx, state.ready_generation, pending.source_after)?;
    if pending.change == ReadyChange::Remove && old == Some(pending.target) {
        pending.target_seen = true;
        pending.source_after = Some(pending.target.worker_id);
        old = next_ready(tx, state.ready_generation, pending.source_after)?;
    }
    let expected = match pending.change {
        ReadyChange::Join => state.ready.ready_count.checked_add(1),
        ReadyChange::Remove => state.ready.ready_count.checked_sub(1),
    }
    .ok_or_else(unknown)?;
    if old.is_some() || !pending.target_seen || pending.root.count() != expected {
        return fault(Fault::Incomplete);
    }
    let mut view = state.ready.view;
    view.membership_revision = view
        .membership_revision
        .checked_add(1)
        .ok_or_else(unknown)?;
    let next = ReadyMembership {
        view,
        ready_root: pending.root.finish(),
        ready_count: pending.root.count(),
    };
    pending.next = Some(next);
    pending.phase = ReadyPhase::AwaitingTimestampFence;
    Ok(ControlReply::ReadySealed {
        transition_id: id,
        expected: pending.expected,
        next,
    })
}
fn publish_ready(
    state: &mut State,
    id: Uuid,
    proof: &QueueCopyStatement,
) -> Result<ControlReply, ProtocolError> {
    let Some(Pending::Ready(pending)) = &state.pending else {
        return fault(Fault::NotFound);
    };
    if pending.transition_id != id
        || pending.phase != ReadyPhase::AwaitingQueueProof
        || pending.timestamp_fence.is_none()
        || proof.ticket
            != (JoinTicket {
                ticket_id: id,
                target: pending.target,
                source_ready: pending.expected,
            })
        || proof.snapshot_id.is_nil()
        || proof.final_cut < proof.snapshot_cut
        || !nonzero(proof.record_version_hash)
    {
        return fault(Fault::Incomplete);
    }
    let expected_source = state.owner.unwrap_or(pending.target);
    if proof.source != expected_source {
        return fault(Fault::Unauthorized);
    }
    if state.ready.ready_count == 0
        && (proof.snapshot_cut != 0
            || proof.final_cut != 0
            || proof.record_version_hash != hash(b"nabedra/ram-queue-cut/v1\0"))
    {
        return fault(Fault::Invalid);
    }
    let pending = pending.clone();
    state.ready = pending.next.ok_or_else(unknown)?;
    state.ready_generation = pending.generation;
    if state.owner.is_none() {
        state.owner = Some(pending.target);
    }
    state.invalid_ready = 0;
    state.pending = None;
    Ok(ControlReply::Published {
        ready: state.ready,
        owner: state.owner,
    })
}
fn begin_fence(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    state: &mut State,
    id: Uuid,
    next: ReadyMembership,
    owner: Option<WorkerIdentity>,
    cold: bool,
    extra_exclusion: Option<WorkerIdentity>,
    authorization: AuthorizationStamp,
    position: AppliedPosition,
    command_digest: Digest,
) -> Result<ControlReply, ProtocolError> {
    let pending = FencePending {
        transition_id: id,
        expected: state.ready,
        next,
        next_owner: owner,
        cold,
        authorization,
        group_inventory_revision: state.group_inventory_revision,
        required_groups: state.group_count,
        fenced_groups: 1,
        excluded_workers: 0,
        extra_exclusion,
    };
    save(
        tx,
        &fence_key(id, scope.group_id),
        &FenceAck {
            group: scope.clone(),
            expected: pending.expected.view.authority(),
            next: pending.next.view.authority(),
            applied_at: position,
            command_digest,
        },
    )?;
    state.pending = Some(Pending::Fence(pending));
    Ok(ControlReply::FenceBegun {
        transition_id: id,
        next,
        required_groups: state.group_count,
    })
}

fn current_ready(
    tx: &mut GroupTransaction<'_>,
    state: &State,
    worker: WorkerIdentity,
) -> Result<bool, ProtocolError> {
    Ok(
        load::<WorkerIdentity>(tx, &ready_key(state.ready_generation, worker.worker_id))?
            == Some(worker)
            && load::<WorkerRecord>(tx, &key(WORKERS, worker.worker_id))?
                .is_some_and(|r| WorkerIdentity::from(&r.enrollment) == worker),
    )
}
fn next_ready(
    tx: &mut GroupTransaction<'_>,
    generation: Uuid,
    after: Option<Uuid>,
) -> Result<Option<WorkerIdentity>, ProtocolError> {
    let prefix = ready_prefix(generation);
    let cursor = after.map(|worker| ready_key(generation, worker));
    let mut next = None;
    tx.scan_page(&prefix, cursor.as_deref(), 1, |_, bytes| {
        next = Some(decode(bytes)?);
        Ok(())
    })?;
    Ok(next)
}
pub(super) fn key(prefix: &[u8], id: Uuid) -> Vec<u8> {
    let mut key = prefix.to_vec();
    key.extend_from_slice(id.as_bytes());
    key
}
pub(super) fn ready_prefix(generation: Uuid) -> Vec<u8> {
    key(b"ct/ready/", generation)
}
pub(super) fn ready_key(generation: Uuid, worker: Uuid) -> Vec<u8> {
    key(&ready_prefix(generation), worker)
}
pub(super) fn member_prefix(generation: Uuid) -> Vec<u8> {
    key(b"ct/member/", generation)
}
pub(super) fn member_key(generation: Uuid, node: u64) -> Vec<u8> {
    let mut key = member_prefix(generation);
    key.extend_from_slice(&node.to_be_bytes());
    key
}
pub(super) fn member_worker_key(generation: Uuid, worker: Uuid) -> Vec<u8> {
    key(&key(b"ct/member-worker/", generation), worker)
}
fn fence_key(transition: Uuid, group: Uuid) -> Vec<u8> {
    key(&key(b"ct/fence/", transition), group)
}
fn exclusion_key(transition: Uuid, worker: Uuid) -> Vec<u8> {
    key(&key(b"ct/exclusion/", transition), worker)
}
pub(super) fn group_seed() -> Digest {
    hash(b"nabedra/control/group-members/v1")
}
pub(super) fn group_step(prior: Digest, member: &ReplicaMember) -> Digest {
    let mut h = Sha256::new();
    h.update(b"nabedra/control/group-member/v1");
    h.update(prior.as_bytes());
    h.update(member.node_id.to_be_bytes());
    h.update(member.worker_id.as_bytes());
    h.update(member.pool_id.as_bytes());
    Digest::from_bytes(h.finalize().into())
}
pub(super) fn group_root(count: u64, chain: Digest) -> Digest {
    let mut h = Sha256::new();
    h.update(b"nabedra/control/group-root/v1");
    h.update(count.to_be_bytes());
    h.update(chain.as_bytes());
    Digest::from_bytes(h.finalize().into())
}
pub(super) fn hash(bytes: &[u8]) -> Digest {
    Digest::from_bytes(Sha256::digest(bytes).into())
}
fn nonzero(value: Digest) -> bool {
    value.as_bytes().iter().any(|b| *b != 0)
}
fn stamp(value: &AuthorizationStamp, policy: Digest) -> bool {
    !value.authorization_id.is_nil()
        && value.policy == policy
        && nonzero(policy)
        && nonzero(value.subject)
        && nonzero(value.evidence)
}
fn exclusion(value: &ExclusionStamp, policy: Digest) -> bool {
    value.worker.validate().is_ok()
        && stamp(&value.authorization, policy)
        && nonzero(value.ownership_fence)
}
fn same_scope(control: &GroupIdentity, other: &GroupIdentity) -> bool {
    control.cluster_id == other.cluster_id && control.database == other.database
}
fn position_valid(position: AppliedPosition) -> bool {
    position.term > 0 && position.node_id > 0
}
pub(super) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, ProtocolError> {
    serde_json::from_slice(bytes).map_err(|_| unavailable("corrupt Control metadata"))
}
fn load<T: DeserializeOwned>(
    tx: &mut GroupTransaction<'_>,
    key: &[u8],
) -> Result<Option<T>, ProtocolError> {
    tx.get(key)?.map(|v| decode(v.as_ref())).transpose()
}
fn save<T: Serialize>(
    tx: &mut GroupTransaction<'_>,
    key: &[u8],
    value: &T,
) -> Result<(), ProtocolError> {
    let bytes = tx.encode_value(value)?;
    tx.put(key, bytes.as_ref())
}
fn fault(value: Fault) -> Result<ControlReply, ProtocolError> {
    Ok(ControlReply::Rejected(value))
}
fn reject(tx: &mut GroupTransaction<'_>, value: Fault) -> Result<Transition, ProtocolError> {
    Ok(Transition::Reject(
        tx.encode(&ControlReply::Rejected(value))?,
    ))
}
fn commit(
    tx: &mut GroupTransaction<'_>,
    value: &ControlReply,
) -> Result<Transition, ProtocolError> {
    Ok(Transition::Commit(tx.encode(value)?))
}
fn unknown() -> ProtocolError {
    unavailable("Control transition cannot be proven")
}

impl crate::oltp_replication::hosted::TypedMachine<super::super::config::ControlConfig>
    for ControlMachine
{
    fn store(&self) -> &GroupStore {
        ControlMachine::store(self)
    }
    async fn apply(
        &self,
        meta: ApplyMeta,
        command: OwnedCommand<ControlCommand>,
    ) -> Result<ChargedBytes, ProtocolError> {
        ControlMachine::apply(self, meta, command).await
    }
}
