//! CONTROL-1 (`specs/oltp/control`, PublishFence `Resolve`): an owner fence
//! resolves the active replica move by its phase. Claimed is cancelled,
//! Published releases the cluster's move slot and is retired by the new
//! owner, Moving still refuses the fence (its rollback is planned, not
//! implemented: mutant `code_moving_busy`).
//!
//! The move record is written directly: BeginReplicaMove's admission (Catalog
//! lineage, Data placement and the operator binding) is not under test here.
use super::*;
use crate::oltp_replication::authority::control::{
    ReplicaMove, ReplicaMovePhase, ReplicaMoveRequest,
    placement::ReplicaMoveLineage,
};

struct Moves {
    fixture: Fixture,
    owner: WorkerIdentity,
    peer: WorkerIdentity,
    ready: ReadyMembership,
}
impl Moves {
    async fn open(directory: &Directory) -> Self {
        let (scope, pool) = identities();
        let mut fixture = Fixture::open(directory, scope, pool).await;
        fixture.initialize().await;
        let owner = fixture.enroll(1).await;
        let peer = fixture.enroll(2).await;
        let third = fixture.enroll(3).await;
        let mut members = Vec::new();
        for worker in [owner, peer, third] {
            fixture.add_ready(worker, &mut members).await;
        }
        let ready = fixture.state().await.ready;
        Self { fixture, owner, peer, ready }
    }

    /// A local-target move of the owner: it removes node 1 (on the owner's
    /// worker) and adds node 4 (also on the owner's worker).
    fn plan(&self, phase: ReplicaMovePhase) -> ReplicaMove {
        let control = self.fixture.machine.store().identity().clone();
        let group = GroupIdentity {
            group_id: nabedra_rt::ids::uuid_v4(),
            kind: GroupKind::Data { table_id: nabedra_rt::ids::uuid_v4(), shard_id: nabedra_rt::ids::uuid_v4() },
            ..control
        };
        let GroupKind::Data { table_id, .. } = group.kind else { unreachable!() };
        let member = |node_id, worker: WorkerIdentity| ReplicaMember {
            node_id,
            worker_id: worker.worker_id,
            pool_id: nabedra_rt::ids::uuid_v4(),
        };
        let old = [member(1, self.owner), member(2, self.peer), member(3, self.peer)];
        let target = member(4, self.owner);
        let expected = GroupRecord {
            identity: group.clone(),
            generation: nabedra_rt::ids::uuid_v4(),
            replica_count: 3,
            replica_root: hash(b"old placement"),
            membership_position: AppliedPosition { term: 1, node_id: 1, index: 5 },
        };
        ReplicaMove {
            id: nabedra_rt::ids::uuid_v4(),
            request: ReplicaMoveRequest {
                group: group.clone(),
                expected_generation: expected.generation,
                remove_node: 1,
                target: target.clone(),
                worker: self.owner,
                pool_generation: 1,
            },
            owner: self.owner,
            authority: self.ready.view.authority(),
            expected,
            next: [target, old[1].clone(), old[2].clone()],
            old,
            lineage: ReplicaMoveLineage {
                catalog: nabedra_rt::ids::uuid_v4(),
                table: table_id,
                table_version: CommitTs::new(1).unwrap(),
                schema_epoch: 1,
                route_digest: hash(b"route"),
            },
            failure_domains: None,
            remote_source: None,
            guard: None,
            phase,
        }
    }

    /// Writes the move as BeginReplicaMove (and the later steps) would: the
    /// plan, the cluster's slot, the group's claim and the occupied nodes;
    /// for a published move also its placement.
    async fn install(&mut self, plan: &ReplicaMove) {
        let position = AppliedPosition { term: 1, node_id: 1, index: self.fixture.next };
        self.fixture.next += 1;
        let plan = plan.clone();
        self.fixture
            .machine
            .store()
            .transaction(
                ApplyMeta { position, command_digest: hash(b"install move"), membership: None },
                BUDGET,
                move |tx| {
                    let group = plan.request.group.group_id;
                    let mut put = |key: Vec<u8>, value: serde_json::Value| -> Result<(), crate::protocol::ProtocolError> {
                        let bytes = tx.encode_value(&value)?;
                        tx.put(&key, bytes.as_ref())
                    };

                    put(b"ct/replica-move-active".to_vec(), json(&plan.id))?;
                    put(key(b"ct/replica-move/", plan.id), json(&plan))?;
                    put(key(b"ct/group-replacement/", group), json(&plan.id))?;
                    for member in [&plan.request.target, &plan.old[0]] {
                        let mut occupied = key(b"ct/permanent-replica/", group);
                        occupied.extend_from_slice(&member.node_id.to_be_bytes());
                        put(occupied, json(member))?;
                    }
                    if let ReplicaMovePhase::Published { placement, .. } = &plan.phase {
                        put(key(GROUPS, group), json(placement))?;
                    }
                    Ok(crate::oltp::pool::Transition::Commit(tx.encode(&ControlReply::ScopedQueueUpdated)?))
                },
            )
            .await
            .unwrap();
    }

    async fn get(&self, key: Vec<u8>) -> Option<Vec<u8>> {
        let raw = self
            .fixture
            .machine
            .store()
            .read(BUDGET, move |view| {
                let value = view.get(&key)?.map(|v| v.as_ref().to_vec());
                view.encode(&value)
            })
            .await
            .unwrap();
        decode(raw.as_ref()).unwrap()
    }

    /// The owner is lost: `peer` fences a new term and publishes it.
    async fn fence(&mut self) -> ControlReply {
        let reply = self
            .fixture
            .apply(ControlCommand::BeginOwnerFence {
                operation_id: nabedra_rt::ids::uuid_v4(),
                expected: self.ready,
                next_owner: self.peer,
                authorization: authorization(),
            })
            .await;
        let ControlReply::FenceBegun { transition_id, .. } = reply else {
            return reply;
        };
        self.fixture
            .apply(ControlCommand::PublishFence { operation_id: nabedra_rt::ids::uuid_v4(), transition_id })
            .await
    }
}


fn published(plan: &ReplicaMove) -> ReplicaMovePhase {
    ReplicaMovePhase::Published {
        placement: GroupRecord {
            identity: plan.request.group.clone(),
            generation: plan.id,
            replica_count: 3,
            replica_root: hash(b"new placement"),
            membership_position: AppliedPosition { term: 1, node_id: 1, index: 9 },
        },
        activated_generation: 1,
        activated_at: AppliedPosition { term: 1, node_id: 4, index: 9 },
        activation_lineage: hash(b"activation"),
    }
}

/// Claimed: the fence cancels the move (open record
/// 2026-11-24-oltp-sim-move-owner-loss; shipped Control answered Busy).
#[tokio::test]
async fn r05_control1_an_owner_fence_cancels_a_claimed_move() {
    let directory = Directory::new();
    let mut moves = Moves::open(&directory).await;
    let plan = moves.plan(ReplicaMovePhase::Claimed);
    moves.install(&plan).await;
    let reply = moves.fence().await;
    assert!(matches!(reply, ControlReply::Published { owner: Some(owner), .. } if owner == moves.peer), "{reply:?}");
    assert!(moves.get(b"ct/replica-move-active".to_vec()).await.is_none());
    assert!(moves.get(key(b"ct/group-replacement/", plan.request.group.group_id)).await.is_none());
    let stored: ReplicaMove = decode(&moves.get(key(b"ct/replica-move/", plan.id)).await.unwrap()).unwrap();
    assert_eq!(stored.phase, ReplicaMovePhase::Cancelled);
    // The lost owner's late arm is refused under the new term.
    assert!(matches!(
        moves
            .fixture
            .apply(ControlCommand::CancelReplicaMove {
                operation_id: nabedra_rt::ids::uuid_v4(),
                transition_id: plan.id,
                worker: moves.peer,
                authorization: authorization(),
            })
            .await,
        ControlReply::Rejected(Fault::Unauthorized)
    ));
    // Topology changes are no longer Busy.
    let state = moves.fixture.state().await;
    assert!(state.pending.is_none());
    let joiner = moves.fixture.enroll(9).await;
    assert_ne!(
        moves
            .fixture
            .apply(ControlCommand::BeginJoin {
                operation_id: nabedra_rt::ids::uuid_v4(),
                expected: state.ready.view,
                target: joiner,
            })
            .await,
        ControlReply::Rejected(Fault::Busy)
    );
}

/// Published: the fence releases the cluster's slot, keeps the group's
/// claim, and the new owner (not the lost one) retires the removed replica.
#[tokio::test]
async fn r05_control1_an_owner_fence_releases_a_published_move_for_retirement() {
    let directory = Directory::new();
    let mut moves = Moves::open(&directory).await;
    let mut plan = moves.plan(ReplicaMovePhase::Claimed);
    plan.phase = published(&plan);
    moves.install(&plan).await;
    let reply = moves.fence().await;
    let ControlReply::Published { ready, owner: Some(owner) } = reply else {
        panic!("the owner fence must resolve a published move: {reply:?}");
    };
    assert_eq!(owner, moves.peer);
    assert!(moves.get(b"ct/replica-move-active".to_vec()).await.is_none());
    assert!(moves.get(key(b"ct/group-replacement/", plan.request.group.group_id)).await.is_some());
    let proof = ReplicaRetiredStamp {
        group: plan.request.group.clone(),
        generation: plan.id,
        replica: plan.old[0].clone(),
        worker: plan.source_worker(),
        applied_at: AppliedPosition { term: 1, node_id: 2, index: 12 },
    };
    let retire = |subject: WorkerIdentity| ControlCommand::ReplicaRetired {
        operation_id: nabedra_rt::ids::uuid_v4(),
        authorization: AuthorizationStamp {
            subject: subject.certificate_sha256,
            evidence: serialized_digest(&proof).unwrap(),
            ..authorization()
        },
        proof: proof.clone(),
        remote: None,
    };
    assert_eq!(
        moves.fixture.apply(retire(moves.owner)).await,
        ControlReply::Rejected(Fault::Unauthorized),
        "the lost owner no longer retires"
    );
    assert_eq!(
        moves.fixture.apply(retire(moves.peer)).await,
        ControlReply::GroupRetirement { generation: plan.id, remaining: 0 }
    );
    assert!(moves.get(key(b"ct/group-replacement/", plan.request.group.group_id)).await.is_none());
    assert_eq!(moves.fixture.state().await.ready, ready);
}

/// Moving: still refused (Busy). The rollback is the planned design
/// (`specs/oltp/control`, `Resolve`); `code_moving_busy` records the gap.
#[tokio::test]
async fn r05_control1_a_moving_move_still_refuses_the_owner_fence() {
    let directory = Directory::new();
    let mut moves = Moves::open(&directory).await;
    let plan = moves.plan(ReplicaMovePhase::Moving);
    moves.install(&plan).await;
    assert_eq!(moves.fence().await, ControlReply::Rejected(Fault::Busy));
    assert!(moves.get(b"ct/replica-move-active".to_vec()).await.is_some());
}

fn json<T: serde::Serialize>(value: &T) -> serde_json::Value {
    serde_json::to_value(value).unwrap()
}
