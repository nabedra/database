//! OQ-2 route 1' (`specs/oltp/queue` at b6f3594e: SealJoin, FenceTso,
//! AbortJoinBuilding, AbortJoinSealed, RemoveMember) and CONTROL-1's pending
//! removal (`specs/oltp/control`): a removal or an owner fence supersedes a
//! sealed join or a pending removal, without publishing the sealed join.
//!
//! The Timestamp group is mirrored by `Tso`, which applies the two rules the
//! real machine applies (`timestamp.rs` FenceReady and Fence): FenceReady needs
//! the exact expected view, and an owner fence keeps the ready root, count and
//! revision. Admission needs the published view at the Timestamp group
//! (`scoped_admission::begin`), which is what G07 checks below.
use super::*;

struct Tso {
    ready: ReadyMembership,
}
impl Tso {
    /// FenceReady is idempotent by transition id: a repeat of the applied
    /// fence returns its receipt.
    fn fence_ready(&mut self, id: Uuid, expected: ReadyMembership, next: ReadyMembership) -> ReadyFenceReceipt {
        assert!(
            self.ready == expected || self.ready == next,
            "the Timestamp group refuses FenceReady from {expected:?} at {:?}",
            self.ready
        );
        self.ready = next;
        ReadyFenceReceipt {
            transition_id: id,
            expected,
            next,
            applied_at: AppliedPosition { term: 1, node_id: 2, index: 100 },
        }
    }
    fn owner_fence(&mut self, ready: ReadyMembership) {
        assert!(
            self.ready.ready_root == ready.ready_root
                && self.ready.ready_count == ready.ready_count
                && self.ready.view.membership_revision == ready.view.membership_revision
                && self.ready.view.owner_term + 1 == ready.view.owner_term,
            "the Timestamp group refuses the owner fence {ready:?} at {:?}",
            self.ready
        );
        self.ready = ready;
    }
}

struct Sealed {
    fixture: Fixture,
    owner: WorkerIdentity,
    peer: WorkerIdentity,
    joiner: WorkerIdentity,
    /// The published ready set before the join (owner, peer and a third).
    published: ReadyMembership,
    join: Uuid,
    /// The join's sealed next view, which holds the joiner.
    next: ReadyMembership,
    tso: Tso,
}
impl Sealed {
    /// SealJoin: a join of `joiner` past Building, with the Timestamp group
    /// still at the published view.
    async fn open(directory: &Directory) -> Self {
        let (scope, pool) = identities();
        let mut fixture = Fixture::open(directory, scope, pool).await;
        fixture.initialize().await;
        let owner = fixture.enroll(1).await;
        let peer = fixture.enroll(2).await;
        let third = fixture.enroll(3).await;
        let joiner = fixture.enroll(4).await;
        let mut members = Vec::new();
        for worker in [owner, peer, third] {
            fixture.add_ready(worker, &mut members).await;
        }
        let published = fixture.state().await.ready;
        let join = nabedra_rt::ids::uuid_v4();
        assert!(matches!(
            fixture
                .apply(ControlCommand::BeginJoin { operation_id: join, expected: published.view, target: joiner })
                .await,
            ControlReply::JoinBegun { .. }
        ));
        members.push(joiner);
        members.sort_by_key(|worker| worker.worker_id);
        assert!(matches!(
            fixture
                .apply(ControlCommand::ReadyPage {
                    operation_id: nabedra_rt::ids::uuid_v4(),
                    transition_id: join,
                    members: Page::new(members).unwrap(),
                })
                .await,
            ControlReply::ReadyBuilding { .. }
        ));
        let ControlReply::ReadySealed { expected, next, .. } = fixture
            .apply(ControlCommand::SealReady { operation_id: nabedra_rt::ids::uuid_v4(), transition_id: join })
            .await
        else {
            panic!("expected a sealed join");
        };
        assert_eq!(expected, published);
        assert_eq!(next.ready_count, 4);
        let sealed = Self { fixture, owner, peer, joiner, published, join, next, tso: Tso { ready: published } };
        sealed.g07().await;
        sealed
    }

    /// FenceTso: the Timestamp group applies the join's FenceReady and
    /// Control records TimestampFenced (the joiner's actor, or the actor of
    /// a superseding change; FenceReady is idempotent).
    async fn fence_tso(&mut self) {
        let receipt = self.tso.fence_ready(self.join, self.published, self.next);
        assert!(matches!(
            self.fixture
                .apply(ControlCommand::TimestampFenced {
                    operation_id: nabedra_rt::ids::uuid_v4(),
                    transition_id: self.join,
                    receipt,
                })
                .await,
            ControlReply::TimestampFenced { .. }
        ));
        self.g07().await;
    }

    /// OLTP-G07 (G07_NoStaleAdmission): an admission needs the published
    /// view at the Timestamp group, and whenever it has it the published
    /// ready set does not hold the joiner, whose copy was never verified.
    async fn g07(&self) {
        let state = self.fixture.state().await;
        let admits = state.ready.view == self.tso.ready.view
            && !matches!(state.pending, Some(Pending::Fence(_)) | Some(Pending::Ready(ReadyPending { change: ReadyChange::Remove, .. })));
        if admits {
            assert_eq!(state.ready.ready_count, 3, "admission with an unverified joiner: {state:?}");
            assert_eq!(state.ready.ready_root, self.published.ready_root);
        }
        assert_eq!(state.ready.ready_count, 3, "the sealed join was published: {state:?}");
    }

    /// RemoveMember of the joiner (AbortJoinSealed's last step): the pending
    /// removal expects the join's fenced view under the current term and
    /// publishes the old set at the next revision once the Timestamp group is
    /// fenced to it, with no queue proof.
    async fn finish_abandoned(&mut self, owner: WorkerIdentity, owner_term: u64) -> ReadyMembership {
        let state = self.fixture.state().await;
        let Some(Pending::Ready(pending)) = state.pending.clone() else {
            panic!("expected the removal of the joiner: {state:?}");
        };
        let mut expected = self.next;
        expected.view.owner_term = owner_term;
        assert_eq!(pending.change, ReadyChange::Remove);
        assert_eq!(pending.target, self.joiner);
        assert_eq!(pending.abandoned_join, Some(self.join));
        assert_eq!(pending.expected, expected, "the removal rebases on the join's next view");
        assert_eq!(pending.phase, ReadyPhase::AwaitingTimestampFence);
        let next = pending.next.unwrap();
        assert_eq!(next.ready_root, self.published.ready_root);
        assert_eq!(next.ready_count, 3);
        assert_eq!(next.view.membership_revision, self.published.view.membership_revision + 2);
        assert_eq!(next.view.owner_term, owner_term);
        self.g07().await;
        // A late page or publication of the abandoned join is refused.
        assert!(matches!(
            self.fixture
                .apply(ControlCommand::PublishReady {
                    operation_id: nabedra_rt::ids::uuid_v4(),
                    transition_id: self.join,
                    proof: QueueCopyStatement {
                        ticket: JoinTicket { ticket_id: self.join, target: self.joiner, source_ready: self.published },
                        source: owner,
                        snapshot_id: nabedra_rt::ids::uuid_v4(),
                        snapshot_cut: 0,
                        final_cut: 0,
                        record_version_hash: hash(b"late join"),
                    },
                })
                .await,
            ControlReply::Rejected(_)
        ));
        let receipt = self.tso.fence_ready(pending.transition_id, pending.expected, next);
        assert_eq!(
            self.fixture
                .apply(ControlCommand::TimestampFenced {
                    operation_id: nabedra_rt::ids::uuid_v4(),
                    transition_id: pending.transition_id,
                    receipt,
                })
                .await,
            ControlReply::Published { ready: next, owner: Some(owner) }
        );
        let state = self.fixture.state().await;
        assert!(state.pending.is_none());
        assert_eq!(state.ready, next);
        assert_eq!(state.ready, self.tso.ready, "admissions reopen at the published view");
        self.g07().await;
        next
    }

    /// The owner is lost and `next_owner` fences a new term. The fence keeps
    /// the Timestamp group's view (the join's next view) and Control
    /// publishes its own set, then installs the removal of the joiner.
    async fn owner_fence(&mut self, next_owner: WorkerIdentity) -> ReadyMembership {
        let reply = self
            .fixture
            .apply(ControlCommand::BeginOwnerFence {
                operation_id: nabedra_rt::ids::uuid_v4(),
                expected: self.published,
                next_owner,
                authorization: authorization(),
            })
            .await;
        let ControlReply::FenceBegun { transition_id, next, .. } = reply else {
            panic!("the owner fence must supersede the sealed join: {reply:?}");
        };
        let mut fenced = self.next;
        fenced.view.owner_term += 1;
        assert_eq!(next, fenced, "the fence keeps the Timestamp group's view");
        self.tso.owner_fence(next);
        self.g07().await;
        let mut published = self.published;
        published.view.owner_term += 1;
        assert_eq!(
            self.fixture
                .apply(ControlCommand::PublishFence { operation_id: nabedra_rt::ids::uuid_v4(), transition_id })
                .await,
            ControlReply::Published { ready: published, owner: Some(next_owner) }
        );
        self.g07().await;
        published
    }

    async fn remove_joiner(&mut self) -> ControlReply {
        self.fixture
            .apply(ControlCommand::BeginRemoval {
                operation_id: nabedra_rt::ids::uuid_v4(),
                expected: self.published.view,
                target: self.joiner,
                exclusion: ExclusionStamp {
                    worker: self.joiner,
                    authorization: authorization(),
                    ownership_fence: hash(b"lost joiner"),
                },
            })
            .await
    }
}

/// OQ-2, owner lost after FenceTso: the owner fence keeps the join's view at
/// the Timestamp group, publishes the old set under the new term, and the new
/// owner removes the joiner. Shipped Control answered Busy.
#[tokio::test]
async fn r05_oq2_owner_lost_after_fence_tso_supersedes_the_sealed_join() {
    let directory = Directory::new();
    let mut sealed = Sealed::open(&directory).await;
    sealed.fence_tso().await;
    let peer = sealed.peer;
    let published = sealed.owner_fence(peer).await;
    let ready = sealed.finish_abandoned(peer, published.view.owner_term).await;
    assert_eq!(sealed.fixture.state().await.owner, Some(peer));
    assert_eq!(ready.view.owner_term, sealed.published.view.owner_term + 1);
}

/// OQ-2, owner lost before FenceTso: the owner fence waits (Busy) until the
/// superseding actor drives the join's idempotent FenceReady and records
/// TimestampFenced, then supersedes as above.
#[tokio::test]
async fn r05_oq2_owner_lost_before_fence_tso_drives_the_join_fence_first() {
    let directory = Directory::new();
    let mut sealed = Sealed::open(&directory).await;
    assert_eq!(
        sealed
            .fixture
            .apply(ControlCommand::BeginOwnerFence {
                operation_id: nabedra_rt::ids::uuid_v4(),
                expected: sealed.published,
                next_owner: sealed.peer,
                authorization: authorization(),
            })
            .await,
        ControlReply::Rejected(Fault::Busy),
        "the Timestamp group may hold either view until FenceTso"
    );
    sealed.fence_tso().await;
    let peer = sealed.peer;
    let published = sealed.owner_fence(peer).await;
    sealed.finish_abandoned(peer, published.view.owner_term).await;
}

/// OQ-2, joiner lost after FenceTso: its removal rebases on the join's next
/// view without publishing the join, and the owner stays.
#[tokio::test]
async fn r05_oq2_joiner_lost_after_fence_tso_is_removed_from_the_join_view() {
    let directory = Directory::new();
    let mut sealed = Sealed::open(&directory).await;
    sealed.fence_tso().await;
    let ControlReply::JoinBegun { ticket, bootstrap } = sealed.remove_joiner().await else {
        panic!("the removal of the joiner must supersede the sealed join");
    };
    assert!(!bootstrap);
    assert_eq!(ticket.source_ready, sealed.next);
    let (owner, term) = (sealed.owner, sealed.published.view.owner_term);
    sealed.finish_abandoned(owner, term).await;
    assert_eq!(sealed.fixture.state().await.owner, Some(owner));
}

/// OQ-2, joiner lost before FenceTso: the removal waits for FenceTso, and the
/// owner's own fence (the supervisor's route for a lost joiner) supersedes
/// the join the same way.
#[tokio::test]
async fn r05_oq2_joiner_lost_before_fence_tso_drives_the_join_fence_first() {
    let directory = Directory::new();
    let mut sealed = Sealed::open(&directory).await;
    assert_eq!(sealed.remove_joiner().await, ControlReply::Rejected(Fault::Busy));
    sealed.fence_tso().await;
    let owner = sealed.owner;
    let published = sealed.owner_fence(owner).await;
    sealed.finish_abandoned(owner, published.view.owner_term).await;
}

/// OLTP-G07 (`G07_NoStaleAdmission`, mutant `publish_join_before_removal`):
/// across a sealed join superseded by an owner fence and a removal, no step
/// publishes the join's set and admissions stay closed (Control's published
/// view differs from the Timestamp group's) until the joiner's removal
/// publishes. `Sealed::g07` runs after every step.
#[tokio::test]
async fn g07_oq2_no_admission_while_the_ready_set_holds_an_unverified_joiner() {
    let directory = Directory::new();
    let mut sealed = Sealed::open(&directory).await;
    sealed.fence_tso().await;
    let state = sealed.fixture.state().await;
    assert_ne!(state.ready.view, sealed.tso.ready.view, "the fenced join closes admissions");
    let peer = sealed.peer;
    let published = sealed.owner_fence(peer).await;
    let state = sealed.fixture.state().await;
    assert_eq!(state.ready, published);
    assert_ne!(state.ready.view, sealed.tso.ready.view, "admissions stay closed after the fence");
    let prefix = ready_prefix(state.ready_generation);
    let raw = sealed
        .fixture
        .machine
        .store()
        .read(BUDGET, move |view| {
            let mut members = Vec::new();
            view.scan_page(&prefix, None, 128, |_, bytes| {
                members.push(decode::<WorkerIdentity>(bytes)?);
                Ok(())
            })?;
            view.encode(&members)
        })
        .await
        .unwrap();
    let keys: Vec<WorkerIdentity> = decode(raw.as_ref()).unwrap();
    assert_eq!(keys.len(), 3);
    assert!(!keys.contains(&sealed.joiner), "the published keys never hold the joiner");
    sealed.finish_abandoned(peer, published.view.owner_term).await;
}

/// CONTROL-1, pending-removal half (`specs/oltp/control`, mutant
/// `prefix_remove_owner_lost`): a removal pending when its owner is lost is
/// carried by the owner fence, sealed or not, and the new owner finishes it.
#[tokio::test]
async fn r05_control1_an_owner_fence_carries_a_pending_removal() {
    for sealed in [false, true] {
        let directory = Directory::new();
        let (scope, pool) = identities();
        let mut fixture = Fixture::open(&directory, scope, pool).await;
        fixture.initialize().await;
        let owner = fixture.enroll(1).await;
        let peer = fixture.enroll(2).await;
        let lost = fixture.enroll(3).await;
        let mut members = Vec::new();
        for worker in [owner, peer, lost] {
            fixture.add_ready(worker, &mut members).await;
        }
        let published = fixture.state().await.ready;
        let removal = nabedra_rt::ids::uuid_v4();
        let ControlReply::JoinBegun { .. } = fixture
            .apply(ControlCommand::BeginRemoval {
                operation_id: removal,
                expected: published.view,
                target: lost,
                exclusion: ExclusionStamp {
                    worker: lost,
                    authorization: authorization(),
                    ownership_fence: hash(b"lost worker"),
                },
            })
            .await
        else {
            panic!("expected a pending removal");
        };
        let mut survivors = vec![owner, peer];
        survivors.sort_by_key(|worker| worker.worker_id);
        let mut tso = Tso { ready: published };
        let mut sealed_next = None;
        if sealed {
            assert!(matches!(
                fixture
                    .apply(ControlCommand::ReadyPage {
                        operation_id: nabedra_rt::ids::uuid_v4(),
                        transition_id: removal,
                        members: Page::new(survivors.clone()).unwrap(),
                    })
                    .await,
                ControlReply::ReadyBuilding { .. }
            ));
            let ControlReply::ReadySealed { next, .. } = fixture
                .apply(ControlCommand::SealReady { operation_id: nabedra_rt::ids::uuid_v4(), transition_id: removal })
                .await
            else {
                panic!("expected a sealed removal");
            };
            let receipt = tso.fence_ready(removal, published, next);
            assert!(matches!(
                fixture
                    .apply(ControlCommand::TimestampFenced {
                        operation_id: nabedra_rt::ids::uuid_v4(),
                        transition_id: removal,
                        receipt,
                    })
                    .await,
                ControlReply::TimestampFenced { .. }
            ));
            sealed_next = Some(next);
        }
        // The owner is lost; the removal's target cannot be the next owner.
        let fence = |next_owner| ControlCommand::BeginOwnerFence {
            operation_id: nabedra_rt::ids::uuid_v4(),
            expected: published,
            next_owner,
            authorization: authorization(),
        };
        assert_eq!(fixture.apply(fence(lost)).await, ControlReply::Rejected(Fault::Stale));
        let reply = fixture.apply(fence(peer)).await;
        let ControlReply::FenceBegun { transition_id, next, .. } = reply else {
            panic!("the owner fence must supersede the pending removal (sealed {sealed}): {reply:?}");
        };
        tso.owner_fence(next);
        let mut fenced = published;
        fenced.view.owner_term += 1;
        assert_eq!(
            fixture
                .apply(ControlCommand::PublishFence { operation_id: nabedra_rt::ids::uuid_v4(), transition_id })
                .await,
            ControlReply::Published { ready: fenced, owner: Some(peer) }
        );
        let state = fixture.state().await;
        let Some(Pending::Ready(carried)) = state.pending else {
            panic!("the fence must carry the removal");
        };
        assert_eq!(carried.transition_id, removal);
        assert_eq!(carried.target, lost);
        assert_eq!(carried.expected, fenced);
        let ticket = JoinTicket { ticket_id: removal, target: lost, source_ready: fenced };
        let ready = match sealed_next {
            None => fixture.publish_ready_members(ticket, &survivors, peer).await,
            Some(mut next) => {
                next.view.owner_term += 1;
                assert_eq!(carried.next, Some(next));
                assert_eq!(carried.phase, ReadyPhase::AwaitingQueueProof);
                assert_eq!(tso.ready, next);
                let proof = QueueCopyStatement {
                    ticket,
                    source: peer,
                    snapshot_id: nabedra_rt::ids::uuid_v4(),
                    snapshot_cut: 0,
                    final_cut: 0,
                    record_version_hash: hash(b"surviving copies"),
                };
                assert_eq!(
                    fixture
                        .apply(ControlCommand::PublishReady {
                            operation_id: nabedra_rt::ids::uuid_v4(),
                            transition_id: removal,
                            proof,
                        })
                        .await,
                    ControlReply::Published { ready: next, owner: Some(peer) }
                );
                next
            }
        };
        assert_eq!(ready.ready_count, 2);
        assert_eq!(ready.view.owner_term, published.view.owner_term + 1);
        assert!(fixture.state().await.pending.is_none());
    }
}

/// The Control state format is checked: a store of another format is
/// refused, never read.
#[tokio::test]
async fn control_state_of_another_format_is_refused() {
    let directory = Directory::new();
    let (scope, pool) = identities();
    let mut fixture = Fixture::open(&directory, scope, pool).await;
    fixture.initialize().await;
    let mut state = fixture.state().await;
    assert_eq!(state.format, CONTROL_STATE_FORMAT);
    assert!(check_format(&state).is_ok());
    state.format = 1;
    assert!(check_format(&state).is_err());
    // A format-1 store has no `format` field at all: it does not decode.
    let mut value = serde_json::to_value(&state).unwrap();
    value.as_object_mut().unwrap().remove("format");
    assert!(serde_json::from_value::<State>(value).is_err());
}
