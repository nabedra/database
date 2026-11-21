use super::*;

use crate::{
    config::WorkerResources,
    oltp::pool::{OltpPool, SharedResourceConfig},
    oltp_replication::{
        authority::control::{ControlMachine, ControlReply, OperatorPolicy, VerifiedMembership},
        manager::{GroupMembershipObservation, LocalWorker},
    },
};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use tokio::sync::oneshot;

#[test]
fn o06_o18_enrollment_refresh_has_bounded_replica_failover() {
    for request in [
        ReadRequest::EnrollmentRevision,
        ReadRequest::Workers {
            revision: 1,
            after: None,
        },
        ReadRequest::Apis {
            revision: 1,
            after: None,
        },
        ReadRequest::RestartPreparation { revision: 1 },
        ReadRequest::PreparedDataWorker { revision: 1 },
    ] {
        assert!(request.supports_bounded_failover());
    }
}
struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct SingleControlVoter(Uuid);
impl ControlRoutes for SingleControlVoter {
    fn next(&self, after: Option<Uuid>) -> Result<Option<Uuid>, ProtocolError> {
        Ok(after.is_none().then_some(self.0))
    }
}

struct ObservedControl {
    inner: Arc<ControlEndpoint>,
    records: AtomicUsize,
    pages: AtomicUsize,
    pause_page: Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
}
impl Endpoint for ObservedControl {
    fn preflight<'a>(
        &'a self,
        session: &'a AuthenticatedSession,
        header: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        self.inner.preflight(session, header)
    }

    fn handle<'a>(
        &'a self,
        session: AuthenticatedSession,
        header: ChargedBytes,
        sections: ChargedSections,
    ) -> EndpointFuture<'a, ChargedReply> {
        Box::pin(async move {
            match self.inner.parse(&session, header.as_slice())? {
                ReadRequest::Group { .. } => {
                    self.records.fetch_add(1, Ordering::Relaxed);
                }
                ReadRequest::GroupMembers { .. } => {
                    self.pages.fetch_add(1, Ordering::Relaxed);
                    let pause = self.pause_page.lock().unwrap().take();
                    if let Some((reached, resume)) = pause {
                        reached.send(()).unwrap();
                        resume.await.unwrap();
                    }
                }
                _ => {}
            }
            self.inner.handle(session, header, sections).await
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o14_c07_m02_replica_routes_require_fresh_control_record_after_replacement() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let directory = Directory(
            std::env::temp_dir().join(format!("nabedra-control-routes-{}", Uuid::new_v4())),
        );
        let limits = WorkerResources::default();
        let LocalWorker {
            manager,
            shared,
            pool,
            topology,
            enrollment,
            operator,
            transport,
            listener,
            directory: peers,
            ..
        } = LocalWorker::open(
            &directory.0,
            "routes",
            limits,
            SharedResourceConfig::default(),
        )
        .await
        .unwrap();
        let control_group = topology.group(GroupKind::Control).unwrap();
        let group = topology.group(GroupKind::Timestamps).unwrap();
        let member = topology.member();
        let store = pool.group(control_group.clone()).await.unwrap();
        let voters = BTreeMap::from([(member.node_id, member.clone())]);
        let config = openraft::Config {
            heartbeat_interval: 50,
            election_timeout_min: 150,
            election_timeout_max: 300,
            ..Default::default()
        };
        let raft = manager
            .start_control(
                store.clone(),
                member.clone(),
                voters.clone(),
                config.clone(),
            )
            .await
            .unwrap();
        raft.raft.initialize(voters.clone()).await.unwrap();
        let (stop, stopped) = oneshot::channel();
        let listener = tokio::spawn(transport.run(listener, async move {
            let _ = stopped.await;
        }));
        let deadline = Some(Duration::from_secs(5));
        raft.raft
            .wait(deadline)
            .current_leader(member.node_id, "Control leader")
            .await
            .unwrap();
        let control = ControlHandle::new(
            ControlMachine::new(store).unwrap(),
            raft.clone(),
            OperatorPolicy {
                policy: Digest::from_bytes([17; 32]),
                certificate_pins: [enrollment.certificate_sha256].into_iter().collect(),
            },
        );
        let ControlReply::Initialized { ready } = control
            .initialize_local(&operator, Uuid::new_v4(), Uuid::new_v4())
            .await
            .unwrap()
            .reply
        else {
            panic!("expected Control initialization");
        };
        let authority = ready.view.authority();
        assert!(matches!(
            control
                .enroll_local(&operator, Uuid::new_v4(), None, enrollment.clone(), None)
                .await
                .unwrap()
                .reply,
            ControlReply::Enrolled(_)
        ));
        let (reached, first_page) = oneshot::channel();
        let (release, resume) = oneshot::channel();
        let observed = Arc::new(ObservedControl {
            inner: ControlEndpoint::new(control.clone(), shared.clone()).unwrap(),
            records: AtomicUsize::new(0),
            pages: AtomicUsize::new(0),
            pause_page: Mutex::new(Some((reached, resume))),
        });
        manager
            .registry()
            .reserve(control_group.clone())
            .unwrap()
            .bind(Purpose::Authority, observed.clone())
            .unwrap();
        let client = || {
            ControlClient::new(
                manager.client(),
                shared.clone(),
                control_group.clone(),
                Arc::new(SingleControlVoter(enrollment.worker_id)),
            )
            .unwrap()
        };
        let rpc = client();
        peers.refresh(&rpc).await.unwrap();

        let publish_members = async |generation, observation: GroupMembershipObservation| {
            assert!(matches!(
                control
                    .group_page_local(
                        &operator,
                        Uuid::new_v4(),
                        generation,
                        Page::new(observation.replicas().to_vec()).unwrap(),
                    )
                    .await
                    .unwrap()
                    .reply,
                ControlReply::GroupBuilding { count: 1, .. }
            ));
            VerifiedMembership::from_consensus(observation, generation, authority).unwrap()
        };
        let seal = async |proof| {
            assert!(matches!(
                control
                    .seal_group_local(&operator, Uuid::new_v4(), proof)
                    .await
                    .unwrap()
                    .reply,
                ControlReply::GroupRegistered { count: 1, .. }
            ));
        };
        let running = manager
            .start_timestamps(
                pool.group(group.clone()).await.unwrap(),
                member.clone(),
                voters.clone(),
                config.clone(),
            )
            .await
            .unwrap();
        running.raft.initialize(voters.clone()).await.unwrap();
        running
            .raft
            .wait(deadline)
            .current_leader(member.node_id, "original timestamp leader")
            .await
            .unwrap();
        let initial = Uuid::new_v4();
        assert!(matches!(
            control
                .begin_group_local(&operator, Uuid::new_v4(), initial, group.clone())
                .await
                .unwrap()
                .reply,
            ControlReply::GroupBuilding { count: 0, .. }
        ));
        let proof = publish_members(initial, running.observe_membership().await.unwrap()).await;
        seal(proof).await;

        let target = ReplicaMember {
            node_id: member.node_id + 1,
            pool_id: Uuid::new_v4(),
            ..member.clone()
        };
        let target_pool = OltpPool::open_development(
            &directory.0.join("replacement"),
            crate::oltp::pool::PoolIdentity {
                pool_id: target.pool_id,
                ..pool.identity().clone()
            },
            shared.clone(),
        )
        .unwrap();
        let successor = manager
            .start_timestamps(
                target_pool.group(group.clone()).await.unwrap(),
                target.clone(),
                voters,
                config,
            )
            .await
            .unwrap();
        running
            .replicate_learner(running.observe_membership().await.unwrap(), target.clone())
            .await
            .unwrap();
        let finalized_index = running
            .raft
            .change_membership_if(BTreeSet::from([target.node_id]), true, [])
            .await
            .unwrap()
            .log_id()
            .index;
        successor
            .raft
            .wait(deadline)
            .applied_index_at_least(
                Some(finalized_index),
                "replacement timestamp membership applied",
            )
            .await
            .unwrap();
        successor.raft.trigger().elect(false).await.unwrap();
        successor
            .raft
            .wait(deadline)
            .current_leader(target.node_id, "replacement timestamp leader")
            .await
            .unwrap();
        let successor_members = successor.observe_stable_membership().await.unwrap();
        assert_eq!(successor_members.replicas(), std::slice::from_ref(&target));
        let original_record = control.group(group.group_id).await.unwrap().unwrap();
        let replacement = Uuid::new_v4();
        assert!(matches!(
            control
                .begin_group_replacement_local(
                    &operator,
                    Uuid::new_v4(),
                    replacement,
                    original_record.clone()
                )
                .await
                .unwrap()
                .reply,
            ControlReply::GroupBuilding { count: 0, .. }
        ));
        let replacement_proof = publish_members(replacement, successor_members).await;

        let find_during_replacement = rpc.replica_routes(&group);
        tokio::pin!(find_during_replacement);
        tokio::select! {
            _ = &mut find_during_replacement => panic!("membership page barrier was bypassed"),
            first = first_page => first.unwrap(),
        }

        let original = rpc.replica_routes(&group).await.unwrap();
        let hit = rpc.replica_routes(&group).await.unwrap();
        assert!(Arc::ptr_eq(&original, &hit));
        assert_eq!(original.members.as_slice(), std::slice::from_ref(&member));
        assert_eq!(observed.records.load(Ordering::Relaxed), 3);
        assert_eq!(observed.pages.load(Ordering::Relaxed), 2);
        drop(hit);

        assert_eq!(original.record, original_record);
        seal(replacement_proof).await;
        release.send(()).unwrap();
        let stale = find_during_replacement
            .await
            .err()
            .expect("stale page was used");
        assert_eq!(stale.code, ErrorCode::StorageUnavailable);

        let current = rpc.replica_routes(&group).await.unwrap();
        assert_eq!(current.record.generation, replacement);
        assert_ne!(current.record.replica_root, original.record.replica_root);
        assert_eq!(current.members.as_slice(), std::slice::from_ref(&target));
        assert!(
            current.record.membership_position.index > original.record.membership_position.index
        );
        assert_eq!(observed.records.load(Ordering::Relaxed), 4);
        assert_eq!(observed.pages.load(Ordering::Relaxed), 3);

        // An old in-flight service failure cannot evict the replacement.
        rpc.invalidate_replica_routes(&original);
        assert!(Arc::ptr_eq(
            &current,
            &rpc.replica_routes(&group).await.unwrap()
        ));
        assert_eq!(observed.pages.load(Ordering::Relaxed), 3);
        let before = shared.working_decode_bytes();
        let old_bytes = original._owner.bytes();
        drop(original);
        assert_eq!(shared.working_decode_bytes(), before - old_bytes);

        rpc.invalidate_replica_routes(&current);
        let fresh = rpc.replica_routes(&group).await.unwrap();
        assert!(!Arc::ptr_eq(&current, &fresh));
        assert_eq!(observed.records.load(Ordering::Relaxed), 6);
        assert_eq!(observed.pages.load(Ordering::Relaxed), 4);
        assert_eq!(current.record, fresh.record);
        assert_eq!(shared.reply_decode_bytes_used(), 0);

        let (node, _, used) = shared.scheduler().usage()[0];
        let pressure = shared
            .scheduler()
            .charge_memory(node, limits.working_bytes() - used)
            .unwrap();
        assert!(Arc::ptr_eq(
            &fresh,
            &rpc.replica_routes(&group).await.unwrap()
        ));
        let uncached = client();
        assert!(uncached.replica_routes.is_none());
        drop(pressure);

        for _ in 0..2 {
            let verified = uncached.replica_routes(&group).await.unwrap();
            assert_eq!(verified.record, fresh.record);
            assert_eq!(verified.members, fresh.members);
        }
        assert_eq!(observed.records.load(Ordering::Relaxed), 9);
        assert_eq!(observed.pages.load(Ordering::Relaxed), 6);

        stop.send(()).unwrap();
        listener.await.unwrap().unwrap();
        manager.shutdown().await.unwrap();
    })
    .await
    .expect("Control route cache test watchdog");
}
