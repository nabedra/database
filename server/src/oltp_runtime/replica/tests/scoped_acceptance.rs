use super::*;
use crate::oltp::mvcc::{catalog::ShardRoute, store::KeySpan};
use crate::oltp_replication::multiplex::EndpointFuture;
use crate::oltp_runtime::replica::scoped::{
    InstalledQuorum, ScopedAck, ScopedCopy, ScopedPlacement, ScopedTransport, accept,
    confirm_terminal,
};
use std::{future::pending, sync::Mutex, time::Duration};
use tokio::time::Instant as Deadline;

struct Receiver {
    queue: WorkerReplica,
    marker: Option<InstalledQuorum>,
}

struct Wire {
    current: Mutex<ScopedPlacement>,
    replicas: [Mutex<Receiver>; 3],
    install_paused: u8,
    mark_paused: u8,
    terminal_paused: u8,
    calls: Mutex<Vec<(WorkerIdentity, &'static str)>>,
    reached_pause: Mutex<Vec<&'static str>>,
    change_after_marker: Option<ScopedPlacement>,
}

impl Wire {
    fn new(fixture: &Fixture, placement: &ScopedPlacement) -> Self {
        assert_eq!(fixture.authority, placement.authority());
        Self {
            current: Mutex::new(placement.clone()),
            replicas: std::array::from_fn(|i| {
                Mutex::new(Receiver {
                    queue: fixture.replica(i),
                    marker: None,
                })
            }),
            install_paused: 0,
            mark_paused: 0,
            terminal_paused: 0,
            calls: Mutex::default(),
            reached_pause: Mutex::default(),
            change_after_marker: None,
        }
    }

    fn marked(&self) -> usize {
        self.replicas
            .iter()
            .filter(|receiver| receiver.lock().unwrap().marker.is_some())
            .count()
    }
}

impl ScopedTransport for Wire {
    fn current(&self) -> EndpointFuture<'_, ScopedPlacement> {
        Box::pin(async { Ok(self.current.lock().unwrap().clone()) })
    }

    fn copy<'a>(
        &'a self,
        placement: &'a ScopedPlacement,
        sender: WorkerIdentity,
        target: WorkerIdentity,
        copy: ScopedCopy<'a>,
    ) -> EndpointFuture<'a, ScopedAck> {
        let (phase, paused) = match copy {
            ScopedCopy::Install(_) => ("install", self.install_paused),
            ScopedCopy::Mark(_) => ("mark", self.mark_paused),
            ScopedCopy::Terminal { .. } => ("terminal", self.terminal_paused),
        };
        self.calls.lock().unwrap().push((target, phase));
        Box::pin(async move {
            let index = self
                .replicas
                .iter()
                .position(|receiver| receiver.lock().unwrap().queue.local_identity() == target)
                .ok_or_else(|| QueueError::Stale.protocol())?;
            if paused & (1 << index) != 0 {
                self.reached_pause.lock().unwrap().push(phase);
                pending::<()>().await;
            }
            tokio::task::yield_now().await;
            let result = (|| {
                let current = self.current.lock().unwrap().clone();
                let ack = copy.prepare_acknowledgement(placement, &current, sender, target)?;
                let mut receiver = self.replicas[index].lock().unwrap();
                match copy {
                    ScopedCopy::Install(frame) => {
                        receiver
                            .queue
                            .install(placement.authority(), sender, frame)?;
                    }
                    ScopedCopy::Mark(evidence) => {
                        let frame = receiver.queue.frame(evidence.request_id())?;
                        evidence.verify(placement, placement.frame_record(&frame)?)?;
                        receiver.marker = Some(evidence);
                    }
                    ScopedCopy::Terminal { frame, .. } => {
                        copy.check_terminal_marker(
                            receiver.marker.ok_or(QueueError::Unavailable)?,
                        )?;
                        receiver
                            .queue
                            .install(placement.authority(), sender, frame)?;
                    }
                }
                Ok(ack)
            })();
            if result.is_ok()
                && phase == "mark"
                && let Some(next) = &self.change_after_marker
                && self.marked() == 2
            {
                *self.current.lock().unwrap() = next.clone();
            }
            result.map_err(QueueError::protocol)
        })
    }
}

fn route(fixture: &Fixture, table_id: Uuid) -> ShardRoute {
    ShardRoute {
        group: GroupIdentity {
            cluster_id: fixture.scope.cluster_id,
            database: fixture.scope.database.clone(),
            group_id: Uuid::new_v4(),
            kind: GroupKind::Data {
                table_id,
                shard_id: Uuid::new_v4(),
            },
            group_format_version: GROUP_FORMAT_VERSION,
        },
        span: KeySpan {
            start: Some(protocol::Key::Int64(-10)),
            end: Some(protocol::Key::Int64(10)),
        },
        schema_epoch: 8,
        routing_epoch: 9,
    }
}

fn placement(fixture: &Fixture, route: ShardRoute, generation: Uuid) -> ScopedPlacement {
    ScopedPlacement::new(
        route,
        generation,
        fixture.authority.ready.view,
        fixture.authority.owner,
        fixture.members.clone().try_into().unwrap(),
    )
    .unwrap()
}

fn frame(fixture: &Fixture, id: Uuid, table_id: Uuid, key: i64) -> RecordFrame {
    let body = format!(
        r#" {{"request_id":"{id}","operation":"table.update","payload":{{"table_id":"{table_id}","key":{key},"values":{{"live":true}}}}}}"#,
    )
    .into_bytes();
    let context = fixture.context(id, &body);
    fixture
        .replica(0)
        .propose(fixture.authority, body, context, 1)
        .unwrap()
}

fn terminal(fixture: &Fixture, frame: &RecordFrame, result: bool) -> RecordFrame {
    let mut header = frame.decode_header().unwrap();
    let response = format!(
        r#"{{"request_id":"{}","data":{result}}}"#,
        header.request_id
    );
    header.version += 1;
    header.state = RequestState::Succeeded;
    header.accepted = true;
    header.completed_at_ms = Some(2);
    header.response_status = Some(200);
    header.section_lengths[2] = response.len() as u64;
    let mut sections = frame.sections.clone();
    sections[2] = Some(fixture.shared.copy_bytes(response.as_bytes()).unwrap());
    RecordFrame {
        header: fixture
            .shared
            .encode(&header, MAX_RECORD_HEADER_BYTES)
            .unwrap(),
        sections,
    }
}

fn deadline() -> Deadline {
    Deadline::now() + Duration::from_secs(1)
}

#[tokio::test(start_paused = true)]
async fn scoped_q03_q06_delayed_replica_does_not_block_exact_acceptance_or_terminal_copy() {
    let fixture = Fixture::new(3);
    let table = Uuid::new_v4();
    let placement = placement(&fixture, route(&fixture, table), Uuid::new_v4());
    let id = Uuid::new_v4();
    let foreign_id = Uuid::new_v4();
    let foreign = frame(&fixture, foreign_id, table, 1);
    let wrong_shard = frame(&fixture, id, table, 10);
    let frame = frame(&fixture, id, table, 0);
    let mut wire = Wire::new(&fixture, &placement);
    let owner = fixture.authority.owner;
    wire.install_paused = 0b100;
    wire.mark_paused = 0b100;
    wire.terminal_paused = 0b100;
    let due = deadline();
    assert_eq!(
        accept(&placement, owner, &wire, &wrong_shard, due).await,
        Err(QueueError::Conflict)
    );
    assert!(wire.calls.lock().unwrap().is_empty());
    let accepted = accept(&placement, owner, &wire, &frame, due).await.unwrap();
    assert!(Deadline::now() < due);
    assert_eq!(wire.marked(), 2);
    for replica in &wire.replicas[0..2] {
        let replica = replica.lock().unwrap();
        let installed = replica.queue.frame(id).unwrap();
        assert_eq!(
            installed.record_version().unwrap(),
            frame.record_version().unwrap()
        );
        assert_eq!(
            installed.sections[0].as_ref().unwrap().as_ref(),
            frame.sections[0].as_ref().unwrap().as_ref()
        );
        assert_eq!(
            installed.sections[1].as_ref().unwrap().as_ref(),
            frame.sections[1].as_ref().unwrap().as_ref()
        );
    }
    assert!(wire.replicas[2].lock().unwrap().queue.record(id).is_none());

    let response = terminal(&fixture, &frame, true);
    let alternate = terminal(&fixture, &frame, false);
    let mut wrong_body = response.clone();
    let mut bytes = frame.sections[0].as_ref().unwrap().as_ref().to_vec();
    bytes[0] = b'\t';
    wrong_body.sections[0] = Some(fixture.shared.copy_bytes(&bytes).unwrap());
    assert!(matches!(
        confirm_terminal(&placement, owner, &wire, accepted, &wrong_body, due).await,
        Err(QueueError::Conflict)
    ));
    let proof = confirm_terminal(&placement, owner, &wire, accepted, &response, due)
        .await
        .unwrap();
    assert_eq!(proof.verify(&placement, &response), Ok(()));
    assert_eq!(
        proof.verify(&placement, &alternate),
        Err(QueueError::Conflict)
    );
    let observed = wire.replicas[1].lock().unwrap();
    assert_eq!(
        observed.queue.record(id).unwrap().response_body.as_deref(),
        response.sections[2].as_ref().map(AsRef::as_ref)
    );
    assert_eq!(wire.calls.lock().unwrap().len(), 9);
    assert_eq!(
        *wire.reached_pause.lock().unwrap(),
        ["install", "mark", "terminal"]
    );
    drop(observed);
    let foreign_response = terminal(&fixture, &foreign, true);
    let invalid = wire
        .copy(
            &placement,
            owner,
            owner,
            ScopedCopy::Terminal {
                accepted,
                frame: &foreign_response,
            },
        )
        .await;
    assert!(matches!(invalid, Err(error) if error.code == protocol::ErrorCode::Conflict));
    assert!(
        wire.replicas[0]
            .lock()
            .unwrap()
            .queue
            .record(foreign_id)
            .is_none()
    );
    let outsider = WorkerIdentity {
        worker_id: Uuid::new_v4(),
        ..owner
    };
    assert!(
        !wire
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|(target, _)| *target == outsider)
    );
    assert_eq!(
        placement.ensure_sender(&placement, owner, outsider),
        Err(QueueError::Stale)
    );
}

#[tokio::test(start_paused = true)]
async fn scoped_q03_r01_minorities_cannot_accept_and_same_request_retry_keeps_the_record() {
    let fixture = Fixture::new(3);
    let table = Uuid::new_v4();
    let placement = placement(&fixture, route(&fixture, table), Uuid::new_v4());
    let id = Uuid::new_v4();
    let frame = frame(&fixture, id, table, 0);
    let mut wire = Wire::new(&fixture, &placement);
    let owner = fixture.authority.owner;

    assert_eq!(
        accept(&placement, owner, &wire, &frame, Deadline::now()).await,
        Err(QueueError::Unavailable)
    );
    assert!(wire.calls.lock().unwrap().is_empty());

    wire.install_paused = 0b110;
    assert_eq!(
        accept(&placement, owner, &wire, &frame, deadline()).await,
        Err(QueueError::Unavailable)
    );
    assert_eq!(wire.marked(), 0);
    assert_eq!(wire.calls.lock().unwrap().len(), 3);
    assert!(wire.replicas[0].lock().unwrap().queue.record(id).is_some());

    wire.install_paused = 0;
    wire.mark_paused = 0b110;
    assert_eq!(
        accept(&placement, owner, &wire, &frame, deadline()).await,
        Err(QueueError::Unavailable)
    );
    assert_eq!(wire.marked(), 1);

    wire.mark_paused = 0;
    let proof = accept(&placement, owner, &wire, &frame, deadline())
        .await
        .unwrap();
    assert!(wire.marked() >= 2);
    wire.terminal_paused = 0b110;
    let response = terminal(&fixture, &frame, true);
    assert!(matches!(
        confirm_terminal(&placement, owner, &wire, proof, &response, deadline()).await,
        Err(QueueError::Unavailable)
    ));
    assert_eq!(
        wire.replicas[0]
            .lock()
            .unwrap()
            .queue
            .record(id)
            .unwrap()
            .response_body
            .as_deref(),
        response.sections[2].as_ref().map(AsRef::as_ref)
    );
    wire.terminal_paused = 0;
    confirm_terminal(&placement, owner, &wire, proof, &response, deadline())
        .await
        .unwrap()
        .verify(&placement, &response)
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn scoped_q05_r03_old_owner_cannot_acknowledge_after_marker_copies_cross_the_fence() {
    let fixture = Fixture::new(3);
    let table = Uuid::new_v4();
    let route = route(&fixture, table);
    let generation = Uuid::new_v4();
    let placement = placement(&fixture, route.clone(), generation);
    let id = Uuid::new_v4();
    let frame = frame(&fixture, id, table, 0);
    let mut wire = Wire::new(&fixture, &placement);
    wire.install_paused = 0b100;
    wire.mark_paused = 0b100;
    let owner = fixture.authority.owner;
    let next_owner = fixture.members[1];
    let mut members: [WorkerIdentity; 3] = fixture.members.clone().try_into().unwrap();
    members[0].incarnation = Uuid::new_v4();
    members[0].enrollment_revision += 1;
    let old_view = fixture.authority.ready.view;
    let next = ScopedPlacement::new(
        route.clone(),
        generation,
        AuthorityView {
            owner_term: old_view.owner_term + 1,
            membership_revision: old_view.membership_revision + 1,
            ..old_view
        },
        next_owner,
        members,
    )
    .unwrap();
    assert_eq!(
        next.ensure_sender(&next, next_owner, owner),
        Err(QueueError::Stale)
    );
    assert!(matches!(
        ScopedPlacement::new(
            route,
            generation,
            old_view,
            owner,
            [owner, owner, next_owner]
        ),
        Err(QueueError::Invalid)
    ));
    wire.change_after_marker = Some(next);
    assert_eq!(
        accept(&placement, owner, &wire, &frame, deadline()).await,
        Err(QueueError::Stale)
    );
    assert_eq!(wire.marked(), 2);
    let survivor = wire.replicas[1].lock().unwrap();
    assert_eq!(
        survivor.queue.frame(id).unwrap().record_version().unwrap(),
        frame.record_version().unwrap()
    );
}
