use super::*;
use crate::{
    config::{ExecutionConfig, WorkerResources},
    oltp_replication::{authority::status::RejectedIdentity, types::AuthorityView},
    oltp_runtime::worker_endpoint::AuthorityReader,
    protocol::{ErrorCode, Outcome},
    request_queue::RequestState,
};
use serde_json::json;
use std::path::PathBuf;
use nabedra_rt::time::timeout;
use tokio::sync::Semaphore;

pub(super) struct CleanupPause {
    pub(super) id: Uuid,
    pub(super) entered: Semaphore,
    pub(super) resume: Semaphore,
}

struct TestDirectory(PathBuf);
impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn create_body(id: Uuid, name: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "request_id": id,
        "operation": "table.create",
        "payload": {"name": name, "schema": {"id": "utf8"}, "key": "id", "format": "parquet"},
    }))
    .unwrap()
}

async fn pending(
    runtime: &WorkerRuntime,
    body: Vec<u8>,
    previous: AuthorityView,
) -> (mpsc::OwnedPermit<PendingRejection>, PendingRejection) {
    use super::super::ReservationResult;
    let prepared = runtime
        .admission
        .prepare(body, runtime.authorization.clone(), super::super::now())
        .await
        .unwrap();
    let gate = runtime
        .gate(prepared.request_id)
        .unwrap()
        .lock_owned()
        .await;
    let ReservationResult::Reserved(reserved) =
        runtime.admission.reserve(prepared, None).await.unwrap()
    else {
        panic!("fixture must reserve a fresh UUID");
    };
    let slot = runtime
        .admission_cleanup
        .reserve(&runtime.services.shared)
        .unwrap();
    let pin = runtime.admission.begin_snapshot(&reserved).await.unwrap();
    let cap = VerifiedNeverAccepted {
        scope: runtime.services.scope.clone(),
        view: previous,
        reservation: reserved.reservation,
        identity: reserved.pin_identity(),
        pin: Some(pin),
        request: None,
        failure: QueueError::Overloaded.protocol(),
        admission_gate: Arc::new(gate),
        live_admission: std::sync::OnceLock::new(),
    };
    runtime.admission_cleanup.hold(slot, cap).unwrap()
}

async fn rejection(runtime: &WorkerRuntime, id: Uuid, digest: Digest) -> RejectedIdentity {
    let Some(IdentityLookup::Rejected(record)) = runtime
        .services
        .status
        .lookup_identity(id, Some(digest))
        .await
        .unwrap()
    else {
        panic!("expected an immutable rejected identity");
    };
    record
}

async fn assert_pin_cancelled(
    runtime: &WorkerRuntime,
    view: AuthorityView,
    record: &RejectedIdentity,
) {
    let error = runtime
        .services
        .timestamps
        .begin_snapshot(view, record.identity.clone(), record.reservation)
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::Conflict);
    assert_eq!(error.outcome, Outcome::NotApplied);
    assert!(error.message.contains("PinExpired"), "{error:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn q02_q06_o09_o13_same_term_ready_revision_reconciles_admission_rejection() {
    timeout(Duration::from_secs(45), async {
        let path = TestDirectory(
            std::env::temp_dir().join(format!("nabedra-admission-{}", nabedra_rt::ids::uuid_v4())),
        );
        let runtime = WorkerRuntime::open(
            &path.0,
            "test",
            "admission-test",
            ExecutionConfig {
                max_pending_requests: 1,
                ..Default::default()
            },
            WorkerResources {
                max_threads: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let fresh = runtime
            .services
            .control
            .current(&runtime.services.scope)
            .await
            .unwrap();
        // Standalone bootstrap has already advanced the initial ready revision.
        // Model an old live ticket without changing the published Control state.
        let previous = AuthorityView {
            membership_revision: fresh.ready.view.membership_revision - 1,
            ..fresh.ready.view
        };
        previous.validate().unwrap();
        assert_eq!(previous.authority(), fresh.ready.view.authority());

        let id = nabedra_rt::ids::uuid_v4();
        let body = create_body(id, "never_accepted");
        let (slot, pending_rejection) = pending(&runtime, body.clone(), previous).await;
        let digest = pending_rejection.cap.reservation.digest;
        let pin = pending_rejection.cap.pin.unwrap();
        let status = &runtime.services.status;
        let timestamps = &runtime.services.timestamps;
        // Advance durable Status while discarding the reply before pin cancellation.
        drop(
            status
                .reject_unaccepted(&pending_rejection.cap)
                .await
                .unwrap(),
        );
        let original = rejection(&runtime, id, digest).await;
        assert_eq!(original.view, previous);
        assert_eq!(original.pin, Some(pin));
        assert_eq!(
            timestamps
                .begin_snapshot(
                    fresh.ready.view,
                    original.identity.clone(),
                    original.reservation
                )
                .await
                .unwrap(),
            pin
        );

        runtime
            .admission_cleanup
            .pause
            .set(CleanupPause {
                id,
                entered: Semaphore::new(0),
                resume: Semaphore::new(0),
            })
            .ok()
            .unwrap();
        drop(slot.send(pending_rejection));
        let pause = runtime.admission_cleanup.pause.get().unwrap();
        pause.entered.acquire().await.unwrap().forget();
        assert!(runtime.gate(id).unwrap().try_lock_owned().is_err());
        let duplicate = runtime.admit(body.clone()).await.unwrap_err();
        assert_eq!(duplicate.outcome, Outcome::Unknown);
        let conflict = runtime
            .admit(create_body(id, "different"))
            .await
            .unwrap_err();
        assert_eq!(conflict.code, ErrorCode::Conflict);

        let unrelated = nabedra_rt::ids::uuid_v4();
        let record = runtime
            .admit(create_body(unrelated, "ordinary"))
            .await
            .unwrap();
        let receipt = loop {
            let receipt = runtime.observe(record.run_id, unrelated).await.unwrap();
            if receipt.state.terminal() {
                break receipt;
            }
            tokio::task::yield_now().await;
        };
        assert_eq!(receipt.state, RequestState::Succeeded);
        assert_eq!(receipt.result.as_ref().unwrap()["name"], "ordinary");
        loop {
            if let Ok(actor) = runtime.actors.try_admit() {
                drop(actor);
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(runtime.gate(id).unwrap().try_lock_owned().is_err());
        pause.resume.add_permits(1);
        drop(runtime.gate(id).unwrap().lock_owned().await);
        assert_eq!(rejection(&runtime, id, digest).await, original);
        assert_pin_cancelled(&runtime, fresh.ready.view, &original).await;
        assert_eq!(
            runtime.admit(body).await.unwrap_err(),
            QueueError::Overloaded.protocol()
        );
        assert!(
            runtime
                .services
                .replica
                .lock()
                .unwrap()
                .record(id)
                .is_none()
        );

        // The other ordering leaves Status Reserved after the lost attempt. The
        // same live gate can establish absence again at the current ready view.
        let id = nabedra_rt::ids::uuid_v4();
        let (slot, pending_rejection) =
            pending(&runtime, create_body(id, "reproved"), previous).await;
        let digest = pending_rejection.cap.reservation.digest;
        let pin = pending_rejection.cap.pin.unwrap();
        drop(slot.send(pending_rejection));
        drop(runtime.gate(id).unwrap().lock_owned().await);
        let renewed = rejection(&runtime, id, digest).await;
        assert_eq!(renewed.view, fresh.ready.view);
        assert_eq!(renewed.pin, Some(pin));
        assert_pin_cancelled(&runtime, fresh.ready.view, &renewed).await;
        runtime.drain().await.unwrap();
    })
    .await
    .expect("native rejection cleanup watchdog");
}
