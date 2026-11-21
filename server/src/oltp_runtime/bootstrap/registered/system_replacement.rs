use super::*;
use crate::oltp_replication::{
    authority::control::ReplacementMountProof,
    manager::ManagedControl,
    multiplex::AppliedPeerDirectory,
};
use std::time::Duration;
use tokio::time::Instant;

pub(super) async fn join(
    worker: &RegisteredWorker,
    control: &ControlHandle,
    running: &ManagedControl,
    original: &[WorkerEnrollment],
    deadline: Instant,
) -> Result<(), ProtocolError> {
    let record = worker.system_replacement.as_ref().ok_or_else(unavailable)?;
    let key = &worker.restart_mount.as_ref().ok_or_else(unavailable)?.key;
    // RegisteredWorker validates the configured expectation against the
    // mounted directory and holds both locks through the peer exchange.
    let proof = ReplacementMountProof::sign(control.identity(), record, &record.intent.mount, key)?;
    loop {
        let mut mounted = false;
        if let Some((applied, accepted, complete)) = control.replacement_record_local(record.intent.id).await? {
            if &applied != record {
                return Err(failure("system replacement differs from committed intent"));
            }
            if complete && worker.manager.replacement_applied(record).await.is_ok() {
                let state = control.inspect().await?.ok_or_else(unavailable)?;
                if state.ready.view.run_id != record.intent.run_id || state.policy != worker.bootstrap_policy {
                    return Err(failure("system replacement run or bootstrap policy differs"));
                }
                return Ok(());
            }
            mounted = accepted;
        }
        if mounted {
            // If leadership moves to this worker between the final Raft
            // transition and its Control publication, it can finish that
            // publication without acquiring queue readiness first.
            if let Err(error) = worker.manager.advance_system_replacement(control, running, &worker.operator).await {
                tracing::debug!(code=?error.code, message=error.message, "system replacement promotion waits");
            }
        } else {
            for enrollment in original.iter().filter(|peer| peer.worker_id != record.intent.previous.worker_id) {
                match worker.manager.send_mount_proof(control.identity(), WorkerIdentity::from(enrollment), &proof).await {
                    Ok(()) => break,
                    Err(error) if error.code == ErrorCode::Conflict => return Err(error),
                    Err(error) => tracing::debug!(code=?error.code, message=error.message, "system replacement mount waits"),
                }
            }
        }
        wait_startup(deadline).await?;
    }
}

pub(super) fn spawn(
    manager: Arc<HostedGroupManager>,
    control: Arc<ControlHandle>,
    running: ManagedControl,
    directory: Arc<AppliedPeerDirectory>,
    operator: Arc<LocalOperatorPermit>,
    mut stop: watch::Receiver<bool>,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = stop.changed() => return,
                _ = tick.tick() => {}
            }
            if directory.has_mounted_replacement() {
                tokio::select! {
                    _ = stop.changed() => return,
                    result = manager.advance_system_replacement(&control, &running, &operator) => {
                        if let Err(error) = result {
                            tracing::debug!(code=?error.code, message=error.message, "system replacement promotion waits");
                        }
                    }
                }
            }
        }
    });
}
