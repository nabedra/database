use super::*;
use crate::oltp_replication::{
    AppliedPeerDirectory, authority::control::ReplacementMountProof, manager::ManagedControl,
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
    let mut mount_committed = worker.replacement_mount_committed;
    let next_run = worker.cold.as_ref().map(|cold| cold.profile.next_run);
    let serving_run = next_run.unwrap_or(record.intent.run_id);
    loop {
        let mut mounted = false;
        if let Some((applied, accepted, complete, run)) =
            control.replacement_record_local(record.intent.id).await?
        {
            if &applied != record {
                return Err(failure("system replacement differs from committed intent"));
            }
            if next_run.is_some() && run == record.intent.run_id {
                // The retained candidate has never held the old RAM queue.
                // Keep its replicas running while the survivors fence that
                // queue, then resume the mounted intent in the new run.
                wait_startup(deadline).await?;
                continue;
            }
            if run != serving_run {
                return Err(failure("system replacement run differs from Control"));
            }
            if complete && worker.manager.replacement_applied(record).await.is_ok() {
                let state = control.inspect().await?.ok_or_else(unavailable)?;
                if state.ready.view.run_id != serving_run || state.policy != worker.bootstrap_policy
                {
                    return Err(failure(
                        "system replacement run or bootstrap policy differs",
                    ));
                }
                #[cfg(test)]
                crate::oltp_runtime::recovery_tests::pause(
                    "system_replacement_rf3_applied",
                    record.intent.next.worker_id,
                )
                .await;
                return Ok(());
            }
            mounted = accepted;
        }
        if mounted {
            // If leadership moves to this worker between the final Raft
            // transition and its Control publication, it can finish that
            // publication without acquiring queue readiness first.
            if let Err(error) = worker
                .manager
                .advance_system_replacement(control, running, &worker.operator)
                .await
            {
                tracing::debug!(code=?error.code, message=error.message, "system replacement promotion waits");
            }
        } else if !mount_committed {
            for enrollment in original
                .iter()
                .filter(|peer| peer.worker_id != record.intent.previous.worker_id)
            {
                match worker
                    .manager
                    .send_mount_proof(control.identity(), WorkerIdentity::from(enrollment), &proof)
                    .await
                {
                    Ok(()) => {
                        mount_committed = true;
                        break;
                    }
                    Err(error) if error.code == ErrorCode::Conflict => {
                        // A first mount may have committed while its response was lost.
                        // This process still owns the exact directory it opened.
                        if worker
                            .manager
                            .check_replacement_mount(
                                control.identity(),
                                WorkerIdentity::from(enrollment),
                                &proof,
                                true,
                            )
                            .await
                            .is_ok_and(|mounted| mounted)
                        {
                            mount_committed = true;
                            break;
                        }
                        return Err(error);
                    }
                    Err(error) if error.code == ErrorCode::UnsupportedOperation => {
                        return Err(error);
                    }
                    Err(error) => {
                        tracing::debug!(code=?error.code, message=error.message, "system replacement mount waits")
                    }
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
    nabedra_rt::task::spawn(async move {
        let mut tick = nabedra_rt::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = stop.changed() => return,
                _ = tick.tick() => {}
            }
            if directory.has_mounted_replacement() || directory.has_deferred_data_repairs() {
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
