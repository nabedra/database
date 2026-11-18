use super::*;
use crate::oltp_replication::authority::control::{Fault, ReadyChange, State};
use tokio::time::Instant;

pub(super) async fn register_mount(
    worker: &RegisteredWorker,
    control: &ControlHandle,
    deadline: Instant,
    errors: &Mutex<Option<ProtocolError>>,
) -> Result<(), ProtocolError> {
    let Some(mount) = &worker.restart_mount else {
        return Ok(());
    };
    let identity = WorkerIdentity::from(&worker.enrollment);
    let operation = id(identity.incarnation, b"rolling-register-mount");
    let reply = startup_read(deadline, errors, "register retained mount", || {
        control.register_restart_mount(
            &worker.operator,
            operation,
            worker.pool.identity().pool_id,
            mount.generation,
            &mount.key,
        )
    })
    .await?;
    match reply.reply {
        ControlReply::RestartMountRegistered(worker) if worker == identity => Ok(()),
        _ => Err(failure("retained mount restart key registration differs")),
    }
}

pub(super) async fn enroll(
    worker: &RegisteredWorker,
    control: &ControlHandle,
    deadline: Instant,
    errors: &Mutex<Option<ProtocolError>>,
) -> Result<(), ProtocolError> {
    let rolling = worker.rolling.as_ref().ok_or_else(unavailable)?;
    let mount = worker.restart_mount.as_ref().ok_or_else(unavailable)?;
    let intent = &rolling.intent;
    let identity = WorkerIdentity::from(&intent.next);
    let operation = id(intent.id, b"rolling-complete-enrollment");
    startup_read(
        deadline,
        errors,
        "complete retained worker enrollment",
        || async {
            let exclusion = worker.restart_exclusion(operation)?;
            let applied = control
                .complete_restart(
                    &worker.operator,
                    operation,
                    intent.clone(),
                    &mount.key,
                    exclusion,
                )
                .await?;
            match applied.reply {
                ControlReply::Enrolled(worker) if worker == identity => Ok(()),
                ControlReply::Rejected(Fault::Busy | Fault::NotReady) => Err(unavailable()),
                _ => Err(failure("rolling restart enrollment was refused by Control")),
            }
        },
    )
    .await?;
    rolling.retain()?;
    eprintln!(
        "nabedra-db: rolling worker {} enrollment applied",
        identity.worker_id
    );
    Ok(())
}

pub(super) async fn begin_queue_join(
    control: &ControlHandle,
    operator: &LocalOperatorPermit,
    local: WorkerIdentity,
    seed: Uuid,
    deadline: Instant,
    errors: &Mutex<Option<ProtocolError>>,
) -> Result<(State, JoinTicket, bool), ProtocolError> {
    loop {
        let state = startup_read(deadline, errors, "ready join phase", || control.inspect())
            .await?
            .ok_or_else(unavailable)?;
        let bootstrap = state.ready.ready_count == 0;
        // Only the configured seed creates the first RAM holder. Later
        // contenders use the currently published revision and serialized phase.
        if let Some(Pending::Ready(pending)) = &state.pending {
            if pending.change == ReadyChange::Join
                && pending.target == local
                && pending.phase == ReadyPhase::Building
                && pending.source_after.is_none()
                && !pending.target_seen
            {
                let ticket = JoinTicket {
                    ticket_id: pending.transition_id,
                    target: local,
                    source_ready: pending.expected,
                };
                return Ok((state, ticket, bootstrap));
            }
        } else if state.pending.is_none() && (!bootstrap || local.worker_id == seed) {
            let operation = crate::oltp_runtime::authority_fence::transition_id(
                "registered-queue-join",
                &(local, state.ready),
            )?;
            let reply = startup_read(deadline, errors, "begin ready join", || {
                control.begin_join_local(operator, operation, state.ready.view, local)
            })
            .await?;
            match reply.reply {
                ControlReply::JoinBegun { ticket, bootstrap } => {
                    return Ok((state, ticket, bootstrap));
                }
                ControlReply::Rejected(Fault::Busy | Fault::Stale | Fault::NotReady) => {}
                _ => {
                    return Err(failure(
                        "registered worker cannot join the current ready revision",
                    ));
                }
            }
        }
        wait_startup(deadline).await?;
    }
}
