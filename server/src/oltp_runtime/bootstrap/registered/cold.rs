use super::*;
use crate::oltp_replication::authority::control::{FencePending, InventoryPage};
use crate::oltp_runtime::authority_fence::{fence_groups, publish_fence, transition_id};
use tokio::time::Instant;

pub(super) struct Recovery {
    fence: FencePending,
    prior_holder: Option<WorkerIdentity>,
}

pub(super) async fn begin(
    worker: &RegisteredWorker,
    control: &ControlHandle,
    seed: Uuid,
    deadline: Instant,
    errors: &Mutex<Option<ProtocolError>>,
) -> Result<Recovery, ProtocolError> {
    let cold = worker.cold.as_ref().ok_or_else(unavailable)?;
    let profile = cold.profile;
    let local = WorkerIdentity::from(&worker.enrollment);
    let fence = loop {
        let state = startup_read(deadline, errors, "cold Control authority", || control.inspect())
            .await?
            .ok_or_else(|| failure("cold restart requires a retained Control authority"))?;
        if state.ready.view.run_id != profile.previous_run || state.policy != worker.bootstrap_policy {
            return Err(failure("cold restart expected a different previous run or topology policy"));
        }
        if let Some(Pending::Fence(fence)) = state.pending {
            if !fence.cold
                || fence.transition_id != profile.transition_id
                || fence.next.view.run_id != profile.next_run
                || fence.expected != state.ready
            {
                return Err(failure("cold restart conflicts with the pending Control fence"));
            }
            break fence;
        }
        if local.worker_id == seed {
            startup_read(deadline, errors, "begin cold Control fence", || async {
                accepted(
                    control
                        .begin_cold_fence_local(
                            &worker.operator,
                            profile.transition_id,
                            state.ready,
                            profile.next_run,
                        )
                        .await?
                        .reply,
                )
            })
            .await?;
        }
        wait_startup(deadline).await?;
    };
    let prior_holder = startup_read(deadline, errors, "previous ready holders", || {
        prior_holder(worker, control, &fence)
    })
    .await?;
    if prior_holder.is_some_and(|holder| holder != cold.previous) {
        return Err(failure("cold restart mount differs from the previous queue holder"));
    }
    let previous = startup_read(deadline, errors, "previous worker enrollment", || {
        control.worker(local.worker_id)
    })
    .await?
    .as_ref()
    .map(WorkerIdentity::from)
    .ok_or_else(|| failure("cold restart requires a retained worker enrollment"))?;
    let operation = id(local.incarnation, b"cold enroll");
    cold.retain()?;
    startup_read(deadline, errors, "cold worker enrollment", || async {
        let exclusion = worker.cold_exclusion(operation, previous)?;
        accepted(
            control
                .enroll_local(
                    &worker.operator,
                    operation,
                    Some(previous),
                    worker.enrollment.clone(),
                    Some(exclusion),
                )
                .await?
                .reply,
        )
    })
    .await?;
    eprintln!("nabedra-db: cold worker {} enrollment applied", local.worker_id);
    Ok(Recovery {
        fence,
        prior_holder,
    })
}

async fn prior_holder(
    worker: &RegisteredWorker,
    control: &ControlHandle,
    fence: &FencePending,
) -> Result<Option<WorkerIdentity>, ProtocolError> {
    let local = worker.enrollment.worker_id;
    let voters = &worker.placements.control;
    let mut holder = fence.extra_exclusion.filter(|previous| previous.worker_id == local);
    if fence.extra_exclusion.is_some_and(|previous| voters.local(previous.worker_id).is_none()) {
        return Err(failure("previous cold join holder has no configured mount"));
    }
    let mut after = None;
    loop {
        let bytes = control.ready_page_read(fence.expected, after).await?;
        let _decode = worker.shared.reserve_scratch(
            ByteClass::Progress,
            crate::oltp::mvcc::json_decode_budget(bytes.as_slice())?,
        )?;
        let page: InventoryPage<WorkerIdentity> =
            serde_json::from_slice(bytes.as_slice()).map_err(|_| unavailable())?;
        for previous in page.items.iter() {
            if voters.local(previous.worker_id).is_none() {
                return Err(failure("previous cold ready holder has no configured mount"));
            }
            if previous.worker_id == local && holder.replace(*previous).is_some() {
                return Err(failure("previous cold holder has multiple eligible incarnations"));
            }
        }
        if page.done {
            return Ok(holder);
        }
        if page.after <= after {
            return Err(unavailable());
        }
        after = page.after;
    }
}

pub(super) async fn verify_system_groups(
    worker: &RegisteredWorker,
    control: &ControlHandle,
    rpc: &ControlClient,
    deadline: Instant,
    errors: &Mutex<Option<ProtocolError>>,
) -> Result<(), ProtocolError> {
    let mut replacement = None;
    for placement in worker.placements.all() {
        let routes = startup_read(deadline, errors, "retained system group placement", || {
            rpc.replica_routes(&placement.identity)
        })
        .await?;
        if routes.members.as_slice() == placement.voters.as_ref() {
            continue;
        }
        // A mounted replacement can have changed Raft before publishing
        // the resulting group record. The retained topology can name only
        // the exact next voters in that committed plan.
        if worker.cold.is_none() {
            return Err(failure("retained system group placement differs from configuration"));
        }
        if replacement.is_none() {
            replacement = startup_read(deadline, errors, "mounted system replacement", || {
                control.active_system_replacement()
            })
            .await?;
        }
        if let Some((record, remaining)) = &replacement
            && remaining.contains(&placement.identity.group_id)
            && let Some(plan) = record
                .groups
                .iter()
                .find(|plan| plan.expected == routes.record)
            && plan.next(record.intent.previous.worker_id)?.as_slice() == placement.voters.as_ref()
        {
            continue;
        }
        return Err(failure(
            "retained system group placement differs from configuration",
        ));
    }
    Ok(())
}

pub(super) async fn verify_workers(
    worker: &RegisteredWorker,
    control: &ControlHandle,
) -> Result<(), ProtocolError> {
    // Every configured worker has already re-enrolled with its own local
    // exclusion, including earlier logical removals that can still hold RAM.
    // Reject an additional enrolled worker outside this fixed mount inventory.
    let state = control.inspect().await?.ok_or_else(unavailable)?;
    let bytes = control.workers(state.enrollment_revision, None).await?;
    let _decode = worker.shared.reserve_scratch(
        ByteClass::Progress,
        crate::oltp::mvcc::json_decode_budget(bytes.as_slice())?,
    )?;
    let page: InventoryPage<WorkerEnrollment> =
        serde_json::from_slice(bytes.as_slice()).map_err(|_| unavailable())?;
    let configured = &worker.placements.control;
    if !page.done
        || page.items.len() != configured.voters.len()
        || page.items.iter().any(|old| configured.local(old.worker_id).is_none())
    {
        return Err(failure("registered cold restart requires every enrolled worker mount"));
    }
    Ok(())
}

pub(super) async fn finish(
    recovery: &Recovery,
    worker: &RegisteredWorker,
    control: &ControlHandle,
    rpc: &Arc<ControlClient>,
    seed: Uuid,
    deadline: Instant,
    errors: &Mutex<Option<ProtocolError>>,
) -> Result<(), ProtocolError> {
    let fence = &recovery.fence;
    if let Some(holder) = recovery.prior_holder {
        startup_read(deadline, errors, "cold prior holder exclusion", || async {
            if control.excluded(fence.transition_id, holder).await? {
                return Ok(());
            }
            let local = WorkerIdentity::from(&worker.enrollment);
            let op = transition_id("cold-exclusion", &(fence.transition_id, holder, local))?;
            let proof = worker.cold_exclusion(op, holder)?;
            accepted(
                control
                    .excluded_local_page(&worker.operator, op, fence.transition_id, vec![proof])
                    .await?
                    .reply,
            )
        })
        .await?;
    }
    let required_exclusions = fence
        .expected
        .ready_count
        .checked_add(u64::from(fence.extra_exclusion.is_some()))
        .ok_or_else(unavailable)?;
    loop {
        let state = startup_read(deadline, errors, "pending cold fence", || control.inspect())
            .await?
            .ok_or_else(unavailable)?;
        if state.ready.view.run_id == fence.next.view.run_id {
            break;
        }
        let Some(Pending::Fence(pending)) = state.pending else {
            return Err(failure("pending cold fence disappeared"));
        };
        if pending.transition_id != fence.transition_id {
            return Err(failure("pending cold fence changed identity"));
        }
        if worker.enrollment.worker_id == seed && pending.excluded_workers == required_exclusions {
            startup_read(deadline, errors, "cold group-local fences", || {
                fence_groups(control, &worker.manager, rpc, fence)
            })
            .await?;
            startup_read(deadline, errors, "publish cold run", || {
                publish_fence(control, &worker.operator, fence)
            })
            .await?;
            break;
        }
        wait_startup(deadline).await?;
    }
    eprintln!(
        "nabedra-db: registered cold run {} published",
        fence.next.view.run_id
    );
    Ok(())
}
