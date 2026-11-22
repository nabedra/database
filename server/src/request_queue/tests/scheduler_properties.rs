use super::*;
use nabedra_check::{Gen, prop};
use serde_json::json;

fn submit(q: &mut RequestQueue, request: Uuid, time: u64) -> Result<(), QueueError> {
    q.propose(body(request.as_u128()), time)?;
    acknowledge_copies(q, request)
}

fn acknowledge_copies(q: &mut RequestQueue, request: Uuid) -> Result<(), QueueError> {
    for worker in q.members().clone() {
        q.acknowledge(request, worker)?;
    }
    Ok(())
}

fn verify(q: &RequestQueue, model: &BTreeMap<Uuid, RequestState>, time: u64) -> Result<(), String> {
    let actual: BTreeMap<_, _> = q.records().map(|r| (r.request_id, r.state)).collect();
    if actual != *model || q.pending() != model.values().filter(|s| !s.terminal()).count() {
        return Err(format!(
            "request conservation: expected {model:?}, got {actual:?}"
        ));
    }
    for (&request, &state) in model {
        let receipt = q.lookup(q.run_id(), request, time).map_err(|error| {
            format!("accepted request {request} lost its {state:?} receipt: {error:?}")
        })?;
        let valid_result = match state {
            RequestState::Succeeded => receipt.result == Some(json!([])) && receipt.error.is_none(),
            RequestState::Failed => {
                receipt.result.is_none()
                    && receipt.error
                        == Some(ProtocolError::new(
                            ErrorCode::Conflict,
                            "cancelled",
                            Outcome::NotApplied,
                        ))
            }
            _ => receipt.result.is_none() && receipt.error.is_none(),
        };
        if receipt.request_id != request
            || receipt.run_id != q.run_id()
            || receipt.state != state
            || !valid_result
            || !q.ready(request)
        {
            return Err(format!(
                "bad receipt or missing replica ACK for {request}: {receipt:?}"
            ));
        }
    }
    let (queue_bytes, result_bytes) = q.charged_bytes();
    if queue_bytes > limits().max_queue_bytes_per_replica
        || result_bytes > limits().max_retained_result_bytes_per_worker
    {
        return Err(format!(
            "queue capacity exceeded: queue {queue_bytes}, results {result_bytes}"
        ));
    }
    Ok(())
}

fn handoff(
    old: RequestQueue,
    members: BTreeSet<Uuid>,
    model: &mut BTreeMap<Uuid, RequestState>,
) -> Result<RequestQueue, String> {
    let stale = old.snapshot();
    let mut survivor = RequestQueue::new(
        old.run_id(),
        old.scheduler_term(),
        old.membership_revision(),
        limits(),
        old.members().clone(),
    )
    .unwrap();
    survivor
        .install_snapshot(stale.clone())
        .map_err(|e| format!("copy before handoff: {e:?}"))?;
    drop(old);
    survivor
        .takeover_in_place(
            stale.scheduler_term + 1,
            stale.membership_revision + 1,
            members,
        )
        .map_err(|e| format!("survivor takeover: {e:?}"))?;
    if survivor.install_snapshot(stale) != Err(QueueError::Stale) {
        return Err("stale snapshot accepted after the membership fence".into());
    }
    for (&request, state) in model.iter_mut() {
        if *state == RequestState::Running {
            *state = RequestState::Reconciling;
        }
        acknowledge_copies(&mut survivor, request)
            .map_err(|e| format!("acknowledge {request} after handoff: {e:?}"))?;
    }
    Ok(survivor)
}

fn run_trace(g: &mut Gen) -> Result<(), String> {
    let mut live = queue(&[2, 3]);
    let mut next_request = 10_000;
    let mut model = BTreeMap::new();
    let mut time = 0;
    for replacement in 2..=3 {
        let steps = g.range_i64(12, 24) as u64;
        for _ in 0..steps {
            time += 1;
            let step = |e| format!("run {replacement}, step {time}: {e:?}");
            let action = g.below(8);
            let target = (!model.is_empty()).then(|| {
                *model
                    .keys()
                    .nth(g.below(model.len() as u64) as usize)
                    .unwrap()
            });
            match (action, target) {
                (0 | 1, _) | (_, None) => {
                    let request = id(next_request);
                    next_request += 1;
                    submit(&mut live, request, time).map_err(step)?;
                    model.insert(request, RequestState::Queued);
                }
                (2, Some(request))
                    if matches!(
                        model[&request],
                        RequestState::Queued | RequestState::Reconciling
                    ) =>
                {
                    live.start(request, Some(b"pin".to_vec())).map_err(step)?;
                    acknowledge_copies(&mut live, request).map_err(step)?;
                    model.insert(request, RequestState::Running);
                }
                (3 | 4, Some(request)) if model[&request] == RequestState::Running => {
                    let (status, response, state) = if action == 3 {
                        (200, success(request), RequestState::Succeeded)
                    } else {
                        let response = json!({
                            "request_id": request,
                            "error": ProtocolError::new(ErrorCode::Conflict, "cancelled", Outcome::NotApplied),
                        });
                        (
                            409,
                            serde_json::to_vec(&response).unwrap(),
                            RequestState::Failed,
                        )
                    };
                    live.finish(request, status, response, time).map_err(step)?;
                    acknowledge_copies(&mut live, request).map_err(step)?;
                    model.insert(request, state);
                }
                (5, Some(request)) => {
                    let old_record = live.record(request).unwrap().clone();
                    if live.propose(body(request.as_u128()), time).map_err(step)? != old_record {
                        return Err(format!(
                            "U03 step {time}: retry changed accepted request {request}"
                        ));
                    }
                }
                (6, _) if live.members().len() < 3 => {
                    let mut members = live.members().clone();
                    members.insert(id(u128::from(time) + 100));
                    live = handoff(live, members, &mut model)?;
                }
                (7, _) if live.members().len() > 1 => {
                    let mut survivors = live.members().clone();
                    survivors.pop_first();
                    live = handoff(live, survivors, &mut model)?;
                }
                _ => {}
            }
            verify(&live, &model, time).map_err(|error| format!("U03 step {time}: {error}"))?;
        }

        // Every run reaches the last-worker boundary with both queued work and
        // a pinned in-flight read, even if the generated prefix was all joins.
        let queued = id(next_request);
        let running = id(next_request + 1);
        next_request += 2;
        for request in [queued, running] {
            submit(&mut live, request, time).map_err(|e| format!("pre-stop submit: {e:?}"))?;
            model.insert(request, RequestState::Queued);
        }
        live.start(running, Some(b"pin".to_vec()))
            .map_err(|e| format!("pre-stop pin: {e:?}"))?;
        acknowledge_copies(&mut live, running).map_err(|e| format!("pre-stop ack: {e:?}"))?;
        model.insert(running, RequestState::Running);
        verify(&live, &model, time)?;

        while live.members().len() > 1 {
            let mut survivors = live.members().clone();
            survivors.pop_first();
            live = handoff(live, survivors, &mut model)?;
            verify(&live, &model, time)?;
        }
        // Retain bytes only as a delayed replay input. No worker holds a queue.
        let stale = live.snapshot();
        drop(live);
        let replacement = id(1_000 + replacement);
        live = RequestQueue::new(
            replacement,
            stale.scheduler_term,
            stale.membership_revision,
            limits(),
            stale.members.clone(),
        )
        .unwrap();
        if live.install_snapshot(stale.clone()) != Err(QueueError::Stale) {
            return Err("SCHED-G04: cold run accepted a replayed snapshot".into());
        }
        for record in stale.records {
            let request = record.request_id;
            if live.install(record) != Err(QueueError::Stale)
                || live.lookup(stale.run_id, request, time) != Err(QueueError::Abandoned)
                || live.lookup(replacement, request, time) != Err(QueueError::NotFound)
            {
                return Err(format!(
                    "SCHED-G04: old request {request} survived the last worker stop"
                ));
            }
        }
        model.clear();
        verify(&live, &model, time)?;
        let fresh = id(next_request);
        next_request += 1;
        submit(&mut live, fresh, time).map_err(|e| format!("cold-run submit: {e:?}"))?;
        model.insert(fresh, RequestState::Queued);
        verify(&live, &model, time)?;
    }
    Ok(())
}

#[test]
fn generated_queue_events_abandon_old_work_only_after_the_last_worker_stops() {
    prop::check("U03 SCHED-G04 OLAP-G07 queue lifetime", run_trace);
}
