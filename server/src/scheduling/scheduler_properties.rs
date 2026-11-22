use super::{ResourceScheduler, WorkerResources, tests::poll_once};
use crate::protocol::{ErrorCode, ProtocolError};
use crate::scheduling::{MemoryCharge, Reservation};
use nabedra_check::{Gen, prop};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
    task::Poll,
};

type Waiter = Pin<Box<dyn Future<Output = Result<Reservation, ProtocolError>>>>;

fn limits(threads: u64, working: u64) -> WorkerResources {
    WorkerResources {
        max_threads: threads,
        memory_limit_bytes: working + 100,
        task_memory_limit_bytes: working,
        control_memory_reserve_bytes: 100,
    }
}

fn request_cap(request: u64) -> usize {
    (request % 3 + 1) as usize
}

fn submit(scheduler: &ResourceScheduler, request: u64, bytes: u64) -> Waiter {
    let scheduler = scheduler.clone();
    Box::pin(async move {
        scheduler
            .reserve(request, request_cap(request), bytes)
            .await
    })
}

fn grant(waiter: &mut Waiter, context: &str) -> Result<Reservation, String> {
    match poll_once(waiter.as_mut()) {
        Poll::Ready(Ok(grant)) => Ok(grant),
        Poll::Ready(Err(error)) => Err(format!("{context}: reservation rejected: {error:?}")),
        Poll::Pending => Err(format!("{context}: eligible reservation did not advance")),
    }
}

#[derive(Clone, Copy)]
enum Admission {
    Normal,
    PastThreadCapacity,
}

fn capacity_trace(g: &mut Gen, admission: Admission) -> Result<(), String> {
    let initial = limits(g.range_i64(1, 4) as u64, 1_200);
    let scheduler = ResourceScheduler::new(vec![(0, initial)]).unwrap();
    let mut configured = BTreeMap::from([(0, initial)]);
    let mut drained = BTreeSet::new();
    let mut active: Vec<(u64, Reservation)> = Vec::new();
    let mut waiting: Vec<(u64, Waiter)> = Vec::new();
    let mut memory: Vec<MemoryCharge> = Vec::new();
    let steps = g.range_i64(32, 64);

    for step in 0..steps {
        if step <= initial.max_threads as i64 {
            if step == initial.max_threads as i64
                && matches!(admission, Admission::PastThreadCapacity)
            {
                // Simulate an admission fence one slot above the committed capacity.
                scheduler
                    .update_node(0, limits(initial.max_threads + 1, 1_200))
                    .unwrap();
            }
            let request = step as u64;
            waiting.push((request, submit(&scheduler, request, 64)));
        } else {
            let node = g.below(4);
            let request = g.below(8);
            match g.below(10) {
                0..=2 => waiting.push((request, submit(&scheduler, request, g.below(600) + 1))),
                3 if !active.is_empty() => {
                    drop(active.swap_remove(g.below(active.len() as u64) as usize));
                }
                4 if !waiting.is_empty() => {
                    drop(waiting.swap_remove(g.below(waiting.len() as u64) as usize));
                }
                5 => {
                    if let Ok(charge) = scheduler.charge_memory(node, g.below(300) + 1) {
                        memory.push(charge);
                    }
                }
                6 if !memory.is_empty() => {
                    drop(memory.swap_remove(g.below(memory.len() as u64) as usize));
                }
                7 if configured.contains_key(&node) => {
                    scheduler.drain_node(node).unwrap();
                    drained.insert(node);
                }
                8 | 9 => {
                    let new = limits(g.below(4) + 1, 900 + g.below(11) * 100);
                    if scheduler.update_node(node, new).is_ok() {
                        configured.insert(node, new);
                        drained.remove(&node);
                    }
                }
                _ => {}
            }
        }

        let mut i = 0;
        while i < waiting.len() {
            match poll_once(waiting[i].1.as_mut()) {
                Poll::Pending => i += 1,
                Poll::Ready(result) => {
                    let (request, _) = waiting.swap_remove(i);
                    match result {
                        Ok(grant) if drained.contains(&grant.node_id()) => {
                            return Err(format!(
                                "U03 step {step}: grant on drained node {}",
                                grant.node_id()
                            ));
                        }
                        Ok(grant) => active.push((request, grant)),
                        Err(error) if error.code == ErrorCode::LimitExceeded => {}
                        Err(error) => {
                            return Err(format!("U03 step {step}: unexpected rejection {error:?}"));
                        }
                    }
                }
            }
        }
        let usage = scheduler.usage();
        if usage.len() != configured.len() {
            return Err(format!(
                "SCHED-G01 U03 step {step}: missing or extra node usage {usage:?}"
            ));
        }
        for (node, threads, bytes) in usage {
            let Some(cap) = configured.get(&node) else {
                return Err(format!("SCHED-G01 U03 step {step}: unexpected node {node}"));
            };
            if threads > cap.max_threads || bytes > cap.working_bytes() {
                return Err(format!(
                    "SCHED-G01 capacity exceeded at U03 step {step}, node {node}:                      threads {threads}/{}, bytes {bytes}/{}",
                    cap.max_threads,
                    cap.working_bytes(),
                ));
            }
            let owners: Vec<_> = active
                .iter()
                .filter(|(_, grant)| grant.node_id() == node)
                .collect();
            let owned_bytes = owners.iter().map(|(_, grant)| grant.bytes()).sum::<u64>()
                + memory
                    .iter()
                    .filter(|charge| charge.node_id() == node)
                    .map(MemoryCharge::bytes)
                    .sum::<u64>();
            if (threads, bytes) != (owners.len() as u64, owned_bytes) {
                return Err(format!(
                    "SCHED-G01 U03 step {step}: node {node} usage differs from reservation owners"
                ));
            }
        }
        for request in 0..8 {
            let nodes: BTreeSet<_> = active
                .iter()
                .filter(|(owner, _)| *owner == request)
                .map(|(_, grant)| grant.node_id())
                .collect();
            if nodes.len() > request_cap(request) {
                return Err(format!(
                    "SCHED-G01 U03 step {step}: request {request} exceeded its worker cap"
                ));
            }
        }
    }
    drop((waiting, active, memory));
    if scheduler
        .usage()
        .iter()
        .any(|&(_, threads, bytes)| (threads, bytes) != (0, 0))
    {
        return Err(format!(
            "SCHED-G01 U03: unsettled reservations {:?}",
            scheduler.usage()
        ));
    }
    Ok(())
}

#[test]
fn generated_events_conserve_admitted_resources() {
    prop::check("U03 SCHED-G01 resource and owner conservation", |g| {
        capacity_trace(g, Admission::Normal)
    });
}

#[test]
fn over_admission_is_caught_by_capacity_property() {
    let seed = 41;
    assert_eq!(
        capacity_trace(&mut Gen::from_seed(seed), Admission::Normal),
        Ok(())
    );
    let error =
        capacity_trace(&mut Gen::from_seed(seed), Admission::PastThreadCapacity).unwrap_err();
    assert!(error.starts_with("SCHED-G01 capacity exceeded"), "{error}");
    eprintln!("Planted over-admission regression caught: {error}");
}

#[test]
fn next_grant_respects_the_dominant_share_bound() {
    prop::check("U03 SCHED-G02 dominant-share bound", |g| {
        let cap = limits(4, 1_200);
        let scheduler = ResourceScheduler::new(vec![(0, cap)]).unwrap();
        let a_bytes = g.range_i64(40, 220) as u64;
        let b_bytes = g.range_i64(160, 880) as u64;
        // A holds two slots. B sometimes has a larger dominant share even
        // with one slot, because its memory share exceeds A's CPU share.
        let a1 = grant(&mut submit(&scheduler, 1, a_bytes / 2), "first A job")?;
        let a2 = grant(
            &mut submit(&scheduler, 1, a_bytes - a1.bytes()),
            "second A job",
        )?;
        let b = grant(&mut submit(&scheduler, 2, b_bytes), "B job")?;
        let gate = grant(&mut submit(&scheduler, 3, 20), "one-slot gate")?;
        let job_bytes = g.range_i64(20, 80) as u64;
        let mut waiting = [
            submit(&scheduler, 1, job_bytes),
            submit(&scheduler, 2, job_bytes),
        ];
        if waiting
            .iter_mut()
            .any(|waiter| poll_once(waiter.as_mut()).is_ready())
        {
            return Err("SCHED-G02: contender ran before a slot was available".into());
        }
        drop(gate);
        // Exact normalized shares use a common denominator of total slots
        // times total working bytes.
        let share =
            |threads, bytes| std::cmp::max(threads * cap.working_bytes(), bytes * cap.max_threads);
        let incumbent = [(2, a1.bytes() + a2.bytes()), (1, b.bytes())];
        let minimum = incumbent
            .iter()
            .map(|&(threads, bytes)| share(threads, bytes))
            .min()
            .unwrap();
        let grants: Vec<_> = waiting
            .iter_mut()
            .enumerate()
            .filter_map(|(i, waiter)| match poll_once(waiter.as_mut()) {
                Poll::Ready(result) => Some((i, result)),
                Poll::Pending => None,
            })
            .collect();
        let [(i, Ok(reservation))] = grants.as_slice() else {
            return Err("SCHED-G02: expected one grant when one slot was released".into());
        };
        let (threads, bytes) = incumbent[*i];
        if share(threads, bytes) != minimum
            || share(threads + 1, bytes + reservation.bytes()) > minimum + share(1, job_bytes)
        {
            return Err(format!(
                "SCHED-G02: request {} won for incumbents {incumbent:?}, minimum share {minimum}",
                i + 1,
            ));
        }
        Ok(())
    });
}

#[test]
fn eligible_peers_progress_within_one_fair_round() {
    prop::check("U03 SCHED-G03 fair-round progress", |g| {
        let peers = g.range_i64(2, 8) as usize;
        let bytes = g.range_i64(16, 200) as u64;
        let scheduler = ResourceScheduler::new(vec![(0, limits(1, 1_200))]).unwrap();
        let mut active = grant(&mut submit(&scheduler, peers as u64, bytes), "initial job")?;
        let mut waiting = (0..peers)
            .map(|request| (submit(&scheduler, request as u64, bytes), 0))
            .collect::<Vec<_>>();
        for (waiter, _) in &mut waiting {
            if !poll_once(waiter.as_mut()).is_pending() {
                return Err("SCHED-G03: contender ran before the slot was released".into());
            }
        }
        for round in 1..=peers * 4 {
            drop(active);
            let mut next = None;
            for (request, (waiter, since)) in waiting.iter_mut().enumerate() {
                match poll_once(waiter.as_mut()) {
                    Poll::Ready(Ok(reservation)) if next.is_none() && round - *since <= peers => {
                        next = Some(reservation);
                        *waiter = submit(&scheduler, request as u64, bytes);
                        *since = round;
                        if !poll_once(waiter.as_mut()).is_pending() {
                            return Err(
                                "SCHED-G03: replacement ran before the slot was released".into()
                            );
                        }
                    }
                    Poll::Pending if round - *since < peers => {}
                    _ => {
                        return Err(format!(
                            "SCHED-G03: request {request} missed its bound in round {round}"
                        ));
                    }
                }
            }
            active = next.ok_or_else(|| format!("SCHED-G03: idle slot in round {round}"))?;
        }
        Ok(())
    });
}

#[test]
fn memory_heavy_request_stops_being_bypassed_after_thirty_two_grants() {
    prop::check("U03 SCHED-G03 32-grant memory aging bound", |g| {
        let working = 1_200;
        let free = g.range_i64(100, 400) as u64;
        let small_bytes = g.range_i64(1, free as i64) as u64;
        let large_bytes = g.range_i64(free as i64 + 1, working as i64) as u64;
        let scheduler = ResourceScheduler::new(vec![(0, limits(2, working))]).unwrap();
        let resident = grant(&mut submit(&scheduler, 0, working - free), "resident job")?;
        let mut large = submit(&scheduler, 1, large_bytes);
        if !poll_once(large.as_mut()).is_pending() {
            return Err("SCHED-G03: large job did not wait for memory".into());
        }
        for request in 2..34 {
            drop(grant(
                &mut submit(&scheduler, request, small_bytes),
                "small bypass job",
            )?);
        }
        let mut bypass = submit(&scheduler, 34, small_bytes);
        if !poll_once(bypass.as_mut()).is_pending() {
            return Err("SCHED-G03: a 33rd grant bypassed the aged job".into());
        }
        drop(resident);
        {
            let state = scheduler.shared.state.lock().unwrap();
            if !state.last_grant.contains_key(&1)
                || state
                    .last_grant
                    .get(&34)
                    .is_some_and(|round| *round < state.last_grant[&1])
            {
                return Err("SCHED-G03: freed memory did not prioritize the aged job".into());
            }
        }
        drop(grant(&mut large, "aged job after memory release")?);
        drop(grant(&mut bypass, "small job after the aged grant")?);
        Ok(())
    });
}
