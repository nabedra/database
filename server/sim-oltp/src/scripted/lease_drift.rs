//! SIM-10: the leader lease under clock drift, inside and past its bound (RANGES-3).
//!
//! The lease assumes that no node's clock rate is off by more than
//! [`LEASE_DRIFT_BOUND_PPM`] (`config.rs`). The schedule: the range leader heartbeats at a
//! slow rate while its voters run fast; it is isolated from them; their leases end first, by
//! their faster clocks; one of them is elected and acknowledges a write; then reads reach the
//! old leader, still inside its own lease by its own clock. A read there that returns the old
//! value after the write's acknowledgement is stale, and the per-key linearizability checker
//! must call it `non-linearizable`.
//!
//! - `lease_beyond_bound`: the leader at -5,000 ppm, the voters at +5,000 ppm, ten times the
//!   bound. It must be red on every build, with or without the RANGES-3 margin: it is the twin
//!   that shows the checker can see a stale lease read at all.
//! - `lease_in_bound`: the leader at minus the bound, the voters at plus it. It must be
//!   green on a build with the RANGES-3 margin. On a bare `LeaseRead` it records the finding.
//!
//! Each case asserts its expectation itself, and the reads that reached the old leader are
//! printed with their answers. A case whose reads never reach the old leader inside its lease
//! fails with `ScheduleStep`, which is not a verdict either way. The cases are ignored because
//! each starts a cluster; `oltp-sim-lease-drift` runs them:
//!
//! ```text
//! RUST_MIN_STACK=16777216 RUSTFLAGS="--cfg tokio_unstable" CARGO_TARGET_DIR=target/sim \
//!   cargo test --locked --release -p nabedra-sim-oltp --lib scripted::lease_drift:: \
//!   -- --ignored --nocapture --test-threads=1
//! ```

use std::{env, sync::Arc, time::Duration};

use nabedra_rt::time;
use nabedra_sim::shrink;
use serde_json::json;

use super::{Answer, Driver, GATEWAYS, WORKERS, run_schedule};
use crate::config::{BeyondDriftBound, CLOCK_DRIFT_BOUND_PPM, LEASE_DRIFT_BOUND_PPM, set_drift};

const KEY: i64 = 41;
/// Reads sent at the old leader once the new one acknowledged its write, one per millisecond.
const READS: u64 = 20;
/// How long the new leader may take to acknowledge a write after the isolation.
const ELECTION_LIMIT: Duration = Duration::from_secs(20);

/// The stale-read verdict the history checker gives.
const STALE: &str = "non-linearizable";

async fn lease(driver: Arc<Driver>, leader_ppm: i64, voter_ppm: i64) -> Result<(), String> {
    let world = driver.cluster.world.clone();
    let leader = driver.leader;
    let voters: Vec<u64> = WORKERS.into_iter().filter(|w| *w != leader).collect();
    let [reads_at, writes_at] = GATEWAYS;
    let caller = driver.caller();
    if leader_ppm.abs().max(voter_ppm.abs()) > LEASE_DRIFT_BOUND_PPM {
        world.insert_ext(BeyondDriftBound);
    }
    let first = driver.insert(KEY, 1).await;
    let answer = caller.call(&mut driver.actor(0), writes_at, &first).await;
    if !matches!(answer, Answer::Ok(_)) {
        return Err(format!("first write: {answer:?}"));
    }
    let rates = [(leader, leader_ppm)]
        .into_iter()
        .chain(voters.iter().map(|&voter| (voter, voter_ppm)));
    for (node, ppm) in rates {
        world.set_timer_floor_ppm(node, Some(ppm.min(-CLOCK_DRIFT_BOUND_PPM)));
        set_drift(&world, node, ppm);
    }
    driver.mark("drift", json!({"leader": leader_ppm, "voters": voter_ppm}));
    // Heartbeats at the new rates, so that every lease is granted under them.
    time::sleep(Duration::from_secs(1)).await;

    // The old leader keeps only the gateway that reads; the other reaches only the voters.
    driver.mark("isolate", json!({"leader": leader, "voters": voters}));
    for &voter in &voters {
        for (from, to) in [(leader, voter), (voter, leader), (reads_at, voter), (voter, reads_at)] {
            world.black_hole(Some(from), Some(to));
        }
    }
    world.black_hole(Some(writes_at), Some(leader));
    world.black_hole(Some(leader), Some(writes_at));
    driver.reset_links(writes_at, leader);
    let isolated = world.elapsed();

    let mut writer = driver.actor(0);
    let mut value = 1;
    loop {
        if world.elapsed() >= isolated + ELECTION_LIMIT {
            return Err("no write was acknowledged after the isolation".into());
        }
        value += 1;
        let write = driver.update(KEY, value).await;
        match caller.call(&mut writer, writes_at, &write).await {
            Answer::Ok(_) => break,
            Answer::NotApplied(_) => {}
            // An unknown outcome ends the logical process.
            Answer::Unknown(_) | Answer::NoReply(_) => writer = driver.actor(0),
        }
    }
    let acknowledged = world.elapsed();
    driver.mark(
        "acknowledged",
        json!({"value": value, "after_isolation_ms": (acknowledged - isolated).as_millis()}),
    );

    let reads: Vec<_> = (0..READS)
        .map(|index| {
            let (driver, caller) = (driver.clone(), caller.clone());
            nabedra_rt::task::spawn(async move {
                time::sleep(Duration::from_millis(index)).await;
                let mut reader = driver.actor(1);
                let get = driver
                    .request("table.get", KEY, json!({"table_id": driver.table, "key": KEY}))
                    .await;
                caller.call(&mut reader, reads_at, &get).await
            })
        })
        .collect();
    let mut served = 0;
    for (index, read) in reads.into_iter().enumerate() {
        let answer = read.await.map_err(|error| format!("read task: {error}"))?;
        served += usize::from(matches!(answer, Answer::Ok(_)));
        driver.mark("read", json!({"index": index, "answer": format!("{answer:?}")}));
    }
    driver.mark("reads served by the old leader", json!(served));
    for &voter in &voters {
        for (from, to) in [(leader, voter), (voter, leader), (reads_at, voter), (voter, reads_at)] {
            world.heal(Some(from), Some(to));
        }
    }
    world.heal(Some(writes_at), Some(leader));
    world.heal(Some(leader), Some(writes_at));
    if served == 0 {
        return Err(format!(
            "no read reached the old leader inside its lease ({} ms after the isolation)",
            (acknowledged - isolated).as_millis()
        ));
    }
    Ok(())
}

/// Runs `drift` on the selected seeds (`NABEDRA_SEED`, `NABEDRA_SEEDS`; default 1 and 1)
/// and checks each verdict: `stale` says whether the checker must find the stale read.
fn judge(name: &str, leader_ppm: i64, voter_ppm: i64, stale: bool) {
    let var = |name: &str, default: u64| {
        env::var(name).map_or(default, |value| value.parse().expect("an integer"))
    };
    let first = var("NABEDRA_SEED", 1);
    let mut wrong = Vec::new();
    for seed in first..first + var("NABEDRA_SEEDS", 1) {
        let run = run_schedule(seed, move |driver| lease(driver, leader_ppm, voter_ppm));
        let kinds = shrink::kinds(&run.violations);
        println!(
            "fault model {name} seed {seed}: trace {:016x}, {} ms virtual, history checker {}, \
             violations {:#?}",
            run.trace_hash, run.virtual_ms, run.config["history_checker"], run.violations,
        );
        let verdict = if stale {
            kinds.len() == 1 && kinds.contains(STALE)
        } else {
            kinds.is_empty()
        };
        if !verdict {
            wrong.push((seed, kinds));
        }
    }
    assert!(wrong.is_empty(), "{name}: seeds with another verdict: {wrong:?}");
}

#[test]
#[ignore = "starts a cluster; oltp-sim-lease-drift runs it"]
fn lease_beyond_bound() {
    judge("lease_beyond_bound", -5_000, 5_000, true);
}

#[test]
#[ignore = "starts a cluster; oltp-sim-lease-drift runs it"]
fn lease_in_bound() {
    let bound = LEASE_DRIFT_BOUND_PPM;
    judge("lease_in_bound", -bound, bound, false);
}

#[test]
#[should_panic(expected = "past the lease bound")]
fn drift_past_the_bound_needs_a_beyond_bound_run() {
    nabedra_rt::sim::run(1, |world| async move {
        world.add_node(1, nabedra_rt::sim::NodeClock::default());
        set_drift(&world, 1, -LEASE_DRIFT_BOUND_PPM);
        set_drift(&world, 1, LEASE_DRIFT_BOUND_PPM);
        world.insert_ext(BeyondDriftBound);
        set_drift(&world, 1, 5_000);
    });
    // The marker is gone after a fresh world: the same drift is refused.
    nabedra_rt::sim::run(1, |world| async move {
        world.add_node(1, nabedra_rt::sim::NodeClock::default());
        set_drift(&world, 1, LEASE_DRIFT_BOUND_PPM + 1);
    });
}
