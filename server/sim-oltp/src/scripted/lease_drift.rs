//! SIM-10: the leader lease under clock drift, inside and past its bound (RANGES-3).
//!
//! The lease assumes that no node's clock rate is off by more than
//! [`LEASE_DRIFT_BOUND_PPM`] (`config.rs`). The schedule:
//!
//! 1. the drifts are set, the leader's slow and its voters' fast, and the leases run a second
//!    at those rates;
//! 2. a write through the gateway that reads commits on the range leader at true times
//!    `[invoked, acked]`. Its append was sent after `invoked`, and a quorum acknowledged it,
//!    so the leader's lease runs at least until `invoked + W / (1 + d_leader)` in true time,
//!    where `W` is the lease window on the leader's clock ([`lease_window`]). It ends no later
//!    than `acked + W / (1 + d_leader)`, from the last heartbeat that can have been acknowledged;
//! 3. at `acked` the leader is cut from its voters and from the gateway that writes; the
//!    gateway that reads reaches only the old leader;
//! 4. a voter is elected (`elected`), and a write through the other gateway is acknowledged
//!    (`ack`);
//! 5. reads go to the old leader from `ack` on, one per millisecond, while the computed window
//!    `[ack, lease end]` is open. A read that returns the old value after `ack` is stale, and
//!    the per-key linearizability checker must call it `non-linearizable`.
//!
//! The computed window, the times and every read's send time and answer are history markers. A
//! case in which no read could be sent inside the window ends with `ScheduleStep`: that is no
//! verdict either way, for any case.
//!
//! The simulator's lease window is the product's for a 400 ms `election_timeout_max`, where a
//! production group's is 3,000 ms: with the RANGES-3 margin the simulator's margin is
//! `400 ms * 2 * rho + 1 ms`, about 1.8 ms.
//!
//! The cases: `lease_beyond_bound` (leader -50,000 ppm, voters +50,000 ppm, about 40 ms of
//! exposure on a 400 ms lease) must be red on every build. `lease_near_bound` (1.5 times the
//! bound) and `lease_in_bound` (the bound) must be clean on a build with the margin. Each
//! asserts its expectation. They are ignored because each starts a cluster;
//! `oltp-sim-lease-drift` runs them:
//!
//! ```text
//! RUST_MIN_STACK=16777216 RUSTFLAGS="--cfg tokio_unstable" CARGO_TARGET_DIR=target/sim \
//!   cargo test --locked --release -p nabedra-sim-oltp --lib scripted::lease_drift:: \
//!   -- --ignored --nocapture --test-threads=1
//! ```

use std::{env, sync::Arc, time::Duration};

use nabedra_rt::{sim::net::Dial, time};
use nabedra_sim::shrink;
use serde_json::json;

use super::{Answer, Driver, GATEWAYS, WORKERS, run_schedule};
use crate::config::{BeyondDriftBound, CLOCK_DRIFT_BOUND_PPM, LEASE_DRIFT_BOUND_PPM, set_drift};

const KEY: i64 = 41;
/// The most reads sent at the old leader, one per millisecond from the acknowledgement.
const READS: u64 = 60;
/// How long the new leader may take to acknowledge a write after the isolation.
const ELECTION_LIMIT: Duration = Duration::from_secs(20);
/// A read reaches the old leader this long after it is sent: client to gateway, then
/// gateway to worker, one network step each.
const READ_PATH: Duration = Duration::from_millis(2);

/// The stale-read verdict the history checker gives.
const STALE: &str = "non-linearizable";

/// The window, on the leader's clock, in which a leader serves a lease read after a quorum
/// acknowledged an append it sent. On this build that is openraft's leader lease, which is
/// `election_timeout_max`, read from the product's Raft configuration. With the RANGES-3 margin
/// it becomes the product's `lease_read_window` of the same timeout.
fn lease_window() -> Duration {
    let config = nabedra_database_server::oltp_replication::raft_config();
    Duration::from_millis(config.election_timeout_max)
}

/// `local` on a clock that runs `ppm` fast, in true time.
fn true_time(local: Duration, ppm: i64) -> Duration {
    let nanos = local.as_nanos() * 1_000_000 / (1_000_000 + ppm) as u128;
    Duration::from_nanos(nanos as u64)
}

async fn lease(driver: Arc<Driver>, leader_ppm: i64, voter_ppm: i64) -> Result<(), String> {
    let world = driver.cluster.world.clone();
    let leader = driver.leader;
    let voters: Vec<u64> = WORKERS.into_iter().filter(|w| *w != leader).collect();
    let [reads_at, writes_at] = GATEWAYS;
    let caller = driver.caller();
    let ms = |at: Duration| at.as_micros() as f64 / 1_000.0;
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
    time::sleep(Duration::from_secs(1)).await;

    // Step 2: the write that pins the old leader's lease.
    let mut pinning = driver.actor(1);
    let pin = driver.update(KEY, 2).await;
    let invoked = world.elapsed();
    let answer = caller.call(&mut pinning, reads_at, &pin).await;
    let acked = world.elapsed();
    if !matches!(answer, Answer::Ok(_)) {
        return Err(format!("pinning write: {answer:?}"));
    }
    let lease = true_time(lease_window(), leader_ppm);
    let (lease_low, lease_high) = (invoked + lease, acked + lease);

    // Step 3.
    for &voter in &voters {
        for (from, to) in [(leader, voter), (voter, leader), (reads_at, voter), (voter, reads_at)] {
            world.black_hole(Some(from), Some(to));
        }
    }
    // The writing gateway finds the old leader gone at once, as after a reset from its host,
    // so its write goes to a voter as soon as one leads; a held link would cost it seconds.
    let refused = [(Some(writes_at), Some(leader)), (Some(leader), Some(writes_at))];
    driver.cluster.net().set_dial_profile(move |from, to| {
        if refused.contains(&(from, to)) {
            Dial::Refuse
        } else {
            Dial::Connect
        }
    });
    driver.reset_links(writes_at, leader);
    driver.mark(
        "isolate",
        json!({
            "leader": leader, "voters": voters, "pin_invoked_ms": ms(invoked),
            "pin_acked_ms": ms(acked), "lease_window_local_ms": ms(lease_window()),
            "lease_ends_ms": [ms(lease_low), ms(lease_high)],
        }),
    );

    // Step 4.
    let elected = async {
        loop {
            match driver.cluster.elected() {
                Some(node) if node != leader => return (Some(node), world.elapsed()),
                _ if world.elapsed() >= acked + ELECTION_LIMIT => return (None, world.elapsed()),
                _ => time::sleep(Duration::from_millis(1)).await,
            }
        }
    };
    let writes = async {
        let mut writer = driver.actor(0);
        let mut value = 2;
        loop {
            if world.elapsed() >= acked + ELECTION_LIMIT {
                return Err("no write was acknowledged after the isolation".to_owned());
            }
            value += 1;
            let write = driver.update(KEY, value).await;
            match caller.call(&mut writer, writes_at, &write).await {
                Answer::Ok(_) => return Ok((value, world.elapsed())),
                Answer::NotApplied(_) => {}
                // An unknown outcome ends the logical process.
                Answer::Unknown(_) | Answer::NoReply(_) => writer = driver.actor(0),
            }
        }
    };
    let ((new_leader, elected_at), written) = tokio::join!(elected, writes);
    let (value, ack) = written?;
    driver.mark(
        "acknowledged",
        json!({
            "value": value, "new_leader": new_leader, "elected_ms": ms(elected_at),
            "ack_ms": ms(ack), "window_ms": [ms(ack), ms(lease_low)],
        }),
    );

    // Step 5: reads inside the computed window only.
    let mut reads = Vec::new();
    for index in 0..READS {
        let sent = world.elapsed();
        if sent + READ_PATH >= lease_low {
            break;
        }
        let (driver, caller) = (driver.clone(), caller.clone());
        reads.push((
            sent,
            nabedra_rt::task::spawn(async move {
                let mut reader = driver.actor(index as usize + 2);
                let get = driver
                    .request("table.get", KEY, json!({"table_id": driver.table, "key": KEY}))
                    .await;
                caller.call(&mut reader, reads_at, &get).await
            }),
        ));
        time::sleep(Duration::from_millis(1)).await;
    }
    let inside = reads.len();
    for (index, (sent, read)) in reads.into_iter().enumerate() {
        let answer = read.await.map_err(|error| format!("read task: {error}"))?;
        driver.mark(
            "read",
            json!({"index": index, "sent_ms": ms(sent), "answer": format!("{answer:?}")}),
        );
    }
    for &voter in &voters {
        for (from, to) in [(leader, voter), (voter, leader), (reads_at, voter), (voter, reads_at)] {
            world.heal(Some(from), Some(to));
        }
    }
    driver.cluster.net().set_dial_profile(|_, _| Dial::Connect);
    if inside == 0 {
        return Err(format!(
            "the computed window is empty: the new leader acknowledged at {} ms, elected at {} ms, \
             and the old leader's lease ends no earlier than {} ms (no later than {} ms)",
            ms(ack),
            ms(elected_at),
            ms(lease_low),
            ms(lease_high),
        ));
    }
    Ok(())
}

/// Runs the twin on the selected seeds (`NABEDRA_SEED`, `NABEDRA_SEEDS`; default 1 and 1)
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
        let history = String::from_utf8_lossy(&run.history);
        // Everything from the isolation on: the computed window, the writes and the reads.
        let marks: Vec<&str> = history
            .lines()
            .skip_while(|line| !line.contains("\"isolate\""))
            .collect();
        println!(
            "fault model {name} seed {seed}: trace {:016x}, {} ms virtual, history checker {}, \
             violations {:#?}\n  {}",
            run.trace_hash,
            run.virtual_ms,
            run.config["history_checker"],
            run.violations,
            marks.join("\n  "),
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
    judge("lease_beyond_bound", -50_000, 50_000, true);
}

#[test]
#[ignore = "starts a cluster; oltp-sim-lease-drift runs it"]
fn lease_near_bound() {
    let near = LEASE_DRIFT_BOUND_PPM * 3 / 2;
    judge("lease_near_bound", -near, near, false);
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
    // The marker belongs to its world: in a fresh one the same drift is refused.
    nabedra_rt::sim::run(1, |world| async move {
        world.add_node(1, nabedra_rt::sim::NodeClock::default());
        set_drift(&world, 1, LEASE_DRIFT_BOUND_PPM + 1);
    });
}
