//! DR-1 and DR-2 of the OLTP direct path design review.
//!
//! DR-1: a mutation answered without a commit (`false`, or `409` for an insert) leaves no
//! record, so an earlier attempt of the same request that arrives later can still apply.
//! O08 requires that "no-op and conflict results remain stable by identity".
//!
//! DR-2: a retry of a request whose earlier attempt is in flight is answered `not_applied`.
//! OLTP-G04 allows that answer only with proof of non-application.
//!
//! Each case runs the delayed attempt (`Delivered`) and two controls without it: `Lost` breaks
//! the connection that carries it, and `NotSent` never sends it.

use std::{env, fs, path::PathBuf, sync::Arc, time::Duration};

use nabedra_rt::time;
use nabedra_sim::{
    SeedRun,
    sweep::{SweepConfig, sweep},
};
use serde_json::{Value, json};

use super::{Answer, Driver, GATEWAYS, SETTLE, WORKERS, run_schedule};

const KEY: i64 = 7;
const A: i64 = 111;
const B: i64 = 222;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Delayed {
    Delivered,
    Lost,
    NotSent,
}

/// Which links of the first gateway fail in DR-2, and what else goes through it.
#[derive(Clone, Copy, Debug)]
enum Outage {
    /// Only the link to the range leader.
    Leader,
    /// The links to the range leader and the nth other worker.
    LeaderAnd(usize),
    /// Every worker.
    All,
    /// Only the link to the range leader, while other clients keep sending through the
    /// gateway until its HTTP admission is full.
    LeaderWithLoad,
}

impl Outage {
    fn workers(self, leader: u64) -> Vec<u64> {
        let others: Vec<_> = WORKERS.into_iter().filter(|w| *w != leader).collect();
        match self {
            Self::Leader | Self::LeaderWithLoad => vec![leader],
            Self::LeaderAnd(index) => vec![leader, others[index]],
            Self::All => WORKERS.to_vec(),
        }
    }
}

fn row(value: i64) -> Value {
    json!({"id": KEY, "value": value})
}

fn expect(step: &str, reached: bool, got: &Answer) -> Result<(), String> {
    if reached {
        Ok(())
    } else {
        Err(format!("{step}: {got:?}"))
    }
}

/// DR-1. `refused` selects the insert that is answered `409`, else the update answered `false`.
async fn dr1(driver: Arc<Driver>, refused: bool, delayed: Delayed) -> Result<(), String> {
    let [held, other] = GATEWAYS;
    let leader = driver.leader;
    let caller = driver.caller();
    let (mut c1, mut c2) = (driver.actor(0), driver.actor(1));
    if refused {
        let seed_row = driver.insert(KEY, B).await;
        let answer = caller.call(&mut c2, other, &seed_row).await;
        expect("seed row", matches!(answer, Answer::Ok(_)), &answer)?;
    }
    let i1 = if refused {
        driver.insert(KEY, A).await
    } else {
        driver.update(KEY, A).await
    };
    driver.mark_counters("before").await?;
    driver.hold(held, &[leader]);
    if delayed == Delayed::NotSent {
        time::sleep(Duration::from_secs(10)).await;
    } else {
        // Step 1 and 3: the first attempt is on the wire and the gateway's deadline passes.
        let answer = caller.call(&mut c1, held, &i1).await;
        expect(
            "first attempt is unknown",
            matches!(answer, Answer::Unknown(_)),
            &answer,
        )?;
    }
    if delayed == Delayed::Lost {
        driver.reset_links(held, leader);
    }
    // Step 4: the retry overtakes it through the other gateway and commits nothing.
    let definite = caller.call(&mut c1, other, &i1).await;
    let reached = if refused {
        matches!(&definite, Answer::NotApplied(error) if error.contains("already exists"))
    } else {
        definite == Answer::Ok(Value::Bool(false))
    };
    expect("retry commits nothing", reached, &definite)?;
    driver.mark_counters("after-retry").await?;
    // Step 5: another client changes the row, so that the request would now apply.
    let i2 = if refused {
        driver.delete(KEY).await
    } else {
        driver.insert(KEY, B).await
    };
    let answer = caller.call(&mut c2, other, &i2).await;
    let reached = match &answer {
        Answer::Ok(value) => !refused || *value == Value::Bool(true),
        _ => false,
    };
    expect("second client commits", reached, &answer)?;
    // Step 6: the delayed attempt arrives.
    driver.heal(held, &[leader]);
    time::sleep(SETTLE).await;
    driver.mark_counters("after-heal").await?;
    let found = driver.read(&mut c2, other, KEY).await?;
    let expected = if refused { Value::Null } else { row(B) };
    if found != expected {
        driver.violate(
            if refused {
                "NotAppliedRequestApplied"
            } else {
                "NoOpRequestApplied"
            },
            format!("request {} answered {definite:?}, yet the row is {found}, not {expected} ({delayed:?})", i1.id),
        );
    }
    // The delayed attempt's record answers any later retry. A request that never ran would
    // now apply, so this only repeats an answer that a control already checked above.
    if delayed == Delayed::Delivered {
        let again = caller.call(&mut c1, other, &i1).await;
        driver.mark_counters("after-second-retry").await?;
        if again != definite {
            driver.violate(
                "UnstableResult",
                format!(
                    "request {} answered {definite:?}, then {again:?} ({delayed:?})",
                    i1.id
                ),
            );
        }
    }
    Ok(())
}

/// DR-1 without a fault or concurrency: a client repeats a request after its answer.
async fn dr1_sequential(driver: Arc<Driver>) -> Result<(), String> {
    let gateway = GATEWAYS[0];
    let caller = driver.caller();
    let (mut c1, mut c2) = (driver.actor(0), driver.actor(1));
    let i1 = driver.update(KEY, A).await;
    let first = caller.call(&mut c1, gateway, &i1).await;
    expect(
        "update of a missing row",
        first == Answer::Ok(Value::Bool(false)),
        &first,
    )?;
    let i2 = driver.insert(KEY, B).await;
    let answer = caller.call(&mut c2, gateway, &i2).await;
    expect("insert", matches!(answer, Answer::Ok(_)), &answer)?;
    let again = caller.call(&mut c1, gateway, &i1).await;
    let found = driver.read(&mut c2, gateway, KEY).await?;
    if again != first || found != row(B) {
        driver.violate(
            "UnstableResult",
            format!(
                "request {} answered {first:?}, then {again:?}; the row is {found}",
                i1.id
            ),
        );
    }
    Ok(())
}

async fn dr2(driver: Arc<Driver>, outage: Outage, delayed: Delayed) -> Result<(), String> {
    let [held, other] = GATEWAYS;
    let workers = outage.workers(driver.leader);
    let caller = driver.caller();
    let (mut c1, mut c2) = (driver.actor(0), driver.actor(1));
    let i2 = driver.insert(KEY, B).await;
    driver.mark_counters("before").await?;
    driver.hold(held, &workers);
    if delayed == Delayed::NotSent {
        time::sleep(Duration::from_secs(10)).await;
    } else {
        let answer = caller.call(&mut c1, held, &i2).await;
        expect(
            "first attempt is unknown",
            matches!(answer, Answer::Unknown(_)),
            &answer,
        )?;
    }
    if delayed == Delayed::Lost {
        for &worker in &workers {
            driver.reset_links(held, worker);
        }
    }
    // Other clients' requests wait on the same dead link.
    let mut load = Vec::new();
    if matches!(outage, Outage::LeaderWithLoad) {
        for index in 0..64 {
            let insert = driver.insert(1_000 + index, 1).await;
            let (caller, mut actor) = (caller.clone(), driver.actor(index as usize));
            load.push(nabedra_rt::task::spawn(async move {
                caller.call(&mut actor, held, &insert).await
            }));
        }
        time::sleep(Duration::from_secs(1)).await;
    }
    // The client retries the same ID and body until it gets a definite answer.
    let mut refusal = None;
    for attempt in 0..3 {
        let answer = caller.call(&mut c1, held, &i2).await;
        driver.mark(
            "retry",
            json!({"attempt": attempt, "answer": format!("{answer:?}")}),
        );
        match answer {
            Answer::Unknown(_) | Answer::NoReply(_) => {}
            Answer::NotApplied(error) => {
                refusal = Some(error);
                break;
            }
            Answer::Ok(value) => return Err(format!("a retry succeeded: {value}")),
        }
    }
    for task in load {
        let _ = task.await;
    }
    driver.heal(held, &workers);
    time::sleep(SETTLE).await;
    driver.mark_counters("after-heal").await?;
    let found = driver.read(&mut c2, other, KEY).await?;
    let Some(refusal) = refusal else {
        // Every retry stayed unknown, which is correct whatever the row holds.
        return Err(format!(
            "no retry was answered not_applied; the row is {found}"
        ));
    };
    if !found.is_null() {
        driver.violate(
            "NotAppliedRequestApplied",
            format!(
                "request {} answered not_applied ({refusal}), yet the row is {found} ({delayed:?})",
                i2.id
            ),
        );
    }
    Ok(())
}

/// Runs `case` for the selected seeds, twice each, and prints the evidence.
fn check(name: &str, case: impl Fn(u64) -> SeedRun + Sync) {
    let var = |name: &str, default: u64| {
        env::var(name).map_or(default, |value| value.parse().expect("an integer"))
    };
    let mut config = SweepConfig::new(
        &format!("scripted-{name}"),
        var("NABEDRA_SEED", 1),
        var("NABEDRA_SEEDS", 1),
    );
    config.require_seeded = true;
    config.verify_replay = 1.0;
    config.replay = format!(
        "RUST_MIN_STACK=16777216 RUSTFLAGS=\"--cfg tokio_unstable\" CARGO_TARGET_DIR=target/sim \
         cargo test --locked --release -p nabedra-sim-oltp --lib \
         scripted::oltp_direct::{name} -- --exact --ignored --nocapture --test-threads=1"
    );
    if let Some(dir) = env::var_os("NABEDRA_SIM_ARTIFACTS") {
        config.artifacts = PathBuf::from(dir);
    }
    let artifacts = config.artifacts.clone();
    let report = sweep(&config, |seed, _| {
        let run = case(seed);
        fs::create_dir_all(&artifacts).unwrap();
        fs::write(
            artifacts.join(format!("{name}-seed-{seed}.history.jsonl")),
            &run.history,
        )
        .unwrap();
        println!(
            "fault model {name} seed {seed}: trace {:016x} ({} events), {} ms virtual, leader {}, \
             history checker {}, violations {:#?}",
            run.trace_hash,
            run.trace_events,
            run.virtual_ms,
            run.config["leader_hint"],
            run.config["history_checker"],
            run.violations,
        );
        run
    });
    println!(
        "fault model {}; replayed traces identical: {}",
        report.summary(),
        report.nondeterministic().is_empty()
    );
    report.assert_passed();
}

macro_rules! cases {
    ($($(#[$doc:meta])* $name:ident: $schedule:expr;)*) => {$(
        $(#[$doc])*
        #[test]
        #[ignore = "open design finding; not part of any gate"]
        fn $name() {
            check(stringify!($name), |seed| run_schedule(seed, $schedule));
        }
    )*};
}

cases! {
    dr1_update: |driver| dr1(driver, false, Delayed::Delivered);
    dr1_update_control_lost: |driver| dr1(driver, false, Delayed::Lost);
    dr1_update_control_not_sent: |driver| dr1(driver, false, Delayed::NotSent);
    dr1_insert_refused: |driver| dr1(driver, true, Delayed::Delivered);
    dr1_insert_refused_control_lost: |driver| dr1(driver, true, Delayed::Lost);
    dr1_insert_refused_control_not_sent: |driver| dr1(driver, true, Delayed::NotSent);
    dr1_sequential_retry: dr1_sequential;
    dr2_leader_link: |driver| dr2(driver, Outage::Leader, Delayed::Delivered);
    dr2_leader_and_first_link: |driver| dr2(driver, Outage::LeaderAnd(0), Delayed::Delivered);
    dr2_leader_and_second_link: |driver| dr2(driver, Outage::LeaderAnd(1), Delayed::Delivered);
    dr2_all_links: |driver| dr2(driver, Outage::All, Delayed::Delivered);
    dr2_admission_full: |driver| dr2(driver, Outage::LeaderWithLoad, Delayed::Delivered);
    dr2_admission_full_control_lost: |driver| dr2(driver, Outage::LeaderWithLoad, Delayed::Lost);
    dr2_admission_full_control_not_sent:
        |driver| dr2(driver, Outage::LeaderWithLoad, Delayed::NotSent);
}
