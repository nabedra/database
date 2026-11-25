//! P2: a replica applies a SET after the primary's synthesized DEL of the
//! same expired key (testing plan, KV planted bugs; CACHE-G05, K03).
//!
//! Replication is one ordered exchange per lane, so a delta cannot overtake
//! another. The fault delivers the SET to the replica a second time, right
//! after the DEL. The replica's sequence check refuses it, and the defect
//! removes that check. Clean twins keep the second delivery.

use std::collections::BTreeMap;
use std::sync::Mutex;

use nabedra_sim::{PlantedSearch, SeedRun, runtime, sweep};

use super::super::super::{
    clock::ClockMode,
    protocol::{LEASE_MS, RENEW_MS},
};
use super::super::harness::{Cluster, Config, Partitions, memory_limit};
use super::super::history::Op;
use super::super::hooks::Knobs;
use super::mutants::{Trial, assert_replay, catches, trial, write_ok};
use super::run_seed_with;
use crate::cache::{command::key_slot, store::EvictionPolicy, wire::Response};

const BUDGET: u64 = 500;
// A replica polls 256 slots every 25 ms, so a round over the slots it copies
// from one source takes a few hundred milliseconds.
const REPLICATION_MS: u64 = 700;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Form {
    /// SET with a TTL. The stale SET carries the deadline that has passed.
    Ttl,
    /// SET without a TTL, then PEXPIRE. The stale SET has no deadline.
    LaterExpire,
}

fn knobs(enabled: bool) -> Knobs {
    Knobs {
        redeliver_set_after_expiry: true,
        replica_accepts_stale_delta: enabled,
        ..Knobs::default()
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    trial: Trial,
    /// Redelivered SETs that the replica refused and applied.
    redelivered: (u64, u64),
    /// The replica's records differed from the primary's after the DEL.
    differed: bool,
    /// The first reads on the successor, one per millisecond.
    visible: usize,
}

fn run(form: Form, seed: u64, generated: bool, enabled: bool, history_only: bool) -> Outcome {
    runtime::run(seed, |world| async move {
        let mut choices = sweep::workload_gen(seed);
        let slot_max = if generated {
            1 + choices.below(250) as u16
        } else {
            32
        };
        let ttl = if generated {
            REPLICATION_MS + choices.below(900)
        } else {
            1_000
        };
        let key = (0..)
            .map(|i| format!("{{stale-set-{i}}}"))
            .find(|key| key_slot(key.as_bytes()) < slot_max)
            .unwrap();
        let slot = key_slot(key.as_bytes());
        let config = Config {
            partitions: Partitions::Loss,
            nodes: 4,
            groups: 3,
            actors: 1,
            slot_bytes: memory_limit(1),
            eviction: EvictionPolicy::NoEviction,
            clock_mode: ClockMode::ClockBound,
            // The widest skew inside the 10 ms bound.
            base_error_ns: 4_450_000,
            declared_ppm: 10,
            skew: true,
            gradual_drift: false,
            knobs: knobs(enabled),
            check_replica_prefix: !history_only,
            keep_full_trace: false,
        };
        let mut cluster = Cluster::boot(world, vec![key.clone()], config).await;
        let initial = cluster.slot(slot);
        let old = initial.primary.unwrap();
        let replica = initial.replica.unwrap();
        let lease = |cluster: &Cluster| {
            let end = cluster.slot(slot).lease.unwrap().expires_ms;
            end - nabedra_rt::sim::EPOCH_MS
        };
        // Cut renewals first. The key expires just before the last grant ends,
        // leaving time to replicate the DEL, so the successor serves soon after.
        cluster.renew_once(old.node).await.unwrap();
        cluster.isolate_controller_links(old.node);
        let expiry = lease(&cluster) - REPLICATION_MS;
        let first = match form {
            Form::Ttl => Some(ttl),
            Form::LaterExpire => None,
        };
        cluster
            .step(expiry.saturating_sub(cluster.now() + ttl))
            .await;
        assert!(write_ok(
            cluster
                .send(&key, Op::Set("old-value".into(), first), Some(old.node))
                .await
        ));
        let mut sequence = 1;
        replicated(&mut cluster, replica, slot, sequence).await;
        if form == Form::LaterExpire {
            sequence += 1;
            let ttl = expiry.saturating_sub(cluster.now());
            assert!(ttl > 100, "no time left for the PEXPIRE");
            assert_eq!(
                cluster
                    .send(&key, Op::Expire(ttl as i64), Some(old.node))
                    .await,
                Some(Response::Integer(1))
            );
            replicated(&mut cluster, replica, slot, sequence).await;
        }
        // The primary synthesizes the DEL and the replica applies it.
        sequence += 1;
        replicated(&mut cluster, replica, slot, sequence).await;
        let primary_copy = cluster.sample(Some(old), slot).await.unwrap();
        let replica_copy = cluster.sample(Some(replica), slot).await.unwrap();
        assert_eq!(
            (primary_copy.sequence, replica_copy.sequence),
            (sequence, sequence)
        );
        let differed = primary_copy.digest != replica_copy.digest;
        // The deleted key reads as absent on the primary while it can serve.
        let absent = cluster.send(&key, Op::Get, Some(old.node)).await;
        assert!(
            !matches!(absent, Some(Response::Bulk(Some(_)))),
            "{absent:?}"
        );
        if generated {
            cluster.step(choices.below(30)).await;
        }
        let deadline = lease(&cluster) + LEASE_MS + 4 * RENEW_MS;
        while !cluster
            .sample(Some(replica), slot)
            .await
            .is_some_and(|copy| copy.authorized)
        {
            assert!(cluster.now() < deadline, "replica was not promoted");
            cluster.step(1).await;
        }
        assert_eq!(cluster.slot(slot).primary, Some(replica));
        let mut visible = 0;
        for _ in 0..50 {
            let value = cluster.send(&key, Op::Get, Some(replica.node)).await;
            visible += usize::from(matches!(value, Some(Response::Bulk(Some(_)))));
            cluster.step(1).await;
        }
        let redelivered = cluster.observed.lock().unwrap().redelivered;
        Outcome {
            trial: trial(&cluster, history_only),
            redelivered,
            differed,
            visible,
        }
    })
}

async fn replicated(
    cluster: &mut Cluster,
    replica: super::super::super::protocol::WorkerId,
    slot: u16,
    sequence: u64,
) {
    for _ in 0..400 {
        if cluster
            .sample(Some(replica), slot)
            .await
            .is_some_and(|copy| copy.sequence >= sequence)
        {
            return;
        }
        cluster.step(10).await;
    }
    panic!("replica did not reach sequence {sequence}");
}

/// The clean twin: the same second delivery, refused by the product check.
fn assert_clean(form: Form, seed: u64, clean: &Outcome) {
    assert!(
        clean.trial.violations.is_empty(),
        "{form:?} seed {seed} clean twin: {:?}",
        clean.trial.violations
    );
    assert!(
        clean.redelivered.0 > 0 && clean.redelivered.1 == 0,
        "{form:?} seed {seed} clean twin redelivered {:?}",
        clean.redelivered
    );
    assert!(!clean.differed, "{form:?} seed {seed} clean twin differed");
    assert_eq!(clean.visible, 0, "{form:?} seed {seed} clean twin");
}

fn assert_applied(form: Form, seed: u64, outcome: &Outcome) {
    assert!(
        outcome.redelivered.1 > 0 && outcome.differed,
        "{form:?} seed {seed} did not apply a stale SET: {outcome:?}"
    );
}

fn scripted(form: Form) {
    let seed = 1;
    let planted = run(form, seed, false, true, false);
    let clean = run(form, seed, false, false, false);
    assert_applied(form, seed, &planted);
    assert_clean(form, seed, &clean);
    assert!(
        catches(&planted.trial.violations, "ReplicaPrefix"),
        "{form:?}: {:?}",
        planted.trial.violations
    );
    assert_eq!(planted, run(form, seed, false, true, false));
    assert_eq!(clean, run(form, seed, false, false, false));
    println!(
        "Cache fault model P2 {form:?} scripted seed {seed}: trace {:016x} clean {:016x} {:?}",
        planted.trial.trace_hash, clean.trial.trace_hash, planted.trial.violations
    );
}

#[test]
fn planted_replica_applies_set_after_expiry_delete_scripted() {
    scripted(Form::Ttl);
}

#[test]
fn planted_replica_applies_set_after_later_expire_scripted() {
    scripted(Form::LaterExpire);
}

/// The random nemesis and the product workloads reach P2. Every tried seed
/// has a clean twin that receives and refuses the same second delivery.
#[test]
fn planted_replica_applies_set_after_expiry_delete_search() {
    let run = |seed, enabled| {
        run_seed_with(
            seed,
            &mut sweep::workload_gen(seed),
            Partitions::Late,
            knobs(enabled),
        )
    };
    let redelivered = |run: &SeedRun, side: usize| run.config["redelivered"][side].as_u64();
    let mut search = PlantedSearch::from_env("P2", 1, BUDGET, &["ReplicaPrefix"]);
    search.budget = search.budget.min(BUDGET);
    let clean_failures = Mutex::new(Vec::new());
    let witnesses = Mutex::new(BTreeMap::new());
    let found = search
        .run(|seed| {
            let clean = run(seed, false);
            if !clean.passed() || redelivered(&clean, 1) != Some(0) {
                clean_failures
                    .lock()
                    .unwrap()
                    .push((seed, clean.violations));
                return Vec::new();
            }
            let planted = run(seed, true);
            let violations = planted.violations.clone();
            witnesses.lock().unwrap().insert(seed, (planted, clean));
            violations
        })
        .unwrap();
    let clean_failures = clean_failures.into_inner().unwrap();
    assert!(clean_failures.is_empty(), "clean twins: {clean_failures:?}");
    assert!(found.tried <= BUDGET);
    let seed = found.seed;
    let (planted, clean) = witnesses.into_inner().unwrap().remove(&seed).unwrap();
    assert!(redelivered(&planted, 1) > Some(0));
    for (first, enabled) in [(&planted, true), (&clean, false)] {
        let again = run(seed, enabled);
        assert_eq!(
            (first.trace_hash, first.trace_events, &first.violations),
            (again.trace_hash, again.trace_events, &again.violations),
            "seed {seed}"
        );
        assert_eq!(first.history, again.history, "seed {seed} history");
    }
    println!(
        "Cache fault model P2 random seed {seed} budget {} tried {} refused {:?} on the clean twin, trace {:016x} clean {:016x} {:?}",
        search.budget,
        found.tried,
        redelivered(&clean, 0),
        planted.trace_hash,
        clean.trace_hash,
        found.violations
    );
}

/// With a TTL on the SET itself, the stale SET carries the deadline that made
/// the primary delete the key. The source synthesized the DEL at an upper
/// clock reading at or past that deadline and below its own expiry, or below
/// its frozen upper bound. A successor serves only when its lower reading has
/// reached that bound, so its deadline check always hides the record. No
/// client sees P2 in this form. Only the copies' records show it.
#[test]
fn ttl_form_is_hidden_from_clients_by_the_deadline_check() {
    for (seed, generated) in std::iter::once((1, false)).chain((1..=20).map(|seed| (seed, true))) {
        let outcome = run(Form::Ttl, seed, generated, true, true);
        assert_applied(Form::Ttl, seed, &outcome);
        assert_eq!(outcome.visible, 0, "seed {seed}: {outcome:?}");
        assert!(
            outcome.trial.violations.is_empty(),
            "seed {seed}: {:?}",
            outcome.trial.violations
        );
    }
}

/// When a later PEXPIRE set the deadline, the stale SET has none and survives
/// the promotion. Clients read the expired value again.
#[test]
fn planted_replica_applies_set_after_later_expire_history() {
    for (seed, generated) in [(1, false), (1, true), (2, true), (3, true)] {
        let planted = run(Form::LaterExpire, seed, generated, true, true);
        let clean = run(Form::LaterExpire, seed, generated, false, true);
        assert_applied(Form::LaterExpire, seed, &planted);
        assert_clean(Form::LaterExpire, seed, &clean);
        assert!(planted.visible > 0, "seed {seed}");
        assert!(
            catches(&planted.trial.violations, "resurrection"),
            "seed {seed}: {:?}",
            planted.trial.violations
        );
        assert_replay(
            seed,
            &planted.trial,
            &run(Form::LaterExpire, seed, generated, true, true).trial,
        );
        println!(
            "Cache fault model P2 LaterExpire history seed {seed} generated {generated}: trace {:016x} clean {:016x} {} visible, {:?}",
            planted.trial.trace_hash,
            clean.trial.trace_hash,
            planted.visible,
            planted.trial.violations[0]
        );
    }
}
