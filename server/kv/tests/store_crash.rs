//! NS04 (crash at every FaultFs mutating operation) and NS05 (power loss) at store level.
//!
//! One small workload (scoped and store-wide batches, journal values, range deletes,
//! synced and unsynced writes, explicit syncs, flushes, compactions, segment rolls and
//! retirement) runs on a `FaultFs`. After the cut, the store reopens on what survived and
//! must hold exactly the oracle state after some prefix `p` of the submitted batches,
//! with every batch acknowledged as durable inside `p`. Reopening again changes nothing.
//! This is a fault model, not physical power-loss evidence.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use common::*;
use nabedra_kv::fs::FaultFs;
use nabedra_kv::{Background, Durability, Store};

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

const BATCHES: usize = 60;

struct Outcome {
    submitted: usize,
    /// Batches `[0, durable)` were acknowledged as durable (a synced commit or a sync).
    durable: usize,
    /// For a power-loss cut: the filesystem image and the counters at the cut.
    cut: Option<(FaultFs, usize, usize)>,
}

fn batches(seed: u64) -> (Vec<ModelBatch>, Vec<Model>) {
    let mut rng = Rng(seed);
    let mut models = vec![Model::new()];
    let mut out = Vec::new();
    for i in 0..BATCHES {
        let b = random_batch(&mut rng, 3, 30, i as u64);
        let mut m = models.last().unwrap().clone();
        b.apply(&mut m);
        models.push(m);
        out.push(b);
    }
    (out, models)
}

fn options(fs: &FaultFs, bg: &Arc<Background>) -> nabedra_kv::Options {
    let mut o = small_options(Arc::new(fs.clone()), bg.clone());
    // Segments of two 4 KiB blocks plus records roll every few batches.
    o.journal.segment_bytes = 12 << 10;
    o.journal.max_group_bytes = 4 << 10;
    o
}

fn workload(
    fs: &FaultFs,
    bg: &Arc<Background>,
    list: &[ModelBatch],
    cut: Option<(usize, u64)>,
) -> Outcome {
    let (_, dir) = (0, std::path::PathBuf::from("/db/kv"));
    let mut outcome = Outcome {
        submitted: 0,
        durable: 0,
        cut: None,
    };
    let Ok(opened) = Store::open(&dir, options(fs, bg)) else {
        return outcome;
    };
    let store = opened.store;
    for (i, b) in list.iter().enumerate() {
        if let Some((at, seed)) = cut
            && at == i
        {
            outcome.cut = Some((fs.power_loss(seed), outcome.submitted, outcome.durable));
        }
        let durability = if i % 4 == 0 {
            Durability::Sync
        } else {
            Durability::NoSync
        };
        let Ok(commit) = store.submit(b.to_batch(), durability, Box::new(())) else {
            break;
        };
        outcome.submitted = i + 1;
        if commit.wait().is_err() {
            break;
        }
        if durability == Durability::Sync {
            outcome.durable = i + 1;
        }
        if i % 7 == 6 {
            match store.sync().and_then(|c| c.wait()) {
                Ok(_) => outcome.durable = i + 1,
                Err(_) => break,
            }
        }
        if i % 12 == 11 && store.flush().is_err() {
            break;
        }
        if i % 24 == 23 && store.compact().is_err() {
            break;
        }
    }
    drop(store);
    outcome
}

/// Reopens `fs` twice and checks the prefix rule.
fn verify(
    fs: FaultFs,
    bg: &Arc<Background>,
    models: &[Model],
    submitted: usize,
    durable: usize,
    ctx: &str,
) {
    let dir = std::path::PathBuf::from("/db/kv");
    let opened = match Store::open(&dir, options(&fs, bg)) {
        Ok(o) => o,
        Err(e) => panic!("{ctx}: reopen failed: {e}"),
    };
    assert!(
        opened.quarantined.is_empty(),
        "{ctx}: quarantined {:?}",
        opened.quarantined
    );
    let state = dump(&opened.store.snapshot());
    let p = (durable..=submitted)
        .find(|&p| models[p] == state)
        .unwrap_or_else(|| {
            let any = models.iter().position(|m| *m == state);
            panic!("{ctx}: state is no prefix in [{durable}, {submitted}] (matches prefix {any:?})")
        });
    let visible = opened.store.visible_seq();
    drop(opened);
    // Reopen what the first recovery left (a crash right after it), and again cleanly.
    let again = Store::open(&dir, options(&fs.crash(), bg))
        .unwrap_or_else(|e| panic!("{ctx}: second reopen failed: {e}"));
    assert_eq!(
        dump(&again.store.snapshot()),
        models[p],
        "{ctx}: second reopen differs"
    );
    assert!(again.store.visible_seq() >= visible.min(again.store.visible_seq()));
}

fn mutating_ops(list: &[ModelBatch], bg: &Arc<Background>) -> u64 {
    let (fs, _) = fault_fs();
    let outcome = workload(&fs, bg, list, None);
    assert_eq!(outcome.submitted, BATCHES);
    let calls = fs.calls();
    let count = |f: &dyn Fn(&nabedra_kv::fs::FsCall) -> bool| calls.iter().filter(|c| f(c)).count();
    let ends =
        |c: &nabedra_kv::fs::FsCall, suffix: &str| c.path.to_string_lossy().ends_with(suffix);
    let retired = count(&|c| {
        c.op == nabedra_kv::fs::FsOp::Rename
            && c.target
                .as_ref()
                .is_some_and(|t| t.to_string_lossy().ends_with(".free"))
    });
    let segments = count(&|c| c.op == nabedra_kv::fs::FsOp::Preallocate);
    let runs = count(&|c| c.op == nabedra_kv::fs::FsOp::SyncAll && ends(c, ".run"));
    let manifests = count(&|c| {
        c.op == nabedra_kv::fs::FsOp::SyncAll && c.path.to_string_lossy().contains("MANIFEST")
    });
    eprintln!(
        "reference run: {} mutating ops, {segments} segment files, {retired} segments retired, {runs} runs written, {manifests} manifest syncs",
        fs.mutating_ops()
    );
    assert!(
        retired > 0 && runs > 2 && manifests > 2,
        "the workload must roll, retire, flush and compact"
    );
    fs.mutating_ops()
}

#[test]
fn ns04_crash_at_every_mutating_operation() {
    let bg = Background::new(2);
    let (list, models) = batches(5);
    let total = mutating_ops(&list, &bg);
    eprintln!("NS04: {total} mutating operations in the reference run");
    // Background work makes later runs differ slightly; enumerate a margin past the end.
    for n in 0..=total + 20 {
        let (fs, _) = fault_fs();
        fs.crash_after(n);
        let outcome = workload(&fs, &bg, &list, None);
        verify(
            fs.crash(),
            &bg,
            &models,
            outcome.submitted,
            outcome.durable,
            &format!("crash after {n} ops"),
        );
    }
}

#[test]
fn ns05_power_loss_keeps_synced_writes_and_an_exact_prefix() {
    let bg = Background::new(2);
    let (list, models) = batches(7);
    let total = mutating_ops(&list, &bg);
    let mut rng = Rng(55);
    let seeds: u64 = std::env::var("NS05_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(150);
    for seed in 0..seeds {
        let (fs, _) = fault_fs();
        if rng.chance(50) {
            // Freeze at a random operation, then lose power.
            let n = rng.below(total + 1);
            fs.crash_after(n);
            let outcome = workload(&fs, &bg, &list, None);
            verify(
                fs.power_loss(seed),
                &bg,
                &models,
                outcome.submitted,
                outcome.durable,
                &format!("seed {seed} crash_after {n} + power loss"),
            );
        } else {
            // Lose power while the store is running, between two batches.
            let at = rng.below(BATCHES as u64) as usize;
            let outcome = workload(&fs, &bg, &list, Some((at, seed)));
            let (image, submitted, durable) = outcome.cut.expect("cut taken");
            verify(
                image,
                &bg,
                &models,
                submitted,
                durable,
                &format!("seed {seed} power loss before batch {at}"),
            );
        }
    }
}
