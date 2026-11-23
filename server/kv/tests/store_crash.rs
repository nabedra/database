//! NS04 (crash at every FaultFs mutating operation) and NS05 (power loss) at store level.
//!
//! One small workload (scoped and store-wide batches, journal values, range deletes,
//! synced and unsynced writes, explicit syncs, flushes, compactions, segment rolls and
//! retirement) runs on a `FaultFs`. After the cut, the store reopens on what survived and
//! must hold exactly the oracle state after some prefix `p` of the submitted batches,
//! with every batch acknowledged as durable inside `p`. Reopening again changes nothing.
//! This is a fault model, not physical power-loss evidence.
#![allow(clippy::disallowed_methods)] // integration tests are outside the phase 7 conversion, which covers kv/src

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use common::*;
use nabedra_kv::fs::{FaultFs, PowerLoss};
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
    cut: Option<(usize, u64, PowerLoss)>,
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
        if let Some((at, seed, model)) = cut
            && at == i
        {
            outcome.cut = Some((
                fs.power_loss_with(seed, model),
                outcome.submitted,
                outcome.durable,
            ));
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
    let bg = Background::new(2).unwrap();
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

fn power_loss_campaign(model: PowerLoss, seeds_var: &str, default_seeds: u64) {
    let bg = Background::new(2).unwrap();
    let (list, models) = batches(7);
    let total = mutating_ops(&list, &bg);
    let mut rng = Rng(55);
    let seeds: u64 = std::env::var(seeds_var)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default_seeds);
    for seed in 0..seeds {
        let (fs, _) = fault_fs();
        if rng.chance(50) {
            // Freeze at a random operation, then lose power.
            let n = rng.below(total + 1);
            fs.crash_after(n);
            let outcome = workload(&fs, &bg, &list, None);
            verify(
                fs.power_loss_with(seed, model),
                &bg,
                &models,
                outcome.submitted,
                outcome.durable,
                &format!("seed {seed} crash_after {n} + power loss {model:?}"),
            );
        } else {
            // Lose power while the store is running, between two batches.
            let at = rng.below(BATCHES as u64) as usize;
            let outcome = workload(&fs, &bg, &list, Some((at, seed, model)));
            let (image, submitted, durable) = outcome.cut.expect("cut taken");
            verify(
                image,
                &bg,
                &models,
                submitted,
                durable,
                &format!("seed {seed} power loss {model:?} before batch {at}"),
            );
        }
    }
}

#[cfg(feature = "sim")]
#[test]
fn phase9_sim_flushed_journal_value_survives_power_loss() {
    use nabedra_rt::sim::{self, NodeClock};
    sim::run(55, |world| async move {
        world.add_node(1, NodeClock::default());
        sim::in_node(1, async {
            let (fs, dir) = fault_fs();
            let bg = Background::new(2).unwrap();
            let store = Store::open(&dir, options(&fs, &bg)).unwrap().store;
            let batch = ModelBatch {
                scope: vec![],
                ops: vec![Op::PutJournal(b"key".to_vec(), b"value".to_vec())],
            };
            let mut model = Model::new();
            batch.apply(&mut model);
            write(&store, &batch, Durability::NoSync).unwrap();
            store.flush().unwrap();

            // Model a power loss where none of the unsynced bytes survive.
            fs.halt();
            let durable = fs.crash();
            drop(store);
            let opened = Store::open(&dir, options(&durable, &bg)).unwrap();
            assert!(opened.quarantined.is_empty());
            assert_eq!(dump(&opened.store.snapshot()), model);
        })
        .await;
    });
}

#[test]
fn ns05_power_loss_keeps_synced_writes_and_an_exact_prefix() {
    power_loss_campaign(PowerLoss::default(), "NS05_SEEDS", 150);
}

/// NS05 with 4 KiB device units that a power loss may destroy whole (zeros or garbage),
/// durable bytes in the unit included (KVfix1's `power_loss_with`).
#[test]
fn ns05_power_loss_with_4k_units_zero_and_garbage() {
    power_loss_campaign(PowerLoss::destroy_4k(), "NS05_4K_SEEDS", 150);
}

/// KVfix4 (NS01 1e6, CI seed 4242 step 49509, "second reopen failed: a journal segment
/// the manifest records is missing"). A Kill stopped a segment roll after its rename and
/// header sync but before the directory sync. The first open appended to that segment
/// and a flush recorded it in the manifest; a crash right after lost the segment's name.
/// The failed directory sync stands for the Kill point: an injected EIO leaves the
/// rename pending exactly as a stop right before the sync would.
#[test]
fn ns04_kvfix4_kill_inside_a_roll_then_crash_after_reopen_keeps_the_manifests_segments() {
    use nabedra_kv::fs::{Fault, FsOp};
    let (fs, dir) = fault_fs();
    let bg = Background::new(2).unwrap();
    let journal = dir.join("journal");
    let options = |fs: &FaultFs| {
        let mut o = small_options(Arc::new(fs.clone()), bg.clone());
        // No flush, compaction or GC before the test asks: the journal directory then
        // sees only the open, the spare preparer and the roll.
        o.memtable_bytes = 4 << 20;
        o.max_replay_bytes = 64 << 20;
        o.max_journal_bytes = 64 << 20;
        o
    };
    let journal_dir_syncs = |fs: &FaultFs| {
        fs.calls()
            .iter()
            .filter(|c| c.op == FsOp::SyncDir && c.path == journal && c.ok)
            .count()
    };
    let store = Store::open(&dir, options(&fs)).unwrap().store;
    let at_open = journal_dir_syncs(&fs);
    let mut model = Model::new();
    let put = |store: &Store, model: &mut Model, i: usize| {
        let key = format!("k{i:04}").into_bytes();
        let mut b = nabedra_kv::WriteBatch::new();
        b.put(&key, &[i as u8; 1000]);
        let r = store
            .submit(b, Durability::Sync, Box::new(()))
            .and_then(|c| c.wait());
        if r.is_ok() {
            model.insert(key, vec![i as u8; 1000]);
        }
        r
    };
    // Past half of the first segment, then wait until the preparer's spare is durable:
    // the roll then takes it and creates no file.
    let mut i = 0;
    while i < 8 {
        put(&store, &mut model, i).unwrap();
        i += 1;
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while journal_dir_syncs(&fs) == at_open {
        assert!(std::time::Instant::now() < deadline, "no spare prepared");
        std::thread::yield_now();
    }
    // The roll's directory sync fails: the journal latches with the rename pending.
    fs.inject(Fault::on(FsOp::SyncDir).path_prefix(&journal));
    while put(&store, &mut model, i).is_ok() {
        i += 1;
        assert!(i < 100, "the journal never rolled");
    }
    fs.halt();
    drop(store);

    // The kill image, then an open that appends to the rolled-to segment and flushes,
    // so the manifest records it. Its later directory syncs (retirement) fail, as if the
    // machine stopped before them.
    let killed = fs.kill();
    let opened = Store::open(&dir, options(&killed)).unwrap();
    assert_eq!(
        dump(&opened.store.snapshot()),
        model,
        "the kill lost synced writes"
    );
    killed.inject(Fault::on(FsOp::SyncDir).path_prefix(&journal).repeat());
    for _ in 0..2 {
        put(&opened.store, &mut model, i).unwrap();
        i += 1;
    }
    let _ = opened.store.flush();
    killed.halt();
    drop(opened);

    // A crash right after: only durable state.
    let again = Store::open(&dir, options(&killed.crash()))
        .unwrap_or_else(|e| panic!("reopen after the crash failed: {e} ({e:?})"));
    assert_eq!(
        dump(&again.store.snapshot()),
        model,
        "synced writes were lost"
    );
}
