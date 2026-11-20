//! The campaign's step list: a seeded, model-independent sequence of operations.
//!
//! A step list never depends on the store's answers, so any sub-list is also a valid
//! workload. The shrinker relies on that: it removes steps and replays the rest.

use nabedra_kv::fs::PowerLoss;

use crate::common::{ModelBatch, Op, Rng, group_key, group_prefix};

/// How a machine stop is turned into the image the store reopens on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CutKind {
    /// Only durable state survives.
    Crash,
    /// Process kill: every completed write survives.
    Kill,
    /// Power loss: durable state plus any subset of the unsynced sectors (512 B), each
    /// new, old or torn, and any subset of the unsynced directory operations.
    PowerLoss(u64),
    /// Power loss with 4 KiB units that may also read back as zeros or garbage.
    PowerLoss4k(u64),
}

impl CutKind {
    pub fn power_loss_mode(self) -> Option<(u64, PowerLoss)> {
        match self {
            CutKind::PowerLoss(seed) => Some((seed, PowerLoss::default())),
            CutKind::PowerLoss4k(seed) => Some((seed, PowerLoss::destroy_4k())),
            CutKind::Crash | CutKind::Kill => None,
        }
    }

    pub fn random(rng: &mut Rng) -> CutKind {
        match rng.below(8) {
            0 | 1 => CutKind::Crash,
            2 => CutKind::Kill,
            3..=5 => CutKind::PowerLoss(rng.next()),
            _ => CutKind::PowerLoss4k(rng.next()),
        }
    }
}

/// An injected I/O error (NS09, NS11). EIO faults fire once; ENOSPC faults repeat until
/// the step's window ends (a full disk that frees up again).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultSpec {
    /// EIO on the n-th `pwrite` anywhere (journal, runs, manifest).
    EioWrite(u64),
    /// EIO on the n-th `fdatasync` (the journal): fsyncgate.
    EioSyncData(u64),
    /// EIO on the n-th `fsync` (runs, manifests, `CURRENT.tmp`, new segments).
    EioSyncAll(u64),
    /// EIO on the n-th directory sync.
    EioSyncDir(u64),
    /// EIO on the n-th rename.
    EioRename(u64),
    /// EIO on the n-th read of a run file (an unreadable block).
    EioReadRun(u64),
    /// ENOSPC on every write under `runs/` (flush and compaction).
    NoSpaceRuns,
    /// ENOSPC on every segment preallocation and every run write: no flush retires a
    /// segment, so the journal runs out of spare segments and a roll fails with
    /// `NoSpace` (the one journal error that does not latch).
    NoSpacePreallocate,
    /// ENOSPC on every write anywhere.
    NoSpaceAll,
}

impl FaultSpec {
    pub fn is_no_space(self) -> bool {
        matches!(
            self,
            FaultSpec::NoSpaceRuns | FaultSpec::NoSpacePreallocate | FaultSpec::NoSpaceAll
        )
    }

    pub fn random(rng: &mut Rng) -> FaultSpec {
        let n = 1 + rng.below(12);
        match rng.below(9) {
            0 => FaultSpec::EioWrite(n),
            1 => FaultSpec::EioSyncData(n),
            2 => FaultSpec::EioSyncAll(n),
            3 => FaultSpec::EioSyncDir(n),
            4 => FaultSpec::EioRename(n),
            5 => FaultSpec::EioReadRun(n),
            6 => FaultSpec::NoSpaceRuns,
            7 => FaultSpec::NoSpacePreallocate,
            _ => FaultSpec::NoSpaceAll,
        }
    }
}

#[derive(Clone, Debug)]
pub enum Step {
    /// A batch, waited for unless `cancel` (the caller polls `done()` once and drops it).
    Write {
        batch: ModelBatch,
        sync: bool,
        cancel: bool,
    },
    /// `Store::sync`.
    Sync,
    /// Hold a snapshot (at most four) with the oracle state it must keep showing.
    TakeSnapshot,
    DropSnapshot(u64),
    /// Point reads on the current state and on a held snapshot (seeded).
    Gets(u64),
    /// An iterator walk on the current state and on a held snapshot (seeded).
    Walk(u64),
    Flush,
    Compact,
    Gc,
    /// `release_scope` of a group prefix.
    Release(u64),
    /// Clean close and reopen.
    Reopen,
    /// Inject a fault for the next `window` steps.
    Fault {
        spec: FaultSpec,
        window: u64,
    },
    /// Take an image of the running machine now and continue on it.
    Cut(CutKind),
    /// Freeze the machine after `after` more mutating filesystem operations; the cut
    /// happens when the store reports the failure.
    ArmCrash {
        after: u64,
        kind: CutKind,
    },
}

/// What a generated workload contains.
#[derive(Clone, Copy, Debug)]
pub struct Mix {
    pub groups: u64,
    pub keys: u64,
    /// In-run cuts and armed crashes (several recoveries per run).
    pub cuts: bool,
    /// EIO and ENOSPC injection.
    pub faults: bool,
    /// Callers that cancel their commit.
    pub cancel: bool,
    pub release: bool,
    pub reopen: bool,
}

impl Mix {
    pub fn full() -> Mix {
        Mix {
            groups: 6,
            keys: 48,
            cuts: true,
            faults: true,
            cancel: true,
            release: true,
            reopen: true,
        }
    }
}

/// A unique value: `<tag.i>` padded, so damage tests can find it by content.
pub fn value(rng: &mut Rng, tag: u64, i: usize, small: bool) -> Vec<u8> {
    let mut v = format!("<{tag}.{i}>").into_bytes();
    let roll = if small { 99 } else { rng.below(100) };
    let len = match roll {
        0..=3 => 1200 + rng.below(1200),
        4..=20 => 200 + rng.below(600),
        _ => rng.below(120),
    };
    v.resize(v.len() + len as usize, b'a' + (tag % 26) as u8);
    v
}

pub fn store_key(k: u64) -> Vec<u8> {
    format!("s/k{k:03}").into_bytes()
}

/// A batch confined to one group (scoped), or a store-wide one over several groups.
pub fn batch(rng: &mut Rng, mix: &Mix, tag: u64) -> ModelBatch {
    let store_wide = rng.chance(12);
    let group = rng.below(mix.groups);
    let max = if rng.chance(10) { 24 } else { 5 };
    let n = 1 + rng.below(max);
    let mut ops = Vec::new();
    let mut bytes = 0;
    for i in 0..n as usize {
        let g = if store_wide {
            rng.below(mix.groups)
        } else {
            group
        };
        let key = if store_wide && rng.chance(30) {
            store_key(rng.below(mix.keys))
        } else {
            group_key(g, rng.below(mix.keys))
        };
        // Batches stay well under the smallest journal segment (12 KiB).
        let small = max > 5 || bytes > 600;
        let op = match rng.below(100) {
            0..=44 => Op::Put(key, value(rng, tag, i, small)),
            45..=69 => Op::PutJournal(key, value(rng, tag, i, small)),
            70..=89 => Op::Delete(key),
            _ => {
                let a = rng.below(mix.keys);
                let b = a + rng.below(mix.keys / 2 + 1);
                let end = if rng.chance(15) {
                    let mut p = group_prefix(g);
                    *p.last_mut().unwrap() += 1;
                    p
                } else {
                    group_key(g, b)
                };
                Op::DeleteRange(group_key(g, a), end)
            }
        };
        if let Op::Put(_, v) | Op::PutJournal(_, v) = &op {
            bytes += v.len();
        }
        ops.push(op);
    }
    ModelBatch {
        scope: if store_wide {
            Vec::new()
        } else {
            group_prefix(group)
        },
        ops,
    }
}

/// `steps` seeded steps. Writes dominate; reads, maintenance, faults and cuts are mixed in.
pub fn generate(seed: u64, steps: usize, mix: &Mix) -> Vec<Step> {
    let mut rng = Rng(seed ^ 0x4E53_3130);
    let mut out = Vec::with_capacity(steps);
    for i in 0..steps as u64 {
        let tag = seed.wrapping_mul(1_000_003).wrapping_add(i) % 1_000_000_007;
        let roll = rng.below(1000);
        let step = match roll {
            0..=599 => Step::Write {
                batch: batch(&mut rng, mix, tag),
                sync: rng.chance(20),
                cancel: mix.cancel && rng.chance(15),
            },
            600..=619 => Step::Sync,
            620..=644 => Step::TakeSnapshot,
            645..=664 => Step::DropSnapshot(rng.next()),
            665..=729 => Step::Gets(rng.next()),
            730..=779 => Step::Walk(rng.next()),
            780..=809 => Step::Flush,
            810..=824 => Step::Compact,
            825..=844 => Step::Gc,
            845..=852 if mix.release => Step::Release(rng.below(mix.groups)),
            853..=858 if mix.reopen => Step::Reopen,
            859..=876 if mix.faults => {
                let spec = FaultSpec::random(&mut rng);
                // A full disk lasts long enough for the journal to reach a roll.
                let window = if spec.is_no_space() {
                    10 + rng.below(50)
                } else {
                    1 + rng.below(12)
                };
                Step::Fault { spec, window }
            }
            877..=884 if mix.cuts => Step::Cut(CutKind::random(&mut rng)),
            885..=894 if mix.cuts => Step::ArmCrash {
                after: rng.below(400),
                kind: CutKind::random(&mut rng),
            },
            _ => Step::Write {
                batch: batch(&mut rng, mix, tag),
                sync: rng.chance(20),
                cancel: false,
            },
        };
        out.push(step);
    }
    out
}
