//! Shrunk failing cases, replayed by `ns10_regressions_replay_shrunk_cases`.
//!
//! When a campaign test fails it shrinks the failing seed (unless `NS10_SHRINK=0`) and
//! prints a `Case` line to paste here. A case whose bug is not fixed yet goes in
//! `OPEN_FINDINGS` instead, replayed by an ignored test until the fix lands.

use crate::workload::Mix;

pub struct Case {
    pub seed: u64,
    /// Length of the seeded step list the indices refer to.
    pub steps: usize,
    pub mix: Mix,
    pub damage_trials: usize,
    /// The shrunk sub-list: indices into `workload::generate(seed, steps, &mix)`.
    pub keep: &'static [usize],
    pub note: &'static str,
}

/// Fixed findings: these must pass.
pub const FIXED: &[Case] = &[
    // KVfix3, CI 7f1e328 (Linux release, `ns05_ns10_thousands_of_seeds`): an `EioReadRun`
    // fault still armed at a clean reopen failed a run read inside the harness's state
    // dump. The harness now retries a dump once after an injected `Io` read error.
    Case {
        seed: 28,
        steps: 400,
        mix: Mix::full(),
        damage_trials: 0,
        keep: &[357, 358, 361, 365],
        note: "injected run read EIO during the dump after a clean reopen",
    },
    // KVfix3, CI 34ce7f4 (Linux debug, `ns05_ns10_power_loss_faults_and_recoveries_sampled`):
    // an armed crash froze the machine during the dump after a clean reopen (background
    // work after the open reached the armed operation), and the harness failed the run
    // instead of cutting. Whether the crash lands in the dump depends on timing.
    Case {
        seed: 6,
        steps: 400,
        mix: Mix::full(),
        damage_trials: 0,
        keep: &[
            271, 272, 295, 296, 297, 302, 306, 308, 309, 311, 312, 314, 315, 316, 317, 318, 319,
            320, 321, 322, 323, 324, 350, 351, 352, 353, 354, 355, 356, 357, 358, 359, 360, 361,
            362, 363, 364, 365, 366, 367,
        ],
        note: "armed crash during the dump after a clean reopen (timing)",
    },
    // KVfix3, CI 55f6d85 (Linux debug, run 44726640722): an `EioRename` fault still armed
    // at a clean reopen failed the open's `CURRENT` switch; the harness failed the run
    // instead of recovering from the crash image.
    Case {
        seed: 11,
        steps: 400,
        mix: Mix::full(),
        damage_trials: 0,
        keep: &[224, 246],
        note: "clean reopen failed under an injected rename EIO",
    },
];

/// Unfixed findings (each has an ignored test naming it in `main.rs`).
pub const OPEN_FINDINGS: &[Case] = &[];
