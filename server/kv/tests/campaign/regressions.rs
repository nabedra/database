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
pub const FIXED: &[Case] = &[];

/// Unfixed findings (each has an ignored test naming it in `main.rs`).
pub const OPEN_FINDINGS: &[Case] = &[];
