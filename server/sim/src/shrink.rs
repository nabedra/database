//! Shrinking a failing run's workload and nemesis tape with `nabedra_check::prop::shrink`.
//!
//! The schedule (tokio's rng, node streams, the net stream) still comes from the seed, and
//! only the tape is edited, so shrinking is a search, not a guarantee. A candidate is kept
//! when any of the original violation kinds reproduces. The shrunk tape is written as
//! `<seed>.tape`; `NABEDRA_SIM_TAPE=<file>` replays it.

use std::collections::BTreeSet;
use std::path::Path;

use nabedra_check::Gen;
use nabedra_check::prop::{self, Shrunk};

/// Runs the shrinker may spend on one failure.
pub const DEFAULT_SHRINK_BUDGET: u64 = 200;

/// The invariant name of a violation: the text before the first `:`, without the
/// `t=<ms>ms ` prefix [`crate::RaftObserver::violate`] adds. `"t=5ms MonotonicEpoch: n1 ..."`
/// is `MonotonicEpoch`.
pub fn violation_kind(violation: &str) -> &str {
    let text = match violation.strip_prefix("t=") {
        Some(rest) => rest
            .split_once("ms ")
            .filter(|(ms, _)| ms.bytes().all(|b| b.is_ascii_digit()))
            .map_or(violation, |(_, text)| text),
        None => violation,
    };
    text.split_once(':').map_or(text, |(kind, _)| kind).trim()
}

/// The distinct kinds of `violations`.
pub fn kinds<S: AsRef<str>>(violations: &[S]) -> BTreeSet<String> {
    violations
        .iter()
        .map(|violation| violation_kind(violation.as_ref()).to_owned())
        .collect()
}

/// Shrinks `tape`, which made `run` report a violation of one of `kinds`. `run` gets a
/// generator replaying the candidate tape (with seed 0: take the schedule seed from the
/// closure, never from `Gen::seed`) and returns the run's violations.
pub fn shrink_tape(
    tape: Vec<u64>,
    kinds: &BTreeSet<String>,
    budget: u64,
    run: impl Fn(&mut Gen) -> Vec<String>,
) -> Shrunk {
    let reproduces = |g: &mut Gen| -> Result<(), String> {
        let violations = run(g);
        let matching: Vec<&String> = violations
            .iter()
            .filter(|violation| kinds.contains(violation_kind(violation)))
            .collect();
        if matching.is_empty() {
            Ok(())
        } else {
            Err(matching
                .iter()
                .map(|v| v.as_str())
                .collect::<Vec<_>>()
                .join("\n"))
        }
    };
    let message = kinds.iter().cloned().collect::<Vec<_>>().join(", ");
    prop::shrink(tape, message, budget, &reproduces)
}

/// Writes a tape as a JSON array of choices.
pub fn write_tape(path: &Path, tape: &[u64]) -> std::io::Result<()> {
    std::fs::write(path, serde_json::to_vec(tape)?)
}

/// Reads a tape written by [`write_tape`].
pub fn read_tape(path: &Path) -> std::io::Result<Vec<u64>> {
    let bytes = std::fs::read(path)?;
    serde_json::from_slice(&bytes)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
}
