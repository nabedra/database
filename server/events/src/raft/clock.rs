//! Guarded time for partition commands (EVENTS-2; plan, "Brokers and storage").
//!
//! A leader stamps publish, lease and retention commands from an interval that holds the
//! true time, not from its own wall clock alone. Each voter of the group contributes one
//! sample: its wall clock, read during a sampling request whose send time the leader
//! records on its monotonic clock. A healthy voter's clock is within
//! `max_clock_error_ms` of true time, so the sample gives a lower endpoint (the reading
//! minus the error) and an upper endpoint (the reading plus the error plus the time since
//! the request was sent). With `f = (voters - 1) / 2` voters that may be wrong, the bounds
//! are the (f+1)-th largest lower endpoint and the (f+1)-th smallest upper endpoint among
//! at least f+1 fresh samples: for RF3, the second of each, which discards one outlier
//! with three samples and takes the union of two. The interval must be nonempty and at
//! most `max_uncertainty_ms` wide; otherwise the stamp is refused and retention waits.
//! `specs/events-time/EventsTime.tla` checks these rules. Guarded time is the only mode:
//! there is no fallback to the leader's own clock.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::rt::Instant;

/// The clock bound a deployment states (EVT-G08's tolerance).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClockPolicy {
    /// The largest wall-clock error of a healthy broker against true time.
    #[serde(default = "default_error")]
    pub max_clock_error_ms: i64,
    /// The widest interval a stamp may use. Wider means the voters disagree by more than
    /// their stated error allows: the command is refused rather than stamped.
    #[serde(default = "default_uncertainty")]
    pub max_uncertainty_ms: i64,
    /// How long a peer's sample stays usable; older samples are taken again.
    #[serde(default = "default_age")]
    pub sample_max_age_ms: u64,
}

fn default_error() -> i64 {
    250
}

fn default_uncertainty() -> i64 {
    2_000
}

fn default_age() -> u64 {
    1_000
}

impl Default for ClockPolicy {
    fn default() -> Self {
        Self {
            max_clock_error_ms: default_error(),
            max_uncertainty_ms: default_uncertainty(),
            sample_max_age_ms: default_age(),
        }
    }
}

/// An interval that holds the true time at the moment it was computed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimeBounds {
    pub lower_ms: i64,
    pub upper_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ClockError {
    #[error("not enough fresh voter clock samples")]
    Insufficient,
    #[error("voter clocks disagree beyond the uncertainty budget")]
    TooWide,
}

#[derive(Clone, Copy, Debug)]
struct Sample {
    wall_ms: i64,
    sent: Instant,
}

/// The clock samples one broker holds for its peers, shared by its groups.
pub(crate) struct Clocks {
    policy: Mutex<ClockPolicy>,
    samples: Mutex<BTreeMap<u64, Sample>>,
    /// One sampling round at a time; concurrent stamps share its result.
    pub(super) round: tokio::sync::Mutex<()>,
}

impl Clocks {
    pub fn new(policy: ClockPolicy) -> Self {
        Self {
            policy: Mutex::new(policy),
            samples: Mutex::default(),
            round: tokio::sync::Mutex::new(()),
        }
    }

    pub fn policy(&self) -> ClockPolicy {
        *self.policy.lock().expect("clock policy lock")
    }

    pub fn set_policy(&self, policy: ClockPolicy) {
        *self.policy.lock().expect("clock policy lock") = policy;
    }

    /// Records `peer`'s wall-clock reading for a request sent at `sent`. An unreachable
    /// peer (`None`) keeps its older sample until that ages out.
    pub fn record(&self, peer: u64, sent: Instant, answer: Option<i64>) {
        let Some(wall_ms) = answer else {
            return;
        };
        let mut samples = self.samples.lock().expect("clock samples lock");
        if samples.get(&peer).is_none_or(|old| old.sent <= sent) {
            samples.insert(peer, Sample { wall_ms, sent });
        }
    }

    /// Whether some peer voter has no sample young enough to skip a new round.
    pub fn needs_round(&self, local: u64, voters: &BTreeSet<u64>, now: Instant) -> bool {
        let refresh = Duration::from_millis(self.policy().sample_max_age_ms / 2);
        let samples = self.samples.lock().expect("clock samples lock");
        voters.iter().filter(|voter| **voter != local).any(|voter| {
            samples
                .get(voter)
                .is_none_or(|sample| now.saturating_duration_since(sample.sent) > refresh)
        })
    }

    /// The guarded interval for a group with these voters, at `now` on this broker's
    /// monotonic clock, whose wall clock reads `local_wall_ms`.
    pub fn bounds(
        &self,
        local: u64,
        local_wall_ms: i64,
        now: Instant,
        voters: &BTreeSet<u64>,
    ) -> Result<TimeBounds, ClockError> {
        let policy = self.policy();
        let max_age = Duration::from_millis(policy.sample_max_age_ms);
        let error = policy.max_clock_error_ms;
        let samples = self.samples.lock().expect("clock samples lock");
        let mut endpoints = Vec::with_capacity(voters.len());
        for voter in voters {
            if *voter == local {
                endpoints.push((
                    local_wall_ms.saturating_sub(error),
                    local_wall_ms.saturating_add(error),
                ));
            } else if let Some(sample) = samples.get(voter) {
                let age = now.saturating_duration_since(sample.sent);
                if age <= max_age {
                    let age_ms = i64::try_from(age.as_millis()).unwrap_or(i64::MAX);
                    endpoints.push((
                        sample.wall_ms.saturating_sub(error),
                        sample
                            .wall_ms
                            .saturating_add(error)
                            .saturating_add(age_ms),
                    ));
                }
            }
        }
        let (lower_ms, upper_ms) = interval(&policy, voters.len(), endpoints)?;
        Ok(TimeBounds { lower_ms, upper_ms })
    }
}

/// The (f+1)-th largest lower and (f+1)-th smallest upper endpoint, with `f` the number
/// of wrong clocks a group of `voters` tolerates.
pub fn interval(
    policy: &ClockPolicy,
    voters: usize,
    endpoints: Vec<(i64, i64)>,
) -> Result<(i64, i64), ClockError> {
    let rank = voters.saturating_sub(1) / 2 + 1;
    if voters == 0 || endpoints.len() < rank {
        return Err(ClockError::Insufficient);
    }
    let mut lowers: Vec<i64> = endpoints.iter().map(|(lower, _)| *lower).collect();
    let mut uppers: Vec<i64> = endpoints.iter().map(|(_, upper)| *upper).collect();
    lowers.sort_unstable_by(|a, b| b.cmp(a));
    uppers.sort_unstable();
    let (lower, upper) = (lowers[rank - 1], uppers[rank - 1]);
    if lower > upper || upper.saturating_sub(lower) > policy.max_uncertainty_ms {
        return Err(ClockError::TooWide);
    }
    Ok((lower, upper))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> ClockPolicy {
        ClockPolicy {
            max_clock_error_ms: 100,
            max_uncertainty_ms: 1_000,
            sample_max_age_ms: 1_000,
        }
    }

    /// EventsTime `Bounds`: RF3 discards one outlier with three samples and takes the
    /// union of two; one sample is not enough; RF1 uses its own clock.
    #[test]
    fn events_2_interval_tolerates_one_wrong_clock_of_three() {
        let p = policy();
        // True time 10_000; n3's clock runs 60 s fast.
        let honest = |reading: i64| (reading - 100, reading + 100);
        let three = vec![honest(10_000), honest(10_020), honest(70_000)];
        let (lower, upper) = interval(&p, 3, three).expect("three samples");
        assert!(lower <= 10_000 && 10_000 <= upper, "{lower}..{upper}");
        // Two samples, one of them wrong: the union is too wide to stamp from.
        assert_eq!(
            interval(&p, 3, vec![honest(10_000), honest(70_000)]),
            Err(ClockError::TooWide)
        );
        // Two honest samples: the union holds true time.
        let (lower, upper) = interval(&p, 3, vec![honest(10_000), honest(9_990)]).unwrap();
        assert!(lower <= 10_000 && 10_000 <= upper);
        assert_eq!(
            interval(&p, 3, vec![honest(10_000)]),
            Err(ClockError::Insufficient)
        );
        assert_eq!(interval(&p, 1, vec![honest(10_000)]), Ok((9_900, 10_100)));
    }

    /// The plan's mutant `one_fast_lower`: taking the largest lower endpoint lets one
    /// fast clock move retention ahead. The second largest does not.
    #[test]
    fn events_2_one_fast_clock_cannot_move_the_lower_bound() {
        let p = policy();
        let fast = (12_000 - 100, 12_000 + 100);
        let (lower, _) = interval(
            &p,
            3,
            vec![(9_900, 10_100), (9_950, 10_150), fast],
        )
        .expect("three samples");
        assert!(lower <= 10_000, "lower {lower} is ahead of true time");
    }

    #[test]
    fn events_2_samples_age_out() {
        let clocks = Clocks::new(policy());
        let voters = BTreeSet::from([1, 2, 3]);
        let start = tokio::time::Instant::now();
        assert_eq!(
            clocks.bounds(1, 10_000, start, &voters),
            Err(ClockError::Insufficient)
        );
        assert!(clocks.needs_round(1, &voters, start));
        clocks.record(2, start, Some(10_010));
        let bounds = clocks.bounds(1, 10_000, start, &voters).expect("two samples");
        assert!(bounds.lower_ms <= 10_000 && bounds.upper_ms >= 10_010);
        // A sample older than its age limit no longer counts.
        let later = start + Duration::from_millis(1_001);
        assert_eq!(
            clocks.bounds(1, 11_001, later, &voters),
            Err(ClockError::Insufficient)
        );
        // An unreachable peer adds nothing; a fresh answer counts again.
        clocks.record(3, later, None);
        assert!(clocks.bounds(1, 11_001, later, &voters).is_err());
        clocks.record(3, later, Some(11_000));
        assert!(clocks.bounds(1, 11_001, later, &voters).is_ok());
    }
}
