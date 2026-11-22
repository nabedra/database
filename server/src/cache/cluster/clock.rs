//! Clock sources for lease fencing. Endpoints enclose the sampled interval.
use std::io;

use nabedra_rt::time::{self, UNIX_EPOCH};
use serde::Deserialize;

const NANOS_PER_MILLISECOND: u64 = 1_000_000;
const MAX_UNCERTAINTY_NS: u64 = 10 * NANOS_PER_MILLISECOND;

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ClockMode {
    ClockBound,
    LocalDocker,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ClockBounds {
    pub lower_ms: u64,
    pub upper_ms: u64,
}

impl ClockBounds {
    pub fn allows(self, deadline_ms: u64) -> bool {
        self.upper_ms < deadline_ms
    }

    pub fn fenced(self, deadline_ms: u64) -> bool {
        self.lower_ms >= deadline_ms
    }
}

pub(super) struct Clock {
    mode: ClockMode,
    last_lower_ns: u64,
    last_upper_ns: u64,
}

impl Clock {
    pub fn new(mode: ClockMode) -> io::Result<Self> {
        #[cfg(not(target_os = "linux"))]
        if matches!(mode, ClockMode::ClockBound) {
            return Err(unavailable("ClockBound requires Linux"));
        }
        Ok(Self {
            mode,
            last_lower_ns: 0,
            last_upper_ns: 0,
        })
    }

    pub fn now(&mut self) -> io::Result<ClockBounds> {
        let (lower, upper) = match self.mode {
            #[cfg(target_os = "linux")]
            ClockMode::ClockBound => clockbound_now()?,
            #[cfg(not(target_os = "linux"))]
            ClockMode::ClockBound => return Err(unavailable("ClockBound requires Linux")),
            ClockMode::LocalDocker => {
                // These fixtures share the Docker VM's clock. This mode does
                // not establish a time bound between separate hosts.
                let time = time::wall_now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(io::Error::other)?;
                let ns = u64::try_from(time.as_nanos())
                    .map_err(|_| unavailable("local Docker clock exceeds range"))?;
                (ns, ns)
            }
        };
        self.validate(lower, upper)
    }

    fn validate(&mut self, lower: u64, upper: u64) -> io::Result<ClockBounds> {
        if upper
            .checked_sub(lower)
            .is_none_or(|width| width > MAX_UNCERTAINTY_NS)
        {
            return Err(unavailable("cache clock uncertainty exceeds 10 ms"));
        }
        if upper < self.last_lower_ns {
            return Err(unavailable("cache clock moved backwards"));
        }
        // A widening interval can have an earlier lower endpoint. The prior
        // lower endpoint still bounds the current time.
        self.last_lower_ns = self.last_lower_ns.max(lower);
        // Keep the upper endpoint monotonic through overlapping samples so
        // a key hidden by expiry cannot become visible again.
        self.last_upper_ns = self.last_upper_ns.max(upper);
        Ok(ClockBounds {
            lower_ms: self.last_lower_ns / NANOS_PER_MILLISECOND,
            upper_ms: self.last_upper_ns.div_ceil(NANOS_PER_MILLISECOND),
        })
    }
}

#[cfg(target_os = "linux")]
fn clockbound_now() -> io::Result<(u64, u64)> {
    use clock_bound_client::{ClockBoundClient, ClockStatus};
    use std::cell::RefCell;

    // A ClockBound mapping is not Send. Each runtime thread reads its own
    // mapping and each logical actor retains its own rollback check.
    thread_local! {
        static CLIENT: RefCell<Option<ClockBoundClient>> = const { RefCell::new(None) };
    }
    CLIENT.with_borrow_mut(|cached| {
        if cached.is_none() {
            *cached = Some(
                ClockBoundClient::new()
                    .map_err(|_| unavailable("ClockBound initialization failed"))?,
            );
        }
        let sample = cached.as_mut().unwrap().now();
        match sample {
            Ok(value) if value.clock_status == ClockStatus::Synchronized => Ok((
                unix_ns(value.earliest.tv_sec(), value.earliest.tv_nsec())?,
                unix_ns(value.latest.tv_sec(), value.latest.tv_nsec())?,
            )),
            _ => {
                *cached = None;
                Err(unavailable("ClockBound is not synchronized"))
            }
        }
    })
}

#[cfg(target_os = "linux")]
fn unix_ns(seconds: i64, nanoseconds: i64) -> io::Result<u64> {
    if !(0..1_000_000_000).contains(&nanoseconds) {
        return Err(unavailable("invalid clock nanoseconds"));
    }
    u64::try_from(seconds)
        .ok()
        .and_then(|value| value.checked_mul(1_000_000_000))
        .and_then(|value| value.checked_add(nanoseconds as u64))
        .ok_or_else(|| unavailable("cache clock exceeds range"))
}

fn unavailable(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::NotConnected, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounds_round_outward_and_switch_only_at_deadline() {
        let mut clock = Clock::new(ClockMode::LocalDocker).unwrap();
        let initial = clock.validate(50_000_001, 51_500_000).unwrap();
        assert_eq!(
            initial,
            ClockBounds {
                lower_ms: 50,
                upper_ms: 52
            }
        );
        assert!(initial.allows(53));
        assert!(!initial.allows(52));
        assert!(!initial.fenced(52));

        let advanced = clock.validate(52_000_001, 56_500_000).unwrap();
        assert!(advanced.fenced(52));
        assert!(!advanced.allows(52));
    }

    #[test]
    fn uncertain_rollback_and_inverted_intervals_cannot_authorize() {
        let mut clock = Clock::new(ClockMode::LocalDocker).unwrap();
        clock.validate(100_000_000, 103_000_000).unwrap();
        assert!(clock.validate(9_000_000, 10_000_000).is_err());
        assert!(clock.validate(100_000_001, 111_000_000).is_err());
        assert!(clock.validate(104_000_000, 103_000_000).is_err());

        let overlaps = clock.validate(98_000_000, 102_000_000).unwrap();
        assert_eq!(overlaps.lower_ms, 100);
        assert_eq!(overlaps.upper_ms, 103);
    }
}
