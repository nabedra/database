//! The clock seam.
//!
//! [`now`] is the current node's monotonic clock and [`wall_now`] its wall clock. Product
//! builds forward to tokio and std. Under `sim`, [`now`] applies the node's monotonic
//! offset and drift and never goes backward, while [`wall_now`] applies the node's wall
//! offset, drift and explicit jumps, which may go backward (scenario O09, OLTP-G02).
//!
//! Deadlines passed to [`sleep_until`], [`timeout_at`] and [`interval_at`] are read on
//! the node clock, so they are converted back to the runtime's clock under `sim`.

pub use std::time::{Duration, SystemTime, UNIX_EPOCH};
pub use tokio::time::{Instant, Interval, MissedTickBehavior, Sleep, Timeout, error::Elapsed};

use std::future::Future;

/// The monotonic clock of the current node.
#[inline]
pub fn now() -> Instant {
    #[cfg(feature = "sim")]
    {
        crate::sim::mono_now()
    }
    #[cfg(not(feature = "sim"))]
    {
        Instant::now()
    }
}

/// The monotonic clock as a std [`std::time::Instant`], for synchronous code that stores
/// the std type. Under `sim` it is a fixed base plus the node's virtual elapsed time.
#[inline]
pub fn now_std() -> std::time::Instant {
    #[cfg(feature = "sim")]
    {
        crate::sim::mono_now_std()
    }
    #[cfg(not(feature = "sim"))]
    {
        std::time::Instant::now()
    }
}

/// The wall clock of the current node.
#[inline]
pub fn wall_now() -> SystemTime {
    #[cfg(feature = "sim")]
    {
        crate::sim::wall_now()
    }
    #[cfg(not(feature = "sim"))]
    {
        SystemTime::now()
    }
}

/// [`wall_now`] as milliseconds since the Unix epoch, 0 if it is before the epoch.
#[inline]
pub fn unix_ms() -> u64 {
    since_epoch().map_or(0, |elapsed| elapsed.as_millis() as u64)
}

/// [`wall_now`] as microseconds since the Unix epoch, 0 if it is before the epoch.
#[inline]
pub fn unix_micros() -> u64 {
    since_epoch().map_or(0, |elapsed| elapsed.as_micros() as u64)
}

/// [`wall_now`] as nanoseconds since the Unix epoch, 0 if it is before the epoch.
#[inline]
pub fn unix_nanos() -> u128 {
    since_epoch().map_or(0, |elapsed| elapsed.as_nanos())
}

#[inline]
fn since_epoch() -> Option<Duration> {
    wall_now().duration_since(UNIX_EPOCH).ok()
}

/// [`tokio::time::sleep`].
#[inline]
pub fn sleep(duration: Duration) -> Sleep {
    #[cfg(feature = "sim")]
    {
        tokio::time::sleep(crate::sim::to_runtime_duration(duration))
    }
    #[cfg(not(feature = "sim"))]
    {
        tokio::time::sleep(duration)
    }
}

/// [`tokio::time::sleep_until`] for a deadline on the node clock ([`now`]).
#[inline]
pub fn sleep_until(deadline: Instant) -> Sleep {
    tokio::time::sleep_until(to_runtime(deadline))
}

/// [`tokio::time::timeout`].
#[inline]
pub fn timeout<F: Future>(duration: Duration, future: F) -> Timeout<F> {
    #[cfg(feature = "sim")]
    {
        tokio::time::timeout(crate::sim::to_runtime_duration(duration), future)
    }
    #[cfg(not(feature = "sim"))]
    {
        tokio::time::timeout(duration, future)
    }
}

/// [`tokio::time::timeout_at`] for a deadline on the node clock ([`now`]).
#[inline]
pub fn timeout_at<F: Future>(deadline: Instant, future: F) -> Timeout<F> {
    tokio::time::timeout_at(to_runtime(deadline), future)
}

/// [`tokio::time::interval`].
#[inline]
pub fn interval(period: Duration) -> Interval {
    #[cfg(feature = "sim")]
    {
        tokio::time::interval(crate::sim::to_runtime_duration(period))
    }
    #[cfg(not(feature = "sim"))]
    {
        tokio::time::interval(period)
    }
}

/// [`tokio::time::interval_at`] for a start on the node clock ([`now`]).
#[inline]
pub fn interval_at(start: Instant, period: Duration) -> Interval {
    #[cfg(feature = "sim")]
    {
        tokio::time::interval_at(to_runtime(start), crate::sim::to_runtime_duration(period))
    }
    #[cfg(not(feature = "sim"))]
    {
        tokio::time::interval_at(start, period)
    }
}

/// [`std::thread::sleep`], for blocking threads only.
///
/// Under `sim` the calling thread does not sleep: blocking work runs inline on the
/// simulation thread, and a real sleep there would stall the whole simulated cluster
/// without advancing virtual time.
#[inline]
pub fn thread_sleep(duration: Duration) {
    #[cfg(feature = "sim")]
    {
        crate::sim::thread_sleep(duration)
    }
    #[cfg(not(feature = "sim"))]
    {
        std::thread::sleep(duration)
    }
}

#[inline]
fn to_runtime(deadline: Instant) -> Instant {
    #[cfg(feature = "sim")]
    {
        crate::sim::to_runtime_instant(deadline)
    }
    #[cfg(not(feature = "sim"))]
    {
        deadline
    }
}
