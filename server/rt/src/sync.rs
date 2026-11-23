//! The synchronisation seam.
//!
//! [`Condvar`] is `std::sync::Condvar` in a product build (`repr(transparent)`, inline
//! forwarders). Under `sim`, a notification also rings a doorbell that the stepped
//! service driver awaits (see [`crate::task::Stepped`]), and a wait on the simulation
//! thread or its blocking helper, inside a world, panics: nothing else runs while that
//! thread waits, so the wait could never end. Code that waits for a service there calls
//! [`crate::task::drive_until`] instead.

use std::sync::{LockResult, MutexGuard, WaitTimeoutResult};
use std::time::Duration;

/// A condition variable: `std::sync::Condvar` in product.
#[cfg_attr(not(feature = "sim"), repr(transparent))]
#[derive(Debug, Default)]
pub struct Condvar {
    inner: std::sync::Condvar,
    #[cfg(feature = "sim")]
    bell: std::sync::Arc<crate::sim::services::Bell>,
}

impl Condvar {
    /// [`std::sync::Condvar::new`].
    #[inline]
    #[must_use]
    pub fn new() -> Condvar {
        Condvar {
            inner: std::sync::Condvar::new(),
            #[cfg(feature = "sim")]
            bell: Default::default(),
        }
    }

    /// [`std::sync::Condvar::wait`].
    #[inline]
    pub fn wait<'a, T>(&self, guard: MutexGuard<'a, T>) -> LockResult<MutexGuard<'a, T>> {
        #[cfg(feature = "sim")]
        crate::sim::services::before_wait("Condvar::wait");
        self.inner.wait(guard)
    }

    /// [`std::sync::Condvar::wait_while`].
    #[inline]
    pub fn wait_while<'a, T, F>(
        &self,
        guard: MutexGuard<'a, T>,
        condition: F,
    ) -> LockResult<MutexGuard<'a, T>>
    where
        F: FnMut(&mut T) -> bool,
    {
        #[cfg(feature = "sim")]
        crate::sim::services::before_wait("Condvar::wait_while");
        self.inner.wait_while(guard, condition)
    }

    /// [`std::sync::Condvar::wait_timeout`].
    #[inline]
    pub fn wait_timeout<'a, T>(
        &self,
        guard: MutexGuard<'a, T>,
        timeout: Duration,
    ) -> LockResult<(MutexGuard<'a, T>, WaitTimeoutResult)> {
        #[cfg(feature = "sim")]
        crate::sim::services::before_wait("Condvar::wait_timeout");
        self.inner.wait_timeout(guard, timeout)
    }

    /// [`std::sync::Condvar::wait_timeout_while`].
    #[inline]
    pub fn wait_timeout_while<'a, T, F>(
        &self,
        guard: MutexGuard<'a, T>,
        timeout: Duration,
        condition: F,
    ) -> LockResult<(MutexGuard<'a, T>, WaitTimeoutResult)>
    where
        F: FnMut(&mut T) -> bool,
    {
        #[cfg(feature = "sim")]
        crate::sim::services::before_wait("Condvar::wait_timeout_while");
        self.inner.wait_timeout_while(guard, timeout, condition)
    }

    /// [`std::sync::Condvar::notify_one`]. Under `sim` it also rings this condvar's
    /// doorbell and the current node's bell.
    #[inline]
    pub fn notify_one(&self) {
        self.inner.notify_one();
        #[cfg(feature = "sim")]
        crate::sim::services::ring(&self.bell);
    }

    /// [`std::sync::Condvar::notify_all`]. Under `sim` it also rings this condvar's
    /// doorbell and the current node's bell.
    #[inline]
    pub fn notify_all(&self) {
        self.inner.notify_all();
        #[cfg(feature = "sim")]
        crate::sim::services::ring(&self.bell);
    }

    /// The doorbell the simulated service driver awaits.
    #[cfg(feature = "sim")]
    #[allow(dead_code)] // skeleton: the stepped driver uses it in P9-RT (b)
    pub(crate) fn bell(&self) -> &std::sync::Arc<crate::sim::services::Bell> {
        &self.bell
    }
}
