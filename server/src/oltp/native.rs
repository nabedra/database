//! Explicit execution-pool and housekeeping reservations. These are logical
//! stack/guard/metadata charges, not measurements of resident memory.
use super::failure;
use crate::protocol::{ErrorCode, ProtocolError};
use serde::Serialize;

// The macOS SDK pthread/pthread.h declares this POSIX function, while the
// pinned libc crate omits its declaration on Apple targets.
#[cfg(target_os = "macos")]
unsafe extern "C" {
    fn pthread_attr_getguardsize(
        attributes: *const libc::pthread_attr_t,
        size: *mut usize,
    ) -> libc::c_int;
}
#[cfg(all(unix, not(target_os = "macos")))]
use libc::pthread_attr_getguardsize;

#[derive(Clone, Copy, Debug, Serialize)]
pub struct NativeThreadBudget {
    pub query_threads: usize,
    pub control_threads: usize,
    pub maintenance_high_threads: usize,
    pub maintenance_low_threads: usize,
    pub housekeeping_timer_threads: usize,
    pub query_control_stack_bytes_per_thread: usize,
    pub native_stack_bytes_per_thread: usize,
    pub total_stack_reservation_bytes: usize,
}
impl NativeThreadBudget {
    pub(super) fn new(
        query: usize,
        control: usize,
        background: i32,
    ) -> Result<Self, ProtocolError> {
        if !(2..=128).contains(&background) {
            return Err(invalid());
        }
        let (native_stack, rust_stack) = native_stack_reservation()?;
        // Owned query/control threads explicitly request2MiB stacks. Reserve
        //4MiB each to include guards and per-thread allocator metadata.

        // RocksDB's process-shared periodic scheduler has one additional
        // std::thread. It is separate from HIGH/LOW execution capacity.
        let bytes = query
            .checked_add(control)
            .and_then(|n| n.checked_mul(rust_stack))
            .and_then(|n| n.checked_add((background as usize + 1).checked_mul(native_stack)?))
            .ok_or_else(invalid)?;
        Ok(Self {
            query_threads: query,
            control_threads: control,
            maintenance_high_threads: 1,
            maintenance_low_threads: background as usize - 1,
            housekeeping_timer_threads: 1,
            query_control_stack_bytes_per_thread: rust_stack,
            native_stack_bytes_per_thread: native_stack,
            total_stack_reservation_bytes: bytes,
        })
    }
}

pub(super) fn validate_thread_defaults(budget: NativeThreadBudget) -> Result<(), ProtocolError> {
    let (native, rust) = native_stack_reservation()?;
    if native > budget.native_stack_bytes_per_thread
        || rust > budget.query_control_stack_bytes_per_thread
    {
        return Err(invalid());
    }
    Ok(())
}

#[cfg(unix)]
fn native_stack_reservation() -> Result<(usize, usize), ProtocolError> {
    // C++ std::thread uses the process pthread defaults. Query them rather than
    // assuming Rust's smaller explicitly configured worker stack applies.
    let mut attributes = std::mem::MaybeUninit::<libc::pthread_attr_t>::uninit();
    if unsafe { libc::pthread_attr_init(attributes.as_mut_ptr()) } != 0 {
        return Err(invalid());
    }
    let mut attributes = unsafe { attributes.assume_init() };
    let mut stack = 0usize;
    let mut guard = 0usize;
    let stack_result = unsafe { libc::pthread_attr_getstacksize(&attributes, &mut stack) };
    let guard_result = unsafe { pthread_attr_getguardsize(&attributes, &mut guard) };
    let destroy_result = unsafe { libc::pthread_attr_destroy(&mut attributes) };
    if stack_result != 0 || guard_result != 0 || destroy_result != 0 || stack == 0 {
        return Err(invalid());
    }
    let native = stack
        .checked_add(guard)
        .and_then(|n| n.checked_add(64 << 10))
        .map(|n| n.max(8 << 20))
        .ok_or_else(invalid)?;
    let rust = (2usize << 20)
        .checked_add(guard)
        .and_then(|n| n.checked_add(64 << 10))
        .map(|n| n.max(4 << 20))
        .ok_or_else(invalid)?;
    Ok((native, rust))
}
#[cfg(not(unix))]
fn native_stack_reservation() -> Result<(usize, usize), ProtocolError> {
    Err(failure(
        ErrorCode::UnsupportedOperation,
        "native stack accounting is unsupported on this platform",
    ))
}
fn invalid() -> ProtocolError {
    failure(
        ErrorCode::ValidationError,
        "native thread stack reservation unavailable or overflowing",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn o05_large_native_pool_reserves_each_default_pthread_stack_and_shared_timer() {
        let report = NativeThreadBudget::new(1, 1, 128).unwrap();
        assert_eq!(report.maintenance_high_threads, 1);
        assert_eq!(report.maintenance_low_threads, 127);
        assert_eq!(report.housekeeping_timer_threads, 1);
        assert!(report.native_stack_bytes_per_thread >= 8 << 20);
        assert_eq!(
            report.total_stack_reservation_bytes,
            2 * report.query_control_stack_bytes_per_thread
                + 129 * report.native_stack_bytes_per_thread
        );
        assert!(NativeThreadBudget::new(usize::MAX, 1, 128).is_err());
    }
}
