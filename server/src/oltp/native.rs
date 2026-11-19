//! Explicit execution-pool and storage-thread reservations. These are logical
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
    /// `kv-flush`: one per worker, shared by every pool.
    pub flush_threads: usize,
    /// `kv-compact`: `background_threads - 1` per worker.
    pub compaction_threads: usize,
    /// `kv-journal`: one per open pool, at most `max_open_pools`.
    pub journal_threads: usize,
    pub query_control_stack_bytes_per_thread: usize,
    pub storage_stack_bytes_per_thread: usize,
    pub total_stack_reservation_bytes: usize,
}
impl NativeThreadBudget {
    /// A budget for one store (the legacy engine, deleted by NS8).
    pub(super) fn new(query: usize, control: usize, background: i32) -> Result<Self, ProtocolError> {
        Self::with_pools(query, control, background, 1)
    }
    pub(super) fn with_pools(
        query: usize,
        control: usize,
        background: i32,
        max_open_pools: usize,
    ) -> Result<Self, ProtocolError> {
        if !(2..=128).contains(&background) {
            return Err(invalid());
        }
        let rust_stack = rust_stack_reservation()?;
        // Every thread is a Rust thread: query and control request 2 MiB stacks,
        // and `nabedra-kv` threads take Rust's 2 MiB default. Reserve 4 MiB each
        // to include guards and per-thread allocator metadata.
        let storage = (background as usize)
            .checked_add(max_open_pools)
            .ok_or_else(invalid)?;
        let bytes = query
            .checked_add(control)
            .and_then(|n| n.checked_add(storage))
            .and_then(|n| n.checked_mul(rust_stack))
            .ok_or_else(invalid)?;
        Ok(Self {
            query_threads: query,
            control_threads: control,
            flush_threads: 1,
            compaction_threads: background as usize - 1,
            journal_threads: max_open_pools,
            query_control_stack_bytes_per_thread: rust_stack,
            storage_stack_bytes_per_thread: rust_stack,
            total_stack_reservation_bytes: bytes,
        })
    }
}

pub(super) fn validate_thread_defaults(budget: NativeThreadBudget) -> Result<(), ProtocolError> {
    let rust = rust_stack_reservation()?;
    if rust > budget.query_control_stack_bytes_per_thread
        || rust > budget.storage_stack_bytes_per_thread
    {
        return Err(invalid());
    }
    Ok(())
}

#[cfg(unix)]
fn rust_stack_reservation() -> Result<usize, ProtocolError> {
    // The guard size comes from the process pthread defaults, which Rust's
    // spawned threads also use.
    let mut attributes = std::mem::MaybeUninit::<libc::pthread_attr_t>::uninit();
    if unsafe { libc::pthread_attr_init(attributes.as_mut_ptr()) } != 0 {
        return Err(invalid());
    }
    let mut attributes = unsafe { attributes.assume_init() };
    let mut guard = 0usize;
    let guard_result = unsafe { pthread_attr_getguardsize(&attributes, &mut guard) };
    let destroy_result = unsafe { libc::pthread_attr_destroy(&mut attributes) };
    if guard_result != 0 || destroy_result != 0 {
        return Err(invalid());
    }
    (2usize << 20)
        .checked_add(guard)
        .and_then(|n| n.checked_add(64 << 10))
        .map(|n| n.max(4 << 20))
        .ok_or_else(invalid)
}
#[cfg(not(unix))]
fn rust_stack_reservation() -> Result<usize, ProtocolError> {
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
    fn o05_storage_threads_reserve_one_stack_each_per_worker_and_pool() {
        let report = NativeThreadBudget::with_pools(1, 1, 128, 8).unwrap();
        assert_eq!(report.flush_threads, 1);
        assert_eq!(report.compaction_threads, 127);
        assert_eq!(report.journal_threads, 8);
        assert!(report.storage_stack_bytes_per_thread >= 4 << 20);
        assert_eq!(
            report.total_stack_reservation_bytes,
            2 * report.query_control_stack_bytes_per_thread
                + (128 + 8) * report.storage_stack_bytes_per_thread
        );
        assert!(NativeThreadBudget::with_pools(usize::MAX, 1, 128, 8).is_err());
        assert!(NativeThreadBudget::with_pools(1, 1, 1, 8).is_err());
    }
}
