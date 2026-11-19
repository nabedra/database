//! OLTP storage: typed group stores in shared native pools, MVCC, backups and
//! the shared helpers they use. The standalone RF1 engine has been removed;
//! every runtime opens pools through `WorkerRuntime`.
mod codec;
#[cfg(test)]
mod migration_acceptance;
mod native;
pub use native::NativeThreadBudget;
pub(crate) mod backup;
pub mod mvcc;
pub mod pool;
mod query;

use crate::protocol::{ErrorCode, Outcome, ProtocolError};
use rocksdb::{Env, Options, WriteOptions};
use std::sync::Mutex;

// RocksDB's default Env is process-global even when its Rust wrapper is new.
// Configure it once; another store may share but cannot increase this budget.
// NS6 moves this and the three helpers below into the pool's storage layer;
// the lead deletes them with the `rocksdb` dependency.
static NATIVE_ENV: Mutex<Option<(i32, Env)>> = Mutex::new(None);

// The pinned native GetBGJobLimits forces HIGH>=1 and LOW>=1. Explicit
// compatibility limits are required to keep HIGH1 when the total exceeds4.
#[allow(deprecated)]
fn configure_native_jobs(options: &mut Options, threads: i32) {
    options.set_max_background_jobs(threads);
    options.set_max_background_flushes(1);
    options.set_max_background_compactions(threads - 1);
    options.set_max_subcompactions(1);
}

fn native_environment(threads: i32) -> Result<Env, ProtocolError> {
    let mut shared = NATIVE_ENV.lock().map_err(|_| {
        failure(
            ErrorCode::StorageUnavailable,
            "native environment lock unavailable",
        )
    })?;
    if let Some((configured, env)) = &*shared {
        if *configured != threads {
            return Err(failure(
                ErrorCode::ValidationError,
                "all local pools must share the existing native background thread budget",
            ));
        }
        return Ok(env.clone());
    }
    let mut env = Env::new().map_err(storage_error)?;
    env.set_high_priority_background_threads(1);
    env.set_bottom_priority_background_threads(0);
    env.set_low_priority_background_threads(threads - 1);
    *shared = Some((threads, env.clone()));
    Ok(env)
}

fn sync_options() -> WriteOptions {
    let mut options = WriteOptions::default();
    options.set_sync(true);
    options.disable_wal(false);
    options
}
pub(crate) fn failure(code: ErrorCode, message: &str) -> ProtocolError {
    ProtocolError::new(code, message, Outcome::NotApplied)
}
pub(crate) fn storage_error(_: rocksdb::Error) -> ProtocolError {
    failure(
        ErrorCode::StorageUnavailable,
        "OLTP storage unavailable or corrupt",
    )
}

pub(crate) fn valid_database(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name.as_bytes()[0].is_ascii_alphanumeric()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_.-".contains(&byte))
}

fn recovery_unknown() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::OutcomeUnknown,
        "OLTP storage requires reopening after an uncertain native write",
        Outcome::Unknown,
    )
}
