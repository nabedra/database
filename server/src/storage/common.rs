//! Storage identity and path validation shared with request records.
/// Sanitized failures. Never contains SDK diagnostics or configuration values.
/// Ambiguous means a PUT may have succeeded; callers must reconcile, not retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageError {
    Configuration,
    InvalidPath,
    NotFound,
    Conflict,
    Unavailable,
    Ambiguous,
    LimitExceeded,
    InvalidCatalog,
    UnsupportedCatalog,
}
impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Configuration => "Storage configuration is missing or invalid",
            Self::InvalidPath => "Invalid storage path",
            Self::NotFound => "Object not found",
            Self::Conflict => "Conditional write conflict",
            Self::Unavailable => "Storage is unavailable",
            Self::Ambiguous => "Storage write outcome is unknown",
            Self::LimitExceeded => "Storage object exceeds byte limit",
            Self::InvalidCatalog => "Invalid catalog",
            Self::UnsupportedCatalog => "Unsupported catalog version",
        })
    }
}
impl std::error::Error for StorageError {}

#[derive(Debug, Clone)]
pub struct Namespace(String);
impl Namespace {
    pub fn new(prefix: &str) -> Result<Self, StorageError> {
        let prefix = prefix.trim_end_matches('/');
        safe_path(prefix)?;
        Ok(Self(format!("{prefix}/")))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
pub(super) fn safe_path(path: &str) -> Result<(), StorageError> {
    if path.is_empty()
        || path.contains(['\\', ':', '%', '?', '#'])
        || path.chars().any(char::is_control)
        || path
            .split('/')
            .any(|p| p.is_empty() || p == "." || p == "..")
    {
        return Err(StorageError::InvalidPath);
    }
    Ok(())
}

/// Attempts of one idempotent read, including the first (OLAP-9).
pub(crate) const READ_ATTEMPTS: u32 = 3;
const READ_RETRY_BASE: std::time::Duration = std::time::Duration::from_millis(50);
const READ_RETRY_CAP: std::time::Duration = std::time::Duration::from_millis(400);

/// Full jitter: a delay drawn uniformly from `[0, min(cap, base * 2^(retry - 1))]`,
/// so that clients that failed together do not retry together.
pub(crate) fn jitter(
    retry: u32,
    base: std::time::Duration,
    cap: std::time::Duration,
) -> std::time::Duration {
    let ceiling = base
        .saturating_mul(1 << retry.saturating_sub(1).min(16))
        .min(cap);
    let nanos = u64::try_from(ceiling.as_nanos()).unwrap_or(u64::MAX);
    if nanos == 0 {
        return std::time::Duration::ZERO;
    }
    std::time::Duration::from_nanos(nabedra_rt::ids::random_u64() % nanos.saturating_add(1))
}

/// Run an idempotent read up to `READ_ATTEMPTS` times while it fails with an
/// error that `transient` accepts, sleeping a jittered delay between tries.
/// Writes are never retried here: a PUT that may have applied is `Ambiguous`.
pub(crate) async fn retry_reads<T, E, F, Fut>(
    mut read: F,
    transient: impl Fn(&E) -> bool,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    let mut attempt = 1;
    loop {
        match read().await {
            Err(error) if attempt < READ_ATTEMPTS && transient(&error) => {
                nabedra_rt::time::sleep(jitter(attempt, READ_RETRY_BASE, READ_RETRY_CAP)).await;
                attempt += 1;
            }
            result => return result,
        }
    }
}

#[cfg(test)]
mod retry_tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    async fn burst(failures: u32, transient: bool) -> (Result<(), ()>, u32) {
        let calls = AtomicU32::new(0);
        let result = retry_reads(
            || async {
                if calls.fetch_add(1, Ordering::SeqCst) < failures {
                    Err(transient)
                } else {
                    Ok(())
                }
            },
            |transient| *transient,
        )
        .await
        .map_err(drop);
        (result, calls.load(Ordering::SeqCst))
    }

    #[tokio::test(start_paused = true)]
    async fn olap_9_reads_retry_a_bounded_number_of_times() {
        assert_eq!(burst(0, true).await, (Ok(()), 1));
        assert_eq!(burst(2, true).await, (Ok(()), 3));
        // A storm costs at most READ_ATTEMPTS requests per read, not more.
        assert_eq!(burst(100, true).await, (Err(()), READ_ATTEMPTS));
        // Denials, absence and other definite answers are not retried.
        assert_eq!(burst(100, false).await, (Err(()), 1));
    }

    #[test]
    fn olap_9_jitter_stays_within_its_exponential_ceiling() {
        let base = std::time::Duration::from_millis(50);
        let cap = std::time::Duration::from_millis(400);
        let mut distinct = std::collections::BTreeSet::new();
        for retry in 1..=6 {
            let ceiling = (base * (1 << (retry - 1))).min(cap);
            for _ in 0..200 {
                let delay = jitter(retry, base, cap);
                assert!(delay <= ceiling, "retry {retry}: {delay:?} > {ceiling:?}");
                distinct.insert(delay);
            }
        }
        assert!(distinct.len() > 100, "delays are spread, not lockstep");
        assert_eq!(
            jitter(1, std::time::Duration::ZERO, cap),
            std::time::Duration::ZERO
        );
    }
}
