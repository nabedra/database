//! Admission accounting for the filesystem behind a pool's journal.
use crate::protocol::{ErrorCode, Outcome, ProtocolError};
use nabedra_kv::journal::format::{BLOCK, RECORD_HEADER_LEN};
use serde::Serialize;
use std::{
    collections::HashMap,
    fs::File,
    path::Path,
    sync::{Arc, Mutex, OnceLock, Weak},
};

const REFUSED: &str = "OLTP filesystem has insufficient unreserved storage";

fn refused() -> ProtocolError {
    ProtocolError::new(ErrorCode::StorageUnavailable, REFUSED, Outcome::NotApplied)
}

pub(crate) fn is_disk_reservation_refusal(error: &ProtocolError) -> bool {
    error.code == ErrorCode::StorageUnavailable
        && error.outcome == Outcome::NotApplied
        && error.message.starts_with(REFUSED)
}

#[derive(Default)]
struct Budget {
    reserved_bytes: u64,
    reservations: u64,
    protected_bytes: u64,
}

#[derive(Default)]
struct Filesystem(Mutex<Budget>);

impl Filesystem {
    fn reserve(
        self: &Arc<Self>,
        bytes: u64,
        sample: impl FnOnce() -> std::io::Result<u64>,
    ) -> Result<DiskReservation, ProtocolError> {
        let mut budget = self.0.lock().unwrap_or_else(|p| p.into_inner());
        // Read free space under the same lock as admission and release. A
        // sample taken before a release could miss the completed write.
        let available = sample().map_err(|_| refused())?;
        if bytes
            > available
                .saturating_sub(budget.protected_bytes)
                .saturating_sub(budget.reserved_bytes)
        {
            return Err(refused());
        }
        budget.reserved_bytes += bytes;
        budget.reservations += 1;
        Ok(DiskReservation {
            budget: self.clone(),
            bytes,
        })
    }
}

pub(super) struct PoolDisk {
    device: u64,
    directory: File,
    budget: Arc<Filesystem>,
    segment_bytes: u64,
}

// The journal can allocate a whole spare before a roll. Hold two segments per
// open pool for this allocation granularity and two for replication, votes,
// decisions and resolution. Pools on the device contribute independently.
const ROLLOVER_SEGMENTS_PER_POOL: u64 = 2;
const SETTLEMENT_SEGMENTS_PER_POOL: u64 = 2;
const PROTECTED_SEGMENTS_PER_POOL: u64 = ROLLOVER_SEGMENTS_PER_POOL + SETTLEMENT_SEGMENTS_PER_POOL;

fn proposal_charge(encoded_bytes: usize, apply_items: usize) -> u64 {
    // Append stores the encoded command once. Apply stores each key up to
    // four times (head, version and, for the anchor, dedup and expiry). A
    // Prepare item can instead store an intent, head and ordinal key copy.
    // The native key encoding adds fewer variable bytes than JSON escaping.
    // Allow 1 KiB per item for record framing, scoped keys, and metadata
    // repeated per write or direct member, including Prepare intent/audit.
    // Another 2 KiB covers entry, log and applied-position metadata.
    let records = (encoded_bytes as u64)
        .saturating_mul(5)
        .saturating_add((apply_items as u64).saturating_mul(1024))
        .saturating_add(2048);
    // Allow a header and worst-case sync pad for each of append and apply.
    // Charge twice the total since the next record can leave up to its own
    // length unused at the end of a segment. GC1 measured ~4 KiB journal
    // bytes per serial direct update with a synced append and deferred apply.
    let padding = 2 * (BLOCK + 2 * RECORD_HEADER_LEN as u64);
    records.saturating_add(padding).saturating_mul(2)
}

impl PoolDisk {
    pub(super) fn device(&self) -> u64 {
        self.device
    }

    pub(super) fn open(path: &Path, segment_bytes: u64) -> Result<Self, ProtocolError> {
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;

        let directory = File::open(path).map_err(|_| refused())?;
        #[cfg(unix)]
        let device = directory.metadata().map_err(|_| refused())?.dev();
        #[cfg(not(unix))]
        let device = 0;
        static BUDGETS: OnceLock<Mutex<HashMap<u64, Weak<Filesystem>>>> = OnceLock::new();
        let mut budgets = BUDGETS
            .get_or_init(Mutex::default)
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        budgets.retain(|_, budget| budget.strong_count() != 0);
        let slot = budgets.entry(device).or_default();
        let budget = slot.upgrade().unwrap_or_else(|| {
            let budget = Arc::new(Filesystem::default());
            *slot = Arc::downgrade(&budget);
            budget
        });
        budget
            .0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .protected_bytes += segment_bytes * PROTECTED_SEGMENTS_PER_POOL;
        Ok(Self {
            device,
            directory,
            budget,
            segment_bytes,
        })
    }

    pub(super) fn reserve(
        &self,
        encoded_bytes: usize,
        apply_items: usize,
    ) -> Result<DiskReservation, ProtocolError> {
        self.budget
            .reserve(proposal_charge(encoded_bytes, apply_items), || {
                available_bytes(&self.directory)
            })
    }

    pub(super) fn stats(&self) -> DiskReservationStats {
        let budget = self.budget.0.lock().unwrap_or_else(|p| p.into_inner());
        DiskReservationStats {
            device: self.device,
            available_bytes: available_bytes(&self.directory).ok(),
            protected_bytes: budget.protected_bytes,
            reserved_bytes: budget.reserved_bytes,
            reservations: budget.reservations,
        }
    }
}

impl Drop for PoolDisk {
    fn drop(&mut self) {
        self.budget
            .0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .protected_bytes -= self.segment_bytes * PROTECTED_SEGMENTS_PER_POOL;
    }
}

pub(crate) struct DiskReservation {
    budget: Arc<Filesystem>,
    bytes: u64,
}

impl Drop for DiskReservation {
    fn drop(&mut self) {
        let mut budget = self.budget.0.lock().unwrap_or_else(|p| p.into_inner());
        budget.reserved_bytes -= self.bytes;
        budget.reservations -= 1;
    }
}

#[derive(Clone, Copy, Serialize)]
pub(crate) struct DiskReservationStats {
    device: u64,
    available_bytes: Option<u64>,
    protected_bytes: u64,
    reserved_bytes: u64,
    reservations: u64,
}

#[cfg(unix)]
fn available_bytes(directory: &File) -> std::io::Result<u64> {
    use std::os::fd::AsRawFd;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: fstatvfs fills the buffer on success and borrows the open fd.
    if unsafe { libc::fstatvfs(directory.as_raw_fd(), stats.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    let stats = unsafe { stats.assume_init() };
    #[allow(clippy::unnecessary_cast)]
    Ok((stats.f_bavail as u64).saturating_mul(stats.f_frsize as u64))
}

#[cfg(not(unix))]
fn available_bytes(_: &File) -> std::io::Result<u64> {
    Err(std::io::ErrorKind::Unsupported.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;

    #[test]
    fn o08_o13_o21_same_filesystem_reserves_once_and_releases() {
        let directory = std::env::temp_dir();
        let first = PoolDisk::open(&directory, 4096).unwrap();
        let second = PoolDisk::open(&directory, 4096).unwrap();
        assert!(Arc::ptr_eq(&first.budget, &second.budget));
        let charge = proposal_charge(512, 2);
        let protected = first.stats().protected_bytes;
        let filesystem = Arc::new(Filesystem(Mutex::new(Budget {
            protected_bytes: protected,
            ..Budget::default()
        })));
        let gate = Barrier::new(2);
        std::thread::scope(|scope| {
            let reserve = || {
                gate.wait();
                filesystem.reserve(charge, || Ok(protected + charge + charge / 2))
            };
            let contender = scope.spawn(reserve);
            let a = reserve();
            let b = contender.join().unwrap();
            let (winner, loser) = match (a, b) {
                (Ok(winner), Err(loser)) | (Err(loser), Ok(winner)) => (winner, loser),
                _ => panic!("exactly one proposal must reserve the measured headroom"),
            };
            assert_eq!(loser, refused());
            drop(winner);
        });
        assert!(
            filesystem
                .reserve(charge, || Ok(protected + charge))
                .is_ok()
        );
        assert_eq!(
            filesystem.reserve(charge, || Ok(protected)).err(),
            Some(refused())
        );
    }
}
