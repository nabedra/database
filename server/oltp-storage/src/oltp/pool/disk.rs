//! Admission accounting for the filesystem behind a pool's journal.
use crate::protocol::{ErrorCode, Outcome, ProtocolError};
use nabedra_kv::journal::format::{BLOCK, RECORD_HEADER_LEN};
use nabedra_rt::fs::os::File;
use serde::Serialize;
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex, OnceLock, Weak},
};

const REFUSED: &str = "OLTP filesystem has insufficient unreserved storage";

fn refused() -> ProtocolError {
    ProtocolError::new(ErrorCode::StorageUnavailable, REFUSED, Outcome::NotApplied)
}

pub fn is_disk_reservation_refusal(error: &ProtocolError) -> bool {
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

struct Filesystem {
    accounting: Mutex<Budget>,
    // Reserve the allocation used as the registry key as long as a pool
    // or reservation owns this budget, even after the World drops.
    #[cfg(feature = "sim")]
    _world: Option<Weak<nabedra_rt::sim::World>>,
}

impl Filesystem {
    fn reserve(
        self: &Arc<Self>,
        bytes: u64,
        sample: impl FnOnce() -> std::io::Result<u64>,
    ) -> Result<DiskReservation, ProtocolError> {
        let mut budget = self.accounting.lock().unwrap_or_else(|p| p.into_inner());
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

#[cfg(not(feature = "sim"))]
type BudgetKey = u64;
#[cfg(feature = "sim")]
type BudgetKey = (usize, Option<u64>, u64);

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
        let directory = File::open(path).map_err(|_| refused())?;
        #[cfg(unix)]
        let device = directory.metadata().map_err(|_| refused())?.dev();
        #[cfg(not(unix))]
        let device = 0;
        static BUDGETS: OnceLock<Mutex<HashMap<BudgetKey, Weak<Filesystem>>>> = OnceLock::new();
        let mut budgets = BUDGETS
            .get_or_init(Mutex::default)
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        budgets.retain(|_, budget| budget.strong_count() != 0);
        // Simulated nodes and parallel worlds may share the host device.
        #[cfg(feature = "sim")]
        let world = nabedra_rt::sim::world();
        #[cfg(feature = "sim")]
        let key = (
            world
                .as_ref()
                .map_or(0, |world| Arc::as_ptr(world) as usize),
            nabedra_rt::sim::current_node(),
            device,
        );
        #[cfg(not(feature = "sim"))]
        let key = device;
        let slot = budgets.entry(key).or_default();
        let budget = slot.upgrade().unwrap_or_else(|| {
            let budget = Arc::new(Filesystem {
                accounting: Mutex::default(),
                #[cfg(feature = "sim")]
                _world: world.as_ref().map(Arc::downgrade),
            });
            *slot = Arc::downgrade(&budget);
            budget
        });
        budget
            .accounting
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
        let budget = self
            .budget
            .accounting
            .lock()
            .unwrap_or_else(|p| p.into_inner());
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
            .accounting
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .protected_bytes -= self.segment_bytes * PROTECTED_SEGMENTS_PER_POOL;
    }
}

pub struct DiskReservation {
    budget: Arc<Filesystem>,
    bytes: u64,
}

impl Drop for DiskReservation {
    fn drop(&mut self) {
        let mut budget = self
            .budget
            .accounting
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        budget.reserved_bytes -= self.bytes;
        budget.reservations -= 1;
    }
}

#[derive(Clone, Copy, Serialize)]
pub struct DiskReservationStats {
    device: u64,
    available_bytes: Option<u64>,
    protected_bytes: u64,
    reserved_bytes: u64,
    reservations: u64,
}

#[cfg(unix)]
fn available_bytes(directory: &File) -> std::io::Result<u64> {
    let stats = directory.statvfs()?;
    Ok(stats.available.saturating_mul(stats.block_size))
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
        let filesystem = Arc::new(Filesystem {
            accounting: Mutex::new(Budget {
                protected_bytes: protected,
                ..Budget::default()
            }),
            #[cfg(feature = "sim")]
            _world: None,
        });
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

    /// P10-DET: simulated nodes in one process never share a disk budget,
    /// while pools of one node on one filesystem still do.
    #[cfg(feature = "sim")]
    #[test]
    fn sim_nodes_have_separate_disk_budgets() {
        let directory = std::env::temp_dir();
        nabedra_rt::sim::run(7, |world| async move {
            world.add_node(1, nabedra_rt::sim::NodeClock::default());
            world.add_node(2, nabedra_rt::sim::NodeClock::default());
            let open = |node| {
                let directory = directory.clone();
                world.spawn_node(
                    node,
                    async move { PoolDisk::open(&directory, 4096).unwrap() },
                )
            };
            let (first, second, other) = (
                open(1).await.unwrap(),
                open(1).await.unwrap(),
                open(2).await.unwrap(),
            );
            assert!(Arc::ptr_eq(&first.budget, &second.budget));
            assert!(!Arc::ptr_eq(&first.budget, &other.budget));
            assert_eq!(
                first.stats().protected_bytes,
                2 * 4096 * PROTECTED_SEGMENTS_PER_POOL
            );
            assert_eq!(
                other.stats().protected_bytes,
                4096 * PROTECTED_SEGMENTS_PER_POOL
            );
        });
    }

    #[cfg(feature = "sim")]
    #[test]
    fn sim_disk_budget_keeps_its_world_key_reserved() {
        let open = || {
            nabedra_rt::sim::run(7, |world| async move {
                world.add_node(1, nabedra_rt::sim::NodeClock::default());
                world
                    .spawn_node(1, async {
                        PoolDisk::open(&std::env::temp_dir(), 4096).unwrap()
                    })
                    .await
                    .unwrap()
            })
        };
        let first = open();
        let world = first.budget._world.as_ref().expect("budget owns its key");
        assert!(world.upgrade().is_none(), "budget must let the world drop");
        let next = open();
        assert_ne!(
            world.as_ptr(),
            next.budget._world.as_ref().unwrap().as_ptr()
        );
        assert!(!Arc::ptr_eq(&first.budget, &next.budget));
    }
}
