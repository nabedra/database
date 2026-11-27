//! Admission for direct requests (plan section 6 step 1, D12).
//!
//! A request takes one in-flight slot and charges its working set against
//! the worker's existing memory budget before anything else runs. Both are
//! try-only: when either is full the leader answers a typed `429 overloaded`
//! with `not_applied`, so the caller knows nothing executed. There is no
//! admission queue; a request waiting on a latch still holds its slot, so
//! the number of waiting requests is bounded by `max_in_flight`.
use crate::{
    oltp::pool::{ByteClass, ScratchLease, WorkerSharedResources},
    oltp_replication::service_rpc::direct::DirectResponse,
};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Working-set charge per request beyond the body: decoded rows, index
/// keys, the encoded entry and its reply.
pub(crate) const REQUEST_OVERHEAD_BYTES: usize = 64 << 10;

/// Fixed charge per concurrent row of a `*_many` group: its task state, key
/// and small proposal fields.
const ROW_OVERHEAD_BYTES: usize = 8 << 10;

/// The charge for a body of `body_bytes`: twice the body (its decoded form
/// and the encoded entry) plus a fixed overhead.
pub(crate) fn working_bytes(body_bytes: usize) -> usize {
    body_bytes
        .saturating_mul(2)
        .saturating_add(REQUEST_OVERHEAD_BYTES)
}

pub(crate) struct DirectAdmission {
    resources: Arc<WorkerSharedResources>,
    slots: Arc<Semaphore>,
    max_in_flight: usize,
}

/// Held for the whole request. Dropping it returns the slot and the bytes.
pub(crate) struct Admitted {
    _slot: OwnedSemaphorePermit,
    _bytes: ScratchLease,
}

impl DirectAdmission {
    pub(crate) fn new(resources: Arc<WorkerSharedResources>, max_in_flight: usize) -> Self {
        let max_in_flight = max_in_flight.max(1);
        Self {
            resources,
            slots: Arc::new(Semaphore::new(max_in_flight)),
            max_in_flight,
        }
    }

    /// Takes a slot and charges the working set, or returns the typed 429.
    pub(crate) fn try_admit(&self, body_bytes: usize) -> Result<Admitted, DirectResponse> {
        let slot = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| DirectResponse::overloaded("direct in-flight budget is full"))?;
        let bytes = self
            .resources
            .reserve_scratch(ByteClass::Data, working_bytes(body_bytes))
            .map_err(|_| DirectResponse::overloaded("worker memory budget is full"))?;
        Ok(Admitted {
            _slot: slot,
            _bytes: bytes,
        })
    }

    /// Charges what `rows` concurrent rows of one admitted group need beyond
    /// the group's own working set, which covers their bytes.
    pub(crate) fn try_rows(&self, rows: usize) -> Result<ScratchLease, DirectResponse> {
        self.resources
            .reserve_scratch(ByteClass::Data, rows.saturating_mul(ROW_OVERHEAD_BYTES))
            .map_err(|_| DirectResponse::overloaded("worker memory budget is full"))
    }

    pub(crate) fn in_flight(&self) -> usize {
        self.max_in_flight - self.slots.available_permits()
    }

    pub(crate) fn max_in_flight(&self) -> usize {
        self.max_in_flight
    }
}
