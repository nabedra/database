//! One HTTP admission/receipt surface for hosted worker runtimes.
//! Authentication, wire validation and response ownership remain in http.rs.
use super::application::PointRequest;
use crate::{
    executor::Executor,
    oltp_replication::multiplex::EndpointFuture,
    protocol::{ErrorCode, Outcome, ProtocolError},
    request_queue::{Receipt, RequestRecord},
    response::EncodedResponse,
    scheduling::{MemoryCharge, ResourceScheduler},
};
use std::sync::Arc;
use uuid::Uuid;

/// Decoded receipt heap remains charged through HTTP serialization/observation.
pub(crate) struct OwnedReceipt {
    pub receipt: Receipt,
    pub _owner: Option<Arc<crate::oltp::pool::ScratchLease>>,
}
impl From<Receipt> for OwnedReceipt {
    fn from(receipt: Receipt) -> Self {
        Self {
            receipt,
            _owner: None,
        }
    }
}
impl std::ops::Deref for OwnedReceipt {
    type Target = Receipt;
    fn deref(&self) -> &Receipt {
        &self.receipt
    }
}

/// Direct requests one HTTP runtime admits at once (PERF1). A direct request
/// spends its time waiting for its leader's commit, not on this process's
/// CPU, so the queued lane's two workers capped a 12 ms commit near 170/s and
/// turned every rise in leader latency into 429s (BENCH2). Memory stays bounded
/// by the per-request charge of twice the body; requests to one leader also
/// wait for that leader's lane slots in the transport.
pub(crate) const DIRECT_INGRESS_SLOTS: usize = 64;

pub(crate) trait HttpRuntime: Send + Sync + 'static {
    /// Admission for queued requests: queue installs, receipts, the queued
    /// remainder of a direct fallback.
    fn ingress(&self) -> &Executor;
    /// Admission for requests the direct gateway routes (`DIRECT_INGRESS_SLOTS`).
    /// One that goes queued after routing takes an `ingress` slot as well.
    fn direct_ingress(&self) -> &Executor {
        self.ingress()
    }
    /// The direct-path gateway (T6), if this runtime has one. It routes
    /// nothing while `data.direct.v1` is inactive.
    fn direct_gateway(&self) -> Option<Arc<super::application::direct::DirectGateway>> {
        None
    }
    fn scheduler(&self) -> ResourceScheduler;
    fn scheduler_node_id(&self) -> u64;
    fn submit(
        self: Arc<Self>,
        body: Vec<u8>,
        parsing_charge: MemoryCharge,
        point: Option<PointRequest>,
    ) -> EndpointFuture<'static, RequestRecord>;
    fn receipt(&self, run: Uuid, id: Uuid) -> EndpointFuture<'_, OwnedReceipt>;
    fn wait<'a>(&'a self, record: &'a RequestRecord) -> EndpointFuture<'a, EncodedResponse>;
    fn health(&self) -> EndpointFuture<'_, EncodedResponse>;
    fn admin_memory(&self) -> Result<Arc<crate::oltp::pool::ScratchLease>, ProtocolError> {
        Err(ProtocolError::new(
            ErrorCode::UnsupportedOperation,
            "administration is unavailable",
            Outcome::NotApplied,
        ))
    }
    fn administer(self: Arc<Self>, _body: Vec<u8>) -> EndpointFuture<'static, EncodedResponse> {
        Box::pin(async {
            Err(ProtocolError::new(
                ErrorCode::UnsupportedOperation,
                "administration is unavailable",
                Outcome::NotApplied,
            ))
        })
    }
    fn stats(&self) -> EndpointFuture<'_, EncodedResponse> {
        Box::pin(async {
            Err(ProtocolError::new(
                ErrorCode::UnsupportedOperation,
                "runtime statistics are unavailable",
                Outcome::NotApplied,
            ))
        })
    }
}
