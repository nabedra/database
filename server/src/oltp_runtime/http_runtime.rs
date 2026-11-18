//! One HTTP admission/receipt surface for standalone and hosted worker runtimes.
//! Authentication, wire validation and response ownership remain in http.rs.
use super::LocalRuntime;
use crate::{
    executor::Executor,
    oltp_replication::multiplex::EndpointFuture,
    protocol::{ErrorCode, Outcome, ProtocolError},
    request_queue::{Receipt, RequestRecord},
    response::EncodedResponse,
    scheduling::ResourceScheduler,
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

pub(crate) trait HttpRuntime: Send + Sync + 'static {
    fn ingress(&self) -> &Executor;
    fn scheduler(&self) -> ResourceScheduler;
    fn scheduler_node_id(&self) -> u64;
    fn submit(self: Arc<Self>, body: Vec<u8>) -> EndpointFuture<'static, RequestRecord>;
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
impl HttpRuntime for LocalRuntime {
    fn ingress(&self) -> &Executor {
        &self.ingress
    }
    fn scheduler(&self) -> ResourceScheduler {
        self.scheduler.clone()
    }
    fn scheduler_node_id(&self) -> u64 {
        0
    }
    fn submit(self: Arc<Self>, body: Vec<u8>) -> EndpointFuture<'static, RequestRecord> {
        Box::pin(async move { LocalRuntime::submit(&self, body).await })
    }
    fn receipt(&self, run: Uuid, id: Uuid) -> EndpointFuture<'_, OwnedReceipt> {
        Box::pin(async move { LocalRuntime::receipt(self, run, id).await.map(Into::into) })
    }
    fn wait<'a>(&'a self, record: &'a RequestRecord) -> EndpointFuture<'a, EncodedResponse> {
        Box::pin(LocalRuntime::wait(self, record))
    }
    fn health(&self) -> EndpointFuture<'_, EncodedResponse> {
        Box::pin(async move {
            if self.engine.requires_recovery() {
                return Err(ProtocolError::new(
                    ErrorCode::StorageUnavailable,
                    "OLTP store requires authoritative reopen",
                    Outcome::Unknown,
                ));
            }
            if !self.accepting() {
                return Err(ProtocolError::new(
                    ErrorCode::StorageUnavailable,
                    "OLTP runtime is draining",
                    Outcome::NotApplied,
                ));
            }
            let manifest = self.engine.manifest();
            Ok(EncodedResponse { status: 200, body: serde_json::to_vec(&serde_json::json!({
                "status":"ready", "engine":manifest.engine.as_str(), "format_version":manifest.format_version,
                "topology":manifest.topology.as_str(), "replication_factor":1,
                "run_id":self.run_id(), "cluster_id":manifest.cluster_id,
                "native_thread_budget":self.engine.thread_budget(),
                "network_stack_bytes":"provided by hosting runtime; excluded from native_thread_budget"
            })).expect("fixed health JSON") })
        })
    }
}
