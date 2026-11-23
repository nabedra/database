use nabedra_protocol::protocol::{ErrorCode, Outcome, ProtocolError};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Pool format 2: a `nabedra-kv` store in `kv/`. Format 1 (the previous engine) is
/// refused (Q1).
pub const POOL_FORMAT_VERSION: u32 = 2;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolIdentity {
    pub engine: String,
    pub pool_format_version: u32,
    pub cluster_id: Uuid,
    pub worker_id: Uuid,
    pub pool_id: Uuid,
}
impl PoolIdentity {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.engine != "oltp"
            || self.pool_format_version != POOL_FORMAT_VERSION
            || self.cluster_id.is_nil()
            || self.worker_id.is_nil()
            || self.pool_id.is_nil()
        {
            return Err(ProtocolError::new(
                ErrorCode::StorageUnavailable,
                "invalid typed OLTP pool identity",
                Outcome::NotApplied,
            ));
        }
        Ok(())
    }
}
