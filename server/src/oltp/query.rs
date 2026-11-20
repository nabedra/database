//! Working-memory charges and bounded serialization shared by the MVCC planner.
use super::failure;
use crate::protocol::{ErrorCode, TableDescriptor};
use serde::Serialize;
use std::io::{self, Write};

/// Conservative charge for decoded BTreeMap nodes, strings, JSON and row data.
/// This bounds task-owned row state, not the storage engine's native allocations.
pub(crate) fn row_charge(
    table: &TableDescriptor,
    encoded: usize,
) -> Result<usize, crate::protocol::ProtocolError> {
    encoded
        .checked_mul(8)
        .and_then(|n| n.checked_add(table.schema.len().checked_mul(256)?))
        .ok_or_else(|| failure(ErrorCode::LimitExceeded, "row working memory overflow"))
}

pub(crate) fn check_body(
    value: &impl Serialize,
    limit: usize,
) -> Result<usize, crate::protocol::ProtocolError> {
    let mut writer = BudgetWriter { written: 0, limit };
    serde_json::to_writer(&mut writer, value).map_err(|error| {
        failure(
            if error.is_io() {
                ErrorCode::LimitExceeded
            } else {
                ErrorCode::InternalError
            },
            "serialized value exceeds its budget or cannot be encoded",
        )
    })?;
    Ok(writer.written)
}

struct BudgetWriter {
    written: usize,
    limit: usize,
}
impl Write for BudgetWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit - self.written {
            return Err(io::Error::other("serialization budget"));
        }
        self.written += bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
