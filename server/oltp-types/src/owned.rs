use crate::types::Digest;
use nabedra_protocol::protocol::{ErrorCode, Outcome, ProtocolError};
use serde::Serialize;

/// Hash an already-owned bounded DTO without allocating a second encoded
/// buffer. Call this from the owned native codec/apply job.
#[inline]
pub fn serialized_digest(value: &impl Serialize) -> Result<Digest, ProtocolError> {
    use sha2::{Digest as _, Sha256};
    struct Writer(Sha256);
    impl std::io::Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer(Sha256::new());
    serde_json::to_writer(&mut writer, value).map_err(|_| {
        ProtocolError::new(
            ErrorCode::ValidationError,
            "invalid typed command JSON",
            Outcome::NotApplied,
        )
    })?;
    Ok(Digest::from_bytes(writer.0.finalize().into()))
}
