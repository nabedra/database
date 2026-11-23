//! Compatibility exports for worker-owned command payloads.
pub use nabedra_oltp_types::owned::{OwnedCommand, OwnedPayload};
pub(crate) use nabedra_oltp_types::owned::serialized_digest;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        oltp::{
            mvcc::CanonicalJson,
            pool::{ByteClass, ScratchLease, SharedResourceConfig, WorkerSharedResources},
        },
        protocol::ProtocolError,
    };
    use nabedra_resources::config::WorkerResources;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;
    #[derive(Serialize, Deserialize)]
    struct Payload {
        fixed: CanonicalJson,
    }
    impl OwnedPayload for Payload {
        fn heap_bytes(&self) -> Result<usize, ProtocolError> {
            self.fixed.heap_bytes()
        }
        fn attach_fields(&self, owner: Arc<ScratchLease>) -> Result<(), ProtocolError> {
            self.fixed.attach_charge(owner)
        }
        fn fields_attached(&self) -> bool {
            self.fixed.is_charged()
        }
    }
    fn shared() -> Arc<WorkerSharedResources> {
        WorkerSharedResources::new(
            1,
            WorkerResources {
                max_threads: 8,
                memory_limit_bytes: 512 << 20,
                task_memory_limit_bytes: 128 << 20,
                control_memory_reserve_bytes: 64 << 20,
            },
            SharedResourceConfig {
                background_threads: 2,
                ..Default::default()
            },
        )
        .unwrap()
    }
    #[test]
    fn f03_m02_cloned_command_and_extracted_json_keep_whole_decode_owner() {
        let shared = shared();
        let baseline = shared.scheduler().usage()[0].2;
        let input = shared
            .copy_bytes(br#"{"fixed":{"id":1,"text":"exact"}}"#)
            .unwrap();
        let command =
            OwnedCommand::<Payload>::decode_json(&input, &shared, ByteClass::Data, 1 << 20)
                .unwrap();
        let cloned = command.clone();
        let field = command.payload().unwrap().fixed.clone();
        let used = shared.scheduler().usage()[0].2;
        assert!(used > baseline);
        drop(command);
        assert_eq!(shared.scheduler().usage()[0].2, used);
        drop(cloned);
        assert_eq!(shared.scheduler().usage()[0].2, used);
        assert_eq!(field.as_str(), r#"{"id":1,"text":"exact"}"#);
        drop(input);
        assert_eq!(shared.buffer_bytes_used(ByteClass::Data), 0);
        assert!(shared.scheduler().usage()[0].2 > baseline);
        drop(field);
        assert_eq!(shared.scheduler().usage()[0].2, baseline);
        assert_eq!(shared.buffer_bytes_used(ByteClass::Data), 0);
    }
    #[test]
    fn m01_u02_unattached_and_undersized_decodes_fail_before_handoff() {
        let shared = shared();
        let input = shared.copy_bytes(br#"{"fixed":{"id":1}}"#).unwrap();
        let raw: OwnedCommand<Payload> = serde_json::from_slice(input.as_slice()).unwrap();
        assert!(raw.payload().is_err());
        assert!(serde_json::to_vec(&raw).is_err());
        let tiny = Arc::new(shared.reserve_scratch(ByteClass::Data, 1).unwrap());
        assert!(OwnedCommand::<Payload>::decode_with_owner(&input, tiny, 1).is_err());
        drop(raw);
        drop(input);
        assert_eq!(shared.buffer_bytes_used(ByteClass::Data), 0);
    }
}
