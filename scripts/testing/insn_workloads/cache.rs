// Included inside cache::standalone in each measured checkout.
pub async fn run_insn_cache() -> u64 {
    use super::{Backend, EvictionPolicy, Response, Standalone, TurnLimits};
    use crate::cache::Command;
    use bytes::Bytes;

    const OPERATIONS: u64 = 8192;
    let backend = Standalone::new(
        1 << 20,
        EvictionPolicy::NoEviction,
        1,
        TurnLimits::default(),
    );
    for _ in 0..OPERATIONS {
        let frame = [b"SET".as_slice(), b"key", b"value", b"PX", b"3600000"]
            .map(Bytes::from_static)
            .to_vec();
        let Command::Data(command) = Command::parse(frame).unwrap() else {
            panic!("expected a data command");
        };
        assert!(
            matches!(backend.execute(command).await, Response::Simple(value) if value == b"OK"[..])
        );
    }
    OPERATIONS
}
