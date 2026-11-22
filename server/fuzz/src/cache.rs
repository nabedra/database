use bytes::{Bytes, BytesMut};
use nabedra_database_server::cache::{
    Command, CommandError, Decoder, FrameLimits, ProtocolError, Response, SLOT_COUNT, key_slot,
};

type Frames = Vec<Vec<Bytes>>;

pub fn fuzz_cache_resp(data: &[u8]) {
    let limits = FrameLimits::default();
    let whole = decode_stream(data, limits, data.len().max(1));
    assert_eq!(decode_stream(data, limits, 17), whole);
    for frame in whole.0 {
        let response = Response::Array(
            frame
                .iter()
                .cloned()
                .map(|arg| Response::Bulk(Some(arg)))
                .collect(),
        );
        let encoded = response
            .encode(limits.max_frame_bytes)
            .expect("decoded frame fits");
        assert_eq!(decode_stream(&encoded, limits, 1), (vec![frame], Ok(())));
    }

    // Hit size-limit branches even when libFuzzer has only short inputs.
    let small = FrameLimits {
        max_frame_bytes: 64,
        max_arguments: 8,
        max_bulk_bytes: 32,
    };
    assert_eq!(
        decode_stream(data, small, 1),
        decode_stream(data, small, data.len().max(1)),
    );
}

fn decode_stream(
    data: &[u8],
    limits: FrameLimits,
    width: usize,
) -> (Frames, Result<(), ProtocolError>) {
    let mut decoder = Decoder::new(limits);
    let mut buffer = BytesMut::new();
    let mut frames = Vec::new();
    for chunk in data.chunks(width) {
        buffer.extend_from_slice(chunk);
        loop {
            let before = buffer.len();
            match decoder.decode(&mut buffer) {
                Ok(Some(frame)) => {
                    assert!(buffer.len() < before, "decoder did not consume a frame");
                    frames.push(frame);
                }
                Ok(None) => break,
                Err(error) => return (frames, Err(error)),
            }
        }
    }
    (frames, decoder.eof(&buffer))
}

pub fn fuzz_cache_command(data: &[u8]) {
    let limits = FrameLimits::default();
    let Some((&selector, tail)) = data.split_first() else {
        check_command(Vec::new());
        return;
    };
    if selector == b'*' {
        for frame in decode_stream(data, limits, data.len()).0 {
            check_command(frame);
        }
    } else {
        // Select a supported command independently of its arguments so mutations
        // routinely reach the parser's command-specific branches.
        let name = COMMANDS[usize::from(selector) % COMMANDS.len()];
        let mut frame = vec![Bytes::from_static(name)];
        if !tail.is_empty() {
            frame.extend(
                tail.split(|&byte| byte == 0)
                    .take(limits.max_arguments - 1)
                    .map(Bytes::copy_from_slice),
            );
        }
        check_command(frame);
    }
}

const COMMANDS: &[&[u8]] = &[
    b"GET", b"SET", b"DEL", b"EXISTS", b"MGET", b"TTL", b"PTTL", b"EXPIRE", b"PEXPIRE", b"PERSIST",
    b"INCRBY", b"AUTH", b"HELLO", b"PING", b"ECHO", b"CLIENT", b"COMMAND", b"CLUSTER", b"QUIT",
];

fn check_command(frame: Vec<Bytes>) {
    let mut lowercase = frame.clone();
    if let Some(name) = lowercase.first_mut() {
        *name = name.to_ascii_lowercase().into();
    }
    match (Command::parse(frame.clone()), Command::parse(lowercase)) {
        (Err(original), Err(lowercase)) => assert_eq!(original, lowercase),
        (Ok(Command::Data(original)), Ok(Command::Data(lowercase))) => {
            assert_eq!(original.kind(), lowercase.kind());
            let keys: Vec<_> = original.keys().cloned().collect();
            let first = keys.first().expect("data command has a key");
            let slot = key_slot(first);
            assert!(usize::from(slot) < SLOT_COUNT);
            let expected_slot = if keys.iter().all(|key| key_slot(key) == slot) {
                Ok(slot)
            } else {
                Err(CommandError::CrossSlot)
            };
            assert_eq!(original.slot(), expected_slot);

            if frame.len() <= FrameLimits::default().max_arguments - 2 {
                let getkeys = [
                    Bytes::from_static(b"COMMAND"),
                    Bytes::from_static(b"GETKEYS"),
                ];
                let parsed = Command::parse(getkeys.into_iter().chain(frame).collect());
                assert!(
                    matches!(parsed, Ok(Command::CommandGetKeys(reported)) if reported == keys)
                );
            }
            assert_eq!(original.into_operation(), lowercase.into_operation());
        }
        (Ok(original), Ok(lowercase)) => {
            assert_eq!(
                std::mem::discriminant(&original),
                std::mem::discriminant(&lowercase)
            );
            assert_eq!(
                original.metadata_response(4096),
                lowercase.metadata_response(4096)
            );
        }
        _ => panic!("command name case changed acceptance"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reaches_cache_decoders() {
        let limits = FrameLimits::default();
        let valid = include_bytes!("../corpus/cache_resp/ping.resp");
        let invalid = include_bytes!("../corpus/cache_resp/invalid-terminator.resp");
        let (frames, end) = decode_stream(valid, limits, 1);
        assert_eq!(frames, vec![vec![Bytes::from_static(b"PING")]]);
        assert_eq!(end, Ok(()));
        assert_eq!(
            decode_stream(invalid, limits, 1).1,
            Err(ProtocolError::InvalidTerminator)
        );

        let valid = include_bytes!("../corpus/cache_command/set-nx-ex.resp");
        let invalid = include_bytes!("../corpus/cache_command/set-conflicting-flags.resp");
        let parse = |data: &[u8]| {
            let mut buffer = BytesMut::from(data);
            let frame = Decoder::new(limits).decode(&mut buffer).unwrap().unwrap();
            Command::parse(frame)
        };
        assert!(matches!(parse(valid), Ok(Command::Data(_))));
        assert!(matches!(parse(invalid), Err(CommandError::Syntax)));
    }
}
