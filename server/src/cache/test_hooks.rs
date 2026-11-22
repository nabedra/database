//! The `DEBUG` command, compiled only with the `test-hooks` feature. Release
//! images never enable it (O02, NS26). Without the feature, `DEBUG` gets the
//! ordinary unknown-command error from the parser.
//!
//! - `DEBUG DIGEST` answers a status reply of 40 hex digits, the
//!   order-independent digest from `SlotStore::digest`. An empty keyspace
//!   digests to 40 zeros, as in Valkey.
//! - `DEBUG SET-ACTIVE-EXPIRE 0|1` pauses or resumes the expiry sweep.
//!   Lazy expiry on access is unaffected.
//! - `DEBUG SLEEP <seconds>` holds the store for that long, as Valkey's
//!   blocking sleep does. It accepts a non-negative decimal of at most 60.
use std::time::Duration;

use bytes::Bytes;

use super::command::CommandError;
use super::wire::Response;

const MAX_SLEEP_SECONDS: f64 = 60.0;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum DebugCommand {
    Digest,
    SetActiveExpire(bool),
    Sleep(Duration),
}

pub(super) fn parse(mut arguments: std::vec::IntoIter<Bytes>) -> Result<DebugCommand, CommandError> {
    let subcommand = arguments.next().ok_or(CommandError::Arity)?;
    let argument = arguments.next();
    if arguments.next().is_some() {
        return Err(CommandError::Arity);
    }
    let word = |name: &[u8]| subcommand.eq_ignore_ascii_case(name);
    match argument {
        None if word(b"DIGEST") => Ok(DebugCommand::Digest),
        Some(flag) if word(b"SET-ACTIVE-EXPIRE") => match flag.as_ref() {
            b"0" => Ok(DebugCommand::SetActiveExpire(false)),
            b"1" => Ok(DebugCommand::SetActiveExpire(true)),
            _ => Err(CommandError::Syntax),
        },
        Some(seconds) if word(b"SLEEP") => std::str::from_utf8(&seconds)
            .ok()
            .filter(|text| text.bytes().all(|b| b.is_ascii_digit() || b == b'.'))
            .and_then(|text| text.parse::<f64>().ok())
            .filter(|seconds| (0.0..=MAX_SLEEP_SECONDS).contains(seconds))
            .map(|seconds| DebugCommand::Sleep(Duration::from_secs_f64(seconds)))
            .ok_or(CommandError::InvalidInteger),
        _ if word(b"DIGEST") || word(b"SET-ACTIVE-EXPIRE") || word(b"SLEEP") => {
            Err(CommandError::Arity)
        }
        _ => Err(CommandError::Unsupported),
    }
}

pub(super) fn digest_response(digest: [u8; 20]) -> Response {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut text = Vec::with_capacity(40);
    for byte in digest {
        text.push(HEX[usize::from(byte >> 4)]);
        text.push(HEX[usize::from(byte & 15)]);
    }
    Response::Simple(Bytes::from(text))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::*;
    use crate::cache::command::Command;
    use crate::cache::listener::Backend;
    use crate::cache::standalone::Standalone;
    use crate::cache::store::{EvictionPolicy, TurnLimits};

    fn frame(parts: &[&str]) -> Vec<Bytes> {
        parts.iter().map(|part| Bytes::from(part.to_string())).collect()
    }

    fn debug(parts: &[&str]) -> Result<DebugCommand, CommandError> {
        let mut all = vec!["debug"];
        all.extend_from_slice(parts);
        match Command::parse(frame(&all))? {
            Command::Debug(command) => Ok(command),
            _ => panic!("DEBUG parsed as another command"),
        }
    }

    fn backend() -> Standalone {
        Standalone::new(1 << 20, EvictionPolicy::NoEviction, 16, TurnLimits::default())
    }

    async fn run(backend: &Standalone, parts: &[&str]) -> Response {
        match Command::parse(frame(parts)).expect("valid command") {
            Command::Data(command) => backend.execute(command).await,
            Command::Debug(command) => backend.debug(command).await,
            _ => panic!("unexpected command"),
        }
    }

    #[test]
    fn test_hooks_parse_debug_subcommands() {
        assert_eq!(debug(&["DIGEST"]), Ok(DebugCommand::Digest));
        assert_eq!(debug(&["digest"]), Ok(DebugCommand::Digest));
        assert_eq!(
            debug(&["set-active-expire", "0"]),
            Ok(DebugCommand::SetActiveExpire(false))
        );
        assert_eq!(
            debug(&["SET-ACTIVE-EXPIRE", "1"]),
            Ok(DebugCommand::SetActiveExpire(true))
        );
        assert_eq!(
            debug(&["SLEEP", "0.25"]),
            Ok(DebugCommand::Sleep(Duration::from_millis(250)))
        );
        assert_eq!(debug(&["SLEEP", "0"]), Ok(DebugCommand::Sleep(Duration::ZERO)));
        assert_eq!(debug(&[]), Err(CommandError::Arity));
        assert_eq!(debug(&["DIGEST", "x"]), Err(CommandError::Arity));
        assert_eq!(debug(&["SLEEP"]), Err(CommandError::Arity));
        assert_eq!(debug(&["SET-ACTIVE-EXPIRE", "2"]), Err(CommandError::Syntax));
        for bad in ["-1", "61", "nan", "inf", "1e3", " 1", ""] {
            assert_eq!(debug(&["SLEEP", bad]), Err(CommandError::InvalidInteger), "{bad}");
        }
        assert_eq!(debug(&["RELOAD"]), Err(CommandError::Unsupported));
    }

    #[tokio::test]
    async fn test_hooks_digest_reply_tracks_changes() {
        let backend = backend();
        let empty = Response::Simple(Bytes::from("0".repeat(40)));
        assert_eq!(run(&backend, &["DEBUG", "DIGEST"]).await, empty);
        run(&backend, &["SET", "a", "1"]).await;
        run(&backend, &["SET", "b", "2", "PX", "100000"]).await;
        let digest = run(&backend, &["DEBUG", "DIGEST"]).await;
        let Response::Simple(text) = &digest else {
            panic!("digest is not a status reply: {digest:?}");
        };
        assert_eq!(text.len(), 40);
        assert!(text.iter().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b)));
        assert_ne!(digest, empty);
        assert_eq!(run(&backend, &["DEBUG", "DIGEST"]).await, digest, "not deterministic");

        run(&backend, &["PERSIST", "b"]).await;
        let persisted = run(&backend, &["DEBUG", "DIGEST"]).await;
        assert_ne!(persisted, digest, "the digest ignores deadlines");
        run(&backend, &["SET", "a", "2"]).await;
        assert_ne!(run(&backend, &["DEBUG", "DIGEST"]).await, persisted);
        run(&backend, &["DEL", "a", "b"]).await;
        assert_eq!(run(&backend, &["DEBUG", "DIGEST"]).await, empty);
    }

    #[test]
    fn test_hooks_digest_ignores_insertion_order() {
        use crate::cache::store::{Operation, SetWhen, SlotStore};
        let set = |key: &'static [u8], value: &'static [u8], ttl_ms| Operation::Set {
            key: Bytes::from_static(key),
            value: Bytes::from_static(value),
            ttl_ms,
            when: SetWhen::Always,
        };
        let ops = [set(b"a", b"1", None), set(b"b", b"2", Some(50)), set(b"c", b"", None)];
        let mut forward = SlotStore::new(1 << 20, EvictionPolicy::NoEviction);
        let mut backward = SlotStore::new(1 << 20, EvictionPolicy::NoEviction);
        for op in &ops {
            let prepared = forward.prepare(op, 0, TurnLimits::default());
            forward.commit(prepared);
        }
        for op in ops.iter().rev() {
            let prepared = backward.prepare(op, 0, TurnLimits::default());
            backward.commit(prepared);
        }
        assert_eq!(forward.digest(0), backward.digest(0));
        assert_ne!(forward.digest(0), [0; 20]);
        // A key past its deadline is not part of the digest.
        assert_ne!(forward.digest(0), forward.digest(50));
        let swapped = [set(b"a", b"2", None), set(b"b", b"1", Some(50)), set(b"c", b"", None)];
        let mut other = SlotStore::new(1 << 20, EvictionPolicy::NoEviction);
        for op in &swapped {
            let prepared = other.prepare(op, 0, TurnLimits::default());
            other.commit(prepared);
        }
        assert_ne!(other.digest(0), forward.digest(0));
    }

    /// With active expiry off, an expired key stays physically present until
    /// it is accessed; turning it back on lets the sweep remove it.
    #[tokio::test]
    async fn test_hooks_set_active_expire_pauses_the_sweep() {
        let backend = backend();
        assert_eq!(
            run(&backend, &["DEBUG", "SET-ACTIVE-EXPIRE", "0"]).await,
            Response::ok()
        );
        assert!(!backend.active_expire.load(Ordering::Relaxed));
        let sweeper = tokio::spawn(crate::cache::standalone::expire(
            std::sync::Arc::clone(&backend.actor),
            backend.started,
            std::sync::Arc::clone(&backend.active_expire),
        ));
        run(&backend, &["SET", "k", "v", "PX", "1"]).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(backend.actor.lock().await.len(), 1, "the sweep ran while paused");

        assert_eq!(
            run(&backend, &["DEBUG", "SET-ACTIVE-EXPIRE", "1"]).await,
            Response::ok()
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while backend.actor.lock().await.len() != 0 {
            assert!(std::time::Instant::now() < deadline, "the sweep did not resume");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        sweeper.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn test_hooks_sleep_holds_the_store() {
        let backend = backend();
        let started = tokio::time::Instant::now();
        let (reply, ()) = tokio::join!(run(&backend, &["DEBUG", "SLEEP", "2.5"]), async {
            tokio::task::yield_now().await;
            assert!(backend.actor.try_lock().is_err(), "SLEEP released the store");
        });
        assert_eq!(reply, Response::ok());
        assert!(started.elapsed() >= Duration::from_millis(2_500));
        assert!(backend.actor.try_lock().is_ok());
    }
}
