use std::future::Future;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::OwnedSemaphorePermit;
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};

use super::admission::Admission;
use super::auth::{Acl, Identity};
use super::command::{
    Command, CommandError, CommandKind, Credentials, DataCommand, hello_response, key_slot,
};
use super::config::ListenerConfig;
use super::store::StoreError;
use super::wire::{Decoder, Response};

pub(super) trait Backend: Send + Sync + 'static {
    fn execute(&self, command: DataCommand) -> impl Future<Output = Response> + Send;

    fn clustered(&self) -> bool {
        false
    }

    fn slots(&self) -> Response {
        Response::error(b"ERR This instance has cluster support disabled")
    }

    fn shards(&self) -> Response {
        self.slots()
    }

    #[cfg(feature = "test-hooks")]
    fn debug(
        &self,
        _command: super::test_hooks::DebugCommand,
    ) -> impl Future<Output = Response> + Send {
        async { Response::error(b"ERR DEBUG is not supported by this backend") }
    }
}

pub(super) async fn listen<B: Backend>(
    socket: TcpListener,
    config: ListenerConfig,
    backend: B,
    stop: impl Future<Output = ()>,
) -> io::Result<()> {
    let admission = Admission::new(config.limits);
    let config = Arc::new(config);
    let backend = Arc::new(backend);
    let mut connections = JoinSet::new();
    let mut id = 0i64;
    tokio::pin!(stop);
    let result = loop {
        tokio::select! {
            biased;
            _ = &mut stop => break Ok(()),
            _ = connections.join_next(), if !connections.is_empty() => {},
            accepted = socket.accept() => {
                let (stream, _) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error)
                        if matches!(
                            error.kind(),
                            io::ErrorKind::Interrupted
                                | io::ErrorKind::ConnectionAborted
                                | io::ErrorKind::ConnectionReset
                        ) => continue,
                    Err(error) if exhausted(&error) => {
                        sleep(Duration::from_millis(25)).await;
                        continue;
                    }
                    Err(error) => break Err(error),
                };
                let Some(connection) = admission.connect() else {
                    if config.tls.is_none() {
                        let _ = stream.try_write(b"-BUSY cache connection limit reached\r\n");
                    }
                    continue;
                };
                let handshake = if config.tls.is_some() {
                    let Some(permit) = admission.handshake() else {
                        continue;
                    };
                    Some(permit)
                } else {
                    None
                };
                id = id.wrapping_add(1);
                let config = Arc::clone(&config);
                let backend = Arc::clone(&backend);
                connections.spawn(async move {
                    let _connection = connection;
                    let _ = connect(stream, &config, &*backend, handshake, id).await;
                });
            }
        }
    };
    connections.shutdown().await;
    result
}

fn exhausted(error: &io::Error) -> bool {
    #[cfg(unix)]
    if matches!(
        error.raw_os_error(),
        Some(libc::EMFILE | libc::ENFILE | libc::ENOBUFS | libc::ENOMEM)
    ) {
        return true;
    }
    error.kind() == io::ErrorKind::OutOfMemory
}

async fn connect<B: Backend>(
    stream: TcpStream,
    config: &ListenerConfig,
    backend: &B,
    handshake: Option<OwnedSemaphorePermit>,
    id: i64,
) -> io::Result<()> {
    stream.set_nodelay(true)?;
    if let Some(tls) = &config.tls {
        let stream = timeout(config.limits.handshake_timeout(), tls.accept(stream)).await??;
        drop(handshake);
        serve_connection(stream, config, backend, id).await
    } else {
        serve_connection(stream, config, backend, id).await
    }
}

async fn serve_connection<S, B>(
    mut stream: S,
    config: &ListenerConfig,
    backend: &B,
    id: i64,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
    B: Backend,
{
    let mut decoder = Decoder::new(config.limits.frame());
    let mut buffered = BytesMut::with_capacity(4096.min(config.limits.max_pipeline_bytes));
    let mut authenticated = None;
    let duration = config.limits.io_timeout();
    loop {
        let frame = timeout(duration, async {
            loop {
                match decoder.decode(&mut buffered) {
                    Ok(Some(frame)) => break Ok(Ok(Some(frame))),
                    Ok(None) => {}
                    Err(error) => break Ok(Err(error)),
                }
                let limit = config.limits.max_pipeline_bytes;
                let wanted = (limit - buffered.len()).min(4096);
                reserve_input(&mut buffered, wanted, limit);
                let available = (buffered.capacity() - buffered.len()).min(limit - buffered.len());
                if stream
                    .read_buf(&mut (&mut buffered).limit(available))
                    .await?
                    == 0
                {
                    break Ok(decoder.eof(&buffered).map(|()| None));
                }
            }
        })
        .await?
        .map_err(|error: io::Error| error)?;
        let frame = match frame {
            Ok(Some(frame)) => frame,
            Ok(None) => return Ok(()),
            Err(error) => {
                timeout(duration, async {
                    error
                        .response()
                        .write_to(&mut stream, config.limits.max_reply_bytes)
                        .await?;
                    stream.flush().await
                })
                .await??;
                return Ok(());
            }
        };
        let mut quit = false;
        let response = match Command::parse(frame) {
            Ok(command) => {
                quit = matches!(command, Command::Quit);
                dispatch(command, config, backend, &mut authenticated, id).await
            }
            Err(error) => error.response(),
        };
        let response = if response
            .encoded_len()
            .is_some_and(|size| size <= config.limits.max_reply_bytes)
        {
            response
        } else {
            Response::error(b"ERR response exceeds cache output limit")
        };
        timeout(duration, async {
            response
                .write_to(&mut stream, config.limits.max_reply_bytes)
                .await?;
            stream.flush().await
        })
        .await??;
        if quit {
            return Ok(());
        }
    }
}

fn reserve_input(buffered: &mut BytesMut, additional: usize, limit: usize) {
    if !buffered.try_reclaim(additional) {
        // The decoder advances the logical start of this buffer. Growing it
        // in place could double backing capacity that includes the old prefix.
        let needed = buffered.len() + additional;
        let capacity = buffered.capacity().saturating_mul(2).max(needed).min(limit);
        let mut replacement = BytesMut::with_capacity(capacity);
        replacement.extend_from_slice(buffered);
        *buffered = replacement;
    }
}

type Session<'a> = Option<(&'a Identity, OwnedSemaphorePermit)>;

fn authenticate<'a>(
    credentials: &Credentials,
    acl: &'a Acl,
    session: &mut Session<'a>,
) -> Result<(), Response> {
    let Some(identity) = acl.authenticate(credentials) else {
        return Err(Response::error(b"WRONGPASS invalid username-password pair"));
    };
    if session
        .as_ref()
        .is_some_and(|(old, _)| std::ptr::eq(*old, identity))
    {
        return Ok(());
    }
    let permit = identity
        .try_connect()
        .ok_or_else(|| Response::error(b"BUSY cache identity connection limit reached"))?;
    *session = Some((identity, permit));
    Ok(())
}

async fn dispatch<'a, B: Backend>(
    command: Command,
    config: &'a ListenerConfig,
    backend: &B,
    session: &mut Session<'a>,
    id: i64,
) -> Response {
    let command = match command {
        Command::Auth(credentials) => {
            return authenticate(&credentials, &config.acl, session)
                .err()
                .unwrap_or_else(Response::ok);
        }
        Command::Hello { auth, name: _name } => {
            if let Some(credentials) = auth {
                if let Err(error) = authenticate(&credentials, &config.acl, session) {
                    return error;
                }
            }
            if !config.anonymous && session.is_none() {
                return Response::error(b"NOAUTH authentication required");
            }
            return hello_response(id, backend.clustered());
        }
        Command::Quit => return Response::ok(),
        command => command,
    };
    if !config.anonymous && session.is_none() {
        return Response::error(b"NOAUTH authentication required");
    }
    let identity = session.as_ref().map(|(identity, _)| *identity);
    if let Some(response) = command.metadata_response(config.limits.max_reply_bytes) {
        return response;
    }
    match command {
        Command::Ping(None) => Response::simple(b"PONG"),
        Command::Ping(Some(value)) | Command::Echo(value) => Response::Bulk(Some(value)),
        Command::ClientSetInfo | Command::ClientSetName(_) => Response::ok(),
        Command::ClusterSlots => backend.slots(),
        Command::ClusterShards => backend.shards(),
        Command::ClusterKeySlot(key) => {
            if !backend.clustered() {
                return backend.slots();
            }
            if identity.is_some_and(|identity| !identity.allows_key(&key)) {
                return forbidden();
            }
            Response::Integer(key_slot(&key).into())
        }
        Command::Data(command) => {
            if identity.is_some_and(|identity| !identity.allows(&command)) {
                return forbidden();
            }
            let kind = command.kind();
            data_response(kind, backend.execute(command).await)
        }
        #[cfg(feature = "test-hooks")]
        Command::Debug(command) => backend.debug(command).await,
        _ => Response::error(b"ERR unsupported command"),
    }
}

/// The store cannot name the command, so its deadline overflow gets Valkey's
/// per-command text here, for the standalone and cluster backends alike.
fn data_response(kind: CommandKind, response: Response) -> Response {
    match &response {
        Response::Error(text) if text.as_ref() == StoreError::InvalidTtl.resp().as_bytes() => {
            CommandError::InvalidExpireTime(kind.name()).response()
        }
        _ => response,
    }
}

fn forbidden() -> Response {
    Response::error(b"NOPERM this credential cannot execute the command on these keys")
}

pub(super) fn shutdown() -> io::Result<impl Future<Output = ()>> {
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    Ok(async move {
        #[cfg(unix)]
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate.recv() => {},
        }
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
    })
}

#[cfg(test)]
mod tests {
    use bytes::Buf;

    use super::*;

    /// Valkey 8.1.11 replies, captured from `valkey/valkey:8.1.11`.
    #[tokio::test]
    async fn k01_expire_time_overflow_matches_valkey() {
        use crate::cache::standalone::Standalone;
        use crate::cache::store::{EvictionPolicy, TurnLimits};
        let backend = Standalone::new(
            1 << 20,
            EvictionPolicy::NoEviction,
            16,
            TurnLimits::default(),
        );
        let run = async |words: &[&'static [u8]]| {
            let frame = words
                .iter()
                .map(|word| bytes::Bytes::from_static(word))
                .collect();
            match Command::parse(frame) {
                Ok(Command::Data(command)) => {
                    let kind = command.kind();
                    data_response(kind, backend.execute(command).await)
                }
                Ok(_) => panic!("not a data command"),
                Err(error) => error.response(),
            }
        };
        assert_eq!(run(&[b"SET", b"k", b"v"]).await, Response::ok());
        let max = b"9223372036854775807";
        for (words, expected) in [
            (&[b"EXPIRE".as_slice(), b"k", max][..], "expire"),
            (&[b"EXPIRE", b"missing", max], "expire"),
            (&[b"EXPIRE", b"k", b"-9223372036854775807"], "expire"),
            (&[b"EXPIRE", b"k", b"-9223372036854776"], "expire"),
            (&[b"PEXPIRE", b"k", max], "pexpire"),
            (&[b"PEXPIRE", b"k", b"9223372036854000000"], "pexpire"),
            (&[b"EXPIRE", b"k", b"9223372036854775"], "expire"),
            (&[b"SET", b"k", b"v", b"PX", max], "set"),
            (&[b"SET", b"k", b"v", b"EX", b"9223372036854775"], "set"),
        ] {
            let text = format!("ERR invalid expire time in '{expected}' command");
            assert_eq!(
                run(words).await,
                Response::Error(bytes::Bytes::from(text)),
                "{words:?}"
            );
        }
        assert_eq!(
            run(&[b"GET", b"k"]).await,
            Response::Bulk(Some(bytes::Bytes::from_static(b"v")))
        );
        for words in [
            [b"EXPIRE".as_slice(), b"k", b"-9223372036854775"],
            [b"PEXPIRE", b"k", b"-9223372036854775808"],
        ] {
            assert_eq!(run(&[b"SET", b"k", b"v"]).await, Response::ok());
            assert_eq!(run(&words).await, Response::Integer(1));
            assert_eq!(run(&[b"EXISTS", b"k"]).await, Response::Integer(0));
        }
    }

    #[test]
    fn k02_pipeline_compacts_within_backing_budget() {
        const LIMIT: usize = 1088 * 1024;
        let payload = vec![0xcd; LIMIT];
        let mut buffered = BytesMut::new();
        reserve_input(&mut buffered, LIMIT, LIMIT);
        buffered.extend_from_slice(&payload);
        for _ in 0..12 {
            buffered.advance(LIMIT * 7 / 8);
            reserve_input(&mut buffered, LIMIT * 7 / 8, LIMIT);
            buffered.extend_from_slice(&payload[..LIMIT * 7 / 8]);
            assert_eq!(&buffered[..], payload);
            assert!(buffered.capacity() <= LIMIT);
        }
    }
}
