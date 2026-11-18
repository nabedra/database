use std::future::Future;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::OwnedSemaphorePermit;
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};

use super::admission::Admission;
use super::auth::{Acl, Identity};
use super::command::{Command, Credentials, DataCommand, hello_response, key_slot};
use super::config::ListenerConfig;
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
    let mut read = [0; 8192];
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
                let available = (config.limits.max_pipeline_bytes - buffered.len()).min(read.len());
                let received = stream.read(&mut read[..available]).await?;
                if received == 0 {
                    break Ok(decoder.eof(&buffered).map(|()| None));
                }
                append_input(
                    &mut buffered,
                    &read[..received],
                    config.limits.max_pipeline_bytes,
                );
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

fn append_input(buffered: &mut BytesMut, received: &[u8], limit: usize) {
    if !buffered.try_reclaim(received.len()) {
        // The decoder advances the logical start of this buffer. Growing it
        // in place could double backing capacity that includes the old prefix.
        let needed = buffered.len() + received.len();
        let capacity = buffered.capacity().saturating_mul(2).max(needed).min(limit);
        let mut replacement = BytesMut::with_capacity(capacity);
        replacement.extend_from_slice(buffered);
        *buffered = replacement;
    }
    buffered.extend_from_slice(received);
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
            backend.execute(command).await
        }
        _ => Response::error(b"ERR unsupported command"),
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

    #[test]
    fn k02_pipeline_compacts_within_backing_budget() {
        const LIMIT: usize = 1088 * 1024;
        let payload = vec![0xcd; LIMIT];
        let mut buffered = BytesMut::new();
        append_input(&mut buffered, &payload, LIMIT);
        for _ in 0..12 {
            buffered.advance(LIMIT * 7 / 8);
            append_input(&mut buffered, &payload[..LIMIT * 7 / 8], LIMIT);
            assert_eq!(&buffered[..], payload);
            assert!(buffered.capacity() <= LIMIT);
        }
    }
}
