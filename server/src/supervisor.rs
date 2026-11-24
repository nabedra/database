//! Engine-neutral framing for the explicitly inherited local supervisor socket.
//! No application request, credential, provider or storage semantics live here.
use nabedra_rt::time::{Duration, Instant, timeout_at};
use serde::Serialize;
use std::io;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::UnixStream,
};
const MAX_FRAME: usize = 16 << 10;
fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid supervisor frame")
}
fn timed_out() -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "supervisor frame deadline")
}
pub(crate) async fn frame(
    stream: &mut (impl AsyncRead + Unpin),
    end: Option<Instant>,
) -> io::Result<Vec<u8>> {
    let mut first = [0; 1];
    match end {
        Some(end) => {
            timeout_at(end, stream.read_exact(&mut first))
                .await
                .map_err(|_| timed_out())??;
        }
        None => {
            stream.read_exact(&mut first).await?;
        }
    }
    let end = end.unwrap_or_else(|| nabedra_rt::time::now() + Duration::from_secs(2));
    let mut prefix = [0; 4];
    prefix[0] = first[0];
    timeout_at(end, stream.read_exact(&mut prefix[1..]))
        .await
        .map_err(|_| timed_out())??;
    let length = u32::from_be_bytes(prefix) as usize;
    if length == 0 || length > MAX_FRAME {
        return Err(invalid());
    }
    let mut body = vec![0; length];
    timeout_at(end, stream.read_exact(&mut body))
        .await
        .map_err(|_| timed_out())??;
    if body.iter().copied().find(|b| !b.is_ascii_whitespace()) != Some(b'{') {
        return Err(invalid());
    }
    Ok(body)
}
pub(crate) async fn send(
    stream: &mut (impl AsyncWrite + Unpin),
    value: &impl Serialize,
    end: Instant,
) -> io::Result<()> {
    let bytes = serde_json::to_vec(value).map_err(|_| invalid())?;
    if bytes.is_empty() || bytes.len() > MAX_FRAME {
        return Err(invalid());
    }
    timeout_at(end, async {
        stream
            .write_all(&(bytes.len() as u32).to_be_bytes())
            .await?;
        stream.write_all(&bytes).await
    })
    .await
    .map_err(|_| timed_out())?
}
pub(crate) fn open() -> io::Result<UnixStream> {
    use std::os::fd::FromRawFd;
    let fd: i32 = std::env::var("NABEDRA_SUPERVISOR_FD")
        .map_err(|_| invalid())?
        .parse()
        .map_err(|_| invalid())?;
    if fd < 3 {
        return Err(invalid());
    }
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
        return Err(invalid());
    }
    // The owning parent explicitly transfers this descriptor via pass_fds.
    let stream = unsafe { std::os::unix::net::UnixStream::from_raw_fd(fd) };
    stream.set_nonblocking(true)?;
    UnixStream::from_std(stream)
}
