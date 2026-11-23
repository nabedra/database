//! The network seam (feature `net`): listeners, connectors and byte streams.
//!
//! Three traits describe a backend:
//!
//! - [`Listener`] accepts connections, like `tokio::net::TcpListener`.
//! - [`Connector`] dials a [`Target`], like `tokio::net::TcpStream::connect`.
//! - [`Stream`] is any established byte stream (`AsyncRead + AsyncWrite + Unpin + Send`).
//!   It is implemented for every such type, so it also names the bound axum's
//!   `serve::Listener::Io` needs.
//!
//! The real backend is tokio TCP: `tokio::net::TcpListener` implements [`Listener`] and
//! [`TcpConnector`] implements [`Connector`], both with `TcpStream` connections. TLS stays
//! out of this crate: the transport runs rustls on the `TcpStream` and reports what the
//! handshake proved as a [`Handshake`] value, so trust checks never read the TLS session.
//!
//! [`AnyListener`] and [`AnyConnector`] are the one injection point product code uses. In a
//! product build they are the TCP backend in a single-field wrapper, [`Conn`] has one
//! variant and [`Established<T>`] is `T`, so every match compiles to the TCP path and
//! nothing is added at run time. With `sim`, the same types reach the simulated backend
//! (`crate::sim::net`) whenever a simulated world is installed on the thread, and TCP
//! otherwise.

use std::future::Future;
use std::io;
use std::net::SocketAddr;

use tokio::io::{AsyncRead, AsyncWrite};
pub use tokio::net::{TcpListener, TcpStream};

/// What a connection handshake proved, as values. Trust checks read these, never the TLS
/// session, so they run the same on the real and the simulated backend.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Handshake {
    /// The negotiated application protocol (ALPN), if any.
    pub alpn: Option<Vec<u8>>,
    /// SHA-256 of the peer's first certificate.
    pub peer_certificate_sha256: [u8; 32],
}

/// Where a [`Connector`] dials.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Target<'a> {
    Addr(SocketAddr),
    /// Tried in order; the first that connects wins.
    Addrs(&'a [SocketAddr]),
    /// Resolved by the backend: DNS for TCP, the registered hosts in a simulation.
    HostPort(&'a str, u16),
}

/// An established byte stream. Implemented for every type with these bounds.
pub trait Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static {}

impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> Stream for T {}

/// Accepts connections.
pub trait Listener: Send + Sync {
    /// An accepted connection, before any handshake.
    type Conn: Send + 'static;

    /// The next connection and the peer's address.
    fn accept(&self) -> impl Future<Output = io::Result<(Self::Conn, SocketAddr)>> + Send;

    fn local_addr(&self) -> io::Result<SocketAddr>;
}

/// Dials connections.
pub trait Connector: Send + Sync {
    /// A dialed connection, before any handshake.
    type Conn: Send + 'static;

    fn connect(&self, target: Target<'_>) -> impl Future<Output = io::Result<Self::Conn>> + Send;
}

impl Listener for TcpListener {
    type Conn = TcpStream;

    #[inline]
    fn accept(&self) -> impl Future<Output = io::Result<(TcpStream, SocketAddr)>> + Send {
        TcpListener::accept(self)
    }

    #[inline]
    fn local_addr(&self) -> io::Result<SocketAddr> {
        TcpListener::local_addr(self)
    }
}

/// The real dialer: `tokio::net::TcpStream::connect`.
#[derive(Clone, Copy, Debug, Default)]
pub struct TcpConnector;

impl Connector for TcpConnector {
    type Conn = TcpStream;

    #[inline]
    async fn connect(&self, target: Target<'_>) -> io::Result<TcpStream> {
        match target {
            Target::Addr(addr) => TcpStream::connect(addr).await,
            Target::Addrs(addrs) => TcpStream::connect(addrs).await,
            Target::HostPort(host, port) => TcpStream::connect((host, port)).await,
        }
    }
}

/// A connection from [`AnyListener`] or [`AnyConnector`], before any handshake.
///
/// A product build has only the `Tcp` variant. The transport runs rustls on it; under
/// `sim` the `Sim` arm runs the simulated handshake with the same ALPN offer.
#[derive(Debug)]
pub enum Conn {
    Tcp(TcpStream),
    #[cfg(feature = "sim")]
    Sim(crate::sim::net::Pending),
}

impl Conn {
    /// The connection as a plain byte stream with no handshake (HTTP without TLS).
    #[inline]
    pub fn plain(self) -> Established<TcpStream> {
        match self {
            #[cfg(not(feature = "sim"))]
            Conn::Tcp(stream) => stream,
            #[cfg(feature = "sim")]
            Conn::Tcp(stream) => Established::Real(stream),
            #[cfg(feature = "sim")]
            Conn::Sim(pending) => Established::Sim(pending.plain()),
        }
    }

    #[inline]
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        match self {
            Conn::Tcp(stream) => stream.peer_addr(),
            #[cfg(feature = "sim")]
            Conn::Sim(pending) => Ok(pending.peer_addr()),
        }
    }

    #[inline]
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        match self {
            Conn::Tcp(stream) => stream.local_addr(),
            #[cfg(feature = "sim")]
            Conn::Sim(pending) => Ok(pending.local_addr()),
        }
    }
}

/// The established stream over a connection: `T` (for example a rustls stream over TCP)
/// in a product build.
#[cfg(not(feature = "sim"))]
pub type Established<T> = T;

/// The established stream over a connection: `T` on the real backend, or a simulated
/// stream after the simulated handshake.
#[cfg(feature = "sim")]
#[derive(Debug)]
pub enum Established<T> {
    Real(T),
    Sim(crate::sim::net::SimStream),
}

#[cfg(feature = "sim")]
impl<T> From<T> for Established<T> {
    fn from(stream: T) -> Self {
        Established::Real(stream)
    }
}

#[cfg(feature = "sim")]
mod established {
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

    use super::Established;

    impl<T: AsyncRead + Unpin> AsyncRead for Established<T> {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            match self.get_mut() {
                Established::Real(stream) => Pin::new(stream).poll_read(cx, buf),
                Established::Sim(stream) => Pin::new(stream).poll_read(cx, buf),
            }
        }
    }

    impl<T: AsyncWrite + Unpin> AsyncWrite for Established<T> {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            match self.get_mut() {
                Established::Real(stream) => Pin::new(stream).poll_write(cx, buf),
                Established::Sim(stream) => Pin::new(stream).poll_write(cx, buf),
            }
        }

        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            match self.get_mut() {
                Established::Real(stream) => Pin::new(stream).poll_flush(cx),
                Established::Sim(stream) => Pin::new(stream).poll_flush(cx),
            }
        }

        fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            match self.get_mut() {
                Established::Real(stream) => Pin::new(stream).poll_shutdown(cx),
                Established::Sim(stream) => Pin::new(stream).poll_shutdown(cx),
            }
        }
    }
}

/// The listener product code binds. A product build wraps a `TcpListener`. Under `sim`
/// it is simulated when it was bound inside an installed world.
#[derive(Debug)]
pub struct AnyListener {
    #[cfg(not(feature = "sim"))]
    tcp: TcpListener,
    #[cfg(feature = "sim")]
    inner: AnyInner,
}

#[cfg(feature = "sim")]
#[derive(Debug)]
enum AnyInner {
    Tcp(TcpListener),
    Sim(crate::sim::net::SimListener),
}

impl From<TcpListener> for AnyListener {
    #[inline]
    fn from(tcp: TcpListener) -> Self {
        #[cfg(not(feature = "sim"))]
        {
            Self { tcp }
        }
        #[cfg(feature = "sim")]
        {
            Self {
                inner: AnyInner::Tcp(tcp),
            }
        }
    }
}

#[cfg(feature = "sim")]
impl From<crate::sim::net::SimListener> for AnyListener {
    fn from(listener: crate::sim::net::SimListener) -> Self {
        Self {
            inner: AnyInner::Sim(listener),
        }
    }
}

impl AnyListener {
    /// `TcpListener::bind(addr)`. Under `sim`, inside an installed world, binds a
    /// simulated listener for the current node instead.
    #[inline]
    pub async fn bind(addr: SocketAddr) -> io::Result<Self> {
        #[cfg(feature = "sim")]
        if crate::sim::world().is_some() {
            return crate::sim::net::SimListener::bind(addr).map(Self::from);
        }
        TcpListener::bind(addr).await.map(Self::from)
    }

    #[inline]
    pub async fn accept(&self) -> io::Result<(Conn, SocketAddr)> {
        #[cfg(not(feature = "sim"))]
        {
            let (stream, peer) = self.tcp.accept().await?;
            Ok((Conn::Tcp(stream), peer))
        }
        #[cfg(feature = "sim")]
        match &self.inner {
            AnyInner::Tcp(tcp) => {
                let (stream, peer) = tcp.accept().await?;
                Ok((Conn::Tcp(stream), peer))
            }
            AnyInner::Sim(sim) => {
                let (pending, peer) = sim.accept().await?;
                Ok((Conn::Sim(pending), peer))
            }
        }
    }

    #[inline]
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        #[cfg(not(feature = "sim"))]
        {
            self.tcp.local_addr()
        }
        #[cfg(feature = "sim")]
        match &self.inner {
            AnyInner::Tcp(tcp) => tcp.local_addr(),
            AnyInner::Sim(sim) => Ok(sim.local_addr()),
        }
    }
}

impl Listener for AnyListener {
    type Conn = Conn;

    #[inline]
    fn accept(&self) -> impl Future<Output = io::Result<(Conn, SocketAddr)>> + Send {
        AnyListener::accept(self)
    }

    #[inline]
    fn local_addr(&self) -> io::Result<SocketAddr> {
        AnyListener::local_addr(self)
    }
}

/// The dialer product code uses: [`TcpConnector`] in a product build. Under `sim`, inside
/// an installed world, it dials the simulated network from the current node.
#[derive(Clone, Copy, Debug, Default)]
pub struct AnyConnector;

impl Connector for AnyConnector {
    type Conn = Conn;

    #[inline]
    async fn connect(&self, target: Target<'_>) -> io::Result<Conn> {
        #[cfg(feature = "sim")]
        if crate::sim::world().is_some() {
            return crate::sim::net::dial(target).await.map(Conn::Sim);
        }
        TcpConnector.connect(target).await.map(Conn::Tcp)
    }
}

/// Resolves `host:port` with the blocking system resolver (`std::net::ToSocketAddrs`).
/// Under `sim`, inside an installed world, answers from the simulated hosts instead.
#[inline]
pub fn resolve(host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
    #[cfg(feature = "sim")]
    if crate::sim::world().is_some() {
        return crate::sim::net::resolve(host, port);
    }
    std::net::ToSocketAddrs::to_socket_addrs(&(host, port)).map(Iterator::collect)
}

/// `tokio::net::lookup_host((host, port))`, collected. Under `sim`, inside an installed
/// world, answers from the simulated hosts instead.
#[inline]
pub async fn lookup_host(host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
    #[cfg(feature = "sim")]
    if crate::sim::world().is_some() {
        return crate::sim::net::resolve(host, port);
    }
    tokio::net::lookup_host((host, port))
        .await
        .map(Iterator::collect)
}

#[cfg(test)]
mod tests;
