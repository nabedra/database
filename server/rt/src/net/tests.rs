//! The network seam outside a simulated world is tokio TCP. These tests run with the
//! `net` feature and, unchanged, with `sim`: outside an installed world the ambient types
//! take the TCP path too.

use std::io;
use std::net::{Ipv4Addr, SocketAddr};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{
    AnyConnector, AnyListener, Conn, Connector, Listener, Stream, Target, TcpConnector,
    TcpListener, lookup_host, resolve,
};

fn loopback() -> SocketAddr {
    SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0)
}

/// One request and one reply over any backend, through the traits only.
async fn echo_once<L, C, S>(
    listener: &L,
    connector: &C,
    target: Target<'_>,
    open: impl Fn(L::Conn) -> S + Copy,
    dialed: impl Fn(C::Conn) -> S,
) -> io::Result<Vec<u8>>
where
    L: Listener,
    C: Connector,
    S: Stream,
{
    let mut client = dialed(connector.connect(target).await?);
    let (accepted, peer) = listener.accept().await?;
    assert_ne!(peer.port(), 0);
    let mut server = open(accepted);
    client.write_all(b"ping").await?;
    client.flush().await?;
    let mut request = [0; 4];
    server.read_exact(&mut request).await?;
    server
        .write_all(&[request.as_slice(), b"-pong"].concat())
        .await?;
    server.shutdown().await?;
    let mut reply = Vec::new();
    client.read_to_end(&mut reply).await?;
    Ok(reply)
}

#[tokio::test]
async fn tcp_backend_round_trips_through_the_traits() {
    let listener = TcpListener::bind(loopback()).await.unwrap();
    let addr = Listener::local_addr(&listener).unwrap();
    let addrs = [SocketAddr::new(addr.ip(), 1), addr];
    for target in [
        Target::Addr(addr),
        // The first address refuses; the second connects.
        Target::Addrs(&addrs),
        Target::HostPort("127.0.0.1", addr.port()),
        Target::HostPort("localhost", addr.port()),
    ] {
        let reply = echo_once(&listener, &TcpConnector, target, |s| s, |s| s)
            .await
            .unwrap_or_else(|error| panic!("{target:?}: {error}"));
        assert_eq!(reply, b"ping-pong", "{target:?}");
    }
}

#[tokio::test]
async fn ambient_types_take_the_tcp_path_outside_a_world() {
    let listener = AnyListener::bind(loopback()).await.unwrap();
    let addr = listener.local_addr().unwrap();
    assert!(addr.ip().is_loopback(), "{addr}");
    let reply = echo_once(
        &listener,
        &AnyConnector,
        Target::Addr(addr),
        Conn::plain,
        Conn::plain,
    )
    .await
    .unwrap();
    assert_eq!(reply, b"ping-pong");

    let dialed = AnyConnector.connect(Target::Addr(addr)).await.unwrap();
    assert!(matches!(dialed, Conn::Tcp(_)), "{dialed:?}");
    assert_eq!(dialed.peer_addr().unwrap(), addr);
    let (accepted, peer) = listener.accept().await.unwrap();
    assert_eq!(accepted.peer_addr().unwrap(), peer);
    assert_eq!(dialed.local_addr().unwrap(), peer);
}

#[tokio::test]
async fn an_existing_tcp_listener_converts() {
    // Callers that bind a `TcpListener` themselves pass it on unchanged.
    let tcp = TcpListener::bind(loopback()).await.unwrap();
    let addr = tcp.local_addr().unwrap();
    let listener = AnyListener::from(tcp);
    assert_eq!(listener.local_addr().unwrap(), addr);
    let reply = echo_once(
        &listener,
        &TcpConnector,
        Target::Addr(addr),
        Conn::plain,
        Conn::plain_tcp,
    )
    .await
    .unwrap();
    assert_eq!(reply, b"ping-pong");
}

#[tokio::test]
async fn a_closed_port_refuses() {
    let tcp = TcpListener::bind(loopback()).await.unwrap();
    let addr = tcp.local_addr().unwrap();
    drop(tcp);
    let error = AnyConnector.connect(Target::Addr(addr)).await.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::ConnectionRefused, "{error}");
    let error = TcpConnector.connect(Target::Addrs(&[])).await.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{error}");
}

#[tokio::test]
async fn resolution_uses_the_system_resolver() {
    let blocking = resolve("localhost", 7).unwrap();
    assert!(
        blocking
            .iter()
            .all(|addr| addr.ip().is_loopback() && addr.port() == 7)
    );
    assert!(!blocking.is_empty());
    let mut lookup = lookup_host("127.0.0.1", 9).await.unwrap();
    lookup.sort();
    assert_eq!(lookup, [SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 9)]);
}

/// The product seam adds nothing: the wrappers are the TCP types' size, the connectors
/// carry no state, and `Established<T>` is `T`.
#[cfg(not(feature = "sim"))]
#[test]
fn product_types_are_the_tcp_types() {
    use std::mem::size_of;

    use super::{Established, TcpStream};

    assert_eq!(size_of::<AnyListener>(), size_of::<TcpListener>());
    assert_eq!(size_of::<Conn>(), size_of::<TcpStream>());
    assert_eq!(size_of::<AnyConnector>(), 0);
    assert_eq!(size_of::<TcpConnector>(), 0);
    fn same<T>(value: T) -> T {
        value
    }
    let _: fn(Established<TcpStream>) -> TcpStream = same::<TcpStream>;
}

/// axum's `serve::Listener` needs `Io: AsyncRead + AsyncWrite + Unpin + Send + 'static`;
/// a plain accepted connection is that, so axum can serve over [`AnyListener`].
#[test]
fn a_plain_connection_is_an_axum_io() {
    fn io<T: Stream>() {}
    io::<super::Established<super::TcpStream>>();
    fn send<T: Send + Sync>() {}
    send::<AnyListener>();
    send::<AnyConnector>();
}

impl Conn {
    /// [`Conn::plain`] for a raw `TcpStream`, so `echo_once` can mix the two backends.
    fn plain_tcp(stream: super::TcpStream) -> super::Established<super::TcpStream> {
        Conn::Tcp(stream).plain()
    }
}
