//! The network backend for authenticated peer connections.
//!
//! The runtime selects the installed simulated network when a world is active.
//! Otherwise the same entry points use TCP and mutual TLS.

use std::{io, net::SocketAddr, time::Duration};

use nabedra_rt::net::{self, AnyConnector, Connector as _, Established, TcpStream};
use rustls::pki_types::{CertificateDer, ServerName};
use sha2::{Digest, Sha256};
use tokio_rustls::{TlsAcceptor, TlsConnector, client, server};

pub use nabedra_rt::net::{AnyListener as Listener, Conn, Handshake, Target};

pub type ClientStream = Established<client::TlsStream<TcpStream>>;
pub type ServerStream = Established<server::TlsStream<TcpStream>>;

/// Selects networking from the current runtime context.
#[derive(Clone, Copy)]
pub struct Backend {
    connector: AnyConnector,
}

#[inline]
pub fn current() -> Backend {
    Backend {
        connector: AnyConnector,
    }
}

impl Backend {
    #[inline]
    pub async fn bind(self, address: SocketAddr) -> io::Result<Listener> {
        Listener::bind(address).await
    }

    /// Bind a configured worker endpoint, including a hostname and port.
    pub async fn bind_endpoint(self, endpoint: &str) -> io::Result<Listener> {
        #[cfg(feature = "sim")]
        if nabedra_rt::sim::world().is_some() {
            if let Ok(addr) = endpoint.parse() {
                return self.bind(addr).await;
            }
            let (host, port) = endpoint
                .rsplit_once(':')
                .ok_or(io::ErrorKind::InvalidInput)?;
            let port = port.parse().map_err(|_| io::ErrorKind::InvalidInput)?;
            let addresses = self.resolve(host, port)?;
            let mut error = io::ErrorKind::AddrNotAvailable.into();
            for address in addresses {
                match self.bind(address).await {
                    Ok(listener) => return Ok(listener),
                    Err(failed) => error = failed,
                }
            }
            return Err(error);
        }
        net::TcpListener::bind(endpoint).await.map(Listener::from)
    }

    #[inline]
    pub async fn dial(self, target: Target<'_>) -> io::Result<Conn> {
        self.connector.connect(target).await
    }

    #[inline]
    pub fn resolve(self, host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
        net::resolve(host, port)
    }
}

#[derive(Clone)]
pub struct Connector(pub(crate) TlsConnector);

impl Connector {
    #[inline]
    pub async fn connect(
        &self,
        target: Target<'_>,
        name: ServerName<'static>,
    ) -> io::Result<(ClientStream, Handshake)> {
        self.handshake(name, current().dial(target).await?).await
    }

    /// Authenticate a connection that was already dialed. Callers can record
    /// address resolution, dialing, and the handshake as distinct phases.
    #[inline]
    pub async fn handshake(
        &self,
        name: ServerName<'static>,
        conn: Conn,
    ) -> io::Result<(ClientStream, Handshake)> {
        match conn {
            Conn::Tcp(socket) => {
                tune_socket(&socket);
                let stream = self.0.connect(name, socket).await?;
                let tls = stream.get_ref().1;
                let proof = proof(tls.alpn_protocol(), tls.peer_certificates())?;
                #[cfg(feature = "sim")]
                let stream = Established::Real(stream);
                Ok((stream, proof))
            }
            #[cfg(feature = "sim")]
            Conn::Sim(pending) => {
                let offers: Vec<_> = self
                    .0
                    .config()
                    .alpn_protocols
                    .iter()
                    .map(Vec::as_slice)
                    .collect();
                let (stream, proof) = pending.client_handshake(&offers).await?;
                Ok((Established::Sim(stream), proof))
            }
        }
    }
}

#[derive(Clone)]
pub struct Acceptor(pub(crate) TlsAcceptor);

impl Acceptor {
    #[inline]
    pub async fn handshake(&self, conn: Conn) -> io::Result<(ServerStream, Handshake)> {
        match conn {
            Conn::Tcp(socket) => {
                tune_socket(&socket);
                let stream = self.0.accept(socket).await?;
                let tls = stream.get_ref().1;
                let proof = proof(tls.alpn_protocol(), tls.peer_certificates())?;
                #[cfg(feature = "sim")]
                let stream = Established::Real(stream);
                Ok((stream, proof))
            }
            #[cfg(feature = "sim")]
            Conn::Sim(pending) => {
                let offers: Vec<_> = self
                    .0
                    .config()
                    .alpn_protocols
                    .iter()
                    .map(Vec::as_slice)
                    .collect();
                let (stream, proof) = pending.server_handshake(&offers).await?;
                Ok((Established::Sim(stream), proof))
            }
        }
    }
}

fn proof(
    alpn: Option<&[u8]>,
    certificates: Option<&[CertificateDer<'_>]>,
) -> io::Result<Handshake> {
    let certificate = certificates
        .and_then(|certificates| certificates.first())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing worker certificate"))?;
    Ok(Handshake {
        alpn: alpn.map(<[u8]>::to_vec),
        peer_certificate_sha256: Sha256::digest(certificate.as_ref()).into(),
    })
}

/// Keepalive also detects failed peers on idle connections.
#[inline]
fn tune_socket(socket: &TcpStream) {
    let _ = socket.set_nodelay(true);
    let keepalive = socket2::TcpKeepalive::new().with_time(Duration::from_secs(10));
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    let keepalive = keepalive.with_interval(Duration::from_secs(5));
    let _ = socket2::SockRef::from(socket).set_tcp_keepalive(&keepalive);
}
