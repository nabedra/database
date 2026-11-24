//! Hyper's HTTP connector with the simulated runtime backend when a world is active.

use std::time::Duration;

#[cfg(not(feature = "sim"))]
pub(crate) use hyper_util::client::legacy::connect::HttpConnector;
#[cfg(feature = "sim")]
pub(crate) use simulated::HttpConnector;

#[inline]
pub(crate) fn new(timeout: Duration) -> HttpConnector {
    let mut tcp = hyper_util::client::legacy::connect::HttpConnector::new();
    tcp.enforce_http(false);
    tcp.set_connect_timeout(Some(timeout));
    #[cfg(not(feature = "sim"))]
    {
        tcp
    }
    #[cfg(feature = "sim")]
    {
        simulated::HttpConnector { tcp, timeout }
    }
}

#[cfg(feature = "sim")]
mod simulated {
    use std::{
        error::Error,
        future::Future,
        io::{self, IoSlice},
        pin::Pin,
        task::{Context, Poll},
        time::Duration,
    };

    use hyper::Uri;
    use hyper_util::{
        client::legacy::connect::{Connected, Connection, HttpConnector as TcpConnector},
        rt::TokioIo,
    };
    use nabedra_rt::net::{self, AnyConnector, Connector, Established, Target, TcpStream};
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
    use tower_service::Service;

    /// Preserve the original Hyper resolver, socket options and address racing
    /// whenever a simulation-enabled test dials on the real backend.
    #[derive(Clone)]
    pub(crate) struct HttpConnector {
        pub(super) tcp: TcpConnector,
        pub(super) timeout: Duration,
    }

    impl Service<Uri> for HttpConnector {
        type Response = TokioIo<HttpStream>;
        type Error = Box<dyn Error + Send + Sync>;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.tcp.poll_ready(cx).map_err(Into::into)
        }

        fn call(&mut self, uri: Uri) -> Self::Future {
            if nabedra_rt::sim::world().is_none() {
                let connect = self.tcp.call(uri);
                return Box::pin(async move {
                    let stream = connect.await?.into_inner();
                    Ok(TokioIo::new(HttpStream(stream.into())))
                });
            }

            let timeout = self.timeout;
            Box::pin(async move {
                let host = uri
                    .host()
                    .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
                let host = host
                    .strip_prefix('[')
                    .and_then(|ip| ip.strip_suffix(']'))
                    .unwrap_or(host);
                let port = uri
                    .port_u16()
                    .unwrap_or(if uri.scheme_str() == Some("https") {
                        443
                    } else {
                        80
                    });
                let stream = nabedra_rt::time::timeout(
                    timeout,
                    AnyConnector.connect(Target::HostPort(host, port)),
                )
                .await??;
                Ok(TokioIo::new(HttpStream(net::Conn::plain(stream))))
            })
        }
    }

    pub(crate) struct HttpStream(Established<TcpStream>);

    impl Connection for HttpStream {
        fn connected(&self) -> Connected {
            match &self.0 {
                Established::Real(stream) => stream.connected(),
                Established::Sim(_) => Connected::new(),
            }
        }
    }

    impl AsyncRead for HttpStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.0).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for HttpStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Pin::new(&mut self.0).poll_write(cx, buf)
        }

        fn poll_write_vectored(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            match &mut self.0 {
                Established::Real(stream) => Pin::new(stream).poll_write_vectored(cx, bufs),
                Established::Sim(stream) => Pin::new(stream).poll_write_vectored(cx, bufs),
            }
        }

        fn is_write_vectored(&self) -> bool {
            matches!(&self.0, Established::Real(stream) if stream.is_write_vectored())
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.0).poll_flush(cx)
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.0).poll_shutdown(cx)
        }
    }
}
