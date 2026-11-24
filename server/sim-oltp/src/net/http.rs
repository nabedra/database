use std::io;

use http_body_util::Full;
use hyper::{
    Request, Response,
    body::{Bytes, Incoming},
    client::conn::http1::{self, SendRequest},
};
use hyper_util::rt::TokioIo;
use nabedra_transport::backend::{self, Target};

/// A Hyper HTTP/1 client on the runtime connector. Construct one in the
/// simulated client's node context and reuse it for sequential requests.
pub struct HttpConnection {
    sender: SendRequest<Full<Bytes>>,
    driver: tokio::task::JoinHandle<()>,
}

impl HttpConnection {
    pub async fn connect(target: Target<'_>) -> io::Result<Self> {
        let stream = backend::current().dial(target).await?.plain();
        let (sender, connection) = http1::handshake(TokioIo::new(stream))
            .await
            .map_err(io::Error::other)?;
        let driver = nabedra_rt::task::spawn(async move {
            let _ = connection.await;
        });
        Ok(Self { sender, driver })
    }

    pub async fn send(
        &mut self,
        request: Request<Full<Bytes>>,
    ) -> Result<Response<Incoming>, hyper::Error> {
        self.sender.ready().await?;
        self.sender.send_request(request).await
    }
}

impl Drop for HttpConnection {
    fn drop(&mut self) {
        self.driver.abort();
    }
}
