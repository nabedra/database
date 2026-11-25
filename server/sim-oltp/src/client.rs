//! HTTP/1 requests through the simulated listener and the real API router.

use std::time::Duration;

use http_body_util::{BodyExt, Full};
use hyper::{Request, body::Bytes};
use nabedra_protocol::protocol::ProtocolError;
use nabedra_rt::{sim::in_node, time::timeout};
use nabedra_transport::backend::Target;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    bootstrap::{DATABASE, http_addr},
    history::Completion,
    net::HttpConnection,
    workloads::resources,
};

pub(crate) struct Reply {
    pub status: u16,
    pub body: Value,
}

impl Reply {
    pub fn completion(self, request_id: Uuid) -> Result<Completion, String> {
        if self.status == 200 {
            if self.body["request_id"] != json!(request_id) {
                return Err(format!(
                    "HTTP success has the wrong request id: {}",
                    self.body
                ));
            }
            return self
                .body
                .get("data")
                .cloned()
                .map(Completion::Ok)
                .ok_or_else(|| format!("HTTP success has no data field: {}", self.body));
        }
        let error: ProtocolError =
            serde_json::from_value(self.body["error"].clone()).map_err(|error| {
                format!(
                    "HTTP {} invalid error envelope: {} ({error})",
                    self.status, self.body
                )
            })?;
        if self.status != error.status_code() {
            return Err(format!(
                "HTTP {} disagrees with protocol error: {}",
                self.status, self.body
            ));
        }
        Ok(Completion::Refused(error))
    }
}

pub(crate) async fn request(
    client: u64,
    gateway: u64,
    path: &str,
    token: &str,
    body: Option<Value>,
    deadline: Duration,
) -> Result<Reply, String> {
    let addr = http_addr(gateway);
    let cut = resources::cut(client);
    let call = in_node(client, async move {
        timeout(deadline, async {
            let mut connection = HttpConnection::connect(Target::Addr(addr))
                .await
                .map_err(|err| format!("connect n{gateway}: {err}"))?;
            let payload = body
                .as_ref()
                .map(serde_json::to_vec)
                .transpose()
                .map_err(|err| format!("encode request: {err}"))?
                .unwrap_or_default();
            let request = Request::builder()
                .method(if body.is_some() { "POST" } else { "GET" })
                .uri(path)
                .header("host", addr.to_string())
                .header("authorization", format!("Bearer {token}"))
                .header("x-nabedra-database", DATABASE)
                .header("content-type", "application/json")
                .body(Full::new(Bytes::from(payload)))
                .map_err(|err| format!("build request: {err}"))?;
            let response = connection
                .send(request)
                .await
                .map_err(|err| format!("HTTP reply: {err}"))?;
            let status = response.status().as_u16();
            let bytes = response
                .into_body()
                .collect()
                .await
                .map_err(|err| format!("HTTP response body: {err}"))?
                .to_bytes();
            let body = serde_json::from_slice(&bytes)
                .map_err(|err| format!("HTTP {status} JSON response: {err}"))?;
            Ok(Reply { status, body })
        })
        .await
        .map_err(|_| format!("HTTP n{gateway} {path} deadline"))?
    });
    match cut {
        None => call.await,
        Some(cut) => cut
            .run(call)
            .await
            .unwrap_or_else(|| Err(resources::CANCELLED.into())),
    }
}

pub(crate) fn operation(id: Uuid, name: &str, payload: Value) -> Value {
    json!({"request_id": id, "operation": name, "payload": payload})
}
