//! API ingress is a Control-enrolled service, never a queue holder or voter.
//! The current owner authenticates the original bearer and database again.
use super::{http_runtime::HttpRuntime, replica::QueueScope, worker_runtime::WorkerRuntime};
use crate::{
    oltp::pool::{ByteClass, ChargedBytes, ExecutionClass, WorkerSharedResources},
    oltp_replication::{
        authority::control::{ControlClient, ServiceAction},
        multiplex::*,
        types::*,
    },
    protocol::{ErrorCode, Outcome, ProtocolError},
    request_queue::{RequestRecord, RequestState},
};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Weak};
use uuid::Uuid;

// Keep small observations on the progress lane. Larger bodies use ApiIngress.
pub(crate) const INLINE_RECEIPT_BYTES: usize = 32 << 10;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ApplicationObservation {
    pub record: RequestRecord,
    pub response_hash: Option<Digest>,
    #[serde(default)]
    pub durable_proof: bool,
}
pub(crate) struct ObservedApplication {
    pub metadata: ApplicationObservation,
    pub body: Option<ChargedBytes>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum Method {
    Submit {
        bytes: u64,
    },
    Observe {
        run_id: Uuid,
        request_id: Uuid,
        #[serde(default)]
        include_result: bool,
    },
    Result {
        observed: Box<ApplicationObservation>,
    },
    Stats,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    bearer: String,
    database: Text<128>,
    method: Method,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplyHeader {
    nonce: Uuid,
    bytes: u64,
    #[serde(default, skip_serializing_if = "zero")]
    inline_bytes: u64,
    error: Option<ProtocolError>,
}

fn zero(value: &u64) -> bool {
    *value == 0
}

pub(crate) struct ApplicationEndpoint {
    runtime: Weak<WorkerRuntime>,
}
impl ApplicationEndpoint {
    pub(crate) fn new(runtime: &Arc<WorkerRuntime>) -> Arc<Self> {
        Arc::new(Self {
            runtime: Arc::downgrade(runtime),
        })
    }
    fn parse(
        &self,
        session: &AuthenticatedSession,
        bytes: &[u8],
    ) -> Result<(Arc<WorkerRuntime>, Header), ProtocolError> {
        if session.recovery_only() || bytes.len() > MAX_HEADER_BYTES {
            return Err(invalid());
        }
        crate::protocol::json_input(bytes, MAX_HEADER_BYTES).map_err(|_| invalid())?;
        let header: Header = serde_json::from_slice(bytes).map_err(|_| invalid())?;
        let data = matches!(header.method, Method::Submit { .. } | Method::Result { .. });
        if session.purpose()
            != if data {
                Purpose::ApiIngress
            } else {
                Purpose::ApiObservation
            }
        {
            return Err(invalid());
        }
        let runtime = self.runtime.upgrade().ok_or_else(unavailable)?;
        runtime.authorize_application(session.group(), &header.bearer, &header.database)?;
        Ok((runtime, header))
    }
}
impl Endpoint for ApplicationEndpoint {
    fn preflight<'a>(
        &'a self,
        session: &'a AuthenticatedSession,
        bytes: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        Box::pin(async move {
            let (runtime, header) = self.parse(session, bytes)?;
            let bytes = if let Method::Submit { bytes } = header.method {
                if bytes == 0 || bytes > crate::protocol::MAX_BODY_BYTES as u64 {
                    return Err(invalid());
                }
                runtime.authorize_api(session.remote()).await?;
                bytes
            } else {
                0
            };
            Ok(ReceivePlan {
                class: if session.purpose() == Purpose::ApiIngress {
                    TrafficClass::ApiIngress
                } else {
                    TrafficClass::ApiObservation
                },
                section_lengths: [bytes, 0, 0],
                decode_budget_bytes: 0,
            })
        })
    }
    fn handle<'a>(
        &'a self,
        session: AuthenticatedSession,
        header: ChargedBytes,
        sections: ChargedSections,
    ) -> EndpointFuture<'a, ChargedReply> {
        Box::pin(async move {
            let (runtime, request) = self.parse(&session, header.as_ref())?;
            // Every action checks current authority. Submit also checks before
            // it can reserve or transfer its request body.
            runtime.authorize_api(session.remote()).await?;
            let shared = runtime.resources().clone();
            let data = session.purpose() == Purpose::ApiIngress;
            let result: Result<(ChargedBytes, Option<ChargedBytes>), ProtocolError> = async {
                match request.method {
                    Method::Submit { bytes } => {
                        let body = sections.section(0).ok_or_else(invalid)?.clone();
                        if body.len() as u64 != bytes
                            || sections.section(1).is_some()
                            || sections.section(2).is_some()
                        {
                            return Err(invalid());
                        }
                        let record = runtime.submit_charged(body, session.remote()).await?;
                        Ok((shared.encode_in(ByteClass::Data, &record, 16 << 10)?, None))
                    }
                    Method::Observe {
                        run_id,
                        request_id,
                        include_result,
                    } => {
                        if (0..3).any(|i| sections.section(i).is_some()) {
                            return Err(invalid());
                        }
                        let observed = runtime
                            .application_observation(run_id, request_id, include_result)
                            .await?;
                        Ok((
                            shared.encode_in(ByteClass::Progress, &observed.metadata, 16 << 10)?,
                            observed.body,
                        ))
                    }
                    Method::Result { observed } => {
                        if (0..3).any(|i| sections.section(i).is_some()) {
                            return Err(invalid());
                        }
                        Ok((runtime.application_result(&observed).await?, None))
                    }
                    Method::Stats => {
                        if (0..3).any(|i| sections.section(i).is_some()) {
                            return Err(invalid());
                        }
                        let response = runtime.stats().await?;
                        Ok((
                            shared.copy_bytes_in(ByteClass::Progress, &response.body)?,
                            None,
                        ))
                    }
                }
            }
            .await;
            let (body, inline, error) = match result {
                Ok((body, inline)) => (Some(body), inline, None),
                Err(error) => (None, None, Some(error)),
            };
            let reply = ReplyHeader {
                nonce: session.nonce(),
                bytes: body.as_ref().map_or(0, |b| b.len() as u64),
                inline_bytes: inline.as_ref().map_or(0, |b| b.len() as u64),
                error,
            };
            Ok(ChargedReply {
                header: shared.encode_in(
                    if data {
                        ByteClass::Data
                    } else {
                        ByteClass::Progress
                    },
                    &reply,
                    MAX_HEADER_BYTES,
                )?,
                sections: [body, inline, None],
            })
        })
    }
}

pub(crate) struct ApplicationClient {
    shared: Arc<WorkerSharedResources>,
    transport: WorkerClient,
    control: Arc<ControlClient>,
    group: GroupIdentity,
    bearer: String,
    _metadata: crate::oltp::pool::ScratchLease,
}
impl ApplicationClient {
    pub(crate) fn new(
        shared: Arc<WorkerSharedResources>,
        transport: WorkerClient,
        control: Arc<ControlClient>,
        group: GroupIdentity,
        bearer: String,
    ) -> Result<Arc<Self>, ProtocolError> {
        if group.kind != GroupKind::Control || bearer.is_empty() || bearer.len() > 4096 {
            return Err(invalid());
        }
        let metadata = shared.reserve_scratch(
            ByteClass::Progress,
            bearer.capacity() + group.database.capacity_bytes() + 4096,
        )?;
        Ok(Arc::new(Self {
            shared,
            transport,
            control,
            group,
            bearer,
            _metadata: metadata,
        }))
    }
    async fn call(
        &self,
        method: Method,
        body: Option<ChargedBytes>,
    ) -> Result<ReceivedReply, ProtocolError> {
        use super::worker_endpoint::AuthorityReader;
        let scope = QueueScope {
            cluster_id: self.group.cluster_id,
            database: self.group.database.clone(),
        };
        let current = self.control.current(&scope).await?;
        let accepts_inline = matches!(
            method,
            Method::Observe {
                include_result: true,
                ..
            }
        );
        let data = matches!(method, Method::Submit { .. } | Method::Result { .. });
        let purpose = if data {
            Purpose::ApiIngress
        } else {
            Purpose::ApiObservation
        };
        let header = self.shared.encode_in(
            ByteClass::Progress,
            &Header {
                bearer: self.bearer.clone(),
                database: self.group.database.clone(),
                method,
            },
            MAX_HEADER_BYTES,
        )?;
        let reply = self
            .transport
            .request(
                &self.group,
                current.owner.worker_id,
                purpose,
                header,
                [body, None, None],
                32 << 10,
            )
            .await?;
        if reply.remote_identity() != current.owner
            || reply.group() != &self.group
            || reply.purpose() != purpose
            || (!accepts_inline && reply.sections[1].is_some())
            || reply.sections[2].is_some()
        {
            return Err(invalid());
        }
        let observed = reply.decode_header::<ReplyHeader>()?;
        if observed.nonce != reply.nonce()
            || observed.bytes != reply.sections[0].as_ref().map_or(0, |b| b.len() as u64)
            || observed.inline_bytes != reply.sections[1].as_ref().map_or(0, |b| b.len() as u64)
            || observed.inline_bytes > INLINE_RECEIPT_BYTES as u64
        {
            return Err(invalid());
        }
        if let Some(error) = &observed.error {
            return Err(error.clone());
        }
        drop(observed);
        Ok(reply)
    }
    pub(crate) async fn submit(&self, body: ChargedBytes) -> Result<RequestRecord, ProtocolError> {
        let reply = self
            .call(
                Method::Submit {
                    bytes: body.len() as u64,
                },
                Some(body),
            )
            .await?;
        let record = reply.decode_section::<RequestRecord>(0)?;
        if !record.accepted
            || !record.body.is_empty()
            || record.pinned_catalog.is_some()
            || record.response_body.is_some()
            || !record.replicas.is_empty()
        {
            return Err(invalid());
        }
        Ok(record.clone())
    }
    pub(crate) async fn observe(
        &self,
        run_id: Uuid,
        request_id: Uuid,
    ) -> Result<ObservedApplication, ProtocolError> {
        let reply = self
            .call(
                Method::Observe {
                    run_id,
                    request_id,
                    include_result: true,
                },
                None,
            )
            .await?;
        let observed = reply.decode_section::<ApplicationObservation>(0)?;
        if observed.record.run_id != run_id
            || observed.record.request_id != request_id
            || Digest::parse(&observed.record.digest).is_err()
            || !observed.record.accepted
            || observed.record.result_expired
            || observed.record.context.is_some()
            || !observed.record.body.is_empty()
            || observed.record.pinned_catalog.is_some()
            || observed.record.response_body.is_some()
            || observed.record.state.terminal() != observed.response_hash.is_some()
            || observed.record.state.terminal() != observed.record.response_status.is_some()
            || (!observed.record.state.terminal() && reply.sections[1].is_some())
        {
            return Err(invalid());
        }
        Ok(ObservedApplication {
            metadata: observed.clone(),
            body: reply.sections[1].clone(),
        })
    }
    pub(crate) async fn stats(&self) -> Result<ChargedBytes, ProtocolError> {
        let reply = self.call(Method::Stats, None).await?;
        reply.sections[0].as_ref().cloned().ok_or_else(invalid)
    }
    pub(crate) async fn result(
        &self,
        observed: ObservedApplication,
    ) -> Result<ChargedBytes, ProtocolError> {
        if !observed.metadata.record.state.terminal() {
            return Err(invalid());
        }
        let hash = observed.metadata.response_hash.ok_or_else(invalid)?;
        if let Some(body) = observed.body {
            verify_response_hash(&body, hash)?;
            return Ok(body);
        }
        let reply = self
            .call(
                Method::Result {
                    observed: Box::new(observed.metadata),
                },
                None,
            )
            .await?;
        let body = reply.sections[0].clone().ok_or_else(invalid)?;
        let held = body.clone();
        self.shared
            .execute(ExecutionClass::Query, 0, 0, move || {
                verify_response_hash(&held, hash)
            })
            .await?;
        Ok(body)
    }
}
fn verify_response_hash(body: &ChargedBytes, expected: Digest) -> Result<(), ProtocolError> {
    use sha2::{Digest as _, Sha256};
    if Digest::from_bytes(Sha256::digest(body.as_ref()).into()) != expected {
        return Err(invalid());
    }
    Ok(())
}
fn invalid() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::ValidationError,
        "invalid authenticated API request",
        Outcome::NotApplied,
    )
}
fn unavailable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "worker API authority unavailable",
        Outcome::Unknown,
    )
}

#[cfg(test)]
#[test]
fn c07_m02_q04_observation_opt_in_checks_exact_bytes() {
    use sha2::{Digest as _, Sha256};

    let run = Uuid::new_v4();
    let id = Uuid::new_v4();
    let legacy = serde_json::json!({"observe": {"run_id": run, "request_id": id}});
    assert!(matches!(
        serde_json::from_value::<Method>(legacy).unwrap(),
        Method::Observe {
            include_result: false,
            ..
        }
    ));
    let header = ReplyHeader {
        nonce: run,
        bytes: 20,
        inline_bytes: 0,
        error: None,
    };
    assert!(
        serde_json::to_value(header)
            .unwrap()
            .get("inline_bytes")
            .is_none()
    );
    let shared = WorkerSharedResources::new(
        1,
        crate::config::WorkerResources::default(),
        crate::oltp::pool::SharedResourceConfig::default(),
    )
    .unwrap();
    let exact = shared
        .copy_bytes_in(ByteClass::Progress, b"result=2")
        .unwrap();
    let altered = shared
        .copy_bytes_in(ByteClass::Progress, b"result=3")
        .unwrap();
    let expected = Digest::from_bytes(Sha256::digest(exact.as_ref()).into());
    verify_response_hash(&exact, expected).unwrap();
    assert_eq!(
        verify_response_hash(&altered, expected).unwrap_err().code,
        ErrorCode::ValidationError
    );
}
