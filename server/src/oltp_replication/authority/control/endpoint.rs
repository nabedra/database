//! Strong Control reads share the authenticated worker listener. This endpoint
//! grants no raw Raft proposal or caller-supplied proof capability.
use super::{ControlHandle, GroupRecord};
use crate::{
    oltp::pool::{ByteClass, ChargedBytes, WorkerSharedResources},
    oltp_replication::{multiplex::*, types::*},
    oltp_runtime::{
        replica::{QueueAuthority, QueueScope},
        worker_endpoint::AuthorityReader,
    },
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

const HEADER_LIMIT: usize = 4096;
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum ReadRequest {
    Current,
    TerminalReader {
        remote: WorkerIdentity,
        timestamps_group_id: Uuid,
    },
    Workers {
        revision: u64,
        after: Option<Uuid>,
    },
    Ready {
        expected: ReadyMembership,
        after: Option<Uuid>,
    },
    Groups {
        revision: u64,
        after: Option<Uuid>,
    },
    GroupMembers {
        expected: Box<GroupRecord>,
        after: Option<u64>,
    },
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadReply {
    nonce: Uuid,
    bytes: u64,
}

pub(crate) struct ControlEndpoint {
    handle: Arc<ControlHandle>,
    shared: Arc<WorkerSharedResources>,
}
impl ControlEndpoint {
    pub(crate) fn new(
        handle: Arc<ControlHandle>,
        shared: Arc<WorkerSharedResources>,
    ) -> Result<Arc<Self>, ProtocolError> {
        if !Arc::ptr_eq(handle.resources(), &shared) {
            return Err(invalid("Control endpoint has a foreign worker budget"));
        }
        Ok(Arc::new(Self { handle, shared }))
    }
    fn parse(
        &self,
        session: &AuthenticatedSession,
        header: &[u8],
    ) -> Result<ReadRequest, ProtocolError> {
        if session.recovery_only()
            || session.purpose() != Purpose::Authority
            || session.group() != self.handle.identity()
            || header.len() > HEADER_LIMIT
        {
            return Err(invalid("invalid Control authority session"));
        }
        crate::protocol::json_input(header, HEADER_LIMIT)
            .map_err(|_| invalid("invalid Control authority header"))?;
        serde_json::from_slice(header).map_err(|_| invalid("invalid Control authority request"))
    }
}
impl Endpoint for ControlEndpoint {
    fn preflight<'a>(
        &'a self,
        session: &'a AuthenticatedSession,
        header: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        Box::pin(async move {
            self.parse(session, header)?;
            // No payload or fresh recursive Control lookup is needed to read
            // Control itself. TLS authenticates locally applied enrollment.
            Ok(ReceivePlan {
                class: TrafficClass::Authority,
                section_lengths: [0; 3],
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
            let request = self.parse(&session, header.as_ref())?;
            if (0..3).any(|i| sections.section(i).is_some()) {
                return Err(invalid("Control read has unexpected payload"));
            }
            let scope = QueueScope {
                cluster_id: session.group().cluster_id,
                database: session.group().database.clone(),
            };
            let bytes = match request {
                ReadRequest::Current => {
                    let current = self.handle.current(&scope).await?;
                    self.shared
                        .encode_in(ByteClass::Progress, &current, HEADER_LIMIT)?
                }
                ReadRequest::TerminalReader {
                    remote,
                    timestamps_group_id,
                } => {
                    let current = self
                        .handle
                        .terminal_reader_view(&scope, remote, timestamps_group_id)
                        .await?;
                    self.shared
                        .encode_in(ByteClass::Progress, &current, HEADER_LIMIT)?
                }
                ReadRequest::Workers { revision, after } => {
                    self.handle.workers(revision, after).await?
                }
                ReadRequest::Ready { expected, after } => {
                    self.handle.ready_page_read(expected, after).await?
                }
                ReadRequest::Groups { revision, after } => {
                    self.handle.groups(revision, after).await?
                }
                ReadRequest::GroupMembers { expected, after } => {
                    self.handle.group_members(*expected, after).await?
                }
            };
            let reply = self.shared.encode_in(
                ByteClass::Progress,
                &ReadReply {
                    nonce: session.nonce(),
                    bytes: bytes.len() as u64,
                },
                HEADER_LIMIT,
            )?;
            Ok(ChargedReply {
                header: reply,
                sections: [Some(bytes), None, None],
            })
        })
    }
}

/// The manager supplies its current locally applied permanent Control voters.
/// Candidates are paged/cursor based; no total-cluster-size cap is introduced.
pub(crate) trait ControlRoutes: Send + Sync + 'static {
    fn preferred(&self) -> Option<Uuid> {
        None
    }
    fn next(&self, after: Option<Uuid>) -> Result<Option<Uuid>, ProtocolError>;
}
pub(crate) struct ControlClient {
    client: WorkerClient,
    shared: Arc<WorkerSharedResources>,
    group: GroupIdentity,
    routes: Arc<dyn ControlRoutes>,
}
impl ControlClient {
    pub(crate) fn new(
        client: WorkerClient,
        shared: Arc<WorkerSharedResources>,
        group: GroupIdentity,
        routes: Arc<dyn ControlRoutes>,
    ) -> Result<Arc<Self>, ProtocolError> {
        group
            .validate()
            .map_err(|_| invalid("invalid Control RPC group"))?;
        if group.kind != GroupKind::Control {
            return Err(invalid("wrong Control RPC group kind"));
        }
        Ok(Arc::new(Self {
            client,
            shared,
            group,
            routes,
        }))
    }
    async fn read(&self, request: &ReadRequest) -> Result<ReceivedReply, ProtocolError> {
        let header = self
            .shared
            .encode_in(ByteClass::Progress, request, HEADER_LIMIT)?;
        let preferred = self.routes.preferred();
        let mut first = preferred;
        let mut after = None;
        let mut error = unavailable();
        loop {
            let worker = if let Some(worker) = first.take() {
                worker
            } else {
                let Some(worker) = self.routes.next(after)? else {
                    break;
                };
                if after.is_some_and(|previous| worker <= previous) {
                    return Err(invalid("Control routes are not strictly ordered"));
                }
                after = Some(worker);
                if Some(worker) == preferred {
                    continue;
                }
                worker
            };
            if worker.is_nil() {
                return Err(invalid("nil Control route"));
            }
            match self
                .client
                .request(
                    &self.group,
                    worker,
                    Purpose::Authority,
                    header.clone(),
                    [None, None, None],
                    64 << 10,
                )
                .await
            {
                Ok(reply) => {
                    if reply.remote_identity().worker_id != worker
                        || reply.group() != &self.group
                        || reply.purpose() != Purpose::Authority
                        || reply.sections[1].is_some()
                        || reply.sections[2].is_some()
                    {
                        return Err(invalid("Control response transport binding differs"));
                    }
                    {
                        let decoded = reply.decode_header::<ReadReply>()?;
                        if decoded.nonce != reply.nonce()
                            || reply.sections[0].as_ref().map(|bytes| bytes.len() as u64)
                                != Some(decoded.bytes)
                        {
                            return Err(invalid("Control response payload differs"));
                        }
                    }
                    return Ok(reply);
                }
                Err(failure) => error = failure,
            }
        }
        Err(error)
    }
    async fn authority(
        &self,
        scope: &QueueScope,
        request: ReadRequest,
    ) -> Result<QueueAuthority, ProtocolError> {
        if scope.cluster_id != self.group.cluster_id || scope.database != self.group.database {
            return Err(invalid("foreign Control query scope"));
        }
        let reply = self.read(&request).await?;
        let current = reply.decode_section::<QueueAuthority>(0)?;
        current
            .ready
            .view
            .validate()
            .map_err(|_| invalid("invalid Control ready view"))?;
        current
            .owner
            .validate()
            .map_err(|_| invalid("invalid Control owner"))?;
        if current.ready.ready_count == 0 {
            return Err(unavailable());
        }
        Ok(*current)
    }
    pub(crate) async fn ready_page(
        &self,
        expected: ReadyMembership,
        after: Option<Uuid>,
    ) -> Result<ReceivedReply, ProtocolError> {
        self.read(&ReadRequest::Ready { expected, after }).await
    }
}
impl AuthorityReader for ControlClient {
    fn current<'a>(&'a self, scope: &'a QueueScope) -> EndpointFuture<'a, QueueAuthority> {
        Box::pin(self.authority(scope, ReadRequest::Current))
    }
    fn terminal_reader_view<'a>(
        &'a self,
        scope: &'a QueueScope,
        remote: WorkerIdentity,
        timestamps_group_id: Uuid,
    ) -> EndpointFuture<'a, QueueAuthority> {
        Box::pin(self.authority(
            scope,
            ReadRequest::TerminalReader {
                remote,
                timestamps_group_id,
            },
        ))
    }
}
fn invalid(message: &'static str) -> ProtocolError {
    ProtocolError::new(ErrorCode::ValidationError, message, Outcome::NotApplied)
}
fn unavailable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "Control quorum is unavailable",
        Outcome::NotApplied,
    )
}
