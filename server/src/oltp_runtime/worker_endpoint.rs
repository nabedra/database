//! Queue RPCs on the shared worker mTLS multiplexer. No application endpoint is
//! enabled by this module; production composition still needs the typed backend
//! and committed Control join/run authority.
use super::replica::*;
use crate::{
    oltp::pool::{ByteClass, ExecutionClass, WorkerSharedResources},
    oltp_replication::{multiplex::*, types::*},
    protocol::{ErrorCode, Outcome, ProtocolError},
    request_queue::{QueueError, RequestState},
};
use serde::{Deserialize, Serialize};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use uuid::Uuid;

/// Implementations use the separate Authority transport lane. They must not
/// acquire queue-data/control body permits or recursively authenticate Control
/// consensus through another strong Control lookup.
pub trait AuthorityReader: Send + Sync + 'static {
    fn current<'a>(&'a self, scope: &'a QueueScope) -> EndpointFuture<'a, QueueAuthority>;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum QueueRequest {
    Install {
        record: RecordHeader,
    },
    JoinPage {
        scope: QueueScope,
        view: AuthorityView,
        page: Box<JoinPageHeader>,
        record: Option<RecordHeader>,
    },
    State {
        scope: QueueScope,
        view: AuthorityView,
        request_id: Uuid,
    },
    Result {
        scope: QueueScope,
        view: AuthorityView,
        request_id: Uuid,
        version: u64,
    },
}
impl QueueRequest {
    fn scope(&self) -> &QueueScope {
        match self {
            Self::Install { record } => &record.scope,
            Self::State { scope, .. }
            | Self::Result { scope, .. }
            | Self::JoinPage { scope, .. } => scope,
        }
    }
    fn view(&self) -> AuthorityView {
        match self {
            Self::Install { record } => record.ready.view,
            Self::State { view, .. } | Self::Result { view, .. } | Self::JoinPage { view, .. } => {
                *view
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueState {
    pub request_id: Uuid,
    pub run_id: Uuid,
    pub version: u64,
    pub state: RequestState,
    pub copies_complete: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum QueueReply {
    Installed {
        worker: WorkerIdentity,
        record: RecordVersion,
        nonce: Uuid,
    },
    Joined {
        worker: WorkerIdentity,
        page: Box<JoinPageHeader>,
        nonce: Uuid,
    },
    State(QueueState),
    Result {
        request_id: Uuid,
        version: u64,
        status: u16,
        response_bytes: u64,
    },
}

pub struct QueueEndpoint {
    replica: Arc<Mutex<WorkerReplica>>,
    shared: Arc<WorkerSharedResources>,
    scope: QueueScope,
    authority: Arc<dyn AuthorityReader>,
    task_memory_bytes: u64,
    next_job: AtomicU64,
    local: WorkerIdentity,
}

#[derive(Clone)]
pub struct QueuePeerClient {
    client: WorkerClient,
    shared: Arc<WorkerSharedResources>,
    group: GroupIdentity,
}
impl QueuePeerClient {
    pub fn new(
        client: WorkerClient,
        shared: Arc<WorkerSharedResources>,
        group: GroupIdentity,
    ) -> Result<Self, ProtocolError> {
        group
            .validate()
            .map_err(|_| malformed("invalid queue group"))?;
        if group.kind != GroupKind::Control {
            return Err(malformed("queue group must be Control"));
        }
        Ok(Self {
            client,
            shared,
            group,
        })
    }
    pub async fn install(
        &self,
        target: WorkerIdentity,
        frame: &RecordFrame,
    ) -> Result<CopyAcknowledgement, ProtocolError> {
        let expected = frame.record_version().map_err(QueueError::protocol)?;
        let header = self.shared.encode(
            &QueueRequest::Install {
                record: frame.decode_header().map_err(QueueError::protocol)?,
            },
            super::replica::MAX_HEADER_BYTES,
        )?;
        let reply = self
            .client
            .request(
                &self.group,
                target.worker_id,
                Purpose::QueueData,
                header,
                frame.sections.clone(),
                4 * super::replica::MAX_HEADER_BYTES,
            )
            .await?;
        if reply.remote_identity() != target
            || reply.group() != &self.group
            || reply.purpose() != Purpose::QueueData
            || reply.sections.iter().any(Option::is_some)
        {
            return Err(malformed("queue ACK transport context differs"));
        }
        let decoded = reply.decode_header::<QueueReply>()?;
        let QueueReply::Installed {
            worker,
            record,
            nonce,
        } = &*decoded
        else {
            return Err(malformed("expected queue installation ACK"));
        };
        if *worker != target || *record != expected || *nonce != reply.nonce() {
            return Err(malformed("queue ACK statement differs"));
        }
        Ok(CopyAcknowledgement::authenticated(*worker, *record))
    }
    pub async fn state(
        &self,
        target: WorkerIdentity,
        scope: QueueScope,
        view: AuthorityView,
        request_id: Uuid,
    ) -> Result<QueueState, ProtocolError> {
        let header = self.shared.encode_in(
            ByteClass::Progress,
            &QueueRequest::State {
                scope,
                view,
                request_id,
            },
            super::replica::MAX_HEADER_BYTES,
        )?;
        let reply = self
            .client
            .request(
                &self.group,
                target.worker_id,
                Purpose::QueueControl,
                header,
                [None, None, None],
                4 * super::replica::MAX_HEADER_BYTES,
            )
            .await?;
        if reply.remote_identity() != target
            || reply.group() != &self.group
            || reply.purpose() != Purpose::QueueControl
            || reply.sections.iter().any(Option::is_some)
        {
            return Err(malformed("queue state transport context differs"));
        }
        let decoded = reply.decode_header::<QueueReply>()?;
        let QueueReply::State(state) = &*decoded else {
            return Err(malformed("expected queue state reply"));
        };
        if state.request_id != request_id || state.run_id != view.run_id {
            return Err(malformed("queue state identity differs"));
        }
        Ok(state.clone())
    }
    pub async fn install_join_page(
        &self,
        target: WorkerIdentity,
        page: &JoinPage,
    ) -> Result<(), ProtocolError> {
        let record = page
            .record
            .as_ref()
            .map(RecordFrame::decode_header)
            .transpose()
            .map_err(QueueError::protocol)?;
        let header = self.shared.encode(
            &QueueRequest::JoinPage {
                scope: QueueScope {
                    cluster_id: self.group.cluster_id,
                    database: self.group.database.clone(),
                },
                view: page.snapshot.ticket.source_ready.view,
                page: Box::new(page.header()),
                record,
            },
            super::replica::MAX_HEADER_BYTES,
        )?;
        let sections = page
            .record
            .as_ref()
            .map_or([None, None, None], |record| record.sections.clone());
        let reply = self
            .client
            .request(
                &self.group,
                target.worker_id,
                Purpose::QueueData,
                header,
                sections,
                0,
            )
            .await?;
        if reply.remote_identity() != target
            || reply.group() != &self.group
            || reply.purpose() != Purpose::QueueData
            || reply.sections.iter().any(Option::is_some)
        {
            return Err(malformed("join ACK transport context differs"));
        }
        if reply.header.len() > super::replica::MAX_HEADER_BYTES {
            return Err(malformed("join ACK exceeds queue header limit"));
        }
        let decode = crate::oltp::mvcc::json_decode_budget(reply.header.as_ref())?;
        let reply = reply.reserve_decode(&self.shared, decode)?;
        let decoded = reply.decode_header::<QueueReply>()?;
        let QueueReply::Joined {
            worker,
            page: copied,
            nonce,
        } = &*decoded
        else {
            return Err(malformed("expected join page ACK"));
        };
        if *worker != target || **copied != page.header() || *nonce != reply.nonce() {
            return Err(malformed("join ACK statement differs"));
        }
        Ok(())
    }
    /// Exact immutable response bytes remain charged through the caller's send.
    pub async fn result(
        &self,
        target: WorkerIdentity,
        scope: QueueScope,
        view: AuthorityView,
        request_id: Uuid,
        version: u64,
    ) -> Result<ReceivedReply, ProtocolError> {
        let header = self.shared.encode_in(
            ByteClass::Progress,
            &QueueRequest::Result {
                scope,
                view,
                request_id,
                version,
            },
            super::replica::MAX_HEADER_BYTES,
        )?;
        let reply = self
            .client
            .request(
                &self.group,
                target.worker_id,
                Purpose::QueueData,
                header,
                [None, None, None],
                4 * super::replica::MAX_HEADER_BYTES,
            )
            .await?;
        if reply.remote_identity() != target
            || reply.group() != &self.group
            || reply.purpose() != Purpose::QueueData
            || reply.sections[1].is_some()
            || reply.sections[2].is_some()
        {
            return Err(malformed("result transport context differs"));
        }
        {
            let decoded = reply.decode_header::<QueueReply>()?;
            let QueueReply::Result {
                request_id: returned,
                version: returned_version,
                status,
                response_bytes,
            } = &*decoded
            else {
                return Err(malformed("expected result response"));
            };
            if *returned != request_id
                || *returned_version != version
                || !(200..600).contains(status)
                || reply.sections[0]
                    .as_ref()
                    .is_none_or(|body| body.len() as u64 != *response_bytes)
            {
                return Err(malformed("result statement differs"));
            }
        }
        Ok(reply)
    }
}
impl QueueEndpoint {
    pub fn new(
        replica: Arc<Mutex<WorkerReplica>>,
        shared: Arc<WorkerSharedResources>,
        scope: QueueScope,
        authority: Arc<dyn AuthorityReader>,
        task_memory_bytes: u64,
    ) -> Result<Arc<Self>, ProtocolError> {
        if task_memory_bytes < super::MAX_DECODED_REQUEST_BYTES as u64 {
            return Err(malformed("queue decode task budget is too small"));
        }
        let guard = replica.lock().map_err(|_| unavailable())?;
        if guard.scope() != &scope {
            return Err(malformed("queue endpoint scope differs"));
        }
        let local = guard.local_identity();
        shared.bind_worker(local.worker_id)?;
        drop(guard);
        Ok(Arc::new(Self {
            replica,
            shared,
            scope,
            authority,
            task_memory_bytes,
            next_job: AtomicU64::new(1),
            local,
        }))
    }

    fn parse(
        &self,
        session: &AuthenticatedSession,
        bytes: &[u8],
    ) -> Result<QueueRequest, ProtocolError> {
        if bytes.len() > super::replica::MAX_HEADER_BYTES
            || session.recovery_only()
            || session.local() != self.local
            || session.group().cluster_id != self.scope.cluster_id
            || session.group().database != self.scope.database
            || session.group().kind != GroupKind::Control
        {
            return Err(malformed("queue session has the wrong scope"));
        }
        crate::protocol::json_input(bytes, super::replica::MAX_HEADER_BYTES)
            .map_err(|_| malformed("invalid queue header bounds"))?;
        let request: QueueRequest =
            serde_json::from_slice(bytes).map_err(|_| malformed("invalid queue RPC header"))?;
        if request.scope() != &self.scope || request.view().validate().is_err() {
            return Err(malformed("queue header has the wrong scope"));
        }
        let permitted = matches!(
            (&request, session.purpose()),
            (QueueRequest::State { .. }, Purpose::QueueControl)
                | (
                    QueueRequest::Install { .. }
                        | QueueRequest::Result { .. }
                        | QueueRequest::JoinPage { .. },
                    Purpose::QueueData
                )
        );
        if !permitted {
            return Err(malformed(
                "queue method is not permitted on this traffic lane",
            ));
        }
        Ok(request)
    }

    async fn check(
        &self,
        session: &AuthenticatedSession,
        request: &QueueRequest,
    ) -> Result<QueueAuthority, ProtocolError> {
        // No queue lock, scheduler CPU grant or payload permit from this module
        // is held while the independent Control authority lookup waits.
        let current = self.authority.current(&self.scope).await?;
        if request.view() != current.ready.view {
            return Err(QueueError::Stale.protocol());
        }
        let record = match request {
            QueueRequest::Install { record } => Some(record),
            QueueRequest::JoinPage { page, record, .. } => {
                if page.snapshot.ticket.source_ready != current.ready
                    || page.snapshot.source != current.owner
                    || page.snapshot.ticket.target != session.local()
                    || page.snapshot.snapshot_id.is_nil()
                    || session.remote() != current.owner
                {
                    return Err(QueueError::Stale.protocol());
                }
                record.as_ref()
            }
            _ => None,
        };
        if let Some(record) = record {
            if session.remote() != current.owner {
                return Err(QueueError::Stale.protocol());
            }
            record
                .preflight(&self.scope, current)
                .map_err(QueueError::protocol)?;
        }
        Ok(current)
    }
    async fn prepare_payload(
        &self,
        session: &AuthenticatedSession,
        current: QueueAuthority,
        record: &RecordHeader,
        sections: &ChargedSections,
    ) -> Result<
        (
            PreparedInstall,
            Arc<crate::oltp::pool::ScratchLease>,
            Arc<Decoded<RecordFrame>>,
        ),
        ProtocolError,
    > {
        let environment = self
            .replica
            .lock()
            .map_err(|_| unavailable())?
            .install_environment(current, session.remote())
            .map_err(QueueError::protocol)?;
        let frame = RecordFrame {
            header: self
                .shared
                .encode(record, super::replica::MAX_HEADER_BYTES)?,
            sections: std::array::from_fn(|index| sections.section(index).cloned()),
        };
        let held = Arc::new(sections.retain_decoded(frame));
        let work_held = held.clone();
        let shared = self.shared.clone();
        let task_limit = self.task_memory_bytes;
        let ready_count = record.ready.ready_count;
        let prepared = self.shared.prepare(ExecutionClass::Query).await?;
        let mut job = self.next_job.load(Ordering::Acquire);
        loop {
            let next = job.checked_add(1).ok_or_else(unavailable)?;
            match self.next_job.compare_exchange_weak(
                job,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => job = observed,
            }
        }
        let grant = self
            .shared
            .scheduler()
            .reserve_on(job, 1, self.shared.scheduler_node_id(), 0)
            .await?;
        let (record, (grant, held)) = prepared
            .run_with_guard((grant, held), move || {
                // Wire buffers already own their actual byte charge. Count
                // JSON structure on the native executor before typed decode:
                // a wide string does not cost one allocation per input byte.
                let mut decode = ready_count
                    .checked_mul(128)
                    .and_then(|n| n.checked_add(64 << 10))
                    .and_then(|n| usize::try_from(n).ok())
                    .ok_or_else(unavailable)?;
                for (index, bytes) in work_held
                    .sections
                    .iter()
                    .enumerate()
                    .filter_map(|(i, bytes)| bytes.as_ref().map(|b| (i, b)))
                {
                    let section = if index == 0 {
                        crate::oltp::mvcc::request_decode_budget(bytes.as_ref())?
                    } else {
                        crate::oltp::mvcc::json_decode_budget(bytes.as_ref())?
                    };
                    // Four lexical budgets cover the typed request/RawValue
                    // string scratch plus temporary old/incoming record clones
                    // during the short queue settlement. Raw wire stays separate.
                    decode = decode
                        .checked_add(section.checked_mul(4).ok_or_else(unavailable)?)
                        .ok_or_else(unavailable)?;
                    if index == 0 {
                        // Predicate decoding retains a RawValue for each nested
                        // logical node. Charge those actual subtree bytes too.
                        decode = decode
                            .checked_add(request_raw_clone_bytes(bytes.as_ref())?)
                            .ok_or_else(unavailable)?;
                    }
                }
                if decode as u64 > task_limit {
                    return Err(decode_capacity());
                }
                let scratch = Arc::new(shared.reserve_scratch(ByteClass::Data, decode)?);
                let record = PreparedInstall::prepare(environment, &work_held)
                    .map_err(QueueError::protocol)?;
                Ok((record, scratch))
            })
            .await?;
        drop(grant);
        let (record, scratch) = record?;
        Ok((record, scratch, held))
    }
}

impl Endpoint for QueueEndpoint {
    fn preflight<'a>(
        &'a self,
        session: &'a AuthenticatedSession,
        header: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        Box::pin(async move {
            let request = self.parse(session, header)?;
            self.check(session, &request).await?;
            let (class, section_lengths, decode_budget_bytes) = match request {
                QueueRequest::Install { record }
                | QueueRequest::JoinPage {
                    record: Some(record),
                    ..
                } => {
                    // Receive only exact raw sections. Working decode memory
                    // is sized from their actual structure after reception.
                    (TrafficClass::QueueData, record.section_lengths, 0)
                }
                QueueRequest::State { .. } => (TrafficClass::QueueControl, [0; 3], 0),
                QueueRequest::Result { .. } => (TrafficClass::QueueData, [0; 3], 0),
                QueueRequest::JoinPage { record: None, .. } => (TrafficClass::QueueData, [0; 3], 0),
            };
            Ok(ReceivePlan {
                class,
                section_lengths,
                decode_budget_bytes,
            })
        })
    }

    fn handle<'a>(
        &'a self,
        session: AuthenticatedSession,
        header: crate::oltp::pool::ChargedBytes,
        sections: ChargedSections,
    ) -> EndpointFuture<'a, ChargedReply> {
        Box::pin(async move {
            let request = self.parse(&session, header.as_ref())?;
            let current = self.check(&session, &request).await?;
            match request {
                QueueRequest::JoinPage {
                    scope,
                    view,
                    page,
                    record,
                } => {
                    let page = *page;
                    let prepared = if let Some(record) = &record {
                        Some(
                            self.prepare_payload(&session, current, record, &sections)
                                .await?,
                        )
                    } else {
                        if (0..3).any(|index| sections.section(index).is_some()) {
                            return Err(malformed("empty join page has unexpected payload"));
                        }
                        None
                    };
                    self.check(
                        &session,
                        &QueueRequest::JoinPage {
                            scope,
                            view,
                            page: Box::new(page),
                            record,
                        },
                    )
                    .await?;
                    // Keep both memory and raw/decode owners until settlement
                    // and reply encoding, including native cancellation.
                    let (record, owners) = match prepared {
                        Some((record, memory, held)) => (Some(record), Some((memory, held))),
                        None => (None, None),
                    };
                    self.replica
                        .lock()
                        .map_err(|_| unavailable())?
                        .install_prepared_join_page(current, session.remote(), page, record)
                        .map_err(QueueError::protocol)?;
                    let header = self.shared.encode_in(
                        ByteClass::Progress,
                        &QueueReply::Joined {
                            worker: session.local(),
                            page: Box::new(page),
                            nonce: session.nonce(),
                        },
                        super::replica::MAX_HEADER_BYTES,
                    )?;
                    drop(owners);
                    Ok(ChargedReply {
                        header,
                        sections: [None, None, None],
                    })
                }
                QueueRequest::Install { record } => {
                    let (prepared_record, memory, held) = self
                        .prepare_payload(&session, current, &record, &sections)
                        .await?;
                    self.check(&session, &QueueRequest::Install { record })
                        .await?;
                    let acknowledgement = self
                        .replica
                        .lock()
                        .map_err(|_| unavailable())?
                        .install_prepared(prepared_record)
                        .map_err(QueueError::protocol)?;
                    let reply = self.shared.encode_in(
                        ByteClass::Progress,
                        &QueueReply::Installed {
                            worker: session.local(),
                            record: acknowledgement.record(),
                            nonce: session.nonce(),
                        },
                        super::replica::MAX_HEADER_BYTES,
                    )?;
                    drop((memory, held));
                    Ok(ChargedReply {
                        header: reply,
                        sections: [None, None, None],
                    })
                }
                QueueRequest::State {
                    view, request_id, ..
                } => {
                    if sections.section(0).is_some()
                        || sections.section(1).is_some()
                        || sections.section(2).is_some()
                    {
                        return Err(malformed("state RPC has unexpected payload"));
                    }
                    let replica = self.replica.lock().map_err(|_| unavailable())?;
                    if replica.authority() != current {
                        return Err(QueueError::Stale.protocol());
                    }
                    let record = replica
                        .retained_record(request_id, now_ms())
                        .map_err(QueueError::protocol)?;
                    let reply = QueueReply::State(QueueState {
                        request_id,
                        run_id: view.run_id,
                        version: record.version,
                        state: record.state,
                        copies_complete: replica.copies_complete(request_id),
                    });
                    Ok(ChargedReply {
                        header: self.shared.encode_in(
                            ByteClass::Progress,
                            &reply,
                            super::replica::MAX_HEADER_BYTES,
                        )?,
                        sections: [None, None, None],
                    })
                }
                QueueRequest::Result {
                    request_id,
                    version,
                    ..
                } => {
                    let replica = self.replica.lock().map_err(|_| unavailable())?;
                    if replica.authority() != current {
                        return Err(QueueError::Stale.protocol());
                    }
                    let record = replica
                        .retained_record(request_id, now_ms())
                        .map_err(QueueError::protocol)?;
                    if !replica.copies_complete(request_id) {
                        return Err(QueueError::Unavailable.protocol());
                    }
                    if record.version != version {
                        return Err(QueueError::Stale.protocol());
                    }
                    let response = record
                        .response_body
                        .as_ref()
                        .ok_or_else(|| QueueError::Unavailable.protocol())?;
                    let body = self.shared.copy_bytes(response)?;
                    let reply = self.shared.encode_in(
                        ByteClass::Progress,
                        &QueueReply::Result {
                            request_id,
                            version,
                            status: record.response_status.ok_or_else(unavailable)?,
                            response_bytes: body.len() as u64,
                        },
                        super::replica::MAX_HEADER_BYTES,
                    )?;
                    Ok(ChargedReply {
                        header: reply,
                        sections: [Some(body), None, None],
                    })
                }
            }
        })
    }
}

fn malformed(message: &'static str) -> ProtocolError {
    ProtocolError::new(ErrorCode::ValidationError, message, Outcome::NotApplied)
}
fn unavailable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "queue worker state unavailable",
        Outcome::Unknown,
    )
}
fn decode_capacity() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::Overloaded,
        "queue decode working memory exceeds task capacity",
        Outcome::NotApplied,
    )
}
/// An allocation-free upper bound for recursive request RawValue copies.
/// Object depth counts the predicate nodes whose raw subtree can remain live;
/// ordinary wide rows stay shallow. Braces inside strings never change depth.
fn request_raw_clone_bytes(bytes: &[u8]) -> Result<usize, ProtocolError> {
    let (mut depth, mut quoted, mut escaped, mut total) = (0usize, false, false, 0usize);
    for byte in bytes {
        if !quoted && *byte == b'{' {
            depth = depth.checked_add(1).ok_or_else(decode_capacity)?;
        }
        total = total.checked_add(depth).ok_or_else(decode_capacity)?;
        if quoted {
            if escaped {
                escaped = false;
            } else if *byte == b'\\' {
                escaped = true;
            } else if *byte == b'"' {
                quoted = false;
            }
        } else {
            if *byte == b'}' {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| malformed("unbalanced request object"))?;
            }
            if *byte == b'"' {
                quoted = true;
            }
        }
    }
    Ok(total)
}
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests;
