//! RAM queue RPCs and fixed terminal metadata on the shared worker mTLS
//! listener. Application operations are admitted and executed by WorkerRuntime
//! under the durable Control/TSO/Status authority.
use super::replica::*;
use crate::{
    oltp::pool::{ByteClass, ExecutionClass, ScratchLease, WorkerSharedResources},
    oltp_replication::{multiplex::*, owned::serialized_digest, service_rpc::ReadGrant, types::*},
    protocol::{ErrorCode, Outcome, ProtocolError},
    request_queue::{QueueError, RequestState},
};
use serde::{Deserialize, Serialize};
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicU64, Ordering},
};
use uuid::Uuid;

mod scoped_payload;

/// Implementations use the separate Authority transport lane. They must not
/// acquire queue-data/control body permits or recursively authenticate Control
/// consensus through another strong Control lookup.
pub trait AuthorityReader: Send + Sync + 'static {
    fn current<'a>(&'a self, scope: &'a QueueScope) -> EndpointFuture<'a, QueueAuthority>;
    fn removal_view<'a>(
        &'a self,
        _scope: &'a QueueScope,
        _owner: WorkerIdentity,
        _ticket: JoinTicket,
    ) -> EndpointFuture<'a, QueueAuthority> {
        Box::pin(async { Err(QueueError::Stale.protocol()) })
    }
    fn excludes_process(&self, _worker: WorkerIdentity) -> EndpointFuture<'_, bool> {
        Box::pin(async { Ok(false) })
    }
    /// One Control snapshot checks the exact current enrollment and TSO group
    /// membership. Ordinary worker authorities cannot read proof metadata.
    fn terminal_reader_view<'a>(
        &'a self,
        _scope: &'a QueueScope,
        _remote: WorkerIdentity,
        _timestamps_group_id: Uuid,
    ) -> EndpointFuture<'a, QueueAuthority> {
        Box::pin(async {
            Err(ProtocolError::new(
                ErrorCode::Unauthorized,
                "terminal proof reader is not authorized",
                Outcome::NotApplied,
            ))
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum QueueRequest {
    Ping {
        scope: QueueScope,
        view: AuthorityView,
    },
    RemovalStatement {
        scope: QueueScope,
        view: AuthorityView,
        ticket: JoinTicket,
    },
    RemovalRecord {
        scope: QueueScope,
        ticket: JoinTicket,
        after: Option<Uuid>,
    },
    Install {
        record: RecordHeader,
    },
    ScopedPayloadV2 {
        queue_group_id: Uuid,
        data_generation: Uuid,
        queue_generation: Uuid,
        record: Box<RecordHeader>,
    },
    ScopedPayloadWitnessV2 {
        scope: QueueScope,
        view: AuthorityView,
        queue_group_id: Uuid,
        data_generation: Uuid,
        queue_generation: Uuid,
        request: RequestBinding,
        version: RecordVersion,
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
    TerminalState {
        scope: QueueScope,
        view: AuthorityView,
        request_id: Uuid,
        timestamps_group_id: Uuid,
    },
    JoinStatement {
        scope: QueueScope,
        view: AuthorityView,
        ticket_id: Uuid,
    },
    AdmissionState {
        scope: QueueScope,
        view: AuthorityView,
        ticket: Box<crate::oltp_replication::service_rpc::OwnerAdmissionTicket>,
    },
    ReadGrantCopy {
        scope: QueueScope,
        view: AuthorityView,
        request_id: Uuid,
        set: ReadGrantSet,
    },
    ReadGrantSettle {
        scope: QueueScope,
        view: AuthorityView,
        grant: Box<ReadGrant>,
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
            Self::ScopedPayloadV2 { record, .. } => &record.scope,
            Self::State { scope, .. }
            | Self::Ping { scope, .. }
            | Self::RemovalRecord { scope, .. }
            | Self::RemovalStatement { scope, .. }
            | Self::TerminalState { scope, .. }
            | Self::JoinStatement { scope, .. }
            | Self::AdmissionState { scope, .. }
            | Self::ReadGrantCopy { scope, .. }
            | Self::ReadGrantSettle { scope, .. }
            | Self::ScopedPayloadWitnessV2 { scope, .. }
            | Self::Result { scope, .. }
            | Self::JoinPage { scope, .. } => scope,
        }
    }
    fn view(&self) -> AuthorityView {
        match self {
            Self::Install { record } => record.ready.view,
            Self::ScopedPayloadV2 { record, .. } => record.ready.view,
            Self::RemovalRecord { ticket, .. } => ticket.source_ready.view,
            Self::State { view, .. }
            | Self::Ping { view, .. }
            | Self::RemovalStatement { view, .. }
            | Self::TerminalState { view, .. }
            | Self::JoinStatement { view, .. }
            | Self::AdmissionState { view, .. }
            | Self::ReadGrantCopy { view, .. }
            | Self::ReadGrantSettle { view, .. }
            | Self::ScopedPayloadWitnessV2 { view, .. }
            | Self::Result { view, .. }
            | Self::JoinPage { view, .. } => *view,
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
    Ping {
        worker: WorkerIdentity,
        ready: ReadyMembership,
        nonce: Uuid,
    },
    RemovalStatement {
        worker: WorkerIdentity,
        statement: Box<QueueCopyStatement>,
        nonce: Uuid,
    },
    RemovalRecord {
        worker: WorkerIdentity,
        ticket: JoinTicket,
        after: Option<Uuid>,
        record: Option<Box<RecordHeader>>,
        nonce: Uuid,
    },
    Installed {
        worker: WorkerIdentity,
        record: RecordVersion,
        nonce: Uuid,
    },
    ReadGrantConfirmed {
        digest: Digest,
        nonce: Uuid,
    },
    Joined {
        worker: WorkerIdentity,
        page: Box<JoinPageHeader>,
        nonce: Uuid,
    },
    State(QueueState),
    TerminalState {
        worker: WorkerIdentity,
        statement: Box<TerminalCopyStatement>,
        nonce: Uuid,
    },
    JoinStatement {
        worker: WorkerIdentity,
        statement: Box<QueueCopyStatement>,
        nonce: Uuid,
    },
    Result {
        request_id: Uuid,
        version: u64,
        status: u16,
        response_bytes: u64,
    },
}

/// Constructed only by the authenticated client response path. Callers cannot
/// deserialize an observation or supply a worker's unverified ACK payload.
pub(crate) struct TerminalObservation {
    worker: WorkerIdentity,
    statement: TerminalCopyStatement,
    nonce: Uuid,
}
/// Only the authenticated transport client creates this completion proof.
/// A page ACK alone never certifies that the complete snapshot and delta exist.
pub(crate) struct JoinObservation {
    worker: WorkerIdentity,
    statement: QueueCopyStatement,
    nonce: Uuid,
}
pub(crate) struct RemovalObservation {
    worker: WorkerIdentity,
    statement: QueueCopyStatement,
    nonce: Uuid,
}
impl RemovalObservation {
    pub(crate) fn into_parts(self) -> (WorkerIdentity, QueueCopyStatement, Uuid) {
        (self.worker, self.statement, self.nonce)
    }
}
impl JoinObservation {
    pub(crate) fn into_parts(self) -> (WorkerIdentity, QueueCopyStatement, Uuid) {
        (self.worker, self.statement, self.nonce)
    }
}
impl TerminalObservation {
    pub(crate) fn into_parts(self) -> (WorkerIdentity, TerminalCopyStatement, Uuid) {
        (self.worker, self.statement, self.nonce)
    }
}

pub struct QueueEndpoint {
    replica: Arc<Mutex<WorkerReplica>>,
    shared: Arc<WorkerSharedResources>,
    scope: QueueScope,
    authority: Arc<dyn AuthorityReader>,
    task_memory_bytes: u64,
    next_job: AtomicU64,
    local: WorkerIdentity,
    admission: OnceLock<Arc<super::admission_tickets::AdmissionTickets>>,
    join_mount: OnceLock<Mutex<[Option<(crate::oltp::pool::OltpPool, u64)>; 2]>>,
    retirement: OnceLock<Arc<dyn crate::oltp_replication::authority::status::StatusService>>,
    scoped_payload: OnceLock<scoped_payload::ScopedPayloadReaders>,
}

#[derive(Clone)]
pub struct QueuePeerClient {
    client: WorkerClient,
    shared: Arc<WorkerSharedResources>,
    group: GroupIdentity,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PeerReachability {
    Reachable,
    Unreachable,
    Indeterminate,
}

impl QueuePeerClient {
    pub(crate) async fn copy_read_grants(
        &self,
        target: WorkerIdentity,
        scope: QueueScope,
        view: AuthorityView,
        request_id: Uuid,
        set: ReadGrantSet,
    ) -> Result<(), ProtocolError> {
        let digest = serialized_digest(&(request_id, &set))?;
        self.read_grant_call(target, QueueRequest::ReadGrantCopy {
            scope, view, request_id, set,
        }, digest).await
    }

    pub(crate) async fn settle_read_grant(
        &self,
        scope: QueueScope,
        view: AuthorityView,
        grant: ReadGrant,
    ) -> Result<(), ProtocolError> {
        self.read_grant_call(grant.worker, QueueRequest::ReadGrantSettle {
            scope, view, grant: Box::new(grant),
        }, serialized_digest(&grant)?).await
    }

    async fn read_grant_call(
        &self,
        target: WorkerIdentity,
        request: QueueRequest,
        expected: Digest,
    ) -> Result<(), ProtocolError> {
        let copying = matches!(&request, QueueRequest::ReadGrantCopy { .. });
        let header =
            self.shared
                .encode_in(ByteClass::Progress, &request, MAX_RECORD_HEADER_BYTES)?;
        let reply = self
            .client
            .request_classified(
                &self.group,
                target,
                Purpose::QueueControl,
                header,
                [None, None, None],
                0,
            )
            .await
            .map_err(|error| {
                if !copying {
                    return error.into();
                }
                match error {
                    RequestError::OutgoingLaneFull => RequestError::OutgoingLaneFull.into(),
                    RequestError::Unreachable => ProtocolError::new(
                        ErrorCode::OutcomeUnknown,
                        "read grant copy acknowledgement unavailable",
                        Outcome::Unknown,
                    ),
                    RequestError::PeerRejected { worker, .. } if worker != target => {
                        malformed("read grant refusal transport context differs")
                    }
                    RequestError::PeerRejected { mut error, .. }
                    | RequestError::Protocol(mut error) => {
                        if error.code == ErrorCode::Overloaded {
                            // The receiver may have installed this exact revision
                            // before work on the acknowledgement hit a limit.
                            error.outcome = Outcome::Unknown;
                        }
                        error
                    }
                }
            })?;
        if reply.remote_identity() != target
            || reply.group() != &self.group
            || reply.purpose() != Purpose::QueueControl
            || reply.sections.iter().any(Option::is_some)
        {
            return Err(malformed("read grant reply transport context differs"));
        }
        let decode = crate::oltp::mvcc::json_decode_budget(reply.header.as_ref())?;
        let reply = reply
            .reserve_reply_decode(&self.shared, decode)
            .await
            .map_err(|mut error| {
                if copying && error.code == ErrorCode::Overloaded {
                    error.outcome = Outcome::Unknown;
                }
                error
            })?;
        let decoded = reply.decode_header::<QueueReply>()?;
        if !matches!(*decoded, QueueReply::ReadGrantConfirmed { digest, nonce }
            if digest == expected && nonce == reply.nonce())
        {
            return Err(malformed("read grant reply differs"));
        }
        Ok(())
    }

    pub(crate) async fn probe(
        &self,
        target: WorkerIdentity,
        scope: QueueScope,
        ready: ReadyMembership,
    ) -> PeerReachability {
        let Ok(header) = self.shared.encode_in(
            ByteClass::Progress,
            &QueueRequest::Ping {
                scope,
                view: ready.view,
            },
            super::replica::MAX_HEADER_BYTES,
        ) else {
            return PeerReachability::Indeterminate;
        };
        let result = self
            .client
            .request_queue_probe(
                &self.group,
                target,
                header,
                4 * super::replica::MAX_HEADER_BYTES,
            )
            .await;
        let reply = match result {
            Ok(reply) => reply,
            Err(RequestError::PeerRejected { worker, .. }) if worker == target => {
                return PeerReachability::Reachable;
            }
            Err(RequestError::Unreachable) => return PeerReachability::Unreachable,
            Err(_) => return PeerReachability::Indeterminate,
        };
        if reply.remote_identity() != target
            || reply.group() != &self.group
            || reply.purpose() != Purpose::QueueControl
            || reply.sections.iter().any(Option::is_some)
        {
            return PeerReachability::Indeterminate;
        }
        let Ok(decoded) = reply.decode_header::<QueueReply>() else {
            return PeerReachability::Indeterminate;
        };
        match &*decoded {
            QueueReply::Ping {
                worker,
                nonce,
                ..
            } if *worker == target && *nonce == reply.nonce() => PeerReachability::Reachable,
            _ => PeerReachability::Indeterminate,
        }
    }
    pub(crate) async fn removal_statement(
        &self,
        target: WorkerIdentity,
        scope: QueueScope,
        ticket: JoinTicket,
    ) -> Result<RemovalObservation, ProtocolError> {
        let header = self.shared.encode_in(
            ByteClass::Progress,
            &QueueRequest::RemovalStatement {
                scope,
                view: ticket.source_ready.view,
                ticket,
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
            return Err(malformed("queue removal transport context differs"));
        }
        let decode = crate::oltp::mvcc::json_decode_budget(reply.header.as_ref())?;
        let reply = reply.reserve_reply_decode(&self.shared, decode).await?;
        let decoded = reply.decode_header::<QueueReply>()?;
        match &*decoded {
            QueueReply::RemovalStatement {
                worker,
                statement,
                nonce,
            } if *worker == target && statement.ticket == ticket && *nonce == reply.nonce() => {
                Ok(RemovalObservation {
                    worker: *worker,
                    statement: **statement,
                    nonce: *nonce,
                })
            }
            _ => Err(malformed("queue removal statement differs")),
        }
    }
    pub(crate) async fn removal_record(
        &self,
        target: WorkerIdentity,
        scope: QueueScope,
        current: QueueAuthority,
        ticket: JoinTicket,
        after: Option<Uuid>,
    ) -> Result<Option<(RecordHeader, RecordFrame)>, ProtocolError> {
        let header = self.shared.encode_in(
            ByteClass::Progress,
            &QueueRequest::RemovalRecord {
                scope: scope.clone(),
                ticket,
                after,
            },
            super::replica::MAX_HEADER_BYTES,
        )?;
        let reply = self
            .client
            .request_classified(
                &self.group,
                target,
                Purpose::QueueData,
                header,
                [None, None, None],
                0,
            )
            .await?;
        if reply.remote_identity() != target
            || reply.group() != &self.group
            || reply.purpose() != Purpose::QueueData
            || reply.header.len() > 2 * MAX_RECORD_HEADER_BYTES
        {
            return Err(malformed("queue removal record transport context differs"));
        }
        let decode = crate::oltp::mvcc::json_decode_budget(reply.header.as_ref())?;
        let reply = reply.reserve_decode(&self.shared, decode)?;
        let decoded = reply.decode_header::<QueueReply>()?;
        let QueueReply::RemovalRecord {
            worker,
            ticket: observed,
            after: cursor,
            record,
            nonce,
        } = &*decoded
        else {
            return Err(malformed("expected queue removal record"));
        };
        if *worker != target || *observed != ticket || *cursor != after || *nonce != reply.nonce() {
            return Err(malformed("queue removal record differs"));
        }
        let Some(record) = record else {
            if reply.sections.iter().any(Option::is_some) {
                return Err(malformed("queue removal end has unexpected payload"));
            }
            return Ok(None);
        };
        record
            .preflight(&scope, current)
            .map_err(QueueError::protocol)?;
        if after.is_some_and(|after| record.request_id <= after)
            || record.all_copy_evidence
            || (0..3).any(|index| {
                reply.sections[index]
                    .as_ref()
                    .map(|bytes| bytes.len() as u64)
                    != (record.section_lengths[index] != 0).then_some(record.section_lengths[index])
            })
        {
            return Err(malformed(
                "queue removal record does not match its cursor or payload",
            ));
        }
        let frame = RecordFrame {
            header: self.shared.encode(record, MAX_RECORD_HEADER_BYTES)?,
            sections: reply.sections.clone(),
        };
        Ok(Some((record.as_ref().clone(), frame)))
    }

    pub(crate) async fn prepare_removal_record(
        &self,
        environment: InstallEnvironment,
        frame: RecordFrame,
    ) -> Result<(PreparedInstall, ScratchLease), ProtocolError> {
        let ready_count = frame
            .decode_header()
            .map_err(QueueError::protocol)?
            .ready
            .ready_count;
        let shared = self.shared.clone();
        self.shared
            .execute(ExecutionClass::Query, 0, 0, move || {
                prepare_queue_record(
                    &shared,
                    shared.task_memory_limit_bytes() as u64,
                    ready_count,
                    environment,
                    &frame,
                )
            })
            .await
    }
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
        self.install_version(target, frame, expected).await
    }

    pub(super) async fn install_version(
        &self,
        target: WorkerIdentity,
        frame: &RecordFrame,
        expected: RecordVersion,
    ) -> Result<CopyAcknowledgement, ProtocolError> {
        let header = self.shared.encode(
            &QueueRequest::Install {
                record: frame.decode_header().map_err(QueueError::protocol)?,
            },
            MAX_RECORD_HEADER_BYTES,
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
    pub(crate) async fn terminal_state(
        &self,
        target: WorkerIdentity,
        scope: QueueScope,
        expected: &TerminalCopyStatement,
        timestamps_group_id: Uuid,
    ) -> Result<TerminalObservation, ProtocolError> {
        expected
            .validate()
            .map_err(|_| malformed("invalid terminal proof statement"))?;
        let header = self.shared.encode_in(
            ByteClass::Progress,
            &QueueRequest::TerminalState {
                scope,
                view: expected.ready.view,
                request_id: expected.request.request_id,
                timestamps_group_id,
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
                0,
            )
            .await?;
        if reply.remote_identity() != target
            || reply.group() != &self.group
            || reply.purpose() != Purpose::QueueControl
            || reply.sections.iter().any(Option::is_some)
        {
            return Err(malformed("terminal metadata transport context differs"));
        }
        if reply.header.len() > super::replica::MAX_HEADER_BYTES {
            return Err(malformed("terminal metadata header exceeds bounds"));
        }
        let decode = crate::oltp::mvcc::json_decode_budget(reply.header.as_ref())?;
        let reply = reply.reserve_reply_decode(&self.shared, decode).await?;
        let decoded = reply.decode_header::<QueueReply>()?;
        let QueueReply::TerminalState {
            worker,
            statement,
            nonce,
        } = &*decoded
        else {
            return Err(malformed("expected terminal metadata reply"));
        };
        if *worker != target || statement.as_ref() != expected || *nonce != reply.nonce() {
            return Err(malformed("terminal metadata statement differs"));
        }
        Ok(TerminalObservation {
            worker: *worker,
            statement: statement.as_ref().clone(),
            nonce: *nonce,
        })
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
            MAX_RECORD_HEADER_BYTES,
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
    pub(crate) async fn join_statement(
        &self,
        target: WorkerIdentity,
        scope: QueueScope,
        expected: &QueueCopyStatement,
    ) -> Result<JoinObservation, ProtocolError> {
        let header = self.shared.encode_in(
            ByteClass::Progress,
            &QueueRequest::JoinStatement {
                scope,
                view: expected.ticket.source_ready.view,
                ticket_id: expected.ticket.ticket_id,
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
            return Err(malformed("join completion transport context differs"));
        }
        if reply.header.len() > super::replica::MAX_HEADER_BYTES {
            return Err(malformed("join completion header exceeds bounds"));
        }
        let decode = crate::oltp::mvcc::json_decode_budget(reply.header.as_ref())?;
        let reply = reply.reserve_reply_decode(&self.shared, decode).await?;
        let decoded = reply.decode_header::<QueueReply>()?;
        let QueueReply::JoinStatement {
            worker,
            statement,
            nonce,
        } = &*decoded
        else {
            return Err(malformed("expected join completion statement"));
        };
        if *worker != target || statement.as_ref() != expected || *nonce != reply.nonce() {
            return Err(malformed("join completion statement differs"));
        }
        Ok(JoinObservation {
            worker: *worker,
            statement: statement.as_ref().clone(),
            nonce: *nonce,
        })
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
impl crate::oltp_replication::service_rpc::OwnerAdmissionReader for QueuePeerClient {
    fn verify<'a>(
        &'a self,
        ticket: &'a crate::oltp_replication::service_rpc::OwnerAdmissionTicket,
    ) -> crate::oltp_replication::authority::consensus::ConsensusFuture<
        'a,
        crate::oltp_replication::service_rpc::OwnerAdmissionObservation,
    > {
        Box::pin(async move {
            let header = self.shared.encode_in(
                ByteClass::Progress,
                &QueueRequest::AdmissionState {
                    scope: QueueScope {
                        cluster_id: self.group.cluster_id,
                        database: self.group.database.clone(),
                    },
                    view: ticket.expected.view,
                    ticket: Box::new(ticket.clone()),
                },
                super::replica::MAX_HEADER_BYTES,
            )?;
            let reply = self
                .client
                .request(
                    &self.group,
                    ticket.owner.worker_id,
                    Purpose::QueueControl,
                    header,
                    [None, None, None],
                    0,
                )
                .await?;
            let observed = reply.sections[0]
                .as_ref()
                .ok_or_else(|| malformed("missing admission proof reply"))?;
            let decode = crate::oltp::mvcc::json_decode_budget(observed.as_slice())?;
            let reply = reply.reserve_reply_decode(&self.shared, decode).await?;
            crate::oltp_replication::service_rpc::OwnerAdmissionObservation::from_queue_reply(
                reply, ticket,
            )
        })
    }
}

impl QueueEndpoint {
    pub(crate) fn require_join_mount(
        &self,
        pool: crate::oltp::pool::OltpPool,
        total_bytes: u64,
    ) -> Result<(), ProtocolError> {
        self.require_join_mounts([Some((pool, total_bytes)), None])
    }
    pub(crate) fn require_join_mounts(
        &self,
        mounts: [Option<(crate::oltp::pool::OltpPool, u64)>; 2],
    ) -> Result<(), ProtocolError> {
        self.join_mount.set(Mutex::new(mounts)).map_err(|_| unavailable())
    }
    pub(crate) fn release_join_mount(&self) -> Result<(), ProtocolError> {
        let mount = self.join_mount.get().ok_or_else(unavailable)?;
        *mount.lock().map_err(|_| unavailable())? = [None, None];
        Ok(())
    }
    pub(crate) fn install_admission_tickets(
        &self,
        tickets: Arc<super::admission_tickets::AdmissionTickets>,
    ) -> Result<(), ProtocolError> {
        self.admission.set(tickets).map_err(|_| unavailable())
    }
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
            admission: OnceLock::new(),
            join_mount: OnceLock::new(),
            retirement: OnceLock::new(),
            scoped_payload: OnceLock::new(),
        }))
    }
    pub(crate) fn install_retirement_reader(
        &self,
        status: Arc<dyn crate::oltp_replication::authority::status::StatusService>,
    ) -> Result<(), ProtocolError> {
        self.retirement.set(status).map_err(|_| unavailable())
    }

    fn parse(
        &self,
        session: &AuthenticatedSession,
        bytes: &[u8],
    ) -> Result<QueueRequest, ProtocolError> {
        if bytes.len() > MAX_RECORD_HEADER_BYTES
            || session.recovery_only()
            || session.local() != self.local
            || session.group().cluster_id != self.scope.cluster_id
            || session.group().database != self.scope.database
            || session.group().kind != GroupKind::Control
        {
            return Err(malformed("queue session has the wrong scope"));
        }
        crate::protocol::json_input(bytes, MAX_RECORD_HEADER_BYTES)
            .map_err(|_| malformed("invalid queue header bounds"))?;
        let request: QueueRequest =
            serde_json::from_slice(bytes).map_err(|_| malformed("invalid queue RPC header"))?;
        if bytes.len() > super::replica::MAX_HEADER_BYTES
            && !matches!(
                request,
                QueueRequest::Install { .. }
                    | QueueRequest::ScopedPayloadV2 { .. }
                    | QueueRequest::JoinPage { .. }
                    | QueueRequest::ReadGrantCopy { .. }
            )
        {
            return Err(malformed("queue header exceeds method limit"));
        }
        if request.scope() != &self.scope || request.view().validate().is_err() {
            return Err(malformed("queue header has the wrong scope"));
        }
        let permitted = matches!(
            (&request, session.purpose()),
            (
                QueueRequest::State { .. }
                    | QueueRequest::Ping { .. }
                    | QueueRequest::RemovalStatement { .. }
                    | QueueRequest::TerminalState { .. }
                    | QueueRequest::JoinStatement { .. }
                    | QueueRequest::AdmissionState { .. }
                    | QueueRequest::ScopedPayloadWitnessV2 { .. }
                    | QueueRequest::ReadGrantCopy { .. }
                    | QueueRequest::ReadGrantSettle { .. },
                Purpose::QueueControl
            ) | (
                QueueRequest::Install { .. }
                    | QueueRequest::ScopedPayloadV2 { .. }
                    | QueueRequest::Result { .. }
                    | QueueRequest::RemovalRecord { .. }
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
        // Ping reports liveness for the authenticated process. Its local view
        // can lag Control and grants no permission to change queue state.
        if matches!(request, QueueRequest::Ping { .. }) {
            return Ok(self.replica.lock().map_err(|_| unavailable())?.authority());
        }
        if let QueueRequest::RemovalRecord { ticket, .. } = request {
            return self.authority.removal_view(&self.scope, session.remote(), *ticket).await;
        }
        // No queue lock, scheduler CPU grant or payload permit from this module
        // is held while the independent Control authority lookup waits.
        let current = if let QueueRequest::TerminalState {
            timestamps_group_id,
            ..
        } = request
        {
            if timestamps_group_id.is_nil() {
                return Err(malformed("missing timestamps group"));
            }
            self.authority
                .terminal_reader_view(&self.scope, session.remote(), *timestamps_group_id)
                .await?
        } else {
            let result = self.authority.current(&self.scope).await;
            if let (
                QueueRequest::ReadGrantCopy {
                    request_id, set, ..
                },
                Err(error),
            ) = (request, &result)
            {
                tracing::debug!(%request_id, owner=%session.remote().worker_id, worker=%session.local().worker_id,
                    revision=set.revision, code=?error.code, message=%error.message,
                    "read grant copy Control lookup failed");
            }
            result?
        };
        if request.view() != current.ready.view {
            return Err(QueueError::Stale.protocol());
        }
        if matches!(request, QueueRequest::RemovalStatement { .. }
            | QueueRequest::ReadGrantCopy { .. } | QueueRequest::ReadGrantSettle { .. })
            && session.remote() != current.owner
        {
            return Err(QueueError::Stale.protocol());
        }
        if let QueueRequest::ReadGrantSettle { grant, .. } = request {
            let authority = current.ready.view.authority();
            if grant.worker != session.local()
                || grant.ticket.is_nil()
                || grant.scope.request.run_id != authority.run_id
                || grant.scope.authority.run_id != authority.run_id
                || grant.scope.authority.owner_term == 0
                || grant.scope.authority.owner_term > authority.owner_term
                || grant.scope.authority == authority && grant.owner != session.remote()
            {
                return Err(QueueError::Stale.protocol());
            }
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
                .encode(record, MAX_RECORD_HEADER_BYTES)?,
            sections: std::array::from_fn(|index| sections.section(index).cloned()),
        };
        let held = Arc::new(sections.retain_decoded(frame));
        let work_held = held.clone();
        let shared = self.shared.clone();
        let task_limit = self.task_memory_bytes;
        let ready_count = record.ready.ready_count;
        let prepared = self.shared.prepare(ExecutionClass::Query).await?;
        let grant = prepared.reserve_compute(self.next_decode_job()?, 0).await?;
        let (record, (grant, held)) = prepared
            .run_with_guard((grant, held), move || {
                prepare_queue_record(&shared, task_limit, ready_count, environment, &work_held)
            })
            .await?;
        drop(grant);
        let (record, scratch) = record?;
        Ok((record, Arc::new(scratch), held))
    }

    fn next_decode_job(&self) -> Result<u64, ProtocolError> {
        let mut job = self.next_job.load(Ordering::Acquire);
        loop {
            let next = job.checked_add(1).ok_or_else(unavailable)?;
            match self.next_job.compare_exchange_weak(
                job,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(job),
                Err(observed) => job = observed,
            }
        }
    }
}

fn prepare_queue_record(
    shared: &WorkerSharedResources,
    task_limit: u64,
    ready_count: u64,
    environment: InstallEnvironment,
    frame: &RecordFrame,
) -> Result<(PreparedInstall, ScratchLease), ProtocolError> {
    // Wire buffers already own their actual byte charge. Count
    // JSON structure on the native executor before typed decode:
    // a wide string does not cost one allocation per input byte.
    let mut decode = ready_count
        .checked_mul(128)
        .and_then(|n| n.checked_add(64 << 10))
        .and_then(|n| usize::try_from(n).ok())
        .ok_or_else(unavailable)?;
    for (index, bytes) in frame
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
    let scratch = shared.reserve_scratch(ByteClass::Data, decode)?;
    let record = PreparedInstall::prepare(environment, frame).map_err(QueueError::protocol)?;
    Ok((record, scratch))
}

impl Endpoint for QueueEndpoint {
    fn preflight<'a>(
        &'a self,
        session: &'a AuthenticatedSession,
        header: &'a [u8],
    ) -> EndpointFuture<'a, ReceivePlan> {
        Box::pin(async move {
            let request = self.parse(session, header)?;
            if let QueueRequest::ScopedPayloadV2 {
                queue_group_id,
                data_generation,
                queue_generation,
                record,
            } = &request
            {
                return self
                    .preflight_scoped_payload(
                        session,
                        [*queue_group_id, *data_generation, *queue_generation],
                        record,
                    )
                    .await;
            }
            if matches!(request, QueueRequest::ScopedPayloadWitnessV2 { .. }) {
                return Ok(ReceivePlan {
                    class: TrafficClass::QueueControl,
                    section_lengths: [0; 3],
                    decode_budget_bytes: 0,
                });
            }
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
                QueueRequest::State { .. }
                | QueueRequest::Ping { .. }
                | QueueRequest::RemovalStatement { .. }
                | QueueRequest::TerminalState { .. }
                | QueueRequest::JoinStatement { .. }
                | QueueRequest::AdmissionState { .. }
                | QueueRequest::ReadGrantCopy { .. }
                | QueueRequest::ReadGrantSettle { .. } => (TrafficClass::QueueControl, [0; 3], 0),
                QueueRequest::Result { .. } | QueueRequest::RemovalRecord { .. } => {
                    (TrafficClass::QueueData, [0; 3], 0)
                }
                QueueRequest::JoinPage { record: None, .. } => (TrafficClass::QueueData, [0; 3], 0),
                QueueRequest::ScopedPayloadV2 { .. }
                | QueueRequest::ScopedPayloadWitnessV2 { .. } => return Err(unavailable()),
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
            if let QueueRequest::ScopedPayloadV2 {
                queue_group_id,
                data_generation,
                queue_generation,
                record,
            } = &request
            {
                return self
                    .install_scoped_payload(
                        &session,
                        [*queue_group_id, *data_generation, *queue_generation],
                        record,
                        &sections,
                    )
                    .await;
            }
            if let QueueRequest::ScopedPayloadWitnessV2 {
                view,
                queue_group_id,
                data_generation,
                queue_generation,
                request,
                version,
                ..
            } = &request
            {
                if (0..3).any(|index| sections.section(index).is_some()) {
                    return Err(malformed("scoped payload witness has unexpected sections"));
                }
                return self
                    .scoped_payload_witness(
                        &session,
                        [*queue_group_id, *data_generation, *queue_generation],
                        *view,
                        *request,
                        *version,
                    )
                    .await;
            }
            let current = if let QueueRequest::Install { record } = &request {
                // Preflight authorizes reception. Check the local view before
                // decoding, then recheck Control immediately before installation.
                let current = self.replica.lock().map_err(|_| unavailable())?.authority();
                if session.remote() != current.owner {
                    return Err(QueueError::Stale.protocol());
                }
                record
                    .preflight(&self.scope, current)
                    .map_err(QueueError::protocol)?;
                current
            } else {
                self.check(&session, &request).await?
            };
            let confirm = |digest| {
                Ok(ChargedReply {
                    header: self.shared.encode_in(ByteClass::Progress,
                        &QueueReply::ReadGrantConfirmed { digest, nonce: session.nonce() }, 1024)?,
                    sections: [None, None, None],
                })
            };
            match request {
                QueueRequest::ScopedPayloadV2 { .. }
                | QueueRequest::ScopedPayloadWitnessV2 { .. } => Err(unavailable()),
                QueueRequest::ReadGrantCopy { request_id, set, .. } => {
                    let digest = serialized_digest(&(request_id, &set))?;
                    self.replica.lock().map_err(|_| unavailable())?
                        .install_read_grants(current, session.remote(), request_id, set)
                        .map_err(QueueError::protocol)?;
                    confirm(digest)
                }
                QueueRequest::ReadGrantSettle { grant, .. } => {
                    let settled = self.shared.read_attempts.settle(*grant);
                    #[cfg(test)]
                    if matches!(settled.as_ref(), Err(error) if error.code == ErrorCode::StorageUnavailable)
                        && current.ready.view.owner_term > grant.scope.authority.owner_term
                    {
                        crate::oltp_replication::service_rpc::read_fixture::record(
                            grant.scope, grant.group, "takeover-settlement-waiting", grant.ticket, grant.worker,
                        );
                    }
                    settled?;
                    confirm(serialized_digest(grant.as_ref())?)
                }
                QueueRequest::Ping { .. } => {
                    if (0..3).any(|i| sections.section(i).is_some()) {
                        return Err(malformed("queue ping has unexpected payload"));
                    }
                    Ok(ChargedReply {
                        header: self.shared.encode_in(
                            ByteClass::Progress,
                            &QueueReply::Ping {
                                worker: session.local(),
                                ready: current.ready,
                                nonce: session.nonce(),
                            },
                            super::replica::MAX_HEADER_BYTES,
                        )?,
                        sections: [None, None, None],
                    })
                }
                QueueRequest::RemovalStatement { ticket, .. } => {
                    if (0..3).any(|i| sections.section(i).is_some()) {
                        return Err(malformed("queue removal has unexpected payload"));
                    }
                    let statement = self
                        .replica
                        .lock()
                        .map_err(|_| unavailable())?
                        .removal_statement(ticket)
                        .map_err(QueueError::protocol)?;
                    Ok(ChargedReply {
                        header: self.shared.encode_in(
                            ByteClass::Progress,
                            &QueueReply::RemovalStatement {
                                worker: session.local(),
                                statement: Box::new(statement),
                                nonce: session.nonce(),
                            },
                            super::replica::MAX_HEADER_BYTES,
                        )?,
                        sections: [None, None, None],
                    })
                }
                QueueRequest::RemovalRecord { ticket, after, .. } => {
                    if (0..3).any(|index| sections.section(index).is_some()) {
                        return Err(malformed("queue removal cursor has unexpected payload"));
                    }
                    let frame = self
                        .replica
                        .lock()
                        .map_err(|_| unavailable())?
                        .removal_record(current, ticket, after)
                        .map_err(QueueError::protocol)?;
                    let (record, sections) = if let Some(frame) = frame {
                        let mut record = frame.decode_header().map_err(QueueError::protocol)?;
                        record.all_copy_evidence = false;
                        (Some(Box::new(record)), frame.sections)
                    } else {
                        (None, [None, None, None])
                    };
                    Ok(ChargedReply {
                        header: self.shared.encode(
                            &QueueReply::RemovalRecord {
                                worker: session.local(),
                                ticket,
                                after,
                                record,
                                nonce: session.nonce(),
                            },
                            2 * MAX_RECORD_HEADER_BYTES,
                        )?,
                        sections,
                    })
                }
                QueueRequest::AdmissionState { ticket, .. } => {
                    if (0..3).any(|index| sections.section(index).is_some()) {
                        return Err(malformed("admission proof has unexpected payload"));
                    }
                    let observed = self
                        .admission
                        .get()
                        .ok_or_else(unavailable)?
                        .verify(session.remote(), &ticket)
                        .await?;
                    Ok(ChargedReply {
                        header: self.shared.encode_in(
                            ByteClass::Progress,
                            &serde_json::json!({"nonce":session.nonce()}),
                            super::replica::MAX_HEADER_BYTES,
                        )?,
                        sections: [
                            Some(self.shared.encode_in(
                                ByteClass::Progress,
                                &observed,
                                super::replica::MAX_HEADER_BYTES,
                            )?),
                            None,
                            None,
                        ],
                    })
                }
                QueueRequest::JoinStatement { ticket_id, .. } => {
                    if (0..3).any(|index| sections.section(index).is_some())
                        || session.remote() != current.owner
                    {
                        return Err(malformed("invalid join completion reader"));
                    }
                    let statement = {
                        let replica = self.replica.lock().map_err(|_| unavailable())?;
                        let statement = replica.join_statement(current).map_err(QueueError::protocol)?;
                        #[cfg(test)]
                        super::recovery_tests::observe_rolling_copy(&replica, &statement);
                        statement
                    };
                    if statement.ticket.ticket_id != ticket_id
                        || statement.ticket.target != session.local()
                        || statement.source != session.remote()
                    {
                        return Err(malformed("join completion ticket differs"));
                    }
                    #[cfg(test)]
                    super::recovery_tests::pause_rolling_copy().await;
                    let mount = self.join_mount.get().map(|mount| {
                        mount.lock().map(|mount| mount.clone()).map_err(|_| unavailable())
                    }).transpose()?;
                    if let Some(mounts) = mount {
                        self.shared.execute(
                            crate::oltp::pool::ExecutionClass::Control, 0, 0, move || {
                                for (pool, expected_bytes) in mounts.into_iter().flatten() {
                                    if pool.observe_registered_mount()?.total_bytes < expected_bytes {
                                        return Err(unavailable());
                                    }
                                }
                                Ok(())
                            }
                        ).await?;
                    }
                    Ok(ChargedReply {
                        header: self.shared.encode_in(
                            ByteClass::Progress,
                            &QueueReply::JoinStatement {
                                worker: session.local(),
                                statement: Box::new(statement),
                                nonce: session.nonce(),
                            },
                            super::replica::MAX_HEADER_BYTES,
                        )?,
                        sections: [None, None, None],
                    })
                }
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
                    let checked = self
                        .replica
                        .lock()
                        .map_err(|_| unavailable())?
                        .install_retirement_check(&record);
                    if checked.is_some()
                        && self
                            .retirement
                            .get()
                            .ok_or_else(unavailable)?
                            .retirement(record.request_id, record.digest)
                            .await?
                            .is_some()
                    {
                        return Err(QueueError::Stale.protocol());
                    }
                    let (prepared_record, memory, held) = self
                        .prepare_payload(&session, current, &record, &sections)
                        .await?;
                    #[cfg(test)]
                    if record.state.terminal() {
                        super::recovery_tests::pause("before-terminal-copy", record.request_id).await;
                    }
                    #[cfg(test)]
                    if record.ready.view.owner_term > 1 {
                        super::recovery_tests::pause("removal_install", record.request_id).await;
                    }
                    self.check(&session, &QueueRequest::Install { record })
                        .await?;
                    let acknowledgement = self
                        .replica
                        .lock()
                        .map_err(|_| unavailable())?
                        .install_prepared_checked(prepared_record, checked)
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
                        .record(request_id)
                        .ok_or_else(|| QueueError::NotFound.protocol())?;
                    if !record.accepted {
                        return Err(QueueError::Unavailable.protocol());
                    }
                    let copies_complete = if record.state.terminal() {
                        replica.terminal_copy_confirmation(current, record).is_some()
                    } else {
                        replica.copies_complete(request_id)
                    };
                    let reply = QueueReply::State(QueueState {
                        request_id,
                        run_id: view.run_id,
                        version: record.version,
                        state: record.state,
                        copies_complete,
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
                QueueRequest::TerminalState { request_id, .. } => {
                    if (0..3).any(|i| sections.section(i).is_some()) {
                        return Err(malformed("terminal metadata RPC has unexpected payload"));
                    }
                    let statement = {
                        let replica = self.replica.lock().map_err(|_| unavailable())?;
                        if replica.authority() != current {
                            return Err(QueueError::Stale.protocol());
                        }
                        replica
                            .terminal_copy_statement(request_id)
                            .map_err(QueueError::protocol)?
                    };
                    Ok(ChargedReply {
                        header: self.shared.encode_in(
                            ByteClass::Progress,
                            &QueueReply::TerminalState {
                                worker: session.local(),
                                statement: Box::new(statement),
                                nonce: session.nonce(),
                            },
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
                    if replica.terminal_copy_confirmation(current, record).is_none() {
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
pub(crate) fn request_raw_clone_bytes(bytes: &[u8]) -> Result<usize, ProtocolError> {
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
