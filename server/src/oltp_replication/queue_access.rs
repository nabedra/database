//! Queue interfaces used by Control and the hosted replication services.
use super::{
    authority::{
        control::ScopedQueuePlacement,
        status::IdentityReservation,
        timestamp::{ScopedTerminalStatement, ScopedTerminalWitness},
    },
    multiplex::{EndpointFuture, Purpose, ReceivedReply},
    types::*,
};
use crate::{
    oltp::pool::WorkerSharedResources,
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use nabedra_oltp_types::queue::{MAX_HEADER_BYTES, QueueAuthority, QueueReply, QueueScope};
use nabedra_protocol::QueueError;
use std::sync::Arc;
use uuid::Uuid;

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

/// Local queue state consulted while sealing a membership copy.
pub(crate) trait QueueCopySource {
    fn local_identity(&self) -> WorkerIdentity;
    fn authority(&self) -> QueueAuthority;
    fn removal_frozen(&self, ticket: JoinTicket) -> bool;
    fn removal_ready_root(&self, ticket: JoinTicket) -> Result<(Digest, u64), QueueError>;
    fn removal_statement(&self, ticket: JoinTicket) -> Result<QueueCopyStatement, QueueError>;
    fn join_statement(&self, current: QueueAuthority) -> Result<QueueCopyStatement, QueueError>;
    fn empty_bootstrap_statement(
        &self,
        ticket: JoinTicket,
    ) -> Result<QueueCopyStatement, QueueError>;
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

impl RemovalObservation {
    pub(crate) async fn from_reply(
        reply: ReceivedReply,
        group: &GroupIdentity,
        target: WorkerIdentity,
        ticket: JoinTicket,
        shared: &Arc<WorkerSharedResources>,
    ) -> Result<Self, ProtocolError> {
        if reply.remote_identity() != target
            || reply.group() != group
            || reply.purpose() != Purpose::QueueControl
            || reply.sections.iter().any(Option::is_some)
        {
            return Err(malformed("queue removal transport context differs"));
        }
        let decode = crate::oltp::mvcc::json_decode_budget(reply.header.as_ref())?;
        let reply = reply.reserve_reply_decode(shared, decode).await?;
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
}

impl TerminalObservation {
    pub(crate) async fn from_reply(
        reply: ReceivedReply,
        group: &GroupIdentity,
        target: WorkerIdentity,
        expected: &TerminalCopyStatement,
        shared: &Arc<WorkerSharedResources>,
    ) -> Result<Self, ProtocolError> {
        if reply.remote_identity() != target
            || reply.group() != group
            || reply.purpose() != Purpose::QueueControl
            || reply.sections.iter().any(Option::is_some)
        {
            return Err(malformed("terminal metadata transport context differs"));
        }
        if reply.header.len() > MAX_HEADER_BYTES {
            return Err(malformed("terminal metadata header exceeds bounds"));
        }
        let decode = crate::oltp::mvcc::json_decode_budget(reply.header.as_ref())?;
        let reply = reply.reserve_reply_decode(shared, decode).await?;
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
}

impl JoinObservation {
    pub(crate) async fn from_reply(
        reply: ReceivedReply,
        group: &GroupIdentity,
        target: WorkerIdentity,
        expected: &QueueCopyStatement,
        shared: &Arc<WorkerSharedResources>,
    ) -> Result<Self, ProtocolError> {
        if reply.remote_identity() != target
            || reply.group() != group
            || reply.purpose() != Purpose::QueueControl
            || reply.sections.iter().any(Option::is_some)
        {
            return Err(malformed("join completion transport context differs"));
        }
        if reply.header.len() > MAX_HEADER_BYTES {
            return Err(malformed("join completion header exceeds bounds"));
        }
        let decode = crate::oltp::mvcc::json_decode_budget(reply.header.as_ref())?;
        let reply = reply.reserve_reply_decode(shared, decode).await?;
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
}

pub(crate) trait TerminalQueueReader: Send + Sync {
    fn terminal_observation<'a>(
        &'a self,
        worker: WorkerIdentity,
        scope: QueueScope,
        expected: &'a TerminalCopyStatement,
        timestamps_group_id: Uuid,
    ) -> EndpointFuture<'a, TerminalObservation>;

    fn scoped_terminal_state<'a>(
        &'a self,
        worker: WorkerIdentity,
        placement: &'a ScopedQueuePlacement,
        statement: &'a ScopedTerminalStatement,
        source_group: Uuid,
    ) -> EndpointFuture<'a, ScopedTerminalWitness>;
}

/// A trusted runtime caller retains the admission gate and has established
/// that the current owner has no RAM record. Wire input cannot implement this
/// interface. The owner issuer recognizes only its own live capability.
pub(crate) trait NeverAcceptedAdmission: std::any::Any + Send + Sync {
    fn scope(&self) -> &QueueScope;
    fn view(&self) -> AuthorityView;
    fn reservation(&self) -> IdentityReservation;
    fn identity(&self) -> &PinIdentity;
    fn pin(&self) -> Option<StoragePin>;
    fn request(&self) -> Option<RequestBinding>;
    fn failure(&self) -> &ProtocolError;
}

fn malformed(message: &'static str) -> ProtocolError {
    ProtocolError::new(ErrorCode::ValidationError, message, Outcome::NotApplied)
}
