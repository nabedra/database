//! Timestamp proof reads use the same queue control lane as legacy terminal reads.

use super::*;
use crate::oltp_replication::authority::{
    control::{ScopedQueuePlacement, ServiceAction},
    timestamp::{ScopedTerminalStatement, ScopedTerminalWitness, ScopedTerminalWitnessReply},
};

impl QueuePeerClient {
    pub(crate) async fn scoped_terminal_state(
        &self,
        target: WorkerIdentity,
        placement: &ScopedQueuePlacement,
        statement: &ScopedTerminalStatement,
        source_group: Uuid,
    ) -> Result<ScopedTerminalWitness, ProtocolError> {
        placement.validate(&placement.group)?;
        if source_group.is_nil()
            || placement.group.cluster_id != self.group.cluster_id
            || placement.group.database != self.group.database
            || !placement.has_holder(target)
        {
            return Err(QueueError::Stale.protocol());
        }
        statement.for_placement(placement)?;
        let header = self.shared.encode_in(
            ByteClass::Progress,
            &QueueRequest::ScopedTerminalStateV2 {
                scope: QueueScope {
                    cluster_id: self.group.cluster_id,
                    database: self.group.database.clone(),
                },
                source_group,
                statement: Box::new(statement.clone()),
            },
            super::super::replica::MAX_HEADER_BYTES,
        )?;
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
            .map_err(ProtocolError::from)?;
        if reply.remote_identity() != target
            || reply.group() != &self.group
            || reply.purpose() != Purpose::QueueControl
            || reply.header.len() > super::super::replica::MAX_HEADER_BYTES
            || reply.sections.iter().any(Option::is_some)
        {
            return Err(malformed("scoped terminal proof transport context differs"));
        }
        let decode = crate::oltp::mvcc::json_decode_budget(reply.header.as_ref())?;
        let reply = reply.reserve_reply_decode(&self.shared, decode).await?;
        let decoded = reply.decode_header::<ScopedTerminalWitnessReply>()?;
        ScopedTerminalWitness::authenticate(
            &reply,
            &self.group,
            target,
            statement,
            ScopedTerminalWitnessReply {
                worker: decoded.worker,
                statement: decoded.statement.clone(),
                nonce: decoded.nonce,
            },
        )
    }
}

impl QueueEndpoint {
    pub(crate) fn note_scoped_pin_release(
        &self,
        placement: &ScopedQueuePlacement,
        statement: &ScopedTerminalStatement,
        release: &crate::oltp_replication::authority::timestamp::ScopedPinRelease,
    ) -> Result<(), ProtocolError> {
        self.with_scoped_store(|store| store.note_pin_release(placement, statement, release))
    }
    pub(super) async fn scoped_terminal_state(
        &self,
        session: &AuthenticatedSession,
        source_group: Uuid,
        statement: &ScopedTerminalStatement,
    ) -> Result<ChargedReply, ProtocolError> {
        let readers = self.scoped_payload.get().ok_or_else(unavailable)?;
        statement.validate()?;
        if source_group.is_nil()
            || session.recovery_only()
            || session.local() != self.local
            || session.group() != &readers.rpc_group
        {
            return Err(QueueError::Stale.protocol());
        }
        let current = readers
            .control
            .scoped_queue_by_id(statement.epoch.queue_group_id)
            .await?
            .ok_or_else(|| QueueError::Stale.protocol())?;
        statement.for_placement(&current)?;
        let role = readers
            .control
            .service_role(
                &self.scope,
                session.remote(),
                ServiceAction::TimestampProof { source_group },
                current.group.clone(),
            )
            .await?;
        if role.effective_authority.run_id != statement.epoch.view.run_id {
            return Err(QueueError::Stale.protocol());
        }
        let terminal = self.with_scoped_store(|store| {
            store.terminal_statement(&current, statement.request.request_id)
        })?;
        if &terminal != statement {
            return Err(QueueError::Conflict.protocol());
        }
        Ok(ChargedReply {
            header: self.shared.encode_in(
                ByteClass::Progress,
                &ScopedTerminalWitnessReply {
                    worker: self.local,
                    statement: terminal,
                    nonce: session.nonce(),
                },
                super::super::replica::MAX_HEADER_BYTES,
            )?,
            sections: [None, None, None],
        })
    }
    pub(crate) async fn retire_scoped_terminal(
        &self,
        placement: &ScopedQueuePlacement,
        statement: &ScopedTerminalStatement,
        now_ms: u64,
    ) -> Result<bool, ProtocolError> {
        statement.for_placement(placement)?;
        if !self
            .with_scoped_store(|store| store.can_expire_terminal(placement, statement, now_ms))?
        {
            return Ok(false);
        }
        let readers = self.scoped_payload.get().ok_or_else(unavailable)?;
        let Some(release) = readers
            .timestamps
            .confirm_scoped_terminal_release(statement.clone())
            .await?
        else {
            return Ok(false);
        };
        if readers
            .control
            .scoped_queue_by_id(placement.group.group_id)
            .await?
            .as_ref()
            != Some(placement)
        {
            return Err(QueueError::Stale.protocol());
        }
        self.with_scoped_store(|store| {
            store.expire_terminal(placement, statement, &release, now_ms)
        })
    }
}
