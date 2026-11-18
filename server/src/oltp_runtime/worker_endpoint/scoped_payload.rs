//! The first copy step stores only a tentative version in the receiving RAM
//! queue. Accepting or executing the version requires separate quorum evidence.

use super::*;
use crate::{
    oltp::mvcc::{catalog::CatalogHandle, json_decode_budget, request_decode_budget},
    oltp_replication::authority::{
        control::{ControlClient, ScopedQueuePlacement},
        status::IdentityReservation,
    },
    oltp_replication::service_rpc::TimestampClient,
    protocol::{self, Key, Operation},
};
use scoped_store::{
    PreparedScopedPayload, ScopedCopyAck, ScopedCopyStamp, validate_scoped_payload_header,
};
use sha2::{Digest as _, Sha256};

pub(super) struct ScopedPayloadReaders {
    rpc_group: GroupIdentity,
    control: Arc<ControlClient>,
    catalog: CatalogHandle,
    timestamps: Arc<TimestampClient>,
}

impl QueuePeerClient {
    fn check_scoped_peer(
        &self,
        current: &ScopedQueuePlacement,
        target: WorkerIdentity,
    ) -> Result<WorkerIdentity, ProtocolError> {
        current.validate(&current.group)?;
        let local = self.client.local_identity();
        if current.group.cluster_id != self.group.cluster_id
            || current.group.database != self.group.database
            || target == local
            || !current.members.contains(&local)
            || !current.members.contains(&target)
            || local != current.owner && target != current.owner
        {
            return Err(QueueError::Stale.protocol());
        }
        Ok(local)
    }

    pub(crate) async fn install_scoped_payload(
        &self,
        current: &ScopedQueuePlacement,
        target: WorkerIdentity,
        frame: &RecordFrame,
        expected: ScopedCopyStamp,
    ) -> Result<ScopedCopyAck, ProtocolError> {
        if self.check_scoped_peer(current, target)? != current.owner {
            return Err(QueueError::Stale.protocol());
        }
        let header = self.shared.encode(
            &QueueRequest::ScopedPayloadV2 {
                queue_group_id: current.group.group_id,
                data_generation: current.data_generation,
                queue_generation: current.queue_generation,
                record: Box::new(frame.decode_header().map_err(QueueError::protocol)?),
            },
            MAX_RECORD_HEADER_BYTES,
        )?;
        let reply = self
            .client
            .request_classified(
                &self.group,
                target,
                Purpose::QueueData,
                header,
                frame.sections.clone(),
                4 * super::super::replica::MAX_HEADER_BYTES,
            )
            .await
            .map_err(ProtocolError::from)?;
        ScopedCopyAck::authenticate(&reply, &self.group, target, expected)
            .map_err(QueueError::protocol)
    }

    pub(crate) async fn witness_scoped_payload(
        &self,
        current: &ScopedQueuePlacement,
        target: WorkerIdentity,
        expected: ScopedCopyStamp,
    ) -> Result<ScopedCopyAck, ProtocolError> {
        self.check_scoped_peer(current, target)?;
        let (request, version) = expected.payload_claim().map_err(QueueError::protocol)?;
        let header = self.shared.encode_in(
            ByteClass::Progress,
            &QueueRequest::ScopedPayloadWitnessV2 {
                scope: QueueScope {
                    cluster_id: self.group.cluster_id,
                    database: self.group.database.clone(),
                },
                view: current.view,
                queue_group_id: current.group.group_id,
                data_generation: current.data_generation,
                queue_generation: current.queue_generation,
                request,
                version,
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
                4 * super::super::replica::MAX_HEADER_BYTES,
            )
            .await
            .map_err(ProtocolError::from)?;
        ScopedCopyAck::authenticate_payload_witness(&reply, &self.group, target, expected)
            .map_err(QueueError::protocol)
    }
}

impl QueueEndpoint {
    pub(crate) fn install_scoped_payload_readers(
        &self,
        rpc_group: GroupIdentity,
        control: Arc<ControlClient>,
        catalog: CatalogHandle,
        timestamps: Arc<TimestampClient>,
    ) -> Result<(), ProtocolError> {
        if rpc_group.kind != GroupKind::Control
            || rpc_group.cluster_id != self.scope.cluster_id
            || rpc_group.database != self.scope.database
            || rpc_group.validate().is_err()
        {
            return Err(malformed("scoped queue RPC group differs"));
        }
        self.scoped_payload
            .set(ScopedPayloadReaders {
                rpc_group,
                control,
                catalog,
                timestamps,
            })
            .map_err(|_| unavailable())
    }

    async fn scoped_copy_placement(
        &self,
        session: &AuthenticatedSession,
        [queue_group_id, data_generation, queue_generation]: [Uuid; 3],
        view: AuthorityView,
    ) -> Result<ScopedQueuePlacement, ProtocolError> {
        let readers = self.scoped_payload.get().ok_or_else(unavailable)?;
        if session.group() != &readers.rpc_group || session.local() == session.remote() {
            return Err(QueueError::Stale.protocol());
        }
        self.replica
            .lock()
            .map_err(|_| unavailable())?
            .scoped_queues()
            .map_err(QueueError::protocol)?;
        let placement = readers
            .control
            .scoped_queue_by_id(queue_group_id)
            .await?
            .ok_or_else(|| QueueError::Stale.protocol())?;
        if placement.data_generation != data_generation
            || placement.queue_generation != queue_generation
            || placement.view != view
            || !placement.members.contains(&session.remote())
            || !placement.members.contains(&session.local())
            || session.local() != placement.owner && session.remote() != placement.owner
        {
            return Err(QueueError::Stale.protocol());
        }
        Ok(placement)
    }

    async fn scoped_payload_placement(
        &self,
        session: &AuthenticatedSession,
        scope: [Uuid; 3],
        record: &RecordHeader,
    ) -> Result<ScopedQueuePlacement, ProtocolError> {
        let placement = self
            .scoped_copy_placement(session, scope, record.ready.view)
            .await?;
        if placement.owner != session.remote() {
            return Err(QueueError::Stale.protocol());
        }
        validate_scoped_payload_header(&placement, record).map_err(QueueError::protocol)?;
        Ok(placement)
    }

    async fn scoped_payload_reservation(
        &self,
        placement: &ScopedQueuePlacement,
        record: &RecordHeader,
    ) -> Result<IdentityReservation, ProtocolError> {
        self.retirement
            .get()
            .ok_or_else(unavailable)?
            .lookup_scoped_reservation(placement, record.request_id, record.digest)
            .await
    }

    pub(super) async fn preflight_scoped_payload(
        &self,
        session: &AuthenticatedSession,
        scope: [Uuid; 3],
        record: &RecordHeader,
    ) -> Result<ReceivePlan, ProtocolError> {
        let placement = self
            .scoped_payload_placement(session, scope, record)
            .await?;
        self.scoped_payload_reservation(&placement, record).await?;
        Ok(ReceivePlan {
            class: TrafficClass::QueueData,
            section_lengths: record.section_lengths,
            decode_budget_bytes: 0,
        })
    }

    pub(super) async fn install_scoped_payload(
        &self,
        session: &AuthenticatedSession,
        scope: [Uuid; 3],
        record: &RecordHeader,
        sections: &ChargedSections,
    ) -> Result<ChargedReply, ProtocolError> {
        let placement = self
            .scoped_payload_placement(session, scope, record)
            .await?;
        let frame = RecordFrame {
            header: self.shared.encode(record, MAX_RECORD_HEADER_BYTES)?,
            sections: std::array::from_fn(|index| sections.section(index).cloned()),
        };
        let raw = frame.clone();
        let task_limit = self.task_memory_bytes;
        let shared = self.shared.clone();
        let (request, table_id, key, token, scratch) = self
            .shared
            .execute(
                ExecutionClass::Query,
                self.next_decode_job()?,
                0,
                move || decode_route_key(&shared, task_limit, &raw),
            )
            .await?;
        if !matches!(placement.group.kind, GroupKind::Data { table_id: table, .. } if table == table_id)
        {
            return Err(QueueError::Conflict.protocol());
        }
        let readers = self.scoped_payload.get().ok_or_else(unavailable)?;
        let reservation = self.scoped_payload_reservation(&placement, record).await?;
        readers
            .timestamps
            .verify_scoped_snapshot(
                &placement,
                token.identity.clone(),
                token.storage_pin,
                request,
                reservation,
            )
            .await?;
        let route = readers
            .catalog
            .scoped_route(&placement, request, &token, &key)
            .await?;
        drop((key, scratch));
        let validated_placement = placement.clone();
        let shared = self.shared.clone();
        let prepared = self
            .shared
            .execute(
                ExecutionClass::Query,
                self.next_decode_job()?,
                0,
                move || {
                    let route = route.route().ok_or_else(|| QueueError::Stale.protocol())?;
                    PreparedScopedPayload::prepare(
                        &validated_placement,
                        route,
                        reservation,
                        frame,
                        &shared,
                    )
                    .map_err(QueueError::protocol)
                },
            )
            .await?;
        let current = self
            .scoped_payload_placement(session, scope, record)
            .await?;
        if current != placement
            || self.scoped_payload_reservation(&current, record).await? != reservation
        {
            return Err(QueueError::Stale.protocol());
        }
        let ack = self
            .replica
            .lock()
            .map_err(|_| unavailable())?
            .scoped_queues()
            .map_err(QueueError::protocol)?
            .install_payload(&current, session.remote(), prepared)
            .map_err(QueueError::protocol)?;
        self.scoped_copy_reply(session, ack)
    }

    pub(super) async fn scoped_payload_witness(
        &self,
        session: &AuthenticatedSession,
        scope: [Uuid; 3],
        view: AuthorityView,
        request: RequestBinding,
        version: RecordVersion,
    ) -> Result<ChargedReply, ProtocolError> {
        if request.run_id != view.run_id
            || request.request_id.is_nil()
            || version.request_id != request.request_id
            || version.version == 0
            || version.version == u64::MAX
        {
            return Err(malformed("scoped payload witness binding differs"));
        }
        let current = self.scoped_copy_placement(session, scope, view).await?;
        let ack = self
            .replica
            .lock()
            .map_err(|_| unavailable())?
            .scoped_queues()
            .map_err(QueueError::protocol)?
            .payload_witness(&current, request, version)
            .map_err(QueueError::protocol)?;
        self.scoped_copy_reply(session, ack)
    }

    fn scoped_copy_reply(
        &self,
        session: &AuthenticatedSession,
        ack: ScopedCopyAck,
    ) -> Result<ChargedReply, ProtocolError> {
        Ok(ChargedReply {
            header: self.shared.encode_in(
                ByteClass::Progress,
                &ack.reply(session.nonce()),
                super::super::replica::MAX_HEADER_BYTES,
            )?,
            sections: [None, None, None],
        })
    }
}

fn decode_route_key(
    shared: &WorkerSharedResources,
    task_limit: u64,
    frame: &RecordFrame,
) -> Result<(RequestBinding, Uuid, Key, QueueReadToken, ScratchLease), ProtocolError> {
    let body: &[u8] = frame.sections[0].as_ref().ok_or_else(unavailable)?.as_ref();
    let context: &[u8] = frame.sections[1].as_ref().ok_or_else(unavailable)?.as_ref();
    let record = frame.decode_header().map_err(QueueError::protocol)?;
    if [body.len() as u64, context.len() as u64, 0] != record.section_lengths
        || frame.sections[2].is_some()
    {
        return Err(malformed("scoped queue payload lengths differ"));
    }
    let body_decode = request_decode_budget(body)?;
    let context_decode = json_decode_budget(context)?;
    let raw_clone = request_raw_clone_bytes(body)?;
    let working = body_decode
        .checked_add(context_decode)
        .and_then(|n| n.checked_mul(4))
        .and_then(|n| n.checked_add(64 << 10))
        .and_then(|n| n.checked_add(raw_clone))
        .ok_or_else(decode_capacity)?;
    let prepare_working = body_decode
        .checked_mul(4)
        .and_then(|n| n.checked_add(64 << 10))
        .and_then(|n| n.checked_add(frame.header.len()))
        .and_then(|n| n.checked_add(body.len()))
        .and_then(|n| n.checked_add(context.len()))
        .ok_or_else(decode_capacity)?;
    if working.max(prepare_working) as u64 > task_limit {
        return Err(decode_capacity());
    }
    let scratch = shared.reserve_scratch(ByteClass::Data, working)?;
    let request = protocol::parse_request(body)?;
    if request.request_id != record.request_id
        || Digest::from_bytes(Sha256::digest(body).into()) != record.digest
    {
        return Err(QueueError::Conflict.protocol());
    }
    let (table, key) = match request.operation {
        Operation::Get(payload) => (payload.table_id, payload.key),
        Operation::Update(payload) => (payload.table_id, payload.key),
        _ => return Err(malformed("operation cannot use a scoped queue payload")),
    };
    let accepted: AcceptedContext = serde_json::from_slice(context)
        .map_err(|_| malformed("invalid scoped queue accepted context"))?;
    let token = accepted
        .read
        .ok_or_else(|| malformed("missing scoped queue pin"))?;
    let request = RequestBinding {
        run_id: record.ready.view.run_id,
        request_id: record.request_id,
        digest: record.digest,
        accepted_context: Digest::from_bytes(Sha256::digest(context).into()),
    };
    if token.identity.run_id != request.run_id
        || token.identity.request_id != request.request_id
        || token.identity.digest != request.digest
    {
        return Err(QueueError::Conflict.protocol());
    }
    let key_heap = match &key {
        Key::Utf8(text) => text.capacity(),
        Key::Int64(_) => 0,
    };
    scratch.shrink_unused(key_heap)?;
    Ok((request, table, key, token, scratch))
}
