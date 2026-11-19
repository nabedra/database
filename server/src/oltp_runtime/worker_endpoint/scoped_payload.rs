//! RF3 queue copies and authenticated witnesses over the worker transport.

use super::*;
use crate::oltp::pool::ChargedBytes;
use crate::{
    oltp::mvcc::{catalog::CatalogHandle, json_decode_budget, request_decode_budget},
    oltp_replication::authority::{
        control::{
            ControlClient, ScopedQueueInventory, ScopedQueuePlacement, ScopedQueueTransition,
        },
        status::{IdentityReservation, RequestKind, VerifiedSettlement, VerifiedStatusDecision},
    },
    oltp_replication::service_rpc::TimestampClient,
    protocol::{self, Key, Operation},
};
use futures_util::{StreamExt, stream::FuturesUnordered};
use scoped_store::{
    PreparedScopedPayload, PreparedScopedTerminal, PreparedScopedTransfer, ScopedAdmissionPermit,
    ScopedCopyAck, ScopedCopyQuorum, ScopedCopyStamp, ScopedLocalEpoch, ScopedQueueStore,
    ScopedSurvivor, ScopedTerminalProof, ScopedTransferRecord, append_scoped_inventory,
    placement_digest, scoped_inventory, validate_scoped_payload_header,
    validate_scoped_terminal_header,
};
use sha2::{Digest as _, Sha256};
use std::{future::Future, time::Duration};
use tokio::time::{Instant, sleep, timeout_at};

pub(super) struct ScopedPayloadReaders {
    pub(super) rpc_group: GroupIdentity,
    pub(super) control: Arc<ControlClient>,
    catalog: CatalogHandle,
    pub(super) timestamps: Arc<TimestampClient>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopedSurvivorReply {
    transfer_id: Uuid,
    basis: Digest,
    worker: WorkerIdentity,
    nonce: Uuid,
    after: Option<Uuid>,
    record: Option<ScopedTransferRecord>,
}

type ScopedTransferFrame = (ScopedTransferRecord, [Option<ChargedBytes>; 3]);

enum TerminalValidation {
    Read,
    Mutation(VerifiedStatusDecision, VerifiedSettlement),
    Replica(ScopedCopyAck),
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
            || !current.has_holder(local)
            || !current.has_holder(target)
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
        self.scoped_copy_call(
            target,
            Purpose::QueueData,
            header,
            frame.sections.clone(),
            expected,
        )
        .await
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
        self.scoped_copy_call(
            target,
            Purpose::QueueControl,
            header,
            [None, None, None],
            expected,
        )
        .await
    }

    async fn scoped_marker_call(
        &self,
        current: &ScopedQueuePlacement,
        target: WorkerIdentity,
        payload: ScopedCopyStamp,
        witness: bool,
    ) -> Result<ScopedCopyAck, ProtocolError> {
        let local = self.check_scoped_peer(current, target)?;
        if (witness && target != current.owner) || (!witness && local != current.owner) {
            return Err(QueueError::Stale.protocol());
        }
        let (request, version) = payload.payload_claim().map_err(QueueError::protocol)?;
        let header = self.shared.encode_in(
            ByteClass::Progress,
            &QueueRequest::ScopedMarkerV2 {
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
                witness,
            },
            super::super::replica::MAX_HEADER_BYTES,
        )?;
        self.scoped_copy_call(
            target,
            Purpose::QueueControl,
            header,
            [None, None, None],
            payload.marker(),
        )
        .await
    }

    async fn copy_scoped_terminal(
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
            &QueueRequest::ScopedTerminalV2 {
                queue_group_id: current.group.group_id,
                data_generation: current.data_generation,
                queue_generation: current.queue_generation,
                record: Box::new(frame.decode_header().map_err(QueueError::protocol)?),
                version: expected.version(),
            },
            MAX_RECORD_HEADER_BYTES,
        )?;
        self.scoped_copy_call(
            target,
            Purpose::QueueData,
            header,
            [None, None, frame.sections[2].clone()],
            expected,
        )
        .await
    }

    async fn witness_scoped_terminal(
        &self,
        current: &ScopedQueuePlacement,
        payload: ScopedCopyStamp,
        terminal: RecordVersion,
    ) -> Result<ScopedCopyAck, ProtocolError> {
        let target = current.owner;
        self.check_scoped_peer(current, target)?;
        let (request, version) = payload.payload_claim().map_err(QueueError::protocol)?;
        let header = self.shared.encode_in(
            ByteClass::Progress,
            &QueueRequest::ScopedTerminalWitnessV2 {
                scope: QueueScope {
                    cluster_id: self.group.cluster_id,
                    database: self.group.database.clone(),
                },
                view: current.view,
                queue_group_id: current.group.group_id,
                data_generation: current.data_generation,
                queue_generation: current.queue_generation,
                request,
                payload: version,
                terminal,
            },
            super::super::replica::MAX_HEADER_BYTES,
        )?;
        self.scoped_copy_call(
            target,
            Purpose::QueueControl,
            header,
            [None, None, None],
            payload.terminal(terminal),
        )
        .await
    }

    async fn repair_scoped_terminal(
        &self,
        current: &ScopedQueuePlacement,
        target: WorkerIdentity,
        retained: &ScopedSurvivor,
        expected: ScopedCopyStamp,
    ) -> Result<ScopedCopyAck, ProtocolError> {
        if self.check_scoped_peer(current, target)? != current.owner {
            return Err(QueueError::Stale.protocol());
        }
        let (record, sections) = retained
            .transfer_frame(&self.shared)
            .map_err(QueueError::protocol)?;
        let header = self.shared.encode(
            &QueueRequest::ScopedTerminalRepairV2 {
                queue_group_id: current.group.group_id,
                data_generation: current.data_generation,
                queue_generation: current.queue_generation,
                record: Box::new(record),
            },
            MAX_RECORD_HEADER_BYTES,
        )?;
        self.scoped_copy_call(target, Purpose::QueueData, header, sections, expected)
            .await
    }

    async fn scoped_copy_call(
        &self,
        target: WorkerIdentity,
        purpose: Purpose,
        header: ChargedBytes,
        sections: [Option<ChargedBytes>; 3],
        expected: ScopedCopyStamp,
    ) -> Result<ScopedCopyAck, ProtocolError> {
        let reply = self
            .client
            .request_classified(&self.group, target, purpose, header, sections, 0)
            .await
            .map_err(ProtocolError::from)?;
        if reply.header.len() > super::super::replica::MAX_HEADER_BYTES
            || reply.sections.iter().any(Option::is_some)
        {
            return Err(malformed("scoped copy acknowledgement exceeds its bounds"));
        }
        let decode = json_decode_budget(reply.header.as_ref())?;
        let reply = reply.reserve_reply_decode(&self.shared, decode).await?;
        ScopedCopyAck::authenticate(&reply, &self.group, target, expected, purpose)
            .map_err(QueueError::protocol)
    }

    async fn fetch_scoped_survivor(
        &self,
        transition: &ScopedQueueTransition,
        source: WorkerIdentity,
        basis: Digest,
        after: Option<Uuid>,
    ) -> Result<Option<ScopedTransferFrame>, ProtocolError> {
        let next = &transition.next;
        if !next.has_holder(self.client.local_identity()) || !transition.previous.has_holder(source)
        {
            return Err(QueueError::Stale.protocol());
        }
        let header = self.shared.encode_in(
            ByteClass::Progress,
            &QueueRequest::ScopedSurvivorV2 {
                scope: QueueScope {
                    cluster_id: self.group.cluster_id,
                    database: self.group.database.clone(),
                },
                view: next.view,
                queue_group_id: next.group.group_id,
                transfer_id: transition.id,
                after,
            },
            super::super::replica::MAX_HEADER_BYTES,
        )?;
        let reply = self
            .client
            .request_classified(
                &self.group,
                source,
                Purpose::QueueData,
                header,
                [None, None, None],
                0,
            )
            .await
            .map_err(ProtocolError::from)?;
        if reply.group() != &self.group
            || reply.remote_identity() != source
            || reply.purpose() != Purpose::QueueData
        {
            return Err(QueueError::Stale.protocol());
        }
        let decode = json_decode_budget(reply.header.as_ref())?;
        let reply = reply.reserve_decode(&self.shared, decode)?;
        let wire = reply.decode_header::<ScopedSurvivorReply>()?;
        if wire.worker != source
            || wire.nonce != reply.nonce()
            || wire.transfer_id != transition.id
            || wire.basis != basis
            || wire.after != after
        {
            return Err(QueueError::Stale.protocol());
        }
        match &wire.record {
            Some(record) if record.request_id() > after.unwrap_or(Uuid::nil()) => {
                Ok(Some((record.clone(), reply.sections.clone())))
            }
            None if reply.sections.iter().all(Option::is_none) => Ok(None),
            _ => Err(QueueError::Conflict.protocol()),
        }
    }
}

impl QueueEndpoint {
    pub(crate) fn install_scoped_queue_peer(
        &self,
        peer: Arc<QueuePeerClient>,
    ) -> Result<(), ProtocolError> {
        let readers = self.scoped_payload.get().ok_or_else(unavailable)?;
        if peer.group != readers.rpc_group || peer.client.local_identity() != self.local {
            return Err(QueueError::Stale.protocol());
        }
        self.scoped_peer.set(peer).map_err(|_| unavailable())
    }

    pub(super) fn with_scoped_store<T>(
        &self,
        f: impl FnOnce(&mut ScopedQueueStore) -> Result<T, QueueError>,
    ) -> Result<T, ProtocolError> {
        let mut replica = self.replica.lock().map_err(|_| unavailable())?;
        let store = replica.scoped_queues().map_err(QueueError::protocol)?;
        f(store).map_err(QueueError::protocol)
    }

    pub(crate) fn reserve_scoped_payload(
        &self,
        body_bytes: usize,
    ) -> Result<ScopedAdmissionPermit, ProtocolError> {
        self.with_scoped_store(|store| store.reserve_admission(body_bytes))
    }

    #[cfg(test)]
    async fn observe_scoped_copy(
        stage: &str,
        placement: &ScopedQueuePlacement,
        quorum: ScopedCopyQuorum,
    ) {
        #[derive(Serialize)]
        struct Copy<'a> {
            placement: &'a ScopedQueuePlacement,
            stamp: ScopedCopyStamp,
            holders: [WorkerIdentity; 2],
        }
        let stamp = quorum.stamp();
        super::super::recovery_tests::transaction_checkpoint(
            stage,
            stamp.binding().request_id,
            &Copy {
                placement,
                stamp,
                holders: quorum.holders(),
            },
        )
        .await;
    }

    async fn require_scoped_owner(
        &self,
        expected: &ScopedQueuePlacement,
    ) -> Result<(), ProtocolError> {
        let readers = self.scoped_payload.get().ok_or_else(unavailable)?;
        if expected.owner != self.local
            || readers
                .control
                .scoped_queue(&expected.group)
                .await?
                .as_ref()
                != Some(expected)
        {
            return Err(QueueError::Stale.protocol());
        }
        Ok(())
    }

    /// The same deadline covers both copy rounds and their authority checks.
    /// Once any install starts, a replication failure leaves the outcome unknown.
    pub(crate) async fn accept_scoped(
        &self,
        peer: &QueuePeerClient,
        current: &ScopedQueuePlacement,
        frame: &RecordFrame,
        deadline: Instant,
        admission: Option<ScopedAdmissionPermit>,
    ) -> Result<ScopedCopyQuorum, ProtocolError> {
        timeout_at(deadline, async {
            let revision =
                self.with_scoped_store(|store| Ok(store.write_revision(current.group.group_id)))?;
            self.require_scoped_owner(current).await?;
            let prepared = self.prepare_scoped_payload(current, frame.clone()).await?;
            let stamp = prepared.stamp();
            self.require_scoped_owner(current).await?;
            let local = self.with_scoped_store(|store| {
                store.require_write_revision(current.group.group_id, revision)?;
                store.install_payload(current, self.local, prepared, admission)
            })?;
            let mut copies = FuturesUnordered::new();
            for target in current
                .members
                .into_iter()
                .filter(|target| *target != self.local && current.has_holder(*target))
            {
                copies.push(async move {
                    (
                        target,
                        peer.install_scoped_payload(current, target, frame, stamp)
                            .await,
                    )
                });
            }
            let (first, remote) = loop {
                match copies.next().await {
                    Some((target, Ok(ack))) => break (target, ack),
                    Some((_, Err(_))) => {}
                    None => return Err(unavailable()),
                }
            };
            let installation = ScopedCopyQuorum::confirm(current, stamp, local, remote)
                .map_err(QueueError::protocol)?;
            #[cfg(test)]
            Self::observe_scoped_copy("scoped-payload-quorum", current, installation).await;
            self.require_scoped_owner(current).await?;
            let marker = self.with_scoped_store(|store| {
                store.install_marker(current, self.local, installation, stamp.binding())
            })?;
            // Keep the other upload in flight. Each marker requires its
            // target's payload ACK, and a slow third holder does not delay us.
            let mark_first = peer.scoped_marker_call(current, first, stamp, false);
            let mark_other = async {
                let (target, installed) = copies.next().await.ok_or_else(unavailable)?;
                installed?;
                peer.scoped_marker_call(current, target, stamp, false).await
            };
            tokio::pin!(mark_first, mark_other);
            let remote = tokio::select! {
                result = &mut mark_first => match result {
                    Ok(ack) => ack,
                    Err(_) => mark_other.await.map_err(|_| unavailable())?,
                },
                result = &mut mark_other => match result {
                    Ok(ack) => ack,
                    Err(_) => mark_first.await.map_err(|_| unavailable())?,
                },
            };
            let accepted = ScopedCopyQuorum::confirm(current, stamp.marker(), marker, remote)
                .map_err(QueueError::protocol)?;
            self.require_scoped_owner(current).await?;
            if Instant::now() >= deadline {
                return Err(unavailable());
            }
            self.with_scoped_store(|store| store.confirm_copies(current, accepted))?;
            #[cfg(test)]
            Self::observe_scoped_copy("scoped-marker-quorum", current, accepted).await;
            Ok(accepted)
        })
        .await
        .map_err(|_| unavailable())?
    }

    async fn scoped_majority<F, Call>(
        &self,
        current: &ScopedQueuePlacement,
        stamp: ScopedCopyStamp,
        local: ScopedCopyAck,
        copy: Call,
    ) -> Result<ScopedCopyQuorum, ProtocolError>
    where
        F: Future<Output = Result<ScopedCopyAck, ProtocolError>>,
        Call: Fn(WorkerIdentity) -> F,
    {
        let mut replies = FuturesUnordered::new();
        for target in current
            .members
            .into_iter()
            .filter(|target| *target != self.local && current.has_holder(*target))
        {
            replies.push(copy(target));
        }
        while let Some(result) = replies.next().await {
            if let Ok(remote) = result {
                return ScopedCopyQuorum::confirm(current, stamp, local, remote)
                    .map_err(QueueError::protocol);
            }
        }
        Err(QueueError::Unavailable.protocol())
    }

    pub(crate) async fn scoped_execution_record(
        &self,
        current: &ScopedQueuePlacement,
        id: Uuid,
    ) -> Result<ScopedSurvivor, ProtocolError> {
        self.require_scoped_owner(current).await?;
        self.with_scoped_store(|store| {
            let record = store.record(current, id)?;
            if record.acceptance.is_none() || record.terminal_stamp().is_some() {
                return Err(QueueError::Unavailable);
            }
            Ok(record.clone())
        })
    }

    pub(crate) async fn scoped_record(
        &self,
        current: &ScopedQueuePlacement,
        id: Uuid,
    ) -> Result<ScopedSurvivor, ProtocolError> {
        self.require_scoped_owner(current).await?;
        self.with_scoped_store(|store| store.record(current, id).cloned())
    }

    pub(crate) async fn next_scoped_record(
        &self,
        current: &ScopedQueuePlacement,
        after: Option<Uuid>,
    ) -> Result<Option<ScopedSurvivor>, ProtocolError> {
        self.require_scoped_owner(current).await?;
        self.with_scoped_store(|store| store.owner_next(current, after))
    }

    /// A source marker proves the old record reached the marker phase. The
    /// frozen source quorum intersects every acknowledged old marker quorum,
    /// but a single surviving marker can also come from an interrupted round.
    /// In either case rebuild both copy proofs before granting execution.
    pub(crate) async fn restore_scoped_acceptance(
        &self,
        peer: &QueuePeerClient,
        current: &ScopedQueuePlacement,
        id: Uuid,
        deadline: Instant,
    ) -> Result<ScopedCopyQuorum, ProtocolError> {
        timeout_at(deadline, async {
            let record = self.scoped_record(current, id).await?;
            if let Some(accepted) = record.acceptance {
                return Ok(accepted);
            }
            if !record.recovered_marker {
                return Err(QueueError::Unavailable.protocol());
            }
            let stamp = record.stamp;
            let local = self.with_scoped_store(|store| store.copy_witness(current, stamp))?;
            let installed = self
                .scoped_majority(current, stamp, local, |target| {
                    peer.witness_scoped_payload(current, target, stamp)
                })
                .await?;
            self.require_scoped_owner(current).await?;
            let marker = self.with_scoped_store(|store| {
                store.install_marker(current, self.local, installed, stamp.binding())
            })?;
            let accepted = self
                .scoped_majority(current, stamp.marker(), marker, |target| {
                    peer.scoped_marker_call(current, target, stamp, false)
                })
                .await?;
            self.require_scoped_owner(current).await?;
            if Instant::now() >= deadline {
                return Err(unavailable());
            }
            self.with_scoped_store(|store| store.confirm_copies(current, accepted))?;
            Ok(accepted)
        })
        .await
        .map_err(|_| unavailable())?
    }

    pub(crate) async fn confirm_scoped_terminal(
        &self,
        peer: &QueuePeerClient,
        current: &ScopedQueuePlacement,
        frame: &RecordFrame,
        decision: Option<(TxnId, ManifestRef)>,
        deadline: Instant,
    ) -> Result<ScopedCopyQuorum, ProtocolError> {
        timeout_at(deadline, async {
            let id = frame
                .decode_header()
                .map_err(QueueError::protocol)?
                .request_id;
            let record = self.scoped_record(current, id).await?;
            let accepted = record.acceptance.ok_or_else(unavailable)?;
            let proof = self
                .scoped_terminal_validation(current, &record, decision)
                .await?;
            let prepared = self
                .prepare_scoped_terminal(
                    current,
                    accepted,
                    record.reservation,
                    proof,
                    frame.clone(),
                )
                .await?;
            let stamp = prepared.stamp();
            self.require_scoped_owner(current).await?;
            let local = self
                .with_scoped_store(|store| store.install_terminal(current, self.local, prepared))?;
            if let Some(quorum) = record.terminal_quorum {
                if quorum.stamp() == stamp {
                    return Ok(quorum);
                }
                return Err(QueueError::Conflict.protocol());
            }
            let retained = self.with_scoped_store(|store| store.record(current, id).cloned())?;
            let mut backoff = Duration::from_millis(10);
            let quorum = loop {
                self.require_scoped_owner(current).await?;
                let retained = &retained;
                let attempt = self
                    .scoped_majority(current, stamp, local, |target| async move {
                        match peer
                            .copy_scoped_terminal(current, target, frame, stamp)
                            .await
                        {
                            Err(error) if error == QueueError::NotFound.protocol() => {
                                peer.repair_scoped_terminal(current, target, retained, stamp)
                                    .await
                            }
                            result => result,
                        }
                    })
                    .await;
                if let Ok(quorum) = attempt {
                    break quorum;
                }
                sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_millis(250));
            };
            self.require_scoped_owner(current).await?;
            if Instant::now() >= deadline {
                return Err(unavailable());
            }
            self.with_scoped_store(|store| store.confirm_copies(current, quorum))?;
            #[cfg(test)]
            Self::observe_scoped_copy("scoped-terminal-quorum", current, quorum).await;
            Ok(quorum)
        })
        .await
        .map_err(|_| unavailable())?
    }

    async fn scoped_transition(
        &self,
        group_id: Uuid,
        transfer_id: Uuid,
    ) -> Result<ScopedQueueTransition, ProtocolError> {
        let readers = self.scoped_payload.get().ok_or_else(unavailable)?;
        let transition = readers
            .control
            .scoped_queue_transition(group_id)
            .await?
            .filter(|transition| transition.id == transfer_id)
            .ok_or_else(|| QueueError::Stale.protocol())?;
        let (source, next) = (&transition.previous, &transition.next);
        source.validate(&source.group)?;
        next.validate(&source.group)?;
        if source.group != next.group
            || source.group.group_id != group_id
            || source.view.run_id != next.view.run_id
            || source.queue_generation != next.queue_generation
            || source.view.owner_term >= next.view.owner_term
            || source.view.membership_revision > next.view.membership_revision
        {
            return Err(QueueError::Stale.protocol());
        }
        Ok(transition)
    }

    pub(crate) async fn freeze_scoped_transfer(
        &self,
        group_id: Uuid,
        transfer_id: Uuid,
    ) -> Result<ScopedQueueInventory, ProtocolError> {
        let revision = self.with_scoped_store(|store| Ok(store.write_revision(group_id)))?;
        let transition = self.scoped_transition(group_id, transfer_id).await?;
        let source = &transition.previous;
        self.with_scoped_store(|store| {
            store.require_write_revision(group_id, revision)?;
            store.freeze(source)
        })?;
        let mut inventory = scoped_inventory(&transition, source).map_err(QueueError::protocol)?;
        let mut after = None;
        while let Some(record) =
            self.with_scoped_store(|store| store.export_frozen_next(source, after))?
        {
            append_scoped_inventory(&mut inventory, record.inventory_entry())
                .map_err(QueueError::protocol)?;
            after = Some(record.stamp.binding().request_id);
            tokio::task::yield_now().await;
        }
        if self
            .scoped_transition(group_id, transfer_id)
            .await?
            .previous
            != *source
        {
            return Err(QueueError::Stale.protocol());
        }
        Ok(inventory)
    }

    pub(crate) async fn install_scoped_transfer(
        &self,
        peer: &QueuePeerClient,
        group_id: Uuid,
        transfer_id: Uuid,
        deadline: Instant,
    ) -> Result<(Digest, ScopedQueueInventory), ProtocolError> {
        timeout_at(deadline, async {
            let transition = self.scoped_transition(group_id, transfer_id).await?;
            let basis = transition.basis()?.ok_or_else(unavailable)?;
            if !transition.next.has_holder(self.local) {
                return Err(QueueError::Stale.protocol());
            }
            self.cleanup_scoped_group(group_id).await?;
            let revision = self.with_scoped_store(|store| Ok(store.write_revision(group_id)))?;
            // A lost report can reuse the already installed cut without
            // allocating a duplicate copy or pulling the source cuts again.
            let mut cached =
                scoped_inventory(&transition, &transition.next).map_err(QueueError::protocol)?;
            let mut after = None;
            while let Some(entry) =
                self.with_scoped_store(|store| store.candidate_next(&transition, after))?
            {
                append_scoped_inventory(&mut cached, entry).map_err(QueueError::protocol)?;
                after = Some(entry.request_id());
                tokio::task::yield_now().await;
            }
            if after.is_some() {
                self.check_scoped_install(&transition, basis, cached)
                    .await?;
                self.with_scoped_store(|store| store.require_write_revision(group_id, revision))?;
                return Ok((basis, cached));
            }
            loop {
                let (cursor, revision) = self.with_scoped_store(|store| {
                    Ok((
                        store.transfer_cursor(&transition, basis)?,
                        store.write_revision(group_id),
                    ))
                })?;
                let Some(source) = transition.previous.members.get(cursor.source).copied() else {
                    break;
                };
                if !transition.previous.has_holder(source) {
                    return Err(QueueError::Stale.protocol());
                }
                let incoming = if source == self.local {
                    self.with_scoped_store(|store| {
                        store.export_frozen_next(&transition.previous, cursor.after)
                    })?
                    .map(|record| record.transfer_frame(&self.shared))
                    .transpose()
                    .map_err(QueueError::protocol)?
                } else {
                    peer.fetch_scoped_survivor(&transition, source, basis, cursor.after)
                        .await?
                };
                let prepared = if let Some((frame, sections)) = incoming {
                    let shared = self.shared.clone();
                    let transfer = transition.clone();
                    let task_limit = self.task_memory_bytes;
                    Some(
                        self.shared
                            .execute(
                                ExecutionClass::Query,
                                self.next_decode_job()?,
                                0,
                                move || {
                                    PreparedScopedTransfer::prepare(
                                        &transfer.previous,
                                        &transfer.next,
                                        frame,
                                        sections,
                                        &shared,
                                        task_limit,
                                    )
                                    .map_err(QueueError::protocol)
                                },
                            )
                            .await?,
                    )
                } else {
                    None
                };
                self.with_scoped_store(|store| {
                    store.require_write_revision(group_id, revision)?;
                    store.advance_transfer(&transition, basis, cursor, prepared)
                })?;
            }
            let (inventory, revision) = self.with_scoped_store(|store| {
                Ok((
                    store.transfer_inventory(&transition, basis)?,
                    store.write_revision(group_id),
                ))
            })?;
            let current = self
                .check_scoped_install(&transition, basis, inventory)
                .await?;
            self.with_scoped_store(|store| {
                store.require_write_revision(group_id, revision)?;
                store.finish_transfer(&current, basis)
            })?;
            Ok((basis, inventory))
        })
        .await
        .map_err(|_| unavailable())?
    }

    async fn check_scoped_install(
        &self,
        transition: &ScopedQueueTransition,
        basis: Digest,
        inventory: ScopedQueueInventory,
    ) -> Result<ScopedQueueTransition, ProtocolError> {
        let current = self
            .scoped_transition(transition.previous.group.group_id, transition.id)
            .await?;
        if current.previous != transition.previous
            || current.next != transition.next
            || current.basis()? != Some(basis)
        {
            return Err(QueueError::Stale.protocol());
        }
        if current
            .installed
            .iter()
            .flatten()
            .any(|reported| *reported != inventory)
        {
            return Err(QueueError::Conflict.protocol());
        }
        Ok(current)
    }

    /// A later published epoch can retire this RAM even when the worker
    /// missed publication or another transfer has since begun.
    pub(crate) async fn release_scoped_transfer(
        &self,
        transition: &ScopedQueueTransition,
    ) -> Result<(), ProtocolError> {
        let source = &transition.previous;
        let epoch = ScopedLocalEpoch::published(source).map_err(QueueError::protocol)?;
        let readers = self.scoped_payload.get().ok_or_else(unavailable)?;
        let group = source.group.group_id;
        if !readers
            .control
            .scoped_queue_epoch_retired(group, epoch.queue_generation, epoch.view)
            .await?
        {
            return Err(QueueError::Stale.protocol());
        }
        self.with_scoped_store(|store| store.release_epoch(group, epoch))
    }

    pub(crate) async fn cleanup_scoped_group(&self, group: Uuid) -> Result<(), ProtocolError> {
        const EPOCHS_PER_PASS: usize = 3;
        let readers = self.scoped_payload.get().ok_or_else(unavailable)?;
        let mut after = None;
        for _ in 0..EPOCHS_PER_PASS {
            let Some(epoch) =
                self.with_scoped_store(|store| Ok(store.cleanup_epoch(group, after)))?
            else {
                break;
            };
            after = Some(epoch.placement);
            let retired = readers
                .control
                .scoped_queue_epoch_retired(group, epoch.queue_generation, epoch.view)
                .await?;
            let replaced = if !retired && let Some((id, source)) = epoch.candidate {
                readers
                    .control
                    .scoped_queue_transition(group)
                    .await?
                    .is_some_and(|pending| {
                        pending.id != id
                            && pending.next.queue_generation == epoch.queue_generation
                            && pending.next.view.run_id == epoch.view.run_id
                            && pending.previous.view.owner_term < epoch.view.owner_term
                            && pending.next.view.owner_term > epoch.view.owner_term
                            && placement_digest(&pending.previous) == Ok(source)
                    })
            } else {
                false
            };
            if retired || replaced {
                self.with_scoped_store(|store| store.release_epoch(group, epoch))?;
            }
        }
        Ok(())
    }

    pub(super) async fn export_scoped_survivor(
        &self,
        session: &AuthenticatedSession,
        group_id: Uuid,
        transfer_id: Uuid,
        view: AuthorityView,
        after: Option<Uuid>,
    ) -> Result<ChargedReply, ProtocolError> {
        let readers = self.scoped_payload.get().ok_or_else(unavailable)?;
        if session.group() != &readers.rpc_group || session.remote() == session.local() {
            return Err(QueueError::Stale.protocol());
        }
        let transition = self.scoped_transition(group_id, transfer_id).await?;
        let basis = transition.basis()?.ok_or_else(unavailable)?;
        if transition.next.view != view
            || !transition.next.has_holder(session.remote())
            || !transition.previous.has_holder(session.local())
            || !transition
                .previous
                .members
                .into_iter()
                .zip(transition.frozen)
                .any(|(source, inventory)| source == session.local() && inventory.is_some())
        {
            return Err(QueueError::Stale.protocol());
        }
        let survivor =
            self.with_scoped_store(|store| store.export_frozen_next(&transition.previous, after))?;
        let (record, sections) = match survivor {
            Some(record) => {
                let (record, sections) = record
                    .transfer_frame(&self.shared)
                    .map_err(QueueError::protocol)?;
                (Some(record), sections)
            }
            None => (None, [None, None, None]),
        };
        Ok(ChargedReply {
            header: self.shared.encode(
                &ScopedSurvivorReply {
                    transfer_id,
                    basis,
                    worker: session.local(),
                    nonce: session.nonce(),
                    after,
                    record,
                },
                crate::oltp_replication::multiplex::MAX_HEADER_BYTES,
            )?,
            sections,
        })
    }

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
            || !placement.has_holder(session.remote())
            || !placement.has_holder(session.local())
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
        #[cfg(test)]
        super::super::recovery_tests::pause("before-scoped-payload-copy", record.request_id).await;
        let revision = self.with_scoped_store(|store| Ok(store.write_revision(scope[0])))?;
        let placement = self
            .scoped_payload_placement(session, scope, record)
            .await?;
        let frame = RecordFrame {
            header: self.shared.encode(record, MAX_RECORD_HEADER_BYTES)?,
            sections: std::array::from_fn(|index| sections.section(index).cloned()),
        };
        let prepared = self.prepare_scoped_payload(&placement, frame).await?;
        let current = self
            .scoped_payload_placement(session, scope, record)
            .await?;
        if current != placement {
            return Err(QueueError::Stale.protocol());
        }
        let ack = self.with_scoped_store(|store| {
            store.require_write_revision(scope[0], revision)?;
            store.install_payload(&current, session.remote(), prepared, None)
        })?;
        self.scoped_copy_reply(session, ack)
    }

    async fn prepare_scoped_payload(
        &self,
        placement: &ScopedQueuePlacement,
        frame: RecordFrame,
    ) -> Result<PreparedScopedPayload, ProtocolError> {
        let record = frame.decode_header().map_err(QueueError::protocol)?;
        validate_scoped_payload_header(placement, &record).map_err(QueueError::protocol)?;
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
        let reservation = self.scoped_payload_reservation(placement, &record).await?;
        readers
            .timestamps
            .verify_scoped_snapshot(
                placement,
                token.identity.clone(),
                token.storage_pin,
                request,
                reservation,
            )
            .await?;
        let route = readers
            .catalog
            .scoped_route(placement, request, &token, &key)
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
        if self.scoped_payload_reservation(placement, &record).await? != reservation {
            return Err(QueueError::Stale.protocol());
        }
        Ok(prepared)
    }

    pub(super) async fn scoped_payload_witness(
        &self,
        session: &AuthenticatedSession,
        scope: [Uuid; 3],
        view: AuthorityView,
        request: RequestBinding,
        version: RecordVersion,
    ) -> Result<ChargedReply, ProtocolError> {
        let current = self.scoped_copy_placement(session, scope, view).await?;
        let ack =
            self.with_scoped_store(|store| store.payload_witness(&current, request, version))?;
        self.scoped_copy_reply(session, ack)
    }

    pub(super) async fn scoped_marker(
        &self,
        session: &AuthenticatedSession,
        scope: [Uuid; 3],
        view: AuthorityView,
        (request, version, witness): (RequestBinding, RecordVersion, bool),
    ) -> Result<ChargedReply, ProtocolError> {
        let current = self.scoped_copy_placement(session, scope, view).await?;
        let stamp =
            self.with_scoped_store(|store| store.claim_payload(&current, request, version))?;
        let ack = if witness && current.owner == session.local() {
            self.with_scoped_store(|store| store.copy_witness(&current, stamp.marker()))?
        } else if !witness && current.owner == session.remote() {
            #[cfg(test)]
            super::super::recovery_tests::pause("before-scoped-marker-copy", request.request_id)
                .await;
            let local = self.with_scoped_store(|store| store.copy_witness(&current, stamp))?;
            let owner = self
                .scoped_peer
                .get()
                .ok_or_else(unavailable)?
                .witness_scoped_payload(&current, current.owner, stamp)
                .await?;
            let proof = ScopedCopyQuorum::confirm(&current, stamp, local, owner)
                .map_err(QueueError::protocol)?;
            if self.scoped_copy_placement(session, scope, view).await? != current {
                return Err(QueueError::Stale.protocol());
            }
            self.with_scoped_store(|store| {
                store.install_marker(&current, session.remote(), proof, request)
            })?
        } else {
            return Err(QueueError::Stale.protocol());
        };
        self.scoped_copy_reply(session, ack)
    }

    async fn scoped_terminal_placement(
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
        validate_scoped_terminal_header(&placement, record).map_err(QueueError::protocol)?;
        Ok(placement)
    }

    pub(super) async fn preflight_scoped_terminal(
        &self,
        session: &AuthenticatedSession,
        scope: [Uuid; 3],
        record: &RecordHeader,
    ) -> Result<ReceivePlan, ProtocolError> {
        let current = self
            .scoped_terminal_placement(session, scope, record)
            .await?;
        self.with_scoped_store(|store| {
            let retained = store.record(&current, record.request_id)?;
            if retained.stamp.binding().digest != record.digest {
                return Err(QueueError::Conflict);
            }
            Ok(())
        })?;
        Ok(ReceivePlan {
            class: TrafficClass::QueueData,
            section_lengths: [0, 0, record.section_lengths[2]],
            decode_budget_bytes: 0,
        })
    }

    pub(super) async fn preflight_scoped_terminal_repair(
        &self,
        session: &AuthenticatedSession,
        scope: [Uuid; 3],
        record: &ScopedTransferRecord,
    ) -> Result<ReceivePlan, ProtocolError> {
        let current = self
            .scoped_payload_placement(session, scope, record.payload_header())
            .await?;
        record
            .validate_terminal_repair(&current)
            .map_err(QueueError::protocol)?;
        let (_, _, section_lengths) = record.terminal_claim().map_err(QueueError::protocol)?;
        Ok(ReceivePlan {
            class: TrafficClass::QueueData,
            section_lengths,
            decode_budget_bytes: 0,
        })
    }

    pub(super) async fn install_scoped_terminal_repair(
        &self,
        session: &AuthenticatedSession,
        scope: [Uuid; 3],
        record: ScopedTransferRecord,
        sections: &ChargedSections,
    ) -> Result<ChargedReply, ProtocolError> {
        #[cfg(test)]
        super::super::recovery_tests::pause("before-scoped-terminal-copy", record.request_id())
            .await;
        let revision = self.with_scoped_store(|store| Ok(store.write_revision(scope[0])))?;
        let view = record.payload_header().ready.view;
        let current = self
            .scoped_payload_placement(session, scope, record.payload_header())
            .await?;
        let (payload, terminal, _) = record.terminal_claim().map_err(QueueError::protocol)?;
        let peer = self.scoped_peer.get().ok_or_else(unavailable)?;
        let witness = peer
            .witness_scoped_terminal(&current, payload, terminal)
            .await?;
        let sections = std::array::from_fn(|index| sections.section(index).cloned());
        let shared = self.shared.clone();
        let placement = current.clone();
        let task_limit = self.task_memory_bytes;
        let prepared = self
            .shared
            .execute(
                ExecutionClass::Query,
                self.next_decode_job()?,
                0,
                move || {
                    PreparedScopedTransfer::prepare(
                        &placement, &placement, record, sections, &shared, task_limit,
                    )
                    .map_err(QueueError::protocol)
                },
            )
            .await?;
        if self.scoped_copy_placement(session, scope, view).await? != current {
            return Err(QueueError::Stale.protocol());
        }
        let ack = self.with_scoped_store(|store| {
            store.require_write_revision(scope[0], revision)?;
            store.install_terminal_repair(&current, witness, prepared)
        })?;
        self.scoped_copy_reply(session, ack)
    }

    pub(super) async fn install_scoped_terminal(
        &self,
        session: &AuthenticatedSession,
        scope: [Uuid; 3],
        header: &RecordHeader,
        version: RecordVersion,
        sections: &ChargedSections,
    ) -> Result<ChargedReply, ProtocolError> {
        #[cfg(test)]
        super::super::recovery_tests::pause("before-scoped-terminal-copy", header.request_id).await;
        let current = self
            .scoped_terminal_placement(session, scope, header)
            .await?;
        if sections.section(0).is_some() || sections.section(1).is_some() {
            return Err(malformed(
                "scoped terminal carries unexpected payload sections",
            ));
        }
        let record =
            self.with_scoped_store(|store| store.record(&current, header.request_id).cloned())?;
        let peer = self.scoped_peer.get().ok_or_else(unavailable)?;
        let terminal_witness = peer
            .witness_scoped_terminal(&current, record.stamp, version)
            .await?;
        let owner = peer
            .scoped_marker_call(&current, current.owner, record.stamp, true)
            .await?;
        if record.marker.is_none() {
            let local =
                self.with_scoped_store(|store| store.copy_witness(&current, record.stamp))?;
            let owner_payload = peer
                .witness_scoped_payload(&current, current.owner, record.stamp)
                .await?;
            let quorum = ScopedCopyQuorum::confirm(&current, record.stamp, local, owner_payload)
                .map_err(QueueError::protocol)?;
            if self
                .scoped_terminal_placement(session, scope, header)
                .await?
                != current
            {
                return Err(QueueError::Stale.protocol());
            }
            self.with_scoped_store(|store| {
                store.install_marker(&current, session.remote(), quorum, record.stamp.binding())
            })?;
        }
        let local =
            self.with_scoped_store(|store| store.copy_witness(&current, record.stamp.marker()))?;
        let accepted = ScopedCopyQuorum::confirm(&current, record.stamp.marker(), local, owner)
            .map_err(QueueError::protocol)?;
        let body = record
            .payload_frame(&self.shared)
            .map_err(QueueError::protocol)?;
        let frame = RecordFrame {
            header: self.shared.encode(header, MAX_RECORD_HEADER_BYTES)?,
            sections: [
                body.sections[0].clone(),
                body.sections[1].clone(),
                sections.section(2).cloned(),
            ],
        };
        let terminal = self
            .prepare_scoped_terminal(
                &current,
                accepted,
                record.reservation,
                TerminalValidation::Replica(terminal_witness),
                frame,
            )
            .await?;
        if self
            .scoped_terminal_placement(session, scope, header)
            .await?
            != current
        {
            return Err(QueueError::Stale.protocol());
        }
        let ack = self.with_scoped_store(|store| {
            store.install_terminal(&current, session.remote(), terminal)
        })?;
        self.scoped_copy_reply(session, ack)
    }

    pub(super) async fn scoped_terminal_witness(
        &self,
        session: &AuthenticatedSession,
        scope: [Uuid; 3],
        view: AuthorityView,
        (request, payload, terminal): (RequestBinding, RecordVersion, RecordVersion),
    ) -> Result<ChargedReply, ProtocolError> {
        let current = self.scoped_copy_placement(session, scope, view).await?;
        if current.owner != session.local() {
            return Err(QueueError::Stale.protocol());
        }
        let (ack, expired) = self.with_scoped_store(|store| {
            let stamp = store
                .claim_payload(&current, request, payload)?
                .terminal(terminal);
            let ack = store.copy_witness(&current, stamp)?;
            let statement = store.terminal_statement(&current, request.request_id)?;
            let expired = store.terminal_retention_elapsed(statement.completed_at_ms, now_ms());
            Ok((ack, expired.then_some(statement)))
        })?;
        if let Some(statement) = expired {
            let readers = self.scoped_payload.get().ok_or_else(unavailable)?;
            // A delayed repair may start after its target erased the record.
            // Once release and expiry are both proved, it cannot reinsert it.
            if readers
                .timestamps
                .confirm_scoped_terminal_release(statement)
                .await?
                .is_some()
            {
                return Err(QueueError::NotFound.protocol());
            }
            if self.scoped_copy_placement(session, scope, view).await? != current {
                return Err(QueueError::Stale.protocol());
            }
        }
        self.scoped_copy_reply(session, ack)
    }

    async fn scoped_terminal_validation(
        &self,
        current: &ScopedQueuePlacement,
        record: &ScopedSurvivor,
        locator: Option<(TxnId, ManifestRef)>,
    ) -> Result<TerminalValidation, ProtocolError> {
        let reservation = record.reservation;
        let request = record.stamp.binding();
        match (reservation.kind, locator) {
            (RequestKind::Read, None) => Ok(TerminalValidation::Read),
            (RequestKind::Mutation, Some((txn, manifest)))
                if txn.request_id == request.request_id
                    && reservation.scoped.is_some_and(|policy| {
                        policy.status_group_id == manifest.status_group_id
                    }) =>
            {
                let status = self.retirement.get().ok_or_else(unavailable)?;
                let (decision, settled) = status
                    .scoped_receipt(current, request, txn, manifest)
                    .await?;
                Ok(TerminalValidation::Mutation(decision, settled))
            }
            _ => Err(QueueError::Conflict.protocol()),
        }
    }

    async fn prepare_scoped_terminal(
        &self,
        current: &ScopedQueuePlacement,
        accepted: ScopedCopyQuorum,
        reservation: IdentityReservation,
        proof: TerminalValidation,
        frame: RecordFrame,
    ) -> Result<PreparedScopedTerminal, ProtocolError> {
        let shared = self.shared.clone();
        let current = current.clone();
        let task_limit = self.task_memory_bytes;
        self.shared
            .execute(
                ExecutionClass::Query,
                self.next_decode_job()?,
                0,
                move || {
                    let result = frame.sections[2].as_ref().ok_or_else(unavailable)?;
                    let working = json_decode_budget(result.as_ref())?
                        .checked_add(result.len())
                        .and_then(|bytes| bytes.checked_add(frame.header.len()))
                        .ok_or_else(decode_capacity)?;
                    if working as u64 > task_limit {
                        return Err(decode_capacity());
                    }
                    let proof = match &proof {
                        TerminalValidation::Mutation(decision, settled) => {
                            ScopedTerminalProof::Mutation { decision, settled }
                        }
                        TerminalValidation::Read => ScopedTerminalProof::Read,
                        TerminalValidation::Replica(owner) => ScopedTerminalProof::Replica(*owner),
                    };
                    PreparedScopedTerminal::prepare(
                        &current,
                        accepted,
                        reservation,
                        proof,
                        frame,
                        &shared,
                    )
                    .map_err(QueueError::protocol)
                },
            )
            .await
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
