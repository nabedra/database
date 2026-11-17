//! Strong participant capabilities. These wrappers cannot be deserialized by
//! callers; only the leader's quorum barrier plus durable snapshot creates one.
use super::*;
use crate::{
    oltp::pool::{ExecutionClass, ScratchLease},
    oltp_replication::authority::consensus::{Applied, Consensus},
};
const META: TransactionBudget = TransactionBudget {
    scratch_bytes: 128 << 10,
    batch_bytes: 0,
    output_bytes: 16 << 10,
};
#[derive(Clone)]
pub(crate) struct ParticipantHandle {
    pub(super) machine: ParticipantMachine,
    pub(super) consensus: Arc<dyn Consensus<ParticipantCommand, ParticipantReply>>,
}
#[derive(Clone)]
pub(crate) struct VerifiedParticipantSeal {
    evidence: PreparedParticipant,
    _owner: Arc<ScratchLease>,
}
impl std::fmt::Debug for VerifiedParticipantSeal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.evidence.fmt(f)
    }
}
impl std::fmt::Debug for VerifiedParticipantResolution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.evidence.fmt(f)
    }
}
impl VerifiedParticipantSeal {
    pub(crate) fn evidence(&self) -> &PreparedParticipant {
        &self.evidence
    }
    pub(crate) fn into_evidence(self) -> PreparedParticipant {
        self.evidence
    }
}
#[derive(Clone)]
pub(crate) struct VerifiedParticipantResolution {
    evidence: ResolutionEvidence,
    _owner: Arc<ScratchLease>,
}
impl VerifiedParticipantResolution {
    pub(crate) fn evidence(&self) -> &ResolutionEvidence {
        &self.evidence
    }
    pub(crate) fn into_evidence(self) -> ResolutionEvidence {
        self.evidence
    }
}
impl ParticipantHandle {
    pub(crate) fn new(
        machine: ParticipantMachine,
        consensus: Arc<dyn Consensus<ParticipantCommand, ParticipantReply>>,
    ) -> Self {
        Self { machine, consensus }
    }
    pub(crate) fn identity(&self) -> &GroupIdentity {
        self.machine.store.identity()
    }
    pub(crate) fn resources(&self) -> &Arc<crate::oltp::pool::WorkerSharedResources> {
        self.machine.store.resources()
    }
    pub(crate) async fn propose(
        &self,
        command: OwnedCommand<ParticipantCommand>,
    ) -> Result<Applied<ParticipantReply>, ProtocolError> {
        self.consensus.propose(command).await
    }
    pub(crate) async fn prepare_page(
        &self,
        command: OwnedCommand<ParticipantCommand>,
    ) -> Result<Applied<ParticipantReply>, ProtocolError> {
        if !matches!(command.payload()?, ParticipantCommand::PreparePage { .. }) {
            return Err(invalid());
        }
        self.propose(command).await
    }
    pub(crate) async fn seal(
        &self,
        authority: Authority,
        txn: TxnId,
        manifest: ManifestRef,
        result_digest: Digest,
    ) -> Result<Applied<ParticipantReply>, ProtocolError> {
        self.propose(OwnedCommand::new_owned(
            ParticipantCommand::Seal {
                authority,
                txn,
                manifest,
                result_digest,
            },
            self.resources(),
            crate::oltp::pool::ByteClass::Data,
        )?)
        .await
    }
    pub(crate) async fn resolve(
        &self,
        authority: Authority,
        proof: SealedStatusDecision,
        max_items: u16,
    ) -> Result<Applied<ParticipantReply>, ProtocolError> {
        self.propose(OwnedCommand::new_owned(
            ParticipantCommand::Resolve {
                authority,
                proof,
                max_items,
            },
            self.resources(),
            crate::oltp::pool::ByteClass::Data,
        )?)
        .await
    }
    pub(crate) async fn read_at(
        &self,
        read_ts: ReadTs,
        key: MvccKey,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.consensus.ensure_linearizable().await?;
        self.machine.read_at(read_ts, key).await
    }
    pub(crate) async fn strong_seal(
        &self,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> Result<VerifiedParticipantSeal, ProtocolError> {
        self.consensus.ensure_linearizable().await?;
        let raw = self
            .machine
            .store
            .read_metadata(META, move |view| {
                let state = view.pinned(&txn_key(b't', txn))?.ok_or_else(invalid)?;
                if state.len() > METADATA_BYTES {
                    return Err(recovery_unknown());
                }
                let state: Txn = serde_json::from_slice(&state).map_err(|_| recovery_unknown())?;
                if state.manifest != manifest || state.decision.is_some() {
                    return Err(invalid());
                }
                let evidence = state.sealed.ok_or_else(invalid)?;
                view.encode(&evidence)
            })
            .await?;
        let owner = Arc::new(
            self.resources()
                .reserve_reply_decode(super::super::json_decode_budget(raw.as_slice())?)
                .await?,
        );
        let resources = self.resources().clone();
        let decode_owner = owner.clone();
        let evidence = resources
            .execute(ExecutionClass::Control, 0, 0, move || {
                let _owner = decode_owner;
                serde_json::from_slice(raw.as_slice()).map_err(|_| recovery_unknown())
            })
            .await?;
        Ok(VerifiedParticipantSeal {
            evidence,
            _owner: owner,
        })
    }
    pub(crate) async fn strong_resolution(
        &self,
        txn: TxnId,
        manifest: ManifestRef,
    ) -> Result<VerifiedParticipantResolution, ProtocolError> {
        self.consensus.ensure_linearizable().await?;
        let raw = self
            .machine
            .store
            .read_metadata(META, move |view| {
                let state = view.pinned(&txn_key(b'd', txn))?.ok_or_else(invalid)?;
                if state.len() > METADATA_BYTES {
                    return Err(recovery_unknown());
                }
                let evidence: ResolutionEvidence =
                    serde_json::from_slice(&state).map_err(|_| recovery_unknown())?;
                if evidence.txn != txn || evidence.manifest != manifest {
                    return Err(invalid());
                }
                view.encode(&evidence)
            })
            .await?;
        let owner = Arc::new(
            self.resources()
                .reserve_reply_decode(super::super::json_decode_budget(raw.as_slice())?)
                .await?,
        );
        let resources = self.resources().clone();
        let decode_owner = owner.clone();
        let evidence = resources
            .execute(ExecutionClass::Control, 0, 0, move || {
                let _owner = decode_owner;
                serde_json::from_slice(raw.as_slice()).map_err(|_| recovery_unknown())
            })
            .await?;
        Ok(VerifiedParticipantResolution {
            evidence,
            _owner: owner,
        })
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub(crate) struct ParticipantEpochs {
    pub authority: Authority,
    pub catalog_group_id: Uuid,
    pub schema_epoch: u64,
    pub routing_epoch: u64,
}
impl ParticipantHandle {
    pub(crate) async fn activation(&self) -> Result<Option<ParticipantEpochs>, ProtocolError> {
        self.consensus.ensure_linearizable().await?;
        let raw = self
            .machine
            .store
            .read_metadata(META, |view| {
                let raw = view.pinned(CONFIG)?;
                let epochs = raw
                    .map(|raw| {
                        if raw.len() > METADATA_BYTES {
                            return Err(recovery_unknown());
                        }
                        serde_json::from_slice::<ParticipantEpochs>(&raw)
                            .map_err(|_| recovery_unknown())
                    })
                    .transpose()?;
                view.encode(&epochs)
            })
            .await?;
        serde_json::from_slice(raw.as_slice()).map_err(|_| recovery_unknown())
    }
    pub(crate) async fn epochs(&self) -> Result<ParticipantEpochs, ProtocolError> {
        self.consensus.ensure_linearizable().await?;
        let raw = self
            .machine
            .store
            .read_metadata(META, |view| {
                let raw = view.pinned(CONFIG)?.ok_or_else(invalid)?;
                if raw.len() > METADATA_BYTES {
                    return Err(recovery_unknown());
                }
                // Unknown range fields are skipped, not allocated.
                let epochs: ParticipantEpochs =
                    serde_json::from_slice(&raw).map_err(|_| recovery_unknown())?;
                view.encode(&epochs)
            })
            .await?;
        serde_json::from_slice(raw.as_slice()).map_err(|_| recovery_unknown())
    }
}

impl ParticipantHandle {
    pub(crate) async fn fence_initial(
        &self,
        transition_id: Uuid,
        next: Authority,
        catalog_group_id: Uuid,
        schema_epoch: u64,
        routing_epoch: u64,
        span: KeySpan,
    ) -> Result<crate::oltp_replication::types::GroupFenceReceipt, ProtocolError> {
        self.fence_command(ParticipantCommand::Fence {
            transition_id,
            expected: None,
            next,
            catalog_group_id,
            schema_epoch,
            routing_epoch,
            span,
        })
        .await
    }
    pub(crate) async fn fence_current(
        &self,
        transition_id: Uuid,
        expected: Authority,
        next: Authority,
    ) -> Result<crate::oltp_replication::types::GroupFenceReceipt, ProtocolError> {
        self.consensus.ensure_linearizable().await?;
        let raw = self
            .machine
            .store
            .read_metadata(META, |view| {
                view.pinned(CONFIG)?
                    .map(|raw| view.copy_output(&raw))
                    .transpose()?
                    .ok_or_else(invalid)
            })
            .await?;
        let config: Config =
            serde_json::from_slice(raw.as_slice()).map_err(|_| recovery_unknown())?;
        // A lost successful response may be retried after the new fence. The
        // transition's durable receipt, not a fresh authority guess, decides it.
        if config.authority != expected && config.authority != next {
            return Err(invalid());
        }
        self.fence_command(ParticipantCommand::Fence {
            transition_id,
            expected: Some(expected),
            next,
            catalog_group_id: config.catalog_group_id,
            schema_epoch: config.schema_epoch,
            routing_epoch: config.routing_epoch,
            span: config.span,
        })
        .await
    }
    async fn fence_command(
        &self,
        command: ParticipantCommand,
    ) -> Result<crate::oltp_replication::types::GroupFenceReceipt, ProtocolError> {
        let applied = self
            .propose(OwnedCommand::new_owned(
                command,
                self.resources(),
                crate::oltp::pool::ByteClass::Progress,
            )?)
            .await?;
        match applied.reply {
            ParticipantReply::Fenced(receipt) => Ok(receipt),
            _ => Err(invalid()),
        }
    }
}
