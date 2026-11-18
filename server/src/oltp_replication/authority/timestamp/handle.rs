//! Consensus-backed timestamp service. Every mutation below returns only after
//! its native state and applied metadata have committed in the same sync batch.
use super::*;
use crate::{
    oltp::pool::ByteClass,
    oltp_replication::authority::consensus::{Applied, Consensus},
};

#[derive(Clone)]
pub(crate) struct TimestampHandle {
    pub(super) machine: TimestampMachine,
    pub(super) consensus: Arc<dyn Consensus<TimestampCommand, TimestampReply>>,
    release_reader: Arc<std::sync::OnceLock<ReleaseReader>>,
    release_proof: Arc<tokio::sync::Mutex<()>>,
    #[cfg(test)]
    release_pause: Arc<std::sync::OnceLock<ReleasePause>>,
}
#[derive(Clone)]
struct ReleaseReader {
    control: Arc<super::super::control::ControlClient>,
    queue: Arc<crate::oltp_runtime::worker_endpoint::QueuePeerClient>,
}
#[cfg(test)]
struct ReleasePause {
    request_id: uuid::Uuid,
    reached: Arc<tokio::sync::Notify>,
    resume: Arc<tokio::sync::Notify>,
}
impl TimestampHandle {
    pub(crate) async fn begin_import_metadata(
        &self,
        authority: Authority,
        archive_id: uuid::Uuid,
        target_id: uuid::Uuid,
        txn: TxnId,
        reservation: IdentityReservation,
    ) -> Result<StoragePin, ProtocolError> {
        match self
            .submit(TimestampCommand::BeginImportMetadata {
                authority,
                archive_id,
                target_id,
                txn,
                reservation,
                pin_id: uuid::Uuid::new_v4(),
            })
            .await?
            .reply
        {
            TimestampReply::Snapshot(pin) => Ok(pin),
            reply => Err(rejected(reply)),
        }
    }
    pub(crate) async fn release_import_metadata(
        &self,
        authority: Authority,
        identity: PinIdentity,
        pin: StoragePin,
        settled: super::super::status::VerifiedSettlement,
    ) -> Result<(), ProtocolError> {
        match self
            .submit(TimestampCommand::ReleaseImportMetadata {
                authority,
                identity,
                pin,
                settled: settled.into_sealed(),
            })
            .await?
            .reply
        {
            TimestampReply::Released => Ok(()),
            reply => Err(rejected(reply)),
        }
    }
    pub(crate) async fn begin_backup(
        &self,
        authority: Authority,
        owner: crate::oltp::backup::BackupOwnerRef,
    ) -> Result<StoragePin, ProtocolError> {
        match self
            .submit(TimestampCommand::BeginBackup {
                authority,
                owner,
                pin_id: uuid::Uuid::new_v4(),
            })
            .await?
            .reply
        {
            TimestampReply::Snapshot(pin) => Ok(pin),
            reply => Err(rejected(reply)),
        }
    }
    pub(crate) async fn release_backup(
        &self,
        authority: Authority,
        proof: crate::oltp::backup::VerifiedBackupPublished,
    ) -> Result<(), ProtocolError> {
        match self
            .submit(TimestampCommand::ReleaseBackup {
                authority,
                proof: crate::oltp::backup::SealedBackupPublished::from_verified(proof)?,
            })
            .await?
            .reply
        {
            TimestampReply::Released => Ok(()),
            reply => Err(rejected(reply)),
        }
    }
    pub(crate) async fn begin_index_metadata(
        &self,
        authority: Authority,
        owner: MaintenanceOwnerRef,
        txn: TxnId,
        reservation: IdentityReservation,
    ) -> Result<StoragePin, ProtocolError> {
        match self
            .submit(TimestampCommand::BeginIndexMetadata {
                authority,
                owner,
                txn,
                reservation,
                pin_id: uuid::Uuid::new_v4(),
            })
            .await?
            .reply
        {
            TimestampReply::Snapshot(pin) => Ok(pin),
            reply => Err(rejected(reply)),
        }
    }
    pub(crate) async fn release_index_metadata(
        &self,
        authority: Authority,
        identity: PinIdentity,
        pin: StoragePin,
        settled: super::super::status::VerifiedSettlement,
    ) -> Result<(), ProtocolError> {
        match self
            .submit(TimestampCommand::ReleaseIndexMetadata {
                authority,
                identity,
                pin,
                settled: settled.into_sealed(),
            })
            .await?
            .reply
        {
            TimestampReply::Released => Ok(()),
            reply => Err(rejected(reply)),
        }
    }
    pub(crate) async fn begin_maintenance(
        &self,
        authority: Authority,
        owner: MaintenanceOwnerRef,
    ) -> Result<StoragePin, ProtocolError> {
        match self
            .submit(TimestampCommand::BeginMaintenance {
                authority,
                owner,
                pin_id: uuid::Uuid::new_v4(),
            })
            .await?
            .reply
        {
            TimestampReply::Snapshot(pin) => Ok(pin),
            reply => Err(rejected(reply)),
        }
    }
    pub(crate) async fn release_maintenance(
        &self,
        authority: Authority,
        proof: crate::oltp::mvcc::store::index::VerifiedIndexAttemptClosed,
    ) -> Result<(), ProtocolError> {
        match self
            .submit(TimestampCommand::ReleaseMaintenance {
                authority,
                closure: MaintenanceClosure::from_verified(proof)?,
            })
            .await?
            .reply
        {
            TimestampReply::Released => Ok(()),
            reply => Err(rejected(reply)),
        }
    }
    pub(crate) async fn cancel_unaccepted(
        &self,
        rejection: super::super::status::VerifiedAdmissionRejection,
    ) -> Result<(), ProtocolError> {
        match self
            .submit(TimestampCommand::CancelUnaccepted {
                proof: rejection.into_sealed(),
            })
            .await?
            .reply
        {
            TimestampReply::Released => Ok(()),
            reply => Err(rejected(reply)),
        }
    }
    pub(crate) async fn current_authority(&self) -> Result<Option<Authority>, ProtocolError> {
        self.consensus.ensure_linearizable().await?;
        let raw = self
            .machine
            .store()
            .read_metadata(BUDGET, |view| {
                let state: Option<State> = view
                    .get(STATE)?
                    .map(|bytes| serde_json::from_slice(bytes.as_slice()).map_err(|_| unknown()))
                    .transpose()?;
                view.encode(&state.map(|state| state.authority))
            })
            .await?;
        serde_json::from_slice(raw.as_slice()).map_err(|_| unknown())
    }
    pub(crate) fn new(
        machine: TimestampMachine,
        consensus: Arc<dyn Consensus<TimestampCommand, TimestampReply>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            machine,
            consensus,
            release_reader: Arc::new(std::sync::OnceLock::new()),
            release_proof: Arc::new(tokio::sync::Mutex::new(())),
            #[cfg(test)]
            release_pause: Arc::new(std::sync::OnceLock::new()),
        })
    }
    pub(crate) fn identity(&self) -> &GroupIdentity {
        self.machine.store().identity()
    }
    pub(crate) fn resources(&self) -> &Arc<crate::oltp::pool::WorkerSharedResources> {
        self.machine.store().resources()
    }
    pub(crate) fn install_release_reader(
        &self,
        control: Arc<super::super::control::ControlClient>,
        queue: Arc<crate::oltp_runtime::worker_endpoint::QueuePeerClient>,
    ) -> Result<(), ProtocolError> {
        self.release_reader
            .set(ReleaseReader { control, queue })
            .map_err(|_| unknown())
    }
    pub(crate) async fn release(
        &self,
        statement: TerminalCopyStatement,
    ) -> Result<VerifiedPinRelease, ProtocolError> {
        use super::super::{
            control::InventoryPage,
            proof::{AuthenticatedAck, TerminalVerifier},
        };
        use crate::oltp_runtime::{replica::QueueScope, worker_endpoint::AuthorityReader};
        // Only one release can walk Control and the Ready queue copies. Check
        // the durable record even when another request owns that work.
        let proof_slot = self.release_proof.try_lock();
        let confirmed = self.release_record(statement.clone()).await?;
        let _proof_slot = if confirmed.is_some() {
            drop(proof_slot);
            None
        } else {
            Some(proof_slot.map_err(|_| {
                ProtocolError::new(
                    ErrorCode::Overloaded,
                    "timestamp terminal copy proof already in progress",
                    Outcome::NotApplied,
                )
            })?)
        };
        let reader = self.release_reader.get().ok_or_else(unknown)?;
        let scope = QueueScope {
            cluster_id: self.identity().cluster_id,
            database: self.identity().database.clone(),
        };
        let current = reader.control.current(&scope).await?;
        if let Some(confirmed) = confirmed {
            // Control may invalidate the old Ready set before the TSO fence
            // advances. The current Control view must still admit this proof.
            if current.ready != statement.ready {
                return Err(unknown());
            }
            return Ok(confirmed);
        }
        let mut verifier =
            TerminalVerifier::begin(statement.clone(), current.ready).map_err(|_| unknown())?;
        #[cfg(test)]
        if let Some(pause) = self
            .release_pause
            .get()
            .filter(|pause| pause.request_id == statement.request.request_id)
        {
            pause.reached.notify_one();
            pause.resume.notified().await;
        }
        let shared = self.machine.store().resources().clone();
        let mut after = None;
        loop {
            let reply = reader.control.ready_page(statement.ready, after).await?;
            let bytes = reply.sections[0].as_ref().ok_or_else(unknown)?.clone();
            let required = shared
                .execute(
                    crate::oltp::pool::ExecutionClass::Control,
                    0,
                    0,
                    move || crate::oltp::mvcc::json_decode_budget(bytes.as_slice()),
                )
                .await?;
            let reply = reply.reserve_reply_decode(&shared, required).await?;
            let page = shared
                .execute(
                    crate::oltp::pool::ExecutionClass::Control,
                    0,
                    0,
                    move || reply.decode_section::<InventoryPage<WorkerIdentity>>(0),
                )
                .await?;
            if page.revision != statement.ready.view.membership_revision
                || (!page.done && page.items.is_empty())
            {
                return Err(unknown());
            }
            for worker in page.items.iter().copied() {
                if after.is_some_and(|previous| worker.worker_id <= previous) {
                    return Err(unknown());
                }
                let observation = reader
                    .queue
                    .terminal_state(worker, scope.clone(), &statement, self.identity().group_id)
                    .await?;
                let (member, observed, nonce) = observation.into_parts();
                if observed != statement {
                    return Err(unknown());
                }
                let ack = AuthenticatedAck::from_transport(
                    member,
                    super::super::proof::statement_digest(&observed).map_err(|_| unknown())?,
                    nonce,
                );
                verifier.accept(worker, &ack).map_err(|_| unknown())?;
                after = Some(worker.worker_id);
            }
            if page.after != after {
                return Err(unknown());
            }
            let done = page.done;
            drop(page);
            if done {
                break;
            }
        }
        let current = reader.control.current(&scope).await?;
        let proof = verifier
            .finish(current.ready)
            .map_err(|_| unknown())?
            .into_sealed();
        let confirmed = self.submit(TimestampCommand::Release { proof }).await?;
        match confirmed.reply {
            TimestampReply::Released => self.confirmed_release(statement).await,
            reply => Err(rejected(reply)),
        }
    }
    #[cfg(test)]
    pub(crate) fn hold_release_proof_for_test(
        &self,
        request_id: uuid::Uuid,
    ) -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
        let reached = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        assert!(
            self.release_pause
                .set(ReleasePause {
                    request_id,
                    reached: reached.clone(),
                    resume: resume.clone(),
                })
                .is_ok()
        );
        (reached, resume)
    }
    /// Read the original durable allocation; never allocate another stamp here.
    pub(crate) async fn confirmed_allocation(
        &self,
        permit: SealedCommitPermit,
    ) -> Result<CommitStamp, ProtocolError> {
        permit.validate()?;
        self.consensus.ensure_linearizable().await?;
        let raw = self
            .machine
            .store()
            .read_metadata(BUDGET, move |view| {
                let key = commit_key(
                    permit.statement().manifest.status_group_id,
                    permit.statement().txn,
                );
                let allocation: CommitAllocation = view
                    .get(&key)?
                    .map(|raw| serde_json::from_slice(raw.as_slice()).map_err(|_| unknown()))
                    .transpose()?
                    .ok_or_else(unknown)?;
                if allocation.permit.allocation_digest()? != permit.allocation_digest()? {
                    return Err(invalid());
                }
                view.encode(&allocation.stamp)
            })
            .await?;
        serde_json::from_slice(raw.as_slice()).map_err(|_| unknown())
    }
    /// Confirm the exact release stored by the timestamp state machine.
    pub(crate) async fn confirmed_release(
        &self,
        expected: TerminalCopyStatement,
    ) -> Result<VerifiedPinRelease, ProtocolError> {
        self.release_record(expected).await?.ok_or_else(unknown)
    }
    async fn release_record(
        &self,
        check: TerminalCopyStatement,
    ) -> Result<Option<VerifiedPinRelease>, ProtocolError> {
        check.validate().map_err(|_| invalid())?;
        self.consensus.ensure_linearizable().await?;
        let raw = self
            .machine
            .store()
            .read_metadata(BUDGET, move |view| {
                let state: State = view
                    .get(STATE)?
                    .map(|raw| serde_json::from_slice(raw.as_slice()).map_err(|_| unknown()))
                    .transpose()?
                    .ok_or_else(unknown)?;
                if state.authority != state.ready.view.authority() {
                    return Err(unknown());
                }
                if state.ready != check.ready {
                    return Err(invalid());
                }
                let record: PinRecord = view
                    .get(&pin_key(check.pin_identity.nonce))?
                    .map(|raw| serde_json::from_slice(raw.as_slice()).map_err(|_| unknown()))
                    .transpose()?
                    .ok_or_else(unknown)?;
                if record.identity != check.pin_identity
                    || record.pin != check.storage_pin
                    || record.request != Some(check.request)
                    || record.purpose != PinPurpose::Application
                {
                    return Err(invalid());
                }
                let (proof, applied_at, response_hash, terminal_version) = match record.state {
                    PinState::Active => {
                        return view.encode(&None::<(SealedTerminalProof, AppliedPosition)>);
                    }
                    PinState::Released {
                        proof,
                        applied_at,
                        response_hash,
                        terminal_version,
                    } => (proof, applied_at, response_hash, terminal_version),
                    PinState::Cancelled | PinState::ColdReleased | PinState::MetadataReleased { .. } => {
                        return Err(unknown());
                    }
                };
                let stored = &proof.statement;
                if stored.validate().is_err()
                    || stored.pin_identity != record.identity
                    || stored.storage_pin != record.pin
                    || Some(stored.request) != record.request
                    || stored.response_hash != response_hash
                    || stored.terminal_version != terminal_version
                    || proof.statement_digest != statement_digest(stored).map_err(|_| unknown())?
                    || proof.acknowledgement_count != stored.ready.ready_count
                    || applied_at.term == 0
                    || applied_at.index == 0
                {
                    return Err(unknown());
                }
                if *stored == check {
                    return view.encode(&Some((proof, applied_at)));
                }
                // A membership change or a legacy record missing its terminal
                // digest can be refreshed only with new all-Ready evidence.
                if response_hash != check.response_hash
                    || terminal_version > check.terminal_version
                    || stored
                        .terminal_digest
                        .is_some_and(|digest| check.terminal_digest != Some(digest))
                    || ((terminal_version == check.terminal_version) != (stored.ready == check.ready))
                {
                    return Err(invalid());
                }
                view.encode(&None::<(SealedTerminalProof, AppliedPosition)>)
            })
            .await?;
        let release: Option<(SealedTerminalProof, AppliedPosition)> =
            serde_json::from_slice(raw.as_slice()).map_err(|_| unknown())?;
        Ok(release.map(|(proof, confirmed_at)| VerifiedPinRelease {
            proof,
            confirmed_at,
        }))
    }
    pub(super) async fn submit(
        &self,
        command: TimestampCommand,
    ) -> Result<Applied<TimestampReply>, ProtocolError> {
        let command = OwnedCommand::new_owned(
            command,
            self.machine.store().resources(),
            ByteClass::Progress,
        )?;
        self.consensus.propose(command).await
    }
    pub(crate) async fn begin_snapshot(
        &self,
        view: AuthorityView,
        identity: PinIdentity,
        reservation: IdentityReservation,
    ) -> Result<StoragePin, ProtocolError> {
        match self
            .submit(TimestampCommand::BeginSnapshot {
                view,
                identity,
                reservation,
                pin_id: uuid::Uuid::new_v4(),
            })
            .await?
            .reply
        {
            TimestampReply::Snapshot(pin) => Ok(pin),
            reply => Err(rejected(reply)),
        }
    }
    pub(crate) async fn bind_snapshot_context(
        &self,
        view: AuthorityView,
        identity: PinIdentity,
        pin: StoragePin,
        request: RequestBinding,
    ) -> Result<(), ProtocolError> {
        match self
            .submit(TimestampCommand::BindSnapshotContext {
                view,
                identity,
                pin,
                request,
            })
            .await?
            .reply
        {
            TimestampReply::ContextBound(bound) if bound == pin => Ok(()),
            reply => Err(rejected(reply)),
        }
    }
    pub(crate) async fn allocate_commit(
        &self,
        permit: SealedCommitPermit,
    ) -> Result<CommitStamp, ProtocolError> {
        permit.validate()?;
        match self
            .submit(TimestampCommand::AllocateCommit { permit })
            .await?
            .reply
        {
            TimestampReply::Commit(stamp) => Ok(stamp),
            reply => Err(rejected(reply)),
        }
    }
    pub(crate) async fn fence_ready(
        &self,
        transition_id: uuid::Uuid,
        expected: ReadyMembership,
        next: ReadyMembership,
    ) -> Result<ReadyFenceReceipt, ProtocolError> {
        match self
            .submit(TimestampCommand::FenceReady {
                transition_id,
                expected,
                next,
            })
            .await?
            .reply
        {
            TimestampReply::ReadyFenced(receipt)
                if receipt.transition_id == transition_id
                    && receipt.expected == expected
                    && receipt.next == next =>
            {
                Ok(receipt)
            }
            reply => Err(rejected(reply)),
        }
    }
    pub(crate) async fn fence(
        &self,
        transition_id: uuid::Uuid,
        expected: Option<Authority>,
        next: Authority,
        ready: ReadyMembership,
    ) -> Result<Applied<TimestampReply>, ProtocolError> {
        self.submit(TimestampCommand::Fence {
            transition_id,
            expected,
            next,
            ready,
        })
        .await
    }
}
pub(super) fn rejected(reply: TimestampReply) -> ProtocolError {
    ProtocolError::new(
        ErrorCode::Conflict,
        format!("timestamp transition rejected: {reply:?}"),
        Outcome::NotApplied,
    )
}
