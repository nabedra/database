//! Immutable Status home for an authoritative Timestamp point decision.
use super::*;
use crate::oltp_replication::authority::{
    control::ScopedCatalogRead, status::outcome::MutationValue, timestamp::ScopedTimestampEpoch,
};

pub(crate) mod projection;

const HOMES: &[u8] = b"status/timestamp-point-home/";
pub(crate) const TIMESTAMP_POINT_LOCATOR_REPLY_LIMIT: usize = 4096;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TimestampPointBinding {
    pub status_group: GroupIdentity,
    pub timestamps_group: GroupIdentity,
    pub participant: ParticipantPlan,
    pub created_authority: Authority,
    pub txn: TxnId,
    pub request: RequestBinding,
    pub pin_identity: PinIdentity,
    pub storage_pin: StoragePin,
    pub result_digest: Digest,
    pub reservation: IdentityReservation,
    pub original_epoch: ScopedTimestampEpoch,
    pub original_scope: ScopedCatalogRead,
    pub catalog_root: Digest,
}

impl TimestampPointBinding {
    pub(crate) fn roots(&self) -> Result<(Digest, ManifestRef), ProtocolError> {
        let status = &self.status_group;
        let timestamps = &self.timestamps_group;
        let data = &self.participant.group;
        let scope = self.original_scope;
        if status.validate().is_err()
            || timestamps.validate().is_err()
            || self.participant.validate().is_err()
            || !matches!(status.kind, GroupKind::Status { .. })
            || timestamps.kind != GroupKind::Timestamps
            || !matches!(data.kind, GroupKind::Data { table_id, .. } if table_id == scope.table_id)
            || timestamps.cluster_id != status.cluster_id
            || data.cluster_id != status.cluster_id
            || timestamps.database != status.database
            || data.database != status.database
            || status.group_id == timestamps.group_id
            || status.group_id == data.group_id
            || timestamps.group_id == data.group_id
            || self.participant.write_count != 1
            || self.participant.page_count != 1
            || self.created_authority.validate().is_err()
            || self.created_authority.run_id != self.request.run_id
            || self.txn.request_id.is_nil()
            || self.txn.request_id != self.request.request_id
            || self.txn.attempt != 1
            || self.pin_identity.run_id != self.request.run_id
            || self.pin_identity.request_id != self.request.request_id
            || self.pin_identity.digest != self.request.digest
            || self.pin_identity.nonce.is_nil()
            || self.storage_pin.pin_id.is_nil()
            || self.storage_pin.allocator_epoch == 0
            || self.storage_pin.read_ts != self.participant.read_ts
            || scope.request != self.request
            || scope.queue_group_id != data.group_id
            || scope.queue_generation.is_nil()
            || scope.data_generation.is_nil()
            || scope.queue_view.validate().is_err()
            || scope.queue_view.run_id != self.request.run_id
            || scope.read_ts != self.storage_pin.read_ts
            || !self.matches_reservation(self.reservation)
            || self.original_epoch.queue_group_id != scope.queue_group_id
            || self.original_epoch.queue_generation != scope.queue_generation
            || self.original_epoch.view != scope.queue_view
            || !self.original_epoch.admits(self.reservation.scoped)
        {
            return Err(invalid());
        }
        let initial = super::super::super::metadata_digest(
            b"nabedra/status-scoped-timestamp-point-manifest/v1\0",
            &(
                status,
                timestamps,
                self.created_authority,
                self.txn,
                self.request,
                &self.pin_identity,
                self.storage_pin,
                self.result_digest,
                self.reservation,
                self.original_epoch,
                scope,
                self.catalog_root,
            ),
        )?;
        Ok((
            initial,
            ManifestRef {
                status_group_id: status.group_id,
                digest: root_step(initial, &self.participant)?,
                participant_count: 1,
            },
        ))
    }

    pub(crate) fn matches_scope(&self, mut scope: ScopedCatalogRead) -> bool {
        if scope.queue_view.validate().is_err()
            || scope.data_generation.is_nil()
            || scope.queue_view.run_id != self.original_scope.queue_view.run_id
            || scope.queue_view.owner_term < self.original_scope.queue_view.owner_term
            || scope.queue_view.membership_revision
                < self.original_scope.queue_view.membership_revision
            || (scope.queue_view.owner_term == self.original_scope.queue_view.owner_term
                && (scope.queue_view != self.original_scope.queue_view
                    || scope.data_generation != self.original_scope.data_generation))
        {
            return false;
        }
        // Callers authenticate the replacement placement at its current fence.
        scope.queue_view = self.original_scope.queue_view;
        scope.data_generation = self.original_scope.data_generation;
        scope == self.original_scope
    }

    pub(crate) fn accepts_authority(&self, authority: Authority) -> bool {
        authority.run_id == self.created_authority.run_id
            && authority.owner_term >= self.created_authority.owner_term
    }

    pub(crate) fn heap_bytes(&self) -> usize {
        self.status_group.database.capacity_bytes()
            + self.timestamps_group.database.capacity_bytes()
            + self.participant.group.database.capacity_bytes()
    }

    fn matches_reservation(&self, reservation: IdentityReservation) -> bool {
        reservation == self.reservation
            && reservation.kind == RequestKind::Mutation
            && reservation.binds(self.request)
            && reservation.reservation_id == self.pin_identity.nonce
            && ScopedStatusEpoch::from_scope(self.original_scope)
                .admits(reservation.scoped, self.status_group.group_id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TimestampDecisionLocator {
    pub binding: TimestampPointBinding,
    pub manifest: ManifestRef,
}

impl TimestampDecisionLocator {
    pub(crate) fn validate(&self) -> Result<(), ProtocolError> {
        if self.binding.roots()?.1 != self.manifest {
            return Err(invalid());
        }
        Ok(())
    }

    pub(crate) fn digest(&self) -> Result<Digest, ProtocolError> {
        self.validate()?;
        super::super::super::metadata_digest(b"nabedra/timestamp-point-home/v1\0", self)
    }

    pub(crate) fn matches_seal(&self, seal: &PreparedParticipant) -> bool {
        self.binding.txn == seal.txn
            && self.manifest == seal.manifest
            && self.binding.participant == seal.participant
            && self.binding.accepts_authority(seal.authority)
            && self.binding.result_digest == seal.result_digest
            && projection::valid_position(seal.sealed_at)
    }

    pub(crate) fn heap_bytes(&self) -> usize {
        self.binding.heap_bytes()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TimestampPointManifest {
    pub binding: TimestampPointBinding,
    pub outcome: MutationOutcome,
}

impl TimestampPointManifest {
    pub(crate) fn manifest(&self) -> Result<ManifestRef, ProtocolError> {
        let (_, manifest) = self.binding.roots()?;
        if !valid_outcome(&self.outcome, self.binding.result_digest)? {
            return Err(invalid());
        }
        Ok(manifest)
    }

    pub(crate) fn heap_bytes(&self) -> usize {
        self.binding.heap_bytes() + self.outcome.heap_bytes()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LocatedTimestampManifest {
    pub locator: TimestampDecisionLocator,
    pub outcome: MutationOutcome,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum TimestampPointHome {
    Absent,
    Legacy,
    Installed(LocatedTimestampManifest),
}

pub(crate) fn valid_outcome(
    outcome: &MutationOutcome,
    expected: Digest,
) -> Result<bool, ProtocolError> {
    Ok(matches!(
        outcome,
        MutationOutcome::Success(MutationValue::Changed(true))
    ) && outcome.digest()? == expected)
}

pub(crate) fn mode_conflict() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::Conflict,
        "request already has a different transaction home",
        Outcome::Unknown,
    )
}

pub(in super::super) fn home_key(request_id: Uuid) -> Vec<u8> {
    key(HOMES, request_id)
}

pub(in super::super) fn install(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    current: Authority,
    epoch: ScopedStatusEpoch,
    candidate: &TimestampPointManifest,
) -> Result<Transition, ProtocolError> {
    let binding = &candidate.binding;
    let Ok(manifest) = candidate.manifest() else {
        return reject(tx, StatusFault::InvalidBinding);
    };
    if &binding.status_group != scope {
        return reject(tx, StatusFault::InvalidBinding);
    }
    let request_id = binding.txn.request_id;
    let Some(identity) = load::<IdentityRecord>(tx, &key(IDENTITIES, request_id))? else {
        return reject(tx, StatusFault::IdentityNotReserved);
    };
    if !binding.matches_reservation(identity.reservation) || identity.restore_origin.is_some() {
        return reject(tx, StatusFault::InvalidBinding);
    }
    let located = LocatedTimestampManifest {
        locator: TimestampDecisionLocator {
            binding: binding.clone(),
            manifest,
        },
        outcome: candidate.outcome.clone(),
    };
    let home_key = home_key(request_id);
    if let Some(stored) = load::<LocatedTimestampManifest>(tx, &home_key)? {
        if stored != located {
            return reject(tx, StatusFault::TransitionConflict);
        }
        projection::verify_in_apply(tx, scope, &identity, &stored, current, None)?;
        return commit(tx, StatusReply::ManifestSealed(manifest));
    }
    if binding.created_authority != current
        || ScopedStatusEpoch::from_scope(binding.original_scope) != epoch
    {
        return reject(tx, StatusFault::StaleAuthority);
    }
    if identity.rejection.is_some() || identity.terminal.is_some() || identity.retirement.is_some()
    {
        return reject(tx, StatusFault::TerminalConflict);
    }
    if has_transaction(tx, request_id)? {
        return reject(tx, StatusFault::TransitionConflict);
    }

    let (initial, _) = binding.roots()?;
    let state = Transaction {
        txn: binding.txn,
        created_authority: current,
        request: binding.request,
        read_ts: binding.storage_pin.read_ts,
        expected: 1,
        initial,
        root: manifest.digest,
        pages: 1,
        count: 1,
        last_group: Some(binding.participant.group.group_id),
        manifest: Some(manifest),
        prepared: 0,
        last_seal: None,
        resolved: 0,
        settled_at: None,
        decision: None,
        result_digest: binding.result_digest,
        outcome: candidate.outcome.clone(),
        decode_owner: None,
    };
    put(tx, &txn_key(b't', binding.txn, &[]), &state)?;
    put(
        tx,
        &txn_key(
            b'g',
            binding.txn,
            binding.participant.group.group_id.as_bytes(),
        ),
        &binding.participant,
    )?;
    put(
        tx,
        &txn_key(b'o', binding.txn, &0_u64.to_be_bytes()),
        &binding.participant,
    )?;
    let encoded = tx.encode_value(&located)?;
    if encoded.as_slice().len() > TIMESTAMP_POINT_LOCATOR_REPLY_LIMIT {
        return reject(tx, StatusFault::InvalidBinding);
    }
    tx.put(&home_key, encoded.as_slice())?;
    commit(tx, StatusReply::ManifestSealed(manifest))
}

fn check_participant_indexes(
    locator: &TimestampDecisionLocator,
    mut read_plan: impl FnMut(&[u8]) -> Result<Option<ParticipantPlan>, ProtocolError>,
) -> Result<(), ProtocolError> {
    let txn = locator.binding.txn;
    let participant = &locator.binding.participant;
    let by_group = read_plan(&txn_key(b'g', txn, participant.group.group_id.as_bytes()))?;
    let by_ordinal = read_plan(&txn_key(b'o', txn, &0_u64.to_be_bytes()))?;
    if by_group.as_ref() != Some(participant) || by_ordinal.as_ref() != Some(participant) {
        return Err(unknown());
    }
    Ok(())
}

fn validate_stored_home(
    scope: &GroupIdentity,
    identity: &IdentityRecord,
    home: &LocatedTimestampManifest,
    state: &Transaction,
) -> Result<(), ProtocolError> {
    let binding = &home.locator.binding;
    let (initial, manifest) = binding.roots().map_err(|_| unknown())?;
    if home.locator.manifest != manifest
        || !binding.matches_reservation(identity.reservation)
        || identity.rejection.is_some()
        || identity.restore_origin.is_some()
        || identity.retirement.is_some()
        || &binding.status_group != scope
        || state.initial != initial
        || state.root != manifest.digest
        || state.expected != 1
        || state.pages != 1
        || state.count != 1
        || state.last_group != Some(binding.participant.group.group_id)
        || state.txn != binding.txn
        || state.request != binding.request
        || state.read_ts != binding.storage_pin.read_ts
        || state.created_authority != binding.created_authority
        || state.result_digest != binding.result_digest
        || state.manifest != Some(manifest)
        || state.prepared != 0
        || state.last_seal.is_some()
        || !valid_outcome(&home.outcome, binding.result_digest).map_err(|_| unknown())?
    {
        return Err(unknown());
    }
    Ok(())
}

impl StatusHandle {
    pub(crate) async fn install_timestamp_point(
        &self,
        authority: Authority,
        point: TimestampPointManifest,
    ) -> Result<ManifestRef, ProtocolError> {
        if &point.binding.status_group != self.identity()
            || authority.validate().is_err()
            || self.scoped.is_none()
        {
            return Err(invalid());
        }
        let manifest = point.manifest()?;
        let expected = LocatedTimestampManifest {
            locator: TimestampDecisionLocator {
                binding: point.binding.clone(),
                manifest,
            },
            outcome: point.outcome.clone(),
        };
        let request = point.binding.request;
        let read_home = || self.timestamp_point_home(authority, request.request_id, request.digest);
        match read_home().await? {
            TimestampPointHome::Installed(actual) if actual == expected => return Ok(manifest),
            TimestampPointHome::Absent => {}
            TimestampPointHome::Installed(_) | TimestampPointHome::Legacy => {
                return Err(mode_conflict());
            }
        }
        let applied = self
            .submit(StatusCommand::InstallTimestampPoint {
                authority,
                point: Box::new(point),
            })
            .await;
        if matches!(applied, Ok(value) if value.reply == StatusReply::ManifestSealed(manifest)) {
            return Ok(manifest);
        }
        match read_home().await? {
            TimestampPointHome::Installed(actual) if actual == expected => Ok(manifest),
            TimestampPointHome::Installed(_) | TimestampPointHome::Legacy => Err(mode_conflict()),
            TimestampPointHome::Absent => Err(unknown()),
        }
    }

    pub(crate) async fn timestamp_point_home(
        &self,
        authority: Authority,
        request_id: Uuid,
        digest: Digest,
    ) -> Result<TimestampPointHome, ProtocolError> {
        let raw = self
            .timestamp_point_home_bytes(authority, request_id, digest, None)
            .await?;
        let (mut home, owner) = self.decode_timestamp_point(raw).await?;
        if let TimestampPointHome::Installed(home) = &mut home {
            home.outcome.retain_owner(owner);
        }
        Ok(home)
    }

    pub(crate) async fn timestamp_point_home_bytes(
        &self,
        authority: Authority,
        request_id: Uuid,
        digest: Digest,
        current_scope: Option<ScopedCatalogRead>,
    ) -> Result<ChargedBytes, ProtocolError> {
        if request_id.is_nil()
            || authority.validate().is_err()
            || current_scope
                .is_some_and(|scope| self.scoped != Some(ScopedStatusEpoch::from_scope(scope)))
        {
            return Err(invalid());
        }
        let barrier = self.consensus.ensure_linearizable().await?;
        let scope = self.identity().clone();
        let epoch = self.scoped;
        self.machine
            .store()
            .read_metadata(LOCATOR_BUDGET, move |view| {
                check_fence(view, authority, epoch)?;
                let identity: Option<IdentityRecord> = read(view, &key(IDENTITIES, request_id))?;
                let home: Option<LocatedTimestampManifest> = read(view, &home_key(request_id))?;
                let Some(identity) = identity else {
                    return if home.is_none() {
                        view.encode(&TimestampPointHome::Absent)
                    } else {
                        Err(unknown())
                    };
                };
                if identity.reservation.request_id != request_id
                    || identity.reservation.digest != digest
                    || epoch.is_some_and(|epoch| {
                        !epoch.admits(identity.reservation.scoped, scope.group_id)
                    })
                {
                    return Err(invalid());
                }
                if let Some(home) = home {
                    if current_scope.is_some_and(|scope| !home.locator.binding.matches_scope(scope))
                    {
                        return Err(invalid());
                    }
                    projection::verify_in_read(
                        view,
                        &scope,
                        &identity,
                        &home,
                        authority,
                        Some(barrier.applied),
                    )?;
                    return view.encode(&TimestampPointHome::Installed(home));
                }
                let mut found = false;
                view.scan_page(&transaction_prefix(request_id), None, 1, |_, _| {
                    found = true;
                    Ok(())
                })?;
                view.encode(&if found
                    || identity.restore_origin.is_some()
                    || identity.terminal.is_some()
                    || identity.rejection.is_some()
                    || identity.reservation.kind != RequestKind::Mutation
                    || identity.reservation.original_run != authority.run_id
                    || identity.reservation.scoped.is_none()
                {
                    TimestampPointHome::Legacy
                } else {
                    TimestampPointHome::Absent
                })
            })
            .await
    }

    pub(crate) async fn read_timestamp_point_manifest(
        &self,
        route: crate::oltp_replication::authority::control::TimestampPointRoute,
        txn: TxnId,
        request_digest: Digest,
        manifest: ManifestRef,
        data_scope: Option<ScopedCatalogRead>,
    ) -> Result<ChargedBytes, ProtocolError> {
        let authority = route.authority;
        if authority.validate().is_err() || manifest.status_group_id != self.identity().group_id {
            return Err(invalid());
        }
        let barrier = self.consensus.ensure_linearizable().await?;
        let group = self.identity().clone();
        self.machine
            .store()
            .read_metadata(LOCATOR_BUDGET, move |view| {
                check_fence(view, authority, data_scope.map(ScopedStatusEpoch::from_scope))?;
                let request_id = txn.request_id;
                let identity: IdentityRecord =
                    read(view, &key(IDENTITIES, request_id))?.ok_or_else(unknown)?;
                let home: LocatedTimestampManifest =
                    read(view, &home_key(request_id))?.ok_or_else(unknown)?;
                let binding = &home.locator.binding;
                if binding.txn != txn
                    || binding.request.digest != request_digest
                    || home.locator.manifest != manifest
                {
                    return Err(invalid());
                }
                if let Some(data_scope) = data_scope {
                    if route.source_group != binding.participant.group.group_id
                        || route.source_generation != data_scope.data_generation
                        || authority.run_id != binding.request.run_id
                        || !binding.matches_scope(data_scope)
                    {
                        return Err(invalid());
                    }
                } else if route.source_group != binding.timestamps_group.group_id {
                    return Err(invalid());
                } else if authority.run_id == binding.request.run_id {
                    let original = ScopedStatusEpoch::from_scope(binding.original_scope);
                    let epoch =
                        read::<ScopedStatusEpoch>(view, &original.key())?.ok_or_else(unknown)?;
                    if !epoch.valid(Some(authority))
                        || !epoch.admits(identity.reservation.scoped, group.group_id)
                        || !binding.matches_scope(ScopedCatalogRead {
                            queue_view: epoch.view,
                            ..binding.original_scope
                        })
                    {
                        return Err(unknown());
                    }
                }
                projection::verify_in_read(
                    view,
                    &group,
                    &identity,
                    &home,
                    authority,
                    Some(barrier.applied),
                )?;
                view.encode(&home)
            })
            .await
    }

    async fn decode_timestamp_point<T: DeserializeOwned + Send + 'static>(
        &self,
        raw: ChargedBytes,
    ) -> Result<(T, Arc<ScratchLease>), ProtocolError> {
        let owner = Arc::new(
            self.resources()
                .reserve_reply_decode(crate::oltp::mvcc::json_decode_budget(raw.as_slice())?)
                .await?,
        );
        self.resources()
            .execute(ExecutionClass::Control, 0, 0, move || {
                let value = serde_json::from_slice(raw.as_slice()).map_err(|_| unknown())?;
                Ok((value, owner))
            })
            .await
    }
}

const LOCATOR_BUDGET: TransactionBudget = TransactionBudget {
    scratch_bytes: 128 << 10,
    batch_bytes: 0,
    output_bytes: TIMESTAMP_POINT_LOCATOR_REPLY_LIMIT,
};

fn check_fence(
    view: &mut GroupRead<'_>,
    authority: Authority,
    epoch: Option<ScopedStatusEpoch>,
) -> Result<(), ProtocolError> {
    if read::<Authority>(view, STATE)? != Some(authority) {
        return Err(unknown());
    }
    if let Some(epoch) = epoch
        && (!epoch.valid(Some(authority))
            || read::<ScopedStatusEpoch>(view, &epoch.key())? != Some(epoch))
    {
        return Err(unknown());
    }
    Ok(())
}
