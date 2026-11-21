//! Coordinate the native Timestamp decision for an eligible single-row update.
use super::*;
use crate::oltp_replication::authority::{
    status::{LocatedTimestampManifest, MutationOutcome, TimestampPointHome},
    timestamp::{TimestampDecisionLocator, TimestampPointBinding, TimestampPointManifest},
};

pub(super) struct PointAdmission<'a> {
    pub reservation: IdentityReservation,
    pub read: &'a QueueReadToken,
    pub epoch: ScopedTimestampEpoch,
}

pub(super) enum PointRecovery {
    Legacy,
    Recorded(Option<VerifiedStatusDecision>),
}

impl TransactionCoordinator {
    pub(super) async fn try_commit_timestamp_point(
        &self,
        authority: Authority,
        scope: ScopedCatalogRead,
        txn: TxnId,
        admission: &PointAdmission<'_>,
        prepared: &PreparedMutation,
    ) -> Result<Option<VerifiedStatusDecision>, ProtocolError> {
        let home = self.timestamp_point_home(authority, scope, txn).await?;
        if matches!(home, TimestampPointHome::Legacy) {
            return Ok(None);
        }
        let [participant] = prepared.participants.as_slice() else {
            return unmarked_only(&home);
        };
        let [writes] = participant.pages.as_slice() else {
            return unmarked_only(&home);
        };
        if prepared.result.as_str() != "true"
            || !matches!(&participant.guards[..], [guard] if guard.mode() == store::lifecycle::GuardMode::Shared)
            || !matches!(&writes[..], [write]
                if matches!(&write.key, crate::oltp::mvcc::MvccKey::Row { .. })
                    && matches!(&write.change, crate::oltp::mvcc::Change::Put(_))
                    && write.expected_version.is_some())
        {
            return unmarked_only(&home);
        }
        let outcome = MutationOutcome::from_success(&prepared.result)?;
        let binding = match &home {
            TimestampPointHome::Installed(installed) => {
                let binding = &installed.locator.binding;
                if binding.participant != participant.plan
                    || binding.reservation != admission.reservation
                    || binding.pin_identity != admission.read.identity
                    || binding.storage_pin != admission.read.storage_pin
                    || binding.catalog_root != admission.read.catalog_root
                    || installed.outcome != outcome
                {
                    return Err(unknown());
                }
                binding.clone()
            }
            TimestampPointHome::Absent => TimestampPointBinding {
                status_group: self.status.group_for_request(txn.request_id).clone(),
                timestamps_group: self.timestamps.identity().clone(),
                participant: participant.plan.clone(),
                created_authority: authority,
                txn,
                request: scope.request,
                pin_identity: admission.read.identity.clone(),
                storage_pin: admission.read.storage_pin,
                result_digest: outcome.digest()?,
                reservation: admission.reservation,
                original_epoch: admission.epoch,
                original_scope: scope,
                catalog_root: admission.read.catalog_root,
            },
            TimestampPointHome::Legacy => return Ok(None),
        };
        let candidate = TimestampPointManifest { binding, outcome };
        let manifest = candidate.manifest()?;
        let page = ParticipantCommand::PreparePage {
            authority,
            txn,
            request: scope.request,
            manifest,
            plan: participant.plan.clone(),
            page_index: 0,
            previous_root: store::initial_root(txn, &participant.plan)?,
            writes: writes.clone(),
            guards: participant.guards.clone(),
        };
        if !store::timestamp_resolution::point_page_fits(&page)? {
            return unmarked_only(&home);
        }
        let page = OwnedCommand::new_owned(page, &self.shared, ByteClass::Data)?;
        let reconcile = matches!(home, TimestampPointHome::Installed(_));
        let locator = TimestampDecisionLocator {
            binding: candidate.binding.clone(),
            manifest,
        };
        if !reconcile
            && self
                .status
                .install_timestamp_point(authority, candidate)
                .await?
                != manifest
        {
            return Err(unknown());
        }
        let data = &participant.participant;
        let reply = data
            .prepare_timestamp_point(locator.clone(), page.clone())
            .await?;
        match reply.reply {
            ParticipantReply::PreparedPage { .. } => {
                let sealed = data
                    .seal_timestamp_point(authority, locator.clone())
                    .await?;
                if !matches!(&sealed.reply, ParticipantReply::Sealed(seal) if locator.matches_seal(seal))
                {
                    return Err(unknown());
                }
                self.timestamps
                    .commit_timestamp_point(&locator, reconcile)
                    .await?;
            }
            ParticipantReply::Rejected(store::Fault::Conflict) => {
                let rejected = data.reject_timestamp_point(locator.clone(), page).await?;
                if !matches!(rejected.reply, ParticipantReply::TimestampPointRejected(_)) {
                    return Err(unknown());
                }
                self.timestamps
                    .abort_timestamp_point(&locator, reconcile)
                    .await?;
            }
            _ => return Err(unknown()),
        }
        #[cfg(test)]
        super::super::recovery_tests::pause("after-scoped-timestamp-decision", txn.request_id)
            .await;
        self.finish_timestamp_point(authority, scope, &locator, data)
            .await
            .map(Some)
    }

    async fn timestamp_point_home(
        &self,
        authority: Authority,
        scope: ScopedCatalogRead,
        txn: TxnId,
    ) -> Result<TimestampPointHome, ProtocolError> {
        let home = self
            .status
            .timestamp_point_home(authority, txn.request_id, scope.request.digest)
            .await?;
        if matches!(&home, TimestampPointHome::Installed(LocatedTimestampManifest { locator, .. })
            if locator.binding.txn != txn || !locator.binding.matches_scope(scope))
        {
            return Err(unknown());
        }
        Ok(home)
    }

    pub(super) async fn recover_timestamp_point(
        &self,
        authority: Authority,
        scope: ScopedCatalogRead,
        txn: TxnId,
    ) -> Result<PointRecovery, ProtocolError> {
        let TimestampPointHome::Installed(LocatedTimestampManifest { locator, .. }) =
            self.timestamp_point_home(authority, scope, txn).await?
        else {
            return Ok(PointRecovery::Legacy);
        };
        if self
            .timestamps
            .observe_timestamp_point(&locator)
            .await?
            .is_none()
        {
            // The Data state machine makes seal and definitive rejection exclusive.
            // Either exact proof may have survived an interrupted owner.
            let (commit, reject) = tokio::join!(
                self.timestamps.commit_timestamp_point(&locator, true),
                self.timestamps.abort_timestamp_point(&locator, true),
            );
            if commit.is_err() && reject.is_err() {
                return Ok(PointRecovery::Recorded(None));
            }
        }
        let data = self
            .resolver
            .participant(&locator.binding.participant.group)
            .await?;
        let decision = self
            .finish_timestamp_point(authority, scope, &locator, &data)
            .await?;
        Ok(PointRecovery::Recorded(Some(decision)))
    }

    async fn finish_timestamp_point(
        &self,
        authority: Authority,
        scope: ScopedCatalogRead,
        locator: &TimestampDecisionLocator,
        data: &ParticipantHandle,
    ) -> Result<VerifiedStatusDecision, ProtocolError> {
        let txn = locator.binding.txn;
        let manifest = locator.manifest;
        if self
            .status
            .timestamp_point_projection(authority, locator)
            .await?
            .is_none()
        {
            let resolved = data
                .resolve_timestamp_point(authority, locator.clone())
                .await?;
            if !matches!(
                resolved.reply,
                ParticipantReply::Resolved { complete: true, .. }
            ) {
                return Err(unknown());
            }
            self.status
                .project_timestamp_point(authority, locator)
                .await?;
        }
        let settled = self.status.settled_proof(txn, manifest).await?;
        self.release_scoped_guard(authority, scope, txn, settled)
            .await?;
        self.status
            .lookup_decision(txn, manifest)
            .await?
            .ok_or_else(unknown)
    }
}

fn unmarked_only(
    home: &TimestampPointHome,
) -> Result<Option<VerifiedStatusDecision>, ProtocolError> {
    if matches!(home, TimestampPointHome::Installed(_)) {
        return Err(unknown());
    }
    Ok(None)
}
