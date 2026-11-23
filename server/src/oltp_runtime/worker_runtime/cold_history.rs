//! Settle old durable decisions and release abandoned application pins.
//! The queue and application request decoder are outside this path.
use super::*;
use crate::oltp::mvcc::store::ParticipantReply;
use crate::oltp_replication::{
    authority::{
        status::{
            IdentityLookup, StatusHandle, StatusService, TerminalIdentity, TimestampPointHome,
            identity::transaction::ColdRecoveryPage,
        },
        timestamp::{
            TimestampHandle,
            cold::{ColdPinCursor, ColdPinStep},
        },
    },
    service_rpc::TimestampClient,
};
use std::time::Duration;

pub(crate) enum Sources {
    Local {
        timestamps: Arc<TimestampHandle>,
        status: Arc<StatusHandle>,
    },
    Registered {
        timestamps: Arc<TimestampClient>,
    },
}

pub(super) fn spawn(runtime: &Arc<WorkerRuntime>) -> tokio::task::JoinHandle<()> {
    let runtime = Arc::downgrade(runtime);
    nabedra_rt::task::spawn(async move {
        let Some(initial) = runtime.upgrade() else {
            return;
        };
        let status_groups = initial.services.remote_status.len().max(1);
        drop(initial);
        let mut cursors = vec![Some((None, false)); status_groups];
        let mut pins = Some((None, false));
        let mut bucket = 0;
        while pins.is_some() || cursors.iter().any(Option::is_some) {
            nabedra_rt::time::sleep(Duration::from_millis(500)).await;
            let Some(runtime) = runtime.upgrade() else {
                break;
            };
            if !runtime.accepting.load(Ordering::Acquire) {
                break;
            }
            let local = runtime.services.local;
            let is_local_owner = runtime
                .services
                .replica
                .lock()
                .is_ok_and(|replica| replica.authority().owner == local);
            if !is_local_owner {
                continue;
            }
            let Ok(current) = runtime
                .services
                .control
                .current(&runtime.services.scope)
                .await
            else {
                continue;
            };
            if current.owner != local {
                continue;
            }
            let authority = current.ready.view.authority();
            let transition_id = match runtime
                .services
                .control
                .published_cold_fence(authority, None)
                .await
            {
                Ok(Some(transition_id)) => transition_id,
                Ok(None) => break,
                Err(_) => continue,
            };
            let Some(sources) = &runtime.services.cold_history else {
                break;
            };
            if let Some((after, retry_pass)) = cursors[bucket] {
                match runtime
                    .cold_history_step(sources, bucket, authority, transition_id, after)
                    .await
                {
                    Ok((page, retry)) => {
                        let retry_pass = retry_pass || retry;
                        cursors[bucket] = if page.done {
                            retry_pass.then_some((None, false))
                        } else {
                            Some((page.after, retry_pass))
                        };
                    }
                    Err(error) => tracing::debug!(bucket,
                        code=?error.code, message=error.message, "cold Status history page awaits retry"),
                }
            }
            bucket = (bucket + 1) % cursors.len();
            if let Some((after, retry_pass)) = pins {
                match runtime
                    .cold_pin_step(sources, authority, transition_id, after)
                    .await
                {
                    Ok(step) => {
                        let retry_pass = retry_pass || step.retry;
                        pins = if step.done {
                            retry_pass.then_some((None, false))
                        } else {
                            Some((step.after, retry_pass))
                        };
                    }
                    Err(error) => tracing::debug!(code=?error.code,
                        message=error.message, "cold application pin page awaits retry"),
                }
            }
        }
    })
}

impl WorkerRuntime {
    async fn cold_history_step(
        &self,
        sources: &Sources,
        bucket: usize,
        authority: Authority,
        transition_id: Uuid,
        after: Option<TxnId>,
    ) -> Result<(ColdRecoveryPage, bool), ProtocolError> {
        let page: ColdRecoveryPage = match sources {
            Sources::Registered { .. } => {
                self.services.remote_status[bucket]
                    .cold_recovery_page(authority, after)
                    .await?
            }
            Sources::Local { status, .. } => {
                let raw = status.cold_recovery_page(authority, after).await?;
                serde_json::from_slice(raw.as_slice()).map_err(|_| unknown())?
            }
        };
        let mut retry = false;
        if let Some(txn) = page.transaction {
            let status: &dyn StatusService = match sources {
                Sources::Registered { .. } => self.services.remote_status[bucket].as_ref(),
                Sources::Local { status, .. } => status.as_ref(),
            };
            let recovered: Result<bool, ProtocolError> = async {
                if self
                    .cold_point_history(status, authority, transition_id, txn)
                    .await?
                {
                    return Ok(true);
                }
                Ok(self.transactions.recover(authority, txn).await?.is_some())
            }
            .await;
            match recovered {
                Ok(true) => {}
                Ok(false) => retry = true,
                Err(error) => {
                    tracing::debug!(request=%txn.request_id, code=?error.code,
                        message=error.message, "cold transaction settlement awaits retry");
                    retry = true;
                }
            }
        }
        Ok((page, retry))
    }

    /// Reconcile only the installed metadata. The protected cold decision and
    /// Data resolution paths also cover a marked attempt with no Data prepare.
    async fn cold_point_history(
        &self,
        status: &dyn StatusService,
        authority: Authority,
        transition_id: Uuid,
        txn: TxnId,
    ) -> Result<bool, ProtocolError> {
        let identity = status
            .lookup_identity(txn.request_id, None)
            .await?
            .ok_or_else(unknown)?;
        let (reservation, marked) = match identity {
            IdentityLookup::TimestampPoint(reservation) => (reservation, true),
            IdentityLookup::Reserved(reservation)
            | IdentityLookup::Abandoned(reservation)
            | IdentityLookup::Terminal(TerminalIdentity::Committed { reservation, .. }) => {
                (reservation, false)
            }
            _ => return Ok(false),
        };
        if reservation.scoped.is_none() {
            return if marked { Err(unknown()) } else { Ok(false) };
        }
        let home = status
            .timestamp_point_home(authority, txn.request_id, reservation.digest)
            .await?;
        let locator = match home {
            TimestampPointHome::Installed(home) => home.locator,
            TimestampPointHome::Legacy if !marked => return Ok(false),
            TimestampPointHome::Absent | TimestampPointHome::Legacy => return Err(unknown()),
        };
        let binding = &locator.binding;
        if binding.txn != txn
            || &binding.status_group != status.identity()
            || &binding.timestamps_group != self.services.timestamps.identity()
            || reservation != binding.reservation
            || binding.created_authority.run_id == authority.run_id
            || binding.created_authority.owner_term >= authority.owner_term
            || self
                .services
                .control
                .published_cold_fence(authority, Some(reservation.original_run))
                .await?
                != Some(transition_id)
        {
            return Err(unknown());
        }

        let timestamps = &self.services.timestamps;
        let (decision, _decision_owner) = match timestamps.observe_timestamp_point(&locator).await?
        {
            Some(proof) => proof,
            None => {
                timestamps
                    .abandon_timestamp_point(&locator, transition_id)
                    .await?
            }
        };
        decision.verify_locator(&locator)?;
        if let Some((projection, _owner)) = status
            .timestamp_point_projection(authority, &locator)
            .await?
        {
            if projection.completion().decision() != &decision {
                return Err(unknown());
            }
            return Ok(true);
        }

        let participant = self
            .services
            .resolver
            .participant(&binding.participant.group)
            .await?;
        let applied = participant
            .resolve_timestamp_point(authority, locator.clone())
            .await?;
        if !matches!(
            applied.reply,
            ParticipantReply::Resolved { complete: true, .. }
        ) {
            return Err(unknown());
        }
        let (projection, _projection_owner) =
            status.project_timestamp_point(authority, &locator).await?;
        if projection.completion().decision() != &decision
            || projection.completion().completed_at() != applied.position
        {
            return Err(unknown());
        }
        Ok(true)
    }

    async fn cold_pin_step(
        &self,
        sources: &Sources,
        authority: Authority,
        transition_id: Uuid,
        after: Option<ColdPinCursor>,
    ) -> Result<ColdPinStep, ProtocolError> {
        match sources {
            Sources::Registered { timestamps } => {
                timestamps
                    .cold_pin_step(authority, transition_id, after)
                    .await
            }
            Sources::Local { timestamps, status } => {
                timestamps
                    .sweep_cold_application_pin(authority, transition_id, after, |pin| async move {
                        if self
                            .services
                            .control
                            .published_cold_fence(authority, Some(pin.identity.run_id))
                            .await?
                            != Some(transition_id)
                        {
                            return Err(unknown());
                        }
                        let raw = status.cold_application_pin_settled(authority, pin).await?;
                        serde_json::from_slice(raw.as_slice()).map_err(|_| unknown())
                    })
                    .await
            }
        }
    }
}
