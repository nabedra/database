//! Settle old durable decisions and release abandoned application pins.
//! The queue and application request decoder are outside this path.
use super::*;
use crate::oltp_replication::{
    authority::{
        status::{StatusHandle, identity::transaction::ColdRecoveryPage},
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
    tokio::spawn(async move {
        let Some(initial) = runtime.upgrade() else {
            return;
        };
        let status_groups = initial.services.remote_status.len().max(1);
        drop(initial);
        let mut cursors = vec![Some((None, false)); status_groups];
        let mut pins = Some((None, false));
        let mut bucket = 0;
        while pins.is_some() || cursors.iter().any(Option::is_some) {
            tokio::time::sleep(Duration::from_millis(500)).await;
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
                    .cold_history_step(sources, bucket, authority, after)
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
            match self.transactions.recover(authority, txn).await {
                Ok(Some(_)) => {}
                Ok(None) => retry = true,
                Err(error) => {
                    tracing::debug!(request=%txn.request_id, code=?error.code,
                        message=error.message, "cold transaction settlement awaits retry");
                    retry = true;
                }
            }
        }
        Ok((page, retry))
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
