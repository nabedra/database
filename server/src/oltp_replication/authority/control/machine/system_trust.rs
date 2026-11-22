use super::*;
use crate::oltp_replication::{
    authority::control::ReplacementRecord, multiplex::AppliedPeerDirectory,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::{Mutex, mpsc};

// REP4 could commit one revocation past its limit before refresh stopped the
// Raft core. Read that durable record on upgrade, while refusing new mounts.
const MAX_RETAINED_REVOCATIONS: usize = system_recovery::MAX_REVOCATIONS + 1;

#[derive(Clone)]
pub(super) struct ReplacementTrust {
    pub(super) directory: Arc<AppliedPeerDirectory>,
    order: Arc<Mutex<()>>,
    retry: mpsc::Sender<()>,
}

impl ControlMachine {
    pub(crate) async fn refresh_replacement_trust(
        &self,
        directory: &AppliedPeerDirectory,
    ) -> Result<(), ProtocolError> {
        let control_id = self.store.identity().group_id;
        let raw = self
            .store
            .read_metadata(
                TransactionBudget {
                    output_bytes: 512 << 10,
                    ..BUDGET
                },
                move |view| {
                    let Some(state) = super::super::service::load::<State>(view, STATE)? else {
                        return view.encode(&None::<Trust>);
                    };
                    if view.get(system_recovery::ACTIVATION)?.is_none() {
                        return view.encode(&None::<Trust>);
                    }
                    let active =
                        super::super::service::load::<Uuid>(view, system_recovery::ACTIVE)?;
                    let candidate = match super::super::service::load::<Uuid>(
                        view,
                        system_recovery::PREPARING,
                    )? {
                        Some(id) => Some((
                            super::super::service::load::<ReplacementRecord>(
                                view,
                                &system_recovery::by_id(id),
                            )?
                            .ok_or_else(unknown)?,
                            active == Some(id),
                        )),
                        None => None,
                    };
                    let mut revoked = Vec::new();
                    let mut after = None;
                    loop {
                        let mut next = None;
                        let page = view.scan_page(
                            system_recovery::REVOKED_WORKERS,
                            after.as_deref(),
                            128,
                            |key, bytes| {
                                if revoked.len() == MAX_RETAINED_REVOCATIONS {
                                    return Err(unknown());
                                }
                                revoked.push(decode::<WorkerIdentity>(bytes)?);
                                next = Some(key.to_vec());
                                Ok(())
                            },
                        )?;
                        if page.exhausted {
                            break;
                        }
                        after = next;
                    }
                    let mut operators = Vec::new();
                    let group =
                        super::super::service::load::<GroupRecord>(view, &key(GROUPS, control_id))?;
                    if let Some(group) = group {
                        let mut members = Vec::new();
                        view.scan_page(&member_prefix(group.generation), None, 16, |_, bytes| {
                            members.push(decode::<ReplicaMember>(bytes)?);
                            Ok(())
                        })?;
                        for member in members {
                            if let Some(worker) = super::super::service::load::<WorkerRecord>(
                                view,
                                &key(WORKERS, member.worker_id),
                            )? {
                                operators.push(worker.enrollment);
                            }
                        }
                    }
                    let hardened = view.get(system_recovery::HARDENING)?.is_some();
                    let deferred_data = view.get(system_recovery::DEFERRED_DATA)?.is_some();
                    view.encode(&Some(Trust {
                        revision: state.enrollment_revision,
                        hardened,
                        deferred_data,
                        operators,
                        revoked,
                        candidate,
                    }))
                },
            )
            .await?;
        if let Some(trust) = decode::<Option<Trust>>(raw.as_ref())? {
            // The revocation set has its own reservation. Installing it must
            // succeed even when rebuilding the candidate/operator view cannot.
            directory.install_revocations(trust.revoked);
            directory.install_system_trust(
                trust.revision,
                trust.operators,
                trust.candidate,
                trust.hardened,
                trust.deferred_data,
            )?;
        } else {
            directory.install_revocations(Vec::new());
        }
        Ok(())
    }
}

#[derive(Serialize, serde::Deserialize)]
struct Trust {
    revision: u64,
    hardened: bool,
    deferred_data: bool,
    operators: Vec<WorkerEnrollment>,
    revoked: Vec<WorkerIdentity>,
    candidate: Option<(ReplacementRecord, bool)>,
}

impl ControlMachine {
    pub(super) async fn refresh_or_retry(&self) {
        let Some(trust) = &self.replacement_trust else {
            return;
        };
        let _order = trust.order.lock().await;
        if let Err(error) = self.refresh_replacement_trust(&trust.directory).await {
            tracing::warn!(code=?error.code, message=%error.message, "Control replacement trust refresh deferred");
            let _ = trust.retry.try_send(());
        }
    }

    pub(crate) async fn with_replacement_directory(
        mut self,
        directory: Arc<AppliedPeerDirectory>,
    ) -> Result<Self, ProtocolError> {
        directory.reserve_revocations(MAX_RETAINED_REVOCATIONS)?;
        directory.require_revocation_snapshot();
        let (retry, mut pending) = mpsc::channel(1);
        let order = Arc::new(Mutex::new(()));
        let weak = Arc::downgrade(&directory);
        let reader = self.clone();
        let retry_order = order.clone();
        tokio::spawn(async move {
            while pending.recv().await.is_some() {
                let mut delay = Duration::from_millis(100);
                loop {
                    tokio::time::sleep(delay).await;
                    if pending.is_closed() {
                        return;
                    }
                    let Some(directory) = weak.upgrade() else {
                        return;
                    };
                    let _order = retry_order.lock().await;
                    if reader.refresh_replacement_trust(&directory).await.is_ok() {
                        break;
                    }
                    delay = (delay * 2).min(Duration::from_secs(5));
                }
            }
        });
        self.replacement_trust = Some(ReplacementTrust {
            directory,
            order,
            retry,
        });
        self.refresh_or_retry().await;
        Ok(self)
    }
}
