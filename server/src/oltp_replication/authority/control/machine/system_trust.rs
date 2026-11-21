use super::*;
use crate::oltp_replication::{
    authority::control::ReplacementRecord, multiplex::AppliedPeerDirectory,
};
use std::sync::Arc;

impl ControlMachine {
    pub(crate) async fn refresh_replacement_trust(
        &self,
        directory: &AppliedPeerDirectory,
    ) -> Result<(), ProtocolError> {
        let control_id = self.store.identity().group_id;
        let raw = self
            .store
            .read_metadata(BUDGET, move |view| {
                let Some(state) = super::super::service::load::<State>(view, STATE)? else {
                    return view.encode(&None::<Trust>);
                };
                if view.get(system_recovery::ACTIVATION)?.is_none() {
                    return view.encode(&None::<Trust>);
                }
                let active = super::super::service::load::<Uuid>(view, system_recovery::ACTIVE)?;
                let candidate =
                    match super::super::service::load::<Uuid>(view, system_recovery::PREPARING)? {
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
                            if revoked.len() == 1024 {
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
                view.encode(&Some(Trust {
                    revision: state.enrollment_revision,
                    operators,
                    revoked,
                    candidate,
                }))
            })
            .await?;
        if let Some(trust) = decode::<Option<Trust>>(raw.as_ref())? {
            directory.install_system_trust(
                trust.revision,
                trust.operators,
                trust.revoked,
                trust.candidate,
            )?;
        }
        directory.replace_revocation_view(false);
        Ok(())
    }
}

#[derive(Serialize, serde::Deserialize)]
struct Trust {
    revision: u64,
    operators: Vec<WorkerEnrollment>,
    revoked: Vec<WorkerIdentity>,
    candidate: Option<(ReplacementRecord, bool)>,
}

impl ControlMachine {
    pub(crate) async fn with_replacement_directory(
        mut self,
        directory: Arc<AppliedPeerDirectory>,
    ) -> Result<Self, ProtocolError> {
        self.refresh_replacement_trust(&directory).await?;
        self.replacement_directory = Some(directory);
        Ok(self)
    }
}
