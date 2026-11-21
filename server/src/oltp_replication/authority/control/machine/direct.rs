//! `data.direct.v1` (plan section 4 "Mixed versions"). Every worker and API
//! ingress advertises its exact enrolled identity. Control activates the
//! capability in the same apply that sees the last required advertisement,
//! durably, and never deactivates it. Once active, a worker identity that
//! did not advertise cannot become a voter or learner of any group.
use super::*;
use crate::oltp_replication::authority::control::direct::{
    DIRECT_VERSION, DirectActivation, DirectCapability, DirectCapabilityOperation,
};

const ACTIVATION: &[u8] = b"ct/direct-capability";
const READERS: &[u8] = b"ct/direct-reader/";
/// Present once a queue owner reserved an eligible mutation unsettled while
/// the capability was inactive (T7c). Never deleted.
const QUEUED_HISTORY: &[u8] = b"ct/direct-queued-history";

pub(in crate::oltp_replication::authority::control) fn activation_key() -> &'static [u8] {
    ACTIVATION
}
pub(in crate::oltp_replication::authority::control) fn reader_key(
    worker: WorkerIdentity,
) -> Vec<u8> {
    key(READERS, worker.worker_id)
}

pub(super) fn apply(
    tx: &mut GroupTransaction<'_>,
    state: &State,
    position: AppliedPosition,
    actor: WorkerIdentity,
    action: &DirectCapabilityOperation,
    unix_ms: u64,
) -> Result<ControlReply, ProtocolError> {
    if actor.validate().is_err() || !is_member(tx, actor)? {
        return fault(Fault::Unauthorized);
    }
    // A Control transaction does not read its own writes: the advert this
    // entry writes counts through `advertising`.
    let advertising = match action {
        DirectCapabilityOperation::Advertise { version } => {
            if *version != DIRECT_VERSION {
                return fault(Fault::Invalid);
            }
            save(tx, &reader_key(actor), &actor)?;
            Some(actor)
        }
        // The owner runs this build: its note is also its advert.
        DirectCapabilityOperation::QueuedHistory => {
            save(tx, &reader_key(actor), &actor)?;
            Some(actor)
        }
        DirectCapabilityOperation::Activate => None,
    };
    let activation = match load::<DirectActivation>(tx, ACTIVATION)? {
        Some(activation) => Some(activation),
        None if can_activate(tx, state, advertising)? => {
            let activation = DirectActivation {
                activated_at: position,
                run_id: state.ready.view.run_id,
                enrollment_revision: state.enrollment_revision,
                queued_history: tx.get(QUEUED_HISTORY)?.is_some(),
                unix_ms,
            };
            save(tx, ACTIVATION, &activation)?;
            Some(activation)
        }
        None => {
            // Recorded in the same entry that answers "inactive": every
            // later activation sees it (T7c).
            if matches!(action, DirectCapabilityOperation::QueuedHistory) {
                save(tx, QUEUED_HISTORY, &true)?;
            }
            None
        }
    };
    Ok(ControlReply::DirectCapability(DirectCapability {
        version: DIRECT_VERSION,
        activation,
        advertised: advertising.is_some() || supports(tx, actor)?,
    }))
}

/// A current worker or API enrollment: only those advertise.
fn is_member(tx: &mut GroupTransaction<'_>, actor: WorkerIdentity) -> Result<bool, ProtocolError> {
    Ok(scoped_queue::capability::is_api(tx, actor)?
        || load::<WorkerRecord>(tx, &key(WORKERS, actor.worker_id))?
            .is_some_and(|worker| WorkerIdentity::from(&worker.enrollment) == actor))
}

pub(super) fn supports(
    tx: &mut GroupTransaction<'_>,
    worker: WorkerIdentity,
) -> Result<bool, ProtocolError> {
    Ok(load::<WorkerIdentity>(tx, &reader_key(worker))? == Some(worker))
}

pub(super) fn active(tx: &mut GroupTransaction<'_>) -> Result<bool, ProtocolError> {
    Ok(tx.get(ACTIVATION)?.is_some())
}

/// After activation, only a worker that advertised its current identity may
/// join a group or the ready set: an older build cannot decode direct
/// entries. Before activation everyone may join.
pub(super) fn admits(
    tx: &mut GroupTransaction<'_>,
    worker: WorkerIdentity,
) -> Result<bool, ProtocolError> {
    Ok(!active(tx)? || supports(tx, worker)?)
}

/// `admits` for a replica named only by its worker id: the enrolled
/// identity must have advertised.
pub(super) fn admits_worker_id(
    tx: &mut GroupTransaction<'_>,
    worker_id: Uuid,
) -> Result<bool, ProtocolError> {
    if !active(tx)? {
        return Ok(true);
    }
    let Some(record) = load::<WorkerRecord>(tx, &key(WORKERS, worker_id))? else {
        return Ok(false);
    };
    supports(tx, WorkerIdentity::from(&record.enrollment))
}

/// Test processes (T7c): while the file named by
/// `NABEDRA_OLTP_TEST_DIRECT_HOLD` exists, Control does not activate, as if
/// a member had not advertised yet. Release builds have no such hook.
#[cfg(test)]
fn held_for_test() -> bool {
    std::env::var_os("NABEDRA_OLTP_TEST_DIRECT_HOLD")
        .is_some_and(|path| std::path::Path::new(&path).exists())
}
#[cfg(not(test))]
fn held_for_test() -> bool {
    false
}

/// Every enrolled worker and API advertised its current identity, and no
/// membership change that could add an unchecked member is in flight.
fn can_activate(
    tx: &mut GroupTransaction<'_>,
    state: &State,
    advertising: Option<WorkerIdentity>,
) -> Result<bool, ProtocolError> {
    if state.pending.is_some()
        || state.restart.is_some()
        || state.ready.ready_count == 0
        || state.invalid_ready != 0
        || tx.get(placement::ACTIVE)?.is_some()
        || held_for_test()
    {
        return Ok(false);
    }
    Ok(
        all_advertised(tx, WORKERS, advertising, |entry: WorkerRecord| {
            WorkerIdentity::from(&entry.enrollment)
        })? && all_advertised(tx, APIS, advertising, |entry: ApiEnrollment| {
            WorkerIdentity::from(entry.transport())
        })?,
    )
}

fn all_advertised<T: serde::de::DeserializeOwned>(
    tx: &mut GroupTransaction<'_>,
    prefix: &[u8],
    advertising: Option<WorkerIdentity>,
    identity: impl Fn(T) -> WorkerIdentity,
) -> Result<bool, ProtocolError> {
    let mut cursor = None;
    loop {
        let mut members = Vec::with_capacity(16);
        let progress = tx.scan_page(prefix, cursor.as_deref(), 16, |key, raw| {
            let worker = identity(decode(raw)?);
            if worker.validate().is_err()
                || key.get(prefix.len()..) != Some(worker.worker_id.as_bytes())
            {
                return Err(unknown());
            }
            members.push(worker);
            Ok(())
        })?;
        for worker in members {
            cursor = Some(super::key(prefix, worker.worker_id));
            if advertising != Some(worker) && !supports(tx, worker)? {
                return Ok(false);
            }
        }
        if progress.exhausted {
            return Ok(true);
        }
    }
}
