//! `data.direct.v1` (plan section 4 "Mixed versions"). Every worker and API
//! ingress advertises its exact enrolled identity. Control activates the
//! capability in the same apply that sees the last required advertisement,
//! durably, and never deactivates it. Once active, a worker identity that
//! did not advertise cannot become a voter or learner of any group.
use super::*;
use crate::oltp_replication::authority::control::direct::{
    DIRECT_BATCH_FEATURE, DIRECT_VERSION, DirectActivation, DirectCapability,
    DirectCapabilityOperation, FeatureActivation, feature_operation_id,
};

const ACTIVATION: &[u8] = b"ct/direct-capability";
const READERS: &[u8] = b"ct/direct-reader/";
/// Present once a queue owner reserved an eligible mutation unsettled while
/// the capability was inactive (T7c). Never deleted.
const QUEUED_HISTORY: &[u8] = b"ct/direct-queued-history";
/// GC2: `data.direct.batch` is enabled (a `FeatureActivation`). Written only
/// by a feature-aware build, never deleted.
const BATCH_ENABLED: &[u8] = b"ct/feature/data.direct.batch";

pub(in crate::oltp_replication::authority::control) fn batch_key() -> &'static [u8] {
    BATCH_ENABLED
}

// Mixed-version tests (GC2): a Control replica of the build before cluster
// features, simulated by switching off every feature-aware rule. Set by
// `ControlMachine::apply` around one apply on the native thread that runs it.
#[cfg(test)]
thread_local! {
    pub(super) static LEGACY_V1: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
#[cfg(test)]
fn legacy_v1() -> bool {
    LEGACY_V1.with(std::cell::Cell::get)
}
#[cfg(not(test))]
fn legacy_v1() -> bool {
    false
}

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
    operation_id: Uuid,
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
    // GC2: the reply and every write above are what a pre-feature build
    // produces for this entry. A feature-aware build may add only the
    // enablement, and only when every enrolled worker advertised it.
    if activation.is_some() && !legacy_v1() {
        let batch_advert = matches!(action, DirectCapabilityOperation::Advertise { .. })
            && operation_id == feature_operation_id(DIRECT_BATCH_FEATURE, actor);
        maybe_enable_batch(tx, state, position, batch_advert.then_some(actor))?;
    }
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

/// `worker`'s current identity advertised `data.direct.batch`: the receipt of
/// its feature advert exists (see `control::direct`).
pub(super) fn supports_batch(
    tx: &mut GroupTransaction<'_>,
    worker: WorkerIdentity,
) -> Result<bool, ProtocolError> {
    let receipt = key(
        b"ct/op/",
        feature_operation_id(DIRECT_BATCH_FEATURE, worker),
    );
    Ok(load::<super::Receipt>(tx, &receipt)?
        .is_some_and(|receipt| matches!(receipt.reply, ControlReply::DirectCapability(_))))
}

/// After enablement, only a worker whose current identity advertised
/// `data.direct.batch` may join: an older build cannot decode `DirectBatch`.
/// A pre-feature Control build has no enablement, so it never refuses here.
fn batch_admits(
    tx: &mut GroupTransaction<'_>,
    worker: WorkerIdentity,
) -> Result<bool, ProtocolError> {
    Ok(legacy_v1() || tx.get(BATCH_ENABLED)?.is_none() || supports_batch(tx, worker)?)
}

/// After activation, only a worker that advertised its current identity may
/// join a group or the ready set: an older build cannot decode direct
/// entries. Before activation everyone may join. The same holds for every
/// enabled cluster feature (GC2).
pub(super) fn admits(
    tx: &mut GroupTransaction<'_>,
    worker: WorkerIdentity,
) -> Result<bool, ProtocolError> {
    Ok((!active(tx)? || supports(tx, worker)?) && batch_admits(tx, worker)?)
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
    let worker = WorkerIdentity::from(&record.enrollment);
    Ok(supports(tx, worker)? && batch_admits(tx, worker)?)
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
    let advertised =
        |tx: &mut GroupTransaction<'_>, worker: WorkerIdentity| -> Result<bool, ProtocolError> {
            Ok(advertising == Some(worker) || supports(tx, worker)?)
        };
    Ok(all_members(tx, WORKERS, advertised, |entry: WorkerRecord| {
        WorkerIdentity::from(&entry.enrollment)
    })? && all_members(tx, APIS, advertised, |entry: ApiEnrollment| {
        WorkerIdentity::from(entry.transport())
    })?)
}

/// GC2: enables `data.direct.batch` once every enrolled worker's current
/// identity advertised it (`advertising` is this entry's own advert, whose
/// receipt this transaction cannot read yet). Every Control voter and
/// learner is an enrolled worker, so they all run a feature-aware build and
/// apply this write alike. API ingresses hold no replica and are not
/// required. Mirrors `can_activate`'s membership guards.
fn maybe_enable_batch(
    tx: &mut GroupTransaction<'_>,
    state: &State,
    position: AppliedPosition,
    advertising: Option<WorkerIdentity>,
) -> Result<(), ProtocolError> {
    if tx.get(BATCH_ENABLED)?.is_some()
        || state.pending.is_some()
        || state.restart.is_some()
        || state.ready.ready_count == 0
        || state.invalid_ready != 0
        || tx.get(placement::ACTIVE)?.is_some()
    {
        return Ok(());
    }
    let advertised =
        |tx: &mut GroupTransaction<'_>, worker: WorkerIdentity| -> Result<bool, ProtocolError> {
            Ok(advertising == Some(worker) || supports_batch(tx, worker)?)
        };
    if all_members(tx, WORKERS, advertised, |entry: WorkerRecord| {
        WorkerIdentity::from(&entry.enrollment)
    })? {
        save(
            tx,
            BATCH_ENABLED,
            &FeatureActivation {
                activated_at: position,
                enrollment_revision: state.enrollment_revision,
            },
        )?;
    }
    Ok(())
}

/// Whether `advertised` holds for every member under `prefix`.
fn all_members<T: serde::de::DeserializeOwned>(
    tx: &mut GroupTransaction<'_>,
    prefix: &[u8],
    advertised: impl Fn(&mut GroupTransaction<'_>, WorkerIdentity) -> Result<bool, ProtocolError>,
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
            if !advertised(tx, worker)? {
                return Ok(false);
            }
        }
        if progress.exhausted {
            return Ok(true);
        }
    }
}
