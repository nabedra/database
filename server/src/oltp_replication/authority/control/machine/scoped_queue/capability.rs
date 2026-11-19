use super::*;

const CAPABILITY: &[u8] = b"ct/scoped-queue-capability";

#[derive(serde::Deserialize, Serialize)]
pub(in crate::oltp_replication::authority::control) struct Admission {
    configuration_id: Uuid,
    run_id: Uuid,
    enrollment_revision: u64,
    enabled: bool,
}

impl Admission {
    pub(in crate::oltp_replication::authority::control) fn key() -> &'static [u8] {
        CAPABILITY
    }

    pub(in crate::oltp_replication::authority::control) fn capability(
        admission: Option<Self>,
        state: &State,
    ) -> ScopedQueueCapability {
        let run_id = state.ready.view.run_id;
        let enrollment_revision = state.enrollment_revision;
        ScopedQueueCapability {
            version: SCOPED_QUEUE_VERSION,
            run_id,
            enrollment_revision,
            configuration_id: admission.as_ref().map(|record| record.configuration_id),
            activation: admission
                .filter(|record| {
                    record.enabled
                        && record.run_id == run_id
                        && record.enrollment_revision == enrollment_revision
                })
                .map(|record| record.configuration_id),
        }
    }
}

pub(in crate::oltp_replication::authority::control) fn reader_key(
    worker: WorkerIdentity,
) -> Vec<u8> {
    key(b"ct/scoped-queue-reader/", worker.worker_id)
}

pub(super) fn supports(
    tx: &mut GroupTransaction<'_>,
    worker: WorkerIdentity,
) -> Result<bool, ProtocolError> {
    Ok(load::<WorkerIdentity>(tx, &reader_key(worker))? == Some(worker))
}

pub(super) fn is_api(
    tx: &mut GroupTransaction<'_>,
    actor: WorkerIdentity,
) -> Result<bool, ProtocolError> {
    Ok(load::<ApiEnrollment>(tx, &key(APIS, actor.worker_id))?
        .is_some_and(|api| WorkerIdentity::from(api.transport()) == actor))
}

pub(super) fn advert(
    tx: &mut GroupTransaction<'_>,
    actor: WorkerIdentity,
    version: u32,
) -> Result<ControlReply, ProtocolError> {
    if version != SCOPED_QUEUE_VERSION
        || !is_api(tx, actor)?
            && !load::<WorkerRecord>(tx, &key(WORKERS, actor.worker_id))?
                .is_some_and(|worker| WorkerIdentity::from(&worker.enrollment) == actor)
    {
        return fault(Fault::Unauthorized);
    }
    save(tx, &reader_key(actor), &actor)?;
    Ok(ControlReply::ScopedQueueUpdated)
}

pub(super) fn set(
    tx: &mut GroupTransaction<'_>,
    state: &State,
    id: Uuid,
    expected: ScopedQueueCapability,
    enabled: bool,
) -> Result<ControlReply, ProtocolError> {
    let current = Admission::capability(load(tx, CAPABILITY)?, state);
    if current != expected {
        return fault(Fault::Stale);
    }
    if enabled
        && (state.pending.is_some()
            || state.restart.is_some()
            || state.ready.ready_count == 0
            || state.invalid_ready != 0
            || !readers_are_compatible(tx, WORKERS, |entry: WorkerRecord| {
                WorkerIdentity::from(&entry.enrollment)
            })?
            || !readers_are_compatible(tx, APIS, |entry: ApiEnrollment| {
                WorkerIdentity::from(entry.transport())
            })?)
    {
        return fault(Fault::NotReady);
    }
    save(
        tx,
        CAPABILITY,
        &Admission {
            configuration_id: id,
            run_id: current.run_id,
            enrollment_revision: current.enrollment_revision,
            enabled,
        },
    )?;
    Ok(ControlReply::ScopedQueueUpdated)
}

fn readers_are_compatible<T: serde::de::DeserializeOwned>(
    tx: &mut GroupTransaction<'_>,
    prefix: &[u8],
    identity: impl Fn(T) -> WorkerIdentity,
) -> Result<bool, ProtocolError> {
    let mut cursor = None;
    loop {
        let mut readers = Vec::with_capacity(16);
        let progress = tx.scan_page(prefix, cursor.as_deref(), 16, |key, raw| {
            let worker = identity(decode(raw)?);
            if worker.validate().is_err()
                || key.get(prefix.len()..) != Some(worker.worker_id.as_bytes())
            {
                return Err(unknown());
            }
            readers.push(worker);
            Ok(())
        })?;
        for worker in readers {
            cursor = Some(key(prefix, worker.worker_id));
            if !supports(tx, worker)? {
                return Ok(false);
            }
        }
        if progress.exhausted {
            return Ok(true);
        }
    }
}
