use super::{VerifiedNeverAccepted, WorkerRuntime, unavailable, unknown};
use crate::{
    oltp::pool::{ByteClass, ScratchLease, WorkerSharedResources},
    oltp_replication::{
        authority::status::{IdentityLookup, MutationOutcome, VerifiedAdmissionRejection},
        types::Digest,
    },
    protocol::ProtocolError,
    request_queue::QueueError,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tokio::{
    sync::{Mutex as AsyncMutex, Notify, OwnedMutexGuard, mpsc, watch},
    task::JoinHandle,
};
use uuid::Uuid;

#[cfg(test)]
mod tests;

#[derive(Default)]
struct HeldGates {
    ids: Mutex<BTreeMap<Uuid, Digest>>,
    changed: Notify,
}

pub(super) struct PendingRejection {
    pub(super) cap: VerifiedNeverAccepted,
    _memory: ScratchLease,
    held: Arc<HeldGates>,
}

impl Drop for PendingRejection {
    fn drop(&mut self) {
        if let Ok(mut ids) = self.held.ids.lock() {
            ids.remove(&self.cap.reservation.request_id);
        }
        self.held.changed.notify_waiters();
    }
}

pub(super) struct AdmissionCleanup {
    held: Arc<HeldGates>,
    queue: mpsc::Sender<PendingRejection>,
    stop: watch::Sender<bool>,
    task: Mutex<Option<JoinHandle<()>>>,
    #[cfg(test)]
    pause: Arc<std::sync::OnceLock<tests::CleanupPause>>,
}

impl AdmissionCleanup {
    pub(super) fn new(
        capacity: usize,
    ) -> (
        Self,
        mpsc::Receiver<PendingRejection>,
        watch::Receiver<bool>,
    ) {
        let (queue, receiver) = mpsc::channel(capacity);
        let (stop, stopped) = watch::channel(false);
        (
            Self {
                held: Arc::new(HeldGates::default()),
                queue,
                stop,
                task: Mutex::new(None),
                #[cfg(test)]
                pause: Arc::new(std::sync::OnceLock::new()),
            },
            receiver,
            stopped,
        )
    }

    pub(super) fn start(
        &self,
        runtime: &Arc<WorkerRuntime>,
        mut queue: mpsc::Receiver<PendingRejection>,
        mut stopped: watch::Receiver<bool>,
    ) -> Result<(), ProtocolError> {
        let runtime = Arc::downgrade(runtime);
        #[cfg(test)]
        let pause = self.pause.clone();
        *self.task.lock().map_err(|_| unknown())? = Some(nabedra_rt::task::spawn(async move {
            tokio::select! {
                biased;
                _ = stopped.wait_for(|stop| *stop) => {},
                _ = async {
                    while let Some(mut pending) = queue.recv().await {
                        #[cfg(test)]
                        {
                            let id = pending.cap.reservation.request_id;
                            if let Some(pause) = pause.get().filter(|pause| pause.id == id) {
                                pause.entered.add_permits(1);
                                pause.resume.acquire().await.unwrap().forget();
                            }
                            super::super::recovery_tests::pause("admission_cleanup", id).await;
                        }
                        retry_never_accepted(&runtime, &mut pending.cap).await;
                    }
                } => {},
            }
        }));
        Ok(())
    }

    pub(super) fn stop(&self) {
        self.stop.send_replace(true);
    }

    pub(super) async fn join(&self) -> Result<(), ProtocolError> {
        let task = self.task.lock().map_err(|_| unknown())?.take();
        if let Some(task) = task {
            task.await.map_err(|_| unknown())?;
        }
        Ok(())
    }

    pub(super) async fn lock(
        &self,
        gate: Arc<AsyncMutex<()>>,
        id: Uuid,
        digest: Digest,
    ) -> Result<OwnedMutexGuard<()>, ProtocolError> {
        let lock = gate.lock_owned();
        tokio::pin!(lock);
        loop {
            let changed = self.held.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(held) = self.held.ids.lock().map_err(|_| unknown())?.get(&id) {
                return Err(if *held == digest {
                    unknown()
                } else {
                    QueueError::Conflict.protocol()
                });
            }
            tokio::select! {
                guard = &mut lock => return Ok(guard),
                _ = &mut changed => {},
            }
        }
    }

    pub(super) fn reserve(
        &self,
        shared: &WorkerSharedResources,
    ) -> Result<(mpsc::OwnedPermit<PendingRejection>, ScratchLease), ProtocolError> {
        // Reserve before creating a pin or sending a rejection. The active
        // reconciler can hold one additional, still charged, item.
        let slot = self
            .queue
            .clone()
            .try_reserve_owned()
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => QueueError::Overloaded.protocol(),
                mpsc::error::TrySendError::Closed(_) => unavailable(),
            })?;
        // One bounded Status failure message (4 KiB), its binding, guard and
        // queue bookkeeping. Live RPC tickets charge their own representation.
        let memory = shared.reserve_scratch(ByteClass::Data, 8 << 10)?;
        Ok((slot, memory))
    }

    pub(super) fn hold(
        &self,
        (slot, memory): (mpsc::OwnedPermit<PendingRejection>, ScratchLease),
        cap: VerifiedNeverAccepted,
    ) -> Result<(mpsc::OwnedPermit<PendingRejection>, PendingRejection), ProtocolError> {
        self.held
            .ids
            .lock()
            .map_err(|_| unknown())?
            .insert(cap.reservation.request_id, cap.reservation.digest);
        self.held.changed.notify_waiters();
        Ok((
            slot,
            PendingRejection {
                cap,
                _memory: memory,
                held: self.held.clone(),
            },
        ))
    }
}

async fn retry_never_accepted(runtime: &Weak<WorkerRuntime>, cap: &mut VerifiedNeverAccepted) {
    loop {
        let Some(runtime) = runtime.upgrade() else {
            return;
        };
        if runtime.reconcile_admission_rejection(cap).await.is_ok() {
            return;
        }
        drop(runtime);
        nabedra_rt::time::sleep(Duration::from_millis(100)).await;
    }
}

impl WorkerRuntime {
    pub(super) async fn settle_never_accepted(
        &self,
        cap: &VerifiedNeverAccepted,
    ) -> Result<(), ProtocolError> {
        let rejection = self.services.status.reject_unaccepted(cap).await?;
        #[cfg(test)]
        if super::super::recovery_tests::lose_admission_rejection(cap.reservation.request_id).await
        {
            return Err(unknown());
        }
        self.cancel_rejected_admission(cap, rejection).await
    }

    async fn cancel_rejected_admission(
        &self,
        _cap: &VerifiedNeverAccepted,
        rejection: VerifiedAdmissionRejection,
    ) -> Result<(), ProtocolError> {
        self.services
            .timestamps
            .cancel_unaccepted(rejection)
            .await?;
        #[cfg(test)]
        {
            use super::super::recovery_tests;
            recovery_tests::mark("settled", _cap.reservation.request_id);
            recovery_tests::transaction_checkpoint(
                "admission_rejection_settled",
                _cap.reservation.request_id,
                &(_cap.view, &_cap.identity, _cap.pin, _cap.request),
            )
            .await;
        }
        Ok(())
    }

    async fn reconcile_admission_rejection(
        &self,
        cap: &mut VerifiedNeverAccepted,
    ) -> Result<(), ProtocolError> {
        use super::AuthorityReader;
        let authority = cap.view.authority();
        let state = self.services.control.inspect().await?.ok_or_else(unknown)?;
        // Control publishes a new authority after all Status groups fence the
        // old one. A ready revision by itself does not fence Status proposals.
        if state.ready.view.authority() != authority {
            return Ok(());
        }
        let status = &self.services.status;
        match status
            .lookup_identity(cap.reservation.request_id, Some(cap.reservation.digest))
            .await?
        {
            Some(IdentityLookup::Rejected(actual))
                if actual.reservation == cap.reservation
                    && actual.view.authority() == authority
                    && actual.identity == cap.identity
                    && actual.pin == cap.pin
                    && actual.request == cap.request
                    && actual.failure == MutationOutcome::failure(cap.failure.clone())? =>
            {
                let rejection = status.rejected_identity(&actual).await?;
                return self.cancel_rejected_admission(cap, rejection).await;
            }
            Some(IdentityLookup::Reserved(actual)) if actual == cap.reservation => {}
            _ => return Err(unknown()),
        }

        // A lost reply can still arrive after the lookup. Keep the same gate
        // and all rejection fields. Status may record either revision first.
        let fresh = self.services.control.current(&self.services.scope).await?;
        if fresh.owner != self.services.local
            || fresh.ready.view.authority() != authority
            || self
                .services
                .replica
                .lock()
                .map_err(|_| unknown())?
                .record(cap.reservation.request_id)
                .is_some()
        {
            return Err(unknown());
        }
        if fresh.ready.view != cap.view {
            cap.view = fresh.ready.view;
            cap.live_admission.take();
        }
        self.settle_never_accepted(cap).await
    }
}
