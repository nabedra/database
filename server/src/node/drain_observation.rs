//! One registered, boot-bound C03c invocation. No idle or drain receipt authority.
use super::{admission::BarrierSample, payload_owner::ControlKey};
use crate::{
    cluster::heartbeat::{
        AcknowledgedHeartbeat, HeartbeatBinding, HeartbeatWriteResult, HeartbeatWriter,
    },
    storage::{ObjectStore, Storage, StorageError},
};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll},
};
use tokio::{
    sync::{Notify, oneshot},
    task::JoinHandle,
    time::Instant,
};

type Outcome = Result<HeartbeatWriteResult, StorageError>;
#[derive(Debug, PartialEq, Eq)]
pub(super) enum ProofError {
    Busy,
    Exhausted,
    Refused,
    Failed,
}
#[derive(Default)]
pub(super) struct ProofSlot {
    next: Option<u64>,
    used: bool,
    binding: Option<Arc<HeartbeatBinding>>,
    writer: Arc<tokio::sync::Mutex<Option<HeartbeatWriter>>>,
    cycle: Option<Registered>,
    #[cfg(test)]
    pub(super) gates: ProofGates,
    #[cfg(test)]
    pub(super) preparing: Option<(oneshot::Sender<()>, std::sync::mpsc::Receiver<()>)>,
    #[cfg(test)]
    pub(super) spawning: Option<oneshot::Sender<bool>>,
}
#[cfg(test)]
#[derive(Default)]
pub(super) struct ProofGates {
    pub(super) before_io: Option<Arc<crate::test_storage::ReadGate>>,
    pub(super) buffered: Option<Arc<crate::test_storage::ReadGate>>,
    pub(super) returning: Option<Arc<crate::test_storage::ReadGate>>,
    pub(super) panic: bool,
}
#[cfg(test)]
async fn gate(gate: Option<Arc<crate::test_storage::ReadGate>>) {
    if let Some(gate) = gate {
        gate.entered.add_permits(1);
        if let Ok(Ok(permit)) =
            tokio::time::timeout(std::time::Duration::from_secs(10), gate.release.acquire()).await
        {
            permit.forget();
        }
    }
}
struct Registered {
    cycle: Arc<Cycle>,
    handle: Option<JoinHandle<()>>,
    terminated: bool,
    reaped: bool,
    failed: bool,
}
pub(super) struct Cycle {
    id: AtomicU64,
    binding: Arc<HeartbeatBinding>,
    data: Mutex<(Option<BarrierSample>, Option<Outcome>)>,
    // Closed storage retention only; no erased callable or caller executor.
    storage: Mutex<Option<Arc<dyn Send + Sync>>>,
    cancelled: AtomicBool,
    cancellation_done: AtomicBool,
    consumed: AtomicBool,
    retired: AtomicBool,
    wake: Arc<Notify>,
}
pub(super) struct ProofInvocation {
    cycle: Arc<Cycle>,
    writer: Arc<tokio::sync::Mutex<Option<HeartbeatWriter>>>,
    #[cfg(test)]
    gates: ProofGates,
}
pub(super) struct ProofTicket {
    cycle: Arc<Cycle>,
}
impl Drop for ProofTicket {
    fn drop(&mut self) {
        self.cycle.cancel();
    }
}
impl Cycle {
    pub(super) fn release(&self) {
        let (sample, outcome) = {
            let mut data = self.data.lock().unwrap();
            (data.0.take(), data.1.take())
        };
        drop((sample, outcome));
        drop(self.storage.lock().unwrap().take());
    }
    fn cancel(&self) {
        let data = self.data.lock().unwrap();
        if !self.cancelled.load(Ordering::Acquire) {
            self.cancelled.store(true, Ordering::Release);
            drop(data);
            // The registered cycle retains this proof-only handoff until completion.
            self.cancellation_done.store(true, Ordering::Release);
            self.wake.notify_waiters();
        }
    }
}
/// Kept associated with the registry marker until Drop. c3d must validate atomically.
pub(super) struct ReapedProof {
    data: Option<(BarrierSample, Outcome)>,
    cycle: Arc<Cycle>,
}
impl ReapedProof {
    pub(super) fn sample(&self) -> &BarrierSample {
        &self.data.as_ref().expect("live completion").0
    }
    pub(super) fn acknowledgement(&self) -> Option<&AcknowledgedHeartbeat> {
        match &self.data.as_ref().expect("live completion").1 {
            Ok(HeartbeatWriteResult::Acknowledged(ack)) => Some(ack),
            _ => None,
        }
    }
}
impl Drop for ReapedProof {
    fn drop(&mut self) {
        drop(self.data.take());
        // Keep the registry marker until the original memory result is released.
        self.cycle.retired.store(true, Ordering::Release);
        self.cycle.wake.notify_waiters();
    }
}
impl ProofSlot {
    /// Only the original successfully polled handle and live transferred result qualify.
    pub(super) fn matches_reaped(&self, proof: &ReapedProof) -> bool {
        self.cycle.as_ref().is_some_and(|r| {
            Arc::ptr_eq(&r.cycle, &proof.cycle)
                && r.handle.is_some()
                && r.terminated
                && r.reaped
                && !r.failed
                && r.cycle.consumed.load(Ordering::Acquire)
                && !r.cycle.retired.load(Ordering::Acquire)
                && (!r.cycle.cancelled.load(Ordering::Acquire)
                    || r.cycle.cancellation_done.load(Ordering::Acquire))
                && proof.data.is_some()
        })
    }
    pub(super) fn retained(&self) -> usize {
        usize::from(self.cycle.is_some())
    }
    pub(super) fn results(&self) -> usize {
        usize::from(
            self.cycle
                .as_ref()
                .is_some_and(|c| c.cycle.data.lock().unwrap().1.is_some()),
        )
    }
    pub(super) fn handles(&self) -> usize {
        usize::from(self.cycle.as_ref().is_some_and(|c| c.handle.is_some()))
    }
    pub(super) fn failed(&self) -> bool {
        self.cycle.as_ref().is_some_and(|c| c.failed)
    }
    pub(super) fn cancel(&self) {
        if let Some(c) = &self.cycle {
            c.cycle.cancel();
        }
    }
    pub(super) fn prepare<B: ObjectStore + 'static>(
        sample: BarrierSample,
        storage: Arc<Storage<B>>,
        wake: Arc<Notify>,
    ) -> Arc<Cycle> {
        let binding = Arc::new(sample.binding().clone());
        Arc::new(Cycle {
            binding,
            id: AtomicU64::new(0),
            data: Mutex::new((Some(sample), None)),
            storage: Mutex::new(Some(storage)),
            cancelled: AtomicBool::new(false),
            cancellation_done: AtomicBool::new(false),
            consumed: AtomicBool::new(false),
            retired: AtomicBool::new(false),
            wake,
        })
    }
    pub(super) fn sample_matches(cycle: &Cycle, state: &super::admission::ReceiverState) -> bool {
        state.barrier_matches(
            cycle
                .data
                .lock()
                .unwrap()
                .0
                .as_ref()
                .expect("prepared sample"),
        )
    }
    pub(super) fn reserve(&mut self, cycle: Arc<Cycle>) -> Result<ProofTicket, ProofError> {
        if self.cycle.is_some() {
            return Err(ProofError::Busy);
        }
        if self.binding.as_ref().is_some_and(|b| b != &cycle.binding) {
            return Err(ProofError::Refused);
        }
        let id = if self.used {
            self.next.ok_or(ProofError::Exhausted)?
        } else {
            1
        };
        self.used = true;
        self.next = id.checked_add(1);
        self.binding.get_or_insert_with(|| cycle.binding.clone());
        cycle.id.store(id, Ordering::Release);
        self.cycle = Some(Registered {
            cycle: cycle.clone(),
            handle: None,
            terminated: false,
            reaped: false,
            failed: false,
        });
        Ok(ProofTicket { cycle })
    }
    // Synchronous publication immediately after spawn; no user callback or await.
    pub(super) fn publish_handle(&mut self, ticket: &ProofTicket, handle: JoinHandle<()>) {
        let registered = self.cycle.as_mut().expect("reserved proof");
        assert!(Arc::ptr_eq(&registered.cycle, &ticket.cycle));
        assert!(registered.handle.is_none());
        registered.handle = Some(handle);
    }
    pub(super) fn invocation(&mut self, ticket: &ProofTicket) -> ProofInvocation {
        ProofInvocation {
            cycle: ticket.cycle.clone(),
            writer: self.writer.clone(),
            #[cfg(test)]
            gates: std::mem::take(&mut self.gates),
        }
    }
    /// Original handle stays in place, including after panic. Result alone is insufficient.
    pub(super) fn poll(&mut self, cx: &mut Context<'_>) -> Option<Arc<Cycle>> {
        let r = self.cycle.as_mut()?;
        if !r.terminated
            && let Some(handle) = &mut r.handle
            && let Poll::Ready(joined) = Pin::new(handle).poll(cx)
        {
            r.terminated = true;
            let data = r.cycle.data.lock().unwrap();
            // Exact readback, Unknown and even Conflict can follow a deadline-dropped
            // reconciliation read. C03c exposes no settlement certificate for those.
            let definite = matches!(
                &data.1,
                Some(Ok(
                    HeartbeatWriteResult::Acknowledged(_) | HeartbeatWriteResult::Exhausted
                )) | Some(Err(StorageError::InvalidCatalog
                    | StorageError::LimitExceeded
                    | StorageError::Conflict))
            );
            r.failed = joined.is_err() || !definite;
            r.reaped = !r.failed;
        }
        if r.reaped
            && (!r.cycle.cancelled.load(Ordering::Acquire)
                || r.cycle.cancellation_done.load(Ordering::Acquire))
            && (r.cycle.retired.load(Ordering::Acquire)
                || (r.cycle.cancellation_done.load(Ordering::Acquire)
                    && !r.cycle.consumed.load(Ordering::Acquire)))
        {
            return self.cycle.take().map(|r| r.cycle);
        }
        None
    }
    pub(super) fn status(&self, ticket: &ProofTicket) -> Result<bool, ProofError> {
        let Some(r) = &self.cycle else {
            return Err(ProofError::Refused);
        };
        if !Arc::ptr_eq(&r.cycle, &ticket.cycle)
            || r.cycle.id.load(Ordering::Acquire) != ticket.cycle.id.load(Ordering::Acquire)
        {
            return Err(ProofError::Refused);
        }
        if r.failed {
            Err(ProofError::Failed)
        } else {
            Ok(r.reaped)
        }
    }
    pub(super) fn take(&self, ticket: &ProofTicket, observer: &ControlKey) -> Option<ReapedProof> {
        if self.status(ticket) != Ok(true) || ticket.cycle.cancelled.load(Ordering::Acquire) {
            return None;
        }
        let mut data = ticket.cycle.data.lock().unwrap();
        if !data.0.as_ref()?.proof_observer_matches(observer) {
            return None;
        }
        if ticket.cycle.consumed.swap(true, Ordering::AcqRel) {
            return None;
        }
        Some(ReapedProof {
            data: Some((data.0.take()?, data.1.take()?)),
            cycle: ticket.cycle.clone(),
        })
    }
}
impl ProofInvocation {
    pub(super) fn run<B: ObjectStore + 'static>(
        self,
        storage: Arc<Storage<B>>,
        deadline: Instant,
        activation: oneshot::Receiver<()>,
    ) -> impl Future<Output = ()> + Send + 'static {
        let Self {
            cycle,
            writer,
            #[cfg(test)]
            gates,
        } = self;
        async move {
            if activation.await.is_err() {
                return;
            }
            #[cfg(test)]
            gate(gates.before_io).await;
            let (binding, sample) = {
                let data = cycle.data.lock().unwrap();
                let sample = data.0.as_ref().expect("registered sample");
                (sample.binding().clone(), sample.heartbeat().clone())
            };
            let mut retained = writer.lock().await;
            if retained.is_none() {
                match HeartbeatWriter::new(binding) {
                    Ok(writer) => *retained = Some(writer),
                    Err(error) => {
                        cycle.data.lock().unwrap().1 = Some(Err(error));
                        return;
                    }
                }
            }
            let result = retained
                .as_mut()
                .unwrap()
                .publish(&storage, sample, deadline)
                .await;
            cycle.data.lock().unwrap().1 = Some(result);
            drop(retained);
            #[cfg(test)]
            {
                gate(gates.buffered).await;
                assert!(!gates.panic, "proof supervisor test panic");
                gate(gates.returning).await;
            }
        }
    }
}

macro_rules! redacted { ($($ty:ty),*) => { $(impl std::fmt::Debug for $ty { fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str(stringify!($ty)) } })* }; }
redacted!(ProofTicket, ReapedProof, ProofSlot);

#[cfg(test)]
impl ProofSlot {
    pub(super) fn finished(&self) -> bool {
        self.cycle
            .as_ref()
            .is_some_and(|r| r.handle.as_ref().is_some_and(|h| h.is_finished()))
    }
    pub(super) fn quarantine(&mut self) -> bool {
        if self
            .cycle
            .as_ref()
            .is_some_and(|r| r.terminated && r.failed)
        {
            self.cycle.take();
            true
        } else {
            false
        }
    }
    pub(super) fn writer_retained(&self) -> bool {
        self.writer.try_lock().is_ok_and(|w| w.is_some())
    }
    pub(super) fn last_cycle_next(&mut self) {
        self.used = true;
        self.next = Some(u64::MAX);
    }
}
