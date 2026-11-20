//! The worker's view of `data.direct.v1` and the `direct_oltp` switch
//! (plan Decisions "Rollout", section 4 "Mixed versions", addendum
//! 2026-11-19).
//!
//! - `is_direct_active()`: Control activated the capability. It never turns
//!   off. While it is on, every eligible request that goes queued must first
//!   commit `DirectForward` at its anchor (T6), even with `direct_oltp`
//!   false, and incremental backups are refused.
//! - `direct_oltp()`: the process configuration (default `true`). `false`
//!   keeps the capability but routes eligible operations queued.
//! - `routes_direct()`: both, so the gateway may send eligible operations to
//!   the range leader (T6) and the leader may propose `Direct` (T5).
use crate::{
    oltp::mvcc::planner::{ResolveFuture, direct_fence::DirectActivationProbe},
    oltp_replication::{
        authority::control::{
            ControlClient, DIRECT_VERSION, DirectCapability, DirectCapabilityOperation,
        },
        types::WorkerIdentity,
    },
    protocol::ProtocolError,
};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{sync::watch, task::JoinHandle};

/// Poll period while the capability is inactive.
const POLL: Duration = Duration::from_secs(5);
/// Every this many polls an inactive worker asks Control to re-evaluate
/// activation (a member that never advertised may have left).
const REEVALUATE_EVERY: u32 = 12;

type OnActive = Box<dyn Fn() + Send + Sync>;

pub(crate) struct DirectSwitch {
    control: Arc<ControlClient>,
    local: WorkerIdentity,
    direct_oltp: bool,
    active: AtomicBool,
    on_active: Mutex<Vec<OnActive>>,
    stop: watch::Sender<bool>,
    watcher: Mutex<Option<JoinHandle<()>>>,
}

impl DirectSwitch {
    pub(crate) fn new(
        control: Arc<ControlClient>,
        local: WorkerIdentity,
        direct_oltp: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            control,
            local,
            direct_oltp,
            active: AtomicBool::new(false),
            on_active: Mutex::new(Vec::new()),
            stop: watch::channel(false).0,
            watcher: Mutex::new(None),
        })
    }

    /// Control activated `data.direct.v1` (as last observed; never reverts).
    pub(crate) fn is_direct_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }
    /// The `direct_oltp` configuration of this process.
    #[allow(dead_code)] // Read by the gateway (T6) and leader executor (T5).
    pub(crate) fn direct_oltp(&self) -> bool {
        self.direct_oltp
    }
    /// Eligible operations may take the direct path.
    #[allow(dead_code)] // Read by the gateway (T6) and leader executor (T5).
    pub(crate) fn routes_direct(&self) -> bool {
        self.direct_oltp && self.is_direct_active()
    }
    /// An eligible operation that goes queued must first commit
    /// `DirectForward` at its anchor (plan addendum), whatever `direct_oltp`
    /// says, because another gateway may have sent the same request id
    /// directly.
    #[allow(dead_code)] // Read by the gateway (T6).
    pub(crate) fn requires_forward(&self) -> bool {
        self.is_direct_active()
    }

    /// Runs `hook` once the capability is active (immediately if it is).
    pub(crate) fn on_active(&self, hook: impl Fn() + Send + Sync + 'static) {
        let mut hooks = self.on_active.lock().unwrap_or_else(|e| e.into_inner());
        if self.is_direct_active() {
            drop(hooks);
            hook();
        } else {
            hooks.push(Box::new(hook));
        }
    }

    fn observe(&self, capability: DirectCapability) -> bool {
        if capability.version != DIRECT_VERSION || !capability.active() {
            return false;
        }
        if !self.active.swap(true, Ordering::AcqRel) {
            let hooks =
                std::mem::take(&mut *self.on_active.lock().unwrap_or_else(|e| e.into_inner()));
            for hook in hooks {
                hook();
            }
        }
        true
    }

    /// A strong Control read. `false` also when Control cannot answer the
    /// request (an older Control leader rejects it).
    pub(crate) async fn refresh(&self) -> Result<bool, ProtocolError> {
        if self.is_direct_active() {
            return Ok(true);
        }
        let capability = self.control.direct_capability().await?;
        Ok(self.observe(capability))
    }

    async fn change(
        &self,
        action: DirectCapabilityOperation,
    ) -> Result<DirectCapability, ProtocolError> {
        // A fresh operation each time: a replayed receipt would return a
        // stale activation state.
        let _ = self.local;
        self.control
            .change_direct_capability(uuid::Uuid::new_v4(), action)
            .await
    }

    /// Advertises this worker's support, then watches for activation. The
    /// task ends once the capability is active or `stop` is called. Errors
    /// (an older Control, a leader change) are retried every `POLL`.
    pub(crate) fn start(self: &Arc<Self>) {
        let switch = Arc::downgrade(self);
        let mut stop = self.stop.subscribe();
        let task = tokio::spawn(async move {
            let mut advertised = false;
            let mut polls = 0u32;
            loop {
                let Some(switch) = switch.upgrade() else {
                    return;
                };
                let result = if !advertised {
                    switch
                        .change(DirectCapabilityOperation::Advertise {
                            version: DIRECT_VERSION,
                        })
                        .await
                } else if polls % REEVALUATE_EVERY == REEVALUATE_EVERY - 1 {
                    switch.change(DirectCapabilityOperation::Activate).await
                } else {
                    switch.control.direct_capability().await
                };
                if let Ok(capability) = result {
                    advertised = capability.advertised;
                    if switch.observe(capability) {
                        return;
                    }
                }
                polls = polls.wrapping_add(1);
                drop(switch);
                tokio::select! {
                    _ = tokio::time::sleep(POLL) => {}
                    _ = stop.changed() => return,
                }
            }
        });
        *self.watcher.lock().unwrap_or_else(|e| e.into_inner()) = Some(task);
    }

    /// Stops the watcher and waits for it.
    pub(crate) async fn stop(&self) {
        let _ = self.stop.send(true);
        let task = self
            .watcher
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }
}

impl Drop for DirectSwitch {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}

impl DirectActivationProbe for DirectSwitch {
    fn active(&self) -> ResolveFuture<'_, bool> {
        Box::pin(self.refresh())
    }
}
