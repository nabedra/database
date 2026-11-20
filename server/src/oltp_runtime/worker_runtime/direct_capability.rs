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
    /// T5's executor and its expiry task, once active (`serve_direct_when_active`).
    executor: Mutex<
        Option<(
            Arc<crate::oltp_runtime::direct::DirectExecutor>,
            JoinHandle<()>,
        )>,
    >,
    /// The Catalog under the `DirectLeader` role, once active: the
    /// executor's schema reads and a worker gateway's topology fills use it,
    /// so neither needs the queue owner's role (T6 concern 3).
    catalog: Mutex<Option<crate::oltp::mvcc::catalog::CatalogHandle>>,
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
            executor: Mutex::new(None),
            catalog: Mutex::new(None),
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
        let executor = self
            .executor
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some((_, expiry)) = executor {
            expiry.abort();
            let _ = expiry.await;
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

/// T3's `ReserveWindow` predicate on a hosted Timestamp replica: true once
/// Control activated `data.direct.v1`, which covers every Timestamp member
/// (every member is an enrolled worker that advertised). A strong read until
/// active; `true` is final.
pub(crate) struct ControlReserveWindowGate {
    control: Arc<ControlClient>,
    active: AtomicBool,
}
impl ControlReserveWindowGate {
    pub(crate) fn new(control: Arc<ControlClient>) -> Arc<Self> {
        Arc::new(Self {
            control,
            active: AtomicBool::new(false),
        })
    }
}
impl crate::oltp_replication::authority::timestamp::DirectTimestampGate
    for ControlReserveWindowGate
{
    fn reserve_window_supported<'a>(
        &'a self,
        _: &'a crate::oltp_replication::types::GroupIdentity,
    ) -> crate::oltp_replication::authority::consensus::ConsensusFuture<'a, bool> {
        Box::pin(async move {
            if self.active.load(Ordering::Acquire) {
                return Ok(true);
            }
            let active = self
                .control
                .direct_capability()
                .await
                .is_ok_and(|capability| capability.active());
            if active {
                self.active.store(true, Ordering::Release);
            }
            Ok(active)
        })
    }
}

/// T5's gate: the leader proposes direct commands only while Control's
/// `data.direct.v1` is active.
impl crate::oltp_runtime::direct::DirectGate for DirectSwitch {
    fn direct_active(&self) -> bool {
        self.is_direct_active()
    }
}

/// What a worker that leads Data ranges needs to serve `Method::Direct`.
pub(crate) struct DirectLeaderServices {
    pub shared: Arc<crate::oltp::pool::WorkerSharedResources>,
    pub transport: crate::oltp_replication::multiplex::WorkerClient,
    pub catalog: crate::oltp_replication::types::GroupIdentity,
    pub timestamps: crate::oltp_replication::types::GroupIdentity,
    /// This worker's own Timestamp replica, if it hosts one.
    pub local_timestamps:
        Option<Arc<dyn crate::oltp_replication::authority::timestamp::DirectTimestampSource>>,
}

impl DirectSwitch {
    /// Once `data.direct.v1` is active, builds T5's executor (a Catalog
    /// schema reader and a coalescing `AllocateDirect` client, both under the
    /// `DirectLeader` role), installs it as this worker's `Method::Direct`
    /// service and starts its expiry task. Errors are logged and leave the
    /// worker on the queued path.
    pub(crate) fn serve_direct_when_active(self: &Arc<Self>, services: DirectLeaderServices) {
        let switch = Arc::downgrade(self);
        self.on_active(move || {
            let Some(switch) = switch.upgrade() else {
                return;
            };
            match switch.build_executor(&services) {
                Ok(()) => {}
                Err(error) => tracing::warn!(
                    code = ?error.code,
                    message = error.message,
                    "direct executor not installed; the worker stays on the queued path"
                ),
            }
        });
    }

    fn build_executor(
        self: &Arc<Self>,
        services: &DirectLeaderServices,
    ) -> Result<(), ProtocolError> {
        use crate::oltp_replication::{
            authority::control::ServiceAction,
            service_rpc::{ParticipantClient, TimestampClient},
        };
        let catalog = ParticipantClient::new(
            services.catalog.clone(),
            services.shared.clone(),
            services.transport.clone(),
            self.control.clone(),
            ServiceAction::DirectLeader,
        )?;
        let catalog = crate::oltp::mvcc::catalog::CatalogHandle::new(Arc::new(
            crate::oltp::mvcc::store::ParticipantHandle::remote(
                services.catalog.clone(),
                services.shared.clone(),
                catalog,
            )?,
        ))?;
        let remote: Arc<dyn crate::oltp_replication::authority::timestamp::DirectTimestampSource> =
            TimestampClient::new(
                services.timestamps.clone(),
                services.shared.clone(),
                services.transport.clone(),
                self.control.clone(),
                ServiceAction::DirectLeader,
            )?;
        let timestamps = crate::oltp_replication::authority::timestamp::DirectTimestampClient::new(
            services.local_timestamps.clone(),
            remote,
        );
        *self.catalog.lock().unwrap_or_else(|e| e.into_inner()) = Some(catalog.clone());
        #[cfg_attr(not(test), allow(unused_mut))]
        let mut config = crate::oltp_runtime::direct::DirectConfig::default();
        // T9 D12: a registered test child can shrink the leader's in-flight budget
        // so a process-level load deterministically exceeds it.
        #[cfg(test)]
        if let Some(limit) = std::env::var("NABEDRA_OLTP_TEST_DIRECT_MAX_IN_FLIGHT")
            .ok()
            .and_then(|value| value.parse().ok())
        {
            config.max_in_flight = limit;
        }
        let executor = crate::oltp_runtime::direct::DirectExecutor::new(
            services.shared.clone(),
            config,
            self.clone(),
            Arc::new(crate::oltp_runtime::direct::CatalogSchema::new(catalog)),
            timestamps,
        );
        executor.install();
        let expiry = executor.spawn_expiry();
        *self.executor.lock().unwrap_or_else(|e| e.into_inner()) = Some((executor, expiry));
        Ok(())
    }

    /// The Catalog under the `DirectLeader` role, once active.
    pub(crate) fn direct_catalog(&self) -> Option<crate::oltp::mvcc::catalog::CatalogHandle> {
        self.catalog
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// The installed executor's `direct` stats, once active.
    pub(crate) fn direct_stats(&self) -> Option<crate::oltp_runtime::direct::DirectStatsSnapshot> {
        self.executor
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|(executor, _)| executor.stats())
    }
}
