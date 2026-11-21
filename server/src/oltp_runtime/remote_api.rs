//! A stateless API attaches to enrolled workers over the existing mTLS service.
//! It opens no data pool and cannot create a run or worker readiness.
use super::{
    application::{ApplicationClient, PointRequest},
    http_runtime::{HttpRuntime, OwnedReceipt},
    replica::QueueScope,
    worker_endpoint::AuthorityReader,
};
use crate::{
    config::WorkerResources,
    executor::Executor,
    oltp::pool::{ByteClass, ExecutionClass, SharedResourceConfig, WorkerSharedResources},
    oltp_replication::{
        TlsMaterial,
        authority::control::{ApiEnrollment, ControlClient, ControlRoutes},
        multiplex::{
            AppliedPeerDirectory, EndpointFuture, EndpointRegistry, TransportConfig,
            WorkerTransport,
        },
        types::*,
    },
    protocol::{ErrorCode, Outcome, ProtocolError},
    request_queue::{Receipt, RequestRecord, RequestState},
    response::{self, EncodedResponse},
    scheduling::{MemoryCharge, ResourceScheduler},
};
use serde::Deserialize;
use std::{
    io::Read,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{net::TcpListener, sync::watch, task::JoinHandle};
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RemoteConfig {
    pub cluster_id: Uuid,
    pub control_group_id: Uuid,
    pub enrollment: ApiEnrollment,
    pub worker_tls: PrivateTls,
    pub control_peers: Vec<WorkerEnrollment>,
    #[serde(default)]
    pub scoped_oltp: bool,
    /// `topology.direct_oltp`: route eligible point operations directly once
    /// `data.direct.v1` is active (default `true`).
    #[serde(default = "crate::config::direct_oltp_default")]
    pub direct_oltp: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PrivateTls {
    pub cert_file: PathBuf,
    pub key_file: PathBuf,
    pub ca_file: PathBuf,
}
struct Routes(Vec<Uuid>);
impl ControlRoutes for Routes {
    fn next(&self, after: Option<Uuid>) -> Result<Option<Uuid>, ProtocolError> {
        Ok(self
            .0
            .iter()
            .copied()
            .find(|id| after.is_none_or(|after| *id > after)))
    }
}
pub(crate) struct RemoteRuntime {
    shared: Arc<WorkerSharedResources>,
    control: Arc<ControlClient>,
    client: Arc<ApplicationClient>,
    gateway: Arc<super::application::direct::DirectGateway>,
    scope: QueueScope,
    ingress: Executor,
    accepting: AtomicBool,
    stop: watch::Sender<bool>,
    tasks: Mutex<Option<(JoinHandle<Result<(), ProtocolError>>, JoinHandle<()>)>>,
    /// `data.direct.v1` and `direct_oltp` for the gateway; stopped on shutdown.
    direct: Arc<super::worker_runtime::direct_capability::DirectSwitch>,
}
impl RemoteRuntime {
    pub(crate) async fn open(
        mut config: RemoteConfig,
        parent: &Path,
        database: &str,
        token: String,
        resources: WorkerResources,
    ) -> Result<Arc<Self>, ProtocolError> {
        config.enrollment.validate()?;
        if config.enrollment.database.as_str() != database
            || resources.max_threads == 0
            || resources.max_threads > 256
        {
            return Err(invalid());
        }
        let group = GroupIdentity {
            cluster_id: config.cluster_id,
            database: config.enrollment.database.clone(),
            group_id: config.control_group_id,
            kind: GroupKind::Control,
            group_format_version: GROUP_FORMAT_VERSION,
        };
        group.validate().map_err(|_| invalid())?;
        let mut ids = config
            .control_peers
            .iter()
            .map(|peer| peer.worker_id)
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids.dedup();
        if ids.len() != config.control_peers.len() || ids.is_empty() || ids.len() > 128 {
            return Err(invalid());
        }
        let material = |path: &mut PathBuf, secret: bool| -> Result<Vec<u8>, ProtocolError> {
            if path.is_relative() {
                *path = parent.join(&*path);
            }
            let file = super::api_process::regular(path, secret)?;
            let mut bytes = Vec::new();
            file.take(65537)
                .read_to_end(&mut bytes)
                .map_err(|_| invalid())?;
            if bytes.len() > 65536 {
                return Err(invalid());
            }
            Ok(bytes)
        };
        let tls = TlsMaterial::from_pem(
            &material(&mut config.worker_tls.cert_file, false)?,
            &material(&mut config.worker_tls.key_file, true)?,
            &material(&mut config.worker_tls.ca_file, false)?,
        )
        .map_err(|_| invalid())?;
        let node =
            (config.enrollment.enrollment.worker_id.as_u128() as u64 & ((1u64 << 63) - 1)).max(1);
        let shared = WorkerSharedResources::new(
            node,
            resources,
            SharedResourceConfig {
                query_threads: resources.max_threads as usize,
                ..Default::default()
            },
        )?;
        shared.bind_worker(config.enrollment.enrollment.worker_id)?;
        let directory = AppliedPeerDirectory::from_bootstrap(
            group.clone(),
            config.control_peers,
            shared.clone(),
        )?;
        let registry = Arc::new(EndpointRegistry::new(shared.clone()));
        let transport = Arc::new(WorkerTransport::new(
            config.enrollment.transport(),
            &tls,
            shared.clone(),
            directory.clone(),
            registry,
            TransportConfig::default(),
        )?);
        let listener = TcpListener::bind(config.enrollment.enrollment.endpoint.as_str())
            .await
            .map_err(|_| unavailable())?;
        let control = ControlClient::new(
            transport.client(),
            shared.clone(),
            group.clone(),
            Arc::new(Routes(ids)),
        )?;
        let (stop, mut stopped) = watch::channel(false);
        let owner = transport.clone();
        let task = tokio::spawn(async move {
            owner
                .run(listener, async move {
                    while !*stopped.borrow_and_update() {
                        if stopped.changed().await.is_err() {
                            break;
                        }
                    }
                })
                .await
        });
        let initialize = async {
            directory.refresh(&control).await?;
            let scope = QueueScope {
                cluster_id: group.cluster_id,
                database: group.database.clone(),
            };
            let grant = control
                .service_role(
                    &scope,
                    WorkerIdentity::from(config.enrollment.transport()),
                    crate::oltp_replication::authority::control::ServiceAction::ApiIngress,
                    group.clone(),
                )
                .await?;
            if grant.owner.is_none() {
                return Err(unavailable());
            }
            if config.scoped_oltp {
                let local = WorkerIdentity::from(config.enrollment.transport());
                let operation = super::authority_fence::transition_id(
                    "scoped-queue-api-capability-v2",
                    &local,
                )?;
                control.advertise_scoped_queue(operation).await?;
            }
            let client = ApplicationClient::new(
                shared.clone(),
                transport.client(),
                control.clone(),
                group,
                token,
                config.scoped_oltp,
            )?;
            Ok((scope, client))
        }
        .await;
        let (scope, client) = match initialize {
            Ok(value) => value,
            Err(error) => {
                let _ = stop.send(true);
                let _ = task.await;
                return Err(error);
            }
        };
        let refresh = tokio::spawn(directory.run_refresh(control.clone(), stop.subscribe()));
        // Gateways advertise too: one that cannot settle request ids at the
        // anchor before going queued must keep the capability inactive.
        let direct = super::worker_runtime::direct_capability::DirectSwitch::new(
            control.clone(),
            WorkerIdentity::from(config.enrollment.transport()),
            config.direct_oltp,
        );
        direct.start();
        let gateway = super::application::direct::DirectGateway::new(
            shared.clone(),
            scope.database.clone(),
            direct.clone(),
            Arc::new(super::application::direct::ApiTopology {
                client: client.clone(),
                control: control.clone(),
            }),
            Arc::new(super::application::direct::WireTransport::new(
                shared.clone(),
                transport.client(),
                control.clone(),
                crate::oltp_replication::authority::control::ServiceAction::ApiIngress,
            )),
        );
        Ok(Arc::new(Self {
            shared,
            control,
            client,
            gateway,
            scope,
            ingress: Executor::new(2, 16)?,
            accepting: AtomicBool::new(true),
            stop,
            tasks: Mutex::new(Some((task, refresh))),
            direct,
        }))
    }
    pub(crate) fn cluster_id(&self) -> Uuid {
        self.scope.cluster_id
    }
    pub(crate) fn application(
        self: &Arc<Self>,
        token: String,
        database: String,
        admin: String,
    ) -> Result<axum::Router, ProtocolError> {
        super::http::router_for_admin(self.clone(), token, database, Some(admin))
    }
    pub(crate) fn stop_accepting(&self) {
        self.accepting.store(false, Ordering::Release);
        self.ingress.stop_accepting();
    }
    pub(crate) async fn drain(&self) -> Result<(), ProtocolError> {
        self.stop_accepting();
        self.ingress.drain().await;
        self.direct.stop().await;
        let _ = self.stop.send(true);
        let tasks = self.tasks.lock().map_err(|_| unavailable())?.take();
        if let Some((transport, refresh)) = tasks {
            refresh.await.map_err(|_| unavailable())?;
            transport.await.map_err(|_| unavailable())??;
        }
        self.shared.drain().await;
        Ok(())
    }
    async fn receipt_owned(&self, run: Uuid, id: Uuid) -> Result<OwnedReceipt, ProtocolError> {
        let observed = self.client.observe(run, id).await?;
        let state = observed.metadata.record.state;
        if !state.terminal() {
            return Ok(Receipt {
                request_id: id,
                run_id: run,
                state,
                result: None,
                error: None,
            }
            .into());
        }
        let bytes = self.client.result(observed).await?;
        let shared = self.shared.clone();
        self.shared
            .execute(ExecutionClass::Control, 0, 0, move || {
                let owner = Arc::new(
                    shared.reserve_scratch(
                        ByteClass::Data,
                        crate::oltp::mvcc::json_decode_budget(bytes.as_ref())?
                            .checked_mul(2)
                            .ok_or_else(unavailable)?,
                    )?,
                );
                let mut value: serde_json::Value =
                    serde_json::from_slice(bytes.as_ref()).map_err(|_| unavailable())?;
                let result = value
                    .get_mut("data")
                    .map(serde_json::Value::take)
                    .filter(|v| !v.is_null());
                let error = value
                    .get_mut("error")
                    .map(serde_json::Value::take)
                    .map(serde_json::from_value)
                    .transpose()
                    .map_err(|_| unavailable())?;
                Ok(OwnedReceipt {
                    receipt: Receipt {
                        request_id: id,
                        run_id: run,
                        state,
                        result,
                        error,
                    },
                    _owner: Some(owner),
                })
            })
            .await
    }
}
impl Drop for RemoteRuntime {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}
impl HttpRuntime for RemoteRuntime {
    fn ingress(&self) -> &Executor {
        &self.ingress
    }
    fn direct_gateway(&self) -> Option<Arc<super::application::direct::DirectGateway>> {
        Some(self.gateway.clone())
    }
    fn scheduler(&self) -> ResourceScheduler {
        self.shared.scheduler()
    }
    fn scheduler_node_id(&self) -> u64 {
        self.shared.scheduler_node_id()
    }
    fn submit(
        self: Arc<Self>,
        body: Vec<u8>,
        mut parsing_charge: MemoryCharge,
        point: Option<PointRequest>,
    ) -> EndpointFuture<'static, RequestRecord> {
        Box::pin(async move {
            if !self.accepting.load(Ordering::Acquire) {
                return Err(unavailable());
            }
            // Decoding is complete. Charge both representations during the
            // handoff, then retain only the transport's charged body.
            let point_bytes = point.as_ref().map_or(0, |point| match &point.key {
                crate::protocol::Key::Utf8(key) => key.capacity(),
                crate::protocol::Key::Int64(_) => 0,
            });
            parsing_charge.shrink_to(
                body.capacity()
                    .checked_add(point_bytes)
                    .ok_or_else(unavailable)? as u64,
            )?;
            let transport_body = self.shared.copy_bytes(&body)?;
            drop(body);
            parsing_charge.shrink_to(point_bytes as u64)?;
            let result = self.client.submit_routed(transport_body, point).await;
            drop(parsing_charge);
            result
        })
    }
    fn receipt(&self, run: Uuid, id: Uuid) -> EndpointFuture<'_, OwnedReceipt> {
        Box::pin(self.receipt_owned(run, id))
    }
    fn wait<'a>(&'a self, record: &'a RequestRecord) -> EndpointFuture<'a, EncodedResponse> {
        Box::pin(async move {
            // Observation uses fresh authoritative routing and mTLS. Avoid
            // making a long-running request generate a stream of quorum reads.
            // Start a bounded observation window when execution reaches the
            // receipt phase or when a scoped receipt needs reconciliation.
            let mut interval = std::time::Duration::from_millis(50);
            let scoped = self
                .client
                .has_scoped_receipt(record.run_id, record.request_id);
            let mut copy_deadline = None;
            let mut observation_deadline = None;
            loop {
                let observe = self.client.observe(record.run_id, record.request_id);
                let deadline = copy_deadline.into_iter().chain(observation_deadline).min();
                let observed = match deadline {
                    Some(deadline) => tokio::time::timeout_at(deadline, observe)
                        .await
                        .map_err(|_| unavailable())?,
                    None => observe.await,
                };
                let state = match observed {
                    Ok(observed) => {
                        // A healthy observation ends the transient failure
                        // streak. The original request may still be queued.
                        observation_deadline = None;
                        let metadata = &observed.metadata.record;
                        if metadata.digest != record.digest
                            || metadata.state == RequestState::Reconciling && !scoped
                        {
                            return Err(unavailable());
                        }
                        let state = metadata.state;
                        if state.terminal() {
                            let status = metadata.response_status.ok_or_else(unavailable)?;
                            let result = self.client.result(observed);
                            let body = match copy_deadline.filter(|_| scoped) {
                                Some(deadline) => tokio::time::timeout_at(deadline, result)
                                    .await
                                    .map_err(|_| unavailable())?,
                                None => result.await,
                            };
                            match body {
                                Ok(body) => {
                                    return Ok(EncodedResponse {
                                        status,
                                        body: body.as_ref().to_vec(),
                                    });
                                }
                                Err(error) if scoped && receipt_retryable(&error) => {}
                                Err(error) => return Err(error),
                            }
                        }
                        state
                    }
                    Err(error) if scoped && receipt_retryable(&error) => RequestState::Reconciling,
                    Err(error) if receipt_retryable(&error) => {
                        observation_deadline.get_or_insert_with(|| {
                            tokio::time::Instant::now() + super::RECEIPT_OBSERVATION_TIMEOUT
                        });
                        RequestState::Reconciling
                    }
                    Err(error) => return Err(error),
                };
                let now = tokio::time::Instant::now();
                if copy_deadline
                    .into_iter()
                    .chain(observation_deadline)
                    .any(|end| now >= end)
                {
                    return Err(unavailable());
                }
                if (state == RequestState::Committing
                    || scoped && state == RequestState::Reconciling)
                    && copy_deadline.is_none()
                {
                    let timeout = if scoped {
                        // The worker's copy round is allowed to recover one
                        // slow peer. The read marker can need another quorum.
                        super::scoped_worker::COPY_TIMEOUT
                    } else {
                        super::RECEIPT_OBSERVATION_TIMEOUT
                    };
                    copy_deadline = Some(now + timeout);
                }
                let next_poll = now + interval;
                let wake = copy_deadline
                    .into_iter()
                    .chain(observation_deadline)
                    .fold(next_poll, std::cmp::min);
                tokio::time::sleep_until(wake).await;
                interval = (interval * 2).min(std::time::Duration::from_millis(500));
            }
        })
    }
    fn health(&self) -> EndpointFuture<'_, EncodedResponse> {
        Box::pin(async move {
            if !self.accepting.load(Ordering::Acquire) {
                return Err(unavailable());
            }
            let current = self.control.current(&self.scope).await?;
            Ok(response::EncodedResponse {status:200,body:serde_json::to_vec(&serde_json::json!({"status":"ready","engine":"oltp",
            "topology":"api-gateway","cluster_id":self.scope.cluster_id,"run_id":current.ready.view.run_id,"ready_workers":current.ready.ready_count})).map_err(|_|unavailable())?})
        })
    }
    fn stats(&self) -> EndpointFuture<'_, EncodedResponse> {
        Box::pin(async move {
            let body = self.client.stats().await?;
            let _scratch = self.shared.reserve_scratch(
                ByteClass::Progress,
                crate::oltp::mvcc::json_decode_budget(body.as_ref())?,
            )?;
            let mut stats: serde_json::Value =
                serde_json::from_slice(body.as_ref()).map_err(|_| unavailable())?;
            let worker = stats.as_object_mut().ok_or_else(unavailable)?;
            // The owner worker reports its own gateway and transport. The
            // gateway that served this endpoint's requests is the API's (NS11
            // finding 8): report it and the API's lane refusals in their
            // place, and keep the worker's under `owner`.
            let owner = serde_json::json!({
                "direct_gateway": worker.remove("direct_gateway"),
                "transport": worker.remove("transport"),
            });
            worker.insert(
                "direct_gateway".into(),
                serde_json::to_value(self.gateway.stats()).map_err(|_| unavailable())?,
            );
            worker.insert("transport".into(), self.client.lane_refusals());
            worker.insert("owner".into(), owner);
            // PERF1: this process's own stage timings next to the worker's.
            worker.insert("api_stage_timing".into(), super::stage_timing::snapshot());
            Ok(EncodedResponse {
                status: 200,
                body: serde_json::to_vec(&stats).map_err(|_| unavailable())?,
            })
        })
    }
}
fn invalid() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::ValidationError,
        "invalid enrolled API gateway configuration",
        Outcome::NotApplied,
    )
}
fn receipt_retryable(error: &ProtocolError) -> bool {
    matches!(
        error.code,
        ErrorCode::StorageUnavailable | ErrorCode::OutcomeUnknown | ErrorCode::Overloaded
    )
}
fn unavailable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "original worker authority or result unavailable",
        Outcome::Unknown,
    )
}
