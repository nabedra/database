//! Manual worker process and authenticated HTTP dispatch over the retained receiver.
use super::{
    admission::{
        self, AcceptedRoute, ControlResult, DurableRequestView, LocalAdmission, PreparedRoute,
        ReceiverState,
    },
    auth::{self, Route},
    http::{self, Result, cpu, invalid, unavailable},
    payload_owner::{Command, DeliveryHold, PayloadLease, PayloadOwners, Reply, Role},
    spool::SpoolLimits,
};
use crate::{
    cluster::{
        heartbeat::{HeartbeatSample, HeartbeatWriteResult, HeartbeatWriter},
        startup_material::composition::{BootstrapReady, prepare_bootstrap},
    },
    executor::Executor,
    protocol::{ErrorCode, ProtocolError},
    storage::{Namespace, RuntimeStore, Storage},
    worker_pool::WorkerPool,
};
use axum::{
    Router,
    body::Body,
    extract::State,
    http::{Request, Response},
    routing::{get, post},
};
use std::{
    num::NonZeroUsize,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{sync::Semaphore, time::Instant};
use uuid::Uuid;

pub(super) struct Worker {
    #[cfg(test)]
    test_job_barrier: Mutex<Option<(Arc<Semaphore>, Arc<Semaphore>)>>,
    #[cfg(test)]
    pub(super) test_terminal_replication_barrier: Mutex<Option<(Arc<Semaphore>, Arc<Semaphore>)>>,
    initialization: Uuid,
    pub(super) ready: Arc<BootstrapReady>,
    pub(super) storage: Arc<Storage<RuntimeStore>>,
    pub(super) registry: crate::storage::registry::DatabaseRegistry,
    pub(super) cpu: WorkerPool,
    pub(super) control_cpu: WorkerPool,
    pub(super) control_budget: crate::scheduling::ResourceScheduler,
    pub(super) progress_budget: crate::scheduling::ResourceScheduler,
    pub(super) coordination_slots: usize,
    pub(super) signer: Arc<http::Signer>,
    pub(super) client: http::HttpClient,
    pub(super) owners: Arc<PayloadOwners>,
    pub(super) receiver: Arc<Mutex<ReceiverState>>,
    pub(super) pin: [u8; 32],
    process: Arc<PayloadLease>,
    operations: Executor,
    partitions: Executor,
    controls: Executor,
    sending: Executor,
    pub(super) operation_transport: Arc<Semaphore>,
    pub(super) queue: super::queue_runtime::QueueRuntime,
    pub(super) remote_scheduler: super::coordinator::RemoteScheduler,
    partition_transport: Arc<Semaphore>,
    pub(super) control_transport: Arc<Semaphore>,
    accepting: AtomicBool,
}
/// A grant owns its registry and physical scheduler reservation through storage clones.
#[derive(Clone)]
pub(super) struct DataGrant {
    storage: Arc<Storage<RuntimeStore>>,
    _handle: Option<crate::storage::registry::DatabaseHandle>,
}
impl DataGrant {
    pub(super) fn storage(&self) -> Arc<Storage<RuntimeStore>> {
        self.storage.clone()
    }
}
struct ResponseRetention {
    body: Body,
    _hold: DeliveryHold,
    _owner: RetainedOwner,
}
enum RetainedOwner {
    Operation {
        _account: PayloadLease,
        _stream: PayloadLease,
    },
    Control {
        _owner: super::payload_owner::ControlLease,
    },
    Drain {
        _owner: super::payload_owner::ControlLease,
        _proof: admission::DrainObservation,
    },
}
impl hyper::body::Body for ResponseRetention {
    type Data = bytes::Bytes;
    type Error = axum::Error;
    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<std::result::Result<hyper::body::Frame<Self::Data>, Self::Error>>>
    {
        std::pin::Pin::new(&mut self.body).poll_frame(cx)
    }
    fn size_hint(&self) -> hyper::body::SizeHint {
        self.body.size_hint()
    }
}
impl Worker {
    pub(super) async fn open_database(
        &self,
        context: crate::storage::context::RequestContext,
    ) -> Result<DataGrant> {
        if self.ready.prepared.input.value.template.mode.is_fixed() {
            return Err(http::failure(ErrorCode::Unauthorized));
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| unavailable())?
            .as_secs();
        context
            .validate(now)
            .map_err(|_| http::failure(ErrorCode::Unauthorized))?;
        let handle = self
            .registry
            .open_accounted(context, now, Some(&self.cpu.scheduler()))
            .await
            .map_err(|e| match e {
                crate::storage::StorageError::LimitExceeded => http::failure(ErrorCode::Overloaded),
                _ => http::failure(ErrorCode::Unauthorized),
            })?;
        Ok(DataGrant {
            storage: handle.storage().clone(),
            _handle: Some(handle),
        })
    }
    pub(super) async fn data_for_record(
        &self,
        record: &crate::request_queue::RequestRecord,
    ) -> Result<DataGrant> {
        match (
            &record.context,
            self.ready.prepared.input.value.template.mode.is_fixed(),
        ) {
            (Some(context), false) => self.open_database(context.clone()).await,
            (None, true) => Ok(DataGrant {
                storage: self.storage.clone(),
                _handle: None,
            }),
            _ => Err(http::failure(ErrorCode::Unauthorized)),
        }
    }
    pub(super) async fn root(&self) -> Result<crate::cluster::state::Control> {
        http::load_root(&self.storage, &self.control_cpu).await
    }
    async fn start(&self, subject: admission::StartSubject) -> Result<admission::Binding> {
        let binding = subject.binding().map_err(|_| unavailable())?;
        self.start_until(subject, binding, Instant::now() + Duration::from_secs(30))
            .await
    }
    async fn start_until(
        &self,
        mut subject: admission::StartSubject,
        binding: admission::Binding,
        deadline: Instant,
    ) -> Result<admission::Binding> {
        loop {
            self.owners.reap();
            let root = self.root().await?;
            let pin = self.pin;
            let (returned, checked) = cpu(&self.control_cpu, move |_| {
                let checked = subject.check(&root, &pin).map_err(|_| unavailable())?;
                Ok((subject, checked))
            })
            .await?;
            subject = returned;
            match self.receiver.lock().unwrap().start_locked(checked) {
                Ok(_ticket) => return Ok(binding),
                Err(admission::AcceptError::Admission(admission::Refusal::Full))
                    if Instant::now() < deadline => {}
                Err(_) => return Err(unavailable()),
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    pub(super) async fn dispatch(
        self: Arc<Self>,
        mut request: Request<Body>,
        transport: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<Response<Body>> {
        let request_key = request
            .extensions_mut()
            .remove::<crate::request_queue::RequestKey>();
        let deadline = Instant::now() + Duration::from_secs(30);
        let path = request.uri().path().to_owned();
        if request.uri().query().is_some() {
            return Err(invalid());
        }
        let job = path == "/internal/v1/jobs";
        let length = http::framing(
            request.headers(),
            if path == "/internal/v1/operations" {
                crate::protocol::MAX_BODY_BYTES
            } else {
                16 * 1024
            },
            job,
        )?;
        if http::one(request.headers(), "content-type")?
            != if job {
                "application/octet-stream"
            } else {
                "application/json"
            }
        {
            return Err(invalid());
        }
        let a = http::one(request.headers(), "x-nabedra-auth")?.to_owned();
        let signature = http::one(request.headers(), "x-nabedra-signature")?.to_owned();
        let dependency = if request.headers().contains_key("x-nabedra-dependency") {
            Some(http::one(request.headers(), "x-nabedra-dependency")?.to_owned())
        } else {
            None
        };
        let signed = cpu(&self.control_cpu, move |_| {
            let actual = auth::ActualRoute::new("POST", &path, dependency.as_deref())
                .map_err(|_| invalid())?;
            auth::verify_request(&a, &signature, &actual)
                .map_err(|_| http::failure(ErrorCode::Unauthorized))
        })
        .await?;
        let response_auth = signed.auth().clone();
        let mut reader = None;
        let bytes = if job {
            let mut framed = http::framed_reader(request.into_body());
            let first = tokio::time::timeout_at(deadline, framed.next_frame())
                .await
                .map_err(|_| unavailable())?
                .map_err(|_| invalid())?
                .ok_or_else(invalid)?;
            if first.kind != super::frame::FrameKind::Control {
                return Err(invalid());
            }
            reader = Some(framed);
            first.payload
        } else {
            http::read_small(request.into_body(), length.ok_or_else(invalid)?, deadline).await?
        };
        let root = self.root().await?;
        let boot = self
            .receiver
            .lock()
            .unwrap()
            .local()
            .ok_or_else(unavailable)?
            .boot();
        let pin = self.pin;
        let (view, content) = cpu(&self.control_cpu, move |_| {
            let content = auth::check_request_content(&signed, bytes).map_err(|_| invalid())?;
            let eligible = auth::check_request_authority(&signed, &root, &boot, &pin)
                .map_err(|_| http::failure(ErrorCode::Unauthorized))?;
            let view =
                DurableRequestView::prepare(&root, eligible, boot).map_err(|_| unavailable())?;
            Ok((view, content))
        })
        .await?;
        let permit = content.dependency().map(|d| d.permit_id());
        let mut prepared = match response_auth.route() {
            Route::Operations => PreparedRoute::Operation(
                self.owners
                    .prepare_pair(Role::Operation, transport, Some(deadline))
                    .map_err(|_| http::failure(ErrorCode::Overloaded))?,
                admission::OperationCandidates::new(),
            ),
            Route::Jobs => PreparedRoute::Job(
                self.owners
                    .prepare_pair(Role::Dependency, transport, Some(deadline))
                    .map_err(|_| http::failure(ErrorCode::Overloaded))?,
            ),
            _ => PreparedRoute::Control(
                self.owners
                    .prepare_control(transport)
                    .map_err(|_| http::failure(ErrorCode::Overloaded))?,
            ),
        };
        let accepted = self
            .owners
            .accept(view, content, &mut prepared, &self.process)
            .map_err(|_| http::failure(ErrorCode::Overloaded))?;
        drop(prepared);
        match accepted {
            AcceptedRoute::Operation(work) => {
                if response_auth.sender().role() != auth::Role::Worker
                    || response_auth.sender().node_id() != self.signer.sender().node_id()
                {
                    return Err(http::failure(ErrorCode::Unauthorized));
                }
                let request_id = response_auth.operation_id().ok_or_else(invalid)?;
                let key = request_key.ok_or_else(invalid)?;
                if key.request_id != request_id {
                    return Err(invalid());
                }
                let record = self.queue.record(key.clone())?;
                let data = self.queue_data_for_record(&record).await?;
                let expected = self.queue.checked_execution(
                    key.clone(),
                    response_auth.request_digest().ok_or_else(invalid)?,
                )?;
                let root = self.root().await?;
                let catalog = data
                    .storage()
                    .load_cluster_catalog()
                    .await
                    .map_err(|_| unavailable())?;
                if !root.template.mode.is_fixed()
                    && catalog.catalog().owner_cluster != Some(root.cluster_id)
                {
                    return Err(unavailable());
                }
                if root.scheduler.coordinator_node != self.signer.sender().node_id()
                    || root.scheduler.scheduler_term != expected.scheduler_term
                    || catalog.catalog().fence != Some(expected)
                {
                    return Err(unavailable());
                }
                let authority = work.start.clone();
                let binding = self.start(work.start).await?;
                let mut account = work.owners.account.ok_or_else(unavailable)?;
                account.ready().await.map_err(|_| unavailable())?;
                let payload = account.take_request().map_err(|_| unavailable())?;
                let auth::RequestBody::Operation(operation) = payload.body else {
                    return Err(invalid());
                };
                let id = operation.request_id;
                let mut stream = work.owners.stream;
                stream.ready().await.map_err(|_| unavailable())?;
                let result = super::coordinator::execute(
                    &self,
                    key,
                    data,
                    binding,
                    authority,
                    *operation,
                    payload.bytes,
                    &mut stream,
                )
                .await;
                let encoded = match result {
                    Ok(v) => v,
                    Err(e) => crate::api::response::encode_error(Some(id), e),
                };
                let body = Body::new(ResponseRetention {
                    body: Body::from(encoded.body.clone()),
                    _hold: payload.delivery,
                    _owner: RetainedOwner::Operation {
                        _account: account,
                        _stream: stream,
                    },
                });
                self.signer
                    .response(&response_auth, encoded.status, &encoded.body, body, false)
            }
            AcceptedRoute::Job(work) => {
                let authority = work.start.clone();
                let _binding = self.start(work.start).await?;
                let mut account = work.owners.account;
                if let Some(account) = &mut account {
                    account.ready().await.map_err(|_| unavailable())?;
                }
                let accepted = super::payload_stream::receive_checked_request(
                    reader.ok_or_else(invalid)?,
                    work.owners.stream,
                )
                .await
                .map_err(|_| invalid())?;
                let job = accepted.envelope;
                let mut lease = accepted.lease;
                let root = self.root().await?;
                let record = self.queue.record_for_job(&job)?;
                let data = self.queue_data_for_record(&record).await?;
                let catalog = data
                    .storage()
                    .load_cluster_catalog()
                    .await
                    .map_err(|_| unavailable())?;
                if !root.template.mode.is_fixed()
                    && catalog.catalog().owner_cluster != Some(root.cluster_id)
                {
                    return Err(unavailable());
                }
                let fence = catalog.catalog().fence.ok_or_else(unavailable)?;
                if root.scheduler.scheduler_term != fence.scheduler_term
                    || root.scheduler.coordinator_node != response_auth.sender().node_id()
                {
                    return Err(unavailable());
                }
                self.queue.validate_job(&job, fence)?;
                let pin = self.pin;
                cpu(&self.control_cpu, move |_| {
                    authority
                        .check(&root, &pin)
                        .map(|_| ())
                        .map_err(|_| unavailable())
                })
                .await?;
                #[cfg(test)]
                {
                    let barrier = self.test_job_barrier.lock().unwrap().clone();
                    if let Some((arrived, release)) = barrier {
                        arrived.add_permits(1);
                        release.acquire().await.map_err(|_| unavailable())?.forget();
                    }
                }
                let Reply::Accepted(result) = lease
                    .command(Command::ExecuteCaptured(data.storage()))
                    .await
                    .map_err(|_| unavailable())?
                else {
                    return Err(invalid());
                };
                let control = super::job::encode_result(&result, &job).map_err(|_| invalid())?;
                let (writer, reader) = tokio::io::duplex(64 * 1024);
                let sending = self.sending.try_admit()?;
                let task = sending.spawn(move || async move {
                    let sent = super::payload_stream::send_result(
                        super::frame::FrameWriter::new(Box::new(writer)),
                        lease,
                        job,
                    )
                    .await;
                    drop(account);
                    sent.map(|_| ()).map_err(|_| unavailable())
                })?;
                // The executor and payload registry retain the actual task through settlement.
                drop(task);
                let body = Body::from_stream(tokio_util::io::ReaderStream::new(reader));
                self.signer
                    .response(&response_auth, 200, &control, body, true)
            }
            AcceptedRoute::Readiness(mut accepted)
            | AcceptedRoute::Open(mut accepted)
            | AcceptedRoute::Close(mut accepted)
            | AcceptedRoute::Drain(mut accepted) => {
                accepted.owner.ready().await.map_err(|_| unavailable())?;
                let value = match accepted.result {
                    ControlResult::Readiness(challenge) => {
                        serde_json::json!({"version":1,"challenge":challenge,"initialized":true})
                    }
                    ControlResult::Open(_) => {
                        serde_json::json!({"version":1,"permit_id":permit.ok_or_else(invalid)?})
                    }
                    ControlResult::Close(closed) => {
                        serde_json::json!({"version":1,"permit_id":permit.ok_or_else(invalid)?,"closed":closed==admission::Close::Closed})
                    }
                    ControlResult::Drain(_) => {
                        self.owners.reap();
                        let root = self.root().await?;
                        let boot = self
                            .receiver
                            .lock()
                            .unwrap()
                            .local()
                            .ok_or_else(unavailable)?
                            .boot();
                        let prepared = admission::BarrierPreparation::prepare(&root, boot)
                            .map_err(|_| unavailable())?;
                        let sample = self
                            .receiver
                            .lock()
                            .unwrap()
                            .sample_barrier(prepared, self.initialization, accepted.owner.key())
                            .map_err(|_| unavailable())?;
                        let ticket = self
                            .owners
                            .publish_proof(sample, self.storage.clone(), deadline)
                            .map_err(|_| unavailable())?;
                        self.owners
                            .wait_proof(&ticket)
                            .await
                            .map_err(|_| unavailable())?;
                        let proof = self
                            .owners
                            .take_proof(&ticket, &accepted.owner.key())
                            .ok_or_else(unavailable)?;
                        drop(ticket);
                        let observation =
                            self.owners.finish_drain(proof).map_err(|_| unavailable())?;
                        let bytes = auth::DrainReply::from_observation(&observation)
                            .encode()
                            .map_err(|_| invalid())?;
                        let body = Body::new(ResponseRetention {
                            body: Body::from(bytes.clone()),
                            _hold: accepted.response,
                            _owner: RetainedOwner::Drain {
                                _owner: accepted.owner,
                                _proof: observation,
                            },
                        });
                        return self
                            .signer
                            .response(&response_auth, 200, &bytes, body, false);
                    }
                };
                let bytes = serde_json::to_vec(&value).map_err(|_| invalid())?;
                let body = Body::new(ResponseRetention {
                    body: Body::from(bytes.clone()),
                    _hold: accepted.response,
                    _owner: RetainedOwner::Control {
                        _owner: accepted.owner,
                    },
                });
                self.signer
                    .response(&response_auth, 200, &bytes, body, false)
            }
        }
    }
}
async fn internal(State(worker): State<Arc<Worker>>, request: Request<Body>) -> Response<Body> {
    let response = async {
        if !worker.accepting.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        let (executor, transport) = match request.uri().path() {
            "/internal/v1/operations" => (&worker.operations, &worker.operation_transport),
            "/internal/v1/jobs" => (&worker.partitions, &worker.partition_transport),
            _ => (&worker.controls, &worker.control_transport),
        };
        let admission = executor.try_admit()?;
        let transport = transport
            .clone()
            .try_acquire_owned()
            .map_err(|_| http::failure(ErrorCode::Overloaded))?;
        admission
            .spawn(move || worker.dispatch(request, transport))?
            .receive()
            .await
    }
    .await;
    response.unwrap_or_else(|error| {
        let encoded = crate::api::response::encode_error(None, error);
        Response::builder()
            .status(encoded.status)
            .header("content-type", "application/json")
            .header("connection", "close")
            .body(Body::from(encoded.body))
            .unwrap()
    })
}
pub async fn run_worker(path: PathBuf) -> std::result::Result<(), &'static str> {
    let (cpu, threads) = WorkerPool::start(
        NonZeroUsize::new(2).unwrap(),
        tokio::runtime::Handle::current(),
    )
    .map_err(|_| "worker initialization failed")?;
    let result = worker_process(path, cpu)
        .await
        .map_err(|_| "worker runtime failed");
    let joined = threads
        .shutdown()
        .await
        .map_err(|_| "worker shutdown failed");
    result.and(joined)
}
async fn worker_process(path: PathBuf, cpu_pool: WorkerPool) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(30);
    let ready = Arc::new(
        cpu(&cpu_pool, move |runtime| {
            prepare_bootstrap(&path, deadline.into_std())
                .and_then(|p| runtime.block_on(p.connect_async()))
                .map_err(|_| invalid())
        })
        .await?,
    );
    let input = &ready.prepared.input.value;
    let namespace = match &input.template.storage {
        crate::cluster::configuration::Storage::Http(s) => &s.namespace,
        crate::cluster::configuration::Storage::S3(s) => &s.namespace,
    };
    let storage = Arc::new(Storage::new(
        ready.backend.client.clone(),
        Namespace::new(namespace).map_err(|_| invalid())?,
    ));
    let control_key = match &input.template.storage {
        crate::cluster::configuration::Storage::S3(options) => {
            #[cfg(feature = "s3")]
            {
                let RuntimeStore::S3(client) = &ready.backend.client else {
                    return Err(invalid());
                };
                Some(
                    crate::storage::context::StorageKey::new(
                        &options.bucket,
                        client.region(),
                        &options.namespace,
                    )
                    .map_err(|_| invalid())?,
                )
            }
            #[cfg(not(feature = "s3"))]
            {
                let _ = options;
                return Err(invalid());
            }
        }
        _ => None,
    };
    let registry = crate::storage::registry::DatabaseRegistry::new(
        (input.template.execution.max_pending_requests as usize)
            .checked_add(super::queue_runtime::coordination_capacity(
                &input.template,
            )?)
            .and_then(|n| n.checked_add(16))
            .ok_or_else(invalid)?,
        input.template.worker_resources().working_bytes() as usize,
        control_key,
    )
    .map_err(|_| invalid())?;
    let client = http::HttpClient::new(ready.prepared.tls.as_ref())?;
    let key = Arc::new(auth::WorkerSigningKey::new().map_err(|_| invalid())?);
    let incarnation = Uuid::new_v4();
    let session = Uuid::new_v4();
    let initialization = Uuid::new_v4();
    let sender = Arc::new(
        auth::Sender::worker(
            input.node_id,
            incarnation,
            session,
            http::hex(&key.public_key()),
        )
        .map_err(|_| invalid())?,
    );
    let signer = Arc::new(http::Signer::worker(
        input.cluster_id,
        (*sender).clone(),
        key.clone(),
    )?);
    let listener = tokio::net::TcpListener::bind(&input.template.listen)
        .await
        .map_err(|_| unavailable())?;
    println!(
        "{}",
        serde_json::json!({"bound_url":format!("{}://{}",if ready.prepared.tls.is_some(){"https"}else{"http"},listener.local_addr().map_err(|_|unavailable())?),"resources":input.template.worker_resources(),"coordination_slots":super::queue_runtime::coordination_capacity(&input.template)?,"job_coordination_slots":super::queue_runtime::job_coordination_capacity(&input.template)?,"data_connection_slots":super::queue_runtime::job_coordination_capacity(&input.template)?,"queue_ingress_connection_slots":16,"queue_receipt_connection_slots":8,"queue_replica_connection_slots":8,"queue_bootstrap_connection_slots":4,"control_connection_slots":8,"header_connection_slots":24,"outgoing_stream_slots":super::queue_runtime::outgoing_capacity(&input.template)?,"control_threads":2,"network_threads":2,"blocking_threads":4,"control_transport_slots":8})
    );
    let coordination_slots = super::queue_runtime::coordination_capacity(&input.template)?;
    let enrollment=serde_json::to_vec(&serde_json::json!({"version":1,"cluster_id":input.cluster_id,"node_id":input.node_id,"request_id":input.request_id,"incarnation":incarnation,"session_id":session,"public_key":http::hex(&key.public_key()),"endpoint":input.advertise_url,"operation_slots":coordination_slots,"partition_slots":super::queue_runtime::job_coordination_capacity(&input.template)?,"queue_size":input.template.queue_size,"resources":input.template.worker_resources()})).map_err(|_|invalid())?;
    let unsigned = auth::EnrollmentUnsigned::decode(&enrollment).map_err(|_| invalid())?;
    let signature = key.sign_enrollment(&unsigned).map_err(|_| invalid())?;
    let enrollment = auth::Enrollment::signed(unsigned, http::hex(&signature))
        .map_err(|_| invalid())?
        .encode()
        .map_err(|_| invalid())?;
    let mut pin = None;
    while Instant::now() < deadline && pin.is_none() {
        for endpoint in &input.template.api_urls {
            let request = Request::builder()
                .method("POST")
                .uri(format!(
                    "{}/internal/v1/enroll",
                    endpoint.trim_end_matches('/')
                ))
                .header("connection", "close")
                .header("content-type", "application/json")
                .header("content-length", enrollment.len())
                .header(
                    "authorization",
                    format!("Bearer {}", input.enrollment_token),
                )
                .body(Body::from(enrollment.clone()))
                .map_err(|_| invalid())?;
            let attempt = async {
                let response = client.request(request, deadline).await?;
                if response.status() != 200 {
                    return Err(unavailable());
                }
                let length =
                    http::framing(response.headers(), 16 * 1024, false)?.ok_or_else(invalid)?;
                let bytes = http::read_small(response.into_body(), length, deadline).await?;
                let reply = auth::EnrollmentReply::decode(&bytes).map_err(|_| invalid())?;
                if reply.cluster_id() != input.cluster_id
                    || reply.node_id() != input.node_id
                    || reply.incarnation() != incarnation
                    || reply.session_id() != session
                {
                    return Err(invalid());
                }
                reply.api_public_key().map_err(|_| invalid())
            }
            .await;
            if let Ok(value) = attempt {
                pin = Some(value);
                break;
            }
        }
        if pin.is_none() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    let pin = pin.ok_or_else(unavailable)?;
    let partition_slots = super::queue_runtime::job_coordination_capacity(&input.template)?;
    let local = LocalAdmission::new_with_job_slots(
        input.cluster_id,
        &key,
        sender,
        initialization,
        coordination_slots,
        partition_slots,
        input.template.queue_size as usize,
    )
    .map_err(|_| invalid())?;
    let receiver = Arc::new(Mutex::new(ReceiverState::distributed(local)));
    let boot = receiver.lock().unwrap().local().unwrap().boot();
    let (partition_pool, partition_threads) = WorkerPool::start_with_resources(
        input.template.worker_resources(),
        tokio::runtime::Handle::current(),
    )
    .map_err(|_| unavailable())?;
    let control_limit = input
        .template
        .worker_resources()
        .control_memory_reserve_bytes;
    if control_limit < 64 * 1024 {
        return Err(invalid());
    }
    let control_budget = crate::scheduling::ResourceScheduler::new(vec![(
        0,
        crate::config::WorkerResources {
            max_threads: 2,
            memory_limit_bytes: control_limit,
            task_memory_limit_bytes: control_limit - 1,
            control_memory_reserve_bytes: 1,
        },
    )])?;
    let progress_bytes = super::queue_runtime::progress_bytes(&input.template);
    let _progress_charge = control_budget.charge_memory(0, progress_bytes)?;
    let progress_budget = crate::scheduling::ResourceScheduler::new(vec![(
        0,
        crate::config::WorkerResources {
            max_threads: 2,
            memory_limit_bytes: progress_bytes,
            task_memory_limit_bytes: progress_bytes - 1,
            control_memory_reserve_bytes: 1,
        },
    )])?;
    let _coordination_charge = control_budget.charge_memory(
        0,
        super::queue_runtime::coordination_bytes(&input.template)?,
    )?;
    let queue_charge_bytes = input
        .template
        .execution
        .max_queue_bytes_per_replica
        .checked_add(
            input
                .template
                .execution
                .max_retained_result_bytes_per_worker,
        )
        .ok_or_else(invalid)?;
    let _queue_charge = partition_pool
        .scheduler()
        .charge_memory(0, queue_charge_bytes)?;
    let owners = Arc::new(PayloadOwners::from_receiver_with_streams(
        partition_pool.clone(),
        receiver.clone(),
        super::queue_runtime::outgoing_capacity(&input.template)?,
    ));
    let scratch = std::env::temp_dir().join(format!("nabedra-worker-{incarnation}"));
    let work=async {
        let path=scratch.clone();cpu(&cpu_pool,move |_|std::fs::create_dir(&path).map_err(|_|unavailable())).await?;
        let mut process=owners.start_boot(&boot,initialization,scratch.clone(),SpoolLimits::new(2*1024*1024*1024,220_000).map_err(|_|invalid())?).map_err(|_|unavailable())?;
        let process_ready=process.process_ready().await.map_err(|_|unavailable())?;
        let configured=super::initialization::InitializedWorker::validate(boot,initialization,ready.clone(),storage.clone(),&cpu_pool).await?;
        receiver.lock().unwrap().complete_initialization(process_ready,configured).map_err(|_|invalid())?;
        let tls=crate::runtime::retained_tls(ready.prepared.tls.as_ref())?;
        let worker=Arc::new(Worker{#[cfg(test)] test_job_barrier:Mutex::new(None),#[cfg(test)] test_terminal_replication_barrier:Mutex::new(None),ready,storage,registry,cpu:partition_pool.clone(),control_cpu:cpu_pool.clone(),control_budget,progress_budget,coordination_slots,signer,client,initialization,owners:owners.clone(),receiver,pin,process:Arc::new(process),operations:Executor::new(coordination_slots,0)?,partitions:Executor::new(partition_slots,0)?,controls:Executor::new(8,0)?,sending:Executor::new(partition_slots,0)?,operation_transport:Arc::new(Semaphore::new(coordination_slots)),partition_transport:Arc::new(Semaphore::new(partition_slots)),control_transport:Arc::new(Semaphore::new(8)),accepting:AtomicBool::new(true),queue:super::queue_runtime::QueueRuntime::new(),remote_scheduler:super::coordinator::RemoteScheduler::new()});
        worker.initialize_queue().await?;
        let root=worker.root().await?;
        let node=root.nodes.iter().find(|n|Some(n.node_id)==worker.signer.sender().node_id()).ok_or_else(unavailable)?;
        let writer=worker.owners.heartbeat_writer();
        *writer.lock().await=Some(HeartbeatWriter::new(crate::cluster::runtime::heartbeat_binding(&root,node)?).map_err(|_|invalid())?);
        let (stop,mut stopped)=tokio::sync::watch::channel(false);
        let queue_owner=worker.clone();let queue_task=tokio::spawn(queue_owner.queue_pump(stop.subscribe()));
        let heartbeat_interval=Duration::from_millis(root.timing.heartbeat_ms);
        let owner=worker.clone();
        let heartbeat=tokio::spawn(async move {
            loop {
                owner.owners.reap();
                let sample={let (_,used_threads,used_bytes)=owner.cpu.scheduler().usage()[0];let state=owner.receiver.lock().unwrap();let snap=state.snapshot_locked();let local=snap.local.ok_or_else(unavailable)?;let (ro,qo,rp,qp)=local.occupancy();HeartbeatSample{available_threads:Some(owner.cpu.resources().max_threads.saturating_sub(used_threads)),available_memory_bytes:Some(owner.cpu.resources().working_bytes().saturating_sub(used_bytes)),running_operations:ro as u64,queued_operations:qo as u64,running_partitions:rp as u64,queued_partitions:qp as u64,drain_id:local.drain_id(),accepting:owner.accepting.load(Ordering::Acquire)&&local.drain_id().is_none()}};
                let outcome=writer.lock().await.as_mut().ok_or_else(unavailable)?.publish(&owner.storage,sample,Instant::now()+Duration::from_secs(5)).await;
                if matches!(outcome,Ok(HeartbeatWriteResult::Exhausted)){owner.accepting.store(false,Ordering::Release);}
                tokio::select!{_=stopped.changed()=>break,_=tokio::time::sleep(heartbeat_interval)=>{}}
            }
            Ok::<(),ProtocolError>(())
        });
        let mut app=Router::new();for path in ["operations","jobs","readiness","dependencies/open","dependencies/close","drain"]{app=app.route(&format!("/internal/v1/{path}"),post(internal));}
        let app=app.route(super::queue_transport::PATH,post(super::queue_runtime::handler)).route("/health/live",get(||async{"true"})).with_state(worker.clone());
        let served=crate::runtime::serve_router_with_budget(listener,app,tls,partition_slots,async{crate::runtime::shutdown_signal().await;worker.accepting.store(false,Ordering::Release);worker.operations.stop_accepting();worker.partitions.stop_accepting();worker.controls.stop_accepting();}).await.map_err(|_|unavailable());
        worker.operations.drain().await;worker.partitions.drain().await;worker.controls.drain().await;worker.sending.drain().await;
        stop.send_replace(true);queue_task.await.map_err(|_|unavailable())??;heartbeat.await.map_err(|_|unavailable())??;
        drop(worker);
        served
    }.await;
    let drained = owners.close_and_drain().await.map_err(|_| {
        eprintln!("nabedra-db: payload shutdown failed");
        unavailable()
    });
    let joined = partition_threads.shutdown().await.map_err(|_| {
        eprintln!("nabedra-db: partition shutdown failed");
        unavailable()
    });
    let removed = cpu(&cpu_pool, move |_| {
        std::fs::remove_dir(scratch)
            .or_else(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    Ok(())
                } else {
                    Err(e)
                }
            })
            .map_err(|_| {
                eprintln!("nabedra-db: scratch cleanup failed");
                unavailable()
            })
    })
    .await;
    if work.is_err() {
        eprintln!("nabedra-db: worker service failed");
    }
    work.and(drained).and(joined).and(removed)
}

#[cfg(test)]
mod shared_tests;
