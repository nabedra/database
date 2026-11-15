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
    storage::{HttpStore, Namespace, Storage},
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
    pub(super) ready: Arc<BootstrapReady>,
    pub(super) storage: Arc<Storage<HttpStore>>,
    pub(super) cpu: WorkerPool,
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
    operation_transport: Arc<Semaphore>,
    partition_transport: Arc<Semaphore>,
    control_transport: Arc<Semaphore>,
    accepting: AtomicBool,
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
    pub(super) async fn root(&self) -> Result<crate::cluster::state::Control> {
        http::load_root(&self.storage, &self.cpu).await
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
            let (returned, checked) = cpu(&self.cpu, move |_| {
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
    async fn dispatch(
        self: Arc<Self>,
        request: Request<Body>,
        transport: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<Response<Body>> {
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
        let a = http::one(request.headers(), "x-meshdb-auth")?.to_owned();
        let signature = http::one(request.headers(), "x-meshdb-signature")?.to_owned();
        let dependency = if request.headers().contains_key("x-meshdb-dependency") {
            Some(http::one(request.headers(), "x-meshdb-dependency")?.to_owned())
        } else {
            None
        };
        let signed = cpu(&self.cpu, move |_| {
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
        let (view, content) = cpu(&self.cpu, move |_| {
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
                let pin = self.pin;
                cpu(&self.cpu, move |_| {
                    authority
                        .check(&root, &pin)
                        .map(|_| ())
                        .map_err(|_| unavailable())
                })
                .await?;
                let Reply::Accepted(result) = lease
                    .command(Command::ExecuteCaptured(self.storage.clone()))
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
                        drop(accepted.response);
                        accepted.owner.finish().await.map_err(|_| unavailable())?;
                        return Err(http::failure(ErrorCode::Overloaded));
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
        cpu(&cpu_pool, move |_| {
            prepare_bootstrap(&path, deadline.into_std())
                .and_then(|p| p.connect())
                .map_err(|_| invalid())
        })
        .await?,
    );
    let input = &ready.prepared.input.value;
    let crate::cluster::configuration::Storage::Http(options) = &input.template.storage else {
        return Err(invalid());
    };
    let storage = Arc::new(Storage::new(
        ready.backend.client.clone(),
        Namespace::new(&options.namespace).map_err(|_| invalid())?,
    ));
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
        serde_json::json!({"bound_url":format!("{}://{}",if ready.prepared.tls.is_some(){"https"}else{"http"},listener.local_addr().map_err(|_|unavailable())?)})
    );
    let enrollment=serde_json::to_vec(&serde_json::json!({"version":1,"cluster_id":input.cluster_id,"node_id":input.node_id,"request_id":input.request_id,"incarnation":incarnation,"session_id":session,"public_key":http::hex(&key.public_key()),"endpoint":input.advertise_url,"operation_slots":input.template.operation_slots,"partition_slots":input.template.partition_slots,"queue_size":input.template.queue_size})).map_err(|_|invalid())?;
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
    let local = LocalAdmission::new(
        input.cluster_id,
        &key,
        sender,
        initialization,
        input.template.operation_slots as usize,
        input.template.partition_slots as usize,
        input.template.queue_size as usize,
    )
    .map_err(|_| invalid())?;
    let receiver = Arc::new(Mutex::new(ReceiverState::distributed(local)));
    let boot = receiver.lock().unwrap().local().unwrap().boot();
    let (partition_pool, partition_threads) = WorkerPool::start(
        NonZeroUsize::new(input.template.partition_slots as usize).ok_or_else(invalid)?,
        tokio::runtime::Handle::current(),
    )
    .map_err(|_| unavailable())?;
    let owners = Arc::new(PayloadOwners::from_receiver(
        partition_pool,
        receiver.clone(),
    ));
    let scratch = std::env::temp_dir().join(format!("meshdb-worker-{incarnation}"));
    let work=async {
        let path=scratch.clone();cpu(&cpu_pool,move |_|std::fs::create_dir(&path).map_err(|_|unavailable())).await?;
        let mut process=owners.start_boot(&boot,initialization,scratch.clone(),SpoolLimits::new(2*1024*1024*1024,220_000).map_err(|_|invalid())?).map_err(|_|unavailable())?;
        let process_ready=process.process_ready().await.map_err(|_|unavailable())?;
        let configured=super::initialization::InitializedWorker::validate(boot,initialization,ready.clone(),storage.clone(),&cpu_pool).await?;
        receiver.lock().unwrap().complete_initialization(process_ready,configured).map_err(|_|invalid())?;
        let tls=crate::runtime::retained_tls(ready.prepared.tls.as_ref())?;
        let worker=Arc::new(Worker{ready,storage,cpu:cpu_pool.clone(),signer,client,owners:owners.clone(),receiver,pin,process:Arc::new(process),operations:Executor::new(16,0)?,partitions:Executor::new(16,0)?,controls:Executor::new(8,0)?,sending:Executor::new(16,0)?,operation_transport:Arc::new(Semaphore::new(16)),partition_transport:Arc::new(Semaphore::new(16)),control_transport:Arc::new(Semaphore::new(8)),accepting:AtomicBool::new(true)});
        let root=worker.root().await?;
        let node=root.nodes.iter().find(|n|Some(n.node_id)==worker.signer.sender().node_id()).ok_or_else(unavailable)?;
        let mut writer=HeartbeatWriter::new(crate::cluster::runtime::heartbeat_binding(&root,node)?).map_err(|_|invalid())?;
        let (stop,mut stopped)=tokio::sync::watch::channel(false);
        let heartbeat_interval=Duration::from_millis(root.timing.heartbeat_ms);
        let owner=worker.clone();
        let heartbeat=tokio::spawn(async move {
            loop {
                owner.owners.reap();
                let sample={let state=owner.receiver.lock().unwrap();let snap=state.snapshot_locked();let local=snap.local.ok_or_else(unavailable)?;let (ro,qo,rp,qp)=local.occupancy();HeartbeatSample{running_operations:ro as u64,queued_operations:qo as u64,running_partitions:rp as u64,queued_partitions:qp as u64,drain_id:local.drain_id(),accepting:owner.accepting.load(Ordering::Acquire)&&local.drain_id().is_none()}};
                let storage=owner.storage.clone();
                let returned=cpu(&owner.cpu,move |runtime|{let outcome=runtime.block_on(writer.publish(&storage,sample,Instant::now()+Duration::from_secs(5)));Ok((writer,outcome))}).await?;
                writer=returned.0;
                if matches!(returned.1,Ok(HeartbeatWriteResult::Exhausted)){owner.accepting.store(false,Ordering::Release);}
                tokio::select!{_=stopped.changed()=>break,_=tokio::time::sleep(heartbeat_interval)=>{}}
            }
            Ok::<(),ProtocolError>(())
        });
        let mut app=Router::new();for path in ["operations","jobs","readiness","dependencies/open","dependencies/close","drain"]{app=app.route(&format!("/internal/v1/{path}"),post(internal));}
        let app=app.route("/health/live",get(||async{"true"})).with_state(worker.clone());
        let served=crate::runtime::serve_router(listener,app,tls,async{crate::runtime::shutdown_signal().await;worker.accepting.store(false,Ordering::Release);worker.operations.stop_accepting();worker.partitions.stop_accepting();worker.controls.stop_accepting();}).await.map_err(|_|unavailable());
        worker.operations.drain().await;worker.partitions.drain().await;worker.controls.drain().await;worker.sending.drain().await;
        stop.send_replace(true);heartbeat.await.map_err(|_|unavailable())??;
        drop(worker);
        served
    }.await;
    let drained = owners.close_and_drain().await.map_err(|_| {
        eprintln!("meshdb-server: payload shutdown failed");
        unavailable()
    });
    let joined = partition_threads.shutdown().await.map_err(|_| {
        eprintln!("meshdb-server: partition shutdown failed");
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
                eprintln!("meshdb-server: scratch cleanup failed");
                unavailable()
            })
    })
    .await;
    if work.is_err() {
        eprintln!("meshdb-server: worker service failed");
    }
    work.and(drained).and(joined).and(removed)
}
