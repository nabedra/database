//! Signed stream traffic, all-ready installation, and sealed join handoff.
use super::{
    Authority, Dependency, Driver, Entry, FENCE_READ_BYTES, Key, Registry, Source, Stored, Version,
    call, check_dependency, install, now_ms, record_bytes,
    state::{MAX_IDENTITIES, MAX_RECORD_BYTES, Recovery, RecoveryView},
    wire::{self, Batch, Capabilities, Error, Format, Opened, Reply, Request, SnapshotOpened},
};
use crate::{
    cluster::{
        member::{Node, NodeState, Registration},
        state::Control,
    },
    engine::stream::MAX_PINNED_QUERY_BYTES,
    node::{
        http,
        queue_runtime::{members, scheduler},
        queue_transport,
        runtime::Worker,
    },
    protocol::{ErrorCode, MAX_PREDICATE_DEPTH, ProtocolError},
    scheduling::{MemoryCharge, ResourceScheduler},
    storage::{LoadedClusterCatalog, RuntimeStore, context::RequestContext},
    worker_pool::WorkerPool,
};
use axum::{
    body::Body,
    extract::State,
    http::{Request as HttpRequest, Response},
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::{sync::OwnedSemaphorePermit, time::Instant};
use uuid::Uuid;

pub(crate) const PATH: &str = "/internal/v1/streams";
const CONTROL_REPLY_BYTES: usize = 16 << 10;
const CONTROL_REQUEST_BYTES: usize = wire::MAX_REQUEST_BYTES + (64 << 10);
const TRANSACTION_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_RECOVERY_KEY_BYTES: usize = 4 << 10;
const RECOVERY_CURSOR_BYTES: u64 = 4 * MAX_RECOVERY_KEY_BYTES as u64;

fn ready_stream_worker(node: &Node) -> bool {
    node.state == NodeState::Ready && node.registration == Registration::Verified
}

fn stream_recovery_view(authority: Authority, root: &Control) -> RecoveryView {
    let mut ready = Sha256::new();
    for node in root.nodes.iter().filter(|node| ready_stream_worker(node)) {
        ready.update(node.node_id.as_bytes());
        ready.update(node.incarnation.unwrap_or_default().as_bytes());
        ready.update(
            node.session
                .as_ref()
                .map_or(Uuid::nil(), |session| session.id)
                .as_bytes(),
        );
    }
    RecoveryView {
        run: authority.run,
        term: authority.term,
        ready: ready.finalize().into(),
    }
}

fn charge_stream_staging(
    control: &ResourceScheduler,
    working: &ResourceScheduler,
    bytes: u64,
) -> Result<MemoryCharge, ProtocolError> {
    control
        .charge_memory(0, bytes)
        .or_else(|_| working.charge_memory(0, bytes))
        .map_err(|_| wire::overloaded())
}

fn predicate_decode_bytes(wire_bytes: usize) -> u64 {
    // A stream record may contain a large manifest. Only the bounded query
    // inside it can retain a raw predicate copy at each decoding level.
    (wire_bytes.min(wire::MAX_REQUEST_BYTES.max(MAX_PINNED_QUERY_BYTES))
        * (MAX_PREDICATE_DEPTH + 8)) as u64
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "command",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum Command {
    StreamRequest {
        request: Request,
        context: Option<RequestContext>,
        delivery: Option<Uuid>,
    },
    StreamInstall {
        entry: Box<Entry>,
    },
    StreamReset {
        run: Uuid,
    },
    StreamPage {
        run: Uuid,
        after: Option<Key>,
    },
    StreamRelease {
        token: Uuid,
        renew: bool,
    },
}
impl Command {
    fn name(&self) -> &'static str {
        match self {
            Self::StreamRequest { .. } => "stream_request",
            Self::StreamInstall { .. } => "stream_install",
            Self::StreamReset { .. } => "stream_reset",
            Self::StreamPage { .. } => "stream_page",
            Self::StreamRelease { .. } => "stream_release",
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum ControlReply {
    Capabilities(Capabilities),
    Opened(Opened),
    SnapshotOpened(SnapshotOpened),
    Eof,
    Closed,
    Error { error: Error },
    Installed,
    Released,
    Page { entry: Option<Box<Entry>> },
}
enum WorkerReply {
    Control(ControlReply),
    Batch(Batch),
}

struct StagedReply {
    reply: ControlReply,
    _charge: MemoryCharge,
}

impl From<Reply> for WorkerReply {
    fn from(reply: Reply) -> Self {
        match reply {
            Reply::Capabilities(value) => Self::Control(ControlReply::Capabilities(value)),
            Reply::Opened(value) => Self::Control(ControlReply::Opened(value)),
            Reply::SnapshotOpened(value) => Self::Control(ControlReply::SnapshotOpened(value)),
            Reply::Batch(batch) => Self::Batch(batch),
            Reply::Eof => Self::Control(ControlReply::Eof),
            Reply::Closed => Self::Control(ControlReply::Closed),
        }
    }
}

pub(crate) async fn remote(
    client: &http::HttpClient,
    signer: &http::Signer,
    root: &Control,
    request: Request,
    context: Option<RequestContext>,
) -> Result<Reply, Error> {
    let node = scheduler(root)?;
    let delivery = matches!(request, Request::Batch { .. }).then(Uuid::new_v4);
    let command = Command::StreamRequest {
        request,
        context,
        delivery,
    };
    let response_cap = if delivery.is_some() {
        wire::MAX_BATCH_BYTES
    } else {
        CONTROL_REPLY_BYTES
    };
    let reply = exchange(client, signer, root, node, &command, response_cap, |_| {
        Ok(())
    })
    .await;
    if let (Err(_), Some(token)) = (&reply, delivery) {
        let _ = tokio::time::timeout(Duration::from_secs(2), release(client, signer, root, token))
            .await;
    }
    let (reply, ()) = reply?;
    match reply {
        WorkerReply::Control(ControlReply::Capabilities(value)) => Ok(Reply::Capabilities(value)),
        WorkerReply::Control(ControlReply::Opened(value)) => Ok(Reply::Opened(value)),
        WorkerReply::Control(ControlReply::SnapshotOpened(value)) => {
            Ok(Reply::SnapshotOpened(value))
        }
        WorkerReply::Control(ControlReply::Eof) => Ok(Reply::Eof),
        WorkerReply::Control(ControlReply::Closed) => Ok(Reply::Closed),
        WorkerReply::Control(ControlReply::Error { error }) => Err(error),
        WorkerReply::Batch(mut batch) if delivery.is_some() => {
            batch.release = delivery;
            Ok(Reply::Batch(batch))
        }
        _ => Err(wire::invalid().into()),
    }
}

pub(crate) async fn release(
    client: &http::HttpClient,
    signer: &http::Signer,
    root: &Control,
    token: Uuid,
) -> Result<(), ProtocolError> {
    settle_delivery(client, signer, root, token, false).await
}

pub(crate) async fn renew(
    client: &http::HttpClient,
    signer: &http::Signer,
    root: &Control,
    token: Uuid,
) -> Result<(), ProtocolError> {
    settle_delivery(client, signer, root, token, true).await
}

async fn settle_delivery(
    client: &http::HttpClient,
    signer: &http::Signer,
    root: &Control,
    token: Uuid,
    renew: bool,
) -> Result<(), ProtocolError> {
    let node = scheduler(root)?;
    let (reply, ()) = exchange(
        client,
        signer,
        root,
        node,
        &Command::StreamRelease { token, renew },
        CONTROL_REPLY_BYTES,
        |_| Ok(()),
    )
    .await?;
    match reply {
        WorkerReply::Control(ControlReply::Released) => Ok(()),
        _ => Err(wire::unavailable()),
    }
}

async fn exchange<T>(
    client: &http::HttpClient,
    signer: &http::Signer,
    root: &Control,
    node: &Node,
    command: &Command,
    response_cap: usize,
    reserve_reply: impl FnOnce(usize) -> Result<T, ProtocolError>,
) -> Result<(WorkerReply, T), ProtocolError> {
    let cap = match command {
        Command::StreamInstall { .. } => MAX_RECORD_BYTES + 4096,
        _ => CONTROL_REQUEST_BYTES,
    };
    let length = queue_transport::encoded_len(command, cap)?;
    let bytes = queue_transport::encode_exact(command, length)?;
    let (bytes, charge) = queue_transport::exchange_accounted(
        client,
        signer,
        root,
        node,
        bytes,
        response_cap.saturating_add(1),
        None,
        reserve_reply,
    )
    .await?;
    let Some(&marker) = bytes.first() else {
        return Err(wire::invalid());
    };
    let reply = match marker {
        0 => {
            WorkerReply::Control(serde_json::from_slice(&bytes[1..]).map_err(|_| wire::invalid())?)
        }
        1 | 2 if bytes.len() > 1 => WorkerReply::Batch(Batch {
            bytes: Bytes::from(bytes).slice(1..),
            format: if marker == 1 {
                Format::Rows
            } else {
                Format::Arrow
            },
            release: None,
        }),
        _ => return Err(wire::invalid()),
    };
    Ok((reply, charge))
}

#[derive(Clone)]
struct WorkerDriver(Arc<Worker>, u64);
impl Driver for WorkerDriver {
    type Store = RuntimeStore;

    fn registry(&self) -> &Arc<Registry> {
        &self.0.queue.streams
    }
    fn pool(&self) -> &WorkerPool {
        &self.0.cpu
    }

    async fn authority(&self) -> Result<Authority, ProtocolError> {
        if self.registry().stopped.load(Ordering::Acquire) {
            return Err(wire::unavailable());
        }
        let (authority, root) = self.0.stream_authority().await?;
        if authority.term != self.1 {
            return Err(wire::unavailable());
        }
        self.0
            .ensure_stream_recovered(stream_recovery_view(authority, &root))
            .await?;
        Ok(authority)
    }

    async fn source(
        &self,
        context: Option<RequestContext>,
    ) -> Result<Source<RuntimeStore>, ProtocolError> {
        let stage = match context {
            Some(context) => {
                let grant = self.0.open_database(context).await?;
                Source::Worker {
                    storage: grant.storage(),
                    database: Some(grant),
                }
            }
            None if self.0.ready.prepared.input.value.template.mode.is_fixed() => Source::Worker {
                storage: self.0.storage.clone(),
                database: None,
            },
            None => return Err(wire::unauthorized()),
        };
        Ok(stage)
    }

    async fn catalog(
        &self,
        source: &Source<RuntimeStore>,
    ) -> Result<LoadedClusterCatalog, ProtocolError> {
        let root = self.0.root().await?;
        if let Source::Worker {
            database: Some(grant),
            ..
        } = source
        {
            self.0.owned_catalog(&root, grant).await
        } else {
            self.0
                .storage
                .load_cluster_catalog()
                .await
                .map_err(|_| wire::unavailable())
        }
    }

    async fn commit(
        &self,
        authority: Authority,
        entry: Entry,
        previous: Option<Version>,
        dependency: Option<Dependency>,
    ) -> Result<Arc<Stored>, Error> {
        let _change = self.0.queue.lock_changes().await;
        let (observed, root) = self.0.stream_authority().await?;
        if observed.run != authority.run {
            return Err(Error::abandoned());
        }
        let view = stream_recovery_view(observed, &root);
        if observed.term != authority.term || !self.registry().recovered(view) {
            return Err(wire::unavailable().into());
        }
        check_dependency(self.registry(), dependency)?;
        let stored = install(self, entry, previous)?;
        let own = self
            .0
            .signer
            .sender()
            .node_id()
            .ok_or_else(wire::unavailable)?;
        let ready = members(&root);
        let install = async {
            for node in root
                .nodes
                .iter()
                .filter(|node| node.node_id != own && ready.contains(&node.node_id))
            {
                self.0.transfer_stream(&root, node, &stored, false).await?;
            }
            Ok::<(), ProtocolError>(())
        };
        tokio::time::timeout(TRANSACTION_TIMEOUT, install)
            .await
            .map_err(|_| wire::unavailable())??;
        let (current, fresh) = self.0.stream_authority().await?;
        if current.run != observed.run {
            return Err(Error::abandoned());
        }
        if stream_recovery_view(current, &fresh) != view {
            return Err(wire::unavailable().into());
        }
        Ok(stored)
    }

    fn may_speculate(&self) -> bool {
        let (_, occupied, bytes) = self.pool().scheduler().usage()[0];
        !self.0.queue.has_pending()
            && occupied < self.pool().resources().max_threads
            && bytes < self.pool().resources().working_bytes() / 2
    }
}

impl Worker {
    pub(in crate::node) async fn recover_streams_before_handoff(
        &self,
        root: &Control,
    ) -> Result<RecoveryView, ProtocolError> {
        let authority = self.stream_fence(root).await?;
        let view = stream_recovery_view(authority, root);
        self.ensure_stream_recovered(view).await?;
        Ok(view)
    }

    /// Called with the ordinary queue sealed, after recovery has completed.
    pub(in crate::node) async fn verify_stream_handoff(
        &self,
        expected: RecoveryView,
    ) -> Result<(), ProtocolError> {
        self.check_stream_view(expected).await?;
        if !self.queue.streams.recovered(expected) {
            return Err(wire::unavailable());
        }
        Ok(())
    }

    async fn ensure_stream_recovered(&self, expected: RecoveryView) -> Result<(), ProtocolError> {
        let registry = &self.queue.streams;
        if registry.recovered(expected) {
            return Ok(());
        }
        tokio::time::timeout(TRANSACTION_TIMEOUT, async {
            let mut progress = registry.recovery.lock().await;
            if registry.recovered(expected) {
                return Ok(());
            }
            let root = self.check_stream_view(expected).await?;
            if progress
                .as_ref()
                .is_none_or(|recovery| recovery.view != expected)
            {
                *progress = None;
                let charge = self
                    .cpu
                    .scheduler()
                    .charge_memory(0, RECOVERY_CURSOR_BYTES)?;
                registry.begin_recovery(expected)?;
                *progress = Some(Recovery::new(expected, charge));
            }
            let recovery = progress.as_mut().unwrap();
            self.repair_stream_replicas(&root, recovery).await?;
            let _change = self.queue.lock_changes().await;
            self.check_stream_view(expected).await?;
            registry.mark_recovered(expected)?;
            *progress = None;
            Ok(())
        })
        .await
        .map_err(|_| wire::unavailable())?
    }

    /// A fenced predecessor can leave a complete record on any surviving
    /// holder. Preserve page progress across request deadlines and restore
    /// all ready copies before admitting stream work in this view.
    async fn repair_stream_replicas(
        &self,
        root: &Control,
        recovery: &mut Recovery,
    ) -> Result<(), ProtocolError> {
        let own = self
            .signer
            .sender()
            .node_id()
            .ok_or_else(wire::unavailable)?;
        let peers = root
            .nodes
            .iter()
            .filter(|node| node.node_id != own && ready_stream_worker(node));
        let run = recovery.view.run;
        if let Some(cursor) = recovery.merge.as_mut() {
            while let Some(node) = peers.clone().nth(cursor.peer) {
                let page = self
                    .fetch_stream_page(root, node, run, cursor.after.as_ref())
                    .await?;
                if let Some((entry, _charge)) = page {
                    cursor.check_page()?;
                    queue_transport::encoded_len(&entry.key, MAX_RECOVERY_KEY_BYTES)?;
                    let key = entry.key.clone();
                    let _change = self.queue.lock_changes().await;
                    self.install_stream_replica(*entry)?;
                    cursor.finish_page(key);
                } else {
                    cursor.finish_peer();
                }
            }
            recovery.merge = None;
        }

        let cursor = &mut recovery.repair;
        while let Some(node) = peers.clone().nth(cursor.peer) {
            if let Some(stored) = self.queue.streams.page(run, cursor.after.as_ref())? {
                queue_transport::encoded_len(&stored.entry.key, MAX_RECOVERY_KEY_BYTES)?;
                self.transfer_stream(root, node, &stored, true).await?;
                cursor.finish_page(stored.entry.key.clone());
            } else {
                cursor.finish_peer();
            }
        }
        Ok(())
    }

    async fn check_stream_view(&self, expected: RecoveryView) -> Result<Control, ProtocolError> {
        let (authority, root) = self.stream_authority().await?;
        if stream_recovery_view(authority, &root) != expected {
            return Err(wire::unavailable());
        }
        Ok(root)
    }

    async fn stream_authority(&self) -> Result<(Authority, Control), ProtocolError> {
        let root = self.root().await?;
        let own = self
            .signer
            .sender()
            .node_id()
            .ok_or_else(wire::unavailable)?;
        if scheduler(&root)?.node_id != own {
            return Err(wire::unavailable());
        }
        let fence = self.stream_fence(&root).await?;
        Ok((fence, root))
    }

    async fn stream_fence(&self, root: &Control) -> Result<Authority, ProtocolError> {
        let _catalog = self.charge_stream_bootstrap(FENCE_READ_BYTES)?;
        let loaded = self
            .storage
            .load_cluster_catalog()
            .await
            .map_err(|_| wire::unavailable())?;
        let fence = loaded.catalog().fence.ok_or_else(wire::unavailable)?;
        if fence.scheduler_term != root.scheduler.scheduler_term {
            return Err(wire::unavailable());
        }
        Ok(Authority {
            run: fence.run_id,
            term: fence.scheduler_term,
        })
    }

    fn charge_stream_bootstrap(&self, bytes: u64) -> Result<MemoryCharge, ProtocolError> {
        charge_stream_staging(&self.control_budget, self.cpu.scheduler(), bytes)
    }

    async fn stream_exchange(
        &self,
        root: &Control,
        node: &Node,
        command: &Command,
        response_cap: usize,
        bootstrap: bool,
    ) -> Result<StagedReply, ProtocolError> {
        let encoded = queue_transport::encoded_len(command, MAX_RECORD_BYTES + 4096)? as u64;
        let reserve = |bytes| {
            if bootstrap {
                self.charge_stream_bootstrap(bytes)
            } else {
                self.cpu
                    .scheduler()
                    .charge_memory(0, bytes)
                    .map_err(|_| wire::overloaded())
            }
        };
        let _request = reserve(encoded + 64 * 1024)?;
        let (reply, charge) = exchange(
            &self.client,
            &self.signer,
            root,
            node,
            command,
            response_cap,
            |length| {
                let decode = match command {
                    Command::StreamPage { .. } => predicate_decode_bytes(length),
                    _ => 0,
                };
                reserve(4 * length as u64 + 4096 + decode)
            },
        )
        .await?;
        match reply {
            WorkerReply::Control(ControlReply::Error {
                error: Error::Protocol(error),
            }) => Err(error),
            WorkerReply::Control(ControlReply::Error { .. }) => Err(wire::unavailable()),
            WorkerReply::Control(reply) => Ok(StagedReply {
                reply,
                _charge: charge,
            }),
            _ => Err(wire::invalid()),
        }
    }

    async fn transfer_stream(
        &self,
        root: &Control,
        node: &Node,
        stored: &Arc<Stored>,
        bootstrap: bool,
    ) -> Result<(), ProtocolError> {
        let command = Command::StreamInstall {
            entry: Box::new(stored.entry.clone()),
        };
        match self
            .stream_exchange(root, node, &command, CONTROL_REPLY_BYTES, bootstrap)
            .await?
            .reply
        {
            ControlReply::Installed => Ok(()),
            _ => Err(wire::invalid()),
        }
    }

    /// The starting worker first loads a complete live-run view from a ready
    /// peer. The scheduler then repeats the copy while sealing admissions,
    /// before publishing the joiner's readiness.
    pub(in crate::node) async fn load_stream_replicas(
        &self,
        root: &Control,
        node: &Node,
        run: Uuid,
    ) -> Result<(), ProtocolError> {
        self.queue.streams.set_run(run, true)?;
        self.merge_stream_pages(root, node, run).await
    }

    async fn merge_stream_pages(
        &self,
        root: &Control,
        node: &Node,
        run: Uuid,
    ) -> Result<(), ProtocolError> {
        let mut after = None;
        for _ in 0..MAX_IDENTITIES {
            let Some((entry, _charge)) = self
                .fetch_stream_page(root, node, run, after.as_ref())
                .await?
            else {
                return Ok(());
            };
            after = Some(entry.key.clone());
            self.install_stream_replica(*entry)?;
        }
        if self
            .fetch_stream_page(root, node, run, after.as_ref())
            .await?
            .is_some()
        {
            return Err(wire::invalid());
        }
        Ok(())
    }

    async fn fetch_stream_page(
        &self,
        root: &Control,
        node: &Node,
        run: Uuid,
        after: Option<&Key>,
    ) -> Result<Option<(Box<Entry>, MemoryCharge)>, ProtocolError> {
        let command = Command::StreamPage {
            run,
            after: after.cloned(),
        };
        let StagedReply { reply, _charge } = self
            .stream_exchange(root, node, &command, MAX_RECORD_BYTES + 4096, true)
            .await?;
        match reply {
            ControlReply::Page { entry: Some(entry) }
                if entry.run == run
                    && entry.version.term <= root.scheduler.scheduler_term
                    && after.is_none_or(|after| after < &entry.key) =>
            {
                Ok(Some((entry, _charge)))
            }
            ControlReply::Page { entry: None } => Ok(None),
            _ => Err(wire::invalid()),
        }
    }

    pub(in crate::node) async fn copy_stream_replicas(
        &self,
        root: &Control,
        node: &Node,
        run: Uuid,
    ) -> Result<(), ProtocolError> {
        let reset = self
            .stream_exchange(
                root,
                node,
                &Command::StreamReset { run },
                CONTROL_REPLY_BYTES,
                true,
            )
            .await;
        match reset {
            Ok(StagedReply {
                reply: ControlReply::Installed,
                ..
            }) => {}
            // Workers without the stream endpoint can still join before a
            // stream identity needs to be transferred to them.
            Err(error)
                if error.code == ErrorCode::NotFound
                    && self.queue.streams.page(run, None)?.is_none() =>
            {
                return Ok(());
            }
            Err(error) => return Err(error),
            _ => return Err(wire::invalid()),
        }
        let mut after = None;
        while let Some(stored) = self.queue.streams.page(run, after.as_ref())? {
            self.transfer_stream(root, node, &stored, true).await?;
            after = Some(stored.entry.key.clone());
        }
        Ok(())
    }

    fn install_stream_replica(&self, entry: Entry) -> Result<(), ProtocolError> {
        let bytes = record_bytes(&entry)?;
        self.queue
            .streams
            .install(entry, None, bytes, self.cpu.scheduler(), now_ms())?;
        Ok(())
    }
}

pub(in crate::node) async fn handler(
    State(worker): State<Arc<Worker>>,
    request: HttpRequest<Body>,
) -> Response<Body> {
    handle(&worker, request).await.unwrap_or_else(|error| {
        let response = crate::api::response::encode_error(None, error);
        Response::builder()
            .status(response.status)
            .header("content-type", "application/json")
            .body(Body::from(response.body))
            .unwrap()
    })
}

async fn handle(
    worker: &Arc<Worker>,
    request: HttpRequest<Body>,
) -> Result<Response<Body>, ProtocolError> {
    let headers = request.headers().clone();
    let root = worker.root().await?;
    let queue_transport::StreamAuthentication {
        nonce,
        sender,
        digest,
        command: expected,
        class,
        stale_term,
    } = queue_transport::preauthenticate_stream(
        &headers,
        &root,
        worker.signer.sender(),
        &worker.pin,
        &mut worker.queue.seen.lock().unwrap(),
    )?;
    let cap = match expected.as_str() {
        "stream_install" => MAX_RECORD_BYTES + 4096,
        "stream_request" => CONTROL_REQUEST_BYTES,
        "stream_reset" | "stream_page" | "stream_release" => CONTROL_REPLY_BYTES,
        _ => return Err(wire::invalid()),
    };
    let transport = worker.queue.transport(&class)?;
    let length = http::framing(&headers, cap, false)?.ok_or_else(wire::invalid)?;
    let reserve = |bytes| match class.as_str() {
        "bootstrap" => worker.charge_stream_bootstrap(bytes),
        "progress" => worker.progress_budget.charge_memory(0, bytes),
        _ => worker.cpu.scheduler().charge_memory(0, bytes),
    };
    let decode = match expected.as_str() {
        "stream_request" | "stream_install" => predicate_decode_bytes(length),
        _ => 0,
    };
    let _ingress =
        reserve(64 * 1024 + length as u64 * 5 + decode).map_err(|_| wire::overloaded())?;
    let bytes = http::read_small(
        request.into_body(),
        length,
        Instant::now() + Duration::from_secs(30),
    )
    .await?;
    if http::digest(&bytes) != digest {
        return Err(wire::invalid());
    }
    let command = http::cpu(&worker.control_cpu, move |_| {
        serde_json::from_slice::<Command>(&bytes).map_err(|_| wire::invalid())
    })
    .await?;
    if command.name() != expected {
        return Err(wire::invalid());
    }
    if stale_term {
        return Err(wire::unavailable());
    }
    let batch_context = match &command {
        Command::StreamRequest {
            context: Some(context),
            delivery: Some(token),
            ..
        } => Some((context.clone(), *token)),
        _ => None,
    };
    let mut reply = accept(worker, &root, sender, command)
        .await
        .unwrap_or_else(|error| WorkerReply::Control(ControlReply::Error { error }));
    if expected == "stream_request" {
        let (authority, current) = worker.stream_authority().await?;
        if authority.term != root.scheduler.scheduler_term
            || stream_recovery_view(authority, &current) != stream_recovery_view(authority, &root)
        {
            return Err(wire::unavailable());
        }
        // Reading or replicating a batch can outlast its storage credentials.
        if matches!(&reply, WorkerReply::Batch(_))
            && let Some((context, token)) = batch_context
            && let Err(error) = crate::request_queue::admission::validate_context(
                &current.template,
                Some(&context),
                now_ms() / 1000,
            )
        {
            worker.queue.streams.release_delivery(token);
            reply = WorkerReply::Control(ControlReply::Error {
                error: error.into(),
            });
        }
    }
    let (marker, body) = match reply {
        WorkerReply::Control(control) => {
            let len = queue_transport::encoded_len(&control, MAX_RECORD_BYTES + 4096)?;
            let charge = reserve(len as u64 + 4096)?;
            let bytes = queue_transport::encode_exact(&control, len)?;
            (
                0,
                Bytes::from_owner(ChargedBytes {
                    bytes: Bytes::from(bytes),
                    _charge: charge,
                }),
            )
        }
        WorkerReply::Batch(batch) => (
            if batch.format == Format::Rows { 1 } else { 2 },
            batch.bytes,
        ),
    };
    let body = Bytes::from_owner(TransportBytes {
        bytes: body,
        _transport: transport,
    });
    queue_transport::stream_reply(&worker.signer, &root, &headers, nonce, marker, body)
}

struct ChargedBytes {
    bytes: Bytes,
    _charge: crate::scheduling::MemoryCharge,
}
impl AsRef<[u8]> for ChargedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
struct TransportBytes {
    bytes: Bytes,
    _transport: OwnedSemaphorePermit,
}
impl AsRef<[u8]> for TransportBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

async fn accept(
    worker: &Arc<Worker>,
    root: &Control,
    sender: Option<Uuid>,
    command: Command,
) -> Result<WorkerReply, Error> {
    let reply = match command {
        Command::StreamRequest {
            request,
            context,
            delivery,
        } => {
            if sender.is_some() {
                return Err(wire::unauthorized().into());
            }
            if matches!(request, Request::Batch { .. }) != delivery.is_some() {
                return Err(wire::invalid().into());
            }
            crate::request_queue::admission::validate_context(
                &root.template,
                context.as_ref(),
                now_ms() / 1000,
            )?;
            let reply = call(
                WorkerDriver(worker.clone(), root.scheduler.scheduler_term),
                request,
                context,
            )
            .await?;
            if let (Reply::Batch(batch), Some(token)) = (&reply, delivery) {
                worker
                    .queue
                    .streams
                    .hold_delivery(token, batch.bytes.clone(), now_ms())?;
            }
            return Ok(WorkerReply::from(reply));
        }
        Command::StreamInstall { entry } => {
            let fence = worker.stream_fence(root).await?;
            if sender != Some(scheduler(root)?.node_id)
                || entry.run != fence.run
                || entry.version.term > fence.term
            {
                return Err(wire::unauthorized().into());
            }
            crate::request_queue::admission::validate_context(
                &root.template,
                entry.context.as_ref(),
                0,
            )
            .or_else(|error| {
                if entry.scan.is_none() {
                    Ok(())
                } else {
                    Err(error)
                }
            })?;
            worker.install_stream_replica(*entry)?;
            ControlReply::Installed
        }
        Command::StreamReset { run } => {
            let fence = worker.stream_fence(root).await?;
            let own = worker
                .signer
                .sender()
                .node_id()
                .ok_or_else(wire::unavailable)?;
            if sender != Some(scheduler(root)?.node_id)
                || fence.run != run
                || root
                    .nodes
                    .iter()
                    .find(|node| node.node_id == own)
                    .is_none_or(|node| {
                        node.state != NodeState::Starting
                            || node.registration != Registration::Registered
                    })
            {
                return Err(wire::unauthorized().into());
            }
            worker.queue.streams.set_run(run, true)?;
            ControlReply::Installed
        }
        Command::StreamPage { run, after } => {
            if sender.is_none() || worker.stream_fence(root).await?.run != run {
                return Err(wire::unauthorized().into());
            }
            ControlReply::Page {
                entry: worker
                    .queue
                    .streams
                    .page(run, after.as_ref())?
                    .map(|stored| Box::new(stored.entry.clone())),
            }
        }
        Command::StreamRelease { token, renew } => {
            if sender.is_some() || token.is_nil() {
                return Err(wire::unauthorized().into());
            }
            if renew {
                worker.queue.streams.renew_delivery(token, now_ms())?;
            } else {
                worker.queue.streams.release_delivery(token);
            }
            ControlReply::Released
        }
    };
    Ok(WorkerReply::Control(reply))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WorkerResources;

    fn memory_budget(bytes: u64) -> ResourceScheduler {
        ResourceScheduler::new(vec![(
            0,
            WorkerResources {
                max_threads: 1,
                memory_limit_bytes: bytes,
                task_memory_limit_bytes: bytes - 1,
                control_memory_reserve_bytes: 1,
            },
        )])
        .unwrap()
    }

    #[test]
    fn st03_small_control_reserve_charges_fence_reads_to_working_memory() {
        let control = memory_budget(4 << 20);
        let working = memory_budget(16 << 20);
        let existing = control.charge_memory(0, 3 << 20).unwrap();

        let page = charge_stream_staging(&control, &working, 64 << 10).unwrap();
        let fence = charge_stream_staging(&control, &working, FENCE_READ_BYTES).unwrap();
        assert_eq!(control.usage()[0].2, (3 << 20) + (64 << 10));
        assert_eq!(working.usage()[0].2, FENCE_READ_BYTES);
        let error = charge_stream_staging(&control, &working, FENCE_READ_BYTES)
            .err()
            .unwrap();
        assert_eq!(error.code, ErrorCode::Overloaded);

        drop((existing, page, fence));
        assert_eq!(control.usage()[0].2, 0);
        assert_eq!(working.usage()[0].2, 0);
    }
}
