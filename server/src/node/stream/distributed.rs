//! Signed stream traffic, all-ready installation, and sealed join handoff.
use super::{
    Authority, Dependency, Driver, Entry, Key, Registry, Source, Stored, Version, call,
    check_dependency, install, now_ms, record_bytes,
    state::MAX_RECORD_BYTES,
    wire::{self, Batch, Capabilities, Error, Format, Opened, Reply, Request, SnapshotOpened},
};
use crate::{
    cluster::{
        member::{Node, NodeState, Registration},
        state::Control,
    },
    node::{
        http,
        queue_runtime::{members, scheduler},
        queue_transport,
        runtime::Worker,
    },
    protocol::ProtocolError,
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
    let reply = exchange(client, signer, root, node, &command, response_cap).await;
    if let (Err(_), Some(token)) = (&reply, delivery) {
        let _ = tokio::time::timeout(Duration::from_secs(2), release(client, signer, root, token))
            .await;
    }
    match reply? {
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
    match exchange(
        client,
        signer,
        root,
        node,
        &Command::StreamRelease { token, renew },
        CONTROL_REPLY_BYTES,
    )
    .await?
    {
        WorkerReply::Control(ControlReply::Released) => Ok(()),
        _ => Err(wire::unavailable()),
    }
}

async fn exchange(
    client: &http::HttpClient,
    signer: &http::Signer,
    root: &Control,
    node: &Node,
    command: &Command,
    response_cap: usize,
) -> Result<WorkerReply, ProtocolError> {
    let cap = match command {
        Command::StreamInstall { .. } => MAX_RECORD_BYTES + 4096,
        _ => CONTROL_REQUEST_BYTES,
    };
    let length = queue_transport::encoded_len(command, cap)?;
    let bytes = queue_transport::encode_exact(command, length)?;
    let bytes = queue_transport::exchange_bounded(
        client,
        signer,
        root,
        node,
        bytes,
        response_cap.saturating_add(1),
    )
    .await?;
    let Some(&marker) = bytes.first() else {
        return Err(wire::invalid());
    };
    match marker {
        0 => Ok(WorkerReply::Control(
            serde_json::from_slice(&bytes[1..]).map_err(|_| wire::invalid())?,
        )),
        1 | 2 if bytes.len() > 1 => Ok(WorkerReply::Batch(Batch {
            bytes: Bytes::from(bytes).slice(1..),
            format: if marker == 1 {
                Format::Rows
            } else {
                Format::Arrow
            },
            release: None,
        })),
        _ => Err(wire::invalid()),
    }
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
        let (authority, _) = self.0.stream_authority().await?;
        if authority.term != self.1 {
            return Err(wire::unavailable());
        }
        self.registry().set_run(authority.run, false)?;
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
        if observed.term != authority.term {
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
        if current.term != observed.term || members(&fresh) != ready {
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

    async fn stream_exchange(
        &self,
        root: &Control,
        node: &Node,
        command: &Command,
        response_cap: usize,
        bootstrap: bool,
    ) -> Result<ControlReply, ProtocolError> {
        let encoded = queue_transport::encoded_len(command, MAX_RECORD_BYTES + 4096)? as u64;
        let budget = (response_cap as u64)
            .checked_mul(4)
            .and_then(|response| response.checked_add(encoded + 64 * 1024))
            .ok_or_else(wire::unavailable)?;
        let _charge = if bootstrap {
            self.control_budget.charge_memory(0, budget)?
        } else {
            self.cpu.scheduler().charge_memory(0, budget)?
        };
        match exchange(
            &self.client,
            &self.signer,
            root,
            node,
            command,
            response_cap,
        )
        .await?
        {
            WorkerReply::Control(ControlReply::Error {
                error: Error::Protocol(error),
            }) => Err(error),
            WorkerReply::Control(ControlReply::Error { .. }) => Err(wire::unavailable()),
            WorkerReply::Control(control) => Ok(control),
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
        let mut after = None;
        loop {
            let command = Command::StreamPage {
                run,
                after: after.clone(),
            };
            match self
                .stream_exchange(root, node, &command, MAX_RECORD_BYTES + 4096, true)
                .await?
            {
                ControlReply::Page { entry: Some(entry) } => {
                    if entry.run != run || after.as_ref().is_some_and(|after| after >= &entry.key) {
                        return Err(wire::invalid());
                    }
                    after = Some(entry.key.clone());
                    self.install_stream_replica(*entry)?;
                }
                ControlReply::Page { entry: None } => return Ok(()),
                _ => return Err(wire::invalid()),
            }
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
            Ok(ControlReply::Installed) => {}
            // Workers without the stream endpoint can still join before a
            // stream identity needs to be transferred to them.
            Err(error)
                if error.code == crate::protocol::ErrorCode::NotFound
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
    let (nonce, sender, digest, expected, class) = queue_transport::preauthenticate(
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
    let budget = match class.as_str() {
        "bootstrap" => &worker.control_budget,
        "progress" => &worker.progress_budget,
        _ => worker.cpu.scheduler(),
    };
    let _ingress = budget.charge_memory(0, 64 * 1024 + length as u64 * 5)?;
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
    let reply = accept(worker, &root, sender, command)
        .await
        .unwrap_or_else(|error| WorkerReply::Control(ControlReply::Error { error }));
    let (marker, body) = match reply {
        WorkerReply::Control(control) => {
            let len = queue_transport::encoded_len(&control, MAX_RECORD_BYTES + 4096)?;
            let charge = budget.charge_memory(0, len as u64 + 4096)?;
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
    if expected == "stream_request" {
        let (authority, current) = worker.stream_authority().await?;
        if authority.term != root.scheduler.scheduler_term || members(&current) != members(&root) {
            return Err(wire::unavailable());
        }
    }
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
