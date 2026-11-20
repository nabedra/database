use std::collections::BTreeMap;
use std::io::SeekFrom;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, ensure};
use axum::body::{Body, to_bytes};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode, header::CONTENT_TYPE};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router, routing::post};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tempfile::TempDir;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{Mutex, Semaphore};
use tokio::time::Instant;
use uuid::Uuid;

use super::{
    BackupLayout, CHUNK_BYTES, Config, EXPORT_SPOOL, cleanup_spool, validate_cut, verify_image,
};
use crate::control::Control;
use crate::raft::{GroupError, PeerTransport, SnapshotCut};

const LAYOUT: &str = "/internal/backup/v1/layout";
const START: &str = "/internal/backup/v1/start";
const CHUNK: &str = "/internal/backup/v1/chunk";
const CLOSE: &str = "/internal/backup/v1/close";
const MAX_LAYOUT_BYTES: usize = 5 * 1024 * 1024;
const LAYOUT_TIMEOUT: Duration = Duration::from_secs(30);
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const TRANSFER_TIMEOUT: Duration = Duration::from_secs(3600);
const PREPARE_TIMEOUT: Duration = Duration::from_secs(600);

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Start {
    transfer_id: Uuid,
    group: String,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Chunk {
    transfer_id: Uuid,
    offset: u64,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Close {
    transfer_id: Uuid,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Page {
    offset: u64,
    data: String,
}

struct Server {
    control: Arc<Control>,
    transfer: Mutex<Option<Transfer>>,
    layout_slots: Semaphore,
    requests: Semaphore,
}

struct Transfer {
    id: Uuid,
    cut: SnapshotCut,
    file: File,
    _directory: TempDir,
    created_at: Instant,
    last_chunk_at: Instant,
}

impl Transfer {
    fn expired(&self) -> bool {
        self.created_at.elapsed() >= TRANSFER_TIMEOUT
            || self.last_chunk_at.elapsed() >= IDLE_TIMEOUT
    }
}

fn release_expired(transfer: &mut Option<Transfer>) {
    if transfer.as_ref().is_some_and(Transfer::expired) {
        transfer.take();
    }
}

pub(in crate::control) fn router(control: Arc<Control>) -> Router {
    let server = Arc::new(Server {
        control,
        transfer: Mutex::new(None),
        layout_slots: Semaphore::new(1),
        requests: Semaphore::new(8),
    });
    let weak = Arc::downgrade(&server);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(5));
        loop {
            tick.tick().await;
            let Some(server) = weak.upgrade() else { break };
            if let Ok(mut transfer) = server.transfer.try_lock() {
                release_expired(&mut transfer);
            }
        }
    });
    Router::new()
        .route(LAYOUT, post(read_layout))
        .route(START, post(start))
        .route(CHUNK, post(chunk))
        .route(CLOSE, post(close))
        .with_state(server)
}

pub(super) fn connect(config: &Config) -> Result<PeerTransport> {
    PeerTransport::new(
        config.cluster_id,
        &config.peer_token,
        config.peer_ca.as_deref(),
        config.peer_identity.as_deref(),
        config.allow_plaintext,
    )
}

pub(super) async fn layout<'a>(
    peers: &PeerTransport,
    addresses: impl Iterator<Item = &'a str>,
) -> Result<BackupLayout> {
    let mut last_error = anyhow!("no broker addresses were configured");
    for address in addresses {
        match peers
            .post_with_timeout(address, LAYOUT, &(), LAYOUT_TIMEOUT)
            .await
        {
            Ok(layout) => return Ok(layout),
            Err(error) => last_error = error,
        }
    }
    Err(last_error).context("read a committed backup layout with no pending operator intent")
}

pub(super) async fn download(
    peers: &PeerTransport,
    group: &str,
    voters: &BTreeMap<u64, String>,
    source_cluster: Uuid,
    path: &Path,
) -> Result<SnapshotCut> {
    let request = Start {
        transfer_id: Uuid::new_v4(),
        group: group.to_owned(),
    };
    let mut last_error = anyhow!("missing snapshot voters");
    for address in voters.values() {
        let result = peers
            .post_with_timeout::<_, SnapshotCut>(address, START, &request, PREPARE_TIMEOUT)
            .await;
        let close_request = Close {
            transfer_id: request.transfer_id,
        };
        match result {
            Ok(cut) => {
                let result = async {
                    validate_cut(group, &cut, source_cluster)?;
                    pull_pages(peers, address, request.transfer_id, &cut, path).await
                }
                .await;
                let _: Result<()> = peers.post(address, CLOSE, &close_request).await;
                result?;
                return Ok(cut);
            }
            Err(error) => {
                let _: Result<()> = peers.post(address, CLOSE, &close_request).await;
                last_error = error;
            }
        }
    }
    Err(last_error).context("no voter could export a quorum-confirmed snapshot")
}

async fn pull_pages(
    peers: &PeerTransport,
    address: &str,
    transfer_id: Uuid,
    cut: &SnapshotCut,
    path: &Path,
) -> Result<()> {
    let mut output = File::create_new(path).await?;
    let mut offset = 0;
    while offset < cut.bytes {
        let request = Chunk {
            transfer_id,
            offset,
        };
        let page: Page = match peers.post(address, CHUNK, &request).await {
            Ok(page) => page,
            Err(_) => peers.post(address, CHUNK, &request).await?,
        };
        let size = (cut.bytes - offset).min(CHUNK_BYTES as u64) as usize;
        ensure!(
            page.offset == offset && page.data.len() == size.div_ceil(3) * 4,
            "snapshot page position or size differs from requested range"
        );
        let bytes = STANDARD.decode(page.data)?;
        ensure!(bytes.len() == size, "invalid snapshot page byte count");
        output.write_all(&bytes).await?;
        offset += size as u64;
    }
    output.sync_all().await?;
    drop(output);
    let path = path.to_owned();
    let cut = cut.clone();
    tokio::task::spawn_blocking(move || verify_image(&path, &cut)).await?
}

async fn read_layout(
    State(server): State<Arc<Server>>,
    headers: HeaderMap,
) -> Result<Response, StatusCode> {
    authenticate(&server, &headers)?;
    let _permit = server
        .layout_slots
        .try_acquire()
        .map_err(|_| StatusCode::TOO_MANY_REQUESTS)?;
    let layout = server
        .control
        .backup_layout()
        .await
        .map_err(|error| match error {
            crate::control::Error::Busy => StatusCode::CONFLICT,
            _ => StatusCode::SERVICE_UNAVAILABLE,
        })?;
    let body = serde_json::to_vec(&layout).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if body.len() > MAX_LAYOUT_BYTES {
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }
    Ok(([(CONTENT_TYPE, "application/json")], Body::from(body)).into_response())
}

async fn start(
    State(server): State<Arc<Server>>,
    request: Request,
) -> Result<Json<SnapshotCut>, StatusCode> {
    let request: Start = read_request(&server, request).await?;
    if request.transfer_id.is_nil() {
        return Err(StatusCode::BAD_REQUEST);
    }
    // Retain the slot and temporary directory until the disk job settles even if
    // the client's connection disappears while the store is exporting the image.
    tokio::spawn(async move { prepare(&server, request).await })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map(Json)
}

async fn prepare(server: &Server, request: Start) -> Result<SnapshotCut, StatusCode> {
    let mut transfer = server
        .transfer
        .try_lock()
        .map_err(|_| StatusCode::TOO_MANY_REQUESTS)?;
    release_expired(&mut transfer);
    if let Some(current) = transfer.as_mut() {
        if current.id == request.transfer_id && current.cut.group == request.group {
            current.last_chunk_at = Instant::now();
            return Ok(current.cut.clone());
        }
        return Err(StatusCode::TOO_MANY_REQUESTS);
    }

    let group = server
        .control
        .broker
        .group(&request.group)
        .ok_or(StatusCode::NOT_FOUND)?;
    if group.leader() != Some(server.control.broker.id()) {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    let root = server.control.config.data_dir.clone();
    let directory = tokio::task::spawn_blocking(move || -> std::io::Result<TempDir> {
        cleanup_spool(&root)?;
        let spool = root.join(EXPORT_SPOOL);
        std::fs::create_dir(&spool)?;
        TempDir::new_in(spool)
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let limit = server
        .control
        .broker
        .disk_available_bytes()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .saturating_sub(server.control.config.min_free_disk_bytes);
    if limit < CHUNK_BYTES as u64 {
        return Err(StatusCode::INSUFFICIENT_STORAGE);
    }
    let path = directory.path().join("image");
    let cut = group
        .export_bounded(&path, limit)
        .await
        .map_err(|error| match error {
            GroupError::NotLeader(_) | GroupError::Unavailable | GroupError::Closed => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            GroupError::Overloaded => StatusCode::TOO_MANY_REQUESTS,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        })?;
    let file = File::open(&path)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let now = Instant::now();
    *transfer = Some(Transfer {
        id: request.transfer_id,
        cut: cut.clone(),
        file,
        _directory: directory,
        created_at: now,
        last_chunk_at: now,
    });
    Ok(cut)
}

async fn chunk(
    State(server): State<Arc<Server>>,
    request: Request,
) -> Result<Json<Page>, StatusCode> {
    let request: Chunk = read_request(&server, request).await?;
    let mut transfer = server
        .transfer
        .try_lock()
        .map_err(|_| StatusCode::TOO_MANY_REQUESTS)?;
    release_expired(&mut transfer);
    let current = transfer
        .as_mut()
        .filter(|current| current.id == request.transfer_id)
        .ok_or(StatusCode::GONE)?;
    if request.offset >= current.cut.bytes || !request.offset.is_multiple_of(CHUNK_BYTES as u64) {
        return Err(StatusCode::RANGE_NOT_SATISFIABLE);
    }
    let size = (current.cut.bytes - request.offset).min(CHUNK_BYTES as u64) as usize;
    let mut bytes = vec![0; size];
    current
        .file
        .seek(SeekFrom::Start(request.offset))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    current
        .file
        .read_exact(&mut bytes)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    current.last_chunk_at = Instant::now();
    Ok(Json(Page {
        offset: request.offset,
        data: STANDARD.encode(bytes),
    }))
}

async fn close(
    State(server): State<Arc<Server>>,
    request: Request,
) -> Result<Json<()>, StatusCode> {
    let request: Close = read_request(&server, request).await?;
    let mut transfer = server
        .transfer
        .try_lock()
        .map_err(|_| StatusCode::TOO_MANY_REQUESTS)?;
    release_expired(&mut transfer);
    if transfer
        .as_ref()
        .is_some_and(|current| current.id == request.transfer_id)
    {
        transfer.take();
    }
    Ok(Json(()))
}

fn authenticate(server: &Server, headers: &HeaderMap) -> Result<(), StatusCode> {
    if !server.control.broker.peers().authenticated(headers) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(())
}

async fn read_request<T: DeserializeOwned>(
    server: &Server,
    request: Request,
) -> Result<T, StatusCode> {
    authenticate(server, request.headers())?;
    let _permit = server
        .requests
        .try_acquire()
        .map_err(|_| StatusCode::TOO_MANY_REQUESTS)?;
    let data = tokio::time::timeout(Duration::from_secs(2), to_bytes(request.into_body(), 1024))
        .await
        .map_err(|_| StatusCode::REQUEST_TIMEOUT)?
        .map_err(|_| StatusCode::PAYLOAD_TOO_LARGE)?;
    serde_json::from_slice(&data).map_err(|_| StatusCode::BAD_REQUEST)
}
