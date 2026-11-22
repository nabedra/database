use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::{Result, anyhow, ensure};
use nabedra_kv::WriteBatch;
use openraft::{
    ErrorSubject, ErrorVerb, RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::{NamedTempFile, TempPath};
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf};
use uuid::Uuid;

use super::store::{
    Database, GroupStore, MAX_RECORD_VALUE_BYTES, StateHeader, StoredSnapshot, entries, prefix_end,
    validate_record,
};
use super::{Node, TypeConfig, storage_error};

const MAGIC: &[u8; 8] = b"NBEVT01\n";
const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_KEY_BYTES: usize = 4096;
const STAGE_BYTES: usize = 4 * 1024 * 1024;
pub(super) const CHUNK_BYTES: usize = 128 * 1024;
pub(super) const MAX_SNAPSHOT_BYTES: u64 = 64 * 1024 * 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotCut {
    pub group: String,
    pub cluster_id: Uuid,
    pub applied_index: Option<u64>,
    pub bytes: u64,
    pub records: u64,
    pub sha256: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ImageHeader {
    group: String,
    cluster_id: Uuid,
    meta: SnapshotMeta<u64, Node>,
}

impl ImageHeader {
    fn cut(&self, bytes: u64, records: u64, sha256: String) -> SnapshotCut {
        SnapshotCut {
            group: self.group.clone(),
            cluster_id: self.cluster_id,
            applied_index: self.meta.last_log_id.map(|id| id.index),
            bytes,
            records,
            sha256,
        }
    }
}

/// A seekable snapshot file. Inbound snapshots delete their temporary file
/// automatically if transport is interrupted before installation.
pub(super) struct SnapshotFile {
    io: tokio::fs::File,
    incoming: Option<TempPath>,
}

impl SnapshotFile {
    fn readable(file: File) -> Self {
        Self {
            io: tokio::fs::File::from_std(file),
            incoming: None,
        }
    }

    fn incoming(dir: &Path) -> Result<Self> {
        let (file, path) = NamedTempFile::new_in(dir)?.into_parts();
        Ok(Self {
            io: tokio::fs::File::from_std(file),
            incoming: Some(path),
        })
    }
}

impl AsyncRead for SnapshotFile {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buffer)
    }
}

impl AsyncWrite for SnapshotFile {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

impl AsyncSeek for SnapshotFile {
    fn start_seek(mut self: Pin<&mut Self>, towards: SeekFrom) -> io::Result<()> {
        Pin::new(&mut self.io).start_seek(towards)
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Pin::new(&mut self.io).poll_complete(cx)
    }
}

#[derive(Clone)]
pub(super) struct Snapshots {
    pub store: GroupStore,
    pub cluster_id: Uuid,
    // Serializes fetching a current file descriptor and committing a new one.
    pub catalog: Arc<std::sync::Mutex<()>>,
}

impl Snapshots {
    pub fn new(store: GroupStore, cluster_id: Uuid) -> Self {
        Self {
            store,
            cluster_id,
            catalog: Arc::new(std::sync::Mutex::new(())),
        }
    }

    pub async fn receive(&self) -> Result<SnapshotFile> {
        let dir = self.store.snapshot_dir();
        self.store.run(move |_| SnapshotFile::incoming(&dir)).await
    }

    fn generate(
        &self,
        file: &mut File,
        snapshot_id: String,
        maximum_bytes: u64,
    ) -> Result<(ImageHeader, SnapshotCut)> {
        let data_limit = maximum_bytes.min(MAX_SNAPSHOT_BYTES).saturating_sub(32);
        let tx = self.store.transaction()?;
        let prefix = self.store.state_prefix(tx.header.generation);
        let header = ImageHeader {
            group: self.store.id.to_string(),
            cluster_id: self.cluster_id,
            meta: SnapshotMeta {
                last_log_id: tx.header.applied,
                last_membership: tx.header.membership.clone(),
                snapshot_id,
            },
        };

        let header_json = serde_json::to_vec(&header)?;
        ensure!(
            header_json.len() <= MAX_HEADER_BYTES,
            "snapshot header exceeds its limit"
        );

        let mut output = BufWriter::with_capacity(CHUNK_BYTES, &mut *file);
        let mut hash = Sha256::new();
        let mut bytes = 0_u64;
        let mut records = 0_u64;
        let mut emit = |data: &[u8]| -> Result<()> {
            bytes += data.len() as u64;
            ensure!(bytes <= data_limit, "snapshot exceeds its size limit");
            output.write_all(data)?;
            hash.update(data);
            Ok(())
        };
        emit(MAGIC)?;
        emit(&(header_json.len() as u32).to_be_bytes())?;
        emit(&header_json)?;

        for entry in entries(&tx.view, &prefix, &prefix_end(&prefix), false) {
            let (key, value) = entry?;
            let key = &key[prefix.len()..];
            validate_record(key, &value)?;
            emit(&(key.len() as u32).to_be_bytes())?;
            emit(&(value.len() as u32).to_be_bytes())?;
            emit(key)?;
            emit(&value)?;
            records += 1;
        }
        emit(&[0; 8])?;
        let digest = hash.finalize();
        output.write_all(&digest)?;
        output.flush()?;
        drop(output);
        file.sync_all()?;

        let cut = header.cut(bytes + 32, records, format!("{digest:x}"));
        Ok((header, cut))
    }

    pub async fn export(&self, path: &Path, maximum_bytes: u64) -> Result<SnapshotCut> {
        let path = path.to_path_buf();
        let manager = self.clone();
        self.store
            .run_bulk(move |_| {
                let dir = path
                    .parent()
                    .filter(|path| !path.as_os_str().is_empty())
                    .unwrap_or_else(|| Path::new("."));
                let mut temporary = NamedTempFile::new_in(dir)?;
                let (_, cut) = manager.generate(
                    temporary.as_file_mut(),
                    crate::rt::uuid_v4().to_string(),
                    maximum_bytes,
                )?;
                temporary
                    .persist_noclobber(&path)
                    .map_err(|error| error.error)?;
                File::open(dir)?.sync_all()?;
                Ok(cut)
            })
            .await
    }

    pub fn seed(&self, path: &Path) -> Result<SnapshotCut> {
        let header = self.store.header()?;
        ensure!(
            header.applied.is_none()
                && header.generation.is_nil()
                && self.store.read::<StoredSnapshot>(b"snapshot")?.is_none()
                && self.store.read::<openraft::Vote<u64>>(b"vote")?.is_none(),
            "restore requires a fresh, unopened group"
        );
        self.stage_image(File::open(path)?, None, None)
    }

    fn stage_image(
        &self,
        mut file: File,
        expected: Option<SnapshotMeta<u64, Node>>,
        incoming: Option<TempPath>,
    ) -> Result<SnapshotCut> {
        let bytes = file.metadata()?.len();
        ensure!(
            bytes <= MAX_SNAPSHOT_BYTES,
            "snapshot exceeds its size limit"
        );
        let replication = expected.is_some();
        ensure!(
            replication == incoming.is_some(),
            "missing snapshot staging file"
        );
        file.seek(SeekFrom::Start(0))?;
        let mut input = BufReader::with_capacity(CHUNK_BYTES, &file);
        let mut hash = Sha256::new();
        let mut magic = [0; 8];
        read_hash(&mut input, &mut hash, &mut magic)?;
        ensure!(&magic == MAGIC, "invalid Events snapshot header");
        let header_bytes = read_len(&mut input, &mut hash)?;
        ensure!(
            header_bytes <= MAX_HEADER_BYTES,
            "snapshot header exceeds its limit"
        );
        let mut header_json = vec![0; header_bytes];
        read_hash(&mut input, &mut hash, &mut header_json)?;
        let image: ImageHeader = serde_json::from_slice(&header_json)?;
        ensure!(
            image.group == *self.store.id,
            "snapshot belongs to another group"
        );
        if let Some(expected) = &expected {
            ensure!(expected == &image.meta, "snapshot metadata mismatch");
            ensure!(
                image.cluster_id == self.cluster_id,
                "snapshot belongs to another cluster"
            );
            ensure!(
                self.store.header()?.applied <= expected.last_log_id,
                "snapshot would roll the applied index back"
            );
        } else {
            ensure!(
                image.cluster_id != self.cluster_id,
                "restore requires a fresh cluster ID"
            );
        }

        let generation = crate::rt::uuid_v4();
        let db = &self.store.db;
        self.store.cleanup_interrupted_stage()?;
        let mut stage = WriteBatch::new();
        let stage_key = self.store.key(b"staged-generation");
        stage.put(&stage_key, &serde_json::to_vec(&generation)?);
        db.write(stage)?;

        let prefix = self.store.state_prefix(generation);
        let mut records = 0_u64;
        let mut stage = WriteBatch::new();
        let mut staged_bytes = 0_usize;
        let mut previous = Vec::new();
        let digest = loop {
            let key_bytes = read_len(&mut input, &mut hash)?;
            let value_bytes = read_len(&mut input, &mut hash)?;
            if key_bytes == 0 {
                ensure!(value_bytes == 0, "invalid snapshot end marker");
                let digest = hash.finalize();
                let mut tail = [0_u8; 32];
                input.read_exact(&mut tail)?;
                ensure!(
                    tail.as_slice() == digest.as_slice(),
                    "snapshot checksum mismatch"
                );
                ensure!(input.read(&mut [0_u8])? == 0, "trailing snapshot bytes");
                break digest;
            }
            ensure!(key_bytes <= MAX_KEY_BYTES, "oversized snapshot record key");
            ensure!(
                value_bytes <= MAX_RECORD_VALUE_BYTES,
                "oversized snapshot record"
            );
            let mut key = vec![0_u8; key_bytes];
            let mut value = vec![0_u8; value_bytes];
            read_hash(&mut input, &mut hash, &mut key)?;
            read_hash(&mut input, &mut hash, &mut value)?;
            ensure!(key > previous, "snapshot record keys must be increasing");
            stage.put(&[prefix.as_slice(), &key].concat(), &value);
            previous = key;
            records += 1;
            staged_bytes += key_bytes + value_bytes + prefix.len();
            if staged_bytes >= STAGE_BYTES {
                db.write(std::mem::take(&mut stage))?;
                staged_bytes = 0;
            }
        };
        if !stage.is_empty() {
            db.write(stage)?;
        }
        let cut = image.cut(bytes, records, format!("{digest:x}"));
        let old_generation = self.store.header()?.generation;
        let mut descriptor = None;
        if let Some(path) = incoming {
            file.sync_all()?;
            let file_id = crate::rt::uuid_v4();
            let dir = self.store.snapshot_dir();
            path.persist(dir.join(file_id.to_string()))
                .map_err(|error| error.error)?;
            File::open(dir)?.sync_all()?;
            descriptor = Some(StoredSnapshot {
                file_id,
                meta: image.meta.clone(),
            });
        }
        let _catalog = self
            .catalog
            .lock()
            .map_err(|_| anyhow!("snapshot catalog lock poisoned"))?;
        let old_snapshot: Option<StoredSnapshot> = self.store.read(b"snapshot")?;
        let mut publish = WriteBatch::new();
        let header = if replication {
            StateHeader {
                generation,
                applied: image.meta.last_log_id,
                membership: image.meta.last_membership.clone(),
            }
        } else {
            StateHeader {
                generation,
                ..StateHeader::default()
            }
        };
        if let Some(descriptor) = descriptor {
            publish.put(
                &self.store.key(b"snapshot"),
                &serde_json::to_vec(&descriptor)?,
            );
        }
        if !replication {
            publish.put(&self.store.key(b"restore-cut"), &serde_json::to_vec(&cut)?);
        }
        self.store.write_header(&mut publish, &header)?;
        self.store.remove_generation(old_generation, &mut publish);
        publish.put(&Database::hosted_key(&self.store.id), b"");
        publish.delete(&stage_key);
        db.write(publish)?;
        if let Some(old_snapshot) = old_snapshot {
            self.remove_file(old_snapshot.file_id);
        }

        Ok(cut)
    }

    fn remove_file(&self, file_id: Uuid) {
        if let Err(error) = fs::remove_file(self.store.snapshot_dir().join(file_id.to_string())) {
            tracing::warn!(group = %self.store.id, %error, "could not remove old snapshot file");
        }
    }

    pub async fn install(
        &self,
        expected: SnapshotMeta<u64, Node>,
        file: SnapshotFile,
    ) -> Result<()> {
        let temporary = file
            .incoming
            .ok_or_else(|| anyhow!("missing incoming snapshot file"))?;
        let file = file.io.into_std().await;
        let manager = self.clone();
        self.store
            .run_bulk_as("install_snapshot", move |_| {
                manager.stage_image(file, Some(expected), Some(temporary))?;
                Ok(())
            })
            .await
    }

    fn read_current(&self) -> Result<Option<Snapshot<TypeConfig>>> {
        let _catalog = self
            .catalog
            .lock()
            .map_err(|_| anyhow!("snapshot catalog lock poisoned"))?;
        let Some(descriptor) = self.store.read::<StoredSnapshot>(b"snapshot")? else {
            return Ok(None);
        };
        let dir = self.store.snapshot_dir();
        let file = File::open(dir.join(descriptor.file_id.to_string()))?;
        Ok(Some(Snapshot {
            meta: descriptor.meta,
            snapshot: Box::new(SnapshotFile::readable(file)),
        }))
    }

    pub async fn current(&self) -> Result<Option<Snapshot<TypeConfig>>> {
        let manager = self.clone();
        self.store.run(move |_| manager.read_current()).await
    }

    fn build(&self) -> Result<Snapshot<TypeConfig>> {
        let dir = self.store.snapshot_dir();
        let mut temporary = NamedTempFile::new_in(&dir)?;
        let file_id = crate::rt::uuid_v4();
        let (header, _) = self.generate(
            temporary.as_file_mut(),
            file_id.to_string(),
            MAX_SNAPSHOT_BYTES,
        )?;
        let mut file = temporary
            .persist(dir.join(file_id.to_string()))
            .map_err(|err| err.error)?;
        file.seek(SeekFrom::Start(0))?;
        File::open(&dir)?.sync_all()?;
        let _catalog = self
            .catalog
            .lock()
            .map_err(|_| anyhow!("snapshot catalog lock poisoned"))?;
        let old: Option<StoredSnapshot> = self.store.read(b"snapshot")?;
        let mut update = WriteBatch::new();
        let descriptor = StoredSnapshot {
            file_id,
            meta: header.meta.clone(),
        };
        update.put(
            &self.store.key(b"snapshot"),
            &serde_json::to_vec(&descriptor)?,
        );
        self.store.db.write(update)?;
        if let Some(old) = old {
            self.remove_file(old.file_id);
        }
        Ok(Snapshot {
            meta: header.meta,
            snapshot: Box::new(SnapshotFile::readable(file)),
        })
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Snapshots {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        let manager = self.clone();
        self.store
            .run_bulk_as("build_snapshot", move |_| manager.build())
            .await
            .map_err(|error| storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, error))
    }
}

fn read_hash(reader: &mut impl Read, hash: &mut Sha256, bytes: &mut [u8]) -> io::Result<()> {
    reader.read_exact(bytes)?;
    hash.update(bytes);
    Ok(())
}

fn read_len(reader: &mut impl Read, hash: &mut Sha256) -> io::Result<usize> {
    let mut bytes = [0; 4];
    read_hash(reader, hash, &mut bytes)?;
    Ok(u32::from_be_bytes(bytes) as usize)
}
