//! Offline OLAP snapshot export. This module links only in an OLAP executable;
//! importing the shared archive is performed by a separate OLTP executable.
use crate::{
    migration_interchange::{ArchiveHeader, ArchiveSummary, ArchiveWriter, SourceEngine},
    protocol::{ErrorCode, Outcome, ProtocolError},
    storage::{ObjectKey, ObjectStore, Storage, TableData},
    worker_pool::WorkerPool,
};
use sha2::{Digest as _, Sha256};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};
/// Capture one immutable catalog and export each referenced partition in order.
/// The source must be kept in its offline write-maintenance window and retained
/// for rollback. A changed catalog invalidates publication; partial output is
/// never advertised as a completed migration archive.
pub async fn export_offline<B: ObjectStore + Clone + Send + Sync + 'static>(
    storage: Storage<B>,
    pool: &WorkerPool,
    destination: &Path,
) -> Result<ArchiveSummary, ProtocolError> {
    let destination = destination.to_path_buf();
    pool.submit(0, move |_, runtime| {
        let captured = runtime
            .block_on(storage.read(&ObjectKey::catalog(), crate::protocol::MAX_CATALOG_BYTES))
            .map_err(|_| storage_error())?;
        let catalog = decode_catalog(&captured.bytes)?;
        let source_snapshot: [u8; 32] = Sha256::digest(&captured.bytes).into();
        let archive_id = nabedra_rt::ids::uuid_v4();
        let parent = destination
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        if destination.exists() {
            return Err(conflict());
        }
        let temporary = parent.join(format!(".nabedra-export-{archive_id}.partial"));
        let mut cleanup = PartialFile(Some(temporary.clone()));
        let file = OpenOptions::new()
            .create_new(true)
            .mode(0o600)
            .write(true)
            .open(&temporary)
            .map_err(|_| storage_error())?;
        let mut writer = ArchiveWriter::new(
            file,
            ArchiveHeader {
                format_version: 1,
                archive_id,
                source_engine: SourceEngine::Olap,
                source_snapshot,
            },
        )?;
        for (name, entry) in &catalog.tables {
            let table = entry.descriptor(name);
            writer.begin_table(table.clone())?;
            match &entry.data {
                TableData::Legacy { snapshot } => {
                    let object = ObjectKey::parse_snapshot(snapshot, table.id)
                        .map_err(|_| storage_error())?;
                    let bytes = runtime
                        .block_on(
                            storage.read(&object, crate::snapshot::MAX_ENCODED_SNAPSHOT_BYTES),
                        )
                        .map_err(|_| storage_error())?;
                    let rows =
                        crate::snapshot::decode(&table, bytes.bytes.into(), Some(entry.row_count))?;
                    crate::engine::validate_partition_rows(&table, &rows, None)?;
                    for row in rows {
                        writer.row(row)?;
                    }
                }
                TableData::Partitioned { files } => {
                    for partition in files {
                        let length =
                            usize::try_from(partition.byte_size).map_err(|_| storage_error())?;
                        crate::snapshot::check_memory_shape(
                            &table,
                            usize::try_from(partition.row_count).map_err(|_| storage_error())?,
                            usize::try_from(partition.payload_bytes)
                                .map_err(|_| storage_error())?,
                            length,
                        )?;
                        let key = ObjectKey::parse_partition(&partition.object, table.id)
                            .map_err(|_| storage_error())?;
                        let bytes = runtime
                            .block_on(storage.read(&key, length))
                            .map_err(|_| storage_error())?;
                        if bytes.bytes.len() != length {
                            return Err(storage_error());
                        }
                        let rows = crate::snapshot::decode(
                            &table,
                            bytes.bytes.into(),
                            Some(partition.row_count),
                        )?;
                        crate::engine::validate_partition_rows(
                            &table,
                            &rows,
                            Some((&partition.range, partition.payload_bytes)),
                        )?;
                        for row in rows {
                            writer.row(row)?;
                        }
                    }
                }
            }
            if writer.end_table()?.rows != entry.row_count {
                return Err(storage_error());
            }
        }
        let (mut file, summary) = writer.finish()?;
        file.flush().map_err(|_| storage_error())?;
        file.sync_all().map_err(|_| storage_error())?;
        let current = runtime
            .block_on(storage.read(&ObjectKey::catalog(), crate::protocol::MAX_CATALOG_BYTES))
            .map_err(|_| storage_error())?;
        if current.etag != captured.etag || current.bytes != captured.bytes {
            return Err(ProtocolError::new(
                ErrorCode::Conflict,
                "OLAP source changed during offline export",
                Outcome::NotApplied,
            ));
        }
        std::fs::hard_link(&temporary, &destination).map_err(|_| conflict())?;
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| storage_error())?;
        std::fs::remove_file(&temporary).map_err(|_| storage_error())?;
        cleanup.0 = None;
        Ok(summary)
    })
    .await?
    .receive()
    .await
}
struct PartialFile(Option<PathBuf>);
impl Drop for PartialFile {
    fn drop(&mut self) {
        if let Some(path) = &self.0 {
            let _ = std::fs::remove_file(path);
        }
    }
}
fn storage_error() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "offline OLAP export failed",
        Outcome::Unknown,
    )
}
fn conflict() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::Conflict,
        "migration destination already exists or cannot be published",
        Outcome::NotApplied,
    )
}

fn decode_catalog(bytes: &[u8]) -> Result<crate::storage::Catalog, ProtocolError> {
    #[derive(serde::Deserialize)]
    struct Version {
        format_version: u32,
    }
    match serde_json::from_slice::<Version>(bytes)
        .map_err(|_| storage_error())?
        .format_version
    {
        1 | 2 => crate::storage::Catalog::decode(bytes).map_err(|_| storage_error()),
        3..=6 => {
            let root =
                crate::storage::ClusterCatalog::decode(bytes).map_err(|_| storage_error())?;
            Ok(crate::storage::Catalog {
                format_version: 2,
                revision: root.revision,
                commit_id: root.commit_id,
                tables: root.tables,
            })
        }
        _ => Err(storage_error()),
    }
}
/// Uses the same retained credential, CA and backend factory as ordinary OLAP
/// startup, but does not initialize, fence or publish any source object.
pub async fn run_configured(
    config: &Path,
    output: &Path,
    confirm: bool,
) -> Result<ArchiveSummary, ProtocolError> {
    if !confirm {
        return Err(ProtocolError::new(
            ErrorCode::ValidationError,
            "offline export requires stopped source writers",
            Outcome::NotApplied,
        ));
    }
    let config = std::fs::canonicalize(config).map_err(|_| storage_error())?;
    let prepared = nabedra_rt::task::spawn_blocking(move || {
        crate::cluster::startup_material::composition::prepare_api(
            &config,
            nabedra_rt::time::now_std() + std::time::Duration::from_secs(30),
        )
    })
    .await
    .map_err(|_| storage_error())?
    .map_err(|_| storage_error())?;
    let ready = prepared
        .connect_async()
        .await
        .map_err(|_| storage_error())?;
    let template = &ready.prepared.input.value.template;
    let namespace = match &template.storage {
        crate::cluster::configuration::Storage::Http(options) => &options.namespace,
        crate::cluster::configuration::Storage::S3(options) => &options.namespace,
    };
    let namespace = crate::storage::Namespace::new(namespace).map_err(|_| storage_error())?;
    let resources = crate::config::WorkerResources {
        max_threads: 1,
        ..template.resources.unwrap_or_default()
    };
    let (pool, workers) =
        WorkerPool::start_with_resources(resources, tokio::runtime::Handle::current())
            .map_err(|_| storage_error())?;
    let storage = Storage::new(ready.backend.client, namespace);
    let result = export_offline(storage, &pool, output).await;
    workers.shutdown().await?;
    result
}
