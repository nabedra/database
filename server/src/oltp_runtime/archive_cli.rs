//! Engine-selected offline archive commands. No OLAP engine is linked here.
use super::{ServeOptions, read_config, valid_database, worker_runtime::WorkerRuntime};
use crate::{
    cli::ArchiveImportOptions,
    config::{ExecutionConfig, WorkerResources},
};
use std::path::PathBuf;

fn token() -> Result<String, &'static str> {
    std::env::var("NABEDRA_TOKEN")
        .ok()
        .filter(|token| !token.trim().is_empty())
        .ok_or("NABEDRA_TOKEN is required")
}
pub async fn import(options: ArchiveImportOptions, restore: bool) -> Result<(), &'static str> {
    if (!restore && (!options.archives.is_empty() || options.restore_ts.is_some()))
        || options.workers == 0
        || options.workers > 256
        || !valid_database(&options.database)
        || (!restore && options.data_dir.symlink_metadata().is_ok())
    {
        return Err("configuration invalid");
    }
    let resources: WorkerResources = match options.resources_config {
        Some(path) => read_config(&path).map_err(|_| "configuration invalid")?,
        None => WorkerResources {
            max_threads: options.workers as u64,
            ..Default::default()
        },
    };
    let execution: ExecutionConfig = match options.execution_config {
        Some(path) => read_config(&path).map_err(|_| "configuration invalid")?,
        None => Default::default(),
    };
    if resources.max_threads == 0 || resources.max_threads > 256 {
        return Err("configuration invalid");
    }
    resources
        .validate_execution(&execution)
        .map_err(|_| "configuration invalid")?;
    let token = token()?;
    let result: Result<serde_json::Value, crate::protocol::ProtocolError> = if restore {
        let mut archives = vec![options.input];
        archives.extend(options.archives);
        let cut = options
            .restore_ts
            .map(crate::oltp_replication::types::CommitTs::new)
            .transpose()
            .map_err(|_| "configuration invalid")?;
        WorkerRuntime::restore_native(
            &archives,
            cut,
            &options.data_dir,
            &options.database,
            &token,
            execution,
            resources,
        )
        .await
        .and_then(|summary| {
            serde_json::to_value(summary).map_err(|_| {
                crate::protocol::ProtocolError::new(
                    crate::protocol::ErrorCode::StorageUnavailable,
                    "restore summary encode failed",
                    crate::protocol::Outcome::Unknown,
                )
            })
        })
    } else {
        WorkerRuntime::import_offline(
            &options.input,
            &options.data_dir,
            &options.database,
            &token,
            execution,
            resources,
        )
        .await
        .and_then(|summary| {
            serde_json::to_value(summary).map_err(|_| {
                crate::protocol::ProtocolError::new(
                    crate::protocol::ErrorCode::StorageUnavailable,
                    "import summary encode failed",
                    crate::protocol::Outcome::Unknown,
                )
            })
        })
    };
    match result {
        Ok(summary) => {
            println!(
                "{}",
                serde_json::to_string(&summary).expect("archive summary")
            );
            Ok(())
        }
        Err(error) => {
            eprintln!(
                "nabedra-db: archive import failed: {:?}: {}",
                error.code, error.message
            );
            Err("OLTP archive import failed")
        }
    }
}
pub async fn backup(
    options: ServeOptions,
    output: PathBuf,
    previous: Option<PathBuf>,
) -> Result<(), &'static str> {
    if !options.cold_start {
        return Err("--cold-start is required");
    }
    let (execution, resources) = options.limits().map_err(|_| "configuration invalid")?;
    let runtime = WorkerRuntime::open_configured(
        &options.data_dir,
        &options.database,
        &token()?,
        execution,
        resources,
        true,
    )
    .await
    .map_err(|_| "OLTP backup initialization failed")?;
    let previous = previous
        .map(|path| WorkerRuntime::native_backup_manifest(&path))
        .transpose()
        .map_err(|_| "previous archive manifest invalid")?;
    let owner = match std::fs::read(output.join("attempt.json")) {
        Ok(bytes) if bytes.len() <= 4096 => {
            serde_json::from_slice(&bytes).map_err(|_| "backup attempt invalid")?
        }
        Ok(_) => return Err("backup attempt invalid"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            crate::oltp::backup::BackupOwnerRef {
                backup_id: previous
                    .as_ref()
                    .map_or_else(nabedra_rt::ids::uuid_v4, |old| old.owner.backup_id),
                attempt: previous
                    .as_ref()
                    .map_or(Some(1), |old| old.owner.attempt.checked_add(1))
                    .ok_or("backup attempt exhausted")?,
            }
        }
        Err(_) => return Err("backup attempt unavailable"),
    };
    let result = runtime.backup_native(&output, owner, previous).await;
    runtime.stop_accepting();
    let drained = runtime
        .drain()
        .await
        .map_err(|_| "OLTP backup shutdown failed");
    match result {
        Ok(manifest) => {
            drained?;
            println!(
                "{}",
                serde_json::to_string(&manifest).expect("backup manifest")
            );
            Ok(())
        }
        Err(error) => {
            let _ = drained;
            eprintln!(
                "nabedra-db: backup failed: {:?}: {}",
                error.code, error.message
            );
            Err("OLTP backup failed")
        }
    }
}
