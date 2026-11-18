//! Offline targets are isolated directories until the complete archive has
//! validated. Source table UUIDs survive; source queues and receipts do not.
use super::*;
use crate::{
    migration_interchange::{ArchiveReader, ArchiveSummary, Record},
    oltp::{
        backup::{
            BackupBackend, BackupManifest, BackupOwnerRef, BackupService, VerifiedBackupPublished,
        },
        mvcc::{
            catalog::PinnedCatalog,
            index::IndexDefinition,
            planner::{MutationPlan, ResolveFuture},
            store::lifecycle::VerifiedLifecycleGuard,
        },
    },
    oltp_replication::{
        authority::status::{RequestKind, StatusReply},
        owned::serialized_digest,
    },
};
use std::{
    fs::File,
    path::{Path, PathBuf},
};
struct MetadataImport {
    pin: PinnedCatalog,
    storage_pin: StoragePin,
    identity: PinIdentity,
    txn: TxnId,
    request: RequestBinding,
}
struct RuntimeBackup {
    runtime: Arc<WorkerRuntime>,
}
impl BackupBackend for RuntimeBackup {
    fn planner(&self) -> &NativePlanner {
        &self.runtime.planner
    }
    fn begin(&self, owner: BackupOwnerRef) -> ResolveFuture<'_, (StoragePin, PinnedCatalog)> {
        Box::pin(async move {
            let authority = self
                .runtime
                .services
                .control
                .current(&self.runtime.services.scope)
                .await?
                .ready
                .view
                .authority();
            let pin = self
                .runtime
                .services
                .timestamps
                .begin_backup(authority, owner)
                .await?;
            Ok((
                pin,
                self.runtime
                    .services
                    .catalog
                    .snapshot(pin.read_ts, authority)?,
            ))
        })
    }
    fn help(&self, intent: IntentRef) -> ResolveFuture<'_, ()> {
        Box::pin(async move { self.runtime.help_intent(intent).await })
    }
    fn close(&self, proof: VerifiedBackupPublished) -> ResolveFuture<'_, ()> {
        Box::pin(async move {
            let authority = self
                .runtime
                .services
                .control
                .current(&self.runtime.services.scope)
                .await?
                .ready
                .view
                .authority();
            self.runtime
                .services
                .timestamps
                .release_backup(authority, proof)
                .await
        })
    }
}
impl WorkerRuntime {
    /// Data-only O20 migration verification. History-preserving backups use
    /// backup_native and must never enter this operation-based importer.
    pub(crate) async fn migration_snapshot(
        self: &Arc<Self>,
        destination: &Path,
        backup_id: Uuid,
        attempt: u64,
    ) -> Result<BackupManifest, ProtocolError> {
        BackupService::new(Arc::new(RuntimeBackup {
            runtime: self.clone(),
        }))
        .create(destination, BackupOwnerRef { backup_id, attempt })
        .await
    }
    async fn begin_import(
        &self,
        archive_id: Uuid,
        target_id: Uuid,
        phase: &'static str,
    ) -> Result<MetadataImport, ProtocolError> {
        let current = self.services.control.current(&self.services.scope).await?;
        if current.owner != self.services.local {
            return Err(unavailable());
        }
        let authority = current.ready.view.authority();
        let id = Uuid::new_v4();
        let digest = serialized_digest(&(
            b"offline-import-metadata/v1",
            archive_id,
            target_id,
            phase,
            id,
        ))?;
        let reservation = match self
            .services
            .status
            .reserve_identity(authority, id, digest, RequestKind::Mutation)
            .await?
            .reply
        {
            StatusReply::Identity(IdentityLookup::Reserved(reservation)) => reservation,
            _ => return Err(unknown()),
        };
        let txn = TxnId {
            request_id: id,
            attempt: 1,
        };
        let storage_pin = self
            .services
            .timestamps
            .begin_import_metadata(authority, archive_id, target_id, txn, reservation)
            .await?;
        let identity = PinIdentity {
            run_id: reservation.original_run,
            request_id: id,
            digest,
            nonce: reservation.reservation_id,
        };
        let pin = self
            .services
            .catalog
            .snapshot(storage_pin.read_ts, authority)?;
        let request = RequestBinding {
            run_id: authority.run_id,
            request_id: id,
            digest,
            accepted_context: serialized_digest(&(
                b"offline-import-context/v1",
                archive_id,
                target_id,
                phase,
                &pin,
            ))?,
        };
        self.services
            .timestamps
            .bind_snapshot_context(current.ready.view, identity.clone(), storage_pin, request)
            .await?;
        Ok(MetadataImport {
            pin,
            storage_pin,
            identity,
            txn,
            request,
        })
    }
    async fn commit_import(
        &self,
        attempt: MetadataImport,
        plan: MutationPlan,
    ) -> Result<(), ProtocolError> {
        let authority = self
            .services
            .control
            .current(&self.services.scope)
            .await?
            .ready
            .view
            .authority();
        let decision = self
            .transactions
            .commit(
                authority,
                attempt.txn,
                attempt.request,
                attempt.pin.read_ts,
                plan,
            )
            .await?;
        self.transactions.resolve(authority, &decision).await?;
        let settled = self
            .services
            .status
            .settled_proof(attempt.txn, decision.statement().manifest)
            .await?;
        self.services
            .timestamps
            .release_import_metadata(authority, attempt.identity, attempt.storage_pin, settled)
            .await?;
        decision.outcome().ok_or_else(unknown)?.result().map(|_| ())
    }
    async fn import_error(
        &self,
        attempt: MetadataImport,
        error: PlanError,
    ) -> Result<(), ProtocolError> {
        let mut error = error;
        let mut guards: Vec<VerifiedLifecycleGuard> = Vec::new();
        let failure = loop {
            match error {
                PlanError::Guarded {
                    cause,
                    guards: held,
                } => {
                    guards.extend(held);
                    error = *cause;
                }
                PlanError::Protocol(error) => break error,
                _ => break unknown(),
            }
        };
        if failure.outcome != Outcome::NotApplied {
            return Err(failure);
        }
        let authority = self
            .services
            .control
            .current(&self.services.scope)
            .await?
            .ready
            .view
            .authority();
        self.services
            .status
            .terminal_failure(authority, attempt.request, failure.clone())
            .await?;
        let settled = self
            .services
            .status
            .failure_settled_proof(attempt.txn, attempt.request)
            .await?;
        for guard in guards {
            self.services
                .catalog
                .participant()
                .release_lifecycle(authority, guard, settled.clone())
                .await?;
        }
        self.services
            .timestamps
            .release_import_metadata(authority, attempt.identity, attempt.storage_pin, settled)
            .await?;
        Err(failure)
    }
    async fn import_rows(
        &self,
        archive: Uuid,
        target: Uuid,
        table: Uuid,
        rows: Vec<protocol::Row>,
        owner: Arc<ScratchLease>,
    ) -> Result<(), ProtocolError> {
        if rows.is_empty() {
            return Ok(());
        }
        let attempt = self.begin_import(archive, target, "rows").await?;
        match self
            .planner
            .plan(
                protocol::Operation::InsertMany(protocol::BatchPayload {
                    table_id: table,
                    rows,
                }),
                &attempt.pin,
                attempt.txn,
                attempt.request,
                owner,
                None,
            )
            .await
        {
            Ok(PlannedOperation::Mutation(plan)) => self.commit_import(attempt, plan).await,
            Ok(_) => Err(unknown()),
            Err(error) => self.import_error(attempt, error).await,
        }
    }
    /// There is no live endpoint during this operation. A fully validated staged
    /// worker is drained before its directory is published at the target path.
    pub(crate) async fn import_offline(
        archive: &Path,
        destination: &Path,
        database: &str,
        token: &str,
        execution: ExecutionConfig,
        resources: WorkerResources,
    ) -> Result<ArchiveSummary, ProtocolError> {
        Self::import_source(
            ImportSource {
                archive: archive.to_path_buf(),
                expected: None,
            },
            destination,
            database,
            token,
            execution,
            resources,
        )
        .await
    }
    /// Read an earlier logical interchange snapshot strictly as migration.
    /// It cannot provide O19 identity/decision history or PITR semantics.
    pub(crate) async fn import_legacy_logical_snapshot(
        backup: &Path,
        destination: &Path,
        database: &str,
        token: &str,
        execution: ExecutionConfig,
        resources: WorkerResources,
    ) -> Result<ArchiveSummary, ProtocolError> {
        use std::io::Read;
        let mut bytes = Vec::new();
        File::open(backup.join("manifest.json"))
            .map_err(|_| unknown())?
            .take((64 << 10) + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| unknown())?;
        if bytes.len() > 64 << 10 {
            return Err(unknown());
        }
        protocol::json_input(&bytes, 64 << 10).map_err(|_| unknown())?;
        let manifest: BackupManifest = serde_json::from_slice(&bytes).map_err(|_| unknown())?;
        if manifest.format_version != 1
            || manifest.archive_id.is_nil()
            || manifest.owner.backup_id.is_nil()
            || manifest.owner.attempt == 0
        {
            return Err(unknown());
        }
        Self::import_source(
            ImportSource {
                archive: backup.join("snapshot.nab"),
                expected: Some(manifest),
            },
            destination,
            database,
            token,
            execution,
            resources,
        )
        .await
    }
    async fn import_source(
        source: ImportSource,
        destination: &Path,
        database: &str,
        token: &str,
        execution: ExecutionConfig,
        resources: WorkerResources,
    ) -> Result<ArchiveSummary, ProtocolError> {
        if destination.exists() {
            return Err(ProtocolError::new(
                ErrorCode::Conflict,
                "offline import requires a new empty target directory",
                Outcome::NotApplied,
            ));
        }
        let parent = destination
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let target_id = Uuid::new_v4();
        let stage = parent.join(format!(".nabedra-import-{target_id}.partial"));
        import_directory(&stage).map_err(|_| unknown())?;
        let cleanup = ImportDirectory(stage.clone());
        let runtime = Self::open(&stage, database, token, execution, resources).await?;
        let result = async {
            if let Some(expected) = &source.expected {
                if runtime
                    .planner
                    .hash_backup_file(source.archive.clone())
                    .await?
                    != expected.archive_sha256
                {
                    return Err(ProtocolError::new(
                        ErrorCode::ValidationError,
                        "backup archive checksum mismatch",
                        Outcome::NotApplied,
                    ));
                }
            }
            let summary = runtime
                .import_archive(
                    &source.archive,
                    target_id,
                    source
                        .expected
                        .as_ref()
                        .map(|m| (m.archive_id, *m.catalog_root.as_bytes())),
                )
                .await?;
            if source
                .expected
                .as_ref()
                .is_some_and(|expected| expected.summary != summary)
            {
                return Err(ProtocolError::new(
                    ErrorCode::ValidationError,
                    "backup manifest summary mismatch",
                    Outcome::NotApplied,
                ));
            }
            let verification = stage.join(".import-verification");
            runtime
                .migration_snapshot(&verification, Uuid::new_v4(), 1)
                .await?;
            runtime
                .verify_import(&source.archive, &verification.join("snapshot.nab"))
                .await?;
            std::fs::remove_dir_all(verification).map_err(|_| unknown())?;
            Ok::<_, ProtocolError>(summary)
        }
        .await;
        runtime.drain().await?;
        drop(runtime);
        let summary = result?;
        // This rename never targets an existing directory: reserve it first as
        // an empty marker, which rename replaces only while it remains empty.
        import_directory(destination).map_err(|_| unknown())?;
        std::fs::rename(&stage, destination).map_err(|_| unknown())?;
        File::open(parent)
            .and_then(|dir| dir.sync_all())
            .map_err(|_| unknown())?;
        drop(cleanup);
        Ok(summary)
    }
    async fn verify_import(&self, source: &Path, exported: &Path) -> Result<(), ProtocolError> {
        let owner = Arc::new(self.resources().reserve_scratch(
            ByteClass::Data,
            self.resources().task_memory_limit_bytes() / 2,
        )?);
        let (source, exported) = (source.to_path_buf(), exported.to_path_buf());
        let working = owner.bytes();
        let (mut original, mut restored) = self
            .resources()
            .execute(ExecutionClass::Query, 0, 0, move || {
                Ok((
                    ArchiveReader::new(File::open(source).map_err(|_| unknown())?, working)?,
                    ArchiveReader::new(File::open(exported).map_err(|_| unknown())?, working)?,
                ))
            })
            .await?;
        loop {
            let (next, a) = self.next_import_summary(original, owner.clone()).await?;
            original = next;
            let (next, b) = self.next_import_summary(restored, owner.clone()).await?;
            restored = next;
            let valid = match (a, b) {
                (None, None) => return Ok(()),
                (Some(Record::Table(a)), Some(Record::Table(b))) => a == b,
                (Some(Record::Index(a)), Some(Record::Index(b))) => a == b,
                (Some(Record::EndTable(a)), Some(Record::EndTable(b))) => a == b,
                (Some(Record::EndArchive(a)), Some(Record::EndArchive(b))) => {
                    a.tables == b.tables && a.rows == b.rows
                }
                _ => false,
            };
            if !valid {
                return Err(ProtocolError::new(
                    ErrorCode::StorageUnavailable,
                    "restored table UUID/schema/count/range hash differs from source",
                    Outcome::NotApplied,
                ));
            }
        }
    }
    async fn next_import_summary(
        &self,
        mut reader: ArchiveReader<File>,
        owner: Arc<ScratchLease>,
    ) -> Result<(ArchiveReader<File>, Option<Record>), ProtocolError> {
        loop {
            let native = self.resources().prepare(ExecutionClass::Query).await?;
            let cpu = native.reserve_compute(0, 0).await?;
            let (result, _guard) = native
                .run_with_guard((owner.clone(), cpu), move || {
                    let record = reader.next_record()?;
                    Ok((
                        reader,
                        match record {
                            Some(Record::Row(_) | Record::Header(_)) => Some(None),
                            Some(record) => Some(Some(record)),
                            None => None,
                        },
                    ))
                })
                .await?;
            let (next, record) = result?;
            reader = next;
            match record {
                Some(None) => {}
                Some(Some(record)) => return Ok((reader, Some(record))),
                None => return Ok((reader, None)),
            }
        }
    }
    async fn import_archive(
        self: &Arc<Self>,
        archive: &Path,
        target: Uuid,
        expected_header: Option<(Uuid, [u8; 32])>,
    ) -> Result<ArchiveSummary, ProtocolError> {
        let shared = self.resources();
        let owner = Arc::new(
            shared.reserve_scratch(ByteClass::Data, shared.task_memory_limit_bytes() / 2)?,
        );
        let path = archive.to_path_buf();
        let working = owner.bytes();
        let mut reader = shared
            .execute(ExecutionClass::Query, 0, 0, move || {
                ArchiveReader::new(File::open(path).map_err(|_| unknown())?, working)
            })
            .await?;
        let (mut archive_id, mut table, mut summary) = (None, None, None);
        let mut rows = Vec::new();
        let mut bytes = 0usize;
        loop {
            let native = shared.prepare(ExecutionClass::Query).await?;
            let cpu = native.reserve_compute(0, 0).await?;
            let (next, record) = native
                .run_with_guard((owner.clone(), cpu), move || {
                    let record = reader.next_record()?;
                    Ok((reader, record))
                })
                .await?
                .0?;
            reader = next;
            let Some(record) = record else { break };
            match record {
                Record::Header(header) => {
                    if expected_header.is_some_and(|(id, root)| {
                        id != header.archive_id || root != header.source_snapshot
                    }) {
                        return Err(ProtocolError::new(
                            ErrorCode::ValidationError,
                            "backup cut/header mismatch",
                            Outcome::NotApplied,
                        ));
                    }
                    archive_id = Some(header.archive_id);
                }
                Record::Table(descriptor) => {
                    let attempt = self
                        .begin_import(archive_id.ok_or_else(unknown)?, target, "table")
                        .await?;
                    match self
                        .planner
                        .plan_import_table(
                            descriptor.clone(),
                            &attempt.pin,
                            attempt.txn,
                            attempt.request,
                            owner.clone(),
                        )
                        .await
                    {
                        Ok(plan) => self.commit_import(attempt, plan).await?,
                        Err(error) => {
                            return self
                                .import_error(attempt, error)
                                .await
                                .map(|_| unreachable!());
                        }
                    };
                    table = Some(descriptor.id);
                }
                Record::Row(row) => {
                    let charge = row
                        .iter()
                        .map(|(name, value)| {
                            name.len()
                                + 768
                                + match value {
                                    protocol::Scalar::Utf8(s) => s.len() * 4,
                                    _ => 32,
                                }
                        })
                        .sum::<usize>();
                    if !rows.is_empty()
                        && (rows.len() == 128 || bytes.saturating_add(charge) > 8 << 20)
                    {
                        self.import_rows(
                            archive_id.ok_or_else(unknown)?,
                            target,
                            table.ok_or_else(unknown)?,
                            std::mem::take(&mut rows),
                            owner.clone(),
                        )
                        .await?;
                        bytes = 0;
                    }
                    if charge > owner.bytes() / 2 {
                        return Err(ProtocolError::new(
                            ErrorCode::LimitExceeded,
                            "import row exceeds configured working memory",
                            Outcome::NotApplied,
                        ));
                    }
                    bytes += charge;
                    rows.push(row);
                }
                Record::Index(index) => {
                    self.import_rows(
                        archive_id.ok_or_else(unknown)?,
                        target,
                        table.ok_or_else(unknown)?,
                        std::mem::take(&mut rows),
                        owner.clone(),
                    )
                    .await?;
                    bytes = 0;
                    self.create_index(
                        IndexDefinition {
                            index_id: index.index_id,
                            table_id: table.ok_or_else(unknown)?,
                            name: Text::new(index.name).map_err(|_| unknown())?,
                            columns: Page::new(
                                index
                                    .columns
                                    .into_iter()
                                    .map(Text::new)
                                    .collect::<Result<Vec<_>, _>>()
                                    .map_err(|_| unknown())?,
                            )
                            .map_err(|_| unknown())?,
                            covering: Page::new(
                                index
                                    .covering
                                    .into_iter()
                                    .map(Text::new)
                                    .collect::<Result<Vec<_>, _>>()
                                    .map_err(|_| unknown())?,
                            )
                            .map_err(|_| unknown())?,
                        },
                        Uuid::new_v4(),
                        1,
                    )
                    .await?;
                }
                Record::EndTable(_) => {
                    self.import_rows(
                        archive_id.ok_or_else(unknown)?,
                        target,
                        table.take().ok_or_else(unknown)?,
                        std::mem::take(&mut rows),
                        owner.clone(),
                    )
                    .await?;
                    bytes = 0;
                }
                Record::EndArchive(value) => summary = Some(value),
            }
        }
        summary.ok_or_else(unknown)
    }
}
struct ImportDirectory(PathBuf);
impl Drop for ImportDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn import_directory(path: &Path) -> std::io::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path)
}

struct ImportSource {
    archive: PathBuf,
    expected: Option<BackupManifest>,
}
