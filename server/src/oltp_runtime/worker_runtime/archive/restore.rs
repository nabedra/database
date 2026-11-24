use super::restore_graph::{RestoreGraph, SplitTranscript};
use super::*;
use crate::oltp::{
    backup::{
        history::{OwnedImage, split_child_scope},
        restore::{
            OfflineRestoreTarget, RestorePlanValidator, RestoredGenesis, VerifiedRestorePlan,
        },
    },
    mvcc::store::{
        index::{IndexClosureArchiveOrigin, install_restored_index_closure},
        split_closure::SplitArchiveOrigin,
    },
    pool::{OltpPool, TransactionBudget, WorkerSharedResources},
};
use crate::oltp_backup_repository::Repository;
use std::io::Seek;

pub(crate) struct VerifiedNativeRestore {
    downloaded: Vec<(File, IndexClosureArchiveOrigin)>,
    pub(crate) plan: VerifiedRestorePlan,
    target_cut: CommitTs,
    graph: RestoreGraph,
}
impl crate::oltp_replication::manager::RegisteredRestore for VerifiedNativeRestore {
    async fn verify(
        backups: &[PathBuf],
        target_cut: Option<CommitTs>,
        database: &str,
        target_run: Option<Uuid>,
        shared: Arc<WorkerSharedResources>,
        remote: Option<Arc<dyn Repository>>,
        keys: Arc<ArchiveKeys>,
    ) -> Result<Self, ProtocolError> {
        Self::verify(backups, target_cut, database, target_run, shared, remote, keys).await
    }

    fn plan(&self) -> &VerifiedRestorePlan {
        &self.plan
    }

    async fn install(
        self,
        target: OfflineRestoreTarget,
        pool: &OltpPool,
    ) -> Result<RestoredGenesis, ProtocolError> {
        let (genesis, _) = self.install(target, pool).await?;
        Ok(genesis)
    }
}

impl VerifiedNativeRestore {
    pub(crate) async fn verify(
        backups: &[PathBuf],
        target_cut: Option<CommitTs>,
        database: &str,
        target_run: Option<Uuid>,
        shared: Arc<WorkerSharedResources>,
        remote: Option<Arc<dyn Repository>>,
        keys: Arc<ArchiveKeys>,
    ) -> Result<Self, ProtocolError> {
        let last = backups.last().ok_or_else(unknown)?;
        let target_cut = match target_cut {
            Some(cut) => cut,
            None => {
                if remote.is_some() {
                    FilesystemRepository::open(last, shared.clone())?;
                }
                read_native_locator(last)?.pin.read_ts
            }
        };
        let mut downloaded = Vec::new();
        let mut plan = RestorePlanValidator::new(target_cut);
        let mut graph = RestoreGraph::new(shared.clone())?;
        // Fully verify immutable transport and the complete history before a
        // target capability exists. No partial target can become application ready.
        for backup in backups {
            let raw: Arc<dyn Repository> = match &remote {
                Some(remote) => {
                    FilesystemRepository::open(backup, shared.clone())?;
                    remote.clone()
                }
                None => FilesystemRepository::open(
                    &std::fs::canonicalize(backup).map_err(|_| unknown())?,
                    shared.clone(),
                )?,
            };
            let repository = ArchiveRepository::new(raw, keys.clone());
            let manifest = read_native_locator(backup)?;
            if manifest.scope.database.as_str() != database {
                return Err(unknown());
            }
            let mut reader = ArchiveReader::open(repository, manifest.key).await?;
            if publication::recorded_manifest(&reader)? != manifest {
                return Err(unknown());
            }
            let path =
                std::env::temp_dir().join(format!(".nabedra-restore-{}.nab", nabedra_rt::ids::uuid_v4()));
            let mut file = OpenOptions::new()
                .create_new(true)
                .mode(0o600)
                .read(true)
                .write(true)
                .open(&path)
                .map_err(|_| unknown())?;
            std::fs::remove_file(path).map_err(|_| unknown())?;
            while let Some(bytes) = reader.next().await? {
                file.write_all(bytes.as_slice()).map_err(|_| unknown())?;
            }
            reader.verify_complete()?;
            file.sync_all().map_err(|_| unknown())?;
            file.rewind().map_err(|_| unknown())?;
            let mut reader = HistoryReader::new(&mut file)?;
            let mut group = None;
            let mut split = SplitTranscript::default();
            let mut point_transcript = point_history::PointTranscript::default();
            while let Some(frame) = reader.next(&shared)? {
                split.observe(&graph, &frame, manifest.scope.cut)?;
                point_transcript.observe(frame.payload()?, &shared)?;
                match frame.payload()? {
                    HistoryFrame::StatusClosure(closure) => {
                        if target_run == Some(closure.authority.run_id) {
                            return Err(unknown());
                        }
                    }
                    HistoryFrame::Identity { identity, .. }
                        if target_run == Some(identity.reservation.original_run) =>
                    {
                        return Err(unknown());
                    }
                    HistoryFrame::BeginGroup(scope) => {
                        graph.group(&manifest.scope, scope, true)?;
                        group = Some(scope.clone());
                    }
                    HistoryFrame::EndGroup { .. } => group = None,
                    HistoryFrame::Image(image) => {
                        graph.image(
                            group.as_ref().ok_or_else(unknown)?,
                            &image.key,
                            image.value.as_ref(),
                            image.commit_ts,
                            true,
                        )?;
                    }
                    HistoryFrame::BeginSplit { row, publication } => {
                        let TxnDecision::Commit { commit_ts } = publication.status.decision else {
                            return Err(unknown());
                        };
                        if commit_ts <= target_cut {
                            for index in 0..row.children.len() {
                                let child = split_child_scope(row, index)?;
                                graph.group(&manifest.scope, &child, false)?;
                            }
                        }
                    }
                    HistoryFrame::CatalogIndexClosure(closure)
                        if manifest.scope.base_exclusive.is_none() =>
                    {
                        graph.stage_base_index_closure(closure)?;
                    }
                    HistoryFrame::ParticipantManifest(participant) => {
                        graph.verify_base_index_transaction(split.decision()?, participant)?;
                    }
                    HistoryFrame::ParticipantEffects(page) => {
                        if page.commit_ts > target_cut {
                            continue;
                        }
                        let scope = GroupImageScope {
                            group: page.group.clone(),
                            schema_epoch: page.plan.schema_epoch,
                            routing_epoch: page.plan.routing_epoch,
                            span: page.span.clone(),
                        };
                        graph.group(&manifest.scope, &scope, false)?;
                        for effect in page.effects.iter() {
                            graph.image(
                                &scope,
                                &effect.key,
                                effect.value.as_ref(),
                                page.commit_ts,
                                false,
                            )?;
                        }
                    }
                    _ => {}
                }
            }
            point_transcript.finish()?;
            let history = reader.verified()?;
            if history.scope() != &manifest.scope
                || history.root() != manifest.history_root
                || history.frames() != manifest.frames
            {
                return Err(unknown());
            }
            plan.include(&history)?;
            file.rewind().map_err(|_| unknown())?;
            downloaded.push((
                file,
                IndexClosureArchiveOrigin {
                    archive_id: manifest.scope.archive_id,
                    history_root: history.root(),
                    cut: manifest.scope.cut,
                },
            ));
        }
        graph.close(target_cut)?;
        Ok(Self {
            downloaded,
            plan: plan.finish()?,
            target_cut,
            graph,
        })
    }

    pub(crate) async fn install(
        mut self,
        target: OfflineRestoreTarget,
        pool: &OltpPool,
    ) -> Result<(RestoredGenesis, NativeRestoreSummary), ProtocolError> {
        let shared = pool.resources();
        let target = Arc::new(target);
        let target_cut = self.target_cut;
        let mut second = RestorePlanValidator::new(target_cut);
        let mut images = 0u64;
        let mut identities = 0u64;
        for (file, origin) in &mut self.downloaded {
            let split_origin = SplitArchiveOrigin {
                archive_id: origin.archive_id,
                history_root: origin.history_root,
                cut: origin.cut,
            };
            let mut reader = HistoryReader::new(file)?;
            let mut group = None;
            let mut split = None;
            let mut status = None;
            while let Some(frame) = reader.next(&shared)? {
                match frame.payload()? {
                    HistoryFrame::BeginGroup(scope) => {
                        let active = self.graph.active(&scope.group)?;
                        let store = pool
                            .group(map_native_group(
                                &scope.group,
                                target.statement().target_pool.cluster_id,
                            ))
                            .await?;
                        crate::oltp::mvcc::store::restore::install_images(
                            &store,
                            target.clone(),
                            scope.clone(),
                            active.clone(),
                            Vec::new(),
                        )
                        .await?;
                        group = Some((scope.clone(), active));
                    }
                    HistoryFrame::Image(image) => {
                        let (scope, active) = group.as_ref().ok_or_else(unknown)?;
                        if let Some(value) = &image.value {
                            install_native_maintenance(
                                &pool,
                                target.clone(),
                                scope.group.clone(),
                                &image.key,
                                value,
                            )
                            .await?;
                        }
                        let store = pool
                            .group(map_native_group(
                                &scope.group,
                                target.statement().target_pool.cluster_id,
                            ))
                            .await?;
                        let mut owned = OwnedImage::from_base(frame.clone(), &shared)?;
                        if let Some((row, publication)) = self
                            .graph
                            .split_for_install(&image.key, image.value.as_ref())?
                        {
                            owned.restore_split_publication(
                                &shared,
                                row,
                                publication,
                                split_origin,
                            )?;
                        }
                        crate::oltp::mvcc::store::restore::install_images(
                            &store,
                            target.clone(),
                            scope.clone(),
                            active.clone(),
                            vec![owned],
                        )
                        .await?;
                        images = images.checked_add(1).ok_or_else(unknown)?;
                    }
                    HistoryFrame::BeginSplit { row, publication } => {
                        let TxnDecision::Commit { commit_ts } = publication.status.decision else {
                            return Err(unknown());
                        };
                        if commit_ts > target_cut {
                            continue;
                        }
                        let scopes = [split_child_scope(row, 0)?, split_child_scope(row, 1)?];
                        for scope in &scopes {
                            if let Some(active) = self.graph.active(&scope.group)? {
                                let store = pool
                                    .group(map_native_group(
                                        &scope.group,
                                        target.statement().target_pool.cluster_id,
                                    ))
                                    .await?;
                                crate::oltp::mvcc::store::restore::install_images(
                                    &store,
                                    target.clone(),
                                    scope.clone(),
                                    Some(active),
                                    Vec::new(),
                                )
                                .await?;
                            }
                        }
                        split = Some((scopes, 0usize));
                    }
                    HistoryFrame::SplitImage(_) => {
                        if let Some((scopes, child)) = &split {
                            let scope = scopes.get(*child).ok_or_else(unknown)?;
                            if let Some(active) = self.graph.active(&scope.group)? {
                                let store = pool
                                    .group(map_native_group(
                                        &scope.group,
                                        target.statement().target_pool.cluster_id,
                                    ))
                                    .await?;
                                crate::oltp::mvcc::store::restore::install_images(
                                    &store,
                                    target.clone(),
                                    scope.clone(),
                                    Some(active),
                                    vec![OwnedImage::from_base(frame.clone(), &shared)?],
                                )
                                .await?;
                                images = images.checked_add(1).ok_or_else(unknown)?;
                            }
                        }
                    }
                    HistoryFrame::EndSplitChild { .. } => {
                        if let Some((_, child)) = &mut split {
                            *child += 1;
                            if *child == 2 {
                                split = None;
                            }
                        }
                    }
                    HistoryFrame::EndGroup { .. } => group = None,
                    HistoryFrame::CatalogIndexClosure(closure)
                        if closure.commit_ts <= target_cut =>
                    {
                        let genesis = target.statement();
                        let source_catalog = GroupIdentity {
                            cluster_id: genesis.source_cluster,
                            database: genesis.database.clone(),
                            group_id: genesis.catalog_group,
                            kind: GroupKind::Catalog,
                            group_format_version: GROUP_FORMAT_VERSION,
                        };
                        let catalog =
                            map_native_group(&source_catalog, genesis.target_pool.cluster_id);
                        let timestamps = GroupIdentity {
                            group_id: genesis.timestamp_group,
                            kind: GroupKind::Timestamps,
                            ..catalog.clone()
                        };
                        for destination in [catalog, timestamps] {
                            let store = pool.group(destination).await?;
                            install_restored_index_closure(
                                &store,
                                target.clone(),
                                &source_catalog,
                                closure.clone(),
                                *origin,
                            )
                            .await?;
                        }
                    }
                    HistoryFrame::StatusClosure(closure) => {
                        // Empty buckets also need retained stores, membership
                        // and a new target Status fence during bootstrap.
                        pool.group(map_native_group(
                            &closure.status,
                            target.statement().target_pool.cluster_id,
                        ))
                        .await?;
                        status = Some(closure.status.clone());
                    }
                    HistoryFrame::Identity { status, .. } => {
                        let store = pool
                            .group(map_native_group(
                                status,
                                target.statement().target_pool.cluster_id,
                            ))
                            .await?;
                        crate::oltp_replication::authority::status::identity::transaction::archive_images::install_identity_history(&store, target.clone(), frame.clone()).await?;
                        identities = identities.checked_add(1).ok_or_else(unknown)?;
                    }
                    HistoryFrame::BeginTransaction(_) => {
                        let source = status.as_ref().ok_or_else(unknown)?;
                        let store = pool
                            .group(map_native_group(
                                source,
                                target.statement().target_pool.cluster_id,
                            ))
                            .await?;
                        crate::oltp_replication::authority::status::identity::transaction::archive_images::install_archived_decision(&store, target.clone(), frame.clone(), source.clone()).await?;
                    }
                    HistoryFrame::ParticipantEffects(effects) => {
                        if effects.commit_ts > target_cut {
                            continue;
                        }
                        let scope = GroupImageScope {
                            group: effects.group.clone(),
                            schema_epoch: effects.plan.schema_epoch,
                            routing_epoch: effects.plan.routing_epoch,
                            span: effects.span.clone(),
                        };
                        let store = pool
                            .group(map_native_group(
                                &effects.group,
                                target.statement().target_pool.cluster_id,
                            ))
                            .await?;
                        for effect in effects.effects.iter() {
                            if let Some(value) = &effect.value {
                                install_native_maintenance(
                                    &pool,
                                    target.clone(),
                                    effects.group.clone(),
                                    &effect.key,
                                    value,
                                )
                                .await?;
                            }
                        }
                        let mut owned = Vec::with_capacity(effects.effects.len());
                        for (index, effect) in effects.effects.iter().enumerate() {
                            let mut image = OwnedImage::from_effect(frame.clone(), index, &shared)?;
                            if let Some((row, publication)) = self
                                .graph
                                .split_for_install(&effect.key, effect.value.as_ref())?
                            {
                                image.restore_split_publication(
                                    &shared,
                                    row,
                                    publication,
                                    split_origin,
                                )?;
                            }
                            owned.push(image);
                        }
                        images = images.checked_add(owned.len() as u64).ok_or_else(unknown)?;
                        crate::oltp::mvcc::store::restore::install_images(
                            &store,
                            target.clone(),
                            scope.clone(),
                            self.graph.active(&scope.group)?,
                            owned,
                        )
                        .await?;
                    }
                    HistoryFrame::Metadata(image) => {
                        let store = pool
                            .group(map_native_group(
                                &image.group,
                                target.statement().target_pool.cluster_id,
                            ))
                            .await?;
                        if image.group.kind == GroupKind::Timestamps {
                            crate::oltp_replication::authority::timestamp::archive::metadata::install_metadata(&store,target.clone(),frame.clone()).await?;
                        } else {
                            crate::oltp_replication::authority::status::identity::transaction::archive_images::install_metadata(&store, target.clone(), vec![frame.clone()]).await?;
                        }
                    }
                    _ => {}
                }
            }
            second.include(&reader.verified()?)?;
        }
        for scope in self.graph.empty_groups()? {
            let scope = scope?;
            let store = pool
                .group(map_native_group(
                    &scope.group,
                    target.statement().target_pool.cluster_id,
                ))
                .await?;
            crate::oltp::mvcc::store::restore::install_images(
                &store,
                target.clone(),
                scope.clone(),
                Some(scope),
                Vec::new(),
            )
            .await?;
        }
        let statement = target.statement().clone();
        let timestamp = GroupIdentity {
            cluster_id: statement.target_pool.cluster_id,
            database: statement.database.clone(),
            group_id: statement.timestamp_group,
            kind: GroupKind::Timestamps,
            group_format_version: GROUP_FORMAT_VERSION,
        };
        let store = pool.group(timestamp.clone()).await?;
        let watermark = statement.maximum_timestamp.get();
        store
            .restore_genesis_batch(
                target.clone(),
                TransactionBudget {
                    scratch_bytes: 64 << 10,
                    batch_bytes: 64 << 10,
                    output_bytes: 4096,
                },
                move |tx| {
                    tx.put_serialized(
                        b"ts/restored-history/genesis-watermark/v1",
                        &(timestamp, watermark),
                    )?;
                    Ok(())
                },
            )
            .await?;
        drop(store);
        let flush = pool.flush_restore(target.clone()).await?;
        let target = Arc::try_unwrap(target).map_err(|_| unknown())?;
        let genesis = target.finish_images(second.finish()?, flush)?;
        let summary = NativeRestoreSummary {
            source_cluster: statement.source_cluster,
            target_cluster: statement.target_pool.cluster_id,
            target_cut,
            segments: statement.segment_count,
            images,
            identities,
            history_root: statement.source_root,
        };
        Ok((genesis, summary))
    }
}
