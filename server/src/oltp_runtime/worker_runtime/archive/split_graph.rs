use super::*;
use crate::{
    oltp::{
        backup::history::split_child_scope,
        mvcc::{
            catalog::ShardRoute,
            store::{
                archive::ArchiveEffects,
                split_closure::{ArchivedSplitPublication, CatalogSplitClosed, split_closure_row},
            },
        },
    },
    oltp_replication::{authority::status::ArchivedDecision, types::ParticipantPlan},
};

#[derive(Serialize, Deserialize)]
struct StagedSplit {
    row: CatalogSplitClosed,
    publication: ArchivedSplitPublication,
    version: CommitTs,
    base: bool,
}

#[derive(Serialize, Deserialize)]
struct ChildBirth {
    txn: TxnId,
    index: usize,
}

#[derive(Serialize, Deserialize)]
struct GenesisRoute {
    scope: GroupImageScope,
    at: CommitTs,
}

#[derive(Default, Serialize, Deserialize)]
struct SplitEffects {
    table: bool,
    root: bool,
    row: bool,
    pages: usize,
    children: [Option<ChildPage>; 2],
    complete: bool,
}

#[derive(Serialize, Deserialize)]
struct ChildPage {
    page: Uuid,
    next: Option<Uuid>,
    offset: usize,
    last: bool,
}

#[derive(Default)]
pub(in crate::oltp_runtime::worker_runtime::archive) struct SplitTranscript {
    base_row: Option<(CatalogSplitClosed, CommitTs, Arc<ScratchLease>)>,
    decision: Option<(ArchivedDecision, Arc<ScratchLease>)>,
}

impl SplitTranscript {
    pub(in crate::oltp_runtime::worker_runtime::archive) fn decision(
        &self,
    ) -> Result<&ArchivedDecision, ProtocolError> {
        self.decision
            .as_ref()
            .map(|(decision, _)| decision)
            .ok_or_else(unknown)
    }

    pub(in crate::oltp_runtime::worker_runtime::archive) fn observe(
        &mut self,
        graph: &RestoreGraph,
        frame: &OwnedCommand<HistoryFrame>,
        cut: CommitTs,
    ) -> Result<(), ProtocolError> {
        match frame.payload()? {
            HistoryFrame::Image(image) => {
                if let Some(row) = split_closure_row(&image.key, image.value.as_ref())? {
                    self.base_row = Some((row, image.commit_ts, frame.decode_owner()?));
                }
            }
            HistoryFrame::CatalogSplitPublication(publication) => {
                let (row, version, _owner) = self.base_row.take().ok_or_else(unknown)?;
                graph.stage_split(&row, publication, version, true)?;
            }
            HistoryFrame::BeginSplit { row, publication } => {
                let TxnDecision::Commit { commit_ts } = publication.status.decision else {
                    return Err(unknown());
                };
                graph.stage_split(row, publication, commit_ts, false)?;
            }
            HistoryFrame::BeginGroup(scope)
                if matches!(scope.group.kind, GroupKind::Data { .. }) =>
            {
                let route = ShardRoute {
                    group: scope.group.clone(),
                    schema_epoch: scope.schema_epoch,
                    routing_epoch: scope.routing_epoch,
                    span: scope.span.clone(),
                };
                if !graph.verify_split_route(&route, cut)? && route.routing_epoch > 1 {
                    return Err(unknown());
                }
            }
            HistoryFrame::BeginTransaction(decision) => {
                self.decision = Some((decision.clone(), frame.decode_owner()?));
            }
            HistoryFrame::ParticipantManifest(plan) => {
                graph.verify_base_split_transaction(self.decision()?, plan)?;
            }
            HistoryFrame::ParticipantEffects(page) => {
                graph.verify_split_effects(page, self.decision()?)?;
            }
            HistoryFrame::EndTransaction { .. } => self.decision = None,
            _ => {}
        }
        Ok(())
    }
}

impl RestoreGraph {
    fn split_path(&self, directory: &str, txn: TxnId) -> PathBuf {
        self.path
            .join(directory)
            .join(format!("{}-{}", txn.request_id, txn.attempt))
    }

    fn read_split(&self, txn: TxnId) -> Result<Option<Staged<StagedSplit>>, ProtocolError> {
        self.read(&self.split_path("splits", txn))
    }

    pub(in crate::oltp_runtime::worker_runtime::archive) fn stage_split(
        &self,
        row: &CatalogSplitClosed,
        publication: &ArchivedSplitPublication,
        version: CommitTs,
        base: bool,
    ) -> Result<(), ProtocolError> {
        let txn = row.guard.binding().txn;
        self.create(
            &self.split_path("splits", txn),
            &StagedSplit {
                row: row.clone(),
                publication: publication.clone(),
                version,
                base,
            },
        )?;
        self.create(
            &self
                .path
                .join("split-sources")
                .join(row.source.group_id.to_string()),
            &txn,
        )?;
        for (index, child) in row.children.iter().enumerate() {
            self.create(
                &self
                    .path
                    .join("split-children")
                    .join(child.group.group_id.to_string()),
                &ChildBirth { txn, index },
            )?;
        }
        Ok(())
    }

    pub(in crate::oltp_runtime::worker_runtime::archive) fn split_for_install(
        &self,
        key: &MvccKey,
        value: Option<&CanonicalJson>,
    ) -> Result<Option<(CatalogSplitClosed, ArchivedSplitPublication)>, ProtocolError> {
        let Some(row) = split_closure_row(key, value)? else {
            return Ok(None);
        };
        let split = self
            .read_split(row.guard.binding().txn)?
            .ok_or_else(unknown)?;
        if split.value.row != row {
            return Err(unknown());
        }
        Ok(Some((row, split.value.publication)))
    }

    pub(in crate::oltp_runtime::worker_runtime::archive) fn verify_base_split_transaction(
        &self,
        decision: &ArchivedDecision,
        plan: &ParticipantPlan,
    ) -> Result<(), ProtocolError> {
        if let Some(split) = self.read_split(decision.statement.txn)? {
            let split = &split.value;
            if !split.base
                || split.publication.first_archive.is_some()
                || split.publication.status != decision.statement
                || split.publication.catalog.group != plan.group
                || decision.read_ts != split.row.guard.binding().read_ts
                || !(4..=5).contains(&plan.write_count)
            {
                return Err(unknown());
            }
            self.create(
                &self.split_path("split-progress", decision.statement.txn),
                &SplitEffects {
                    complete: true,
                    ..Default::default()
                },
            )?;
        }
        Ok(())
    }

    pub(in crate::oltp_runtime::worker_runtime::archive) fn verify_split_effects(
        &self,
        page: &ArchiveEffects,
        decision: &ArchivedDecision,
    ) -> Result<(), ProtocolError> {
        let staged = self.read_split(page.txn)?;
        let staged = staged.as_ref().map(|split| &split.value);
        if page.group.kind != GroupKind::Catalog {
            return if staged.is_none() {
                Ok(())
            } else {
                Err(unknown())
            };
        }
        let path = self.split_path("split-progress", page.txn);
        let mut progress = self
            .read::<SplitEffects>(&path)?
            .map(|p| p.value)
            .unwrap_or_default();
        for effect in page.effects.iter() {
            let raw = effect.value.as_ref().map(CanonicalJson::as_str);
            if let MvccKey::CatalogRoutingPage { .. } = &effect.key {
                if let Some(raw) = raw {
                    let routes: RoutingPage = serde_json::from_str(raw).map_err(|_| unknown())?;
                    self.stage_genesis_routes(&routes, page.commit_ts)?;
                }
            }
            let Some(split) = staged else {
                if let MvccKey::CatalogRoutingRoot { .. } = &effect.key {
                    if let Some(raw) = raw {
                        let root: RoutingRoot = serde_json::from_str(raw).map_err(|_| unknown())?;
                        if root.routing_epoch > 1 {
                            return Err(unknown());
                        }
                    }
                }
                if matches!(effect.key, MvccKey::CatalogSplitClosed { .. }) {
                    return Err(unknown());
                }
                continue;
            };
            let row = &split.row;
            let table = row.guard.binding().table_id;
            match &effect.key {
                MvccKey::CatalogTable { table_id } if *table_id == table && !progress.table => {
                    let record: TableRecord =
                        serde_json::from_str(raw.ok_or_else(unknown)?).map_err(|_| unknown())?;
                    if record.descriptor.id != table
                        || record.schema_epoch != row.source_schema_epoch
                        || record.routing_epoch != row.published_routing_epoch
                    {
                        return Err(unknown());
                    }
                    progress.table = true;
                }
                MvccKey::CatalogRoutingRoot { table_id }
                    if *table_id == table && !progress.root =>
                {
                    let root: RoutingRoot =
                        serde_json::from_str(raw.ok_or_else(unknown)?).map_err(|_| unknown())?;
                    if root.first_page.is_none()
                        || root.schema_epoch != row.source_schema_epoch
                        || root.routing_epoch != row.published_routing_epoch
                    {
                        return Err(unknown());
                    }
                    progress.root = true;
                }
                MvccKey::CatalogRoutingPage { table_id, page_id } if *table_id == table => {
                    let routes: RoutingPage =
                        serde_json::from_str(raw.ok_or_else(unknown)?).map_err(|_| unknown())?;
                    progress.route_page(row, *page_id, &routes)?;
                }
                MvccKey::CatalogSplitClosed { .. } if !progress.row => {
                    if split_closure_row(&effect.key, effect.value.as_ref())?.as_ref() != Some(row)
                    {
                        return Err(unknown());
                    }
                    progress.row = true;
                }
                _ => return Err(unknown()),
            }
        }
        if let Some(split) = staged {
            if split.base
                || split.publication.status != decision.statement
                || split.publication.catalog.group != page.group
                || split.publication.catalog.manifest != page.manifest
                || decision.read_ts != split.row.guard.binding().read_ts
                || page.commit_ts != split.version
                || progress.complete
            {
                return Err(unknown());
            }
            if page.complete {
                progress.close(page.plan.write_count)?;
            }
            let file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .mode(0o600)
                .open(path)
                .map_err(|_| unknown())?;
            serde_json::to_writer(file, &progress).map_err(|_| unknown())?;
        }
        Ok(())
    }

    fn stage_genesis_routes(
        &self,
        routes: &RoutingPage,
        at: CommitTs,
    ) -> Result<(), ProtocolError> {
        for route in routes
            .shards
            .iter()
            .filter(|route| route.routing_epoch == 1)
        {
            let scope = GroupImageScope {
                group: route.group.clone(),
                span: route.span.clone(),
                schema_epoch: route.schema_epoch,
                routing_epoch: route.routing_epoch,
            };
            validate_archive_scope(&scope)?;
            let path = self
                .path
                .join("split-genesis-routes")
                .join(route.group.group_id.to_string());
            if let Some(prior) = self.read::<GenesisRoute>(&path)? {
                if prior.value.scope != scope {
                    return Err(unknown());
                }
                if prior.value.at <= at {
                    continue;
                }
                std::fs::remove_file(&path).map_err(|_| unknown())?;
            }
            self.create(&path, &GenesisRoute { scope, at })?;
        }
        Ok(())
    }

    fn child_birth(&self, group: Uuid) -> Result<Option<Staged<ChildBirth>>, ProtocolError> {
        self.read(&self.path.join("split-children").join(group.to_string()))
    }

    pub(in crate::oltp_runtime::worker_runtime::archive) fn verify_split_inventory(
        &self,
        verify_source: bool,
    ) -> Result<(), ProtocolError> {
        for entry in std::fs::read_dir(self.path.join("splits")).map_err(|_| unknown())? {
            let split: Staged<StagedSplit> = self
                .read(&entry.map_err(|_| unknown())?.path())?
                .ok_or_else(unknown)?;
            let split = &split.value;
            if split.publication.first_archive.is_none()
                && self
                    .read::<SplitEffects>(
                        &self.split_path("split-progress", split.row.guard.binding().txn),
                    )?
                    .is_none_or(|progress| !progress.value.complete)
            {
                return Err(unknown());
            }
            if verify_source && !split.base {
                self.verify_split_source(split)?;
            }
        }
        Ok(())
    }

    fn verify_split_source(&self, split: &StagedSplit) -> Result<(), ProtocolError> {
        let row = &split.row;
        let guard_cut = row.guard.binding().read_ts;
        let source = GroupImageScope {
            group: row.source.clone(),
            schema_epoch: row.source_schema_epoch,
            routing_epoch: row.source_routing_epoch,
            span: row.source_span.clone(),
        };
        if let Some(birth) = self.child_birth(source.group.group_id)? {
            let parent = self.read_split(birth.value.txn)?.ok_or_else(unknown)?;
            let mut born = split_child_scope(&parent.value.row, birth.value.index)?;
            if parent.value.publication.first_archive.is_some() {
                born.group.cluster_id = source.group.cluster_id;
            }
            if born != source || parent.value.version > guard_cut {
                return Err(unknown());
            }
            return Ok(());
        }
        let base = self.read::<SourceGroup>(
            &self
                .path
                .join("source")
                .join(source.group.group_id.to_string()),
        )?;
        if base.is_some_and(|base| base.value.has_base && base.value.scope == source) {
            return Ok(());
        }
        let genesis = self.read::<GenesisRoute>(
            &self
                .path
                .join("split-genesis-routes")
                .join(source.group.group_id.to_string()),
        )?;
        if genesis
            .is_some_and(|genesis| genesis.value.scope == source && genesis.value.at <= guard_cut)
        {
            return Ok(());
        }
        Err(unknown())
    }

    pub(super) fn verify_split_route(
        &self,
        route: &ShardRoute,
        cut: CommitTs,
    ) -> Result<bool, ProtocolError> {
        let group = route.group.group_id;
        if let Some(retired) =
            self.read::<TxnId>(&self.path.join("split-sources").join(group.to_string()))?
        {
            if self
                .read_split(retired.value)?
                .ok_or_else(unknown)?
                .value
                .version
                <= cut
            {
                return Err(unknown());
            }
        }
        let Some(birth) = self.child_birth(group)? else {
            return Ok(false);
        };
        let split = self.read_split(birth.value.txn)?.ok_or_else(unknown)?;
        let split = &split.value;
        let mut child = split_child_scope(&split.row, birth.value.index)?;
        if split.publication.first_archive.is_some() {
            child.group.cluster_id = route.group.cluster_id;
        }
        if child.group != route.group
            || child.span != route.span
            || child.schema_epoch != route.schema_epoch
            || child.routing_epoch != route.routing_epoch
            || split.version > cut
        {
            return Err(unknown());
        }
        Ok(true)
    }
}

impl SplitEffects {
    fn route_page(
        &mut self,
        row: &CatalogSplitClosed,
        page: Uuid,
        routes: &RoutingPage,
    ) -> Result<(), ProtocolError> {
        self.pages += 1;
        if self.pages > 2 || routes.shards.is_empty() || routes.next_page == Some(page) {
            return Err(unknown());
        }
        for (offset, route) in routes.shards.iter().enumerate() {
            if route.group.group_id == row.source.group_id
                || (offset > 0
                    && (route.span.start.is_none()
                        || routes.shards[offset - 1].span.end != route.span.start))
            {
                return Err(unknown());
            }
            for (index, child) in row.children.iter().enumerate() {
                if child.group.group_id != route.group.group_id {
                    continue;
                }
                if self.children[index].is_some()
                    || route.group != child.group
                    || route.span != child.span
                    || route.schema_epoch != row.source_schema_epoch
                    || route.routing_epoch != row.child_epoch()?
                {
                    return Err(unknown());
                }
                self.children[index] = Some(ChildPage {
                    page,
                    next: routes.next_page,
                    offset,
                    last: offset + 1 == routes.shards.len(),
                });
            }
        }
        Ok(())
    }

    fn close(&mut self, write_count: u64) -> Result<(), ProtocolError> {
        let [Some(left), Some(right)] = &self.children else {
            return Err(unknown());
        };
        let adjacent = if left.page == right.page {
            left.offset + 1 == right.offset
        } else {
            left.last && left.next == Some(right.page) && right.offset == 0
        };
        if !adjacent
            || !self.table
            || !self.root
            || !self.row
            || write_count != self.pages as u64 + 3
        {
            return Err(unknown());
        }
        self.complete = true;
        Ok(())
    }
}
