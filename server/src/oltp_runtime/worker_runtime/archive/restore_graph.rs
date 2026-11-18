use super::*;
use crate::oltp::{
    mvcc::{
        CanonicalJson,
        catalog::{RoutingPage, RoutingRoot, TableRecord},
        index::{
            IndexAttemptClosedRecord, IndexClosedKind, IndexNameRecord, IndexPage, IndexPhase,
            IndexRecord, IndexRoot, closure_id,
        },
        store::{
            index::{ArchivedIndexClosure, phase_certifies_closure},
            restore::{validate_archive_image, validate_archive_scope},
        },
    },
    pool::{ScratchLease, WorkerSharedResources},
};

const STAGED_RECORD_BYTES: usize = 2 * crate::oltp::mvcc::MAX_FIXED_COMMAND_BYTES + 4096;
#[path = "split_graph.rs"]
mod split_graph;
pub(super) use split_graph::SplitTranscript;

/// Store latest visible Catalog records and exact active groups on disk. The
/// archive can contain arbitrarily many table names, pages or obsolete groups.
pub(super) struct RestoreGraph {
    path: PathBuf,
    catalog: Option<GroupImageScope>,
    shared: Arc<WorkerSharedResources>,
}

#[derive(Serialize, Deserialize)]
struct Version {
    key: MvccKey,
    timestamp: CommitTs,
    value: Option<String>,
    present_at_base: bool,
}

struct Staged<T> {
    value: T,
    _memory: ScratchLease,
}

#[derive(Serialize, Deserialize)]
struct SourceGroup {
    scope: GroupImageScope,
    has_base: bool,
}

impl RestoreGraph {
    pub(super) fn new(shared: Arc<WorkerSharedResources>) -> Result<Self, ProtocolError> {
        let path = std::env::temp_dir().join(format!("nabedra-restore-graph-{}", Uuid::new_v4()));
        ensure_private_directory(&path)?;
        let graph = Self {
            path,
            catalog: None,
            shared,
        };
        for child in [
            "names",
            "catalog",
            "source",
            "active",
            "empty",
            "visited-pages",
            "index-closure-transactions",
            "index-roots",
            "visited-index-pages",
            "visited-indexes",
            "splits",
            "split-progress",
            "split-children",
            "split-sources",
            "split-genesis-routes",
        ] {
            ensure_private_directory(&graph.path.join(child))?;
        }
        Ok(graph)
    }

    pub(super) fn group(
        &mut self,
        archive: &HistoryScope,
        scope: &GroupImageScope,
        base: bool,
    ) -> Result<(), ProtocolError> {
        validate_archive_scope(scope)?;
        if scope.group.cluster_id != archive.logical_cluster
            || scope.group.database != archive.database
        {
            return Err(unknown());
        }
        if scope.group.kind == GroupKind::Catalog {
            if scope.group.group_id != archive.catalog_group
                || scope.span.start.is_some()
                || scope.span.end.is_some()
            {
                return Err(unknown());
            }
            if base && self.catalog.replace(scope.clone()).is_some() {
                return Err(unknown());
            }
            return Ok(());
        }
        if archive.system_groups().any(|id| id == scope.group.group_id) {
            return Err(unknown());
        }
        let path = self
            .path
            .join("source")
            .join(scope.group.group_id.to_string());
        if let Some(previous) = self.read::<SourceGroup>(&path)? {
            if base
                || previous.value.scope.group != scope.group
                || previous.value.scope.span != scope.span
            {
                return Err(unknown());
            }
            return Ok(());
        }
        self.create(
            &path,
            &SourceGroup {
                scope: scope.clone(),
                has_base: base,
            },
        )
    }

    fn record_path(&self, key: &MvccKey) -> Result<PathBuf, ProtocolError> {
        let directory = match key {
            MvccKey::CatalogName { .. } => "names",
            MvccKey::CatalogIndexesRoot { .. } => "index-roots",
            _ => "catalog",
        };
        let digest = serialized_digest(key)?;
        Ok(self.path.join(directory).join(digest.to_string()))
    }

    pub(super) fn image(
        &self,
        scope: &GroupImageScope,
        key: &MvccKey,
        value: Option<&CanonicalJson>,
        timestamp: CommitTs,
        base: bool,
    ) -> Result<(), ProtocolError> {
        validate_archive_image(scope, key)?;
        if !matches!(
            key,
            MvccKey::CatalogName { .. }
                | MvccKey::CatalogTable { .. }
                | MvccKey::CatalogRoutingRoot { .. }
                | MvccKey::CatalogRoutingPage { .. }
                | MvccKey::CatalogIndex { .. }
                | MvccKey::CatalogIndexClosed { .. }
                | MvccKey::CatalogSplitClosed { .. }
                | MvccKey::CatalogIndexName { .. }
                | MvccKey::CatalogIndexesRoot { .. }
                | MvccKey::CatalogIndexesPage { .. }
        ) {
            return Ok(());
        }
        let path = self.record_path(key)?;
        let old = self.read::<Version>(&path)?;
        if let Some(previous) = &old {
            if previous.value.key != *key {
                return Err(unknown());
            }
            if matches!(
                key,
                MvccKey::CatalogIndexClosed { .. } | MvccKey::CatalogSplitClosed { .. }
            ) && previous.value.timestamp != timestamp
            {
                return Err(unknown());
            }
            if previous.value.timestamp > timestamp {
                return Ok(());
            }
            if previous.value.timestamp == timestamp {
                if previous.value.value.as_deref() != value.map(CanonicalJson::as_str) {
                    return Err(unknown());
                }
                return Ok(());
            }
        }
        let memory = value.map_or(0, |value| value.as_str().len());
        let _memory = self
            .shared
            .reserve_scratch(ByteClass::Data, memory + key.heap_bytes())?;
        let version = Version {
            key: key.clone(),
            timestamp,
            value: value.map(|value| value.as_str().to_owned()),
            present_at_base: if base {
                value.is_some()
            } else {
                old.is_some_and(|previous| previous.value.present_at_base)
            },
        };
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(0o600)
            .open(path)
            .map_err(|_| unknown())?;
        serde_json::to_writer(file, &version).map_err(|_| unknown())
    }

    fn read<T: serde::de::DeserializeOwned>(
        &self,
        path: &Path,
    ) -> Result<Option<Staged<T>>, ProtocolError> {
        let file = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(unknown()),
        };
        let len = usize::try_from(file.metadata().map_err(|_| unknown())?.len())
            .map_err(|_| unknown())?;
        if len > STAGED_RECORD_BYTES {
            return Err(unknown());
        }
        let _bytes = self.shared.reserve_scratch(ByteClass::Data, len)?;
        let mut bytes = Vec::with_capacity(len);
        file.take((len as u64) + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| unknown())?;
        if bytes.len() != len {
            return Err(unknown());
        }
        let memory = self.shared.reserve_scratch(
            ByteClass::Data,
            crate::oltp::mvcc::json_decode_budget(&bytes)?,
        )?;
        let value = serde_json::from_slice(&bytes).map_err(|_| unknown())?;
        Ok(Some(Staged {
            value,
            _memory: memory,
        }))
    }

    fn create(&self, path: &Path, value: &impl Serialize) -> Result<(), ProtocolError> {
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(path)
            .map_err(|_| unknown())?;
        serde_json::to_writer(file, value).map_err(|_| unknown())
    }

    fn value<T: serde::de::DeserializeOwned>(
        &self,
        version: &Version,
    ) -> Result<Staged<T>, ProtocolError> {
        let value = version.value.as_ref().ok_or_else(unknown)?;
        let memory = self.shared.reserve_scratch(
            ByteClass::Data,
            crate::oltp::mvcc::json_decode_budget(value.as_bytes())?,
        )?;
        let value = serde_json::from_str(value).map_err(|_| unknown())?;
        Ok(Staged {
            value,
            _memory: memory,
        })
    }

    fn record(&self, key: &MvccKey) -> Result<Staged<Version>, ProtocolError> {
        let found: Staged<Version> = self.read(&self.record_path(key)?)?.ok_or_else(unknown)?;
        if found.value.key != *key {
            return Err(unknown());
        }
        Ok(found)
    }

    fn index_closure_txn(&self, txn: TxnId) -> PathBuf {
        self.path
            .join("index-closure-transactions")
            .join(format!("{}-{}", txn.request_id, txn.attempt))
    }

    pub(super) fn stage_base_index_closure(
        &self,
        closure: &ArchivedIndexClosure,
    ) -> Result<(), ProtocolError> {
        if closure.first_archive.is_none() {
            self.create(&self.index_closure_txn(closure.txn), closure)?;
        }
        Ok(())
    }

    pub(super) fn verify_base_index_transaction(
        &self,
        decision: &crate::oltp_replication::authority::status::ArchivedDecision,
        plan: &ParticipantPlan,
    ) -> Result<(), ProtocolError> {
        let path = self.index_closure_txn(decision.statement.txn);
        if let Some(closure) = self.read::<ArchivedIndexClosure>(&path)? {
            let closure = closure.value;
            if closure.source != plan.group
                || decision.statement.txn != closure.txn
                || decision.statement.manifest != closure.manifest
                || decision.statement.decision
                    != (TxnDecision::Commit {
                        commit_ts: closure.commit_ts,
                    })
            {
                return Err(unknown());
            }
            std::fs::remove_file(path).map_err(|_| unknown())?;
        }
        Ok(())
    }

    pub(super) fn close(&self, cut: CommitTs) -> Result<(), ProtocolError> {
        if std::fs::read_dir(self.path.join("index-closure-transactions"))
            .map_err(|_| unknown())?
            .next()
            .is_some()
        {
            return Err(unknown());
        }
        let catalog = self.catalog.as_ref().ok_or_else(unknown)?;
        self.verify_split_inventory(true)?;
        for entry in std::fs::read_dir(self.path.join("index-roots")).map_err(|_| unknown())? {
            let version: Staged<Version> = self
                .read(&entry.map_err(|_| unknown())?.path())?
                .ok_or_else(unknown)?;
            let MvccKey::CatalogIndexesRoot { table_id } = version.value.key else {
                return Err(unknown());
            };
            let root = self.value::<IndexRoot>(&version.value)?;
            self.validate_index_inventory(table_id, &root.value)?;
        }
        for entry in std::fs::read_dir(self.path.join("names")).map_err(|_| unknown())? {
            let entry = entry.map_err(|_| unknown())?;
            let name: Staged<Version> = self.read(&entry.path())?.ok_or_else(unknown)?;
            let MvccKey::CatalogName { name: table_name } = &name.value.key else {
                return Err(unknown());
            };
            if name.value.value.is_none() {
                continue;
            }
            let table_id = self.value::<NameRecord>(&name.value)?.value.table_id;
            let version = self.record(&MvccKey::CatalogTable { table_id })?;
            let table = self.value::<TableRecord>(&version.value)?;
            let table = &table.value;
            table.descriptor.validate()?;
            if table_id.is_nil()
                || table.descriptor.id != table_id
                || table.descriptor.name != table_name.as_str()
            {
                return Err(unknown());
            }
            let root = self.record(&MvccKey::CatalogRoutingRoot { table_id })?;
            let root = self.value::<RoutingRoot>(&root.value)?;
            if root.value.schema_epoch != table.schema_epoch
                || root.value.routing_epoch != table.routing_epoch
                || root.value.first_page.is_none()
            {
                return Err(unknown());
            }
            let mut page_id = root.value.first_page;
            let mut previous_end = None;
            let mut seen_span = false;
            let key_type = *table
                .descriptor
                .schema
                .get(&table.descriptor.key)
                .ok_or_else(unknown)?;
            while let Some(current) = page_id {
                if current.is_nil() {
                    return Err(unknown());
                }
                self.create(
                    &self
                        .path
                        .join("visited-pages")
                        .join(format!("{table_id}-{current}")),
                    &(),
                )?;
                let page = self.record(&MvccKey::CatalogRoutingPage {
                    table_id,
                    page_id: current,
                })?;
                let page = self.value::<RoutingPage>(&page.value)?;
                if page.value.shards.is_empty() {
                    return Err(unknown());
                }
                for route in page.value.shards.iter() {
                    validate_archive_scope(&GroupImageScope {
                        group: route.group.clone(),
                        schema_epoch: route.schema_epoch,
                        routing_epoch: route.routing_epoch,
                        span: route.span.clone(),
                    })?;
                    for bound in [&route.span.start, &route.span.end].into_iter().flatten() {
                        bound.validate(key_type)?;
                    }
                    if route.group.cluster_id != catalog.group.cluster_id
                        || route.group.database != catalog.group.database
                        || !matches!(route.group.kind, GroupKind::Data { table_id: id, .. } if id == table_id)
                        || route.schema_epoch != table.schema_epoch
                        || route.routing_epoch > table.routing_epoch
                        || route.span.start != previous_end
                        || (seen_span && previous_end.is_none())
                    {
                        return Err(unknown());
                    }
                    let source: Option<Staged<SourceGroup>> = self.read(
                        &self
                            .path
                            .join("source")
                            .join(route.group.group_id.to_string()),
                    )?;
                    if source.as_ref().is_some_and(|source| {
                        source.value.scope.group != route.group
                            || source.value.scope.span != route.span
                    }) {
                        return Err(unknown());
                    }
                    // An existing table's newly routed group needs a copied
                    // baseline; post-base Status effects alone cannot recreate it.
                    let has_baseline = source.as_ref().is_some_and(|source| source.value.has_base);
                    let has_split_baseline = self.verify_split_route(route, cut)?;
                    if (version.value.present_at_base && !has_baseline && !has_split_baseline)
                        || (route.routing_epoch > 1 && !has_split_baseline)
                    {
                        return Err(unknown());
                    }
                    let scope = GroupImageScope {
                        group: route.group.clone(),
                        schema_epoch: route.schema_epoch,
                        routing_epoch: route.routing_epoch,
                        span: route.span.clone(),
                    };
                    self.create(
                        &self
                            .path
                            .join("active")
                            .join(route.group.group_id.to_string()),
                        &scope,
                    )?;
                    if source.is_none() {
                        self.create(
                            &self
                                .path
                                .join("empty")
                                .join(route.group.group_id.to_string()),
                            &scope,
                        )?;
                    }
                    previous_end = route.span.end.clone();
                    seen_span = true;
                }
                page_id = page.value.next_page;
            }
            if !seen_span || previous_end.is_some() {
                return Err(unknown());
            }
        }
        for entry in std::fs::read_dir(self.path.join("catalog")).map_err(|_| unknown())? {
            let version: Staged<Version> = self
                .read(&entry.map_err(|_| unknown())?.path())?
                .ok_or_else(unknown)?;
            match &version.value.key {
                MvccKey::CatalogTable { table_id } if version.value.value.is_some() => {
                    let table = self.value::<TableRecord>(&version.value)?;
                    let name = Text::try_from(table.value.descriptor.name.as_str())
                        .map_err(|_| unknown())?;
                    let named = self.record(&MvccKey::CatalogName { name })?;
                    if self.value::<NameRecord>(&named.value)?.value.table_id != *table_id {
                        return Err(unknown());
                    }
                }
                MvccKey::CatalogIndex { table_id, index_id } => {
                    let index = self.value::<IndexRecord>(&version.value)?;
                    if index.value.definition.table_id != *table_id
                        || index.value.definition.index_id != *index_id
                        || !self
                            .index_inventory_path("visited-indexes", *table_id, *index_id)
                            .is_file()
                    {
                        return Err(unknown());
                    }
                    if !matches!(index.value.phase, IndexPhase::Building { .. }) {
                        let (build_id, attempt, _) = index.value.phase.attempt_binding();
                        let owner = MaintenanceOwnerRef {
                            catalog_group_id: catalog.group.group_id,
                            table_id: *table_id,
                            index_id: *index_id,
                            build_id,
                            attempt,
                        };
                        let closed = self.record(&MvccKey::CatalogIndexClosed {
                            table_id: *table_id,
                            closure_id: closure_id(&owner)?,
                        })?;
                        self.validate_index_closure(&closed.value, &version.value, &index.value)?;
                    }
                }
                MvccKey::CatalogIndexClosed { .. } => {
                    let closure = self.value::<IndexAttemptClosedRecord>(&version.value)?;
                    let owner = closure.value.owner;
                    let index = self.record(&MvccKey::CatalogIndex {
                        table_id: owner.table_id,
                        index_id: owner.index_id,
                    })?;
                    let phase = self.value::<IndexRecord>(&index.value)?;
                    self.validate_index_closure(&version.value, &index.value, &phase.value)?;
                }
                MvccKey::CatalogIndexesPage { table_id, page_id } => {
                    if !self
                        .index_inventory_path("visited-index-pages", *table_id, *page_id)
                        .is_file()
                    {
                        return Err(unknown());
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn index_inventory_path(&self, directory: &str, table_id: Uuid, id: Uuid) -> PathBuf {
        self.path.join(directory).join(format!("{table_id}-{id}"))
    }

    fn validate_index_inventory(
        &self,
        table_id: Uuid,
        root: &IndexRoot,
    ) -> Result<(), ProtocolError> {
        let mut page_id = root.first_page;
        let mut last = None;
        let mut total = 0u64;
        while let Some(id) = page_id {
            self.create(
                &self.index_inventory_path("visited-index-pages", table_id, id),
                &(),
            )?;
            let page = self.record(&MvccKey::CatalogIndexesPage {
                table_id,
                page_id: id,
            })?;
            let page = self.value::<IndexPage>(&page.value)?;
            if page.value.indexes.is_empty() {
                return Err(unknown());
            }
            for index_id in page.value.indexes.iter().copied() {
                self.create(
                    &self.index_inventory_path("visited-indexes", table_id, index_id),
                    &(),
                )?;
                let version = self.record(&MvccKey::CatalogIndex { table_id, index_id })?;
                let record = self.value::<IndexRecord>(&version.value)?;
                let name =
                    Text::try_from(record.value.definition.name.as_str()).map_err(|_| unknown())?;
                let named = self.record(&MvccKey::CatalogIndexName { table_id, name })?;
                if self.value::<IndexNameRecord>(&named.value)?.value.index_id != index_id {
                    return Err(unknown());
                }
                total = total.checked_add(1).ok_or_else(unknown)?;
            }
            last = Some(id);
            page_id = page.value.next_page;
        }
        if last != root.last_page || total != root.epoch {
            return Err(unknown());
        }
        Ok(())
    }

    fn validate_index_closure(
        &self,
        closed: &Version,
        index: &Version,
        record: &IndexRecord,
    ) -> Result<(), ProtocolError> {
        let closure = self.value::<IndexAttemptClosedRecord>(closed)?;
        let closure = &closure.value;
        let (_, attempt, _) = record.phase.attempt_binding();
        let ordered = if attempt > closure.owner.attempt {
            closed.timestamp <= index.timestamp
        } else if matches!(record.phase, IndexPhase::Retired { .. }) {
            closed.timestamp < index.timestamp
                || (closed.timestamp == index.timestamp && closure.kind == IndexClosedKind::Retired)
        } else {
            closed.timestamp == index.timestamp
        };
        if !ordered || !phase_certifies_closure(&record.phase, closure) {
            return Err(unknown());
        }
        Ok(())
    }

    pub(super) fn active(
        &self,
        group: &GroupIdentity,
    ) -> Result<Option<GroupImageScope>, ProtocolError> {
        if group.kind == GroupKind::Catalog {
            return Ok(self.catalog.clone());
        }
        let scope = self
            .read::<GroupImageScope>(&self.path.join("active").join(group.group_id.to_string()))?;
        Ok(scope.map(|scope| scope.value))
    }

    pub(super) fn empty_groups(
        &self,
    ) -> Result<impl Iterator<Item = Result<GroupImageScope, ProtocolError>> + '_, ProtocolError>
    {
        let groups = std::fs::read_dir(self.path.join("empty")).map_err(|_| unknown())?;
        Ok(groups.map(|entry| {
            let entry = entry.map_err(|_| unknown())?;
            let item: Staged<GroupImageScope> = self.read(&entry.path())?.ok_or_else(unknown)?;
            Ok(item.value)
        }))
    }
}

impl Drop for RestoreGraph {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
