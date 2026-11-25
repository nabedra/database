//! Table descriptors and writable indexes for the leader (plan section 6
//! step 4: "validate the row against the cached table descriptor").
//!
//! A cache entry is keyed by the table, the route's schema epoch and the
//! group's direct schema epoch and lifecycle (T7). Direct epochs are local
//! to a group, so a split child can reuse a parent's earlier epoch under a
//! different lifecycle. Every DirectCommit carries the direct epoch and
//! apply refuses a stale one. The DDL fence of plan section 7
//! commits a new direct epoch, bound to the table lifecycle id the Catalog is
//! about to publish, to every Data group of the table before the Catalog
//! publishes a new descriptor or writable index set. A load accepts only the
//! table record whose `lifecycle_id` the group's direct schema names, so an
//! entry stays valid for as long as the group's direct epoch is unchanged.
use crate::{
    oltp::mvcc::{
        MvccKey,
        catalog::{CatalogHandle, CatalogRead},
        index::{IndexDefinition, IndexPage, IndexRecord, IndexRoot},
        store::direct::DirectSchema as GroupSchema,
        store::direct::MAX_DIRECT_INDEX_CHANGES,
    },
    oltp_replication::{
        authority::consensus::ConsensusFuture,
        types::{Authority, ReadTs},
    },
    protocol::{ErrorCode, Outcome, ProtocolError, TableDescriptor},
};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};
use uuid::Uuid;

/// What the leader needs to compute a row's writes.
#[derive(Clone, Debug)]
pub(crate) struct DirectTable {
    pub descriptor: TableDescriptor,
    /// The Catalog checks below read the record's epoch and lifecycle id
    /// directly; only the test schema reads these two copies.
    #[cfg_attr(not(test), allow(dead_code))]
    pub schema_epoch: u64,
    /// The Catalog table record's lifecycle id this entry was read from.
    #[cfg_attr(not(test), allow(dead_code))]
    pub lifecycle_id: Uuid,
    /// Every writable (building or ready) index at `schema_epoch`.
    pub indexes: Vec<IndexDefinition>,
}

pub(crate) enum SchemaLookup {
    Table(Arc<DirectTable>),
    /// The catalog holds an intent (a DDL is in flight): use the queued path.
    Blocked,
    /// The catalog's table is at another schema epoch.
    StaleEpoch,
    /// No such table.
    Missing,
}

pub(crate) trait DirectSchema: Send + Sync {
    /// The table at the route's `schema_epoch`. With `group`, only the
    /// descriptor whose lifecycle the group's direct schema admits; a
    /// different one means a DDL is between its fence and its publication
    /// (`Blocked`). Without it (forward markers, which need only the key
    /// column), any current descriptor.
    fn table(
        &self,
        table_id: Uuid,
        schema_epoch: u64,
        group: Option<GroupSchema>,
        authority: Authority,
    ) -> ConsensusFuture<'_, SchemaLookup>;
    /// The table record's current lifecycle id, which a first activation of
    /// a Data group binds (T7). `None` when the table is missing, a DDL
    /// holds an intent on it, or a DDL holds or awaits the table's exclusive
    /// lifecycle guard (T7b: a DDL that saw `data.direct.v1` inactive does
    /// not fence, so its pre-DDL lifecycle must not be bound). The guard is
    /// read before the table record, linearizably: a DDL that takes the
    /// guard after that read already sees the capability active and fences.
    fn lifecycle(&self, table_id: Uuid, authority: Authority) -> ConsensusFuture<'_, Option<Uuid>>;
}

/// Fixed tables, for tests.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct StaticSchema {
    tables: Mutex<HashMap<Uuid, Arc<DirectTable>>>,
    /// Tables whose exclusive lifecycle guard a modelled DDL holds.
    exclusive: Mutex<std::collections::HashSet<Uuid>>,
    /// The next this many lifecycle reads fail as an unreachable Catalog.
    failing_reads: Mutex<u32>,
    /// Test hook: table reads never answer (a stalled Catalog leader).
    #[cfg(test)]
    stalled_table_reads: std::sync::atomic::AtomicBool,
}
#[cfg(test)]
impl StaticSchema {
    /// Models a Catalog that cannot answer the next `count` lifecycle reads.
    pub(crate) fn fail_lifecycle_reads(&self, count: u32) {
        *self.failing_reads.lock().unwrap_or_else(|p| p.into_inner()) = count;
    }
    /// Models a DDL taking (`true`) or releasing the table's exclusive
    /// lifecycle guard.
    pub(crate) fn hold_exclusive(&self, table_id: Uuid, held: bool) {
        let mut exclusive = self.exclusive.lock().unwrap_or_else(|p| p.into_inner());
        if held {
            exclusive.insert(table_id);
        } else {
            exclusive.remove(&table_id);
        }
    }
    /// Models a Catalog leader that stops answering table reads.
    #[cfg(test)]
    pub(crate) fn stall_table_reads(&self, stalled: bool) {
        self.stalled_table_reads
            .store(stalled, std::sync::atomic::Ordering::Release);
    }
    pub(crate) fn insert(&self, table: DirectTable) {
        self.tables
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(table.descriptor.id, Arc::new(table));
    }
}
#[cfg(test)]
impl DirectSchema for StaticSchema {
    fn lifecycle(&self, table_id: Uuid, _: Authority) -> ConsensusFuture<'_, Option<Uuid>> {
        let failing = {
            let mut left = self.failing_reads.lock().unwrap_or_else(|p| p.into_inner());
            let failing = *left > 0;
            *left = left.saturating_sub(1);
            failing
        };
        if failing {
            return Box::pin(async {
                Err(ProtocolError::new(
                    ErrorCode::StorageUnavailable,
                    "modelled Catalog unavailable",
                    Outcome::Unknown,
                ))
            });
        }
        if self
            .exclusive
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains(&table_id)
        {
            return Box::pin(async { Ok(None) });
        }
        let found = self
            .tables
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&table_id)
            .map(|table| table.lifecycle_id);
        Box::pin(async move { Ok(found) })
    }
    fn table(
        &self,
        table_id: Uuid,
        schema_epoch: u64,
        group: Option<GroupSchema>,
        _: Authority,
    ) -> ConsensusFuture<'_, SchemaLookup> {
        #[cfg(test)]
        if self
            .stalled_table_reads
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Box::pin(std::future::pending());
        }
        let found = self
            .tables
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&table_id)
            .cloned();
        Box::pin(async move {
            Ok(match found {
                None => SchemaLookup::Missing,
                Some(table) if table.schema_epoch != schema_epoch => SchemaLookup::StaleEpoch,
                Some(table) if group.is_some_and(|group| !group.admits(table.lifecycle_id)) => {
                    SchemaLookup::Blocked
                }
                Some(table) => SchemaLookup::Table(table),
            })
        })
    }
}

/// Reads the table record and its writable indexes from the Catalog at the
/// newest version, and caches them per schema epoch.
pub(crate) struct CatalogSchema {
    catalog: CatalogHandle,
    cache: Mutex<Cache>,
}
const CACHE_ENTRIES: usize = 256;
#[derive(Default)]
struct Cache {
    entries: HashMap<CacheKey, Arc<DirectTable>>,
    order: VecDeque<CacheKey>,
}
impl CatalogSchema {
    pub(crate) fn new(catalog: CatalogHandle) -> Self {
        Self {
            catalog,
            cache: Mutex::default(),
        }
    }
    fn cached(&self, key: CacheKey) -> Option<Arc<DirectTable>> {
        self.cache
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entries
            .get(&key)
            .cloned()
    }
    fn remember(&self, key: CacheKey, table: Arc<DirectTable>) {
        let mut cache = self.cache.lock().unwrap_or_else(|p| p.into_inner());
        if cache.entries.insert(key, table).is_none() {
            cache.order.push_back(key);
        }
        while cache.order.len() > CACHE_ENTRIES {
            if let Some(old) = cache.order.pop_front() {
                cache.entries.remove(&old);
            }
        }
    }
    async fn load(
        &self,
        table_id: Uuid,
        schema_epoch: u64,
        group: Option<GroupSchema>,
        authority: Authority,
    ) -> Result<SchemaLookup, ProtocolError> {
        let latest = ReadTs::new(u64::MAX).map_err(|_| invalid())?;
        let pin = self.catalog.snapshot(latest, authority)?;
        let table = match self.catalog.describe(&pin, table_id).await? {
            CatalogRead::Blocked(_) => return Ok(SchemaLookup::Blocked),
            CatalogRead::Value { record: None, .. } => return Ok(SchemaLookup::Missing),
            CatalogRead::Value {
                record: Some(record),
                ..
            } => record,
        };
        if table.schema_epoch != schema_epoch {
            return Ok(SchemaLookup::StaleEpoch);
        }
        if group.is_some_and(|group| !group.admits(table.lifecycle_id)) {
            // Fenced but not yet published (or a publication that failed
            // after its fence): the queued path waits on the DDL's guard.
            return Ok(SchemaLookup::Blocked);
        }
        let mut indexes = Vec::new();
        let root = match self
            .catalog
            .read::<IndexRoot>(&pin, MvccKey::CatalogIndexesRoot { table_id })
            .await?
        {
            CatalogRead::Blocked(_) => return Ok(SchemaLookup::Blocked),
            CatalogRead::Value { record, .. } => record,
        };
        let mut next = root.as_ref().and_then(|root| root.first_page);
        while let Some(page_id) = next {
            let page = match self
                .catalog
                .read::<IndexPage>(&pin, MvccKey::CatalogIndexesPage { table_id, page_id })
                .await?
            {
                CatalogRead::Blocked(_) => return Ok(SchemaLookup::Blocked),
                CatalogRead::Value { record, .. } => record.ok_or_else(invalid)?,
            };
            for index_id in page.indexes.iter() {
                let record = match self
                    .catalog
                    .read::<IndexRecord>(
                        &pin,
                        MvccKey::CatalogIndex {
                            table_id,
                            index_id: *index_id,
                        },
                    )
                    .await?
                {
                    CatalogRead::Blocked(_) => return Ok(SchemaLookup::Blocked),
                    CatalogRead::Value { record, .. } => record.ok_or_else(invalid)?,
                };
                if std::env::var("P11_TOPO_MUT").as_deref() == Ok("skip-building") && !record.phase.ready() {
                    continue;
                }
                if !record.phase.writable() {
                    continue;
                }
                if record.schema_epoch != table.schema_epoch {
                    // The queued planner refuses this as "index schema
                    // changed"; the direct path leaves it to the queued path.
                    return Ok(SchemaLookup::Blocked);
                }
                record.definition.validate(&table.descriptor)?;
                indexes.push(record.definition.clone());
                // Each index writes at most a delete and a put per row.
                if indexes.len() * 2 > MAX_DIRECT_INDEX_CHANGES {
                    return Ok(SchemaLookup::Blocked);
                }
            }
            next = page.next_page;
        }
        let table = Arc::new(DirectTable {
            descriptor: table.descriptor.clone(),
            schema_epoch: table.schema_epoch,
            lifecycle_id: table.lifecycle_id,
            indexes,
        });
        self.remember((table_id, schema_epoch, group), table.clone());
        Ok(SchemaLookup::Table(table))
    }
}
impl DirectSchema for CatalogSchema {
    fn table(
        &self,
        table_id: Uuid,
        schema_epoch: u64,
        group: Option<GroupSchema>,
        authority: Authority,
    ) -> ConsensusFuture<'_, SchemaLookup> {
        Box::pin(async move {
            if let Some(table) = self.cached((table_id, schema_epoch, group)) {
                return Ok(SchemaLookup::Table(table));
            }
            self.load(table_id, schema_epoch, group, authority).await
        })
    }
    fn lifecycle(&self, table_id: Uuid, authority: Authority) -> ConsensusFuture<'_, Option<Uuid>> {
        Box::pin(async move {
            // The guard first: see the trait.
            if self
                .catalog
                .participant()
                .lifecycle_exclusive(table_id)
                .await?
                .is_some()
            {
                return Ok(None);
            }
            let latest = ReadTs::new(u64::MAX).map_err(|_| invalid())?;
            let pin = self.catalog.snapshot(latest, authority)?;
            Ok(match self.catalog.describe(&pin, table_id).await? {
                CatalogRead::Value {
                    record: Some(record),
                    ..
                } => Some(record.lifecycle_id),
                _ => None,
            })
        })
    }
}
/// (table, route schema epoch, group direct epoch and lifecycle when checked).
type CacheKey = (Uuid, u64, Option<GroupSchema>);

fn invalid() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "catalog index metadata is incomplete",
        Outcome::NotApplied,
    )
}
