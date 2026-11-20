//! Table descriptors and writable indexes for the leader (plan section 6
//! step 4: "validate the row against the cached table descriptor").
//!
//! A cache entry is keyed by `(table_id, schema_epoch)`. Every DirectCommit
//! carries its schema epoch and apply refuses a stale one, so an entry can be
//! reused for as long as the group's epoch is unchanged. This relies on the
//! DDL fence of plan section 7 (T7): every change of the writable index set
//! or the descriptor commits a new `schema_epoch` to the table's Data groups
//! before the Catalog publishes it.
use crate::{
    oltp::mvcc::{
        MvccKey,
        catalog::{CatalogHandle, CatalogRead},
        index::{IndexDefinition, IndexPage, IndexRecord, IndexRoot},
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
    pub schema_epoch: u64,
    /// Every writable (building or ready) index at `schema_epoch`.
    pub indexes: Vec<IndexDefinition>,
}

pub(crate) enum SchemaLookup {
    Table(Arc<DirectTable>),
    /// The catalog holds an intent (a DDL is in flight): use the queued path.
    Blocked,
    /// The catalog's table is at another schema epoch.
    StaleEpoch {
        current: u64,
    },
    /// No such table.
    Missing,
}

pub(crate) trait DirectSchema: Send + Sync {
    fn table(
        &self,
        table_id: Uuid,
        schema_epoch: u64,
        authority: Authority,
    ) -> ConsensusFuture<'_, SchemaLookup>;
}

/// Fixed tables, for tests and single-process composition.
#[derive(Default)]
pub(crate) struct StaticSchema {
    tables: Mutex<HashMap<Uuid, Arc<DirectTable>>>,
}
impl StaticSchema {
    pub(crate) fn insert(&self, table: DirectTable) {
        self.tables
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(table.descriptor.id, Arc::new(table));
    }
}
impl DirectSchema for StaticSchema {
    fn table(
        &self,
        table_id: Uuid,
        schema_epoch: u64,
        _: Authority,
    ) -> ConsensusFuture<'_, SchemaLookup> {
        let found = self
            .tables
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&table_id)
            .cloned();
        Box::pin(async move {
            Ok(match found {
                None => SchemaLookup::Missing,
                Some(table) if table.schema_epoch != schema_epoch => SchemaLookup::StaleEpoch {
                    current: table.schema_epoch,
                },
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
    entries: HashMap<(Uuid, u64), Arc<DirectTable>>,
    order: VecDeque<(Uuid, u64)>,
}
impl CatalogSchema {
    pub(crate) fn new(catalog: CatalogHandle) -> Self {
        Self {
            catalog,
            cache: Mutex::default(),
        }
    }
    fn cached(&self, key: (Uuid, u64)) -> Option<Arc<DirectTable>> {
        self.cache
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entries
            .get(&key)
            .cloned()
    }
    fn remember(&self, table: Arc<DirectTable>) {
        let key = (table.descriptor.id, table.schema_epoch);
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
            return Ok(SchemaLookup::StaleEpoch {
                current: table.schema_epoch,
            });
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
            indexes,
        });
        self.remember(table.clone());
        Ok(SchemaLookup::Table(table))
    }
}
impl DirectSchema for CatalogSchema {
    fn table(
        &self,
        table_id: Uuid,
        schema_epoch: u64,
        authority: Authority,
    ) -> ConsensusFuture<'_, SchemaLookup> {
        Box::pin(async move {
            if let Some(table) = self.cached((table_id, schema_epoch)) {
                return Ok(SchemaLookup::Table(table));
            }
            self.load(table_id, schema_epoch, authority).await
        })
    }
}

fn invalid() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "catalog index metadata is incomplete",
        Outcome::NotApplied,
    )
}
