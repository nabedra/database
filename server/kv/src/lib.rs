//! `nabedra-kv`: an embedded, crash-safe, ordered key-value store.
//!
//! One store is an LSM: a group-committed journal, sharded memtables, immutable sorted
//! runs and leveled compaction. The design and the guarantees are in
//! `plans/native-storage-engine.md`. The modules below are owned by separate tasks;
//! [`types`], [`batch`], [`coding`], [`error`] and the [`fs`] traits are the shared
//! contract and change only through the lead, as does [`source`].

pub mod batch;
pub mod cache;
pub mod coding;
mod compaction;
pub mod crc;
pub mod error;
pub mod fs;
mod gc;
pub mod iter;
pub mod journal;
pub mod mac;
pub mod manifest;
pub mod memtable;
pub mod quarantine;
mod recovery;
pub mod run;
pub mod source;
pub mod store;
pub mod types;
pub mod version;

pub use batch::{BatchOp, WriteBatch};
pub use cache::{BlockCache, CacheKey};
pub use error::{Damage, DamageReason, Error, FileId, Result};
pub use iter::{DbIter, MergingIterator, ValueResolver};
pub use memtable::{Lookup, Memtable, MemtableBudget, MemtableOptions, SnapshotList};
pub use quarantine::QuarantineLimit;
pub use run::{Run, RunMeta, RunReadContext, RunWriter, RunWriterOptions, TableCache};
pub use source::{InternalIterator, RangeTombstone};
pub use store::{
    Background, Commit, Durability, Iter, JournalIntegrity, JournalOptions, Opened, Options,
    RecoveryReport, Snapshot, Store, StoreStats, THREAD_STACK_BYTES,
};
pub use types::{Kind, Locator, Seq};
pub use version::LevelOptions;
