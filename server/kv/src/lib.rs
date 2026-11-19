//! `nabedra-kv`: an embedded, crash-safe, ordered key-value store.
//!
//! One store is an LSM: a group-committed journal, sharded memtables, immutable sorted
//! runs and leveled compaction. The design and the guarantees are in
//! `plans/native-storage-engine.md`. The modules below are owned by separate tasks;
//! [`types`], [`batch`], [`coding`], [`error`] and the [`fs`] traits are the shared
//! contract and change only through the lead.

pub mod batch;
pub mod cache;
pub mod coding;
pub mod crc;
pub mod error;
pub mod fs;
pub mod iter;
pub mod journal;
pub mod memtable;
pub mod run;
pub mod types;

pub use batch::{BatchOp, WriteBatch};
pub use error::{Error, FileId, Result};
pub use types::{Kind, Locator, Seq};
