//! Identities shared by the index, the feed and queries.

use std::fmt;
use std::sync::Arc;

pub const MAX_DIM: usize = 4096;
pub const MAX_LIMIT: usize = 1000;

/// The Data group that feeds a range. A split child or a restored range has another one.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct RangeId(pub u128);

/// A log index of a range's Data group.
pub type Pos = u64;
pub type CommitTs = u64;

/// The primary key, encoded so that byte order is key order.
pub type Key = Arc<[u8]>;

/// A partition. Numbers are never used again.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct PartId(pub u32);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Op {
    Cosine,
    Dot,
    L2,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ColumnType {
    Bool,
    Int,
    Float,
    Text,
    Tags,
}

/// The value of a filter column. These columns are never null.
#[derive(Clone, PartialEq, Debug)]
pub enum Value {
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(Arc<str>),
    Tags(Arc<[Arc<str>]>),
}

impl Value {
    pub fn kind(&self) -> ColumnType {
        match self {
            Value::Bool(_) => ColumnType::Bool,
            Value::Int(_) => ColumnType::Int,
            Value::Float(_) => ColumnType::Float,
            Value::Text(_) => ColumnType::Text,
            Value::Tags(_) => ColumnType::Tags,
        }
    }
}

/// What one index holds: the dimensions of its vector column and the filter columns.
#[derive(Clone, PartialEq, Debug)]
pub struct Schema {
    pub dim: usize,
    pub columns: Vec<ColumnType>,
    /// A column (not tags) whose every value has a tree of its own.
    pub partition_by: Option<usize>,
}

/// A row image of the feed.
#[derive(Clone, PartialEq, Debug)]
pub struct Row {
    /// `None` is a null vector: the row never ranks on this column.
    pub vec: Option<Arc<[f32]>>,
    /// One value per column of the schema.
    pub cols: Vec<Value>,
    /// The complete row, returned with a hit.
    pub image: Arc<[u8]>,
}

#[derive(Clone, PartialEq, Debug)]
pub enum Change {
    /// `None` deletes the key.
    Row {
        key: Key,
        commit_ts: CommitTs,
        row: Option<Row>,
    },
    /// No later entry of the range commits at or below this timestamp.
    Closed(CommitTs),
}

#[derive(Clone, PartialEq, Debug)]
pub struct Record {
    pub at: Pos,
    pub change: Change,
}

/// Contiguous changes of one range, `(after, through]` in log order.
#[derive(Clone, PartialEq, Debug)]
pub struct Page {
    pub after: Pos,
    pub through: Pos,
    pub records: Vec<Record>,
}

#[derive(Clone, PartialEq, Debug)]
pub enum Error {
    /// The range is not held here.
    NotHeld(RangeId),
    /// The range did not reach the position or the fresh barrier that this needs yet.
    Behind {
        range: RangeId,
        applied: Pos,
    },
    /// Damage took the range out of service until it resyncs.
    OutOfService {
        range: RangeId,
        reason: String,
    },
    /// The page does not continue the range's position.
    Gap {
        delivered: Pos,
    },
    /// The feed broke a rule. The range has to resync.
    InvalidFeed(&'static str),
    /// Work in flight reached its limit. Try again after `step`.
    Busy,
    /// The extension store is already owned by a live index.
    StoreOwned,
    /// The cut is below what the worker retains.
    CutTooOld {
        oldest: u64,
    },
    /// The cut is above what the worker applied or the range closed.
    CutTooNew {
        newest: u64,
    },
    /// A resync or a drop replaced an index the pin names.
    PinFailed,
    InvalidQuery(&'static str),
    /// A checkpoint file or manifest could not be written or read.
    Io(String),
    /// An invariant broke. The call fails instead of answering in part.
    Internal(&'static str),
    /// A query used up its `cpu`, `io` or `memory` budget. Nothing of it is returned.
    OverBudget(&'static str),
    /// The worker refused the work before it started (`429`).
    Overloaded,
    /// The vector store could not write or read a vector, or found it damaged.
    Store(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::NotHeld(r) => write!(f, "range {:032x} is not held here", r.0),
            Error::Behind { range, applied } => {
                write!(f, "range {:032x} is behind, at {applied}", range.0)
            }
            Error::OutOfService { range, reason } => {
                write!(f, "range {:032x} is out of service: {reason}", range.0)
            }
            Error::Gap { delivered } => write!(f, "the page does not continue {delivered}"),
            Error::InvalidFeed(why) => write!(f, "invalid feed: {why}"),
            Error::Busy => f.write_str("too much work in flight"),
            Error::StoreOwned => f.write_str("the extension store is owned by another index"),
            Error::CutTooOld { oldest } => write!(f, "the oldest servable cut is {oldest}"),
            Error::CutTooNew { newest } => write!(f, "the newest servable cut is {newest}"),
            Error::PinFailed => f.write_str("the pinned index was replaced"),
            Error::InvalidQuery(why) => write!(f, "invalid query: {why}"),
            Error::Io(why) => write!(f, "checkpoint: {why}"),
            Error::Internal(why) => write!(f, "internal: {why}"),
            Error::OverBudget(what) => write!(f, "the query used up its {what} budget"),
            Error::Overloaded => f.write_str("the search worker is overloaded"),
            Error::Store(why) => write!(f, "vector store: {why}"),
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;
