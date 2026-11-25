//! The replicated search state of one Data group: its search columns and its
//! closed floor (`plans/oltp-vector.md`, sections 3 and 7, spec
//! `specs/vector/snapshot/SearchSnapshot.tla`).
//!
//! Every command here is proposed only while `data.vector.v1` is active: a
//! replica of an older build cannot decode it.
use super::{feed::Changes, *};
use crate::oltp::mvcc::vector::{ColumnId, MAX_SEARCH_COLUMNS, SearchColumn};

/// The columns whose vectors this group stores. A table with full-text columns
/// only has an empty list. A dropped column's id never returns.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchSchema {
    /// Control's search schema epoch. 0 after a restore, until the first fence:
    /// the rows have vectors whose columns are not known, so writes wait.
    pub epoch: u64,
    /// In ascending id order.
    pub columns: Vec<SearchColumn>,
    /// The highest id this group ever had.
    pub high: ColumnId,
}
impl SearchSchema {
    pub(super) fn column(&self, id: ColumnId) -> Option<&SearchColumn> {
        self.columns.iter().find(|column| column.id == id)
    }
}
/// What a split copies besides rows.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SplitSearch {
    pub schema: SearchSchema,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closed_floor: Option<CommitTs>,
}
impl SplitSearch {
    /// What a page of a frozen source carries at its cut.
    pub(super) fn of(config: &Config, read_ts: ReadTs) -> Option<Box<Self>> {
        config
            .split_source
            .as_ref()
            .filter(|source| source.statement.read_ts == read_ts)
            .and(config.search.as_ref())
            .map(|schema| {
                Box::new(Self {
                    schema: schema.clone(),
                    closed_floor: config.closed_floor,
                })
            })
    }
    pub(super) fn heap_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + columns_bytes(&self.schema.columns)
    }
}

/// Sets the group's search columns and raises its direct epoch in one entry, so
/// no commit that read the previous columns applies after it. It takes only a
/// larger `epoch`: a late or repeated fence of an old Control leader changes
/// nothing (DB-20).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchFence {
    pub authority: Authority,
    pub table_id: Uuid,
    pub epoch: u64,
    /// As for `DirectSchemaChange::Fence`.
    pub lifecycle: Option<Uuid>,
    pub columns: Vec<SearchColumn>,
}
impl SearchFence {
    pub(super) fn heap_bytes(&self) -> usize {
        columns_bytes(&self.columns)
    }
}
fn columns_bytes(columns: &Vec<SearchColumn>) -> usize {
    columns.capacity() * std::mem::size_of::<SearchColumn>()
        + columns
            .iter()
            .filter_map(|column| column.source.as_ref())
            .map(|source| source.capacity_bytes())
            .sum::<usize>()
}
/// Closes `ts`: no later commit at or below it applies in this range. The leader
/// proposes a timestamp that the Timestamp group already handed out.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Closed {
    pub authority: Authority,
    pub ts: CommitTs,
    /// Leader wall clock for `ts`. Only the age of a snapshot query reads it.
    pub unix_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SearchReply {
    Fenced {
        epoch: u64,
        direct: Option<direct::DirectSchema>,
    },
    Closed {
        floor: CommitTs,
    },
    Set {
        commit_ts: CommitTs,
    },
    /// An earlier delivery of the same embedding took effect.
    AlreadySet,
    /// The row, its text or the column it was made for is gone.
    Obsolete,
    /// The attempt counted, and the record still waits.
    Counted {
        attempts: u32,
    },
    /// The attempt counted, and the record failed.
    EmbedFailed {
        attempts: u32,
    },
    Requeued {
        attempts: u32,
        limit: u32,
    },
    /// No such record, source version or attempt.
    Unmatched,
    Rejected(Fault),
}
pub(super) fn reply(
    tx: &mut GroupTransaction<'_>,
    reply: SearchReply,
) -> Result<Transition, ProtocolError> {
    let wrote = !matches!(
        reply,
        SearchReply::Rejected(_)
            | SearchReply::AlreadySet
            | SearchReply::Obsolete
            | SearchReply::Unmatched
    );
    let encoded = tx.encode(&ParticipantReply::Search(reply))?;
    Ok(if wrote {
        Transition::Commit(encoded)
    } else {
        Transition::Reject(encoded)
    })
}
pub(super) fn rejected(
    tx: &mut GroupTransaction<'_>,
    fault: Fault,
) -> Result<Transition, ProtocolError> {
    reply(tx, SearchReply::Rejected(fault))
}

/// The group's columns, once they are known.
pub(super) fn ready(config: &Config) -> Option<&SearchSchema> {
    config.search.as_ref().filter(|schema| schema.epoch != 0)
}
/// The range stores vectors, but its columns are not known yet.
pub(super) fn unknown(config: &Config) -> bool {
    config.search.is_some() && ready(config).is_none()
}
/// A split froze the range, or has not finished copying it.
pub(super) fn frozen(config: &Config) -> bool {
    config.split_source.is_some() || config.split_child.is_some_and(|child| !child.complete)
}
/// The search state of a group that a restore filled with vectors or records.
pub(super) fn restored(
    tx: &mut GroupTransaction<'_>,
) -> Result<Option<SearchSchema>, ProtocolError> {
    for tag in [row::VECTOR, embed::EMBED] {
        if tx.first_from(&[tag], &[tag])?.is_some() {
            return Ok(Some(SearchSchema {
                epoch: 0,
                columns: Vec::new(),
                high: 0,
            }));
        }
    }
    Ok(None)
}

pub(super) fn fence(
    tx: &mut GroupTransaction<'_>,
    group: &GroupIdentity,
    config: Option<Config>,
    position: AppliedPosition,
    command: &SearchFence,
) -> Result<Transition, ProtocolError> {
    let Some(mut config) = config else {
        return rejected(tx, Fault::NotInitialized);
    };
    if !matches!(&group.kind, GroupKind::Data { table_id, .. } if *table_id == command.table_id)
        || command.epoch == 0
        || command.columns.len() > MAX_SEARCH_COLUMNS
        || !command.columns.iter().all(SearchColumn::valid)
        || !command.columns.is_sorted_by(|a, b| a.id < b.id)
    {
        return rejected(tx, Fault::InvalidBinding);
    }
    if config.authority != command.authority {
        return rejected(tx, Fault::StaleAuthority);
    }
    if frozen(&config) {
        return rejected(tx, Fault::SplitInProgress);
    }
    let (epoch, high) = config
        .search
        .as_ref()
        .map_or((0, 0), |schema| (schema.epoch, schema.high));
    if command.epoch <= epoch {
        let direct = config.direct_schema;
        return reply(tx, SearchReply::Fenced { epoch, direct });
    }
    // An id names one incarnation: it keeps its definition and never returns.
    let reused = command.columns.iter().any(|column| {
        match ready(&config).and_then(|schema| schema.column(column.id)) {
            Some(existing) => existing != column,
            None => epoch != 0 && column.id <= high,
        }
    });
    if reused {
        return rejected(tx, Fault::InvalidBinding);
    }
    let Some(direct) = direct::fenced(&config, command.lifecycle) else {
        return rejected(tx, Fault::ResourceLimit);
    };
    let first = config.search.is_none();
    let last = command.columns.last().map_or(0, |column| column.id);
    config.search = Some(SearchSchema {
        epoch: command.epoch,
        columns: command.columns.clone(),
        high: last.max(high),
    });
    config.direct_schema = Some(direct);
    if encoded_length(&config, METADATA_BYTES).is_err() {
        return rejected(tx, Fault::ResourceLimit);
    }
    tx.put_serialized(CONFIG, &config)?;
    if first {
        feed::start(tx, position)?;
    }
    reply(
        tx,
        SearchReply::Fenced {
            epoch: command.epoch,
            direct: Some(direct),
        },
    )
}

pub(super) fn closed(
    tx: &mut GroupTransaction<'_>,
    config: Option<Config>,
    position: AppliedPosition,
    command: &Closed,
) -> Result<Transition, ProtocolError> {
    let Some(mut config) = config else {
        return rejected(tx, Fault::NotInitialized);
    };
    if config.authority != command.authority {
        return rejected(tx, Fault::StaleAuthority);
    }
    if ready(&config).is_none() {
        return rejected(tx, Fault::DirectInactive);
    }
    if frozen(&config) {
        return rejected(tx, Fault::SplitInProgress);
    }
    // A prepared transaction may commit at a timestamp that was already taken.
    if tx.first_from(&[INTENT], &[INTENT])?.is_some() {
        return rejected(tx, Fault::Incomplete);
    }
    if let Some(floor) = config.closed_floor.filter(|floor| *floor >= command.ts) {
        return reply(tx, SearchReply::Closed { floor });
    }
    config.closed_floor = Some(command.ts);
    if encoded_length(&config, METADATA_BYTES).is_err() {
        return rejected(tx, Fault::ResourceLimit);
    }
    tx.put_serialized(CONFIG, &config)?;
    let mut changes = Changes::new(&config);
    changes.closed(command.ts, command.unix_ms)?;
    changes.flush(tx, position)?;
    reply(tx, SearchReply::Closed { floor: command.ts })
}
