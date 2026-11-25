//! Per-entry change records (`plans/oltp-vector.md`, section 3, DB-5).
//!
//! Apply writes one record per log entry that changed a row or closed a
//! timestamp, in the entry's own batch: what it did, after rejections, replays
//! and resolves. A rejected entry's batch is discarded, so it leaves none. The
//! feed is a function of these records only, never of the current rows.
//!
//! Records belong to one replica, like its log: they live outside the state
//! generation, no snapshot carries them, and the log's purge deletes them with
//! its entries (`GroupTransaction::purge_changes`). Each record names the index
//! of the record before it, and the replicated state names the last one. So a
//! reader sees every gap, whatever purge, snapshot or upgrade caused it.
use super::{row::RowImage, *};
use crate::oltp::{
    mvcc::CanonicalJson,
    pool::{ByteClass, GroupRead, ScratchLease},
};

/// The index of the last entry with a record, or the one the feed starts after.
const LAST: &[u8] = b"mvcc/changes/v1";
const FORMAT: u8 = 1;
const HEADER: usize = 1 + 8 + 8 + 8;
const PUT: u8 = 1;
const DELETE: u8 = 2;
const CLOSED: u8 = 3;

/// What one entry did, collected while it applies.
pub(super) struct Changes {
    /// Only a range with search columns has a feed.
    on: bool,
    items: Vec<u8>,
}
impl Changes {
    pub(super) fn new(config: &Config) -> Self {
        Self {
            on: config.search.is_some(),
            items: Vec::new(),
        }
    }
    /// A row version. Index entries follow from rows, so they are not recorded.
    pub(super) fn row(
        &mut self,
        logical: &[u8],
        commit_ts: CommitTs,
        live: bool,
    ) -> Result<(), ProtocolError> {
        if logical.first() != Some(&1) {
            return Ok(());
        }
        self.push(if live { PUT } else { DELETE }, commit_ts, logical)
    }
    pub(super) fn closed(&mut self, ts: CommitTs, unix_ms: u64) -> Result<(), ProtocolError> {
        self.push(CLOSED, ts, &unix_ms.to_be_bytes())
    }
    fn push(&mut self, kind: u8, ts: CommitTs, bytes: &[u8]) -> Result<(), ProtocolError> {
        if !self.on {
            return Ok(());
        }
        let length = u32::try_from(bytes.len()).map_err(|_| invalid())?;
        self.items
            .try_reserve(bytes.len() + 13)
            .map_err(|_| invalid())?;
        self.items.push(kind);
        self.items.extend_from_slice(&ts.get().to_be_bytes());
        self.items.extend_from_slice(&length.to_be_bytes());
        self.items.extend_from_slice(bytes);
        Ok(())
    }
    pub(super) fn flush(
        self,
        tx: &mut GroupTransaction<'_>,
        position: AppliedPosition,
    ) -> Result<(), ProtocolError> {
        if self.items.is_empty() {
            return Ok(());
        }
        let previous = load::<u64>(tx, LAST)?.ok_or_else(recovery_unknown)?;
        tx.with_scratch_buffer(HEADER + self.items.len(), |tx, record| {
            record[0] = FORMAT;
            record[1..9].copy_from_slice(&position.term.to_be_bytes());
            record[9..17].copy_from_slice(&position.node_id.to_be_bytes());
            record[17..HEADER].copy_from_slice(&previous.to_be_bytes());
            record[HEADER..].copy_from_slice(&self.items);
            tx.put_change(position.index, record)
        })?;
        tx.put_serialized(LAST, &position.index)
    }
}
/// Starts the feed after `position`: rows before it come from a range snapshot.
/// Called when a range gains search columns, and for each page of a split copy.
pub(super) fn start(
    tx: &mut GroupTransaction<'_>,
    position: AppliedPosition,
) -> Result<(), ProtocolError> {
    tx.put_serialized(LAST, &position.index)
}

/// One change in log order.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum RowChange {
    /// The row version an entry wrote, or `None` for a delete.
    Row {
        key: MvccKey,
        commit_ts: CommitTs,
        row: Option<ChangedRow>,
    },
    Closed {
        ts: CommitTs,
        unix_ms: u64,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChangedRow {
    pub value: CanonicalJson,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search: Option<Box<RowImage>>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EntryChanges {
    pub at: AppliedPosition,
    pub changes: Vec<RowChange>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ChangePage {
    /// Every change after the requested index and at or before `through`.
    Changes {
        entries: Vec<EntryChanges>,
        through: AppliedPosition,
    },
    /// A record after the requested index, or a row or vector it names, is gone:
    /// the reader installs a range snapshot.
    Gone { applied: AppliedPosition },
    /// This replica has not applied the requested index yet.
    Behind { applied: AppliedPosition },
    /// The range has no search columns.
    Inactive,
}
impl OwnedPayload for ChangePage {
    fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        let Self::Changes { entries, .. } = self else {
            return Ok(0);
        };
        let mut bytes = entries.capacity() * std::mem::size_of::<EntryChanges>();
        for entry in entries {
            bytes += entry.changes.capacity() * std::mem::size_of::<RowChange>();
            for change in &entry.changes {
                let RowChange::Row { key, row, .. } = change else {
                    continue;
                };
                bytes = bytes.checked_add(key.heap_bytes()).ok_or_else(invalid)?;
                if let Some(row) = row {
                    bytes = bytes
                        .checked_add(row.value.heap_bytes()?)
                        .and_then(|n| {
                            n.checked_add(match &row.search {
                                Some(search) => search.heap_bytes().ok()?,
                                None => 0,
                            })
                        })
                        .ok_or_else(invalid)?;
                }
            }
        }
        Ok(bytes)
    }
    fn attach_fields(&self, owner: Arc<ScratchLease>) -> Result<(), ProtocolError> {
        self.rows().try_for_each(|row| {
            row.value.attach_charge(owner.clone())?;
            row.search
                .as_ref()
                .map_or(Ok(()), |search| search.attach_charge(owner.clone()))
        })
    }
    fn fields_attached(&self) -> bool {
        self.rows().all(|row| {
            row.value.is_charged()
                && row.search.as_ref().is_none_or(|search| search.is_charged())
        })
    }
}
impl ChangePage {
    fn rows(&self) -> impl Iterator<Item = &ChangedRow> {
        let entries = match self {
            Self::Changes { entries, .. } => &entries[..],
            _ => &[],
        };
        entries
            .iter()
            .flat_map(|entry| &entry.changes)
            .filter_map(|change| match change {
                RowChange::Row { row, .. } => row.as_ref(),
                RowChange::Closed { .. } => None,
            })
    }
}

impl ParticipantMachine {
    /// The changes after log index `after`, of whole entries, at most `max_bytes`
    /// of rows and vectors. Only applied entries have records, so a reader never
    /// sees an entry that Raft can truncate.
    pub async fn change_page(
        &self,
        after: u64,
        max_entries: u16,
        max_bytes: usize,
    ) -> Result<OwnedCommand<ChangePage>, ProtocolError> {
        if max_entries == 0
            || max_entries > 1024
            || max_bytes < 64 << 10
            || max_bytes > super::super::MAX_FIXED_COMMAND_BYTES
            || !matches!(self.store.identity().kind, GroupKind::Data { .. })
        {
            return Err(invalid());
        }
        let owner = Arc::new(
            self.store
                .resources()
                .reserve_scratch_wait(ByteClass::Data, 2 * max_bytes + (256 << 10), 0)
                .await?,
        );
        let store = self.store.clone();
        let result = self
            .store
            .read_owned(
                TransactionBudget {
                    scratch_bytes: 32 << 20,
                    batch_bytes: 0,
                    output_bytes: 0,
                },
                owner,
                move |view, owner| {
                    let applied = store
                        .applied_position_in(view)?
                        .ok_or_else(recovery_unknown)?;
                    let page = read_page(view, applied, after, max_entries, max_bytes, &owner)?;
                    OwnedCommand::new_with_owner(page, owner)
                },
            )
            .await;
        self.note_damage(result)
    }
}

fn read_page(
    view: &mut GroupRead<'_>,
    applied: AppliedPosition,
    after: u64,
    max_entries: u16,
    max_bytes: usize,
    owner: &Arc<ScratchLease>,
) -> Result<ChangePage, ProtocolError> {
    let raw = view.pinned(CONFIG)?.ok_or_else(invalid)?;
    if raw.len() > METADATA_BYTES {
        return Err(recovery_unknown());
    }
    let config: Config = serde_json::from_slice(&raw).map_err(|_| recovery_unknown())?;
    if config.search.is_none() {
        return Ok(ChangePage::Inactive);
    }
    let last: u64 = view
        .pinned(LAST)?
        .and_then(|raw| serde_json::from_slice(&raw).ok())
        .ok_or_else(recovery_unknown)?;
    if after > applied.index {
        return Ok(ChangePage::Behind { applied });
    }
    let gone = || Ok(ChangePage::Gone { applied });
    let mut entries: Vec<EntryChanges> = Vec::new();
    let mut budget = max_bytes;
    let mut reached = after;
    while reached < last {
        let Some(next) = reached.checked_add(1) else {
            return Err(recovery_unknown());
        };
        let Some((index, raw)) = view.change_from(next)? else {
            return gone();
        };
        let entry = Entry::decode(index, &raw)?;
        if entry.previous > reached || index > last {
            return gone();
        }
        // A page ends before an entry that does not fit.
        let Some(changes) = entry.read(view, &mut budget, owner)? else {
            return gone();
        };
        let Some(changes) = changes else {
            if entries.is_empty() {
                return Err(failure(
                    ErrorCode::LimitExceeded,
                    "one log entry's changes exceed the page budget",
                ));
            }
            break;
        };
        entries.push(EntryChanges {
            at: entry.at,
            changes,
        });
        reached = index;
        if entries.len() == usize::from(max_entries) {
            break;
        }
    }
    let through = match entries.last() {
        Some(entry) if reached < last => entry.at,
        _ => applied,
    };
    owner.shrink_unused((max_bytes - budget).saturating_add(192 << 10).min(owner.bytes()))?;
    Ok(ChangePage::Changes { entries, through })
}

struct Entry<'a> {
    at: AppliedPosition,
    previous: u64,
    items: &'a [u8],
}
impl<'a> Entry<'a> {
    fn decode(index: u64, raw: &'a [u8]) -> Result<Self, ProtocolError> {
        let (header, items) = raw
            .split_first_chunk::<HEADER>()
            .filter(|(header, _)| header[0] == FORMAT)
            .ok_or_else(recovery_unknown)?;
        let number = |at: usize| {
            u64::from_be_bytes(header[at..at + 8].try_into().expect("fixed header"))
        };
        Ok(Self {
            at: AppliedPosition {
                term: number(1),
                node_id: number(9),
                index,
            },
            previous: number(17),
            items,
        })
    }
    /// The entry's changes: `None` when a row or vector it names is gone, and
    /// `Some(None)` when they do not fit in `budget`.
    #[allow(clippy::type_complexity)]
    fn read(
        &self,
        view: &mut GroupRead<'_>,
        budget: &mut usize,
        owner: &Arc<ScratchLease>,
    ) -> Result<Option<Option<Vec<RowChange>>>, ProtocolError> {
        let mut left = *budget;
        let mut changes = Vec::new();
        let mut items = self.items;
        while !items.is_empty() {
            let (head, rest) = items.split_first_chunk::<13>().ok_or_else(recovery_unknown)?;
            let ts = u64::from_be_bytes(head[1..9].try_into().expect("fixed item"));
            let ts = CommitTs::new(ts).map_err(|_| recovery_unknown())?;
            let length = u32::from_be_bytes(head[9..].try_into().expect("fixed item"));
            let (bytes, rest) = rest
                .split_at_checked(length as usize)
                .ok_or_else(recovery_unknown)?;
            items = rest;
            let Some(cost) = left.checked_sub(bytes.len() + 1024) else {
                return Ok(Some(None));
            };
            left = cost;
            changes.push(match head[0] {
                CLOSED => RowChange::Closed {
                    ts,
                    unix_ms: u64::from_be_bytes(
                        bytes.try_into().map_err(|_| recovery_unknown())?,
                    ),
                },
                PUT | DELETE => {
                    let Some(raw) = row::exact(view, bytes, ts)? else {
                        return Ok(None);
                    };
                    let version = row::Version::decode(&raw)?;
                    if version.value.is_some() != (head[0] == PUT) {
                        return Err(recovery_unknown());
                    }
                    let row = match version.value {
                        None => None,
                        Some(value) => {
                            let Some(cost) = row::image_cost(view, bytes, &version)? else {
                                return Ok(None);
                            };
                            let Some(cost) = value
                                .get()
                                .len()
                                .checked_add(cost)
                                .and_then(|cost| left.checked_sub(cost))
                            else {
                                return Ok(Some(None));
                            };
                            left = cost;
                            Some(ChangedRow {
                                search: row::image(view, bytes, &version, false, owner)?,
                                value: CanonicalJson::from_stored_json(
                                    value.get().as_bytes(),
                                    owner.clone(),
                                )?,
                            })
                        }
                    };
                    RowChange::Row {
                        key: archive::decode_logical(bytes)?,
                        commit_ts: ts,
                        row,
                    }
                }
                _ => return Err(recovery_unknown()),
            });
        }
        *budget = left;
        Ok(Some(Some(changes)))
    }
}
