//! Atomic write batches. The encoding is also the journal record payload.
//!
//! ```text
//! batch = op*
//! op    = kind:u8 ‖ varint32 key_len ‖ key ‖ [varint32 value_len ‖ value]
//! ```
//!
//! `Delete` carries no value. `RangeDelete` stores the inclusive start as the key and the
//! exclusive end as the value. `JournalPut` stores the raw value bytes: the journal keeps
//! them where they are and the memtable receives a [`crate::types::Locator`] instead,
//! computed from the record's file offset plus [`JournalValueRef::offset`].
//!
//! Operation `i` takes sequence `first_seq + i`, so a later operation on the same key in
//! the same batch wins, including a put after a covering range delete.
//!
//! A batch may carry a *scope*: a key prefix every key and range of the batch lies under
//! (for a pool group, `0x40 ‖ group id`). The journal writes the scope in the record
//! header, outside the payload, so recovery can name the one group a damaged record
//! belongs to. The empty scope means store-wide and allows any key. The journal rejects
//! a batch whose operations leave its scope ([`WriteBatch::leaves_scope`]).

use crate::coding::{get_length_prefixed, put_length_prefixed};
use crate::error::{Error, FileId, Result};
use crate::types::Kind;

/// Where a journal value sits inside [`WriteBatch::data`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JournalValueRef {
    /// Index of the operation inside the batch.
    pub op_index: u32,
    /// Offset of the first value byte inside the encoded batch.
    pub offset: u32,
    pub len: u32,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WriteBatch {
    data: Vec<u8>,
    count: u32,
    journal_values: Vec<JournalValueRef>,
    scope: Vec<u8>,
    leaves_scope: bool,
}

/// Longest scope a journal record header can carry.
pub const MAX_SCOPE_LEN: usize = 255;

/// The smallest key greater than every key that starts with `prefix`, or `None` when no
/// such key exists (empty prefix or all `0xFF`).
pub fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last != 0xFF {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

fn op_within_scope(scope: &[u8], op: &BatchOp<'_>) -> bool {
    if scope.is_empty() {
        return true;
    }
    match op {
        BatchOp::Put { key, .. } | BatchOp::JournalPut { key, .. } | BatchOp::Delete { key } => {
            key.starts_with(scope)
        }
        BatchOp::RangeDelete { start, end } => {
            start.starts_with(scope)
                && (end.starts_with(scope)
                    || prefix_successor(scope)
                        .is_some_and(|successor| *end == successor.as_slice()))
        }
    }
}

/// One decoded operation, borrowing from the batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BatchOp<'a> {
    Put {
        key: &'a [u8],
        value: &'a [u8],
    },
    /// `offset` is the value's offset inside the encoded batch.
    JournalPut {
        key: &'a [u8],
        value: &'a [u8],
        offset: u32,
    },
    Delete {
        key: &'a [u8],
    },
    RangeDelete {
        start: &'a [u8],
        end: &'a [u8],
    },
}

impl BatchOp<'_> {
    pub fn kind(&self) -> Kind {
        match self {
            BatchOp::Put { .. } => Kind::Put,
            BatchOp::JournalPut { .. } => Kind::JournalPut,
            BatchOp::Delete { .. } => Kind::Delete,
            BatchOp::RangeDelete { .. } => Kind::RangeDelete,
        }
    }
}

impl WriteBatch {
    pub fn new() -> WriteBatch {
        WriteBatch::default()
    }

    pub fn with_capacity_bytes(bytes: usize) -> WriteBatch {
        WriteBatch {
            data: Vec::with_capacity(bytes),
            ..WriteBatch::default()
        }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.push(Kind::Put, key, Some(value));
    }

    /// Stores `value` in the journal and only a locator in the LSM. Use it for large
    /// values that are deleted soon after, such as Raft log entries.
    pub fn put_journal(&mut self, key: &[u8], value: &[u8]) {
        let op_index = self.count;
        self.push(Kind::JournalPut, key, Some(value));
        let offset = self.data.len() - value.len();
        self.journal_values.push(JournalValueRef {
            op_index,
            offset: u32::try_from(offset).expect("batch larger than u32::MAX"),
            len: value.len() as u32,
        });
    }

    pub fn delete(&mut self, key: &[u8]) {
        self.push(Kind::Delete, key, None);
    }

    /// Deletes every key in `[start, end)`. An empty or inverted range deletes nothing.
    pub fn delete_range(&mut self, start: &[u8], end: &[u8]) {
        self.push(Kind::RangeDelete, start, Some(end));
    }

    /// Confines the batch to keys under `prefix` (see the module docs). May be called
    /// before or after adding operations; [`WriteBatch::leaves_scope`] reflects every
    /// operation either way. Panics if `prefix` is longer than [`MAX_SCOPE_LEN`].
    pub fn set_scope(&mut self, prefix: &[u8]) {
        assert!(
            prefix.len() <= MAX_SCOPE_LEN,
            "scope longer than {MAX_SCOPE_LEN} bytes"
        );
        self.scope.clear();
        self.scope.extend_from_slice(prefix);
        let scope = &self.scope;
        self.leaves_scope = self.iter().any(|op| !op_within_scope(scope, &op));
    }

    /// The scope prefix; empty means store-wide.
    pub fn scope(&self) -> &[u8] {
        &self.scope
    }

    /// True when some operation lies outside the scope. The journal refuses such a batch
    /// with `Error::InvalidArgument("batch leaves its scope")`.
    pub fn leaves_scope(&self) -> bool {
        self.leaves_scope
    }

    fn push(&mut self, kind: Kind, key: &[u8], value: Option<&[u8]>) {
        assert!(self.count < u32::MAX, "too many operations in one batch");
        if !self.scope.is_empty() && !self.leaves_scope {
            let op = match (kind, value) {
                (Kind::Delete, _) => BatchOp::Delete { key },
                (Kind::RangeDelete, Some(end)) => BatchOp::RangeDelete { start: key, end },
                (_, value) => BatchOp::Put {
                    key,
                    value: value.unwrap_or_default(),
                },
            };
            self.leaves_scope = !op_within_scope(&self.scope, &op);
        }
        self.data.push(kind as u8);
        put_length_prefixed(&mut self.data, key);
        if let Some(value) = value {
            put_length_prefixed(&mut self.data, value);
        }
        assert!(
            self.data.len() <= u32::MAX as usize,
            "batch larger than u32::MAX"
        );
        self.count += 1;
    }

    /// Number of operations, which is also the number of sequence numbers it takes.
    pub fn count(&self) -> u32 {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Size of the encoded batch, the journal payload.
    pub fn encoded_bytes(&self) -> usize {
        self.data.len()
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn journal_values(&self) -> &[JournalValueRef] {
        &self.journal_values
    }

    /// Removes every operation and the scope.
    pub fn clear(&mut self) {
        self.data.clear();
        self.count = 0;
        self.journal_values.clear();
        self.scope.clear();
        self.leaves_scope = false;
    }

    pub fn iter(&self) -> BatchIter<'_> {
        BatchIter {
            data: &self.data,
            at: 0,
        }
    }

    /// Decodes and validates an encoded batch, for example a replayed journal payload.
    /// `file` and `base_offset` locate the payload for corruption errors.
    pub fn from_encoded(data: Vec<u8>, file: FileId, base_offset: u64) -> Result<WriteBatch> {
        let mut count = 0u32;
        let mut journal_values = Vec::new();
        let mut iter = BatchIter { data: &data, at: 0 };
        while iter.at < data.len() {
            let op_at = iter.at;
            let op = iter.next_op().ok_or(Error::Corruption {
                file,
                offset: base_offset + op_at as u64,
                what: "malformed batch operation",
            })?;
            if let BatchOp::JournalPut { value, offset, .. } = op {
                journal_values.push(JournalValueRef {
                    op_index: count,
                    offset,
                    len: value.len() as u32,
                });
            }
            count = count.checked_add(1).ok_or(Error::Corruption {
                file,
                offset: base_offset,
                what: "batch operation count overflow",
            })?;
        }
        Ok(WriteBatch {
            data,
            count,
            journal_values,
            scope: Vec::new(),
            leaves_scope: false,
        })
    }
}

/// Iterates the operations of a batch in order.
pub struct BatchIter<'a> {
    data: &'a [u8],
    at: usize,
}

impl<'a> BatchIter<'a> {
    fn next_op(&mut self) -> Option<BatchOp<'a>> {
        let data: &'a [u8] = self.data;
        let kind = Kind::from_u8(*data.get(self.at)?)?;
        let mut at = self.at + 1;
        let (key, used) = get_length_prefixed(&data[at..])?;
        at += used;
        let op = match kind {
            Kind::Delete => BatchOp::Delete { key },
            Kind::Put | Kind::JournalPut | Kind::RangeDelete => {
                let (value, used) = get_length_prefixed(&data[at..])?;
                let value_at = at + used - value.len();
                at += used;
                match kind {
                    Kind::Put => BatchOp::Put { key, value },
                    Kind::JournalPut => BatchOp::JournalPut {
                        key,
                        value,
                        offset: value_at as u32,
                    },
                    _ => BatchOp::RangeDelete {
                        start: key,
                        end: value,
                    },
                }
            }
        };
        self.at = at;
        Some(op)
    }
}

impl<'a> Iterator for BatchIter<'a> {
    type Item = BatchOp<'a>;

    /// Batches built through the API or accepted by [`WriteBatch::from_encoded`] are
    /// always well formed, so this stops only at the end.
    fn next(&mut self) -> Option<BatchOp<'a>> {
        if self.at >= self.data.len() {
            return None;
        }
        let op = self.next_op();
        if op.is_none() {
            self.at = self.data.len();
        }
        op
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_round_trips_with_journal_value_offsets() {
        let mut batch = WriteBatch::new();
        batch.put(b"a", b"1");
        batch.put_journal(b"log/1", b"entry-bytes");
        batch.delete(b"b");
        batch.delete_range(b"c", b"d");
        assert_eq!(batch.count(), 4);
        let jv = batch.journal_values()[0];
        assert_eq!(jv.op_index, 1);
        assert_eq!(
            &batch.data()[jv.offset as usize..][..jv.len as usize],
            b"entry-bytes"
        );

        let decoded = WriteBatch::from_encoded(batch.data().to_vec(), FileId::Other, 0).unwrap();
        assert_eq!(decoded, batch);
        let ops: Vec<_> = decoded.iter().collect();
        assert_eq!(
            ops[0],
            BatchOp::Put {
                key: b"a",
                value: b"1"
            }
        );
        assert_eq!(
            ops[1],
            BatchOp::JournalPut {
                key: b"log/1",
                value: b"entry-bytes",
                offset: jv.offset
            }
        );
        assert_eq!(ops[2], BatchOp::Delete { key: b"b" });
        assert_eq!(
            ops[3],
            BatchOp::RangeDelete {
                start: b"c",
                end: b"d"
            }
        );
    }

    #[test]
    fn scope_tracks_every_operation() {
        let mut batch = WriteBatch::new();
        batch.set_scope(b"g1/");
        batch.put(b"g1/a", b"1");
        batch.delete(b"g1/b");
        batch.delete_range(b"g1/c", b"g10");
        assert_eq!(prefix_successor(b"g1/").as_deref(), Some(&b"g10"[..]));
        assert!(!batch.leaves_scope());
        batch.delete_range(b"g1/c", b"g2");
        assert!(batch.leaves_scope());

        let mut batch = WriteBatch::new();
        batch.put(b"other", b"1");
        assert!(!batch.leaves_scope(), "the empty scope allows any key");
        batch.set_scope(b"g1/");
        assert!(
            batch.leaves_scope(),
            "set_scope re-checks earlier operations"
        );
        batch.clear();
        assert!(batch.scope().is_empty() && !batch.leaves_scope());
        assert_eq!(prefix_successor(&[0xFF, 0xFF]), None);
        assert_eq!(prefix_successor(&[1, 0xFF]), Some(vec![2]));
    }

    #[test]
    fn malformed_batches_are_corruption() {
        let mut batch = WriteBatch::new();
        batch.put(b"key", b"value");
        let mut data = batch.data().to_vec();
        data.pop();
        assert!(matches!(
            WriteBatch::from_encoded(data, FileId::Journal { segment: 3 }, 100),
            Err(Error::Corruption {
                file: FileId::Journal { segment: 3 },
                offset: 100,
                ..
            })
        ));
        assert!(WriteBatch::from_encoded(vec![9, 0], FileId::Other, 0).is_err());
    }
}
