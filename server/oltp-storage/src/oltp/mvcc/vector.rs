//! Vector values and the search instructions of one row write
//! (`plans/oltp-vector.md`, section 2). Storage treats the bytes as opaque: the
//! gateway validated them, and a search worker interprets them.
use super::{CanonicalJson, JsonStorage, invalid_json};
use crate::{
    oltp::pool::{ScratchLease, WorkerSharedResources},
    protocol::ProtocolError,
};
use nabedra_oltp_types::types::Text;
use serde::{Deserialize, Serialize, Serializer};
use std::{
    fmt,
    sync::{Arc, OnceLock},
};

/// Assigned per table and never reused, so it names one incarnation.
pub type ColumnId = u16;
/// Search columns per table (contract, "Limits").
pub const MAX_SEARCH_COLUMNS: usize = 8;
/// A row is at most 1 MiB, so no vector of it is larger.
pub const MAX_VECTOR_BYTES: usize = 1 << 20;
/// Vectors per `list[Vector]` value.
const MAX_MULTI: usize = 1024;
/// `kind u8 ‖ CRC32C u32 (little endian)` before the bytes. The checksum covers
/// the kind and the bytes.
pub(crate) const FRAME_BYTES: usize = 5;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VectorKind {
    Dense,
    Sparse,
    Multi,
}
impl VectorKind {
    fn code(self) -> u8 {
        match self {
            Self::Dense => 1,
            Self::Sparse => 2,
            Self::Multi => 3,
        }
    }
    fn from_code(code: u8) -> Option<Self> {
        Some(match code {
            1 => Self::Dense,
            2 => Self::Sparse,
            3 => Self::Multi,
            _ => return None,
        })
    }
}

/// One vector in a command, an image or a reply: base64 in a JSON string, so it
/// is charged and attached like every other fixed JSON value.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VectorValue {
    pub kind: VectorKind,
    data: CanonicalJson,
}
impl VectorValue {
    pub fn new(
        shared: &WorkerSharedResources,
        kind: VectorKind,
        bytes: &[u8],
    ) -> Result<Self, ProtocolError> {
        if bytes.len() > MAX_VECTOR_BYTES {
            return Err(invalid_json());
        }
        let data = shared.encode(&Base64(bytes), base64_len(bytes.len()) + 2)?;
        Ok(Self {
            kind,
            data: CanonicalJson(Arc::new(JsonStorage::Owned(data))),
        })
    }
    /// A value read from the store. `owner` already covers its encoding.
    pub(crate) fn from_stored(
        kind: VectorKind,
        bytes: &[u8],
        owner: Arc<ScratchLease>,
    ) -> Result<Self, ProtocolError> {
        if Self::retained_bytes(bytes.len()) > owner.bytes() {
            return Err(invalid_json());
        }
        let raw = serde_json::value::to_raw_value(&Base64(bytes)).map_err(|_| invalid_json())?;
        let cell = OnceLock::new();
        let _ = cell.set(owner);
        Ok(Self {
            kind,
            data: CanonicalJson(Arc::new(JsonStorage::Unattached {
                raw: Arc::from(raw),
                owner: cell,
            })),
        })
    }
    /// Memory that `from_stored` retains for `bytes` value bytes.
    pub(crate) fn retained_bytes(bytes: usize) -> usize {
        base64_len(bytes).saturating_add(256)
    }
    fn text(&self) -> &[u8] {
        self.data
            .as_str()
            .as_bytes()
            .get(1..self.data.as_str().len().saturating_sub(1))
            .unwrap_or_default()
    }
    /// The value bytes' length, or `None` for anything that is not base64.
    pub fn byte_len(&self) -> Option<usize> {
        let json = self.data.as_str().as_bytes();
        if json.len() < 2 || json[0] != b'"' || json[json.len() - 1] != b'"' {
            return None;
        }
        decoded_len(self.text()).filter(|len| *len <= MAX_VECTOR_BYTES)
    }
    /// Writes the stored record into `out`, which holds `FRAME_BYTES` plus the
    /// value bytes. `None` for anything that is not base64.
    pub(crate) fn frame(&self, out: &mut [u8]) -> Option<()> {
        let (head, bytes) = out.split_at_mut_checked(FRAME_BYTES)?;
        decode(self.text(), bytes)?;
        let kind = self.kind.code();
        head[0] = kind;
        head[1..].copy_from_slice(&checksum(kind, bytes).to_le_bytes());
        Some(())
    }
    /// The value bytes, for tests and readers that own their memory.
    pub fn to_bytes(&self) -> Option<Vec<u8>> {
        let mut out = vec![0; self.byte_len()?];
        decode(self.text(), &mut out)?;
        Some(out)
    }
    pub(crate) fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        self.data.heap_bytes()
    }
    pub(crate) fn attach_charge(&self, owner: Arc<ScratchLease>) -> Result<(), ProtocolError> {
        self.data.attach_charge(owner)
    }
    pub(crate) fn is_charged(&self) -> bool {
        self.data.is_charged()
    }
}

/// A stored record's kind and bytes, or `None` when it is damaged.
pub(crate) fn open_frame(raw: &[u8]) -> Option<(VectorKind, &[u8])> {
    let (head, bytes) = raw.split_at_checked(FRAME_BYTES)?;
    let kind = VectorKind::from_code(head[0])?;
    let stored = u32::from_le_bytes(head[1..].try_into().ok()?);
    (stored == checksum(head[0], bytes)).then_some((kind, bytes))
}
fn checksum(kind: u8, bytes: &[u8]) -> u32 {
    nabedra_kv::crc::extend(nabedra_kv::crc::crc32c(&[kind]), bytes)
}

/// One search column with a vector, as the group's replicated schema holds it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchColumn {
    pub id: ColumnId,
    pub kind: VectorKind,
    /// Dimensions of one vector, 0 for a sparse column.
    pub dim: u32,
    /// The text column an embedder makes this vector from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<Text<128>>,
}
impl SearchColumn {
    pub(crate) fn valid(&self) -> bool {
        let dim = match self.kind {
            VectorKind::Sparse => self.dim == 0,
            VectorKind::Dense | VectorKind::Multi => (1..=4096).contains(&self.dim),
        };
        dim && (self.source.is_none() || self.kind == VectorKind::Dense)
    }
    /// Whether `value` has this column's kind and dimensions.
    pub(crate) fn accepts(&self, value: &VectorValue) -> bool {
        let Some(len) = value.byte_len().filter(|_| value.kind == self.kind) else {
            return false;
        };
        let vector = self.dim as usize * 4;
        match self.kind {
            VectorKind::Dense => len == vector,
            VectorKind::Sparse => true,
            VectorKind::Multi => len.is_multiple_of(vector) && len / vector <= MAX_MULTI,
        }
    }
}

/// What a write of one row does to its vectors. A column that is not named
/// keeps its vector, and the row's source text decides about an embedded one.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowSearch {
    /// In ascending column order. `None` is null.
    pub vectors: Vec<VectorChange>,
    /// Embedded columns that backfill reached with this write.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub backfill: Vec<ColumnId>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VectorChange {
    pub column: ColumnId,
    pub value: Option<VectorValue>,
}
impl RowSearch {
    pub(crate) fn values(&self) -> impl Iterator<Item = &VectorValue> {
        self.vectors.iter().filter_map(|change| change.value.as_ref())
    }
    pub(crate) fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        let base = self.vectors.capacity() * std::mem::size_of::<VectorChange>()
            + self.backfill.capacity() * std::mem::size_of::<ColumnId>();
        self.values().try_fold(base, |bytes, value| {
            bytes.checked_add(value.heap_bytes()?).ok_or_else(invalid_json)
        })
    }
    pub(crate) fn attach_charge(&self, owner: Arc<ScratchLease>) -> Result<(), ProtocolError> {
        self.values()
            .try_for_each(|value| value.attach_charge(owner.clone()))
    }
    pub(crate) fn is_charged(&self) -> bool {
        self.values().all(VectorValue::is_charged)
    }
    /// Structure and sizes, against the group's search columns.
    pub(crate) fn valid(&self, columns: &[SearchColumn]) -> bool {
        let find = |id| columns.iter().find(|column| column.id == id);
        self.vectors.is_sorted_by(|a, b| a.column < b.column)
            && self.backfill.is_sorted_by(|a, b| a < b)
            && self.vectors.iter().all(|change| {
                find(change.column).is_some_and(|column| {
                    column.source.is_none()
                        && change.value.as_ref().is_none_or(|value| column.accepts(value))
                })
            })
            && self
                .backfill
                .iter()
                .all(|id| find(*id).is_some_and(|column| column.source.is_some()))
    }
}

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Standard base64 with padding, written without an intermediate buffer.
struct Base64<'a>(&'a [u8]);
impl fmt::Display for Base64<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for part in self.0.chunks(768) {
            let mut text = [0u8; 1024];
            let mut used = 0;
            for group in part.chunks(3) {
                let word = [0, 1, 2]
                    .iter()
                    .fold(0u32, |word, i| word << 8 | u32::from(*group.get(*i).unwrap_or(&0)));
                for (i, out) in text[used..used + 4].iter_mut().enumerate() {
                    *out = if i <= group.len() {
                        ALPHABET[(word >> (18 - 6 * i)) as usize & 63]
                    } else {
                        b'='
                    };
                }
                used += 4;
            }
            f.write_str(std::str::from_utf8(&text[..used]).map_err(|_| fmt::Error)?)?;
        }
        Ok(())
    }
}
impl Serialize for Base64<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}
fn base64_len(bytes: usize) -> usize {
    bytes.div_ceil(3).saturating_mul(4)
}
fn decoded_len(text: &[u8]) -> Option<usize> {
    if !text.len().is_multiple_of(4) {
        return None;
    }
    let padding = text.iter().rev().take(3).take_while(|byte| **byte == b'=').count();
    (text.len() / 4 * 3).checked_sub(padding).filter(|_| padding < 3)
}
fn digit(byte: u8) -> Option<u32> {
    Some(u32::from(match byte {
        b'A'..=b'Z' => byte - b'A',
        b'a'..=b'z' => byte - b'a' + 26,
        b'0'..=b'9' => byte - b'0' + 52,
        b'+' => 62,
        b'/' => 63,
        _ => return None,
    }))
}
/// Strict: one text per value, so equal bytes have equal commands and digests.
fn decode(text: &[u8], out: &mut [u8]) -> Option<()> {
    if decoded_len(text) != Some(out.len()) {
        return None;
    }
    for (group, bytes) in text.chunks_exact(4).zip(out.chunks_mut(3)) {
        let mut word = 0;
        for (i, byte) in group.iter().enumerate() {
            let value = if i <= bytes.len() {
                digit(*byte)?
            } else if *byte == b'=' {
                0
            } else {
                return None;
            };
            word = word << 6 | value;
        }
        let word: [u8; 4] = u32::to_be_bytes(word);
        if word[1 + bytes.len()..].iter().any(|byte| *byte != 0) {
            return None;
        }
        bytes.copy_from_slice(&word[1..1 + bytes.len()]);
    }
    Some(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oltp::pool::{ByteClass, SharedResourceConfig};
    use nabedra_resources::config::WorkerResources;

    fn shared() -> Arc<WorkerSharedResources> {
        WorkerSharedResources::new(0, WorkerResources::default(), SharedResourceConfig::default())
            .unwrap()
    }
    fn parse(text: &str) -> VectorValue {
        serde_json::from_value(serde_json::json!({"kind": "dense", "data": text})).unwrap()
    }

    #[test]
    fn vs16_vector_values_round_trip_every_length_and_reject_other_text() {
        let shared = shared();
        for len in (0..300).chain([767, 768, 769, 4096, MAX_VECTOR_BYTES]) {
            let bytes: Vec<u8> = (0..len).map(|i| (i * 31 + len) as u8).collect();
            let value = VectorValue::new(&shared, VectorKind::Multi, &bytes).unwrap();
            assert_eq!(value.byte_len(), Some(len));
            assert_eq!(value.to_bytes().unwrap(), bytes);
            let owner = shared
                .reserve_scratch(ByteClass::Data, VectorValue::retained_bytes(len))
                .unwrap();
            let stored = VectorValue::from_stored(value.kind, &bytes, Arc::new(owner)).unwrap();
            assert_eq!(stored, value);
            let mut frame = vec![0; FRAME_BYTES + len];
            value.frame(&mut frame).unwrap();
            assert_eq!(open_frame(&frame), Some((VectorKind::Multi, bytes.as_slice())));
        }
        assert!(VectorValue::new(&shared, VectorKind::Dense, &vec![0; MAX_VECTOR_BYTES + 1]).is_err());
        assert_eq!(parse("QUJD").to_bytes().unwrap(), b"ABC");
        assert_eq!(parse("QUI=").to_bytes().unwrap(), b"AB");
        // Not base64, not padded, or a second text for the same bytes.
        for text in ["Q", "QUJ", "QUJD=", "QU=D", "QUJ=", "Q===", "====", "QU-D", "QUI\\u003d", "A\u{e9}A"] {
            assert_eq!(parse(text).to_bytes(), None, "{text}");
        }
    }

    #[test]
    fn vs16_a_damaged_vector_record_never_opens() {
        let shared = shared();
        let value = VectorValue::new(&shared, VectorKind::Dense, &[1, 2, 3, 4, 5, 6, 7, 8]).unwrap();
        let mut frame = vec![0; FRAME_BYTES + 8];
        value.frame(&mut frame).unwrap();
        for bit in 0..frame.len() * 8 {
            let mut damaged = frame.clone();
            damaged[bit / 8] ^= 1 << (bit % 8);
            assert_eq!(open_frame(&damaged), None, "bit {bit}");
        }
        for len in 0..frame.len() {
            assert_eq!(open_frame(&frame[..len]), None, "length {len}");
        }
    }

    #[test]
    fn vs16_a_column_accepts_only_its_kind_and_dimensions() {
        let shared = shared();
        let value = |kind, len| VectorValue::new(&shared, kind, &vec![7; len]).unwrap();
        let column = |kind, dim| SearchColumn { id: 1, kind, dim, source: None };
        let dense = column(VectorKind::Dense, 3);
        assert!(dense.accepts(&value(VectorKind::Dense, 12)));
        assert!(!dense.accepts(&value(VectorKind::Dense, 16)));
        assert!(!dense.accepts(&value(VectorKind::Multi, 12)));
        let multi = column(VectorKind::Multi, 1);
        assert!(multi.accepts(&value(VectorKind::Multi, 0)));
        assert!(multi.accepts(&value(VectorKind::Multi, 4 * MAX_MULTI)));
        assert!(!multi.accepts(&value(VectorKind::Multi, 4 * MAX_MULTI + 4)));
        assert!(!multi.accepts(&value(VectorKind::Multi, 6)));
        assert!(!column(VectorKind::Dense, 0).valid());
        assert!(!column(VectorKind::Dense, 4097).valid());
        assert!(!column(VectorKind::Sparse, 1).valid());
    }
}
