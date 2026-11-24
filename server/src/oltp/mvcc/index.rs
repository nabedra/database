//! Operator-declared secondary/composite/covering index metadata and ordered
//! scalar keys. Building indexes are maintained by writers but never queried.
pub(crate) mod build;
use super::{catalog::PinnedCatalog, store::KeySpan};
use crate::{
    oltp::failure,
    protocol::{ErrorCode, ProtocolError, Row, Scalar, TableDescriptor},
};
use nabedra_oltp_types::types::{Page, StoragePin, Text};
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, collections::BTreeSet};
use uuid::Uuid;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum IndexValue {
    Int64(i64),
    FloatBits(u64),
    Bool(bool),
    Utf8(String),
}
impl IndexValue {
    pub fn from_scalar(value: &Scalar) -> Result<Self, ProtocolError> {
        Ok(match value {
            Scalar::Int64(v) => Self::Int64(*v),
            Scalar::Bool(v) => Self::Bool(*v),
            Scalar::Utf8(v) => Self::Utf8(v.clone()),
            Scalar::Float64(v) if v.is_finite() => {
                Self::FloatBits(if *v == 0.0 { 0 } else { v.to_bits() })
            }
            _ => return Err(invalid("nonfinite index scalar")),
        })
    }
    pub fn valid(&self) -> bool {
        match self {
            Self::FloatBits(v) => {
                f64::from_bits(*v).is_finite() && (*v == 0 || f64::from_bits(*v) != 0.0)
            }
            _ => true,
        }
    }
    pub fn heap_bytes(&self) -> usize {
        match self {
            Self::Utf8(v) => v.capacity(),
            _ => 0,
        }
    }
    pub fn encoded_len(&self) -> usize {
        match self {
            Self::Int64(_) | Self::FloatBits(_) => 9,
            Self::Bool(_) => 2,
            Self::Utf8(s) => 1 + s.len() + s.bytes().filter(|b| *b == 0).count() + 2,
        }
    }
    pub(crate) fn encode(&self, out: &mut [u8]) -> usize {
        match self {
            Self::Int64(v) => {
                out[0] = 1;
                out[1..9].copy_from_slice(&((*v as u64) ^ (1 << 63)).to_be_bytes());
                9
            }
            Self::FloatBits(v) => {
                out[0] = 2;
                out[1..9].copy_from_slice(&float_order(*v).to_be_bytes());
                9
            }
            Self::Bool(v) => {
                out[0] = 3;
                out[1] = u8::from(*v);
                2
            }
            Self::Utf8(v) => {
                out[0] = 4;
                let mut i = 1;
                for byte in v.bytes() {
                    out[i] = byte;
                    i += 1;
                    if byte == 0 {
                        out[i] = 255;
                        i += 1;
                    }
                }
                out[i..i + 2].copy_from_slice(&[0, 0]);
                i + 2
            }
        }
    }
    pub(crate) fn decode(bytes: &[u8]) -> Result<(Self, usize), ProtocolError> {
        match bytes.first() {
            Some(tag @ (1 | 2)) if bytes.len() >= 9 => {
                let encoded = u64::from_be_bytes(
                    bytes[1..9]
                        .try_into()
                        .map_err(|_| invalid("index scalar"))?,
                );
                let value = if *tag == 1 {
                    Self::Int64((encoded ^ (1 << 63)) as i64)
                } else {
                    Self::FloatBits(if encoded & (1 << 63) != 0 {
                        encoded ^ (1 << 63)
                    } else {
                        !encoded
                    })
                };
                if !value.valid() {
                    return Err(invalid("index float encoding"));
                }
                Ok((value, 9))
            }
            Some(3) if bytes.len() >= 2 && bytes[1] <= 1 => Ok((Self::Bool(bytes[1] != 0), 2)),
            Some(4) => {
                let mut end = 1;
                while end < bytes.len() {
                    if bytes[end] != 0 {
                        end += 1
                    } else {
                        match bytes.get(end + 1) {
                            Some(255) => end += 2,
                            Some(0) => {
                                end += 2;
                                break;
                            }
                            _ => return Err(invalid("index escape")),
                        }
                    }
                }
                if end > bytes.len() {
                    return Err(invalid("index terminator"));
                }
                let mut raw = Vec::new();
                raw.try_reserve_exact(end.saturating_sub(1))
                    .map_err(|_| invalid("index key allocation"))?;
                let mut i = 1;
                while i < bytes.len() {
                    if bytes[i] != 0 {
                        raw.push(bytes[i]);
                        i += 1
                    } else {
                        match bytes.get(i + 1) {
                            Some(255) => {
                                raw.push(0);
                                i += 2
                            }
                            Some(0) => {
                                return Ok((
                                    Self::Utf8(
                                        String::from_utf8(raw)
                                            .map_err(|_| invalid("index UTF8"))?,
                                    ),
                                    i + 2,
                                ));
                            }
                            _ => return Err(invalid("index escape")),
                        }
                    }
                }
                Err(invalid("index terminator"))
            }
            _ => Err(invalid("index scalar tag")),
        }
    }
}
fn float_order(bits: u64) -> u64 {
    if bits & (1 << 63) != 0 {
        !bits
    } else {
        bits ^ (1 << 63)
    }
}
impl Ord for IndexValue {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Int64(a), Self::Int64(b)) => a.cmp(b),
            (Self::FloatBits(a), Self::FloatBits(b)) => float_order(*a).cmp(&float_order(*b)),
            (Self::Bool(a), Self::Bool(b)) => a.cmp(b),
            (Self::Utf8(a), Self::Utf8(b)) => a.cmp(b),
            _ => tag(self).cmp(&tag(other)),
        }
    }
}
impl PartialOrd for IndexValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
fn tag(v: &IndexValue) -> u8 {
    match v {
        IndexValue::Int64(_) => 1,
        IndexValue::FloatBits(_) => 2,
        IndexValue::Bool(_) => 3,
        IndexValue::Utf8(_) => 4,
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IndexDefinition {
    pub index_id: Uuid,
    pub table_id: Uuid,
    pub name: Text<64>,
    pub columns: Page<Text<64>>,
    pub covering: Page<Text<64>>,
}
impl IndexDefinition {
    pub(crate) fn validate(&self, table: &TableDescriptor) -> Result<(), ProtocolError> {
        if self.index_id.is_nil() || self.table_id != table.id || self.columns.is_empty() {
            return Err(invalid("invalid index identity or columns"));
        }
        crate::protocol::validate_identifier(self.name.as_str())?;
        let mut seen = BTreeSet::new();
        for column in self.columns.iter() {
            if !table.schema.contains_key(column.as_str()) || !seen.insert(column.as_str()) {
                return Err(invalid("unknown or duplicate index column"));
            }
        }
        seen.clear();
        for column in self.covering.iter() {
            if !table.schema.contains_key(column.as_str()) || !seen.insert(column.as_str()) {
                return Err(invalid("unknown or duplicate covering column"));
            }
        }
        Ok(())
    }
    /// The unique primary key already ends every physical entry. Columns at
    /// and after an explicitly listed primary key cannot break any further row
    /// ties, so they are stored once as cover data instead of repeating the key.
    pub(crate) fn physical_columns<'a>(&'a self, table: &TableDescriptor) -> &'a [Text<64>] {
        let end = self
            .columns
            .iter()
            .position(|c| c.as_str() == table.key)
            .unwrap_or(self.columns.len());
        &self.columns[..end]
    }
    pub(crate) fn values(
        &self,
        table: &TableDescriptor,
        row: &Row,
    ) -> Result<Page<IndexValue>, ProtocolError> {
        Page::new(
            self.physical_columns(table)
                .iter()
                .map(|c| IndexValue::from_scalar(&row[c.as_str()]))
                .collect::<Result<Vec<_>, _>>()?,
        )
        .map_err(|_| invalid("index tuple width"))
    }
    pub(crate) fn cover(&self, table: &TableDescriptor, row: &Row) -> Row {
        let mut cover = Row::new();
        for (column, value) in row {
            if column == &table.key
                || self
                    .columns
                    .iter()
                    .chain(self.covering.iter())
                    .any(|c| c.as_str() == column)
            {
                cover.insert(column.clone(), value.clone());
            }
        }
        cover
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum IndexPhase {
    Building {
        build_id: Uuid,
        attempt: u64,
        pin: StoragePin,
        source: PinnedCatalog,
    },
    Ready {
        build_id: Uuid,
        attempt: u64,
        pin: StoragePin,
    },
    Failed {
        build_id: Uuid,
        attempt: u64,
        pin: StoragePin,
    },
    Retired {
        build_id: Uuid,
        attempt: u64,
        pin: StoragePin,
    },
}
impl IndexPhase {
    pub(crate) fn attempt_binding(&self) -> (Uuid, u64, StoragePin) {
        match self {
            Self::Building {
                build_id,
                attempt,
                pin,
                ..
            }
            | Self::Ready {
                build_id,
                attempt,
                pin,
            }
            | Self::Failed {
                build_id,
                attempt,
                pin,
            }
            | Self::Retired {
                build_id,
                attempt,
                pin,
            } => (*build_id, *attempt, *pin),
        }
    }
    pub(crate) fn writable(&self) -> bool {
        matches!(self, Self::Building { .. } | Self::Ready { .. })
    }
    pub(crate) fn ready(&self) -> bool {
        matches!(self, Self::Ready { .. })
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IndexRecord {
    pub definition: IndexDefinition,
    pub phase: IndexPhase,
    pub schema_epoch: u64,
    pub routing_epoch: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IndexNameRecord {
    pub index_id: Uuid,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IndexRoot {
    pub first_page: Option<Uuid>,
    pub last_page: Option<Uuid>,
    pub epoch: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IndexPage {
    pub indexes: Page<Uuid>,
    pub next_page: Option<Uuid>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IndexProgress {
    pub build_id: Uuid,
    pub attempt: u64,
    pub group_id: Uuid,
    pub span: KeySpan,
    pub done: bool,
}
fn invalid(message: &'static str) -> ProtocolError {
    failure(ErrorCode::ValidationError, message)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum IndexClosedKind {
    Ready,
    Failed,
    Retired,
    Superseded,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IndexAttemptClosedRecord {
    pub owner: nabedra_oltp_types::types::MaintenanceOwnerRef,
    pub pin: StoragePin,
    pub closed_by: nabedra_oltp_types::types::TxnId,
    pub kind: IndexClosedKind,
}
pub(crate) fn closure_id(
    owner: &nabedra_oltp_types::types::MaintenanceOwnerRef,
) -> Result<Uuid, ProtocolError> {
    use sha2::{Digest as _, Sha256};
    let bytes = serde_json::to_vec(owner).map_err(|_| invalid("index owner"))?;
    let mut hash = Sha256::new();
    hash.update(b"nabedra/index-closed/v1\0");
    hash.update(bytes);
    let result = hash.finalize();
    Ok(Uuid::from_bytes(
        result[..16].try_into().expect("fixed digest"),
    ))
}

pub(crate) fn progress_id(index: Uuid, group: Uuid) -> Uuid {
    use sha2::{Digest as _, Sha256};
    let mut hash = Sha256::new();
    hash.update(b"nabedra/index-progress/v1\0");
    hash.update(index.as_bytes());
    hash.update(group.as_bytes());
    Uuid::from_bytes(hash.finalize()[..16].try_into().expect("fixed digest"))
}
