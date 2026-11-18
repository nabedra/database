//! Exact mutation outcomes contain only the existing metadata/unit/count/bool
//! results or a bounded protocol error. Row/query results are never accepted.
use super::*;
use crate::{
    oltp::mvcc::CanonicalJson,
    oltp_replication::types::{Page, Text},
    protocol::{ColumnType, TableDescriptor, TableFormat},
};
use std::{collections::BTreeMap, fmt};
/// Equality describes the durable value. The local ownership lease is excluded
/// from serialization and follows every clone of heap-bearing reply metadata.
#[derive(Clone, Default)]
struct ReplyOwner(Option<std::sync::Arc<crate::oltp::pool::ScratchLease>>);
impl fmt::Debug for ReplyOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ReplyOwner")
    }
}
impl PartialEq for ReplyOwner {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for ReplyOwner {}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OutcomeColumn {
    name: Text<64>,
    #[serde(deserialize_with = "bounded_column_type")]
    ty: ColumnType,
}
fn bounded_column_type<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<ColumnType, D::Error> {
    let ty = Text::<7>::deserialize(deserializer)?;
    match ty.as_str() {
        "utf8" => Ok(ColumnType::Utf8),
        "int64" => Ok(ColumnType::Int64),
        "float64" => Ok(ColumnType::Float64),
        "bool" => Ok(ColumnType::Bool),
        _ => Err(serde::de::Error::custom("invalid bounded column type")),
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OutcomeTable {
    id: uuid::Uuid,
    name: Text<64>,
    key: Text<64>,
    schema: Page<OutcomeColumn>,
    #[serde(skip)]
    owner: ReplyOwner,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum MutationValue {
    Unit,
    Count(u64),
    Changed(bool),
    Created(OutcomeTable),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MutationFailure {
    code: ErrorCode,
    message: Text<4096>,
    outcome: Outcome,
    #[serde(skip)]
    owner: ReplyOwner,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum MutationOutcome {
    Success(MutationValue),
    Failure(MutationFailure),
}
impl MutationOutcome {
    pub(crate) fn is_not_applied(&self) -> bool {
        matches!(
            self,
            Self::Failure(MutationFailure {
                outcome: Outcome::NotApplied,
                ..
            })
        )
    }
    pub(crate) fn retain_owner(&mut self, owner: std::sync::Arc<crate::oltp::pool::ScratchLease>) {
        match self {
            Self::Success(MutationValue::Created(table)) => table.owner.0 = Some(owner),
            Self::Failure(error) => error.owner.0 = Some(owner),
            _ => {}
        }
    }
    pub(crate) fn response_hash(&self, request_id: uuid::Uuid) -> Result<Digest, ProtocolError> {
        match self.result() {
            Ok(data) => crate::oltp_replication::owned::serialized_digest(
                &crate::protocol::SuccessResponse { request_id, data },
            ),
            Err(error) => {
                crate::oltp_replication::owned::serialized_digest(&crate::protocol::ErrorResponse {
                    request_id: Some(request_id),
                    error,
                })
            }
        }
    }
    pub(crate) fn digest(&self) -> Result<Digest, ProtocolError> {
        let value = match self.result() {
            Ok(value) => value,
            Err(error) => serde_json::to_value(error).map_err(|_| invalid())?,
        };
        crate::oltp_replication::owned::serialized_digest(&value)
    }
    pub(crate) fn from_success(json: &CanonicalJson) -> Result<Self, ProtocolError> {
        let input = json.as_str();
        let value = match input.as_bytes().first() {
            Some(b'n') if input == "null" => MutationValue::Unit,
            Some(b't' | b'f') => {
                MutationValue::Changed(serde_json::from_str(input).map_err(|_| invalid())?)
            }
            Some(b'0'..=b'9') => {
                MutationValue::Count(serde_json::from_str(input).map_err(|_| invalid())?)
            }
            Some(b'{') => {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Descriptor {
                    id: uuid::Uuid,
                    name: Text<64>,
                    key: Text<64>,
                    format: Text<7>,
                    schema: Columns,
                }
                struct Columns(BTreeMap<Text<64>, ColumnType>);
                impl<'de> Deserialize<'de> for Columns {
                    fn deserialize<D: serde::Deserializer<'de>>(
                        deserializer: D,
                    ) -> Result<Self, D::Error> {
                        struct Visitor;
                        impl<'de> serde::de::Visitor<'de> for Visitor {
                            type Value = Columns;
                            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                                f.write_str("at most128 typed schema columns")
                            }
                            fn visit_map<A: serde::de::MapAccess<'de>>(
                                self,
                                mut map: A,
                            ) -> Result<Columns, A::Error> {
                                let mut out = BTreeMap::new();
                                while out.len() < 128 {
                                    let Some(key) = map.next_key::<Text<64>>()? else {
                                        return Ok(Columns(out));
                                    };
                                    if out.contains_key(&key) {
                                        return Err(serde::de::Error::custom(
                                            "duplicate schema column",
                                        ));
                                    }
                                    let ty = map.next_value::<Text<7>>()?;
                                    let ty = match ty.as_str() {
                                        "utf8" => ColumnType::Utf8,
                                        "int64" => ColumnType::Int64,
                                        "float64" => ColumnType::Float64,
                                        "bool" => ColumnType::Bool,
                                        _ => {
                                            return Err(serde::de::Error::custom(
                                                "invalid column type",
                                            ));
                                        }
                                    };
                                    out.insert(key, ty);
                                }
                                if map.next_key::<serde::de::IgnoredAny>()?.is_some() {
                                    return Err(serde::de::Error::custom(
                                        "too many schema columns",
                                    ));
                                }
                                Ok(Columns(out))
                            }
                        }
                        deserializer.deserialize_map(Visitor)
                    }
                }
                let table: Descriptor = serde_json::from_str(input).map_err(|_| invalid())?;
                if table.format.as_str() != "parquet" {
                    return Err(invalid());
                }
                let created = OutcomeTable {
                    owner: ReplyOwner::default(),
                    id: table.id,
                    name: table.name,
                    key: table.key,
                    schema: Page::new(
                        table
                            .schema
                            .0
                            .into_iter()
                            .map(|(name, ty)| OutcomeColumn { name, ty })
                            .collect(),
                    )
                    .map_err(|_| invalid())?,
                };
                created.descriptor()?.validate()?;
                MutationValue::Created(created)
            }
            _ => return Err(invalid()),
        };
        Ok(Self::Success(value))
    }
    pub(crate) fn failure(error: ProtocolError) -> Result<Self, ProtocolError> {
        Ok(Self::Failure(MutationFailure {
            owner: ReplyOwner::default(),
            code: error.code,
            message: Text::new(error.message).map_err(|_| invalid())?,
            outcome: error.outcome,
        }))
    }
    pub(crate) fn heap_bytes(&self) -> usize {
        match self {
            Self::Success(MutationValue::Created(table)) => {
                table.name.capacity_bytes()
                    + table.key.capacity_bytes()
                    + table.schema.capacity() * std::mem::size_of::<OutcomeColumn>()
                    + table
                        .schema
                        .iter()
                        .map(|column| column.name.capacity_bytes())
                        .sum::<usize>()
            }
            Self::Failure(error) => error.message.capacity_bytes(),
            _ => 0,
        }
    }
    pub(crate) fn result(&self) -> Result<serde_json::Value, ProtocolError> {
        match self {
            Self::Success(value) => match value {
                MutationValue::Unit => Ok(serde_json::Value::Null),
                MutationValue::Changed(value) => Ok((*value).into()),
                MutationValue::Count(value) => Ok((*value).into()),
                MutationValue::Created(table) => {
                    serde_json::to_value(table.descriptor()?).map_err(|_| invalid())
                }
            },
            Self::Failure(error) => Err(ProtocolError::new(
                error.code,
                error.message.as_str(),
                error.outcome,
            )),
        }
    }
}
impl OutcomeTable {
    fn descriptor(&self) -> Result<TableDescriptor, ProtocolError> {
        let mut schema = BTreeMap::new();
        let mut previous = None;
        for column in self.schema.iter() {
            if previous.is_some_and(|name: &str| name >= column.name.as_str()) {
                return Err(invalid());
            }
            previous = Some(column.name.as_str());
            schema.insert(column.name.as_str().to_owned(), column.ty);
        }
        let descriptor = TableDescriptor {
            id: self.id,
            name: self.name.as_str().to_owned(),
            key: self.key.as_str().to_owned(),
            format: TableFormat::Parquet,
            schema,
        };
        descriptor.validate()?;
        Ok(descriptor)
    }
}
