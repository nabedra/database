//! Exact mutation outcomes contain only the existing metadata/unit/count/bool
//! results or a bounded protocol error. Row/query results are never accepted.
use super::proofs::invalid;
use crate::{
    owned::serialized_digest,
    types::{Digest, Page, Text},
};
use nabedra_protocol::protocol::{
    ColumnType, Embed, ErrorCode, ErrorResponse, Outcome, ProtocolError, SearchOptions,
    SuccessResponse, TableDescriptor, TableFormat,
};
use nabedra_resources::memory::ScratchLease;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt};
/// Equality describes the durable value. The local ownership lease is excluded
/// from serialization and follows every clone of heap-bearing reply metadata.
#[derive(Clone, Default)]
struct ReplyOwner(Option<std::sync::Arc<ScratchLease>>);
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
pub struct OutcomeColumn {
    name: Text<64>,
    /// Its decoder owns no text.
    ty: ColumnType,
}
/// At most 128 named columns, each once.
struct Columns<T>(BTreeMap<Text<64>, T>);
impl<T> Default for Columns<T> {
    fn default() -> Self {
        Self(BTreeMap::new())
    }
}
impl<T> Columns<T> {
    fn page<U>(self, item: impl Fn(Text<64>, T) -> U) -> Result<Page<U>, ProtocolError> {
        let items = self.0.into_iter().map(|(name, value)| item(name, value));
        Page::new(items.collect()).map_err(|_| invalid())
    }
}
impl<'de, T: Deserialize<'de>> Deserialize<'de> for Columns<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor<T>(std::marker::PhantomData<T>);
        impl<'de, T: Deserialize<'de>> serde::de::Visitor<'de> for Visitor<T> {
            type Value = Columns<T>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("at most 128 named columns")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Columns<T>, A::Error> {
                let mut out = BTreeMap::new();
                while out.len() < 128 {
                    let Some(key) = map.next_key::<Text<64>>()? else {
                        return Ok(Columns(out));
                    };
                    if out.contains_key(&key) {
                        return Err(serde::de::Error::custom("duplicate schema column"));
                    }
                    out.insert(key, map.next_value()?);
                }
                if map.next_key::<serde::de::IgnoredAny>()?.is_some() {
                    return Err(serde::de::Error::custom("too many schema columns"));
                }
                Ok(Columns(out))
            }
        }
        deserializer.deserialize_map(Visitor(std::marker::PhantomData))
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EmbedText {
    source: Text<64>,
    model: Text<64>,
}
/// `SearchOptions` with byte-limited text, one entry per column.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutcomeSearch {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    partition_by: Option<Text<64>>,
    full_text: Page<OutcomeFullText>,
    embed: Page<OutcomeEmbed>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutcomeFullText {
    column: Text<64>,
    language: Text<32>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutcomeEmbed {
    column: Text<64>,
    source: Text<64>,
    model: Text<64>,
}
impl OutcomeSearch {
    fn options(&self) -> SearchOptions {
        SearchOptions {
            partition_by: self.partition_by.as_ref().map(|name| name.to_string()),
            full_text: self
                .full_text
                .iter()
                .map(|f| (f.column.to_string(), f.language.to_string()))
                .collect(),
            embed: self
                .embed
                .iter()
                .map(|e| {
                    let embed = Embed {
                        source: e.source.to_string(),
                        model: e.model.to_string(),
                    };
                    (e.column.to_string(), embed)
                })
                .collect(),
        }
    }
    fn heap_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.partition_by.as_ref().map_or(0, Text::capacity_bytes)
            + self.full_text.capacity() * std::mem::size_of::<OutcomeFullText>()
            + self
                .full_text
                .iter()
                .map(|f| f.column.capacity_bytes() + f.language.capacity_bytes())
                .sum::<usize>()
            + self.embed.capacity() * std::mem::size_of::<OutcomeEmbed>()
            + self
                .embed
                .iter()
                .map(|e| {
                    e.column.capacity_bytes() + e.source.capacity_bytes() + e.model.capacity_bytes()
                })
                .sum::<usize>()
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutcomeTable {
    id: uuid::Uuid,
    name: Text<64>,
    key: Text<64>,
    schema: Page<OutcomeColumn>,
    /// Absent on tables without search options.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    search: Option<Box<OutcomeSearch>>,
    #[serde(skip)]
    owner: ReplyOwner,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum MutationValue {
    Unit,
    Count(u64),
    Changed(bool),
    Created(OutcomeTable),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationFailure {
    code: ErrorCode,
    message: Text<4096>,
    outcome: Outcome,
    #[serde(skip)]
    owner: ReplyOwner,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum MutationOutcome {
    Success(MutationValue),
    Failure(MutationFailure),
}
impl MutationOutcome {
    pub fn is_not_applied(&self) -> bool {
        matches!(
            self,
            Self::Failure(MutationFailure {
                outcome: Outcome::NotApplied,
                ..
            })
        )
    }
    pub fn retain_owner(&mut self, owner: std::sync::Arc<ScratchLease>) {
        match self {
            Self::Success(MutationValue::Created(table)) => table.owner.0 = Some(owner),
            Self::Failure(error) => error.owner.0 = Some(owner),
            _ => {}
        }
    }
    pub fn response_hash(&self, request_id: uuid::Uuid) -> Result<Digest, ProtocolError> {
        match self.result() {
            Ok(data) => serialized_digest(&SuccessResponse { request_id, data }),
            Err(error) => serialized_digest(&ErrorResponse {
                request_id: Some(request_id),
                error,
            }),
        }
    }
    pub fn digest(&self) -> Result<Digest, ProtocolError> {
        let value = match self.result() {
            Ok(value) => value,
            Err(error) => serde_json::to_value(error).map_err(|_| invalid())?,
        };
        serialized_digest(&value)
    }
    pub fn from_success(json: &impl AsRef<str>) -> Result<Self, ProtocolError> {
        let input = json.as_ref();
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
                    schema: Columns<ColumnType>,
                    #[serde(default)]
                    partition_by: Option<Text<64>>,
                    #[serde(default)]
                    full_text: Columns<Text<32>>,
                    #[serde(default)]
                    embed: Columns<EmbedText>,
                }
                let table: Descriptor = serde_json::from_str(input).map_err(|_| invalid())?;
                if table.format.as_str() != "parquet" {
                    return Err(invalid());
                }
                let search = OutcomeSearch {
                    partition_by: table.partition_by,
                    full_text: table
                        .full_text
                        .page(|column, language| OutcomeFullText { column, language })?,
                    embed: table.embed.page(|column, embed| OutcomeEmbed {
                        column,
                        source: embed.source,
                        model: embed.model,
                    })?,
                };
                let created = OutcomeTable {
                    owner: ReplyOwner::default(),
                    id: table.id,
                    name: table.name,
                    key: table.key,
                    schema: table.schema.page(|name, ty| OutcomeColumn { name, ty })?,
                    search: (!search.options().is_empty()).then(|| Box::new(search)),
                };
                created.descriptor()?.validate()?;
                MutationValue::Created(created)
            }
            _ => return Err(invalid()),
        };
        Ok(Self::Success(value))
    }
    pub fn failure(error: ProtocolError) -> Result<Self, ProtocolError> {
        Ok(Self::Failure(MutationFailure {
            owner: ReplyOwner::default(),
            code: error.code,
            message: Text::new(error.message).map_err(|_| invalid())?,
            outcome: error.outcome,
        }))
    }
    pub fn heap_bytes(&self) -> usize {
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
                    + table
                        .search
                        .as_ref()
                        .map_or(0, |search| search.heap_bytes())
            }
            Self::Failure(error) => error.message.capacity_bytes(),
            _ => 0,
        }
    }
    pub fn result(&self) -> Result<serde_json::Value, ProtocolError> {
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
            search: self
                .search
                .as_ref()
                .map(|search| search.options())
                .unwrap_or_default(),
        };
        descriptor.validate()?;
        Ok(descriptor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const PLAIN: &str = r#"{"id":"67e55044-10b1-426f-9247-bb680e5fe0c8","name":"users","schema":{"active":"bool","age":"int64","id":"utf8","score":"float64"},"key":"id","format":"parquet"}"#;

    /// Captured at 265a111d, before vector columns existed.
    #[test]
    fn vec_g14_a_created_table_without_search_options_keeps_its_bytes_and_digests() {
        let created = MutationOutcome::from_success(&PLAIN).unwrap();
        let stored = r#"{"success":{"created":{"id":"67e55044-10b1-426f-9247-bb680e5fe0c8","name":"users","key":"id","schema":[{"name":"active","ty":"bool"},{"name":"age","ty":"int64"},{"name":"id","ty":"utf8"},{"name":"score","ty":"float64"}]}}}"#;
        assert_eq!(serde_json::to_string(&created).unwrap(), stored);
        assert_eq!(
            serde_json::from_str::<MutationOutcome>(stored).unwrap(),
            created
        );
        assert_eq!(
            created.digest().unwrap().to_string(),
            "776b607dd25fd7e7f8ede7ae8a08956ec4b2688345cc281450dabf1bd82db66c"
        );
        let response = created.response_hash(uuid::Uuid::from_u128(7)).unwrap();
        assert_eq!(
            response.to_string(),
            "d7c9fbc3180738cafbb05afcf051bfe560092e94df602e3003ed0b0b34a330a0"
        );
        assert_eq!(created.heap_bytes(), 151);
    }

    #[test]
    fn a_created_search_table_round_trips_and_stays_bounded() {
        let table = r#"{"id":"67e55044-10b1-426f-9247-bb680e5fe0c8","name":"chunks","schema":{"acl":{"type":"list","item":"utf8"},"body":"utf8","id":"utf8","m":{"type":"list","item":{"type":"vector","dim":128}},"s":{"type":"vector","sparse":true},"v":{"type":"vector","dim":4096}},"key":"id","format":"parquet","partition_by":"id","full_text":{"body":"english"},"embed":{"v":{"source":"body","model":"docs-v1"}}}"#;
        let created = MutationOutcome::from_success(&table).unwrap();
        let stored = serde_json::to_string(&created).unwrap();
        let again: MutationOutcome = serde_json::from_str(&stored).unwrap();
        assert_eq!(again, created);
        let descriptor: TableDescriptor = serde_json::from_str(table).unwrap();
        assert_eq!(
            again.result().unwrap(),
            serde_json::to_value(&descriptor).unwrap()
        );
        assert_eq!(again.digest().unwrap(), created.digest().unwrap());
        let long = "x".repeat(65);
        for bad in [
            table.replace("english", &"x".repeat(33)),
            table.replace("docs-v1", &long),
            table.replace(
                r#""partition_by":"id""#,
                &format!(r#""partition_by":"{long}""#),
            ),
            table.replace(r#""partition_by":"id""#, r#""partition_by":"acl""#),
            table.replace(
                r#"{"body":"english"}"#,
                r#"{"body":"english","body":"english"}"#,
            ),
            table.replace("4096", "4097"),
            table.replace(r#""sparse":true"#, r#""sparse":true,"x":1"#),
        ] {
            assert_ne!(bad, table);
            assert!(MutationOutcome::from_success(&bad).is_err(), "{bad}");
        }
        let bad = stored.replace("english", &long);
        assert!(serde_json::from_str::<MutationOutcome>(&bad).is_err());
    }
}
