//! Wire data and validation only. Call `parse_request`, resolve a table, then
//! `Operation::validate_for_table` before executing table-bound operations.
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::DeserializeOwned};
use serde_json::{Value, value::RawValue};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use uuid::Uuid;

pub const MAX_BODY_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_CATALOG_BYTES: usize = 1024 * 1024;
pub const MAX_BATCH_ROWS: usize = 10_000;
pub const MAX_RESULT_ROWS: usize = 10_000;
pub const MAX_TABLE_ROWS: usize = 100_000;
pub const MAX_DECODED_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_SCHEMA_COLUMNS: usize = 128;
/// Root predicate has depth one, including comparison leaves.
pub const MAX_PREDICATE_DEPTH: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    ValidationError,
    UnsupportedOperation,
    Unauthorized,
    NotFound,
    Conflict,
    LimitExceeded,
    Overloaded,
    StorageUnavailable,
    OutcomeUnknown,
    InternalError,
}
impl ErrorCode {
    pub fn status_code(self) -> u16 {
        match self {
            Self::ValidationError | Self::UnsupportedOperation => 400,
            Self::Unauthorized => 401,
            Self::NotFound => 404,
            Self::Conflict => 409,
            Self::LimitExceeded => 413,
            Self::Overloaded => 429,
            Self::StorageUnavailable | Self::OutcomeUnknown => 503,
            Self::InternalError => 500,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    NotApplied,
    Unknown,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtocolError {
    pub code: ErrorCode,
    pub message: String,
    pub outcome: Outcome,
}
impl ProtocolError {
    pub fn new(code: ErrorCode, message: impl Into<String>, outcome: Outcome) -> Self {
        Self {
            code,
            message: message.into(),
            outcome,
        }
    }
    pub fn status_code(&self) -> u16 {
        self.code.status_code()
    }
}
impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for ProtocolError {}
fn invalid(message: impl Into<String>) -> ProtocolError {
    ProtocolError::new(ErrorCode::ValidationError, message, Outcome::NotApplied)
}
fn limit(message: &str) -> ProtocolError {
    ProtocolError::new(ErrorCode::LimitExceeded, message, Outcome::NotApplied)
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuccessResponse<T> {
    pub request_id: Uuid,
    pub data: T,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErrorResponse {
    pub request_id: Option<Uuid>,
    pub error: ProtocolError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ColumnType {
    #[serde(rename = "utf8")]
    Utf8,
    #[serde(rename = "int64")]
    Int64,
    #[serde(rename = "float64")]
    Float64,
    #[serde(rename = "bool")]
    Bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum TableFormat {
    #[serde(rename = "parquet")]
    Parquet,
}
// Deserialize a string before matching variants: Serde's derived unit enums
// also accept externally tagged objects, which are not part of this wire format.
macro_rules! string_enum_deserialize {
    ($ty:ident, $($wire:literal => $variant:ident),+ $(,)?) => {
        impl<'de> Deserialize<'de> for $ty {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                match String::deserialize(d)?.as_str() {
                    $($wire => Ok(Self::$variant),)+
                    other => Err(serde::de::Error::unknown_variant(other, &[$($wire),+])),
                }
            }
        }
    };
}
string_enum_deserialize!(ColumnType, "utf8" => Utf8, "int64" => Int64,
    "float64" => Float64, "bool" => Bool);
string_enum_deserialize!(TableFormat, "parquet" => Parquet);
string_enum_deserialize!(ComparisonOp, "eq" => Eq, "ne" => Ne, "gt" => Gt,
    "ge" => Ge, "lt" => Lt, "le" => Le);

pub type Schema = BTreeMap<String, ColumnType>;
pub type Row = BTreeMap<String, Scalar>;

/// No null, arrays, objects, numeric coercion, or executable expressions.
/// Deserialization requires JSON raw-value support so syntax is checked before
/// serde_json can interpret its private number markers. Deserialize directly
/// from JSON or a serde_json::Value, rather than through Serde content buffers.
#[derive(Debug, Clone, PartialEq)]
pub enum Scalar {
    Utf8(String),
    Int64(i64),
    Float64(f64),
    Bool(bool),
}
impl<'de> Deserialize<'de> for Scalar {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = Box::<RawValue>::deserialize(d)?;
        let text = raw.get().trim_start();
        if text.starts_with(['{', '[']) || text == "null" {
            return Err(serde::de::Error::custom("expected a non-null scalar"));
        }
        match serde_json::from_str::<Value>(text).map_err(serde::de::Error::custom)? {
            Value::String(v) => Ok(Self::Utf8(v)),
            Value::Bool(v) => Ok(Self::Bool(v)),
            Value::Number(v) => {
                // arbitrary_precision retains integer syntax even outside i64 bounds.
                let text = v.to_string();
                if text.contains(['.', 'e', 'E']) {
                    let n = text.parse::<f64>().map_err(serde::de::Error::custom)?;
                    if n.is_finite() {
                        Ok(Self::Float64(n))
                    } else {
                        Err(serde::de::Error::custom("float64 must be finite"))
                    }
                } else {
                    text.parse::<i64>()
                        .map(Self::Int64)
                        .map_err(serde::de::Error::custom)
                }
            }
            _ => Err(serde::de::Error::custom("expected a non-null scalar")),
        }
    }
}
impl Serialize for Scalar {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Utf8(v) => s.serialize_str(v),
            Self::Int64(v) => s.serialize_i64(*v),
            Self::Bool(v) => s.serialize_bool(*v),
            Self::Float64(v) if v.is_finite() => s.serialize_f64(*v),
            Self::Float64(_) => Err(serde::ser::Error::custom("float64 must be finite")),
        }
    }
}
/// Key syntax is restricted before a schema is available.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Key {
    Utf8(String),
    Int64(i64),
}
impl Key {
    pub fn validate(&self, ty: ColumnType) -> Result<(), ProtocolError> {
        match (self, ty) {
            (Self::Utf8(_), ColumnType::Utf8) | (Self::Int64(_), ColumnType::Int64) => Ok(()),
            _ => Err(invalid("key type does not match schema")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableDescriptor {
    pub id: Uuid,
    pub name: String,
    pub schema: Schema,
    pub key: String,
    pub format: TableFormat,
}
impl TableDescriptor {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_schema(&self.name, &self.schema, &self.key)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreatePayload {
    pub name: String,
    pub schema: Schema,
    pub key: String,
    pub format: TableFormat,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DescribePayload {
    pub name: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmptyPayload {}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TablePayload {
    pub table_id: Uuid,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowPayload {
    pub table_id: Uuid,
    pub row: Row,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchPayload {
    pub table_id: Uuid,
    pub rows: Vec<Row>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyPayload {
    pub table_id: Uuid,
    pub key: Key,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdatePayload {
    pub table_id: Uuid,
    pub key: Key,
    pub values: Row,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryPayload {
    pub table_id: Uuid,
    #[serde(deserialize_with = "deserialize_object")]
    pub query: Query,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "operation", content = "payload", deny_unknown_fields)]
pub enum Operation {
    #[serde(rename = "table.create")]
    Create(CreatePayload),
    #[serde(rename = "table.describe")]
    Describe(DescribePayload),
    #[serde(rename = "table.list")]
    List(EmptyPayload),
    #[serde(rename = "table.drop")]
    Drop(TablePayload),
    #[serde(rename = "table.insert")]
    Insert(RowPayload),
    #[serde(rename = "table.insert_many")]
    InsertMany(BatchPayload),
    #[serde(rename = "table.get")]
    Get(KeyPayload),
    #[serde(rename = "table.update")]
    Update(UpdatePayload),
    #[serde(rename = "table.upsert")]
    Upsert(RowPayload),
    #[serde(rename = "table.delete")]
    Delete(KeyPayload),
    #[serde(rename = "query.collect")]
    Collect(QueryPayload),
    #[serde(rename = "query.count")]
    Count(QueryPayload),
    #[serde(rename = "query.explain")]
    Explain(QueryPayload),
}
impl<'de> Deserialize<'de> for Operation {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            operation: String,
            payload: Box<RawValue>,
        }
        // Preserve raw scalars even when payload precedes the operation tag.
        let wire: Wire = deserialize_object(d)?;
        decode_operation(&wire.operation, &wire.payload).map_err(serde::de::Error::custom)
    }
}
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Request {
    pub request_id: Uuid,
    #[serde(flatten)]
    pub operation: Operation,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRequest {
    request_id: Uuid,
    operation: String,
    payload: Box<RawValue>,
}
impl<'de> Deserialize<'de> for Request {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        decode_request(deserialize_object(d)?).map_err(serde::de::Error::custom)
    }
}
/// Authoritative byte entry point preserves unsupported_operation classification
/// and enforces body size. Direct Serde deserialization has only Serde errors.
pub fn parse_request(bytes: &[u8]) -> Result<Request, ProtocolError> {
    if bytes.len() > MAX_BODY_BYTES {
        return Err(limit("request body exceeds 8 MiB"));
    }
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let raw = deserialize_object(&mut deserializer)
        .map_err(|_| invalid("invalid request envelope or UUID"))?;
    deserializer
        .end()
        .map_err(|_| invalid("trailing request content"))?;
    decode_request(raw)
}
fn payload<T: DeserializeOwned>(raw: &RawValue) -> Result<T, ProtocolError> {
    deserialize_object(&mut serde_json::Deserializer::from_str(raw.get()))
        .map_err(|_| invalid("malformed operation payload"))
}
fn decode_request(raw: RawRequest) -> Result<Request, ProtocolError> {
    let operation = decode_operation(&raw.operation, &raw.payload)?;
    operation.validate()?;
    Ok(Request {
        request_id: raw.request_id,
        operation,
    })
}
fn decode_operation(name: &str, p: &RawValue) -> Result<Operation, ProtocolError> {
    let operation = match name {
        "table.create" => Operation::Create(payload(p)?),
        "table.describe" => Operation::Describe(payload(p)?),
        "table.list" => Operation::List(payload(p)?),
        "table.drop" => Operation::Drop(payload(p)?),
        "table.insert" => Operation::Insert(payload(p)?),
        "table.insert_many" => Operation::InsertMany(payload(p)?),
        "table.get" => Operation::Get(payload(p)?),
        "table.update" => Operation::Update(payload(p)?),
        "table.upsert" => Operation::Upsert(payload(p)?),
        "table.delete" => Operation::Delete(payload(p)?),
        "query.collect" => Operation::Collect(payload(p)?),
        "query.count" => Operation::Count(payload(p)?),
        "query.explain" => Operation::Explain(payload(p)?),
        _ => {
            return Err(ProtocolError::new(
                ErrorCode::UnsupportedOperation,
                "unsupported operation",
                Outcome::NotApplied,
            ));
        }
    };
    Ok(operation)
}
impl Operation {
    /// Stable wire operation name for diagnostics.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Create(_) => "table.create",
            Self::Describe(_) => "table.describe",
            Self::List(_) => "table.list",
            Self::Drop(_) => "table.drop",
            Self::Insert(_) => "table.insert",
            Self::InsertMany(_) => "table.insert_many",
            Self::Get(_) => "table.get",
            Self::Update(_) => "table.update",
            Self::Upsert(_) => "table.upsert",
            Self::Delete(_) => "table.delete",
            Self::Collect(_) => "query.collect",
            Self::Count(_) => "query.count",
            Self::Explain(_) => "query.explain",
        }
    }

    /// Whether execution may publish a catalog change, including no-op writes.
    pub fn is_mutation(&self) -> bool {
        match self {
            Self::Create(_)
            | Self::Drop(_)
            | Self::Insert(_)
            | Self::InsertMany(_)
            | Self::Update(_)
            | Self::Upsert(_)
            | Self::Delete(_) => true,
            Self::Describe(_)
            | Self::List(_)
            | Self::Get(_)
            | Self::Collect(_)
            | Self::Count(_)
            | Self::Explain(_) => false,
        }
    }

    /// Checks independent of catalog resolution. Call again for constructed values.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        match self {
            Self::Create(p) => validate_schema(&p.name, &p.schema, &p.key),
            Self::Describe(p) => validate_identifier(&p.name),
            Self::InsertMany(p) if p.rows.len() > MAX_BATCH_ROWS => {
                Err(limit("batch exceeds 10000 rows"))
            }
            Self::Collect(p) | Self::Count(p) | Self::Explain(p) => p.query.validate_structure(),
            _ => Ok(()),
        }
    }
    /// Catalog resolution and not_found errors belong to the engine. This method
    /// validates types and column references against the resolved descriptor.
    pub fn validate_for_table(&self, table: &TableDescriptor) -> Result<(), ProtocolError> {
        self.validate()?;
        table.validate()?;
        let id = match self {
            Self::Drop(p) => p.table_id,
            Self::Insert(p) | Self::Upsert(p) => p.table_id,
            Self::InsertMany(p) => p.table_id,
            Self::Get(p) | Self::Delete(p) => p.table_id,
            Self::Update(p) => p.table_id,
            Self::Collect(p) | Self::Count(p) | Self::Explain(p) => p.table_id,
            _ => return Ok(()),
        };
        if id != table.id {
            return Err(invalid("descriptor does not match table_id"));
        }
        match self {
            Self::Insert(p) | Self::Upsert(p) => validate_row(&p.row, table, RowMode::Complete),
            Self::InsertMany(p) => p
                .rows
                .iter()
                .try_for_each(|row| validate_row(row, table, RowMode::Complete)),
            Self::Get(p) | Self::Delete(p) => p.key.validate(table.schema[&table.key]),
            Self::Update(p) => {
                p.key.validate(table.schema[&table.key])?;
                validate_row(&p.values, table, RowMode::PartialUpdate)
            }
            Self::Collect(p) | Self::Count(p) | Self::Explain(p) => p.query.validate(&table.schema),
            _ => Ok(()),
        }
    }
}

pub fn validate_identifier(name: &str) -> Result<(), ProtocolError> {
    let b = name.as_bytes();
    if !(1..=64).contains(&b.len())
        || !(b[0].is_ascii_alphabetic() || b[0] == b'_')
        || !b[1..]
            .iter()
            .all(|c| c.is_ascii_alphanumeric() || *c == b'_')
    {
        return Err(invalid("identifier must match [A-Za-z_][A-Za-z0-9_]{0,63}"));
    }
    Ok(())
}
pub fn validate_schema(name: &str, schema: &Schema, key: &str) -> Result<(), ProtocolError> {
    validate_identifier(name)?;
    if !(1..=MAX_SCHEMA_COLUMNS).contains(&schema.len()) {
        return Err(invalid("schema requires 1 to 128 columns"));
    }
    for column in schema.keys() {
        validate_identifier(column)?;
    }
    match schema.get(key) {
        Some(ColumnType::Utf8 | ColumnType::Int64) => Ok(()),
        _ => Err(invalid("primary key must name a utf8 or int64 column")),
    }
}
pub fn validate_scalar(value: &Scalar, ty: ColumnType) -> Result<(), ProtocolError> {
    match (value, ty) {
        (Scalar::Utf8(_), ColumnType::Utf8)
        | (Scalar::Int64(_), ColumnType::Int64)
        | (Scalar::Bool(_), ColumnType::Bool) => Ok(()),
        (Scalar::Float64(v), ColumnType::Float64) if v.is_finite() => Ok(()),
        _ => Err(invalid("scalar must match exact column type and be finite")),
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowMode {
    Complete,
    PartialUpdate,
}
pub fn validate_row(
    row: &Row,
    table: &TableDescriptor,
    mode: RowMode,
) -> Result<(), ProtocolError> {
    if mode == RowMode::Complete && row.len() != table.schema.len() {
        return Err(invalid("complete row must contain every schema column"));
    }
    if mode == RowMode::PartialUpdate && row.contains_key(&table.key) {
        return Err(invalid("update cannot contain the primary key"));
    }
    for (column, value) in row {
        let ty = table
            .schema
            .get(column)
            .ok_or_else(|| invalid("unknown row column"))?;
        validate_scalar(value, *ty)?;
    }
    Ok(())
}

// deserialize_with keeps Option fields required while accepting explicit null.
fn required_nullable<'de, D: Deserializer<'de>, T: Deserialize<'de>>(
    d: D,
) -> Result<Option<T>, D::Error> {
    Option::deserialize(d)
}
fn required_nullable_limit<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u64>, D::Error> {
    // Check raw syntax before Serde can interpret private number/raw-value maps.
    let raw = Box::<RawValue>::deserialize(d)?;
    let text = raw.get().trim();
    if text == "null" {
        return Ok(None);
    }
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
        return Err(serde::de::Error::custom(
            "limit must be null or a nonnegative integer",
        ));
    }
    // Tables contain at most MAX_TABLE_ROWS, so overflow cannot affect which
    // rows collect/count return. Keep the engine's Option<u64> contract.
    Ok(Some(text.bytes().fold(0_u64, |n, b| {
        n.saturating_mul(10).saturating_add(u64::from(b - b'0'))
    })))
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Query {
    #[serde(deserialize_with = "required_nullable")]
    pub select: Option<Vec<String>>,
    #[serde(deserialize_with = "required_nullable")]
    pub filter: Option<Predicate>,
    #[serde(deserialize_with = "required_nullable_object")]
    pub order_by: Option<OrderBy>,
    /// Integer wire limits above u64::MAX saturate internally.
    #[serde(deserialize_with = "required_nullable_limit")]
    pub limit: Option<u64>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderBy {
    pub column: String,
    pub desc: bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum Predicate {
    Comparison(Comparison),
    Logical(LogicalPredicate),
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Comparison {
    pub op: ComparisonOp,
    pub column: String,
    pub value: Scalar,
}
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum LogicalPredicate {
    And { args: Vec<Predicate> },
    Or { args: Vec<Predicate> },
    Not { arg: Box<Predicate> },
}
impl<'de> Deserialize<'de> for Predicate {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = Box::<RawValue>::deserialize(d)?;
        decode_predicate(&raw, 1).map_err(serde::de::Error::custom)
    }
}
fn decode_predicate(raw: &RawValue, depth: usize) -> Result<Predicate, ProtocolError> {
    // Untagged/internal-tagged derives buffer scalar numbers as private maps.
    // Dispatch from raw JSON instead, including every nested predicate. Bound
    // recursion here because each raw subtree starts a fresh JSON deserializer.
    if depth > MAX_PREDICATE_DEPTH {
        return Err(invalid("predicate depth exceeds 32"));
    }
    #[derive(Deserialize)]
    struct Tag {
        op: String,
    }
    let tag: Tag = payload(raw)?;
    match tag.op.as_str() {
        "and" | "or" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Args {
                #[serde(rename = "op")]
                _op: String,
                args: Vec<Box<RawValue>>,
            }
            let wire: Args = payload(raw)?;
            let args = wire
                .args
                .iter()
                .map(|arg| decode_predicate(arg, depth + 1))
                .collect::<Result<_, _>>()?;
            Ok(Predicate::Logical(if tag.op == "and" {
                LogicalPredicate::And { args }
            } else {
                LogicalPredicate::Or { args }
            }))
        }
        "not" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Arg {
                #[serde(rename = "op")]
                _op: String,
                arg: Box<RawValue>,
            }
            let wire: Arg = payload(raw)?;
            let arg = Box::new(decode_predicate(&wire.arg, depth + 1)?);
            Ok(Predicate::Logical(LogicalPredicate::Not { arg }))
        }
        _ => payload(raw).map(Predicate::Comparison),
    }
}
impl<'de> Deserialize<'de> for LogicalPredicate {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        match Predicate::deserialize(d)? {
            Predicate::Logical(value) => Ok(value),
            Predicate::Comparison(_) => {
                Err(serde::de::Error::custom("expected a logical predicate"))
            }
        }
    }
}
// Serde structs can otherwise accept positional JSON arrays. Wire objects must
// carry named fields, including nested query/order/predicate objects.
pub(crate) fn deserialize_object<'de, D: Deserializer<'de>, T: Deserialize<'de>>(
    d: D,
) -> Result<T, D::Error> {
    struct ObjectVisitor<T>(std::marker::PhantomData<T>);
    impl<'de, T: Deserialize<'de>> serde::de::Visitor<'de> for ObjectVisitor<T> {
        type Value = T;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a JSON object")
        }
        fn visit_map<M: serde::de::MapAccess<'de>>(self, map: M) -> Result<T, M::Error> {
            T::deserialize(serde::de::value::MapAccessDeserializer::new(map))
        }
    }
    d.deserialize_map(ObjectVisitor(std::marker::PhantomData))
}
fn required_nullable_object<'de, D: Deserializer<'de>, T: Deserialize<'de>>(
    d: D,
) -> Result<Option<T>, D::Error> {
    struct Object<T>(T);
    impl<'de, T: Deserialize<'de>> Deserialize<'de> for Object<T> {
        fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            deserialize_object(d).map(Self)
        }
    }
    Option::<Object<T>>::deserialize(d).map(|v| v.map(|o| o.0))
}
impl Query {
    pub fn validate_structure(&self) -> Result<(), ProtocolError> {
        if let Some(columns) = &self.select {
            if columns.is_empty() {
                return Err(invalid("select must be nonempty"));
            }
            let mut seen = BTreeSet::new();
            for col in columns {
                validate_identifier(col)?;
                if !seen.insert(col) {
                    return Err(invalid("selected columns must be unique"));
                }
            }
        }
        if let Some(order) = &self.order_by {
            validate_identifier(&order.column)?;
        }
        if let Some(filter) = &self.filter {
            filter.validate_at(None, 1)?;
        }
        Ok(())
    }
    pub fn validate(&self, schema: &Schema) -> Result<(), ProtocolError> {
        self.validate_structure()?;
        if let Some(cols) = &self.select {
            for col in cols {
                column_type(schema, col)?;
            }
        }
        if let Some(order) = &self.order_by {
            column_type(schema, &order.column)?;
        }
        if let Some(filter) = &self.filter {
            filter.validate_at(Some(schema), 1)?;
        }
        Ok(())
    }
}
fn column_type(schema: &Schema, column: &str) -> Result<ColumnType, ProtocolError> {
    schema
        .get(column)
        .copied()
        .ok_or_else(|| invalid("unknown query column"))
}
impl Predicate {
    pub fn validate(&self, schema: &Schema) -> Result<(), ProtocolError> {
        self.validate_at(Some(schema), 1)
    }
    fn validate_at(&self, schema: Option<&Schema>, depth: usize) -> Result<(), ProtocolError> {
        if depth > MAX_PREDICATE_DEPTH {
            return Err(invalid("predicate depth exceeds 32"));
        }
        match self {
            Self::Comparison(p) => {
                validate_identifier(&p.column)?;
                if let Some(schema) = schema {
                    let ty = column_type(schema, &p.column)?;
                    validate_scalar(&p.value, ty)?;
                    if ty == ColumnType::Bool
                        && !matches!(p.op, ComparisonOp::Eq | ComparisonOp::Ne)
                    {
                        return Err(invalid("bool supports only eq and ne"));
                    }
                } else if matches!(p.value,Scalar::Float64(v) if !v.is_finite()) {
                    return Err(invalid("float64 must be finite"));
                }
                Ok(())
            }
            Self::Logical(LogicalPredicate::And { args } | LogicalPredicate::Or { args }) => {
                if args.is_empty() {
                    return Err(invalid("logical args must be nonempty"));
                }
                args.iter()
                    .try_for_each(|arg| arg.validate_at(schema, depth + 1))
            }
            Self::Logical(LogicalPredicate::Not { arg }) => arg.validate_at(schema, depth + 1),
        }
    }
}
