//! Vector, list and full-text columns: types, values, filters and operations
//! (`plans/oltp-vector.md`, section 1). Everything here decodes strictly and
//! checks a bound before it allocates.
use super::*;
use serde::ser::SerializeMap;
use sha2::{Digest as _, Sha256};
use std::borrow::Cow;

pub const MAX_VECTOR_DIM: u16 = 4096;
pub const MAX_VECTORS_PER_VALUE: u16 = 1024;
pub const MAX_SEARCH_COLUMNS: usize = 8;
/// One row of a table with search columns, its vectors counted as raw bytes.
pub const MAX_ROW_BYTES: usize = 1 << 20;
pub const MAX_RANK_LIMIT: u64 = 1000;
pub const MAX_RANKINGS: usize = 256;
/// The result column of a ranked query.
pub const SCORE_COLUMN: &str = "_score";

// ---- base64 of little-endian 4-byte words (standard alphabet, padded) ----

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const SEXTET: [u8; 256] = {
    let mut table = [0xff; 256];
    let mut i = 0;
    while i < 64 {
        table[ALPHABET[i] as usize] = i as u8;
        i += 1;
    }
    table
};

fn encode_words(words: impl Iterator<Item = [u8; 4]>, count: usize) -> String {
    let bytes = count.saturating_mul(4);
    let mut out = String::with_capacity(bytes.div_ceil(3) * 4);
    let mut push = |group: [u8; 3], len: usize| {
        let bits = u32::from_be_bytes([0, group[0], group[1], group[2]]);
        for i in 0..4 {
            out.push(if i <= len {
                ALPHABET[(bits >> (18 - 6 * i)) as usize & 63] as char
            } else {
                '='
            });
        }
    };
    let (mut group, mut len) = ([0; 3], 0);
    for byte in words.flatten() {
        group[len] = byte;
        len += 1;
        if len == 3 {
            push(group, len);
            (group, len) = ([0; 3], 0);
        }
    }
    if len != 0 {
        push(group, len);
    }
    out
}

/// The words of `text`, at most `max`. Accepts exactly what `encode_words`
/// produces, so every value has one encoding.
fn decode_words(text: &str, max: usize) -> Result<Vec<[u8; 4]>, ProtocolError> {
    let bad = || invalid("vector data must be standard padded base64 of 4-byte values");
    let bytes = text.as_bytes();
    if !bytes.len().is_multiple_of(4) {
        return Err(bad());
    }
    let padding = bytes
        .iter()
        .rev()
        .take(2)
        .take_while(|b| **b == b'=')
        .count();
    let decoded = bytes.len() / 4 * 3 - padding;
    if !decoded.is_multiple_of(4) {
        return Err(bad());
    }
    if decoded / 4 > max {
        return Err(limit("vector value has too many entries"));
    }
    let mut out = Vec::with_capacity(decoded / 4);
    let (mut word, mut len) = ([0; 4], 0);
    for (index, group) in bytes.as_chunks::<4>().0.iter().enumerate() {
        let last = (index + 1) * 4 == bytes.len();
        let kept = if last { 4 - padding } else { 4 };
        let mut bits = 0u32;
        for byte in &group[..kept] {
            let sextet = SEXTET[*byte as usize];
            if sextet == 0xff {
                return Err(bad());
            }
            bits = bits << 6 | u32::from(sextet);
        }
        bits <<= 6 * (4 - kept);
        let [_, data @ ..] = bits.to_be_bytes();
        // Unused trailing bits must be zero.
        if data[kept - 1..].iter().any(|byte| *byte != 0) {
            return Err(bad());
        }
        for byte in &data[..kept - 1] {
            word[len] = *byte;
            len += 1;
            if len == 4 {
                out.push(word);
                len = 0;
            }
        }
    }
    Ok(out)
}

fn decode_floats(text: &str, max: usize) -> Result<Vec<f32>, ProtocolError> {
    let values: Vec<f32> = decode_words(text, max)?
        .into_iter()
        .map(f32::from_le_bytes)
        .collect();
    finite(&values)?;
    Ok(values)
}
fn encode_floats(values: &[f32]) -> String {
    encode_words(values.iter().map(|v| v.to_le_bytes()), values.len())
}
fn finite(values: &[f32]) -> Result<(), ProtocolError> {
    if values.iter().all(|v| v.is_finite()) {
        Ok(())
    } else {
        Err(invalid("vector values must be finite"))
    }
}

// ---- values ----

/// A vector value exactly as written, never normalized. Every float is finite.
#[derive(Debug, Clone, PartialEq)]
pub enum Vector {
    Dense(Vec<f32>),
    /// Strictly ascending indices, one weight each.
    Sparse {
        indices: Vec<u32>,
        weights: Vec<f32>,
    },
    /// `count` vectors of equal dimension, one after the other.
    Multi {
        values: Vec<f32>,
        count: u16,
    },
}
impl Vector {
    /// The rules that hold for every value, whatever its column.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        let dim = |dim: usize| {
            if (1..=MAX_VECTOR_DIM as usize).contains(&dim) {
                Ok(())
            } else {
                Err(invalid("vector dimension must be 1 to 4096"))
            }
        };
        match self {
            Self::Dense(values) => {
                dim(values.len())?;
                finite(values)
            }
            Self::Sparse { indices, weights } => {
                if indices.len() != weights.len() {
                    return Err(invalid("sparse vector needs one weight per index"));
                }
                if !indices.is_sorted_by(|a, b| a < b) {
                    return Err(invalid("sparse indices must be strictly ascending"));
                }
                finite(weights)
            }
            Self::Multi { values, count } => {
                if *count > MAX_VECTORS_PER_VALUE {
                    return Err(limit("value exceeds 1024 vectors"));
                }
                match usize::from(*count) {
                    0 if values.is_empty() => Ok(()),
                    0 => Err(invalid("vector count does not match its data")),
                    n if values.len().is_multiple_of(n) => {
                        dim(values.len() / n)?;
                        finite(values)
                    }
                    _ => Err(invalid("vector count does not match its data")),
                }
            }
        }
    }
    pub fn matches(&self, ty: ColumnType) -> bool {
        match (self, ty) {
            (Self::Dense(values), ColumnType::Vector(dim)) => values.len() == usize::from(dim),
            (Self::Sparse { .. }, ColumnType::SparseVector) => true,
            (Self::Multi { values, count }, ColumnType::VectorList(dim)) => {
                values.len() == usize::from(*count) * usize::from(dim)
            }
            _ => false,
        }
    }
    /// The bytes of the floats and indices.
    pub fn raw_len(&self) -> usize {
        4 * match self {
            Self::Dense(values) | Self::Multi { values, .. } => values.len(),
            Self::Sparse { indices, weights } => indices.len() + weights.len(),
        }
    }
    /// The same wire object without data.
    fn emptied(&self) -> impl Serialize {
        #[derive(Serialize)]
        struct Sparse {
            i: &'static str,
            f32: &'static str,
        }
        #[derive(Serialize)]
        #[serde(untagged)]
        enum Wire {
            Dense { f32: &'static str },
            Sparse { sparse: Sparse },
            Multi { f32s: &'static str, n: u16 },
        }
        match self {
            Self::Dense(_) => Wire::Dense { f32: "" },
            Self::Sparse { .. } => Wire::Sparse {
                sparse: Sparse { i: "", f32: "" },
            },
            Self::Multi { count, .. } => Wire::Multi {
                f32s: "",
                n: *count,
            },
        }
    }
    /// Any vector of the value has length zero, so its cosine is undefined.
    fn has_zero(&self) -> bool {
        let zero = |values: &[f32]| values.iter().all(|v| *v == 0.0);
        match self {
            Self::Dense(values) => zero(values),
            Self::Sparse { weights, .. } => zero(weights),
            Self::Multi { values, count } => match usize::from(*count) {
                0 => true,
                n => values.chunks_exact((values.len() / n).max(1)).any(zero),
            },
        }
    }
}
impl Serialize for Vector {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Sparse {
            i: String,
            f32: String,
        }
        self.validate().map_err(serde::ser::Error::custom)?;
        let mut map = s.serialize_map(None)?;
        match self {
            Self::Dense(values) => map.serialize_entry("f32", &encode_floats(values))?,
            Self::Sparse { indices, weights } => map.serialize_entry(
                "sparse",
                &Sparse {
                    i: encode_words(indices.iter().map(|i| i.to_le_bytes()), indices.len()),
                    f32: encode_floats(weights),
                },
            )?,
            Self::Multi { values, count } => {
                map.serialize_entry("f32s", &encode_floats(values))?;
                map.serialize_entry("n", count)?;
            }
        }
        map.end()
    }
}
/// A field that may be absent but is never null.
fn present<'de, D: Deserializer<'de>, T: Deserialize<'de>>(d: D) -> Result<Option<T>, D::Error> {
    T::deserialize(d).map(Some)
}
#[derive(Deserialize)]
struct Borrowed<'a>(#[serde(borrow)] Cow<'a, str>);

fn decode_vector(raw: &str) -> Result<Vector, ProtocolError> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Sparse<'a> {
        #[serde(borrow)]
        i: Borrowed<'a>,
        #[serde(borrow)]
        f32: Borrowed<'a>,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Wire<'a> {
        #[serde(borrow, default, deserialize_with = "present")]
        f32: Option<Borrowed<'a>>,
        #[serde(borrow, default, deserialize_with = "present")]
        f32s: Option<Borrowed<'a>>,
        #[serde(default, deserialize_with = "present")]
        n: Option<u16>,
        #[serde(borrow, default, deserialize_with = "present")]
        sparse: Option<&'a RawValue>,
    }
    const MAX_WORDS: usize = MAX_ROW_BYTES / 4;
    // Escaped characters cost up to six bytes each; a row holds far less.
    if raw.len() > 6 * MAX_ROW_BYTES {
        return Err(limit("vector value exceeds 1 MiB"));
    }
    let vector = match object::<Wire<'_>>(raw)? {
        Wire {
            f32: Some(data),
            f32s: None,
            n: None,
            sparse: None,
        } => Vector::Dense(decode_floats(&data.0, MAX_VECTOR_DIM.into())?),
        Wire {
            f32: None,
            f32s: Some(data),
            n: Some(count),
            sparse: None,
        } => Vector::Multi {
            values: decode_floats(&data.0, MAX_WORDS)?,
            count,
        },
        Wire {
            f32: None,
            f32s: None,
            n: None,
            sparse: Some(sparse),
        } => {
            let sparse: Sparse<'_> = object(sparse.get())?;
            Vector::Sparse {
                indices: decode_words(&sparse.i.0, MAX_WORDS)?
                    .into_iter()
                    .map(u32::from_le_bytes)
                    .collect(),
                weights: decode_floats(&sparse.f32.0, MAX_WORDS)?,
            }
        }
        _ => return Err(invalid("malformed vector value")),
    };
    vector.validate()?;
    Ok(vector)
}
/// One JSON object, never the positional array Serde structs also accept.
fn object<'a, T: Deserialize<'a>>(raw: &'a str) -> Result<T, ProtocolError> {
    deserialize_object(&mut serde_json::Deserializer::from_str(raw))
        .map_err(|_| invalid("malformed vector value"))
}

/// One value of a row in any column type.
fn decode_value(raw: &RawValue) -> Result<Scalar, ProtocolError> {
    let text = raw.get();
    match text.as_bytes().first() {
        Some(b'{') => decode_vector(text).map(|v| Scalar::Vector(Box::new(v))),
        Some(b'[') => serde_json::from_str::<Box<[String]>>(text)
            .map(Scalar::Utf8List)
            .map_err(|_| invalid("a list value must hold strings only")),
        _ if text == "null" => Ok(Scalar::Null),
        _ => Scalar::from_json(text).map_err(|_| invalid("malformed row value")),
    }
}
/// One row object in any column types. The last of repeated columns wins, as
/// in a scalar row, unless `unique`.
fn decode_values(raw: &str, unique: bool) -> Result<Row, ProtocolError> {
    struct Visitor(bool);
    impl<'de> serde::de::Visitor<'de> for Visitor {
        type Value = Result<Row, ProtocolError>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a row object")
        }
        fn visit_map<M: serde::de::MapAccess<'de>>(
            self,
            mut map: M,
        ) -> Result<Self::Value, M::Error> {
            let mut row = Row::new();
            while let Some((name, raw)) = map.next_entry::<String, &'de RawValue>()? {
                match decode_value(raw) {
                    Ok(value) => {
                        if row.insert(name, value).is_some() && self.0 {
                            return Ok(Err(invalid("a row names a column twice")));
                        }
                    }
                    Err(error) => return Ok(Err(error)),
                }
            }
            Ok(Ok(row))
        }
    }
    let mut decoder = serde_json::Deserializer::from_str(raw);
    let row = Deserializer::deserialize_map(&mut decoder, Visitor(unique))
        .map_err(|_| invalid("malformed row"))??;
    decoder.end().map_err(|_| invalid("malformed row"))?;
    Ok(row)
}
/// A row in any column types: the `row` of a request, or a stored row.
pub fn decode_row(raw: &str) -> Result<Row, ProtocolError> {
    decode_values(raw, false)
}
/// A row of `table.upsert_many`: no column twice.
pub fn decode_upsert_row(raw: &str) -> Result<Row, ProtocolError> {
    decode_values(raw, true)
}

pub(super) fn validate_value(value: &Scalar, ty: ColumnType) -> Result<(), ProtocolError> {
    match (value, ty) {
        (Scalar::Utf8List(_), ColumnType::Utf8List) => Ok(()),
        (Scalar::Null, ty) if ty.is_vector() => Ok(()),
        (Scalar::Vector(vector), ty) if vector.matches(ty) => vector.validate(),
        (Scalar::Vector(_), ty) if ty.is_vector() => Err(invalid(
            "vector kind or dimension does not match its column",
        )),
        _ => Err(invalid("value must match exact column type")),
    }
}
/// The row limit of a table with search columns. It counts the row's JSON
/// with the raw bytes of vectors in place of their base64. Floats count one
/// byte, because clients spell them differently.
pub(super) fn validate_row_size(row: &Row) -> Result<(), ProtocolError> {
    struct Count(usize);
    impl std::io::Write for Count {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0 = self.0.saturating_add(buf.len());
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    // The braces, a colon per column and a comma between columns.
    let mut count = Count(2 * row.len() + 1);
    for (name, value) in row {
        let json = serde_json::to_writer(&mut count, name);
        json.and_then(|()| match value {
            Scalar::Float64(_) => serde_json::to_writer(&mut count, &0),
            Scalar::Vector(vector) => {
                count.0 = count.0.saturating_add(vector.raw_len());
                serde_json::to_writer(&mut count, &vector.emptied())
            }
            value => serde_json::to_writer(&mut count, value),
        })
        .map_err(|_| invalid("malformed row"))?;
    }
    if count.0 > MAX_ROW_BYTES {
        return Err(limit("row exceeds 1 MiB"));
    }
    Ok(())
}

// ---- column types and table options ----

impl Serialize for ColumnType {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Dense {
            r#type: &'static str,
            dim: u16,
        }
        let vector = |dim| Dense {
            r#type: "vector",
            dim,
        };
        fn list<S: Serializer>(s: S, item: impl Serialize) -> Result<S::Ok, S::Error> {
            let mut map = s.serialize_map(Some(2))?;
            map.serialize_entry("type", "list")?;
            map.serialize_entry("item", &item)?;
            map.end()
        }
        match self {
            Self::Utf8 => s.serialize_str("utf8"),
            Self::Int64 => s.serialize_str("int64"),
            Self::Float64 => s.serialize_str("float64"),
            Self::Bool => s.serialize_str("bool"),
            Self::Vector(dim) => vector(*dim).serialize(s),
            Self::SparseVector => {
                let mut map = s.serialize_map(Some(2))?;
                map.serialize_entry("type", "vector")?;
                map.serialize_entry("sparse", &true)?;
                map.end()
            }
            Self::VectorList(dim) => list(s, vector(*dim)),
            Self::Utf8List => list(s, "utf8"),
        }
    }
}
fn checked_dim<E: serde::de::Error>(dim: u16) -> Result<u16, E> {
    if (1..=MAX_VECTOR_DIM).contains(&dim) {
        Ok(dim)
    } else {
        Err(E::custom("vector dimension must be 1 to 4096"))
    }
}
/// Decodes without an owned string.
impl<'de> Deserialize<'de> for ColumnType {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = ColumnType;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a column type")
            }
            fn visit_str<E: serde::de::Error>(self, name: &str) -> Result<ColumnType, E> {
                Ok(match name {
                    "utf8" => ColumnType::Utf8,
                    "int64" => ColumnType::Int64,
                    "float64" => ColumnType::Float64,
                    "bool" => ColumnType::Bool,
                    other => {
                        return Err(E::unknown_variant(
                            other,
                            &["utf8", "int64", "float64", "bool"],
                        ));
                    }
                })
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                map: M,
            ) -> Result<ColumnType, M::Error> {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Wire {
                    r#type: Kind,
                    #[serde(default, deserialize_with = "present")]
                    dim: Option<u16>,
                    #[serde(default, deserialize_with = "present")]
                    sparse: Option<bool>,
                    #[serde(default, deserialize_with = "present")]
                    item: Option<Item>,
                }
                let wire = Wire::deserialize(serde::de::value::MapAccessDeserializer::new(map))?;
                match (wire.r#type, wire.dim, wire.sparse, wire.item) {
                    (Kind::Vector, Some(dim), None, None) => {
                        checked_dim(dim).map(ColumnType::Vector)
                    }
                    (Kind::Vector, None, Some(true), None) => Ok(ColumnType::SparseVector),
                    (Kind::List, None, None, Some(Item::Utf8)) => Ok(ColumnType::Utf8List),
                    (Kind::List, None, None, Some(Item::Vector(dim))) => {
                        Ok(ColumnType::VectorList(dim))
                    }
                    _ => Err(serde::de::Error::custom("malformed column type")),
                }
            }
        }
        d.deserialize_any(Visitor)
    }
}
enum Kind {
    Vector,
    List,
}
impl<'de> Deserialize<'de> for Kind {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl serde::de::Visitor<'_> for Visitor {
            type Value = Kind;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("vector or list")
            }
            fn visit_str<E: serde::de::Error>(self, name: &str) -> Result<Kind, E> {
                match name {
                    "vector" => Ok(Kind::Vector),
                    "list" => Ok(Kind::List),
                    other => Err(E::unknown_variant(other, &["vector", "list"])),
                }
            }
        }
        d.deserialize_str(Visitor)
    }
}
enum Item {
    Utf8,
    Vector(u16),
}
impl<'de> Deserialize<'de> for Item {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = Item;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a list item type")
            }
            fn visit_str<E: serde::de::Error>(self, name: &str) -> Result<Item, E> {
                match name {
                    "utf8" => Ok(Item::Utf8),
                    other => Err(E::unknown_variant(other, &["utf8"])),
                }
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(self, map: M) -> Result<Item, M::Error> {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Wire {
                    r#type: Kind,
                    dim: u16,
                }
                match Wire::deserialize(serde::de::value::MapAccessDeserializer::new(map))? {
                    Wire {
                        r#type: Kind::Vector,
                        dim,
                    } => checked_dim(dim).map(Item::Vector),
                    _ => Err(serde::de::Error::custom("malformed list item type")),
                }
            }
        }
        d.deserialize_any(Visitor)
    }
}

/// What the database maintains for search. Empty options are not on the wire.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct SearchOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_by: Option<String>,
    /// Column to language.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub full_text: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub embed: BTreeMap<String, Embed>,
}
/// Vectors of `source`'s text under `model`, an operator-defined name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Embed {
    pub source: String,
    pub model: String,
}
impl Embed {
    fn validate(&self, column: ColumnType, schema: &Schema) -> Result<(), ProtocolError> {
        if !matches!(column, ColumnType::Vector(_)) {
            return Err(invalid("embed requires a dense vector column"));
        }
        if schema.get(&self.source) != Some(&ColumnType::Utf8) {
            return Err(invalid("embed source must name a utf8 column"));
        }
        let name = self.model.as_bytes();
        if !(1..=64).contains(&name.len())
            || !name[0].is_ascii_alphanumeric()
            || !name
                .iter()
                .all(|c| c.is_ascii_alphanumeric() || b"_.-".contains(c))
        {
            return Err(invalid(
                "embedding model must match [A-Za-z0-9][A-Za-z0-9_.-]{0,63}",
            ));
        }
        Ok(())
    }
}
fn validate_language(language: &str) -> Result<(), ProtocolError> {
    if !(1..=32).contains(&language.len())
        || !language
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c == b'_')
    {
        return Err(invalid("full-text language must match [a-z_]{1,32}"));
    }
    Ok(())
}
impl SearchOptions {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
    pub fn validate(&self, schema: &Schema) -> Result<(), ProtocolError> {
        let columns = search_columns(schema, self);
        if columns > MAX_SEARCH_COLUMNS {
            return Err(invalid("a table has at most 8 search columns"));
        }
        if columns != 0 && schema.contains_key(SCORE_COLUMN) {
            return Err(invalid("_score is reserved on tables with search columns"));
        }
        for ty in schema.values() {
            if let ColumnType::Vector(dim) | ColumnType::VectorList(dim) = ty
                && !(1..=MAX_VECTOR_DIM).contains(dim)
            {
                return Err(invalid("vector dimension must be 1 to 4096"));
            }
        }
        if let Some(column) = &self.partition_by
            && !matches!(
                schema.get(column),
                Some(ColumnType::Utf8 | ColumnType::Int64)
            )
        {
            return Err(invalid("partition_by must name a utf8 or int64 column"));
        }
        for (column, language) in &self.full_text {
            if schema.get(column) != Some(&ColumnType::Utf8) {
                return Err(invalid("full_text must name a utf8 column"));
            }
            validate_language(language)?;
        }
        for (column, embed) in &self.embed {
            let ty = schema
                .get(column)
                .ok_or_else(|| invalid("embed must name a schema column"))?;
            embed.validate(*ty, schema)?;
        }
        Ok(())
    }
}
pub(super) fn search_columns(schema: &Schema, search: &SearchOptions) -> usize {
    schema.values().filter(|ty| ty.is_vector()).count() + search.full_text.len()
}
/// Whether a table with these columns needs `data.vector.v1`.
pub fn uses_search(schema: &Schema, search: &SearchOptions) -> bool {
    !search.is_empty() || schema.values().any(|ty| !ty.is_scalar())
}

/// The refusal while the cluster feature is off.
pub fn search_disabled() -> ProtocolError {
    unsupported("vector tables need the cluster feature data.vector.v1, which is not enabled")
}
fn unsupported(message: &str) -> ProtocolError {
    ProtocolError::new(
        ErrorCode::UnsupportedOperation,
        message,
        Outcome::NotApplied,
    )
}

/// A map of 1 to 128 entries, as a schema has columns, without repeated keys.
/// An empty one is absent on the wire.
fn unique_map<'de, D: Deserializer<'de>, T: Deserialize<'de>>(
    d: D,
) -> Result<BTreeMap<String, T>, D::Error> {
    struct Visitor<T>(std::marker::PhantomData<T>);
    impl<'de, T: Deserialize<'de>> serde::de::Visitor<'de> for Visitor<T> {
        type Value = BTreeMap<String, T>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("an object of column options")
        }
        fn visit_map<M: serde::de::MapAccess<'de>>(
            self,
            mut map: M,
        ) -> Result<Self::Value, M::Error> {
            use serde::de::Error;
            let mut out = BTreeMap::new();
            while let Some(name) = map.next_key::<String>()? {
                if out.len() == MAX_SCHEMA_COLUMNS {
                    return Err(M::Error::custom("too many column options"));
                }
                if out.insert(name, map.next_value()?).is_some() {
                    return Err(M::Error::custom("duplicate column option"));
                }
            }
            if out.is_empty() {
                return Err(M::Error::custom("empty column options"));
            }
            Ok(out)
        }
    }
    d.deserialize_map(Visitor(std::marker::PhantomData))
}
/// `Embed` from an object only, as `deserialize_object` does for a payload.
struct EmbedWire(Embed);
impl<'de> Deserialize<'de> for EmbedWire {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        deserialize_object(d).map(Self)
    }
}
fn embeds<'de, D: Deserializer<'de>>(d: D) -> Result<BTreeMap<String, Embed>, D::Error> {
    Ok(unique_map::<_, EmbedWire>(d)?
        .into_iter()
        .map(|(column, EmbedWire(embed))| (column, embed))
        .collect())
}
macro_rules! described {
    ($wire:ident => $ty:ident { $($field:ident: $of:ty),+ }) => {
        // Serde cannot combine flatten with deny_unknown_fields.
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        pub(super) struct $wire {
            $($field: $of,)+
            #[serde(default, deserialize_with = "present")]
            partition_by: Option<String>,
            #[serde(default, deserialize_with = "unique_map")]
            full_text: BTreeMap<String, String>,
            #[serde(default, deserialize_with = "embeds")]
            embed: BTreeMap<String, Embed>,
        }
        impl From<$wire> for $ty {
            fn from(wire: $wire) -> Self {
                Self {
                    $($field: wire.$field,)+
                    search: SearchOptions {
                        partition_by: wire.partition_by,
                        full_text: wire.full_text,
                        embed: wire.embed,
                    },
                }
            }
        }
    };
}
described!(DescriptorWire => TableDescriptor {
    id: Uuid, name: String, schema: Schema, key: String, format: TableFormat
});
described!(CreateWire => CreatePayload {
    name: String, schema: Schema, key: String, format: TableFormat
});

// ---- filters ----

/// Filters on `list[str]` and utf8 columns.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum TextPredicate {
    Contains { column: String, value: String },
    ContainsAny { column: String, values: Vec<String> },
    StartsWith { column: String, value: String },
}
impl TextPredicate {
    pub fn column(&self) -> &str {
        match self {
            Self::Contains { column, .. }
            | Self::ContainsAny { column, .. }
            | Self::StartsWith { column, .. } => column,
        }
    }
    pub(super) fn validate(&self, schema: Option<&Schema>) -> Result<(), ProtocolError> {
        validate_identifier(self.column())?;
        let wanted = match self {
            Self::StartsWith { .. } => ColumnType::Utf8,
            _ => ColumnType::Utf8List,
        };
        match schema {
            Some(schema) if column_type(schema, self.column())? != wanted => Err(invalid(
                "contains requires a list column and starts_with a utf8 column",
            )),
            _ => Ok(()),
        }
    }
}
pub(super) fn decode_text_predicate(
    op: &str,
    raw: &RawValue,
) -> Result<TextPredicate, ProtocolError> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct One {
        #[serde(rename = "op")]
        _op: String,
        column: String,
        value: String,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Any {
        #[serde(rename = "op")]
        _op: String,
        column: String,
        values: Vec<String>,
    }
    if op == "contains_any" {
        let Any { column, values, .. } = payload(raw)?;
        return Ok(TextPredicate::ContainsAny { column, values });
    }
    let One { column, value, .. } = payload(raw)?;
    Ok(if op == "contains" {
        TextPredicate::Contains { column, value }
    } else {
        TextPredicate::StartsWith { column, value }
    })
}
/// At most `max` elements; a longer list stops decoding at the first one over.
fn bounded_list<'de, D: Deserializer<'de>, T: Deserialize<'de>>(
    d: D,
    max: usize,
) -> Result<Vec<T>, D::Error> {
    struct Visitor<T>(usize, std::marker::PhantomData<T>);
    impl<'de, T: Deserialize<'de>> serde::de::Visitor<'de> for Visitor<T> {
        type Value = Vec<T>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "a list of at most {} items", self.0)
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut seq: A,
        ) -> Result<Self::Value, A::Error> {
            let mut out = Vec::new();
            while out.len() < self.0 {
                match seq.next_element()? {
                    Some(item) => out.push(item),
                    None => return Ok(out),
                }
            }
            match seq.next_element::<serde::de::IgnoredAny>()? {
                None => Ok(out),
                Some(_) => Err(serde::de::Error::custom("list exceeds its limit")),
            }
        }
    }
    d.deserialize_seq(Visitor(max, std::marker::PhantomData))
}
fn full_filter<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Predicate>, D::Error> {
    Option::<&'de RawValue>::deserialize(d)?
        .map(|raw| decode_predicate(raw, 1, Grammar::Full))
        .transpose()
        .map_err(serde::de::Error::custom)
}

// ---- operations ----

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeysPayload {
    pub table_id: Uuid,
    pub keys: Vec<Key>,
}
impl KeysPayload {
    pub(super) fn validate(&self) -> Result<(), ProtocolError> {
        if self.keys.len() > MAX_BATCH_ROWS {
            return Err(limit("batch exceeds 10000 rows"));
        }
        let mut seen = std::collections::HashSet::with_capacity(self.keys.len());
        if !self.keys.iter().all(|key| seen.insert(key)) {
            return Err(duplicate_key());
        }
        Ok(())
    }
}
pub fn duplicate_key() -> ProtocolError {
    invalid("a key appears twice in one call")
}

/// The request ID of one row of `table.upsert_many` or `table.delete_many`.
/// It depends on the call and the key only, so a row keeps it however the
/// rows are grouped.
pub fn row_request_id(call: Uuid, key: &Key) -> Result<Uuid, ProtocolError> {
    let key = serde_json::to_vec(key).map_err(|_| invalid("malformed key"))?;
    let digest = Sha256::new()
        .chain_update(b"nabedra-row-v1")
        .chain_update(call.as_bytes())
        .chain_update(&key)
        .finalize();
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = bytes[6] & 0x0f | 0x80;
    bytes[8] = bytes[8] & 0x3f | 0x80;
    Ok(Uuid::from_bytes(bytes))
}

/// The primary key of `row`.
pub fn row_key(row: &Row, key_column: &str) -> Result<Key, ProtocolError> {
    match row.get(key_column) {
        Some(Scalar::Utf8(key)) => Ok(Key::Utf8(key.clone())),
        Some(Scalar::Int64(key)) => Ok(Key::Int64(*key)),
        _ => Err(invalid("row has no primary key")),
    }
}
/// The request one row of `table.upsert_many` runs as, on either path. Its
/// encoding is canonical, so its digest does not depend on the call's chunks.
pub fn upsert_row_request(
    call: Uuid,
    table_id: Uuid,
    key_column: &str,
    row: Row,
) -> Result<Request, ProtocolError> {
    Ok(Request {
        request_id: row_request_id(call, &row_key(&row, key_column)?)?,
        operation: Operation::Upsert(RowPayload { table_id, row }),
    })
}
/// As `upsert_row_request`, for one key of `table.delete_many`.
pub fn delete_row_request(call: Uuid, table_id: Uuid, key: Key) -> Result<Request, ProtocolError> {
    Ok(Request {
        request_id: row_request_id(call, &key)?,
        operation: Operation::Delete(KeyPayload { table_id, key }),
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AddColumnPayload {
    pub table_id: Uuid,
    pub name: String,
    pub r#type: ColumnType,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_embed"
    )]
    pub embed: Option<Embed>,
}
fn present_embed<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Embed>, D::Error> {
    deserialize_object(d).map(Some)
}
impl AddColumnPayload {
    pub(super) fn validate(&self) -> Result<(), ProtocolError> {
        validate_identifier(&self.name)?;
        if !self.r#type.is_vector() {
            return Err(invalid("add_column adds vector columns only"));
        }
        if self.name == SCORE_COLUMN {
            return Err(invalid("_score is reserved on tables with search columns"));
        }
        Ok(())
    }
    pub(super) fn validate_for(&self, table: &TableDescriptor) -> Result<(), ProtocolError> {
        let mut next = table.clone();
        if next.schema.insert(self.name.clone(), self.r#type).is_some() {
            return Err(invalid("column already exists"));
        }
        if let Some(embed) = &self.embed {
            next.search.embed.insert(self.name.clone(), embed.clone());
        }
        next.validate()
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnPayload {
    pub table_id: Uuid,
    pub name: String,
}
impl ColumnPayload {
    pub(super) fn validate_for(&self, table: &TableDescriptor) -> Result<(), ProtocolError> {
        if !column_type(&table.schema, &self.name)?.is_vector() {
            return Err(invalid("drop_column drops vector columns only"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AddFullTextPayload {
    pub table_id: Uuid,
    pub column: String,
    pub language: String,
}
impl AddFullTextPayload {
    pub(super) fn validate(&self) -> Result<(), ProtocolError> {
        validate_identifier(&self.column)?;
        validate_language(&self.language)
    }
    pub(super) fn validate_for(&self, table: &TableDescriptor) -> Result<(), ProtocolError> {
        let mut next = table.clone();
        let full_text = &mut next.search.full_text;
        if full_text
            .insert(self.column.clone(), self.language.clone())
            .is_some()
        {
            return Err(invalid("column already has full text"));
        }
        next.validate()
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FullTextPayload {
    pub table_id: Uuid,
    pub column: String,
}
impl FullTextPayload {
    pub(super) fn validate_for(&self, table: &TableDescriptor) -> Result<(), ProtocolError> {
        if !table.search.full_text.contains_key(&self.column) {
            return Err(invalid("column has no full text"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RankOp {
    Cosine,
    Dot,
    L2,
    Match,
}
string_enum_deserialize!(RankOp, "cosine" => Cosine, "dot" => Dot, "l2" => L2, "match" => Match);
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum RankValue {
    /// Text to match, or to embed with the column's model.
    Text(String),
    Vector(Vector),
}
impl<'de> Deserialize<'de> for RankValue {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = <&RawValue>::deserialize(d)?;
        if raw.get().starts_with('{') {
            decode_vector(raw.get())
                .map(Self::Vector)
                .map_err(serde::de::Error::custom)
        } else {
            serde_json::from_str(raw.get())
                .map(Self::Text)
                .map_err(serde::de::Error::custom)
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Ranking {
    pub column: String,
    pub op: RankOp,
    pub value: RankValue,
}
impl<'de> Deserialize<'de> for Ranking {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            column: String,
            op: RankOp,
            value: RankValue,
        }
        let Wire { column, op, value } = deserialize_object(d)?;
        Ok(Self { column, op, value })
    }
}
impl Ranking {
    fn validate_structure(&self) -> Result<(), ProtocolError> {
        validate_identifier(&self.column)?;
        match (&self.value, self.op) {
            (RankValue::Vector(_), RankOp::Match) => Err(invalid("match takes text")),
            (RankValue::Vector(vector), op) => {
                vector.validate()?;
                if matches!(vector, Vector::Multi { count: 0, .. }) {
                    return Err(invalid("a query needs at least one vector"));
                }
                if op == RankOp::Cosine && vector.has_zero() {
                    return Err(invalid("cosine is undefined for a zero query"));
                }
                Ok(())
            }
            (RankValue::Text(_), _) => Ok(()),
        }
    }
    fn validate(&self, table: &TableDescriptor) -> Result<(), ProtocolError> {
        let ty = column_type(&table.schema, &self.column)?;
        let served = match ty {
            ColumnType::Vector(_) => self.op != RankOp::Match,
            ColumnType::SparseVector | ColumnType::VectorList(_) => {
                matches!(self.op, RankOp::Cosine | RankOp::Dot)
            }
            ColumnType::Utf8 => {
                self.op == RankOp::Match && table.search.full_text.contains_key(&self.column)
            }
            _ => false,
        };
        if !served {
            return Err(invalid("the column does not support this operation"));
        }
        match &self.value {
            RankValue::Vector(vector) if !vector.matches(ty) => Err(invalid(
                "query vector kind or dimension does not match its column",
            )),
            RankValue::Text(_)
                if ty.is_vector() && !table.search.embed.contains_key(&self.column) =>
            {
                Err(invalid(
                    "the column has no embedding model for a text query",
                ))
            }
            _ => Ok(()),
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
pub enum Fusion {
    Rrf,
    /// One weight per ranking.
    Weights(Vec<f64>),
}
impl Serialize for Fusion {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Rrf => s.serialize_str("rrf"),
            Self::Weights(weights) => weights.serialize(s),
        }
    }
}
impl<'de> Deserialize<'de> for Fusion {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = <&RawValue>::deserialize(d)?.get();
        if raw.starts_with('[') {
            let mut decoder = serde_json::Deserializer::from_str(raw);
            return bounded_list(&mut decoder, MAX_RANKINGS)
                .map(Self::Weights)
                .map_err(serde::de::Error::custom);
        }
        match serde_json::from_str::<Cow<'_, str>>(raw) {
            Ok(name) if name == "rrf" => Ok(Self::Rrf),
            _ => Err(serde::de::Error::custom(
                "fusion is \"rrf\" or a list of weights",
            )),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Consistency {
    Latest,
    Snapshot,
}
string_enum_deserialize!(Consistency, "latest" => Latest, "snapshot" => Snapshot);
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Rerank {
    pub ranking: Ranking,
    pub candidates: u32,
}
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RankOptions {
    pub rankings: Vec<Ranking>,
    pub fusion: Fusion,
    pub candidates: u32,
    pub recall: f64,
    pub exact: bool,
    pub consistency: Consistency,
    pub rerank: Option<Rerank>,
}
impl<'de> Deserialize<'de> for RankOptions {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct RerankWire {
            ranking: Ranking,
            #[serde(default = "default_rerank")]
            candidates: u32,
        }
        fn default_rerank() -> u32 {
            200
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            #[serde(deserialize_with = "rankings")]
            rankings: Vec<Ranking>,
            #[serde(default = "default_fusion")]
            fusion: Fusion,
            #[serde(default = "default_candidates")]
            candidates: u32,
            #[serde(default = "default_recall")]
            recall: f64,
            #[serde(default)]
            exact: bool,
            #[serde(default = "default_consistency")]
            consistency: Consistency,
            #[serde(default, deserialize_with = "required_nullable_object")]
            rerank: Option<RerankWire>,
        }
        fn default_fusion() -> Fusion {
            Fusion::Rrf
        }
        fn default_candidates() -> u32 {
            100
        }
        fn default_recall() -> f64 {
            0.95
        }
        fn default_consistency() -> Consistency {
            Consistency::Latest
        }
        let wire: Wire = deserialize_object(d)?;
        Ok(Self {
            rankings: wire.rankings,
            fusion: wire.fusion,
            candidates: wire.candidates,
            recall: wire.recall,
            exact: wire.exact,
            consistency: wire.consistency,
            rerank: wire.rerank.map(|w| Rerank {
                ranking: w.ranking,
                candidates: w.candidates,
            }),
        })
    }
}
fn rankings<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<Ranking>, D::Error> {
    bounded_list(d, MAX_RANKINGS)
}
impl RankOptions {
    /// `fused` rankings share the fusion weights.
    fn validate_structure(&self, fused: usize) -> Result<(), ProtocolError> {
        if self.candidates == 0 {
            return Err(invalid("candidates must be positive"));
        }
        if !(self.recall > 0.0 && self.recall <= 1.0) {
            return Err(invalid("recall must be above 0 and at most 1"));
        }
        if self.rankings.len() > MAX_RANKINGS {
            return Err(limit("query exceeds 256 rankings"));
        }
        if let Fusion::Weights(weights) = &self.fusion
            && (weights.len() != fused || !weights.iter().all(|w| w.is_finite() && *w >= 0.0))
        {
            return Err(invalid(
                "fusion needs one finite, nonnegative weight per ranking",
            ));
        }
        if let Some(rerank) = &self.rerank {
            if rerank.candidates == 0 {
                return Err(invalid("candidates must be positive"));
            }
            rerank.ranking.validate_structure()?;
        }
        self.rankings
            .iter()
            .try_for_each(Ranking::validate_structure)
    }
    fn validate(&self, table: &TableDescriptor) -> Result<(), ProtocolError> {
        self.rankings
            .iter()
            .chain(self.rerank.iter().map(|rerank| &rerank.ranking))
            .try_for_each(|ranking| ranking.validate(table))
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RankQuery {
    #[serde(deserialize_with = "required_nullable")]
    pub select: Option<Vec<String>>,
    #[serde(deserialize_with = "full_filter")]
    pub filter: Option<Predicate>,
    pub limit: u64,
}
impl RankQuery {
    /// The same selection and filter as an unranked query.
    fn query(&self) -> Query {
        Query {
            select: self.select.clone(),
            filter: self.filter.clone(),
            order_by: None,
            limit: Some(self.limit),
        }
    }
    fn validate_structure(&self) -> Result<(), ProtocolError> {
        if self.limit > MAX_RANK_LIMIT {
            return Err(limit("ranked query limit exceeds 1000"));
        }
        self.query().validate_structure()
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RankPayload {
    pub table_id: Uuid,
    #[serde(deserialize_with = "deserialize_object")]
    pub query: RankQuery,
    pub rank: RankOptions,
}
impl RankPayload {
    pub(super) fn validate_structure(&self) -> Result<(), ProtocolError> {
        if self.rank.rankings.is_empty() {
            return Err(invalid("rank needs at least one ranking"));
        }
        self.query.validate_structure()?;
        self.rank.validate_structure(self.rank.rankings.len())
    }
    pub(super) fn validate(&self, table: &TableDescriptor) -> Result<(), ProtocolError> {
        self.query.query().validate(&table.schema)?;
        self.rank.validate(table)
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RankManyPayload {
    pub table_id: Uuid,
    #[serde(deserialize_with = "deserialize_object")]
    pub query: RankQuery,
    pub rank: RankOptions,
    #[serde(deserialize_with = "rankings")]
    pub batch: Vec<Ranking>,
}
impl RankManyPayload {
    pub(super) fn validate_structure(&self) -> Result<(), ProtocolError> {
        if !self.rank.rankings.is_empty() {
            return Err(invalid("rank_many takes its rankings in batch"));
        }
        if self.batch.len() > MAX_RANKINGS {
            return Err(limit("batch exceeds 256 rankings"));
        }
        self.query.validate_structure()?;
        if self.batch.is_empty() {
            return Err(invalid("rank_many needs at least one ranking"));
        }
        self.rank.validate_structure(1)?;
        self.batch.iter().try_for_each(Ranking::validate_structure)
    }
    pub(super) fn validate(&self, table: &TableDescriptor) -> Result<(), ProtocolError> {
        self.query.query().validate(&table.schema)?;
        self.rank.validate(table)?;
        self.batch.iter().try_for_each(|r| r.validate(table))
    }
}

impl Operation {
    /// Refuses what this build decodes but does not serve yet, and a table
    /// that needs `data.vector.v1` while the cluster has not enabled it.
    pub fn check_served(&self, vector: bool) -> Result<(), ProtocolError> {
        match self {
            Self::AddColumn(_)
            | Self::DropColumn(_)
            | Self::AddFullText(_)
            | Self::DropFullText(_)
            | Self::SearchStatus(_)
            | Self::Rank(_)
            | Self::RankMany(_) => Err(unsupported("the operation is not supported yet")),
            Self::Create(p) if !vector && uses_search(&p.schema, &p.search) => {
                Err(search_disabled())
            }
            _ => Ok(()),
        }
    }
    /// As `check_served`, for the queued path, which runs the rows of a
    /// `*_many` call one by one.
    pub fn check_queued(&self, vector: bool) -> Result<(), ProtocolError> {
        if matches!(self, Self::UpsertMany(_) | Self::DeleteMany(_)) {
            return Err(unsupported("the operation needs the direct path"));
        }
        self.check_served(vector)
    }
}

/// One row's result in the reply of `table.upsert_many` or `table.delete_many`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "o", rename_all = "snake_case")]
pub enum RowOutcome {
    Applied,
    NotApplied { error: ProtocolError },
    OutcomeUnknown { error: ProtocolError },
}
impl<'de> Deserialize<'de> for RowOutcome {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            o: String,
            #[serde(default, deserialize_with = "present")]
            error: Option<ProtocolError>,
        }
        let Wire { o, error } = deserialize_object(d)?;
        match (o.as_str(), error) {
            ("applied", None) => Ok(Self::Applied),
            ("not_applied", Some(error)) if error.outcome == Outcome::NotApplied => {
                Ok(Self::NotApplied { error })
            }
            ("outcome_unknown", Some(error)) if error.outcome == Outcome::Unknown => {
                Ok(Self::OutcomeUnknown { error })
            }
            _ => Err(serde::de::Error::custom("malformed row outcome")),
        }
    }
}
impl From<ProtocolError> for RowOutcome {
    fn from(error: ProtocolError) -> Self {
        match error.outcome {
            Outcome::NotApplied => Self::NotApplied { error },
            Outcome::Unknown => Self::OutcomeUnknown { error },
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowOutcomes {
    pub outcomes: Vec<RowOutcome>,
}

// ---- decoding ----

/// A create as an engine without search columns accepts it.
pub(super) fn scalar_create(create: CreatePayload) -> Result<CreatePayload, ProtocolError> {
    if uses_search(&create.schema, &create.search) {
        return Err(invalid("malformed operation payload"));
    }
    Ok(create)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FullRow<'a> {
    table_id: Uuid,
    #[serde(borrow)]
    row: &'a RawValue,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FullRows<'a> {
    table_id: Uuid,
    #[serde(borrow, deserialize_with = "raw_rows")]
    rows: Vec<&'a RawValue>,
}
fn raw_rows<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<&'de RawValue>, D::Error> {
    bounded_list(d, MAX_BATCH_ROWS + 1)
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FullUpdate<'a> {
    table_id: Uuid,
    key: Key,
    #[serde(borrow)]
    values: &'a RawValue,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FullKeys {
    table_id: Uuid,
    #[serde(deserialize_with = "raw_keys")]
    keys: Vec<Key>,
}
fn raw_keys<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<Key>, D::Error> {
    bounded_list(d, MAX_BATCH_ROWS + 1)
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FullQueryPayload {
    table_id: Uuid,
    #[serde(deserialize_with = "deserialize_object")]
    query: FullQuery,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FullQuery {
    #[serde(deserialize_with = "required_nullable")]
    select: Option<Vec<String>>,
    #[serde(deserialize_with = "full_filter")]
    filter: Option<Predicate>,
    #[serde(deserialize_with = "required_nullable_object")]
    order_by: Option<OrderBy>,
    #[serde(deserialize_with = "required_nullable_limit")]
    limit: Option<u64>,
}

/// The operations whose grammar is larger than `Grammar::Scalar`'s.
pub(super) fn decode_operation(
    name: &str,
    p: &RawValue,
) -> Result<Option<Operation>, ProtocolError> {
    let row = || -> Result<RowPayload, ProtocolError> {
        let FullRow { table_id, row } = payload_ref(p)?;
        let row = decode_row(row.get())?;
        Ok(RowPayload { table_id, row })
    };
    let rows = |decode: fn(&str) -> Result<Row, ProtocolError>| {
        let FullRows { table_id, rows } = payload_ref(p)?;
        if rows.len() > MAX_BATCH_ROWS {
            return Err(limit("batch exceeds 10000 rows"));
        }
        let rows = rows
            .into_iter()
            .map(|row| decode(row.get()))
            .collect::<Result<_, _>>()?;
        Ok(BatchPayload { table_id, rows })
    };
    let query = || -> Result<QueryPayload, ProtocolError> {
        let FullQueryPayload { table_id, query } = payload_ref(p)?;
        Ok(QueryPayload {
            table_id,
            query: Query {
                select: query.select,
                filter: query.filter,
                order_by: query.order_by,
                limit: query.limit,
            },
        })
    };
    Ok(Some(match name {
        "table.create" => Operation::Create(payload(p)?),
        "table.insert" => Operation::Insert(row()?),
        "table.upsert" => Operation::Upsert(row()?),
        "table.insert_many" => Operation::InsertMany(rows(decode_row)?),
        "table.upsert_many" => Operation::UpsertMany(rows(decode_upsert_row)?),
        "table.update" => {
            let FullUpdate {
                table_id,
                key,
                values,
            } = payload_ref(p)?;
            Operation::Update(UpdatePayload {
                table_id,
                key,
                values: decode_row(values.get())?,
            })
        }
        "query.collect" => Operation::Collect(query()?),
        "query.count" => Operation::Count(query()?),
        "query.explain" => Operation::Explain(query()?),
        "table.delete_many" => {
            let FullKeys { table_id, keys } = payload_ref(p)?;
            Operation::DeleteMany(KeysPayload { table_id, keys })
        }
        "table.add_column" => Operation::AddColumn(payload(p)?),
        "table.drop_column" => Operation::DropColumn(payload(p)?),
        "table.add_full_text" => Operation::AddFullText(payload(p)?),
        "table.drop_full_text" => Operation::DropFullText(payload(p)?),
        "table.search_status" => Operation::SearchStatus(payload(p)?),
        "query.rank" => Operation::Rank(payload_ref(p)?),
        "query.rank_many" => Operation::RankMany(payload_ref(p)?),
        _ => return Ok(None),
    }))
}
/// As `payload`, for a type that borrows from it.
fn payload_ref<'a, T: Deserialize<'a>>(raw: &'a RawValue) -> Result<T, ProtocolError> {
    deserialize_object(&mut serde_json::Deserializer::from_str(raw.get()))
        .map_err(|_| invalid("malformed operation payload"))
}

#[cfg(test)]
mod fixtures;
#[cfg(test)]
mod tests;
