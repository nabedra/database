//! The wire fixtures that both SDKs share, `orm/contract/vector/vectors.json`:
//! the server decodes every accepted request and reply to the same JSON, and
//! refuses every refused one that has a wire form.
use super::*;
use serde_json::{Map, json};
use std::collections::BTreeSet;

/// Refused in the client before anything is encoded.
const CLIENT_ONLY: [&str; 10] = [
    "stream a vector column",
    "a ranked count",
    "a ranked explain",
    "a ranked stream",
    "a ranked stream_snapshot",
    "a ranking in where",
    "rerank without rank",
    "rank_many after rank",
    "waiting without an embedded column",
    "a wait that is not a bool",
];

fn fixtures() -> Value {
    let path = std::env::var("NABEDRA_VECTOR_FIXTURES").unwrap_or_else(|_| {
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../orm/contract/vector/vectors.json"
        )
        .into()
    });
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
    #[derive(Deserialize)]
    struct FixtureSource<'a> {
        tables: Value,
        #[serde(borrow)]
        create: Vec<&'a RawValue>,
        #[serde(borrow)]
        describe: Vec<&'a RawValue>,
        #[serde(borrow)]
        calls: Vec<&'a RawValue>,
    }
    let source: FixtureSource<'_> = serde_json::from_str(&text).unwrap();
    // Keep intentionally malformed Unicode as raw JSON. Test the server's
    // refusal of that encoding separately below.
    let mut malformed = BTreeSet::new();
    let mut decode = |entries: Vec<&RawValue>| -> Vec<Value> {
        entries
            .into_iter()
            .filter_map(|raw| match serde_json::from_str(raw.get()) {
                Ok(value) => Some(value),
                Err(error) => {
                    let fields: BTreeMap<&str, &RawValue> =
                        serde_json::from_str(raw.get()).unwrap();
                    let name: String = serde_json::from_str(fields["name"].get()).unwrap();
                    assert!(SDK_ONLY_UTF16.contains(&name.as_str()), "{name}: {error}");
                    assert!(fields.contains_key("error") && !fields.contains_key("payload"));
                    assert!(malformed.insert(name));
                    None
                }
            })
            .collect()
    };
    let fixtures = json!({
        "tables": source.tables,
        "create": decode(source.create),
        "describe": decode(source.describe),
        "calls": decode(source.calls)
    });
    assert_eq!(malformed.len(), SDK_ONLY_UTF16.len());
    fixtures
}

const SDK_ONLY_UTF16: [&str; 10] = [
    "a language with a lone surrogate",
    "a model with a lone surrogate",
    "tags with a lone surrogate",
    "contains a lone surrogate",
    "contains_any of a set with a lone surrogate",
    "contains_any of a list with a lone low surrogate",
    "starts_with a lone surrogate",
    "match a lone surrogate",
    "embed a lone surrogate",
    "add full text in a language with a lone surrogate",
];

#[test]
fn vs18_invalid_utf16_is_refused_on_the_wire() {
    let schema = r#"{"id":"utf8","body":"utf8","vec":{"type":"vector","dim":2}}"#;
    let table = Uuid::from_u128(11);
    let request = Uuid::from_u128(1);
    let cases = [
        (
            "table.create",
            format!(
                r#"{{"name":"made","schema":{schema},"key":"id","format":"parquet","full_text":{{"body":"en\ud800"}}}}"#
            ),
        ),
        (
            "table.create",
            format!(
                r#"{{"name":"made","schema":{schema},"key":"id","format":"parquet","embed":{{"vec":{{"source":"body","model":"m\udfff"}}}}}}"#
            ),
        ),
        (
            "table.upsert",
            format!(r#"{{"table_id":"{table}","row":{{"id":"row","tags":["\ud800"]}}}}"#),
        ),
        (
            "table.add_full_text",
            format!(r#"{{"table_id":"{table}","column":"body","language":"\ud800"}}"#),
        ),
        (
            "query.collect",
            format!(
                r#"{{"table_id":"{table}","query":{{"select":null,"filter":{{"op":"contains_any","column":"tags","values":["\udfff"]}},"order_by":null,"limit":null}}}}"#
            ),
        ),
        (
            "query.rank",
            format!(
                r#"{{"table_id":"{table}","query":{{"select":null,"filter":null,"limit":1}},"rank":{{"rankings":[{{"column":"body","op":"match","value":"\ud800"}}]}}}}"#
            ),
        ),
    ];
    for (operation, payload) in cases {
        let body = format!(
            r#"{{"request_id":"{request}","operation":"{operation}","payload":{payload}}}"#
        );
        let error = parse_oltp_request(body.as_bytes()).expect_err(operation);
        assert_eq!(error.code, ErrorCode::ValidationError, "{operation}");
        assert_eq!(error.outcome, Outcome::NotApplied, "{operation}");
    }
}
fn text(value: &Value) -> &str {
    value.as_str().unwrap()
}
fn list(value: &Value) -> &[Value] {
    value.as_array().unwrap()
}
fn request(
    operation: &str,
    payload: &Value,
    table: &TableDescriptor,
) -> Result<Operation, ProtocolError> {
    let body =
        json!({"request_id": Uuid::from_u128(1), "operation": operation, "payload": payload});
    let request = parse_oltp_request(&serde_json::to_vec(&body).unwrap())?;
    request.operation.validate_for_table(table)?;
    Ok(request.operation)
}
fn descriptor(data: &str) -> Result<TableDescriptor, ProtocolError> {
    let table: TableDescriptor =
        serde_json::from_str(data).map_err(|_| invalid("malformed descriptor"))?;
    table.validate()?;
    Ok(table)
}
fn table(fixtures: &Value, case: &Value) -> TableDescriptor {
    descriptor(text(&fixtures["tables"][text(&case["table"])])).unwrap()
}
fn same(actual: &impl Serialize, expected: &str) {
    assert_eq!(
        serde_json::to_value(actual).unwrap(),
        serde_json::from_str::<Value>(expected).unwrap()
    );
}

// ---- the fixtures' spelling of client values, encoded without any check ----

fn float(value: &Value) -> Option<f32> {
    let value = value.get("$float").unwrap_or(value);
    let value = match value {
        Value::String(text) => text.parse().ok()?,
        value => value.as_f64()?,
    };
    Some(value as f32)
}
fn vector(value: &Value) -> Option<Vec<f32>> {
    let items = value.get("$list").or(value.get("$float32"))?;
    list(items).iter().map(float).collect()
}
/// The encoding a client would send. What it cannot encode stays as it is,
/// which no column accepts either.
fn wire(value: &Value) -> Value {
    if let Some(values) = vector(value) {
        return json!({"f32": encode_floats(&values)});
    }
    if let Some(repeat) = value.get("$repeat") {
        let count = list(repeat)[1].as_u64().unwrap();
        return wire(&(0..count).map(|_| list(repeat)[0].clone()).collect());
    }
    if let Some(pairs) = value.get("$sparse") {
        let pairs: Option<BTreeMap<u32, f32>> = list(pairs)
            .iter()
            .map(|pair| Some((u32::try_from(pair[0].as_u64()?).ok()?, float(&pair[1])?)))
            .collect();
        let Some(pairs) = pairs else {
            return value.clone();
        };
        let indices = pairs.keys().map(|i| i.to_le_bytes());
        let weights: Vec<f32> = pairs.values().copied().collect();
        return json!({"sparse": {
            "i": encode_words(indices, pairs.len()),
            "f32": encode_floats(&weights),
        }});
    }
    if let Some(repeat) = value.get("$text") {
        let count = list(repeat)[1].as_u64().unwrap() as usize;
        return json!(text(&list(repeat)[0]).repeat(count));
    }
    if let Some(number) = value.get("$int") {
        return serde_json::from_str(text(number)).unwrap();
    }
    if let Some(number) = value.get("$float") {
        return number.clone();
    }
    match value {
        Value::Array(items) => {
            let vectors: Option<Vec<Vec<f32>>> = items.iter().map(vector).collect();
            match vectors {
                Some(vectors) if !vectors.is_empty() => {
                    json!({"f32s": encode_floats(&vectors.concat()), "n": vectors.len()})
                }
                _ => items.iter().map(wire).collect(),
            }
        }
        Value::Object(map) => map
            .iter()
            .map(|(name, value)| (name.clone(), wire(value)))
            .collect::<Map<_, _>>()
            .into(),
        value => value.clone(),
    }
}
fn ranking(value: &Value) -> Value {
    let items = list(value);
    let value = match items.get(2).unwrap_or(&Value::Null) {
        Value::Array(items) if items.is_empty() => json!({"f32s": "", "n": 0}),
        value => wire(value),
    };
    json!({"column": items[0], "op": items[1], "value": value})
}
fn rankings(value: &Value) -> Value {
    match value.get("$repeat") {
        Some(repeat) => {
            let count = list(repeat)[1].as_u64().unwrap();
            (0..count).map(|_| ranking(&list(repeat)[0])).collect()
        }
        None => list(value).iter().map(ranking).collect(),
    }
}
fn filter(value: &Value) -> Value {
    let Some([op, column, value]) = value.as_array().map(Vec::as_slice) else {
        return value.clone();
    };
    let field = if op == "contains_any" {
        "values"
    } else {
        "value"
    };
    json!({"op": op, "column": column, field: wire(value)})
}
fn query(id: Uuid, options: &Value) -> (&'static str, Value) {
    let get = |name: &str| options.get(name).cloned().unwrap_or(Value::Null);
    let order = match (get("order_by"), get("order_by_first")) {
        (Value::Null, Value::Null) => Value::Null,
        (Value::Null, column) | (column, _) => json!({"column": column, "desc": false}),
    };
    let many = options["terminal"] == "rank_many";
    if options.get("rank").is_none() && !many {
        let query = json!({"select": get("select"), "filter": filter(&get("where")), "order_by": order, "limit": get("limit")});
        let operation = if options["terminal"] == "count" {
            "query.count"
        } else {
            "query.collect"
        };
        return (operation, json!({"table_id": id, "query": query}));
    }
    let limit = get(if many { "limit_many" } else { "limit" });
    let mut query =
        json!({"select": get("select"), "filter": filter(&get("where")), "limit": wire(&limit)});
    if order != Value::Null {
        query["order_by"] = order;
    }
    let mut rank = json!({"rankings": [], "fusion": "rrf", "candidates": 100, "recall": 0.95, "exact": false, "consistency": "latest", "rerank": null});
    if let Some(changes) = options.get("rank_options") {
        for (name, value) in changes.as_object().unwrap() {
            rank[name] = wire(value);
        }
    }
    if let Some(rerank) = options.get("rerank") {
        let items = list(rerank);
        rank["rerank"] = json!({"ranking": ranking(&items[0])});
        if let Some(candidates) = items.get(1) {
            rank["rerank"]["candidates"] = wire(candidates);
        }
    }
    if !many {
        rank["rankings"] = rankings(&options["rank"]);
        return (
            "query.rank",
            json!({"table_id": id, "query": query, "rank": rank}),
        );
    }
    let batch = rankings(&options["rankings"]);
    (
        "query.rank_many",
        json!({"table_id": id, "query": query, "rank": rank, "batch": batch}),
    )
}
/// The request a client without any check would send for a refused call.
fn refused(id: Uuid, call: &Value) -> (String, Value) {
    let method = text(&call["method"]);
    let args = list(&call["args"]);
    let empty = json!({});
    let options = call.get("options").unwrap_or(&empty);
    let mut payload = match method {
        "query" => {
            let (operation, payload) = query(id, options);
            return (operation.into(), payload);
        }
        "insert" | "upsert" => json!({"row": wire(&args[0])}),
        "insert_many" | "upsert_many" => json!({"rows": wire(&args[0])}),
        "update" => json!({"key": args[0], "values": wire(&args[1])}),
        "delete_many" => json!({"keys": wire(&args[0])}),
        "add_column" => json!({"name": args[0], "type": args[1]["$type"]}),
        "drop_column" => json!({"name": args[0]}),
        "add_full_text" => json!({"column": args[0], "language": args[1]}),
        "drop_full_text" => json!({"column": args[0]}),
        other => panic!("no wire form for {other}"),
    };
    for (name, value) in options.as_object().unwrap() {
        payload[name] = value.clone();
    }
    payload["table_id"] = json!(id);
    (format!("table.{method}"), payload)
}

#[test]
fn vs18_creates_and_descriptors_decode_as_the_sdks_expect() {
    let fixtures = fixtures();
    let (mut accepted, mut rejected) = (0, 0);
    for case in list(&fixtures["create"]) {
        let name = text(&case["name"]);
        if let Some(payload) = case.get("payload") {
            // With an error, the client refuses a reply that differs.
            let payload: Value = serde_json::from_str(text(payload)).unwrap();
            let table = descriptor(text(&case["data"])).unwrap();
            let operation = request("table.create", &payload, &table).unwrap();
            assert_eq!(
                serde_json::to_value(&operation).unwrap()["payload"],
                payload,
                "{name}"
            );
            same(&table, text(&case["data"]));
            accepted += 1;
        } else {
            let mut payload =
                json!({"name": "made", "schema": case["schema"], "key": "id", "format": "parquet"});
            for (option, value) in case["options"].as_object().unwrap() {
                payload[option] = value.clone();
            }
            let table = descriptor(text(&fixtures["tables"]["users"])).unwrap();
            let error = request("table.create", &payload, &table).expect_err(name);
            let expected = if case["error"]["type"] == "LimitExceededError" {
                ErrorCode::LimitExceeded
            } else {
                ErrorCode::ValidationError
            };
            assert_eq!(error.code, expected, "{name}");
            rejected += 1;
        }
    }
    for case in list(&fixtures["describe"]) {
        let (name, data) = (text(&case["name"]), text(&case["data"]));
        match descriptor(data) {
            Ok(table) => {
                assert!(case.get("error").is_none(), "{name}");
                same(&table, data);
                accepted += 1;
            }
            Err(_) => {
                assert!(case.get("error").is_some(), "{name}");
                rejected += 1;
            }
        }
    }
    assert!(accepted >= 10 && rejected >= 60, "{accepted} {rejected}");
}

#[test]
fn vs18_calls_decode_as_the_sdks_expect() {
    let fixtures = fixtures();
    let (mut accepted, mut replies, mut rejected, mut skipped) = (0, 0, 0, 0);
    for case in list(&fixtures["calls"]) {
        let name = text(&case["name"]);
        let table = table(&fixtures, case);
        if let Some(payload) = case.get("payload") {
            // Sent to the server, whatever it answers in the case.
            let payload: Value = serde_json::from_str(text(payload)).unwrap();
            let operation = text(&case["operation"]);
            let decoded = request(operation, &payload, &table)
                .unwrap_or_else(|error| panic!("{name}: {error}"));
            assert_eq!(
                serde_json::to_value(&decoded).unwrap()["payload"],
                payload,
                "{name}"
            );
            accepted += 1;
            let data = case["response"].get("data").and_then(Value::as_str);
            let Some(data) = data else { continue };
            let refuse = case.get("error").is_some();
            let decoded = match operation {
                "table.get" => decode_row(data).and_then(|row| {
                    validate_row(&row, &table, RowMode::Stored)?;
                    same(&row, data);
                    Ok(())
                }),
                "table.upsert_many" | "table.delete_many" => {
                    // A refusing client stops at a refused row, so only
                    // malformed replies are refused by the decoder.
                    if refuse && !name.starts_with("invalid outcomes") {
                        continue;
                    }
                    let rows = payload.get("rows").or(payload.get("keys")).unwrap();
                    match serde_json::from_str::<RowOutcomes>(data) {
                        Ok(reply) if reply.outcomes.len() == list(rows).len() => {
                            same(&reply, data);
                            Ok(())
                        }
                        _ => Err(invalid("malformed outcomes")),
                    }
                }
                _ => continue,
            };
            assert_eq!(decoded.is_err(), refuse, "{name}: {data}");
            replies += 1;
        } else if case.get("error").is_some() {
            if CLIENT_ONLY.contains(&name) {
                skipped += 1;
                continue;
            }
            let (operation, payload) = refused(table.id, &case["call"]);
            let error = match request(&operation, &payload, &table) {
                Ok(_) => panic!("{name}: {operation} {payload}"),
                Err(error) => error,
            };
            assert_eq!(error.outcome, Outcome::NotApplied, "{name}");
            assert!(
                matches!(
                    error.code,
                    ErrorCode::ValidationError | ErrorCode::LimitExceeded
                ),
                "{name}: {error}"
            );
            rejected += 1;
        }
    }
    assert_eq!(skipped, CLIENT_ONLY.len());
    assert!(
        accepted >= 100 && replies >= 40 && rejected >= 180,
        "{accepted} {replies} {rejected}"
    );
}

/// The translation above keeps accepted values accepted, so a refusal comes
/// from the rule under test.
#[test]
fn vs18_the_fixture_translation_keeps_valid_calls_valid() {
    let fixtures = fixtures();
    let mut checked = 0;
    for case in list(&fixtures["calls"]) {
        let method = text(&case["call"]["method"]);
        let valid = case.get("payload").is_some() && case.get("error").is_none();
        if !valid || method == "get" || method == "search_status" {
            continue;
        }
        let options = case["call"].get("options");
        // A fluent chain that overrides an earlier call has no flat form.
        if options.is_some_and(|options| options.get("rank_first").is_some()) {
            continue;
        }
        let table = table(&fixtures, case);
        let (operation, payload) = refused(table.id, &case["call"]);
        if operation != text(&case["operation"]) || request(&operation, &payload, &table).is_err() {
            continue;
        }
        checked += 1;
    }
    assert!(checked >= 25, "{checked}");
}
