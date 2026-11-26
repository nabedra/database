//! The server decoder against what the Python SDK sends (task C1, VEC-G12).
//! `tests/data/sdk_sent_requests.jsonl` holds one line per scenario of
//! `orm/contract/vector/adversarial.json`, plus a few `extra` probes: the exact
//! bodies the SDK sent, or none when it refused the call itself. The
//! TypeScript SDK sends the same bytes for every one of them.
//! `NABEDRA_SDK_REPLAY` names another capture of the same form. Every sent
//! request must decode and validate against its table, so a server that
//! tightens fails here; the wire forms of client refusals pin where it is
//! looser or answers with another code.
use nabedra_protocol::protocol::*;
use serde_json::{Value, json};

const CAPTURE: &str = include_str!("data/sdk_sent_requests.jsonl");
/// The tables of `orm/python/tests/support/fake_server.py`.
const TABLES: [(&str, &str); 5] = [
    (
        "docs",
        r#"{"id":"00000000-0000-0000-0000-000000000021","name":"docs","key":"id","format":"parquet","schema":{"id":"utf8","n":"int64","f":"float64","b":"bool","vec":{"type":"vector","dim":2},"sp":{"type":"vector","sparse":true},"mu":{"type":"list","item":{"type":"vector","dim":2}},"tags":{"type":"list","item":"utf8"}}}"#,
    ),
    (
        "plain",
        r#"{"id":"00000000-0000-0000-0000-000000000022","name":"plain","key":"id","format":"parquet","schema":{"id":"utf8","n":"int64","f":"float64","b":"bool"}}"#,
    ),
    (
        "chunks",
        r#"{"id":"00000000-0000-0000-0000-000000000023","name":"chunks","key":"id","format":"parquet","schema":{"id":"utf8","body":"utf8","note":"utf8","year":"int64","emb":{"type":"vector","dim":3},"sp":{"type":"vector","sparse":true},"mu":{"type":"list","item":{"type":"vector","dim":2}},"tags":{"type":"list","item":"utf8"}},"full_text":{"body":"english"},"embed":{"emb":{"source":"body","model":"m1"}}}"#,
    ),
    (
        "lean",
        r#"{"id":"00000000-0000-0000-0000-000000000024","name":"lean","key":"id","format":"parquet","schema":{"id":"utf8","body":"utf8","emb":{"type":"vector","dim":3}}}"#,
    ),
    (
        "numbered",
        r#"{"id":"00000000-0000-0000-0000-000000000025","name":"numbered","key":"id","format":"parquet","schema":{"id":"int64","vec":{"type":"vector","dim":1}}}"#,
    ),
];
const MODEL: &[&str] = &[
    "extra create with embed model 'docs v1'",
    "extra create with a 65-character embed model",
    "extra create with embed model '-m'",
    "extra add_column with embed model 'a b'",
];
const LANGUAGE: &[&str] = &[
    "extra create with language 'English'",
    "extra create with language 'en-US'",
    "extra create with a 33-character language",
    "extra add_full_text with language 'English'",
];
const RANKINGS: &[&str] = &["extra rank of 257 rankings"];
/// The sparse case is not in the committed capture: 1.4 MB that does not compress.
const QUERY_SIZE: &[&str] = &[
    "extra multi query of 65 vectors of 4096",
    "extra sparse query of 262145 entries",
];
/// Sent by the SDK and refused by the server, each with its open bug.
const OPEN: [&[&str]; 4] = [MODEL, LANGUAGE, RANKINGS, QUERY_SIZE];

struct Case {
    name: String,
    table: Result<TableDescriptor, String>,
    bodies: Vec<String>,
}

/// A captured text: itself, or segments whose concatenation it is, where
/// `{"r": unit, "n": count}` repeats a unit and `{"q": parts, "i": from, "n": count}`
/// joins the parts with each of `count` numbers from `from`.
fn expand(value: &Value, out: &mut String) {
    match value {
        Value::String(text) => out.push_str(text),
        Value::Array(segments) => segments.iter().for_each(|segment| expand(segment, out)),
        Value::Object(segment) => {
            let count = segment["n"].as_u64().unwrap();
            if let Some(unit) = segment.get("r") {
                out.push_str(&unit.as_str().unwrap().repeat(count as usize));
                return;
            }
            let parts: Vec<&str> = segment["q"]
                .as_array()
                .unwrap()
                .iter()
                .map(|p| p.as_str().unwrap())
                .collect();
            let from = segment["i"].as_u64().unwrap();
            for number in from..from + count {
                out.push_str(&parts.join(&number.to_string()));
            }
        }
        other => panic!("malformed capture segment {other}"),
    }
}
fn text(value: &Value) -> String {
    let mut out = String::new();
    expand(value, &mut out);
    out
}
fn descriptor(data: &str) -> Result<TableDescriptor, String> {
    let table: TableDescriptor =
        serde_json::from_str(data).map_err(|e| format!("malformed descriptor: {e}"))?;
    table.validate().map_err(|e| format!("{:?}: {e}", e.code))?;
    Ok(table)
}
fn named(name: &str) -> TableDescriptor {
    descriptor(TABLES.iter().find(|(n, _)| *n == name).unwrap().1).unwrap()
}
fn cases() -> Vec<Case> {
    let capture = match std::env::var("NABEDRA_SDK_REPLAY") {
        Ok(path) => std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}")),
        Err(_) => CAPTURE.to_owned(),
    };
    capture
        .lines()
        .map(|line| {
            let case: Value = serde_json::from_str(line).unwrap();
            let table = match case["table"].as_str().unwrap() {
                "custom" => descriptor(&text(&case["descriptor"])),
                name => Ok(named(name)),
            };
            Case {
                name: case["name"].as_str().unwrap().to_owned(),
                table,
                bodies: case["requests"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|r| text(&r["raw"]))
                    .collect(),
            }
        })
        .collect()
}
/// Decoded and checked against the table, as the server does before it runs a request.
fn accept(table: &Result<TableDescriptor, String>, body: &str) -> Result<(), String> {
    let table = table
        .as_ref()
        .map_err(|e| format!("the SDK used a descriptor the server refuses: {e}"))?;
    let request = parse_oltp_request(body.as_bytes()).map_err(|e| format!("{:?}: {e}", e.code))?;
    request
        .operation
        .validate_for_table(table)
        .map_err(|e| format!("{:?}: {e}", e.code))
}
/// The refusals of the sent requests of these cases, and how many requests there were.
fn refusals(names: &[&str]) -> (Vec<String>, usize) {
    let (mut found, mut requests) = (Vec::new(), 0);
    for case in cases()
        .iter()
        .filter(|case| names.contains(&case.name.as_str()))
    {
        for (index, body) in case.bodies.iter().enumerate() {
            requests += 1;
            if let Err(error) = accept(&case.table, body) {
                found.push(format!("{} #{index}: {error}", case.name));
            }
        }
    }
    (found, requests)
}
fn open_bug(names: &[&str]) {
    let (refused, requests) = refusals(names);
    assert!(requests > 0, "no captured request of {names:?}");
    assert!(
        refused.is_empty(),
        "{} refused:\n{}",
        refused.len(),
        refused.join("\n")
    );
}

#[test]
fn every_request_the_python_sdk_sends_is_accepted() {
    let (mut total, mut sent, mut requests, mut refused) = (0, 0, 0, Vec::new());
    for case in cases() {
        total += 1;
        if case.bodies.is_empty() {
            continue;
        }
        sent += 1;
        if OPEN.iter().any(|names| names.contains(&case.name.as_str())) {
            continue;
        }
        for (index, body) in case.bodies.iter().enumerate() {
            requests += 1;
            if let Err(error) = accept(&case.table, body) {
                refused.push(format!("{} #{index}: {error}", case.name));
            }
        }
    }
    assert!(
        refused.is_empty(),
        "{} refused:\n{}",
        refused.len(),
        refused.join("\n")
    );
    assert!(
        total >= 615 && sent >= 385 && requests >= 470,
        "{total} {sent} {requests}"
    );
}

/// The SDKs take any nonempty model name; the server wants [A-Za-z0-9][A-Za-z0-9_.-]{0,63}.
#[test]
#[ignore = "open bug 2026-11-26-sdk-server-model-name"]
fn sdk_model_names_are_server_model_names() {
    open_bug(MODEL);
}

/// The SDKs take any nonempty language; the server wants [a-z_]{1,32}.
#[test]
#[ignore = "open bug 2026-11-26-sdk-server-language"]
fn sdk_languages_are_server_languages() {
    open_bug(LANGUAGE);
}

/// `rank()` has no client bound on its rankings; the server decodes at most 256.
#[test]
#[ignore = "open bug 2026-11-26-sdk-server-rank-257"]
fn sdk_rank_sends_at_most_256_rankings() {
    open_bug(RANKINGS);
}

/// A query vector is bounded by the 8 MiB body only in the SDKs, by 2^18 words on the server.
#[test]
#[ignore = "open bug 2026-11-26-sdk-server-query-vector-size"]
fn sdk_query_vectors_fit_the_server_bound() {
    // The value the SDK sends for 262,145 entries, rebuilt: the full capture has it byte for byte.
    let indices: Vec<u8> = (0..262_145u32).flat_map(u32::to_le_bytes).collect();
    let weights: Vec<u8> = (0..262_145).flat_map(|_| 1f32.to_le_bytes()).collect();
    let value = json!({"sparse": {"i": b64(&indices), "f32": b64(&weights)}});
    let sparse = wire(
        &named("chunks"),
        "query.rank",
        rank(json!([ranking("sp", "dot", value)]), 3),
    );
    let (refused, requests) = refusals(&QUERY_SIZE[..1]);
    assert!(requests > 0, "no captured multi query");
    assert!(
        refused.is_empty() && sparse.is_ok(),
        "{refused:?}, sparse: {sparse:?}"
    );
}

fn b64(bytes: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = u32::from(b[0]) << 16 | u32::from(b[1]) << 8 | u32::from(b[2]);
        for i in 0..4 {
            out.push(if i <= chunk.len() {
                A[(n >> (18 - 6 * i)) as usize & 63] as char
            } else {
                '='
            });
        }
    }
    out
}
fn floats(values: &[f32]) -> Value {
    json!({"f32": b64(&values.iter().flat_map(|v| v.to_le_bytes()).collect::<Vec<u8>>())})
}
fn ranking(column: &str, op: &str, value: Value) -> Value {
    json!({"column": column, "op": op, "value": value})
}
/// A `query.rank` payload on `chunks` as the SDKs spell it.
fn rank(rankings: Value, limit: u64) -> Value {
    json!({"table_id": named("chunks").id, "query": {"select": ["id", "body", "note", "year", "tags"], "filter": null,
        "limit": limit}, "rank": {"rankings": rankings, "fusion": "rrf", "candidates": 100, "recall": 0.95,
        "exact": false, "consistency": "latest", "rerank": null}})
}
fn wire(table: &TableDescriptor, operation: &str, payload: Value) -> Result<(), ErrorCode> {
    let body = json!({"request_id": "67e55044-10b1-426f-9247-bb680e5fe0c8", "operation": operation, "payload": payload});
    let request = parse_oltp_request(&serde_json::to_vec(&body).unwrap()).map_err(|e| e.code)?;
    request
        .operation
        .validate_for_table(table)
        .map_err(|e| e.code)
}
/// A `docs` row; the server counts it as 125 bytes plus the tag's length.
fn docs_row(tag: usize, mu: Value) -> Value {
    json!({"table_id": named("docs").id, "rows": [{"id": "a", "n": 1, "f": 1.2345678901234567, "b": true,
        "vec": floats(&[1.0, 2.0]), "sp": {"sparse": {"i": "", "f32": ""}}, "mu": mu, "tags": ["x".repeat(tag)]}]})
}
/// A `docs` row whose `vec`, its last column, holds `count` values.
fn dense(count: usize) -> Value {
    let mut payload = docs_row(1, json!({"f32s": "", "n": 0}));
    payload["rows"][0]["vec"] = floats(&vec![0.5; count]);
    payload
}
fn create(schema: Value, options: Value) -> Value {
    let mut payload = json!({"name": "made", "schema": schema, "key": "id", "format": "parquet"});
    payload
        .as_object_mut()
        .unwrap()
        .extend(options.as_object().unwrap().clone());
    payload
}

/// The closest wire form of calls the SDKs refuse before sending, with the server's answer.
#[test]
fn client_refusals_have_these_server_answers() {
    use ErrorCode::{LimitExceeded, ValidationError};
    let (docs, chunks) = (named("docs"), named("chunks"));
    let no_multi = json!({"f32s": "", "n": 0});
    let vectors = |count: usize| {
        let mut schema = json!({"id": "utf8"});
        for i in 0..count {
            schema[format!("v{i}")] = json!({"type": "vector", "dim": 2});
        }
        schema
    };
    let mut wide = json!({"id": "utf8"});
    for i in 0..128 {
        wide[format!("c{i}")] = json!("int64");
    }
    let query = ranking("emb", "cosine", floats(&[1.0, 0.0, 0.0]));
    let mut many = rank(json!([]), 2);
    many["batch"] = json!(vec![query.clone(); 257]);
    let strings = json!({"id": "utf8", "t": "utf8"});
    let cases = [
        // "create with nine vector columns": the SDKs raise LimitExceededError.
        (
            "nine vector columns",
            wire(&docs, "table.create", create(vectors(9), json!({}))),
            Err(LimitExceeded),
        ),
        // "misuse rank_many of 257": the SDKs raise LimitExceededError.
        (
            "rank_many of 257",
            wire(&chunks, "query.rank_many", many),
            Err(LimitExceeded),
        ),
        // "create with 129 columns": the SDKs raise LimitExceededError.
        (
            "129 columns",
            wire(&docs, "table.create", create(wide, json!({}))),
            Err(ValidationError),
        ),
        // "extra float row at the server's 1 MiB": the SDKs count 1.2345678901234567 as 18 bytes, the server as 1.
        (
            "float row at 1 MiB",
            wire(
                &docs,
                "table.upsert_many",
                docs_row(1_048_451, no_multi.clone()),
            ),
            Ok(()),
        ),
        (
            "float row over 1 MiB",
            wire(&docs, "table.upsert_many", docs_row(1_048_452, no_multi)),
            Err(LimitExceeded),
        ),
        // The same answer as the SDKs. A row object goes out with sorted keys, so `vec` is last.
        (
            "dense 4097 values",
            wire(&docs, "table.upsert_many", dense(4097)),
            Err(LimitExceeded),
        ),
        (
            "eight vector columns",
            wire(&docs, "table.create", create(vectors(8), json!({}))),
            Ok(()),
        ),
        (
            "limit 1001",
            wire(&chunks, "query.rank", rank(json!([query]), 1001)),
            Err(LimitExceeded),
        ),
        (
            "cosine on a zero query",
            wire(
                &chunks,
                "query.rank",
                rank(
                    json!([ranking("emb", "cosine", floats(&[0.0, -0.0, 0.0]))]),
                    3,
                ),
            ),
            Err(ValidationError),
        ),
        (
            "empty language",
            wire(
                &docs,
                "table.create",
                create(strings, json!({"full_text": {"t": ""}})),
            ),
            Err(ValidationError),
        ),
    ];
    for (name, actual, expected) in cases {
        assert_eq!(actual, expected, "{name}");
    }
}

/// A row limit applies even when more fields follow the oversized value.
#[test]
fn a_limit_in_any_column_of_a_row_is_limit_exceeded() {
    let docs = named("docs");
    let row = docs_row(1, json!({"f32s": floats(&[0.0; 2050])["f32"], "n": 1025}));
    assert_eq!(
        wire(&docs, "table.upsert_many", row),
        Err(ErrorCode::LimitExceeded),
        "multi 1025 vectors in mu"
    );
    assert_eq!(
        wire(&docs, "table.upsert_many", dense(4097)),
        Err(ErrorCode::LimitExceeded),
        "dense 4097 in vec"
    );
}

#[test]
fn ranked_lists_report_the_typed_limit_error() {
    let chunks = named("chunks");
    let item = ranking("emb", "dot", floats(&[1.0, 2.0, 3.0]));
    for count in [256, 257, 300] {
        let want = if count == 256 {
            Ok(())
        } else {
            Err(ErrorCode::LimitExceeded)
        };
        let list = json!(vec![item.clone(); count]);
        assert_eq!(wire(&chunks, "query.rank", rank(list.clone(), 10)), want);
        let mut many = rank(json!([]), 10);
        many["batch"] = list;
        assert_eq!(wire(&chunks, "query.rank_many", many), want);
    }
}
