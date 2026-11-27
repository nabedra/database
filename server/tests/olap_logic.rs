//! Nabedra logic tests: golden `.nlt` files under `tests/logic/`, run in process
//! against the OLAP engine through the real `/v1/operations` router. The format
//! is documented in `tests/logic/README.md`. The Python and TypeScript ORM
//! replays read the same files and must render results identically.
//!
//! `NABEDRA_BLESS=1` rewrites expected blocks in place. `NABEDRA_LOGIC_FILTER`
//! restricts the run to files whose relative path contains the value.
//! `NABEDRA_LOGIC_RECORD=<dir>` writes every server exchange per file, for the
//! ORM replays' offline transcript check (`NABEDRA_LOGIC_TRANSCRIPTS`).
#![allow(clippy::disallowed_methods)] // integration tests drive real processes and real time; the phase 7 seam covers server/src
#![cfg(feature = "olap")]

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use nabedra_database_server::{
    api,
    engine::Engine,
    executor::Executor,
    partition_size::PartitionSize,
    storage::{Namespace, Storage},
    worker_pool::{WorkerPool, WorkerThreads},
};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use tower::ServiceExt;
#[path = "support/storage.rs"]
mod support;
use support::Fake;

const TOKEN: &str = "logic-token";
const MISSING_TABLE_ID: &str = "ffffffff-ffff-4fff-bfff-ffffffffffff";

// ---------------------------------------------------------------------------
// Format

#[derive(Debug, Clone, PartialEq)]
enum Kind {
    StatementOk,
    StatementError(String),
    Query {
        sort: Sort,
        hash: bool,
        label: Option<String>,
    },
    Explain,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Sort {
    RowSort,
    NoSort,
}

#[derive(Debug, Clone)]
struct Record {
    kind: Kind,
    /// 1-based line of the header.
    line: usize,
    skip: Option<String>,
    command: String,
    /// Expected lines, when the record has a `----` block.
    expected: Option<Vec<String>>,
    /// 0-based index of the first line after the command (the `----` line or
    /// the record end), and of the line after the record.
    body_end: usize,
    end: usize,
}

#[derive(Debug, Clone)]
struct File {
    name: String,
    engines: Vec<String>,
    records: Vec<Record>,
    lines: Vec<String>,
}

fn parse(text: &str) -> Result<File, String> {
    let lines: Vec<String> = text.lines().map(str::to_owned).collect();
    let name = lines
        .first()
        .and_then(|l| l.strip_prefix("# name:"))
        .map(|n| n.trim().to_owned())
        .ok_or("line 1: the file must start with `# name: <name>`")?;
    let mut engines = vec!["olap".to_owned(), "oltp".to_owned()];
    let mut records = Vec::new();
    let mut skip: Option<(usize, String)> = None;
    let mut i = 1;
    while i < lines.len() {
        let line = lines[i].trim_end();
        if line.is_empty() || line.starts_with('#') {
            i += 1;
            continue;
        }
        let at = |msg: &str| format!("line {}: {msg}", i + 1);
        if let Some(rest) = line.strip_prefix("require ") {
            if !records.is_empty() || skip.is_some() {
                return Err(at("require must come before the first record"));
            }
            let set: Vec<String> = rest.split_whitespace().map(str::to_owned).collect();
            if set.is_empty() || set.iter().any(|e| e != "olap" && e != "oltp") {
                return Err(at("require takes olap and/or oltp"));
            }
            engines = set;
            i += 1;
            continue;
        }
        if let Some(reason) = line.strip_prefix("skipif") {
            let reason = reason.trim();
            if reason.is_empty() || skip.is_some() {
                return Err(at("skipif needs a reason and one record"));
            }
            skip = Some((i, reason.to_owned()));
            i += 1;
            continue;
        }
        let words: Vec<&str> = line.split_whitespace().collect();
        let kind = match words.as_slice() {
            ["statement", "ok"] => Kind::StatementOk,
            ["statement", "error", ..] => {
                let pattern = line["statement error".len()..].trim();
                if pattern.is_empty() {
                    return Err(at("statement error needs a code or message substring"));
                }
                Kind::StatementError(pattern.to_owned())
            }
            ["query", sort, rest @ ..] => {
                let sort = match *sort {
                    "rowsort" => Sort::RowSort,
                    "nosort" => Sort::NoSort,
                    other => return Err(at(&format!("unknown sort mode {other:?}"))),
                };
                let (mut hash, mut label) = (false, None);
                for word in rest {
                    if *word == "hash" && !hash {
                        hash = true;
                    } else if let Some(name) = word.strip_prefix("label-")
                        && label.is_none()
                        && !name.is_empty()
                    {
                        label = Some(name.to_owned());
                    } else {
                        return Err(at(&format!("unknown query option {word:?}")));
                    }
                }
                Kind::Query { sort, hash, label }
            }
            ["explain"] => Kind::Explain,
            _ => return Err(at(&format!("unknown directive {line:?}"))),
        };
        if let Some((skip_line, _)) = &skip
            && *skip_line + 1 != i
        {
            return Err(at("skipif must be on the line right before its record"));
        }
        let header = i;
        i += 1;
        let start = i;
        let takes_result = !matches!(kind, Kind::StatementOk | Kind::StatementError(_));
        while i < lines.len()
            && !lines[i].trim().is_empty()
            && !(takes_result && lines[i] == "----")
        {
            if lines[i] == "----" {
                return Err(format!("line {}: statements have no ---- block", i + 1));
            }
            i += 1;
        }
        if i == start {
            return Err(format!("line {}: record has no operation", header + 1));
        }
        let command = lines[start..i].join("\n");
        let body_end = i;
        let mut expected = None;
        if i < lines.len() && lines[i] == "----" {
            i += 1;
            let first = i;
            while i < lines.len() && !lines[i].is_empty() {
                i += 1;
            }
            expected = Some(lines[first..i].to_vec());
        }
        match (&kind, &expected) {
            (Kind::Query { label: None, .. } | Kind::Explain, None) => {
                return Err(format!(
                    "line {}: query needs a ---- block unless it has a label",
                    header + 1
                ));
            }
            (Kind::Query { hash: true, .. }, Some(block)) if block.len() != 1 => {
                return Err(format!(
                    "line {}: hash expects one `N values hashing to <hex>` line",
                    header + 1
                ));
            }
            _ => {}
        }
        records.push(Record {
            kind,
            line: header + 1,
            skip: skip.take().map(|s| s.1),
            command,
            expected,
            body_end,
            end: i,
        });
    }
    if skip.is_some() {
        return Err("skipif at the end of the file has no record".into());
    }
    Ok(File {
        name,
        engines,
        records,
        lines,
    })
}

// ---------------------------------------------------------------------------
// Translation and rendering (identical in the ORM replays)

struct Translator {
    prefix: String,
    tables: BTreeMap<String, String>,
}

impl Translator {
    fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.into(),
            tables: BTreeMap::new(),
        }
    }

    fn translate(&self, command: &str) -> Result<(String, Value), String> {
        let value: Value =
            serde_json::from_str(command).map_err(|e| format!("invalid operation JSON: {e}"))?;
        let Value::Object(mut payload) = value else {
            return Err("operation must be a JSON object".into());
        };
        let Some(Value::String(op)) = payload.remove("op") else {
            return Err("operation needs a string `op`".into());
        };
        if let Some(Value::String(name)) = payload.remove("table") {
            let id = self
                .tables
                .get(&name)
                .map_or(MISSING_TABLE_ID, String::as_str);
            payload.insert("table_id".into(), Value::String(id.into()));
        }
        if (op == "table.create" || op == "table.describe")
            && let Some(Value::String(name)) = payload.get_mut("name")
        {
            *name = format!("{}{name}", self.prefix);
        }
        if op.starts_with("query.") && !payload.contains_key("query") {
            let mut query = Map::new();
            for key in ["select", "filter", "order_by", "limit"] {
                query.insert(key.into(), payload.remove(key).unwrap_or(Value::Null));
            }
            payload.insert("query".into(), Value::Object(query));
        }
        Ok((op, Value::Object(payload)))
    }

    fn observe(&mut self, op: &str, data: &Value) {
        if op == "table.create"
            && let (Some(Value::String(name)), Some(Value::String(id))) =
                (data.get("name"), data.get("id"))
        {
            let logical = name.strip_prefix(&self.prefix).unwrap_or(name);
            self.tables.insert(logical.into(), id.clone());
        }
    }

    fn logical<'a>(&self, name: &'a str) -> &'a str {
        name.strip_prefix(&self.prefix).unwrap_or(name)
    }

    fn descriptor_row(&self, d: &Value) -> Result<Vec<String>, String> {
        let name = d["name"].as_str().ok_or("descriptor without name")?;
        let key = d["key"].as_str().ok_or("descriptor without key")?;
        let schema = d["schema"].as_object().ok_or("descriptor without schema")?;
        let mut columns: Vec<_> = schema.iter().collect();
        columns.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        let schema = columns
            .iter()
            .map(|(c, t)| format!("{c}:{}", t.as_str().unwrap_or("?")))
            .collect::<Vec<_>>()
            .join(",");
        Ok(vec![render_string(self.logical(name)), key.into(), schema])
    }

    fn render(&self, op: &str, payload: &Value, data: &Value) -> Result<Vec<Vec<String>>, String> {
        let row = |row: &Value| -> Result<Vec<String>, String> {
            let object = row.as_object().ok_or("result row is not an object")?;
            let columns: Vec<String> = match payload["query"]["select"].as_array() {
                Some(select) if op == "query.collect" => select
                    .iter()
                    .map(|c| c.as_str().unwrap_or_default().to_owned())
                    .collect(),
                _ => {
                    let mut keys: Vec<String> = object.keys().cloned().collect();
                    keys.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
                    keys
                }
            };
            if columns.len() != object.len() || columns.iter().any(|c| !object.contains_key(c)) {
                return Err(format!(
                    "wrong result columns: expected {columns:?}, got {object:?}"
                ));
            }
            columns.iter().map(|c| render_value(&object[c])).collect()
        };
        match op {
            "table.create" | "table.describe" => Ok(vec![self.descriptor_row(data)?]),
            "table.list" => {
                let mut rows = Vec::new();
                for d in data.as_array().ok_or("table.list result is not a list")? {
                    if d["name"]
                        .as_str()
                        .is_some_and(|n| n.starts_with(&self.prefix))
                    {
                        rows.push(self.descriptor_row(d)?);
                    }
                }
                rows.sort_by(|a, b| a[0].as_bytes().cmp(b[0].as_bytes()));
                Ok(rows)
            }
            "query.collect" => data
                .as_array()
                .ok_or("query.collect result is not a list")?
                .iter()
                .map(row)
                .collect(),
            "table.get" if data.is_null() => Ok(vec![]),
            "table.get" => Ok(vec![row(data)?]),
            "query.explain" => {
                let text = data
                    .as_str()
                    .ok_or("query.explain result is not a string")?;
                Ok(normalize_explain(text, &self.prefix)
                    .lines()
                    .map(|l| vec![render_string(l)])
                    .collect())
            }
            _ if data.is_null() => Ok(vec![]),
            _ => Ok(vec![vec![render_value(data)?]]),
        }
    }
}

fn render_string(s: &str) -> String {
    if s.is_empty() {
        return "(empty)".into();
    }
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            c if (c as u32) < 0x20 || c == '\x7f' => write!(out, "\\x{:02x}", c as u32).unwrap(),
            c => out.push(c),
        }
    }
    out
}

fn render_value(v: &Value) -> Result<String, String> {
    Ok(match v {
        Value::Null => "NULL".into(),
        Value::Bool(b) => b.to_string(),
        Value::String(s) => render_string(s),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.to_string()
            } else if let Some(u) = n.as_u64() {
                u.to_string()
            } else {
                canonical_float(n.as_f64().ok_or("number is not representable")?)
            }
        }
        other => return Err(format!("cannot render {other}")),
    })
}

/// Shortest round-trip digits, plain decimal for exponents in (-7, 21),
/// scientific otherwise. Zero of either sign is `0.0`.
fn canonical_float(v: f64) -> String {
    if v == 0.0 {
        return "0.0".into();
    }
    let sign = if v < 0.0 { "-" } else { "" };
    // Use the wire serializer's shortest digits. Rust's LowerExp chooses
    // different digits for some ties, for example 0.50000762939453125.
    let wire = serde_json::to_string(&v.abs()).expect("finite float");
    let (mantissa, exp) = wire
        .split_once('e')
        .map_or((wire.as_str(), 0), |(m, e)| (m, e.parse::<i32>().unwrap()));
    let integer_len = mantissa.find('.').unwrap_or(mantissa.len()) as i32;
    let digits = mantissa.replace('.', "");
    let significant = digits.trim_start_matches('0');
    let first_exp = exp + integer_len - (digits.len() - significant.len()) as i32 - 1;
    format_float(sign, significant, first_exp)
}

fn format_float(sign: &str, digits: &str, exp: i32) -> String {
    let digits = digits.trim_end_matches('0');
    let digits = if digits.is_empty() { "0" } else { digits };
    if -7 < exp && exp < 21 {
        if exp >= 0 {
            let int_len = exp as usize + 1;
            let (int, frac) = if digits.len() > int_len {
                (digits[..int_len].to_owned(), digits[int_len..].to_owned())
            } else {
                (format!("{digits:0<int_len$}"), "0".to_owned())
            };
            format!("{sign}{int}.{frac}")
        } else {
            format!("{sign}0.{}{digits}", "0".repeat((-exp - 1) as usize))
        }
    } else {
        let (first, rest) = digits.split_at(1);
        let rest = if rest.is_empty() {
            String::new()
        } else {
            format!(".{rest}")
        };
        format!("{sign}{first}{rest}e{exp}")
    }
}

fn is_uuid_at(b: &[u8], i: usize) -> bool {
    const GROUPS: [usize; 5] = [8, 4, 4, 4, 12];
    let mut p = i;
    for (n, len) in GROUPS.iter().enumerate() {
        if n > 0 {
            if b.get(p) != Some(&b'-') {
                return false;
            }
            p += 1;
        }
        for _ in 0..*len {
            if !b.get(p).is_some_and(u8::is_ascii_hexdigit) {
                return false;
            }
            p += 1;
        }
    }
    true
}

fn normalize_explain(text: &str, prefix: &str) -> String {
    let text = if prefix.is_empty() {
        text.to_owned()
    } else {
        text.replace(prefix, "")
    };
    let b = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < b.len() {
        if is_uuid_at(b, i) {
            out.push_str("<uuid>");
            i += 36;
        } else {
            let c = text[i..].chars().next().unwrap();
            out.push(c);
            i += c.len_utf8();
        }
    }
    out
}

fn sorted(mut rows: Vec<Vec<String>>, sort: Sort) -> Vec<String> {
    let mut lines: Vec<String> = rows.drain(..).map(|r| r.join("\t")).collect();
    if sort == Sort::RowSort {
        lines.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    }
    lines
}

fn hash_line(rows: &[Vec<String>], sort: Sort) -> String {
    let mut ordered: Vec<&Vec<String>> = rows.iter().collect();
    if sort == Sort::RowSort {
        ordered.sort_by(|a, b| a.join("\t").as_bytes().cmp(b.join("\t").as_bytes()));
    }
    let mut hasher = Sha256::new();
    let mut n = 0;
    for value in ordered.iter().flat_map(|r| r.iter()) {
        hasher.update(value.as_bytes());
        hasher.update(b"\n");
        n += 1;
    }
    let mut hex = String::new();
    for byte in hasher.finalize().iter() {
        write!(hex, "{byte:02x}").unwrap();
    }
    format!("{n} values hashing to {hex}")
}

// ---------------------------------------------------------------------------
// Execution

/// One wire exchange: `Ok(data)` or `Err((code, message))`.
trait Exec {
    async fn exec(&mut self, op: &str, payload: Value) -> Result<Value, (String, String)>;
}

struct OlapEngine {
    router: Router,
    _workers: WorkerThreads,
}

impl OlapEngine {
    async fn start() -> Self {
        let storage = Storage::new(Fake::default(), Namespace::new("logic/").unwrap());
        let (pool, workers) = WorkerPool::start(
            NonZeroUsize::new(2).unwrap(),
            tokio::runtime::Handle::current(),
        )
        .unwrap();
        let engine = Engine::new(storage, pool, PartitionSize::default());
        let executor = Executor::new(2, 64).unwrap();
        let router =
            api::router_configured(engine, executor, TOKEN.into(), Default::default(), false)
                .unwrap()
                .0;
        Self {
            router,
            _workers: workers,
        }
    }
}

impl Exec for OlapEngine {
    async fn exec(&mut self, op: &str, payload: Value) -> Result<Value, (String, String)> {
        let id = uuid::Uuid::new_v4().to_string();
        let body = json!({"request_id": id, "operation": op, "payload": payload}).to_string();
        let request = Request::post("/v1/operations")
            .header("authorization", format!("Bearer {TOKEN}"))
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();
        let response = self.router.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let value: Value = serde_json::from_slice(&bytes)
            .map_err(|e| ("protocol".to_owned(), format!("non-JSON response: {e}")))?;
        if status == StatusCode::OK {
            Ok(value["data"].clone())
        } else {
            Err((
                value["error"]["code"].as_str().unwrap_or("?").to_owned(),
                value["error"]["message"].as_str().unwrap_or("").to_owned(),
            ))
        }
    }
}

#[derive(Debug, Default)]
struct Outcome {
    assertions: usize,
    skipped: Vec<String>,
    failures: Vec<String>,
    mismatches: Vec<String>,
    /// Actual expected blocks by record index, for blessing.
    actual: BTreeMap<usize, Vec<String>>,
}

async fn run_file(path: &str, file: &File, exec: &mut impl Exec, prefix: &str) -> Outcome {
    let mut out = Outcome::default();
    let mut tr = Translator::new(prefix);
    let mut labels: BTreeMap<String, (usize, Vec<String>)> = BTreeMap::new();
    for (index, record) in file.records.iter().enumerate() {
        let at = format!("{path}:{}", record.line);
        if let Some(reason) = &record.skip {
            out.skipped.push(format!("{at} skipif {reason}"));
            continue;
        }
        out.assertions += 1;
        let (op, payload) = match tr.translate(&record.command) {
            Ok(v) => v,
            Err(e) => {
                out.failures.push(format!("{at}: {e}"));
                continue;
            }
        };
        let result = exec.exec(&op, payload.clone()).await;
        if let Ok(data) = &result {
            tr.observe(&op, data);
        }
        match (&record.kind, result) {
            (Kind::StatementOk, Ok(_)) => {}
            (Kind::StatementError(pattern), Err((code, message)))
                if code == *pattern || message.contains(pattern.as_str()) => {}
            (Kind::StatementError(pattern), Err((code, message))) => out.failures.push(format!(
                "{at}: expected error {pattern:?}\n  actual error: {code}: {message}"
            )),
            (Kind::StatementError(pattern), Ok(data)) => out.failures.push(format!(
                "{at}: expected error {pattern:?}\n  actual: success {data}"
            )),
            (_, Err((code, message))) => out.failures.push(format!(
                "{at}: expected success\n  actual error: {code}: {message}"
            )),
            (kind, Ok(data)) => {
                let rows = match tr.render(&op, &payload, &data) {
                    Ok(rows) => rows,
                    Err(e) => {
                        out.failures.push(format!("{at}: {e}"));
                        continue;
                    }
                };
                let (sort, hash, label) = match kind {
                    Kind::Query { sort, hash, label } => (*sort, *hash, label.clone()),
                    _ => (Sort::NoSort, false, None),
                };
                let actual = if hash {
                    vec![hash_line(&rows, sort)]
                } else {
                    sorted(rows, sort)
                };
                if let Some(label) = label {
                    match labels.get(&label) {
                        Some((line, first)) if *first != actual => {
                            out.failures.push(format!(
                                "{at}: label-{label} disagrees with line {line}\n  expected:\n{}\n  actual:\n{}",
                                indent(first),
                                indent(&actual)
                            ));
                            continue;
                        }
                        Some(_) => {}
                        None => {
                            labels.insert(label, (record.line, actual.clone()));
                        }
                    }
                }
                if let Some(expected) = &record.expected {
                    let mut expected = expected.clone();
                    if sort == Sort::RowSort && !hash {
                        expected.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
                    }
                    if expected != actual {
                        out.mismatches.push(format!(
                            "{at}: wrong result\n  expected:\n{}\n  actual:\n{}",
                            indent(&expected),
                            indent(&actual)
                        ));
                    }
                    out.actual.insert(index, actual);
                }
            }
        }
    }
    out
}

fn indent(lines: &[String]) -> String {
    if lines.is_empty() {
        return "    (no rows)".into();
    }
    lines
        .iter()
        .map(|l| format!("    {l}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn blessed(file: &File, actual: &BTreeMap<usize, Vec<String>>) -> String {
    let mut out: Vec<String> = Vec::new();
    let mut i = 0;
    for (index, record) in file.records.iter().enumerate() {
        let Some(rows) = actual.get(&index) else {
            continue;
        };
        out.extend(file.lines[i..record.body_end].iter().cloned());
        out.push("----".into());
        out.extend(rows.iter().cloned());
        i = record.end;
    }
    out.extend(file.lines[i..].iter().cloned());
    let mut text = out.join("\n");
    text.push('\n');
    text
}

fn corpus_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/logic")
}

fn collect_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    entries.sort();
    for path in entries {
        if path.is_dir() {
            collect_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "nlt") {
            out.push(path);
        }
    }
}

async fn run_text(path: &str, text: &str) -> Result<(File, Outcome), String> {
    run_recorded(path, text, None).await
}

/// With `record`, every exchange is appended as `{"op", "data"}` or
/// `{"op", "error": {"code", "message"}}`, with `data` as raw JSON text so the
/// ORM replays can decode it exactly as their transports do.
async fn run_recorded(
    path: &str,
    text: &str,
    record: Option<&Path>,
) -> Result<(File, Outcome), String> {
    let file = parse(text).map_err(|e| format!("{path}: {e}"))?;
    if !file.engines.iter().any(|e| e == "olap") {
        return Ok((file, Outcome::default()));
    }
    let mut engine = Recording {
        inner: OlapEngine::start().await,
        log: Vec::new(),
    };
    let outcome = run_file(path, &file, &mut engine, "").await;
    if let Some(out) = record {
        std::fs::create_dir_all(out.parent().unwrap()).unwrap();
        std::fs::write(out, serde_json::to_string_pretty(&engine.log).unwrap()).unwrap();
    }
    Ok((file, outcome))
}

struct Recording<E> {
    inner: E,
    log: Vec<Value>,
}

impl<E: Exec> Exec for Recording<E> {
    async fn exec(&mut self, op: &str, payload: Value) -> Result<Value, (String, String)> {
        let result = self.inner.exec(op, payload).await;
        self.log.push(match &result {
            Ok(data) => json!({"op": op, "data": data.to_string()}),
            Err((code, message)) => json!({"op": op, "error": {"code": code, "message": message}}),
        });
        result
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn logic_corpus() {
    let started = std::time::Instant::now();
    let root = corpus_dir();
    let mut paths = Vec::new();
    collect_files(&root, &mut paths);
    let filter = std::env::var("NABEDRA_LOGIC_FILTER").unwrap_or_default();
    let bless = std::env::var("NABEDRA_BLESS").is_ok_and(|v| v == "1");
    let (mut passed, mut failed, mut ignored) = (0, 0, 0);
    let mut failures = Vec::new();
    let mut files = 0;
    for path in &paths {
        let rel = path.strip_prefix(&root).unwrap().display().to_string();
        if !rel.contains(&filter) {
            continue;
        }
        files += 1;
        let text = std::fs::read_to_string(path).unwrap();
        let record = std::env::var_os("NABEDRA_LOGIC_RECORD")
            .map(|dir| Path::new(&dir).join(format!("{rel}.json")));
        let (file, outcome) =
            match run_recorded(&format!("tests/logic/{rel}"), &text, record.as_deref()).await {
                Ok(v) => v,
                Err(e) => {
                    failed += 1;
                    failures.push(e);
                    continue;
                }
            };
        if !file.engines.iter().any(|e| e == "olap") {
            println!("not-run: olap-not-required tests/logic/{rel}");
            ignored += file.records.len();
            continue;
        }
        if file.engines.iter().any(|e| e == "oltp") {
            println!(
                "not-run: oltp-runner-deferred tests/logic/{rel} ({})",
                file.name
            );
        }
        for skip in &outcome.skipped {
            println!("skipped: {skip}");
        }
        ignored += outcome.skipped.len();
        let file_failures = outcome.failures.len() + outcome.mismatches.len();
        failed += file_failures;
        passed += outcome.assertions - file_failures;
        if bless && outcome.failures.is_empty() {
            let new = blessed(&file, &outcome.actual);
            if new != text {
                std::fs::write(path, new).unwrap();
                println!("blessed: tests/logic/{rel}");
            }
        } else {
            failures.extend(outcome.mismatches);
        }
        failures.extend(outcome.failures);
    }
    for failure in &failures {
        println!("FAILED {failure}");
    }
    println!(
        "olap-logic: {files} files; oltp: not-run: oltp-runner-deferred (the oltp feature is not in the default build)"
    );
    println!(
        "logic result: {passed} passed; {failed} mismatched or failed; {ignored} skipped; finished in {:.2}s",
        started.elapsed().as_secs_f64()
    );
    assert!(files > 0, "no .nlt files matched {filter:?}");
    assert!(
        failures.is_empty(),
        "{} logic assertion(s) failed",
        failures.len()
    );
    assert!(passed + failed > 0, "no logic records ran");
}

// ---------------------------------------------------------------------------
// Unit and sensitivity tests

const BASE: &str = "# name: sensitivity
require olap

statement ok
{\"op\":\"table.create\",\"name\":\"t\",\"schema\":{\"id\":\"int64\",\"v\":\"utf8\"},\"key\":\"id\",\"format\":\"parquet\"}

statement ok
{\"op\":\"table.insert_many\",\"table\":\"t\",\"rows\":[{\"id\":1,\"v\":\"b\"},{\"id\":2,\"v\":\"a\"}]}
";

async fn failures_of(extra: &str) -> Vec<String> {
    let text = format!("{BASE}\n{extra}");
    let (_, outcome) = run_text("planted.nlt", &text).await.unwrap();
    outcome
        .failures
        .into_iter()
        .chain(outcome.mismatches)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sensitivity_correct_answers_pass() {
    let ok = failures_of(
        "query nosort\n{\"op\":\"query.collect\",\"table\":\"t\",\"order_by\":{\"column\":\"v\",\"desc\":false}}\n----\n1\tb\n2\ta\n",
    )
    .await;
    assert_eq!(ok.len(), 1, "a wrong-order guard must fail: {ok:?}");
    let ok = failures_of(
        "query nosort\n{\"op\":\"query.collect\",\"table\":\"t\",\"order_by\":{\"column\":\"v\",\"desc\":false}}\n----\n2\ta\n1\tb\n\nquery rowsort label-x\n{\"op\":\"query.collect\",\"table\":\"t\",\"select\":[\"id\"]}\n\nquery rowsort label-x\n{\"op\":\"query.collect\",\"table\":\"t\",\"select\":[\"id\"],\"filter\":{\"op\":\"ge\",\"column\":\"id\",\"value\":0}}\n",
    )
    .await;
    assert!(ok.is_empty(), "{ok:?}");
    let explain = failures_of(
        r#"explain
{"op":"query.explain","table":"t"}
----
scan t (<uuid>)
filter none
sort id ASC (primary key)
limit none
projection all
partitions 1; worker execution
"#,
    )
    .await;
    assert!(explain.is_empty(), "{explain:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sensitivity_wrong_expected_row_fails() {
    let failures = failures_of(
        "query rowsort\n{\"op\":\"query.collect\",\"table\":\"t\"}\n----\n1\tb\n2\tWRONG\n",
    )
    .await;
    assert_eq!(failures.len(), 1, "{failures:?}");
    let f = &failures[0];
    assert!(f.starts_with("planted.nlt:10: wrong result"), "{f}");
    assert!(f.contains("2\tWRONG") && f.contains("2\ta"), "{f}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sensitivity_row_order_under_nosort_fails() {
    // Primary-key order is 1, 2; the expected block lists 2, 1.
    let failures =
        failures_of("query nosort\n{\"op\":\"query.collect\",\"table\":\"t\"}\n----\n2\ta\n1\tb\n")
            .await;
    assert_eq!(failures.len(), 1, "{failures:?}");
    // The same rows pass under rowsort.
    let failures = failures_of(
        "query rowsort\n{\"op\":\"query.collect\",\"table\":\"t\"}\n----\n2\ta\n1\tb\n",
    )
    .await;
    assert!(failures.is_empty(), "{failures:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sensitivity_label_disagreement_fails() {
    let failures = failures_of(
        "query rowsort label-x\n{\"op\":\"query.collect\",\"table\":\"t\",\"select\":[\"id\"]}\n\nquery rowsort label-x\n{\"op\":\"query.collect\",\"table\":\"t\",\"select\":[\"id\"],\"filter\":{\"op\":\"gt\",\"column\":\"id\",\"value\":1}}\n",
    )
    .await;
    assert_eq!(failures.len(), 1, "{failures:?}");
    assert!(
        failures[0].contains("label-x disagrees with line 10"),
        "{}",
        failures[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sensitivity_wrong_hash_and_error_fail() {
    let failures = failures_of(
        "query rowsort hash\n{\"op\":\"query.collect\",\"table\":\"t\"}\n----\n4 values hashing to 00\n\nstatement error not_found\n{\"op\":\"query.count\",\"table\":\"t\"}\n\nstatement error validation_error\n{\"op\":\"query.count\",\"table\":\"nope\"}\n",
    )
    .await;
    assert_eq!(failures.len(), 3, "{failures:?}");
}

#[test]
fn canonical_floats() {
    for (v, s) in [
        (0.0, "0.0"),
        (-0.0, "0.0"),
        (2.0, "2.0"),
        (-0.5, "-0.5"),
        (1.5, "1.5"),
        (100.0, "100.0"),
        (0.001, "0.001"),
        (1e-7, "1e-7"),
        (1.5e-7, "1.5e-7"),
        (1e-6, "0.000001"),
        (1e20, "100000000000000000000.0"),
        (1e21, "1e21"),
        (123456.789, "123456.789"),
        (0.1 + 0.2, "0.30000000000000004"),
        (0.50000762939453125, "0.5000076293945312"),
        (f64::MAX, "1.7976931348623157e308"),
        (f64::MIN_POSITIVE, "2.2250738585072014e-308"),
    ] {
        assert_eq!(canonical_float(v), s, "{v:e}");
    }
}

#[test]
fn rendering_and_translation() {
    assert_eq!(render_string(""), "(empty)");
    assert_eq!(render_string("a\tb\\c\nd"), "a\\tb\\\\c\\nd");
    assert_eq!(render_string("\0x\x1f\x7f"), "\\x00x\\x1f\\x7f");
    assert_eq!(
        normalize_explain("scan p_t (11111111-1111-4111-8111-111111111111)", "p_"),
        "scan t (<uuid>)"
    );
    let mut tr = Translator::new("p_");
    tr.observe("table.create", &json!({"name": "p_t", "id": "abc"}));
    let (op, payload) = tr
        .translate(r#"{"op":"query.count","table":"t","limit":3}"#)
        .unwrap();
    assert_eq!(op, "query.count");
    assert_eq!(
        payload,
        json!({"table_id":"abc","query":{"select":null,"filter":null,"order_by":null,"limit":3}})
    );
    let (_, payload) = tr
        .translate(r#"{"op":"table.get","table":"missing","key":1}"#)
        .unwrap();
    assert_eq!(payload["table_id"], MISSING_TABLE_ID);
    let (_, payload) = tr
        .translate(r#"{"op":"table.describe","name":"t"}"#)
        .unwrap();
    assert_eq!(payload["name"], "p_t");
    let projection = json!({"query":{"select":["id"]}});
    assert!(
        tr.render("query.collect", &projection, &json!([{"id":1,"extra":2}]))
            .unwrap_err()
            .starts_with("wrong result columns")
    );
}

#[test]
fn parser_rejects_malformed_files() {
    for bad in [
        "statement ok\n{}\n",
        "# name: x\nquery nosort\n{}\n",
        "# name: x\nquery sideways\n{}\n----\n",
        "# name: x\nstatement ok\n{}\n----\n",
        "# name: x\nskipif\nstatement ok\n{}\n",
        "# name: x\nstatement ok\n\n",
        "# name: x\nquery nosort hash\n{}\n----\na\nb\n",
        "# name: x\nstatement ok\n{}\n\nrequire olap\n",
        "# name: x\nbogus\n",
    ] {
        assert!(parse(bad).is_err(), "accepted {bad:?}");
    }
    let file = parse("# name: x\nrequire olap\nskipif bug 12\nstatement ok\n{}\n").unwrap();
    assert_eq!(file.engines, ["olap"]);
    assert_eq!(file.records[0].skip.as_deref(), Some("bug 12"));
}

#[test]
fn bless_rewrites_only_expected_blocks() {
    let text = "# name: x\n\n# keep\nquery nosort\n{\"op\":\"query.count\"}\n----\nold\n\nstatement ok\n{}\n";
    let file = parse(text).unwrap();
    let actual = BTreeMap::from([(0, vec!["new".to_owned(), "two".to_owned()])]);
    assert_eq!(
        blessed(&file, &actual),
        "# name: x\n\n# keep\nquery nosort\n{\"op\":\"query.count\"}\n----\nnew\ntwo\n\nstatement ok\n{}\n"
    );
}
