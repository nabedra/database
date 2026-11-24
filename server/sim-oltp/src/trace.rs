//! The `nabedra.trace/1` ndjson sink for TLA+ trace validation.
//!
//! Active only when `NABEDRA_TRACE_DIR` is set (this crate builds only with `sim`). One
//! file per run and spec, `$NABEDRA_TRACE_DIR/<Spec>-<seed>.ndjson`: a header line, then
//! one line per probe and per harness fault, in the order they took effect. The format is
//! the one in `wave-p8/design-review.md` ("Trace format"), which
//! `scripts/testing/tla_trace.py lint` checks.
//!
//! Action names come from [`ACTIONS`]. The design-review wave's `sim-mapping/oltp*.md`
//! files did not exist when this sink was written, so every probe keeps its own name and
//! its fields, except payload digests, go into `vars` under their probe field names.
//! Update the table when the mapping lands. A replayed seed rewrites the same file with the same bytes.

use std::{
    io,
    path::{Path, PathBuf},
};

use serde_json::{Value, json};

use crate::probes::ProbeEvent;

/// The spec the OLTP direct-path traces belong to (`specs/oltp/OltpDirect.tla`).
pub const SPEC: &str = "OltpDirect";

/// Probe and fault names to spec action names. Unlisted names map to themselves.
pub const ACTIONS: &[(&str, &str)] = &[];

const FORMAT: &str = "nabedra.trace/1";
const UNKNOWN_SHA: &str = "0000000000000000000000000000000000000000";

pub fn action(name: &str) -> &str {
    ACTIONS
        .iter()
        .find(|(from, _)| *from == name)
        .map_or(name, |(_, to)| to)
}

pub struct TraceFile {
    path: PathBuf,
    lines: Vec<String>,
    seq: u64,
    last_t: u64,
}

impl TraceFile {
    /// A trace for this run when `NABEDRA_TRACE_DIR` is set.
    pub fn from_env(seed: u64, workload: &str) -> Option<Self> {
        let dir = std::env::var_os("NABEDRA_TRACE_DIR").filter(|dir| !dir.is_empty())?;
        Some(Self::new(Path::new(&dir), seed, workload, &sha()))
    }

    pub fn new(dir: &Path, seed: u64, workload: &str, sha: &str) -> Self {
        let header = json!({
            "trace": FORMAT,
            "spec": SPEC,
            "seed": seed,
            "sha": sha,
            "workload": workload,
            "params": {},
        });
        Self {
            path: dir.join(format!("{SPEC}-{seed}.ndjson")),
            lines: vec![header.to_string()],
            seq: 0,
            last_t: 0,
        }
    }

    /// A probe step. `vars` holds the probe's fields; absent options are left out.
    pub fn probe(&mut self, event: &ProbeEvent) {
        let mut vars = crate::probes::fields(&event.probe);
        vars.insert("incarnation".into(), json!(event.incarnation));
        if !event.live {
            vars.insert("live".into(), json!(false));
        }
        self.step(
            event.t_ns,
            event.node,
            event.probe.name(),
            Value::Object(vars),
        );
    }

    /// A harness fault step, such as `Crash`, `Restart` or `Wipe`.
    pub fn fault(&mut self, t_ns: u64, node: u64, name: &str) {
        self.step(t_ns, Some(node), name, json!({}));
    }

    fn step(&mut self, t_ns: u64, node: Option<u64>, name: &str, vars: Value) {
        // Probes and faults share the World clock; never let a line go back in time.
        self.last_t = self.last_t.max(t_ns);
        let line = json!({
            "seq": self.seq,
            "t": self.last_t,
            "node": node.map_or_else(|| "sim".to_owned(), |node| format!("n{node}")),
            "action": action(name),
            "vars": without_nulls(vars),
        });
        self.seq += 1;
        self.lines.push(line.to_string());
    }

    pub fn write(self) -> io::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut text = self.lines.join("\n");
        text.push('\n');
        std::fs::write(&self.path, text)
    }

    pub fn lines(&self) -> &[String] {
        &self.lines
    }
}

/// TLA+ has no null: drop null fields, and spell a null array element `"none"`.
fn without_nulls(value: Value) -> Value {
    match value {
        Value::Object(fields) => Value::Object(
            fields
                .into_iter()
                .filter(|(_, value)| !value.is_null())
                .map(|(key, value)| (key, without_nulls(value)))
                .collect(),
        ),
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .map(|item| {
                    if item.is_null() {
                        Value::String("none".into())
                    } else {
                        without_nulls(item)
                    }
                })
                .collect(),
        ),
        other => other,
    }
}

/// The commit under test: `NABEDRA_TRACE_SHA`, then `GITHUB_SHA`, then `git rev-parse`.
fn sha() -> String {
    let valid = |sha: &str| sha.len() == 40 && sha.bytes().all(|b| b.is_ascii_hexdigit());
    for name in ["NABEDRA_TRACE_SHA", "GITHUB_SHA"] {
        if let Ok(sha) = std::env::var(name)
            && valid(&sha)
        {
            return sha.to_ascii_lowercase();
        }
    }
    std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .map(|sha| sha.trim().to_owned())
        .filter(|sha| valid(sha))
        .unwrap_or_else(|| UNKNOWN_SHA.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::probes::{Probe, RouteRecord, Span};
    use nabedra_protocol::protocol::Key;

    #[test]
    fn lines_follow_the_trace_format() {
        let dir = std::env::temp_dir().join(format!("nabedra-trace-test-{}", std::process::id()));
        let mut trace = TraceFile::new(&dir, 7, "smoke", UNKNOWN_SHA);
        trace.probe(&ProbeEvent {
            seq: 0,
            t_ns: 5,
            node: Some(2),
            incarnation: 1,
            live: true,
            probe: Probe::RouteEpochPublished {
                table: uuid::Uuid::nil(),
                commit_ts: 1,
                record: RouteRecord::Page {
                    page_id: uuid::Uuid::nil(),
                    next_page: None,
                    spans: vec![Span {
                        start: None,
                        end: Some(Key::Int64(4)),
                    }],
                },
            },
        });
        trace.fault(3, 2, "Crash");
        let lines: Vec<Value> = trace
            .lines()
            .iter()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(lines[0]["trace"], FORMAT);
        assert_eq!(lines[0]["spec"], SPEC);
        assert_eq!(lines[0]["seed"], 7);
        assert_eq!(
            lines[1],
            json!({"seq":0,"t":5,"node":"n2","action":"RouteEpochPublished","vars":{
                "table":"00000000-0000-0000-0000-000000000000","commit_ts":1,
                "record":{"page":{"page_id":"00000000-0000-0000-0000-000000000000",
                "spans":[{"end":4}]}},"incarnation":1}})
        );
        // Time never decreases, and seq counts every step.
        assert_eq!(lines[2]["seq"], 1);
        assert_eq!(lines[2]["t"], 5);
        assert_eq!(lines[2]["action"], "Crash");
        trace.write().unwrap();
        let written = std::fs::read_to_string(dir.join("OltpDirect-7.ndjson")).unwrap();
        assert_eq!(written.lines().count(), 3);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
