//! Jepsen EDN export, so Elle (or Knossos) can cross-check a history offline.
//!
//! Each event becomes one EDN map per line, the form `elle-cli` reads:
//!
//! ```text
//! {:index 0, :type :invoke, :process 3, :f :txn, :value [[:append "x" 1] [:r "y" nil]], :time 123}
//! ```
//!
//! `txn` micro-op names become keywords. A single-key op with a `key` gets Jepsen's
//! independent-key tuple `[key value]` as its value. The nemesis process becomes `:nemesis`.
//! Object keys become keywords when they are valid keyword names and strings otherwise.

use crate::history::{Event, EventType};
use serde_json::Value;
use std::fmt::Write;

pub fn export(events: &[Event]) -> String {
    let mut out = String::new();
    for ev in events {
        out.push_str(&event(ev));
        out.push('\n');
    }
    out
}

/// The whole history as one EDN vector.
pub fn export_vector(events: &[Event]) -> String {
    let mut out = String::from("[");
    for (i, ev) in events.iter().enumerate() {
        if i > 0 {
            out.push_str("\n ");
        }
        out.push_str(&event(ev));
    }
    out.push_str("]\n");
    out
}

pub fn event(ev: &Event) -> String {
    let mut s = String::from("{");
    let ty = match ev.kind {
        EventType::Invoke => "invoke",
        EventType::Ok => "ok",
        EventType::Fail => "fail",
        EventType::Info => "info",
    };
    let process = if ev.is_nemesis() {
        ":nemesis".to_string()
    } else {
        ev.process.to_string()
    };
    let f = if ev.is_nemesis() {
        ev.marker_name()
    } else {
        ev.f.clone()
    };
    let _ = write!(
        s,
        ":index {}, :type :{ty}, :process {process}, :f {}",
        ev.index,
        keyword(&f)
    );
    let value = if ev.f == "txn" {
        txn_value(&ev.value)
    } else if let Some(k) = &ev.key {
        format!("[{} {}]", string(k), value(&ev.value))
    } else {
        value(&ev.value)
    };
    let _ = write!(s, ", :value {value}");
    if let Some(t) = ev.time_ns {
        let _ = write!(s, ", :time {t}");
    }
    if let Some(n) = &ev.node {
        let _ = write!(s, ", :node {}", string(n));
    }
    if let Some(r) = &ev.request_id {
        let _ = write!(s, ", :request-id {}", string(r));
    }
    s.push('}');
    s
}

fn txn_value(v: &Value) -> String {
    let Some(mops) = v.as_array() else {
        return value(v);
    };
    let parts: Vec<String> = mops
        .iter()
        .map(|m| match m.as_array() {
            Some(m) if m.len() == 3 && m[0].is_string() => format!(
                "[{} {} {}]",
                keyword(m[0].as_str().unwrap_or_default()),
                value(&m[1]),
                value(&m[2])
            ),
            _ => value(m),
        })
        .collect();
    format!("[{}]", parts.join(" "))
}

fn valid_keyword(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || "*+!-_?<>=".contains(c))
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || "*+!-_?<>=.".contains(c))
}

fn keyword(s: &str) -> String {
    if valid_keyword(s) {
        format!(":{s}")
    } else {
        string(s)
    }
}

pub fn string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

pub fn value(v: &Value) -> String {
    match v {
        Value::Null => "nil".into(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => string(s),
        Value::Array(a) => format!("[{}]", a.iter().map(value).collect::<Vec<_>>().join(" ")),
        Value::Object(m) => format!(
            "{{{}}}",
            m.iter()
                .map(|(k, v)| format!("{} {}", keyword(k), value(v)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}
