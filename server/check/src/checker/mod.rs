//! History checkers. Each takes the events of one history and returns a [`Verdict`].
//!
//! | Checker | Promise | Anomaly kinds |
//! | --- | --- | --- |
//! | [`linearizable::check_kv`] | per-key registers are linearizable | `non-linearizable` |
//! | [`txn::check`] | transactions are (strictly) serializable | `G0` `G1a` `G1b` `G1c` `G-single` `G2`, the `-realtime` variants, `incompatible-order`, `garbage-read`, `duplicate-elements` |
//! | [`outcome::check`] | a failed or `not_applied` write never takes effect | `failed-write-observed` |
//! | [`direct::check`] | direct-path registers and single-range batches are linearizable, each request identity commits at most once and keeps its first definite result, and failed attempts have no effects | `non-linearizable` `failed-write-observed` `dedup-miss` `unstable-result` `request-id-reuse-undetected` `request-id-reuse-misdetected` |
//! | [`direct::check_with`] | an opt-in rule about refusals, off by default ([`direct::request_rules`]) | `not-applied-took-effect` |
//! | [`queue::check`] | log offsets, ordering, durability and redelivery | `duplicate-offset` `duplicate-event` `offset-gap` `out-of-order` `lost-publish` `redelivery-missing` |
//! | [`relaxed_kv::check`] | asynchronous Cache replica promise (CACHE-G05) | `invented-value` `resurrection` `stale-read` `lost-write` |
//!
//! A malformed history (for example a completion without an invoke) yields the kind
//! `malformed-history`, so it can never pass silently.

use serde::Serialize;

pub mod direct;
pub mod edn;
pub mod linearizable;
pub mod outcome;
pub mod queue;
pub mod relaxed_kv;
pub mod txn;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Anomaly {
    pub kind: String,
    pub detail: String,
    /// `index` fields of the events involved.
    pub events: Vec<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Verdict {
    pub valid: bool,
    pub anomalies: Vec<Anomaly>,
    pub checked_ops: usize,
    /// Refused inserts that [`direct::check`] could not classify and therefore did not take as a
    /// result. Not an anomaly, but with many of them the retry rule checks little, so a caller has
    /// to print the count. See [`direct::results`].
    #[serde(skip_serializing_if = "none")]
    pub unclassified_conflicts: usize,
}

fn none(count: &usize) -> bool {
    *count == 0
}

impl Verdict {
    pub fn new(checked_ops: usize, anomalies: Vec<Anomaly>) -> Self {
        Verdict {
            valid: anomalies.is_empty(),
            anomalies,
            checked_ops,
            unclassified_conflicts: 0,
        }
    }

    pub fn malformed(msg: String) -> Self {
        Verdict::new(
            0,
            vec![Anomaly {
                kind: "malformed-history".into(),
                detail: msg,
                events: vec![],
            }],
        )
    }

    /// Distinct anomaly kinds, sorted.
    pub fn kinds(&self) -> Vec<String> {
        let mut k: Vec<String> = self.anomalies.iter().map(|a| a.kind.clone()).collect();
        k.sort();
        k.dedup();
        k
    }

    pub fn has(&self, kind: &str) -> bool {
        self.anomalies.iter().any(|a| a.kind == kind)
    }

    /// Combines verdicts of several checkers over the same history.
    pub fn merge(verdicts: impl IntoIterator<Item = Verdict>) -> Verdict {
        let mut anomalies = Vec::new();
        let mut checked = 0;
        let mut unclassified = 0;
        for v in verdicts {
            checked = checked.max(v.checked_ops);
            unclassified = unclassified.max(v.unclassified_conflicts);
            anomalies.extend(v.anomalies);
        }
        Verdict {
            unclassified_conflicts: unclassified,
            ..Verdict::new(checked, anomalies)
        }
    }
}

impl Anomaly {
    pub fn new(kind: &str, detail: String, events: Vec<usize>) -> Self {
        Anomaly {
            kind: kind.to_string(),
            detail,
            events,
        }
    }
}

/// A stable text form of a JSON value, for hashing and interning (`serde_json::Value` has no
/// `Hash`).
pub(crate) fn canon(v: &serde_json::Value) -> String {
    v.to_string()
}
