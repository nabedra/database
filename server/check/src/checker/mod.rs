//! History checkers. Each takes the events of one history and returns a [`Verdict`].
//!
//! | Checker | Promise | Anomaly kinds |
//! | --- | --- | --- |
//! | [`linearizable::check_kv`] | per-key registers are linearizable | `non-linearizable` |
//! | [`txn::check`] | transactions are (strictly) serializable | `G0` `G1a` `G1b` `G1c` `G-single` `G2`, the `-realtime` variants, `incompatible-order`, `garbage-read`, `duplicate-elements` |
//! | [`outcome::check`] | a failed or `not_applied` write never takes effect | `failed-write-observed` |
//! | [`queue::check`] | log offsets, ordering, durability and redelivery | `duplicate-offset` `duplicate-event` `offset-gap` `out-of-order` `lost-publish` `redelivery-missing` |
//! | [`relaxed_kv::check`] | asynchronous Cache replica promise (CACHE-G05) | `invented-value` `resurrection` `stale-read` `lost-write` |
//!
//! A malformed history (for example a completion without an invoke) yields the kind
//! `malformed-history`, so it can never pass silently.

use serde::Serialize;

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
}

impl Verdict {
    pub fn new(checked_ops: usize, anomalies: Vec<Anomaly>) -> Self {
        Verdict {
            valid: anomalies.is_empty(),
            anomalies,
            checked_ops,
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
        for v in verdicts {
            checked = checked.max(v.checked_ops);
            anomalies.extend(v.anomalies);
        }
        Verdict::new(checked, anomalies)
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
