//! Configuration randomizer, in the style of ClickHouse's `SettingsRandomizer`.
//!
//! A register lists configuration knobs with their ranges. A run samples every knob from its seed,
//! so the sampled configuration replays with the seed, and records the result in the evidence
//! record's `mode` field. Values shrink like any other draw: `false`, the first choice, `lo`.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::r#gen::Gen;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Knob {
    /// `true` or `false`.
    Bool(&'static str),
    /// One of the listed strings. The first is the default and the shrink target.
    Choice(&'static str, &'static [&'static str]),
    /// An integer in `[lo, hi]`, inclusive.
    IntRange(&'static str, i64, i64),
}

impl Knob {
    pub fn name(&self) -> &'static str {
        match self {
            Knob::Bool(n) | Knob::Choice(n, _) | Knob::IntRange(n, _, _) => n,
        }
    }
}

/// Draws one value per knob, in the order given. Panics on a duplicate knob name, an empty
/// choice list or an empty range, since each is a mistake in the knob table.
pub fn sample(g: &mut Gen, knobs: &[Knob]) -> BTreeMap<String, Value> {
    let mut out = BTreeMap::new();
    for k in knobs {
        let v = match k {
            Knob::Bool(_) => Value::Bool(g.bool()),
            Knob::Choice(n, opts) => {
                assert!(!opts.is_empty(), "knob {n} has no choices");
                Value::String(g.pick(opts).to_string())
            }
            Knob::IntRange(_, lo, hi) => Value::from(g.range_i64(*lo, *hi)),
        };
        let prev = out.insert(k.name().to_string(), v);
        assert!(prev.is_none(), "duplicate knob {}", k.name());
    }
    out
}

/// One line for logs and failure messages, such as `direct_oltp=true workers=3`.
pub fn describe(sample: &BTreeMap<String, Value>) -> String {
    let parts: Vec<String> = sample
        .iter()
        .map(|(k, v)| match v {
            Value::String(s) => format!("{k}={s}"),
            other => format!("{k}={other}"),
        })
        .collect();
    parts.join(" ")
}
