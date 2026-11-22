//! Configuration randomizer. (Skeleton.)

use std::collections::BTreeMap;

use crate::r#gen::Gen;

#[derive(Clone, Debug)]
pub enum Knob {
    Bool(&'static str),
    Choice(&'static str, &'static [&'static str]),
    IntRange(&'static str, i64, i64),
}

pub fn sample(g: &mut Gen, knobs: &[Knob]) -> BTreeMap<String, serde_json::Value> {
    let mut out = BTreeMap::new();
    for k in knobs {
        let (name, v) = match k {
            Knob::Bool(n) => (*n, serde_json::Value::Bool(g.bool())),
            Knob::Choice(n, opts) => (*n, serde_json::Value::String(g.pick(opts).to_string())),
            Knob::IntRange(n, lo, hi) => (*n, serde_json::Value::from(g.range_i64(*lo, *hi))),
        };
        out.insert(name.to_string(), v);
    }
    out
}
