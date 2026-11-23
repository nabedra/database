//! Client history in the `nabedra-check` JSON-lines format, and a local queue checker.
//!
//! The format is one object per line:
//! `{"index":0,"type":"invoke","process":0,"f":"publish","key":"p0","value":...,
//!   "request_id":"r-1","node":"n1","time_ns":123}`, where `type` is `invoke`, `ok`,
//! `fail` or `info` (unknown outcome). The checks here are the offset, loss and duplicate
//! assertions of `jepsen.tests.kafka`, applied to one partition.

use std::collections::{BTreeMap, BTreeSet};
#[cfg(test)]
use std::io::Write;
#[cfg(test)]
use std::path::Path;

use serde::{Serialize, Serializer};
use serde_json::Value;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Event {
    pub index: usize,
    #[serde(rename = "type")]
    pub kind: &'static str,
    #[serde(serialize_with = "serialize_process")]
    pub process: u64,
    pub f: &'static str,
    #[serde(skip_serializing_if = "str::is_empty")]
    pub key: &'static str,
    pub value: Value,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub request_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub node: String,
    pub time_ns: u64,
}

fn serialize_process<S: Serializer>(process: &u64, serializer: S) -> Result<S::Ok, S::Error> {
    if *process == u64::MAX {
        serializer.serialize_str("nemesis")
    } else {
        serializer.serialize_u64(*process)
    }
}

#[derive(Default)]
pub struct History {
    pub events: Vec<Event>,
}

impl History {
    #[allow(clippy::too_many_arguments)]
    pub fn push(
        &mut self,
        kind: &'static str,
        process: u64,
        f: &'static str,
        value: Value,
        request_id: &str,
        node: u64,
        time_ms: u64,
    ) {
        let index = self.events.len();
        self.events.push(Event {
            index,
            kind,
            process,
            f,
            key: super::cluster::GROUP,
            value,
            request_id: request_id.into(),
            node: format!("n{node}"),
            time_ns: time_ms * 1_000_000,
        });
    }

    #[cfg(test)]
    pub fn write_jsonl(&self, path: &Path) -> std::io::Result<()> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        self.write(std::io::BufWriter::new(std::fs::File::create(path)?))
    }

    #[cfg(test)]
    fn write(&self, mut output: impl Write) -> std::io::Result<()> {
        for event in &self.events {
            serde_json::to_writer(&mut output, event)?;
            output.write_all(b"\n")?;
        }
        output.flush()
    }

    pub fn heal(&mut self, time_ms: u64) {
        self.events.push(Event {
            index: self.events.len(),
            kind: "ok",
            process: u64::MAX,
            f: "heal",
            key: "",
            value: Value::Null,
            request_id: String::new(),
            node: String::new(),
            time_ns: time_ms * 1_000_000,
        });
    }

    #[cfg(test)]
    pub fn check_queue(&self, bound: std::time::Duration) -> std::io::Result<Vec<String>> {
        use nabedra_check::{checker::queue, history::read_jsonl};

        let mut jsonl = Vec::new();
        self.write(&mut jsonl)?;
        let events = read_jsonl(jsonl.as_slice())?;
        let verdict = queue::check_with(
            &events,
            &queue::Options {
                redelivery_bound_ns: Some(bound.as_nanos().try_into().expect("bound fits u64")),
                ..queue::Options::default()
            },
        );
        Ok(verdict
            .anomalies
            .into_iter()
            .map(|a| {
                format!(
                    "queue checker {}: {} (events {:?})",
                    a.kind, a.detail, a.events
                )
            })
            .collect())
    }
}

/// The partition as every replica holds it at the end: offset to event ID.
pub type FinalLog = BTreeMap<u64, Uuid>;

fn uuid_of(value: &Value) -> Option<Uuid> {
    value.as_str()?.parse().ok()
}

/// Checks the history against the final log. Returns one line per anomaly.
pub fn check(history: &History, log: &FinalLog, final_checkpoint: u64) -> Vec<String> {
    let mut anomalies = Vec::new();
    let mut offset_of: BTreeMap<Uuid, u64> = BTreeMap::new();
    let mut event_at: BTreeMap<u64, Uuid> = BTreeMap::new();
    let mut invoked: BTreeSet<Uuid> = BTreeSet::new();
    let mut batches = BTreeMap::new();
    let retained: BTreeSet<_> = log.values().copied().collect();
    let mut delivered: BTreeSet<u64> = BTreeSet::new();
    let mut claim = |id: Uuid, offset: u64, source: &str, index: usize, out: &mut Vec<String>| {
        if let Some(previous) = offset_of
            .insert(id, offset)
            .filter(|prior| *prior != offset)
        {
            out.push(format!(
                "EVT-G03 offset-reassigned: event {id} at offset {previous} and {offset} ({source}, history index {index})"
            ));
        }
        if let Some(other) = event_at.insert(offset, id).filter(|other| *other != id) {
            out.push(format!(
                "EVT-G02 duplicate-offset: offset {offset} holds {other} and {id} ({source}, history index {index})"
            ));
        }
    };

    for event in &history.events {
        match (event.f, event.kind) {
            ("publish", "invoke") => {
                let ids: Vec<_> = event
                    .value
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(uuid_of)
                    .collect();
                invoked.extend(ids.iter().copied());
                // This workload retries whole batches, so even an unknown outcome can
                // leave only the entire batch or none of it in the final log.
                let present = ids.iter().filter(|id| retained.contains(id)).count();
                if present != 0 && present != ids.len() {
                    anomalies.push(format!(
                        "EVT-G04 partial-batch: {present} of {} invoked events survived (history index {})",
                        ids.len(), event.index,
                    ));
                }
                batches.insert((event.process, event.request_id.as_str()), ids);
            }
            ("publish", "ok") => {
                let items = |field| event.value[field].as_array().into_iter().flatten();
                let ids: Vec<_> = items("event_ids").filter_map(uuid_of).collect();
                let offsets: Vec<_> = items("offsets").filter_map(Value::as_u64).collect();
                let pairs: Vec<_> = ids.iter().copied().zip(offsets.iter().copied()).collect();
                let request = (event.process, event.request_id.as_str());
                if ids.len() != offsets.len() || batches.get(&request) != Some(&ids) {
                    anomalies.push(format!(
                        "EVT-G04 incomplete-receipt: the receipt does not match the invoked event IDs (history index {})",
                        event.index,
                    ));
                }
                for (id, offset) in &pairs {
                    claim(*id, *offset, "publish ok", event.index, &mut anomalies);
                    if log.get(offset) != Some(id) {
                        anomalies.push(format!(
                            "EVT-G01 lost-write: acknowledged event {id} at offset {offset} is not in the final log (history index {})",
                            event.index
                        ));
                    }
                }
                let fresh = offsets.windows(2).all(|pair| pair[1] == pair[0] + 1);
                if !fresh && offsets.iter().collect::<BTreeSet<_>>().len() == offsets.len() {
                    // A batch of new events gets consecutive offsets; a retry of an older
                    // batch returns that batch's offsets, which are consecutive too.
                    anomalies.push(format!(
                        "EVT-G04 split-batch: batch offsets {offsets:?} are not consecutive (history index {})",
                        event.index
                    ));
                }
            }
            ("poll", "ok") => {
                let after = event
                    .value
                    .get("after")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                let mut expected = after + 1;
                for pair in event.value["events"].as_array().into_iter().flatten() {
                    let (Some(offset), Some(id)) = (
                        pair.get(0).and_then(Value::as_u64),
                        pair.get(1).and_then(uuid_of),
                    ) else {
                        continue;
                    };
                    if offset != expected {
                        anomalies.push(format!(
                            "EVT-G02 poll-order: poll after {after} delivered offset {offset}, expected {expected} (history index {})",
                            event.index
                        ));
                    }
                    expected = offset + 1;
                    delivered.insert(offset);
                    claim(id, offset, "poll", event.index, &mut anomalies);
                    if log.get(&offset) != Some(&id) {
                        anomalies.push(format!(
                            "EVT-G02 phantom-read: poll delivered {id} at offset {offset}, the final log holds {:?} (history index {})",
                            log.get(&offset),
                            event.index
                        ));
                    }
                }
            }
            _ => {}
        }
    }

    // The final log: offsets 1..=tail with no gaps, only invoked events, no event twice.
    let mut seen = BTreeSet::new();
    for (expected, (offset, id)) in (1u64..).zip(log) {
        if *offset != expected {
            anomalies.push(format!(
                "EVT-G02 gap: the final log has offset {offset} where {expected} was expected"
            ));
            break;
        }
        if !invoked.contains(id) {
            anomalies.push(format!(
                "EVT-G02 invented-event: offset {offset} holds {id}, which no client published"
            ));
        }
        if !seen.insert(*id) {
            anomalies.push(format!(
                "EVT-G03 duplicate-event: event {id} appears twice in the final log"
            ));
        }
    }
    for (event_id, offset) in &offset_of {
        if log.get(offset).is_some_and(|held| held != event_id)
            && !log.values().any(|id| id == event_id)
        {
            anomalies.push(format!(
                "EVT-G01 lost-write: event {event_id} was observed at offset {offset} but is missing from the final log"
            ));
        }
    }
    // At-least-once delivery: after the final drain every event at or below the
    // checkpoint was delivered by some poll.
    for offset in log.keys().filter(|offset| **offset <= final_checkpoint) {
        if !delivered.contains(offset) {
            anomalies.push(format!(
                "EVT-G05 not-delivered: offset {offset} is below the final checkpoint {final_checkpoint} but no poll delivered it"
            ));
        }
    }
    let tail = log.keys().next_back().copied().unwrap_or(0);
    if final_checkpoint != tail {
        anomalies.push(format!(
            "EVT-G05 drain-incomplete: the final checkpoint is {final_checkpoint}, the tail {tail}"
        ));
    }
    anomalies.sort();
    anomalies.dedup();
    anomalies
}
