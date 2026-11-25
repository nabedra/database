//! The leader of a Raft group as the other voters see it: a leader's AppendEntries carries its
//! committed vote, the term and node a quorum elected. Nothing a worker reports about itself
//! is used.

use std::collections::BTreeSet;

use serde_json::Value;
use uuid::Uuid;

/// The frame head, then the request prologue: magic, cluster, group, group digest, purpose.
const PROLOGUE: usize = 13;
const GROUP: usize = PROLOGUE + 24;
const PURPOSE: usize = PROLOGUE + 72;
/// `Purpose::Raft` and `Purpose::RaftProgress`.
const RAFT: [u8; 2] = [1, 17];
/// The length of the Raft header ends the prologue, and the header follows it.
const LENGTH: usize = PROLOGUE + 169;
const HEADER: usize = PROLOGUE + 173;

/// A leader that a quorum elected, first seen on the wire at `at` (virtual ms).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Election {
    pub at: u64,
    pub group: Uuid,
    pub term: u64,
    /// The worker that sent the append.
    pub node: u64,
}

#[derive(Default)]
pub(super) struct Watch {
    groups: BTreeSet<Uuid>,
    elections: Vec<Election>,
}

impl Watch {
    pub(super) fn add(&mut self, group: Uuid) {
        self.groups.insert(group);
    }

    pub(super) fn elections(&self) -> &[Election] {
        &self.elections
    }

    /// Reads one request frame that `from` sent.
    pub(super) fn request(&mut self, at: u64, from: Option<u64>, message: &[u8]) {
        let Some(node) = from else { return };
        let Some((group, term)) = append(message) else {
            return;
        };
        if !self.groups.contains(&group) {
            return;
        }
        let newer = self
            .elections
            .iter()
            .all(|seen| seen.group != group || seen.term < term);
        if newer {
            self.elections.push(Election {
                at,
                group,
                term,
                node,
            });
        }
    }
}

/// The group and the committed vote's term of an AppendEntries request.
fn append(message: &[u8]) -> Option<(Uuid, u64)> {
    let purpose = *message.get(PURPOSE)?;
    if !RAFT.contains(&purpose) {
        return None;
    }
    let group = Uuid::from_slice(message.get(GROUP..GROUP + 16)?).ok()?;
    let length = message.get(LENGTH..HEADER)?.try_into().ok()?;
    let length = u32::from_be_bytes(length) as usize;
    let header: Value = serde_json::from_slice(message.get(HEADER..HEADER + length)?).ok()?;
    if header["method"] != "append" || header["vote"]["committed"] != true {
        return None;
    }
    Some((group, header["vote"]["leader_id"]["term"].as_u64()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(group: Uuid, purpose: u8, header: &Value) -> Vec<u8> {
        let mut out = vec![0; HEADER];
        out[GROUP..GROUP + 16].copy_from_slice(group.as_bytes());
        out[PURPOSE] = purpose;
        let header = serde_json::to_vec(header).unwrap();
        out[LENGTH..HEADER].copy_from_slice(&(header.len() as u32).to_be_bytes());
        out.extend(header);
        out.extend(b"section lengths and sections follow");
        out
    }

    fn header(method: &str, term: u64, committed: bool) -> Value {
        serde_json::json!({
            "from_node": 1, "to_node": 2, "method": method,
            "vote": {"leader_id": {"term": term, "node_id": 1}, "committed": committed},
        })
    }

    /// Review 115, M6: a counter that grew is not leadership. Only an append with a committed
    /// vote of a newer term is.
    #[test]
    fn only_an_append_with_a_committed_vote_elects() {
        let group = Uuid::from_u128(7);
        let mut watch = Watch::default();
        watch.add(group);
        watch.request(5, Some(1), &frame(group, 1, &header("vote", 1, false)));
        watch.request(6, Some(1), &frame(group, 1, &header("append", 1, false)));
        watch.request(7, Some(1), &frame(group, 13, &header("append", 1, true)));
        watch.request(
            8,
            Some(1),
            &frame(Uuid::nil(), 1, &header("append", 1, true)),
        );
        assert!(watch.elections().is_empty());
        watch.request(9, Some(1), &frame(group, 17, &header("append", 1, true)));
        watch.request(10, Some(1), &frame(group, 1, &header("append", 1, true)));
        watch.request(11, Some(3), &frame(group, 1, &header("append", 2, true)));
        // A deposed leader still sends appends of its old term.
        watch.request(12, Some(1), &frame(group, 1, &header("append", 1, true)));
        let seen: Vec<_> = watch
            .elections()
            .iter()
            .map(|e| (e.at, e.term, e.node))
            .collect();
        assert_eq!(seen, [(9, 1, 1), (11, 2, 3)]);
    }
}
