//! Membership rules (design 3.6 and 3.7, `membership`). Each rule is a pure state machine over
//! observations, so a fixture can feed it legal and illegal sequences. The workload feeds it
//! public observations on every seed. The forms that need the protocol probes of #116 are
//! `not_run` until they exist (`PROBE_FORMS`).
//!
//! Every rule quotes the text it enforces. The design, the plans, the contracts and the
//! registers are the sources. What rests on `docs/oltp-recovery.md` alone reports under a kind of
//! its own ([`GUIDANCE_ONLY`]): a finding, and no text that a rule can be counted on.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use serde_json::Value;
use uuid::Uuid;

/// The kinds that rest on operator guidance (`docs/oltp-recovery.md`) alone. No plan, contract,
/// register or design text carries them, so they report findings and detect nothing for a count.
pub(crate) const GUIDANCE_ONLY: [(&str, &str); 3] = [
    (
        OPERATOR_GUIDANCE,
        "retries, conflicts and abandonment of operator requests rest on docs/oltp-recovery.md alone",
    ),
    (
        "RemovedNeverRejoins",
        "that a replaced worker stays out rests on docs/oltp-recovery.md alone",
    ),
    (
        "ReplacementRestart",
        "step 7 of docs/oltp-recovery.md is its only text",
    ),
];

/// What an operator's retry, conflict or abandonment must answer. `docs/oltp-recovery.md`: "An
/// identical retry returns the same record", "A prepared intent authorizes only its exact
/// candidate", "Abandonment applies only before mount commits" and "A retry after interruption
/// uses the same planned removed and replacement replicas". OLTP-G03 does not cover these IDs:
/// its identities execute "cluster-wide on the queued path and per table and key on the direct
/// path", and an operator's request is on neither.
pub(crate) const OPERATOR_GUIDANCE: &str = "OperatorGuidance";

/// The parts of the rules that public observations cannot reach, and why.
pub(crate) const NOT_RUN_FORMS: [(&str, &str); 7] = [
    (
        "WipedNeverVotes over VoteSaved",
        "this rule reads no probe; the swept form judges acknowledged writes, and MonotonicVote judges each saved vote",
    ),
    (
        "MonotonicVote across a stale disk",
        "the stale disk is an opt-in schedule",
    ),
    (
        "CapabilityBeforeUse over public answers",
        "no worker serves its stats before every voter runs, so no answer can show an early use (counted per seed as \"early capability answers\")",
    ),
    ("CapabilityBeforeUse over Applied", "needs an advert probe"),
    (
        "CapabilityBeforeUse with a build that lacks a feature",
        "ControlMachine::known_features_for_test is cfg(test) product code",
    ),
    (
        "RemovedNeverRejoins over the removed worker's process",
        "its old file is refused for a member too, and the file a member would be accepted with needs the preparation that the swept form judges",
    ),
    (
        "R03 over a request that outlives every holder that accepted it",
        "the script settles every receipt before the next worker stops",
    ),
];

/// A replica's storage across process restarts. It changes when the volume that holds the
/// replica's vote and log is lost.
pub(crate) type Generation = u64;

/// `WipedNeverVotes`. Design 3.7: "votes under a node identity come from the storage
/// generation that first voted under it." OLTP-G08: "Membership changes never lose a required
/// voter, a queue copy, or a committed entry." O06: "Reject corrupt/foreign replicas."
///
/// An acknowledged write needs the durable vote of a majority of its group's voters. A replica
/// identity whose storage was lost has none of its earlier votes and entries, so a majority
/// that counts it is not one.
#[derive(Default)]
pub(crate) struct WipedNeverVotes {
    first: BTreeMap<(Uuid, u64), Generation>,
}

impl WipedNeverVotes {
    /// A vote or a durable append under `(group, replica)` from storage `generation`. The probe
    /// form calls this for every `VoteSaved` and `Appended`.
    pub(crate) fn voted(
        &mut self,
        group: Uuid,
        replica: u64,
        generation: Generation,
    ) -> Result<(), String> {
        let first = *self.first.entry((group, replica)).or_insert(generation);
        if first == generation {
            return Ok(());
        }
        Err(format!(
            "replica {replica} of group {group} voted from storage generation {generation}, and generation {first} had voted under that identity"
        ))
    }

    /// A write was proposed and acknowledged while exactly `reachable` of the group's voters
    /// could take part. Every voter of the group is listed, with the storage generation it runs
    /// on, or `None` while it cannot take part.
    pub(crate) fn committed(
        &mut self,
        group: Uuid,
        voters: &[(u64, Option<Generation>)],
    ) -> Result<(), String> {
        let majority = voters.len() / 2 + 1;
        let up: Vec<(u64, Generation)> = voters
            .iter()
            .filter_map(|(replica, generation)| Some((*replica, (*generation)?)))
            .collect();
        let retained = up
            .iter()
            .filter(|(replica, generation)| {
                self.first
                    .get(&(group, *replica))
                    .is_none_or(|first| first == generation)
            })
            .count();
        if retained >= majority {
            return Ok(());
        }
        let wiped: Vec<u64> = up
            .iter()
            .filter(|(replica, generation)| {
                self.first
                    .get(&(group, *replica))
                    .is_some_and(|first| first != generation)
            })
            .map(|(replica, _)| *replica)
            .collect();
        Err(format!(
            "group {group} acknowledged a write with {retained} of {} voters on their own storage; replicas {wiped:?} run on storage that never voted under their identity",
            voters.len()
        ))
    }
}

/// How one attempt of a write in a window with a frozen voter was answered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum WindowAnswer {
    Acknowledged,
    /// The write was sent and no quorum answered for it: a timeout, an unknown outcome or
    /// unavailable storage.
    NoQuorum,
    /// Refused for a reason that says nothing about the quorum, such as a full executor.
    Other(String),
}

/// What a window with one voter frozen showed about a write beside a wiped voter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Window {
    /// `WipedNeverVotes::committed` judges it.
    Acknowledged,
    /// The write needed the wiped voter, and it was not acknowledged.
    Unacknowledged,
    /// The window showed nothing.
    NotJudged(String),
}

/// Judge one write of a window from its attempts. `control` are the attempts of the same write
/// in the same window before the wipe, where two retained voters could take part.
///
/// A write that stays unanswered shows that the wiped voter gave no vote only if the same write
/// was acknowledged with that voter's storage in place, if the wiped process ran for the whole
/// window, and if every attempt reached the quorum.
pub(crate) fn window(control: &[WindowAnswer], wiped: &[WindowAnswer], wiped_ran: bool) -> Window {
    if wiped.contains(&WindowAnswer::Acknowledged) {
        return Window::Acknowledged;
    }
    let reason = if !wiped_ran {
        "the wiped process did not run for the whole window".into()
    } else if !control.contains(&WindowAnswer::Acknowledged) {
        format!(
            "the control write was not acknowledged: {:?}",
            control.last()
        )
    } else if wiped.is_empty() {
        "no write was attempted".into()
    } else if let Some(other) = wiped
        .iter()
        .find(|answer| matches!(answer, WindowAnswer::Other(_)))
    {
        format!("an attempt did not reach the quorum: {other:?}")
    } else {
        return Window::Unacknowledged;
    };
    Window::NotJudged(reason)
}

/// What the prepared restart of a removed worker shows, from the survivors' answers. Only the
/// refusal of Control's own state machine judges: it came after the checks that also refuse a
/// member whose certificate pin a survivor lacks (`ReplacementRestart`).
///
/// Returns whether the answers judged anything. `docs/oltp-recovery.md`, replacement, step 6:
/// "`mounted` means Control checked the signed mount proof, revoked the lost worker ID and
/// certificate".
pub(crate) fn removed_stays_out(answers: &[Prepared]) -> Result<bool, String> {
    if let Some(Prepared::Accepted(record)) = answers
        .iter()
        .find(|answer| matches!(answer, Prepared::Accepted(_)))
    {
        return Err(format!("its restart is prepared: {record}"));
    }
    Ok(answers.contains(&Prepared::Revoked))
}

/// One answer to a restart preparation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Prepared {
    Accepted(String),
    /// Control refused the worker as unauthorized.
    Revoked,
    /// Refused for another reason, or unknown.
    Other,
}

/// One named membership change: the request that asked for it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NamedChange {
    pub group: Uuid,
    pub remove: u64,
    pub add: u64,
}

/// What one answer to a change said.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Certified {
    /// The change is complete: `old` became `next`.
    Retired {
        /// The change the answer names.
        named: NamedChange,
        old: BTreeSet<u64>,
        next: BTreeSet<u64>,
    },
    /// The request was accepted and is not complete, or its outcome is unknown.
    Pending,
    /// A definite refusal.
    Refused,
}

/// A broken rule and the kind it reports under.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Broken {
    pub kind: &'static str,
    pub detail: String,
}

fn named_removal(detail: String) -> Result<(), Broken> {
    Err(Broken {
        kind: "RetryCertifiesNamedRemoval",
        detail,
    })
}

/// `RetryCertifiesNamedRemoval` (named in design 3.7). O17: "Never remove a required voter or
/// queue copy before replacement/copy evidence."
///
/// An answer that reports a replacement as complete certifies the removal its request names:
/// the named voter was a member, it no longer is, the named replacement is, and nothing else
/// changed. Every answer under one request ID certifies the same change.
///
/// Another body under an ID that is answered as complete is [`OPERATOR_GUIDANCE`]: O17 says
/// nothing about request identities.
#[derive(Default)]
pub(crate) struct RetryCertifiesNamedRemoval {
    requests: BTreeMap<Uuid, NamedChange>,
    certified: BTreeMap<Uuid, Certified>,
    /// Voters that a certified change removed. Replica identities are never reused.
    removed: BTreeSet<(Uuid, u64)>,
}

impl RetryCertifiesNamedRemoval {
    /// An answer to `request` under `id`, with the group's published voters read after it.
    pub(crate) fn answered(
        &mut self,
        id: Uuid,
        request: &NamedChange,
        answer: &Certified,
        published: Option<&BTreeSet<u64>>,
    ) -> Result<(), Broken> {
        let original = self
            .requests
            .entry(id)
            .or_insert_with(|| request.clone())
            .clone();
        let Certified::Retired { named, old, next } = answer else {
            return Ok(());
        };
        if *request != original {
            return Err(Broken {
                kind: OPERATOR_GUIDANCE,
                detail: format!(
                    "{id} first named {original:?}, and the body that names {request:?} was certified"
                ),
            });
        }
        if *named != original {
            return named_removal(format!(
                "{id} names {original:?}, and its answer certifies {named:?}"
            ));
        }
        let mut expected = old.clone();
        expected.remove(&original.remove);
        expected.insert(original.add);
        if !old.contains(&original.remove) || old.contains(&original.add) || *next != expected {
            return named_removal(format!(
                "{id} names {original:?}, and its answer certifies voters {old:?} to {next:?}"
            ));
        }
        if let Some(earlier) = self.certified.get(&id)
            && earlier != answer
        {
            return named_removal(format!(
                "{id} was certified as {earlier:?} and its retry as {answer:?}"
            ));
        }
        self.certified.insert(id, answer.clone());
        self.removed.insert((original.group, original.remove));
        match published {
            Some(voters) if voters.contains(&original.remove) => named_removal(format!(
                "{id} was certified as complete, and voter {} is still published in {voters:?}",
                original.remove
            )),
            _ => Ok(()),
        }
    }

    /// The published voters of `group` at any later time.
    pub(crate) fn published(&self, group: Uuid, voters: &BTreeSet<u64>) -> Result<(), String> {
        match voters
            .iter()
            .find(|voter| self.removed.contains(&(group, **voter)))
        {
            Some(voter) => Err(format!(
                "voter {voter} of group {group} was certified as removed and is published in {voters:?}"
            )),
            None => Ok(()),
        }
    }
}

/// `RequiredVoters`. O17: "Never remove a required voter or queue copy before replacement/copy
/// evidence." and "Keep minimum AZ placement and replica count." OLTP-G08: "Membership changes
/// never lose a required voter".
///
/// Every published placement of a group has the required number of voters, on as many workers.
pub(crate) fn required_voters(required: usize, members: &[Value]) -> Result<(), String> {
    let nodes: BTreeSet<_> = members
        .iter()
        .filter_map(|member| member["node_id"].as_u64())
        .collect();
    let workers: BTreeSet<_> = members
        .iter()
        .filter_map(|member| member["worker_id"].as_str())
        .collect();
    if members.len() == required && nodes.len() == required && workers.len() == required {
        return Ok(());
    }
    Err(format!(
        "{} members, {} voters and {} workers are published, {required} are required: {members:?}",
        members.len(),
        nodes.len(),
        workers.len()
    ))
}

/// A capability that Control gates.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Capability {
    /// `data.direct.v1`: direct entries.
    Direct,
    /// `data.direct.batch`: batched direct entries.
    Batch,
}

impl fmt::Display for Capability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Capability::Direct => "data.direct.v1",
            Capability::Batch => "data.direct.batch",
        })
    }
}

/// `CapabilityBeforeUse`. Design 3.7: "no entry needing feature F commits while a voter lacks
/// F." `plans/oltp-direct-path.md`, mixed versions: "Control activates it automatically when
/// every voter and learner advertises it. No leader proposes `Direct` before activation", and
/// cluster features: "Control enables a feature once every enrolled worker advertised it with
/// its current identity. [...] The enablement is a durable Control fact that never reverts".
///
/// A worker that never ran advertised nothing.
#[derive(Default)]
pub(crate) struct CapabilityBeforeUse {
    active: BTreeSet<Capability>,
    ran: BTreeSet<u64>,
}

impl CapabilityBeforeUse {
    /// A process of `worker` started, so it may advertise from now on.
    pub(crate) fn started(&mut self, worker: u64) {
        self.ran.insert(worker);
    }

    /// A worker reports `capability` as active, or work that needs it as done, while `voters`
    /// are the enrolled voters.
    pub(crate) fn in_use(
        &mut self,
        capability: Capability,
        what: &str,
        voters: &[u64],
    ) -> Result<(), String> {
        let lacking: Vec<u64> = voters
            .iter()
            .copied()
            .filter(|voter| !self.ran.contains(voter))
            .collect();
        if !lacking.is_empty() {
            return Err(format!(
                "{capability} is in use ({what}) and voters {lacking:?} never ran, so they advertised nothing"
            ));
        }
        self.active.insert(capability);
        Ok(())
    }

    /// Whether some of `voters` never ran, so that a capability must not be in use.
    pub(crate) fn some_never_ran(&self, voters: &[u64]) -> bool {
        voters.iter().any(|voter| !self.ran.contains(voter))
    }

    /// Control answers that `capability` is not active.
    pub(crate) fn inactive(&self, capability: Capability) -> Result<(), String> {
        if self.active.contains(&capability) {
            return Err(format!("{capability} was active and is no longer"));
        }
        Ok(())
    }
}

/// The state of one accepted request, as its receipt reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Receipt {
    Outstanding,
    Succeeded,
    /// A definite refusal. The quoted text does not forbid one after acceptance.
    Failed,
    Abandoned,
}

/// `QueueHandoff`. R03: "Keep the run; copy before readiness and transfer before the previous
/// last holder exits." OLTP-G07: "Accepted queued work stays recoverable while a complete copy
/// survives."
///
/// A rolling restart or a replacement stops one holder at a time, so every accepted request
/// keeps a holder and is never abandoned, and the run stays the same.
pub(crate) struct QueueHandoff {
    run: Uuid,
    accepted: BTreeMap<Uuid, Receipt>,
}

impl QueueHandoff {
    pub(crate) fn new(run: Uuid) -> Self {
        Self {
            run,
            accepted: BTreeMap::new(),
        }
    }

    pub(crate) fn accepted(&mut self, request: Uuid) {
        self.accepted.insert(request, Receipt::Outstanding);
    }

    pub(crate) fn run(&self, run: Uuid) -> Result<(), String> {
        if run == self.run {
            return Ok(());
        }
        Err(format!(
            "a restart that kept a holder changed the run from {} to {run}",
            self.run
        ))
    }

    /// A worker that just reported ready holds `held` outstanding requests, and `outstanding`
    /// ones that were accepted before it joined were still outstanding after that answer. R03:
    /// "copy before readiness". Returns whether this judged anything.
    ///
    /// The joiner's gauge counts records, so requests accepted during the join may hide a missing
    /// one. It never blames a joiner that holds them all.
    pub(crate) fn joiner(&self, held: u64, outstanding: u64) -> Result<bool, String> {
        if held < outstanding {
            return Err(format!(
                "a joiner reported ready with {held} outstanding requests, and {outstanding} accepted before its join were outstanding"
            ));
        }
        Ok(outstanding > 0)
    }

    pub(crate) fn receipt(&mut self, request: Uuid, now: Receipt) -> Result<(), String> {
        let Some(before) = self.accepted.insert(request, now.clone()) else {
            return Err(format!(
                "request {request} has a receipt and was never accepted"
            ));
        };
        match (&before, &now) {
            (_, Receipt::Abandoned) => Err(format!(
                "request {request} was accepted in run {} and was abandoned while a holder lived",
                self.run
            )),
            (Receipt::Outstanding, _) => Ok(()),
            (before, now) if before != now => Err(format!(
                "request {request} ended as {before:?} and is {now:?} now"
            )),
            _ => Ok(()),
        }
    }

    pub(crate) fn outstanding(&self) -> Vec<Uuid> {
        self.accepted
            .iter()
            .filter(|(_, receipt)| **receipt == Receipt::Outstanding)
            .map(|(request, _)| *request)
            .collect()
    }
}
