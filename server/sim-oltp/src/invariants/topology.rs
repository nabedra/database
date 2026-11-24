//! Rules for topology and schema changes under load.
//!
//! Each rule quotes the text it enforces, from design 3.7 ("design"), the OLTP contract in
//! `docs/testing/oltp-contract.md` ("contract"), `docs/testing/guarantees/oltp.md`
//! ("guarantees"), `plans/oltp-direct-path.md` ("plan") or `plans/oltp-engine.md`. The rules
//! take plain observations and return what they found, so each has legal and illegal fixtures
//! in `topology_tests.rs`. No rule reads a product symbol.
//!
//! The first group is judged from public answers that the `topology` workload records. The
//! second group is fed by protocol events that no public answer shows, so the sweep reports
//! those rules as `not_run` (see [`NOT_RUN`]).

use std::collections::{BTreeMap, BTreeSet};

use uuid::Uuid;

/// Design: "`CoveringIndexComplete`: an acknowledged insert appears in every covering index
/// scan at a later snapshot (the workload runs the scans)."
///
/// A scan invoked after the acknowledgment takes its snapshot after it.
pub(crate) const COVERING: &str = "CoveringIndexComplete";

/// Contract, O12: "Committed writes during the build are present exactly once."
pub(crate) const EXACTLY_ONCE: &str = "IndexExactlyOnce";

/// Guarantees, OLTP-G04: "Return `not_applied` only with proof of non-application." A scan
/// that shows a write whose only definite answer was `not_applied`.
pub(crate) const REFUSED_WRITE: &str = "RefusedWriteInIndex";

/// Contract, O12: "Queries never choose an incomplete index." Plan (engine): "Queries must not
/// select the index before publication." and "failed builds cannot expose partial results."
pub(crate) const INCOMPLETE: &str = "IncompleteIndexChosen";

/// Contract, D11: "No row lands in a dropped table". Guarantees, OLTP-G06: "Routes, topology
/// and schema epochs are never served stale." with the failure model "a table drop [...] can
/// race writes and leave a gateway with stale routing or schema information".
pub(crate) const DROPPED: &str = "DroppedTableSilent";

/// Contract, D07: "Stale routes refresh once."
pub(crate) const STALE_ROUTE: &str = "StaleRouteRefreshesOnce";

/// Guarantees, OLTP-G04: "Return `not_applied` only with proof of non-application." A schema
/// change refused with `not_applied` whose record later exists.
pub(crate) const REFUSED_DDL: &str = "RefusedDdlApplied";

/// Design: "`FenceAfterValidation`: a Data fence epoch names a published Catalog lifecycle."
/// Plan, section 7: "Before the Catalog publishes either change, the DDL coordinator commits
/// the new `schema_epoch` to every Data group of the table".
///
/// The fence precedes the publication, so the rule is judged once no schema change of the
/// table is open. A Data group whose fence then names an unpublished lifecycle admits no
/// descriptor, and its leader cannot serve a mutation directly. That consequence is public:
/// with every schema change settled and the cluster healthy, direct mutations of the table
/// keep being sent to the queued path because of the schema.
pub(crate) const FENCE: &str = "FenceAfterValidation";

/// Design: "`SchemaEpochAtCommit`: a commit's schema epoch is the one in effect at its
/// timestamp." Contract, D11: "no write uses a retired schema epoch".
pub(crate) const SCHEMA_EPOCH: &str = "SchemaEpochAtCommit";

/// Design: "`RetiredLeaderSilent`: a retired range leader never answers success."
pub(crate) const RETIRED: &str = "RetiredLeaderSilent";

/// Contract, O28: "Paged metadata covers every range exactly once at the pinned epoch".
pub(crate) const ROUTES: &str = "RouteCoverage";

/// Every rule the workload judges from public observations.
pub(crate) const RULES: [&str; 9] = [
    COVERING,
    EXACTLY_ONCE,
    REFUSED_WRITE,
    INCOMPLETE,
    DROPPED,
    STALE_ROUTE,
    REFUSED_DDL,
    FENCE,
    ROUTES,
];

/// What cannot run yet, and why. The sweep records each as `not_run`.
pub(crate) const NOT_RUN: [(&str, &str); 4] = [
    (
        SCHEMA_EPOCH,
        "needs probes: a DirectCommit's schema epoch and timestamp at apply, and the schema epoch and timestamp of each Catalog publication and drop. #116 reports neither",
    ),
    (
        RETIRED,
        "needs probes: a range leader's retirement (split source closed, replica removed) and the replica that answered a direct request. #116's GatewayAttempt names the voter asked, not the answer",
    ),
    (
        "FenceAfterValidation over Data fences",
        "needs probes: each Data group's DirectSchema change and each published Catalog lifecycle. The public form ran",
    ),
    (
        "CoveringIndexComplete at the scan's own snapshot",
        "needs a probe with the snapshot timestamp of a query. The public form orders by invocation",
    ),
];

/// How a write by the single owner of a key ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Ended {
    /// Success that changed the row.
    Acked,
    /// A definite `not_applied`.
    Refused,
    /// No definite answer before the run ended.
    Unknown,
}

/// One write of a key that only its auditor writes, so the writes of a key are sequential.
/// `invoked` and `completed` are positions in one run-wide order of events.
#[derive(Clone, Debug)]
pub(crate) struct Write {
    pub key: i64,
    /// The new value, or `None` for a delete.
    pub value: Option<i64>,
    pub insert: bool,
    pub invoked: u64,
    pub completed: u64,
    pub ended: Ended,
}

/// One successful scan of the audited value range.
#[derive(Clone, Debug)]
pub(crate) struct Scan {
    pub invoked: u64,
    pub completed: u64,
    pub rows: Vec<(i64, i64)>,
}

/// [`COVERING`], [`EXACTLY_ONCE`] and [`REFUSED_WRITE`] over one table.
pub(crate) fn audit_scans(writes: &[Write], scans: &[Scan]) -> Vec<(&'static str, String)> {
    let mut found = Vec::new();
    let mut by_key: BTreeMap<i64, Vec<&Write>> = BTreeMap::new();
    for write in writes {
        by_key.entry(write.key).or_default().push(write);
    }
    for scan in scans {
        let mut rows: BTreeMap<i64, Vec<i64>> = BTreeMap::new();
        for &(key, value) in &scan.rows {
            rows.entry(key).or_default().push(value);
        }
        for (key, values) in &rows {
            if values.len() > 1 {
                found.push((
                    EXACTLY_ONCE,
                    format!(
                        "a scan at {} shows key {key} {} times: {values:?}",
                        scan.invoked,
                        values.len()
                    ),
                ));
            }
            for value in values {
                let history = by_key.get(key).map_or(&[][..], Vec::as_slice);
                if let Some((rule, why)) = illegal_row(history, scan, *value) {
                    found.push((
                        rule,
                        format!(
                            "a scan at {} shows key {key} with {value}: {why}",
                            scan.invoked
                        ),
                    ));
                }
            }
        }
        for (key, history) in &by_key {
            let Some((index, last)) = settled_before(history, scan) else {
                continue;
            };
            let Some(value) = last.value else {
                continue;
            };
            let later = &history[index + 1..];
            let quiet = later
                .iter()
                .all(|write| write.invoked >= scan.completed || write.ended == Ended::Refused);
            let shown = rows.get(key).map_or(&[][..], Vec::as_slice);
            // A refused write that took effect is reported as that.
            let refused = later.iter().any(|write| {
                write.ended == Ended::Refused
                    && match write.value {
                        Some(value) => shown.contains(&value),
                        None => shown.is_empty(),
                    }
            });
            if quiet && !shown.contains(&value) {
                let rule = match (refused, last.insert) {
                    (true, _) => REFUSED_WRITE,
                    (false, true) => COVERING,
                    (false, false) => EXACTLY_ONCE,
                };
                found.push((
                    rule,
                    format!(
                        "a scan invoked at {} omits key {key}, whose {} of {value} was acknowledged at {}; it shows {shown:?}",
                        scan.invoked,
                        if last.insert { "insert" } else { "update" },
                        last.completed,
                    ),
                ));
            }
        }
    }
    found
}

/// The last acknowledged write before `scan` began.
fn settled_before<'a>(history: &[&'a Write], scan: &Scan) -> Option<(usize, &'a Write)> {
    history
        .iter()
        .enumerate()
        .rev()
        .find(|(_, write)| write.ended == Ended::Acked && write.completed < scan.invoked)
        .map(|(index, write)| (index, *write))
}

/// Why a scan may not show `value`.
fn illegal_row(history: &[&Write], scan: &Scan, value: i64) -> Option<(&'static str, String)> {
    let Some(position) = history
        .iter()
        .rposition(|write| write.value == Some(value) && write.invoked < scan.completed)
    else {
        return Some((EXACTLY_ONCE, "no write of that value was sent".into()));
    };
    let write = history[position];
    if write.ended == Ended::Refused {
        return Some((
            REFUSED_WRITE,
            format!(
                "that write was refused as not applied at {}",
                write.completed
            ),
        ));
    }
    // A write that never got a definite answer may still land later.
    if write.ended == Ended::Unknown {
        return None;
    }
    let overwritten = settled_before(history, scan).filter(|(index, _)| *index > position);
    overwritten.map(|(_, later)| {
        (
            EXACTLY_ONCE,
            format!("a later write was acknowledged at {}", later.completed),
        )
    })
}

/// A state an operator read for one index build.
#[derive(Clone, Debug)]
pub(crate) struct Status {
    pub index: String,
    pub invoked: u64,
    pub completed: u64,
    pub state: String,
}

/// An index an `explain` named.
#[derive(Clone, Debug)]
pub(crate) struct Chosen {
    pub index: String,
    pub invoked: u64,
    pub completed: u64,
}

/// [`INCOMPLETE`]. `created` holds the indexes whose creation was sent, by name. Phases only
/// move forward (building, ready, retired; or building, failed), so a status read after the
/// explain that still says building proves the index was not ready when it was chosen.
pub(crate) fn incomplete_chosen(
    created: &BTreeSet<String>,
    statuses: &[Status],
    chosen: &[Chosen],
) -> Vec<String> {
    let mut found = Vec::new();
    for choice in chosen {
        if !created.contains(&choice.index) {
            found.push(format!(
                "an explain at {} chose {}, which was never declared",
                choice.invoked, choice.index
            ));
            continue;
        }
        for status in statuses.iter().filter(|s| s.index == choice.index) {
            let later = status.invoked > choice.completed;
            let earlier = status.completed < choice.invoked;
            let wrong = match status.state.as_str() {
                "building" => later,
                "failed" => true,
                "retired" => earlier,
                _ => false,
            };
            if wrong {
                found.push(format!(
                    "an explain from {} to {} chose {}, and a status from {} to {} reads {}",
                    choice.invoked,
                    choice.completed,
                    choice.index,
                    status.invoked,
                    status.completed,
                    status.state
                ));
                break;
            }
        }
    }
    found
}

/// One answer to an operation on a table that is dropped in the run.
#[derive(Clone, Debug)]
pub(crate) struct TableCall {
    pub what: String,
    pub invoked: u64,
    pub success: bool,
}

/// [`DROPPED`]: after the drop was acknowledged, no operation invoked later succeeds.
pub(crate) fn dropped_silent(acknowledged: u64, calls: &[TableCall]) -> Vec<String> {
    calls
        .iter()
        .filter(|call| call.success && call.invoked > acknowledged)
        .map(|call| {
            format!(
                "{} invoked at {} succeeded after the drop was acknowledged at {acknowledged}",
                call.what, call.invoked
            )
        })
        .collect()
}

/// One request sent alone through a gateway whose cached route a topology change replaced,
/// and how many stale-route answers the workers gave meanwhile.
#[derive(Clone, Debug)]
pub(crate) struct Refresh {
    pub gateway: u64,
    pub key: i64,
    /// The first request of this gateway to that key's range since the change.
    pub first: bool,
    pub stale_answers: u64,
}

/// [`STALE_ROUTE`].
pub(crate) fn refreshes_once(refreshes: &[Refresh]) -> Vec<String> {
    refreshes
        .iter()
        .filter(|refresh| refresh.stale_answers > u64::from(refresh.first))
        .map(|refresh| {
            format!(
                "{} request for key {} through n{} met {} stale-route answers",
                if refresh.first {
                    "the first"
                } else {
                    "a later"
                },
                refresh.key,
                refresh.gateway,
                refresh.stale_answers
            )
        })
        .collect()
}

/// [`REFUSED_DDL`]: `refused` are definite refusals of index creations by build ID, `found`
/// the build IDs whose record an operator read afterwards.
pub(crate) fn refused_ddl(refused: &BTreeMap<Uuid, String>, found: &BTreeSet<Uuid>) -> Vec<String> {
    refused
        .iter()
        .filter(|(build, _)| found.contains(build))
        .map(|(build, why)| format!("index build {build} was refused ({why}) and exists"))
        .collect()
}

/// Direct mutations of one table at rest, after every schema change of it settled.
#[derive(Clone, Debug)]
pub(crate) struct AtRest {
    pub table: &'static str,
    pub gateway: u64,
    pub writes: u64,
    /// Mutations the leaders sent to the queued path because of the schema.
    pub schema_fallbacks: u64,
    /// How long the workload kept polling for a direct commit, in virtual ms.
    pub polled_ms: u64,
}

/// [`FENCE`] in its public form.
pub(crate) fn fence_at_rest(rest: &[AtRest]) -> Vec<String> {
    rest.iter()
        .filter(|rest| rest.schema_fallbacks > 0)
        .map(|rest| {
            format!(
                "with no schema change open, {} of {} mutations of {} through n{} were still sent to the queued path because of the schema after {} ms",
                rest.schema_fallbacks, rest.writes, rest.table, rest.gateway, rest.polled_ms
            )
        })
        .collect()
}

/// [`ROUTES`]: the number of ranges a query plan covers against the splits that published.
pub(crate) fn route_count(table: &str, published_splits: u64, planned: u64) -> Option<String> {
    (planned != published_splits + 1).then(|| {
        format!("{table} has {published_splits} published splits and a plan over {planned} ranges")
    })
}

/// [`SCHEMA_EPOCH`], fed by probes.
#[cfg_attr(not(test), allow(dead_code))] // Fed by probes (see `NOT_RUN`).
#[derive(Default)]
pub(crate) struct SchemaEpochs {
    /// Per table, the epoch in effect from a timestamp. `None` is a drop.
    changes: BTreeMap<Uuid, BTreeMap<u64, Option<u64>>>,
    commits: Vec<(Uuid, u64, u64)>,
}

#[cfg_attr(not(test), allow(dead_code))] // Fed by probes (see `NOT_RUN`).
impl SchemaEpochs {
    pub(crate) fn published(&mut self, table: Uuid, epoch: Option<u64>, at: u64) {
        self.changes.entry(table).or_default().insert(at, epoch);
    }

    pub(crate) fn committed(&mut self, table: Uuid, epoch: u64, at: u64) {
        self.commits.push((table, epoch, at));
    }

    /// A replica can report a commit before the publication below it, so the rule is judged
    /// over the whole run.
    pub(crate) fn finish(&self) -> Vec<String> {
        let mut found = Vec::new();
        for &(table, epoch, at) in &self.commits {
            let in_effect = self
                .changes
                .get(&table)
                .and_then(|changes| changes.range(..=at).next_back())
                .and_then(|(_, epoch)| *epoch);
            if in_effect != Some(epoch) {
                found.push(format!(
                    "a commit on {table} at {at} carries schema epoch {epoch}, and {in_effect:?} was in effect"
                ));
            }
        }
        found
    }
}

/// [`RETIRED`], fed by probes.
#[cfg_attr(not(test), allow(dead_code))] // Fed by probes (see `NOT_RUN`).
#[derive(Default)]
pub(crate) struct RetiredLeaders {
    replicas: BTreeSet<(Uuid, u64)>,
    groups: BTreeSet<Uuid>,
}

#[cfg_attr(not(test), allow(dead_code))] // Fed by probes (see `NOT_RUN`).
impl RetiredLeaders {
    /// A move removed this replica of `group`.
    pub(crate) fn retire_replica(&mut self, group: Uuid, replica: u64) {
        self.replicas.insert((group, replica));
    }

    /// A split replaced the whole range.
    pub(crate) fn retire_group(&mut self, group: Uuid) {
        self.groups.insert(group);
    }

    pub(crate) fn answered(&self, group: Uuid, replica: u64, success: bool) -> Option<String> {
        let retired = self.groups.contains(&group) || self.replicas.contains(&(group, replica));
        (success && retired)
            .then(|| format!("replica {replica} of retired range {group} answered success"))
    }
}

/// [`FENCE`], fed by probes.
#[cfg_attr(not(test), allow(dead_code))] // Fed by probes (see `NOT_RUN`).
#[derive(Default)]
pub(crate) struct DataFences {
    /// Per table, the lifecycle the Catalog published last (`None`: dropped).
    published: BTreeMap<Uuid, Option<Uuid>>,
    /// Per table and Data group, the lifecycle its last fence names, and the fence's epoch.
    fences: BTreeMap<(Uuid, Uuid), (Option<Uuid>, u64)>,
    open: BTreeMap<Uuid, u64>,
}

#[cfg_attr(not(test), allow(dead_code))] // Fed by probes (see `NOT_RUN`).
impl DataFences {
    pub(crate) fn began(&mut self, table: Uuid) {
        *self.open.entry(table).or_default() += 1;
    }

    pub(crate) fn published(&mut self, table: Uuid, lifecycle: Option<Uuid>) {
        self.published.insert(table, lifecycle);
    }

    pub(crate) fn fenced(&mut self, table: Uuid, group: Uuid, epoch: u64, named: Option<Uuid>) {
        self.fences.insert((table, group), (named, epoch));
    }

    /// A schema change of `table` published, or ended without publishing.
    pub(crate) fn ended(&mut self, table: Uuid) -> Vec<String> {
        let open = self.open.entry(table).or_default();
        *open = open.saturating_sub(1);
        self.check(table)
    }

    fn check(&self, table: Uuid) -> Vec<String> {
        if self.open.get(&table).is_some_and(|open| *open > 0) {
            return Vec::new();
        }
        let published = self.published.get(&table).copied();
        self.fences
            .range((table, Uuid::nil())..=(table, Uuid::max()))
            .filter(|(_, (named, _))| published != Some(*named))
            .map(|((_, group), (named, epoch))| {
                format!(
                    "the fence of group {group} at epoch {epoch} names lifecycle {named:?}, and {published:?} is published for table {table}"
                )
            })
            .collect()
    }

    pub(crate) fn finish(&self) -> Vec<String> {
        let tables: BTreeSet<Uuid> = self.fences.keys().map(|(table, _)| *table).collect();
        tables
            .into_iter()
            .flat_map(|table| self.check(table))
            .collect()
    }
}

#[cfg(test)]
#[path = "topology_tests.rs"]
mod tests;
