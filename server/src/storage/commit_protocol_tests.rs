//! OLAP commit protocol (OLAP-G01 to G05, scenarios T01 and T03 to T06) on the real
//! publication code (`PreparedPublication`, storage/publication.rs) and the legacy
//! coordinator (`commit::Coordinator`), over `SimObjectStore` with seeded faults and
//! seeded interleavings on a current-thread runtime, so each seed replays exactly.
//!
//! Two planted bugs prove the checks are sensitive: an adapter that reports a
//! possibly applied PUT as a definite Conflict (T04's false `not_applied`) and one
//! that ignores `If-Match` (a CAS loser that overwrites).
use super::{
    CatalogEntry, CatalogFence, ClusterCatalog, Decision, Download, FenceTransition, HistoryBudget,
    LoadedClusterCatalog, MAX_DECISION_BYTES, Namespace, ObjectKey, ObjectStore,
    PreparedPublication, PublicationOutcome, Storage, StorageError, WriteCondition,
    sim::{Fault, Rule, SimObjectStore},
};
use crate::protocol::Outcome;
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    sync::atomic::{AtomicU64, Ordering},
};
use tokio::time::{Duration, Instant};
use uuid::Uuid;

/// Planted bugs, applied by a wrapper around the sim store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Bug {
    None,
    /// The adapter turns `Ambiguous` into `Conflict`: a lost reply becomes "not applied".
    AmbiguousAsConflict,
    /// The adapter drops `If-Match` on the catalog: a CAS loser overwrites the winner.
    IgnoreMatch,
}
#[derive(Clone)]
struct Planted {
    sim: SimObjectStore,
    bug: Bug,
}
impl ObjectStore for Planted {
    async fn get(&self, key: &str) -> Result<Download, StorageError> {
        self.sim.get(key).await
    }
    async fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        condition: WriteCondition<'_>,
    ) -> Result<String, StorageError> {
        match (self.bug, condition) {
            (Bug::IgnoreMatch, WriteCondition::Match(_)) => {
                let current = self.sim.object(key).map(|o| o.1).unwrap_or_default();
                self.sim
                    .put(key, bytes, WriteCondition::Match(&current))
                    .await
            }
            (Bug::AmbiguousAsConflict, condition) => {
                match self.sim.put(key, bytes, condition).await {
                    Err(StorageError::Ambiguous) => Err(StorageError::Conflict),
                    other => other,
                }
            }
            (_, condition) => self.sim.put(key, bytes, condition).await,
        }
    }
}

const NS: &str = "proto";
fn storage(sim: &SimObjectStore, bug: Bug) -> Storage<Planted> {
    Storage::new(
        Planted {
            sim: sim.clone(),
            bug,
        },
        Namespace::new(NS).unwrap(),
    )
}
fn budget() -> HistoryBudget {
    HistoryBudget::new(
        1000,
        1000 * MAX_DECISION_BYTES,
        Instant::now() + Duration::from_secs(30),
    )
    .unwrap()
}
fn current_thread() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}
fn request_id(w: usize, m: usize) -> Uuid {
    Uuid::from_u128((7 << 120) | ((w as u128) << 32) | m as u128)
}
fn table_name(w: usize, m: usize) -> String {
    format!("t{w}_{m}")
}
fn entry(id: Uuid) -> CatalogEntry {
    serde_json::from_value(serde_json::json!({"id":id,"schema":{"id":"int64"},"key":"id","format":"parquet","data":{"layout":"partitioned","files":[]},"row_count":0})).unwrap()
}
/// A decision for request (w, m) against `base`, with a fresh attempt id.
fn decision(base: &LoadedClusterCatalog, w: usize, m: usize, id: Uuid) -> Decision {
    let rid = request_id(w, m);
    Decision {
        version: 1,
        id,
        parent: base.catalog().decision_head,
        base_revision: base.catalog().revision,
        revision: base.catalog().revision + 1,
        request_id: rid,
        request_body: format!(
            "{{\"request_id\":\"{rid}\",\"operation\":\"table.insert\",\"payload\":{{\"table_id\":\"{}\",\"row\":{{\"id\":1}}}}}}",
            Uuid::from_u128(99)
        ),
        response_status: 200,
        response_body: format!("{{\"request_id\":\"{rid}\",\"data\":{{\"w\":{w},\"m\":{m}}}}}"),
    }
}

/// What one publication attempt claimed about its own decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Claim {
    /// Terminal: the request's decision is this id (ours or an earlier attempt's).
    Committed(Uuid),
    /// NeedsRecompute or a pre-root error: this attempt's decision did not apply.
    NotApplied(Uuid),
    Unknown(Uuid),
}

struct Writer<'a> {
    storage: &'a Storage<Planted>,
    ids: &'a AtomicU64,
    claims: &'a RefCell<Vec<Claim>>,
}
impl Writer<'_> {
    fn next_id(&self) -> Uuid {
        Uuid::from_u128((9 << 120) | self.ids.fetch_add(1, Ordering::Relaxed) as u128)
    }
    /// Commit request (w, m) until a terminal result: recompute on a CAS loss,
    /// resolve an unknown outcome by publishing again with the same identity.
    async fn commit(&self, w: usize, m: usize) -> Result<Decision, String> {
        for _ in 0..300 {
            let Ok(base) = self.storage.load_cluster_catalog().await else {
                continue;
            };
            let id = self.next_id();
            let mut tables = base.catalog().tables.clone();
            tables.insert(table_name(w, m), entry(request_id(w, m)));
            let prepared =
                PreparedPublication::prepare(base.clone(), tables, decision(&base, w, m, id))
                    .map_err(|e| format!("prepare: {e:?}"))?;
            match prepared.publish(self.storage, budget()).await {
                Ok(PublicationOutcome::Terminal(d)) => {
                    if d.request_id != request_id(w, m) {
                        return Err(format!("({w},{m}) got another request's decision"));
                    }
                    self.claims.borrow_mut().push(Claim::Committed(d.id));
                    return Ok(d);
                }
                Ok(PublicationOutcome::NeedsRecompute) | Err(_) => {
                    self.claims.borrow_mut().push(Claim::NotApplied(id));
                }
                Ok(PublicationOutcome::Unknown) => {
                    self.claims.borrow_mut().push(Claim::Unknown(id));
                }
                Ok(PublicationOutcome::IdentityConflict) => {
                    return Err(format!("({w},{m}) identity conflict on its own retry"));
                }
            }
        }
        Err(format!("({w},{m}) starved after 300 attempts"))
    }
}

/// The reachable decision chain of the final root, head first.
async fn chain(s: &Storage<Planted>) -> Result<(LoadedClusterCatalog, Vec<Decision>), String> {
    let root = s
        .load_cluster_catalog()
        .await
        .map_err(|e| format!("final root: {e:?}"))?;
    let mut out = Vec::new();
    let mut next = root.catalog().decision_head;
    while let Some(id) = next {
        let object = s
            .read(&ObjectKey::decision(id), MAX_DECISION_BYTES)
            .await
            .map_err(|e| format!("decision {id}: {e:?}"))?;
        let d = Decision::decode(&object.bytes).map_err(|e| format!("decode {id}: {e:?}"))?;
        next = d.parent;
        out.push(d);
        if out.len() > 10_000 {
            return Err("decision chain cycles".into());
        }
    }
    Ok((root, out))
}
/// Every commit id that any applied catalog version ever carried.
fn applied_roots(sim: &SimObjectStore) -> BTreeSet<Uuid> {
    sim.applied()
        .into_iter()
        .filter(|a| a.key == format!("{NS}/catalog.json"))
        .filter_map(|a| ClusterCatalog::decode(&a.bytes).ok())
        .map(|c| c.commit_id)
        .collect()
}

struct Plan {
    writers: usize,
    commits: usize,
    fault_seed: u64,
    per_mille: u64,
    interleave_seed: u64,
}

/// Iceberg-style lock-step committers: N writers each make M commits, all N
/// released together by a barrier per round. Afterwards exactly N x M decisions
/// are reachable, the catalog equals their serial application, every claim
/// holds, and a replay returns the original result.
fn lockstep(plan: &Plan, bug: Bug) -> Result<(), String> {
    current_thread().block_on(async {
        let sim = SimObjectStore::new();
        let s = storage(&sim, bug);
        s.initialize_cluster_catalog()
            .await
            .map_err(|e| format!("init: {e:?}"))?;
        sim.interleave(plan.interleave_seed, 3);
        sim.random_faults(
            plan.fault_seed,
            plan.per_mille,
            vec![
                Fault::Status(412),
                Fault::Status(409),
                Fault::Status(503),
                Fault::LostResponse,
                Fault::DelayApply { steps: 4 },
                Fault::EmbeddedError,
            ],
        );
        let ids = AtomicU64::new(1);
        let claims = RefCell::new(Vec::new());
        let barrier = tokio::sync::Barrier::new(plan.writers);
        let writer = Writer {
            storage: &s,
            ids: &ids,
            claims: &claims,
        };
        let results = futures_util::future::join_all((0..plan.writers).map(|w| {
            let (writer, barrier) = (&writer, &barrier);
            async move {
                let mut out = Vec::new();
                for m in 0..plan.commits {
                    barrier.wait().await;
                    out.push(writer.commit(w, m).await);
                }
                out
            }
        }))
        .await;
        sim.clear_faults();
        sim.apply_delayed();
        let (root, chain) = chain(&s).await?;
        let n = plan.writers * plan.commits;
        // Exactly N x M decisions, one per request, revisions consecutive.
        let mut by_request = BTreeMap::new();
        for d in &chain {
            if by_request.insert(d.request_id, d.clone()).is_some() {
                return Err(format!("request {} decided twice", d.request_id));
            }
        }
        if chain.len() != n || root.catalog().revision != n as u64 {
            return Err(format!(
                "{} decisions reachable at revision {}, expected {n}; faults {:?}",
                chain.len(),
                root.catalog().revision,
                sim.fired()
            ));
        }
        // The catalog equals the serial application of the chain.
        let expected: BTreeSet<String> = (0..plan.writers)
            .flat_map(|w| (0..plan.commits).map(move |m| table_name(w, m)))
            .collect();
        let tables: BTreeSet<String> = root.catalog().tables.keys().cloned().collect();
        if tables != expected {
            return Err(format!("catalog tables {tables:?} != serial application"));
        }
        // Every reported result is the reachable decision (T06), and every
        // "not applied" claim is true (T04: no false not_applied).
        for (w, results) in results.into_iter().enumerate() {
            for (m, r) in results.into_iter().enumerate() {
                let d = r?;
                if by_request.get(&request_id(w, m)) != Some(&d) {
                    return Err(format!(
                        "({w},{m}) reported a decision that is not reachable"
                    ));
                }
            }
        }
        let reachable: BTreeSet<Uuid> = chain.iter().map(|d| d.id).collect();
        let ever_root = applied_roots(&sim);
        for claim in claims.borrow().iter() {
            match *claim {
                Claim::Committed(id) if !reachable.contains(&id) => {
                    return Err(format!("committed {id} is not reachable"));
                }
                Claim::NotApplied(id) if reachable.contains(&id) || ever_root.contains(&id) => {
                    return Err(format!(
                        "attempt {id} was reported not applied but it applied; faults {:?}",
                        sim.fired()
                    ));
                }
                _ => {}
            }
        }
        // T06: a replay of a committed request returns the original decision and
        // publishes no new root.
        let base = s
            .load_cluster_catalog()
            .await
            .map_err(|e| format!("{e:?}"))?;
        let before = sim.applied().len();
        let replay = PreparedPublication::prepare(
            base.clone(),
            base.catalog().tables.clone(),
            decision(&base, 0, 0, Uuid::from_u128(1 << 64)),
        )
        .map_err(|e| format!("{e:?}"))?
        .publish(&s, budget())
        .await;
        if replay
            != Ok(PublicationOutcome::Terminal(
                by_request[&request_id(0, 0)].clone(),
            ))
        {
            return Err(format!("replay returned {replay:?}"));
        }
        if sim.applied().len() != before {
            return Err("replay wrote to the store".into());
        }
        Ok(())
    })
}

fn plan(g: &mut nabedra_check::Gen) -> Plan {
    Plan {
        writers: 2 + g.below(3) as usize,
        commits: 1 + g.below(4) as usize,
        fault_seed: g.u64(),
        per_mille: g.below(200),
        interleave_seed: g.u64(),
    }
}

#[test]
fn lockstep_committers_serial_equivalence_under_faults() {
    nabedra_check::prop::check("olap_commit_lockstep", |g| lockstep(&plan(g), Bug::None));
}

/// Without faults the race is pure contention: every CAS loser recomputes (T03).
#[test]
fn lockstep_committers_without_faults() {
    for seed in 0..32 {
        let p = Plan {
            writers: 4,
            commits: 3,
            fault_seed: 0,
            per_mille: 0,
            interleave_seed: seed,
        };
        lockstep(&p, Bug::None).unwrap_or_else(|e| panic!("seed {seed}: {e}"));
    }
}

/// T03: a CAS loser sees NeedsRecompute, leaves the winner intact, and its
/// recomputed commit contains both changes.
#[test]
fn t03_cas_loser_recomputes_and_never_overwrites() {
    current_thread().block_on(async {
        let sim = SimObjectStore::new();
        let s = storage(&sim, Bug::None);
        let base = s.initialize_cluster_catalog().await.unwrap();
        let prepare = |w: usize, base: &LoadedClusterCatalog, id: u128| {
            let mut tables = base.catalog().tables.clone();
            tables.insert(table_name(w, 0), entry(request_id(w, 0)));
            PreparedPublication::prepare(
                base.clone(),
                tables,
                decision(base, w, 0, Uuid::from_u128(id)),
            )
            .unwrap()
        };
        let winner = prepare(0, &base, 1);
        let loser = prepare(1, &base, 2);
        assert!(matches!(
            winner.publish(&s, budget()).await,
            Ok(PublicationOutcome::Terminal(_))
        ));
        assert_eq!(
            loser.publish(&s, budget()).await,
            Ok(PublicationOutcome::NeedsRecompute)
        );
        let fresh = s.load_cluster_catalog().await.unwrap();
        assert_eq!(fresh.catalog().commit_id, Uuid::from_u128(1));
        let again = prepare(1, &fresh, 3);
        assert!(matches!(
            again.publish(&s, budget()).await,
            Ok(PublicationOutcome::Terminal(_))
        ));
        let (root, chain) = chain(&s).await.unwrap();
        assert_eq!(chain.len(), 2);
        assert_eq!(root.catalog().tables.len(), 2);
    });
}

/// T04 on the publication path: the root PUT applies and its reply is lost. The
/// writer reads back its decision and reports it committed, with no second root.
#[test]
fn t04_lost_root_reply_resolves_to_the_committed_decision() {
    current_thread().block_on(async {
        let sim = SimObjectStore::new();
        let s = storage(&sim, Bug::None);
        let base = s.initialize_cluster_catalog().await.unwrap();
        sim.inject(Rule::put("catalog.json", Fault::LostResponse));
        let mut tables = base.catalog().tables.clone();
        tables.insert("t".into(), entry(Uuid::from_u128(5)));
        let d = decision(&base, 0, 0, Uuid::from_u128(1));
        let outcome = PreparedPublication::prepare(base, tables, d.clone())
            .unwrap()
            .publish(&s, budget())
            .await;
        assert_eq!(outcome, Ok(PublicationOutcome::Terminal(d)));
        assert_eq!(sim.fired().len(), 1);
        let roots = sim
            .applied()
            .iter()
            .filter(|a| a.key.ends_with("catalog.json"))
            .count();
        assert_eq!(roots, 2, "init plus exactly one publication");
    });
}

/// T04 on the legacy coordinator (commit.rs): never `not_applied` for a commit
/// that applied. Returns Err describing a violation.
async fn coordinator_t04(bug: Bug, readback_fails: bool) -> Result<(), String> {
    let sim = SimObjectStore::new();
    let s = storage(&sim, bug);
    s.initialize_catalog().await.map_err(|e| format!("{e:?}"))?;
    let coordinator = crate::commit::Coordinator::new(s);
    sim.inject(Rule::put("catalog.json", Fault::LostResponse));
    if readback_fails {
        sim.inject(Rule::get("catalog.json", Fault::Status(503)).after(1));
    }
    let session = coordinator.begin().await.map_err(|e| format!("{e:?}"))?;
    let commit_id = session.commit_id();
    let mut candidate = session.candidate();
    candidate
        .tables
        .insert("t".into(), entry(Uuid::from_u128(5)));
    let result = session.publish_partitions(candidate, vec![]).await;
    let applied = sim
        .object(&format!("{NS}/catalog.json"))
        .and_then(|(bytes, _)| super::Catalog::decode(&bytes).ok())
        .is_some_and(|c| c.commit_id == commit_id);
    if !applied {
        return Err("the lost-reply PUT did not apply".into());
    }
    match result {
        Ok(loaded) if loaded.catalog.commit_id == commit_id => Ok(()),
        Ok(_) => Err("reported another commit".into()),
        Err(e) if e.outcome == Outcome::NotApplied => {
            Err(format!("false not_applied for an applied commit: {e:?}"))
        }
        Err(e) if readback_fails && e.outcome == Outcome::Unknown => Ok(()),
        Err(e) => Err(format!("unexpected {e:?}")),
    }
}

#[test]
fn t04_coordinator_never_reports_false_not_applied() {
    current_thread().block_on(async {
        coordinator_t04(Bug::None, false).await.unwrap();
        coordinator_t04(Bug::None, true).await.unwrap();
    });
}

/// T05: an old-run root PUT that is delayed while a new run fences the catalog.
/// Ordered after the fence it fails; ordered before it, it commits and stays
/// visible. The fence is never rolled back and the old run cannot publish again.
/// Returns whether the old commit ended up visible (ordered before the fence).
fn t05(delay: u64, hold: bool) -> Result<bool, String> {
    current_thread().block_on(async {
        let sim = SimObjectStore::new();
        let s = storage(&sim, Bug::None);
        let base = s
            .initialize_cluster_catalog()
            .await
            .map_err(|e| format!("{e:?}"))?;
        let old = CatalogFence {
            run_id: Uuid::from_u128(1),
            scheduler_term: 1,
        };
        let new = CatalogFence {
            run_id: Uuid::from_u128(2),
            scheduler_term: 1,
        };
        let fenced = s
            .fence_cluster_catalog(&base, old, FenceTransition::Migrate)
            .await
            .map_err(|e| format!("migrate: {e:?}"))?;
        let mut tables = fenced.catalog().tables.clone();
        tables.insert("old".into(), entry(Uuid::from_u128(5)));
        let d = decision(&fenced, 0, 0, Uuid::from_u128(10));
        let prepared = PreparedPublication::prepare_fenced(fenced.clone(), tables, d.clone(), old)
            .map_err(|e| format!("{e:?}"))?;
        let gate = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
        sim.inject(Rule::put(
            "catalog.json",
            if hold {
                Fault::Hold(gate.clone())
            } else {
                Fault::DelayApply { steps: delay }
            },
        ));
        // The new run fences while the old PUT is pending (or right after it).
        let new_run = async {
            for _ in 0..20 {
                let Ok(current) = s.load_cluster_catalog().await else {
                    continue;
                };
                match s
                    .fence_cluster_catalog(&current, new, FenceTransition::ColdStart)
                    .await
                {
                    Ok(done) => {
                        gate.add_permits(1);
                        return Ok(done);
                    }
                    Err(StorageError::Conflict | StorageError::Ambiguous) => continue,
                    Err(e) => return Err(format!("fence: {e:?}")),
                }
            }
            Err("fence starved".to_string())
        };
        let (outcome, fence) = tokio::join!(prepared.publish(&s, budget()), new_run);
        let fence = fence?;
        sim.apply_delayed();
        let (root, chain) = chain(&s).await?;
        if root.catalog().fence != Some(new) {
            return Err("the new-run fence was rolled back".into());
        }
        let visible = chain.iter().any(|x| x.id == d.id);
        let roots: Vec<_> = sim
            .applied()
            .into_iter()
            .filter(|a| a.key.ends_with("catalog.json"))
            .collect();
        let old_step = roots
            .iter()
            .find(|a| ClusterCatalog::decode(&a.bytes).is_ok_and(|c| c.commit_id == d.id))
            .map(|a| a.step);
        let fence_step = roots
            .iter()
            .find(|a| a.etag == fence.etag())
            .map(|a| a.step);
        match (visible, old_step) {
            (true, Some(step)) if fence_step.is_some_and(|f| step < f) => {}
            (false, None) => {}
            _ => {
                return Err(format!(
                    "old PUT visible={visible} applied_at={old_step:?} fence_at={fence_step:?}"
                ));
            }
        }
        match &outcome {
            Ok(PublicationOutcome::Terminal(x)) if !visible || x.id != d.id => {
                return Err("claimed commit is not visible".into());
            }
            Ok(PublicationOutcome::NeedsRecompute) | Err(_) if visible => {
                return Err("visible commit reported not applied".into());
            }
            _ => {}
        }
        // The old run can no longer publish against the fenced root.
        let fresh = s
            .load_cluster_catalog()
            .await
            .map_err(|e| format!("{e:?}"))?;
        let retry = PreparedPublication::prepare_fenced(
            fresh.clone(),
            fresh.catalog().tables.clone(),
            decision(&fresh, 0, 1, Uuid::from_u128(11)),
            old,
        );
        if !matches!(retry, Err(StorageError::Conflict)) {
            return Err("old run prepared against the new fence".into());
        }
        Ok(visible)
    })
}

#[test]
fn t05_fence_orders_against_held_put() {
    assert!(
        !t05(0, true).unwrap(),
        "a PUT released after the fence applied"
    );
}

#[test]
fn t05_fence_orders_against_delayed_puts() {
    nabedra_check::prop::check("olap_t05_delay", |g| t05(1 + g.below(12), false).map(drop));
    // Both orders must occur across the delays, or the property tests nothing.
    let orders: BTreeSet<bool> = (1..13).map(|delay| t05(delay, false).unwrap()).collect();
    assert_eq!(
        orders.len(),
        2,
        "only one PUT/fence order occurred: {orders:?}"
    );
}

/// Planted bug 1 (T04): an adapter that reports a lost reply as Conflict makes
/// the coordinator say not_applied for an applied commit, and makes publication
/// claim NeedsRecompute for a late PUT that later applies. Both must be caught.
#[test]
fn planted_false_not_applied_is_caught() {
    let caught = current_thread().block_on(coordinator_t04(Bug::AmbiguousAsConflict, false));
    assert!(
        caught
            .as_ref()
            .is_err_and(|e| e.contains("false not_applied")),
        "coordinator planted bug not caught: {caught:?}"
    );
    let found = (0..64).find_map(|seed| {
        let p = Plan {
            writers: 3,
            commits: 3,
            fault_seed: seed,
            per_mille: 150,
            interleave_seed: seed,
        };
        lockstep(&p, Bug::AmbiguousAsConflict)
            .err()
            .filter(|e| e.contains("reported not applied but it applied"))
    });
    assert!(
        found.is_some(),
        "publication planted bug not caught in 64 seeds"
    );
    println!("planted false not_applied caught: {}", found.unwrap());
}

/// Planted bug 2 (T03): an adapter that ignores If-Match lets a CAS loser
/// overwrite the winner. The lock-step check must catch it without faults.
#[test]
fn planted_cas_loser_overwrite_is_caught() {
    let found = (0..32).find_map(|seed| {
        let p = Plan {
            writers: 3,
            commits: 2,
            fault_seed: 0,
            per_mille: 0,
            interleave_seed: seed,
        };
        lockstep(&p, Bug::IgnoreMatch)
            .err()
            .filter(|e| e.contains("reachable"))
    });
    assert!(found.is_some(), "CAS overwrite not caught in 32 seeds");
    println!("planted CAS overwrite caught: {}", found.unwrap());
}
