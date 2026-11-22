//! The `ObjectStore` contract suite (storage.rs trait docs), in the style of Delta
//! Lake's `LogStoreSuiteBase`: one generic suite runs against every store.
//!
//! Targets:
//! - `SimObjectStore`, always.
//! - `HttpStore` against `scripts/local_store.py --test-faults`, when Python 3.11+
//!   is available; otherwise each case prints `not-run: python-unavailable`.
//! - `S3Store`, only when `NABEDRA_TEST_S3_BUCKET` is set; otherwise the test
//!   prints `not-run: s3-unconfigured` and makes no assertion.
//!
//! A case that fails on a fake but passes on S3 marks the fake wrong. A case that
//! passes on a fake but fails on S3 marks the contract wrong.
use super::{
    Download, Namespace, ObjectStore, StorageError, WriteCondition,
    sim::{Fault, Rule, SimObjectStore},
};
use std::{
    future::Future,
    io::BufRead,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::io::AsyncReadExt;
use uuid::Uuid;

/// What a contract case needs beyond `ObjectStore`. Optional operations return
/// `None` when the store has no such operation, and the case reports not-run.
pub(crate) trait Target: ObjectStore + Clone + 'static {
    fn delete(&self, key: &str) -> impl Future<Output = Option<Result<(), StorageError>>> + Send;
    fn list(
        &self,
        prefix: &str,
    ) -> impl Future<Output = Option<Result<Vec<String>, StorageError>>> + Send;
    /// Arrange `fault` (the `X-Nabedra-Test-Fault` vocabulary) for the next
    /// operation on `key`. False when the store cannot inject faults.
    fn inject(&self, key: &str, fault: &str) -> bool;
}

impl Target for SimObjectStore {
    async fn delete(&self, key: &str) -> Option<Result<(), StorageError>> {
        Some(SimObjectStore::delete(self, key, None))
    }
    async fn list(&self, prefix: &str) -> Option<Result<Vec<String>, StorageError>> {
        Some(Ok(SimObjectStore::list(self, prefix)))
    }
    fn inject(&self, key: &str, fault: &str) -> bool {
        let (op, fault) = match fault {
            "lost-response" => (true, Fault::LostResponse),
            "embedded-error" => (true, Fault::EmbeddedError),
            f if f.starts_with("delay-apply=") => (true, Fault::DelayApply { steps: 2 }),
            f if f.starts_with("get-status=") => (false, Fault::Status(f[11..].parse().unwrap())),
            f if f.starts_with("status=") => (true, Fault::Status(f[7..].parse().unwrap())),
            _ => return false,
        };
        self.inject(if op {
            Rule::put(key, fault)
        } else {
            Rule::get(key, fault)
        });
        true
    }
}

impl Target for super::HttpStore {
    async fn delete(&self, _: &str) -> Option<Result<(), StorageError>> {
        None
    }
    async fn list(&self, _: &str) -> Option<Result<Vec<String>, StorageError>> {
        None
    }
    fn inject(&self, _: &str, fault: &str) -> bool {
        // local_store.py spells GET faults as plain status faults.
        self.push_test_fault(match fault {
            "embedded-error" => "status=200",
            f => f.strip_prefix("get-").unwrap_or(f),
        });
        true
    }
}

#[cfg(feature = "s3")]
impl Target for super::S3Store {
    async fn delete(&self, key: &str) -> Option<Result<(), StorageError>> {
        Some(self.test_delete(key).await)
    }
    async fn list(&self, prefix: &str) -> Option<Result<Vec<String>, StorageError>> {
        Some(self.test_list(prefix).await)
    }
    fn inject(&self, _: &str, _: &str) -> bool {
        false
    }
}

/// Counts every request a case sends, for the S3 cost record.
#[derive(Clone)]
pub(crate) struct Counted<T> {
    inner: T,
    count: Arc<AtomicU64>,
}
impl<T: Target> ObjectStore for Counted<T> {
    async fn get(&self, key: &str) -> Result<Download, StorageError> {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.inner.get(key).await
    }
    async fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        condition: WriteCondition<'_>,
    ) -> Result<String, StorageError> {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.inner.put(key, bytes, condition).await
    }
}
impl<T: Target> Target for Counted<T> {
    async fn delete(&self, key: &str) -> Option<Result<(), StorageError>> {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.inner.delete(key).await
    }
    async fn list(&self, prefix: &str) -> Option<Result<Vec<String>, StorageError>> {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.inner.list(prefix).await
    }
    fn inject(&self, key: &str, fault: &str) -> bool {
        self.inner.inject(key, fault)
    }
}

/// Makes one store per case, each with its own fresh namespace.
pub(crate) trait Factory {
    type Store: Target;
    /// Returns the store and its namespace prefix (ending in `/`).
    fn store(&self, case: &str) -> impl Future<Output = (Self::Store, String)>;
}

pub(crate) enum Verdict {
    Pass,
    NotRun(&'static str),
    Fail(String),
}
type CaseResult = Result<Verdict, String>;

async fn read<T: ObjectStore>(s: &T, key: &str) -> Result<(Vec<u8>, String), StorageError> {
    let mut d = s.get(key).await?;
    let mut bytes = Vec::new();
    d.body
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| StorageError::Unavailable)?;
    Ok((bytes, d.etag))
}
fn heartbeat(ns: &str) -> String {
    format!("{ns}cluster/heartbeats/{}.json", Uuid::new_v4())
}
fn decision(ns: &str) -> String {
    format!("{ns}decisions/{}.json", Uuid::new_v4())
}
fn ensure(ok: bool, msg: impl FnOnce() -> String) -> Result<(), String> {
    if ok { Ok(()) } else { Err(msg()) }
}
/// A definite failed condition: nothing applied, and the store says so.
fn definite_refusal(r: &Result<String, StorageError>) -> bool {
    matches!(
        r,
        Err(StorageError::Conflict | StorageError::NotFound | StorageError::Unavailable)
    )
}

/// N clients race `If-None-Match: *` on one key: exactly one wins, and the
/// object holds the winner's bytes with the winner's version.
async fn create_race<T: Target>(s: T, ns: String) -> CaseResult {
    const N: usize = 8;
    let key = heartbeat(&ns);
    let barrier = Arc::new(tokio::sync::Barrier::new(N));
    let mut tasks = Vec::new();
    for i in 0..N {
        let (s, key, barrier) = (s.clone(), key.clone(), barrier.clone());
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            (
                i,
                s.put(
                    &key,
                    format!("writer-{i}").into_bytes(),
                    WriteCondition::Create,
                )
                .await,
            )
        }));
    }
    let mut winners = Vec::new();
    for t in tasks {
        let (i, r) = t.await.map_err(|e| e.to_string())?;
        match r {
            Ok(etag) => winners.push((i, etag)),
            Err(StorageError::Conflict) => {}
            other => return Err(format!("writer {i}: unexpected {other:?}")),
        }
    }
    ensure(winners.len() == 1, || format!("{} winners", winners.len()))?;
    let (bytes, etag) = read(&s, &key).await.map_err(|e| format!("get: {e:?}"))?;
    let (i, won) = &winners[0];
    ensure(
        bytes == format!("writer-{i}").into_bytes() && &etag == won,
        || "stored object is not the winner's".into(),
    )?;
    Ok(Verdict::Pass)
}

/// GET returns the latest acknowledged write and its version. Distinct
/// contents produce distinct versions and a stale `If-Match` changes nothing.
async fn versions_and_stale_match<T: Target>(s: T, ns: String) -> CaseResult {
    let key = heartbeat(&ns);
    let mut current_bytes = b"revision-0".to_vec();
    let first = s
        .put(&key, current_bytes.clone(), WriteCondition::Create)
        .await
        .map_err(|e| format!("create: {e:?}"))?;
    let mut seen = vec![first.clone()];
    let mut current = first.clone();
    for round in 1..=3 {
        let got = read(&s, &key).await.map_err(|e| format!("get: {e:?}"))?;
        ensure(got == (current_bytes, current.clone()), || {
            format!("round {round}: GET is not the last acknowledged write")
        })?;
        current_bytes = format!("revision-{round}").into_bytes();
        let next = s
            .put(&key, current_bytes.clone(), WriteCondition::Match(&current))
            .await
            .map_err(|e| format!("round {round} match: {e:?}"))?;
        ensure(!seen.contains(&next), || {
            format!("round {round}: version {next} repeated for distinct bytes")
        })?;
        seen.push(next.clone());
        current = next;
    }
    let stale = s
        .put(&key, b"stale".to_vec(), WriteCondition::Match(&first))
        .await;
    ensure(matches!(stale, Err(StorageError::Conflict)), || {
        format!("stale If-Match returned {stale:?}")
    })?;
    let got = read(&s, &key).await.map_err(|e| format!("get: {e:?}"))?;
    ensure(got == (current_bytes, current), || {
        "stale If-Match changed the object".into()
    })?;
    Ok(Verdict::Pass)
}

/// `If-Match` against a key that never existed fails definitely and creates nothing.
async fn match_missing<T: Target>(s: T, ns: String) -> CaseResult {
    let key = heartbeat(&ns);
    let r = s
        .put(&key, b"x".to_vec(), WriteCondition::Match("\"g999999999\""))
        .await;
    ensure(definite_refusal(&r), || {
        format!("If-Match on missing key: {r:?}")
    })?;
    let got = read(&s, &key).await;
    ensure(matches!(got, Err(StorageError::NotFound)), || {
        format!("missing key after refused write: {:?}", got.map(|g| g.1))
    })?;
    Ok(Verdict::Pass)
}

/// After a delete, `If-Match` with the deleted version fails and recreates nothing.
async fn match_after_delete<T: Target>(s: T, ns: String) -> CaseResult {
    let key = heartbeat(&ns);
    let etag = s
        .put(&key, b"x".to_vec(), WriteCondition::Create)
        .await
        .map_err(|e| format!("create: {e:?}"))?;
    match s.delete(&key).await {
        None => return Ok(Verdict::NotRun("delete-not-supported")),
        Some(r) => r.map_err(|e| format!("delete: {e:?}"))?,
    }
    let r = s
        .put(&key, b"y".to_vec(), WriteCondition::Match(&etag))
        .await;
    ensure(definite_refusal(&r), || {
        format!("If-Match after delete: {r:?}")
    })?;
    let got = read(&s, &key).await;
    ensure(matches!(got, Err(StorageError::NotFound)), || {
        "deleted key reappeared".into()
    })?;
    Ok(Verdict::Pass)
}

/// Conditional writers race a delete. At most one writer wins before deletion
/// and the key stays absent after all contenders finish.
async fn delete_race<T: Target>(s: T, ns: String) -> CaseResult {
    const N: usize = 4;
    let key = heartbeat(&ns);
    let etag = s
        .put(&key, b"base".to_vec(), WriteCondition::Create)
        .await
        .map_err(|e| format!("create: {e:?}"))?;
    match s.delete(&format!("{ns}cluster/control.json")).await {
        None => return Ok(Verdict::NotRun("delete-not-supported")),
        Some(result) => result.map_err(|e| format!("delete probe: {e:?}"))?,
    }
    let barrier = Arc::new(tokio::sync::Barrier::new(N + 1));
    let mut tasks = Vec::new();
    for i in 0..N {
        let (s, key, etag, barrier) = (s.clone(), key.clone(), etag.clone(), barrier.clone());
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            let r = s
                .put(
                    &key,
                    format!("w{i}").into_bytes(),
                    WriteCondition::Match(&etag),
                )
                .await;
            (i, r)
        }));
    }
    let deleter = {
        let (s, key, barrier) = (s.clone(), key.clone(), barrier.clone());
        tokio::spawn(async move {
            barrier.wait().await;
            s.delete(&key).await.unwrap()
        })
    };
    let mut winners = 0;
    for t in tasks {
        let (i, r) = t.await.map_err(|e| e.to_string())?;
        match r {
            Ok(_) => winners += 1,
            // S3 may return 404 when the delete wins. Its PUT adapter maps 404
            // to Unavailable, and the GET assertion below verifies absence.
            Err(StorageError::Conflict | StorageError::NotFound | StorageError::Unavailable) => {}
            other => return Err(format!("writer {i}: {other:?}")),
        }
    }
    deleter
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| format!("delete: {e:?}"))?;
    ensure(winners <= 1, || format!("{winners} winners"))?;
    let got = read(&s, &key).await;
    ensure(matches!(got, Err(StorageError::NotFound)), || {
        format!("GET after the delete race: {:?}", got.map(|(_, etag)| etag))
    })?;
    Ok(Verdict::Pass)
}

/// Every acknowledged write is listed immediately afterwards.
async fn list_after_write<T: Target>(s: T, ns: String) -> CaseResult {
    let mut keys: Vec<String> = (0..3).map(|_| decision(&ns)).collect();
    for k in &keys {
        s.put(k, b"{}".to_vec(), WriteCondition::Create)
            .await
            .map_err(|e| format!("create: {e:?}"))?;
        let listed = match s.list(&format!("{ns}decisions/")).await {
            None => return Ok(Verdict::NotRun("list-not-supported")),
            Some(r) => r.map_err(|e| format!("list: {e:?}"))?,
        };
        ensure(listed.contains(k), || format!("{k} not listed after write"))?;
    }
    keys.sort();
    let listed = s
        .list(&format!("{ns}decisions/"))
        .await
        .unwrap()
        .map_err(|e| format!("list: {e:?}"))?;
    ensure(listed == keys, || {
        format!("listed {listed:?}, wrote {keys:?}")
    })?;
    Ok(Verdict::Pass)
}

/// Faults the fakes inject (sim rules, or `X-Nabedra-Test-Fault` over HTTP) map to
/// the documented errors and apply, or not, exactly as the fault states.
async fn injected_faults<T: Target>(s: T, ns: String) -> CaseResult {
    // (fault, expected PUT result, applied?). An applied write must never be
    // reported as a definite refusal: that is the false not_applied of T04.
    let cases: [(&str, fn(&Result<String, StorageError>) -> bool, bool); 6] = [
        (
            "lost-response",
            |r| matches!(r, Err(StorageError::Ambiguous)),
            true,
        ),
        (
            "status=412",
            |r| matches!(r, Err(StorageError::Conflict)),
            false,
        ),
        // Statuses an adapter does not define may surface conservatively as
        // Ambiguous; only a definite refusal or Ambiguous is acceptable.
        ("status=409", Result::is_err, false),
        (
            "status=503",
            |r| matches!(r, Err(StorageError::Ambiguous)),
            false,
        ),
        ("status=404", Result::is_err, false),
        (
            "embedded-error",
            |r| matches!(r, Err(StorageError::Ambiguous)),
            false,
        ),
    ];
    for (fault, expected, applied) in cases {
        let key = heartbeat(&ns);
        if !s.inject(&key, fault) {
            return Ok(Verdict::NotRun("faults-not-injectable"));
        }
        let r = s.put(&key, b"f".to_vec(), WriteCondition::Create).await;
        ensure(expected(&r), || format!("{fault}: PUT returned {r:?}"))?;
        let got = read(&s, &key).await;
        let expected = if applied {
            Ok(b"f".to_vec())
        } else {
            Err(StorageError::NotFound)
        };
        ensure(got.map(|(bytes, _)| bytes) == expected, || {
            format!("{fault}: GET did not confirm the expected write outcome")
        })?;
        // A fault never consumes the condition: a lost response leaves the object
        // in place, and a refused one leaves the key free.
        let again = s.put(&key, b"g".to_vec(), WriteCondition::Create).await;
        ensure(again.is_ok() != applied, || {
            format!("{fault}: retry {again:?}")
        })?;
    }
    for (fault, expected) in [
        ("get-status=503", StorageError::Unavailable),
        ("get-status=404", StorageError::NotFound),
    ] {
        let key = heartbeat(&ns);
        s.put(&key, b"x".to_vec(), WriteCondition::Create)
            .await
            .map_err(|e| format!("create: {e:?}"))?;
        s.inject(&key, fault);
        let r = read(&s, &key).await.map(|r| r.1);
        ensure(r == Err(expected), || {
            format!("{fault}: GET returned {r:?}")
        })?;
        ensure(read(&s, &key).await.is_ok(), || {
            format!("{fault}: object lost")
        })?;
    }
    // A late PUT: the caller sees Ambiguous now, the write applies later.
    let key = heartbeat(&ns);
    s.inject(&key, "delay-apply=300");
    let r = s.put(&key, b"late".to_vec(), WriteCondition::Create).await;
    ensure(matches!(r, Err(StorageError::Ambiguous)), || {
        format!("delay-apply: PUT returned {r:?}")
    })?;
    let mut applied = false;
    for _ in 0..50 {
        if let Ok((bytes, _)) = read(&s, &key).await {
            ensure(bytes == b"late", || "late PUT applied other bytes".into())?;
            applied = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    ensure(applied, || "late PUT never applied".into())?;
    Ok(Verdict::Pass)
}

/// Runs every case against one factory and returns `(case, verdict)` lines.
pub(crate) async fn run<F: Factory>(f: &F) -> Vec<(&'static str, Verdict)> {
    macro_rules! case {
        ($out:ident, $name:ident) => {{
            let (s, ns) = f.store(stringify!($name)).await;
            let v = match $name(s, ns).await {
                Ok(v) => v,
                Err(msg) => Verdict::Fail(msg),
            };
            $out.push((stringify!($name), v));
        }};
    }
    let mut out = Vec::new();
    case!(out, create_race);
    case!(out, versions_and_stale_match);
    case!(out, match_missing);
    case!(out, match_after_delete);
    case!(out, delete_race);
    case!(out, list_after_write);
    case!(out, injected_faults);
    // The trait has no multipart upload, so the multipart case has nothing to test.
    out.push((
        "multipart_conditions",
        Verdict::NotRun("multipart-not-in-trait"),
    ));
    out
}

/// Prints the verdicts and panics on any failure.
pub(crate) fn report(target: &str, verdicts: &[(&'static str, Verdict)]) {
    let mut failed = Vec::new();
    for (case, v) in verdicts {
        match v {
            Verdict::Pass => println!("contract {target} {case}: pass"),
            Verdict::NotRun(why) => println!("contract {target} {case}: not-run: {why}"),
            Verdict::Fail(msg) => {
                println!("contract {target} {case}: FAIL: {msg}");
                failed.push(format!("{case}: {msg}"));
            }
        }
    }
    assert!(failed.is_empty(), "{target} contract failures: {failed:?}");
}

struct SimFactory(SimObjectStore);
impl Factory for SimFactory {
    type Store = SimObjectStore;
    async fn store(&self, case: &str) -> (SimObjectStore, String) {
        (self.0.clone(), format!("contract/{case}/"))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn contract_sim() {
    let verdicts = run(&SimFactory(SimObjectStore::new())).await;
    report("sim", &verdicts);
    // The sim supports every optional operation, so no case may be skipped there.
    let skipped: Vec<_> = verdicts
        .iter()
        .filter(|(c, v)| matches!(v, Verdict::NotRun(_)) && *c != "multipart_conditions")
        .map(|(c, _)| *c)
        .collect();
    assert!(skipped.is_empty(), "sim skipped {skipped:?}");
}

/// A sim-only deterministic delete race: a conditional PUT that is in flight when
/// its key is deleted fails with Conflict (S3's 409), and nothing is resurrected.
#[tokio::test]
async fn sim_conflict_on_concurrent_delete() {
    let s = SimObjectStore::new();
    let key = "contract/del/cluster/control.json";
    let etag = s
        .put(key, b"a".to_vec(), WriteCondition::Create)
        .await
        .unwrap();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    s.inject(Rule::put(key, Fault::Hold(gate.clone())));
    let held = {
        let (s, etag) = (s.clone(), etag.clone());
        tokio::spawn(async move {
            s.put(key, b"b".to_vec(), WriteCondition::Match(&etag))
                .await
        })
    };
    tokio::task::yield_now().await;
    while gate.available_permits() == 0 && s.requests() < 2 {
        tokio::task::yield_now().await;
    }
    SimObjectStore::delete(&s, key, None).unwrap();
    // Recreate with the same bytes: the held PUT must still lose.
    s.put(key, b"a".to_vec(), WriteCondition::Create)
        .await
        .unwrap();
    gate.add_permits(1);
    assert_eq!(held.await.unwrap(), Err(StorageError::Conflict));
    assert_eq!(s.object(key).unwrap().0, b"a");
    assert_eq!(
        SimObjectStore::delete(&s, "contract/del/catalog.json", Some("\"s1\"")),
        Err(StorageError::NotFound)
    );
}

/// Seeded random faults keep the safety half of the contract: no acknowledged
/// write is lost, no refused write appears, and versions never repeat.
#[test]
fn sim_seeded_faults_keep_safety() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    nabedra_check::prop::check("sim_seeded_faults_keep_safety", |g| {
        rt.block_on(async {
            let s = SimObjectStore::new();
            s.random_faults(
                g.u64(),
                300,
                vec![
                    Fault::Status(412),
                    Fault::Status(409),
                    Fault::Status(503),
                    Fault::LostResponse,
                    Fault::DelayApply { steps: 3 },
                    Fault::EmbeddedError,
                ],
            );
            let key = "p/catalog.json";
            let mut known: Option<String> = None;
            let mut acked = std::collections::BTreeSet::new();
            for step in 0..(5 + g.below(30)) {
                let bytes = format!("v{step}").into_bytes();
                let r = match &known {
                    None => s.put(key, bytes, WriteCondition::Create).await,
                    Some(e) => s.put(key, bytes, WriteCondition::Match(e)).await,
                };
                if let Ok(e) = &r {
                    if !acked.insert(e.clone()) {
                        return Err(format!("version {e} repeated"));
                    }
                    let now = s.object(key).map(|o| o.1);
                    // A queued late PUT may apply right after; only a later
                    // version may replace the acknowledged one.
                    if now.as_deref() != Some(e.as_str()) && s.delayed_len() == 0 {
                        return Err(format!("acknowledged {e} not current: {now:?}"));
                    }
                }
                // Reconcile like a careful caller: re-read the version.
                known = match read(&s, key).await {
                    Ok((_, e)) => Some(e),
                    Err(StorageError::NotFound) => None,
                    Err(_) => known,
                };
            }
            s.apply_delayed();
            let versions: Vec<_> = s.applied().into_iter().map(|a| a.etag).collect();
            let unique: std::collections::BTreeSet<_> = versions.iter().collect();
            if unique.len() != versions.len() {
                return Err("an applied version repeated".into());
            }
            Ok(())
        })
    });
}

fn python() -> Option<String> {
    let candidates = std::env::var("NABEDRA_PYTHON")
        .into_iter()
        .chain(["python3.11", "python3.12", "python3.13", "python3"].map(String::from));
    for p in candidates {
        let ok = std::process::Command::new(&p)
            .args([
                "-c",
                "import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .is_ok_and(|s| s.success());
        if ok {
            return Some(p);
        }
    }
    None
}

/// `scripts/local_store.py --test-faults` on a free port, killed on drop.
pub(crate) struct LocalStore {
    child: std::process::Child,
    dir: std::path::PathBuf,
    pub url: String,
}
impl LocalStore {
    pub fn start() -> Option<Self> {
        let python = python()?;
        let dir = std::env::temp_dir().join(format!("nabedra-contract-{}", Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let script = concat!(env!("CARGO_MANIFEST_DIR"), "/../scripts/local_store.py");
        let mut child = std::process::Command::new(python)
            .args(["-B", script, "--test-faults", "--port", "0", "--database"])
            .arg(dir.join("objects.sqlite"))
            .env("NABEDRA_STORE_TOKEN", "contract-secret")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let mut line = String::new();
        std::io::BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        let v: serde_json::Value = serde_json::from_str(&line).expect("local store address");
        Some(Self {
            child,
            dir,
            url: format!("http://127.0.0.1:{}", v["port"]),
        })
    }
}
impl Drop for LocalStore {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
struct HttpFactory(String);
impl Factory for HttpFactory {
    type Store = super::HttpStore;
    async fn store(&self, case: &str) -> (super::HttpStore, String) {
        let ns = format!("contract/{case}-{}", Uuid::new_v4());
        let store = super::HttpStore::new(
            &self.0,
            Namespace::new(&ns).unwrap(),
            "contract-secret",
            None,
        )
        .unwrap();
        (store, format!("{ns}/"))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn contract_local_store() {
    let Some(local) = LocalStore::start() else {
        println!("not-run: python-unavailable (needs Python 3.11 or later)");
        return;
    };
    report("local", &run(&HttpFactory(local.url.clone())).await);
}

/// Run prefix for real S3: `runs/<request_id>/`, from NABEDRA_REQUEST_ID or a new UUID.
pub(crate) fn s3_run_prefix() -> String {
    let id = std::env::var("NABEDRA_REQUEST_ID")
        .ok()
        .filter(|id| {
            !id.is_empty()
                && id.len() <= 128
                && id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        })
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    format!("runs/{id}")
}

#[cfg(feature = "s3")]
struct S3Factory {
    bucket: String,
    region: String,
    prefix: String,
    count: Arc<AtomicU64>,
}
#[cfg(feature = "s3")]
impl Factory for S3Factory {
    type Store = Counted<super::S3Store>;
    async fn store(&self, case: &str) -> (Self::Store, String) {
        let ns = format!("{}/contract/{case}-{}", self.prefix, Uuid::new_v4());
        let inner = super::S3Store::connect_region(
            &self.bucket,
            Namespace::new(&ns).unwrap(),
            Some(&self.region),
        )
        .await
        .expect("S3 test bucket configuration");
        (
            Counted {
                inner,
                count: self.count.clone(),
            },
            format!("{ns}/"),
        )
    }
}

#[cfg(feature = "s3")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn contract_s3() {
    let Some(bucket) = std::env::var("NABEDRA_TEST_S3_BUCKET")
        .ok()
        .filter(|bucket| !bucket.is_empty())
    else {
        println!("not-run: s3-unconfigured");
        return;
    };
    let region = std::env::var("NABEDRA_TEST_S3_REGION").unwrap_or_else(|_| "us-east-1".into());
    let factory = S3Factory {
        bucket,
        region,
        prefix: s3_run_prefix(),
        count: Arc::default(),
    };
    let verdicts = run(&factory).await;
    // Print the cost line before any assertion, so failed runs are counted too.
    println!(
        "nabedra-s3-requests: {} prefix={}/",
        factory.count.load(Ordering::Relaxed),
        factory.prefix
    );
    report("s3", &verdicts);
}
