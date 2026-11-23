//! NS6: the pool on `nabedra-kv`. Fault and crash scenarios run the store on
//! `FaultFs`; their power-loss evidence is a fault model, not a physical cut.
use super::*;
use nabedra_kv::fs::{Fault, FaultError, FaultFs, Fs, FsFile, FsLock, FsOp, OpenMode};
use std::sync::atomic::AtomicUsize;

fn fault_pool(
    dir: &Dir,
    fault: &FaultFs,
    identity: PoolIdentity,
    shared: Arc<WorkerSharedResources>,
) -> OltpPool {
    fs::create_dir_all(&dir.0).unwrap();
    fault.create_dir_all_durable(&dir.0);
    let fs: Arc<dyn Fs> = Arc::new(fault.clone());
    OltpPool::open_development_on(&dir.0, identity, shared, fs).unwrap()
}
fn reopen(
    dir: &Dir,
    fault: &FaultFs,
    identity: PoolIdentity,
    shared: Arc<WorkerSharedResources>,
) -> Result<OltpPool, ProtocolError> {
    let fs: Arc<dyn Fs> = Arc::new(fault.clone());
    OltpPool::open_development_on(&dir.0, identity, shared, fs)
}
fn journal_dir(dir: &Dir) -> PathBuf {
    dir.0.join("kv").join("journal")
}
fn put_entry(
    index: u64,
    key: &'static [u8],
    value: Vec<u8>,
) -> impl FnOnce(&mut GroupTransaction<'_>) -> Result<Transition, ProtocolError> + Send + 'static {
    move |tx| {
        tx.put(key, &value)?;
        Ok(Transition::Commit(tx.copy_output(&index.to_be_bytes())?))
    }
}

/// T4 contract (lead note, items 2 and 3): a rejected entry writes none of its
/// transaction's batch, and redelivering an applied position returns the
/// stored reply without running the transition again, also after a reopen.
#[tokio::test]
async fn o11_rejected_entry_writes_nothing_and_redelivery_returns_the_stored_reply() {
    let dir = Dir::new();
    let identity = pool_identity();
    let resources = shared(SharedResourceConfig::default());
    let group_id;
    {
        let pool = OltpPool::open_development(&dir.0, identity.clone(), resources.clone()).unwrap();
        group_id = group_identity(&pool);
        let group = pool.group(group_id.clone()).await.unwrap();
        group
            .transaction(meta(0), budget(), put_entry(0, b"row", b"kept".to_vec()))
            .await
            .unwrap();
        let reply = group
            .transaction(meta(1), budget(), |tx| {
                tx.put(b"row", b"rejected")?;
                tx.put(b"other", b"rejected")?;
                tx.delete(b"row")?;
                Ok(Transition::Reject(tx.copy_output(b"conflict")?))
            })
            .await
            .unwrap();
        assert_eq!(reply.as_slice(), b"conflict");
        assert_eq!(get(&group, b"row").await, b"kept");
        assert!(
            group
                .read(budget(), |view| view
                    .get(b"other")
                    .and_then(|v| v.ok_or_else(capacity)))
                .await
                .is_err()
        );
        let replay = group
            .transaction(meta(1), budget(), |_| {
                panic!("redelivery reran the transition")
            })
            .await
            .unwrap();
        assert_eq!(replay.as_slice(), b"conflict");
    }
    let pool = OltpPool::open_development(&dir.0, identity, resources.clone()).unwrap();
    let group = pool.retained_group(group_id).await.unwrap();
    let replay = group
        .transaction(meta(1), budget(), |_| {
            panic!("redelivery reran the transition")
        })
        .await
        .unwrap();
    assert_eq!(replay.as_slice(), b"conflict");
    let replay = group
        .transaction(meta(0), budget(), |_| panic!("stale redelivery ran"))
        .await;
    // An older position is not the latest applied entry: refused, never rerun.
    assert!(replay.is_err());
    assert_eq!(get(&group, b"row").await, b"kept");
    assert_eq!(
        group
            .applied_state(budget())
            .await
            .unwrap()
            .unwrap()
            .position,
        meta(1).position
    );
}

/// O29 and NS09 at pool level, injected through FaultFs: EIO on the journal
/// sync after the record was written. The write's outcome is unknown, every
/// alias of the pool refuses work, an independent pool keeps serving, nothing
/// retries, and a reopen after a crash recovers from disk only.
#[tokio::test]
async fn o29_ns09_journal_sync_eio_latches_every_alias_and_reopen_recovers_from_disk() {
    let dir = Dir::new();
    let fault = FaultFs::new();
    let identity = pool_identity();
    let resources = shared(SharedResourceConfig::default());
    let pool = fault_pool(&dir, &fault, identity.clone(), resources.clone());
    let group_id = group_identity(&pool);
    let group = pool.group(group_id.clone()).await.unwrap();
    let alias_pool = pool.clone();
    let alias = alias_pool.group(group_id.clone()).await.unwrap();
    let healthy_dir = Dir::new();
    let healthy = OltpPool::open_development(
        &healthy_dir.0,
        PoolIdentity {
            pool_id: nabedra_rt::ids::uuid_v4(),
            ..identity.clone()
        },
        resources.clone(),
    )
    .unwrap();
    let independent = healthy.group(group_identity(&healthy)).await.unwrap();
    group
        .transaction(
            meta(0),
            budget(),
            put_entry(0, b"row/original", b"original".to_vec()),
        )
        .await
        .unwrap();
    let baseline = resources.scheduler().usage();
    let visible = pool.inner.store.visible_seq();
    fault.inject(
        Fault::on(FsOp::SyncData)
            .path_prefix(journal_dir(&dir))
            .error(FaultError::Eio),
    );
    let result = group
        .transaction(
            meta(1),
            budget(),
            put_entry(1, b"row/new", b"ambiguous".to_vec()),
        )
        .await;
    assert_eq!(result.err().unwrap(), recovery_unknown());
    assert!(pool.requires_recovery());
    assert!(alias_pool.requires_recovery());
    assert!(!healthy.requires_recovery());
    // The failed write never became visible.
    assert_eq!(pool.inner.store.visible_seq(), visible);
    assert!(
        pool.get_latest(
            &[
                group.prefix(&pool.snapshot(), false).unwrap(),
                b"row/new".to_vec()
            ]
            .concat()
        )
        .unwrap()
        .is_none()
    );
    let callbacks = Arc::new(AtomicUsize::new(0));
    for candidate in [&group, &alias] {
        let observed = callbacks.clone();
        let read = candidate.read(budget(), move |view| {
            observed.fetch_add(1, Ordering::Relaxed);
            view.copy_output(b"unexpected")
        });
        assert_eq!(read.await.err().unwrap(), recovery_unknown());
        let observed = callbacks.clone();
        let retry = candidate.transaction(meta(1), budget(), move |tx| {
            observed.fetch_add(1, Ordering::Relaxed);
            Ok(Transition::Commit(tx.copy_output(b"unexpected")?))
        });
        assert_eq!(retry.await.err().unwrap(), recovery_unknown());
    }
    assert_eq!(callbacks.load(Ordering::Relaxed), 0);
    assert_eq!(
        alias_pool.group(group_id.clone()).await.err().unwrap(),
        recovery_unknown()
    );
    // Nothing retried the sync.
    let syncs = fault
        .calls()
        .iter()
        .filter(|call| {
            call.op == FsOp::SyncData && call.path.starts_with(journal_dir(&dir)) && !call.ok
        })
        .count();
    assert_eq!(syncs, 1);
    independent
        .transaction(
            meta(0),
            budget(),
            put_entry(0, b"value", b"healthy".to_vec()),
        )
        .await
        .unwrap();
    assert_eq!(get(&independent, b"value").await, b"healthy");
    assert_eq!(resources.control_bytes_used(), 0);
    assert_eq!(resources.scheduler().usage(), baseline);
    drop((group, alias, pool, alias_pool));

    // fsyncgate: the failed sync's bytes never reach the disk.
    let fault = fault.crash();
    let pool = reopen(&dir, &fault, identity, resources.clone()).unwrap();
    assert!(!pool.requires_recovery());
    let group = pool.retained_group(group_id).await.unwrap();
    assert_eq!(get(&group, b"row/original").await, b"original");
    assert_eq!(
        group
            .applied_state(budget())
            .await
            .unwrap()
            .unwrap()
            .position,
        meta(0).position
    );
    let applied = group
        .transaction(
            meta(1),
            budget(),
            put_entry(1, b"row/new", b"after-recovery".to_vec()),
        )
        .await
        .unwrap();
    assert_eq!(applied.as_slice(), 1u64.to_be_bytes());
    assert_eq!(get(&group, b"row/new").await, b"after-recovery");
}

/// A FaultFs whose journal `sync_data` waits while a gate is closed, so a test
/// can hold commits in the journal thread.
#[derive(Clone)]
struct Gated {
    inner: FaultFs,
    closed: Arc<(Mutex<bool>, Condvar)>,
}
struct GatedFile {
    inner: Arc<dyn FsFile>,
    closed: Arc<(Mutex<bool>, Condvar)>,
}
impl Gated {
    fn set(&self, closed: bool) {
        *self.closed.0.lock().unwrap() = closed;
        self.closed.1.notify_all();
    }
}
impl Fs for Gated {
    fn open(&self, path: &Path, mode: OpenMode) -> std::io::Result<Arc<dyn FsFile>> {
        Ok(Arc::new(GatedFile {
            inner: self.inner.open(path, mode)?,
            closed: self.closed.clone(),
        }))
    }
    fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()> {
        self.inner.rename(from, to)
    }
    fn remove(&self, path: &Path) -> std::io::Result<()> {
        self.inner.remove(path)
    }
    fn exists(&self, path: &Path) -> std::io::Result<bool> {
        self.inner.exists(path)
    }
    fn create_dir_all(&self, dir: &Path) -> std::io::Result<()> {
        self.inner.create_dir_all(dir)
    }
    fn list(&self, dir: &Path) -> std::io::Result<Vec<PathBuf>> {
        self.inner.list(dir)
    }
    fn sync_dir(&self, dir: &Path) -> std::io::Result<()> {
        self.inner.sync_dir(dir)
    }
    fn lock(&self, path: &Path) -> std::io::Result<FsLock> {
        self.inner.lock(path)
    }
}
impl FsFile for GatedFile {
    fn write_all_at(&self, buf: &[u8], offset: u64) -> std::io::Result<()> {
        self.inner.write_all_at(buf, offset)
    }
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
        self.inner.read_exact_at(buf, offset)
    }
    fn sync_data(&self) -> std::io::Result<()> {
        let mut closed = self.closed.0.lock().unwrap();
        while *closed {
            closed = self.closed.1.wait(closed).unwrap();
        }
        drop(closed);
        self.inner.sync_data()
    }
    fn sync_all(&self) -> std::io::Result<()> {
        self.inner.sync_all()
    }
    fn preallocate(&self, len: u64) -> std::io::Result<()> {
        self.inner.preallocate(len)
    }
    fn set_len(&self, len: u64) -> std::io::Result<()> {
        self.inner.set_len(len)
    }
    fn len(&self) -> std::io::Result<u64> {
        self.inner.len()
    }
}

/// NS08 (pool level), O05, O06: 64 groups x 4 lanes (state apply unsynced,
/// synced log, synced log metadata, unsynced commit hint) write concurrently
/// on one pool with 1 query and 1 control executor thread. Journal fsyncs are
/// fewer than synced commits; while a journal sync is held, synced writers wait
/// without an executor slot, so reads on both lanes still run; a final synced
/// write makes every earlier unsynced write durable under a crash; and the
/// counters reach the worker stats.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ns08_o05_o06_group_commit_shares_fsyncs_across_64_groups_and_4_lanes() {
    const GROUPS: usize = 64;
    const ROUNDS: u64 = 3;
    let dir = Dir::new();
    let fault = FaultFs::new();
    fs::create_dir_all(&dir.0).unwrap();
    fault.create_dir_all_durable(&dir.0);
    let gated = Gated {
        inner: fault.clone(),
        closed: Arc::new((Mutex::new(false), Condvar::new())),
    };
    let identity = pool_identity();
    let config = SharedResourceConfig::default();
    assert_eq!((config.query_threads, config.control_threads), (1, 1));
    let resources = shared(config);
    let pool = OltpPool::open_development_on(
        &dir.0,
        identity.clone(),
        resources.clone(),
        Arc::new(gated.clone()),
    )
    .unwrap();
    let mut groups = Vec::new();
    let mut deferrals = Vec::new();
    for _ in 0..GROUPS {
        let group = pool.group(group_identity(&pool)).await.unwrap();
        deferrals.push(group.defer_apply_sync(&group).unwrap());
        groups.push(group);
    }
    let before = pool.inner.store.stats();
    let mut tasks = Vec::new();
    for (n, group) in groups.iter().enumerate() {
        for lane in 0..4u8 {
            let group = group.clone();
            tasks.push(nabedra_rt::task::spawn(async move {
                for round in 0..ROUNDS {
                    let value = format!("{n}/{lane}/{round}").into_bytes();
                    let key: &'static [u8] = match lane {
                        0 => b"state",
                        1 => b"log",
                        2 => b"vote",
                        _ => b"committed",
                    };
                    let work = put_entry(round, key, value);
                    match lane {
                        0 => group.transaction(meta(round), budget(), work).await,
                        1 => group.log_transaction(budget(), work).await,
                        2 => group.log_transaction_metadata(budget(), work).await,
                        _ => {
                            group
                                .log_transaction_metadata_unsynced(budget(), work)
                                .await
                        }
                    }
                    .unwrap();
                }
            }));
        }
    }
    for task in tasks {
        task.await.unwrap();
    }
    let after = pool.inner.store.stats();
    let syncs = after.journal_syncs - before.journal_syncs;
    let synced = after.synced_commits - before.synced_commits;
    let commits = after.commits - before.commits;
    eprintln!(
        "ns08: {commits} commits, {synced} synced commits, {syncs} journal fsyncs ({:.3} fsyncs per synced commit)",
        syncs as f64 / synced as f64
    );
    assert_eq!(synced, GROUPS as u64 * 2 * ROUNDS);
    assert!(syncs < synced, "{syncs} fsyncs for {synced} synced commits");
    for group in &groups {
        assert_eq!(group.write_stats().deferred_writes, 2 * ROUNDS);
        assert_eq!(group.write_stats().synced_requests, 2 * ROUNDS);
    }

    // Hold the journal sync: synced writers on every Query-lane group wait for
    // storage, yet the single query and control threads still serve reads.
    gated.set(true);
    let mut waiting = Vec::new();
    for group in groups.iter().take(8) {
        let group = group.clone();
        waiting.push(nabedra_rt::task::spawn(async move {
            group
                .log_transaction(budget(), put_entry(9, b"log", b"held".to_vec()))
                .await
                .unwrap();
        }));
    }
    nabedra_rt::time::sleep(Duration::from_millis(100)).await;
    assert!(waiting.iter().all(|task| !task.is_finished()));
    let read = nabedra_rt::time::timeout(Duration::from_secs(5), get(&groups[20], b"state")).await;
    assert_eq!(read.expect("query read waited behind storage"), b"20/0/2");
    let control =
        nabedra_rt::time::timeout(Duration::from_secs(5), groups[21].applied_state(budget())).await;
    assert_eq!(
        control
            .expect("control read waited behind storage")
            .unwrap()
            .unwrap()
            .position,
        meta(ROUNDS - 1).position
    );
    gated.set(false);
    for task in waiting {
        task.await.unwrap();
    }

    // Unsynced applies and hints of every group, then one synced write: a crash
    // keeps all of them (prefix durability across the pool).
    for (n, group) in groups.iter().enumerate() {
        group
            .transaction(
                meta(ROUNDS),
                budget(),
                put_entry(ROUNDS, b"state", format!("{n}/final").into_bytes()),
            )
            .await
            .unwrap();
        group
            .log_transaction_metadata_unsynced(
                budget(),
                put_entry(0, b"committed", format!("{n}/hint").into_bytes()),
            )
            .await
            .unwrap();
        assert_eq!(group.write_stats().unsynced_applied_index, Some(ROUNDS));
    }
    assert!(pool.has_unsynced_writes());
    groups[0]
        .log_transaction(budget(), put_entry(0, b"log", b"barrier".to_vec()))
        .await
        .unwrap();
    for group in &groups {
        assert_eq!(group.write_stats().unsynced_applied_index, None);
    }
    let stats = serde_json::to_value(resources.storage_stats()).unwrap();
    assert!(stats["journal_syncs"].as_u64().unwrap() > 0, "{stats}");
    assert!(stats["synced_commits"].as_u64().unwrap() > 0, "{stats}");
    assert!(stats["pools"].as_u64().unwrap() >= 1, "{stats}");
    let ids: Vec<_> = groups
        .iter()
        .map(|group| group.identity().clone())
        .collect();
    fault.halt();
    drop(groups);
    drop(pool);
    let fault = fault.crash();
    let pool = reopen(&dir, &fault, identity, resources.clone()).unwrap();
    for (n, id) in ids.into_iter().enumerate() {
        let group = pool.retained_group(id).await.unwrap();
        assert_eq!(
            get(&group, b"state").await,
            format!("{n}/final").into_bytes()
        );
        assert_eq!(
            group
                .applied_state(budget())
                .await
                .unwrap()
                .unwrap()
                .position,
            meta(ROUNDS).position
        );
        let hint = group
            .log_read(budget(), |view| {
                view.get(b"committed").and_then(|v| v.ok_or_else(capacity))
            })
            .await
            .unwrap();
        assert_eq!(hint.as_slice(), format!("{n}/hint").as_bytes());
    }
}

/// RV-T2 Q4: when the log lives in another pool, deferral is refused and every
/// application write stays synced.
#[tokio::test]
async fn o11_separate_pool_log_keeps_application_synced() {
    let resources = shared(SharedResourceConfig::default());
    let first_dir = Dir::new();
    let second_dir = Dir::new();
    let identity = pool_identity();
    let first =
        OltpPool::open_development(&first_dir.0, identity.clone(), resources.clone()).unwrap();
    let second = OltpPool::open_development(
        &second_dir.0,
        PoolIdentity {
            pool_id: nabedra_rt::ids::uuid_v4(),
            ..identity
        },
        resources.clone(),
    )
    .unwrap();
    let id = group_identity(&first);
    let state = first.group(id.clone()).await.unwrap();
    let log = second.group(id).await.unwrap();
    assert!(state.defer_apply_sync(&log).is_none());
    assert!(!state.applies_deferred());
    for index in 0..4 {
        state
            .transaction(
                meta(index),
                budget(),
                put_entry(index, b"row", b"v".to_vec()),
            )
            .await
            .unwrap();
    }
    let stats = state.write_stats();
    assert_eq!((stats.synced_requests, stats.deferred_writes), (4, 0));
    assert_eq!(stats.unsynced_applied_index, None);
    assert!(!first.has_unsynced_writes());
}

/// RV-T2 Q4: several groups on one pool interleave unsynced applies, synced
/// appends and explicit syncs. A group's unsynced applied index clears exactly
/// when a sync that started after its last apply completed, whichever group
/// issued that sync.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o11_concurrent_groups_track_unsynced_applies_against_an_oracle() {
    let dir = Dir::new();
    let resources = shared(SharedResourceConfig::default());
    let pool = OltpPool::open_development(&dir.0, pool_identity(), resources.clone()).unwrap();
    let mut groups = Vec::new();
    let mut deferrals = Vec::new();
    for _ in 0..6 {
        let group = pool.group(group_identity(&pool)).await.unwrap();
        deferrals.push(group.defer_apply_sync(&group).unwrap());
        groups.push(group);
    }
    let mut rng = 0x5eed_u64;
    let mut next = || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let mut applied = vec![0u64; groups.len()];
    for _ in 0..120 {
        // A burst of concurrent applies on random groups.
        let mut tasks = Vec::new();
        let mut chosen = std::collections::BTreeSet::new();
        for _ in 0..4 {
            chosen.insert((next() % groups.len() as u64) as usize);
        }
        for &n in &chosen {
            let group = groups[n].clone();
            let index = applied[n];
            tasks.push(nabedra_rt::task::spawn(async move {
                group
                    .transaction(
                        meta(index),
                        budget(),
                        put_entry(index, b"row", index.to_be_bytes().to_vec()),
                    )
                    .await
                    .unwrap();
            }));
            applied[n] += 1;
        }
        for task in tasks {
            task.await.unwrap();
        }
        for &n in &chosen {
            assert_eq!(
                groups[n].write_stats().unsynced_applied_index,
                Some(applied[n] - 1)
            );
        }
        assert!(pool.has_unsynced_writes());
        // A synced append of some group, or an explicit sync, covers them all.
        match next() % 3 {
            0 => pool.sync_wal().await.unwrap(),
            _ => {
                let n = (next() % groups.len() as u64) as usize;
                groups[n]
                    .log_transaction(budget(), put_entry(0, b"log", vec![1]))
                    .await
                    .unwrap();
            }
        }
        for group in &groups {
            assert_eq!(group.write_stats().unsynced_applied_index, None);
        }
        assert!(!pool.has_unsynced_writes());
    }
}

/// RV-T2 Q5: once the Raft host stops, application writes are synced again.
#[tokio::test]
async fn o11_stopped_host_clears_deferred_application() {
    let dir = Dir::new();
    let resources = shared(SharedResourceConfig::default());
    let pool = OltpPool::open_development(&dir.0, pool_identity(), resources).unwrap();
    let group = pool.group(group_identity(&pool)).await.unwrap();
    let host = group.defer_apply_sync(&group).unwrap();
    group
        .transaction(meta(0), budget(), put_entry(0, b"row", vec![0]))
        .await
        .unwrap();
    assert_eq!(group.write_stats().deferred_writes, 1);
    drop(host);
    group
        .transaction(meta(1), budget(), put_entry(1, b"row", vec![1]))
        .await
        .unwrap();
    let stats = group.write_stats();
    assert_eq!((stats.deferred_writes, stats.synced_requests), (1, 1));
    assert_eq!(stats.unsynced_applied_index, None);
}

/// NS22 (pool level), Q1: a format 1 (RocksDB) pool, a raw RocksDB directory
/// and a legacy OltpEngine store are refused as unsupported store formats
/// naming the path, and their directories are byte-identical afterwards.
#[test]
fn ns22_rocksdb_pools_and_legacy_stores_are_refused_untouched() {
    fn tree(path: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
        let mut out = BTreeMap::new();
        for entry in fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if entry.file_type().unwrap().is_dir() {
                out.insert(path.clone(), Vec::new());
                out.extend(tree(&path));
            } else {
                out.insert(path.clone(), fs::read(&path).unwrap());
            }
        }
        out
    }
    let rocksdb_files = |path: &Path| {
        fs::create_dir_all(path).unwrap();
        for (name, body) in [
            ("CURRENT", &b"MANIFEST-000005\n"[..]),
            ("IDENTITY", b"6f1c3a52-rocksdb"),
            ("LOCK", b""),
            ("MANIFEST-000005", b"\x00\x01rocksdb manifest"),
            ("OPTIONS-000007", b"[Version]\nrocksdb_version=11.8.1\n"),
            ("000004.log", b"\x10\x20wal"),
            ("000009.sst", b"sst bytes"),
        ] {
            fs::write(path.join(name), body).unwrap();
        }
    };
    let resources = shared(SharedResourceConfig::default());
    let identity = pool_identity();

    let format_one = Dir::new();
    rocksdb_files(&format_one.0);
    let old = PoolIdentity {
        pool_format_version: 1,
        ..identity.clone()
    };
    fs::write(
        format_one.0.join(POOL_MANIFEST),
        serde_json::to_vec(&old).unwrap(),
    )
    .unwrap();
    let raw = Dir::new();
    rocksdb_files(&raw.0);
    let legacy = Dir::new();
    rocksdb_files(&legacy.0);
    fs::write(
        legacy.0.join("NABEDRA_OLTP.json"),
        b"{\"engine\":\"oltp\",\"store_format_version\":2}",
    )
    .unwrap();

    for dir in [&format_one, &raw, &legacy] {
        let before = tree(&dir.0);
        let error = OltpPool::open_development(&dir.0, identity.clone(), resources.clone())
            .err()
            .expect("an old store was opened");
        assert_eq!(error.code, ErrorCode::StorageUnavailable);
        assert!(
            error.message.contains("unsupported store format"),
            "{}",
            error.message
        );
        assert!(
            error.message.contains(&dir.0.display().to_string()),
            "{}",
            error.message
        );
        assert!(
            error.message.contains("docs/oltp-rocksdb-upgrade.md"),
            "{}",
            error.message
        );
        assert_eq!(tree(&dir.0), before, "{} changed", dir.0.display());
    }
    // A format 1 identity is not a valid identity for opening either.
    assert!(old.validate().is_err());
    // A RocksDB directory inside a format 2 pool's store path is refused by the engine.
    let nested = Dir::new();
    fs::create_dir_all(&nested.0).unwrap();
    fs::write(
        nested.0.join(POOL_MANIFEST),
        serde_json::to_vec(&identity).unwrap(),
    )
    .unwrap();
    rocksdb_files(&nested.0.join("kv"));
    let before = tree(&nested.0);
    let error = OltpPool::open_development(&nested.0, identity, resources)
        .err()
        .unwrap();
    assert!(
        error.message.contains("unsupported store format"),
        "{}",
        error.message
    );
    assert!(
        error
            .message
            .contains(&nested.0.join("kv").display().to_string()),
        "{}",
        error.message
    );
    assert_eq!(tree(&nested.0), before);
}

/// Finds `needle` in the pool's journal segments and flips one bit of it.
fn damage_journal_value(dir: &Dir, fault: &FaultFs, needle: &[u8]) {
    for path in fault.list(&journal_dir(dir)).unwrap() {
        let bytes = fault.contents(&path).unwrap();
        if let Some(at) = bytes
            .windows(needle.len())
            .position(|window| window == needle)
        {
            fault
                .flip_bit(&path, (at + needle.len() / 2) as u64, 3)
                .unwrap();
            return;
        }
    }
    panic!("value not found in the journal");
}

/// Section 3 G1 and R6 at pool level: a damaged synced record of one group,
/// with a later synced record as evidence, quarantines that group. The pool
/// opens, the group does not start and is reported, other groups serve.
/// Damage across too many groups refuses the pool.
#[tokio::test]
async fn o06_ns07_damaged_group_is_quarantined_reported_and_never_started() {
    let dir = Dir::new();
    let fault = FaultFs::new();
    let identity = pool_identity();
    let resources = shared(SharedResourceConfig::default());
    let pool = fault_pool(&dir, &fault, identity.clone(), resources.clone());
    let mut ids = Vec::new();
    for n in 0..12u8 {
        let group = pool.group(group_identity(&pool)).await.unwrap();
        let needle = format!("damage-target-{n:02}-0123456789abcdef").into_bytes();
        group
            .log_transaction(budget(), put_entry(0, b"entry", needle))
            .await
            .unwrap();
        ids.push(group.identity().clone());
    }
    // Evidence: later synced records name the damaged ones durable.
    let last = pool.group(ids[0].clone()).await.unwrap();
    last.log_transaction(budget(), put_entry(1, b"entry2", b"later".to_vec()))
        .await
        .unwrap();
    drop(last);
    drop(pool);
    let fault = fault.kill();
    damage_journal_value(&dir, &fault, b"damage-target-03-0123456789abcdef");
    let pool = reopen(&dir, &fault, identity.clone(), resources.clone()).unwrap();
    assert_eq!(pool.quarantined_groups(), vec![ids[3].group_id]);
    assert_eq!(resources.storage_stats().quarantined_groups, 1);
    let refused = pool.retained_group(ids[3].clone()).await.err().unwrap();
    assert!(
        refused.message.contains("quarantined"),
        "{}",
        refused.message
    );
    for (n, id) in ids.iter().enumerate().filter(|(n, _)| *n != 3) {
        let group = pool.retained_group(id.clone()).await.unwrap();
        let entry = group
            .log_read(budget(), |view| {
                view.get(b"entry").and_then(|v| v.ok_or_else(capacity))
            })
            .await
            .unwrap();
        assert_eq!(
            entry.as_slice(),
            format!("damage-target-{n:02}-0123456789abcdef").as_bytes()
        );
    }
    drop(pool);

    // R6 counts the groups quarantined by one open (RV-NS6 F2): group 03 was
    // quarantined by the previous open, so new damage in group 07 is one of
    // twelve. The pool opens with both quarantined.
    damage_journal_value(&dir, &fault, b"damage-target-07-0123456789abcdef");
    let pool = reopen(&dir, &fault, identity.clone(), resources.clone()).unwrap();
    let mut quarantined = pool.quarantined_groups();
    quarantined.sort();
    let mut expected = vec![ids[3].group_id, ids[7].group_id];
    expected.sort();
    assert_eq!(quarantined, expected);
    assert_eq!(resources.storage_stats().quarantined_groups, 2);
    for n in [3, 7] {
        let refused = pool.retained_group(ids[n].clone()).await.err().unwrap();
        assert!(
            refused.message.contains("quarantined"),
            "{}",
            refused.message
        );
    }
    let healthy = pool.retained_group(ids[1].clone()).await.unwrap();
    healthy
        .log_transaction(budget(), put_entry(2, b"entry3", b"still".to_vec()))
        .await
        .unwrap();
    drop(healthy);
    drop(pool);

    // R6: two of twelve groups (more than 10%) quarantined by one open is refused.
    damage_journal_value(&dir, &fault, b"damage-target-09-0123456789abcdef");
    damage_journal_value(&dir, &fault, b"damage-target-10-0123456789abcdef");
    let error = reopen(&dir, &fault, identity, resources).err().unwrap();
    assert_eq!(error.code, ErrorCode::StorageUnavailable);
    assert!(error.message.contains("too many"), "{}", error.message);
}

/// NS10 (scaled, in process): two pools load four times the worker's cache
/// plus write buffer, then mix reads and writes. Memtables stay inside
/// `write_buffer_bytes` (writers stall instead), the block cache inside
/// `cache_bytes`, scheduler charges do not grow, and every value reads back.
/// The plan's 2.15 GB cgroup run (`memory.peak`) is not this test.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::disallowed_methods)] // a short-lived memory sampler thread for this test; not a service
async fn ns10_o21_two_pools_beyond_memory_budget_stay_inside_their_charges() {
    let config = SharedResourceConfig {
        cache_bytes: 1 << 20,
        write_buffer_bytes: 2 << 20,
        ..SharedResourceConfig::default()
    };
    let resources = shared(config);
    let identity = pool_identity();
    let dirs = [Dir::new(), Dir::new()];
    let pools: Vec<_> = dirs
        .iter()
        .map(|dir| {
            OltpPool::open_development(
                &dir.0,
                PoolIdentity {
                    pool_id: nabedra_rt::ids::uuid_v4(),
                    ..identity.clone()
                },
                resources.clone(),
            )
            .unwrap()
        })
        .collect();
    let baseline = resources.scheduler().usage();
    let target = 4 * (config.cache_bytes + config.write_buffer_bytes);
    let value_bytes = 8 << 10;
    let rows_per_pool = target / value_bytes / 2 + 1;
    let peak = Arc::new(AtomicUsize::new(0));
    let sampling = Arc::new(AtomicBool::new(true));
    let sampler = {
        let resources = resources.clone();
        let peak = peak.clone();
        let sampling = sampling.clone();
        std::thread::spawn(move || {
            while sampling.load(Ordering::Relaxed) {
                peak.fetch_max(resources.storage.memtables.used(), Ordering::Relaxed);
                assert!(resources.storage.cache.usage_bytes() <= config.cache_bytes + (64 << 10));
                nabedra_rt::time::thread_sleep(Duration::from_millis(1));
            }
        })
    };
    let mut groups = Vec::new();
    for pool in &pools {
        groups.push(pool.group(group_identity(pool)).await.unwrap());
    }
    let mut tasks = Vec::new();
    for group in groups.clone() {
        tasks.push(nabedra_rt::task::spawn(async move {
            for row in 0..rows_per_pool as u64 {
                group
                    .log_transaction(
                        TransactionBudget {
                            batch_bytes: 64 << 10,
                            ..budget()
                        },
                        move |tx| {
                            tx.put(&row.to_be_bytes(), &vec![row as u8; 8 << 10])?;
                            Ok(Transition::Commit(tx.copy_output(b"")?))
                        },
                    )
                    .await
                    .unwrap();
                if row % 7 == 0 && row > 0 {
                    let probe = row / 2;
                    let value = group
                        .log_read(budget(), move |view| {
                            view.get(&probe.to_be_bytes())
                                .and_then(|v| v.ok_or_else(capacity))
                        })
                        .await
                        .unwrap();
                    assert_eq!(value.as_slice(), vec![probe as u8; 8 << 10]);
                }
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    for group in &groups {
        for row in (0..rows_per_pool as u64).step_by(13) {
            let value = group
                .log_read(budget(), move |view| {
                    view.get(&row.to_be_bytes())
                        .and_then(|v| v.ok_or_else(capacity))
                })
                .await
                .unwrap();
            assert_eq!(value.as_slice(), vec![row as u8; 8 << 10]);
        }
    }
    sampling.store(false, Ordering::Relaxed);
    sampler.join().unwrap();
    let peak = peak.load(Ordering::Relaxed);
    let flushed: u64 = pools
        .iter()
        .map(|pool| pool.inner.store.stats().flush_bytes)
        .sum();
    eprintln!(
        "ns10 scaled: loaded {} MiB into 2 pools, memtable peak {} KiB of {} KiB, flushed {} MiB",
        (2 * rows_per_pool * value_bytes) >> 20,
        peak >> 10,
        config.write_buffer_bytes >> 10,
        flushed >> 20
    );
    // A writer may overshoot by the batch it inserts before the next stall check.
    assert!(
        peak <= config.write_buffer_bytes + (128 << 10),
        "memtables used {peak}"
    );
    assert!(flushed > 0);
    assert_eq!(resources.scheduler().usage(), baseline);
    assert_eq!(resources.control_bytes_used(), 0);
}

/// O05: `nabedra-kv` threads. Run in a child process so other tests' threads
/// do not count: each worker starts `kv-flush` x1 and `kv-compact` x(n-1), and
/// each open pool `kv-journal` and `kv-journal-prep`, which stop when the pool closes.
#[cfg(not(feature = "sim"))]
#[test]
fn o05_kv_threads_are_per_worker_plus_two_journal_threads_per_open_pool() {
    const FLAG: &str = "NABEDRA_NS6_THREAD_PROBE";
    const NAME: &str = "oltp::pool::tests::kv::o05_kv_threads_are_per_worker_plus_two_journal_threads_per_open_pool";
    if std::env::var_os(FLAG).is_none() {
        for background in ["2", "5"] {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", NAME, "--nocapture", "--test-threads=1"])
                .env(FLAG, background)
                .status()
                .unwrap();
            assert!(
                status.success(),
                "O05 probe child failed with background={background}"
            );
        }
        return;
    }
    fn count() -> usize {
        #[cfg(target_os = "linux")]
        {
            fs::read_dir("/proc/self/task").unwrap().count()
        }
        #[cfg(target_os = "macos")]
        {
            let mut info = std::mem::MaybeUninit::<libc::proc_taskinfo>::uninit();
            let size = std::mem::size_of::<libc::proc_taskinfo>();
            let copied = unsafe {
                libc::proc_pidinfo(
                    libc::getpid(),
                    libc::PROC_PIDTASKINFO,
                    0,
                    info.as_mut_ptr().cast(),
                    size as i32,
                )
            };
            assert_eq!(copied as usize, size);
            unsafe { info.assume_init().pti_threadnum as usize }
        }
    }
    #[cfg(target_os = "linux")]
    fn named(name: &str) -> usize {
        fs::read_dir("/proc/self/task")
            .unwrap()
            .filter(|entry| {
                fs::read_to_string(entry.as_ref().unwrap().path().join("comm"))
                    .is_ok_and(|comm| comm.trim() == name)
            })
            .count()
    }
    let background: i32 = std::env::var(FLAG).unwrap().parse().unwrap();
    let config = SharedResourceConfig {
        background_threads: background,
        ..SharedResourceConfig::default()
    };
    let before = count();
    let resources = shared(config);
    let after_worker = count();
    assert_eq!(
        after_worker - before,
        config.query_threads + config.control_threads + background as usize
    );
    let budget = resources.thread_budget();
    assert_eq!(budget.flush_threads, 1);
    assert_eq!(budget.compaction_threads, background as usize - 1);
    assert_eq!(budget.journal_threads, 2 * config.max_open_pools);
    let identity = pool_identity();
    let first_dir = Dir::new();
    let second_dir = Dir::new();
    let first =
        OltpPool::open_development(&first_dir.0, identity.clone(), resources.clone()).unwrap();
    let after_first = count();
    let second = OltpPool::open_development(
        &second_dir.0,
        PoolIdentity {
            pool_id: nabedra_rt::ids::uuid_v4(),
            ..identity
        },
        resources.clone(),
    )
    .unwrap();
    let after_second = count();
    assert_eq!(after_first - after_worker, 2);
    assert_eq!(after_second - after_first, 2);
    #[cfg(target_os = "linux")]
    {
        assert_eq!(named("kv-flush"), 1);
        assert_eq!(named("kv-compact"), background as usize - 1);
        assert_eq!(named("kv-journal"), 2);
        assert_eq!(named("kv-journal-prep"), 2);
        assert_eq!(named("rocksdb:high") + named("rocksdb:low"), 0);
    }
    drop(second);
    // Closing joins the pool's threads, but Linux can list an exited thread in
    // /proc/self/task for a moment afterwards; wait for it to go.
    let deadline = nabedra_rt::time::now_std() + std::time::Duration::from_secs(5);
    while count() != after_first && nabedra_rt::time::now_std() < deadline {
        nabedra_rt::time::thread_sleep(std::time::Duration::from_millis(10));
    }
    assert_eq!(count(), after_first);
    eprintln!(
        "O05 kv probe: before={before} worker={after_worker} first_pool={after_first} second_pool={after_second} background={background} budget={budget:?}"
    );
    drop(first);
    drop(resources);
}

/// NS08 / NS17 (pool level, NS5b pacing): with L0 at its slowdown trigger, a
/// group write is paced on a timer, not on the executor. The single query
/// thread keeps serving reads while the write waits, the write lands after its
/// delay, and a cancelled caller does not drop the dispatched write, also when
/// it is cancelled while the executor still builds the write (RV-NS6 F4).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ns08_ns17_l0_slowdown_paces_writes_off_the_executor() {
    let dir = Dir::new();
    let fault = FaultFs::new();
    fs::create_dir_all(&dir.0).unwrap();
    fault.create_dir_all_durable(&dir.0);
    let resources = shared(SharedResourceConfig::default());
    let pool = OltpPool::open_development_tuned(
        &dir.0,
        pool_identity(),
        resources,
        Arc::new(fault.clone()),
        |options| {
            options.levels.l0_compaction_trigger = 64;
            options.levels.l0_slowdown_writes = 1;
            options.levels.l0_stop_writes = 64;
            // 64 KiB/s: a 16 KiB batch waits about 250 ms.
            options.levels.delayed_write_rate = 64 << 10;
        },
    )
    .unwrap();
    let group = pool.group(group_identity(&pool)).await.unwrap();
    group
        .transaction(meta(0), budget(), put_entry(0, b"row", b"before".to_vec()))
        .await
        .unwrap();
    // One flushed L0 run reaches the slowdown trigger.
    let flushing = pool.clone();
    nabedra_rt::task::spawn_blocking(move || flushing.inner.store.flush().unwrap())
        .await
        .unwrap();
    assert!(pool.inner.store.stats().l0_runs >= 1);

    let started = nabedra_rt::time::now_std();
    let writer = group.clone();
    let paced = nabedra_rt::task::spawn(async move {
        writer
            .transaction(meta(1), budget(), put_entry(1, b"row", vec![7u8; 16 << 10]))
            .await
            .unwrap();
        nabedra_rt::time::elapsed(started)
    });
    nabedra_rt::time::sleep(Duration::from_millis(50)).await;
    assert!(!paced.is_finished(), "the write was not paced");
    // The paced write holds no executor slot: the one query thread serves a read.
    let read = nabedra_rt::time::timeout(Duration::from_millis(150), get(&group, b"row"))
        .await
        .expect("read waited behind a paced write");
    assert_eq!(read, b"before");
    let waited = paced.await.unwrap();
    assert!(waited >= Duration::from_millis(150), "paced for {waited:?}");
    assert_eq!(get(&group, b"row").await, vec![7u8; 16 << 10]);

    // A caller cancelled while its write waits on the pacing timer: the write
    // still lands, exactly once, in order.
    let writer = group.clone();
    let cancelled = nabedra_rt::task::spawn(async move {
        writer
            .transaction(meta(2), budget(), put_entry(2, b"row", vec![9u8; 16 << 10]))
            .await
    });
    nabedra_rt::time::sleep(Duration::from_millis(50)).await;
    cancelled.abort();
    let _ = cancelled.await;
    nabedra_rt::time::timeout(Duration::from_secs(5), async {
        while get(&group, b"row").await != vec![9u8; 16 << 10] {
            nabedra_rt::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the dispatched write was dropped with its caller");
    let applied = group.applied_state(budget()).await.unwrap().unwrap();
    assert_eq!(applied.position, meta(2).position);

    // RV-NS6 F4: a caller cancelled while the executor still builds its paced
    // write. The executor finishes the batch after the caller is gone; the
    // write must land all the same, exactly once.
    assert!(pool.inner.store.stats().l0_runs >= 1);
    let started = nabedra_rt::time::now_std();
    let writer = group.clone();
    let cancelled = nabedra_rt::task::spawn(async move {
        writer
            .transaction(meta(3), budget(), move |tx| {
                // Still inside the executor when the caller is cancelled.
                nabedra_rt::time::thread_sleep(Duration::from_millis(200));
                tx.put(b"row", &[5u8; 16 << 10])?;
                Ok(Transition::Commit(tx.copy_output(&3u64.to_be_bytes())?))
            })
            .await
    });
    nabedra_rt::time::sleep(Duration::from_millis(50)).await;
    cancelled.abort();
    let _ = cancelled.await;
    nabedra_rt::time::timeout(Duration::from_secs(5), async {
        while get(&group, b"row").await != vec![5u8; 16 << 10] {
            nabedra_rt::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("a write cancelled while the executor built it was dropped");
    // It took the paced path: 200 ms in the executor, then the pacing delay.
    let landed = nabedra_rt::time::elapsed(started);
    assert!(
        landed >= Duration::from_millis(350),
        "landed after {landed:?}"
    );
    let applied = group.applied_state(budget()).await.unwrap().unwrap();
    assert_eq!(applied.position, meta(3).position);
    // Order holds: the next write lands after it.
    group
        .transaction(meta(4), budget(), put_entry(4, b"row", vec![4u8; 16 << 10]))
        .await
        .unwrap();
    assert_eq!(get(&group, b"row").await, vec![4u8; 16 << 10]);
}

/// O05 / M02 regression (NS6): the journal thread wakes a committer before it
/// drops the write's `on_done` (order and generation guards, batch charge).
/// A group write must still return only after those are released, or the
/// caller's next request can find the arena still charged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o05_m02_a_returned_write_has_released_its_guards_and_charges() {
    let dir = Dir::new();
    let resources = shared(SharedResourceConfig::default());
    let pool = OltpPool::open_development(&dir.0, pool_identity(), resources.clone()).unwrap();
    let group = pool.group(group_identity(&pool)).await.unwrap();
    let _host = group.defer_apply_sync(&group).unwrap();
    let baseline = resources.scheduler().usage();
    for index in 0..300u64 {
        let value = vec![index as u8; 1 << 10];
        let reply = if index % 2 == 0 {
            group
                .transaction(meta(index / 2), budget(), put_entry(index, b"state", value))
                .await
                .unwrap()
        } else {
            group
                .log_transaction(budget(), put_entry(index, b"log", value))
                .await
                .unwrap()
        };
        drop(reply);
        assert_eq!(resources.scheduler().usage(), baseline, "write {index}");
        // The order guard is free as well: the next write takes it at once.
        assert!(group.inner.order.try_lock().is_ok(), "write {index}");
        assert!(group.inner.log_order.try_lock().is_ok(), "write {index}");
    }
}

mod rv_ns6;

mod ns14;

pub(crate) async fn o29_pool_recovery() {
    use crate::{oltp_acceptance::clause, oltp_replication::typed_log::tests::GatedSync};

    let dir = Dir::new();
    let disk = FaultFs::new();
    fs::create_dir_all(&dir.0).unwrap();
    disk.create_dir_all_durable(&dir.0);
    let fs = GatedSync::new().over(disk.clone());
    let resources = shared(SharedResourceConfig::default());
    let identity = pool_identity();
    let pool = OltpPool::open_development_on(
        &dir.0,
        identity.clone(),
        resources.clone(),
        Arc::new(fs.clone()),
    )
    .unwrap();
    let group_id = group_identity(&pool);
    let group = pool.group(group_id.clone()).await.unwrap();
    let alias_pool = pool.clone();
    let alias = alias_pool.group(group_id.clone()).await.unwrap();

    let healthy_dir = Dir::new();
    let healthy = OltpPool::open_development(
        &healthy_dir.0,
        PoolIdentity {
            pool_id: nabedra_rt::ids::uuid_v4(),
            ..identity.clone()
        },
        resources.clone(),
    )
    .unwrap();
    let proof = healthy.group(group_identity(&healthy)).await.unwrap();
    let decided = nabedra_rt::ids::uuid_v4().to_string().into_bytes();
    proof
        .transaction(meta(0), budget(), put_entry(0, b"prior", decided.clone()))
        .await
        .unwrap();

    let table_id = nabedra_rt::ids::uuid_v4().to_string().into_bytes();
    let original = table_id.clone();
    let live = pool.kv_store().visible_seq();
    disk.clear_calls();
    fs.fail_after_sync();
    let ambiguous = group
        .transaction(meta(0), budget(), move |tx| {
            tx.put(b"table", &table_id)?;
            Ok(Transition::Commit(tx.copy_output(&table_id)?))
        })
        .await;
    assert_eq!(ambiguous.err(), Some(recovery_unknown()));
    assert_eq!(pool.kv_store().visible_seq(), live);
    let syncs = || {
        disk.calls()
            .into_iter()
            .filter(|c| c.op == FsOp::SyncData && c.path.starts_with(journal_dir(&dir)) && c.ok)
            .count()
    };
    assert_eq!(syncs(), 1, "the failed live write must already be durable");

    let attempted = Arc::new(AtomicUsize::new(0));
    for handle in [&group, &alias] {
        let observed = attempted.clone();
        let strong_read = handle.read(budget(), move |view| {
            observed.fetch_add(1, Ordering::Relaxed);
            view.copy_output(b"unexpected read")
        });
        assert_eq!(strong_read.await.err(), Some(recovery_unknown()));
        let observed = attempted.clone();
        let same_request = handle.transaction(meta(0), budget(), move |tx| {
            observed.fetch_add(1, Ordering::Relaxed);
            let regenerated = nabedra_rt::ids::uuid_v4().to_string();
            tx.put(b"table", regenerated.as_bytes())?;
            Ok(Transition::Commit(tx.copy_output(regenerated.as_bytes())?))
        });
        assert_eq!(same_request.await.err(), Some(recovery_unknown()));
    }
    assert_eq!(attempted.load(Ordering::Relaxed), 0);
    assert_eq!(syncs(), 1, "no second decision was appended");
    assert!(pool.requires_recovery() && alias_pool.requires_recovery());
    assert_eq!(
        alias_pool.group(group_id.clone()).await.err(),
        Some(recovery_unknown())
    );
    clause(
        "O29",
        "shared-recovery-state",
        "passed",
        "The original pool and both existing and newly requested group aliases shared a recovery latch after post-sync EIO.",
    );

    let replay = proof
        .transaction(meta(0), budget(), |_| panic!("reran a known decision"))
        .await
        .unwrap();
    assert_eq!(replay.as_slice(), 0u64.to_be_bytes());
    assert_eq!(get(&proof, b"prior").await, decided);
    proof
        .transaction(
            meta(1),
            budget(),
            put_entry(1, b"healthy", b"usable".to_vec()),
        )
        .await
        .unwrap();
    assert_eq!(get(&proof, b"healthy").await, b"usable");
    assert!(!healthy.requires_recovery());
    clause(
        "O29",
        "healthy-pool-isolation",
        "passed",
        "The unrelated pool provided a prior applied reply and served an additional write and read while the first pool was latched.",
    );

    drop((alias, alias_pool, group, pool));
    let disk = disk.crash();
    let recovered = reopen(&dir, &disk, identity, resources).unwrap();
    assert!(!recovered.requires_recovery());
    let group = recovered.retained_group(group_id).await.unwrap();
    let applied = group.applied_state(budget()).await.unwrap().unwrap();
    assert_eq!(applied.position, meta(0).position);
    assert_eq!(applied.command_digest, meta(0).command_digest);
    assert_eq!(get(&group, b"table").await, original);
    let recovered_reply = group
        .transaction(meta(0), budget(), |_| panic!("re-executed recovered work"))
        .await
        .unwrap();
    assert_eq!(recovered_reply.as_slice(), original);
    clause(
        "O29",
        "recovery-without-pending-replay",
        "passed",
        "Native recovery exposed the persisted UUID and exact reply at the original position. Retrying that entry did not execute its transition.",
    );
    clause(
        "O29",
        "fault-model-label",
        "passed",
        "FaultFs reports EIO after persisting the journal sync. The assertion is deterministic fault-model evidence.",
    );
}
