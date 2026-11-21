use std::time::{Duration, Instant};

use nabedra_kv::fs::FaultFs;
use nabedra_kv::{DamageReason, Error};
use openraft::{CommittedLeaderId, Entry, EntryPayload, Vote};

use super::super::log::LogStore;
use super::super::{Broker, BrokerConfig, GroupError, TypeConfig};
use super::*;

#[tokio::test]
async fn group_close_fences_detached_storage_work() -> Result<()> {
    let disk = tempfile::tempdir()?;
    let database = Database::open(disk.path(), Uuid::new_v4(), 1)?;
    let old = GroupStore::new(database.clone(), "close-fence");
    let key = old.key(b"applied-marker");
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, parked) = std::sync::mpsc::channel();
    let waiter = {
        let old = old.clone();
        let key = key.clone();
        tokio::spawn(async move {
            old.run(move |db| {
                entered
                    .send(())
                    .map_err(|_| anyhow::anyhow!("test observer stopped"))?;
                parked.recv()?;
                let mut write = WriteBatch::new();
                write.put(&key, b"first-generation");
                db.write(write)
            })
            .await
        })
    };
    started.await?;
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    assert!(
        old.sealed.try_write().is_err(),
        "detached disk job released its permit"
    );

    release.send(())?;
    old.seal().await;
    let new = GroupStore::new(database, "close-fence");
    let attempted_key = key.clone();
    assert!(
        old.run(move |db| {
            let mut write = WriteBatch::new();
            write.put(&attempted_key, b"late-old-write");
            db.write(write)
        })
        .await
        .is_err()
    );
    assert_eq!(
        new.run(move |db| db.get(&key)).await?,
        Some(Bytes::from_static(b"first-generation"))
    );
    Ok(())
}

// ---- NS27: Events on nabedra-kv -------------------------------------------------------

pub(super) const CLUSTER: Uuid = Uuid::from_u128(0x27);

pub(super) fn entry(term: u64, index: u64) -> Entry<TypeConfig> {
    Entry {
        log_id: log_id(term, index),
        payload: EntryPayload::Blank,
    }
}

pub(super) fn log_id(term: u64, index: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(term, 1), index)
}

/// Small memtables and journal segments, so a short workload flushes, compacts and rolls
/// segments and the crash boundaries cover background work too.
pub(super) fn small(fs: &FaultFs) -> nabedra_kv::Options {
    let mut options = Database::options(Arc::new(fs.clone()));
    options.memtable_bytes = 6 << 10;
    options.max_replay_bytes = 12 << 10;
    options.journal.segment_bytes = 32 << 10;
    options.journal.max_group_bytes = 4 << 10;
    options.levels.l0_compaction_trigger = 2;
    options.levels.target_file_size = 8 << 10;
    options.run_writer.block_size = 512;
    options
}

pub(super) fn fault_root(fs: &FaultFs, root: &Path) {
    fs.create_dir_all_durable(root);
}

#[test]
fn ns27_scope_of_confines_group_batches_only() {
    assert_eq!(scope_of(b"group/p-1_x/log/\0\0"), "group/p-1_x/".len());
    assert_eq!(scope_of(b"group/m/state-header"), "group/m/".len());
    assert_eq!(scope_of(b"group/m/"), "group/m/".len());
    assert_eq!(scope_of(b"group//log"), 0);
    assert_eq!(scope_of(b"group/no-slash"), 0);
    assert_eq!(scope_of(b"hosted/m"), 0);
    assert_eq!(scope_of(DATABASE_IDENTITY), 0);
    assert_eq!(scope_of(b"retired/v1/m"), 0);

    // Transactions, log writes and staging touch one group: they take its scope.
    let mut group = WriteBatch::new();
    group.put(b"group/g1/state/x", b"v");
    group.put_journal(b"group/g1/log/\0\0\0\0\0\0\0\x01", b"entry");
    group.delete_range(b"group/g1/", &prefix_end(b"group/g1/"));
    confine(&mut group);
    assert_eq!(group.scope(), b"group/g1/");
    assert!(!group.leaves_scope());

    // The publish batch, `forget` and `mark_hosted` stay store-wide single batches.
    let mut mixed = WriteBatch::new();
    mixed.put(b"group/g1/state-header", b"h");
    mixed.put(b"hosted/g1", b"");
    confine(&mut mixed);
    assert_eq!(mixed.scope(), b"");
    let mut other_group = WriteBatch::new();
    other_group.put(b"group/g1/a", b"");
    other_group.put(b"group/g2/a", b"");
    confine(&mut other_group);
    assert_eq!(other_group.scope(), b"");
    let mut store_wide = WriteBatch::new();
    store_wide.put(b"hosted/g1", b"");
    confine(&mut store_wide);
    assert_eq!(store_wide.scope(), b"");
}

#[test]
fn ns27_rocksdb_root_is_refused_and_left_unchanged() -> Result<()> {
    let disk = tempfile::tempdir()?;
    let legacy = disk.path().join("rocksdb");
    std::fs::create_dir(&legacy)?;
    std::fs::write(legacy.join("CURRENT"), b"MANIFEST-000005\n")?;
    let before = listing(disk.path())?;
    let error = match Database::open(disk.path(), CLUSTER, 1) {
        Ok(_) => panic!("a RocksDB root must not open"),
        Err(error) => error,
    };
    match error.downcast_ref::<Error>() {
        Some(Error::UnsupportedFormat { found }) => {
            assert!(found.contains(&legacy.display().to_string()), "{found}")
        }
        other => panic!("expected UnsupportedFormat, got {other:?} ({error:#})"),
    }
    assert_eq!(listing(disk.path())?, before);
    Ok(())
}

fn listing(root: &Path) -> Result<Vec<(PathBuf, Vec<u8>)>> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir)? {
            let path = entry?.path();
            if path.is_dir() {
                out.push((path.clone(), Vec::new()));
                stack.push(path);
            } else {
                out.push((path.clone(), std::fs::read(&path)?));
            }
        }
    }
    out.sort();
    Ok(out)
}

/// What a reopened broker store holds, as the Events code reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Observed {
    hosted: Vec<String>,
    retired: Vec<Option<u64>>,
    logs: Vec<Vec<(u64, u64)>>,
    /// Per group: (last purged, last log id).
    states: Vec<LastIds>,
    votes: Vec<Option<Vote<u64>>>,
    committed: Vec<Option<LogId<u64>>>,
    records: Vec<(String, usize)>,
    staged: Option<Uuid>,
    raw_stage: usize,
}

type LastIds = (Option<LogId<u64>>, Option<LogId<u64>>);

const GROUPS: [&str; 2] = ["g1", "g2"];

fn observe(db: &Arc<Database>) -> Result<Observed> {
    let mut observed = Observed {
        hosted: db.stored_group_ids()?,
        retired: Vec::new(),
        logs: Vec::new(),
        states: Vec::new(),
        votes: Vec::new(),
        committed: Vec::new(),
        records: Vec::new(),
        staged: None,
        raw_stage: 0,
    };
    for id in GROUPS {
        let group = GroupStore::new(db.clone(), id);
        let log = LogStore(group.clone());
        observed.retired.push(group.retired_at()?);
        observed.logs.push(
            log.entries((Unbounded, Unbounded), usize::MAX)?
                .iter()
                .map(|entry| (entry.log_id.index, entry.log_id.leader_id.term))
                .collect(),
        );
        let state = log.log_state()?;
        observed
            .states
            .push((state.last_purged_log_id, state.last_log_id));
        observed.votes.push(group.read(b"vote")?);
        observed
            .committed
            .push(group.read::<Option<LogId<u64>>>(b"committed")?.flatten());
    }
    let g1 = GroupStore::new(db.clone(), "g1");
    observed.records = g1
        .transaction()?
        .scan(b"", None, 4096)?
        .into_iter()
        .map(|(key, value)| (String::from_utf8_lossy(&key).into_owned(), value.len()))
        .collect();
    observed.staged = g1.read(b"staged-generation")?;
    let stage = g1.key(b"state/");
    observed.raw_stage = entries(&db.snapshot(), &stage, &prefix_end(&stage), true).count();
    Ok(observed)
}

fn txn(db: &Arc<Database>, applied: u64, ops: &[(&[u8], Option<&[u8]>)]) -> Result<()> {
    let group = GroupStore::new(db.clone(), "g1");
    let mut tx = group.transaction()?;
    for (key, value) in ops {
        match value {
            Some(value) => tx.put(key, value)?,
            None => tx.delete(key)?,
        }
    }
    tx.header.applied = Some(log_id(1, applied));
    tx.commit()
}

/// The steps of the crash workload. Every step is exactly one synced write the Events
/// code makes, and returns only after it is durable.
fn step(db: &Arc<Database>, n: usize) -> Result<()> {
    let g1 = GroupStore::new(db.clone(), "g1");
    let g2 = GroupStore::new(db.clone(), "g2");
    let log1 = LogStore(g1.clone());
    let log2 = LogStore(g2.clone());
    let big = vec![b'e'; 900];
    match n {
        0 => db.mark_hosted("g1"),
        1 => log1.append_entries((1..=5).map(|i| entry(1, i)).collect()),
        2 => log1.put_record(b"vote", &Vote::new(1, 1)),
        3 => txn(db, 3, &[(b"a", Some(b"1")), (b"b", Some(&big))]),
        4 => log1.put_record(b"committed", &Some(log_id(1, 4))),
        5 => log1.append_entries((6..=12).map(|i| entry(1, i)).collect()),
        6 => log1.truncate_from(9),
        7 => log1.append_entries((9..=14).map(|i| entry(2, i)).collect()),
        8 => log1.put_record(b"vote", &Vote::new(2, 1)),
        9 => log1.purge_through(log_id(1, 6)),
        10 => txn(
            db,
            8,
            &[(b"a", None), (b"c", Some(&big)), (b"d", Some(b"4"))],
        ),
        11 => db.mark_hosted("g2"),
        12 => log2.append_entries((1..=6).map(|i| entry(1, i)).collect()),
        // An interrupted snapshot stage: its marker and records, then the cleanup.
        13 => {
            let mut write = WriteBatch::new();
            write.put(&g1.key(b"staged-generation"), &serde_json::to_vec(&STAGED)?);
            db.write(write)
        }
        14 => {
            let mut stage = WriteBatch::new();
            for i in 0..8_u8 {
                stage.put(&[g1.state_prefix(STAGED).as_slice(), &[i]].concat(), &big);
            }
            db.write(stage)
        }
        15 => g1.cleanup_interrupted_stage(),
        16 => log1.purge_through(log_id(2, 14)),
        17 => log2.purge_through(log_id(1, 2)),
        18 => g2.forget(20),
        19 => log1.append_entries((15..=18).map(|i| entry(2, i)).collect()),
        _ => unreachable!(),
    }
}

const STEPS: usize = 20;
const STAGED: Uuid = Uuid::from_u128(12);

/// Runs the workload until a step fails. Returns the number of completed steps, or None
/// when the open itself failed.
fn workload(root: &Path, fs: &FaultFs) -> Option<usize> {
    let db = Database::open_with(root, CLUSTER, 1, small(fs)).ok()?;
    for n in 0..STEPS {
        if step(&db, n).is_err() {
            return Some(n);
        }
    }
    Some(STEPS)
}

fn reference(root: &Path) -> Result<(Vec<Observed>, u64)> {
    let fs = FaultFs::new();
    fault_root(&fs, root);
    let db = Database::open_with(root, CLUSTER, 1, small(&fs))?;
    let mut states = vec![observe(&db)?];
    for n in 0..STEPS {
        step(&db, n)?;
        states.push(observe(&db)?);
    }
    let stats = db.store().stats();
    assert!(
        stats.flush_bytes > 0 && stats.journal_syncs > STEPS as u64,
        "the workload must reach flushes: {stats:?}"
    );
    drop(db);
    Ok((states, fs.mutating_ops()))
}

#[test]
fn ns27_synced_events_writes_survive_a_crash_at_every_boundary() -> Result<()> {
    let disk = tempfile::tempdir()?;
    let root = disk.path();
    let (states, total) = reference(root)?;
    // The workload must cover log, purge, staging and forget paths with real changes.
    assert_ne!(states[STEPS], states[0]);
    let mut checked = 0;
    for n in 0..=total + 8 {
        let fs = FaultFs::new();
        fault_root(&fs, root);
        fs.crash_after(n);
        let done = workload(root, &fs);
        for (kind, image) in [("crash", fs.crash()), ("power loss", fs.power_loss(n))] {
            let reopened = Database::open_with(root, CLUSTER, 1, small(&image))
                .unwrap_or_else(|error| panic!("{kind} after {n} ops: reopen failed: {error:#}"));
            let seen = observe(&reopened)?;
            // Every acknowledged step survives; the step in flight may or may not.
            let allowed: Vec<&Observed> = match done {
                None => vec![&states[0]],
                Some(done) => states[done..(done + 2).min(STEPS + 1)].iter().collect(),
            };
            assert!(
                allowed.contains(&&seen),
                "{kind} after {n} of {total} ops, {done:?} steps acknowledged:\nSEEN {seen:?}\n\
                 ALLOWED {allowed:?}"
            );
            drop(reopened);
            let again = Database::open_with(root, CLUSTER, 1, small(&image))?;
            assert_eq!(
                observe(&again)?,
                seen,
                "{kind} after {n}: a second reopen changed state"
            );
            checked += 1;
        }
    }
    assert!(
        checked >= 2 * total as usize,
        "{checked} images for {total} ops"
    );
    Ok(())
}

#[test]
fn ns27_last_log_id_matches_the_oracle_after_truncate_and_purge() -> Result<()> {
    let disk = tempfile::tempdir()?;
    let fs = FaultFs::new();
    fault_root(&fs, disk.path());
    let mut db = Database::open_with(disk.path(), CLUSTER, 1, small(&fs))?;
    // Neighbours on both sides of the group's log keys, in this group and in others
    // whose ids share a prefix with it.
    for id in ["g", "g1", "g1-x", "g10"] {
        let group = GroupStore::new(db.clone(), id);
        LogStore(group.clone()).put_record(b"committed", &Some(log_id(9, 9)))?;
        LogStore(group).put_record(b"vote", &Vote::new(9, 1))?;
    }
    for id in ["g", "g1-x", "g10"] {
        LogStore(GroupStore::new(db.clone(), id))
            .append_entries((1..=40).map(|i| entry(9, i)).collect())?;
    }

    let mut oracle: BTreeMap<u64, u64> = BTreeMap::new();
    let mut purged: Option<LogId<u64>> = None;
    let mut rng = 0x2700_u64;
    let mut next = || {
        rng = rng
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        rng >> 33
    };
    let mut term = 1;
    for round in 0..400 {
        let log = LogStore(GroupStore::new(db.clone(), "g1"));
        let first = purged.map_or(1, |id| id.index + 1);
        let last = oracle.keys().next_back().copied().unwrap_or(first - 1);
        match next() % 10 {
            0..=4 => {
                let count = 1 + next() % 6;
                log.append_entries((last + 1..=last + count).map(|i| entry(term, i)).collect())?;
                for i in last + 1..=last + count {
                    oracle.insert(i, term);
                }
            }
            5 | 6 if last >= first => {
                let from = first + next() % (last - first + 1);
                log.truncate_from(from)?;
                oracle.retain(|&i, _| i < from);
                term += 1;
            }
            7 | 8 if last >= first => {
                let through = first + next() % (last - first + 1);
                let id = log_id(oracle[&through], through);
                log.purge_through(id)?;
                oracle.retain(|&i, _| i > through);
                purged = Some(id);
            }
            9 => {
                // Flush to runs, then reopen: the last log id is recovered from the store.
                db.store().flush()?;
                drop(log);
                drop(db);
                db = Database::open_with(disk.path(), CLUSTER, 1, small(&fs))?;
                continue;
            }
            _ => continue,
        }
        let state = log.log_state()?;
        let expected = oracle
            .iter()
            .next_back()
            .map(|(&i, &t)| log_id(t, i))
            .or(purged);
        assert_eq!(state.last_log_id, expected, "round {round}");
        assert_eq!(state.last_purged_log_id, purged, "round {round}");
        let read: Vec<(u64, u64)> = log
            .entries((Unbounded, Unbounded), usize::MAX)?
            .iter()
            .map(|entry| (entry.log_id.index, entry.log_id.leader_id.term))
            .collect();
        assert_eq!(
            read,
            oracle.iter().map(|(&i, &t)| (i, t)).collect::<Vec<_>>()
        );
    }
    db.store().flush()?;
    assert!(
        db.store().level_runs().iter().sum::<usize>() > 0,
        "the log reached runs"
    );
    for id in ["g", "g1-x", "g10"] {
        let state = LogStore(GroupStore::new(db.clone(), id)).log_state()?;
        assert_eq!(state.last_log_id, Some(log_id(9, 40)), "neighbour {id}");
    }
    Ok(())
}

pub(super) fn broker_config(dir: &Path) -> BrokerConfig {
    BrokerConfig {
        id: 1,
        cluster_id: CLUSTER,
        data_dir: dir.into(),
        peer_token: "local-testing-peer-token-with-32-bytes".into(),
        peer_ca: None,
        peer_identity: None,
        allow_plaintext: true,
    }
}

/// Every `(file, offset)` under `dir` that holds `needle`.
pub(super) fn find(fs: &FaultFs, dir: &Path, needle: &[u8]) -> Vec<(PathBuf, u64)> {
    let mut hits = Vec::new();
    for path in fs.list(dir).unwrap_or_default() {
        let Ok(data) = fs.contents(&path) else {
            continue;
        };
        for (offset, window) in data.windows(needle.len()).enumerate() {
            if window == needle {
                hits.push((path.clone(), offset as u64));
            }
        }
    }
    hits
}

pub(super) fn wait_until(what: &str, done: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ns27_quarantined_group_is_not_started_and_is_reported() -> Result<()> {
    let disk = tempfile::tempdir()?;
    let fs = FaultFs::new();
    fault_root(&fs, disk.path());
    let broker = Broker::open_with(broker_config(disk.path()), small(&fs))?;
    let machine = Arc::new(crate::partition::Machine);
    let marker = b"<<ns27-damaged-group-record>>".to_vec();
    broker
        .database
        .run(move |db| {
            db.mark_hosted("damaged")?;
            db.mark_hosted("healthy")?;
            let mut write = WriteBatch::new();
            write.put(b"group/damaged/election-floor/v1", &marker);
            db.write(write)?;
            // A later synced record: evidence that the damaged one was acknowledged.
            let mut write = WriteBatch::new();
            write.put(b"group/healthy/unrelated-marker", b"{}");
            db.write(write)?;
            let mut write = WriteBatch::new();
            write.put(b"retired/v1/unrelated", b"1");
            db.write(write)
        })
        .await?;
    let image = fs.kill();
    drop(broker);
    let hits = find(
        &image,
        &disk.path().join(KV_DIR).join("journal"),
        b"<<ns27-damaged-group-record>>",
    );
    assert_eq!(hits.len(), 1, "{hits:?}");
    let (segment, offset) = &hits[0];
    image.flip_bit(segment, offset + 4, 2)?;

    let broker = Broker::open_with(broker_config(disk.path()), small(&image))?;
    let reported = broker.quarantined_groups();
    assert_eq!(reported.len(), 1, "{reported:?}");
    assert_eq!(reported[0].group, "damaged");
    assert!(reported[0].reason.contains("JournalRecord"), "{reported:?}");
    assert!(reported[0].first_bad_seq.is_some());
    // The startup path that reopens stored groups skips it; the healthy group remains.
    assert_eq!(broker.stored_group_ids()?, vec!["healthy".to_owned()]);
    assert!(
        broker
            .database
            .stored_group_ids()?
            .contains(&"damaged".to_owned())
    );

    // Not started: no Raft node, no vote, no route.
    let refused = broker.open_group("damaged", machine.clone()).await;
    assert!(
        matches!(&refused, Err(GroupError::Other(error)) if error.to_string().contains("quarantined")),
        "{:?}",
        refused.err()
    );
    assert!(broker.group("damaged").is_none());
    let healthy = broker.open_group("healthy", machine.clone()).await?;
    assert!(healthy.status().running);
    broker.close_group("healthy").await?;

    // Rule 4: once the membership change that removes this replica commits, retiring it
    // releases the damaged bytes; the retirement marker fences stale requests.
    assert!(broker.retire_group("damaged", 7).await?);
    assert!(broker.was_retired("damaged")?);
    let database = broker.database.clone();
    tokio::task::spawn_blocking(move || {
        wait_until("release", || database.quarantined_groups().is_empty())
    })
    .await?;
    assert!(!broker.retire_group("damaged", 7).await?);
    assert_eq!(broker.stored_group_ids()?, vec!["healthy".to_owned()]);
    Ok(())
}

#[test]
fn ns27_interrupted_release_of_a_retired_damaged_group_finishes_at_open() -> Result<()> {
    let disk = tempfile::tempdir()?;
    let fs = FaultFs::new();
    fault_root(&fs, disk.path());
    let db = Database::open_with(disk.path(), CLUSTER, 1, small(&fs))?;
    db.mark_hosted("gone")?;
    let mut write = WriteBatch::new();
    write.put(b"group/gone/vote", b"<<ns27-gone-record>>");
    db.write(write)?;
    db.mark_hosted("kept")?;
    let image = fs.kill();
    drop(db);
    let hits = find(
        &image,
        &disk.path().join(KV_DIR).join("journal"),
        b"<<ns27-gone-record>>",
    );
    assert_eq!(hits.len(), 1, "{hits:?}");
    image.flip_bit(&hits[0].0, hits[0].1 + 4, 1)?;

    let db = Database::open_with(disk.path(), CLUSTER, 1, small(&image))?;
    assert!(db.is_quarantined("gone"));
    // The retirement markers committed, then the process stopped before the release.
    let mut write = WriteBatch::new();
    write.delete(&Database::hosted_key("gone"));
    write.put(b"retired/v1/gone", b"3");
    db.write(write)?;
    let image = image.kill();
    drop(db);

    let db = Database::open_with(disk.path(), CLUSTER, 1, small(&image))?;
    wait_until("release", || !db.is_quarantined("gone"));
    assert_eq!(db.get(b"group/gone/vote")?, None);
    assert_eq!(db.stored_group_ids()?, vec!["kept".to_owned()]);
    Ok(())
}

#[test]
fn ns27_damage_found_while_serving_is_reported_by_group() -> Result<()> {
    let disk = tempfile::tempdir()?;
    let fs = FaultFs::new();
    fault_root(&fs, disk.path());
    let db = Database::open_with(disk.path(), CLUSTER, 1, small(&fs))?;
    let mut damaged = db.take_damage_events().expect("taken once");
    assert!(db.take_damage_events().is_none());
    let log = LogStore(GroupStore::new(db.clone(), "live"));
    let mut payload = entry(1, 1);
    payload.payload = EntryPayload::Normal(super::super::RaftCommand::Single(serde_json::json!(
        "<<ns27-live-journal-value>>"
    )));
    log.append_entries(vec![payload])?;
    let hits = find(
        &fs,
        &disk.path().join(KV_DIR).join("journal"),
        b"<<ns27-live-journal-value>>",
    );
    assert_eq!(hits.len(), 1, "{hits:?}");
    fs.flip_bit(&hits[0].0, hits[0].1 + 3, 5)?;

    assert!(log.entries((Unbounded, Unbounded), usize::MAX).is_err());
    assert_eq!(damaged.try_recv().ok().as_deref(), Some("live"));
    let reported = db.quarantined_groups();
    assert_eq!(reported.len(), 1, "{reported:?}");
    assert!(
        reported[0]
            .reason
            .contains(&format!("{:?}", DamageReason::JournalValue))
    );
    // Other keys keep serving.
    assert!(db.is_hosted("live").is_ok());
    Ok(())
}
