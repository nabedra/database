//! RV-EV regression tests: a full device never stops an Events Raft core (M1), a write
//! into a quarantined group is refused (L3), a failed release finishes without a restart
//! (L1), a graceful stop closes the store (L4), writes are paced (RV-FINAL L6), and the
//! RV-EV L6 test gaps (split appends, Events journal GC, damage found while serving).

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use nabedra_kv::fs::{Fault, FaultError, FaultFs, FsOp};
use openraft::{Entry, EntryPayload};
use serde_json::{Value, json};

use super::super::log::LogStore;
use super::super::{Broker, Group, GroupError, RaftCommand, StateMachine, TypeConfig};
use super::tests::{CLUSTER, broker_config, entry, fault_root, find, log_id, small};
use super::*;

/// Records each command's id once and bumps a stored count. `calls` counts every `apply`
/// call, so an entry applied twice shows.
#[derive(Default)]
struct Counter {
    calls: AtomicU64,
}

impl StateMachine for Counter {
    fn apply(&self, tx: &mut Transaction<'_>, command: Value) -> Result<Value> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let id = command["id"].as_u64().context("command id")?;
        let count = decode::<u64>(tx.get(b"count")?)?.unwrap_or(0) + 1;
        tx.put(b"count", &serde_json::to_vec(&count)?)?;
        tx.put(format!("seen/{id:020}").as_bytes(), b"1")?;
        Ok(json!(count))
    }
}

fn command(id: u64) -> Value {
    json!({ "id": id, "pad": "p".repeat(700) })
}

/// An initialized single-voter group led by this broker.
async fn rf1(broker: &Arc<Broker>, id: &str, machine: Arc<Counter>) -> Result<Arc<Group>> {
    let group = broker.open_group(id, machine).await?;
    group
        .initialize(BTreeMap::from([(1, "http://127.0.0.1:1".into())]))
        .await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while group.leader() != Some(1) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    Ok(group)
}

/// (stored count, recorded ids) as the group's state machine holds them.
async fn applied(group: &Group) -> Result<(u64, BTreeSet<u64>)> {
    Ok(group
        .read(|tx| {
            let count = decode::<u64>(tx.get(b"count")?)?.unwrap_or(0);
            let seen = tx
                .scan(b"seen/", None, 4096)?
                .into_iter()
                .map(|(key, _)| Ok(std::str::from_utf8(&key[5..])?.parse::<u64>()?))
                .collect::<Result<BTreeSet<u64>>>()?;
            Ok((count, seen))
        })
        .await?)
}

fn journal_dir(root: &Path) -> PathBuf {
    root.join(KV_DIR).join("journal")
}

/// The device is full: every journal segment preallocation fails with ENOSPC. kv
/// preallocates each segment, so the next roll returns a voided `NoSpace`.
fn fill_device(fs: &FaultFs, root: &Path) {
    fs.inject(
        Fault::on(FsOp::Preallocate)
            .path_prefix(journal_dir(root))
            .error(FaultError::Enospc)
            .repeat(),
    );
}

fn total_retries(health: &StorageHealth) -> u64 {
    health.no_space_retries.values().sum()
}

/// Writes one command at a time until one does not complete within a second: it met the
/// full device and is retrying. Returns the acknowledged ids and the pending write.
async fn write_until_stalled(
    group: &Arc<Group>,
    next: &mut u64,
) -> (
    BTreeSet<u64>,
    (u64, tokio::task::JoinHandle<Result<Value, GroupError>>),
) {
    let mut acked = BTreeSet::new();
    for _ in 0..400 {
        let id = *next;
        *next += 1;
        let writer = group.clone();
        let mut task = tokio::spawn(async move { writer.write(command(id)).await });
        match tokio::time::timeout(Duration::from_secs(1), &mut task).await {
            Ok(result) => {
                result
                    .expect("write task")
                    .unwrap_or_else(|error| panic!("write {id} failed: {error}"));
                acked.insert(id);
            }
            Err(_) => return (acked, (id, task)),
        }
    }
    panic!("no write met the full device");
}

/// NS27, F01, F03 (RV-EV M1): a full device makes Raft storage writes retry instead of
/// stopping the group's core. Once space frees up, the same core commits the stalled and
/// the next writes, and every entry is applied exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ns27_f01_f03_rv_ev_m1_full_device_does_not_stop_the_raft_core() -> Result<()> {
    let disk = tempfile::tempdir()?;
    let fs = FaultFs::new();
    fault_root(&fs, disk.path());
    let broker = Broker::open_with(broker_config(disk.path()), small(&fs))?;
    let machine = Arc::new(Counter::default());
    let group = rf1(&broker, "full", machine.clone()).await?;
    let mut next = 0;
    for _ in 0..3 {
        group.write(command(next)).await?;
        next += 1;
    }

    fill_device(&fs, disk.path());
    let (mut acked, (stalled, pending)) = write_until_stalled(&group, &mut next).await;
    acked.extend(0..3);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let health = broker.storage_health();
    assert!(group.status().running, "the core kept running");
    assert!(health.no_space, "{health:?}");
    assert!(health.last_no_space_ms.is_some(), "{health:?}");
    assert!(total_retries(&health) >= 3, "{health:?}");
    assert!(!health.recovery_required, "{health:?}");

    fs.clear_faults();
    match pending.await? {
        Ok(_) => {
            acked.insert(stalled);
        }
        // The request deadline passed while the device was full; the entry still commits.
        Err(GroupError::OutcomeUnknown) => {}
        Err(error) => panic!("stalled write {stalled}: {error}"),
    }
    let same = broker.group("full").expect("the group is still registered");
    assert!(Arc::ptr_eq(&same, &group), "no new group or core");
    group.write(command(next)).await?;
    acked.insert(next);
    assert!(group.status().running);
    assert!(!broker.storage_health().no_space);

    let (count, seen) = applied(&group).await?;
    assert_eq!(count as usize, seen.len(), "an entry was applied twice");
    assert!(acked.is_subset(&seen), "acked {acked:?}, seen {seen:?}");
    assert!(seen.len() <= acked.len() + 1, "seen {seen:?}");
    assert_eq!(machine.calls.load(Ordering::SeqCst), count);
    broker.close_group("full").await?;
    Ok(())
}

/// NS27, F01 (RV-EV M1): `NoSpace` on the append, the apply and the committed-index write
/// is retried where it failed. The writes complete on the same core, the retries are
/// counted per operation, and no entry is applied twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ns27_f01_rv_ev_m1_no_space_on_append_and_apply_is_retried() -> Result<()> {
    let disk = tempfile::tempdir()?;
    let fs = FaultFs::new();
    fault_root(&fs, disk.path());
    let broker = Broker::open_with(broker_config(disk.path()), small(&fs))?;
    let machine = Arc::new(Counter::default());
    let group = rf1(&broker, "retry", machine.clone()).await?;
    group.write(command(0)).await?;
    broker.database.inject_no_space.lock().unwrap().extend([
        ("append", 4),
        ("apply", 4),
        ("committed", 4),
    ]);
    for id in 1..=5 {
        group.write(command(id)).await?;
    }
    let health = broker.storage_health();
    for label in ["append", "apply", "committed"] {
        assert_eq!(health.no_space_retries.get(label), Some(&4), "{health:?}");
    }
    assert!(group.status().running);
    assert!(!health.no_space, "{health:?}");
    let (count, seen) = applied(&group).await?;
    assert_eq!(seen, (0..=5).collect());
    assert_eq!(count, 6);
    assert_eq!(machine.calls.load(Ordering::SeqCst), 6);
    broker.close_group("retry").await?;
    Ok(())
}

/// NS27 (RV-EV M1): closing a group ends a write parked on a full device; the retry never
/// holds the core's shutdown.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ns27_rv_ev_m1_close_ends_a_no_space_retry() -> Result<()> {
    let disk = tempfile::tempdir()?;
    let fs = FaultFs::new();
    fault_root(&fs, disk.path());
    let broker = Broker::open_with(broker_config(disk.path()), small(&fs))?;
    let group = rf1(&broker, "closing", Arc::new(Counter::default())).await?;
    fill_device(&fs, disk.path());
    let mut next = 0;
    let (_, (_, pending)) = write_until_stalled(&group, &mut next).await;
    tokio::time::timeout(Duration::from_secs(5), broker.close_group("closing"))
        .await
        .expect("close returns while a write waits for space")?;
    assert!(broker.group("closing").is_none());
    assert!(pending.await?.is_err());
    fs.clear_faults();
    Ok(())
}

/// NS27 (RV-EV L3): once a group is quarantined at run time, writes into its scope are
/// refused at once, whether or not the broker has stopped the group yet. Other groups
/// keep writing.
#[test]
fn ns27_rv_ev_l3_writes_into_a_quarantined_group_are_refused() -> Result<()> {
    let disk = tempfile::tempdir()?;
    let fs = FaultFs::new();
    fault_root(&fs, disk.path());
    let db = Database::open_with(disk.path(), CLUSTER, 1, small(&fs))?;
    let log = LogStore(GroupStore::new(db.clone(), "live"));
    let mut payload = entry(1, 1);
    payload.payload = EntryPayload::Normal(RaftCommand::Single(json!("<<rv-ev-l3-value>>")));
    log.append_entries(vec![payload])?;
    let hits = find(&fs, &journal_dir(disk.path()), b"<<rv-ev-l3-value>>");
    assert_eq!(hits.len(), 1, "{hits:?}");
    fs.flip_bit(&hits[0].0, hits[0].1 + 3, 5)?;
    assert!(log.entries((Unbounded, Unbounded), usize::MAX).is_err());
    assert!(db.is_quarantined("live"));

    for refused in [
        log.put_record(b"vote", &openraft::Vote::new(2, 1)),
        log.append_entries(vec![entry(1, 2)]),
        log.purge_through(log_id(1, 1)),
    ] {
        let error = refused.expect_err("a write into a quarantined group");
        assert!(error.to_string().contains("quarantined"), "{error:#}");
    }
    LogStore(GroupStore::new(db.clone(), "other")).append_entries(vec![entry(1, 1)])?;
    db.mark_hosted("other")?;
    Ok(())
}

/// NS27, L6 test gap (RV-EV): damage found while a group serves stops that group's Raft
/// node through the broker's damage task, and the group is reported.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ns27_rv_ev_damage_found_while_serving_closes_the_running_group() -> Result<()> {
    let disk = tempfile::tempdir()?;
    let fs = FaultFs::new();
    fault_root(&fs, disk.path());
    let broker = Broker::open_with(broker_config(disk.path()), small(&fs))?;
    let group = rf1(&broker, "serving", Arc::new(Counter::default())).await?;
    let healthy = rf1(&broker, "healthy", Arc::new(Counter::default())).await?;
    group.write(command(1)).await?;
    // A log record of the running group that nothing has read yet (a read would cache
    // it), far above its tail, as a lagging follower's entry would be.
    let mut damaged = entry(1, 1000);
    damaged.payload = EntryPayload::Normal(RaftCommand::Single(json!("<<rv-ev-serving-value>>")));
    let store = GroupStore::new(broker.database.clone(), "serving");
    group
        .snapshots
        .store
        .run(move |_| LogStore(store).append_entries(vec![damaged]))
        .await?;
    let hits = find(&fs, &journal_dir(disk.path()), b"<<rv-ev-serving-value>>");
    assert_eq!(hits.len(), 1, "{hits:?}");
    fs.flip_bit(&hits[0].0, hits[0].1 + 3, 5)?;

    // A read of the damaged entry (as a lagging follower's replication would do).
    let store = GroupStore::new(broker.database.clone(), "serving");
    let read = broker
        .database
        .run(move |_| {
            LogStore(store)
                .entries((Unbounded, Unbounded), usize::MAX)
                .map(|entries| entries.len())
        })
        .await;
    assert!(read.is_err(), "{read:?}");
    tokio::time::timeout(Duration::from_secs(10), async {
        while broker.group("serving").is_some() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the broker stops the damaged group");
    // Its Raft node is stopped: it neither acknowledges nor commits.
    assert!(group.write(command(3)).await.is_err());
    let reported = broker.quarantined_groups();
    assert_eq!(reported.len(), 1, "{reported:?}");
    assert_eq!(reported[0].group, "serving");
    assert!(broker.stored_group_ids()?.iter().all(|id| id != "serving"));
    assert!(broker.hosted_group_ids()?.contains(&"serving".to_owned()));
    // The healthy group keeps committing.
    healthy.write(command(2)).await?;
    assert!(healthy.status().running);
    broker.close_group("healthy").await?;
    Ok(())
}

/// NS27 (RV-EV L1): when the release after a durable retirement fails (here the flush
/// meets a full device), retrying the retirement finishes the release without a
/// restart. The retirement marker still fences a stale request afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ns27_rv_ev_l1_failed_release_is_retried_without_a_restart() -> Result<()> {
    let disk = tempfile::tempdir()?;
    let fs = FaultFs::new();
    fault_root(&fs, disk.path());
    let broker = Broker::open_with(broker_config(disk.path()), small(&fs))?;
    broker
        .database
        .run(|db| {
            db.mark_hosted("damaged")?;
            let mut write = WriteBatch::new();
            write.put(b"group/damaged/vote", b"<<rv-ev-l1-record>>");
            db.write(write)?;
            // A later synced record: evidence that the damaged one was acknowledged.
            db.mark_hosted("healthy")
        })
        .await?;
    let image = fs.kill();
    drop(broker);
    let hits = find(&image, &journal_dir(disk.path()), b"<<rv-ev-l1-record>>");
    assert_eq!(hits.len(), 1, "{hits:?}");
    image.flip_bit(&hits[0].0, hits[0].1 + 4, 2)?;

    let broker = Broker::open_with(broker_config(disk.path()), small(&image))?;
    assert!(broker.is_quarantined("damaged"));
    image.inject(
        Fault::on(FsOp::Write)
            .path_prefix(disk.path().join(KV_DIR).join("runs"))
            .error(FaultError::Enospc)
            .repeat(),
    );
    assert!(broker.retire_group("damaged", 7).await.is_err());
    assert!(broker.was_retired("damaged")?, "the retirement is durable");
    assert!(
        broker.is_quarantined("damaged"),
        "the release did not finish"
    );
    assert!(broker.retire_group("damaged", 7).await.is_err());

    image.clear_faults();
    assert!(broker.retire_group("damaged", 7).await?);
    assert!(!broker.is_quarantined("damaged"));
    assert!(broker.quarantined_groups().is_empty());
    assert!(!broker.retire_group("damaged", 7).await?);
    assert_eq!(broker.hosted_group_ids()?, vec!["healthy".to_owned()]);
    Ok(())
}

/// NS27 (RV-EV L4): a graceful stop closes every group and then the store, and waits for
/// it: the directory lock is free when `shutdown` returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ns27_rv_ev_l4_graceful_shutdown_closes_the_store() -> Result<()> {
    let disk = tempfile::tempdir()?;
    let fs = FaultFs::new();
    fault_root(&fs, disk.path());
    let broker = Broker::open_with(broker_config(disk.path()), small(&fs))?;
    let group = rf1(&broker, "stopping", Arc::new(Counter::default())).await?;
    group.write(command(1)).await?;
    drop(group);
    broker.shutdown(Duration::from_secs(10)).await?;

    let kv_dir = disk.path().join(KV_DIR);
    let reopened = tokio::task::spawn_blocking(move || Store::open(&kv_dir, small(&fs))).await?;
    let opened = reopened.expect("the store was closed and its lock released");
    tokio::task::spawn_blocking(move || opened.store.close()).await??;
    Ok(())
}

/// RV-FINAL L6: Events writes are paced while L0 is at its slowdown trigger, so a write
/// burst slows down gradually instead of meeting the hard stop.
#[test]
fn ns27_rv_final_l6_events_writes_are_paced() -> Result<()> {
    let disk = tempfile::tempdir()?;
    let fs = FaultFs::new();
    fault_root(&fs, disk.path());
    let mut options = small(&fs);
    options.memtable_bytes = 8 << 20;
    options.max_replay_bytes = 8 << 20;
    options.levels.l0_compaction_trigger = 64;
    options.levels.l0_slowdown_writes = 1;
    options.levels.l0_stop_writes = 128;
    options.levels.delayed_write_rate = 64 << 10;
    let db = Database::open_with(disk.path(), CLUSTER, 1, options)?;
    let group = GroupStore::new(db.clone(), "paced");
    let write = |n: u8| {
        let mut batch = WriteBatch::new();
        batch.put(&group.key(&[b'k', n]), &[n; 16 << 10]);
        db.write(batch)
    };
    write(0)?;
    db.store().flush()?;
    assert_eq!(
        db.store().level_runs()[0],
        1,
        "one L0 run: writes slow down"
    );
    let started = Instant::now();
    for n in 1..=4 {
        write(n)?;
    }
    // 64 KiB at 64 KiB/s.
    assert!(
        started.elapsed() >= Duration::from_millis(600),
        "{:?}",
        started.elapsed()
    );
    Ok(())
}

/// About 1.9 MiB of command per entry: ten entries exceed one 16 MiB append batch.
fn big_entry(index: u64) -> Entry<TypeConfig> {
    let fill = char::from(b'a' + (index % 26) as u8);
    Entry {
        log_id: log_id(1, index),
        payload: EntryPayload::Normal(RaftCommand::Single(json!(
            fill.to_string().repeat(1_900_000)
        ))),
    }
}

fn split_options(fs: &FaultFs) -> nabedra_kv::Options {
    let mut options = Database::options(Arc::new(fs.clone()));
    options.journal.segment_bytes = 20 << 20;
    options
}

fn entry_bytes(entries: &[Entry<TypeConfig>]) -> Vec<Vec<u8>> {
    entries
        .iter()
        .map(|entry| serde_json::to_vec(entry).expect("entry encodes"))
        .collect()
}

/// NS27, L6 test gap (RV-EV): an append above the 16 MiB batch size is written as several
/// synced batches. A crash or power loss at any point leaves a prefix of the entries (Raft
/// allows that, since the append is acknowledged only after the last batch), and an
/// acknowledged append survives whole.
#[test]
fn ns27_rv_ev_split_append_leaves_a_prefix_at_every_crash_point() -> Result<()> {
    let disk = tempfile::tempdir()?;
    let root = disk.path();
    let entries: Vec<_> = (1..=10).map(big_entry).collect();
    let expected = entry_bytes(&entries);
    let total = {
        let fs = FaultFs::new();
        fault_root(&fs, root);
        let db = Database::open_with(root, CLUSTER, 1, split_options(&fs))?;
        let journal = db.store().stats().journal_syncs;
        LogStore(GroupStore::new(db.clone(), "split")).append_entries(entries.clone())?;
        assert!(
            db.store().stats().journal_syncs >= journal + 2,
            "the append took more than one synced batch"
        );
        drop(db);
        fs.mutating_ops()
    };
    let mut prefixes = BTreeSet::new();
    for n in 0..=total + 2 {
        let fs = FaultFs::new();
        fault_root(&fs, root);
        fs.crash_after(n);
        let acknowledged = Database::open_with(root, CLUSTER, 1, split_options(&fs))
            .ok()
            .map(|db| {
                LogStore(GroupStore::new(db.clone(), "split"))
                    .append_entries(entries.clone())
                    .is_ok()
            });
        for (kind, image) in [("crash", fs.crash()), ("power loss", fs.power_loss(n))] {
            let db = Database::open_with(root, CLUSTER, 1, split_options(&image))
                .unwrap_or_else(|error| panic!("{kind} after {n} ops: reopen: {error:#}"));
            let log = LogStore(GroupStore::new(db.clone(), "split"));
            let read = entry_bytes(&log.entries((Unbounded, Unbounded), usize::MAX)?);
            assert!(
                read.len() <= expected.len() && read[..] == expected[..read.len()],
                "{kind} after {n} ops: not a prefix ({} entries)",
                read.len()
            );
            if acknowledged == Some(true) {
                assert_eq!(read.len(), expected.len(), "{kind} after {n}: acked append");
            }
            let state = log.log_state()?;
            assert_eq!(
                state.last_log_id.map(|id| id.index),
                (!read.is_empty()).then_some(read.len() as u64),
                "{kind} after {n} ops"
            );
            prefixes.insert(read.len());
        }
    }
    assert!(
        prefixes.contains(&0) && prefixes.contains(&10),
        "{prefixes:?}"
    );
    assert!(
        prefixes.iter().any(|&len| len > 0 && len < 10),
        "some crash point left a partial prefix: {prefixes:?}"
    );
    Ok(())
}

// ---- NS13 for Events: Raft log entries as journal values under journal GC -------------

const GC_SEGMENT: u64 = 32 << 10;
const GC_MAX_JOURNAL: u64 = 6 * GC_SEGMENT;
const LAGGING: &str = "lagging";
const BUSY: &str = "busy";

fn gc_options(fs: &FaultFs) -> nabedra_kv::Options {
    let mut options = Database::options(Arc::new(fs.clone()));
    options.journal.segment_bytes = GC_SEGMENT;
    options.journal.max_group_bytes = 16 << 10;
    options.max_journal_bytes = GC_MAX_JOURNAL;
    // Log entries are journal values, so the memtable holds only locators. Flushes run
    // when the test asks for them.
    options.max_replay_bytes = 8 << 20;
    options
}

#[derive(Default)]
struct GcOracle {
    entries: BTreeMap<&'static str, BTreeMap<u64, Vec<u8>>>,
    purged: BTreeMap<&'static str, u64>,
    next: BTreeMap<&'static str, u64>,
}

impl GcOracle {
    fn append(&mut self, db: &Arc<Database>, group: &'static str) -> Result<()> {
        let index = *self.next.entry(group).or_insert(1);
        self.next.insert(group, index + 1);
        let pad = char::from(b'a' + (index % 26) as u8)
            .to_string()
            .repeat(1200 + (index as usize * 37) % 600);
        let entry = Entry {
            log_id: log_id(1, index),
            payload: EntryPayload::Normal(RaftCommand::Single(json!(pad))),
        };
        let bytes = serde_json::to_vec(&entry)?;
        LogStore(GroupStore::new(db.clone(), group)).append_entries(vec![entry])?;
        self.entries.entry(group).or_default().insert(index, bytes);
        Ok(())
    }

    fn purge(&mut self, db: &Arc<Database>, group: &'static str, upto: u64) -> Result<()> {
        LogStore(GroupStore::new(db.clone(), group)).purge_through(log_id(1, upto))?;
        self.entries
            .entry(group)
            .or_default()
            .retain(|&index, _| index > upto);
        self.purged.insert(group, upto);
        Ok(())
    }

    /// The busy group appends `count` entries and keeps only its last four. With
    /// `flush_every`, it flushes and runs journal GC after that many appends.
    fn busy(&mut self, db: &Arc<Database>, count: usize, flush_every: Option<usize>) -> Result<()> {
        for n in 0..count {
            self.append(db, BUSY)?;
            let last = self.next[BUSY] - 1;
            if last % 8 == 7 {
                self.purge(db, BUSY, last - 4)?;
            }
            if flush_every.is_some_and(|every| n % every == every - 1) {
                db.store().flush()?;
                db.store().gc_journal()?;
            }
        }
        Ok(())
    }

    fn lag_bytes(&self) -> u64 {
        self.entries.get(LAGGING).map_or(0, |entries| {
            entries.values().map(|bytes| bytes.len() as u64).sum()
        })
    }

    /// Every retained entry of both groups reads back exactly; no locator dangles.
    fn check(&self, db: &Arc<Database>, context: &str) {
        for group in [LAGGING, BUSY] {
            let log = LogStore(GroupStore::new(db.clone(), group));
            let found: BTreeMap<u64, Vec<u8>> = log
                .entries((Unbounded, Unbounded), usize::MAX)
                .unwrap_or_else(|error| panic!("{context}: {group}: read: {error:#}"))
                .iter()
                .map(|entry| (entry.log_id.index, serde_json::to_vec(entry).unwrap()))
                .collect();
            let expected = self.entries.get(group).cloned().unwrap_or_default();
            assert!(
                found == expected,
                "{context}: {group} retained {:?}, expected {:?}",
                found.keys().collect::<Vec<_>>(),
                expected.keys().collect::<Vec<_>>()
            );
            let state = log.log_state().unwrap();
            let purged = self.purged.get(group).copied();
            assert_eq!(
                state.last_purged_log_id.map(|id| id.index),
                purged,
                "{context}: {group} purge marker"
            );
            assert_eq!(
                state.last_log_id.map(|id| id.index),
                expected.keys().next_back().copied().or(purged),
                "{context}: {group} tail"
            );
        }
        assert_eq!(db.store().stats().recycled_value_reads, 0, "{context}");
        assert!(db.quarantined_groups().is_empty(), "{context}");
    }
}

fn live_segments(fs: &FaultFs, root: &Path) -> Vec<u64> {
    let mut live: Vec<u64> = fs
        .list(&journal_dir(root))
        .unwrap_or_default()
        .iter()
        .filter(|file| file.extension().is_some_and(|ext| ext == "jnl"))
        .filter_map(|file| file.file_stem()?.to_str()?.parse().ok())
        .collect();
    live.sort_unstable();
    live
}

fn maintain(db: &Database) -> nabedra_kv::Result<()> {
    db.store().flush()?;
    db.store().compact()?;
    db.store().gc_journal().map(|_| ())
}

/// NS13, NS27 (RV-EV L6 test gap; plan section 2, "Journal values"): an Events group whose
/// log lags pins the oldest journal segment. Once the journal passes `max_journal_bytes`,
/// GC relocates its entries (`relocated_bytes` counts them) and old segments recycle, so
/// the journal stays near its budget. Reads match an oracle after every phase and after
/// two crash reopens.
#[test]
fn ns13_ns27_rv_ev_lagging_events_group_pins_the_journal_until_gc_relocates_it() -> Result<()> {
    let disk = tempfile::tempdir()?;
    let root = disk.path();
    let fs = FaultFs::new();
    fault_root(&fs, root);
    let db = Database::open_with(root, CLUSTER, 1, gc_options(&fs))?;
    let mut oracle = GcOracle::default();

    // Phase 1: inside the budget nothing moves, and the lagging group pins segment 0.
    for _ in 0..4 {
        oracle.append(&db, LAGGING)?;
    }
    let lagging = oracle.lag_bytes();
    let pinned = live_segments(&fs, root)[0];
    oracle.busy(&db, 24, Some(8))?;
    db.store().flush()?;
    db.store().gc_journal()?;
    oracle.check(&db, "phase 1");
    assert_eq!(live_segments(&fs, root)[0], pinned);
    assert_eq!(db.store().stats().relocated_bytes, 0);

    // Phase 2: far past the budget, GC relocates the lagging entries.
    oracle.busy(&db, 160, Some(8))?;
    maintain(&db)?;
    oracle.check(&db, "phase 2");
    let live = live_segments(&fs, root);
    let stats = db.store().stats();
    assert!(stats.journal_bytes > 3 * GC_MAX_JOURNAL, "{stats:?}");
    assert!(
        live[0] > pinned,
        "GC moved the lagging entries off {pinned}: {live:?}"
    );
    assert!(
        stats.relocated_bytes >= lagging,
        "{stats:?}, lagging {lagging}"
    );
    assert!(
        live.len() as u64 * GC_SEGMENT <= GC_MAX_JOURNAL + 3 * GC_SEGMENT,
        "the journal stays near its budget: {live:?}"
    );

    // Phase 3: the lagging group purges; the journal keeps recycling.
    oracle.purge(&db, LAGGING, 3)?;
    oracle.busy(&db, 160, Some(8))?;
    maintain(&db)?;
    oracle.check(&db, "phase 3");
    let live = live_segments(&fs, root);
    assert!(live.len() as u64 * GC_SEGMENT <= GC_MAX_JOURNAL + 3 * GC_SEGMENT);

    // A last unflushed lagging entry, then a crash: the reopen replays it.
    oracle.append(&db, LAGGING)?;
    let crashed = fs.crash();
    drop(db);
    let db = Database::open_with(root, CLUSTER, 1, gc_options(&crashed))?;
    oracle.check(&db, "reopen");
    drop(db);
    let db = Database::open_with(root, CLUSTER, 1, gc_options(&crashed.crash()))?;
    oracle.check(&db, "second reopen");
    Ok(())
}

/// The relocation workload up to the GC window: lagging entries in the oldest segment,
/// then a busy tail far past the budget with no flush.
fn relocation_prefix(db: &Arc<Database>, oracle: &mut GcOracle) -> Result<()> {
    for _ in 0..4 {
        oracle.append(db, LAGGING)?;
    }
    oracle.busy(db, 16, Some(8))?;
    oracle.busy(db, 48, None)
}

fn relocation_window(db: &Database) -> nabedra_kv::Result<()> {
    db.store().flush()?;
    db.store().gc_journal()?;
    db.store().compact()?;
    db.store().gc_journal().map(|_| ())
}

/// NS04, NS13, NS27 (RV-EV L6 test gap): a crash at every mutating operation of an Events
/// journal-GC window (relocation, the flush that lets it retire, compaction, retirement)
/// recovers both groups' logs exactly, with no dangling locator, twice.
#[test]
fn ns04_ns13_ns27_rv_ev_crash_at_every_events_relocation_step_keeps_the_log() -> Result<()> {
    let (window, relocated) = {
        let disk = tempfile::tempdir()?;
        let fs = FaultFs::new();
        fault_root(&fs, disk.path());
        let db = Database::open_with(disk.path(), CLUSTER, 1, gc_options(&fs))?;
        let mut oracle = GcOracle::default();
        relocation_prefix(&db, &mut oracle)?;
        let start = fs.mutating_ops();
        let before = db.store().stats().relocated_bytes;
        relocation_window(&db)?;
        oracle.check(&db, "reference");
        (
            fs.mutating_ops() - start,
            db.store().stats().relocated_bytes - before,
        )
    };
    assert!(relocated > 0, "the window relocates");
    let mut crashed = 0;
    for k in 0..=window + 4 {
        let disk = tempfile::tempdir()?;
        let root = disk.path();
        let fs = FaultFs::new();
        fault_root(&fs, root);
        let db = Database::open_with(root, CLUSTER, 1, gc_options(&fs))?;
        let mut oracle = GcOracle::default();
        relocation_prefix(&db, &mut oracle)?;
        fs.crash_after(fs.mutating_ops() + k);
        let _ = relocation_window(&db);
        crashed += usize::from(fs.is_crashed());
        let image = fs.crash();
        drop(db);
        let context = format!("crash after window op {k} of {window}");
        let db = Database::open_with(root, CLUSTER, 1, gc_options(&image))
            .unwrap_or_else(|error| panic!("{context}: reopen: {error:#}"));
        oracle.check(&db, &context);
        drop(db);
        let db = Database::open_with(root, CLUSTER, 1, gc_options(&image.crash()))
            .unwrap_or_else(|error| panic!("{context}: second reopen: {error:#}"));
        oracle.check(&db, &format!("{context}, second reopen"));
    }
    // Background work adds a timing-dependent number of operations; most crash points
    // must still land inside the window.
    assert!(
        crashed as u64 * 4 >= window * 3,
        "{crashed} of {window} crash points inside the window"
    );
    Ok(())
}
