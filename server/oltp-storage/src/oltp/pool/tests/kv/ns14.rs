//! NS14 at pool level: sticky R6 (NS24, O06), release only with the evidence
//! that the damaged replica was removed (NS23 rule 4), a release interrupted
//! before its manifest edit (rule 4, crash), and run-time damage publication
//! (rule 1). Damage is injected on `FaultFs`; crash evidence is that fault model.
use super::*;
use crate::oltp::pool::{ACCEPT_DAMAGE_FILE, DamagedReplicaRemoved, ReleaseOutcome};
use nabedra_oltp_types::types::ReplicaMember;

/// Creates `count` groups with one synced entry each, holding `needle(n)`,
/// then a later synced write as evidence. Returns the group identities.
async fn damaged_layout(
    pool: &OltpPool,
    count: u8,
    needle: impl Fn(u8) -> String,
) -> Vec<GroupIdentity> {
    let mut ids = Vec::new();
    for n in 0..count {
        let group = pool.group(group_identity(pool)).await.unwrap();
        group
            .log_transaction(budget(), put_entry(0, b"entry", needle(n).into_bytes()))
            .await
            .unwrap();
        ids.push(group.identity().clone());
    }
    let last = pool.group(ids[0].clone()).await.unwrap();
    last.log_transaction(budget(), put_entry(1, b"entry2", b"later".to_vec()))
        .await
        .unwrap();
    ids
}

fn removal(pool: &PoolIdentity, group: &GroupIdentity) -> DamagedReplicaRemoved {
    DamagedReplicaRemoved {
        group: group.clone(),
        removed: ReplicaMember {
            node_id: 2,
            worker_id: pool.worker_id,
            pool_id: pool.pool_id,
        },
        committed_at: AppliedPosition {
            term: 3,
            node_id: 1,
            index: 40,
        },
    }
}

fn scope_is_empty(pool: &OltpPool, id: Uuid) -> bool {
    let snapshot = pool.kv_store().snapshot();
    let scope = group_scope(id);
    let mut iter = RawIter::bounded(&snapshot, &scope, &prefix_end(&scope), false);
    iter.seek(&scope);
    let empty = iter.key().is_none();
    iter.status().unwrap();
    empty
}

async fn entry(pool: &OltpPool, id: &GroupIdentity) -> Vec<u8> {
    let group = pool.retained_group(id.clone()).await.unwrap();
    group
        .log_read(budget(), |view| {
            view.get(b"entry").and_then(|v| v.ok_or_else(capacity))
        })
        .await
        .unwrap()
        .as_slice()
        .to_vec()
}

/// NS24 / O06, R6 (RV-NS6 F2, NS6fix open concern 1): new damage of two of
/// twelve groups in one open refuses the pool, and keeps refusing it on every
/// later open, although kv persisted the quarantine at the first one. An
/// operator's marker accepts the damage once; the pool then opens with both
/// groups quarantined, removes the marker and stays openable.
#[tokio::test]
async fn ns24_o06_r6_refusal_is_sticky_until_an_operator_accepts_it() {
    let dir = Dir::new();
    let fault = FaultFs::new();
    let identity = pool_identity();
    let resources = shared(SharedResourceConfig::default());
    let pool = fault_pool(&dir, &fault, identity.clone(), resources.clone());
    let needle = |n: u8| format!("ns24-r6-target-{n:02}-0123456789abcdef");
    let ids = damaged_layout(&pool, 12, needle).await;
    drop(pool);
    let fault = fault.kill();
    damage_journal_value(&dir, &fault, needle(4).as_bytes());
    damage_journal_value(&dir, &fault, needle(5).as_bytes());

    for attempt in 0..3 {
        let error = reopen(&dir, &fault, identity.clone(), resources.clone())
            .err()
            .unwrap_or_else(|| panic!("open {attempt} accepted damage of 2 of 12 groups"));
        assert_eq!(error.code, ErrorCode::StorageUnavailable);
        assert!(error.message.contains("too many"), "{}", error.message);
    }

    let marker = dir.0.join(ACCEPT_DAMAGE_FILE);
    fs::write(&marker, b"").unwrap();
    let pool = reopen(&dir, &fault, identity.clone(), resources.clone()).unwrap();
    assert!(!marker.exists(), "the operator's marker is consumed");
    let mut quarantined = pool.quarantined_groups();
    quarantined.sort();
    let mut expected = vec![ids[4].group_id, ids[5].group_id];
    expected.sort();
    assert_eq!(quarantined, expected);
    drop(pool);

    let pool = reopen(&dir, &fault, identity, resources).unwrap();
    let mut quarantined = pool.quarantined_groups();
    quarantined.sort();
    assert_eq!(quarantined, expected);
    assert_eq!(
        entry(&pool, &ids[1]).await,
        needle(1).into_bytes(),
        "a healthy group still serves"
    );
}

/// NS23 / O06, section 3 rules 1 and 4 at pool level: a quarantined group
/// never opens and its refusals are counted. Its bytes are deleted only with
/// the evidence that its replica was removed from the group, for this pool.
/// The release deletes exactly that group's scope and its snapshot files,
/// clears the quarantine, keeps every other group, and is idempotent.
#[tokio::test]
async fn ns23_o06_release_deletes_only_a_removed_replicas_group() {
    let dir = Dir::new();
    let fault = FaultFs::new();
    let identity = pool_identity();
    let resources = shared(SharedResourceConfig::default());
    let pool = fault_pool(&dir, &fault, identity.clone(), resources.clone());
    let needle = |n: u8| format!("ns23-release-target-{n:02}-0123456789abcdef");
    let ids = damaged_layout(&pool, 3, needle).await;
    drop(pool);
    let fault = fault.kill();
    damage_journal_value(&dir, &fault, needle(1).as_bytes());
    let pool = reopen(&dir, &fault, identity.clone(), resources.clone()).unwrap();
    let damaged = ids[1].clone();
    assert_eq!(pool.quarantined_groups(), vec![damaged.group_id]);
    let damage = pool.group_damage(damaged.group_id).unwrap();
    assert_eq!(damage.cause, crate::oltp::pool::DamageCause::JournalRecord);
    assert!(damage.first_bad_seq.is_some());

    // Rule 1: the group never opens; each attempt is refused and counted.
    let before = pool.quarantine_refusals();
    let refused = pool.retained_group(damaged.clone()).await.err().unwrap();
    assert!(
        refused.message.starts_with("storage corrupt"),
        "{}",
        refused.message
    );
    assert_eq!(pool.quarantine_refusals(), before + 1);

    // A stale snapshot file of the damaged group.
    let snapshots = dir.0.join("snapshots").join(damaged.group_id.to_string());
    fs::create_dir_all(&snapshots).unwrap();
    let stale = snapshots.join(format!("snap-{}.bin", nabedra_rt::ids::uuid_v4()));
    fs::write(&stale, b"stale").unwrap();

    // Evidence for another pool deletes nothing.
    let mut foreign = removal(&identity, &damaged);
    foreign.removed.pool_id = nabedra_rt::ids::uuid_v4();
    pool.release_damaged_group(foreign).await.unwrap_err();
    assert!(pool.is_group_quarantined(damaged.group_id));
    assert!(stale.exists());
    assert_eq!(pool.damage_releases(), 0);

    let outcome = pool
        .release_damaged_group(removal(&identity, &damaged))
        .await
        .unwrap();
    assert_eq!(outcome, ReleaseOutcome::Released);
    assert!(!pool.is_group_quarantined(damaged.group_id));
    assert!(pool.damaged_groups().is_empty());
    assert!(scope_is_empty(&pool, damaged.group_id));
    assert!(!stale.exists(), "the group's snapshot files are retired");
    assert_eq!(pool.damage_releases(), 1);
    assert_eq!(resources.storage_stats().quarantined_groups, 0);
    for n in [0usize, 2] {
        assert_eq!(entry(&pool, &ids[n]).await, needle(n as u8).into_bytes());
    }
    let again = pool
        .release_damaged_group(removal(&identity, &damaged))
        .await
        .unwrap();
    assert_eq!(again, ReleaseOutcome::AlreadyReleased);
    drop(pool);

    let pool = reopen(&dir, &fault, identity, resources).unwrap();
    assert!(pool.quarantined_groups().is_empty());
    assert!(scope_is_empty(&pool, damaged.group_id));
    // The damaged identity's record is gone: it cannot be reopened.
    assert!(pool.retained_group(damaged).await.is_err());
    assert_eq!(entry(&pool, &ids[2]).await, needle(2).into_bytes());
}

/// NS23 rule 4, crash: a release whose manifest edit fails leaves the group
/// quarantined, and nothing of another group is touched. After the restart the
/// group is still quarantined, and repeating the release (as the damage
/// reporter does when Control's record says the replica was replaced)
/// finishes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ns23_o06_a_release_cut_before_its_manifest_edit_finishes_after_restart() {
    let dir = Dir::new();
    let fault = FaultFs::new();
    let identity = pool_identity();
    let resources = shared(SharedResourceConfig::default());
    let pool = fault_pool(&dir, &fault, identity.clone(), resources.clone());
    let needle = |n: u8| format!("ns23-cut-target-{n:02}-0123456789abcdef");
    let ids = damaged_layout(&pool, 3, needle).await;
    drop(pool);
    let fault = fault.kill();
    damage_journal_value(&dir, &fault, needle(0).as_bytes());
    let pool = reopen(&dir, &fault, identity.clone(), resources.clone()).unwrap();
    let damaged = ids[0].clone();
    assert_eq!(pool.quarantined_groups(), vec![damaged.group_id]);

    let kv = dir.0.join("kv");
    for path in fault.list(&kv).unwrap() {
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        if name.starts_with("MANIFEST-") {
            for op in [FsOp::Write, FsOp::SyncData, FsOp::SyncAll] {
                fault.inject(Fault::on(op).path_prefix(&path).repeat());
            }
        }
    }
    pool.release_damaged_group(removal(&identity, &damaged))
        .await
        .expect_err("the manifest edit failed");
    assert!(pool.is_group_quarantined(damaged.group_id));
    assert_eq!(pool.damage_releases(), 0);
    drop(pool);

    let fault = fault.kill();
    let pool = reopen(&dir, &fault, identity.clone(), resources.clone()).unwrap();
    assert_eq!(pool.quarantined_groups(), vec![damaged.group_id]);
    for n in [1usize, 2] {
        assert_eq!(entry(&pool, &ids[n]).await, needle(n as u8).into_bytes());
    }
    let outcome = pool
        .release_damaged_group(removal(&identity, &damaged))
        .await
        .unwrap();
    assert_eq!(outcome, ReleaseOutcome::Released);
    drop(pool);
    let pool = reopen(&dir, &fault.kill(), identity, resources).unwrap();
    assert!(pool.quarantined_groups().is_empty());
    assert!(scope_is_empty(&pool, damaged.group_id));
}

/// NS23 rule 1 at run time: kv quarantines a group while its handle is open
/// (a damaged run block met by a read). The pool publishes the group on its
/// damage channel, so its host can stop; a release is refused while that
/// handle is still open, and succeeds once it is dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ns23_o06_run_time_damage_is_published_and_an_open_handle_blocks_release() {
    let dir = Dir::new();
    let fault = FaultFs::new();
    fs::create_dir_all(&dir.0).unwrap();
    fault.create_dir_all_durable(&dir.0);
    let identity = pool_identity();
    let resources = shared(SharedResourceConfig::default());
    let tune: fn(&mut nabedra_kv::Options) = |options| {
        options.levels.l0_compaction_trigger = 64;
        options.levels.l0_slowdown_writes = 63;
        options.levels.l0_stop_writes = 64;
    };
    let open = |fault: &FaultFs| {
        OltpPool::open_development_tuned(
            &dir.0,
            identity.clone(),
            resources.clone(),
            Arc::new(fault.clone()),
            tune,
        )
        .unwrap()
    };
    let flush = |pool: &OltpPool| {
        let pool = pool.clone();
        nabedra_rt::task::spawn_blocking(move || pool.kv_store().flush().unwrap())
    };
    let pool = open(&fault);
    flush(&pool).await.unwrap();
    let a = pool.group(group_identity(&pool)).await.unwrap();
    for n in 0..40u8 {
        let mut value = format!("<<ns23-a-row-{n:02}>>").into_bytes();
        value.resize(1 << 10, b'.');
        let key = format!("row-{n:02}").into_bytes();
        a.log_transaction(budget(), move |tx| {
            tx.put(&key, &value)?;
            Ok(Transition::Commit(tx.copy_output(&[1])?))
        })
        .await
        .unwrap();
    }
    flush(&pool).await.unwrap();
    let a_id = a.identity().clone();
    drop((a, pool));
    let runs = dir.0.join("kv").join("runs");
    let (run, at) = fault
        .list(&runs)
        .unwrap()
        .into_iter()
        .find_map(|path| {
            let bytes = fault.contents(&path).ok()?;
            let needle = b"<<ns23-a-row-35>>";
            bytes
                .windows(needle.len())
                .position(|w| w == needle)
                .map(|at| (path, at as u64))
        })
        .unwrap();
    fault.flip_bit(&run, at + 4, 2).unwrap();

    let pool = open(&fault);
    assert!(pool.quarantined_groups().is_empty());
    let mut damage = pool.watch_damage();
    assert!(damage.borrow().is_empty());
    let a = pool.retained_group(a_id.clone()).await.unwrap();
    a.log_read(budget(), |view| {
        view.get(b"row-35").and_then(|v| v.ok_or_else(capacity))
    })
    .await
    .unwrap_err();
    nabedra_rt::time::timeout(
        Duration::from_secs(5),
        damage.wait_for(|groups| groups.contains(&a_id.group_id)),
    )
    .await
    .expect("run-time damage is published")
    .unwrap();

    let blocked = pool
        .release_damaged_group(removal(&identity, &a_id))
        .await
        .unwrap_err();
    assert_eq!(blocked.code, ErrorCode::Conflict);
    assert!(pool.is_group_quarantined(a_id.group_id));
    drop(a);
    assert_eq!(
        pool.release_damaged_group(removal(&identity, &a_id))
            .await
            .unwrap(),
        ReleaseOutcome::Released
    );
    drop(pool);
    // Run-time quarantine was accepted (R6) and then released: the reopen
    // counts no new damage and finds nothing quarantined.
    let pool = open(&fault.kill());
    assert!(pool.quarantined_groups().is_empty());
}

/// NS24 / O26 / O06: one worker, two pools. The second pool is damaged in
/// turn by R1 (a store-wide record with evidence), R2 (EIO on a recovery
/// read), R3 (a bad `CURRENT`) and R6 (two of twelve groups at once). Each
/// open of it is refused with the matching error, again on every retry,
/// while the first pool keeps serving writes and reads, and no refused open
/// leaks a pool slot. After R2's device reads again the pool opens with every
/// synced write: an unreadable region is never truncated as a torn tail.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ns24_o26_o06_a_refused_pool_leaves_the_other_pool_serving() {
    let resources = shared(SharedResourceConfig::default());
    let healthy_dir = Dir::new();
    let healthy_fault = FaultFs::new();
    let identity = pool_identity();
    let healthy = fault_pool(
        &healthy_dir,
        &healthy_fault,
        identity.clone(),
        resources.clone(),
    );
    let serving = healthy.group(group_identity(&healthy)).await.unwrap();
    let mut index = 0;
    for case in ["r1", "r2", "r3", "r6"] {
        let dir = Dir::new();
        let fault = FaultFs::new();
        let damaged = PoolIdentity {
            pool_id: nabedra_rt::ids::uuid_v4(),
            ..identity.clone()
        };
        let pool = fault_pool(&dir, &fault, damaged.clone(), resources.clone());
        let needle = |n: u8| format!("ns24-{case}-target-{n:02}-0123456789abcdef");
        let ids = damaged_layout(&pool, 12, needle).await;
        drop(pool);
        let fault = fault.kill();
        let expected = match case {
            "r1" => {
                damage_journal_value(&dir, &fault, damaged.pool_id.to_string().as_bytes());
                "storage corrupt"
            }
            "r2" => {
                let target = needle(3).into_bytes();
                let mut injected = false;
                for segment in fault.list(&journal_dir(&dir)).unwrap() {
                    let bytes = fault.contents(&segment).unwrap();
                    if let Some(at) = bytes
                        .windows(target.len())
                        .position(|w| w == target.as_slice())
                    {
                        fault.inject(
                            Fault::on(FsOp::Read)
                                .path_prefix(&segment)
                                .range(at as u64, target.len() as u64)
                                .repeat(),
                        );
                        injected = true;
                    }
                }
                assert!(injected);
                "I/O error"
            }
            "r3" => {
                fault
                    .flip_bit(&dir.0.join("kv").join("CURRENT"), 0, 1)
                    .unwrap();
                "storage corrupt"
            }
            _ => {
                damage_journal_value(&dir, &fault, needle(2).as_bytes());
                damage_journal_value(&dir, &fault, needle(3).as_bytes());
                "too many"
            }
        };
        for attempt in 0..3 {
            let error = reopen(&dir, &fault, damaged.clone(), resources.clone())
                .err()
                .unwrap_or_else(|| panic!("{case}: open {attempt} was not refused"));
            assert_eq!(error.code, ErrorCode::StorageUnavailable, "{case}");
            assert!(
                error.message.contains(expected),
                "{case}: {}",
                error.message
            );
            index += 1;
            serving
                .log_transaction(
                    budget(),
                    put_entry(index, b"healthy", index.to_be_bytes().to_vec()),
                )
                .await
                .unwrap();
            let read = serving
                .log_read(budget(), |view| {
                    view.get(b"healthy").and_then(|v| v.ok_or_else(capacity))
                })
                .await
                .unwrap();
            assert_eq!(read.as_slice(), index.to_be_bytes(), "{case}");
        }
        if case == "r2" {
            fault.clear_faults();
            let pool = reopen(&dir, &fault, damaged, resources.clone()).unwrap();
            assert_eq!(pool.kv_store().stats().tail_truncations, 0);
            assert!(pool.quarantined_groups().is_empty());
            for (n, id) in ids.iter().enumerate() {
                assert_eq!(entry(&pool, id).await, needle(n as u8).into_bytes());
            }
        }
    }
    // Twelve refused opens released their pool slots and charges.
    let extra_dir = Dir::new();
    let extra = fault_pool(
        &extra_dir,
        &FaultFs::new(),
        PoolIdentity {
            pool_id: nabedra_rt::ids::uuid_v4(),
            ..identity
        },
        resources.clone(),
    );
    assert_eq!(resources.storage_stats().pools, 2);
    drop((serving, healthy, extra));
}

/// LSM-5: a run-time quarantine whose kv manifest edit failed (ENOSPC on the
/// manifest, say) lives in kv's memory only; the pool's synced record of its
/// accepted groups, a journal write that needs no new file, still names the
/// group. This is the state a crash then leaves. The next open restores the
/// quarantine from that record before any group starts, persists it in kv, and
/// a later open still has it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ns23_lsm5_a_quarantine_kv_lost_is_restored_from_the_pool_record() {
    let dir = Dir::new();
    let fault = FaultFs::new();
    let identity = pool_identity();
    let resources = shared(SharedResourceConfig::default());
    let pool = fault_pool(&dir, &fault, identity.clone(), resources.clone());
    let a = pool.group(group_identity(&pool)).await.unwrap();
    a.log_transaction(budget(), put_entry(0, b"entry", b"lsm5".to_vec()))
        .await
        .unwrap();
    let a_id = a.identity().clone();
    drop(a);
    let mut batch = nabedra_kv::WriteBatch::new();
    batch.put(
        b"\0typed-pool-quarantine-v1",
        &serde_json::to_vec(&std::collections::BTreeSet::from([a_id.group_id])).unwrap(),
    );
    pool.kv_store()
        .submit(batch, nabedra_kv::Durability::Sync, Box::new(()))
        .unwrap()
        .done()
        .await
        .unwrap();
    assert!(!pool.is_group_quarantined(a_id.group_id));
    drop(pool);

    let fault = fault.kill();
    let pool = reopen(&dir, &fault, identity.clone(), resources.clone()).unwrap();
    assert!(
        pool.is_group_quarantined(a_id.group_id),
        "the group kv forgot must not start"
    );
    assert_eq!(pool.quarantined_groups(), vec![a_id.group_id]);
    drop(pool);
    let pool = reopen(&dir, &fault.crash(), identity, resources).unwrap();
    assert!(pool.is_group_quarantined(a_id.group_id), "kv persisted it");
}
