//! SNAP1: Raft snapshot export off the apply path. A snapshot is a cut (sync
//! plus point reads under the application order); its file is written from the
//! cut's pinned view in bounded native jobs, only when a follower needs it.
use super::*;
use std::time::Instant;

fn large() -> TransactionBudget {
    TransactionBudget {
        scratch_bytes: 1 << 20,
        batch_bytes: 6 << 20,
        output_bytes: 64 << 10,
    }
}

/// Writes `batches` × 1,024 rows of 4 KiB, applied at indexes `0..batches`.
async fn fill(group: &GroupStore, batches: u64) {
    for batch in 0..batches {
        group
            .transaction(meta(batch), large(), move |tx| {
                let value = [batch as u8; 4096];
                for row in 0..1024u64 {
                    tx.put(&(batch * 1024 + row).to_be_bytes(), &value)?;
                }
                Ok(Transition::Commit(tx.copy_output(b"null")?))
            })
            .await
            .unwrap();
    }
}

async fn probe(group: &GroupStore, index: u64) {
    group
        .transaction(meta(index), budget(), move |tx| {
            tx.put(b"probe", &index.to_be_bytes())?;
            Ok(Transition::Commit(tx.copy_output(b"null")?))
        })
        .await
        .unwrap();
}

/// SNAP1 / O11 (PERF1's soak stalls): applies of the exporting group keep
/// completing while its snapshot file is written, on a worker with one query
/// thread. Before SNAP1 the export held the application order and the query
/// thread through iterate, write, fsync and publish, so a concurrent apply
/// waited for the whole export (1.4-3.6 s per replica in BENCH2's soaks).
/// Now an apply waits at most for the cut or one bounded write job. The file
/// still matches its header exactly: installed elsewhere, it holds the probe
/// value of exactly the applied index it names.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snap1_o11_applies_proceed_while_a_snapshot_file_is_written() {
    let resources = shared(SharedResourceConfig::default());
    assert_eq!(SharedResourceConfig::default().query_threads, 1);
    let dir = Dir::new();
    let pool = OltpPool::open_development(&dir.0, pool_identity(), resources.clone()).unwrap();
    let identity = group_identity(&pool);
    let group = pool.group(identity.clone()).await.unwrap();
    const BATCHES: u64 = 12; // 48 MiB of rows
    fill(&group, BATCHES).await;

    let started = Instant::now();
    let export = tokio::spawn({
        let group = group.clone();
        async move {
            let snapshot = group.export_snapshot(budget()).await;
            (snapshot, started.elapsed())
        }
    });
    let mut latencies = Vec::new();
    let mut next = BATCHES;
    while !export.is_finished() {
        let begun = Instant::now();
        probe(&group, next).await;
        latencies.push(begun.elapsed());
        next += 1;
    }
    let (snapshot, took) = export.await.unwrap();
    let snapshot = snapshot.unwrap();
    let max = latencies.iter().max().copied().unwrap_or_default();
    latencies.sort();
    let p50 = latencies
        .get(latencies.len() / 2)
        .copied()
        .unwrap_or_default();
    eprintln!(
        "SNAP1 export {:?} for {} bytes; {} applies during it, apply p50 {:?} max {:?}",
        took,
        snapshot.bytes,
        latencies.len(),
        p50,
        max
    );
    assert!(snapshot.bytes > (BATCHES * 1024 * 4096));
    assert!(
        latencies.len() >= 4,
        "only {} applies completed during a {took:?} export",
        latencies.len()
    );
    assert!(
        max * 4 < took,
        "an apply waited {max:?} during a {took:?} export"
    );

    // The file matches its header: install it in another pool.
    let applied = snapshot.header.applied.unwrap().index;
    assert!((BATCHES - 1..next).contains(&applied), "{applied}");
    let target_dir = Dir::new();
    let mut target_id = pool_identity();
    target_id.cluster_id = pool.identity().cluster_id;
    target_id.worker_id = pool.identity().worker_id;
    let target = OltpPool::open_development(&target_dir.0, target_id, resources.clone()).unwrap();
    let copy = target.group(identity).await.unwrap();
    copy.install_snapshot(snapshot.clone(), large())
        .await
        .unwrap();
    assert_eq!(
        copy.applied_state(budget())
            .await
            .unwrap()
            .unwrap()
            .position
            .index,
        applied
    );
    let probed = copy
        .read(budget(), |view| view.get(b"probe"))
        .await
        .unwrap()
        .map(|value| u64::from_be_bytes(value.as_slice().try_into().unwrap()));
    assert_eq!(probed, (applied >= BATCHES).then_some(applied));
    assert_eq!(get(&copy, b"\0\0\0\0\0\0\0\0").await, [0u8; 4096]);
}

/// SNAP1 / O16: a cut pins its view only until its file exists, the file is a
/// transient outgoing name that goes with its last handle, and a header-only
/// cut (a policy build) writes nothing at all.
#[tokio::test]
async fn snap1_o16_a_cut_writes_its_file_once_and_releases_its_view() {
    let resources = shared(SharedResourceConfig::default());
    let dir = Dir::new();
    let pool = OltpPool::open_development(&dir.0, pool_identity(), resources.clone()).unwrap();
    let group = pool.group(group_identity(&pool)).await.unwrap();
    group
        .transaction(meta(0), budget(), |tx| {
            tx.put(b"key", b"cut")?;
            Ok(Transition::Commit(tx.copy_output(b"null")?))
        })
        .await
        .unwrap();
    let snapshots = dir
        .0
        .join("snapshots")
        .join(group.identity().group_id.to_string());

    let policy = group.cut_snapshot(budget(), false).await.unwrap();
    assert_eq!(policy.header.applied.unwrap().index, 0);
    assert!(!policy.pins_view());
    assert!(policy.file(budget()).await.is_err());
    assert!(!snapshots.exists(), "a policy cut wrote a file");

    let cut = group.cut_snapshot(budget(), true).await.unwrap();
    assert!(cut.pins_view());
    // Applied after the cut: not in its file.
    probe(&group, 1).await;
    let file = cut.file(budget()).await.unwrap();
    assert!(!cut.pins_view(), "the written cut still pins its view");
    assert_eq!(file.header, cut.header);
    assert_eq!(file.header.applied.unwrap().index, 0);
    assert!(file.path.starts_with(snapshots.join("outgoing")));
    assert!(file.path.exists());
    let again = cut.file(budget()).await.unwrap();
    assert_eq!(again.path, file.path, "a cut writes its file once");
    // No current snapshot is published by an export.
    assert!(group.current_snapshot(budget()).await.unwrap().is_none());
    let path = file.path.clone();
    drop((file, again));
    assert!(path.exists(), "the cut still owns its file");
    drop(cut);
    assert!(!path.exists(), "the last handle left its outgoing file");
    assert_eq!(resources.control_bytes_used(), 0);
}

/// SNAP1 / O11 / NS10: a crash between the cut and the end of a transfer
/// leaves at most an outgoing file; reopen removes it and the group's applied
/// state and current snapshot are untouched. Nothing is published, so there is
/// no publication to interleave with an install.
#[tokio::test]
async fn snap1_o11_ns10_crash_between_cut_and_transfer_leaves_nothing_on_reopen() {
    let resources = shared(SharedResourceConfig::default());
    let dir = Dir::new();
    let identity = pool_identity();
    let pool = OltpPool::open_development(&dir.0, identity.clone(), resources.clone()).unwrap();
    let group_id = group_identity(&pool);
    let group = pool.group(group_id.clone()).await.unwrap();
    group
        .transaction(meta(0), budget(), |tx| {
            tx.put(b"key", b"kept")?;
            Ok(Transition::Commit(tx.copy_output(b"null")?))
        })
        .await
        .unwrap();
    let snapshot = group.export_snapshot(budget()).await.unwrap();
    // The name a crash would leave behind (the process dies with the handle).
    let orphan = snapshot
        .path
        .with_file_name(format!("send-{}.bin", Uuid::new_v4()));
    fs::copy(&snapshot.path, &orphan).unwrap();
    drop(snapshot);
    drop(group);
    drop(pool);
    assert!(orphan.exists());

    let pool = OltpPool::open_development(&dir.0, identity.clone(), resources.clone()).unwrap();
    let group = pool.group(group_id.clone()).await.unwrap();
    assert!(!orphan.exists(), "reopen kept an outgoing file");
    assert_eq!(get(&group, b"key").await, b"kept");
    assert_eq!(
        group
            .applied_state(budget())
            .await
            .unwrap()
            .unwrap()
            .position
            .index,
        0
    );
    assert!(group.current_snapshot(budget()).await.unwrap().is_none());
    // A foreign name in the outgoing directory is refused, not deleted.
    let foreign = orphan.with_file_name("not-a-snapshot");
    fs::write(&foreign, b"x").unwrap();
    drop(group);
    drop(pool);
    let pool = OltpPool::open_development(&dir.0, identity, resources).unwrap();
    assert!(pool.group(group_id).await.is_err());
    assert!(foreign.exists());
}
