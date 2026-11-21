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
    let p50 = latencies.get(latencies.len() / 2).copied().unwrap_or_default();
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
