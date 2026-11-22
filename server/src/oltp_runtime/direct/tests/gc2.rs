//! GC2: `DirectBatch` on a real RF3 Data group over mTLS. Commits to one
//! range that arrive while its proposal is in flight share one entry while
//! `data.direct.batch` is enabled; with it off every commit is its own
//! entry, as before.
use super::*;

/// `count` concurrent inserts of fresh keys `first..`, each with its own
/// request id. Returns (request id, body, response) per request.
async fn concurrent_inserts(
    cluster: &Arc<Cluster>,
    leader: usize,
    first: i64,
    count: i64,
) -> Vec<(Uuid, Vec<u8>, DirectResponse)> {
    let mut tasks = Vec::new();
    for id in first..first + count {
        let cluster = cluster.clone();
        tasks.push(tokio::spawn(async move {
            let request_id = Uuid::new_v4();
            let body = cluster.body(
                request_id,
                "table.insert",
                serde_json::json!({"row": {"id": id, "value": id, "tag": "gc2", "score": 0.0}}),
            );
            let request = cluster.request(leader, &body, request_id);
            let response = cluster.voters[leader]
                .executor
                .execute(&cluster.voters[leader].handle, request)
                .await;
            (request_id, body, response)
        }));
    }
    let mut out = Vec::new();
    for task in tasks {
        out.push(task.await.unwrap());
    }
    out
}

/// Gate off (the feature not enabled): no `DirectBatch` is ever proposed,
/// however concurrent the load; each commit is one Data entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o02_d01_gc2_gate_off_proposes_one_entry_per_commit() {
    let cluster = Arc::new(Cluster::new(Options::default()).await);
    let leader = cluster.leader().await;
    let before = cluster.last_index(leader);
    let results = concurrent_inserts(&cluster, leader, 1, 48).await;
    for (_, _, response) in &results {
        assert_eq!(*response, done("null"));
    }
    let stats = cluster.voters[leader].executor.stats();
    assert_eq!(stats.batch_entries, 0, "no DirectBatch before enablement");
    assert_eq!(stats.batched_commits, 0);
    assert_eq!(
        cluster.last_index(leader) - before,
        48,
        "one Data entry per commit"
    );
    Arc::try_unwrap(cluster).ok().unwrap().shutdown().await;
}

/// Gate on: concurrent commits share entries, every member gets its own
/// result, every replica applies the same rows and one dedup record per
/// request, and every member retried returns its own original result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o03_d01_d03_gc2_concurrent_commits_share_entries_and_retries_replay_per_member() {
    let cluster = Arc::new(
        Cluster::new(Options {
            batch: true,
            ..Options::default()
        })
        .await,
    );
    let leader = cluster.leader().await;
    let dedup_before = cluster.keys_with(leader, b'D').await;
    let before = cluster.last_index(leader);
    let count = 96;
    let results = concurrent_inserts(&cluster, leader, 1, count).await;
    for (_, _, response) in &results {
        assert_eq!(*response, done("null"));
    }
    let entries = cluster.last_index(leader) - before;
    let stats = cluster.voters[leader].executor.stats();
    eprintln!(
        "gc2: {count} commits in {entries} entries, {} batches carrying {}",
        stats.batch_entries, stats.batched_commits
    );
    assert!(stats.batch_entries > 0, "{stats:?}");
    assert!(entries < count as u64, "commits shared entries");
    assert_eq!(
        entries,
        stats.batch_entries + (stats.entries_proposed - stats.batched_commits),
        "every entry is a batch or a lone commit"
    );
    // RD3: the members histogram counts every batch entry once.
    assert_eq!(
        stats.batch_members.values().sum::<u64>(),
        stats.batch_entries,
        "{stats:?}"
    );
    // Follower apply equals the leader's: every row and record.
    cluster.converge(leader).await;
    for voter in 0..3 {
        assert_eq!(
            cluster.keys_with(voter, b'D').await,
            dedup_before + count as usize,
            "one dedup record per request on voter {voter}"
        );
        for id in 1..=count {
            assert_eq!(
                cluster.latest(voter, cluster.row_key(id)).await,
                cluster.latest(leader, cluster.row_key(id)).await
            );
        }
    }
    let versions: Vec<_> = {
        let mut out = Vec::new();
        for id in 1..=count {
            out.push(cluster.latest(leader, cluster.row_key(id)).await);
        }
        out
    };
    // Every member retried, concurrently (so retries batch too): each gets
    // its own original result and nothing applies twice.
    let mut tasks = Vec::new();
    for (request_id, body, _) in results {
        let cluster = cluster.clone();
        tasks.push(tokio::spawn(async move {
            let request = cluster.request(leader, &body, request_id);
            cluster.voters[leader]
                .executor
                .execute(&cluster.voters[leader].handle, request)
                .await
        }));
    }
    for task in tasks {
        assert_eq!(task.await.unwrap(), done("null"));
    }
    for (index, id) in (1..=count).enumerate() {
        assert_eq!(
            cluster.latest(leader, cluster.row_key(id)).await,
            versions[index]
        );
    }
    assert_eq!(
        cluster.keys_with(leader, b'D').await,
        dedup_before + count as usize
    );
    Arc::try_unwrap(cluster).ok().unwrap().shutdown().await;
}

/// O08 with batching: same-key requests serialize on the leader's latches,
/// so they never share a batch (apply never refuses a shared key, and no
/// update is lost); distinct keys still batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o08_gc2_same_key_requests_never_share_a_batch() {
    let cluster = Arc::new(
        Cluster::new(Options {
            batch: true,
            ..Options::default()
        })
        .await,
    );
    let leader = cluster.leader().await;
    for id in [1, 2] {
        assert_eq!(
            cluster
                .op(
                    leader,
                    "table.insert",
                    serde_json::json!({"row": {"id": id, "value": 0, "tag": "t", "score": 0.0}}),
                )
                .await,
            done("null")
        );
    }
    let mut tasks = Vec::new();
    for task in 0..32i64 {
        let cluster = cluster.clone();
        tasks.push(tokio::spawn(async move {
            cluster
                .op(
                    leader,
                    "table.update",
                    serde_json::json!({"key": 1 + task % 2, "values": {"value": 100 + task}}),
                )
                .await
        }));
    }
    for task in tasks {
        assert_eq!(task.await.unwrap(), done("true"));
    }
    let stats = cluster.voters[leader].executor.stats();
    assert_eq!(
        stats.retries["conflict"], 0,
        "apply never refused a member: {stats:?}"
    );
    for id in [1, 2] {
        let (_, row) = cluster.latest(leader, cluster.row_key(id)).await;
        let row: serde_json::Value = serde_json::from_str(&row.unwrap()).unwrap();
        let last = row["value"].as_i64().unwrap();
        assert_eq!(
            (last - 100) % 2,
            id - 1,
            "row {id} got only its own updates"
        );
        let mut live = 0;
        for value in (0..1).chain(100..132) {
            if cluster
                .latest(leader, cluster.index_key(id, value))
                .await
                .1
                .is_some()
            {
                live += 1;
                assert_eq!(value, last);
            }
        }
        assert_eq!(live, 1, "one live index entry for row {id}");
    }
    cluster.converge(leader).await;
    for follower in (0..3).filter(|v| *v != leader) {
        for id in [1, 2] {
            assert_eq!(
                cluster.latest(follower, cluster.row_key(id)).await,
                cluster.latest(leader, cluster.row_key(id)).await
            );
        }
    }
    Arc::try_unwrap(cluster).ok().unwrap().shutdown().await;
}

/// A lone write is proposed at once as a one-entry commit, with the feature
/// on: serial writes never form a batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn o24_gc2_a_lone_commit_is_proposed_at_once_as_one_entry() {
    let cluster = Cluster::new(Options {
        batch: true,
        ..Options::default()
    })
    .await;
    let leader = cluster.leader().await;
    let before = cluster.last_index(leader);
    for id in 1..=20 {
        assert_eq!(
            cluster
                .op(
                    leader,
                    "table.insert",
                    serde_json::json!({"row": {"id": id, "value": id, "tag": "s", "score": 0.0}}),
                )
                .await,
            done("null")
        );
    }
    let stats = cluster.voters[leader].executor.stats();
    assert_eq!(stats.batch_entries, 0, "{stats:?}");
    assert_eq!(cluster.last_index(leader) - before, 20);
    cluster.shutdown().await;
}

/// A proposal that cannot reach a quorum fails every member it carries:
/// none succeeds, each reports not-applied or unknown, nothing applies, and
/// the range proposes again afterwards (the batcher never stays busy).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d05_gc2_a_failed_batch_proposal_reaches_every_member() {
    let mut cluster = Cluster::new(Options {
        batch: true,
        ..Options::default()
    })
    .await;
    let leader = cluster.leader().await;
    let followers: Vec<usize> = (0..3).filter(|v| *v != leader).collect();
    cluster.stop(followers[0]).await;
    cluster.stop(followers[1]).await;
    let cluster = Arc::new(cluster);
    let mut tasks = Vec::new();
    for id in 1..=24i64 {
        let cluster = cluster.clone();
        tasks.push(tokio::spawn(async move {
            let request_id = Uuid::new_v4();
            let body = cluster.body(
                request_id,
                "table.insert",
                serde_json::json!({"row": {"id": id, "value": id, "tag": "q", "score": 0.0}}),
            );
            let request =
                cluster.request_at(leader, &body, request_id, 1, Duration::from_millis(1500));
            cluster.voters[leader]
                .executor
                .execute(&cluster.voters[leader].handle, request)
                .await
        }));
    }
    for task in tasks {
        let response = task.await.unwrap();
        assert!(
            !matches!(response, DirectResponse::Done(_)),
            "no success without a quorum: {response:?}"
        );
        if let Some((_, outcome)) = code(&response) {
            assert!(matches!(outcome, Outcome::Unknown | Outcome::NotApplied));
        }
    }
    for id in 1..=24 {
        assert_eq!(
            cluster.latest(leader, cluster.row_key(id)).await,
            (None, None),
            "row {id} never applied"
        );
    }
    assert_eq!(
        cluster.clock(leader).pending_len(),
        0,
        "every member settled"
    );
    Arc::try_unwrap(cluster).ok().unwrap().shutdown().await;
}
