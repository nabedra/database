//! Phase 2 regressions of the 2026-11-26 design review (fx-oltp-direct):
//! OD-2 (no-ops recorded) and OD-7 (the record decides before validation).
use super::*;

async fn run_id(cluster: &Cluster, leader: usize, id: Uuid, body: &[u8]) -> DirectResponse {
    cluster.voters[leader]
        .executor
        .execute(&cluster.voters[leader].handle, cluster.request(leader, body, id))
        .await
}

/// OD-2, the vector session's DR-1 trace: a delete of a missing row answers
/// `false`, another client inserts the row, then the delete's identity runs
/// again (a stalled first attempt that lands, or a retry after a lost reply).
/// It must replay `false` and leave the inserted row in place.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d02_od2_a_delete_of_a_missing_row_keeps_its_answer() {
    let cluster = Cluster::new(Options::default()).await;
    let leader = cluster.leader().await;
    let id = nabedra_rt::ids::uuid_v4();
    let delete = cluster.body(id, "table.delete", serde_json::json!({"key": 1}));
    assert_eq!(run_id(&cluster, leader, id, &delete).await, done("false"));
    assert_eq!(
        cluster
            .op(
                leader,
                "table.insert",
                serde_json::json!({"row": {"id": 1, "value": 1, "tag": "a", "score": 0.0}}),
            )
            .await,
        done("null")
    );
    assert_eq!(
        run_id(&cluster, leader, id, &delete).await,
        done("false"),
        "the first definite answer of the identity"
    );
    assert!(
        cluster.latest(leader, cluster.row_key(1)).await.1.is_some(),
        "the row inserted after the answer is still there"
    );
    cluster.shutdown().await;
}

/// OD-2: the same for an update of a missing row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d02_od2_an_update_of_a_missing_row_keeps_its_answer() {
    let cluster = Cluster::new(Options::default()).await;
    let leader = cluster.leader().await;
    let id = nabedra_rt::ids::uuid_v4();
    let update = cluster.body(
        id,
        "table.update",
        serde_json::json!({"key": 1, "values": {"value": 9}}),
    );
    assert_eq!(run_id(&cluster, leader, id, &update).await, done("false"));
    assert_eq!(
        cluster
            .op(
                leader,
                "table.insert",
                serde_json::json!({"row": {"id": 1, "value": 1, "tag": "a", "score": 0.0}}),
            )
            .await,
        done("null")
    );
    let version = cluster.latest(leader, cluster.row_key(1)).await.0;
    assert_eq!(run_id(&cluster, leader, id, &update).await, done("false"));
    assert_eq!(
        cluster.latest(leader, cluster.row_key(1)).await.0,
        version,
        "the replayed update wrote nothing"
    );
    assert_eq!(
        cluster
            .op(leader, "table.get", serde_json::json!({"key": 1}))
            .await,
        done(r#"{"id":1,"score":0.0,"tag":"a","value":1}"#)
    );
    cluster.shutdown().await;
}

/// OD-2: an insert of a present row is a row conflict. The leader does not
/// answer it with an unrecorded 409; it hands the id to the gateway's anchor
/// settlement and the queued path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d02_od2_a_row_conflict_is_settled_at_the_anchor() {
    let cluster = Cluster::new(Options::default()).await;
    let leader = cluster.leader().await;
    let row = serde_json::json!({"row": {"id": 1, "value": 1, "tag": "a", "score": 0.0}});
    assert_eq!(cluster.op(leader, "table.insert", row.clone()).await, done("null"));
    let response = cluster.op(leader, "table.insert", row).await;
    assert_eq!(
        response,
        DirectResponse::Fallback {
            reason: FallbackReason::Ineligible
        }
    );
    cluster.shutdown().await;
}

/// OD-7: a retry of a committed request whose body no longer validates
/// against the current descriptor replays its result instead of answering a
/// validation error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d11_od7_a_retained_result_precedes_validation() {
    let cluster = Cluster::new(Options::default()).await;
    let leader = cluster.leader().await;
    let id = nabedra_rt::ids::uuid_v4();
    let insert = cluster.body(
        id,
        "table.insert",
        serde_json::json!({"row": {"id": 1, "value": 1, "tag": "a", "score": 0.0}}),
    );
    assert_eq!(run_id(&cluster, leader, id, &insert).await, done("null"));
    // The descriptor no longer has the `tag` column the body writes.
    let mut table = descriptor(cluster.table);
    table.schema.remove("tag");
    cluster.schema.insert(DirectTable {
        descriptor: table,
        schema_epoch: 1,
        lifecycle_id: cluster.table,
        indexes: vec![IndexDefinition {
            index_id: cluster.index,
            table_id: cluster.table,
            name: Text::try_from("by_value").unwrap(),
            columns: Page::new(vec![Text::try_from("value").unwrap()]).unwrap(),
            covering: Page::new(Vec::new()).unwrap(),
        }],
    });
    assert_eq!(
        run_id(&cluster, leader, id, &insert).await,
        done("null"),
        "the retained result decides first"
    );
    // A new id with the same body is refused by the new descriptor.
    let fresh = nabedra_rt::ids::uuid_v4();
    let other = cluster.body(
        fresh,
        "table.insert",
        serde_json::json!({"row": {"id": 2, "value": 2, "tag": "b", "score": 0.0}}),
    );
    assert_eq!(
        code(&run_id(&cluster, leader, fresh, &other).await),
        Some((ErrorCode::ValidationError, Outcome::NotApplied))
    );
    cluster.shutdown().await;
}

/// OD-2 cost: a delete of a missing row now writes like a delete, never
/// more. Each proposes one entry with one dedup record and expiry key; a
/// real delete also tombstones its index entry, a miss has none. Neither
/// tombstone is reclaimed later: Data groups have no row-version GC yet (no
/// code deletes `'v'` keys), so a miss grows storage exactly as a delete or
/// an update of the same key does. Prints the mean latency of both.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d02_od2_cost_a_missed_delete_writes_no_more_than_a_delete() {
    const N: i64 = 50;
    let cluster = Cluster::new(Options::default()).await;
    let leader = cluster.leader().await;
    for id in 1..=N {
        let row = serde_json::json!({"row": {"id": id, "value": id, "tag": "c", "score": 0.0}});
        assert_eq!(cluster.op(leader, "table.insert", row).await, done("null"));
    }
    async fn counts(cluster: &Cluster, leader: usize) -> [usize; 4] {
        [
            cluster.keys_with(leader, b'v').await,
            cluster.keys_with(leader, b'h').await,
            cluster.keys_with(leader, b'D').await,
            cluster.keys_with(leader, b'E').await,
        ]
    }
    let mut measured = Vec::new();
    for (label, first, expected) in [("delete", 1, "true"), ("miss", 1_001, "false")] {
        let before = counts(&cluster, leader).await;
        let entries = cluster.last_index(leader);
        let started = nabedra_rt::time::now_std();
        for id in first..first + N {
            let response = cluster
                .op(leader, "table.delete", serde_json::json!({"key": id}))
                .await;
            assert_eq!(response, done(expected), "{label} {id}");
        }
        let elapsed = nabedra_rt::time::elapsed(started);
        let after = counts(&cluster, leader).await;
        let delta: Vec<usize> = after.iter().zip(before).map(|(a, b)| a - b).collect();
        measured.push((label, delta, cluster.last_index(leader) - entries, elapsed));
    }
    for (label, delta, entries, elapsed) in &measured {
        eprintln!(
            "od2 cost {label}: {entries} entries, new keys v/h/D/E {delta:?}, mean {:.3} ms",
            elapsed.as_secs_f64() * 1e3 / N as f64
        );
    }
    let (_, delete, delete_entries, _) = &measured[0];
    let (_, miss, miss_entries, _) = &measured[1];
    let n = N as usize;
    assert_eq!(*delete_entries, *miss_entries, "one entry per request on both");
    assert_eq!(delete, &vec![2 * n, 0, n, n], "row and index tombstones, record, expiry");
    assert_eq!(miss, &vec![n, n, n, n], "one tombstone and a new head per absent key");
    cluster.shutdown().await;
}
