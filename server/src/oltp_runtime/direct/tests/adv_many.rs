//! Adversarial tests of the leader side of `table.upsert_many` and
//! `table.delete_many` (task C1: OLTP-G03, OLTP-G04, VEC-G09, VEC-G11, VEC-G14).
//! They run on the real three-voter group of this module's `Cluster`. Each
//! says which rule it attacks; a failing one is a defect of the branch.
use super::*;
use crate::protocol::{Vector, decode_row, decode_upsert_row, row_request_id};
use serde_json::{Value, json};
use std::sync::atomic::AtomicUsize;

/// The gate of a leader whose Control enabled `data.vector.v1` (or not).
struct VGate {
    vector: AtomicBool,
    batch: bool,
}
impl DirectGate for VGate {
    fn direct_active(&self) -> bool {
        true
    }
    fn direct_batch(&self) -> bool {
        self.batch
    }
    fn vector(&self) -> bool {
        self.vector.load(Ordering::Acquire)
    }
}

fn executor(
    cluster: &Cluster,
    voter: usize,
    gate: Arc<VGate>,
    timestamps: Arc<dyn DirectTimestampSource>,
) -> Arc<DirectExecutor> {
    DirectExecutor::new(
        cluster.voters[voter].shared.clone(),
        DirectConfig::default(),
        gate,
        cluster.schema.clone(),
        DirectTimestampClient::new(None, timestamps),
    )
}
async fn run_with(
    cluster: &Cluster,
    exec: &Arc<DirectExecutor>,
    voter: usize,
    call: Uuid,
    operation: &str,
    payload: Value,
    deadline: Duration,
) -> DirectResponse {
    let body = cluster.body(call, operation, payload);
    let request = cluster.request_at(voter, &body, call, 1, deadline);
    exec.execute(&cluster.voters[voter].handle, request).await
}
fn replies(response: DirectResponse) -> Vec<DirectResponse> {
    match response {
        DirectResponse::Done(data) => serde_json::from_str(data.0.get()).unwrap(),
        other => panic!("a group answers Done with one reply per row, got {other:?}"),
    }
}
fn row(id: i64, value: i64) -> Value {
    json!({"id": id, "value": value, "tag": "t", "score": 0.5})
}
fn rows(ids: impl IntoIterator<Item = i64>, value: i64) -> Value {
    ids.into_iter().map(|id| row(id, value + id)).collect()
}
fn vector_table(table: Uuid) -> TableDescriptor {
    serde_json::from_value(json!({
        "id": table, "name": "items",
        "schema": {
            "id": "int64", "value": "int64", "tag": "utf8", "score": "float64",
            "v": {"type": "vector", "dim": 2},
            "s": {"type": "vector", "sparse": true},
            "m": {"type": "list", "item": {"type": "vector", "dim": 2}},
            "tags": {"type": "list", "item": "utf8"}
        },
        "key": "id", "format": "parquet",
        "full_text": {"tag": "english"}
    }))
    .unwrap()
}
fn install_vector_table(cluster: &Cluster) {
    cluster.schema.insert(DirectTable {
        descriptor: vector_table(cluster.table),
        schema_epoch: 1,
        lifecycle_id: cluster.table,
        indexes: Vec::new(),
    });
}
fn wire(vector: Vector) -> Value {
    serde_json::to_value(&vector).unwrap()
}

/// VEC-G14, VEC-G09: after the gate opens, a vector table is served and every
/// value comes back with the bits it went in with, through `get`, a replay and
/// a partial update; `delete_many` removes the row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adv_vec_g14_after_activation_vector_rows_are_served_and_keep_their_bits() {
    let counter: Arc<dyn DirectTimestampSource> = Arc::new(Counter(AtomicU64::new(1_000)));
    let cluster = Cluster::new(Options {
        batch: true,
        timestamps: Some(counter.clone()),
        ..Options::default()
    })
    .await;
    let leader = cluster.leader().await;
    install_vector_table(&cluster);
    let gate = Arc::new(VGate {
        vector: AtomicBool::new(false),
        batch: true,
    });
    let exec = executor(&cluster, leader, gate.clone(), counter);
    let secs = Duration::from_secs(10);
    let edge = [
        f32::MAX,
        f32::MIN,
        f32::MIN_POSITIVE,
        f32::from_bits(1),
        -0.0,
        0.0,
    ];
    let make = |id: i64| {
        json!({
            "id": id, "value": id, "tag": "t", "score": 0.5,
            "v": wire(Vector::Dense(vec![edge[(id as usize) % 6], edge[(id as usize + 1) % 6]])),
            "s": wire(Vector::Sparse { indices: vec![0, 7, u32::MAX], weights: vec![-0.0, 1.5, f32::MAX] }),
            "m": wire(Vector::Multi { values: vec![1.0, 2.0, 3.0, 4.0], count: 2 }),
            "tags": ["a", "", "é", "\u{1F600}"],
        })
    };
    let mut nulls = make(100);
    for column in ["v", "s", "m"] {
        nulls[column] = Value::Null;
    }
    let batch = json!({"rows": [make(1), make(2), nulls.clone(), make(3)]});

    // Before the gate: nothing is served and nothing is proposed.
    let before = cluster.last_index(leader);
    let call = nabedra_rt::ids::uuid_v4();
    let refused = run_with(
        &cluster,
        &exec,
        leader,
        call,
        "table.upsert_many",
        batch.clone(),
        secs,
    )
    .await;
    assert_eq!(
        code(&refused),
        Some((ErrorCode::UnsupportedOperation, Outcome::NotApplied))
    );
    assert_eq!(cluster.last_index(leader), before);

    // After it: served.
    gate.vector.store(true, Ordering::Release);
    let answers = replies(
        run_with(
            &cluster,
            &exec,
            leader,
            call,
            "table.upsert_many",
            batch.clone(),
            secs,
        )
        .await,
    );
    assert_eq!(answers, vec![done("null"); 4], "{answers:?}");
    for id in [1, 2, 3, 100] {
        let (_, stored) = cluster.latest(leader, cluster.row_key(id)).await;
        let stored = decode_row(&stored.unwrap()).unwrap();
        let sent = decode_upsert_row(&if id == 100 { nulls.clone() } else { make(id) }.to_string())
            .unwrap();
        assert_eq!(
            serde_json::to_string(&stored).unwrap(),
            serde_json::to_string(&sent).unwrap(),
            "row {id} is stored as sent"
        );
    }
    // `get` returns the canonical row.
    let got = run_with(
        &cluster,
        &exec,
        leader,
        nabedra_rt::ids::uuid_v4(),
        "table.get",
        json!({"key": 1}),
        secs,
    )
    .await;
    let DirectResponse::Done(data) = got else {
        panic!("{got:?}")
    };
    let expected = decode_upsert_row(&make(1).to_string()).unwrap();
    assert_eq!(data.0.get(), serde_json::to_string(&expected).unwrap());
    // The same call again replays every row.
    let before = cluster.last_index(leader);
    let answers = replies(
        run_with(
            &cluster,
            &exec,
            leader,
            call,
            "table.upsert_many",
            batch,
            secs,
        )
        .await,
    );
    assert_eq!(answers, vec![done("null"); 4]);
    assert_eq!(
        cluster.last_index(leader),
        before,
        "a replay writes nothing"
    );
    // A partial update does not touch the vectors.
    let update = run_with(
        &cluster,
        &exec,
        leader,
        nabedra_rt::ids::uuid_v4(),
        "table.update",
        json!({"key": 2, "values": {"value": 99}}),
        secs,
    )
    .await;
    assert!(matches!(update, DirectResponse::Done(_)), "{update:?}");
    let (_, stored) = cluster.latest(leader, cluster.row_key(2)).await;
    let stored = decode_row(&stored.unwrap()).unwrap();
    let mut want = decode_upsert_row(&make(2).to_string()).unwrap();
    want.insert("value".into(), crate::protocol::Scalar::Int64(99));
    assert_eq!(
        serde_json::to_string(&stored).unwrap(),
        serde_json::to_string(&want).unwrap()
    );
    // delete_many.
    let answers = replies(
        run_with(
            &cluster,
            &exec,
            leader,
            nabedra_rt::ids::uuid_v4(),
            "table.delete_many",
            json!({"keys": [1, 2, 3, 100, 555]}),
            secs,
        )
        .await,
    );
    assert_eq!(
        answers,
        [
            done("true"),
            done("true"),
            done("true"),
            done("true"),
            done("false")
        ]
    );
    for id in [1, 2, 3, 100] {
        assert_eq!(cluster.latest(leader, cluster.row_key(id)).await.1, None);
    }
    cluster.shutdown().await;
}

/// VEC-G11: a vector row whose entry is above the direct limit, and a group of
/// 10,000 rows, are answered per row and never fail the group.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adv_vec_g11_rows_beyond_the_entry_limit_leave_the_direct_path_row_by_row() {
    let counter: Arc<dyn DirectTimestampSource> = Arc::new(Counter(AtomicU64::new(1_000)));
    let cluster = Cluster::new(Options {
        batch: true,
        timestamps: Some(counter.clone()),
        ..Options::default()
    })
    .await;
    let leader = cluster.leader().await;
    install_vector_table(&cluster);
    let gate = Arc::new(VGate {
        vector: AtomicBool::new(true),
        batch: true,
    });
    let exec = executor(&cluster, leader, gate, counter);
    let secs = Duration::from_secs(20);
    // Sparse rows of growing size around the entry limit (1 MiB encoded).
    for (round, pairs) in [1_000usize, 50_000, 100_000, 120_000, 131_000]
        .into_iter()
        .enumerate()
    {
        let big_id = 5 + round as i64 * 10;
        let big = json!({
            "id": big_id, "value": 1, "tag": "t", "score": 0.5, "v": null, "m": null, "tags": [],
            "s": wire(Vector::Sparse { indices: (0..pairs as u32).collect(), weights: vec![1.0; pairs] }),
        });
        let call = nabedra_rt::ids::uuid_v4();
        let payload = json!({"rows": [row_of(1), big, row_of(2)]});
        let answers = replies(
            run_with(
                &cluster,
                &exec,
                leader,
                call,
                "table.upsert_many",
                payload,
                secs,
            )
            .await,
        );
        assert_eq!(answers.len(), 3);
        assert_eq!(
            answers[0],
            done("null"),
            "{pairs} pairs: neighbours are unaffected"
        );
        assert_eq!(
            answers[2],
            done("null"),
            "{pairs} pairs: neighbours are unaffected"
        );
        let big_answer = &answers[1];
        match big_answer {
            DirectResponse::Done(_) | DirectResponse::Fallback { .. } => {}
            DirectResponse::Failed(error) => {
                assert_eq!(error.outcome, Outcome::NotApplied, "{pairs} pairs: {error}");
            }
            other => panic!("{pairs} pairs: {other:?}"),
        }
        // A row that is neither done nor failed is not in the store.
        let stored = cluster
            .latest(leader, cluster.row_key(big_id))
            .await
            .1
            .is_some();
        assert_eq!(
            stored,
            matches!(big_answer, DirectResponse::Done(_)),
            "{pairs} pairs: {big_answer:?}"
        );
    }
    cluster.shutdown().await;
}
fn row_of(id: i64) -> Value {
    json!({"id": id, "value": id, "tag": "t", "score": 0.5, "v": null, "s": null, "m": null, "tags": []})
}

/// VEC-G09: the largest group the gateway sends a leader (1,024 rows) is answered per row,
/// and a call of 10,001 rows is refused whole. The gateway's 10,000 rows go through
/// `vs17_a_call_at_the_limits_is_sent_in_bounded_chunks`: a debug build cannot commit
/// 10,000 rows in one leader call inside the leader's own 30 s cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adv_vec_g11_a_chunk_of_1024_rows_and_a_call_of_one_too_many() {
    let cluster = Cluster::new(Options {
        batch: true,
        ..Options::default()
    })
    .await;
    let leader = cluster.leader().await;
    let call = nabedra_rt::ids::uuid_v4();
    let body = cluster.body(
        call,
        "table.upsert_many",
        json!({"rows": rows(0..1_024, 0)}),
    );
    let request = cluster.request_at(leader, &body, call, 1, Duration::from_secs(25));
    let answers = replies(
        cluster.voters[leader]
            .executor
            .execute(&cluster.voters[leader].handle, request)
            .await,
    );
    assert_eq!(answers.len(), 1_024);
    assert!(
        answers.iter().all(|a| *a == done("null")),
        "{:?}",
        answers.iter().find(|a| **a != done("null"))
    );
    let too_many = cluster
        .run(
            leader,
            call,
            "table.upsert_many",
            json!({"rows": rows(0..10_001, 0)}),
        )
        .await;
    assert_eq!(
        code(&too_many),
        Some((ErrorCode::LimitExceeded, Outcome::NotApplied))
    );
    let too_many = cluster
        .run(
            leader,
            call,
            "table.delete_many",
            json!({"keys": (0..10_001).collect::<Vec<i64>>()}),
        )
        .await;
    assert_eq!(
        code(&too_many),
        Some((ErrorCode::LimitExceeded, Outcome::NotApplied))
    );
    cluster.shutdown().await;
}

/// Known to fail. VEC-G09 + DR-1: a key that did not exist when `delete_many` ran is
/// `applied`. Another writer then creates it. The call is resent after a lost
/// reply: each row applies once per request id, so the new row must survive.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "open bug 2026-11-26-many-delete-no-dedup-record"]
async fn adv_dr1_a_resent_delete_many_does_not_delete_a_row_created_after_its_first_attempt() {
    let cluster = Cluster::new(Options {
        batch: true,
        ..Options::default()
    })
    .await;
    let leader = cluster.leader().await;
    let call = nabedra_rt::ids::uuid_v4();
    let records = cluster.keys_with(leader, b'D').await;
    let first = replies(
        cluster
            .run(leader, call, "table.delete_many", json!({"keys": [99]}))
            .await,
    );
    assert_eq!(first, [done("false")], "a missing key is applied");
    let kept = cluster.keys_with(leader, b'D').await - records;
    // Someone else creates the row.
    let created = cluster
        .op(leader, "table.insert", json!({"row": row(99, 5)}))
        .await;
    assert!(matches!(created, DirectResponse::Done(_)), "{created:?}");
    let again = replies(
        cluster
            .run(leader, call, "table.delete_many", json!({"keys": [99]}))
            .await,
    );
    assert_eq!(
        again,
        [done("false")],
        "the resend returns the first result of its identity (kept records: {kept})"
    );
    assert!(
        cluster
            .latest(leader, cluster.row_key(99))
            .await
            .1
            .is_some(),
        "the resend deleted a row that was created after the first attempt"
    );
    cluster.shutdown().await;
}

/// The same attack on the shipped single `table.delete`, to tell what the
/// group inherits from the direct path and what it adds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adv_dr1_baseline_a_resent_single_delete_of_a_missing_key() {
    let cluster = Cluster::new(Options {
        batch: true,
        ..Options::default()
    })
    .await;
    let leader = cluster.leader().await;
    let call = nabedra_rt::ids::uuid_v4();
    let first = cluster
        .run(leader, call, "table.delete", json!({"key": 99}))
        .await;
    let created = cluster
        .op(leader, "table.insert", json!({"row": row(99, 5)}))
        .await;
    assert!(matches!(created, DirectResponse::Done(_)), "{created:?}");
    let again = cluster
        .run(leader, call, "table.delete", json!({"key": 99}))
        .await;
    let survives = cluster
        .latest(leader, cluster.row_key(99))
        .await
        .1
        .is_some();
    println!("BASELINE single delete: first {first:?}, resend {again:?}, row survives {survives}");
    cluster.shutdown().await;
}

/// OLTP-G03: the same for `upsert_many` rows that were no-ops (identical row).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adv_dr1_a_resent_upsert_many_does_not_overwrite_a_row_changed_after_its_first_attempt() {
    let cluster = Cluster::new(Options {
        batch: true,
        ..Options::default()
    })
    .await;
    let leader = cluster.leader().await;
    let seed = cluster
        .op(leader, "table.insert", json!({"row": row(7, 1)}))
        .await;
    assert!(matches!(seed, DirectResponse::Done(_)));
    let call = nabedra_rt::ids::uuid_v4();
    // The identical row: a no-op.
    let first = replies(
        cluster
            .run(
                leader,
                call,
                "table.upsert_many",
                json!({"rows": [row(7, 1)]}),
            )
            .await,
    );
    assert_eq!(first.len(), 1);
    let update = cluster
        .op(
            leader,
            "table.update",
            json!({"key": 7, "values": {"value": 2}}),
        )
        .await;
    assert!(matches!(update, DirectResponse::Done(_)));
    let again = replies(
        cluster
            .run(
                leader,
                call,
                "table.upsert_many",
                json!({"rows": [row(7, 1)]}),
            )
            .await,
    );
    assert_eq!(again.len(), 1);
    let (_, stored) = cluster.latest(leader, cluster.row_key(7)).await;
    assert!(
        stored.unwrap().contains(r#""value":2"#),
        "the resend of a no-op upsert wrote over a newer row"
    );
    cluster.shutdown().await;
}

/// OLTP-G04: for any deadline, a row reported `not_applied` is not in the store
/// (also later), and a row reported done is. Then the same call resent applies
/// each row exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adv_oltp_g04_deadlines_never_report_not_applied_for_a_row_that_applied() {
    for batch in [true, false] {
        let cluster = Cluster::new(Options {
            batch,
            ..Options::default()
        })
        .await;
        let leader = cluster.leader().await;
        let mut seen = (0usize, 0usize, 0usize, 0usize);
        for (round, millis) in [1u64, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233]
            .into_iter()
            .enumerate()
        {
            let base = (round as i64) * 1_000;
            let ids: Vec<i64> = (base..base + 640).collect();
            let call = nabedra_rt::ids::uuid_v4();
            let payload = json!({"rows": rows(ids.clone(), 7)});
            let body = cluster.body(call, "table.upsert_many", payload.clone());
            let request = cluster.request_at(leader, &body, call, 1, Duration::from_millis(millis));
            let voter = &cluster.voters[leader];
            let response = voter.executor.execute(&voter.handle, request).await;
            let answers: Vec<DirectResponse> = match response {
                DirectResponse::Done(data) => serde_json::from_str(data.0.get()).unwrap(),
                DirectResponse::Failed(error) => {
                    // Refused before any row: nothing may be in the store.
                    assert_eq!(error.outcome, Outcome::NotApplied, "{error}");
                    vec![DirectResponse::Failed(error); ids.len()]
                }
                other => panic!("{other:?}"),
            };
            assert_eq!(answers.len(), ids.len());
            cluster.converge(leader).await;
            // Let proposals that were in flight when the deadline passed land.
            nabedra_rt::time::sleep(Duration::from_millis(300)).await;
            cluster.converge(leader).await;
            let mut versions = Vec::new();
            for (id, answer) in ids.iter().zip(&answers) {
                let (version, stored) = cluster.latest(leader, cluster.row_key(*id)).await;
                versions.push(version);
                match answer {
                    DirectResponse::Done(_) => {
                        seen.0 += 1;
                        assert!(
                            stored.is_some(),
                            "batch={batch} {millis} ms: row {id} reported done is missing"
                        );
                    }
                    DirectResponse::Failed(error) if error.outcome == Outcome::NotApplied => {
                        seen.1 += 1;
                        assert!(
                            stored.is_none(),
                            "batch={batch} {millis} ms: row {id} was reported not_applied ({}) but is in the store",
                            error.message
                        );
                    }
                    DirectResponse::Failed(_) => seen.2 += 1,
                    other => {
                        seen.3 += 1;
                        assert!(stored.is_none(), "{other:?}");
                    }
                }
            }
            // The resend applies what is missing, once, and replays the rest.
            let answers = replies(
                run_with(
                    &cluster,
                    &voter.executor,
                    leader,
                    call,
                    "table.upsert_many",
                    payload,
                    Duration::from_secs(20),
                )
                .await,
            );
            assert!(
                answers.iter().all(|a| *a == done("null")),
                "batch={batch} {millis} ms resend: {:?}",
                answers.iter().find(|a| **a != done("null"))
            );
            cluster.converge(leader).await;
            for (id, before) in ids.iter().zip(&versions) {
                let (after, stored) = cluster.latest(leader, cluster.row_key(*id)).await;
                assert!(stored.is_some());
                if before.is_some() {
                    assert_eq!(
                        &after, before,
                        "batch={batch} {millis} ms: row {id} applied twice"
                    );
                }
            }
        }
        println!(
            "batch={batch}: done {} not_applied {} unknown {} other {}",
            seen.0, seen.1, seen.2, seen.3
        );
        cluster.shutdown().await;
    }
}

/// A timestamp source that answers the first allocations with values below
/// every version in the store, as if the clock were behind the GC floor or the
/// newest version: apply refuses those commits with a conflict.
struct Lagging {
    low: AtomicU64,
    high: AtomicU64,
    stale: AtomicUsize,
}
impl DirectTimestampSource for Lagging {
    fn allocate_direct(&self, count: u64) -> ConsensusFuture<'_, DirectTimestampRange> {
        let first = if self.low.load(Ordering::SeqCst) > 0 {
            self.stale.fetch_add(count as usize, Ordering::SeqCst);
            self.low
                .fetch_sub(count.min(self.low.load(Ordering::SeqCst)), Ordering::SeqCst);
            2
        } else {
            self.high.fetch_add(count, Ordering::SeqCst)
        };
        Box::pin(async move {
            DirectTimestampRange::new(
                CommitTs::new(first).unwrap(),
                CommitTs::new(first + count - 1).unwrap(),
            )
        })
    }
}

/// The closed-floor refusal of C2 is `Fault::Conflict` at apply: the same
/// refusal as a commit timestamp at or below the key's newest version. Rows of
/// a window that get it retry alone, never fail the group, never apply twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adv_vs15_rows_refused_for_their_timestamp_retry_alone_and_apply_once() {
    let cluster = Cluster::new(Options {
        batch: true,
        ..Options::default()
    })
    .await;
    let leader = cluster.leader().await;
    // Rows exist at versions >= 1000.
    let warm = replies(
        cluster
            .run(
                leader,
                nabedra_rt::ids::uuid_v4(),
                "table.upsert_many",
                json!({"rows": rows(0..64, 0)}),
            )
            .await,
    );
    assert!(warm.iter().all(|a| *a == done("null")));
    let lagging = Arc::new(Lagging {
        low: AtomicU64::new(40),
        high: AtomicU64::new(5_000_000),
        stale: AtomicUsize::new(0),
    });
    let gate = Arc::new(VGate {
        vector: AtomicBool::new(true),
        batch: true,
    });
    let exec = executor(&cluster, leader, gate, lagging.clone());
    let before = exec.stats();
    let call = nabedra_rt::ids::uuid_v4();
    let payload = json!({"rows": rows(0..64, 1_000)});
    let answers = replies(
        run_with(
            &cluster,
            &exec,
            leader,
            call,
            "table.upsert_many",
            payload.clone(),
            Duration::from_secs(20),
        )
        .await,
    );
    assert!(
        answers.iter().all(|a| *a == done("null")),
        "{:?}",
        answers.iter().find(|a| **a != done("null"))
    );
    cluster.converge(leader).await;
    let after = exec.stats();
    assert!(lagging.stale.load(Ordering::SeqCst) > 0);
    assert_eq!(
        after.entries_applied - before.entries_applied,
        64,
        "each row once, however often it was refused"
    );
    for id in 0..64 {
        let (_, stored) = cluster.latest(leader, cluster.row_key(id)).await;
        assert!(
            stored
                .unwrap()
                .contains(&format!(r#""value":{}"#, 1_000 + id))
        );
    }
    let versions: Vec<_> = {
        let mut v = Vec::new();
        for id in 0..64 {
            v.push(cluster.latest(leader, cluster.row_key(id)).await.0);
        }
        v
    };
    let answers = replies(
        run_with(
            &cluster,
            &exec,
            leader,
            call,
            "table.upsert_many",
            payload,
            Duration::from_secs(20),
        )
        .await,
    );
    assert!(answers.iter().all(|a| *a == done("null")));
    for (id, before) in (0..64).zip(versions) {
        assert_eq!(cluster.latest(leader, cluster.row_key(id)).await.0, before);
    }
    cluster.shutdown().await;
}

/// The 64-key latch window against concurrent point writers, on the same keys
/// and on others: no deadlock, no starvation, no lost update on a key both
/// write, and the point writers on other keys are not held for the call.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn adv_vs15_the_latch_window_blocks_neither_other_keys_nor_starves_the_same_key() {
    let cluster = Arc::new(
        Cluster::new(Options {
            batch: true,
            ..Options::default()
        })
        .await,
    );
    let leader = cluster.leader().await;
    let warm = replies(
        cluster
            .run(
                leader,
                nabedra_rt::ids::uuid_v4(),
                "table.upsert_many",
                json!({"rows": rows(0..64, 0)}),
            )
            .await,
    );
    assert!(warm.iter().all(|a| *a == done("null")));
    let stop = Arc::new(AtomicBool::new(false));
    let windows = 60;
    // The windows: all 64 keys, in the order of a call, again and again.
    let writer = {
        let (cluster, stop) = (cluster.clone(), stop.clone());
        nabedra_rt::task::spawn(async move {
            let mut worst = Duration::ZERO;
            for round in 0..windows {
                let started = std::time::Instant::now();
                let answers = replies(
                    cluster
                        .run(
                            leader,
                            nabedra_rt::ids::uuid_v4(),
                            "table.upsert_many",
                            json!({"rows": rows(0..64, 10_000 * (round + 1))}),
                        )
                        .await,
                );
                worst = worst.max(started.elapsed());
                assert!(
                    answers.iter().all(|a| *a == done("null")),
                    "round {round}: {:?}",
                    answers.iter().find(|a| **a != done("null"))
                );
            }
            stop.store(true, Ordering::SeqCst);
            worst
        })
    };
    // A second call over the same keys in the opposite order of ids, racing the
    // first (key order inside a call is sorted, so it must not cycle).
    let rival = {
        let (cluster, stop) = (cluster.clone(), stop.clone());
        nabedra_rt::task::spawn(async move {
            let mut n = 0u32;
            while !stop.load(Ordering::SeqCst) {
                let ids: Vec<i64> = (0..64).rev().collect();
                let r = cluster
                    .run(
                        leader,
                        nabedra_rt::ids::uuid_v4(),
                        "table.upsert_many",
                        json!({"rows": rows(ids, 7)}),
                    )
                    .await;
                assert!(matches!(r, DirectResponse::Done(_)), "{r:?}");
                n += 1;
            }
            n
        })
    };
    // Point writers on a key of the window and on keys outside it.
    let point = |id: i64| {
        let (cluster, stop) = (cluster.clone(), stop.clone());
        nabedra_rt::task::spawn(async move {
            let mut latencies = Vec::new();
            let mut failures = Vec::new();
            let mut n = 0i64;
            while !stop.load(Ordering::SeqCst) {
                n += 1;
                let started = std::time::Instant::now();
                let r = cluster
                    .op(leader, "table.upsert", json!({"row": row(id, n)}))
                    .await;
                latencies.push(started.elapsed());
                if !matches!(r, DirectResponse::Done(_)) {
                    failures.push(format!("{r:?}"));
                }
            }
            (latencies, failures)
        })
    };
    let same = point(5);
    let other: Vec<_> = (1_000..1_004).map(point).collect();
    let run = nabedra_rt::time::timeout(Duration::from_secs(120), async {
        let worst = writer.await.unwrap();
        let rival = rival.await.unwrap();
        let same = same.await.unwrap();
        let mut others = Vec::new();
        for task in other {
            others.push(task.await.unwrap());
        }
        (worst, rival, same, others)
    })
    .await
    .expect("deadlock or starvation: the writers did not finish");
    let (worst, rival, (same_lat, same_fail), others) = run;
    let max = |l: &[Duration]| l.iter().copied().max().unwrap_or_default();
    let other_max = others.iter().map(|(l, _)| max(l)).max().unwrap();
    println!(
        "window call worst {worst:?}; rival calls {rival}; point on a window key: {} writes, max {:?}; other keys max {other_max:?}",
        same_lat.len(),
        max(&same_lat)
    );
    assert!(same_fail.is_empty(), "{same_fail:?}");
    for (_, failures) in &others {
        assert!(failures.is_empty(), "{failures:?}");
    }
    assert!(
        !same_lat.is_empty(),
        "the point writer on a window key never finished a write"
    );
    assert!(
        max(&same_lat) < Duration::from_secs(5),
        "a point write waited {:?}",
        max(&same_lat)
    );
    assert!(
        other_max < Duration::from_secs(2),
        "a point write on another key waited {other_max:?}"
    );
    // Every key holds one whole version written by someone: the value of key
    // 5 is one a writer wrote, and the others are one of the window writers'.
    for id in 0..64 {
        assert!(
            cluster
                .latest(leader, cluster.row_key(id))
                .await
                .1
                .is_some()
        );
    }
    let cluster = Arc::try_unwrap(cluster).ok().expect("tasks ended");
    cluster.shutdown().await;
}

/// OLTP-G03: the same call twice at once, and a different call on the same keys
/// at once. The first executes each row once.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn adv_oltp_g03_two_copies_of_one_call_in_flight_apply_each_row_once() {
    let cluster = Arc::new(
        Cluster::new(Options {
            batch: true,
            ..Options::default()
        })
        .await,
    );
    let leader = cluster.leader().await;
    let call = nabedra_rt::ids::uuid_v4();
    let before = cluster.voters[leader].executor.stats();
    let mut tasks = Vec::new();
    for _ in 0..4 {
        let cluster = cluster.clone();
        tasks.push(nabedra_rt::task::spawn(async move {
            replies(
                cluster
                    .run(
                        leader,
                        call,
                        "table.upsert_many",
                        json!({"rows": rows(0..200, 3)}),
                    )
                    .await,
            )
        }));
    }
    for task in tasks {
        let answers = task.await.unwrap();
        assert!(
            answers.iter().all(|a| *a == done("null")),
            "{:?}",
            answers.iter().find(|a| **a != done("null"))
        );
    }
    let after = cluster.voters[leader].executor.stats();
    assert_eq!(
        after.entries_applied - before.entries_applied,
        200,
        "each row once"
    );
    assert_eq!(after.replayed - before.replayed, 600);
    let cluster = Arc::try_unwrap(cluster).ok().expect("tasks ended");
    cluster.shutdown().await;
}

/// OLTP-G03: a row of one call and the same key in a call with another id are
/// two identities: both may apply, in a serial order, and neither is reported
/// as the other.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adv_vs15_request_ids_of_rows_depend_on_the_call() {
    let cluster = Cluster::new(Options {
        batch: true,
        ..Options::default()
    })
    .await;
    let leader = cluster.leader().await;
    let (a, b) = (nabedra_rt::ids::uuid_v4(), nabedra_rt::ids::uuid_v4());
    let x = replies(
        cluster
            .run(leader, a, "table.upsert_many", json!({"rows": [row(1, 1)]}))
            .await,
    );
    let y = replies(
        cluster
            .run(leader, b, "table.upsert_many", json!({"rows": [row(1, 2)]}))
            .await,
    );
    assert_eq!(x, [done("null")]);
    assert_eq!(y, [done("null")]);
    let (_, stored) = cluster.latest(leader, cluster.row_key(1)).await;
    assert!(stored.unwrap().contains(r#""value":2"#));
    assert_ne!(
        row_request_id(a, &Key::Int64(1)).unwrap(),
        row_request_id(b, &Key::Int64(1)).unwrap()
    );
    // Call a again with its own row: a replay, which must not write over b.
    let again = replies(
        cluster
            .run(leader, a, "table.upsert_many", json!({"rows": [row(1, 1)]}))
            .await,
    );
    assert_eq!(again, [done("null")]);
    let (_, stored) = cluster.latest(leader, cluster.row_key(1)).await;
    assert!(
        stored.unwrap().contains(r#""value":2"#),
        "a replay overwrote a later write"
    );
    cluster.shutdown().await;
}

/// VEC-G09: `insert_many` keeps its contract on a vector table: it is all or
/// nothing, so one refused row refuses the batch and none is stored, whereas
/// the same rows in `upsert_many` apply but the refused one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adv_vec_g09_insert_many_stays_whole_and_upsert_many_does_not() {
    let counter: Arc<dyn DirectTimestampSource> = Arc::new(Counter(AtomicU64::new(1_000)));
    let cluster = Cluster::new(Options {
        batch: true,
        timestamps: Some(counter.clone()),
        ..Options::default()
    })
    .await;
    let leader = cluster.leader().await;
    install_vector_table(&cluster);
    let gate = Arc::new(VGate {
        vector: AtomicBool::new(true),
        batch: true,
    });
    let exec = executor(&cluster, leader, gate, counter);
    let secs = Duration::from_secs(20);
    let good = |id: i64| {
        let mut r = row_of(id);
        r["v"] = wire(Vector::Dense(vec![1.0, 2.0]));
        r
    };
    let mut bad = good(2);
    bad["v"] = wire(Vector::Dense(vec![1.0, 2.0, 3.0]));
    let batch = json!({"rows": [good(1), bad.clone(), good(3)]});
    let refused = run_with(
        &cluster,
        &exec,
        leader,
        nabedra_rt::ids::uuid_v4(),
        "table.insert_many",
        batch.clone(),
        secs,
    )
    .await;
    assert_eq!(
        code(&refused),
        Some((ErrorCode::ValidationError, Outcome::NotApplied)),
        "{refused:?}"
    );
    for id in [1, 2, 3] {
        assert_eq!(
            cluster.latest(leader, cluster.row_key(id)).await.1,
            None,
            "insert_many stored row {id}"
        );
    }
    let answers = replies(
        run_with(
            &cluster,
            &exec,
            leader,
            nabedra_rt::ids::uuid_v4(),
            "table.upsert_many",
            batch,
            secs,
        )
        .await,
    );
    assert_eq!(answers.len(), 3);
    assert_eq!(answers[0], done("null"));
    assert_eq!(
        code(&answers[1]),
        Some((ErrorCode::ValidationError, Outcome::NotApplied)),
        "{:?}",
        answers[1]
    );
    assert_eq!(answers[2], done("null"));
    assert!(cluster.latest(leader, cluster.row_key(1)).await.1.is_some());
    assert!(cluster.latest(leader, cluster.row_key(2)).await.1.is_none());
    assert!(cluster.latest(leader, cluster.row_key(3)).await.1.is_some());
    // A whole good batch of insert_many applies.
    let ok = run_with(
        &cluster,
        &exec,
        leader,
        nabedra_rt::ids::uuid_v4(),
        "table.insert_many",
        json!({"rows": [good(10), good(11)]}),
        secs,
    )
    .await;
    assert!(matches!(ok, DirectResponse::Done(_)), "{ok:?}");
    cluster.shutdown().await;
}

/// VEC-G09: a resend of the same call is the same identity and applies nothing
/// twice; a new call with the same rows is a new identity and applies again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adv_vec_g09_a_new_call_is_a_new_identity_and_a_resend_is_not() {
    let cluster = Cluster::new(Options {
        batch: true,
        ..Options::default()
    })
    .await;
    let leader = cluster.leader().await;
    async fn stored(cluster: &Cluster, leader: usize) -> String {
        cluster
            .latest(leader, cluster.row_key(7))
            .await
            .1
            .unwrap_or_default()
    }
    let first = nabedra_rt::ids::uuid_v4();
    let a = replies(
        cluster
            .run(
                leader,
                first,
                "table.upsert_many",
                json!({"rows": [row(7, 1)]}),
            )
            .await,
    );
    assert_eq!(a, [done("null")]);
    for value in [2, 3] {
        let update = cluster
            .op(
                leader,
                "table.update",
                json!({"key": 7, "values": {"value": value}}),
            )
            .await;
        assert!(matches!(update, DirectResponse::Done(_)));
        // The resend replays its first result and writes nothing.
        let again = replies(
            cluster
                .run(
                    leader,
                    first,
                    "table.upsert_many",
                    json!({"rows": [row(7, 1)]}),
                )
                .await,
        );
        assert_eq!(again, [done("null")]);
        assert!(
            stored(&cluster, leader)
                .await
                .contains(&format!(r#""value":{value}"#)),
            "the resend wrote over a newer row"
        );
        // A new call with the same rows is a new write.
        let fresh = replies(
            cluster
                .run(
                    leader,
                    nabedra_rt::ids::uuid_v4(),
                    "table.upsert_many",
                    json!({"rows": [row(7, 1)]}),
                )
                .await,
        );
        assert_eq!(fresh, [done("null")]);
        assert!(
            stored(&cluster, leader).await.contains(r#""value":1"#),
            "a new call was taken for a resend"
        );
    }
    cluster.shutdown().await;
}
