//! Adversarial tests of `table.upsert_many` / `table.delete_many` at the
//! gateway (task C1: OLTP-G03, OLTP-G04, VEC-G09, VEC-G11, VEC-G14), on the
//! real three-voter groups of this module's `Cluster`.
use super::*;
use crate::oltp_replication::authority::timestamp::DirectTimestampClient;
use crate::oltp_runtime::direct::{DirectConfig, DirectExecutor, DirectGate};
use crate::protocol::{RowOutcome, RowOutcomes, Vector};
use std::sync::atomic::AtomicUsize;

/// Every answer of a call, or the call's own refusal.
async fn call(
    gateway: &DirectGateway,
    body: &[u8],
    queue: Option<&dyn QueuedRows>,
) -> Result<Vec<RowOutcome>, (ErrorCode, Outcome)> {
    let request: Value = serde_json::from_slice(body).unwrap();
    match gateway.route_with(body, false, queue).await {
        Ok(Routed::Done(encoded)) => {
            assert_eq!(encoded.status, 200);
            let mut envelope: Value = serde_json::from_slice(&encoded.body).unwrap();
            assert_eq!(envelope["request_id"], request["request_id"]);
            let outcomes: RowOutcomes = serde_json::from_value(envelope["data"].take()).unwrap();
            let payload = &request["payload"];
            let rows = payload.get("rows").or(payload.get("keys")).unwrap();
            assert_eq!(outcomes.outcomes.len(), rows.as_array().unwrap().len());
            Ok(outcomes.outcomes)
        }
        Ok(Routed::Queued) => panic!("a many call is never queued whole"),
        Err(error) => Err((error.code, error.outcome)),
    }
}
fn row(id: i64, value: i64) -> Value {
    json!({"id": id, "value": value})
}
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// OLTP-G04, VEC-G09: whatever the transport does (lost replies, a leader that
/// is down or refuses, a deadline that passes mid-call), a row reported
/// `not_applied` never executed, a row reported `applied` did, and resending
/// the call ends with every row applied once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adv_oltp_g04_random_faults_never_misreport_a_row() {
    let mut tallies = (0usize, 0usize, 0usize);
    for seed in 1..=14u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
        let mut cluster = Cluster::batching().await;
        let left = cluster.add_group(span(None, Some(250)), 1, &[1]).await;
        let right = cluster.add_group(span(Some(250), None), 1, &[1]).await;
        let source = Source::new(vec![cluster.fill(&[left, right])], cluster.voters());
        let transport = cluster.transport();
        let millis = 150 + rng.below(1_200) as u64;
        let gateway = cluster
            .gateway(direct_on(), source, transport.clone())
            .with_deadline(Duration::from_millis(millis));
        // Warm both ranges so the faults hit steady state.
        let warm = cluster.upsert_many(nabedra_rt::ids::uuid_v4(), rows([1_000_000, 249, 250], 0));
        let _ = call(&gateway, &warm, None).await;

        let count = 64 + rng.below(300);
        let mut ids: Vec<i64> = Vec::new();
        while ids.len() < count {
            let id = rng.below(500) as i64;
            if !ids.contains(&id) {
                ids.push(id);
            }
        }
        let delete = rng.below(4) == 0;
        let id = nabedra_rt::ids::uuid_v4();
        if delete {
            // A delete of a missing key leaves no record (see the open bug
            // 2026-11-26-many-delete-no-dedup-record): delete rows that exist.
            let fill = cluster.upsert_many(nabedra_rt::ids::uuid_v4(), rows(ids.clone(), 1));
            let patient = cluster
                .gateway(
                    direct_on(),
                    Source::new(vec![cluster.fill(&[left, right])], cluster.voters()),
                    transport.clone(),
                )
                .with_deadline(Duration::from_secs(30));
            assert!(applied(
                &call(&patient, &fill, None).await.unwrap_or_default()
            ));
        }
        let body = if delete {
            cluster.delete_many(id, json!(ids))
        } else {
            cluster.upsert_many(id, rows(ids.clone(), 100))
        };
        // The fault plan.
        match rng.below(5) {
            0 => transport.lose.store(1 + rng.below(3), Ordering::SeqCst),
            1 => {
                transport.lose.store(1, Ordering::SeqCst);
                let t = Arc::downgrade(&transport);
                *transport.after_lose.lock().unwrap() = Some(Box::new(move || {
                    t.upgrade().unwrap().deny.store(true, Ordering::SeqCst);
                }));
            }
            2 => {
                transport.lose.store(1, Ordering::SeqCst);
                let t = Arc::downgrade(&transport);
                *transport.after_lose.lock().unwrap() = Some(Box::new(move || {
                    t.upgrade().unwrap().down.lock().unwrap().extend([1, 2, 3]);
                }));
            }
            3 => transport.deny.store(true, Ordering::SeqCst),
            _ => {}
        }
        let outcomes = call(&gateway, &body, None).await;
        // Let proposals that were in flight land.
        nabedra_rt::time::sleep(Duration::from_millis(400)).await;
        let outcomes = match outcomes {
            Ok(outcomes) => outcomes,
            Err((code, outcome)) => {
                assert_eq!(
                    outcome,
                    Outcome::NotApplied,
                    "seed {seed}: a refused call {code:?}"
                );
                vec![
                    RowOutcome::NotApplied {
                        error: ProtocolError::new(code, "call", Outcome::NotApplied)
                    };
                    ids.len()
                ]
            }
        };
        let mut settled = Vec::new();
        for (id_row, outcome) in ids.iter().zip(&outcomes) {
            let group = if *id_row < 250 { left } else { right };
            let present = cluster.row_settled(group, id, *id_row).await;
            settled.push(present);
            match outcome {
                RowOutcome::Applied => {
                    tallies.0 += 1;
                    assert!(
                        present,
                        "seed {seed}: row {id_row} reported applied has no record"
                    );
                }
                RowOutcome::NotApplied { error } => {
                    tallies.1 += 1;
                    assert!(
                        !present,
                        "seed {seed} ({millis} ms): row {id_row} reported not_applied ({}) but executed",
                        error.message
                    );
                }
                RowOutcome::OutcomeUnknown { .. } => tallies.2 += 1,
            }
        }
        // Faults over: the same call applies every row, once.
        transport.deny.store(false, Ordering::SeqCst);
        transport.down.lock().unwrap().clear();
        transport.lose.store(0, Ordering::SeqCst);
        *transport.after_lose.lock().unwrap() = None;
        let mut before = Vec::new();
        for id_row in &ids {
            let group = if *id_row < 250 { left } else { right };
            before.push(cluster.latest(group, 0, *id_row).await.0);
        }
        let gateway = cluster
            .gateway(
                direct_on(),
                Source::new(vec![cluster.fill(&[left, right])], cluster.voters()),
                transport.clone(),
            )
            .with_deadline(Duration::from_secs(20));
        let outcomes = call(&gateway, &body, None).await.unwrap();
        assert!(
            applied(&outcomes),
            "seed {seed}: {:?}",
            outcomes.iter().find(|o| **o != RowOutcome::Applied)
        );
        for ((id_row, before), was) in ids.iter().zip(before).zip(settled) {
            let group = if *id_row < 250 { left } else { right };
            let after = cluster.latest(group, 0, *id_row).await.0;
            if was {
                assert_eq!(after, before, "seed {seed}: row {id_row} applied twice");
            }
        }
        cluster.shutdown().await;
    }
    println!(
        "applied {} not_applied {} unknown {}",
        tallies.0, tallies.1, tallies.2
    );
}

/// VEC-G11 end to end: a vector row above the direct entry limit leaves the
/// direct path alone, its neighbours stay direct, and its queued request is the
/// canonical one the marker settled.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adv_vec_g11_a_vector_row_above_the_entry_limit_is_settled_and_queued_alone() {
    let mut cluster = Cluster::batching().await;
    let group = cluster.add_group(span(None, None), 1, &[1]).await;
    // A leader whose Control enabled the feature.
    struct On;
    impl DirectGate for On {
        fn direct_active(&self) -> bool {
            true
        }
        fn direct_batch(&self) -> bool {
            true
        }
        fn vector(&self) -> bool {
            true
        }
    }
    let table: TableDescriptor = serde_json::from_value(json!({
        "id": cluster.table, "name": "items",
        "schema": {"id": "int64", "value": "int64",
                   "s": {"type": "vector", "sparse": true}},
        "key": "id", "format": "parquet"
    }))
    .unwrap();
    cluster.schema.insert(DirectTable {
        descriptor: table,
        schema_epoch: 1,
        indexes: Vec::new(),
        lifecycle_id: cluster.table,
    });
    for node in &mut cluster.nodes {
        node.executor = DirectExecutor::new(
            node.shared.clone(),
            DirectConfig::default(),
            Arc::new(On),
            cluster.schema.clone(),
            DirectTimestampClient::new(None, cluster.timestamps.clone()),
        );
    }
    let source = Source::new(vec![cluster.fill(&[group])], cluster.voters());
    let transport = cluster.transport();
    let gateway = cluster.gateway(direct_on(), source, transport);
    let sparse = |pairs: usize| {
        serde_json::to_value(Vector::Sparse {
            indices: (0..pairs as u32).collect(),
            weights: vec![1.0; pairs],
        })
        .unwrap()
    };
    for pairs in [10usize, 100_000, 120_000, 131_000] {
        let call_id = nabedra_rt::ids::uuid_v4();
        let big_id = pairs as i64;
        let body = cluster.upsert_many(
            call_id,
            json!([
                {"id": 1, "value": 1, "s": sparse(3)},
                {"id": big_id, "value": 2, "s": sparse(pairs)},
                {"id": 3, "value": 3, "s": null},
            ]),
        );
        let queue = Queue::new(None);
        let outcomes = call(&gateway, &body, Some(&queue)).await.unwrap();
        assert!(applied(&outcomes), "{pairs} pairs: {outcomes:?}");
        let bodies = queue.bodies();
        let direct = cluster.latest(group, 0, big_id).await.1.is_some();
        assert_eq!(
            direct,
            bodies.is_empty(),
            "{pairs} pairs: the row is either in the store or queued, never both or neither"
        );
        if !bodies.is_empty() {
            assert_eq!(bodies.len(), 1, "{pairs} pairs");
            assert!(
                cluster.row_settled(group, call_id, big_id).await,
                "settled before queueing"
            );
        }
        assert!(cluster.latest(group, 0, 1).await.1.is_some());
        assert!(cluster.latest(group, 0, 3).await.1.is_some());
    }
    cluster.shutdown().await;
}

/// A queued path that refuses before it accepts anything.
struct RefusingQueue(ProtocolError);
impl QueuedRows for RefusingQueue {
    fn run(&self, _body: Vec<u8>) -> EndpointFuture<'_, EncodedResponse> {
        let error = self.0.clone();
        Box::pin(async move { Err(error) })
    }
}

/// OLTP-G04: a row that cannot run directly is queued after its id is settled.
/// If the call is resent, an earlier attempt may have queued and executed it
/// (the reply was lost). A queued path that then refuses admission says
/// nothing about that earlier attempt, so the row is `outcome_unknown`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adv_oltp_g04_a_queue_that_refuses_a_resent_row_does_not_prove_it_was_not_applied() {
    let mut cluster = Cluster::batching().await;
    let group = cluster.add_group(span(None, None), 1, &[1]).await;
    let mut wide = descriptor(cluster.table);
    wide.schema.insert("value".to_string(), ColumnType::Utf8);
    cluster.schema.insert(DirectTable {
        descriptor: wide,
        schema_epoch: 1,
        indexes: Vec::new(),
        lifecycle_id: cluster.table,
    });
    let source = Source::new(vec![cluster.fill(&[group])], cluster.voters());
    let gateway = cluster.gateway(direct_on(), source, cluster.transport());
    let id = nabedra_rt::ids::uuid_v4();
    let big = "x".repeat((1 << 20) - 22);
    let body = cluster.upsert_many(id, json!([{"id": 2, "value": big}]));
    // First attempt: the row is settled and queued; the queue accepts it.
    let queue = Queue::new(None);
    assert!(applied(&call(&gateway, &body, Some(&queue)).await.unwrap()));
    assert_eq!(queue.bodies().len(), 1);
    // The resend: the leader finds the marker, the row goes to the queue
    // again, and this time the queue is full.
    for error in [
        ProtocolError::new(ErrorCode::Overloaded, "full", Outcome::NotApplied),
        ProtocolError::new(
            ErrorCode::Overloaded,
            "request parsing memory capacity exhausted",
            Outcome::NotApplied,
        ),
        ProtocolError::new(
            ErrorCode::StorageUnavailable,
            "no ingress slot",
            Outcome::NotApplied,
        ),
    ] {
        let outcomes = call(&gateway, &body, Some(&RefusingQueue(error.clone())))
            .await
            .unwrap();
        assert!(
            matches!(outcomes[0], RowOutcome::OutcomeUnknown { .. }),
            "{error}: an earlier attempt may have run the row, yet the call reports {:?}",
            outcomes[0]
        );
    }
    cluster.shutdown().await;
}

/// OLTP-G03: after a split in the middle of the call, rows that were already
/// applied on the parent are not applied again on a child when the call is
/// resent in other chunks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adv_vs15_a_resend_after_a_split_in_other_chunks_applies_nothing_twice() {
    let mut cluster = Cluster::batching().await;
    let left = cluster.add_group(span(None, Some(500)), 2, &[1]).await;
    let right = cluster.add_group(span(Some(500), None), 2, &[1]).await;
    let mut before = cluster.fill(&[left]);
    before.shards[0].span = span(None, None);
    before.shards[0].routing_epoch = 1;
    let after = cluster.fill(&[left, right]);
    let source = Source::new(vec![before, after.clone()], cluster.voters());
    let gateway = cluster.gateway(direct_on(), source, cluster.transport());
    let id = nabedra_rt::ids::uuid_v4();
    let all: Vec<i64> = (0..1_000).step_by(5).collect();
    let outcomes = call(
        &gateway,
        &cluster.upsert_many(id, rows(all.clone(), 0)),
        None,
    )
    .await
    .unwrap();
    assert!(applied(&outcomes));
    let mut versions = Vec::new();
    for key in &all {
        let g = if *key < 500 { left } else { right };
        versions.push(cluster.latest(g, 0, *key).await.0);
    }
    // Same call, other chunks (every third row, reversed), against the new map.
    let gateway = cluster.gateway(
        direct_on(),
        Source::new(vec![after], cluster.voters()),
        cluster.transport(),
    );
    let third: Vec<i64> = all.iter().copied().step_by(3).rev().collect();
    let outcomes = call(
        &gateway,
        &cluster.upsert_many(id, rows(third.clone(), 0)),
        None,
    )
    .await
    .unwrap();
    assert!(
        applied(&outcomes),
        "{:?}",
        outcomes.iter().find(|o| **o != RowOutcome::Applied)
    );
    for (key, version) in all.iter().zip(&versions) {
        let g = if *key < 500 { left } else { right };
        assert_eq!(&cluster.latest(g, 0, *key).await.0, version, "row {key}");
    }
    cluster.shutdown().await;
}

/// Two rows that name the same key in different spellings of the same JSON
/// number are one key, and the call is refused whole before anything is sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn adv_vs15_one_key_in_two_spellings_is_a_duplicate() {
    let mut cluster = Cluster::batching().await;
    let group = cluster.add_group(span(None, None), 1, &[1]).await;
    let source = Source::new(vec![cluster.fill(&[group])], cluster.voters());
    let transport = cluster.transport();
    let gateway = cluster.gateway(direct_on(), source, transport.clone());
    let id = nabedra_rt::ids::uuid_v4();
    for body in [
        // -0 is 0; a leading zero is not JSON; an exponent is not an int64.
        cluster.delete_many(id, serde_json::from_str("[0,-0]").unwrap()),
        cluster.upsert_many(
            id,
            serde_json::from_str(r#"[{"id":0,"value":1},{"id":-0,"value":2}]"#).unwrap(),
        ),
    ] {
        let result = call(&gateway, &body, None).await;
        assert!(
            result.is_err() || result.as_ref().is_ok_and(|o| applied(o) && o.len() == 2),
            "{result:?}"
        );
        if let Ok(o) = &result {
            // Accepted as two rows: then they are two identities of one key,
            // which the call must not run.
            panic!("two spellings of key 0 were accepted as two rows: {o:?}");
        }
    }
    // A Unicode key twice in two normal forms is two keys (no normalisation).
    assert_eq!(transport.sent.load(Ordering::Relaxed), 0);
    let _ = AtomicUsize::new(0);
    cluster.shutdown().await;
}

fn b64(bytes: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = u32::from(b[0]) << 16 | u32::from(b[1]) << 8 | u32::from(b[2]);
        for i in 0..4 {
            out.push(if i <= chunk.len() {
                A[(n >> (18 - 6 * i)) as usize & 63] as char
            } else {
                '='
            });
        }
    }
    out
}
fn floats(values: &[f32]) -> String {
    b64(&values
        .iter()
        .flat_map(|v| v.to_le_bytes())
        .collect::<Vec<u8>>())
}
/// A one-range cluster whose leaders serve a table with a dense, a sparse and
/// a multi column once Control enabled the feature.
async fn vector_gateway() -> (Cluster, usize, Arc<DirectGateway>, Arc<InProcess>) {
    let mut cluster = Cluster::batching().await;
    let group = cluster.add_group(span(None, None), 1, &[1]).await;
    struct On;
    impl DirectGate for On {
        fn direct_active(&self) -> bool {
            true
        }
        fn direct_batch(&self) -> bool {
            true
        }
        fn vector(&self) -> bool {
            true
        }
    }
    let table: TableDescriptor = serde_json::from_value(json!({
        "id": cluster.table, "name": "items",
        "schema": {"id": "int64", "value": "int64",
                   "v": {"type": "vector", "dim": 2},
                   "s": {"type": "vector", "sparse": true},
                   "m": {"type": "list", "item": {"type": "vector", "dim": 2}},
                   "tags": {"type": "list", "item": "utf8"}},
        "key": "id", "format": "parquet"
    }))
    .unwrap();
    cluster.schema.insert(DirectTable {
        descriptor: table,
        schema_epoch: 1,
        indexes: Vec::new(),
        lifecycle_id: cluster.table,
    });
    for node in &mut cluster.nodes {
        node.executor = DirectExecutor::new(
            node.shared.clone(),
            DirectConfig::default(),
            Arc::new(On),
            cluster.schema.clone(),
            DirectTimestampClient::new(None, cluster.timestamps.clone()),
        );
    }
    let source = Source::new(vec![cluster.fill(&[group])], cluster.voters());
    let transport = cluster.transport();
    let gateway = cluster.gateway(direct_on(), source, transport.clone());
    (cluster, group, gateway, transport)
}
fn vrow(id: i64, extra: Value) -> Value {
    let mut row = json!({
        "id": id, "value": id, "tags": ["a"],
        "v": {"f32": floats(&[1.0, 2.0])},
        "s": {"sparse": {"i": floats(&[]), "f32": floats(&[])}},
        "m": {"f32s": floats(&[1.0, 2.0, 3.0, 4.0]), "n": 2},
    });
    for (key, value) in extra.as_object().unwrap() {
        row[key] = value.clone();
    }
    row
}

/// VEC-G11: a row that cannot be decoded (bad base64, a float that is not
/// finite, indices out of order, a column twice) refuses the whole call before
/// anything is sent, and nothing is stored.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adv_vec_g11_an_undecodable_vector_row_refuses_the_call_and_sends_nothing() {
    let (cluster, group, gateway, transport) = vector_gateway().await;
    let idx = |v: &[u32]| b64(&v.iter().flat_map(|i| i.to_le_bytes()).collect::<Vec<u8>>());
    let bad = [
        vrow(2, json!({"v": {"f32": "AAA"}})),
        vrow(2, json!({"v": {"f32": floats(&[f32::NAN, 1.0])}})),
        vrow(2, json!({"v": {"f32": floats(&[f32::INFINITY, 1.0])}})),
        vrow(
            2,
            json!({"s": {"sparse": {"i": idx(&[2, 1]), "f32": floats(&[1.0, 1.0])}}}),
        ),
        vrow(
            2,
            json!({"s": {"sparse": {"i": idx(&[1, 1]), "f32": floats(&[1.0, 1.0])}}}),
        ),
        vrow(2, json!({"m": {"f32s": floats(&[1.0, 2.0]), "n": 1025}})),
        vrow(2, json!({"v": {"f32": floats(&[1.0, 2.0]), "x": 1}})),
        vrow(2, json!({"tags": [1]})),
    ];
    for row in bad {
        let body = cluster.upsert_many(
            nabedra_rt::ids::uuid_v4(),
            json!([vrow(1, json!({})), row.clone(), vrow(3, json!({}))]),
        );
        let result = call(&gateway, &body, None).await;
        match result {
            Err((code, outcome)) => {
                assert!(
                    matches!(code, ErrorCode::ValidationError | ErrorCode::LimitExceeded),
                    "{code:?} for {row}"
                );
                assert_eq!(outcome, Outcome::NotApplied);
            }
            Ok(outcomes) => panic!("{row}: {outcomes:?}"),
        }
    }
    assert_eq!(transport.sent.load(Ordering::Relaxed), 0);
    for id in [1, 2, 3] {
        assert!(cluster.latest(group, 0, id).await.1.is_none(), "row {id}");
    }
    cluster.shutdown().await;
}

/// VEC-G09, VEC-G11: a well-formed row of the wrong dimension, kind or shape
/// is refused by its leader alone, as `not_applied` and a validation error;
/// its neighbours apply (atomic per row, like UNLOGGED), and none of the
/// refused rows is stored.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adv_vec_g09_a_row_of_the_wrong_shape_is_refused_alone_and_the_rest_apply() {
    let (cluster, group, gateway, _transport) = vector_gateway().await;
    let idx = |v: &[u32]| b64(&v.iter().flat_map(|i| i.to_le_bytes()).collect::<Vec<u8>>());
    let rows = vec![
        vrow(1, json!({})),
        vrow(2, json!({"v": {"f32": floats(&[1.0, 2.0, 3.0])}})),
        vrow(
            3,
            json!({"v": {"sparse": {"i": idx(&[1]), "f32": floats(&[1.0])}}}),
        ),
        vrow(4, json!({})),
        vrow(5, json!({"m": {"f32s": floats(&[1.0, 2.0, 3.0]), "n": 1}})),
        vrow(6, json!({"s": {"f32": floats(&[1.0])}})),
        vrow(7, json!({"value": null})),
        vrow(8, json!({"extra": 1})),
        vrow(9, json!({"v": null, "s": null, "m": null})),
    ];
    let body = cluster.upsert_many(nabedra_rt::ids::uuid_v4(), Value::Array(rows));
    let outcomes = call(&gateway, &body, None).await.unwrap();
    let refused = [2usize, 3, 5, 6, 7, 8];
    for (index, outcome) in outcomes.iter().enumerate() {
        let id = index as i64 + 1;
        if refused.contains(&(id as usize)) {
            assert!(
                matches!(outcome, RowOutcome::NotApplied { error } if error.code == ErrorCode::ValidationError && error.outcome == Outcome::NotApplied),
                "row {id}: {outcome:?}"
            );
            assert!(
                cluster.latest(group, 0, id).await.1.is_none(),
                "row {id} was refused but stored"
            );
        } else {
            assert_eq!(*outcome, RowOutcome::Applied, "row {id}");
            assert!(cluster.latest(group, 0, id).await.1.is_some(), "row {id}");
        }
    }
    cluster.shutdown().await;
}
