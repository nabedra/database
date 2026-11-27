//! Groups of rows on the range leader (`table.upsert_many`, `delete_many`):
//! shared entries, one answer per row, and the vector gate (VEC-G09, VEC-G14).
use super::*;
use crate::protocol::{row_request_id, upsert_row_request};
use serde_json::json;

fn row(id: i64) -> serde_json::Value {
    json!({"id": id, "value": id, "tag": "t", "score": 0.5})
}
/// The answers of a group, one per row.
fn replies(response: DirectResponse) -> Vec<DirectResponse> {
    let DirectResponse::Done(data) = response else {
        panic!("{response:?}");
    };
    serde_json::from_str(data.0.get()).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vs15_rows_of_a_group_share_entries_and_a_resend_replays_each() {
    let cluster = Cluster::new(Options {
        batch: true,
        ..Options::default()
    })
    .await;
    let leader = cluster.leader().await;
    let (before, records) = (
        cluster.last_index(leader),
        cluster.keys_with(leader, b'D').await,
    );
    let call = nabedra_rt::ids::uuid_v4();
    let count = 500;
    let rows: Vec<_> = (0..count).map(row).collect();
    let payload = json!({ "rows": rows });
    let answers = replies(
        cluster
            .run(leader, call, "table.upsert_many", payload.clone())
            .await,
    );
    assert_eq!(answers, vec![done("null"); count as usize]);
    let entries = cluster.last_index(leader) - before;
    let stats = cluster.voters[leader].executor.stats();
    assert_eq!(
        entries, 8,
        "{count} rows in windows of 64: {:?}",
        stats.batch_members
    );
    cluster.converge(leader).await;
    for voter in 0..3 {
        assert_eq!(
            cluster.keys_with(voter, b'D').await,
            records + count as usize,
            "one dedup record per row"
        );
    }
    // Index entries are written with their rows.
    for id in [0, count - 1] {
        let (version, _) = cluster.latest(leader, cluster.row_key(id)).await;
        assert_eq!(
            cluster.latest(leader, cluster.index_key(id, id)).await.0,
            version
        );
    }
    // The call again, and half of it in another chunk: no new entry.
    let after = cluster.last_index(leader);
    let half = json!({ "rows": rows[200..450] });
    for (payload, len) in [(payload, count as usize), (half, 250)] {
        let answers = replies(
            cluster
                .run(leader, call, "table.upsert_many", payload)
                .await,
        );
        assert_eq!(answers, vec![done("null"); len]);
    }
    assert_eq!(cluster.last_index(leader), after);
    let stats = cluster.voters[leader].executor.stats();
    assert_eq!(stats.replayed, count as u64 + 250);

    let keys: Vec<i64> = (count - 10..count + 10).collect();
    let answers = replies(
        cluster
            .op(leader, "table.delete_many", json!({ "keys": keys }))
            .await,
    );
    let expected = [vec![done("true"); 10], vec![done("false"); 10]].concat();
    assert_eq!(answers, expected, "a missing key is a success");
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vs15_each_row_of_a_group_has_its_own_answer() {
    let cluster = Cluster::new(Options {
        batch: true,
        ..Options::default()
    })
    .await;
    let leader = cluster.leader().await;
    let call = nabedra_rt::ids::uuid_v4();
    // Row 2 is settled for the queued path, row 4 committed with other values.
    let request_id = row_request_id(call, &Key::Int64(2)).unwrap();
    let two = crate::protocol::decode_upsert_row(&row(2).to_string()).unwrap();
    let request = upsert_row_request(call, cluster.table, "id", two).unwrap();
    let body = serde_json::to_vec(&request).unwrap();
    let voter = &cluster.voters[leader];
    assert_eq!(
        voter
            .executor
            .forward(&voter.handle, cluster.request(leader, &body, request_id))
            .await,
        DirectResponse::Fallback {
            reason: FallbackReason::Forwarded
        }
    );
    let mut other = row(4);
    other["value"] = json!(40);
    let answers = replies(
        cluster
            .run(leader, call, "table.upsert_many", json!({"rows": [other]}))
            .await,
    );
    assert_eq!(answers, [done("null")]);
    let mut wrong = row(3);
    wrong["tag"] = json!(3);
    let mut extra = row(6);
    extra["more"] = json!(1);
    let rows = json!([row(1), row(2), wrong, row(4), row(5), extra]);
    let answers = replies(
        cluster
            .run(leader, call, "table.upsert_many", json!({ "rows": rows }))
            .await,
    );
    assert_eq!(answers[0], done("null"));
    assert_eq!(
        answers[1],
        DirectResponse::Fallback {
            reason: FallbackReason::Forwarded
        }
    );
    let not_applied = |code| Some((code, Outcome::NotApplied));
    assert_eq!(code(&answers[2]), not_applied(ErrorCode::ValidationError));
    assert_eq!(code(&answers[3]), not_applied(ErrorCode::Conflict));
    assert_eq!(answers[4], done("null"));
    assert_eq!(code(&answers[5]), not_applied(ErrorCode::ValidationError));
    for (id, present) in [(1, true), (2, false), (3, false), (5, true), (6, false)] {
        let (_, value) = cluster.latest(leader, cluster.row_key(id)).await;
        assert_eq!(value.is_some(), present, "{id}");
    }
    let (_, four) = cluster.latest(leader, cluster.row_key(4)).await;
    assert!(four.unwrap().contains(r#""value":40"#));

    // A follower and a stale route answer for the whole group.
    let follower = (leader + 1) % 3;
    let payload = json!({"rows": [row(7)]});
    assert!(matches!(
        cluster
            .run(follower, call, "table.upsert_many", payload.clone())
            .await,
        DirectResponse::NotLeader { .. }
    ));
    let body = cluster.body(call, "table.upsert_many", payload);
    let request = cluster.request_at(leader, &body, call, 9, Duration::from_secs(5));
    assert!(matches!(
        voter.executor.execute(&voter.handle, request).await,
        DirectResponse::StaleRoute { .. }
    ));
    // A repeated key never reaches a leader from this gateway; one that does
    // is refused whole.
    for (operation, payload) in [
        ("table.delete_many", json!({"keys": [7, 7]})),
        ("table.upsert_many", json!({"rows": [row(7), row(7)]})),
        ("table.upsert_many", json!({"rows": [{"value": 1}, row(7)]})),
    ] {
        assert_eq!(
            code(&cluster.run(leader, call, operation, payload).await),
            not_applied(ErrorCode::ValidationError)
        );
    }
    assert_eq!(
        cluster.latest(leader, cluster.row_key(7)).await,
        (None, None)
    );
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vs20_a_leader_serves_no_vector_table_before_the_feature_is_enabled() {
    let cluster = Cluster::new(Options::default()).await;
    let leader = cluster.leader().await;
    let mut table = descriptor(cluster.table);
    table.schema.insert("v".to_string(), ColumnType::Vector(1));
    cluster.schema.insert(DirectTable {
        descriptor: table,
        schema_epoch: 1,
        lifecycle_id: cluster.table,
        indexes: Vec::new(),
    });
    let before = cluster.last_index(leader);
    let mut full = row(1);
    full["v"] = json!({"f32": "AACAPw=="});
    for (operation, payload) in [
        ("table.insert", json!({ "row": full })),
        ("table.upsert", json!({ "row": row(2) })),
        ("table.insert_many", json!({ "rows": [full] })),
        ("table.upsert_many", json!({ "rows": [full] })),
        ("table.update", json!({"key": 1, "values": {"value": 2}})),
        ("table.delete", json!({"key": 1})),
        ("table.delete_many", json!({"keys": [1]})),
        ("table.get", json!({"key": 1})),
    ] {
        assert_eq!(
            code(&cluster.op(leader, operation, payload).await),
            Some((ErrorCode::UnsupportedOperation, Outcome::NotApplied)),
            "{operation}"
        );
    }
    assert_eq!(cluster.last_index(leader), before, "nothing was proposed");
    cluster.shutdown().await;
}
