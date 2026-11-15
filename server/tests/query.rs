use meshdb_server::protocol;
use protocol::*;
#[path = "../src/engine/query.rs"]
mod query;

fn table() -> TableDescriptor {
    serde_json::from_str(r#"{"id":"00000000-0000-0000-0000-000000000001","name":"people","schema":{"id":"int64","name":"utf8","age":"int64","score":"float64","active":"bool"},"key":"id","format":"parquet"}"#).unwrap()
}
fn rows() -> Vec<Row> {
    [
        (5, "eve", 40, 8.0, true),
        (3, "cara", 25, 9.0, true),
        (1, "amy", 30, 9.0, true),
        (4, "dan", 17, 10.0, true),
        (2, "bob", 35, 10.0, false),
        (6, "fox", 50, 7.0, true),
    ]
    .into_iter()
    .map(|(id, name, age, score, active)| {
        Row::from([
            ("id".into(), Scalar::Int64(id)),
            ("name".into(), Scalar::Utf8(name.into())),
            ("age".into(), Scalar::Int64(age)),
            ("score".into(), Scalar::Float64(score)),
            ("active".into(), Scalar::Bool(active)),
        ])
    })
    .collect()
}
fn all() -> Query {
    Query {
        select: None,
        filter: None,
        order_by: None,
        limit: None,
    }
}
fn ids(rows: &[Row]) -> Vec<i64> {
    rows.iter()
        .map(|r| match r["id"] {
            Scalar::Int64(v) => v,
            _ => panic!("id"),
        })
        .collect()
}

#[test]
fn composed_typed_query_filters_orders_limits_then_projects() {
    let q: Query = serde_json::from_str(r#"{
        "select":["name"],"filter":{"op":"and","args":[
          {"op":"eq","column":"active","value":true},
          {"op":"ge","column":"age","value":18},
          {"op":"or","args":[{"op":"gt","column":"name","value":"b"},{"op":"eq","column":"name","value":"amy"}]},
          {"op":"not","arg":{"op":"eq","column":"id","value":6}},
          {"op":"le","column":"score","value":9.0}]},
        "order_by":{"column":"score","desc":true},"limit":2}"#).unwrap();
    assert_eq!(
        query::collect(&table(), &q, &rows()).unwrap(),
        vec![
            Row::from([("name".into(), Scalar::Utf8("amy".into()))]),
            Row::from([("name".into(), Scalar::Utf8("cara".into()))]),
        ]
    );
    assert_eq!(query::count(&table(), &q, &rows()).unwrap(), 2);
    let parts = rows();
    let matched: Vec<_> = parts
        .chunks(2)
        .flat_map(|part| query::local_collect(&table(), &q, part.to_vec()))
        .collect();
    assert_eq!(
        query::reduce_collect(&table(), &q, &matched).unwrap(),
        query::collect(&table(), &q, &parts).unwrap()
    );
    assert_eq!(
        parts
            .chunks(2)
            .map(|part| query::local_count(&q, part))
            .sum::<u64>()
            .min(q.limit.unwrap()),
        2
    );
}

#[test]
fn deterministic_default_and_explicit_order_including_float_zero() {
    assert_eq!(
        ids(&query::collect(&table(), &all(), &rows()).unwrap()),
        vec![1, 2, 3, 4, 5, 6]
    );
    let mut q = all();
    q.order_by = Some(OrderBy {
        column: "score".into(),
        desc: false,
    });
    assert_eq!(
        ids(&query::collect(&table(), &q, &rows()).unwrap()),
        vec![6, 5, 1, 3, 2, 4]
    );
    let mut zero_rows = rows();
    for r in &mut zero_rows {
        r.insert(
            "score".into(),
            Scalar::Float64(if r["id"] == Scalar::Int64(1) {
                0.0
            } else {
                -0.0
            }),
        );
    }
    q.order_by.as_mut().unwrap().desc = true;
    assert_eq!(
        ids(&query::collect(&table(), &q, &zero_rows).unwrap()),
        vec![1, 2, 3, 4, 5, 6]
    );
    let mut t = table();
    t.key = "name".into();
    assert_eq!(
        ids(&query::collect(&t, &all(), &rows()).unwrap()),
        vec![1, 2, 3, 4, 5, 6]
    );
}

#[test]
fn result_boundary_and_count_limits_do_not_truncate() {
    let t = table();
    let seed = rows().remove(0);
    let many: Vec<_> = (0..10_001)
        .map(|id| {
            let mut r = seed.clone();
            r.insert("id".into(), Scalar::Int64(id));
            r
        })
        .collect();
    assert_eq!(
        query::collect(&t, &all(), &many).unwrap_err().code,
        ErrorCode::LimitExceeded
    );
    assert_eq!(query::count(&t, &all(), &many).unwrap(), 10_001);
    let mut q = all();
    q.limit = Some(10_000);
    assert_eq!(query::collect(&t, &q, &many).unwrap().len(), 10_000);
    assert_eq!(query::count(&t, &q, &many).unwrap(), 10_000);
    q.limit = Some(u64::MAX);
    assert_eq!(query::count(&t, &q, &many).unwrap(), 10_001);
    assert_eq!(query::collect(&t, &q, &rows()).unwrap().len(), 6);
    q.limit = Some(0);
    assert!(query::collect(&t, &q, &many).unwrap().is_empty());
    assert_eq!(query::count(&t, &q, &many).unwrap(), 0);
}

#[test]
fn serialized_projected_array_has_a_body_budget() {
    let mut data = rows();
    data[0].insert("name".into(), Scalar::Utf8("\n".repeat(MAX_BODY_BYTES / 2)));
    assert_eq!(
        query::collect(&table(), &all(), &data).unwrap_err().code,
        ErrorCode::LimitExceeded
    );
    let q = Query {
        select: Some(vec!["id".into()]),
        ..all()
    };
    assert_eq!(query::collect(&table(), &q, &data).unwrap().len(), 6);
    assert_eq!(query::count(&table(), &all(), &data).unwrap(), 6);
    let huge_filter = Query {
        filter: Some(Predicate::Comparison(Comparison {
            op: ComparisonOp::Eq,
            column: "name".into(),
            value: data[0]["name"].clone(),
        })),
        ..all()
    };
    assert_eq!(
        query::explain(&table(), &huge_filter).err().unwrap().code,
        ErrorCode::LimitExceeded
    );
}

#[test]
fn validates_public_inputs_and_explains_without_rows() {
    let t = table();
    let mut invalid = all();
    invalid.select = Some(vec!["missing".into()]);
    assert_eq!(
        query::collect(&t, &invalid, &[]).unwrap_err().code,
        ErrorCode::ValidationError
    );
    assert_eq!(
        query::count(&t, &invalid, &[]).unwrap_err().code,
        ErrorCode::ValidationError
    );
    assert_eq!(
        query::explain(&t, &invalid).unwrap_err().code,
        ErrorCode::ValidationError
    );
    let mut bad_table = t.clone();
    bad_table.key = "score".into();
    assert_eq!(
        query::collect(&bad_table, &all(), &[]).unwrap_err().code,
        ErrorCode::ValidationError
    );
    let q: Query = serde_json::from_str(r#"{"select":["name"],"filter":{"op":"eq","column":"active","value":true},"order_by":{"column":"score","desc":true},"limit":2}"#).unwrap();
    let plan = query::explain(&t, &q).unwrap();
    for text in [
        "scan",
        "people",
        "filter",
        "active",
        "sort",
        "score DESC",
        "id ASC",
        "limit 2",
        "projection name",
    ] {
        assert!(plan.contains(text), "{plan} missing {text}");
    }
    let plan = query::explain(&t, &all()).unwrap();
    for text in ["filter none", "sort id ASC", "limit none", "projection all"] {
        assert!(plan.contains(text), "{plan}");
    }
}
