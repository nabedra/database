use meshdb_server::protocol;
use protocol::*;
#[path = "../src/engine/mutation.rs"]
mod mutation;
use mutation::{MutationResult, apply};

fn table() -> TableDescriptor {
    serde_json::from_str(r#"{"id":"00000000-0000-0000-0000-000000000001","name":"items","schema":{"id":"int64","name":"utf8","score":"float64","active":"bool"},"key":"id","format":"parquet"}"#).unwrap()
}
fn row(id: i64) -> Row {
    Row::from([
        ("id".into(), Scalar::Int64(id)),
        ("name".into(), Scalar::Utf8(format!("item{id}"))),
        ("score".into(), Scalar::Float64(-0.0)),
        ("active".into(), Scalar::Bool(false)),
    ])
}
fn insert(t: &TableDescriptor, row: Row) -> Operation {
    Operation::Insert(RowPayload {
        table_id: t.id,
        row,
    })
}
fn batch(t: &TableDescriptor, rows: Vec<Row>) -> Operation {
    Operation::InsertMany(BatchPayload {
        table_id: t.id,
        rows,
    })
}
fn update(t: &TableDescriptor, key: Key, values: Row) -> Operation {
    Operation::Update(UpdatePayload {
        table_id: t.id,
        key,
        values,
    })
}
fn upsert(t: &TableDescriptor, row: Row) -> Operation {
    Operation::Upsert(RowPayload {
        table_id: t.id,
        row,
    })
}
fn delete(t: &TableDescriptor, key: Key) -> Operation {
    Operation::Delete(KeyPayload {
        table_id: t.id,
        key,
    })
}
fn rejects(t: &TableDescriptor, op: &Operation, rows: Vec<Row>, code: ErrorCode) {
    let error = apply(t, op, rows).unwrap_err();
    assert_eq!(error.code, code);
    assert_eq!(error.outcome, Outcome::NotApplied);
}

#[test]
fn public_mutations_preserve_types_and_return_wire_results() {
    let t = table();
    let first = apply(&t, &insert(&t, row(1)), vec![]).unwrap();
    assert!(first.changed);
    assert_eq!(first.result, MutationResult::Null);
    assert_eq!(
        serde_json::to_value(&first.result).unwrap(),
        serde_json::Value::Null
    );
    assert_eq!(first.rows, vec![row(1)]);
    let many = apply(&t, &batch(&t, vec![row(2), row(3)]), first.rows).unwrap();
    assert!(many.changed);
    assert_eq!(many.result, MutationResult::Count(2));
    assert_eq!(serde_json::to_value(&many.result).unwrap(), 2);
    let values = Row::from([
        ("score".into(), Scalar::Float64(1.23456789012345)),
        ("active".into(), Scalar::Bool(true)),
    ]);
    let changed = apply(&t, &update(&t, Key::Int64(2), values.clone()), many.rows).unwrap();
    assert!(changed.changed);
    assert_eq!(changed.result, MutationResult::Found(true));
    assert_eq!(serde_json::to_value(&changed.result).unwrap(), true);
    assert_eq!(changed.rows[1]["name"], row(2)["name"]);
    for (column, value) in values {
        assert_eq!(changed.rows[1][&column], value);
    }
    let removed = apply(&t, &delete(&t, Key::Int64(2)), changed.rows).unwrap();
    assert!(removed.changed);
    assert_eq!(removed.result, MutationResult::Found(true));
    assert_eq!(removed.rows, vec![row(1), row(3)]);
}

#[test]
fn duplicate_rejection_is_atomic_including_a_late_conflict() {
    let t = table();
    let original = vec![row(1)];
    for op in [
        insert(&t, row(1)),
        batch(&t, vec![row(2), row(2)]),
        batch(&t, vec![row(2), row(1)]),
    ] {
        rejects(&t, &op, original.clone(), ErrorCode::Conflict);
    }
    // Failed attempts produce no candidate rows and cannot affect caller data.
    assert_eq!(
        apply(&t, &insert(&t, row(2)), original).unwrap().rows,
        vec![row(1), row(2)]
    );
}

#[test]
fn missing_and_noop_mutations_report_found_separately_from_changed() {
    let t = table();
    for (op, result) in [
        (batch(&t, vec![]), MutationResult::Count(0)),
        (
            update(&t, Key::Int64(9), Row::new()),
            MutationResult::Found(false),
        ),
        (delete(&t, Key::Int64(9)), MutationResult::Found(false)),
        (
            update(&t, Key::Int64(1), Row::new()),
            MutationResult::Found(true),
        ),
        (
            update(
                &t,
                Key::Int64(1),
                Row::from([("active".into(), Scalar::Bool(false))]),
            ),
            MutationResult::Found(true),
        ),
        (upsert(&t, row(1)), MutationResult::Null),
    ] {
        let m = apply(&t, &op, vec![row(1)]).unwrap();
        assert_eq!(m.result, result);
        assert!(!m.changed);
        assert_eq!(m.rows, vec![row(1)]);
    }
}

#[test]
fn upsert_replaces_complete_rows_and_preserves_float_bits() {
    let t = table();
    let mut replacement = row(1);
    replacement.insert("name".into(), Scalar::Utf8("replacement".into()));
    replacement.insert("score".into(), Scalar::Float64(0.0));
    let m = apply(&t, &upsert(&t, replacement.clone()), vec![row(1)]).unwrap();
    assert!(m.changed);
    assert_eq!(m.rows, vec![replacement.clone()]);
    let m = apply(
        &t,
        &update(
            &t,
            Key::Int64(1),
            Row::from([("score".into(), Scalar::Float64(-0.0))]),
        ),
        m.rows,
    )
    .unwrap();
    assert!(m.changed);
    let Scalar::Float64(score) = m.rows[0]["score"] else {
        panic!("float expected")
    };
    assert_eq!(score.to_bits(), (-0.0_f64).to_bits());
    let m = apply(&t, &upsert(&t, row(2)), m.rows).unwrap();
    assert!(m.changed);
    assert_eq!(m.rows[1], row(2));
    replacement.remove("active");
    rejects(
        &t,
        &upsert(&t, replacement),
        m.rows,
        ErrorCode::ValidationError,
    );
}

#[test]
fn validates_public_operations_before_lookup_or_changes() {
    let t = table();
    let mut wrong_type = row(2);
    wrong_type.insert("score".into(), Scalar::Int64(1));
    let mut nonfinite = row(2);
    nonfinite.insert("score".into(), Scalar::Float64(f64::NAN));
    let mut wrong_id = t.clone();
    wrong_id.id = uuid::Uuid::nil();
    for op in [
        insert(&t, wrong_type),
        insert(&t, nonfinite),
        insert(&wrong_id, row(2)),
        delete(&t, Key::Utf8("1".into())),
        update(
            &t,
            Key::Int64(9),
            Row::from([("id".into(), Scalar::Int64(9))]),
        ),
        update(
            &t,
            Key::Int64(9),
            Row::from([("active".into(), Scalar::Int64(1))]),
        ),
    ] {
        rejects(&t, &op, vec![row(1)], ErrorCode::ValidationError);
    }
    let mut invalid_table = t.clone();
    invalid_table.key = "missing".into();
    rejects(
        &invalid_table,
        &batch(&t, vec![]),
        vec![],
        ErrorCode::ValidationError,
    );
    rejects(
        &t,
        &Operation::Get(KeyPayload {
            table_id: t.id,
            key: Key::Int64(1),
        }),
        vec![row(1)],
        ErrorCode::UnsupportedOperation,
    );
    rejects(
        &t,
        &Operation::List(EmptyPayload {}),
        vec![],
        ErrorCode::UnsupportedOperation,
    );
    let mut strings = t.clone();
    strings.key = "name".into();
    let m = apply(
        &strings,
        &delete(&strings, Key::Utf8("item1".into())),
        vec![row(1)],
    )
    .unwrap();
    assert!(m.changed && m.rows.is_empty());
    rejects(
        &strings,
        &delete(&strings, Key::Int64(1)),
        vec![row(1)],
        ErrorCode::ValidationError,
    );
    let mut duplicate = row(2);
    duplicate.insert("name".into(), row(1)["name"].clone());
    rejects(
        &strings,
        &batch(&strings, vec![row(1), duplicate]),
        vec![],
        ErrorCode::Conflict,
    );
}

#[test]
fn batch_and_resulting_row_limits_reject_without_truncation() {
    let t = table();
    let rows: Vec<_> = (0..MAX_BATCH_ROWS as i64).map(row).collect();
    assert_eq!(
        apply(&t, &batch(&t, rows.clone()), vec![])
            .unwrap()
            .rows
            .len(),
        MAX_BATCH_ROWS
    );
    let mut excess = rows;
    excess.push(row(MAX_BATCH_ROWS as i64));
    rejects(&t, &batch(&t, excess), vec![], ErrorCode::LimitExceeded);
    let full: Vec<_> = (0..MAX_TABLE_ROWS as i64).map(row).collect();
    for op in [
        insert(&t, row(-1)),
        upsert(&t, row(-1)),
        batch(&t, vec![row(-1), row(-2)]),
    ] {
        rejects(&t, &op, full.clone(), ErrorCode::LimitExceeded);
    }
    assert_eq!(
        apply(&t, &upsert(&t, row(0)), full.clone())
            .unwrap()
            .rows
            .len(),
        MAX_TABLE_ROWS
    );
    let mut almost = full;
    almost.pop();
    assert_eq!(
        apply(&t, &insert(&t, row(-1)), almost).unwrap().rows.len(),
        MAX_TABLE_ROWS
    );
}
