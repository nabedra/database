use super::*;
use bytes::Bytes;
use serde_json::json;

#[test]
fn incremental_matches_collecting_and_stops_on_error() {
    let table: TableDescriptor = serde_json::from_value(json!({"id":"00000000-0000-0000-0000-000000000006","name":"items","schema":{"id":"int64"},"key":"id","format":"parquet"})).unwrap();
    let rows: Vec<Row> = [3, 1, 2]
        .into_iter()
        .map(|id| serde_json::from_value(json!({"id":id})).unwrap())
        .collect();
    let range = KeyRange {
        lower: None,
        upper: None,
    };
    let target = "1B".parse().unwrap();
    let collected = encode_partition_range(&table, rows.clone(), range.clone(), target).unwrap();
    let mut emitted = Vec::new();
    encode_partition_range_each(&table, rows.clone(), range.clone(), target, |part| {
        emitted.push(part);
        Ok(())
    })
    .unwrap();
    assert_eq!(emitted.len(), 3);
    for (a, b) in collected.iter().zip(&emitted) {
        assert_eq!(a.range, b.range);
        assert_eq!(
            (a.row_count, a.payload_bytes),
            (b.row_count, b.payload_bytes)
        );
        assert_eq!(
            super::super::decode(&table, Bytes::from(a.bytes.clone()), Some(a.row_count)).unwrap(),
            super::super::decode(&table, Bytes::from(b.bytes.clone()), Some(b.row_count)).unwrap()
        );
        assert!(b.bytes.len() > target.bytes());
    }
    let mut calls = 0;
    let err = encode_partition_range_each(&table, rows, range.clone(), target, |_| {
        calls += 1;
        Err(error(ErrorCode::Conflict, "stop"))
    })
    .unwrap_err();
    assert_eq!(calls, 1);
    assert_eq!(err.code, ErrorCode::Conflict);
    assert_eq!(err.message, "stop");
    encode_partition_range_each(&table, vec![], range, target, |_| panic!("empty emission"))
        .unwrap();
}
