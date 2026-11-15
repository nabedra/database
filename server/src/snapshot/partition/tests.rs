use super::*;
use serde_json::json;

#[test]
fn incremental_emission_stops_on_error_and_validates_all_rows_before_output() {
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
    let mut calls = 0;
    let err = encode_partition_range_each(&table, rows.clone(), range.clone(), target, |_| {
        calls += 1;
        Err(error(ErrorCode::Conflict, "stop"))
    })
    .unwrap_err();
    assert_eq!(calls, 1);
    assert_eq!(err.code, ErrorCode::Conflict);
    assert_eq!(err.message, "stop");

    // A later invalid row must fail before a caller can upload any earlier file.
    // The tiny target would emit each valid row separately without preflight.
    let mut malformed = rows.clone();
    malformed.push(serde_json::from_value(json!({"id":"wrong type"})).unwrap());
    let mut duplicate = rows.clone();
    duplicate.push(rows[0].clone());
    for (input, input_range, code) in [
        (malformed, range.clone(), ErrorCode::ValidationError),
        (duplicate, range.clone(), ErrorCode::Conflict),
        (
            rows,
            KeyRange {
                lower: None,
                upper: Some(crate::protocol::Key::Int64(3)),
            },
            ErrorCode::ValidationError,
        ),
    ] {
        let mut emissions = 0;
        let err = encode_partition_range_each(&table, input, input_range, target, |_| {
            emissions += 1;
            Ok(())
        })
        .unwrap_err();
        assert_eq!(err.code, code);
        assert_eq!(err.outcome, crate::protocol::Outcome::NotApplied);
        assert_eq!(emissions, 0);
    }
    encode_partition_range_each(&table, vec![], range, target, |_| panic!("empty emission"))
        .unwrap();
}
