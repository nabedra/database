#![allow(clippy::disallowed_methods)] // integration tests drive real processes and real time; the phase 7 seam covers server/src
use nabedra_database_server::{
    partition_size::MAX_PARTITION_BYTES, protocol::*, snapshot::*, storage::KeyRange,
};
use uuid::Uuid;

fn table() -> TableDescriptor {
    TableDescriptor {
        id: Uuid::nil(),
        name: "items".into(),
        key: "id".into(),
        format: TableFormat::Parquet,
        schema: [
            ("id".into(), ColumnType::Int64),
            ("s".into(), ColumnType::Utf8),
            ("f".into(), ColumnType::Float64),
            ("b".into(), ColumnType::Bool),
        ]
        .into(),
    }
}
fn row(id: i64) -> Row {
    [
        ("id".into(), Scalar::Int64(id)),
        ("s".into(), Scalar::Utf8("雪🌍".into())),
        ("f".into(), Scalar::Float64(-1.25)),
        ("b".into(), Scalar::Bool(true)),
    ]
    .into()
}
#[test]
fn roundtrip_and_closed_empty_file() {
    let t = table();
    let mut rows = vec![row(i64::MIN), row(i64::MAX)];
    rows[1].insert("f".into(), Scalar::Float64(f64::MAX));
    rows[1].insert("b".into(), Scalar::Bool(false));
    let bytes = encode(&t, &rows).unwrap();
    assert_eq!(decode(&t, bytes.into(), Some(2)).unwrap(), rows);
    let bytes = encode(&t, &[]).unwrap();
    assert_eq!(&bytes[bytes.len() - 4..], b"PAR1");
    assert!(decode(&t, bytes.into(), Some(0)).unwrap().is_empty());
    let mut string_key = t.clone();
    string_key.key = "s".into();
    assert_eq!(
        decode(
            &string_key,
            encode(&string_key, &rows[..1]).unwrap().into(),
            Some(1)
        )
        .unwrap(),
        rows[..1]
    );
    assert_eq!(
        validate_rows(&string_key, &rows).unwrap_err().code,
        ErrorCode::Conflict
    );
}
#[test]
fn invalid_rows_and_stored_data_are_sanitized() {
    let t = table();
    assert_eq!(
        validate_rows(&t, &[row(1), row(1)]).unwrap_err().code,
        ErrorCode::Conflict
    );
    let mut r = row(1);
    r.insert("f".into(), Scalar::Float64(f64::NAN));
    assert_eq!(
        encode(&t, &[r]).unwrap_err().code,
        ErrorCode::ValidationError
    );
    let mut r = row(1);
    r.remove("s");
    assert!(encode(&t, &[r]).is_err());
    assert_eq!(
        decode(&t, bytes::Bytes::from_static(b"garbage"), None)
            .unwrap_err()
            .code,
        ErrorCode::StorageUnavailable
    );
    let bytes = encode(&t, &[row(1)]).unwrap();
    assert!(decode(&t, bytes.clone().into(), Some(2)).is_err());
    let mut wrong = t.clone();
    wrong.schema.insert("f".into(), ColumnType::Int64);
    assert!(decode(&wrong, bytes.into(), None).is_err());
}
#[test]
fn row_and_payload_limits() {
    let mut t = table();
    t.schema = [("id".into(), ColumnType::Int64)].into();
    let rows: Vec<Row> = (0..MAX_TABLE_ROWS)
        .map(|i| [("id".into(), Scalar::Int64(i as i64))].into())
        .collect();
    assert!(validate_rows(&t, &rows).is_ok());
    let mut excess = rows;
    excess.push(excess[0].clone());
    assert_eq!(
        validate_rows(&t, &excess).unwrap_err().code,
        ErrorCode::LimitExceeded
    );
    t.schema.insert("s".into(), ColumnType::Utf8);
    let base: Row = [
        ("id".into(), Scalar::Int64(0)),
        ("s".into(), Scalar::Utf8(String::new())),
    ]
    .into();
    let overhead = validate_rows(&t, std::slice::from_ref(&base)).unwrap();
    let mut boundary = base;
    boundary.insert(
        "s".into(),
        Scalar::Utf8("x".repeat(MAX_DECODED_PAYLOAD_BYTES - overhead)),
    );
    assert_eq!(
        validate_rows(&t, std::slice::from_ref(&boundary)).unwrap(),
        MAX_DECODED_PAYLOAD_BYTES
    );
    if let Scalar::Utf8(s) = boundary.get_mut("s").unwrap() {
        s.push('x');
    }
    assert_eq!(
        encode(&t, &[boundary]).unwrap_err().code,
        ErrorCode::LimitExceeded
    );
}

fn stored_fixture(
    fields: Vec<arrow_schema::Field>,
    arrays: Vec<arrow_array::ArrayRef>,
) -> bytes::Bytes {
    use parquet::{
        arrow::ArrowWriter,
        file::properties::{EnabledStatistics, WriterProperties},
    };
    let schema = std::sync::Arc::new(arrow_schema::Schema::new(fields));
    let batch = arrow_array::RecordBatch::try_new(schema.clone(), arrays).unwrap();
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .build();
    let mut bytes = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
    bytes.into()
}
#[test]
fn stored_null_nonfinite_duplicates_and_wrong_physical_type() {
    use arrow_array::{ArrayRef, Float64Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field};
    use std::sync::Arc;
    let mut t = table();
    t.schema = [("id".into(), ColumnType::Int64)].into();
    let id = |nullable| Field::new("id", DataType::Int64, nullable);
    let bad: Vec<(Vec<Field>, Vec<ArrayRef>)> = vec![
        (vec![id(true)], vec![Arc::new(Int64Array::from(vec![None]))]),
        (
            vec![id(false)],
            vec![Arc::new(Int64Array::from(vec![1, 1]))],
        ),
        (
            vec![Field::new("id", DataType::Utf8, false)],
            vec![Arc::new(StringArray::from(vec!["1"]))],
        ),
    ];
    for (fields, arrays) in bad {
        let e = decode(&t, stored_fixture(fields, arrays), None).unwrap_err();
        assert_eq!(e.code, ErrorCode::StorageUnavailable);
        assert_eq!(e.outcome, Outcome::NotApplied);
    }
    t.schema.insert("f".into(), ColumnType::Float64);
    let bytes = stored_fixture(
        vec![Field::new("f", DataType::Float64, false), id(false)],
        vec![
            Arc::new(Float64Array::from(vec![f64::INFINITY])),
            Arc::new(Int64Array::from(vec![1])),
        ],
    );
    assert_eq!(
        decode(&t, bytes, None).unwrap_err().code,
        ErrorCode::StorageUnavailable
    );
}
#[test]
fn oversized_metadata_and_string_page_fail_before_arrow_allocation() {
    use parquet::file::{
        metadata::ParquetMetaDataWriter, reader::FileReader,
        serialized_reader::SerializedFileReader,
    };
    let t = table();
    let original = encode(&t, &[row(1)]).unwrap();
    let mut page = original.clone();
    let pos = page
        .windows("雪🌍".len())
        .position(|b| b == "雪🌍".as_bytes())
        .unwrap();
    page[pos - 4..pos].copy_from_slice(&u32::MAX.to_le_bytes());
    assert_eq!(
        decode(&t, page.into(), None).unwrap_err().code,
        ErrorCode::LimitExceeded
    );
    let reader = SerializedFileReader::new(bytes::Bytes::from(original.clone())).unwrap();
    let mut metadata = reader.metadata().clone().into_builder();
    let groups = metadata.take_row_groups();
    let group = groups[0]
        .clone()
        .into_builder()
        .set_num_rows(MAX_TABLE_ROWS as i64 + 1)
        .build()
        .unwrap();
    let metadata = metadata.add_row_group(group).build();
    let footer_len = u32::from_le_bytes(
        original[original.len() - 8..original.len() - 4]
            .try_into()
            .unwrap(),
    ) as usize;
    let mut bytes = original[..original.len() - 8 - footer_len].to_vec();
    ParquetMetaDataWriter::new(&mut bytes, &metadata)
        .finish()
        .unwrap();
    assert_eq!(
        decode(&t, bytes.into(), None).unwrap_err().code,
        ErrorCode::LimitExceeded
    );
    let mut truncated = original;
    truncated.truncate(truncated.len() - 6);
    assert_eq!(
        decode(&t, truncated.into(), None).unwrap_err().code,
        ErrorCode::StorageUnavailable
    );
}

#[test]
fn roundtrip_full_128_column_schema() {
    let mut t = table();
    t.schema = (0..128)
        .map(|i| (format!("c{i:03}{}", "x".repeat(60)), ColumnType::Int64))
        .collect();
    t.key = format!("c000{}", "x".repeat(60));
    let row: Row = t
        .schema
        .keys()
        .map(|k| (k.clone(), Scalar::Int64(7)))
        .collect();
    let rows = vec![row];
    let files = encode_partition_range(
        &t,
        rows.clone(),
        KeyRange {
            lower: None,
            upper: None,
        },
        Default::default(),
    )
    .unwrap();
    assert_eq!(files.len(), 1);
    let bytes = &files[0].bytes;
    let footer_bytes =
        u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap());
    assert!(footer_bytes < 1024 * 1024);
    assert_eq!(decode(&t, bytes.clone().into(), Some(1)).unwrap(), rows);
    assert_eq!(
        decode(&t, encode(&t, &rows).unwrap().into(), Some(1)).unwrap(),
        rows
    );
    assert!(
        decode(&t, encode(&t, &[]).unwrap().into(), Some(0))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn partition_splits_preserve_typed_order_ranges_and_payload() {
    for string_keys in [false, true] {
        let mut t = table();
        if string_keys {
            t.key = "s".into();
        }
        let mut rows = vec![row(12), row(-9), row(2), row(-1), row(0), row(99)];
        for (r, s) in rows.iter_mut().zip(["雪", "é", "a", "🌍", "z", "Ω"]) {
            r.insert("s".into(), Scalar::Utf8(s.repeat(100)));
        }
        let mut expected = rows.clone();
        expected.sort_by(|a, b| match (&a[&t.key], &b[&t.key]) {
            (Scalar::Int64(a), Scalar::Int64(b)) => a.cmp(b),
            (Scalar::Utf8(a), Scalar::Utf8(b)) => a.cmp(b),
            _ => unreachable!(),
        });
        let range = if string_keys {
            KeyRange {
                lower: Some(Key::Utf8(String::new())),
                upper: None,
            }
        } else {
            KeyRange {
                lower: Some(Key::Int64(-100)),
                upper: Some(Key::Int64(1000)),
            }
        };
        let target = "1800 B".parse().unwrap();
        let files = encode_partition_range(&t, rows.clone(), range.clone(), target).unwrap();
        assert!(files.len() > 1);
        assert_eq!(files.first().unwrap().range.lower, range.lower);
        assert_eq!(files.last().unwrap().range.upper, range.upper);
        assert_eq!(
            files.iter().map(|f| f.payload_bytes).sum::<u64>(),
            validate_rows(&t, &rows).unwrap() as u64
        );
        let mut decoded = Vec::new();
        for (i, file) in files.iter().enumerate() {
            assert!(file.bytes.len() <= 1800 || file.row_count == 1);
            assert!(file.bytes.len() <= MAX_PARTITION_BYTES);
            let batch = decode(&t, file.bytes.clone().into(), Some(file.row_count)).unwrap();
            for r in &batch {
                let key = match &r[&t.key] {
                    Scalar::Int64(n) => Key::Int64(*n),
                    Scalar::Utf8(s) => Key::Utf8(s.clone()),
                    _ => unreachable!(),
                };
                assert!(file.range.contains(&key, t.schema[&t.key]).unwrap());
            }
            if i > 0 {
                let first = match &batch[0][&t.key] {
                    Scalar::Int64(n) => Key::Int64(*n),
                    Scalar::Utf8(s) => Key::Utf8(s.clone()),
                    _ => unreachable!(),
                };
                assert_eq!(file.range.lower, Some(first));
                assert_eq!(files[i - 1].range.upper, file.range.lower);
            }
            decoded.extend(batch);
        }
        assert_eq!(decoded, expected);
    }
}

#[test]
fn partition_empty_singleton_and_invalid_inputs() {
    let t = table();
    let range = KeyRange {
        lower: None,
        upper: Some(Key::Int64(5)),
    };
    let target = "1 B".parse().unwrap();
    assert!(
        encode_partition_range(&t, vec![], range.clone(), target)
            .unwrap()
            .is_empty()
    );
    let files = encode_partition_range(&t, vec![row(-1)], range.clone(), target).unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].range, range);
    assert!(files[0].bytes.len() > target.bytes());
    for rows in [vec![row(5)], vec![row(1), row(1)]] {
        assert!(encode_partition_range(&t, rows, range.clone(), target).is_err());
    }
    let mut invalid = row(1);
    invalid.remove("b");
    assert!(encode_partition_range(&t, vec![invalid], range, target).is_err());
    let invalid_range = KeyRange {
        lower: Some(Key::Utf8("a".into())),
        upper: None,
    };
    assert!(encode_partition_range(&t, vec![], invalid_range, target).is_err());
}

#[test]
fn partition_real_hard_cap_splits_multiple_rows_and_rejects_singleton() {
    let mut t = table();
    t.schema = [
        ("id".into(), ColumnType::Int64),
        ("s".into(), ColumnType::Utf8),
    ]
    .into();
    let range = KeyRange {
        lower: None,
        upper: None,
    };
    let empty_row = |id| -> Row {
        [
            ("id".into(), Scalar::Int64(id)),
            ("s".into(), Scalar::Utf8(String::new())),
        ]
        .into()
    };
    let overhead = validate_rows(&t, &[empty_row(0), empty_row(1)]).unwrap();
    let rows: Vec<Row> = (0..2)
        .map(|id| {
            let mut row = empty_row(id);
            row.insert(
                "s".into(),
                Scalar::Utf8("x".repeat((MAX_PARTITION_BYTES - overhead) / 2)),
            );
            row
        })
        .collect();
    assert_eq!(validate_rows(&t, &rows).unwrap(), MAX_DECODED_PAYLOAD_BYTES);
    // The decoded candidate fits, but the real closed codec output crosses the
    // file cap. The default target equals the cap, so this proves a hard split.
    assert!(encode(&t, &rows).unwrap().len() > MAX_PARTITION_BYTES);
    let files = encode_partition_range(&t, rows, range.clone(), Default::default()).unwrap();
    assert_eq!(files.len(), 2);
    assert!(
        files
            .iter()
            .all(|f| f.row_count == 1 && f.bytes.len() <= MAX_PARTITION_BYTES)
    );
    drop(files);
    let mut row = empty_row(0);
    let overhead = validate_rows(&t, std::slice::from_ref(&row)).unwrap();
    row.insert(
        "s".into(),
        Scalar::Utf8("x".repeat(MAX_PARTITION_BYTES - overhead)),
    );
    assert_eq!(
        validate_rows(&t, std::slice::from_ref(&row)).unwrap(),
        MAX_DECODED_PAYLOAD_BYTES
    );
    assert!(encode(&t, std::slice::from_ref(&row)).unwrap().len() > MAX_PARTITION_BYTES);
    let e = encode_partition_range(&t, vec![row], range, Default::default()).unwrap_err();
    assert_eq!(e.code, ErrorCode::LimitExceeded);
    assert_eq!(e.outcome, Outcome::NotApplied);
}
