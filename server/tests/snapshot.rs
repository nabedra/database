use meshdb_server::{protocol::*, snapshot::*};
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
        .map(|i| (format!("c{i:03}"), ColumnType::Int64))
        .collect();
    t.key = "c000".into();
    let row: Row = t
        .schema
        .keys()
        .map(|k| (k.clone(), Scalar::Int64(7)))
        .collect();
    let rows = vec![row];
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
