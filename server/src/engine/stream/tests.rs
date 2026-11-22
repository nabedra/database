use super::*;
use crate::{
    engine::query,
    protocol::{ColumnType, Key, Row, Scalar, TableFormat},
    snapshot,
    storage::{
        Download, KeyRange, Namespace, ObjectKey, PartitionFile, StorageError, WriteCondition,
    },
    test_storage::Fake,
};
use arrow_array::{BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_ipc::reader::StreamReader;
use arrow_schema::{DataType, Field, Schema};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::{Compression, Encoding},
    file::properties::{EnabledStatistics, WriterProperties},
};
use serde_json::json;
use std::{num::NonZeroUsize, sync::Mutex};
use tokio::runtime::Handle;

mod olap_oracles;

#[derive(Default)]
struct Store {
    objects: Fake,
    reads: Mutex<Vec<String>>,
}

impl ObjectStore for Store {
    async fn get(&self, key: &str) -> Result<Download, StorageError> {
        self.reads.lock().unwrap().push(key.to_owned());
        self.objects.get(key).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        condition: WriteCondition<'_>,
    ) -> Result<String, StorageError> {
        self.objects.put(key, bytes, condition).await
    }
}

fn table() -> TableDescriptor {
    TableDescriptor {
        id: Uuid::new_v4(),
        name: "stream_rows".into(),
        key: "id".into(),
        format: TableFormat::Parquet,
        schema: [
            ("id".into(), ColumnType::Int64),
            ("active".into(), ColumnType::Bool),
            ("score".into(), ColumnType::Float64),
            ("text".into(), ColumnType::Utf8),
        ]
        .into(),
    }
}

fn row(id: i64, text: String) -> Row {
    Row::from([
        ("id".into(), Scalar::Int64(id)),
        ("active".into(), Scalar::Bool(id % 2 == 0)),
        ("score".into(), Scalar::Float64(id as f64 / 4.0)),
        ("text".into(), Scalar::Utf8(text)),
    ])
}

fn grouped_parquet(rows: &[Row]) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("active", DataType::Boolean, false),
        Field::new("id", DataType::Int64, false),
        Field::new("score", DataType::Float64, false),
        Field::new("text", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(BooleanArray::from(
                rows.iter()
                    .map(|row| match row["active"] {
                        Scalar::Bool(value) => value,
                        _ => unreachable!(),
                    })
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(
                |row| match row["id"] {
                    Scalar::Int64(value) => value,
                    _ => unreachable!(),
                },
            ))),
            Arc::new(Float64Array::from_iter_values(rows.iter().map(
                |row| match row["score"] {
                    Scalar::Float64(value) => value,
                    _ => unreachable!(),
                },
            ))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(
                |row| match &row["text"] {
                    Scalar::Utf8(value) => value,
                    _ => unreachable!(),
                },
            ))),
        ],
    )
    .unwrap();
    let properties = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::PLAIN)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_offset_index_disabled(true)
        .set_max_row_group_row_count(Some(4))
        .set_max_row_group_bytes(None)
        .build();
    let mut writer = ArrowWriter::try_new(Vec::new(), schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.into_inner().unwrap()
}

fn catalog(table: &TableDescriptor, row_count: usize, data: TableData) -> Catalog {
    let mut catalog = Catalog::empty();
    catalog.commit_id = Uuid::new_v4();
    catalog.tables.insert(
        table.name.clone(),
        CatalogEntry {
            id: table.id,
            schema: table.schema.clone(),
            key: table.key.clone(),
            format: table.format,
            row_count: row_count as u64,
            data,
        },
    );
    catalog.validate().unwrap();
    catalog
}

fn all() -> Query {
    Query {
        select: None,
        filter: None,
        order_by: None,
        limit: None,
    }
}

fn options(format: BatchFormat, batch_rows: usize) -> BatchOptions {
    BatchOptions {
        batch_rows,
        max_batch_bytes: 48 * 1024,
        format,
        drop_last: false,
    }
}

fn decode_rows(bytes: &[u8]) -> Vec<Row> {
    #[derive(Deserialize)]
    struct Frame {
        rows: Vec<Row>,
    }
    serde_json::from_slice::<Frame>(bytes).unwrap().rows
}

struct ScanFixture {
    storage: Storage<Store>,
    pool: WorkerPool,
}

impl ScanFixture {
    fn new() -> (Self, crate::worker_pool::WorkerThreads) {
        let storage = Storage::new(Store::default(), Namespace::new("stream/").unwrap());
        let (pool, workers) =
            WorkerPool::start(NonZeroUsize::new(2).unwrap(), Handle::current()).unwrap();
        (Self { storage, pool }, workers)
    }

    async fn legacy(&self, table: &TableDescriptor, rows: &[Row]) -> Catalog {
        let key = ObjectKey::snapshot(table.id, Uuid::new_v4());
        self.storage
            .put(
                &key,
                snapshot::encode(table, rows).unwrap(),
                WriteCondition::Create,
            )
            .await
            .unwrap();
        catalog(
            table,
            rows.len(),
            TableData::Legacy {
                snapshot: key.as_str().into(),
            },
        )
    }

    async fn partitioned(&self, table: &TableDescriptor, rows: &[Row], split: i64) -> Catalog {
        let files = [(None, Some(split)), (Some(split), None)];
        let mut manifest = Vec::new();
        let commit = Uuid::new_v4();
        let (left, right) = rows.split_at(split as usize);
        for (index, ((lower, upper), partition)) in files.into_iter().zip([left, right]).enumerate()
        {
            let data = grouped_parquet(partition);
            assert!(
                ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(&data))
                    .unwrap()
                    .metadata()
                    .num_row_groups()
                    > 1
            );
            let key = ObjectKey::partition(table.id, commit, index as u32);
            manifest.push(PartitionFile {
                object: key.as_str().into(),
                range: KeyRange {
                    lower: lower.map(Key::Int64),
                    upper: upper.map(Key::Int64),
                },
                byte_size: data.len() as u64,
                row_count: partition.len() as u64,
                payload_bytes: partition.iter().map(snapshot::row_payload).sum::<usize>() as u64,
            });
            self.storage
                .put(&key, data, WriteCondition::Create)
                .await
                .unwrap();
        }
        catalog(
            table,
            rows.len(),
            TableData::Partitioned { files: manifest },
        )
    }

    async fn rows(
        &self,
        scan: &Arc<PinnedScan>,
        mut cursor: Cursor,
        batch_rows: usize,
        restart_reader: bool,
    ) -> Vec<Vec<Row>> {
        let mut reader = ScanReader::default();
        let mut batches = Vec::new();
        loop {
            let batch = reader
                .next_batch(
                    &self.storage,
                    &self.pool,
                    scan.clone(),
                    cursor,
                    options(BatchFormat::Rows, batch_rows),
                )
                .await
                .unwrap();
            let Some(data) = batch.data else {
                return batches;
            };
            batches.push(decode_rows(data.bytes()));
            cursor = batch.cursor;
            if restart_reader {
                cursor = serde_json::from_slice(&serde_json::to_vec(&cursor).unwrap()).unwrap();
                reader = ScanReader::default();
            }
        }
    }
}

#[tokio::test]
async fn st01_partition_and_group_replay_matches_serial_query() {
    let (fixture, workers) = ScanFixture::new();
    let table = table();
    let rows: Vec<_> = (0..27).map(|id| row(id, format!("row-{id}-é"))).collect();
    let catalog = fixture.partitioned(&table, &rows, 13).await;
    let mut query: Query = serde_json::from_str(
        &json!({
            "select": ["text"],
            "filter": {"op":"and","args":[
                {"op":"or","args":[
                    {"op":"ge","column":"score","value":4.25},
                    {"op":"eq","column":"id","value":3}
                ]},
                {"op":"not","arg":{"op":"eq","column":"active","value":true}}
            ]},
            "order_by": {"column":"id","desc":false},
            "limit": 5
        })
        .to_string(),
    )
    .unwrap();
    let scan = Arc::new(PinnedScan::capture(&catalog, table.id, query.clone()).unwrap());
    let batches = fixture
        .rows(&scan, scan.cursor(0, 1).unwrap(), 3, true)
        .await;
    assert_eq!(batches.iter().map(Vec::len).collect::<Vec<_>>(), [3, 2]);
    let actual: Vec<_> = batches.into_iter().flatten().collect();
    assert_eq!(actual, query::collect(&table, &query, &rows).unwrap());
    assert_eq!(
        actual
            .into_iter()
            .map(|row| row["text"].clone())
            .collect::<Vec<_>>(),
        [3, 17, 19, 21, 23].map(|id| Scalar::Utf8(format!("row-{id}-é")))
    );

    query.limit = Some(0);
    let zero = Arc::new(PinnedScan::capture(&catalog, table.id, query.clone()).unwrap());
    fixture.storage.backend().reads.lock().unwrap().clear();
    assert!(
        fixture
            .rows(&zero, zero.cursor(0, 1).unwrap(), 3, false)
            .await
            .is_empty()
    );
    assert!(fixture.storage.backend().reads.lock().unwrap().is_empty());

    for order in [
        json!({"column":"id","desc":true}),
        json!({"column":"score","desc":false}),
    ] {
        query.order_by = Some(serde_json::from_value(order).unwrap());
        assert_eq!(
            PinnedScan::capture(&catalog, table.id, query.clone())
                .unwrap_err()
                .code,
            ErrorCode::UnsupportedOperation
        );
    }
    workers.shutdown().await.unwrap();
}

#[tokio::test]
async fn st01_st05_shards_seek_to_ranges_and_legacy_rows_sort_once() {
    let (fixture, workers) = ScanFixture::new();
    let table = table();
    let rows: Vec<_> = (0..27).map(|id| row(id, id.to_string())).collect();
    let catalog = fixture.partitioned(&table, &rows, 13).await;
    let scan = Arc::new(PinnedScan::capture(&catalog, table.id, all()).unwrap());
    let mut union = Vec::new();
    for shard in 0..2 {
        fixture.storage.backend().reads.lock().unwrap().clear();
        let batches = fixture
            .rows(&scan, scan.cursor(shard, 2).unwrap(), 5, false)
            .await;
        let reads = fixture.storage.backend().reads.lock().unwrap();
        assert_eq!(reads.len(), 1, "one object download per physical shard");
        assert!(reads[0].ends_with(&format!("/{shard}.parquet")));
        union.extend(batches.into_iter().flatten());
    }
    assert_eq!(union, rows);
    fixture.storage.backend().reads.lock().unwrap().clear();
    assert!(
        fixture
            .rows(&scan, scan.cursor(0, 30).unwrap(), 5, false)
            .await
            .is_empty()
    );
    assert!(fixture.storage.backend().reads.lock().unwrap().is_empty());

    let mut unordered = rows[..7].to_vec();
    unordered.reverse();
    let legacy = fixture.legacy(&table, &unordered).await;
    let scan = Arc::new(PinnedScan::capture(&legacy, table.id, all()).unwrap());
    let batches = fixture
        .rows(&scan, scan.cursor(0, 1).unwrap(), 3, false)
        .await;
    assert_eq!(batches.concat(), rows[..7]);
    workers.shutdown().await.unwrap();
}

#[tokio::test]
async fn st04_stream_authorization_checks_only_examined_files() {
    let (fixture, workers) = ScanFixture::new();
    let table = table();
    let rows: Vec<_> = (0..27).map(|id| row(id, id.to_string())).collect();
    let catalog = fixture.partitioned(&table, &rows, 13).await;
    let scan = Arc::new(PinnedScan::capture(&catalog, table.id, all()).unwrap());
    let first = scan.cursor(0, 1).unwrap();
    let mut reader = ScanReader::default();
    let batch = reader
        .next_batch(
            &fixture.storage,
            &fixture.pool,
            scan.clone(),
            first,
            options(BatchFormat::Rows, 13),
        )
        .await
        .unwrap();
    fixture.storage.backend().reads.lock().unwrap().clear();
    scan.authorize_examined(&fixture.storage, &fixture.pool, first, batch.cursor)
        .await
        .unwrap();
    {
        let reads = fixture.storage.backend().reads.lock().unwrap();
        assert_eq!(reads.len(), 1);
        assert!(reads[0].ends_with("/0.parquet"));
    }
    let successor = reader
        .next_batch(
            &fixture.storage,
            &fixture.pool,
            scan.clone(),
            batch.cursor,
            options(BatchFormat::Rows, 2),
        )
        .await
        .unwrap();
    fixture.storage.backend().reads.lock().unwrap().clear();
    scan.authorize_examined(
        &fixture.storage,
        &fixture.pool,
        batch.cursor,
        successor.cursor,
    )
    .await
    .unwrap();
    {
        let reads = fixture.storage.backend().reads.lock().unwrap();
        assert_eq!(reads.len(), 1);
        assert!(reads[0].ends_with("/1.parquet"));
    }
    let eof = reader
        .next_batch(
            &fixture.storage,
            &fixture.pool,
            scan.clone(),
            successor.cursor,
            BatchOptions {
                drop_last: true,
                ..options(BatchFormat::Rows, 32)
            },
        )
        .await
        .unwrap();
    assert!(eof.data.is_none());
    fixture.storage.backend().reads.lock().unwrap().clear();
    scan.authorize_examined(
        &fixture.storage,
        &fixture.pool,
        successor.cursor,
        eof.cursor,
    )
    .await
    .unwrap();
    scan.authorize_examined(&fixture.storage, &fixture.pool, eof.cursor, eof.cursor)
        .await
        .unwrap();
    {
        let reads = fixture.storage.backend().reads.lock().unwrap();
        assert_eq!(reads.len(), 1);
        assert!(reads[0].ends_with("/1.parquet"));
    }
    workers.shutdown().await.unwrap();
}

#[tokio::test]
async fn st01_projection_keeps_partition_integrity_checks() {
    let (fixture, workers) = ScanFixture::new();
    let table = table();
    let mut rows: Vec<_> = (0..27).map(|id| row(id, id.to_string())).collect();
    rows[3].insert("score".into(), Scalar::Float64(f64::NAN));
    let catalog = fixture.partitioned(&table, &rows, 13).await;
    let query = Query {
        select: Some(vec!["id".into()]),
        ..all()
    };
    let scan = Arc::new(PinnedScan::capture(&catalog, table.id, query).unwrap());
    let cursor = scan.cursor(0, 2).unwrap();
    let before = fixture.pool.scheduler().usage();
    let failed = ScanReader::default()
        .next_batch(
            &fixture.storage,
            &fixture.pool,
            scan,
            cursor,
            options(BatchFormat::Rows, 2),
        )
        .await;
    assert_eq!(failed.err().unwrap().code, ErrorCode::StorageUnavailable);
    assert_eq!(fixture.pool.scheduler().usage(), before);
    workers.shutdown().await.unwrap();
}

#[tokio::test]
async fn st02_oversize_or_dropped_tail_returns_charges_and_preserves_cursor() {
    let (fixture, workers) = ScanFixture::new();
    let table = table();
    let rows: Vec<_> = (0..5).map(|id| row(id, "\\é".repeat(2048))).collect();
    let catalog = fixture.legacy(&table, &rows).await;
    let query = Query {
        select: Some(vec!["text".into(), "id".into()]),
        ..all()
    };
    let scan = Arc::new(PinnedScan::capture(&catalog, table.id, query).unwrap());
    for format in [BatchFormat::Rows, BatchFormat::Arrow] {
        let opts = options(format, 3);
        let cursor = scan.cursor(0, 1).unwrap();
        let mut reader = ScanReader::default();
        let usage = fixture.pool.scheduler().usage();
        let rejected = reader
            .next_batch(
                &fixture.storage,
                &fixture.pool,
                scan.clone(),
                cursor,
                BatchOptions {
                    max_batch_bytes: 64,
                    ..opts
                },
            )
            .await;
        assert_eq!(rejected.err().unwrap().code, ErrorCode::LimitExceeded);
        assert_eq!(fixture.pool.scheduler().usage(), usage);

        let first = reader
            .next_batch(&fixture.storage, &fixture.pool, scan.clone(), cursor, opts)
            .await
            .unwrap();
        let first_bytes = first.data.unwrap().bytes().clone();
        match format {
            BatchFormat::Rows => {
                let decoded = decode_rows(&first_bytes);
                assert_eq!(
                    decoded
                        .iter()
                        .map(|row| row["id"].clone())
                        .collect::<Vec<_>>(),
                    [0, 1, 2].map(Scalar::Int64)
                );
            }
            BatchFormat::Arrow => {
                assert!(first_bytes.ends_with(&[255, 255, 255, 255, 0, 0, 0, 0]));
                let mut ipc = StreamReader::try_new(first_bytes.as_ref(), None).unwrap();
                let data = ipc.next().unwrap().unwrap();
                assert_eq!(data.schema().field(0).name(), "text");
                assert_eq!(data.schema().field(1).name(), "id");
                let ids = data
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert_eq!(ids.values().as_ref(), &[0, 1, 2]);
                assert!(ipc.next().is_none());
            }
        }
        let tail = reader
            .next_batch(
                &fixture.storage,
                &fixture.pool,
                scan.clone(),
                first.cursor,
                opts,
            )
            .await
            .unwrap();
        let tail_data = tail.data.unwrap();
        match format {
            BatchFormat::Rows => assert_eq!(decode_rows(tail_data.bytes()).len(), 2),
            BatchFormat::Arrow => assert_eq!(
                StreamReader::try_new(tail_data.bytes().as_ref(), None)
                    .unwrap()
                    .next()
                    .unwrap()
                    .unwrap()
                    .num_rows(),
                2
            ),
        }
        let rejected_tail = reader
            .next_batch(
                &fixture.storage,
                &fixture.pool,
                scan.clone(),
                first.cursor,
                BatchOptions {
                    max_batch_bytes: 64,
                    ..opts
                },
            )
            .await;
        assert_eq!(rejected_tail.err().unwrap().code, ErrorCode::LimitExceeded);
        let dropped_tail = reader
            .next_batch(
                &fixture.storage,
                &fixture.pool,
                scan.clone(),
                first.cursor,
                BatchOptions {
                    drop_last: true,
                    max_batch_bytes: 64,
                    ..opts
                },
            )
            .await
            .unwrap();
        assert!(dropped_tail.data.is_none() && dropped_tail.cursor.exhausted());
        drop(tail_data);
        assert!(fixture.pool.scheduler().usage()[0].2 > usage[0].2);
        drop(first_bytes);
        assert_eq!(fixture.pool.scheduler().usage(), usage);
    }
    workers.shutdown().await.unwrap();
}

#[tokio::test]
async fn st01_st02_u01_local_serve_streams_over_one_connection() {
    use crate::{engine::Engine, executor::Executor, partition_size::PartitionSize, runtime};
    use axum::http::{HeaderMap, Method, Request, StatusCode};
    use http_body_util::{BodyExt, Full};
    use hyper::client::conn::http1;
    use hyper_util::rt::TokioIo;
    use tokio::{
        net::{TcpListener, TcpStream},
        sync::oneshot,
        time::{Duration, timeout},
    };

    async fn call(
        conn: &mut http1::SendRequest<Full<Bytes>>,
        method: Method,
        path: &str,
        value: Option<serde_json::Value>,
    ) -> (StatusCode, HeaderMap, Bytes) {
        let mut request = Request::builder()
            .method(method)
            .uri(path)
            .header("host", "localhost")
            .header("authorization", "Bearer stream-test-token");
        if value.is_some() {
            request = request.header("content-type", "application/json");
        }
        let body = Full::new(value.map_or_else(Bytes::new, |v| Bytes::from(v.to_string())));
        let response = conn
            .send_request(request.body(body).unwrap())
            .await
            .unwrap();
        let (parts, body) = response.into_parts();
        (
            parts.status,
            parts.headers,
            body.collect().await.unwrap().to_bytes(),
        )
    }

    timeout(Duration::from_secs(15), async {
        let (fixture, workers) = ScanFixture::new();
        let table = table();
        let input: Vec<_> = (0..5).rev().map(|i| row(i, format!("id-{i}"))).collect();
        let catalog = fixture.legacy(&table, &input).await;
        fixture
            .storage
            .put(
                &ObjectKey::catalog(),
                catalog.encode().unwrap(),
                WriteCondition::Create,
            )
            .await
            .unwrap();
        let engine = Engine::new(fixture.storage, fixture.pool, PartitionSize::default());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = oneshot::channel();
        let serving = tokio::spawn(runtime::serve_configured(
            listener,
            engine,
            Executor::new(4, 4).unwrap(),
            "stream-test-token".into(),
            None,
            async move {
                stopped.await.unwrap();
                Ok(())
            },
            Default::default(),
            false,
            true,
        ));
        let socket = TcpStream::connect(address).await.unwrap();
        let (mut conn, driver) = http1::handshake(TokioIo::new(socket)).await.unwrap();
        let driver = tokio::spawn(driver);

        let (status, headers, caps) =
            call(&mut conn, Method::GET, "/v1/streams/capabilities", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers["cache-control"], "no-store");
        let caps: serde_json::Value = serde_json::from_slice(&caps).unwrap();
        let (status, _, opened) = call(&mut conn, Method::POST, "/v1/streams", Some(json!({
            "request_id": Uuid::new_v4(), "expected_run_id": caps["run_id"], "table_id": table.id,
            "query": {"select":["id"],"filter":null,"order_by":null,"limit":null},
            "batch_rows": 2, "prefetch_batches": 0, "format": "rows", "drop_last": false,
            "snapshot_id": null, "shard": 0
        }))).await;
        assert_eq!(status, StatusCode::CREATED);
        let opened: serde_json::Value = serde_json::from_slice(&opened).unwrap();
        let path = format!(
            "/v1/streams/{}/{}",
            opened["run_id"].as_str().unwrap(),
            opened["stream_id"].as_str().unwrap()
        );
        for (sequence, ids) in [vec![0, 1], vec![2, 3], vec![4]].iter().enumerate() {
            let (status, _, body) = call(
                &mut conn,
                Method::POST,
                &format!("{path}/batches/{sequence}"),
                None,
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            let rows: Vec<_> = decode_rows(&body)
                .into_iter()
                .map(|row| row["id"].clone())
                .collect();
            assert_eq!(
                rows,
                ids.iter().copied().map(Scalar::Int64).collect::<Vec<_>>()
            );
        }
        let (status, _, body) =
            call(&mut conn, Method::POST, &format!("{path}/batches/3"), None).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(body.is_empty());
        let (status, _, _) = call(&mut conn, Method::DELETE, &path, None).await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        drop(conn);
        driver.await.unwrap().unwrap();
        stop.send(()).unwrap();
        serving.await.unwrap().unwrap();
        workers.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}
