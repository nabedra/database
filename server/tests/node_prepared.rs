use meshdb_server::{
    node::{
        job::{JobEnvelope, PartMeta, ResultEnvelope},
        payload::{PreparedReceiver, PreparedResult, ValidatedPart},
        spool::{Spool, SpoolLimits},
    },
    protocol::{Key, Row},
    snapshot,
    storage::KeyRange,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::io::Read;

fn controls(count: Option<u32>) -> (JobEnvelope, ResultEnvelope) {
    let j = json!({"version":1,"cluster_id":"00000000-0000-0000-0000-000000000001","operation_id":"00000000-0000-0000-0000-000000000002","job_id":"00000000-0000-0000-0000-000000000003","attempt_id":"00000000-0000-0000-0000-000000000004","request_digest":"a".repeat(64),"snapshot":{"catalog_revision":2,"catalog_commit_id":"00000000-0000-0000-0000-000000000005","catalog_etag":"token"},"ordinal":0,"table":{"id":"00000000-0000-0000-0000-000000000006","name":"items","schema":{"id":"int64"},"key":"id","format":"parquet"},"task":{"kind":"prepare","payload":{"file":null,"range":{"lower":null,"upper":null},"mutation":{"kind":"delete","payload":{"table_id":"00000000-0000-0000-0000-000000000006","key":1}},"target_bytes":1024}}});
    let mut r = j.clone();
    r.as_object_mut().unwrap().remove("table");
    r.as_object_mut().unwrap().remove("task");
    r["result"] = json!({"kind":"prepared","payload":{"effect":{"kind":"found","payload":{"value":true}},"replacement_count":count}});
    (
        serde_json::from_value(j).unwrap(),
        serde_json::from_value(r).unwrap(),
    )
}
struct Scratch {
    parent: std::path::PathBuf,
    spool: Option<Spool>,
}
impl Scratch {
    fn new() -> Self {
        let parent = std::env::temp_dir().join(format!("meshdb-prepared-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&parent).unwrap();
        Self {
            spool: Some(
                Spool::new(&parent, SpoolLimits::new(100 * 1024 * 1024, 8).unwrap()).unwrap(),
            ),
            parent,
        }
    }
    fn op(
        &self,
        bytes: u64,
        files: u64,
    ) -> std::sync::Arc<meshdb_server::node::spool::SpoolOperation> {
        std::sync::Arc::new(
            self.spool
                .as_ref()
                .unwrap()
                .operation(SpoolLimits::new(bytes, files).unwrap()),
        )
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        drop(self.spool.take());
        std::fs::remove_dir(&self.parent).unwrap();
    }
}
fn part(j: &JobEnvelope, ids: &[i64], range: KeyRange) -> (PartMeta, Vec<u8>) {
    let rows: Vec<Row> = ids
        .iter()
        .map(|id| serde_json::from_value(json!({"id":id})).unwrap())
        .collect();
    let bytes = snapshot::encode(&j.table, &rows).unwrap();
    (
        PartMeta {
            range,
            row_count: ids.len() as u64,
            payload_bytes: snapshot::validate_rows(&j.table, &rows).unwrap() as u64,
            byte_size: bytes.len() as u64,
            content_digest: hash(&bytes),
        },
        bytes,
    )
}
fn hash(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
fn whole() -> KeyRange {
    KeyRange {
        lower: None,
        upper: None,
    }
}
fn feed(p: &mut PreparedReceiver, meta: PartMeta, bytes: &[u8]) {
    p.begin_part(meta).unwrap();
    for chunk in bytes.chunks(37) {
        p.append(chunk).unwrap();
    }
    p.end_part().unwrap();
}
#[test]
fn unchanged_empty_and_verified_files_preserve_controls_and_ownership() {
    fn send<T: Send + 'static>() {}
    send::<PreparedReceiver>();
    send::<PreparedResult>();
    send::<ValidatedPart>();
    let s = Scratch::new();
    let op = s.op(100 * 1024 * 1024, 8);
    for count in [None, Some(0), Some(1), Some(2)] {
        let (j, r) = controls(count);
        let mut p = PreparedReceiver::new(op.clone(), &r, &j).unwrap();
        for i in 0..count.unwrap_or(0) {
            let range = if count == Some(2) {
                if i == 0 {
                    KeyRange {
                        lower: None,
                        upper: Some(Key::Int64(2)),
                    }
                } else {
                    KeyRange {
                        lower: Some(Key::Int64(2)),
                        upper: None,
                    }
                }
            } else {
                whole()
            };
            let (meta, bytes) = part(&j, &[i as i64 + 1], range);
            feed(&mut p, meta, &bytes);
        }
        let result = p.finish().unwrap();
        assert_eq!(result.envelope(), &r);
        let (envelope, parts) = result.into_parts();
        assert_eq!(envelope, r);
        assert_eq!(parts.as_ref().map(Vec::len), count.map(|n| n as usize));
        for p in parts.into_iter().flatten() {
            let expected = p.metadata().clone();
            let (meta, mut reader) = p.into_reader().unwrap();
            assert_eq!(meta, expected);
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).unwrap();
            assert_eq!(bytes.len() as u64, meta.byte_size);
        }
    }
}
#[test]
fn independent_file_validation_failures_poison_receiver() {
    let s = Scratch::new();
    let op = s.op(100 * 1024 * 1024, 8);
    let (j, r) = controls(Some(1));
    for case in 0..7 {
        let (mut meta, mut bytes) = part(&j, if case == 6 { &[2, 1] } else { &[1, 2] }, whole());
        match case {
            0 => meta.byte_size += 1,
            1 => meta.content_digest = "0".repeat(64),
            2 => {
                let mut other = j.table.clone();
                other
                    .schema
                    .insert("other".into(), meshdb_server::protocol::ColumnType::Int64);
                let rows = vec![serde_json::from_value(json!({"id":1,"other":2})).unwrap()];
                bytes = snapshot::encode(&other, &rows).unwrap();
                meta.row_count = 1;
                meta.payload_bytes = part(&j, &[1], whole()).0.payload_bytes;
                meta.byte_size = bytes.len() as u64;
                meta.content_digest = hash(&bytes);
            }
            3 => meta.row_count = 3,
            4 => meta.payload_bytes += 1,
            5 => {}
            _ => {}
        }
        let mut job = j.clone();
        if case == 5 {
            meta.range.lower = Some(Key::Int64(2));
            if let meshdb_server::node::job::Task::Prepare { range, .. } = &mut job.task {
                *range = meta.range.clone();
            }
        }
        let mut p = PreparedReceiver::new(op.clone(), &r, &job).unwrap();
        p.begin_part(meta).unwrap();
        p.append(&bytes).unwrap();
        assert!(p.end_part().is_err(), "case {case}");
        assert!(p.finish().is_err());
    }
}
#[test]
fn transitions_counts_caps_and_quota_fail_closed_and_cleanup() {
    let s = Scratch::new();
    let op = s.op(100 * 1024 * 1024, 8);
    let (j, r) = controls(Some(1));
    let (meta, bytes) = part(&j, &[1], whole());
    for case in 0..9 {
        let mut p = PreparedReceiver::new(op.clone(), &r, &j).unwrap();
        match case {
            0 => assert!(p.append(b"x").is_err()),
            1 => assert!(p.end_part().is_err()),
            2 => {
                p.begin_part(meta.clone()).unwrap();
                assert!(p.begin_part(meta.clone()).is_err());
            }
            3 => {
                p.begin_part(meta.clone()).unwrap();
                assert!(p.append(&[]).is_err());
            }
            4 => {
                p.begin_part(meta.clone()).unwrap();
                assert!(p.append(&vec![0; 65537]).is_err());
            }
            5 => {
                let mut m = meta.clone();
                m.byte_size = 1;
                p.begin_part(m).unwrap();
                assert!(p.append(&bytes).is_err());
            }
            6 => {
                let mut m = meta.clone();
                m.range.lower = Some(Key::Int64(0));
                assert!(p.begin_part(m).is_err());
            }
            7 => {
                feed(&mut p, meta.clone(), &bytes);
                assert!(p.begin_part(meta.clone()).is_err());
            }
            8 => {
                p.begin_part(meta.clone()).unwrap();
                p.append(&bytes[..5]).unwrap();
            }
            _ => unreachable!(),
        }
        if case != 8 {
            assert!(p.append(b"x").is_err());
            assert!(p.end_part().is_err());
            assert!(p.begin_part(meta.clone()).is_err());
        }
        assert!(p.finish().is_err());
    }
    assert!(
        PreparedReceiver::new(op.clone(), &r, &j)
            .unwrap()
            .finish()
            .is_err()
    );
    let tiny = s.op(1, 1);
    let mut p = PreparedReceiver::new(tiny.clone(), &r, &j).unwrap();
    p.begin_part(meta.clone()).unwrap();
    assert!(p.append(&bytes).is_err());
    assert!(p.finish().is_err());
    let mut w = tiny.writer(1).unwrap();
    w.append(b"x").unwrap();
    drop(w);
    let slots = s.op(100000, 1);
    let mut p = PreparedReceiver::new(slots.clone(), &r, &j).unwrap();
    feed(&mut p, meta.clone(), &bytes);
    assert!(slots.writer(1).is_err());
    let result = p.finish().unwrap();
    let (_, parts) = result.into_parts();
    let (_, reader) = parts.unwrap().pop().unwrap().into_reader().unwrap();
    assert!(slots.writer(1).is_err());
    drop(reader);
    drop(slots.writer(1).unwrap());
    let mut bad = r.clone();
    bad.ordinal += 1;
    assert!(PreparedReceiver::new(op.clone(), &bad, &j).is_err());
}

#[test]
fn construction_metadata_admission_and_abandoned_attempts() {
    use meshdb_server::node::job::{JobResult, PreparationValidator};
    fn send<T: Send + 'static>() {}
    send::<PreparationValidator>();
    let s = Scratch::new();
    let op = s.op(100000, 1);
    let (j, r) = controls(Some(1));
    let (meta, bytes) = part(&j, &[1], whole());
    // Validator and receiver outlive the original job/result stack values.
    let (validator, receiver) = {
        let (job, result) = controls(None);
        (
            PreparationValidator::new(&result, &job).unwrap(),
            PreparedReceiver::new(op.clone(), &result, &job).unwrap(),
        )
    };
    validator.finish().unwrap();
    assert_eq!(format!("{receiver:?}"), "PreparedReceiver");
    drop(receiver.finish().unwrap());
    for count in [None, Some(0), Some(110001)] {
        let (_, result) = controls(count);
        if count == Some(110001) {
            assert!(PreparedReceiver::new(op.clone(), &result, &j).is_err());
        } else {
            let mut p = PreparedReceiver::new(op.clone(), &result, &j).unwrap();
            assert!(p.begin_part(meta.clone()).is_err());
            assert!(p.finish().is_err());
        }
    }
    let mut wrong = r.clone();
    wrong.result = JobResult::Error {
        code: meshdb_server::protocol::ErrorCode::ValidationError,
        message: "safe".into(),
    };
    assert!(PreparedReceiver::new(op.clone(), &wrong, &j).is_err());
    for case in 0..3 {
        let mut p = PreparedReceiver::new(op.clone(), &r, &j).unwrap();
        let mut bad = meta.clone();
        match case {
            0 => bad.byte_size = 67108865,
            1 => bad.content_digest = "A".repeat(64),
            _ => bad.row_count = 100001,
        };
        let error = p.begin_part(bad).unwrap_err();
        assert_eq!(error.message, "invalid prepared payload");
        // Rejected metadata never reserves a file slot, even while poisoned state survives.
        drop(op.writer(1).unwrap());
        assert!(p.finish().is_err());
    }
    {
        let mut p = PreparedReceiver::new(op.clone(), &r, &j).unwrap();
        p.begin_part(meta.clone()).unwrap();
        p.append(&bytes[..5]).unwrap();
        assert!(op.writer(1).is_err());
    }
    drop(op.writer(1).unwrap());
    let mut p = PreparedReceiver::new(op.clone(), &r, &j).unwrap();
    feed(&mut p, meta, &bytes);
    let result = p.finish().unwrap();
    assert_eq!(format!("{result:?}"), "PreparedResult");
    let (_, parts) = result.into_parts();
    assert_eq!(format!("{:?}", parts.as_ref().unwrap()[0]), "ValidatedPart");
    drop(parts);
    drop(op.writer(1).unwrap());
    let (_, two) = controls(Some(2));
    let mut p = PreparedReceiver::new(op.clone(), &two, &j).unwrap();
    let (meta, bytes) = part(
        &j,
        &[1],
        KeyRange {
            lower: None,
            upper: Some(Key::Int64(2)),
        },
    );
    feed(&mut p, meta, &bytes);
    assert!(p.finish().is_err());
    drop(op.writer(1).unwrap());
}
