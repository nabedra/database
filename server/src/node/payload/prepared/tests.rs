use super::*;
use crate::{
    node::{
        job::Effect,
        spool::{Spool, SpoolLimits},
    },
    protocol::{Key, Row},
    storage::KeyRange,
};
use serde_json::json;
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
        let parent =
            std::env::temp_dir().join(format!("nabedra-prepared-{}", uuid::Uuid::new_v4()));
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
    ) -> std::sync::Arc<nabedra_database_server::node::spool::SpoolOperation> {
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

fn encoded(
    j: &JobEnvelope,
    id: i64,
    lower: Option<i64>,
    upper: Option<i64>,
) -> snapshot::EncodedPartition {
    let rows: Vec<Row> = vec![serde_json::from_value(json!({"id":id})).unwrap()];
    snapshot::EncodedPartition {
        range: KeyRange {
            lower: lower.map(Key::Int64),
            upper: upper.map(Key::Int64),
        },
        row_count: 1,
        payload_bytes: snapshot::validate_rows(&j.table, &rows).unwrap() as u64,
        bytes: snapshot::encode(&j.table, &rows).unwrap(),
    }
}
#[test]
fn local_null_empty_and_multiple_parts_handoff() {
    let s = Scratch::new();
    let op = s.op(100000, 2);
    let (j, _) = controls(None);
    for changed in [false, true] {
        let output = LocalPrepared::new(op.clone(), &j, Effect::Found { value: true }, changed)
            .unwrap()
            .finish()
            .unwrap();
        let (envelope, parts) = output.into_parts();
        assert_eq!(parts.as_ref().map(Vec::len), changed.then_some(0));
        assert_eq!(envelope, controls(changed.then_some(0)).1);
    }
    let mut local =
        LocalPrepared::new(op.clone(), &j, Effect::Found { value: true }, true).unwrap();
    local.push(encoded(&j, 1, None, Some(2))).unwrap();
    local.push(encoded(&j, 2, Some(2), None)).unwrap();
    let (envelope, parts) = local.finish().unwrap().into_parts();
    assert_eq!(envelope, controls(Some(2)).1);
    assert!(op.writer(1).is_err());
    for part in parts.unwrap() {
        let (meta, mut reader) = part.into_reader().unwrap();
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).unwrap();
        assert_eq!(
            snapshot::decode(&j.table, Bytes::from(bytes), Some(meta.row_count))
                .unwrap()
                .len(),
            1
        );
    }
    drop(op.writer(1).unwrap());
}
#[test]
fn local_failures_poison_and_retain_prior_parts() {
    let s = Scratch::new();
    let op = s.op(100000, 1);
    let (j, _) = controls(None);
    let mut local =
        LocalPrepared::new(op.clone(), &j, Effect::Found { value: true }, true).unwrap();
    local.push(encoded(&j, 1, None, Some(2))).unwrap();
    assert!(local.push(encoded(&j, 2, Some(2), None)).is_err());
    assert!(op.writer(1).is_err());
    assert!(local.finish().is_err());
    drop(op.writer(1).unwrap());
    let mut local =
        LocalPrepared::new(op.clone(), &j, Effect::Found { value: true }, false).unwrap();
    assert!(local.push(encoded(&j, 1, None, None)).is_err());
    assert!(local.finish().is_err());
    assert!(LocalPrepared::new(op.clone(), &j, Effect::Null {}, true).is_err());
    let mut local =
        LocalPrepared::new(op.clone(), &j, Effect::Found { value: true }, true).unwrap();
    local.push(encoded(&j, 2, Some(2), None)).unwrap();
    assert!(local.finish().is_err());
    drop(op.writer(1).unwrap());
}

#[test]
fn closed_file_digest_mismatch_fails_even_with_valid_parquet() {
    let s = Scratch::new();
    let op = s.op(100000, 1);
    let (j, _) = controls(None);
    let part = encoded(&j, 1, None, None);
    let meta = PartMeta {
        range: part.range,
        row_count: part.row_count,
        payload_bytes: part.payload_bytes,
        byte_size: part.bytes.len() as u64,
        content_digest: "0".repeat(64),
    };
    meta.validate(&j.table).unwrap();
    // The actual file is independently valid for every semantic field.
    snapshot::decode(&j.table, Bytes::from(part.bytes.clone()), Some(1)).unwrap();
    let mut writer = op.writer(meta.byte_size).unwrap();
    writer.append(&part.bytes).unwrap();
    let payload = writer.finish().unwrap();
    assert!(verify_closed_file(&j.table, &meta, &payload).is_err());
    drop(payload);
    drop(op.writer(1).unwrap());
}

#[test]
fn emitted_quota_failure_and_bad_local_metadata_cannot_finish() {
    let s = Scratch::new();
    let (j, _) = controls(None);
    let first_len = encoded(&j, 1, None, Some(2)).bytes.len() as u64;
    let op = s.op(first_len + 1, 2);
    let mut local =
        LocalPrepared::new(op.clone(), &j, Effect::Found { value: true }, true).unwrap();
    let rows: Vec<Row> = [1, 2]
        .into_iter()
        .map(|id| serde_json::from_value(json!({"id":id})).unwrap())
        .collect();
    let mut calls = 0;
    let error = snapshot::encode_partition_range_each(
        &j.table,
        rows,
        KeyRange {
            lower: None,
            upper: None,
        },
        "1B".parse().unwrap(),
        |part| {
            calls += 1;
            local.push(part).map_err(|e| {
                crate::protocol::ProtocolError::new(
                    e.code,
                    "local preparation failed",
                    crate::protocol::Outcome::NotApplied,
                )
            })
        },
    )
    .unwrap_err();
    assert_eq!(error.code, ErrorCode::ValidationError);
    assert_eq!(calls, 2);
    // The failed second writer is gone, but the first file's bytes stay charged.
    let mut probe = op.writer(2).unwrap();
    assert!(probe.append(b"xx").is_err());
    drop(probe);
    assert!(local.finish().is_err());
    let mut probe = op.writer(first_len + 1).unwrap();
    probe.append(&vec![0; first_len as usize + 1]).unwrap();
    drop(probe);

    let mut local =
        LocalPrepared::new(op.clone(), &j, Effect::Found { value: true }, true).unwrap();
    let mut bad = encoded(&j, 1, None, None);
    bad.payload_bytes += 1; // Valid metadata bound, wrong actual decoded accounting.
    assert!(local.push(bad).is_err());
    assert!(local.finish().is_err());
    drop(op.writer(1).unwrap());
}
