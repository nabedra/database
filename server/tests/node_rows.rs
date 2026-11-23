#![allow(clippy::disallowed_methods)] // integration tests drive real processes and real time; the phase 7 seam covers server/src
use nabedra_database_server::{
    node::{
        job::{JobEnvelope, ResultEnvelope},
        payload::{decode_rows, encode_rows},
        spool::{Spool, SpoolLimits, SpoolOperation, SpoolPayload},
    },
    protocol::{Row, Scalar},
};
use serde_json::json;
struct Scratch {
    parent: std::path::PathBuf,
    spool: Option<Spool>,
}
impl Scratch {
    fn new() -> Self {
        let parent = std::env::temp_dir().join(format!("nabedra-rows-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&parent).unwrap();
        let spool = Spool::new(&parent, SpoolLimits::new(100 * 1024 * 1024, 8).unwrap()).unwrap();
        Self {
            parent,
            spool: Some(spool),
        }
    }
    fn op(&self) -> SpoolOperation {
        self.spool
            .as_ref()
            .unwrap()
            .operation(SpoolLimits::new(100 * 1024 * 1024, 8).unwrap())
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        drop(self.spool.take());
        std::fs::remove_dir(&self.parent).unwrap();
    }
}
fn controls(collect: bool, count: u64, bytes: u64) -> (JobEnvelope, ResultEnvelope) {
    let file = json!({"kind":"legacy","payload":{"object":"tables/00000000-0000-0000-0000-000000000006/00000000-0000-0000-0000-000000000007.parquet","row_count":count.max(1)}});
    let task = if collect {
        json!({"kind":"collect","payload":{"file":file,"query":{"select":["text"],"limit":0,"filter":{"column":"id","op":"ge","value":1},"order_by":{"column":"score","desc":true}}}})
    } else {
        json!({"kind":"get","payload":{"file":file,"key":1}})
    };
    let j = json!({"version":1,"cluster_id":"00000000-0000-0000-0000-000000000001","operation_id":"00000000-0000-0000-0000-000000000002","job_id":"00000000-0000-0000-0000-000000000003","attempt_id":"00000000-0000-0000-0000-000000000004","request_digest":"a".repeat(64),"snapshot":{"catalog_revision":2,"catalog_commit_id":"00000000-0000-0000-0000-000000000005","catalog_etag":"token"},"ordinal":0,"table":{"id":"00000000-0000-0000-0000-000000000006","name":"items","schema":{"id":"int64","score":"float64","text":"utf8","ok":"bool"},"key":"id","format":"parquet"},"task":task});
    let mut r = j.clone();
    r.as_object_mut().unwrap().remove("table");
    r.as_object_mut().unwrap().remove("task");
    r["result"] = if collect {
        json!({"kind":"collect","payload":{"source_rows":count.max(1),"source_payload_bytes":bytes,"matched_rows":count}})
    } else {
        json!({"kind":"get","payload":{"source_rows":1,"source_payload_bytes":bytes,"found":count!=0}})
    };
    (
        serde_json::from_value(j).unwrap(),
        serde_json::from_value(r).unwrap(),
    )
}
fn raw(op: &SpoolOperation, text: &str) -> SpoolPayload {
    let mut w = op.writer(50 * 1024 * 1024).unwrap();
    for c in text.as_bytes().chunks(65536) {
        w.append(c).unwrap();
    }
    w.finish().unwrap()
}
#[test]
fn full_rows_roundtrip_and_empty_results() {
    let s = Scratch::new();
    let op = s.op();
    for collect in [false, true] {
        let (j, r) = controls(collect, 1, 1024);
        let p = raw(
            &op,
            r#"[{"id":1,"score":-0.0,"text":"a\u0000\ud83d\ude00","ok":true}]"#,
        );
        let rows = decode_rows(&p, &r, &j).unwrap();
        assert_eq!(rows.len(), 1);
        assert!(matches!(rows[0]["score"],Scalar::Float64(v) if v.to_bits()==(-0.0f64).to_bits()));
        let out = encode_rows(&op, &rows, &r, &j).unwrap();
        assert_eq!(decode_rows(&out, &r, &j).unwrap(), rows);
        let (j, r) = controls(collect, 0, 1024);
        assert!(decode_rows(&raw(&op, "[]"), &r, &j).unwrap().is_empty());
    }
}
#[test]
fn malformed_and_semantically_invalid_rows_fail() {
    let s = Scratch::new();
    let op = s.op();
    let (j, r) = controls(false, 1, 1024);
    for text in [
        r#"[{"id":1,"id":1,"score":0.0,"text":"","ok":true}]"#,
        r#"[{"id":1,"score":0.0,"text":"","ok":true,"extra":0}]"#,
        r#"[{"id":1}]"#,
        r#"[{"id":1,"score":{},"text":"","ok":true}]"#,
        r#"[{"id":1,"score":0,"text":"","ok":true}]"#,
        r#"[{"id":1.0,"score":0.0,"text":"","ok":true}]"#,
        r#"[{"id":9223372036854775808,"score":0.0,"text":"","ok":true}]"#,
        r#"[{"id":1,"score":1e999,"text":"","ok":true}]"#,
        r#"[{"id":2,"score":0.0,"text":"","ok":true}]"#,
        r#"[{"id":1,"score":[],"text":"","ok":true}]"#,
        r#"[{"id":1,"score":{"$serde_json::private::Number":"0.0"},"text":"","ok":true}]"#,
        r#"[{"id":1,"score":1e+,"text":"","ok":true}]"#,
        r#"[{"id":1,"score":0.0,"text":"\ud800","ok":true}]"#,
        r#"[{"id":1,"score":0.0,"text":"","ok":1}]"#,
        r#"[{"id":1,"score":0.0,"text":"","ok":true}] null"#,
        r#"[{"id":1,"score":0.0,"text":"","ok":true}]x"#,
        "[] null",
        "[",
        "[null]",
        r#"[{"id":1,"score":null,"text":"","ok":true}]"#,
    ] {
        let error = decode_rows(&raw(&op, text), &r, &j).unwrap_err();
        assert_eq!(error.message, "invalid row payload");
    }
    let row: Row = serde_json::from_str(r#"{"id":1,"score":0.0,"text":"","ok":true}"#).unwrap();
    let (j, r) = controls(false, 1, 8);
    assert!(encode_rows(&op, &[row], &r, &j).is_err());
}
#[test]
fn numeric_token_bound_and_large_escaped_get() {
    let s = Scratch::new();
    let op = s.op();
    let (j, r) = controls(false, 1, 4 * 1024 * 1024);
    for (n, ok) in [(128, true), (129, false)] {
        let token = format!("0.{}", "0".repeat(n - 2));
        let text = format!(r#"[{{"id":1,"score":{token},"text":"","ok":true}}]"#);
        assert_eq!(decode_rows(&raw(&op, &text), &r, &j).is_ok(), ok);
    }
    let rows = vec![
        [
            ("id".into(), Scalar::Int64(1)),
            ("score".into(), Scalar::Float64(0.0)),
            ("text".into(), Scalar::Utf8("\0".repeat(3 * 1024 * 1024))),
            ("ok".into(), Scalar::Bool(true)),
        ]
        .into_iter()
        .collect(),
    ];
    let p = encode_rows(&op, &rows, &r, &j).unwrap();
    assert!(p.len() > 16 * 1024 * 1024);
    assert_eq!(decode_rows(&p, &r, &j).unwrap(), rows);
}

#[test]
fn collect_checks_local_order_filter_keys_counts_and_constructed_controls() {
    let s = Scratch::new();
    let op = s.op();
    let (j, r) = controls(true, 3, 1024);
    let text = r#"[{"id":2,"score":5.0,"text":"first","ok":true},{"id":1,"score":0.0,"text":"second","ok":false},{"id":3,"score":-0.0,"text":"third","ok":true}]"#;
    let rows = decode_rows(&raw(&op, text), &r, &j).unwrap();
    assert_eq!(rows.len(), 3); // select and limit=0 are global reduction only.
    assert_eq!(rows[0].len(), 4);
    for bad in [
        vec![rows[1].clone(), rows[0].clone(), rows[2].clone()],
        vec![rows[0].clone(), rows[2].clone(), rows[1].clone()],
        vec![rows[0].clone(), rows[1].clone(), rows[1].clone()],
        rows[..2].to_vec(),
    ] {
        assert!(encode_rows(&op, &bad, &r, &j).is_err());
        let encoded = serde_json::to_string(&bad).unwrap();
        assert!(decode_rows(&raw(&op, &encoded), &r, &j).is_err());
    }
    let mut bad = rows.clone();
    bad[2].insert("id".into(), Scalar::Int64(0));
    assert!(decode_rows(&raw(&op, &serde_json::to_string(&bad).unwrap()), &r, &j).is_err());
    let mut bad_result = r.clone();
    bad_result.attempt_id = uuid::Uuid::new_v4();
    assert!(decode_rows(&raw(&op, text), &bad_result, &j).is_err());
    let mut bad_job = j.clone();
    bad_job.table.key = "missing".into();
    assert!(decode_rows(&raw(&op, text), &r, &bad_job).is_err());
    let mut bad_result = r.clone();
    if let nabedra_database_server::node::job::JobResult::Collect { source_rows, .. } =
        &mut bad_result.result
    {
        *source_rows = 2;
    }
    assert!(decode_rows(&raw(&op, text), &bad_result, &j).is_err());
    // Exact logical accounting is 8 + four cell names/headers + values.
    let exact = 8 + (8 + 2 + 8) + (8 + 5 + 8) + (8 + 4) + (8 + 2 + 1);
    let (get, mut result) = controls(false, 1, exact);
    let text = r#"[{"ok":true,"text":"","score":-0.0,"\u0069d":1}]"#;
    assert!(decode_rows(&raw(&op, text), &result, &get).is_ok());
    if let nabedra_database_server::node::job::JobResult::Get {
        source_payload_bytes,
        ..
    } = &mut result.result
    {
        *source_payload_bytes -= 1;
    }
    assert!(decode_rows(&raw(&op, text), &result, &get).is_err());
}

#[test]
fn empty_source_scalar_edges_and_scratch_capacity() {
    use nabedra_database_server::node::job::{FileInput, JobResult, Task};
    let scratch = Scratch::new();
    let op = scratch.op();
    let (mut job, mut result) = controls(true, 0, 0);
    if let Task::Collect {
        file: FileInput::Legacy { row_count, .. },
        ..
    } = &mut job.task
    {
        *row_count = 0;
    }
    if let JobResult::Collect { source_rows, .. } = &mut result.result {
        *source_rows = 0;
    }
    assert!(
        decode_rows(&raw(&op, "[]"), &result, &job)
            .unwrap()
            .is_empty()
    );
    let (mut job, result) = controls(true, 2, 1024);
    if let Task::Collect { query, .. } = &mut job.task {
        query.filter = None;
        query.order_by = None;
    }
    let rows = decode_rows(&raw(&op, r#"[{"id":-9223372036854775808,"score":-0e0,"text":"","ok":false},{"id":9223372036854775807,"score":1.0e10,"text":"","ok":true}]"#), &result, &job).unwrap();
    assert!(matches!(rows[0]["score"], Scalar::Float64(v) if v.to_bits() == (-0.0f64).to_bits()));
    let tiny = scratch
        .spool
        .as_ref()
        .unwrap()
        .operation(SpoolLimits::new(8, 1).unwrap());
    assert!(encode_rows(&tiny, &rows, &result, &job).is_err());
    let mut recovered = tiny.writer(8).unwrap();
    recovered.append(b"12345678").unwrap();
    drop(recovered.finish().unwrap());
    // Matching Count envelopes are valid control records but outside this API.
    if let Task::Collect { file, query } = job.task.clone() {
        job.task = Task::Count { file, query };
    }
    let mut count_result = result.clone();
    if let JobResult::Collect {
        source_rows,
        source_payload_bytes,
        matched_rows,
    } = result.result
    {
        count_result.result = JobResult::Count {
            source_rows,
            source_payload_bytes,
            matched_rows,
        };
    }
    assert!(decode_rows(&raw(&op, "[]"), &count_result, &job).is_err());
}
