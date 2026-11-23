use super::*;
use crate::node::frame::FrameReader;
use crate::node::job::{JobEnvelope, ResultEnvelope};
use crate::node::payload_owner::{PayloadLease, PayloadOwners, Role};
use crate::node::spool::SpoolLimits;
use crate::worker_pool::WorkerPool;
use std::{path::PathBuf, sync::Arc};
use tokio::io::AsyncWriteExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
pub(super) fn limits() -> SpoolLimits {
    SpoolLimits::new(4096, 2).unwrap()
}
pub(super) fn fixture() -> (
    PayloadOwners,
    crate::worker_pool::WorkerThreads,
    std::path::PathBuf,
    Arc<tokio::sync::OwnedSemaphorePermit>,
) {
    fixture_workers(1)
}
pub(super) fn fixture_workers(
    workers: usize,
) -> (
    PayloadOwners,
    crate::worker_pool::WorkerThreads,
    std::path::PathBuf,
    Arc<tokio::sync::OwnedSemaphorePermit>,
) {
    let (pool, threads) = WorkerPool::start(
        std::num::NonZeroUsize::new(workers).unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let parent = std::env::temp_dir().join(format!("nabedra-owner-{}", nabedra_rt::ids::uuid_v4()));
    std::fs::create_dir(&parent).unwrap();
    let guard = Arc::new(Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap());
    (
        PayloadOwners::new(
            pool,
            Arc::new(Semaphore::new(1)),
            Arc::new(Semaphore::new(1)),
            Arc::new(Semaphore::new(1)),
        ),
        threads,
        parent,
        guard,
    )
}

pub(super) fn controls() -> (Box<JobEnvelope>, Box<ResultEnvelope>) {
    use serde_json::json;
    let j = json!({"version":1,"cluster_id":"00000000-0000-0000-0000-000000000001","operation_id":"00000000-0000-0000-0000-000000000002","job_id":"00000000-0000-0000-0000-000000000003","attempt_id":"00000000-0000-0000-0000-000000000004","request_digest":"a".repeat(64),"snapshot":{"catalog_revision":2,"catalog_commit_id":"00000000-0000-0000-0000-000000000005","catalog_etag":"token"},"ordinal":0,"table":{"id":"00000000-0000-0000-0000-000000000006","name":"items","schema":{"id":"int64"},"key":"id","format":"parquet"},"task":{"kind":"get","payload":{"file":{"kind":"legacy","payload":{"object":"tables/00000000-0000-0000-0000-000000000006/00000000-0000-0000-0000-000000000007.parquet","row_count":1}},"key":1}}});
    let mut r = j.clone();
    r.as_object_mut().unwrap().remove("table");
    r.as_object_mut().unwrap().remove("task");
    r["result"] =
        json!({"kind":"get","payload":{"source_rows":1,"source_payload_bytes":32,"found":false}});
    (
        Box::new(serde_json::from_value(j).unwrap()),
        Box::new(serde_json::from_value(r).unwrap()),
    )
}
pub(super) async fn anchors(
    owners: &PayloadOwners,
    parent: PathBuf,
    guard: Arc<OwnedSemaphorePermit>,
    quota: SpoolLimits,
) -> (PayloadLease, PayloadLease) {
    let mut process = owners.try_process(parent, limits()).unwrap();
    process.ready().await.unwrap();
    let mut op = owners.try_operation(&process, quota, guard).unwrap();
    op.ready().await.unwrap();
    (process, op)
}
pub(super) async fn finish(
    owners: PayloadOwners,
    threads: crate::worker_pool::WorkerThreads,
    parent: PathBuf,
) {
    owners.close_and_drain().await.unwrap();
    threads.shutdown().await.unwrap();
    assert_eq!(std::fs::read_dir(&parent).unwrap().count(), 0);
    std::fs::remove_dir(parent).unwrap();
}

fn frame(bytes: &mut Vec<u8>, kind: u8, payload: &[u8]) {
    bytes.push(kind);
    bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    bytes.extend_from_slice(payload);
}
#[tokio::test]
async fn rows_require_payload_and_real_eof() {
    let (owners, threads, parent, guard) = fixture();
    let (process, op) = anchors(&owners, parent.clone(), guard.clone(), limits()).await;
    let mut outcomes = Vec::new();
    for payload in [true, false] {
        let (job, result) = controls();
        let mut bytes = Vec::new();
        frame(&mut bytes, 1, &serde_json::to_vec(&result).unwrap());
        if payload {
            frame(&mut bytes, 2, b"[]");
        }
        frame(&mut bytes, 4, b"");
        frame(&mut bytes, 5, b"");
        let (mut writer, reader) = tokio::io::duplex(4096);
        writer.write_all(&bytes).await.unwrap();
        writer.shutdown().await.unwrap();
        let lease = owners
            .try_admit(&op, Role::Operation, guard.clone())
            .unwrap();
        match receive_result(
            FrameReader::new(Box::new(reader) as Box<dyn tokio::io::AsyncRead + Unpin + Send>),
            lease,
            job,
        )
        .await
        {
            Ok(accepted) => {
                outcomes.push(true);
                accepted.lease.dispose().await.unwrap();
            }
            Err(_) => {
                outcomes.push(false);
            }
        }
    }
    op.dispose().await.unwrap();
    process.dispose().await.unwrap();
    finish(owners, threads, parent).await;
    assert_eq!(outcomes, [true, false]);
}

pub(super) fn reader(reader: impl tokio::io::AsyncRead + Unpin + Send + 'static) -> Reader {
    FrameReader::new(Box::new(reader))
}
async fn exchange(
    owners: &PayloadOwners,
    op: &PayloadLease,
    guard: &Arc<OwnedSemaphorePermit>,
    bytes: Vec<u8>,
    job: Option<Box<JobEnvelope>>,
) -> bool {
    let (mut writer, transport) = tokio::io::duplex(16384);
    writer.write_all(&bytes).await.unwrap();
    writer.shutdown().await.unwrap();
    let lease = owners
        .try_admit(op, Role::Operation, guard.clone())
        .unwrap();
    if let Some(job) = job {
        match receive_result(reader(transport), lease, job).await {
            Ok(accepted) => {
                let _ = &accepted.envelope;
                accepted.lease.dispose().await.unwrap();
                true
            }
            Err(_) => false,
        }
    } else {
        match receive_request(reader(transport), lease).await {
            Ok(accepted) => {
                accepted.envelope.validate().unwrap();
                accepted.lease.dispose().await.unwrap();
                true
            }
            Err(_) => false,
        }
    }
}
pub(super) fn prepared(count: Option<u32>) -> (Box<JobEnvelope>, Box<ResultEnvelope>) {
    let (job, _) = controls();
    let mut j = serde_json::to_value(job).unwrap();
    j["task"] = serde_json::json!({"kind":"prepare","payload":{"file":null,"range":{"lower":null,"upper":null},"mutation":{"kind":"delete","payload":{"table_id":j["table"]["id"],"key":1}},"target_bytes":1024}});
    let mut r = j.clone();
    r.as_object_mut().unwrap().remove("table");
    r.as_object_mut().unwrap().remove("task");
    r["result"] = serde_json::json!({"kind":"prepared","payload":{"effect":{"kind":"found","payload":{"value":true}},"replacement_count":count}});
    (
        Box::new(serde_json::from_value(j).unwrap()),
        Box::new(serde_json::from_value(r).unwrap()),
    )
}
#[tokio::test]
async fn grouped_requests_prepared_and_grammar() {
    use crate::{node::job::PartMeta, protocol::Row, snapshot, storage::KeyRange};
    use sha2::{Digest, Sha256};
    let (owners, threads, parent, guard) = fixture();
    let (process, op) = anchors(&owners, parent.clone(), guard.clone(), limits()).await;
    let mut outcomes = Vec::new();
    let mut expected = Vec::new();
    let (job, result) = controls();
    let mut request = Vec::new();
    frame(&mut request, 1, &serde_json::to_vec(&job).unwrap());
    frame(&mut request, 5, b"");
    outcomes.push(exchange(&owners, &op, &guard, request, None).await);
    expected.push(true);
    // A semantic row payload followed by any ordinary frame must fail.
    for extra in [None, Some(1), Some(2), Some(3), Some(4)] {
        let mut bytes = Vec::new();
        frame(&mut bytes, 1, &serde_json::to_vec(&result).unwrap());
        frame(&mut bytes, 2, b"[]");
        frame(&mut bytes, 4, b"");
        if let Some(kind) = extra {
            frame(&mut bytes, kind, if kind == 4 { b"" } else { b"{}" });
        }
        frame(&mut bytes, 5, b"");
        outcomes.push(exchange(&owners, &op, &guard, bytes, Some(job.clone())).await);
        expected.push(extra.is_none());
    }
    for trailing in [false, true] {
        let mut bytes = Vec::new();
        frame(&mut bytes, 1, &serde_json::to_vec(&result).unwrap());
        if trailing {
            frame(&mut bytes, 2, b"[]");
            frame(&mut bytes, 4, b"");
            frame(&mut bytes, 5, b"");
            bytes.push(9);
        } else {
            bytes.extend_from_slice(&[2, 0, 0, 0, 5, b'[']);
        }
        outcomes.push(exchange(&owners, &op, &guard, bytes, Some(job.clone())).await);
        expected.push(false);
    }
    for kind in ["count", "collect", "error"] {
        let mut j = serde_json::to_value(&job).unwrap();
        if kind != "error" {
            j["task"] = serde_json::json!({"kind":kind,"payload":{"file":j["task"]["payload"]["file"],"query":{"select":null,"filter":null,"order_by":null,"limit":null}}});
        }
        let mut r = serde_json::to_value(&result).unwrap();
        r["result"] = if kind == "error" {
            serde_json::json!({"kind":"error","payload":{"code":"storage_unavailable","message":"unavailable"}})
        } else {
            serde_json::json!({"kind":kind,"payload":{"source_rows":1,"source_payload_bytes":32,"matched_rows":0}})
        };
        let mut bytes = Vec::new();
        frame(&mut bytes, 1, &serde_json::to_vec(&r).unwrap());
        if kind == "collect" {
            frame(&mut bytes, 2, b"[]");
            frame(&mut bytes, 4, b"");
        }
        frame(&mut bytes, 5, b"");
        outcomes.push(
            exchange(
                &owners,
                &op,
                &guard,
                bytes,
                Some(Box::new(serde_json::from_value(j).unwrap())),
            )
            .await,
        );
        expected.push(true);
    }
    for count in [None, Some(0), Some(1)] {
        let (j, r) = prepared(count);
        let rows: Vec<Row> = vec![serde_json::from_value(serde_json::json!({"id":1})).unwrap()];
        let parquet = snapshot::encode(&j.table, &rows).unwrap();
        let meta = PartMeta {
            range: KeyRange {
                lower: None,
                upper: None,
            },
            row_count: 1,
            payload_bytes: snapshot::validate_rows(&j.table, &rows).unwrap() as u64,
            byte_size: parquet.len() as u64,
            content_digest: Sha256::digest(&parquet)
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect(),
        };
        for bad in [false, true] {
            let mut bytes = Vec::new();
            frame(&mut bytes, 1, &serde_json::to_vec(&r).unwrap());
            if count == Some(1) || bad {
                let mut meta = meta.clone();
                if bad {
                    meta.content_digest = "0".repeat(64);
                }
                frame(&mut bytes, 3, &serde_json::to_vec(&meta).unwrap());
                frame(&mut bytes, 2, &parquet);
                frame(&mut bytes, 4, b"");
            }
            frame(&mut bytes, 5, b"");
            outcomes.push(exchange(&owners, &op, &guard, bytes, Some(j.clone())).await);
            expected.push(!bad);
        }
    }
    op.dispose().await.unwrap();
    process.dispose().await.unwrap();
    finish(owners, threads, parent).await;
    assert_eq!(outcomes, expected);
}

// Observe actual consumed bytes and a Pending read without timing assumptions.
struct Observed {
    inner: tokio::io::DuplexStream,
    bytes: Arc<std::sync::atomic::AtomicUsize>,
    pending: Option<tokio::sync::oneshot::Sender<()>>,
}
impl tokio::io::AsyncRead for Observed {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let poll = std::pin::Pin::new(&mut self.inner).poll_read(cx, buf);
        self.bytes.fetch_add(
            buf.filled().len() - before,
            std::sync::atomic::Ordering::SeqCst,
        );
        if poll.is_pending()
            && let Some(tx) = self.pending.take()
        {
            let _ = tx.send(());
        }
        poll
    }
}
#[tokio::test]
async fn eof_pending_and_accepted_append_cancellation_retain_owner() {
    use crate::node::payload_owner::tests::receive_gates;
    use std::sync::atomic::{AtomicUsize, Ordering};
    for append in [false, true] {
        let (owners, threads, parent, guard) = fixture_workers(2);
        let (process, op) = anchors(
            &owners,
            parent.clone(),
            guard.clone(),
            SpoolLimits::new(2, 1).unwrap(),
        )
        .await;
        let (job, result) = controls();
        let mut bytes = Vec::new();
        frame(&mut bytes, 1, &serde_json::to_vec(&result).unwrap());
        frame(&mut bytes, 2, b"[]");
        let append_end = bytes.len();
        if append {
            frame(&mut bytes, 2, b"[]");
        } // Must never be consumed after cancellation.
        frame(&mut bytes, 4, b"");
        frame(&mut bytes, 5, b"");
        let (mut writer, transport) = tokio::io::duplex(4096);
        writer.write_all(&bytes).await.unwrap(); // Keep writer open after STREAM_END.
        let lease = owners
            .try_admit(&op, Role::Operation, guard.clone())
            .unwrap();
        let (started, release, disposed, finish_return) = receive_gates(&lease);
        let (pending_tx, pending) = tokio::sync::oneshot::channel();
        let consumed = Arc::new(AtomicUsize::new(0));
        let observed = Observed {
            inner: transport,
            bytes: consumed.clone(),
            pending: Some(pending_tx),
        };
        let task = nabedra_rt::task::spawn(receive_result(reader(observed), lease, job));
        nabedra_rt::time::timeout(std::time::Duration::from_secs(5), started)
            .await
            .unwrap()
            .unwrap();
        if !append {
            release.send(()).unwrap();
            nabedra_rt::time::timeout(std::time::Duration::from_secs(5), pending)
                .await
                .unwrap()
                .unwrap();
        }
        task.abort();
        let cancelled = task.await.is_err();
        let lifetime_during_command = Arc::strong_count(&guard);
        let quota_retained =
            !append || crate::node::payload_owner::tests::quota_refused(&owners, &op).await;
        let retained_during_command = owners
            .try_admit(&op, Role::Operation, guard.clone())
            .is_err();
        let consumed_before = consumed.load(Ordering::SeqCst);
        if append {
            release.send(()).unwrap();
        }
        nabedra_rt::time::timeout(std::time::Duration::from_secs(5), disposed)
            .await
            .unwrap()
            .unwrap();
        let quota_released = !crate::node::payload_owner::tests::quota_refused(&owners, &op).await;
        let lifetime_after_disposal = Arc::strong_count(&guard);
        let retained_after_disposal = owners
            .try_admit(&op, Role::Operation, guard.clone())
            .is_err();
        finish_return.send(()).unwrap();
        drop(writer);
        op.dispose().await.unwrap();
        process.dispose().await.unwrap();
        finish(owners, threads, parent).await;
        assert!(
            cancelled
                && retained_during_command
                && retained_after_disposal
                && quota_retained
                && quota_released
        );
        assert_eq!(lifetime_during_command, 3);
        assert_eq!(lifetime_after_disposal, 3);
        assert_eq!(Arc::strong_count(&guard), 1);
        assert_eq!(
            consumed_before,
            if append { append_end } else { bytes.len() }
        );
        assert_eq!(consumed.load(Ordering::SeqCst), consumed_before);
    }
}

#[tokio::test]
async fn fragmented_rows_and_unused_receive_lease() {
    let (owners, threads, parent, guard) = fixture();
    let (process, op) = anchors(&owners, parent.clone(), guard.clone(), limits()).await;
    let (j, r) = controls();
    let mut j = serde_json::to_value(j).unwrap();
    j["table"]["schema"] = serde_json::json!({"id":"int64","s":"utf8","v":"float64"});
    let job: Box<JobEnvelope> = Box::new(serde_json::from_value(j).unwrap());
    let mut r = serde_json::to_value(r).unwrap();
    r["result"]["payload"]["found"] = true.into();
    r["result"]["payload"]["source_payload_bytes"] = 100.into();
    let result: ResultEnvelope = serde_json::from_value(r).unwrap();
    let mut bytes = Vec::new();
    frame(&mut bytes, 1, &serde_json::to_vec(&result).unwrap());
    for fragment in [br#"[{"id":1,"s":"a\"#.as_slice(), br#"n","v":1."#, b"25}]"] {
        frame(&mut bytes, 2, fragment);
    }
    frame(&mut bytes, 4, b"");
    frame(&mut bytes, 5, b"");
    let (mut writer, transport) = tokio::io::duplex(4096);
    writer.write_all(&bytes).await.unwrap();
    writer.shutdown().await.unwrap();
    let lease = owners
        .try_admit(&op, Role::Operation, guard.clone())
        .unwrap();
    let mut accepted = receive_result(reader(transport), lease, job.clone())
        .await
        .unwrap();
    let rows = match accepted
        .lease
        .command(crate::node::payload_owner::Command::TakeRows)
        .await
        .unwrap()
    {
        crate::node::payload_owner::Reply::Rows(rows) => rows,
        _ => unreachable!(),
    };
    accepted.lease.dispose().await.unwrap();
    let lease = owners
        .try_admit(&op, Role::Operation, guard.clone())
        .unwrap();
    let (writer, transport) = tokio::io::duplex(1);
    drop(receive_result(reader(transport), lease, job)); // Deliberately never polled.
    drop(writer);
    op.dispose().await.unwrap();
    process.dispose().await.unwrap();
    finish(owners, threads, parent).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["s"], crate::protocol::Scalar::Utf8("a\n".into()));
    assert_eq!(rows[0]["v"], crate::protocol::Scalar::Float64(1.25));
}
