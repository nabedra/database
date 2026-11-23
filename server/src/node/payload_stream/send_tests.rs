use super::tests::*;
use super::*;
use crate::node::{frame::FrameWriter, payload_owner::Role};
use tokio::io::AsyncWriteExt;

#[tokio::test]
async fn registered_request_roundtrip() {
    let (owners, threads, parent, guard) = fixture();
    let (process, op) = anchors(&owners, parent.clone(), guard.clone(), limits()).await;
    let (job, _) = controls();
    let lease = owners
        .try_admit(&op, Role::Operation, guard.clone())
        .unwrap();
    let (tx, rx) = tokio::io::duplex(32);
    let send = nabedra_rt::task::spawn(async move {
        let mut tx = send_request(
            FrameWriter::new(Box::new(tx) as Box<dyn tokio::io::AsyncWrite + Unpin + Send>),
            lease,
            job,
        )
        .await
        .unwrap();
        tx.shutdown().await.unwrap();
    });
    let incoming = owners
        .try_admit(&op, Role::Dependency, guard.clone())
        .unwrap();
    let accepted = receive_request(reader(rx), incoming).await.unwrap();
    let (expected, _) = controls();
    assert_eq!(
        serde_json::to_value(&accepted.envelope).unwrap(),
        serde_json::to_value(expected).unwrap()
    );
    accepted.lease.dispose().await.unwrap();
    send.await.unwrap();
    op.dispose().await.unwrap();
    process.dispose().await.unwrap();
    finish(owners, threads, parent).await;
}

async fn rows_input(
    lease: &mut PayloadLease,
    job: Box<JobEnvelope>,
    result: Box<ResultEnvelope>,
    chunks: impl IntoIterator<Item = Vec<u8>>,
) {
    lease.ready().await.unwrap();
    lease
        .command(Command::BeginRows { job, result })
        .await
        .unwrap();
    for chunk in chunks {
        lease.command(Command::Append(chunk)).await.unwrap();
    }
    lease.command(Command::CloseRows).await.unwrap();
    lease.command(Command::CommitEof).await.unwrap();
}
#[tokio::test]
async fn registered_results_roundtrip() {
    let (owners, threads, parent, guard) = fixture();
    let (process, op) = anchors(&owners, parent.clone(), guard.clone(), limits()).await;
    for kind in ["get", "collect", "count", "error", "null", "zero", "one"] {
        let (mut job, mut result) = if ["null", "zero", "one"].contains(&kind) {
            prepared(match kind {
                "null" => None,
                "zero" => Some(0),
                _ => Some(1),
            })
        } else {
            controls()
        };
        if ["collect", "count"].contains(&kind) {
            let mut j = serde_json::to_value(&job).unwrap();
            j["task"] = serde_json::json!({"kind":kind,"payload":{"file":j["task"]["payload"]["file"],"query":{"select":null,"filter":null,"order_by":null,"limit":null}}});
            *job = serde_json::from_value(j).unwrap();
            let mut r = serde_json::to_value(&result).unwrap();
            r["result"] = serde_json::json!({"kind":kind,"payload":{"source_rows":1,"source_payload_bytes":32,"matched_rows":0}});
            *result = serde_json::from_value(r).unwrap();
        } else if kind == "error" {
            let mut r = serde_json::to_value(&result).unwrap();
            r["result"] = serde_json::json!({"kind":"error","payload":{"code":"storage_unavailable","message":"unavailable"}});
            *result = serde_json::from_value(r).unwrap();
        }
        let mut lease = owners
            .try_admit(&op, Role::Operation, guard.clone())
            .unwrap();
        lease.ready().await.unwrap();
        let receiving = lease
            .command(Command::BeginResult {
                bytes: serde_json::to_vec(&result).unwrap(),
                expected: job.clone(),
            })
            .await
            .unwrap();
        match receiving {
            Reply::Receiving(ReceiveKind::Rows) => {
                lease
                    .command(Command::Append(b"[]".to_vec()))
                    .await
                    .unwrap();
                lease.command(Command::CloseRows).await.unwrap();
            }
            Reply::Receiving(ReceiveKind::Prepared(count)) => {
                if count == 1 {
                    use sha2::{Digest, Sha256};
                    let rows = vec![serde_json::from_value(serde_json::json!({"id":1})).unwrap()];
                    let bytes = crate::snapshot::encode(&job.table, &rows).unwrap();
                    let meta = serde_json::json!({"range":{"lower":null,"upper":null},"row_count":1,"payload_bytes":crate::snapshot::validate_rows(&job.table,&rows).unwrap(),"byte_size":bytes.len(),"content_digest":Sha256::digest(&bytes).iter().map(|b| format!("{b:02x}")).collect::<String>()});
                    lease
                        .command(Command::BeginPart(serde_json::to_vec(&meta).unwrap()))
                        .await
                        .unwrap();
                    lease.command(Command::Append(bytes)).await.unwrap();
                    lease.command(Command::ClosePart).await.unwrap();
                }
                lease.command(Command::CompletePrepared).await.unwrap();
            }
            _ => {}
        }
        lease.command(Command::CommitEof).await.unwrap();
        let (tx, rx) = tokio::io::duplex(31);
        let expected = job.clone();
        let send = nabedra_rt::task::spawn(async move {
            let mut tx = send_result(FrameWriter::new(Box::new(tx) as Transport), lease, job)
                .await
                .unwrap();
            tx.shutdown().await.unwrap();
        });
        let incoming = owners
            .try_admit(&op, Role::Dependency, guard.clone())
            .unwrap();
        let accepted = receive_result(reader(rx), incoming, expected)
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(&accepted.envelope).unwrap(),
            serde_json::to_value(result).unwrap()
        );
        accepted.lease.dispose().await.unwrap();
        send.await.unwrap();
    }
    op.dispose().await.unwrap();
    process.dispose().await.unwrap();
    finish(owners, threads, parent).await;
}

#[tokio::test]
async fn prepared_transition_releases_exhausted_reader_before_next_open() {
    use crate::node::payload_owner::tests::observe_part_open_quota;
    use sha2::{Digest, Sha256};

    let (owners, threads, parent, guard) = fixture();
    let (other, other_threads, other_parent, other_guard) = fixture();
    // Each independent operation/process has exactly two file slots.
    let (process, op) = anchors(&owners, parent.clone(), guard.clone(), limits()).await;
    let (other_process, other_op) =
        anchors(&other, other_parent.clone(), other_guard.clone(), limits()).await;
    let (job, result) = prepared(Some(2));
    let mut lease = owners
        .try_admit(&op, Role::Operation, guard.clone())
        .unwrap();
    lease.ready().await.unwrap();
    lease
        .command(Command::BeginResult {
            bytes: serde_json::to_vec(&result).unwrap(),
            expected: job.clone(),
        })
        .await
        .unwrap();
    for (id, range) in [
        (1, serde_json::json!({"lower":null,"upper":2})),
        (2, serde_json::json!({"lower":2,"upper":null})),
    ] {
        let rows = vec![serde_json::from_value(serde_json::json!({"id":id})).unwrap()];
        let bytes = crate::snapshot::encode(&job.table, &rows).unwrap();
        let meta = serde_json::json!({
            "range":range,"row_count":1,
            "payload_bytes":crate::snapshot::validate_rows(&job.table,&rows).unwrap(),
            "byte_size":bytes.len(),
            "content_digest":Sha256::digest(&bytes).iter().map(|b| format!("{b:02x}")).collect::<String>()
        });
        lease
            .command(Command::BeginPart(serde_json::to_vec(&meta).unwrap()))
            .await
            .unwrap();
        lease.command(Command::Append(bytes)).await.unwrap();
        lease.command(Command::ClosePart).await.unwrap();
    }
    lease.command(Command::CompletePrepared).await.unwrap();
    lease.command(Command::CommitEof).await.unwrap();
    let observations = observe_part_open_quota(&lease);
    let (tx, rx) = tokio::io::duplex(31);
    let expected = job.clone();
    let send = nabedra_rt::task::spawn(async move {
        let mut tx = send_result(FrameWriter::new(Box::new(tx) as Transport), lease, job).await?;
        tx.shutdown().await.map_err(|_| OwnerError::Invalid)
    });
    let incoming = other
        .try_admit(&other_op, Role::Dependency, other_guard)
        .unwrap();
    let accepted = receive_result(reader(rx), incoming, expected)
        .await
        .unwrap();
    let envelope_matches =
        serde_json::to_value(&accepted.envelope).unwrap() == serde_json::to_value(result).unwrap();
    accepted.lease.dispose().await.unwrap();
    let sent = send.await.unwrap();
    let observed = observations.lock().unwrap().clone();
    op.dispose().await.unwrap();
    process.dispose().await.unwrap();
    other_op.dispose().await.unwrap();
    other_process.dispose().await.unwrap();
    finish(owners, threads, parent).await;
    finish(other, other_threads, other_parent).await;

    assert!(sent.is_ok());
    assert!(envelope_matches);
    // First open still owns both files. At the second open the exhausted file
    // must already be deleted and its slot reusable, before into_reader runs.
    assert_eq!(observed, [true, false]);
}

#[tokio::test]
async fn escaped_get_streams_between_distinct_pools() {
    use crate::node::spool::SpoolLimits;
    let (owners, threads, parent, guard) = fixture();
    let (other, other_threads, other_parent, other_guard) = fixture();
    let quota = SpoolLimits::new(32 * 1024 * 1024, 2).unwrap();
    let mut process = owners.try_process(parent.clone(), quota).unwrap();
    process.ready().await.unwrap();
    let mut op = owners
        .try_operation(&process, quota, guard.clone())
        .unwrap();
    op.ready().await.unwrap();
    let mut other_process = other.try_process(other_parent.clone(), quota).unwrap();
    other_process.ready().await.unwrap();
    let mut other_op = other
        .try_operation(&other_process, quota, other_guard.clone())
        .unwrap();
    other_op.ready().await.unwrap();
    let (j, r) = controls();
    let mut j = serde_json::to_value(j).unwrap();
    j["table"]["schema"]["s"] = "utf8".into();
    let job: Box<JobEnvelope> = Box::new(serde_json::from_value(j).unwrap());
    let mut r = serde_json::to_value(r).unwrap();
    r["result"]["payload"]["found"] = true.into();
    r["result"]["payload"]["source_payload_bytes"] = (3 * 1024 * 1024 + 100).into();
    let result = Box::new(serde_json::from_value(r).unwrap());
    let mut lease = owners
        .try_admit(&op, Role::Operation, guard.clone())
        .unwrap();
    let mut encoded_bytes = 0usize;
    let chunks = std::iter::once(b"[{\"id\":1,\"s\":\"".to_vec())
        .chain((0..384).map(|_| br"\u0000".repeat(8192)))
        .chain(std::iter::once(b"\"}]".to_vec()))
        .inspect(|chunk| encoded_bytes += chunk.len());
    rows_input(&mut lease, job.clone(), result, chunks).await;
    let (tx, rx) = tokio::io::duplex(257);
    let expected = job.clone();
    let send = nabedra_rt::task::spawn(async move {
        let mut tx = send_result(FrameWriter::new(Box::new(tx) as Transport), lease, job)
            .await
            .unwrap();
        tx.shutdown().await.unwrap();
    });
    let incoming = other
        .try_admit(&other_op, Role::Operation, other_guard)
        .unwrap();
    let mut accepted = receive_result(reader(rx), incoming, expected)
        .await
        .unwrap();
    let Reply::Rows(rows) = accepted.lease.command(Command::TakeRows).await.unwrap() else {
        panic!()
    };
    assert_eq!(
        rows[0]["s"],
        crate::protocol::Scalar::Utf8("\0".repeat(3 * 1024 * 1024))
    );
    accepted.lease.dispose().await.unwrap();
    send.await.unwrap();
    op.dispose().await.unwrap();
    process.dispose().await.unwrap();
    other_op.dispose().await.unwrap();
    other_process.dispose().await.unwrap();
    finish(owners, threads, parent).await;
    finish(other, other_threads, other_parent).await;
    assert!(encoded_bytes > crate::node::job::MAX_CONTROL_BYTES);
}

// Reports a real Pending write only after the first DATA header has started.
struct BlockedWriter {
    inner: tokio::io::DuplexStream,
    written: usize,
    threshold: usize,
    blocked: Option<tokio::sync::oneshot::Sender<()>>,
}
impl tokio::io::AsyncWrite for BlockedWriter {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let poll = std::pin::Pin::new(&mut self.inner).poll_write(cx, bytes);
        if let std::task::Poll::Ready(Ok(n)) = poll {
            self.written += n;
        }
        if poll.is_pending()
            && self.written >= self.threshold
            && let Some(tx) = self.blocked.take()
        {
            let _ = tx.send(());
        }
        poll
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
async fn event<T>(future: impl std::future::Future<Output = T>) -> T {
    nabedra_rt::time::timeout(std::time::Duration::from_secs(10), future)
        .await
        .expect("event watchdog")
}
#[tokio::test]
async fn blocked_writes_and_accepted_reads_retain_ownership() {
    use crate::node::{
        payload_owner::tests::{quota_refused, send_gates},
        spool::SpoolLimits,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    // Stop during the first DATA write, during an accepted read, and after the EOF read.
    for mode in 0..3 {
        let (owners, threads, parent, guard) = fixture_workers(2);
        let (process, op) = anchors(
            &owners,
            parent.clone(),
            guard.clone(),
            SpoolLimits::new(2, 1).unwrap(),
        )
        .await;
        let (job, result) = controls();
        let control_len = serde_json::to_vec(&result).unwrap().len() + 5;
        let mut lease = owners
            .try_admit(&op, Role::Operation, guard.clone())
            .unwrap();
        rows_input(&mut lease, job.clone(), result, [b"[]".to_vec()]).await;
        let count = Arc::new(AtomicUsize::new(0));
        let (read_started, release_read, disposed, finish_return) =
            send_gates(&lease, mode == 1, count.clone());
        let (tx, rx) = tokio::io::duplex(1);
        let (blocked_tx, blocked) = tokio::sync::oneshot::channel();
        let output = BlockedWriter {
            inner: tx,
            written: 0,
            threshold: control_len + if mode == 2 { 7 } else { 1 },
            blocked: Some(blocked_tx),
        };
        let task =
            nabedra_rt::task::spawn(send_result(FrameWriter::new(Box::new(output)), lease, job));
        let mut input = reader(rx);
        assert_eq!(
            event(input.next_frame()).await.unwrap().unwrap().kind,
            FrameKind::Control
        );
        if mode == 1 {
            event(read_started).await.unwrap();
        } else {
            if mode == 2 {
                assert_eq!(
                    event(input.next_frame()).await.unwrap().unwrap().payload,
                    b"[]"
                );
            }
            event(blocked).await.unwrap();
        }
        // A real finite command on the same pool completes while network/read is held.
        let charged = event(quota_refused(&owners, &op)).await;
        let reads = count.load(Ordering::SeqCst);
        task.abort();
        let cancelled = task.await.is_err();
        let retained = owners
            .try_admit(&op, Role::Operation, guard.clone())
            .is_err();
        if mode == 1 {
            release_read.send(()).unwrap();
        }
        event(disposed).await.unwrap();
        let released = !event(quota_refused(&owners, &op)).await;
        let retained_until_reap = owners
            .try_admit(&op, Role::Operation, guard.clone())
            .is_err();
        let lifetime = Arc::strong_count(&guard);
        finish_return.send(()).unwrap();
        drop(input);
        op.dispose().await.unwrap();
        process.dispose().await.unwrap();
        finish(owners, threads, parent).await;
        assert!(charged && released && cancelled && retained && retained_until_reap);
        assert_eq!(reads, if mode == 2 { 2 } else { 1 });
        assert_eq!(count.load(Ordering::SeqCst), reads);
        assert_eq!(lifetime, 3);
        assert_eq!(Arc::strong_count(&guard), 1);
    }
}

#[tokio::test]
async fn invalid_output_and_unpolled_sends_dispose() {
    let (owners, threads, parent, guard) = fixture();
    let (process, op) = anchors(&owners, parent.clone(), guard.clone(), limits()).await;
    let (mut job, _) = controls();
    job.version = 99;
    let lease = owners
        .try_admit(&op, Role::Operation, guard.clone())
        .unwrap();
    let (tx, mut rx) = tokio::io::duplex(1);
    let result = send_request(FrameWriter::new(Box::new(tx)), lease, job).await;
    assert!(matches!(result, Err(OwnerError::Invalid)));
    use tokio::io::AsyncReadExt;
    assert_eq!(rx.read(&mut [0]).await.unwrap(), 0);
    for request in [false, true] {
        let role = if request {
            Role::Operation
        } else {
            Role::Dependency
        };
        let lease = owners.try_admit(&op, role, guard.clone()).unwrap();
        let (job, _) = controls();
        let (tx, rx) = tokio::io::duplex(1);
        if request {
            drop(send_request(FrameWriter::new(Box::new(tx)), lease, job));
        } else {
            drop(send_result(FrameWriter::new(Box::new(tx)), lease, job));
        }
        drop(rx);
    }
    op.dispose().await.unwrap();
    process.dispose().await.unwrap();
    finish(owners, threads, parent).await;
}
