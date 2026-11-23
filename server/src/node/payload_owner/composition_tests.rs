use super::*;
use crate::{
    node::{
        frame::{FrameReader, FrameWriter},
        job::{Effect, PartMeta},
        payload_stream::{self, ReceiveKind},
    },
    protocol::{Key, Scalar},
    storage::KeyRange,
};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

// A single local composition case. All fallible observations precede teardown;
// assertions follow gate release, task settlement, registry drain and pool joins.
#[tokio::test]
async fn captured_split_prepare_roundtrips_original_registered_output() {
    let sender = LocalFixture::new().await;
    let receiver = LocalFixture::new().await;
    let job = prepare(
        &sender.job,
        serde_json::json!({"kind":"update","payload":{"table_id":sender.job.table.id,"key":2,"values":{"v":false}}}),
        true,
    );
    let sender_capacity = Arc::new(Semaphore::new(1));
    let receiver_capacity = Arc::new(Semaphore::new(1));
    let mut local = None;
    let mut accepted = None;
    let mut send_task = None;
    let mut receive_task = None;
    let mut dispose_task = None;
    let mut send_release = None;
    let mut receive_release = None;
    let mut checks = Vec::new();
    let watchdog = std::time::Duration::from_secs(10);
    let observation = async {
        let lease = sender.owners.try_local(
            &sender.operation,
            Role::Operation,
            Arc::new(sender_capacity.clone().try_acquire_owned().unwrap()),
            None,
            sender.job.cluster_id,
            job.clone(),
            sender.store.clone(),
            sender.partition.clone(),
            Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap(),
            Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap(),
        )?;
        let (result, lease) = lease.local_complete().await?;
        let send_slot = lease.slot.clone();
        local = Some(lease);
        let mut expected = serde_json::to_value(&job).unwrap();
        expected.as_object_mut().unwrap().remove("table");
        expected.as_object_mut().unwrap().remove("task");
        expected["result"] = serde_json::json!({"kind":"prepared","payload":{"effect":{"kind":"found","payload":{"value":true}},"replacement_count":3}});
        checks.push((
            "exact handler result",
            serde_json::to_value(&result).unwrap() == expected,
        ));
        checks.push((
            "captured input read exactly once",
            sender.store.backend().state.lock().unwrap().gets == 1,
        ));
        checks.push((
            "handler scratch retained",
            quota_charged(&sender)
                .await
                .map_err(|_| OwnerError::Invalid)?,
        ));
        checks.push((
            "receiver scratch initially free",
            !quota_charged(&receiver)
                .await
                .map_err(|_| OwnerError::Invalid)?,
        ));
        let (returning_tx, returning) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        send_release = Some(release_tx);
        send_slot.hooks.lock().unwrap().returning = Some((returning_tx, release_rx));
        let incoming = receiver.owners.try_admit(
            &receiver.operation,
            Role::Dependency,
            Arc::new(receiver_capacity.clone().try_acquire_owned().unwrap()),
        )?;
        let receive_slot = incoming.slot.clone();
        let (disposed_tx, disposed) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        receive_release = Some(release_tx);
        receive_slot.hooks.lock().unwrap().returning = Some((disposed_tx, release_rx));
        let (tx, rx) = tokio::io::duplex(31);
        let expected_job = job.clone();
        // Move the ORIGINAL handler lease into the actual sender future.
        let lease = local.take().ok_or(OwnerError::Invalid)?;
        let send_job = job.clone();
        send_task = Some(nabedra_rt::task::spawn(async move {
            let mut writer = payload_stream::send_result(
                FrameWriter::new(Box::new(tx) as Box<dyn AsyncWrite + Unpin + Send>),
                lease,
                send_job,
            )
            .await?;
            writer.shutdown().await.map_err(|_| OwnerError::Invalid)
        }));
        receive_task = Some(nabedra_rt::task::spawn(payload_stream::receive_result(
            FrameReader::new(Box::new(rx) as Box<dyn AsyncRead + Unpin + Send>),
            incoming,
            expected_job,
        )));
        returning.await.map_err(|_| OwnerError::Invalid)?;
        checks.push((
            "normal send disposed scratch",
            !quota_charged(&sender)
                .await
                .map_err(|_| OwnerError::Invalid)?,
        ));
        checks.push((
            "sender held until reap",
            sender_capacity.available_permits() == 0
                && sender.owners.shared.operation_transport.available_permits() == 0,
        ));
        checks.push((
            "sender admission refused before reap",
            sender.start(job.clone()).is_err(),
        ));
        send_release
            .take()
            .ok_or(OwnerError::Invalid)?
            .send(())
            .map_err(|_| OwnerError::Invalid)?;
        let sent = send_task.as_mut().ok_or(OwnerError::Invalid)?.await;
        send_task.take();
        sent.map_err(|_| OwnerError::Invalid)??;
        let received = receive_task.as_mut().ok_or(OwnerError::Invalid)?.await;
        receive_task.take();
        accepted = Some(received.map_err(|_| OwnerError::Invalid)??);
        checks.push((
            "sender released after reap",
            sender_capacity.available_permits() == 1
                && sender.owners.shared.operation_transport.available_permits() == 1,
        ));
        checks.push((
            "receiver scratch retained independently",
            quota_charged(&receiver)
                .await
                .map_err(|_| OwnerError::Invalid)?,
        ));
        let received = accepted.as_mut().ok_or(OwnerError::Invalid)?;
        checks.push((
            "received envelope matches handler and expected",
            serde_json::to_value(&received.envelope).unwrap() == expected
                && *received.envelope == *result,
        ));
        let Reply::Sending(control, ReceiveKind::Prepared(3)) = received
            .lease
            .command(Command::StartSend {
                job: job.clone(),
                request: false,
            })
            .await?
        else {
            return Err(OwnerError::Invalid);
        };
        checks.push((
            "inspection preserves envelope",
            serde_json::from_slice::<serde_json::Value>(&control).unwrap() == expected,
        ));
        let mut all_rows = Vec::new();
        for id in 1..=3 {
            let Reply::Bytes(meta) = received.lease.command(Command::NextPart).await? else {
                return Err(OwnerError::Invalid);
            };
            let meta: PartMeta = serde_json::from_slice(&meta).map_err(|_| OwnerError::Invalid)?;
            let mut bytes = Vec::new();
            loop {
                let Reply::Bytes(chunk) = received.lease.command(Command::ReadNext).await? else {
                    return Err(OwnerError::Invalid);
                };
                if chunk.is_empty() {
                    break;
                }
                bytes.extend(chunk);
                if bytes.len() > 64 * 1024 {
                    return Err(OwnerError::Invalid);
                }
            }
            let table = job.table.clone();
            // Only bounded ordinary bytes enter this closure, never a spool owner.
            let (rows, metadata_matches) = receiver
                .partition
                .submit(0, move |_, _| {
                    use sha2::{Digest, Sha256};
                    let expected_row: Row = [
                        ("id".into(), Scalar::Int64(id)),
                        ("v".into(), Scalar::Bool(id != 2)),
                    ]
                    .into();
                    let expected_rows = vec![expected_row];
                    let expected_bytes = crate::snapshot::encode(&table, &expected_rows).unwrap();
                    let expected_meta = PartMeta {
                        range: KeyRange {
                            lower: (id > 1).then_some(Key::Int64(id)),
                            upper: (id < 3).then_some(Key::Int64(id + 1)),
                        },
                        row_count: 1,
                        payload_bytes: crate::snapshot::validate_rows(&table, &expected_rows)
                            .unwrap() as u64,
                        byte_size: expected_bytes.len() as u64,
                        content_digest: Sha256::digest(&expected_bytes)
                            .iter()
                            .map(|b| format!("{b:02x}"))
                            .collect(),
                    };
                    let rows = crate::snapshot::decode(&table, bytes::Bytes::from(bytes), Some(1))
                        .unwrap();
                    Ok((rows.clone(), meta == expected_meta && rows == expected_rows))
                })
                .await
                .map_err(|_| OwnerError::Invalid)?
                .receive()
                .await
                .map_err(|_| OwnerError::Invalid)?;
            checks.push((
                "independent replacement rows/ranges/counts/bytes/digest",
                metadata_matches,
            ));
            all_rows.extend(rows);
        }
        checks.push((
            "complete captured rows survive update",
            all_rows.len() == 3
                && matches!(
                    result.result,
                    JobResult::Prepared {
                        effect: Effect::Found { value: true },
                        replacement_count: Some(3)
                    }
                ),
        ));
        let received = accepted.take().ok_or(OwnerError::Invalid)?;
        dispose_task = Some(nabedra_rt::task::spawn(received.lease.dispose()));
        disposed.await.map_err(|_| OwnerError::Invalid)?;
        checks.push((
            "receiver scratch released by disposal",
            !quota_charged(&receiver)
                .await
                .map_err(|_| OwnerError::Invalid)?,
        ));
        checks.push((
            "receiver held until reap",
            receiver_capacity.available_permits() == 0
                && receiver
                    .owners
                    .shared
                    .dependency_transport
                    .available_permits()
                    == 0,
        ));
        checks.push((
            "receiver admission refused before reap",
            receiver
                .owners
                .try_admit(
                    &receiver.operation,
                    Role::Dependency,
                    receiver.guard.clone(),
                )
                .is_err(),
        ));
        receive_release
            .take()
            .ok_or(OwnerError::Invalid)?
            .send(())
            .map_err(|_| OwnerError::Invalid)?;
        let disposed = dispose_task.as_mut().ok_or(OwnerError::Invalid)?.await;
        dispose_task.take();
        disposed.map_err(|_| OwnerError::Invalid)??;
        checks.push((
            "receiver released after reap",
            receiver_capacity.available_permits() == 1
                && receiver
                    .owners
                    .shared
                    .dependency_transport
                    .available_permits()
                    == 1,
        ));
        Ok::<_, OwnerError>(())
    };
    let observed = nabedra_rt::time::timeout(watchdog, observation).await;
    // Timeout drops observation futures, but never detaches original task handles.
    if let Some(release) = send_release {
        let _ = release.send(());
    }
    if let Some(release) = receive_release {
        let _ = release.send(());
    }
    drop(local);
    drop(accepted);
    if let Some(task) = send_task {
        task.abort();
        let _ = task.await;
    }
    if let Some(task) = receive_task {
        task.abort();
        let _ = task.await;
    }
    if let Some(task) = dispose_task {
        task.abort();
        let _ = task.await;
    }
    // finish attempts anchor disposal, actual supervisor reaping, both pool joins,
    // scratch removal and the existing no-PUT observations before asserting.
    let (sender_finished, receiver_finished) = tokio::join!(
        nabedra_rt::task::spawn(sender.finish()),
        nabedra_rt::task::spawn(receiver.finish()),
    );
    assert!(
        sender_finished.is_ok(),
        "sender teardown: {sender_finished:?}"
    );
    assert!(
        receiver_finished.is_ok(),
        "receiver teardown: {receiver_finished:?}"
    );
    assert!(
        matches!(observed, Ok(Ok(()))),
        "composition observation: {observed:?}"
    );
    for (name, passed) in checks {
        assert!(passed, "{name}");
    }
}
