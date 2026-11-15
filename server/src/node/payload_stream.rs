//! Payload framing runs in the admitted owner's single registered supervisor.
//! All semantic work and disk ownership remain in finite worker commands.
use super::{
    frame::{FrameKind, FrameReader, FrameWriter},
    job::{JobEnvelope, ResultEnvelope},
    payload_owner::{Command, OwnerError, PayloadLease, Reply, Slot, run_command},
};
use crate::worker_pool::WorkerPool;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};

pub(super) type Reader = FrameReader<Box<dyn AsyncRead + Unpin + Send>>;
#[derive(Debug)]
pub(super) enum ReceiveKind {
    Control,
    Rows,
    Prepared(u32),
}
pub(crate) struct AcceptedRequest {
    pub(crate) envelope: Box<JobEnvelope>,
    pub(crate) lease: PayloadLease,
}
pub(crate) struct AcceptedResult {
    pub(crate) envelope: Box<ResultEnvelope>,
    pub(crate) lease: PayloadLease,
}
// The lease moves into the future at construction, so dropping it unpolled cancels.
pub(crate) async fn receive_request(
    reader: Reader,
    lease: PayloadLease,
) -> Result<AcceptedRequest, OwnerError> {
    let (reply, lease) = lease.receive(reader, None).await?;
    let Reply::RequestAccepted(envelope) = reply else {
        lease.dispose().await?;
        return Err(OwnerError::Invalid);
    };
    Ok(AcceptedRequest { envelope, lease })
}
pub(crate) async fn receive_result(
    reader: Reader,
    lease: PayloadLease,
    expected: Box<JobEnvelope>,
) -> Result<AcceptedResult, OwnerError> {
    let (reply, lease) = lease.receive(reader, Some(expected)).await?;
    let Reply::Accepted(envelope) = reply else {
        lease.dispose().await?;
        return Err(OwnerError::Invalid);
    };
    Ok(AcceptedResult { envelope, lease })
}

/// Continue the same reader after checked admission consumed CONTROL exactly once.
pub(super) async fn receive_checked_request(
    reader: Reader,
    lease: PayloadLease,
) -> Result<AcceptedRequest, OwnerError> {
    let (reply, lease) = lease.receive_checked(reader).await?;
    let Reply::RequestAccepted(envelope) = reply else {
        lease.dispose().await?;
        return Err(OwnerError::Invalid);
    };
    Ok(AcceptedRequest { envelope, lease })
}

pub(super) async fn drive_checked(
    mut reader: Reader,
    expected: Option<Box<JobEnvelope>>,
    checked: Option<super::payload_owner::OwnedRequestPayload>,
    checked_reply: Option<(super::auth::ReplyHandoff, Box<JobEnvelope>)>,
    pool: &WorkerPool,
    slot: &Arc<Slot>,
) -> Result<Reply, OwnerError> {
    // Retain the original checked bytes and delivery hold until terminal EOF.
    // In this branch CONTROL was consumed by the authenticated HTTP prelude.
    let mut retention = None;
    let mut reply_retention = None;
    let kind = if let Some((handoff, expected)) = checked_reply {
        let (binding, bytes, body, observation, pending) = handoff.into_parts();
        let super::auth::ReplyBody::Job(result) = body else {
            return Err(OwnerError::Invalid);
        };
        reply_retention = Some((binding, bytes, observation, pending));
        let Reply::Receiving(kind) =
            run_command(pool, slot, Command::BeginCheckedResult { result, expected }).await?
        else {
            return Err(OwnerError::Invalid);
        };
        kind
    } else if let Some(payload) = checked {
        if expected.is_some() {
            return Err(OwnerError::Invalid);
        }
        let (job, hold) = payload.into_job()?;
        retention = Some(hold);
        run_command(pool, slot, Command::BeginCheckedRequest(job)).await?;
        ReceiveKind::Control
    } else {
        let first = slot
            .next_frame(&mut reader)
            .await?
            .ok_or(OwnerError::Invalid)?;
        if first.kind != FrameKind::Control {
            return Err(OwnerError::Invalid);
        }
        if let Some(expected) = expected {
            let reply = run_command(
                pool,
                slot,
                Command::BeginResult {
                    bytes: first.payload,
                    expected,
                },
            )
            .await?;
            let Reply::Receiving(kind) = reply else {
                return Err(OwnerError::Invalid);
            };
            kind
        } else {
            run_command(pool, slot, Command::BeginRequest(first.payload)).await?;
            ReceiveKind::Control
        }
    };
    match kind {
        ReceiveKind::Control => {}
        ReceiveKind::Rows => payload(&mut reader, pool, slot, Command::CloseRows).await?,
        ReceiveKind::Prepared(count) => {
            for _ in 0..count {
                let meta = slot
                    .next_frame(&mut reader)
                    .await?
                    .ok_or(OwnerError::Invalid)?;
                if meta.kind != FrameKind::PartMeta {
                    return Err(OwnerError::Invalid);
                }
                run_command(pool, slot, Command::BeginPart(meta.payload)).await?;
                payload(&mut reader, pool, slot, Command::ClosePart).await?;
            }
            run_command(pool, slot, Command::CompletePrepared).await?;
        }
    }
    if slot.next_frame(&mut reader).await?.is_some() {
        return Err(OwnerError::Invalid);
    }
    let result = run_command(pool, slot, Command::CommitEof).await;
    drop(retention);
    drop(reply_retention);
    result
}
async fn payload(
    reader: &mut Reader,
    pool: &WorkerPool,
    slot: &Arc<Slot>,
    close: Command,
) -> Result<(), OwnerError> {
    loop {
        let frame = slot.next_frame(reader).await?.ok_or(OwnerError::Invalid)?;
        match frame.kind {
            FrameKind::Data => {
                run_command(pool, slot, Command::Append(frame.payload)).await?;
            }
            FrameKind::PayloadEnd => {
                run_command(pool, slot, close).await?;
                return Ok(());
            }
            _ => return Err(OwnerError::Invalid),
        }
    }
}
#[cfg(test)]
mod tests;

#[cfg(test)]
mod send_tests;

// Only the finished memory/transport owner crosses back to the caller.
pub(super) type Transport = Box<dyn AsyncWrite + Unpin + Send>;
pub(super) type Writer = FrameWriter<Transport>;
pub(crate) async fn send_request(
    writer: Writer,
    lease: PayloadLease,
    job: Box<JobEnvelope>,
) -> Result<Transport, OwnerError> {
    lease.send(writer, job, true).await
}
pub(crate) async fn send_result(
    writer: Writer,
    lease: PayloadLease,
    expected: Box<JobEnvelope>,
) -> Result<Transport, OwnerError> {
    lease.send(writer, expected, false).await
}
pub(super) async fn drive_send(
    mut writer: Writer,
    job: Box<JobEnvelope>,
    request: bool,
    pool: &WorkerPool,
    slot: &Arc<Slot>,
) -> Result<Transport, OwnerError> {
    let Reply::Sending(control, kind) =
        run_command(pool, slot, Command::StartSend { job, request }).await?
    else {
        return Err(OwnerError::Invalid);
    };
    slot.output(writer.write_frame(FrameKind::Control, &control))
        .await?;
    match kind {
        ReceiveKind::Control => {}
        ReceiveKind::Rows => send_payload(&mut writer, pool, slot).await?,
        ReceiveKind::Prepared(count) => {
            for _ in 0..count {
                let Reply::Bytes(meta) = run_command(pool, slot, Command::NextPart).await? else {
                    return Err(OwnerError::Invalid);
                };
                slot.output(writer.write_frame(FrameKind::PartMeta, &meta))
                    .await?;
                send_payload(&mut writer, pool, slot).await?;
            }
        }
    }
    slot.output(writer.finish()).await?;
    writer.into_inner().map_err(|_| OwnerError::Invalid)
}
async fn send_payload(
    writer: &mut Writer,
    pool: &WorkerPool,
    slot: &Arc<Slot>,
) -> Result<(), OwnerError> {
    loop {
        let Reply::Bytes(bytes) = run_command(pool, slot, Command::ReadNext).await? else {
            return Err(OwnerError::Invalid);
        };
        if bytes.is_empty() {
            break;
        }
        slot.output(writer.write_frame(FrameKind::Data, &bytes))
            .await?;
    }
    slot.output(writer.write_frame(FrameKind::PayloadEnd, &[]))
        .await
}
