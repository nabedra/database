#![allow(clippy::disallowed_methods)] // integration tests drive real processes and real time; the phase 7 seam covers server/src
use nabedra_database_server::node::{
    frame::{FrameError, FrameKind as K, FrameReader, FrameWriter, MAX_DATA_BYTES},
    job::MAX_CONTROL_BYTES,
};
use std::{
    future::{Future, poll_fn},
    task::Poll,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
use tokio::time::{Duration, timeout};

fn header(kind: u8, len: u32) -> [u8; 5] {
    let n = len.to_be_bytes();
    [kind, n[0], n[1], n[2], n[3]]
}

// Poll real duplex I/O to Pending before cancelling; no timing assumption.
async fn pending_once<F: Future>(future: F) {
    let mut future = std::pin::pin!(future);
    poll_fn(|cx| match future.as_mut().poll(cx) {
        Poll::Pending => Poll::Ready(()),
        Poll::Ready(_) => panic!("expected pending I/O"),
    })
    .await;
}

#[tokio::test]
async fn binary_roundtrip_and_exact_bounds() {
    timeout(Duration::from_secs(20), async {
        let (tx, rx) = duplex(1024);
        let writer = tokio::spawn(async move {
            let mut w = FrameWriter::new(tx);
            for (kind, len) in [
                (K::Control, 1),
                (K::Control, MAX_CONTROL_BYTES),
                (K::PartMeta, MAX_CONTROL_BYTES),
                (K::Data, 1),
                (K::Data, MAX_DATA_BYTES),
            ] {
                let payload = vec![0xff; len];
                w.write_frame(kind, &payload).await.unwrap();
            }
            w.write_frame(K::PayloadEnd, &[]).await.unwrap();
            w.finish().await.unwrap();
            assert_eq!(w.finish().await, Err(FrameError::Completed));
            assert_eq!(
                w.write_frame(K::Data, b"x").await,
                Err(FrameError::Completed)
            );
            w.into_inner().unwrap().shutdown().await.unwrap();
        });
        let mut r = FrameReader::new(rx);
        for (kind, len) in [
            (K::Control, 1),
            (K::Control, MAX_CONTROL_BYTES),
            (K::PartMeta, MAX_CONTROL_BYTES),
            (K::Data, 1),
            (K::Data, MAX_DATA_BYTES),
            (K::PayloadEnd, 0),
        ] {
            let frame = r.next_frame().await.unwrap().unwrap();
            assert_eq!(frame.kind, kind);
            assert_eq!(frame.payload.len(), len);
            assert!(frame.payload.iter().all(|b| *b == 0xff));
        }
        assert_eq!(r.next_frame().await.unwrap(), None);
        assert_eq!(r.next_frame().await, Err(FrameError::Completed));
        writer.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn fragmented_header_payload_and_eof_proof() {
    timeout(Duration::from_secs(5), async {
        let (mut tx, rx) = duplex(1);
        let writer = tokio::spawn(async move {
            for byte in header(2, 4)
                .into_iter()
                .chain([0, 255, 13, 10])
                .chain(header(4, 0))
                .chain(header(5, 0))
            {
                tx.write_all(&[byte]).await.unwrap();
            }
            tx.shutdown().await.unwrap();
        });
        let mut r = FrameReader::new(rx);
        assert_eq!(
            r.next_frame().await.unwrap().unwrap().payload,
            [0, 255, 13, 10]
        );
        assert_eq!(r.next_frame().await.unwrap().unwrap().kind, K::PayloadEnd);
        assert_eq!(r.next_frame().await.unwrap(), None);
        writer.await.unwrap();

        let (mut tx, rx) = duplex(16);
        tx.write_all(&header(5, 0)).await.unwrap();
        let mut r = FrameReader::new(rx);
        pending_once(r.next_frame()).await; // STREAM_END alone cannot complete.
        assert_eq!(r.next_frame().await, Err(FrameError::Poisoned));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn invalid_declared_headers_fail_without_payload_or_eof() {
    timeout(Duration::from_secs(5), async {
        for (kind, len, expected) in [
            (0, 1, FrameError::UnknownKind),
            (255, u32::MAX, FrameError::UnknownKind),
            (1, 0, FrameError::InvalidLength),
            (3, 0, FrameError::InvalidLength),
            (2, 0, FrameError::InvalidLength),
            (4, 1, FrameError::InvalidLength),
            (5, 1, FrameError::InvalidLength),
            (1, MAX_CONTROL_BYTES as u32 + 1, FrameError::InvalidLength),
            (3, MAX_CONTROL_BYTES as u32 + 1, FrameError::InvalidLength),
            (2, MAX_DATA_BYTES as u32 + 1, FrameError::InvalidLength),
            (2, u32::MAX, FrameError::InvalidLength),
        ] {
            let (mut tx, rx) = duplex(16);
            tx.write_all(&header(kind, len)).await.unwrap();
            let mut r = FrameReader::new(rx);
            assert_eq!(r.next_frame().await, Err(expected));
            assert_eq!(r.next_frame().await, Err(FrameError::Poisoned));
            drop(tx);
        }
        let (tx, mut rx) = duplex(16);
        let mut w = FrameWriter::new(tx);
        for kind in [K::Control, K::PartMeta, K::Data] {
            assert_eq!(
                w.write_frame(kind, &[]).await,
                Err(FrameError::InvalidLength)
            );
        }
        for (kind, size) in [
            (K::Control, MAX_CONTROL_BYTES + 1),
            (K::PartMeta, MAX_CONTROL_BYTES + 1),
            (K::Data, MAX_DATA_BYTES + 1),
        ] {
            assert_eq!(
                w.write_frame(kind, &vec![0; size]).await,
                Err(FrameError::InvalidLength)
            );
        }
        for kind in [K::PayloadEnd, K::StreamEnd] {
            assert_eq!(
                w.write_frame(kind, b"x").await,
                Err(FrameError::InvalidLength)
            );
        }
        assert_eq!(
            w.write_frame(K::StreamEnd, &[]).await,
            Err(FrameError::UseFinish)
        );
        // Validation emitted no bytes and kept the writer reusable.
        w.finish().await.unwrap();
        let mut actual = [0; 5];
        rx.read_exact(&mut actual).await.unwrap();
        assert_eq!(actual, header(5, 0));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn truncation_trailing_bytes_and_io_failure_poison() {
    timeout(Duration::from_secs(5), async {
        let mut cases = vec![
            (vec![], FrameError::Truncated),
            (vec![1, 0], FrameError::Truncated),
        ];
        cases.push((
            [header(2, 2).as_slice(), b"x"].concat(),
            FrameError::Truncated,
        ));
        cases.push((
            [header(2, 1).as_slice(), b"x"].concat(),
            FrameError::Truncated,
        ));
        cases.push((
            [header(5, 0).as_slice(), b"x"].concat(),
            FrameError::TrailingBytes,
        ));
        for (bytes, error) in cases {
            let (mut tx, rx) = duplex(32);
            let writer = tokio::spawn(async move {
                tx.write_all(&bytes).await.unwrap();
                tx.shutdown().await.unwrap();
            });
            let mut r = FrameReader::new(rx);
            let first = r.next_frame().await;
            let failure = if matches!(first, Ok(Some(_))) {
                r.next_frame().await
            } else {
                first
            };
            assert_eq!(failure, Err(error));
            assert_eq!(r.next_frame().await, Err(FrameError::Poisoned));
            writer.await.unwrap();
        }
        let (tx, rx) = duplex(1);
        drop(rx);
        let mut w = FrameWriter::new(tx);
        assert_eq!(w.write_frame(K::Data, b"x").await, Err(FrameError::Io));
        assert_eq!(w.finish().await, Err(FrameError::Poisoned));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancelling_partial_io_poisons_reader_and_writer() {
    timeout(Duration::from_secs(5), async {
        let (mut tx, rx) = duplex(16);
        tx.write_all(&[2, 0]).await.unwrap();
        let mut r = FrameReader::new(rx);
        pending_once(r.next_frame()).await;
        assert_eq!(r.next_frame().await, Err(FrameError::Poisoned));

        let (tx, mut rx) = duplex(2);
        let mut w = FrameWriter::new(tx);
        pending_once(w.write_frame(K::Data, b"binary")).await;
        let mut partial = [0; 2];
        rx.read_exact(&mut partial).await.unwrap();
        assert_eq!(partial, [2, 0]);
        assert_eq!(
            w.write_frame(K::Data, b"x").await,
            Err(FrameError::Poisoned)
        );
        assert_eq!(w.finish().await, Err(FrameError::Poisoned));
    })
    .await
    .unwrap();
}
