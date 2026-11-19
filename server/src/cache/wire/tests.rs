use super::*;

#[test]
fn k01_binary_requests_survive_all_fragmentation_boundaries_and_pipelines() {
    let command = b"*3\r\n$3\r\nSET\r\n$0\r\n\r\n$6\r\n\0\r\n\xffxy\r\n";
    let ping = b"*1\r\n$4\r\nPING\r\n";
    for split in 0..command.len() {
        let mut decoder = Decoder::new(FrameLimits::default());
        let mut input = BytesMut::from(&command[..split]);
        assert_eq!(decoder.decode(&mut input), Ok(None));
        input.extend_from_slice(&command[split..]);
        input.extend_from_slice(ping);
        let arguments = decoder.decode(&mut input).unwrap().unwrap();
        assert_eq!(
            arguments,
            [
                Bytes::from_static(b"SET"),
                Bytes::new(),
                Bytes::from_static(b"\0\r\n\xffxy")
            ]
        );
        assert_eq!(input, &ping[..]);
        assert_eq!(
            decoder.decode(&mut input).unwrap().unwrap(),
            [Bytes::from_static(b"PING")]
        );
        assert_eq!(decoder.eof(&input), Ok(()));
    }
}

#[test]
fn k02_reject_declared_limits_before_reading_bodies() {
    let limits = FrameLimits {
        max_frame_bytes: 32,
        max_arguments: 4,
        max_bulk_bytes: 24,
    };
    for (bad_frame, expected) in [
        (&b"*5"[..], ProtocolError::TooManyArguments),
        (&b"*1\r\n$25"[..], ProtocolError::BulkTooLarge),
        (
            &b"*2\r\n$5\r\nabcde\r\n$18\r\n"[..],
            ProtocolError::FrameTooLarge,
        ),
        (
            &b"*1\r\n$9999999999999999999999999999999999"[..],
            ProtocolError::BulkTooLarge,
        ),
        (&b"*0\r\n"[..], ProtocolError::InvalidLength),
        (&b"*1\r\n$-1\r\n"[..], ProtocolError::InvalidLength),
        (&b"*01\r\n"[..], ProtocolError::InvalidLength),
        (&b"*1\r\n$1\r\nx\r "[..], ProtocolError::InvalidTerminator),
        (&b"*1\r\n+text\r\n"[..], ProtocolError::ExpectedBulk),
        (&b"PING\r\n"[..], ProtocolError::ExpectedArray),
    ] {
        let mut parser = Decoder::new(limits);
        assert_eq!(parser.decode(&mut BytesMut::from(bad_frame)), Err(expected));
    }

    let mut parser = Decoder::new(limits);
    let ping = b"*1\r\n$4\r\nPING\r\n";
    let mut pipeline = BytesMut::from(&ping.repeat(4)[..]);
    assert!(pipeline.len() > limits.max_frame_bytes);
    for _ in 0..4 {
        assert_eq!(
            parser.decode(&mut pipeline).unwrap().unwrap(),
            [Bytes::from_static(b"PING")]
        );
    }

    let mut short = BytesMut::from(&b"*1\r\n$9\r\na"[..]);
    assert_eq!(parser.decode(&mut short), Ok(None));
    assert_eq!(parser.eof(&short), Err(ProtocolError::Truncated));
}

#[test]
fn k01_responses_distinguish_empty_miss_binary_and_signed_extremes() {
    let response = Response::Array(vec![
        Response::Bulk(None),
        Response::Bulk(Some(Bytes::new())),
        Response::bulk(b"\xff\r\n"),
        Response::Integer(i64::MIN),
        Response::Integer(i64::MAX),
        Response::NullArray,
        Response::Error(Bytes::from_static(b"ERR x\r\n+injected")),
    ]);
    let expected = b"*7\r\n$-1\r\n$0\r\n\r\n$3\r\n\xff\r\n\r\n:-9223372036854775808\r\n:9223372036854775807\r\n*-1\r\n-ERR x  +injected\r\n";
    assert_eq!(response.encoded_len(), Some(expected.len()));
    assert_eq!(response.encode(expected.len()).unwrap(), &expected[..]);
    assert_eq!(response.encode(expected.len() - 1), Err(OutputTooLarge));
}

#[tokio::test]
async fn k02_vectored_output_resumes_short_writes_and_bounds_bulk_replies() {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    #[derive(Default)]
    struct ShortWriter(Vec<u8>);

    impl AsyncWrite for ShortWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            data: &[u8],
        ) -> Poll<io::Result<usize>> {
            let amount = data.len().min(101);
            self.0.extend_from_slice(&data[..amount]);
            Poll::Ready(Ok(amount))
        }
        fn poll_write_vectored(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            data: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            self.poll_write(cx, &data[0])
        }
        fn is_write_vectored(&self) -> bool {
            true
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    let response = Response::Bulk(Some(Bytes::from(vec![0xff; 4096])));
    let max_bytes = response.encoded_len().unwrap();
    let mut writer = ShortWriter::default();
    assert_eq!(
        response
            .write_to(&mut writer, max_bytes - 1)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert!(writer.0.is_empty());
    response.write_to(&mut writer, max_bytes).await.unwrap();
    assert_eq!(writer.0, response.encode(max_bytes).unwrap());
}
