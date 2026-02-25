//! Server-side gRPC encoding body adapted from tonic's `codec/encode.rs`.
//!
//! Why this file exists:
//! - tonic's default encoder path expects protobuf `Encoder::encode(...)` for each message.
//! - our hot path already has messages serialized as `Bytes` (`PreEncoded`), so re-encoding would
//!   add extra CPU work and allocations.
//!
//! What is intentionally different from tonic:
//! - server-only surface (`EncodeBody::new_server`), no client-role logic.
//! - each frame is represented as `EncodedBytes { header, payload }` so we can keep the
//!   pre-encoded payload as-is and only prepend the 5-byte gRPC header.
//! - compression is still supported (gzip/zstd) and mirrors tonic's framing rules.
//!
//! In short: this keeps tonic-compatible wire framing while avoiding unnecessary serialization.

use {
    crate::codec::PreEncoded,
    bytes::{Buf, BufMut, Bytes, BytesMut},
    http::HeaderMap,
    http_body::{Body, Frame},
    pin_project::pin_project,
    std::{
        collections::VecDeque,
        fmt::Display,
        io,
        marker::PhantomData,
        pin::Pin,
        task::{ready, Context, Poll},
    },
    tokio_stream::{adapters::Fuse, Stream, StreamExt},
    tonic::{
        codec::{CompressionEncoding, Encoder, SingleMessageCompressionOverride, HEADER_SIZE},
        Status,
    },
};

/// Unless overridden, this is the buffer size used for compression scratch space.
const DEFAULT_CODEC_BUFFER_SIZE: usize = 8 * 1024;
const DEFAULT_YIELD_THRESHOLD: usize = 32 * 1024;
const DEFAULT_MAX_SEND_MESSAGE_SIZE: usize = usize::MAX;

/// Rope of encoded gRPC bytes emitted as one HTTP frame.
///
/// Every message contributes two chunks: 5-byte gRPC header + payload bytes.
#[derive(Debug, Clone)]
pub struct EncodedBytes {
    // gRPC message header:
    // - byte 0: compressed flag (0/1)
    // - bytes 1..=4: message length (u32, big-endian)
    header: [u8; HEADER_SIZE],
    header_pos: usize,
    // Already serialized protobuf message payload.
    payload: Bytes,
    payload_pos: usize,
    remaining: usize,
}

impl EncodedBytes {
    fn new(header: [u8; HEADER_SIZE], payload: Bytes) -> Self {
        let remaining = HEADER_SIZE + payload.len();
        Self {
            header,
            header_pos: 0,
            payload,
            payload_pos: 0,
            remaining,
        }
    }

}

impl Buf for EncodedBytes {
    fn remaining(&self) -> usize {
        self.remaining
    }

    fn chunk(&self) -> &[u8] {
        if self.remaining == 0 {
            return &[];
        }

        if self.header_pos < HEADER_SIZE {
            &self.header[self.header_pos..]
        } else {
            &self.payload[self.payload_pos..]
        }
    }

    fn advance(&mut self, mut cnt: usize) {
        assert!(cnt <= self.remaining, "advance past end of EncodedBytes");

        let header_remaining = HEADER_SIZE - self.header_pos;
        if cnt <= header_remaining {
            self.header_pos += cnt;
            self.remaining -= cnt;
            return;
        }

        self.header_pos = HEADER_SIZE;
        self.remaining -= header_remaining;
        cnt -= header_remaining;

        self.payload_pos += cnt;
        self.remaining -= cnt;
    }
}

/// A batched rope of multiple gRPC-framed messages.
///
/// This lets us preserve tonic-like threshold/pending flush behavior while each element remains
/// an `EncodedBytes { header, payload }` pair.
#[derive(Debug, Default, Clone)]
pub struct EncodedBatch {
    parts: VecDeque<EncodedBytes>,
    remaining: usize,
}

impl EncodedBatch {
    fn push(&mut self, bytes: EncodedBytes) {
        self.remaining += bytes.remaining();
        self.parts.push_back(bytes);
    }

    fn is_empty(&self) -> bool {
        self.remaining == 0
    }
}

impl Buf for EncodedBatch {
    fn remaining(&self) -> usize {
        self.remaining
    }

    fn chunk(&self) -> &[u8] {
        if self.remaining == 0 {
            return &[];
        }

        self.parts
            .front()
            .map(Buf::chunk)
            .expect("remaining > 0 implies at least one part")
    }

    fn advance(&mut self, mut cnt: usize) {
        assert!(cnt <= self.remaining, "advance past end of EncodedBatch");

        while cnt > 0 {
            let front = self.parts.front_mut().expect("remaining > 0 implies part exists");
            let front_remaining = front.remaining();
            if cnt < front_remaining {
                front.advance(cnt);
                self.remaining -= cnt;
                return;
            }

            front.advance(front_remaining);
            self.remaining -= front_remaining;
            cnt -= front_remaining;
            self.parts.pop_front();
        }
    }
}

#[pin_project(project = PreEncodedBytesProj)]
#[derive(Debug)]
struct PreEncodedBytes<T, U> {
    #[pin]
    source: Fuse<U>,
    compression_encoding: Option<CompressionEncoding>,
    max_message_size: Option<usize>,
    compression_buf: BytesMut,
    staged: EncodedBatch,
    error: Option<Status>,
    _marker: PhantomData<T>,
}

impl<T, U> PreEncodedBytes<T, U>
where
    U: Stream,
{
    fn new(
        source: U,
        compression_encoding: Option<CompressionEncoding>,
        compression_override: SingleMessageCompressionOverride,
        max_message_size: Option<usize>,
    ) -> Self {
        let compression_encoding =
            if compression_override == SingleMessageCompressionOverride::Disable {
                None
            } else {
                compression_encoding
            };

        Self {
            source: source.fuse(),
            compression_encoding,
            max_message_size,
            compression_buf: BytesMut::with_capacity(DEFAULT_CODEC_BUFFER_SIZE),
            staged: EncodedBatch::default(),
            error: None,
            _marker: PhantomData,
        }
    }

}

impl<T, U> Stream for PreEncodedBytes<T, U>
where
    T: Encoder<Error = Status>,
    T::Item: PreEncoded,
    <T::Item as PreEncoded>::Error: Display,
    U: Stream<Item = Result<T::Item, Status>>,
{
    type Item = Result<EncodedBatch, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if let Some(status) = this.error.take() {
            return Poll::Ready(Some(Err(status)));
        }

        loop {
            match this.source.as_mut().poll_next(cx) {
                Poll::Pending if this.staged.is_empty() => return Poll::Pending,
                Poll::Ready(None) if this.staged.is_empty() => return Poll::Ready(None),
                Poll::Pending | Poll::Ready(None) => {
                    return Poll::Ready(Some(Ok(std::mem::take(this.staged))));
                }
                Poll::Ready(Some(Ok(item))) => {
                    let payload = match item.pre_encoded() {
                        Ok(payload) => payload,
                        Err(err) => {
                            return Poll::Ready(Some(Err(Status::internal(format!(
                                "Error encoding: {err}"
                            )))));
                        }
                    };
                    let encoded = match encode_item(
                        payload,
                        *this.compression_encoding,
                        *this.max_message_size,
                        this.compression_buf,
                    ) {
                        Ok(encoded) => encoded,
                        Err(status) => return Poll::Ready(Some(Err(status))),
                    };

                    this.staged.push(encoded);
                    if this.staged.remaining() >= DEFAULT_YIELD_THRESHOLD {
                        return Poll::Ready(Some(Ok(std::mem::take(this.staged))));
                    }
                }
                Poll::Ready(Some(Err(status))) => {
                    if this.staged.is_empty() {
                        return Poll::Ready(Some(Err(status)));
                    }
                    *this.error = Some(status);
                    return Poll::Ready(Some(Ok(std::mem::take(this.staged))));
                }
            }
        }
    }
}

fn encode_item(
    payload: Bytes,
    compression_encoding: Option<CompressionEncoding>,
    max_message_size: Option<usize>,
    compression_buf: &mut BytesMut,
) -> Result<EncodedBytes, Status> {
    // Payload may be compressed depending on negotiated grpc-encoding.
    let (compressed, payload) = maybe_compress(payload, compression_encoding, compression_buf)?;

    let payload_len = payload.len();
    let header = build_header(compressed, payload_len, max_message_size)?;

    Ok(EncodedBytes::new(header, payload))
}

fn maybe_compress(
    payload: Bytes,
    compression_encoding: Option<CompressionEncoding>,
    compression_buf: &mut BytesMut,
) -> Result<(bool, Bytes), Status> {
    let Some(encoding) = compression_encoding else {
        return Ok((false, payload));
    };

    compression_buf.clear();
    compress(encoding, payload.as_ref(), compression_buf)
        .map_err(|err| Status::internal(format!("Error compressing: {err}")))?;

    Ok((true, compression_buf.split().freeze()))
}

fn build_header(
    compressed: bool,
    len: usize,
    max_message_size: Option<usize>,
) -> Result<[u8; HEADER_SIZE], Status> {
    // Keep tonic's size checks so wire behavior and status mapping stay consistent.
    let limit = max_message_size.unwrap_or(DEFAULT_MAX_SEND_MESSAGE_SIZE);

    if len > limit {
        return Err(Status::out_of_range(format!(
            "Error, encoded message length too large: found {len} bytes, the limit is: {limit} bytes"
        )));
    }

    if len > u32::MAX as usize {
        return Err(Status::resource_exhausted(format!(
            "Cannot return body with more than 4GB of data but got {len} bytes"
        )));
    }

    let mut header = [0u8; HEADER_SIZE];
    header[0] = compressed as u8;
    header[1..].copy_from_slice(&(len as u32).to_be_bytes());
    Ok(header)
}

#[allow(unused_variables, unreachable_code)]
fn compress(
    encoding: CompressionEncoding,
    payload: &[u8],
    out_buf: &mut BytesMut,
) -> Result<(), io::Error> {
    out_buf.reserve(payload.len().max(DEFAULT_CODEC_BUFFER_SIZE));
    let mut out_writer = out_buf.writer();

    match encoding {
        CompressionEncoding::Gzip => {
            let mut gzip_encoder = flate2::read::GzEncoder::new(
                payload,
                // Keep parity with tonic defaults.
                flate2::Compression::new(6),
            );
            std::io::copy(&mut gzip_encoder, &mut out_writer)?;
        }
        CompressionEncoding::Zstd => {
            let mut zstd_encoder = zstd::stream::read::Encoder::new(
                payload,
                // Keep parity with tonic defaults.
                zstd::DEFAULT_COMPRESSION_LEVEL,
            )?;
            std::io::copy(&mut zstd_encoder, &mut out_writer)?;
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Unsupported compression encoding: {encoding:?}"),
            ))
        }
    }

    Ok(())
}

/// A specialized implementation of [Body] for encoding pre-encoded grpc messages.
#[pin_project]
#[derive(Debug)]
pub struct EncodeBody<T, U> {
    #[pin]
    inner: PreEncodedBytes<T, U>,
    state: EncodeState,
}

#[derive(Debug)]
struct EncodeState {
    error: Option<Status>,
    is_end_stream: bool,
}

impl<T, U> EncodeBody<T, U>
where
    T: Encoder<Error = Status>,
    T::Item: PreEncoded,
    <T::Item as PreEncoded>::Error: Display,
    U: Stream<Item = Result<T::Item, Status>>,
{
    /// Turns a stream of grpc results (message or error status) into [EncodeBody] for servers.
    pub fn new_server(
        _encoder: T,
        source: U,
        compression_encoding: Option<CompressionEncoding>,
        compression_override: SingleMessageCompressionOverride,
        max_message_size: Option<usize>,
    ) -> Self {
        Self {
            inner: PreEncodedBytes::new(
                source,
                compression_encoding,
                compression_override,
                max_message_size,
            ),
            state: EncodeState {
                error: None,
                is_end_stream: false,
            },
        }
    }
}

impl EncodeState {
    fn trailers(&mut self) -> Option<Result<HeaderMap, Status>> {
        if self.is_end_stream {
            return None;
        }

        self.is_end_stream = true;
        let status = self.error.take().unwrap_or_else(|| Status::ok(""));

        let mut headers = HeaderMap::with_capacity(8);
        Some(status.add_header(&mut headers).map(|()| headers))
    }
}

impl<T, U> Body for EncodeBody<T, U>
where
    T: Encoder<Error = Status>,
    T::Item: PreEncoded,
    <T::Item as PreEncoded>::Error: Display,
    U: Stream<Item = Result<T::Item, Status>>,
{
    type Data = EncodedBatch;
    type Error = Status;

    fn is_end_stream(&self) -> bool {
        self.state.is_end_stream
    }

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let self_proj = self.project();

        match ready!(self_proj.inner.poll_next(cx)) {
            Some(Ok(d)) => {
                if d.is_empty() {
                    Poll::Ready(None)
                } else {
                    Some(Ok(Frame::data(d))).into()
                }
            }
            Some(Err(status)) => {
                self_proj.state.is_end_stream = true;
                let mut headers = HeaderMap::with_capacity(8);
                match status.add_header(&mut headers) {
                    Ok(()) => Some(Ok(Frame::trailers(headers))).into(),
                    Err(status) => Some(Err(status)).into(),
                }
            }
            None => self_proj
                .state
                .trailers()
                .map(|t| t.map(Frame::trailers))
                .into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        std::io,
        tonic::codec::EncodeBuf,
    };

    #[derive(Debug, Clone)]
    struct DummyItem(Bytes);

    impl PreEncoded for DummyItem {
        type Error = io::Error;

        fn pre_encoded(&self) -> Result<Bytes, Self::Error> {
            Ok(self.0.clone())
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct DummyEncoder;

    impl Encoder for DummyEncoder {
        type Item = DummyItem;
        type Error = Status;

        fn encode(&mut self, _item: Self::Item, _dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[test]
    fn encoded_bytes_advances_across_parts() {
        let mut header = [0u8; HEADER_SIZE];
        header[1..].copy_from_slice(&(4u32).to_be_bytes());
        let mut encoded = EncodedBytes::new(header, Bytes::from_static(b"defg"));

        assert_eq!(encoded.remaining(), HEADER_SIZE + 4);
        assert_eq!(encoded.chunk(), &header);

        encoded.advance(2);
        assert_eq!(encoded.remaining(), HEADER_SIZE + 2);
        assert_eq!(encoded.chunk(), &header[2..]);

        encoded.advance(HEADER_SIZE - 2);
        assert_eq!(encoded.remaining(), 4);
        assert_eq!(encoded.chunk(), b"defg");

        encoded.advance(4);
        assert_eq!(encoded.remaining(), 0);
        assert_eq!(encoded.chunk(), b"");
    }

    #[test]
    fn encode_item_without_compression_builds_header_and_payload_parts() {
        let payload = Bytes::from_static(b"hello");
        let mut scratch = BytesMut::new();
        let encoded = encode_item(payload, None, None, &mut scratch).expect("encode item");

        assert_eq!(encoded.payload, Bytes::from_static(b"hello"));
        assert_eq!(encoded.header[0], 0);
        assert_eq!(&encoded.header[1..5], &(5u32).to_be_bytes());
    }

    #[test]
    fn encode_item_with_gzip_sets_compressed_flag() {
        let payload = Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let mut scratch = BytesMut::new();
        let encoded = encode_item(
            payload,
            Some(CompressionEncoding::Gzip),
            None,
            &mut scratch,
        )
        .expect("encode item");

        assert_eq!(encoded.header[0], 1);
        assert!(!encoded.payload.is_empty());
    }

    #[test]
    fn encode_item_with_zstd_roundtrips_payload() {
        let payload = Bytes::from_static(b"zstd-payload-zstd-payload-zstd-payload");
        let mut scratch = BytesMut::new();
        let encoded = encode_item(
            payload.clone(),
            Some(CompressionEncoding::Zstd),
            None,
            &mut scratch,
        )
        .expect("encode item");

        assert_eq!(encoded.header[0], 1);

        let decompressed = zstd::stream::decode_all(encoded.payload.as_ref())
            .expect("zstd decompress");
        assert_eq!(decompressed.as_slice(), payload.as_ref());
    }

    #[tokio::test]
    async fn server_body_emits_data_then_error_trailers() {
        use http_body_util::BodyExt as _;

        let source = tokio_stream::iter(vec![
            Ok(DummyItem(Bytes::from_static(b"abc"))),
            Err(Status::internal("boom")),
        ]);
        let mut body = EncodeBody::new_server(
            DummyEncoder,
            source,
            None,
            SingleMessageCompressionOverride::default(),
            None,
        );

        let frame = body
            .frame()
            .await
            .expect("first frame exists")
            .expect("first frame ok");
        assert!(frame.is_data());

        let frame = body
            .frame()
            .await
            .expect("second frame exists")
            .expect("second frame ok");
        let trailers = frame.into_trailers().expect("trailers frame");
        assert_eq!(
            trailers
                .get(Status::GRPC_STATUS)
                .expect("grpc-status present"),
            "13"
        );
    }

    fn parse_grpc_payloads(mut bytes: Bytes) -> Vec<Bytes> {
        let mut out = Vec::new();
        while bytes.remaining() > 0 {
            assert!(bytes.remaining() >= HEADER_SIZE);
            let compressed = bytes.get_u8();
            assert_eq!(compressed, 0, "test parser expects uncompressed test payloads");
            let len = bytes.get_u32() as usize;
            assert!(bytes.remaining() >= len);
            out.push(bytes.split_to(len));
        }
        out
    }

    #[tokio::test]
    async fn server_body_batches_messages_and_defers_error_to_next_poll() {
        use http_body_util::BodyExt as _;

        let source = tokio_stream::iter(vec![
            Ok(DummyItem(Bytes::from_static(b"first"))),
            Ok(DummyItem(Bytes::from_static(b"second"))),
            Err(Status::internal("boom")),
        ]);
        let mut body = EncodeBody::new_server(
            DummyEncoder,
            source,
            None,
            SingleMessageCompressionOverride::default(),
            None,
        );

        let frame = body
            .frame()
            .await
            .expect("first frame exists")
            .expect("first frame ok");
        let mut data = frame.into_data().expect("data frame");
        let payloads = parse_grpc_payloads(data.copy_to_bytes(data.remaining()));
        assert_eq!(payloads, vec![Bytes::from_static(b"first"), Bytes::from_static(b"second")]);

        let frame = body
            .frame()
            .await
            .expect("second frame exists")
            .expect("second frame ok");
        let trailers = frame.into_trailers().expect("trailers frame");
        assert_eq!(
            trailers
                .get(Status::GRPC_STATUS)
                .expect("grpc-status present"),
            "13"
        );
    }
}
