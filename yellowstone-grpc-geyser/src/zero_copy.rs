//! Sends `Subscribe` updates without copying their cached account and transaction encodings
//! into each client's buffer. Compressed streams and every other method keep tonic's path.

use {
    crate::plugin::{
        filter::{encoder::LARGE_ACCOUNT_DATA_BYTES, message::FilteredUpdate},
        proto::geyser_server::Geyser,
    },
    bytes::{buf::UninitSlice, BufMut, Bytes, BytesMut},
    http_body::{Body, Frame},
    prost::Message,
    std::{
        collections::VecDeque,
        convert::Infallible,
        future::Future,
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
    },
    tokio_stream::Stream,
    tonic::{
        codec::{Codec, CompressionEncoding, Streaming},
        server::NamedService,
        Status,
    },
    tower::Service,
    yellowstone_grpc_proto::geyser::SubscribeRequest,
};

/// The HTTP/2 path of the `Subscribe` method.
const SUBSCRIBE_PATH: &str = "/geyser.Geyser/Subscribe";

/// The smallest cached encoding sent by reference; smaller ones are cheaper to copy.
const MIN_SHARED_BYTES: usize = 16 * 1024;

/// The copied bytes a body gathers from ready updates before it hands them to HTTP/2.
const YIELD_BYTES: usize = 32 * 1024;

/// The gRPC message prefix: one compression flag byte and a big-endian `u32` length.
const GRPC_HEADER_BYTES: usize = 5;

// Large account data is never cached, so it must always be big enough to share.
const _: () = assert!(MIN_SHARED_BYTES <= LARGE_ACCOUNT_DATA_BYTES);

/// A [`BufMut`] that copies bytes into a buffer, except a slice that is one of the shared
/// encodings, which it queues by reference.
///
/// # Examples
///
/// ```ignore
/// let mut buf = SegmentedBuf { shared: &sorted_shared, copied: &mut copied, chunks: &mut chunks };
/// update.encode_raw(&mut buf);
/// ```
struct SegmentedBuf<'a> {
    /// The encodings to share, sorted by address.
    shared: &'a [Bytes],
    /// The bytes copied since the last shared encoding.
    copied: &'a mut BytesMut,
    /// The finished chunks, in the order they go on the wire.
    chunks: &'a mut VecDeque<Bytes>,
}

impl SegmentedBuf<'_> {
    /// Finds the shared encoding a slice is.
    ///
    /// # Arguments
    ///
    /// * `src` - The slice about to be written.
    ///
    /// # Returns
    ///
    /// The part of a shared piece the slice lies in, if it is large enough to share, or [`None`].
    fn shared_for(&self, src: &[u8]) -> Option<Bytes> {
        if src.len() < MIN_SHARED_BYTES {
            return None;
        }
        let address = src.as_ptr() as usize;
        let after = self
            .shared
            .partition_point(|encoding| encoding.as_ptr() as usize <= address);
        let encoding = self.shared.get(after.checked_sub(1)?)?;
        let start = encoding.as_ptr() as usize;
        (address + src.len() <= start + encoding.len()).then(|| encoding.slice_ref(src))
    }
}

// SAFETY: every method delegates to the `BytesMut` copy buffer, which upholds the contract.
unsafe impl BufMut for SegmentedBuf<'_> {
    /// Returns the space left in the copy buffer.
    ///
    /// # Returns
    ///
    /// The copy buffer's remaining capacity.
    fn remaining_mut(&self) -> usize {
        self.copied.remaining_mut()
    }

    /// Marks bytes of the copy buffer as written.
    ///
    /// # Arguments
    ///
    /// * `cnt` - The number of bytes written.
    ///
    /// # Safety
    ///
    /// The caller must have initialised `cnt` bytes of [`SegmentedBuf::chunk_mut`].
    unsafe fn advance_mut(&mut self, cnt: usize) {
        // SAFETY: the caller upholds the `BufMut::advance_mut` contract for the copy buffer.
        unsafe { self.copied.advance_mut(cnt) }
    }

    /// Returns the writable tail of the copy buffer.
    ///
    /// # Returns
    ///
    /// The copy buffer's uninitialised tail.
    fn chunk_mut(&mut self) -> &mut UninitSlice {
        self.copied.chunk_mut()
    }

    /// Writes one byte straight into the copy buffer; a byte is never a shared encoding.
    ///
    /// # Arguments
    ///
    /// * `n` - The byte to write.
    fn put_u8(&mut self, n: u8) {
        self.copied.put_u8(n);
    }

    /// Writes a slice, queueing it by reference when it is a shared encoding.
    ///
    /// # Arguments
    ///
    /// * `src` - The bytes to write.
    fn put_slice(&mut self, src: &[u8]) {
        let Some(shared) = self.shared_for(src) else {
            self.copied.put_slice(src);
            return;
        };
        if !self.copied.is_empty() {
            self.chunks.push_back(self.copied.split().freeze());
        }
        self.chunks.push_back(shared);
    }
}

/// Appends one update as a gRPC message, sharing its large cached encodings.
///
/// # Arguments
///
/// * `update` - The update to encode.
/// * `shared` - Scratch space for the update's shared encodings.
/// * `copied` - The copy buffer the message's own bytes go to.
/// * `chunks` - The finished chunks, which get any shared encoding and the bytes before it.
///
/// # Errors
///
/// Returns [`Status::out_of_range`] when the message is longer than a gRPC length can say.
fn encode_update(
    update: &FilteredUpdate,
    shared: &mut Vec<Bytes>,
    copied: &mut BytesMut,
    chunks: &mut VecDeque<Bytes>,
) -> Result<(), Status> {
    let message_bytes = u32::try_from(update.encoded_len())
        .map_err(|_| Status::out_of_range("update is longer than a gRPC message can be"))?;
    shared.clear();
    update.prepare_shared_encodings(MIN_SHARED_BYTES, shared);
    shared.sort_unstable_by_key(|encoding| encoding.as_ptr() as usize);
    shared.dedup_by_key(|encoding| encoding.as_ptr() as usize);
    let shared_bytes: usize = shared.iter().map(Bytes::len).sum();

    copied.reserve(GRPC_HEADER_BYTES + (message_bytes as usize).saturating_sub(shared_bytes));
    copied.put_u8(0);
    copied.put_u32(message_bytes);
    update.encode_raw(&mut SegmentedBuf {
        shared,
        copied,
        chunks,
    });
    // The chunks hold what goes on the wire; anything kept here would only pin memory.
    shared.clear();
    Ok(())
}

/// Builds the trailers that end a stream with a status.
///
/// # Arguments
///
/// * `status` - The status to report.
///
/// # Returns
///
/// The `grpc-status` trailers, or `Internal` ones if the status does not fit in a header.
fn trailers_for(status: Status) -> http::HeaderMap {
    let mut trailers = http::HeaderMap::new();
    if status.add_header(&mut trailers).is_err() {
        trailers.clear();
        let _ = Status::internal("invalid status").add_header(&mut trailers);
    }
    trailers
}

/// The response body of a `Subscribe` stream: each update as a gRPC message, with large cached
/// encodings sent by reference, then the trailers.
///
/// # Examples
///
/// ```ignore
/// let body = ZeroCopyEncodeBody::new(updates);
/// let response = http::Response::new(tonic::body::Body::new(body));
/// ```
pub(crate) struct ZeroCopyEncodeBody<S> {
    /// The updates to send, ending with an error or the end of the stream.
    updates: S,
    /// Scratch space for one update's shared encodings.
    shared: Vec<Bytes>,
    /// The bytes copied since the last chunk was queued.
    copied: BytesMut,
    /// The chunks ready for HTTP/2, in wire order.
    chunks: VecDeque<Bytes>,
    /// The trailers to send once the chunks are out.
    trailers: Option<http::HeaderMap>,
    /// Whether the trailers went out.
    finished: bool,
}

impl<S> ZeroCopyEncodeBody<S> {
    /// Creates a body for a stream of updates.
    ///
    /// # Arguments
    ///
    /// * `updates` - The updates to send.
    ///
    /// # Returns
    ///
    /// A body that has sent nothing yet.
    pub(crate) fn new(updates: S) -> Self {
        Self {
            updates,
            shared: Vec::new(),
            copied: BytesMut::new(),
            chunks: VecDeque::new(),
            trailers: None,
            finished: false,
        }
    }

    /// Queues the copied bytes as one chunk.
    fn flush_copied(&mut self) {
        if !self.copied.is_empty() {
            self.chunks.push_back(self.copied.split().freeze());
        }
    }

    /// Queues what is copied, then the trailers for a status.
    ///
    /// # Arguments
    ///
    /// * `status` - The status that ends the stream.
    fn finish(&mut self, status: Status) {
        self.flush_copied();
        self.trailers = Some(trailers_for(status));
    }
}

impl<S> Body for ZeroCopyEncodeBody<S>
where
    S: Stream<Item = Result<FilteredUpdate, Status>> + Unpin,
{
    type Data = Bytes;
    type Error = Status;

    /// Returns the next chunk, gathering ready updates until a chunk is due.
    ///
    /// # Arguments
    ///
    /// * `cx` - The context of the task sending the body.
    ///
    /// # Returns
    ///
    /// A data frame, the trailers, the end of the body, or [`Poll::Pending`] if no update is
    /// ready.
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        let this = self.get_mut();
        loop {
            if let Some(chunk) = this.chunks.pop_front() {
                return Poll::Ready(Some(Ok(Frame::data(chunk))));
            }
            if let Some(trailers) = this.trailers.take() {
                this.finished = true;
                return Poll::Ready(Some(Ok(Frame::trailers(trailers))));
            }
            if this.finished {
                return Poll::Ready(None);
            }
            match Pin::new(&mut this.updates).poll_next(cx) {
                Poll::Ready(Some(Ok(update))) => {
                    if let Err(status) = encode_update(
                        &update,
                        &mut this.shared,
                        &mut this.copied,
                        &mut this.chunks,
                    ) {
                        this.finish(status);
                    } else if this.copied.len() >= YIELD_BYTES {
                        this.flush_copied();
                    }
                }
                Poll::Ready(Some(Err(status))) => this.finish(status),
                Poll::Ready(None) => this.finish(Status::ok("")),
                Poll::Pending if this.copied.is_empty() => return Poll::Pending,
                Poll::Pending => this.flush_copied(),
            }
        }
    }

    /// Tells if the trailers went out.
    ///
    /// # Returns
    ///
    /// `true` once nothing is left to send.
    fn is_end_stream(&self) -> bool {
        self.finished && self.chunks.is_empty()
    }
}

/// Wraps the generated `Geyser` server and answers uncompressed `Subscribe` streams with a
/// [`ZeroCopyEncodeBody`]. Every other request goes to the wrapped server unchanged.
///
/// # Examples
///
/// ```ignore
/// let geyser = Arc::new(grpc_service);
/// let server = GeyserServer::from_arc(Arc::clone(&geyser));
/// let service = ZeroCopySubscribe::new(server, geyser, true, max_decoding, &config.compression.send);
/// routes.add_service(service);
/// ```
#[derive(Clone)]
pub(crate) struct ZeroCopySubscribe<S, G> {
    /// The generated server, which handles everything this wrapper does not.
    inner: S,
    /// The service whose `subscribe` this wrapper calls.
    geyser: Arc<G>,
    /// Whether `Subscribe` streams take the zero-copy path.
    enabled: bool,
    /// The largest request message accepted, as on the generated server.
    max_decoding_message_size: Option<usize>,
    /// The names of the encodings the server may compress responses with.
    send_encodings: Vec<String>,
}

impl<S, G> ZeroCopySubscribe<S, G> {
    /// Wraps a generated server.
    ///
    /// # Arguments
    ///
    /// * `inner` - The generated server.
    /// * `geyser` - The service behind the generated server.
    /// * `enabled` - Whether `Subscribe` streams take the zero-copy path.
    /// * `max_decoding_message_size` - The largest request message accepted.
    /// * `send_encodings` - The encodings the server compresses responses with.
    ///
    /// # Returns
    ///
    /// The wrapped server.
    pub(crate) fn new(
        inner: S,
        geyser: Arc<G>,
        enabled: bool,
        max_decoding_message_size: Option<usize>,
        send_encodings: &[CompressionEncoding],
    ) -> Self {
        let send_encodings = send_encodings.iter().map(ToString::to_string).collect();
        Self {
            inner,
            geyser,
            enabled,
            max_decoding_message_size,
            send_encodings,
        }
    }

    /// Tells if a request takes the zero-copy path: an uncompressed `Subscribe` stream whose
    /// response the server would not compress either.
    ///
    /// # Arguments
    ///
    /// * `request` - The incoming request.
    ///
    /// # Returns
    ///
    /// `true` if this wrapper answers the request itself.
    fn handles<B>(&self, request: &http::Request<B>) -> bool {
        if !self.enabled || request.uri().path() != SUBSCRIBE_PATH {
            return false;
        }
        let headers = request.headers();
        let request_compressed = headers
            .get("grpc-encoding")
            .is_some_and(|encoding| encoding.as_bytes() != b"identity");
        let response_compressed = headers
            .get_all("grpc-accept-encoding")
            .iter()
            .filter_map(|value| value.to_str().ok())
            .flat_map(|value| value.split(','))
            .any(|name| self.send_encodings.iter().any(|sent| sent == name.trim()));
        !request_compressed && !response_compressed
    }
}

/// Answers one `Subscribe` stream with a [`ZeroCopyEncodeBody`].
///
/// # Arguments
///
/// * `geyser` - The service to subscribe on.
/// * `request` - The HTTP/2 request carrying the stream of `SubscribeRequest`s.
/// * `max_decoding_message_size` - The largest request message accepted.
///
/// # Returns
///
/// The streaming response, or a trailers-only response if `subscribe` fails.
async fn subscribe<G, B>(
    geyser: Arc<G>,
    request: http::Request<B>,
    max_decoding_message_size: Option<usize>,
) -> http::Response<tonic::body::Body>
where
    G: Geyser,
    G::SubscribeStream: Unpin,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
{
    let (parts, body) = request.into_parts();
    let mut codec = tonic_prost::ProstCodec::<FilteredUpdate, SubscribeRequest>::default();
    let requests = Streaming::new_request(codec.decoder(), body, None, max_decoding_message_size);
    let request = tonic::Request::from_http(http::Request::from_parts(parts, requests));
    match geyser.subscribe(request).await {
        Ok(response) => {
            let (metadata, updates, extensions) = response.into_parts();
            let body = tonic::body::Body::new(ZeroCopyEncodeBody::new(updates));
            let mut response = http::Response::new(body);
            *response.headers_mut() = metadata.into_headers();
            response.headers_mut().insert(
                http::header::CONTENT_TYPE,
                tonic::metadata::GRPC_CONTENT_TYPE,
            );
            *response.extensions_mut() = extensions;
            response
        }
        Err(status) => status.into_http(),
    }
}

impl<S, G, B> Service<http::Request<B>> for ZeroCopySubscribe<S, G>
where
    S: Service<http::Request<B>, Response = http::Response<tonic::body::Body>, Error = Infallible>,
    S::Future: Send + 'static,
    G: Geyser,
    G::SubscribeStream: Unpin,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;

    /// Waits until the generated server is ready.
    ///
    /// # Arguments
    ///
    /// * `cx` - The context of the calling task.
    ///
    /// # Returns
    ///
    /// The generated server's readiness.
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        self.inner.poll_ready(cx)
    }

    /// Answers an uncompressed `Subscribe` stream itself, and passes anything else on.
    ///
    /// # Arguments
    ///
    /// * `request` - The incoming request.
    ///
    /// # Returns
    ///
    /// The future of the response.
    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        if !self.handles(&request) {
            return Box::pin(self.inner.call(request));
        }
        let geyser = Arc::clone(&self.geyser);
        let max_decoding_message_size = self.max_decoding_message_size;
        Box::pin(async move { Ok(subscribe(geyser, request, max_decoding_message_size).await) })
    }
}

impl<S: NamedService, G> NamedService for ZeroCopySubscribe<S, G> {
    const NAME: &'static str = S::NAME;
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::plugin::filter::{
            fixtures,
            message::{FilteredUpdateAccount, FilteredUpdateOneof},
            FilterAccountsDataSlice,
        },
        http_body_util::BodyExt,
        prost_types::Timestamp,
        solana_pubkey::Pubkey,
    };

    fn account_update(data_bytes: usize) -> FilteredUpdate {
        let data = (0..data_bytes).map(|byte| byte as u8).collect();
        let account =
            fixtures::account_info(Pubkey::new_unique(), Pubkey::new_unique(), data, 1, None);
        FilteredUpdate {
            filters: Default::default(),
            message: FilteredUpdateOneof::Account(FilteredUpdateAccount {
                account: fixtures::message_account(account),
                data_slice: Default::default(),
            }),
            created_at: Timestamp::default(),
        }
    }

    fn grpc_message(update: &FilteredUpdate) -> Vec<u8> {
        let message = update.encode_to_vec();
        let mut framed = vec![0];
        framed.extend_from_slice(&(message.len() as u32).to_be_bytes());
        framed.extend_from_slice(&message);
        framed
    }

    fn encode(update: &FilteredUpdate) -> Vec<Bytes> {
        let (mut shared, mut copied, mut chunks) = (Vec::new(), BytesMut::new(), VecDeque::new());
        encode_update(update, &mut shared, &mut copied, &mut chunks).expect("encode");
        assert!(
            shared.is_empty(),
            "no shared bytes stay pinned after an update"
        );
        chunks.push_back(copied.freeze());
        chunks.into()
    }

    #[test]
    fn an_account_just_under_the_large_size_is_sent_from_its_cached_encoding() {
        let update = account_update(LARGE_ACCOUNT_DATA_BYTES - 10);

        let chunks = encode(&update);

        assert_eq!(chunks.concat(), grpc_message(&update));
        let FilteredUpdateOneof::Account(account) = &update.message else {
            unreachable!()
        };
        let cached = account.account.account.get_pre_encoded().expect("cached");
        assert!(chunks.iter().any(|chunk| chunk.as_ptr() == cached.as_ptr()));
    }

    #[test]
    fn a_large_account_is_sent_from_its_data_without_being_cached() {
        let update = account_update(LARGE_ACCOUNT_DATA_BYTES * 4);

        let chunks = encode(&update);

        assert_eq!(chunks.concat(), grpc_message(&update));
        let FilteredUpdateOneof::Account(account) = &update.message else {
            unreachable!()
        };
        let data = &account.account.account.data;
        assert!(chunks.iter().any(|chunk| chunk.as_ptr() == data.as_ptr()));
        assert!(account.account.account.get_pre_encoded().is_none());
    }

    #[test]
    fn a_large_slice_of_account_data_is_sent_by_reference() {
        let mut update = account_update(LARGE_ACCOUNT_DATA_BYTES * 4);
        let start = 100;
        let end = start + LARGE_ACCOUNT_DATA_BYTES * 2;
        let FilteredUpdateOneof::Account(account) = &mut update.message else {
            unreachable!()
        };
        account.data_slice =
            FilterAccountsDataSlice::new_unchecked(std::iter::once(start..end).collect());
        let data = account.account.account.data.clone();

        let chunks = encode(&update);

        assert_eq!(chunks.concat(), grpc_message(&update));
        assert!(chunks
            .iter()
            .any(|chunk| chunk.as_ptr() == data[start..].as_ptr()));
    }

    #[test]
    fn a_small_account_is_copied() {
        let update = account_update(100);

        let chunks = encode(&update);

        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks.concat(), grpc_message(&update));
    }

    #[test]
    fn a_request_that_accepts_a_send_encoding_is_left_to_tonic() {
        let service =
            ZeroCopySubscribe::new((), Arc::new(()), true, None, &[CompressionEncoding::Gzip]);
        let request = |accept: Option<&str>| {
            let mut request = http::Request::builder().uri(SUBSCRIBE_PATH);
            if let Some(accept) = accept {
                request = request.header("grpc-accept-encoding", accept);
            }
            request.body(()).expect("request")
        };

        assert!(service.handles(&request(None)));
        assert!(service.handles(&request(Some("identity, zstd"))));
        assert!(!service.handles(&request(Some("identity, gzip"))));
    }

    #[tokio::test]
    async fn the_body_sends_every_update_in_order_then_an_ok_status() {
        let (large, small) = (account_update(MIN_SHARED_BYTES * 2), account_update(10));
        let expected = [grpc_message(&large), grpc_message(&small)].concat();
        let updates = tokio_stream::iter([Ok(large), Ok(small)]);

        let collected = ZeroCopyEncodeBody::new(updates)
            .collect()
            .await
            .expect("collect");

        let trailers = collected.trailers().expect("trailers").clone();
        assert_eq!(collected.to_bytes(), expected);
        assert_eq!(trailers["grpc-status"], "0");
    }

    #[tokio::test]
    async fn the_body_ends_with_the_status_of_a_stream_error() {
        let update = account_update(10);
        let expected = grpc_message(&update);
        let updates = tokio_stream::iter([Ok(update), Err(Status::internal("broken"))]);

        let collected = ZeroCopyEncodeBody::new(updates)
            .collect()
            .await
            .expect("collect");

        let trailers = collected.trailers().expect("trailers").clone();
        assert_eq!(collected.to_bytes(), expected);
        assert_eq!(trailers["grpc-status"], "13");
    }
}
