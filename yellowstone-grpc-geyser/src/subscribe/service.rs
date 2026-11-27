/*
 * Copyright 2019 gRPC authors.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */

//! The plugin's own Subscribe handler inside tonic's server, and the service that sends it every
//! Subscribe call. The handler answers in the steps of tonic 0.14.9's `Grpc::streaming`
//! (`src/server/grpc.rs`), with the negotiation ported in `compression::negotiation`.

use {
    super::{
        head::build_response_head,
        response_body::{GroupSharingSettings, StreamFraming, SubscribeResponseBody},
    },
    crate::{
        body::block_shape::BlockShapeWriteSettings,
        compression::{
            check_request_encoding, negotiate_response_encoding, CompressingEncoding,
            EnabledEncodings, ResponseEncoding, WholeMessageCompressor,
        },
        config::SubscribeBodyKind,
        grpc::GrpcService,
        plugin::proto::geyser_server::{GeyserServer, SERVICE_NAME},
        subscribe::entry::SubscribeQueueEntry,
    },
    std::{
        convert::Infallible,
        sync::Arc,
        task::{Context, Poll},
    },
    tonic::{
        body::Body,
        codec::{Codec, EncodeBody, SingleMessageCompressionOverride, Streaming},
        codegen::{Body as HttpBody, BoxFuture, Service, StdError},
        server::NamedService,
        Request,
    },
    tonic_prost::ProstCodec,
    yellowstone_grpc_proto::geyser::SubscribeRequest,
};

/// The path of every Subscribe call.
pub const SUBSCRIBE_PATH: &str = "/geyser.Geyser/Subscribe";

/// The settings tonic's handler takes from `GeyserServer` for a Subscribe call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubscribeCallSettings {
    /// The request encodings accepted besides identity, from `compression.accept`.
    pub accept_encodings: EnabledEncodings,
    /// The response encodings offered besides identity, from `compression.send`.
    pub send_encodings: EnabledEncodings,
    /// Whether zstd goes first when the client accepts it, from the build's compression values.
    pub prefer_zstd: bool,
    /// The largest request message, decoded; `None` keeps tonic's 4 MiB.
    pub max_decoding_message_size: Option<usize>,
    /// The largest response message, encoded; `None` keeps tonic's `usize::MAX`.
    pub max_encoding_message_size: Option<usize>,
    /// Whose body writes the stream: tonic's encoder or the plugin's, from `subscribe_body`.
    pub subscribe_body: SubscribeBodyKind,
    /// The plugin's body copies pieces under it and references bigger shared segments.
    pub copy_threshold_bytes: usize,
}

/// The plugin's Subscribe handler: tonic's steps around [`GrpcService::start_subscribe`].
pub struct SubscribeService {
    grpc_service: Arc<GrpcService>,
    call_settings: SubscribeCallSettings,
    stream_compressor: Arc<dyn WholeMessageCompressor>,
    /// The plugin's compression groups, from `compression.mode: shared`; `None` compresses each
    /// stream's messages for that stream alone.
    group_sharing: Option<GroupSharingSettings>,
    /// The plugin's Block shape budget and waiter backstop, which every body writes within.
    block_shape_settings: BlockShapeWriteSettings,
}

impl SubscribeService {
    pub const fn new(
        grpc_service: Arc<GrpcService>,
        call_settings: SubscribeCallSettings,
        stream_compressor: Arc<dyn WholeMessageCompressor>,
        group_sharing: Option<GroupSharingSettings>,
        block_shape_settings: BlockShapeWriteSettings,
    ) -> Self {
        Self {
            grpc_service,
            call_settings,
            stream_compressor,
            group_sharing,
            block_shape_settings,
        }
    }

    /// Answers one Subscribe call in the steps of tonic's `Grpc::streaming`. An unsupported
    /// request encoding or a failed setup gives tonic's trailers-only status.
    pub async fn serve<RequestBody>(
        &self,
        http_request: http::Request<RequestBody>,
    ) -> http::Response<Body>
    where
        RequestBody: HttpBody + Send + 'static,
        RequestBody::Error: Into<StdError> + Send,
    {
        let response_encoding = negotiate_response_encoding(
            http_request.headers(),
            self.call_settings.send_encodings,
            self.call_settings.prefer_zstd,
        );
        let request_encoding = match check_request_encoding(
            http_request.headers(),
            self.call_settings.accept_encodings,
        ) {
            Ok(request_encoding) => request_encoding,
            Err(encoding_status) => return encoding_status.into_http(),
        };
        let mut subscribe_codec = ProstCodec::<SubscribeQueueEntry, SubscribeRequest>::default();
        let subscribe_request = Request::from_http(http_request.map(|request_body| {
            Streaming::new_request(
                subscribe_codec.decoder(),
                request_body,
                request_encoding.and_then(ResponseEncoding::tonic_encoding),
                self.call_settings.max_decoding_message_size,
            )
        }));
        let setup_response = match self
            .grpc_service
            .start_subscribe(subscribe_request, response_encoding)
            .await
        {
            Ok(setup_response) => setup_response,
            Err(setup_status) => return setup_status.into_http(),
        };
        let (response_parts, response_stream) =
            build_response_head(setup_response, response_encoding);
        let response_body = match self.call_settings.subscribe_body {
            SubscribeBodyKind::Tonic => Body::new(EncodeBody::new_server(
                subscribe_codec.encoder(),
                response_stream,
                response_encoding.tonic_encoding(),
                SingleMessageCompressionOverride::default(),
                self.call_settings.max_encoding_message_size,
            )),
            SubscribeBodyKind::Plugin => Body::new(SubscribeResponseBody::new(
                response_stream,
                self.stream_framing(response_encoding),
                self.call_settings.copy_threshold_bytes,
                self.call_settings
                    .max_encoding_message_size
                    .unwrap_or(usize::MAX),
                self.block_shape_settings.clone(),
            )),
        };
        http::Response::from_parts(response_parts, response_body)
    }

    fn stream_framing(&self, response_encoding: ResponseEncoding) -> StreamFraming {
        match CompressingEncoding::of_response_encoding(response_encoding) {
            None => StreamFraming::Identity,
            Some(compressing_encoding) => StreamFraming::Compressed {
                compressing_encoding,
                stream_compressor: Arc::clone(&self.stream_compressor),
                group_sharing: self.group_sharing.clone(),
            },
        }
    }
}

/// The service registered as `geyser.Geyser`: Subscribe calls go to [`SubscribeService`], every
/// other path to the generated server, so an unknown method still reaches its fallback.
#[derive(Clone)]
pub struct GeyserServiceSplitter {
    subscribe_service: Arc<SubscribeService>,
    geyser_server: GeyserServer<GrpcService>,
}

impl GeyserServiceSplitter {
    pub fn new(
        subscribe_service: SubscribeService,
        geyser_server: GeyserServer<GrpcService>,
    ) -> Self {
        Self {
            subscribe_service: Arc::new(subscribe_service),
            geyser_server,
        }
    }
}

impl NamedService for GeyserServiceSplitter {
    const NAME: &'static str = SERVICE_NAME;
}

impl<RequestBody> Service<http::Request<RequestBody>> for GeyserServiceSplitter
where
    RequestBody: HttpBody + Send + 'static,
    RequestBody::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, task_context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        <GeyserServer<GrpcService> as Service<http::Request<RequestBody>>>::poll_ready(
            &mut self.geyser_server,
            task_context,
        )
    }

    fn call(&mut self, http_request: http::Request<RequestBody>) -> Self::Future {
        if http_request.uri().path() != SUBSCRIBE_PATH {
            return self.geyser_server.call(http_request);
        }
        let subscribe_service = Arc::clone(&self.subscribe_service);
        Box::pin(async move { Ok(subscribe_service.serve(http_request).await) })
    }
}
