use std::{error::Error, sync::Arc, task::{Context, Poll}};

use bytes::{Buf, Bytes};
use http_body_util::{combinators::UnsyncBoxBody, BodyExt};
use hyper::body::Body;
use prost::Message as ProstMessage;
use tonic::{codec::{CompressionEncoding, EnabledCompressionEncodings}, service::interceptor::InterceptedService};
use tonic::server::StreamingService;
use tower_service::Service;

use std::future::Future;
use std::pin::Pin;
type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;
use http;

use crate::{
    codec::{encode::EncodeBody, Encode, EncodeOutput},
    plugin::{filter::message::FilteredUpdate, proto::geyser_server::Geyser},
};

pub type BoxFuture<T, E> = self::Pin<Box<dyn self::Future<Output = Result<T, E>> + Send + 'static>>;

#[derive(Debug)]
pub struct GeyserServer<T> {
    inner: Arc<T>,
    accept_compression_encodings: EnabledCompressionEncodings,
    send_compression_encodings: EnabledCompressionEncodings,
    max_decoding_message_size: Option<usize>,
    max_encoding_message_size: Option<usize>,
}
impl<T> GeyserServer<T> {
    pub fn new(inner: T) -> Self {
        Self::from_arc(Arc::new(inner))
    }
    pub fn from_arc(inner: Arc<T>) -> Self {
        Self {
            inner,
            accept_compression_encodings: Default::default(),
            send_compression_encodings: Default::default(),
            max_decoding_message_size: None,
            max_encoding_message_size: None,
        }
    }
    #[allow(dead_code)]
    pub fn with_interceptor<F>(
        inner: T,
        interceptor: F,
    ) -> InterceptedService<Self, F>
    where
        F: tonic::service::Interceptor,
    {
        InterceptedService::new(Self::new(inner), interceptor)
    }
    /// Enable decompressing requests with the given encoding.
    #[must_use]
    pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
        self.accept_compression_encodings.enable(encoding);
        self
    }
    /// Compress responses with the given encoding, if the client supports it.
    #[must_use]
    pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
        self.send_compression_encodings.enable(encoding);
        self
    }
    /// Limits the maximum size of a decoded message.
    ///
    /// Default: `4MB`
    #[must_use]
    pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
        self.max_decoding_message_size = Some(limit);
        self
    }
    /// Limits the maximum size of an encoded message.
    ///
    /// Default: `usize::MAX`
    #[must_use]
    pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
        self.max_encoding_message_size = Some(limit);
        self
    }
}



pub struct GeyserBody {
    inner: UnsyncBoxBody<Bytes, tonic::Status>,
}

impl GeyserBody {
    fn new<B>(body: B) -> Self
    where
        B: http_body::Body<Data = Bytes, Error = tonic::Status> + Send + 'static,
    {
        Self {
            inner: body.boxed_unsync(),
        }
    }
}

impl Default for GeyserBody {
    fn default() -> Self {
        Self::new(http_body_util::Empty::<Bytes>::new().map_err(|err| match err {}))
    }
}

impl http_body::Body for GeyserBody {
    type Data = Bytes;
    type Error = tonic::Status;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        std::pin::Pin::new(&mut self.inner).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

fn wrap_tonic_body<B>(body: B) -> GeyserBody
where
    B: http_body::Body<Data = Bytes, Error = tonic::Status> + Send + 'static,
{
    GeyserBody::new(body)
}

#[derive(Debug, Clone, Copy, Default)]
struct SubscribeEncoder;

impl Encode for SubscribeEncoder {
    type Item = FilteredUpdate;
    type Error = tonic::Status;

    fn encode(
        &mut self,
        item: Self::Item,
        dst: &mut bytes::BytesMut,
    ) -> Result<EncodeOutput, Self::Error> {
        item.encode(dst)
            .map_err(|error| tonic::Status::internal(format!("Error encoding: {error}")))?;
        Ok(EncodeOutput::Buffer)
    }
}

fn accepted_response_encoding(
    headers: &http::HeaderMap,
    enabled: EnabledCompressionEncodings,
) -> Option<CompressionEncoding> {
    let value = headers.get("grpc-accept-encoding")?.to_str().ok()?;
    for encoding in value.split(',').map(|s| s.trim()) {
        match encoding {
            "gzip" if enabled.is_enabled(CompressionEncoding::Gzip) => {
                return Some(CompressionEncoding::Gzip)
            }
            "zstd" if enabled.is_enabled(CompressionEncoding::Zstd) => {
                return Some(CompressionEncoding::Zstd)
            }
            _ => {}
        }
    }
    None
}

fn request_encoding_if_supported(
    headers: &http::HeaderMap,
    enabled: EnabledCompressionEncodings,
) -> Result<Option<CompressionEncoding>, tonic::Status> {
    let Some(value) = headers.get("grpc-encoding") else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .map_err(|_| tonic::Status::unimplemented("invalid grpc-encoding header"))?;

    match value {
        "identity" => Ok(None),
        "gzip" if enabled.is_enabled(CompressionEncoding::Gzip) => {
            Ok(Some(CompressionEncoding::Gzip))
        }
        "zstd" if enabled.is_enabled(CompressionEncoding::Zstd) => {
            Ok(Some(CompressionEncoding::Zstd))
        }
        other => Err(tonic::Status::unimplemented(format!(
            "Content is compressed with `{other}` which isn't supported"
        ))),
    }
}

fn insert_response_encoding_header(
    headers: &mut http::HeaderMap,
    encoding: CompressionEncoding,
) {
    let value = match encoding {
        CompressionEncoding::Gzip => "gzip",
        CompressionEncoding::Zstd => "zstd",
        _ => return,
    };
    headers.insert(
        "grpc-encoding",
        http::HeaderValue::from_static(value),
    );
}

impl<T, B> Service<http::Request<B>> for GeyserServer<T>
where
    T: Geyser,
    B: Body + std::marker::Send + 'static,
    B::Error: Into<StdError> + std::marker::Send + 'static,
{
    type Response = http::Response<GeyserBody>;
    type Error = std::convert::Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        match req.uri().path() {
            "/geyser.Geyser/Subscribe" => {
                #[allow(non_camel_case_types)]
                struct SubscribeSvc<T: Geyser>(pub Arc<T>);
                impl<
                    T: Geyser,
                > tonic::server::StreamingService<
                    yellowstone_grpc_proto::geyser::SubscribeRequest,
                > for SubscribeSvc<T> {
                    type Response = crate::plugin::filter::message::FilteredUpdate;
                    type ResponseStream = T::SubscribeStream;
                    type Future = BoxFuture<
                        tonic::Response<Self::ResponseStream>,
                        tonic::Status,
                    >;
                    fn call(
                        &mut self,
                        request: tonic::Request<
                            tonic::Streaming<
                                yellowstone_grpc_proto::geyser::SubscribeRequest,
                            >,
                        >,
                    ) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        let fut = async move {
                            <T as Geyser>::subscribe(&inner, request).await
                        };
                        Box::pin(fut)
                    }
                }
                let accept_compression_encodings = self.accept_compression_encodings;
                let send_compression_encodings = self.send_compression_encodings;
                let max_decoding_message_size = self.max_decoding_message_size;
                let max_encoding_message_size = self.max_encoding_message_size;
                let inner = self.inner.clone();
                let fut = async move {
                    let mut method = SubscribeSvc(inner);
                    let accept_encoding =
                        accepted_response_encoding(req.headers(), send_compression_encodings);
                    let request_encoding = match request_encoding_if_supported(
                        req.headers(),
                        accept_compression_encodings,
                    ) {
                        Ok(encoding) => encoding,
                        Err(status) => return Ok(status.into_http::<GeyserBody>()),
                    };

                    let request = req.map(|body| {
                        tonic::Streaming::new_request(
                            tonic_prost::ProstDecoder::<
                                yellowstone_grpc_proto::geyser::SubscribeRequest,
                            >::default(),
                            body,
                            request_encoding,
                            max_decoding_message_size,
                        )
                    });
                    let request = tonic::Request::from_http(request);

                    let response = match method.call(request).await {
                        Ok(response) => response,
                        Err(status) => return Ok(status.into_http::<GeyserBody>()),
                    };

                    let (metadata, body, extensions) = response.into_parts();
                    let mut response = http::Response::new(GeyserBody::default());
                    *response.version_mut() = http::Version::HTTP_2;
                    *response.headers_mut() = metadata.into_headers();
                    *response.extensions_mut() = extensions;

                    response.headers_mut().insert(
                        http::header::CONTENT_TYPE,
                        tonic::metadata::GRPC_CONTENT_TYPE,
                    );
                    if let Some(encoding) = accept_encoding {
                        insert_response_encoding_header(response.headers_mut(), encoding);
                    }

                    let body = EncodeBody::new_server(
                        SubscribeEncoder,
                        body,
                        accept_encoding,
                        tonic::codec::SingleMessageCompressionOverride::default(),
                        max_encoding_message_size,
                    );
                    *response.body_mut() = GeyserBody::new(body);

                    Ok(response)
                };
                Box::pin(fut)

                

            }
            "/geyser.Geyser/SubscribeDeshred" => {
                #[allow(non_camel_case_types)]
                struct SubscribeDeshredSvc<T: Geyser>(pub Arc<T>);
                impl<
                    T: Geyser,
                > tonic::server::StreamingService<
                    yellowstone_grpc_proto::geyser::SubscribeDeshredRequest,
                > for SubscribeDeshredSvc<T> {
                    type Response = yellowstone_grpc_proto::geyser::SubscribeUpdateDeshred;
                    type ResponseStream = T::SubscribeDeshredStream;
                    type Future = BoxFuture<
                        tonic::Response<Self::ResponseStream>,
                        tonic::Status,
                    >;
                    fn call(
                        &mut self,
                        request: tonic::Request<
                            tonic::Streaming<
                                yellowstone_grpc_proto::geyser::SubscribeDeshredRequest,
                            >,
                        >,
                    ) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        let fut = async move {
                            <T as Geyser>::subscribe_deshred(&inner, request).await
                        };
                        Box::pin(fut)
                    }
                }
                let accept_compression_encodings = self.accept_compression_encodings;
                let send_compression_encodings = self.send_compression_encodings;
                let max_decoding_message_size = self.max_decoding_message_size;
                let max_encoding_message_size = self.max_encoding_message_size;
                let inner = self.inner.clone();
                let fut = async move {
                    let method = SubscribeDeshredSvc(inner);
                    let codec = tonic_prost::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec)
                        .apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        )
                        .apply_max_message_size_config(
                            max_decoding_message_size,
                            max_encoding_message_size,
                        );
                    let res = grpc.streaming(method, req).await;
                    Ok(res.map(wrap_tonic_body))
                };
                Box::pin(fut)
            }
            "/geyser.Geyser/SubscribeReplayInfo" => {
                #[allow(non_camel_case_types)]
                struct SubscribeReplayInfoSvc<T: Geyser>(pub Arc<T>);
                impl<
                    T: Geyser,
                > tonic::server::UnaryService<
                    yellowstone_grpc_proto::geyser::SubscribeReplayInfoRequest,
                > for SubscribeReplayInfoSvc<T> {
                    type Response = yellowstone_grpc_proto::geyser::SubscribeReplayInfoResponse;
                    type Future = BoxFuture<
                        tonic::Response<Self::Response>,
                        tonic::Status,
                    >;
                    fn call(
                        &mut self,
                        request: tonic::Request<
                            yellowstone_grpc_proto::geyser::SubscribeReplayInfoRequest,
                        >,
                    ) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        let fut = async move {
                            <T as Geyser>::subscribe_first_available_slot(
                                    &inner,
                                    request,
                                )
                                .await
                        };
                        Box::pin(fut)
                    }
                }
                let accept_compression_encodings = self.accept_compression_encodings;
                let send_compression_encodings = self.send_compression_encodings;
                let max_decoding_message_size = self.max_decoding_message_size;
                let max_encoding_message_size = self.max_encoding_message_size;
                let inner = self.inner.clone();
                let fut = async move {
                    let method = SubscribeReplayInfoSvc(inner);
                    let codec = tonic_prost::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec)
                        .apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        )
                        .apply_max_message_size_config(
                            max_decoding_message_size,
                            max_encoding_message_size,
                        );
                    let res = grpc.unary(method, req).await;
                    Ok(res.map(wrap_tonic_body))
                };
                Box::pin(fut)
            }
            "/geyser.Geyser/Ping" => {
                #[allow(non_camel_case_types)]
                struct PingSvc<T: Geyser>(pub Arc<T>);
                impl<
                    T: Geyser,
                > tonic::server::UnaryService<
                    yellowstone_grpc_proto::geyser::PingRequest,
                > for PingSvc<T> {
                    type Response = yellowstone_grpc_proto::geyser::PongResponse;
                    type Future = BoxFuture<
                        tonic::Response<Self::Response>,
                        tonic::Status,
                    >;
                    fn call(
                        &mut self,
                        request: tonic::Request<
                            yellowstone_grpc_proto::geyser::PingRequest,
                        >,
                    ) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        let fut = async move {
                            <T as Geyser>::ping(&inner, request).await
                        };
                        Box::pin(fut)
                    }
                }
                let accept_compression_encodings = self.accept_compression_encodings;
                let send_compression_encodings = self.send_compression_encodings;
                let max_decoding_message_size = self.max_decoding_message_size;
                let max_encoding_message_size = self.max_encoding_message_size;
                let inner = self.inner.clone();
                let fut = async move {
                    let method = PingSvc(inner);
                    let codec = tonic_prost::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec)
                        .apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        )
                        .apply_max_message_size_config(
                            max_decoding_message_size,
                            max_encoding_message_size,
                        );
                    let res = grpc.unary(method, req).await;
                    Ok(res.map(wrap_tonic_body))
                };
                Box::pin(fut)
            }
            "/geyser.Geyser/GetLatestBlockhash" => {
                #[allow(non_camel_case_types)]
                struct GetLatestBlockhashSvc<T: Geyser>(pub Arc<T>);
                impl<
                    T: Geyser,
                > tonic::server::UnaryService<
                    yellowstone_grpc_proto::geyser::GetLatestBlockhashRequest,
                > for GetLatestBlockhashSvc<T> {
                    type Response = yellowstone_grpc_proto::geyser::GetLatestBlockhashResponse;
                    type Future = BoxFuture<
                        tonic::Response<Self::Response>,
                        tonic::Status,
                    >;
                    fn call(
                        &mut self,
                        request: tonic::Request<
                            yellowstone_grpc_proto::geyser::GetLatestBlockhashRequest,
                        >,
                    ) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        let fut = async move {
                            <T as Geyser>::get_latest_blockhash(&inner, request).await
                        };
                        Box::pin(fut)
                    }
                }
                let accept_compression_encodings = self.accept_compression_encodings;
                let send_compression_encodings = self.send_compression_encodings;
                let max_decoding_message_size = self.max_decoding_message_size;
                let max_encoding_message_size = self.max_encoding_message_size;
                let inner = self.inner.clone();
                let fut = async move {
                    let method = GetLatestBlockhashSvc(inner);
                    let codec = tonic_prost::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec)
                        .apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        )
                        .apply_max_message_size_config(
                            max_decoding_message_size,
                            max_encoding_message_size,
                        );
                    let res = grpc.unary(method, req).await;
                    Ok(res.map(wrap_tonic_body))
                };
                Box::pin(fut)
            }
            "/geyser.Geyser/GetBlockHeight" => {
                #[allow(non_camel_case_types)]
                struct GetBlockHeightSvc<T: Geyser>(pub Arc<T>);
                impl<
                    T: Geyser,
                > tonic::server::UnaryService<
                    yellowstone_grpc_proto::geyser::GetBlockHeightRequest,
                > for GetBlockHeightSvc<T> {
                    type Response = yellowstone_grpc_proto::geyser::GetBlockHeightResponse;
                    type Future = BoxFuture<
                        tonic::Response<Self::Response>,
                        tonic::Status,
                    >;
                    fn call(
                        &mut self,
                        request: tonic::Request<
                            yellowstone_grpc_proto::geyser::GetBlockHeightRequest,
                        >,
                    ) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        let fut = async move {
                            <T as Geyser>::get_block_height(&inner, request).await
                        };
                        Box::pin(fut)
                    }
                }
                let accept_compression_encodings = self.accept_compression_encodings;
                let send_compression_encodings = self.send_compression_encodings;
                let max_decoding_message_size = self.max_decoding_message_size;
                let max_encoding_message_size = self.max_encoding_message_size;
                let inner = self.inner.clone();
                let fut = async move {
                    let method = GetBlockHeightSvc(inner);
                    let codec = tonic_prost::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec)
                        .apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        )
                        .apply_max_message_size_config(
                            max_decoding_message_size,
                            max_encoding_message_size,
                        );
                    let res = grpc.unary(method, req).await;
                    Ok(res.map(wrap_tonic_body))
                };
                Box::pin(fut)
            }
            "/geyser.Geyser/GetSlot" => {
                #[allow(non_camel_case_types)]
                struct GetSlotSvc<T: Geyser>(pub Arc<T>);
                impl<
                    T: Geyser,
                > tonic::server::UnaryService<
                    yellowstone_grpc_proto::geyser::GetSlotRequest,
                > for GetSlotSvc<T> {
                    type Response = yellowstone_grpc_proto::geyser::GetSlotResponse;
                    type Future = BoxFuture<
                        tonic::Response<Self::Response>,
                        tonic::Status,
                    >;
                    fn call(
                        &mut self,
                        request: tonic::Request<
                            yellowstone_grpc_proto::geyser::GetSlotRequest,
                        >,
                    ) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        let fut = async move {
                            <T as Geyser>::get_slot(&inner, request).await
                        };
                        Box::pin(fut)
                    }
                }
                let accept_compression_encodings = self.accept_compression_encodings;
                let send_compression_encodings = self.send_compression_encodings;
                let max_decoding_message_size = self.max_decoding_message_size;
                let max_encoding_message_size = self.max_encoding_message_size;
                let inner = self.inner.clone();
                let fut = async move {
                    let method = GetSlotSvc(inner);
                    let codec = tonic_prost::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec)
                        .apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        )
                        .apply_max_message_size_config(
                            max_decoding_message_size,
                            max_encoding_message_size,
                        );
                    let res = grpc.unary(method, req).await;
                    Ok(res.map(wrap_tonic_body))
                };
                Box::pin(fut)
            }
            "/geyser.Geyser/IsBlockhashValid" => {
                #[allow(non_camel_case_types)]
                struct IsBlockhashValidSvc<T: Geyser>(pub Arc<T>);
                impl<
                    T: Geyser,
                > tonic::server::UnaryService<
                    yellowstone_grpc_proto::geyser::IsBlockhashValidRequest,
                > for IsBlockhashValidSvc<T> {
                    type Response = yellowstone_grpc_proto::geyser::IsBlockhashValidResponse;
                    type Future = BoxFuture<
                        tonic::Response<Self::Response>,
                        tonic::Status,
                    >;
                    fn call(
                        &mut self,
                        request: tonic::Request<
                            yellowstone_grpc_proto::geyser::IsBlockhashValidRequest,
                        >,
                    ) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        let fut = async move {
                            <T as Geyser>::is_blockhash_valid(&inner, request).await
                        };
                        Box::pin(fut)
                    }
                }
                let accept_compression_encodings = self.accept_compression_encodings;
                let send_compression_encodings = self.send_compression_encodings;
                let max_decoding_message_size = self.max_decoding_message_size;
                let max_encoding_message_size = self.max_encoding_message_size;
                let inner = self.inner.clone();
                let fut = async move {
                    let method = IsBlockhashValidSvc(inner);
                    let codec = tonic_prost::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec)
                        .apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        )
                        .apply_max_message_size_config(
                            max_decoding_message_size,
                            max_encoding_message_size,
                        );
                    let res = grpc.unary(method, req).await;
                    Ok(res.map(wrap_tonic_body))
                };
                Box::pin(fut)
            }
            "/geyser.Geyser/GetVersion" => {
                #[allow(non_camel_case_types)]
                struct GetVersionSvc<T: Geyser>(pub Arc<T>);
                impl<
                    T: Geyser,
                > tonic::server::UnaryService<
                    yellowstone_grpc_proto::geyser::GetVersionRequest,
                > for GetVersionSvc<T> {
                    type Response = yellowstone_grpc_proto::geyser::GetVersionResponse;
                    type Future = BoxFuture<
                        tonic::Response<Self::Response>,
                        tonic::Status,
                    >;
                    fn call(
                        &mut self,
                        request: tonic::Request<
                            yellowstone_grpc_proto::geyser::GetVersionRequest,
                        >,
                    ) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        let fut = async move {
                            <T as Geyser>::get_version(&inner, request).await
                        };
                        Box::pin(fut)
                    }
                }
                let accept_compression_encodings = self.accept_compression_encodings;
                let send_compression_encodings = self.send_compression_encodings;
                let max_decoding_message_size = self.max_decoding_message_size;
                let max_encoding_message_size = self.max_encoding_message_size;
                let inner = self.inner.clone();
                let fut = async move {
                    let method = GetVersionSvc(inner);
                    let codec = tonic_prost::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec)
                        .apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        )
                        .apply_max_message_size_config(
                            max_decoding_message_size,
                            max_encoding_message_size,
                        );
                    let res = grpc.unary(method, req).await;
                    Ok(res.map(wrap_tonic_body))
                };
                Box::pin(fut)
            }
            _ => {
                Box::pin(async move {
                    let mut response = http::Response::new(GeyserBody::default());
                    let headers = response.headers_mut();
                    headers
                        .insert(
                            tonic::Status::GRPC_STATUS,
                            (tonic::Code::Unimplemented as i32).into(),
                        );
                    headers
                        .insert(
                            http::header::CONTENT_TYPE,
                            tonic::metadata::GRPC_CONTENT_TYPE,
                        );
                    Ok(response)
                })
            }
        }
    }
}
impl<T> Clone for GeyserServer<T> {
    fn clone(&self) -> Self {
        let inner = self.inner.clone();
        Self {
            inner,
            accept_compression_encodings: self.accept_compression_encodings,
            send_compression_encodings: self.send_compression_encodings,
            max_decoding_message_size: self.max_decoding_message_size,
            max_encoding_message_size: self.max_encoding_message_size,
        }
    }
}
/// Generated gRPC service name
pub const SERVICE_NAME: &str = "geyser.Geyser";
impl<T> tonic::server::NamedService for GeyserServer<T> {
    const NAME: &'static str = SERVICE_NAME;
}
