use std::{error::Error, sync::Arc, task::{Context, Poll}};

use bytes::Bytes;
use hyper::body::Body;
use tonic::{codec::{CompressionEncoding, EnabledCompressionEncodings}, service::interceptor::InterceptedService};
use tower_service::Service;

use std::future::Future;
use std::pin::Pin;
type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;
use http;

use crate::plugin::proto::geyser_server::Geyser;

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


pub struct ZeroCopyBuf {
    header: [u8; 0],
    bytes: Bytes,
}

pub enum GeyserBody {
    TonicBody(tonic::body::Body),
}

impl<T, B> Service<http::Request<B>> for GeyserServer<T>
where
    T: Geyser,
    B: Body + std::marker::Send + 'static,
    B::Error: Into<StdError> + std::marker::Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
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
                    let method = SubscribeSvc(inner);
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
                    Ok(res)
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
                    Ok(res)
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
                    Ok(res)
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
                    Ok(res)
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
                    Ok(res)
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
                    Ok(res)
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
                    Ok(res)
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
                    Ok(res)
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
                    Ok(res)
                };
                Box::pin(fut)
            }
            _ => {
                Box::pin(async move {
                    let mut response = http::Response::new(
                        tonic::body::Body::default(),
                    );
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