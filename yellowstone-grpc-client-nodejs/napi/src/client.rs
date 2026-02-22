// GrpcClient: A persistent, reusable gRPC client
//
// ## Problem:
// The yellowstone-grpc library's builder returns `GeyserGrpcClient<impl Interceptor>`,
// which is an opaque type that cannot be directly stored in a struct. We need to:
// 1. Store a persistent client connection (created once, reused for all operations)
// 2. Make the client accessible to NAPI for JavaScript interop
// 3. Avoid exposing complex generic types to NAPI (which doesn't understand Rust generics)
// 4. Avoid unsafe code
//
// ## Solution:
// We use a two-layer architecture with type erasure:
//
// 1. **Internal Layer (`ClientHolder<I>`)**:
//    - Generic holder that can wrap any `GeyserGrpcClient<I: Interceptor>`
//    - Stores the client in `Arc<Mutex<>>` for thread-safe async access
//    - Implements business logic methods that delegate to the underlying client
//
// 2. **NAPI Layer (`GrpcClient`)**:
//    - Uses `Arc<dyn Any + Send + Sync>` to erase the generic type
//    - Private field prevents NAPI from trying to expose it to JavaScript
//    - Methods downcast back to the concrete type when needed
//
// ## Why This Works:
// - The builder configured with `x_token` creates `InterceptorXToken`
// - We know the concrete type will always be `ClientHolder<InterceptorXToken>`
// - Type erasure with `Any` allows us to store it without exposing generics to NAPI
// - Downcast is safe because we control the type at creation time

use napi::bindgen_prelude::PromiseRaw;
use napi::Env;
use napi_derive::napi;
use std::sync::Arc;
use yellowstone_grpc_proto::geyser::CommitmentLevel;

use crate::{
  bindings::JsChannelOptions,
  init_crypto_provider,
  js_types::{
    JsGetBlockHeightRequest, JsGetBlockHeightResponse, JsGetLatestBlockhashRequest,
    JsGetLatestBlockhashResponse, JsGetSlotRequest, JsGetSlotResponse, JsGetVersionRequest,
    JsGetVersionResponse, JsIsBlockhashValidRequest, JsIsBlockhashValidResponse, JsPingRequest,
    JsPongResponse, JsSubscribeReplayInfoRequest, JsSubscribeReplayInfoResponse,
  },
  utils,
};

/// Internal module containing the generic client holder implementation.
/// This is kept separate from the NAPI-exposed types to avoid generic type exposure.
pub mod internal {
  use std::sync::Arc;
  use tokio::sync::Mutex;
  use yellowstone_grpc_client::{GeyserGrpcClient, Interceptor};

  /// Generic holder for GeyserGrpcClient with any interceptor type.
  /// Wraps the client in Arc<Mutex<>> to allow safe sharing across async tasks.
  pub struct ClientHolder<I: Interceptor> {
    pub client: Arc<Mutex<GeyserGrpcClient<I>>>,
  }

  impl<I: Interceptor + Send + Sync + 'static> ClientHolder<I> {
    /// Creates a new holder with a connected client.
    /// The client is wrapped in Arc<Mutex<>> for thread-safe access.
    pub fn new(client: GeyserGrpcClient<I>) -> Self {
      Self {
        client: Arc::new(Mutex::new(client)),
      }
    }
  }
}

/// Main client struct exposed to JavaScript via NAPI.
///
/// The client maintains a persistent gRPC connection that is created once
/// in the constructor and reused for all subsequent operations.
#[napi]
pub struct GrpcClient {
  /// Type-erased holder storing the actual client.
  ///
  /// We use `Arc<dyn Any>` to hide the generic type from NAPI:
  /// - NAPI cannot handle generic types or complex Rust types
  /// - The field is private, so NAPI doesn't try to expose it to JavaScript
  /// - We downcast back to the concrete type in each method
  ///
  /// This is safe because:
  /// - The builder configuration guarantees the interceptor type
  /// - We always create `ClientHolder<InterceptorXToken>`
  /// - The downcast will always succeed (or fail gracefully)
  pub(crate) holder: Arc<dyn std::any::Any + Send + Sync>,
}

#[napi]
impl GrpcClient {
  // Implementation note:
  // Every unary method follows the same pattern:
  // 1. clone the type-erased holder
  // 2. downcast to the concrete `ClientHolder<InterceptorXToken>`
  // 3. execute the async gRPC call under a mutex guard
  // 4. convert protobuf response to generated JS type in callback
  //
  // `spawn_future_with_callback` is used so `Env` never crosses the async
  // boundary, which avoids `Send` issues with napi handles.

  /// Creates a new gRPC client and establishes a connection.
  ///
  /// This is an async factory method that:
  /// 1. Initializes the crypto provider (required for TLS)
  /// 2. Builds the client with the provided configuration
  /// 3. Establishes the gRPC connection
  /// 4. Wraps the client for safe concurrent access
  ///
  /// The connection is persistent and will be reused for all subsequent operations.
  #[napi(factory)]
  pub async fn new(
    endpoint: String,
    x_token: Option<String>,
    channel_options: Option<JsChannelOptions>,
  ) -> napi::Result<Self> {
    init_crypto_provider();

    // Build the client with the provided configuration
    let builder = utils::get_client_builder(endpoint, x_token, channel_options).await?;

    // Connect and get the client (returns GeyserGrpcClient<impl Interceptor>)
    let client = builder
      .connect()
      .await
      .map_err(|e| napi::Error::from_reason(e.to_string()))?;

    // Wrap in our generic holder which can accept any interceptor type
    let holder = internal::ClientHolder::new(client);

    // Erase the generic type by storing as Arc<dyn Any>
    Ok(Self {
      holder: Arc::new(holder),
    })
  }

  #[napi]
  pub fn get_latest_blockhash<'env>(
    &self,
    environment: &'env Env,
    request: JsGetLatestBlockhashRequest,
  ) -> napi::Result<PromiseRaw<'env, JsGetLatestBlockhashResponse>> {
    let grpc_client_holder = self.holder.clone();
    let commitment_level_option = request
      .commitment
      .and_then(|c| CommitmentLevel::try_from(c).ok());

    environment.spawn_future_with_callback(
      async move {
        let grpc_client_holder = grpc_client_holder
          .downcast_ref::<internal::ClientHolder<yellowstone_grpc_client::InterceptorXToken>>()
          .ok_or_else(|| napi::Error::from_reason("Invalid client type"))?;

        let mut grpc_client_guard = grpc_client_holder.client.lock().await;

        let protobuf_response = grpc_client_guard
          .get_latest_blockhash(commitment_level_option)
          .await
          .map_err(|error| napi::Error::from_reason(error.to_string()))?;

        Ok(protobuf_response)
      },
      move |callback_environment, protobuf_response| {
        JsGetLatestBlockhashResponse::from_protobuf_to_js_type(
          callback_environment,
          protobuf_response,
        )
      },
    )
  }

  #[napi]
  pub fn ping<'env>(
    &self,
    environment: &'env Env,
    request: JsPingRequest,
  ) -> napi::Result<PromiseRaw<'env, JsPongResponse>> {
    let grpc_client_holder = self.holder.clone();
    let ping_count = request.count;

    environment.spawn_future_with_callback(
      async move {
        let grpc_client_holder = grpc_client_holder
          .downcast_ref::<internal::ClientHolder<yellowstone_grpc_client::InterceptorXToken>>()
          .ok_or_else(|| napi::Error::from_reason("Invalid client type"))?;

        let mut grpc_client_guard = grpc_client_holder.client.lock().await;

        let protobuf_response = grpc_client_guard
          .ping(ping_count)
          .await
          .map_err(|error| napi::Error::from_reason(error.to_string()))?;

        Ok(protobuf_response)
      },
      move |callback_environment, protobuf_response| {
        JsPongResponse::from_protobuf_to_js_type(callback_environment, protobuf_response)
      },
    )
  }

  #[napi]
  pub fn get_block_height<'env>(
    &self,
    environment: &'env Env,
    request: JsGetBlockHeightRequest,
  ) -> napi::Result<PromiseRaw<'env, JsGetBlockHeightResponse>> {
    let grpc_client_holder = self.holder.clone();
    let commitment_level_option = request
      .commitment
      .and_then(|c| CommitmentLevel::try_from(c).ok());

    environment.spawn_future_with_callback(
      async move {
        let grpc_client_holder = grpc_client_holder
          .downcast_ref::<internal::ClientHolder<yellowstone_grpc_client::InterceptorXToken>>()
          .ok_or_else(|| napi::Error::from_reason("Invalid client type"))?;

        let mut grpc_client_guard = grpc_client_holder.client.lock().await;

        let protobuf_response = grpc_client_guard
          .get_block_height(commitment_level_option)
          .await
          .map_err(|error| napi::Error::from_reason(error.to_string()))?;

        Ok(protobuf_response)
      },
      move |callback_environment, protobuf_response| {
        JsGetBlockHeightResponse::from_protobuf_to_js_type(callback_environment, protobuf_response)
      },
    )
  }

  #[napi]
  pub fn get_slot<'env>(
    &self,
    environment: &'env Env,
    request: JsGetSlotRequest,
  ) -> napi::Result<PromiseRaw<'env, JsGetSlotResponse>> {
    let grpc_client_holder = self.holder.clone();
    let commitment_level_option = request
      .commitment
      .and_then(|c| CommitmentLevel::try_from(c).ok());

    environment.spawn_future_with_callback(
      async move {
        let grpc_client_holder = grpc_client_holder
          .downcast_ref::<internal::ClientHolder<yellowstone_grpc_client::InterceptorXToken>>()
          .ok_or_else(|| napi::Error::from_reason("Invalid client type"))?;

        let mut grpc_client_guard = grpc_client_holder.client.lock().await;

        let protobuf_response = grpc_client_guard
          .get_slot(commitment_level_option)
          .await
          .map_err(|error| napi::Error::from_reason(error.to_string()))?;

        Ok(protobuf_response)
      },
      move |callback_environment, protobuf_response| {
        JsGetSlotResponse::from_protobuf_to_js_type(callback_environment, protobuf_response)
      },
    )
  }

  #[napi]
  pub fn is_blockhash_valid<'env>(
    &self,
    environment: &'env Env,
    request: JsIsBlockhashValidRequest,
  ) -> napi::Result<PromiseRaw<'env, JsIsBlockhashValidResponse>> {
    let grpc_client_holder = self.holder.clone();
    let blockhash_value = request.blockhash;
    let commitment_level_option = request
      .commitment
      .and_then(|c| CommitmentLevel::try_from(c).ok());

    environment.spawn_future_with_callback(
      async move {
        let grpc_client_holder = grpc_client_holder
          .downcast_ref::<internal::ClientHolder<yellowstone_grpc_client::InterceptorXToken>>()
          .ok_or_else(|| napi::Error::from_reason("Invalid client type"))?;

        let mut grpc_client_guard = grpc_client_holder.client.lock().await;

        let protobuf_response = grpc_client_guard
          .is_blockhash_valid(blockhash_value, commitment_level_option)
          .await
          .map_err(|error| napi::Error::from_reason(error.to_string()))?;

        Ok(protobuf_response)
      },
      move |callback_environment, protobuf_response| {
        JsIsBlockhashValidResponse::from_protobuf_to_js_type(
          callback_environment,
          protobuf_response,
        )
      },
    )
  }

  #[napi]
  pub fn get_version<'env>(
    &self,
    environment: &'env Env,
    _get_version_request: JsGetVersionRequest,
  ) -> napi::Result<PromiseRaw<'env, JsGetVersionResponse>> {
    let grpc_client_holder = self.holder.clone();

    environment.spawn_future_with_callback(
      async move {
        let grpc_client_holder = grpc_client_holder
          .downcast_ref::<internal::ClientHolder<yellowstone_grpc_client::InterceptorXToken>>()
          .ok_or_else(|| napi::Error::from_reason("Invalid client type"))?;

        let mut grpc_client_guard = grpc_client_holder.client.lock().await;

        let protobuf_response = grpc_client_guard
          .get_version()
          .await
          .map_err(|error| napi::Error::from_reason(error.to_string()))?;

        Ok(protobuf_response)
      },
      move |callback_environment, protobuf_response| {
        JsGetVersionResponse::from_protobuf_to_js_type(callback_environment, protobuf_response)
      },
    )
  }

  #[napi]
  pub fn subscribe_replay_info<'env>(
    &self,
    environment: &'env Env,
    _subscribe_replay_info_request: JsSubscribeReplayInfoRequest,
  ) -> napi::Result<PromiseRaw<'env, JsSubscribeReplayInfoResponse>> {
    let grpc_client_holder = self.holder.clone();

    environment.spawn_future_with_callback(
      async move {
        let grpc_client_holder = grpc_client_holder
          .downcast_ref::<internal::ClientHolder<yellowstone_grpc_client::InterceptorXToken>>()
          .ok_or_else(|| napi::Error::from_reason("Invalid client type"))?;

        let mut grpc_client_guard = grpc_client_holder.client.lock().await;

        let protobuf_response = grpc_client_guard
          .subscribe_replay_info()
          .await
          .map_err(|error| napi::Error::from_reason(error.to_string()))?;

        Ok(protobuf_response)
      },
      move |callback_environment, protobuf_response| {
        JsSubscribeReplayInfoResponse::from_protobuf_to_js_type(
          callback_environment,
          protobuf_response,
        )
      },
    )
  }

  /// Creates a subscription stream bound to this client connection.
  ///
  /// The returned value is consumed by the JS SDK `ClientDuplexStream` wrapper,
  /// which handles Node stream lifecycle and protobuf-shape normalization.
  //
  // subscribe should only be available via the `GrpcClient`
  #[allow(private_interfaces)]
  #[napi]
  pub fn subscribe(&self, env: &napi::Env) -> napi::Result<crate::DuplexStream> {
    crate::DuplexStream::subscribe(env, self)
  }
}
