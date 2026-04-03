// GrpcClient: A persistent, reusable gRPC client

use napi::bindgen_prelude::PromiseRaw;
use napi::Env;
use napi_derive::napi;
use yellowstone_grpc_client::GeyserGrpcClient;
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

fn napi_error_with_cause(
  status: napi::Status,
  reason: impl Into<String>,
  cause: &dyn std::error::Error,
) -> napi::Error {
  fn to_napi_cause(status: napi::Status, source: &dyn std::error::Error) -> napi::Error {
    let mut cause = napi::Error::new(status, source.to_string());
    if let Some(next) = source.source() {
      cause.set_cause(to_napi_cause(status, next));
    }
    cause
  }

  let mut error = napi::Error::new(status, reason.into());
  error.set_cause(to_napi_cause(status, cause));
  error
}

fn napi_error(status: napi::Status, reason: impl Into<String>) -> napi::Error {
  let reason = reason.into();
  let mut error = napi::Error::new(status, reason.clone());
  error.set_cause(napi::Error::new(status, reason));
  error
}

/// Main client struct exposed to JavaScript via NAPI.
///
/// The client maintains a persistent gRPC connection that is created once
/// in the constructor and reused for all subsequent operations.
#[napi]
pub struct GrpcClient {
  pub(crate) client: GeyserGrpcClient,
}

#[napi]
impl GrpcClient {
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
    let client = builder.connect().await.map_err(|error| {
      napi_error_with_cause(
        napi::Status::GenericFailure,
        "failed to connect to gRPC endpoint",
        &error,
      )
    })?;

    Ok(Self { client: client })
  }

  #[napi]
  pub fn get_latest_blockhash<'env>(
    &self,
    environment: &'env Env,
    request: JsGetLatestBlockhashRequest,
  ) -> napi::Result<PromiseRaw<'env, JsGetLatestBlockhashResponse>> {
    let commitment_level_option = request
      .commitment
      .and_then(|c| CommitmentLevel::try_from(c).ok());

    let mut client = self.client.clone();

    environment.spawn_future_with_callback(
      async move {
        let protobuf_response = client
          .get_latest_blockhash(commitment_level_option)
          .await
          .map_err(|error| {
            napi_error_with_cause(
              napi::Status::GenericFailure,
              "get_latest_blockhash request failed",
              &error,
            )
          })?;

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
    let ping_count = request.count;

    let mut client = self.client.clone();

    environment.spawn_future_with_callback(
      async move {
        let protobuf_response = client.ping(ping_count).await.map_err(|error| {
          napi_error_with_cause(napi::Status::GenericFailure, "ping request failed", &error)
        })?;

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
    let commitment_level_option = request
      .commitment
      .and_then(|c| CommitmentLevel::try_from(c).ok());

    let mut client = self.client.clone();

    environment.spawn_future_with_callback(
      async move {
        let protobuf_response = client
          .get_block_height(commitment_level_option)
          .await
          .map_err(|error| {
            napi_error_with_cause(
              napi::Status::GenericFailure,
              "get_block_height request failed",
              &error,
            )
          })?;

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
    let commitment_level_option = request
      .commitment
      .and_then(|c| CommitmentLevel::try_from(c).ok());

    let mut client = self.client.clone();

    environment.spawn_future_with_callback(
      async move {
        let protobuf_response =
          client
            .get_slot(commitment_level_option)
            .await
            .map_err(|error| {
              napi_error_with_cause(
                napi::Status::GenericFailure,
                "get_slot request failed",
                &error,
              )
            })?;

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
    let blockhash_value = request.blockhash;
    let commitment_level_option = request
      .commitment
      .and_then(|c| CommitmentLevel::try_from(c).ok());

    let mut client = self.client.clone();

    environment.spawn_future_with_callback(
      async move {
        let protobuf_response = client
          .is_blockhash_valid(blockhash_value, commitment_level_option)
          .await
          .map_err(|error| {
            napi_error_with_cause(
              napi::Status::GenericFailure,
              "is_blockhash_valid request failed",
              &error,
            )
          })?;

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
    let mut client = self.client.clone();

    environment.spawn_future_with_callback(
      async move {
        let protobuf_response = client.get_version().await.map_err(|error| {
          napi_error_with_cause(
            napi::Status::GenericFailure,
            "get_version request failed",
            &error,
          )
        })?;

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
    let mut client = self.client.clone();

    environment.spawn_future_with_callback(
      async move {
        let protobuf_response = client.subscribe_replay_info().await.map_err(|error| {
          napi_error_with_cause(
            napi::Status::GenericFailure,
            "subscribe_replay_info request failed",
            &error,
          )
        })?;

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
  pub fn subscribe<'env>(
    &self,
    env: &'env napi::Env,
  ) -> napi::Result<PromiseRaw<'env, crate::DuplexStream>> {
    crate::DuplexStream::subscribe(env, self)
  }

  /// Creates a deshred subscription stream bound to this client connection.
  ///
  /// Unlike `subscribe()`, this method opens the underlying gRPC stream before
  /// resolving, so server-side `UNIMPLEMENTED` errors bubble to TypeScript
  /// callers through the returned Promise.
  #[allow(private_interfaces)]
  #[napi]
  pub fn subscribe_deshred<'env>(
    &self,
    env: &'env napi::Env,
  ) -> napi::Result<PromiseRaw<'env, crate::DuplexStreamDeshred>> {
    crate::DuplexStreamDeshred::subscribe(env, self)
  }
}
