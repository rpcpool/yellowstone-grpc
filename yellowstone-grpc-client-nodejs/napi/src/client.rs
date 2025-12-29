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

use napi_derive::napi;
use std::sync::Arc;

use crate::{
  bindings::{JsChannelOptions, JsGetLatestBlockhashRequest, JsGetLatestBlockhashResponse},
  init_crypto_provider, utils,
};

/// Internal module containing the generic client holder implementation.
/// This is kept separate from the NAPI-exposed types to avoid generic type exposure.
mod internal {
  use std::sync::Arc;
  use tokio::sync::Mutex;
  use yellowstone_grpc_client::{GeyserGrpcClient, Interceptor};
  use yellowstone_grpc_proto::geyser::{GetLatestBlockhashRequest, GetLatestBlockhashResponse};

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

    /// Gets the latest blockhash from the Solana cluster.
    /// This method acquires the mutex lock and delegates to the underlying client.
    /// Accepts the full protobuf request and returns the full protobuf response.
    pub async fn get_latest_blockhash(
      &self,
      request: GetLatestBlockhashRequest,
    ) -> Result<GetLatestBlockhashResponse, String> {
      let mut client = self.client.lock().await;

      // Convert the optional i32 commitment to CommitmentLevel enum
      let commitment = request.commitment.and_then(|c| {
        use yellowstone_grpc_proto::geyser::CommitmentLevel;
        CommitmentLevel::try_from(c).ok()
      });

      client
        .get_latest_blockhash(commitment)
        .await
        .map_err(|e| e.to_string())
    }

    // Additional gRPC methods can be added here following the same pattern:
    // - Lock the client
    // - Call the underlying gRPC method
    // - Return the result
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
  holder: Arc<dyn std::any::Any + Send + Sync>,
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

  /// Gets the latest blockhash from the Solana cluster.
  ///
  /// This method:
  /// 1. Accepts a JavaScript-compatible request object
  /// 2. Converts the request to protobuf format
  /// 3. Downcasts the holder to the concrete type
  /// 4. Delegates to the holder's method
  /// 5. Converts the protobuf response back to JavaScript-compatible format
  /// 6. Returns the full response including blockhash and last valid block height
  #[napi]
  pub async fn get_latest_blockhash(
    &self,
    request: JsGetLatestBlockhashRequest,
  ) -> napi::Result<JsGetLatestBlockhashResponse> {
    // Downcast from `dyn Any` back to the concrete ClientHolder type.
    // This will always succeed because we control the type at creation time.
    // If it fails, it indicates a serious logic error in our code.
    let holder = self
      .holder
      .downcast_ref::<internal::ClientHolder<yellowstone_grpc_client::InterceptorXToken>>()
      .ok_or_else(|| napi::Error::from_reason("Invalid client type"))?;

    // Convert JavaScript request to protobuf format
    let pb_request = utils::js_to_get_latest_blockhash_request(request);

    // Delegate to the holder's implementation
    let pb_response = holder
      .get_latest_blockhash(pb_request)
      .await
      .map_err(|e| napi::Error::from_reason(e))?;

    // Convert protobuf response to JavaScript-compatible format
    let js_response = utils::get_latest_blockhash_response_to_js(pb_response);

    Ok(js_response)
  }
}
