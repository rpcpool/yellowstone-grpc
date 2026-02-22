use napi_derive::napi;
use serde::Deserialize;

//--------------------------------------------------------------------------------
// Bindings.
//--------------------------------------------------------------------------------

#[napi]
#[derive(Deserialize, Debug, Clone)]
pub enum JsCompressionAlgorithm {
  Gzip,
  Zstd,
}

/// ChannelOptions from JS.
///
/// Struct to hold the channel options configuration
/// passed from JS.
///
/// Note:
/// The type u32 is being used because of napi's built-in
/// support for u32 to number (JS) conversion.
///
#[napi(object)]
#[derive(Deserialize, Debug, Clone)]
pub struct JsChannelOptions {
  //--------------------
  // Configs.
  //--------------------
  pub grpc_connect_timeout: Option<u32>,
  pub grpc_buffer_size: Option<u32>,
  pub grpc_http2_keep_alive_interval: Option<u32>,
  pub grpc_initial_connection_window_size: Option<u32>,
  pub grpc_initial_stream_window_size: Option<u32>,
  pub grpc_keep_alive_timeout: Option<u32>,
  pub grpc_tcp_keepalive: Option<u32>,
  pub grpc_timeout: Option<u32>,
  pub grpc_max_decoding_message_size: Option<u32>,
  pub grpc_max_encoding_message_size: Option<u32>,
  pub grpc_default_compression_algorithm: Option<JsCompressionAlgorithm>,
  //--------------------
  // Flags.
  //--------------------
  pub grpc_set_x_request_snapshot: Option<bool>,
  pub grpc_http2_adaptive_window: Option<bool>,
  pub grpc_keep_alive_while_idle: Option<bool>,
  pub grpc_tcp_nodelay: Option<bool>,
}

impl Default for JsChannelOptions {
  /// Sets the following configuration to
  /// sensible defaults.
  ///
  /// ```rust
  /// JsChannelOptions {
  ///   grpc_connect_timeout: Duration::from_secs(10),
  ///   grpc_timeout: Some(Duration::from_secs(30)),
  ///   grpc_initial_connection_window_size: Some(8 * 1024 * 1024), // 8MB
  ///   grpc_initial_stream_window_size: Some(4 * 1024 * 1024),     // 4MB
  ///   grpc_max_decoding_message_size: Some(1 * 1024 * 1024 * 1024), // 1GB
  ///   grpc_max_encoding_message_size: Some(32 * 1024 * 1024),     // 32MB
  ///   grpc_http2_adaptive_window: Some(true),
  ///   grpc_tcp_nodelay: Some(true),
  ///   grpc_keep_alive_while_idle: None,
  ///   grpc_buffer_size: None,
  ///   grpc_http2_keep_alive_interval: None,
  ///   grpc_keep_alive_timeout: None,
  ///   grpc_tcp_keepalive: None,
  ///   grpc_default_compression_algorithm: None,
  ///   grpc_set_x_request_snapshot: None,
  /// }
  /// ```
  fn default() -> Self {
    Self {
      grpc_connect_timeout: Some(10_000),
      grpc_timeout: Some(30_000),
      grpc_initial_connection_window_size: Some(8 * 1024 * 1024), // 8MB
      grpc_initial_stream_window_size: Some(4 * 1024 * 1024),     // 4MB
      grpc_max_decoding_message_size: Some(1024 * 1024 * 1024),   // 1GB
      grpc_max_encoding_message_size: Some(32 * 1024 * 1024),     // 32MB
      grpc_http2_adaptive_window: Some(true),
      grpc_tcp_nodelay: Some(true),
      grpc_keep_alive_while_idle: None,
      grpc_buffer_size: None,
      grpc_http2_keep_alive_interval: None,
      grpc_keep_alive_timeout: None,
      grpc_tcp_keepalive: None,
      grpc_default_compression_algorithm: None,
      grpc_set_x_request_snapshot: None,
    }
  }
}
