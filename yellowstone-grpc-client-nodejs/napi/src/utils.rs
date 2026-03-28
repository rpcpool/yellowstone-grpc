use crate::bindings::{JsChannelOptions, JsCompressionAlgorithm};
use napi::bindgen_prelude::{Result, Status};
use std::time::Duration;
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcBuilder};
use yellowstone_grpc_proto::tonic::codec::CompressionEncoding;

fn to_napi_cause(status: Status, source: &dyn std::error::Error) -> napi::Error {
  let mut cause = napi::Error::new(status, source.to_string());
  if let Some(next) = source.source() {
    cause.set_cause(to_napi_cause(status, next));
  }
  cause
}

fn invalid_arg_with_cause(reason: impl Into<String>, cause: &dyn std::error::Error) -> napi::Error {
  let mut error = napi::Error::new(Status::InvalidArg, reason.into());
  error.set_cause(to_napi_cause(Status::InvalidArg, cause));
  error
}

pub async fn get_client_builder(
  endpoint: String,
  x_token: Option<String>,
  channel_options: Option<JsChannelOptions>,
) -> Result<GeyserGrpcBuilder> {
  let use_tls = endpoint.starts_with("https://");

  let mut grpc_client_builder = match GeyserGrpcBuilder::from_shared(endpoint) {
    Ok(builder) => builder,
    Err(error) => {
      return Err(invalid_arg_with_cause(
        "invalid gRPC endpoint configuration",
        &error,
      ))
    }
  };

  grpc_client_builder = match grpc_client_builder.x_token(x_token) {
    Ok(builder) => builder,
    Err(error) => {
      return Err(invalid_arg_with_cause(
        "invalid x-token configuration",
        &error,
      ))
    }
  };

  if use_tls {
    grpc_client_builder =
      match grpc_client_builder.tls_config(ClientTlsConfig::new().with_enabled_roots()) {
        Ok(builder) => builder,
        Err(error) => {
          return Err(invalid_arg_with_cause(
            "invalid TLS configuration for gRPC endpoint",
            &error,
          ))
        }
      };
  }

  const DEFAULT_GRPC_CONNECT_TIMEOUT_MS: u32 = 10_000;
  const DEFAULT_GRPC_TIMEOUT_MS: u32 = 30_000;
  const DEFAULT_GRPC_INITIAL_CONNECTION_WINDOW_SIZE: u32 = 8 * 1024 * 1024;
  const DEFAULT_GRPC_INITIAL_STREAM_WINDOW_SIZE: u32 = 4 * 1024 * 1024;
  const DEFAULT_GRPC_MAX_DECODING_MESSAGE_SIZE: u32 = 1024 * 1024 * 1024;
  const DEFAULT_GRPC_MAX_ENCODING_MESSAGE_SIZE: u32 = 32 * 1024 * 1024;
  const DEFAULT_GRPC_HTTP2_ADAPTIVE_WINDOW: bool = true;
  const DEFAULT_GRPC_TCP_NODELAY: bool = true;

  let default_channel_options = JsChannelOptions::default();
  let grpc_connect_timeout_millis = default_channel_options
    .grpc_connect_timeout
    .unwrap_or(DEFAULT_GRPC_CONNECT_TIMEOUT_MS);
  let grpc_timeout_millis = default_channel_options
    .grpc_timeout
    .unwrap_or(DEFAULT_GRPC_TIMEOUT_MS);
  let grpc_initial_stream_window_size = default_channel_options
    .grpc_initial_stream_window_size
    .unwrap_or(DEFAULT_GRPC_INITIAL_STREAM_WINDOW_SIZE);
  let grpc_initial_connection_window_size = default_channel_options
    .grpc_initial_connection_window_size
    .unwrap_or(DEFAULT_GRPC_INITIAL_CONNECTION_WINDOW_SIZE);
  let grpc_max_decoding_message_size = default_channel_options
    .grpc_max_decoding_message_size
    .unwrap_or(DEFAULT_GRPC_MAX_DECODING_MESSAGE_SIZE);
  let grpc_max_encoding_message_size = default_channel_options
    .grpc_max_encoding_message_size
    .unwrap_or(DEFAULT_GRPC_MAX_ENCODING_MESSAGE_SIZE);
  let grpc_http2_adaptive_window = default_channel_options
    .grpc_http2_adaptive_window
    .unwrap_or(DEFAULT_GRPC_HTTP2_ADAPTIVE_WINDOW);
  let grpc_tcp_nodelay = default_channel_options
    .grpc_tcp_nodelay
    .unwrap_or(DEFAULT_GRPC_TCP_NODELAY);

  grpc_client_builder = grpc_client_builder
    .connect_timeout(Duration::from_millis(grpc_connect_timeout_millis.into()))
    .timeout(Duration::from_millis(grpc_timeout_millis.into()))
    .initial_stream_window_size(Some(grpc_initial_stream_window_size))
    .initial_connection_window_size(Some(grpc_initial_connection_window_size))
    .max_decoding_message_size(grpc_max_decoding_message_size as usize)
    .max_encoding_message_size(grpc_max_encoding_message_size as usize)
    .http2_adaptive_window(grpc_http2_adaptive_window)
    .tcp_nodelay(grpc_tcp_nodelay);

  if let Some(channel_options_override) = channel_options {
    if let Some(grpc_connect_timeout_millis) = channel_options_override.grpc_connect_timeout {
      grpc_client_builder = grpc_client_builder
        .connect_timeout(Duration::from_millis(grpc_connect_timeout_millis as u64));
    }
    if let Some(grpc_buffer_size) = channel_options_override.grpc_buffer_size {
      grpc_client_builder = grpc_client_builder.buffer_size(grpc_buffer_size as usize);
    }
    if let Some(grpc_http2_keep_alive_interval_millis) =
      channel_options_override.grpc_http2_keep_alive_interval
    {
      grpc_client_builder = grpc_client_builder.http2_keep_alive_interval(Duration::from_millis(
        grpc_http2_keep_alive_interval_millis as u64,
      ));
    }
    if let Some(grpc_initial_connection_window_size) =
      channel_options_override.grpc_initial_connection_window_size
    {
      grpc_client_builder =
        grpc_client_builder.initial_connection_window_size(grpc_initial_connection_window_size);
    }
    if let Some(grpc_initial_stream_window_size) =
      channel_options_override.grpc_initial_stream_window_size
    {
      grpc_client_builder =
        grpc_client_builder.initial_stream_window_size(grpc_initial_stream_window_size);
    }
    if let Some(grpc_keep_alive_timeout_millis) = channel_options_override.grpc_keep_alive_timeout {
      grpc_client_builder = grpc_client_builder
        .keep_alive_timeout(Duration::from_millis(grpc_keep_alive_timeout_millis as u64));
    }
    if let Some(grpc_tcp_keepalive_millis) = channel_options_override.grpc_tcp_keepalive {
      grpc_client_builder = grpc_client_builder.tcp_keepalive(Some(Duration::from_millis(
        grpc_tcp_keepalive_millis as u64,
      )));
    }
    if let Some(grpc_timeout_millis) = channel_options_override.grpc_timeout {
      grpc_client_builder =
        grpc_client_builder.timeout(Duration::from_millis(grpc_timeout_millis as u64));
    }
    if let Some(grpc_max_decoding_message_size) =
      channel_options_override.grpc_max_decoding_message_size
    {
      grpc_client_builder =
        grpc_client_builder.max_decoding_message_size(grpc_max_decoding_message_size as usize);
    }
    if let Some(grpc_max_encoding_message_size) =
      channel_options_override.grpc_max_encoding_message_size
    {
      grpc_client_builder =
        grpc_client_builder.max_encoding_message_size(grpc_max_encoding_message_size as usize);
    }
    if let Some(grpc_set_x_request_snapshot) = channel_options_override.grpc_set_x_request_snapshot
    {
      grpc_client_builder = grpc_client_builder.set_x_request_snapshot(grpc_set_x_request_snapshot);
    }
    if let Some(grpc_http2_adaptive_window) = channel_options_override.grpc_http2_adaptive_window {
      grpc_client_builder = grpc_client_builder.http2_adaptive_window(grpc_http2_adaptive_window);
    }
    if let Some(grpc_keep_alive_while_idle) = channel_options_override.grpc_keep_alive_while_idle {
      grpc_client_builder = grpc_client_builder.keep_alive_while_idle(grpc_keep_alive_while_idle);
    }
    if let Some(grpc_tcp_nodelay) = channel_options_override.grpc_tcp_nodelay {
      grpc_client_builder = grpc_client_builder.tcp_nodelay(grpc_tcp_nodelay);
    }
    if let Some(grpc_default_compression_algorithm) =
      channel_options_override.grpc_default_compression_algorithm
    {
      let compression_encoding = match grpc_default_compression_algorithm {
        JsCompressionAlgorithm::Gzip => CompressionEncoding::Gzip,
        JsCompressionAlgorithm::Zstd => CompressionEncoding::Zstd,
      };

      grpc_client_builder = grpc_client_builder
        .send_compressed(compression_encoding)
        .accept_compressed(compression_encoding);
    }
  }

  Ok(grpc_client_builder)
}

#[cfg(test)]
mod tests {
  use super::get_client_builder;

  #[tokio::test]
  async fn get_client_builder_invalid_endpoint_includes_cause() {
    let error = get_client_builder("http://127.0.0.1:10000\n".to_string(), None, None)
      .await
      .expect_err("invalid endpoint should fail");
    assert!(
      error
        .to_string()
        .contains("invalid gRPC endpoint configuration"),
      "unexpected error message: {error}"
    );
    assert!(
      error.cause.is_some(),
      "expected native cause on invalid endpoint"
    );
  }

  #[tokio::test]
  async fn get_client_builder_invalid_x_token_includes_cause() {
    let error = get_client_builder(
      "http://127.0.0.1:10000".to_string(),
      Some("bad\nheader".to_string()),
      None,
    )
    .await
    .expect_err("invalid x-token should fail");
    assert!(
      error.to_string().contains("invalid x-token configuration"),
      "unexpected error message: {error}"
    );
    assert!(
      error.cause.is_some(),
      "expected native cause on invalid x-token"
    );
  }
}
