use crate::bindings::{JsChannelOptions, JsCompressionAlgorithm};
use napi::bindgen_prelude::{Result, Status};
use std::time::Duration;
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcBuilder};
use yellowstone_grpc_proto::tonic::codec::CompressionEncoding;

pub async fn get_client_builder(
  endpoint: String,
  x_token: Option<String>,
  channel_options: Option<JsChannelOptions>,
) -> Result<GeyserGrpcBuilder> {
  let use_tls = endpoint.starts_with("https://");

  let mut grpc_client_builder = match GeyserGrpcBuilder::from_shared(endpoint) {
    Ok(builder) => builder,
    Err(error) => return Err(napi::Error::new(Status::InvalidArg, error)),
  };

  grpc_client_builder = match grpc_client_builder.x_token(x_token) {
    Ok(builder) => builder,
    Err(error) => return Err(napi::Error::new(Status::InvalidArg, error)),
  };

  if use_tls {
    grpc_client_builder =
      match grpc_client_builder.tls_config(ClientTlsConfig::new().with_enabled_roots()) {
        Ok(builder) => builder,
        Err(error) => return Err(napi::Error::new(Status::InvalidArg, error)),
      };
  }

  let default_channel_options = JsChannelOptions::default();
  grpc_client_builder = grpc_client_builder
    .connect_timeout(Duration::from_millis(
      default_channel_options
        .grpc_connect_timeout
        .expect("connect_timeout is defined in Default")
        .into(),
    ))
    .timeout(Duration::from_millis(
      default_channel_options
        .grpc_timeout
        .expect("timeout is defined in Default")
        .into(),
    ))
    .initial_stream_window_size(default_channel_options.grpc_initial_stream_window_size)
    .initial_connection_window_size(default_channel_options.grpc_initial_connection_window_size)
    .max_decoding_message_size(
      default_channel_options
        .grpc_max_decoding_message_size
        .expect("max_decoding_message_size is defined in Default") as usize,
    )
    .max_encoding_message_size(
      default_channel_options
        .grpc_max_encoding_message_size
        .expect("max_encoding_message_size is defined in Default") as usize,
    )
    .http2_adaptive_window(
      default_channel_options
        .grpc_http2_adaptive_window
        .expect("grpc_http2_adaptive_window is defined in Default"),
    )
    .tcp_nodelay(
      default_channel_options
        .grpc_tcp_nodelay
        .expect("grpc_tcp_nodelay is defined in Default"),
    );

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
