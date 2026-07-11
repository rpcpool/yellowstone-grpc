use crate::bindings::{
  JsChannelOptions, JsCompressionAlgorithm, JsReconnectConfig, JsReconnectReplayPolicy,
};
use napi::bindgen_prelude::{Result, Status};
use std::time::Duration;
use yellowstone_grpc_client::{
  reconnect::ReplayPolicy, Backoff, ClientTlsConfig, GeyserGrpcBuilder, ReconnectConfig,
};
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

fn invalid_arg(reason: impl Into<String>) -> napi::Error {
  let reason = reason.into();
  let mut error = napi::Error::new(Status::InvalidArg, reason.clone());
  error.set_cause(napi::Error::new(Status::InvalidArg, reason));
  error
}

fn parse_u64_string(value: &str, field_name: &str) -> Result<u64> {
  value.parse::<u64>().map_err(|error| {
    invalid_arg_with_cause(
      format!("invalid {field_name}: expected a u64 string"),
      &error,
    )
  })
}

fn replay_policy_from_js(replay_policy: Option<JsReconnectReplayPolicy>) -> Result<ReplayPolicy> {
  let Some(replay_policy) = replay_policy else {
    return Ok(ReplayPolicy::default());
  };

  match (replay_policy.from_checkpoint, replay_policy.fresh) {
    (Some(from_checkpoint), None) => {
      let checkpoint_buffer = parse_u64_string(
        &from_checkpoint.checkpoint_buffer,
        "reconnect.replayPolicy.fromCheckpoint.checkpointBuffer",
      )?;
      Ok(ReplayPolicy::FromCheckpoint { checkpoint_buffer })
    }
    (None, Some(true)) => Ok(ReplayPolicy::Fresh),
    (None, Some(false)) => Err(invalid_arg(
      "invalid reconnect.replayPolicy.fresh: expected true when set",
    )),
    (None, None) => Err(invalid_arg(
      "invalid reconnect.replayPolicy: expected fromCheckpoint or fresh",
    )),
    (Some(_), Some(_)) => Err(invalid_arg(
      "invalid reconnect.replayPolicy: expected exactly one of fromCheckpoint or fresh",
    )),
  }
}

fn reconnect_config_from_js(
  reconnect_config: Option<JsReconnectConfig>,
) -> Result<Option<ReconnectConfig>> {
  let Some(reconnect_config) = reconnect_config else {
    return Ok(None);
  };

  let mut native_config = ReconnectConfig::default();
  native_config.replay_policy = replay_policy_from_js(reconnect_config.replay_policy)?;

  if let Some(slot_retention) = reconnect_config.slot_retention {
    if slot_retention == 0 {
      return Err(invalid_arg(
        "invalid reconnect.slotRetention: expected a positive integer",
      ));
    }
    native_config = native_config.with_slot_retention(slot_retention as usize);
  }

  if let Some(backoff) = reconnect_config.backoff {
    let initial_interval = backoff
      .initial_interval_ms
      .map(|ms| Duration::from_millis(ms as u64))
      .unwrap_or(native_config.backoff.initial_interval);
    let multiplier = backoff
      .multiplier
      .unwrap_or(native_config.backoff.multiplier);
    let max_retries = backoff
      .max_retries
      .unwrap_or(native_config.backoff.max_retries);

    if !multiplier.is_finite() || multiplier < 1.0 {
      return Err(invalid_arg(
        "invalid reconnect.backoff.multiplier: expected a finite number >= 1",
      ));
    }

    native_config =
      native_config.with_backoff(Backoff::new(initial_interval, multiplier, max_retries));
  }

  Ok(Some(native_config))
}

pub async fn get_client_builder(
  endpoint: String,
  x_token: Option<String>,
  channel_options: Option<JsChannelOptions>,
  reconnect_config: Option<JsReconnectConfig>,
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

  if let Some(reconnect_config) = reconnect_config_from_js(reconnect_config)? {
    grpc_client_builder = grpc_client_builder.set_reconnect_config(reconnect_config);
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
  use crate::bindings::{
    JsReconnectBackoff, JsReconnectConfig, JsReconnectFromCheckpoint, JsReconnectReplayPolicy,
  };
  use std::time::Duration;
  use yellowstone_grpc_client::{reconnect::ReplayPolicy, ReconnectConfig};

  #[tokio::test]
  async fn get_client_builder_invalid_endpoint_includes_cause() {
    let error = get_client_builder("http://127.0.0.1:10000\n".to_string(), None, None, None)
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

  #[tokio::test]
  async fn get_client_builder_omits_reconnect_config_by_default() {
    let builder = get_client_builder("http://127.0.0.1:10000".to_string(), None, None, None)
      .await
      .expect("valid endpoint should build");

    assert!(builder.reconnect_config.is_none());
  }

  #[tokio::test]
  async fn get_client_builder_applies_reconnect_config() {
    let builder = get_client_builder(
      "http://127.0.0.1:10000".to_string(),
      None,
      None,
      Some(JsReconnectConfig {
        backoff: Some(JsReconnectBackoff {
          initial_interval_ms: Some(125),
          multiplier: Some(1.5),
          max_retries: Some(8),
        }),
        replay_policy: Some(JsReconnectReplayPolicy {
          from_checkpoint: Some(JsReconnectFromCheckpoint {
            checkpoint_buffer: "12".to_string(),
          }),
          fresh: None,
        }),
        slot_retention: Some(300),
      }),
    )
    .await
    .expect("valid reconnect config should build");

    let reconnect_config = builder
      .reconnect_config
      .expect("reconnect config should be set");

    assert_eq!(
      reconnect_config.backoff.initial_interval,
      Duration::from_millis(125)
    );
    assert_eq!(reconnect_config.backoff.multiplier, 1.5);
    assert_eq!(reconnect_config.backoff.max_retries, 8);
    assert_eq!(reconnect_config.slot_retention, 300);
    match reconnect_config.replay_policy {
      ReplayPolicy::FromCheckpoint { checkpoint_buffer } => assert_eq!(checkpoint_buffer, 12),
      ReplayPolicy::Fresh => panic!("expected checkpoint replay policy"),
    }
  }

  #[tokio::test]
  async fn get_client_builder_uses_default_replay_policy() {
    let builder = get_client_builder(
      "http://127.0.0.1:10000".to_string(),
      None,
      None,
      Some(JsReconnectConfig {
        backoff: None,
        replay_policy: None,
        slot_retention: None,
      }),
    )
    .await
    .expect("valid reconnect config should build");

    let reconnect_config = builder
      .reconnect_config
      .expect("reconnect config should be set");

    match (
      reconnect_config.replay_policy,
      ReconnectConfig::default().replay_policy,
    ) {
      (
        ReplayPolicy::FromCheckpoint { checkpoint_buffer },
        ReplayPolicy::FromCheckpoint {
          checkpoint_buffer: expected,
        },
      ) => assert_eq!(checkpoint_buffer, expected),
      _ => panic!("expected default checkpoint replay policy"),
    }
  }

  #[tokio::test]
  async fn get_client_builder_applies_fresh_replay_policy() {
    let builder = get_client_builder(
      "http://127.0.0.1:10000".to_string(),
      None,
      None,
      Some(JsReconnectConfig {
        backoff: None,
        replay_policy: Some(JsReconnectReplayPolicy {
          from_checkpoint: None,
          fresh: Some(true),
        }),
        slot_retention: None,
      }),
    )
    .await
    .expect("valid reconnect config should build");

    let reconnect_config = builder
      .reconnect_config
      .expect("reconnect config should be set");

    match reconnect_config.replay_policy {
      ReplayPolicy::Fresh => {}
      ReplayPolicy::FromCheckpoint { .. } => panic!("expected fresh replay policy"),
    }
  }

  #[tokio::test]
  async fn get_client_builder_accepts_checkpoint_buffer_above_u32_max() {
    let builder = get_client_builder(
      "http://127.0.0.1:10000".to_string(),
      None,
      None,
      Some(JsReconnectConfig {
        backoff: None,
        replay_policy: Some(JsReconnectReplayPolicy {
          from_checkpoint: Some(JsReconnectFromCheckpoint {
            checkpoint_buffer: "4294967296".to_string(),
          }),
          fresh: None,
        }),
        slot_retention: None,
      }),
    )
    .await
    .expect("u64 checkpoint buffer should build");

    let reconnect_config = builder
      .reconnect_config
      .expect("reconnect config should be set");

    match reconnect_config.replay_policy {
      ReplayPolicy::FromCheckpoint { checkpoint_buffer } => {
        assert_eq!(checkpoint_buffer, 4294967296)
      }
      ReplayPolicy::Fresh => panic!("expected checkpoint replay policy"),
    }
  }

  #[tokio::test]
  async fn get_client_builder_rejects_invalid_reconnect_config() {
    let error = get_client_builder(
      "http://127.0.0.1:10000".to_string(),
      None,
      None,
      Some(JsReconnectConfig {
        backoff: Some(JsReconnectBackoff {
          initial_interval_ms: None,
          multiplier: Some(0.5),
          max_retries: None,
        }),
        replay_policy: None,
        slot_retention: None,
      }),
    )
    .await
    .expect_err("invalid multiplier should fail");

    assert!(
      error
        .to_string()
        .contains("invalid reconnect.backoff.multiplier"),
      "unexpected error message: {error}"
    );
  }

  #[tokio::test]
  async fn get_client_builder_rejects_zero_slot_retention() {
    let error = get_client_builder(
      "http://127.0.0.1:10000".to_string(),
      None,
      None,
      Some(JsReconnectConfig {
        backoff: None,
        replay_policy: None,
        slot_retention: Some(0),
      }),
    )
    .await
    .expect_err("zero slot retention should fail");

    assert!(
      error
        .to_string()
        .contains("invalid reconnect.slotRetention"),
      "unexpected error message: {error}"
    );
  }

  #[tokio::test]
  async fn get_client_builder_rejects_empty_replay_policy() {
    let error = get_client_builder(
      "http://127.0.0.1:10000".to_string(),
      None,
      None,
      Some(JsReconnectConfig {
        backoff: None,
        replay_policy: Some(JsReconnectReplayPolicy {
          from_checkpoint: None,
          fresh: None,
        }),
        slot_retention: None,
      }),
    )
    .await
    .expect_err("empty replay policy should fail");

    assert!(
      error.to_string().contains("invalid reconnect.replayPolicy"),
      "unexpected error message: {error}"
    );
  }

  #[tokio::test]
  async fn get_client_builder_rejects_ambiguous_replay_policy() {
    let error = get_client_builder(
      "http://127.0.0.1:10000".to_string(),
      None,
      None,
      Some(JsReconnectConfig {
        backoff: None,
        replay_policy: Some(JsReconnectReplayPolicy {
          from_checkpoint: Some(JsReconnectFromCheckpoint {
            checkpoint_buffer: "2".to_string(),
          }),
          fresh: Some(true),
        }),
        slot_retention: None,
      }),
    )
    .await
    .expect_err("ambiguous replay policy should fail");

    assert!(
      error
        .to_string()
        .contains("expected exactly one of fromCheckpoint or fresh"),
      "unexpected error message: {error}"
    );
  }

  #[tokio::test]
  async fn get_client_builder_rejects_false_fresh_replay_policy() {
    let error = get_client_builder(
      "http://127.0.0.1:10000".to_string(),
      None,
      None,
      Some(JsReconnectConfig {
        backoff: None,
        replay_policy: Some(JsReconnectReplayPolicy {
          from_checkpoint: None,
          fresh: Some(false),
        }),
        slot_retention: None,
      }),
    )
    .await
    .expect_err("false fresh policy should fail");

    assert!(
      error
        .to_string()
        .contains("invalid reconnect.replayPolicy.fresh"),
      "unexpected error message: {error}"
    );
  }

  #[tokio::test]
  async fn get_client_builder_rejects_invalid_checkpoint_buffer() {
    let error = get_client_builder(
      "http://127.0.0.1:10000".to_string(),
      None,
      None,
      Some(JsReconnectConfig {
        backoff: None,
        replay_policy: Some(JsReconnectReplayPolicy {
          from_checkpoint: Some(JsReconnectFromCheckpoint {
            checkpoint_buffer: "not-a-number".to_string(),
          }),
          fresh: None,
        }),
        slot_retention: None,
      }),
    )
    .await
    .expect_err("invalid checkpoint buffer should fail");

    assert!(
      error
        .to_string()
        .contains("invalid reconnect.replayPolicy.fromCheckpoint.checkpointBuffer"),
      "unexpected error message: {error}"
    );
    assert!(error.cause.is_some(), "expected native parse cause");
  }
}
