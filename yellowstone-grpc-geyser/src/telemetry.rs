//! OpenTelemetry telemetry module for distributed tracing support.
//!
//! This module provides initialization and shutdown functions for OpenTelemetry
//! tracing with OTLP export support.

use {
    crate::config::ConfigOpenTelemetry,
    opentelemetry::trace::TracerProvider as _,
    opentelemetry_otlp::WithExportConfig,
    opentelemetry_sdk::{
        propagation::TraceContextPropagator,
        trace::{RandomIdGenerator, Sampler, TracerProvider},
        Resource,
    },
    std::sync::OnceLock,
    tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter},
};

static TRACER_PROVIDER: OnceLock<TracerProvider> = OnceLock::new();

/// Initialize OpenTelemetry tracing with the given configuration.
///
/// This sets up an OTLP exporter and configures the tracing subscriber
/// with an OpenTelemetry layer.
pub fn init_telemetry(config: &ConfigOpenTelemetry) -> anyhow::Result<()> {
    // Set up the trace context propagator for W3C trace context
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    // Build the OTLP exporter
    let mut exporter_builder = opentelemetry_otlp::SpanExporter::builder().with_tonic();

    if let Some(endpoint) = &config.endpoint {
        exporter_builder = exporter_builder.with_endpoint(endpoint);
    }

    // Add custom headers if configured
    if !config.headers.is_empty() {
        exporter_builder = exporter_builder.with_metadata(
            config
                .headers
                .iter()
                .filter_map(|(k, v)| {
                    let key = k.parse().ok()?;
                    let value = v.parse().ok()?;
                    Some((key, value))
                })
                .collect(),
        );
    }

    let exporter = exporter_builder.build()?;

    // Build the tracer provider with sampling configuration
    let sampler = if config.sampling_ratio >= 1.0 {
        Sampler::AlwaysOn
    } else if config.sampling_ratio <= 0.0 {
        Sampler::AlwaysOff
    } else {
        Sampler::TraceIdRatioBased(config.sampling_ratio)
    };

    let provider = TracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_sampler(sampler)
        .with_id_generator(RandomIdGenerator::default())
        .with_resource(
            Resource::builder()
                .with_service_name(&config.service_name)
                .build(),
        )
        .build();

    let tracer = provider.tracer("yellowstone-grpc-geyser");

    // Store the provider for later shutdown
    let _ = TRACER_PROVIDER.set(provider);

    // Set up the tracing subscriber with OpenTelemetry layer
    let telemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(filter)
        .with(telemetry_layer)
        .try_init()
        .map_err(|e| anyhow::anyhow!("Failed to initialize tracing subscriber: {}", e))?;

    log::info!(
        "OpenTelemetry tracing initialized with service_name={}, sampling_ratio={}",
        config.service_name,
        config.sampling_ratio
    );

    Ok(())
}

/// Shutdown OpenTelemetry tracing, flushing any pending spans.
pub fn shutdown_telemetry() {
    if let Some(provider) = TRACER_PROVIDER.get() {
        if let Err(e) = provider.shutdown() {
            log::error!("Failed to shutdown OpenTelemetry tracer provider: {:?}", e);
        } else {
            log::info!("OpenTelemetry tracing shut down successfully");
        }
    }
}
