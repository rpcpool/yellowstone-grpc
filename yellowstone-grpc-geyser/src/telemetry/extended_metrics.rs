//! The families `telemetry.extended_metrics` adds (arch 12.2): one collector that sums the striped
//! counts and reads the instance's budgets at scrape, the registration one load holds, and the
//! count of a Subscribe session.

use {
    crate::{
        body::block_shape::{BlockShapeBudget, BlockShapeEvent, PrivateShapeCause},
        compression::{slot::RetainedCompressionBudget, CompressingEncoding, ResponseEncoding},
        metrics::REGISTRY,
        telemetry::data_plane_counts::{
            CompressionGroupEvent, CompressionOutcome, DataPlaneCount, DataPlaneCounts,
            PluginCallback, WaitedClaim, DATA_PLANE_COUNTS,
        },
    },
    prometheus::{
        core::{Collector, Desc},
        proto::{Counter, Gauge, LabelPair, Metric, MetricFamily, MetricType},
        Error as PrometheusError, Registry,
    },
    std::{
        collections::HashMap,
        fmt,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, LazyLock,
        },
    },
};

/// The server transport of every Subscribe stream until own serving (arch A16).
const SERVER_TRANSPORT: &str = "tonic";

/// `block_shape_writes_total`'s outcomes: a shape's build, or a member's private write by cause.
const BLOCK_SHAPE_WRITE_OUTCOMES: [(&str, BlockShapeEvent); 6] = [
    ("built", BlockShapeEvent::ShapeBuild),
    (
        "private_table_full",
        BlockShapeEvent::PrivateWrite(PrivateShapeCause::TableFull),
    ),
    (
        "private_in_flight",
        BlockShapeEvent::PrivateWrite(PrivateShapeCause::InFlight),
    ),
    (
        "private_failed_or_retired",
        BlockShapeEvent::PrivateWrite(PrivateShapeCause::FailedOrRetired),
    ),
    (
        "private_budget_full",
        BlockShapeEvent::PrivateWrite(PrivateShapeCause::BudgetFull),
    ),
    (
        "private_never_claims",
        BlockShapeEvent::PrivateWrite(PrivateShapeCause::NeverClaims),
    ),
];

/// `block_shape_events_total`'s events.
const BLOCK_SHAPE_EVENT_LABELS: [(&str, BlockShapeEvent); 4] = [
    ("selection_walk", BlockShapeEvent::SelectionWalk),
    (
        "racing_duplicate_shape",
        BlockShapeEvent::RacingDuplicateShape,
    ),
    (
        "pinned_block_fallback",
        BlockShapeEvent::PinnedBlockFallback,
    ),
    ("key_matching_pass", BlockShapeEvent::KeyMatchingPass),
];

/// The budgets whose bytes `retained_bytes` reads: the group messages' when sharing is on, and
/// the Block shapes'.
#[derive(Debug, Clone)]
pub struct RetainedBudgets {
    pub compression_budget: Option<Arc<RetainedCompressionBudget>>,
    pub block_shape_budget: Arc<BlockShapeBudget>,
}

/// The extended families' descs. They are the same for every config, so every registration of
/// the collector has one id.
#[derive(Clone)]
struct ExtendedFamilyDescs {
    subscribe_streams: Desc,
    retained_bytes: Desc,
    compressions: Desc,
    compression_group_events: Desc,
    claim_backstop_waits: Desc,
    block_shape_writes: Desc,
    block_shape_events: Desc,
    callback_panics: Desc,
}

impl ExtendedFamilyDescs {
    fn new() -> Self {
        Self {
            subscribe_streams: family_desc(
                "subscribe_streams",
                "Subscribe sessions open, by response encoding and server transport, never below 0; a \
                 session counts until its loop ends, up to a ping interval after its client left",
                &["encoding", "transport"],
            ),
            retained_bytes: family_desc(
                "retained_bytes",
                "Bytes the data plane retains, by kind",
                &["kind"],
            ),
            compressions: family_desc(
                "compressions_total",
                "Compressions of members' messages by the plugin's response body, by encoding and \
                 outcome; each compression has one outcome",
                &["encoding", "outcome"],
            ),
            compression_group_events: family_desc(
                "compression_group_events_total",
                "Compression group events that are not compressions, by encoding",
                &["encoding", "event"],
            ),
            claim_backstop_waits: family_desc(
                "claim_backstop_waits_total",
                "Claim waits that found every waker cell taken and armed their own timer, by claim",
                &["claim"],
            ),
            block_shape_writes: family_desc(
                "block_shape_writes_total",
                "Block shape bodies built, and Block members that wrote privately by cause",
                &["outcome"],
            ),
            block_shape_events: family_desc(
                "block_shape_events_total",
                "Block shape routing and write events, by event",
                &["event"],
            ),
            callback_panics: family_desc(
                "callback_panics_total",
                "Panics caught in plugin callbacks, by callback",
                &["callback"],
            ),
        }
    }

    fn every_desc(&self) -> Vec<&Desc> {
        vec![
            &self.subscribe_streams,
            &self.retained_bytes,
            &self.compressions,
            &self.compression_group_events,
            &self.claim_backstop_waits,
            &self.block_shape_writes,
            &self.block_shape_events,
            &self.callback_panics,
        ]
    }
}

fn family_desc(family_name: &str, family_help: &str, label_keys: &[&str]) -> Desc {
    Desc::new(
        format!("yellowstone_grpc_geyser_{family_name}"),
        family_help.to_owned(),
        label_keys
            .iter()
            .map(|label_key| (*label_key).to_owned())
            .collect(),
        HashMap::new(),
    )
    .expect("a valid family name and label keys")
}

/// The extended families, rendered at scrape from a set of striped counts and one instance's
/// budgets.
#[derive(Clone)]
pub struct ExtendedCollector {
    data_plane_counts: &'static DataPlaneCounts,
    retained_budgets: RetainedBudgets,
    family_descs: ExtendedFamilyDescs,
}

impl ExtendedCollector {
    pub fn new(
        data_plane_counts: &'static DataPlaneCounts,
        retained_budgets: RetainedBudgets,
    ) -> Self {
        Self {
            data_plane_counts,
            retained_budgets,
            family_descs: ExtendedFamilyDescs::new(),
        }
    }

    /// The stream gauge: every ended count is read before every started count, so a session that
    /// ends during the scrape never counts below 0, and each value saturates at 0.
    fn subscribe_streams_family(&self) -> MetricFamily {
        let ended_streams = ResponseEncoding::ALL.map(|response_encoding| {
            self.data_plane_counts
                .sum(DataPlaneCount::SubscribeStreamEnded(response_encoding))
        });
        let started_streams = ResponseEncoding::ALL.map(|response_encoding| {
            self.data_plane_counts
                .sum(DataPlaneCount::SubscribeStreamStarted(response_encoding))
        });
        let mut streams_family =
            empty_family(&self.family_descs.subscribe_streams, MetricType::GAUGE);
        for (response_encoding, (started_count, ended_count)) in ResponseEncoding::ALL
            .into_iter()
            .zip(started_streams.into_iter().zip(ended_streams))
        {
            if started_count > 0 {
                push_gauge(
                    &mut streams_family,
                    &[
                        ("encoding", response_encoding.encoding_name()),
                        ("transport", SERVER_TRANSPORT),
                    ],
                    started_count.saturating_sub(ended_count) as f64,
                );
            }
        }
        streams_family
    }

    fn retained_bytes_family(&self) -> MetricFamily {
        let mut retained_family =
            empty_family(&self.family_descs.retained_bytes, MetricType::GAUGE);
        if let Some(compression_budget) = &self.retained_budgets.compression_budget {
            push_gauge(
                &mut retained_family,
                &[("kind", "compressed")],
                compression_budget.retained_bytes() as f64,
            );
        }
        push_gauge(
            &mut retained_family,
            &[("kind", "block_shape")],
            self.retained_budgets.block_shape_budget.retained_bytes() as f64,
        );
        retained_family
    }

    fn compressions_family(&self) -> MetricFamily {
        let mut compressions_family =
            empty_family(&self.family_descs.compressions, MetricType::COUNTER);
        for compressing_encoding in CompressingEncoding::ALL {
            for compression_outcome in CompressionOutcome::ALL {
                push_counter(
                    &mut compressions_family,
                    &[
                        (
                            "encoding",
                            compressing_encoding.response_encoding().encoding_name(),
                        ),
                        ("outcome", compression_outcome.label_value()),
                    ],
                    self.data_plane_counts.sum(DataPlaneCount::Compression(
                        compressing_encoding,
                        compression_outcome,
                    )),
                );
            }
        }
        compressions_family
    }

    fn compression_group_events_family(&self) -> MetricFamily {
        let mut group_events_family = empty_family(
            &self.family_descs.compression_group_events,
            MetricType::COUNTER,
        );
        for compressing_encoding in CompressingEncoding::ALL {
            for group_event in CompressionGroupEvent::ALL {
                push_counter(
                    &mut group_events_family,
                    &[
                        (
                            "encoding",
                            compressing_encoding.response_encoding().encoding_name(),
                        ),
                        ("event", group_event.label_value()),
                    ],
                    self.data_plane_counts
                        .sum(DataPlaneCount::CompressionGroupEvent(
                            compressing_encoding,
                            group_event,
                        )),
                );
            }
        }
        group_events_family
    }

    fn claim_backstop_waits_family(&self) -> MetricFamily {
        let mut backstop_waits_family =
            empty_family(&self.family_descs.claim_backstop_waits, MetricType::COUNTER);
        for waited_claim in WaitedClaim::ALL {
            push_counter(
                &mut backstop_waits_family,
                &[("claim", waited_claim.label_value())],
                self.data_plane_counts
                    .sum(DataPlaneCount::ClaimBackstopWait(waited_claim)),
            );
        }
        backstop_waits_family
    }

    fn block_shape_family(
        &self,
        family_desc: &Desc,
        label_key: &str,
        shape_labels: &[(&str, BlockShapeEvent)],
    ) -> MetricFamily {
        let mut shape_family = empty_family(family_desc, MetricType::COUNTER);
        for &(label_value, shape_event) in shape_labels {
            push_counter(
                &mut shape_family,
                &[(label_key, label_value)],
                self.data_plane_counts
                    .sum(DataPlaneCount::BlockShape(shape_event)),
            );
        }
        shape_family
    }

    fn callback_panics_family(&self) -> MetricFamily {
        let mut panics_family =
            empty_family(&self.family_descs.callback_panics, MetricType::COUNTER);
        for plugin_callback in PluginCallback::ALL {
            push_counter(
                &mut panics_family,
                &[("callback", plugin_callback.label_value())],
                self.data_plane_counts
                    .sum(DataPlaneCount::CallbackPanic(plugin_callback)),
            );
        }
        panics_family
    }
}

impl Collector for ExtendedCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.family_descs.every_desc()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        vec![
            self.subscribe_streams_family(),
            self.retained_bytes_family(),
            self.compressions_family(),
            self.compression_group_events_family(),
            self.claim_backstop_waits_family(),
            self.block_shape_family(
                &self.family_descs.block_shape_writes,
                "outcome",
                &BLOCK_SHAPE_WRITE_OUTCOMES,
            ),
            self.block_shape_family(
                &self.family_descs.block_shape_events,
                "event",
                &BLOCK_SHAPE_EVENT_LABELS,
            ),
            self.callback_panics_family(),
        ]
    }
}

fn empty_family(family_desc: &Desc, family_type: MetricType) -> MetricFamily {
    let mut metric_family = MetricFamily::default();
    metric_family.set_name(family_desc.fq_name.clone());
    metric_family.set_help(family_desc.help.clone());
    metric_family.set_field_type(family_type);
    metric_family
}

/// A series with `label_pairs`, which list the family's label keys in order.
fn labelled_metric(label_pairs: &[(&str, &str)]) -> Metric {
    let mut series_metric = Metric::default();
    series_metric.set_label(
        label_pairs
            .iter()
            .map(|(label_key, label_value)| {
                let mut label_pair = LabelPair::default();
                label_pair.set_name((*label_key).to_owned());
                label_pair.set_value((*label_value).to_owned());
                label_pair
            })
            .collect(),
    );
    series_metric
}

/// Adds a counter series once it counts, as a labelled vector's child appears at its first count.
fn push_counter(metric_family: &mut MetricFamily, label_pairs: &[(&str, &str)], count_sum: u64) {
    if count_sum == 0 {
        return;
    }
    let mut series_metric = labelled_metric(label_pairs);
    let mut series_counter = Counter::default();
    series_counter.set_value(count_sum as f64);
    series_metric.set_counter(series_counter);
    metric_family.mut_metric().push(series_metric);
}

fn push_gauge(metric_family: &mut MetricFamily, label_pairs: &[(&str, &str)], gauge_value: f64) {
    let mut series_metric = labelled_metric(label_pairs);
    let mut series_gauge = Gauge::default();
    series_gauge.set_value(gauge_value);
    series_metric.set_gauge(series_gauge);
    metric_family.mut_metric().push(series_metric);
}

/// A registry, and the generation of the newest extended registration in it.
pub struct ExtendedMetricsRegistry {
    metrics_registry: &'static Registry,
    newest_registration: AtomicU64,
}

impl ExtendedMetricsRegistry {
    pub const fn new(metrics_registry: &'static Registry) -> Self {
        Self {
            metrics_registry,
            newest_registration: AtomicU64::new(0),
        }
    }
}

/// The process registry's, which `load_plugin` registers in.
pub static PROCESS_EXTENDED_METRICS_REGISTRY: LazyLock<ExtendedMetricsRegistry> =
    LazyLock::new(|| ExtendedMetricsRegistry::new(&REGISTRY));

/// One load's extended families in a registry. Its drop removes them, unless a newer
/// registration replaced them.
pub struct ExtendedMetricsRegistration {
    extended_registry: &'static ExtendedMetricsRegistry,
    registered_collector: ExtendedCollector,
    registration_generation: u64,
}

impl ExtendedMetricsRegistration {
    /// Registers `extended_collector`. A registration an earlier load left is replaced.
    pub fn register(
        extended_registry: &'static ExtendedMetricsRegistry,
        extended_collector: ExtendedCollector,
    ) -> prometheus::Result<Self> {
        let metrics_registry = extended_registry.metrics_registry;
        if let Err(register_error) = metrics_registry.register(Box::new(extended_collector.clone()))
        {
            let PrometheusError::AlreadyReg = register_error else {
                return Err(register_error);
            };
            metrics_registry.unregister(Box::new(extended_collector.clone()))?;
            metrics_registry.register(Box::new(extended_collector.clone()))?;
        }
        let registration_generation = extended_registry
            .newest_registration
            .fetch_add(1, Ordering::SeqCst)
            + 1;
        Ok(Self {
            extended_registry,
            registered_collector: extended_collector,
            registration_generation,
        })
    }
}

impl fmt::Debug for ExtendedMetricsRegistration {
    fn fmt(&self, debug_formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_formatter
            .debug_struct("ExtendedMetricsRegistration")
            .field("registration_generation", &self.registration_generation)
            .finish_non_exhaustive()
    }
}

impl Drop for ExtendedMetricsRegistration {
    fn drop(&mut self) {
        let extended_registry = self.extended_registry;
        let newest_registration = extended_registry.newest_registration.load(Ordering::SeqCst);
        if newest_registration != self.registration_generation {
            return;
        }
        // An unload that panics drops this while it unwinds, so an error only logs.
        if let Err(unregister_error) = extended_registry
            .metrics_registry
            .unregister(Box::new(self.registered_collector.clone()))
        {
            log::warn!("the extended metric families stay registered: {unregister_error}");
        }
    }
}

/// A Subscribe session in `subscribe_streams`: counted as started when built, as ended when
/// dropped.
#[derive(Debug)]
pub struct CountedSubscribeStream {
    response_encoding: ResponseEncoding,
}

impl CountedSubscribeStream {
    pub fn start(response_encoding: ResponseEncoding) -> Self {
        DATA_PLANE_COUNTS.add(DataPlaneCount::SubscribeStreamStarted(response_encoding), 1);
        Self { response_encoding }
    }
}

impl Drop for CountedSubscribeStream {
    fn drop(&mut self) {
        DATA_PLANE_COUNTS.add(
            DataPlaneCount::SubscribeStreamEnded(self.response_encoding),
            1,
        );
    }
}
