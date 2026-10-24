//! Scenario files: what to replay, how the plugin runs, who subscribes, and which cases to sweep.
//! See `scenarios/*.toml` for annotated examples.

use {
    crate::{driver::LaneMode, fleet::link::LinkSpec},
    anyhow::Context,
    serde::{Deserialize, Serialize},
    std::{collections::BTreeMap, path::Path},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub window: Window,
    #[serde(default)]
    pub plugin: PluginSpec,
    #[serde(default)]
    pub driver: DriverSpec,
    #[serde(default)]
    pub mix: MixSpec,
    #[serde(default)]
    pub probes: ProbeSpec,
    #[serde(default)]
    pub fleet: FleetSpec,
    #[serde(default)]
    pub cases: Vec<Case>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Window {
    /// `heaviest`, `first`, or a slot number where pacing starts.
    #[serde(default = "default_start")]
    pub start: String,
    /// Paced slots per run.
    #[serde(default = "default_slots")]
    pub slots: u64,
    /// Slots replayed unpaced before pacing, to fill the plugin's replay buffer.
    #[serde(default = "default_prefill")]
    pub prefill_slots: u64,
    /// Seconds after the start of pacing excluded from statistics.
    #[serde(default = "default_warmup")]
    pub warmup_secs: u64,
}

impl Default for Window {
    fn default() -> Self {
        Self {
            start: default_start(),
            slots: default_slots(),
            prefill_slots: default_prefill(),
            warmup_secs: default_warmup(),
        }
    }
}

fn default_start() -> String {
    "heaviest".to_owned()
}
const fn default_slots() -> u64 {
    1500
}
const fn default_prefill() -> u64 {
    300
}
const fn default_warmup() -> u64 {
    60
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginSpec {
    /// grpc.replay_stored_slots; the bundled config keeps 1000.
    #[serde(default)]
    pub replay_stored_slots: Option<u64>,
    #[serde(default)]
    pub worker_threads: Option<usize>,
    /// Plugin config template; configs/geyser-streaming.json when unset.
    #[serde(default)]
    pub config_template: Option<String>,
    /// Environment for the plugin, e.g. experiment switches.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// SCHED_FIFO priority for plugin and driver threads; needs CAP_SYS_NICE.
    #[serde(default)]
    pub fifo_priority: Option<i32>,
}

/// How the driver calls the plugin.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DriverSpec {
    /// `agave`: one thread per agave caller, with agave's ordering guarantees; `single`: one thread.
    #[serde(default = "default_lanes")]
    pub lanes: LaneMode,
    /// Scheduler-worker threads that deliver transaction account writes.
    #[serde(default = "default_sched_lanes")]
    pub sched_lanes: usize,
    /// Entry-notifier and transaction-status threads share the plugin CPUs.
    #[serde(default = "default_true")]
    pub notifiers_on_plugin_cpus: bool,
}

impl Default for DriverSpec {
    fn default() -> Self {
        Self {
            lanes: default_lanes(),
            sched_lanes: default_sched_lanes(),
            notifiers_on_plugin_cpus: true,
        }
    }
}

const fn default_lanes() -> LaneMode {
    LaneMode::Agave
}
const fn default_sched_lanes() -> usize {
    4
}
const fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MixSpec {
    /// Load streams in the generated profile; cases pick the first N.
    #[serde(default = "default_streams")]
    pub streams: usize,
    #[serde(default = "default_commitment")]
    pub commitment: BTreeMap<String, f64>,
    #[serde(default = "default_compression")]
    pub compression: BTreeMap<String, f64>,
    /// Full-block subscribers at confirmed.
    #[serde(default)]
    pub blocks: usize,
    #[serde(default)]
    pub deshred: usize,
    /// Mean streams per `x-subscription-id`.
    #[serde(default = "default_streams_per_id")]
    pub streams_per_id: f64,
    /// Share of subscribed keys that never update: `auto` calibrates it against `rate_targets`.
    #[serde(default = "default_cold")]
    pub cold_key_share: String,
    /// Per-stream message rate quantiles (p50, p90, p99) the calibration aims for.
    #[serde(default = "default_rate_targets")]
    pub rate_targets: [f64; 3],
    /// Total message rate per stream the calibration aims for.
    #[serde(default = "default_total_per_stream")]
    pub total_per_stream: f64,
    /// Slots of the fixture sampled for key activity.
    #[serde(default = "default_sample_slots")]
    pub sample_slots: u64,
    #[serde(default = "default_seed")]
    pub seed: u64,
    /// Network profiles, drawn per load stream by weight. Empty means every stream is local.
    #[serde(default)]
    pub network: Vec<NetworkProfile>,
    #[serde(default)]
    pub churn: Option<ChurnSpec>,
    #[serde(default)]
    pub slow: Option<SlowSpec>,
    /// Unary calls per second by method, e.g. `{ get_slot = 240, get_latest_blockhash = 240 }`.
    #[serde(default)]
    pub unary: BTreeMap<String, f64>,
    /// Connections each unary method is spread over.
    #[serde(default = "default_unary_connections")]
    pub unary_connections: usize,
}

const fn default_unary_connections() -> usize {
    4
}

impl Default for MixSpec {
    fn default() -> Self {
        Self {
            streams: default_streams(),
            commitment: default_commitment(),
            compression: default_compression(),
            blocks: 0,
            deshred: 0,
            streams_per_id: default_streams_per_id(),
            cold_key_share: default_cold(),
            rate_targets: default_rate_targets(),
            total_per_stream: default_total_per_stream(),
            sample_slots: default_sample_slots(),
            seed: default_seed(),
            network: Vec::new(),
            churn: None,
            slow: None,
            unary: BTreeMap::new(),
            unary_connections: default_unary_connections(),
        }
    }
}

const fn default_streams() -> usize {
    400
}
fn default_commitment() -> BTreeMap<String, f64> {
    BTreeMap::from([
        ("processed".to_owned(), 90.0),
        ("confirmed".to_owned(), 7.0),
        ("finalized".to_owned(), 3.0),
    ])
}
fn default_compression() -> BTreeMap<String, f64> {
    BTreeMap::from([
        ("none".to_owned(), 40.0),
        ("zstd".to_owned(), 50.0),
        ("gzip".to_owned(), 10.0),
    ])
}
const fn default_streams_per_id() -> f64 {
    3.7
}
fn default_cold() -> String {
    "auto".to_owned()
}
const fn default_rate_targets() -> [f64; 3] {
    [7.0, 120.0, 4900.0]
}
const fn default_total_per_stream() -> f64 {
    220.0
}
const fn default_sample_slots() -> u64 {
    300
}
const fn default_seed() -> u64 {
    1
}

/// A class of network paths; each stream draws its delay and bandwidth uniformly in the ranges.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkProfile {
    pub name: String,
    #[serde(default = "default_weight")]
    pub weight: f64,
    /// One-way delay range in milliseconds.
    #[serde(default)]
    pub delay_ms: [f64; 2],
    #[serde(default)]
    pub jitter_ms: f64,
    /// Link rate range in megabits per second; unset means unlimited.
    #[serde(default)]
    pub bandwidth_mbps: Option<[f64; 2]>,
    /// Queue on the path beyond the bandwidth-delay product, in KiB.
    #[serde(default = "crate::fleet::link::default_buffer_kib")]
    pub buffer_kib: u64,
    #[serde(default)]
    pub socket_rcvbuf_kib: Option<u32>,
    #[serde(default)]
    pub stall_every_secs: Option<f64>,
    #[serde(default)]
    pub stall_ms: f64,
    /// HTTP/2 windows of these clients in KiB, e.g. 64 for grpc-js and early grpc-go, 2048 for tonic.
    #[serde(default)]
    pub stream_window_kib: Option<u32>,
    #[serde(default)]
    pub connection_window_kib: Option<u32>,
}

const fn default_weight() -> f64 {
    1.0
}

impl NetworkProfile {
    /// Client windows when either is set; the connection window is at least the stream window.
    pub fn windows_kib(&self) -> Option<[u32; 2]> {
        match (self.stream_window_kib, self.connection_window_kib) {
            (None, None) => None,
            (stream, connection) => {
                let stream = stream.unwrap_or(8192);
                Some([stream, connection.unwrap_or(stream * 4).max(stream)])
            }
        }
    }

    /// The link for one stream, with `unit_*` in [0, 1) picking points in the ranges.
    pub fn link(&self, unit_delay: f64, unit_bandwidth: f64) -> LinkSpec {
        let pick = |range: [f64; 2], unit: f64| range[0] + (range[1] - range[0]) * unit;
        LinkSpec {
            delay_ms: pick(self.delay_ms, unit_delay),
            jitter_ms: self.jitter_ms,
            bandwidth_mbps: self.bandwidth_mbps.map(|range| pick(range, unit_bandwidth)),
            buffer_kib: self.buffer_kib,
            socket_rcvbuf_kib: self.socket_rcvbuf_kib,
            stall_every_secs: self.stall_every_secs,
            stall_ms: self.stall_ms,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChurnSpec {
    /// Percent of load streams that reconnect.
    pub share: f64,
    pub every_secs: f64,
    /// Resume this many slots behind the newest slot; 0 resumes live.
    #[serde(default)]
    pub from_slot_back: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SlowSpec {
    /// Percent of load streams on a slow link.
    pub share: f64,
    pub bandwidth_mbps: f64,
    #[serde(default)]
    pub delay_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProbeSpec {
    /// Probes at processed; half uncompressed, half zstd.
    #[serde(default = "default_processed_probes")]
    pub processed: usize,
    /// Probes at confirmed.
    #[serde(default = "default_confirmed")]
    pub confirmed: usize,
    /// Network profile name for the probes; local when unset.
    #[serde(default)]
    pub network: Option<String>,
    /// Account pubkeys the probes watch; one busy account from the fixture when empty.
    #[serde(default)]
    pub accounts: Vec<String>,
    /// Owners the probes watch; one busy owner from the fixture when empty.
    #[serde(default)]
    pub owners: Vec<String>,
    /// Transaction `account_include` keys; two busy keys from the fixture when empty.
    #[serde(default)]
    pub transactions: Vec<String>,
}

impl Default for ProbeSpec {
    fn default() -> Self {
        Self {
            processed: default_processed_probes(),
            confirmed: default_confirmed(),
            network: None,
            accounts: Vec::new(),
            owners: Vec::new(),
            transactions: Vec::new(),
        }
    }
}

const fn default_processed_probes() -> usize {
    8
}
const fn default_confirmed() -> usize {
    2
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetSpec {
    #[serde(default = "default_streams_per_connection")]
    pub streams_per_connection: usize,
    #[serde(default = "default_stream_window_mib")]
    pub stream_window_mib: u32,
    #[serde(default = "default_connection_window_mib")]
    pub connection_window_mib: u32,
    /// Seconds between scrapes of the plugin's metrics, as a metrics agent does; 0 disables.
    #[serde(default = "default_scrape_secs")]
    pub scrape_secs: f64,
}

impl Default for FleetSpec {
    fn default() -> Self {
        Self {
            streams_per_connection: default_streams_per_connection(),
            stream_window_mib: default_stream_window_mib(),
            connection_window_mib: default_connection_window_mib(),
            scrape_secs: default_scrape_secs(),
        }
    }
}

const fn default_scrape_secs() -> f64 {
    10.0
}

const fn default_streams_per_connection() -> usize {
    1
}
const fn default_stream_window_mib() -> u32 {
    8
}
const fn default_connection_window_mib() -> u32 {
    32
}

/// One sweep: every combination of the listed values, each repeated `repeats` times per variant.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Case {
    pub name: String,
    /// Load streams (first N of the profile).
    #[serde(default)]
    pub streams: Vec<usize>,
    #[serde(default)]
    pub blocks: Vec<usize>,
    #[serde(default)]
    pub deshred: Vec<usize>,
    #[serde(default = "default_repeats")]
    pub repeats: usize,
    /// Extra plugin environment for this case.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// `[mix]` keys this case replaces, e.g. `mix = { compression = { zstd = 100.0 } }`.
    #[serde(default)]
    pub mix: Option<toml::Table>,
    /// `[probes]` keys this case replaces.
    #[serde(default)]
    pub probes: Option<toml::Table>,
}

const fn default_repeats() -> usize {
    1
}

/// One planned run of a case.
#[derive(Debug, Clone, Serialize)]
pub struct CasePoint {
    pub case: String,
    pub streams: usize,
    pub blocks: usize,
    pub deshred: usize,
    pub repeat: usize,
    pub env: BTreeMap<String, String>,
    /// Name of the subscriber profile: `base`, or the case name when the case overrides the mix.
    pub profile: String,
    #[serde(skip)]
    pub mix: MixSpec,
    #[serde(skip)]
    pub probes: ProbeSpec,
}

impl CasePoint {
    pub fn label(&self) -> String {
        format!(
            "{}-n{}-b{}-d{}-r{}",
            self.case, self.streams, self.blocks, self.deshred, self.repeat
        )
    }

    /// Runs with the same label minus the repeat are compared with each other.
    pub fn group(&self) -> String {
        format!(
            "{}-n{}-b{}-d{}",
            self.case, self.streams, self.blocks, self.deshred
        )
    }
}

impl Scenario {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        Self::load_with(path, &[])
    }

    /// Loads a scenario with `KEY=VALUE` overrides of dotted keys, e.g. `plugin.fifo_priority=40`.
    pub fn load_with(path: &Path, overrides: &[String]) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let mut table: toml::Table =
            toml::from_str(&text).with_context(|| format!("failed to parse {}", path.display()))?;
        for item in overrides {
            let (key, raw) = item
                .split_once('=')
                .with_context(|| format!("--set must be KEY=VALUE: {item}"))?;
            set_path(&mut table, key.trim(), parse_value(raw.trim()))?;
        }
        let scenario: Self = toml::Value::Table(table)
            .try_into()
            .with_context(|| format!("failed to parse {}", path.display()))?;
        scenario.validate()?;
        Ok(scenario)
    }

    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.cases.is_empty(),
            "scenario {} has no [[cases]]",
            self.name
        );
        let mut names = std::collections::HashSet::new();
        for case in &self.cases {
            anyhow::ensure!(
                names.insert(case.name.as_str()),
                "case name {} is used twice",
                case.name
            );
            anyhow::ensure!(
                !case.name.is_empty()
                    && case
                        .name
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.'),
                "case name {:?} must use only letters, digits, '_' and '.'",
                case.name
            );
            let (mix, probes) = self
                .case_mix(case)
                .with_context(|| format!("case {}: bad mix or probes override", case.name))?;
            for streams in &case.streams {
                anyhow::ensure!(
                    *streams <= mix.streams,
                    "case {} asks for {streams} streams; mix.streams is {}",
                    case.name,
                    mix.streams
                );
            }
            for blocks in &case.blocks {
                anyhow::ensure!(
                    *blocks <= mix.blocks,
                    "case {} asks for {blocks} block subscribers; mix.blocks is {}",
                    case.name,
                    mix.blocks
                );
            }
            for deshred in &case.deshred {
                anyhow::ensure!(
                    *deshred <= mix.deshred,
                    "case {} asks for {deshred} deshred subscribers; mix.deshred is {}",
                    case.name,
                    mix.deshred
                );
            }
            if let Some(name) = &probes.network {
                anyhow::ensure!(
                    mix.network.iter().any(|profile| &profile.name == name),
                    "case {}: probes.network {name} is not a mix.network profile",
                    case.name
                );
            }
            for method in mix.unary.keys() {
                anyhow::ensure!(
                    crate::fleet::unary::METHODS.contains(&method.as_str()),
                    "case {}: unknown unary method {method}; expected one of {:?}",
                    case.name,
                    crate::fleet::unary::METHODS
                );
            }
        }
        Ok(())
    }

    /// The case's mix and probes: the scenario's, with the case's overrides applied.
    pub fn case_mix(&self, case: &Case) -> anyhow::Result<(MixSpec, ProbeSpec)> {
        Ok((
            overlay(&self.mix, case.mix.as_ref())?,
            overlay(&self.probes, case.probes.as_ref())?,
        ))
    }

    /// Every run point of every case, in order.
    pub fn points(&self) -> Vec<CasePoint> {
        let mut points = Vec::new();
        for case in &self.cases {
            let (mix, probes) = self.case_mix(case).expect("validated on load");
            let profile = if case.mix.is_some() || case.probes.is_some() {
                case.name.clone()
            } else {
                "base".to_owned()
            };
            let streams = non_empty(&case.streams, mix.streams);
            let blocks = non_empty(&case.blocks, 0);
            let deshred = non_empty(&case.deshred, mix.deshred);
            for repeat in 0..case.repeats.max(1) {
                for &n in &streams {
                    for &b in &blocks {
                        for &d in &deshred {
                            let mut env = self.plugin.env.clone();
                            env.extend(case.env.clone());
                            points.push(CasePoint {
                                case: case.name.clone(),
                                streams: n,
                                blocks: b,
                                deshred: d,
                                repeat,
                                env,
                                profile: profile.clone(),
                                mix: mix.clone(),
                                probes: probes.clone(),
                            });
                        }
                    }
                }
            }
        }
        points
    }
}

/// A TOML value, or the raw text as a string when it does not parse, so `start=first` needs no quotes.
fn parse_value(raw: &str) -> toml::Value {
    toml::from_str::<toml::Table>(&format!("value = {raw}"))
        .ok()
        .and_then(|mut table| table.remove("value"))
        .unwrap_or_else(|| toml::Value::String(raw.to_owned()))
}

/// Sets a dotted key such as `mix.churn.share`, creating tables on the way.
fn set_path(table: &mut toml::Table, key: &str, value: toml::Value) -> anyhow::Result<()> {
    let (parents, last) = match key.rsplit_once('.') {
        Some((parents, last)) => (parents.split('.').collect::<Vec<_>>(), last),
        None => (Vec::new(), key),
    };
    let mut current = table;
    for part in parents {
        current = current
            .entry(part.to_owned())
            .or_insert_with(|| toml::Value::Table(toml::Table::new()))
            .as_table_mut()
            .with_context(|| format!("{part} in {key} is not a table"))?;
    }
    current.insert(last.to_owned(), value);
    Ok(())
}

/// `base` with the top-level keys of `over` replaced; unknown keys are rejected.
fn overlay<T: Serialize + serde::de::DeserializeOwned>(
    base: &T,
    over: Option<&toml::Table>,
) -> anyhow::Result<T> {
    let mut value = toml::Value::try_from(base)?;
    if let (Some(table), Some(over)) = (value.as_table_mut(), over) {
        for (key, item) in over {
            table.insert(key.clone(), item.clone());
        }
    }
    Ok(value.try_into()?)
}

fn non_empty(values: &[usize], default: usize) -> Vec<usize> {
    if values.is_empty() {
        vec![default]
    } else {
        values.to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::Scenario;

    #[test]
    fn case_overrides_replace_mix_keys() {
        let scenario: Scenario = toml::from_str(
            r#"
            name = "t"
            [mix]
            streams = 10
            [[cases]]
            name = "zstd"
            mix = { compression = { zstd = 100.0 }, streams = 20 }
            streams = [20]
            [[cases]]
            name = "plain"
            "#,
        )
        .unwrap();
        scenario.validate().unwrap();
        let points = scenario.points();
        assert_eq!(points[0].profile, "zstd");
        assert_eq!(points[0].mix.compression.len(), 1);
        assert_eq!(points[0].mix.streams, 20);
        assert_eq!(points[1].profile, "base");
        assert_eq!(points[1].mix.compression.len(), 3);
        let dir = std::env::temp_dir().join(format!("ygb-scenario-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("s.toml");
        std::fs::write(&path, "name = \"t\"\n[[cases]]\nname = \"c\"\n").unwrap();
        let set = |items: &[&str]| {
            Scenario::load_with(
                &path,
                &items
                    .iter()
                    .map(|item| item.to_string())
                    .collect::<Vec<_>>(),
            )
        };
        let scenario = set(&[
            "plugin.fifo_priority=40",
            "window.start=first",
            "mix.churn.share=5",
            "mix.churn.every_secs=60",
        ])
        .unwrap();
        assert_eq!(scenario.plugin.fifo_priority, Some(40));
        assert_eq!(scenario.window.start, "first");
        assert_eq!(
            scenario.mix.churn.as_ref().map(|churn| churn.share),
            Some(5.0)
        );
        assert!(set(&["plugin.fifo_prio=40"]).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
        let bad: Scenario = toml::from_str(
            r#"
            name = "t"
            [[cases]]
            name = "typo"
            mix = { compresion = { zstd = 100.0 } }
            "#,
        )
        .unwrap();
        assert!(bad.validate().is_err());
    }

    #[test]
    fn bundled_scenarios_parse() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("scenarios");
        let mut count = 0;
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|ext| ext == "toml") {
                let scenario = Scenario::load(&path)
                    .unwrap_or_else(|error| panic!("{}: {error:#}", path.display()));
                assert!(!scenario.points().is_empty(), "{}", path.display());
                count += 1;
            }
        }
        assert!(count > 0);
    }
}
