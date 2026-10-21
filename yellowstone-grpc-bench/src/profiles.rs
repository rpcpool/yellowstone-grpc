//! `ygb profiles`: synthetic subscribers shaped like production, with keys drawn from a fixture.
//! Filter shapes follow fra215 `filter_stats`; the cold-key share is calibrated to production rates.
//! Networks, churn, slow links and subscription ids come from the scenario's `[mix]`.

use {
    crate::{
        fixture::{FixtureReader, Manifest, StreamId},
        fleet::profile::{
            AccountsFilter, BlocksFilter, Churn, Commitment, Compression, Profile, Request, Role,
            Subscriber, TransactionsFilter, UnaryLoad,
        },
        frames::{self, Decoded},
        scenario::{MixSpec, NetworkProfile, ProbeSpec, Scenario},
    },
    log::info,
    std::{
        collections::{BTreeMap, HashMap, HashSet},
        fs,
        path::{Path, PathBuf},
    },
    yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof,
};

#[derive(Debug, Clone, clap::Args)]
pub struct GenArgs {
    #[clap(long)]
    fixture: PathBuf,
    /// Scenario whose `[mix]` and `[probes]` drive the profile; built-in defaults otherwise
    #[clap(long)]
    scenario: Option<PathBuf>,
    /// Override mix.streams
    #[clap(long)]
    streams: Option<usize>,
    /// Override mix.seed
    #[clap(long)]
    seed: Option<u64>,
    #[clap(long)]
    out: PathBuf,
}

pub fn run(args: GenArgs) -> anyhow::Result<()> {
    let (mut mix, probes) = match &args.scenario {
        Some(path) => {
            let scenario = Scenario::load(path)?;
            (scenario.mix, scenario.probes)
        }
        None => (MixSpec::default(), ProbeSpec::default()),
    };
    if let Some(streams) = args.streams {
        mix.streams = streams;
    }
    if let Some(seed) = args.seed {
        mix.seed = seed;
    }
    let profile = generate(&args.fixture, &mix, &probes)?;
    fs::write(&args.out, serde_json::to_vec_pretty(&profile)?)?;
    info!("wrote {}: {}", args.out.display(), profile.description);
    Ok(())
}

/// Account and transaction-key activity in the sampled window, as rates per second.
pub struct Activity {
    pubkeys: Vec<(String, f64)>,
    owners: Vec<(String, f64)>,
    tx_keys: Vec<(String, f64)>,
    entry_rate: f64,
    slot_rate: f64,
}

pub fn generate(fixture: &Path, mix: &MixSpec, probes: &ProbeSpec) -> anyhow::Result<Profile> {
    generate_from(&sample(fixture, mix.sample_slots)?, fixture, mix, probes)
}

/// Builds a profile from activity already sampled from `fixture`.
pub fn generate_from(
    activity: &Activity,
    fixture: &Path,
    mix: &MixSpec,
    probes: &ProbeSpec,
) -> anyhow::Result<Profile> {
    let total_target = mix.total_per_stream * mix.streams as f64;
    let candidates: Vec<f64> = if mix.cold_key_share == "auto" {
        vec![0.0, 0.5, 0.8, 0.9, 0.95, 0.98, 0.99, 0.995, 0.998, 0.999]
    } else {
        vec![mix.cold_key_share.parse()?]
    };
    let mut best: Option<(f64, f64, Vec<Subscriber>, Vec<f64>)> = None;
    for cold in candidates {
        let (subscribers, rates) = load_streams(mix, activity, cold)?;
        let q = quantiles(&rates);
        let total: f64 = rates.iter().sum();
        let score = q
            .iter()
            .zip(&mix.rate_targets)
            .map(|(got, want)| (got.max(0.1) / want.max(0.1)).ln().abs())
            .sum::<f64>()
            + (total.max(1.0) / total_target.max(1.0)).ln().abs();
        info!(
            "cold {cold:.3}: per-stream msg/s p50 {:.1} p90 {:.1} p99 {:.1}, total {total:.0} (target {total_target:.0}), score {score:.2}",
            q[0], q[1], q[2]
        );
        if best
            .as_ref()
            .is_none_or(|(best_score, ..)| score < *best_score)
        {
            best = Some((score, cold, subscribers, rates));
        }
    }
    let (_, cold, mut load, rates) = best.expect("at least one candidate");
    load.extend(special_streams(mix));
    assign_customers(mix, &mut load)?;
    let mut subscribers = load;
    subscribers.extend(probe_streams(mix, probes)?);
    Ok(Profile {
        description: format!(
            "{} load streams from {} (seed {}, cold key share {cold:.3}, predicted {:.0} msg/s), {} blocks, {} deshred, {} probes",
            mix.streams,
            fixture.display(),
            mix.seed,
            rates.iter().sum::<f64>(),
            mix.blocks,
            mix.deshred,
            probes.streamlab + probes.confirmed,
        ),
        subscribers,
        unary: mix
            .unary
            .iter()
            .filter(|(_, rate)| **rate > 0.0)
            .map(|(method, rate)| UnaryLoad {
                method: method.clone(),
                rate_per_sec: *rate,
                connections: mix.unary_connections,
            })
            .collect(),
    })
}

/// Per-key update rates over `sample_slots` slots near the start of the fixture.
pub fn sample(fixture: &Path, sample_slots: u64) -> anyhow::Result<Activity> {
    let manifest = Manifest::load(fixture)?;
    let first_slot = manifest.first_slot.unwrap_or_default() + 64;
    let last_slot = first_slot + sample_slots;
    let mut pubkeys: HashMap<Vec<u8>, u64> = HashMap::new();
    let mut owners: HashMap<Vec<u8>, u64> = HashMap::new();
    let mut tx_keys: HashMap<Vec<u8>, u64> = HashMap::new();
    let (mut entries, mut metas) = (0u64, 0u64);
    let (mut first_ns, mut last_ns) = (u64::MAX, 0u64);
    let mut reader = FixtureReader::open_from(fixture, &manifest.chunks, first_slot);
    while let Some(frame) = reader.next_frame()? {
        if frame.stream == StreamId::Deshred {
            continue;
        }
        let Decoded::Main(update) = frames::decode(frame.stream, &frame.data)? else {
            continue;
        };
        let info = frames::main_info(&update);
        let Some(slot) = info.slot else { continue };
        if slot < first_slot {
            continue;
        }
        if slot >= last_slot {
            break;
        }
        if let Some(created) = info.created_at_ns {
            first_ns = first_ns.min(created);
            last_ns = last_ns.max(created);
        }
        match update.update_oneof {
            Some(UpdateOneof::Account(msg)) => {
                if let Some(account) = msg.account {
                    *pubkeys.entry(account.pubkey).or_default() += 1;
                    *owners.entry(account.owner).or_default() += 1;
                }
            }
            Some(UpdateOneof::Transaction(msg)) => {
                let Some(info) = msg.transaction else {
                    continue;
                };
                if info.is_vote {
                    continue;
                }
                let mut keys: HashSet<Vec<u8>> = HashSet::new();
                if let Some(message) = info.transaction.and_then(|tx| tx.message) {
                    keys.extend(message.account_keys);
                }
                if let Some(meta) = info.meta {
                    keys.extend(meta.loaded_writable_addresses);
                    keys.extend(meta.loaded_readonly_addresses);
                }
                for key in keys {
                    *tx_keys.entry(key).or_default() += 1;
                }
            }
            Some(UpdateOneof::Entry(_)) => entries += 1,
            Some(UpdateOneof::BlockMeta(_)) => metas += 1,
            _ => {}
        }
    }
    anyhow::ensure!(last_ns > first_ns, "sample window is empty");
    let secs = (last_ns - first_ns) as f64 / 1e9;
    let to_rates = |map: HashMap<Vec<u8>, u64>| {
        let mut rates: Vec<(String, f64)> = map
            .into_iter()
            .map(|(key, count)| (bs58::encode(key).into_string(), count as f64 / secs))
            .collect();
        rates.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        rates
    };
    let activity = Activity {
        pubkeys: to_rates(pubkeys),
        owners: to_rates(owners),
        tx_keys: to_rates(tx_keys),
        entry_rate: entries as f64 / secs,
        slot_rate: metas as f64 / secs,
    };
    info!(
        "sampled {secs:.0} s: {} pubkeys, {} owners, {} tx keys, {:.0} entries/s, {:.2} slots/s",
        activity.pubkeys.len(),
        activity.owners.len(),
        activity.tx_keys.len(),
        activity.entry_rate,
        activity.slot_rate
    );
    Ok(activity)
}

/// Picks seen keys uniformly, or with probability `cold` a fresh random key that never updates.
struct Sampler<'a> {
    keys: &'a [(String, f64)],
    cold: f64,
}

impl<'a> Sampler<'a> {
    const fn new(keys: &'a [(String, f64)], cold: f64) -> Self {
        Self { keys, cold }
    }

    fn pick(&self, rng: &mut Rng) -> (String, f64) {
        if self.keys.is_empty() || rng.chance(self.cold) {
            let mut key = [0u8; 32];
            for chunk in key.chunks_mut(8) {
                chunk.copy_from_slice(&rng.next().to_le_bytes());
            }
            return (bs58::encode(key).into_string(), 0.0);
        }
        let (key, rate) = &self.keys[rng.range(0, self.keys.len() - 1)];
        (key.clone(), *rate)
    }
}

/// SplitMix64: small and deterministic, enough for reproducible profiles.
pub struct Rng(pub u64);

impl Rng {
    pub const fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    pub fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }

    pub fn chance(&mut self, p: f64) -> bool {
        self.unit() < p
    }

    pub const fn range(&mut self, low: usize, high: usize) -> usize {
        low + (self.next() % (high - low + 1) as u64) as usize
    }

    /// Index chosen with probability proportional to `weights`.
    pub fn weighted(&mut self, weights: &[f64]) -> usize {
        let mut target = self.unit() * weights.iter().sum::<f64>();
        for (index, weight) in weights.iter().enumerate() {
            if target < *weight {
                return index;
            }
            target -= weight;
        }
        weights.len() - 1
    }
}

fn split<T: Copy>(
    map: &BTreeMap<String, f64>,
    names: &[(&str, T)],
) -> anyhow::Result<Vec<(f64, T)>> {
    for key in map.keys() {
        anyhow::ensure!(
            names.iter().any(|(name, _)| name == key),
            "unknown split key {key}; expected one of {:?}",
            names.iter().map(|(name, _)| *name).collect::<Vec<_>>()
        );
    }
    Ok(names
        .iter()
        .map(|(name, value)| (map.get(*name).copied().unwrap_or(0.0), *value))
        .collect())
}

/// Production-shaped load streams with their predicted message rates.
fn load_streams(
    mix: &MixSpec,
    activity: &Activity,
    cold: f64,
) -> anyhow::Result<(Vec<Subscriber>, Vec<f64>)> {
    let commitment = split(
        &mix.commitment,
        &[
            ("processed", Commitment::Processed),
            ("confirmed", Commitment::Confirmed),
            ("finalized", Commitment::Finalized),
        ],
    )?;
    let compression = split(
        &mix.compression,
        &[
            ("none", Compression::None),
            ("zstd", Compression::Zstd),
            ("gzip", Compression::Gzip),
        ],
    )?;
    let commitment_weights: Vec<f64> = commitment.iter().map(|(weight, _)| *weight).collect();
    let compression_weights: Vec<f64> = compression.iter().map(|(weight, _)| *weight).collect();
    let mut rng = Rng(mix.seed);
    let pubkeys = Sampler::new(&activity.pubkeys, cold);
    let owners = Sampler::new(&activity.owners, 0.0);
    let tx_keys = Sampler::new(&activity.tx_keys, cold);
    let mut subscribers = Vec::new();
    let mut rates = Vec::new();

    for index in 0..mix.streams {
        let commitment = commitment[rng.weighted(&commitment_weights)].1;
        let compression = compression[rng.weighted(&compression_weights)].1;
        let mut request = Request {
            commitment,
            ..Default::default()
        };
        let mut rate = 0.0;
        if rng.chance(0.6) {
            request.slots = Some(false);
            rate += activity.slot_rate * 3.0;
        }
        if rng.chance(0.12) {
            request.blocks_meta = true;
            rate += activity.slot_rate;
        }
        if rng.chance(0.015) {
            request.entries = true;
            rate += activity.entry_rate;
        }
        if rng.chance(0.8) {
            let mut chosen_keys: HashMap<String, f64> = HashMap::new();
            let mut chosen_owners: HashMap<String, f64> = HashMap::new();
            let filters = 1 + (rng.unit().ln() / (1.0 - 1.0 / 12.0f64).ln()) as usize;
            for filter in 0..filters.min(64) {
                let shape = rng.unit();
                let mut account_filter = AccountsFilter::default();
                if shape < 0.11 {
                    let (owner, owner_rate) = owners.pick(&mut rng);
                    account_filter.owner.push(owner.clone());
                    chosen_owners.insert(owner, owner_rate);
                } else {
                    let count = if shape < 0.64 {
                        1
                    } else if shape < 0.99 {
                        rng.range(100, 1000)
                    } else {
                        rng.range(2, 99)
                    };
                    for _ in 0..count {
                        let (key, key_rate) = pubkeys.pick(&mut rng);
                        account_filter.account.push(key.clone());
                        chosen_keys.insert(key, key_rate);
                    }
                    account_filter.account.sort();
                    account_filter.account.dedup();
                }
                request
                    .accounts
                    .insert(format!("a{filter}"), account_filter);
            }
            rate += chosen_keys.values().sum::<f64>() + chosen_owners.values().sum::<f64>();
        }
        if rng.chance(0.43) {
            let shape = rng.unit();
            let count = if shape < 0.29 {
                1
            } else if shape < 0.77 {
                rng.range(101, 1000)
            } else {
                rng.range(2, 100)
            };
            let mut chosen: HashMap<String, f64> = HashMap::new();
            for _ in 0..count {
                let (key, key_rate) = tx_keys.pick(&mut rng);
                chosen.insert(key, key_rate);
            }
            let mut include: Vec<String> = chosen.keys().cloned().collect();
            include.sort();
            request.transactions.insert(
                "t0".to_owned(),
                TransactionsFilter {
                    vote: Some(false),
                    account_include: include,
                    ..Default::default()
                },
            );
            rate += chosen.values().sum::<f64>();
        }
        // About 3% of fra215 customers also take transaction statuses (filter_stats, transaction_status).
        if rng.chance(0.03) {
            let mut chosen: HashMap<String, f64> = HashMap::new();
            for _ in 0..rng.range(1, 100) {
                let (key, key_rate) = tx_keys.pick(&mut rng);
                chosen.insert(key, key_rate);
            }
            let mut include: Vec<String> = chosen.keys().cloned().collect();
            include.sort();
            request.transactions_status.insert(
                "s0".to_owned(),
                TransactionsFilter {
                    vote: Some(false),
                    account_include: include,
                    ..Default::default()
                },
            );
            rate += chosen.values().sum::<f64>();
        }
        rates.push(rate);
        subscribers.push(Subscriber {
            name: format!("load-{index}"),
            class: format!("load-{}", commitment_name(commitment)),
            role: Role::Load,
            compression,
            count: 1,
            request,
            link: None,
            subscription_id: None,
            churn: None,
            windows_kib: None,
        });
    }
    Ok((subscribers, rates))
}

/// Full-block and deshred subscribers, after the load streams so cases can cap each group.
fn special_streams(mix: &MixSpec) -> Vec<Subscriber> {
    let mut subscribers = Vec::new();
    for index in 0..mix.blocks {
        let mut blocks = BTreeMap::new();
        blocks.insert(
            "blocks".to_owned(),
            BlocksFilter {
                include_transactions: Some(true),
                include_accounts: Some(index % 3 == 0),
                include_entries: Some(false),
                ..Default::default()
            },
        );
        subscribers.push(Subscriber {
            name: format!("blocks-{index}"),
            class: "blocks".to_owned(),
            role: Role::Load,
            compression: if index % 2 == 0 {
                Compression::Zstd
            } else {
                Compression::None
            },
            count: 1,
            request: Request {
                commitment: Commitment::Confirmed,
                blocks,
                ..Default::default()
            },
            link: None,
            subscription_id: None,
            churn: None,
            windows_kib: None,
        });
    }
    for index in 0..mix.deshred {
        subscribers.push(Subscriber {
            name: format!("deshred-{index}"),
            class: "deshred".to_owned(),
            role: Role::Load,
            compression: Compression::None,
            count: 1,
            request: Request {
                slots: Some(false),
                deshred: Some(TransactionsFilter {
                    vote: Some(false),
                    ..Default::default()
                }),
                ..Default::default()
            },
            link: None,
            subscription_id: None,
            churn: None,
            windows_kib: None,
        });
    }
    subscribers
}

/// Groups streams into customers, each on one network path, then marks churning and slow streams.
fn assign_customers(mix: &MixSpec, subscribers: &mut [Subscriber]) -> anyhow::Result<()> {
    let weights: Vec<f64> = mix.network.iter().map(|profile| profile.weight).collect();
    let mut rng = Rng(mix.seed ^ 0x6E65_7477_6F72_6B00);
    let mut customer = 0usize;
    let mut remaining = 0usize;
    let mut path = None;
    for sub in subscribers.iter_mut() {
        if remaining == 0 {
            customer += 1;
            remaining = geometric(&mut rng, mix.streams_per_id);
            path = (!mix.network.is_empty()).then(|| {
                let profile: &NetworkProfile = &mix.network[rng.weighted(&weights)];
                (profile.link(rng.unit(), rng.unit()), profile.windows_kib())
            });
        }
        remaining -= 1;
        sub.subscription_id = Some(format!("customer-{customer}"));
        sub.link = path
            .as_ref()
            .map(|(link, _)| link.clone())
            .filter(|link| !link.is_local());
        sub.windows_kib = path.as_ref().and_then(|(_, windows)| *windows);
    }
    let mut rng = Rng(mix.seed ^ 0x6368_7572_6E00);
    for sub in subscribers
        .iter_mut()
        .filter(|sub| sub.class.starts_with("load-"))
    {
        if let Some(churn) = &mix.churn {
            if rng.chance(churn.share / 100.0) {
                sub.churn = Some(Churn {
                    every_secs: churn.every_secs,
                    from_slot_back: churn.from_slot_back,
                });
            }
        }
        if let Some(slow) = &mix.slow {
            if rng.chance(slow.share / 100.0) {
                let mut link = sub.link.clone().unwrap_or_default();
                link.bandwidth_mbps = Some(slow.bandwidth_mbps);
                link.delay_ms = link.delay_ms.max(slow.delay_ms);
                sub.link = Some(link);
            }
        }
    }
    Ok(())
}

/// Customer size with the given mean, at least 1.
fn geometric(rng: &mut Rng, mean: f64) -> usize {
    if mean <= 1.0 {
        return 1;
    }
    let p = 1.0 / mean;
    1 + (rng.unit().max(1e-12).ln() / (1.0 - p).ln()) as usize
}

/// Streamlab's exact subscription from nyc289, uncompressed and zstd, plus confirmed copies.
fn probe_streams(mix: &MixSpec, probes: &ProbeSpec) -> anyhow::Result<Vec<Subscriber>> {
    let (link, windows_kib) = match &probes.network {
        Some(name) => {
            let profile = mix
                .network
                .iter()
                .find(|profile| &profile.name == name)
                .ok_or_else(|| anyhow::anyhow!("unknown probe network {name}"))?;
            (
                Some(profile.link(0.5, 0.5)).filter(|link| !link.is_local()),
                profile.windows_kib(),
            )
        }
        None => (None, None),
    };
    let streamlab = || {
        let mut accounts = BTreeMap::new();
        accounts.insert(
            "streamlab_ping".to_owned(),
            AccountsFilter {
                account: vec!["ping6gwBZx1ccMMFyLgkVSupUmujYrFidEXuNRPq989".to_owned()],
                owner: vec![],
            },
        );
        accounts.insert(
            "streamlab_owner".to_owned(),
            AccountsFilter {
                account: vec![],
                owner: vec!["9H6tua7jkLhdm3w8BvgpTn5LZNU7g4ZynDmCiNN3q6Rp".to_owned()],
            },
        );
        let mut transactions = BTreeMap::new();
        transactions.insert(
            "streamlab".to_owned(),
            TransactionsFilter {
                account_include: [
                    "9H6tua7jkLhdm3w8BvgpTn5LZNU7g4ZynDmCiNN3q6Rp",
                    "HEvSKofvBgfaexv23kMabbYqxasxU3mQ4ibBMEmJWHny",
                    "ping6gwBZx1ccMMFyLgkVSupUmujYrFidEXuNRPq989",
                    "rec5EKMGg6MxZYaMdyBfgwp4d5rB9T1VQH5pJv5LtFJ",
                ]
                .map(str::to_owned)
                .to_vec(),
                ..Default::default()
            },
        );
        Request {
            accounts,
            transactions,
            entries: true,
            ..Default::default()
        }
    };
    let probe = |name: &str, class: &str, compression, count, request| Subscriber {
        name: name.to_owned(),
        class: class.to_owned(),
        role: Role::Probe,
        compression,
        count,
        request,
        link: link.clone(),
        subscription_id: Some(name.to_owned()),
        churn: None,
        windows_kib,
    };
    let half = probes.streamlab / 2;
    let mut subscribers = Vec::new();
    if probes.streamlab - half > 0 {
        subscribers.push(probe(
            "streamlab-none",
            "streamlab",
            Compression::None,
            probes.streamlab - half,
            streamlab(),
        ));
    }
    if half > 0 {
        subscribers.push(probe(
            "streamlab-zstd",
            "streamlab",
            Compression::Zstd,
            half,
            streamlab(),
        ));
    }
    if probes.confirmed > 0 {
        subscribers.push(probe(
            "streamlab-confirmed",
            "streamlab-confirmed",
            Compression::None,
            probes.confirmed,
            Request {
                commitment: Commitment::Confirmed,
                ..streamlab()
            },
        ));
    }
    Ok(subscribers)
}

const fn commitment_name(commitment: Commitment) -> &'static str {
    match commitment {
        Commitment::Processed => "processed",
        Commitment::Confirmed => "confirmed",
        Commitment::Finalized => "finalized",
    }
}

fn quantiles(rates: &[f64]) -> Vec<f64> {
    let mut sorted = rates.to_vec();
    sorted.sort_by(f64::total_cmp);
    if sorted.is_empty() {
        return vec![0.0; 3];
    }
    [0.5, 0.9, 0.99]
        .iter()
        .map(|q| sorted[((sorted.len() - 1) as f64 * q).round() as usize])
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{geometric, Rng};

    #[test]
    fn geometric_mean_is_close() {
        let mut rng = Rng(3);
        let total: usize = (0..20_000).map(|_| geometric(&mut rng, 3.7)).sum();
        let mean = total as f64 / 20_000.0;
        assert!((mean - 3.7).abs() < 0.2, "mean {mean}");
    }
}
