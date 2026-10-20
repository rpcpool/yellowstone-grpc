//! `ygb profiles`: synthetic subscribers shaped like production, with pubkeys drawn from a fixture.
//! The share of cold keys (never updated) is chosen so predicted rates match production.

use {
    crate::{
        fixture::{FixtureReader, Manifest, StreamId},
        fleet::profile::{
            AccountsFilter, BlocksFilter, Commitment, Compression, Profile, Request, Role, Subscriber,
            TransactionsFilter,
        },
        frames::{self, Decoded},
    },
    log::info,
    std::{
        collections::{BTreeMap, HashMap, HashSet},
        fs,
        path::PathBuf,
    },
    yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof,
};

#[derive(Debug, Clone, clap::Args)]
pub struct GenArgs {
    #[clap(long)]
    fixture: PathBuf,
    /// Slots of the fixture sampled for account and transaction rates
    #[clap(long, default_value_t = 300)]
    sample_slots: u64,
    /// Load streams to generate
    #[clap(long, default_value_t = 400)]
    streams: usize,
    #[clap(long, default_value_t = 1)]
    seed: u64,
    /// Percent of streams at processed, confirmed, finalized
    #[clap(long, default_value = "90,7,3")]
    commitment_split: String,
    /// Percent of streams with no compression, zstd, gzip
    #[clap(long, default_value = "40,50,10")]
    compression_split: String,
    /// Full-block subscribers at confirmed
    #[clap(long, default_value_t = 6)]
    blocks: usize,
    /// Deshred subscribers
    #[clap(long, default_value_t = 4)]
    deshred: usize,
    /// Per-stream message rate targets (p50, p90, p99), from fra215 per-id rates over 3.7 streams per id
    #[clap(long, default_value = "7,120,4900")]
    rate_targets: String,
    /// Total message rate target per stream, from fra215: 94k msg/s over 427 streams
    #[clap(long, default_value_t = 220.0)]
    total_per_stream: f64,
    #[clap(long)]
    out: PathBuf,
}

/// Account and transaction-key activity in the sampled window, as rates per second.
struct Activity {
    pubkeys: Vec<(String, f64)>,
    owners: Vec<(String, f64)>,
    tx_keys: Vec<(String, f64)>,
    entry_rate: f64,
    slot_rate: f64,
}

pub fn run(args: GenArgs) -> anyhow::Result<()> {
    let activity = sample(&args)?;
    let targets = parse_split(&args.rate_targets, 3)?;
    let total_target = args.total_per_stream * args.streams as f64;
    let mut best: Option<(f64, f64, Vec<Subscriber>, Vec<f64>)> = None;
    for cold in [0.0, 0.5, 0.8, 0.9, 0.95, 0.98, 0.99, 0.995, 0.998, 0.999] {
        let (subscribers, rates) = generate(&args, &activity, cold)?;
        let quantiles = quantiles(&rates);
        let total: f64 = rates.iter().sum();
        let score: f64 = quantiles
            .iter()
            .zip(&targets)
            .map(|(got, want)| ((got.max(0.1)) / want.max(0.1)).ln().abs())
            .sum::<f64>()
            + (total.max(1.0) / total_target).ln().abs();
        info!(
            "cold {cold:.3}: predicted per-stream msg/s p50 {:.1} p90 {:.1} p99 {:.1}, total {total:.0} (target {total_target:.0}), score {score:.2}",
            quantiles[0],
            quantiles[1],
            quantiles[2],
        );
        let alpha = cold;
        if best.as_ref().is_none_or(|(best_score, ..)| score < *best_score) {
            best = Some((score, alpha, subscribers, rates));
        }
    }
    let (_, alpha, mut subscribers, rates) = best.expect("at least one alpha");
    subscribers.extend(probes());
    let profile = Profile {
        description: format!(
            "{} load streams from {} (seed {}, cold key share {alpha:.3}, predicted total {:.0} msg/s) plus probes",
            args.streams,
            args.fixture.display(),
            args.seed,
            rates.iter().sum::<f64>()
        ),
        subscribers,
    };
    fs::write(&args.out, serde_json::to_vec_pretty(&profile)?)?;
    info!("wrote {} with cold key share {alpha:.3}", args.out.display());
    Ok(())
}

fn sample(args: &GenArgs) -> anyhow::Result<Activity> {
    let manifest = Manifest::load(&args.fixture)?;
    let first_slot = manifest.first_slot.unwrap_or_default() + 20;
    let last_slot = first_slot + args.sample_slots;
    let mut pubkeys: HashMap<Vec<u8>, u64> = HashMap::new();
    let mut owners: HashMap<Vec<u8>, u64> = HashMap::new();
    let mut tx_keys: HashMap<Vec<u8>, u64> = HashMap::new();
    let (mut entries, mut metas) = (0u64, 0u64);
    let (mut first_ns, mut last_ns) = (u64::MAX, 0u64);
    let mut reader = FixtureReader::open(&args.fixture, &manifest.chunks);
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
                let Some(info) = msg.transaction else { continue };
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
        if rng.chance(self.cold) {
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
struct Rng(u64);

impl Rng {
    const fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }

    fn chance(&mut self, p: f64) -> bool {
        self.unit() < p
    }

    const fn range(&mut self, low: usize, high: usize) -> usize {
        low + (self.next() % (high - low + 1) as u64) as usize
    }

    fn split<'a, T>(&mut self, percents: &[f64], values: &'a [T]) -> &'a T {
        let mut target = self.unit() * percents.iter().sum::<f64>();
        for (percent, value) in percents.iter().zip(values) {
            if target < *percent {
                return value;
            }
            target -= percent;
        }
        values.last().expect("non-empty split")
    }
}

fn generate(args: &GenArgs, activity: &Activity, cold: f64) -> anyhow::Result<(Vec<Subscriber>, Vec<f64>)> {
    let commitment_split = parse_split(&args.commitment_split, 3)?;
    let compression_split = parse_split(&args.compression_split, 3)?;
    let mut rng = Rng(args.seed);
    let pubkeys = Sampler::new(&activity.pubkeys, cold);
    let owners = Sampler::new(&activity.owners, 0.0);
    let tx_keys = Sampler::new(&activity.tx_keys, cold);
    let mut subscribers = Vec::new();
    let mut rates = Vec::new();

    for index in 0..args.streams {
        let commitment = *rng.split(
            &commitment_split,
            &[Commitment::Processed, Commitment::Confirmed, Commitment::Finalized],
        );
        let compression = *rng.split(
            &compression_split,
            &[Compression::None, Compression::Zstd, Compression::Gzip],
        );
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
                request.accounts.insert(format!("a{filter}"), account_filter);
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
        rates.push(rate);
        subscribers.push(Subscriber {
            name: format!("load-{index}"),
            class: format!("load-{}", commitment_name(commitment)),
            role: Role::Load,
            compression,
            count: 1,
            request,
        });
    }

    for index in 0..args.blocks {
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
            compression: if index % 2 == 0 { Compression::Zstd } else { Compression::None },
            count: 1,
            request: Request {
                commitment: Commitment::Confirmed,
                blocks,
                ..Default::default()
            },
        });
    }
    for index in 0..args.deshred {
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
        });
    }
    Ok((subscribers, rates))
}

/// Measured subscribers: streamlab's exact subscription, compressed and not, plus one at confirmed.
fn probes() -> Vec<Subscriber> {
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
    vec![
        Subscriber {
            name: "streamlab-none".to_owned(),
            class: "streamlab".to_owned(),
            role: Role::Probe,
            compression: Compression::None,
            count: 4,
            request: streamlab(),
        },
        Subscriber {
            name: "streamlab-zstd".to_owned(),
            class: "streamlab".to_owned(),
            role: Role::Probe,
            compression: Compression::Zstd,
            count: 4,
            request: streamlab(),
        },
        Subscriber {
            name: "streamlab-confirmed".to_owned(),
            class: "streamlab-confirmed".to_owned(),
            role: Role::Probe,
            compression: Compression::None,
            count: 2,
            request: Request {
                commitment: Commitment::Confirmed,
                ..streamlab()
            },
        },
    ]
}

const fn commitment_name(commitment: Commitment) -> &'static str {
    match commitment {
        Commitment::Processed => "processed",
        Commitment::Confirmed => "confirmed",
        Commitment::Finalized => "finalized",
    }
}

fn parse_split(value: &str, len: usize) -> anyhow::Result<Vec<f64>> {
    let parts: Vec<f64> = value
        .split(',')
        .map(|part| part.trim().parse::<f64>())
        .collect::<Result<_, _>>()?;
    anyhow::ensure!(parts.len() == len, "expected {len} comma-separated numbers in {value}");
    Ok(parts)
}

fn quantiles(rates: &[f64]) -> Vec<f64> {
    let mut sorted = rates.to_vec();
    sorted.sort_by(f64::total_cmp);
    [0.5, 0.9, 0.99]
        .iter()
        .map(|q| sorted[((sorted.len() - 1) as f64 * q).round() as usize])
        .collect()
}
