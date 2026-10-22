//! `ygb trim`: copies the part of a fixture one scenario window needs, so it can move to another machine.
//! It keeps the heaviest window and the slots profiles sample, so runs on the copy replay the same slots
//! with the same subscriber profile as on the original.

use {
    crate::{bench, fixture::Manifest, scenario::Window},
    anyhow::Context,
    log::info,
    std::{fs, path::PathBuf},
};

/// Slots kept beyond what the window needs at each end; chunks advance about 13 slots each.
const MARGIN_SLOTS: u64 = 50;

#[derive(Debug, clap::Args)]
pub struct TrimArgs {
    /// Fixture to read
    #[clap(long)]
    pub fixture: PathBuf,
    /// Directory for the smaller fixture; must not exist yet
    #[clap(long)]
    pub out: PathBuf,
    /// Paced slots of the largest scenario window to keep (prod-like uses 1500)
    #[clap(long, default_value_t = 1500)]
    pub slots: u64,
    /// Prefill slots of that window (prod-like uses 300)
    #[clap(long, default_value_t = 300)]
    pub prefill_slots: u64,
    /// First paced slot of the window; the heaviest window when unset
    #[clap(long)]
    pub start_slot: Option<u64>,
    /// Slots that profiles sample after the fixture's first slot (mix.sample_slots, at most 300 in scenarios/)
    #[clap(long, default_value_t = 300)]
    pub sample_slots: u64,
}

pub fn run(args: TrimArgs) -> anyhow::Result<()> {
    anyhow::ensure!(
        !args.out.exists(),
        "{} exists; pick a new --out",
        args.out.display()
    );
    let manifest = Manifest::load(&args.fixture)?;
    let window = Window {
        start: "heaviest".to_owned(),
        slots: args.slots,
        prefill_slots: args.prefill_slots,
        warmup_secs: 0,
    };
    let paced = match args.start_slot {
        Some(slot) => slot,
        None => bench::window_start(&window, &manifest)?,
    };
    // `ygb bench` needs prefill + 64 slots before the window and 200 after it.
    let from = paced.saturating_sub(args.prefill_slots + 64 + MARGIN_SLOTS);
    let to = paced + args.slots + 200 + MARGIN_SLOTS;
    // Profiles sample key activity from first_slot + 64 on; keeping those slots keeps the profile.
    let first = manifest.first_slot.unwrap_or(from);
    let sample_to = first + 64 + args.sample_slots + MARGIN_SLOTS;
    let wanted =
        |min: u64, max: u64| (max >= from && min <= to) || (max >= first && min <= sample_to);
    let chunks: Vec<_> = manifest
        .chunks
        .iter()
        .filter(|chunk| match (chunk.min_slot, chunk.max_slot) {
            (Some(min), Some(max)) => wanted(min, max),
            _ => false,
        })
        .cloned()
        .collect();
    anyhow::ensure!(
        !chunks.is_empty(),
        "no chunk of {} covers slots {from}..={to}",
        args.fixture.display()
    );
    let bytes: u64 = chunks.iter().map(|chunk| chunk.bytes).sum();
    info!(
        "keeping {} of {} chunks: slots {first}..={sample_to} for profiles, {from}..={to} for the window from {paced}; {:.1} GB uncompressed",
        chunks.len(),
        manifest.chunks.len(),
        bytes as f64 / 1e9
    );
    fs::create_dir_all(&args.out)?;
    for chunk in &chunks {
        let target = args.out.join(&chunk.file);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(args.fixture.join(&chunk.file), &target)
            .with_context(|| format!("failed to copy {}", chunk.file))?;
    }
    let mut trimmed = manifest;
    trimmed.chunks = chunks;
    trimmed.last_slot = Some(to);
    trimmed.notes.insert(
        "trimmed_from".to_owned(),
        format!(
            "{}, slots {first}..={sample_to} for profiles and {from}..={to} for the window",
            args.fixture.display()
        ),
    );
    trimmed.save(&args.out)?;
    info!("wrote {}", args.out.display());
    Ok(())
}
