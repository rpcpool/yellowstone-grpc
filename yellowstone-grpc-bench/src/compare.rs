//! `ygb compare`: paired A/B of two runs over the same fixture window.
//! The unit is the slot: per-slot p99 of each probe group, paired by slot, with a block-bootstrap CI.

use {
    crate::report,
    serde::Serialize,
    std::{fs, path::PathBuf},
};

const BOOTSTRAP_ROUNDS: usize = 2000;
const BLOCK_SLOTS: usize = 10;

#[derive(Debug, Clone, clap::Args)]
pub struct CompareArgs {
    /// Baseline run directory
    #[clap(long)]
    pub a: PathBuf,
    /// Candidate run directory
    #[clap(long)]
    pub b: PathBuf,
    #[clap(long, default_value_t = 60)]
    pub warmup_secs: u64,
    /// Minimum samples in a slot for its p99 to count
    #[clap(long, default_value_t = 5)]
    pub min_samples: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct CompareRow {
    pub group: String,
    pub slots: usize,
    pub a_median_slot_p99_us: f64,
    pub b_median_slot_p99_us: f64,
    pub median_diff_us: f64,
    pub ci_low_us: f64,
    pub ci_high_us: f64,
    pub change_pct: f64,
}

pub fn run(args: CompareArgs) -> anyhow::Result<()> {
    let rows = compare_dirs(&args.a, &args.b, args.warmup_secs, args.min_samples)?;
    print_rows(&args.a, &args.b, &rows);
    let out = args.b.join(format!(
        "compare-vs-{}.json",
        args.a
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default()
    ));
    fs::write(out, serde_json::to_vec_pretty(&rows)?)?;
    Ok(())
}

/// Per probe group: median of paired per-slot p99 differences (b - a) with a block-bootstrap CI.
pub fn compare_dirs(
    a: &std::path::Path,
    b: &std::path::Path,
    warmup_secs: u64,
    min_samples: usize,
) -> anyhow::Result<Vec<CompareRow>> {
    let a = report::slot_p99s(a, warmup_secs, min_samples)?;
    let b = report::slot_p99s(b, warmup_secs, min_samples)?;
    let mut rows = Vec::new();
    for (group, a_slots) in &a {
        let Some(b_slots) = b.get(group) else {
            continue;
        };
        let pairs: Vec<(f64, f64)> = a_slots
            .iter()
            .filter_map(|(slot, a_p99)| {
                b_slots
                    .get(slot)
                    .map(|b_p99| (*a_p99 as f64, *b_p99 as f64))
            })
            .collect();
        if pairs.len() < BLOCK_SLOTS * 3 {
            continue;
        }
        let diffs: Vec<f64> = pairs.iter().map(|(a, b)| b - a).collect();
        let (low, high) = bootstrap_ci(&diffs);
        let a_median = median(pairs.iter().map(|(a, _)| *a).collect());
        rows.push(CompareRow {
            group: group.clone(),
            slots: pairs.len(),
            a_median_slot_p99_us: a_median / 1e3,
            b_median_slot_p99_us: median(pairs.iter().map(|(_, b)| *b).collect()) / 1e3,
            median_diff_us: median(diffs.clone()) / 1e3,
            ci_low_us: low / 1e3,
            ci_high_us: high / 1e3,
            change_pct: median(diffs) / a_median.max(1.0) * 100.0,
        });
    }
    Ok(rows)
}

fn print_rows(a: &std::path::Path, b: &std::path::Path, rows: &[CompareRow]) {
    println!("A = {}\nB = {}", a.display(), b.display());
    println!(
        "{:<44} {:>6} {:>12} {:>12} {:>12} {:>22} {:>8}",
        "group", "slots", "A slot p99", "B slot p99", "B-A median", "95% CI", "change"
    );
    for row in rows {
        println!(
            "{:<44} {:>6} {:>10.0}us {:>10.0}us {:>10.0}us {:>9.0}..{:<9.0}us {:>7.1}%",
            row.group,
            row.slots,
            row.a_median_slot_p99_us,
            row.b_median_slot_p99_us,
            row.median_diff_us,
            row.ci_low_us,
            row.ci_high_us,
            row.change_pct
        );
    }
}

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

/// 95% CI of the median paired difference, resampling blocks of consecutive slots.
fn bootstrap_ci(diffs: &[f64]) -> (f64, f64) {
    let blocks: Vec<&[f64]> = diffs.chunks(BLOCK_SLOTS).collect();
    let mut state = 0x5EED_u64;
    let mut medians = Vec::with_capacity(BOOTSTRAP_ROUNDS);
    for _ in 0..BOOTSTRAP_ROUNDS {
        let mut sample = Vec::with_capacity(diffs.len());
        for _ in 0..blocks.len() {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            sample.extend_from_slice(blocks[(state >> 33) as usize % blocks.len()]);
        }
        medians.push(median(sample));
    }
    medians.sort_by(f64::total_cmp);
    let at = |q: f64| medians[((medians.len() - 1) as f64 * q).round() as usize];
    (at(0.025), at(0.975))
}

#[cfg(test)]
mod tests {
    use super::bootstrap_ci;

    #[test]
    fn ci_contains_constant_shift() {
        let diffs: Vec<f64> = (0..200).map(|i| 100.0 + f64::from(i % 7)).collect();
        let (low, high) = bootstrap_ci(&diffs);
        assert!(low >= 100.0 && high <= 106.0, "{low}..{high}");
    }
}
