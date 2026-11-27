//! The safety summary line (arch 12.3): at each interval, one warning line names every safety
//! count that rose since the last line, with its rise and total. Nothing rose: no line.

use {
    crate::telemetry::data_plane_counts::{
        DataPlaneCount, DataPlaneCounts, PluginCallback, PoolSafetyEvent, WaitedClaim,
    },
    std::fmt::Write as _,
    tokio::time::Instant,
};

/// The safety counts and the names their line shows: the failures first, then the pressure.
fn summary_counts() -> Vec<(String, DataPlaneCount)> {
    let mut summary_counts: Vec<(String, DataPlaneCount)> = PluginCallback::ALL
        .into_iter()
        .map(|plugin_callback| {
            (
                format!(
                    "callback_panics{{callback=\"{}\"}}",
                    plugin_callback.label_value()
                ),
                DataPlaneCount::CallbackPanic(plugin_callback),
            )
        })
        .collect();
    summary_counts.push(("owner_early_ends".to_owned(), DataPlaneCount::OwnerEarlyEnd));
    summary_counts.extend(PoolSafetyEvent::ALL.into_iter().map(|pool_safety_event| {
        (
            format!(
                "compressor_pool_events{{event=\"{}\"}}",
                pool_safety_event.label_value()
            ),
            DataPlaneCount::PoolSafety(pool_safety_event),
        )
    }));
    summary_counts.extend(WaitedClaim::ALL.into_iter().map(|waited_claim| {
        (
            format!(
                "claim_backstop_waits{{claim=\"{}\"}}",
                waited_claim.label_value()
            ),
            DataPlaneCount::ClaimBackstopWait(waited_claim),
        )
    }));
    summary_counts
}

/// The summary line's state: each safety count's sum at the last tick, and when the last line
/// was written.
pub struct SafetySummary {
    data_plane_counts: &'static DataPlaneCounts,
    summary_counts: Vec<(String, DataPlaneCount)>,
    baseline_sums: Vec<u64>,
    baseline_generation: u64,
    last_line_at: Instant,
}

impl SafetySummary {
    /// Takes the baseline at `now`, so a line names only the rises after it.
    pub fn new(data_plane_counts: &'static DataPlaneCounts, now: Instant) -> Self {
        let summary_counts = summary_counts();
        let baseline_generation = data_plane_counts.reset_generation();
        let baseline_sums = summary_counts
            .iter()
            .map(|(_, data_plane_count)| data_plane_counts.sum(*data_plane_count))
            .collect();
        Self {
            data_plane_counts,
            summary_counts,
            baseline_sums,
            baseline_generation,
            last_line_at: now,
        }
    }

    /// The line for every count that rose since the last tick, `None` when none rose. After a
    /// reset, each rise counts from 0.
    pub fn next_line(&mut self, now: Instant) -> Option<String> {
        let reset_generation = self.data_plane_counts.reset_generation();
        if reset_generation != self.baseline_generation {
            self.baseline_sums.fill(0);
            self.baseline_generation = reset_generation;
        }
        let mut moved_counts = String::new();
        for ((count_name, data_plane_count), baseline_sum) in
            self.summary_counts.iter().zip(&mut self.baseline_sums)
        {
            let current_sum = self.data_plane_counts.sum(*data_plane_count);
            let count_rise = current_sum.saturating_sub(*baseline_sum);
            *baseline_sum = current_sum;
            if count_rise > 0 {
                if !moved_counts.is_empty() {
                    moved_counts.push_str(", ");
                }
                write!(
                    moved_counts,
                    "{count_name} +{count_rise} (total {current_sum})"
                )
                .expect("a String takes every write");
            }
        }
        // A reset during the reads mixed both sides; the next tick counts from 0.
        if self.data_plane_counts.reset_generation() != reset_generation {
            return None;
        }
        if moved_counts.is_empty() {
            return None;
        }
        let since_last_line = now.saturating_duration_since(self.last_line_at);
        self.last_line_at = now;
        Some(format!(
            "safety counts rose since the last summary, {since_last_line:.1?} ago: {moved_counts}"
        ))
    }
}
