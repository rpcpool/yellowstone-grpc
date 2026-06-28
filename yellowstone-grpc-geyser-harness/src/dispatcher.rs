use crate::bridge;
use crate::generator::GeyserEvent;
use crate::metrics::{BenchReport, ThreadHistogram};
use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, ReplicaAccountInfoVersions,
};
use std::panic::AssertUnwindSafe;
use std::time::{Duration, Instant};

pub fn run(
    plugin: &dyn GeyserPlugin,
    events: &[GeyserEvent],
    thread_count: usize,
    warmup: Duration,
    duration: Duration,
) -> BenchReport {
    let start = Instant::now();
    let warmup_end = start + warmup;
    let deadline = warmup_end + duration;
    let chunk_size = (events.len() + thread_count - 1) / thread_count;

    let histograms: Vec<ThreadHistogram> = std::thread::scope(|s| {
        let handles: Vec<_> = events
            .chunks(chunk_size.max(1))
            .map(|chunk| {
                s.spawn(move || {
                    let mut hist = ThreadHistogram::new();
                    loop {
                        for event in chunk {
                            let now = Instant::now();
                            if now >= deadline {
                                return hist;
                            }
                            let recording = now >= warmup_end;

                            let result =
                                std::panic::catch_unwind(AssertUnwindSafe(|| {
                                    dispatch_one(plugin, event)
                                }));

                            let elapsed_nanos = now.elapsed().as_nanos() as u64;

                            match result {
                                Ok(Ok(())) if recording => {
                                    record(&mut hist, event, elapsed_nanos);
                                }
                                Ok(Err(_)) => hist.errors += 1,
                                Err(_) => hist.panics += 1,
                                _ => {}
                            }
                        }
                    }
                })
            })
            .collect();

        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let _actual = start.elapsed().saturating_sub(warmup);
    BenchReport::from_thread_histograms(histograms, duration)
}

fn dispatch_one(
    plugin: &dyn GeyserPlugin,
    event: &GeyserEvent,
) -> Result<(), agave_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError> {
    match event {
        GeyserEvent::Account {
            slot, is_startup, ..
        } => {
            let info = bridge::account_view(event);
            plugin.update_account(
                ReplicaAccountInfoVersions::V0_0_3(&info),
                *slot,
                *is_startup,
            )
        }
        GeyserEvent::Slot {
            slot,
            parent,
            status,
        } => plugin.update_slot_status(*slot, *parent, status),
    }
}

fn record(hist: &mut ThreadHistogram, event: &GeyserEvent, nanos: u64) {
    match event {
        GeyserEvent::Account { .. } => hist.record_account(nanos),
        GeyserEvent::Slot { .. } => hist.record_slot(nanos),
    }
}