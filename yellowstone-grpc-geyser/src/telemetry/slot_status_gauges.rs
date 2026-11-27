//! The last slot of each status, for `slot_status{status}` from agave's slot status callback and
//! `slot_status_plugin{status}` from block reconstruction, without a label lookup.

use {
    crate::plugin::message::SlotStatus as PluginSlotStatus,
    agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus as AgaveSlotStatus,
    std::sync::atomic::{AtomicU64, Ordering},
};

/// How many statuses each family has.
pub const SLOT_STATUS_KINDS: usize = 7;

/// The last slot of each status, stored as `slot + 1`: 0 means unset since the last reset, and
/// renders no series. A family's words share one line, written about 15 times a second.
#[derive(Debug)]
pub struct SlotStatusGauges {
    last_slots: [AtomicU64; SLOT_STATUS_KINDS],
}

/// `slot_status`: agave's statuses, as its slot status callbacks report them.
pub static AGAVE_SLOT_STATUS_GAUGES: SlotStatusGauges = SlotStatusGauges::new();

/// `slot_status_plugin`: the plugin's statuses, as block reconstruction takes them in.
pub static PLUGIN_SLOT_STATUS_GAUGES: SlotStatusGauges = SlotStatusGauges::new();

impl SlotStatusGauges {
    pub const fn new() -> Self {
        Self {
            last_slots: [const { AtomicU64::new(0) }; SLOT_STATUS_KINDS],
        }
    }

    /// Sets the status at `status_index` to `slot`; a later set wins, as `IntGauge::set` does.
    pub fn set(&self, status_index: usize, slot: u64) {
        self.last_slots[status_index].store(slot.saturating_add(1), Ordering::Relaxed);
    }

    /// The last slot of the status at `status_index`, or `None` while it is not set.
    pub fn last_slot(&self, status_index: usize) -> Option<u64> {
        let stored_slot = self.last_slots[status_index].load(Ordering::Relaxed);
        stored_slot.checked_sub(1)
    }

    /// Forgets every status, as `reset_metrics` does at each load.
    pub fn reset(&self) {
        for last_slot in &self.last_slots {
            last_slot.store(0, Ordering::Relaxed);
        }
    }
}

impl Default for SlotStatusGauges {
    fn default() -> Self {
        Self::new()
    }
}

/// The index of agave's `slot_status` in [`AGAVE_SLOT_STATUS_GAUGES`].
pub const fn agave_slot_status_index(slot_status: &AgaveSlotStatus) -> usize {
    match slot_status {
        AgaveSlotStatus::Processed => 0,
        AgaveSlotStatus::Rooted => 1,
        AgaveSlotStatus::Confirmed => 2,
        AgaveSlotStatus::FirstShredReceived => 3,
        AgaveSlotStatus::Completed => 4,
        AgaveSlotStatus::CreatedBank => 5,
        AgaveSlotStatus::Dead(_) => 6,
    }
}

/// Each agave status's label, from agave's own `as_str`, in index order.
pub fn agave_slot_status_labels() -> [&'static str; SLOT_STATUS_KINDS] {
    [
        AgaveSlotStatus::Processed,
        AgaveSlotStatus::Rooted,
        AgaveSlotStatus::Confirmed,
        AgaveSlotStatus::FirstShredReceived,
        AgaveSlotStatus::Completed,
        AgaveSlotStatus::CreatedBank,
        AgaveSlotStatus::Dead(String::new()),
    ]
    .map(|slot_status| slot_status.as_str())
}

/// The index of the plugin's `slot_status` in [`PLUGIN_SLOT_STATUS_GAUGES`].
pub const fn plugin_slot_status_index(slot_status: PluginSlotStatus) -> usize {
    slot_status as usize
}

/// Each plugin status's label, from the plugin's `as_str`, in index order.
pub fn plugin_slot_status_labels() -> [&'static str; SLOT_STATUS_KINDS] {
    [
        PluginSlotStatus::Processed,
        PluginSlotStatus::Confirmed,
        PluginSlotStatus::Finalized,
        PluginSlotStatus::FirstShredReceived,
        PluginSlotStatus::Completed,
        PluginSlotStatus::CreatedBank,
        PluginSlotStatus::Dead,
    ]
    .map(|slot_status| slot_status.as_str())
}
