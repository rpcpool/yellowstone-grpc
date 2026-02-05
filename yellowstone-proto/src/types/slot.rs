use std::time::SystemTime;
use solana_clock::Slot;

use crate::types::common::SlotStatus;

#[derive(Debug, Clone, PartialEq)]
pub struct SlotInfo {
    pub slot: Slot,
    pub parent: Option<Slot>,
    pub status: SlotStatus,
    pub dead_error: Option<String>,
    pub created_at: SystemTime,
}