use agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus;
use rand::Rng;
use rand_chacha::ChaCha8Rng;
use rand::SeedableRng;

pub struct EventGeneratorConfig {
    pub account_count: u64,
    pub account_data_size: usize,
    pub slot_interval: u64,
    pub seed: u64,
}

pub enum GeyserEvent {
    Account {
        pubkey: [u8; 32],
        lamports: u64,
        owner: [u8; 32],
        executable: bool,
        rent_epoch: u64,
        data: Vec<u8>,
        slot: u64,
        is_startup: bool,
        write_version: u64,
    },
    Slot {
        slot: u64,
        parent: Option<u64>,
        status: SlotStatus,
    },
}

/// Pre-generates all events. One allocation per account (the data Vec).
/// Called once before the benchmark loop.
pub fn generate(config: &EventGeneratorConfig) -> Vec<GeyserEvent> {
    let mut rng = ChaCha8Rng::seed_from_u64(config.seed);
    let slot_count = if config.slot_interval > 0 {
        config.account_count / config.slot_interval
    } else {
        0
    };
    let capacity = config.account_count as usize + slot_count as usize;
    let mut events = Vec::with_capacity(capacity);
    let mut slot = 1u64;
    let mut write_version = 1u64;

    for i in 0..config.account_count {
        if config.slot_interval > 0 && i > 0 && i % config.slot_interval == 0 {
            slot += 1;
            events.push(GeyserEvent::Slot {
                slot,
                parent: Some(slot - 1),
                status: SlotStatus::Processed,
            });
        }

        let mut pubkey = [0u8; 32];
        rng.fill(&mut pubkey);
        let mut owner = [0u8; 32];
        rng.fill(&mut owner);
        let mut data = vec![0u8; config.account_data_size];
        rng.fill(data.as_mut_slice());

        events.push(GeyserEvent::Account {
            pubkey,
            lamports: rng.gen(),
            owner,
            executable: false,
            rent_epoch: 0,
            data,
            slot,
            is_startup: false,
            write_version,
        });
        write_version += 1;
    }

    events
}