//! A plugin unload ends every thread the plugin started, as agave needs before it closes the
//! library. One test per binary, so `/proc/self/task` lists only this test's threads.

#![cfg(target_os = "linux")]

use {
    std::{
        collections::{BTreeMap, HashMap},
        fs,
        sync::mpsc as std_mpsc,
        time::{Duration, Instant},
    },
    yellowstone_grpc_geyser::{
        plugin::entry::Plugin,
        plugin_interface::{
            geyser_plugin_interface::{
                GeyserPlugin, ReplicaAccountInfoV3, ReplicaAccountInfoVersions, ReplicaBlockInfoV5,
                ReplicaBlockInfoVersions, SlotStatus,
            },
            transaction_status_meta::RewardsAndNumPartitions,
        },
    },
    yellowstone_grpc_proto::prelude::{
        geyser_client::GeyserClient, CommitmentLevel, SubscribeRequest,
        SubscribeRequestFilterAccounts, SubscribeRequestFilterSlots,
    },
};

/// The longest an unload may take here: far below the plugin's 30 s runtime timeout.
const UNLOAD_BOUND: Duration = Duration::from_secs(10);

/// The longest a detached plugin thread may run on after the unload returns.
const DETACHED_THREAD_BOUND: Duration = Duration::from_secs(1);

/// The threads of this process, by thread id.
///
/// # Returns
///
/// Each thread's id and name, as `/proc/self/task/*/comm` gives them.
fn thread_names() -> BTreeMap<u32, String> {
    let mut names_by_thread_id = BTreeMap::new();
    for task_entry in fs::read_dir("/proc/self/task").unwrap() {
        let task_entry = task_entry.unwrap();
        let thread_id = task_entry.file_name().to_string_lossy().parse().unwrap();
        if let Ok(thread_comm) = fs::read_to_string(task_entry.path().join("comm")) {
            names_by_thread_id.insert(thread_id, thread_comm.trim().to_owned());
        }
    }
    names_by_thread_id
}

/// The threads started since `earlier_threads` was read, other than the clients' thread.
///
/// # Arguments
///
/// * `earlier_threads` - The threads before the plugin loaded.
///
/// # Returns
///
/// The names of the threads that are new.
fn plugin_threads(earlier_threads: &BTreeMap<u32, String>) -> Vec<String> {
    thread_names()
        .into_iter()
        .filter(|(thread_id, thread_name)| {
            !earlier_threads.contains_key(thread_id) && thread_name != "testClients"
        })
        .map(|(_, thread_name)| thread_name)
        .collect()
}

/// Feeds one sealed bank: CreatedBank, 64 account writes, the block meta and its statuses.
///
/// # Arguments
///
/// * `geyser_plugin` - The loaded plugin.
/// * `bank_slot` - The bank's slot, which is also its bank id.
/// * `watched_pubkey` - The account the clients route on, written first.
fn feed_bank(geyser_plugin: &Plugin, bank_slot: u64, watched_pubkey: &[u8; 32]) {
    let parent_slot = bank_slot - 1;
    geyser_plugin
        .update_bank_status(
            bank_slot,
            Some(parent_slot),
            &SlotStatus::CreatedBank,
            bank_slot,
        )
        .unwrap();
    for write_version in 0..64_u64 {
        let mut account_pubkey = *watched_pubkey;
        if write_version > 0 {
            account_pubkey[..8].copy_from_slice(&(bank_slot * 1000 + write_version).to_le_bytes());
        }
        let account_info = ReplicaAccountInfoV3 {
            pubkey: &account_pubkey,
            lamports: write_version,
            owner: &[7; 32],
            executable: false,
            rent_epoch: 0,
            data: &[0; 64],
            write_version,
            txn: None,
        };
        geyser_plugin
            .update_account_for_bank(
                ReplicaAccountInfoVersions::V0_0_3(&account_info),
                bank_slot,
                bank_slot,
            )
            .unwrap();
    }
    let block_rewards = RewardsAndNumPartitions {
        rewards: &[],
        num_partitions: None,
    };
    let block_info = ReplicaBlockInfoV5 {
        parent_slot,
        parent_blockhash: &format!("blockhash{parent_slot}"),
        slot: bank_slot,
        blockhash: &format!("blockhash{bank_slot}"),
        rewards: &block_rewards,
        block_time: None,
        block_height: Some(bank_slot),
        executed_transaction_count: 0,
        entry_count: 0,
    };
    geyser_plugin
        .notify_block_metadata_for_bank(ReplicaBlockInfoVersions::V0_0_5(&block_info), bank_slot)
        .unwrap();
    for bank_status in [
        SlotStatus::Processed,
        SlotStatus::Confirmed,
        SlotStatus::Rooted,
    ] {
        geyser_plugin
            .update_bank_status(bank_slot, Some(parent_slot), &bank_status, bank_slot)
            .unwrap();
    }
}

/// Six clients, at each level and with or without a replay, subscribe and read until their
/// stream ends.
///
/// # Arguments
///
/// * `grpc_port` - The plugin's gRPC port.
/// * `watched_pubkey` - The account the clients route on.
/// * `ready_tx` - Told once every client has subscribed.
/// * `stream_ends_tx` - Gets when each client's stream ends.
async fn run_clients(
    grpc_port: u16,
    watched_pubkey: [u8; 32],
    ready_tx: std_mpsc::Sender<()>,
    stream_ends_tx: std_mpsc::Sender<Instant>,
) {
    let grpc_channel =
        tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{grpc_port}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
    let mut client_tasks = Vec::new();
    for client_number in 0..6_usize {
        let client_commitment = [
            CommitmentLevel::Processed,
            CommitmentLevel::Confirmed,
            CommitmentLevel::Finalized,
        ][client_number % 3];
        let subscribe_request = SubscribeRequest {
            slots: HashMap::from([("s".to_owned(), SubscribeRequestFilterSlots::default())]),
            accounts: HashMap::from([(
                "a".to_owned(),
                SubscribeRequestFilterAccounts {
                    account: vec![bs58::encode(watched_pubkey).into_string()],
                    ..Default::default()
                },
            )]),
            commitment: Some(client_commitment as i32),
            from_slot: (client_number >= 3).then_some(1),
            ..Default::default()
        };
        let (request_tx, request_rx) = tokio::sync::mpsc::unbounded_channel();
        request_tx.send(subscribe_request).unwrap();
        let mut geyser_client = GeyserClient::new(grpc_channel.clone());
        let stream_ends_tx = stream_ends_tx.clone();
        let subscribe_result = geyser_client
            .subscribe(tokio_stream::wrappers::UnboundedReceiverStream::new(
                request_rx,
            ))
            .await;
        client_tasks.push(tokio::spawn(async move {
            let _open_requests = request_tx;
            if let Ok(subscribe_response) = subscribe_result {
                let mut update_stream = subscribe_response.into_inner();
                while let Ok(Some(_)) = update_stream.message().await {}
            }
            let _ = stream_ends_tx.send(Instant::now());
        }));
    }
    ready_tx.send(()).unwrap();
    for client_task in client_tasks {
        client_task.await.unwrap();
    }
}

/// An unload with live clients returns within [`UNLOAD_BOUND`], ends every client's stream,
/// and leaves no plugin thread: the bank thread and the runtime's workers are gone when it
/// returns, and every other plugin thread within [`DETACHED_THREAD_BOUND`].
#[test]
fn an_unload_ends_every_plugin_thread_and_client_stream() {
    let earlier_threads = thread_names();
    let grpc_port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let config_dir = std::env::temp_dir().join(format!("plugin-unload-{}", std::process::id()));
    fs::create_dir_all(&config_dir).unwrap();
    let config_path = config_dir.join("config.json");
    let plugin_config = serde_json::json!({
        "libpath": "unused",
        "log": { "level": "error" },
        "tokio": { "worker_threads": 4 },
        "grpc": {
            "listen": [{ "address": format!("127.0.0.1:{grpc_port}") }],
            "replay_stored_slots": 8,
        },
    });
    fs::write(&config_path, plugin_config.to_string()).unwrap();
    let mut geyser_plugin = Plugin::default();
    geyser_plugin
        .on_load(config_path.to_str().unwrap(), false)
        .unwrap();
    // A new thread sets its own name, so it can show up unnamed for a moment.
    let named_deadline = Instant::now() + DETACHED_THREAD_BOUND;
    while !plugin_threads(&earlier_threads).contains(&"solGrpcBankDrop".to_owned()) {
        assert!(Instant::now() < named_deadline, "no bank thread");
        std::thread::sleep(Duration::from_millis(1));
    }

    let watched_pubkey = [42; 32];
    let (ready_tx, ready_rx) = std_mpsc::channel();
    let (stream_ends_tx, stream_ends_rx) = std_mpsc::channel();
    let clients_thread = std::thread::Builder::new()
        .name("testClients".to_owned())
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(run_clients(
                    grpc_port,
                    watched_pubkey,
                    ready_tx,
                    stream_ends_tx,
                ))
        })
        .unwrap();
    ready_rx.recv().unwrap();
    for bank_slot in 1..=40 {
        feed_bank(&geyser_plugin, bank_slot, &watched_pubkey);
    }

    let unload_started_at = Instant::now();
    geyser_plugin.on_unload();
    let unload_took = unload_started_at.elapsed();
    let threads_at_return = plugin_threads(&earlier_threads);
    assert!(
        unload_took < UNLOAD_BOUND,
        "the unload took {unload_took:?}"
    );
    assert!(
        threads_at_return.iter().all(|thread_name| {
            thread_name != "solGrpcBankDrop" && !thread_name.starts_with("solGeyserGrpc")
        }),
        "left when the unload returned: {threads_at_return:?}"
    );
    let detached_deadline = Instant::now() + DETACHED_THREAD_BOUND;
    while !plugin_threads(&earlier_threads).is_empty() && Instant::now() < detached_deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    let threads_left = plugin_threads(&earlier_threads);
    assert!(
        threads_left.is_empty(),
        "left after the unload: {threads_left:?}"
    );

    clients_thread.join().unwrap();
    let stream_ends: Vec<Instant> = stream_ends_rx.try_iter().collect();
    assert_eq!(stream_ends.len(), 6);
    assert!(stream_ends
        .iter()
        .all(|stream_end| stream_end.duration_since(unload_started_at) < UNLOAD_BOUND));
    let _ = fs::remove_dir_all(&config_dir);
}
