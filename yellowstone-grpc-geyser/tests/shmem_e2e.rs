use std::path::Path;
use std::time::Duration;

use tokio::sync::mpsc;
use yellowstone_conduit::{EventWriter, ShmemRegion};
use yellowstone_grpc_geyser::plugin::message::Message;
use yellowstone_grpc_geyser::plugin::shmem::codec::ProstGeyserCodec;
use yellowstone_grpc_geyser::plugin::shmem::run_shmem_reader;
use yellowstone_shmem_common::{
    GeyserMessage, MessageAccount, MessageAccountInfo, MessageSlot, SlotStatus,
};
use yellowstone_shmem_plugin::codec::GeyserCodec;

const DCACHE_CAPACITY: u64 = 1 << 14;
const MCACHE_CAPACITY: u64 = 64 * 1024 * 1024;

fn make_ring(path: &str) -> EventWriter {
    let _ = std::fs::remove_file(path);
    let region =
        ShmemRegion::create(Path::new(path), DCACHE_CAPACITY, MCACHE_CAPACITY).unwrap();
    EventWriter::new(region)
}

fn write_message(writer: &EventWriter, msg: &GeyserMessage) {
    let codec = ProstGeyserCodec;
    let size = codec.encoded_size(msg);
    let (offset, len) = writer.claim_mcache(size);
    unsafe {
        let buf =
            std::slice::from_raw_parts_mut(writer.region_mcache_ptr(offset as usize), size);
        codec.encode_into(msg, buf);
    }
    writer.write_event(msg.slot(), msg.event_type() as u8, offset, len);
}

fn make_account(data_len: usize, slot: u64) -> GeyserMessage {
    GeyserMessage::Account(MessageAccount {
        account: MessageAccountInfo {
            pubkey: vec![1u8; 32],
            lamports: 1_000_000,
            owner: vec![2u8; 32],
            executable: false,
            rent_epoch: u64::MAX,
            data: vec![0xABu8; data_len],
            write_version: 42,
            txn_signature: Some(vec![3u8; 64]),
        },
        slot,
        is_startup: false,
        plugin_ts_ns: 0,
    })
}

fn make_slot(slot: u64) -> GeyserMessage {
    GeyserMessage::Slot(MessageSlot {
        slot,
        parent: Some(slot - 1),
        status: SlotStatus::Processed,
    })
}

async fn collect_messages(
    rx: &mut mpsc::UnboundedReceiver<Message>,
    count: usize,
    timeout: Duration,
) -> Vec<Message> {
    let mut messages = Vec::new();
    let deadline = tokio::time::Instant::now() + timeout;
    while messages.len() < count && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Some(msg)) => messages.push(msg),
            _ => {}
        }
    }
    messages
}

/// Basic account round trip — write one account, read it back, verify fields.
#[tokio::test]
async fn test_account_roundtrip() {
    const PATH: &str = "/tmp/shmem-e2e-account";
    let writer = make_ring(PATH);
    let (tx, mut rx) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        let _ = run_shmem_reader(Path::new(PATH), tx).await;
    });

    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let msg = make_account(256, 300_000_000);
    write_message(&writer, &msg);

    let messages = collect_messages(&mut rx, 1, Duration::from_secs(2)).await;
    assert_eq!(messages.len(), 1, "expected 1 message, got {}", messages.len());

    match &messages[0] {
        Message::Account(a) => {
            assert_eq!(a.account.data.len(), 256);
            assert!(a.account.data.iter().all(|&b| b == 0xAB));
            assert_eq!(a.slot, 300_000_000);
        }
        other => panic!("expected Account, got {:?}", other),
    }
}

/// Slot round trip.
#[tokio::test]
async fn test_slot_roundtrip() {
    const PATH: &str = "/tmp/shmem-e2e-slot";
    let writer = make_ring(PATH);
    let (tx, mut rx) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        let _ = run_shmem_reader(Path::new(PATH), tx).await;
    });

    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let msg = make_slot(300_000_001);
    write_message(&writer, &msg);

    let messages = collect_messages(&mut rx, 1, Duration::from_secs(2)).await;
    assert_eq!(messages.len(), 1);

    match &messages[0] {
        Message::Slot(s) => {
            assert_eq!(s.slot, 300_000_001);
        }
        other => panic!("expected Slot, got {:?}", other),
    }
}

/// Large account (10KB data) — exercises bip-buffer wrap paths.
#[tokio::test]
async fn test_large_account_roundtrip() {
    const PATH: &str = "/tmp/shmem-e2e-large";
    let writer = make_ring(PATH);
    let (tx, mut rx) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        let _ = run_shmem_reader(Path::new(PATH), tx).await;
    });

    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let msg = make_account(10 * 1024, 300_000_002);
    write_message(&writer, &msg);

    let messages = collect_messages(&mut rx, 1, Duration::from_secs(2)).await;
    assert_eq!(messages.len(), 1);

    match &messages[0] {
        Message::Account(a) => {
            assert_eq!(a.account.data.len(), 10 * 1024);
            assert!(a.account.data.iter().all(|&b| b == 0xAB));
        }
        other => panic!("expected Account, got {:?}", other),
    }
}

/// Mixed message types — account, slot, account — verify ordering preserved.
#[tokio::test]
async fn test_mixed_message_ordering() {
    const PATH: &str = "/tmp/shmem-e2e-mixed";
    let writer = make_ring(PATH);
    let (tx, mut rx) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        let _ = run_shmem_reader(Path::new(PATH), tx).await;
    });

    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    write_message(&writer, &make_account(256, 1));
    write_message(&writer, &make_slot(1));
    write_message(&writer, &make_account(512, 2));

    let messages = collect_messages(&mut rx, 3, Duration::from_secs(2)).await;
    assert_eq!(messages.len(), 3);

    assert!(matches!(messages[0], Message::Account(_)));
    assert!(matches!(messages[1], Message::Slot(_)));
    assert!(matches!(messages[2], Message::Account(_)));
}

/// Mcache wrap — write enough messages to wrap the mcache multiple times.
/// Verify no corruption on messages that arrive after the wrap.
#[tokio::test]
async fn test_mcache_wrap_no_corruption() {
    const PATH: &str = "/tmp/shmem-e2e-wrap";
    // Small mcache to force wrap quickly.
    let _ = std::fs::remove_file(PATH);
    let region = ShmemRegion::create(Path::new(PATH), DCACHE_CAPACITY, 1024 * 1024).unwrap();
    let writer = EventWriter::new(region);

    let (tx, mut rx) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        let _ = run_shmem_reader(Path::new(PATH), tx).await;
    });

    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Write enough to wrap 1MB mcache several times.
    let count = 5000usize;
    for i in 0..count as u64 {
        write_message(&writer, &make_account(256, i));
    }

    let messages = collect_messages(&mut rx, 100, Duration::from_secs(3)).await;
    assert!(messages.len() > 0, "expected some messages after mcache wrap");

    // Every account that arrives must have intact data.
    for msg in &messages {
        if let Message::Account(a) = msg {
            assert!(
                a.account.data.iter().all(|&b| b == 0xAB),
                "corrupted account data after mcache wrap"
            );
        }
    }
}

/// Data integrity — write 100 accounts with different data patterns,
/// verify each decoded account has the correct pattern.
#[tokio::test]
async fn test_data_integrity_across_messages() {
    const PATH: &str = "/tmp/shmem-e2e-integrity";
    let writer = make_ring(PATH);
    let (tx, mut rx) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        let _ = run_shmem_reader(Path::new(PATH), tx).await;
    });

    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let count = 100usize;
    for i in 0..count as u64 {
        let pattern = (i % 256) as u8;
        let msg = GeyserMessage::Account(MessageAccount {
            account: MessageAccountInfo {
                pubkey: vec![pattern; 32],
                lamports: i * 1000,
                owner: vec![2u8; 32],
                executable: false,
                rent_epoch: u64::MAX,
                data: vec![pattern; 64],
                write_version: i,
                txn_signature: None,
            },
            slot: i,
            is_startup: false,
            plugin_ts_ns: 0,
        });
        write_message(&writer, &msg);
    }

    let messages = collect_messages(&mut rx, count, Duration::from_secs(3)).await;
    assert!(messages.len() > 0);

    for msg in &messages {
        if let Message::Account(a) = msg {
            let pattern = (a.slot % 256) as u8;
            assert_eq!(a.account.pubkey.as_ref()[0], pattern, "pubkey pattern mismatch at slot {}", a.slot);
            assert!(
                a.account.data.iter().all(|&b| b == pattern),
                "data pattern mismatch at slot {}",
                a.slot
            );
        }
    }
}