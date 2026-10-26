use {
    criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput},
    futures::{FutureExt, Stream, StreamExt},
    std::{hint::black_box, pin::Pin, task::Poll},
    tonic::Status,
    yellowstone_grpc_client::{DedupState, DedupStream},
    yellowstone_grpc_proto::{
        geyser::{SubscribeUpdateAccount, SubscribeUpdateAccountInfo},
        prelude::{
            subscribe_update::UpdateOneof, SlotStatus, SubscribeUpdate, SubscribeUpdateBlockMeta,
            SubscribeUpdateEntry, SubscribeUpdateSlot, SubscribeUpdateTransaction,
            SubscribeUpdateTransactionInfo,
        },
    },
};

const SLOTS: u64 = 1_000;
const TXS_PER_SLOT: u64 = 50;
const ENTRIES_PER_SLOT: u64 = 20;
const ACCOUNTS_PER_SLOT: u64 = 50;
const REPLAY_SLOTS: u64 = 100;

/// Replays a fixed message list; `reconnect_count` changes when the list says so.
struct VecStream {
    messages: std::vec::IntoIter<(u32, SubscribeUpdate)>,
    count: u32,
}

impl Stream for VecStream {
    type Item = Result<SubscribeUpdate, Status>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.messages.next().map(|(count, msg)| {
            self.count = count;
            Ok(msg)
        }))
    }
}

impl yellowstone_grpc_client::ReconnectCounter for VecStream {
    fn reconnect_count(&self) -> u32 {
        self.count
    }
}

const fn update(oneof: UpdateOneof) -> SubscribeUpdate {
    SubscribeUpdate {
        filters: vec![],
        update_oneof: Some(oneof),
        created_at: None,
    }
}

const fn status(slot: u64, status: SlotStatus) -> SubscribeUpdate {
    update(UpdateOneof::Slot(SubscribeUpdateSlot {
        slot,
        parent: None,
        status: status as i32,
        dead_error: None,
        bank_id: Some(slot),
    }))
}

fn block_meta(slot: u64) -> SubscribeUpdate {
    update(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
        slot,
        blockhash: format!("hash-{slot}"),
        parent_slot: slot.saturating_sub(1),
        bank_id: slot,
        ..Default::default()
    }))
}

/// One slot as a full-stream subscriber sees it: statuses, payloads, then BlockMeta.
fn slot_messages(slot: u64) -> Vec<SubscribeUpdate> {
    let mut msgs = vec![status(slot, SlotStatus::SlotProcessed)];
    for index in 0..TXS_PER_SLOT {
        msgs.push(update(UpdateOneof::Transaction(
            SubscribeUpdateTransaction {
                transaction: Some(SubscribeUpdateTransactionInfo {
                    index,
                    ..Default::default()
                }),
                slot,
                bank_id: slot,
            },
        )));
    }
    for index in 0..ENTRIES_PER_SLOT {
        msgs.push(update(UpdateOneof::Entry(SubscribeUpdateEntry {
            slot,
            index,
            ..Default::default()
        })));
    }
    for i in 0..ACCOUNTS_PER_SLOT {
        let mut pubkey = vec![0; 32];
        pubkey[..8].copy_from_slice(&i.to_le_bytes());
        let mut signature = vec![0; 64];
        signature[..8].copy_from_slice(&slot.to_le_bytes());
        signature[8..16].copy_from_slice(&i.to_le_bytes());
        msgs.push(update(UpdateOneof::Account(SubscribeUpdateAccount {
            account: Some(SubscribeUpdateAccountInfo {
                pubkey,
                txn_signature: Some(signature),
                ..Default::default()
            }),
            slot,
            is_startup: false,
            bank_id: Some(slot),
        })));
    }
    msgs.push(block_meta(slot));
    msgs.push(status(slot, SlotStatus::SlotConfirmed));
    msgs.push(status(slot, SlotStatus::SlotFinalized));
    msgs
}

fn live() -> Vec<(u32, SubscribeUpdate)> {
    (1..=SLOTS)
        .flat_map(slot_messages)
        .map(|msg| (0, msg))
        .collect()
}

/// A live stream, a reconnect, then a replay of the last `REPLAY_SLOTS` sealed slots.
fn reconnect_replay() -> Vec<(u32, SubscribeUpdate)> {
    let mut msgs = live();
    msgs.extend(
        (SLOTS - REPLAY_SLOTS + 1..=SLOTS)
            .flat_map(slot_messages)
            .map(|msg| (1, msg)),
    );
    msgs
}

fn blockmeta_only() -> Vec<(u32, SubscribeUpdate)> {
    (1..=SLOTS * 10).map(|slot| (0, block_meta(slot))).collect()
}

fn drain(messages: Vec<(u32, SubscribeUpdate)>) -> usize {
    let mut stream = DedupStream::new(
        VecStream {
            messages: messages.into_iter(),
            count: 0,
        },
        DedupState::default(),
    );
    let mut delivered = 0;
    while let Some(Some(msg)) = stream.next().now_or_never() {
        black_box(msg.expect("bench stream never errors"));
        delivered += 1;
    }
    delivered
}

fn bench_dedup(c: &mut Criterion) {
    let mut group = c.benchmark_group("dedup_stream");
    for (name, messages) in [
        ("live", live()),
        ("reconnect_replay", reconnect_replay()),
        ("blockmeta_only", blockmeta_only()),
    ] {
        group.throughput(Throughput::Elements(messages.len() as u64));
        group.bench_function(name, |b| {
            b.iter_batched(|| messages.clone(), drain, BatchSize::LargeInput)
        });
    }
    group.finish();
}

criterion_group!(benches, bench_dedup);
criterion_main!(benches);
