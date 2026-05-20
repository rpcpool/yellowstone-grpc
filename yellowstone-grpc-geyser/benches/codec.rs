use std::hint::black_box;
use std::sync::OnceLock;
use std::time::SystemTime;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use prost_types::Timestamp;
use solana_pubkey::Pubkey;
use solana_signature::Signature;
use yellowstone_grpc_geyser::plugin::message::MessageAccountInfo;
use yellowstone_grpc_geyser::plugin::shmem::codec::DirectCopyCodec;
use yellowstone_grpc_geyser::plugin::shmem::decoder::decode_account;
use yellowstone_shmem_common::{MessageAccount, MessageAccountInfo as ShmemAccountInfo};

/// Simulates the raw byte slices agave passes to the geyser plugin.
/// `ReplicaAccountInfoV3` has lifetime-bound refs into agave internals
/// so we construct the equivalent here.
struct FakeGeyserAccount {
    pubkey: [u8; 32],
    owner: [u8; 32],
    data: Vec<u8>,
    lamports: u64,
    rent_epoch: u64,
    executable: bool,
    write_version: u64,
    signature: [u8; 64],
}

impl FakeGeyserAccount {
    fn new(data_len: usize) -> Self {
        Self {
            pubkey: [1u8; 32],
            owner: [2u8; 32],
            data: vec![0xABu8; data_len],
            lamports: 1_000_000,
            rent_epoch: u64::MAX,
            executable: false,
            write_version: 42,
            signature: [3u8; 64],
        }
    }

    /// Mirrors `MessageAccountInfo::from_geyser`, the current path write cost.
    /// `data.to_vec()` + `Bytes::from()` + `Pubkey::try_from` x2 + `Signature` copy.
    fn into_dm_account_info(&self) -> MessageAccountInfo {
        MessageAccountInfo {
            pubkey: Pubkey::try_from(self.pubkey.as_slice()).unwrap(),
            lamports: self.lamports,
            owner: Pubkey::try_from(self.owner.as_slice()).unwrap(),
            executable: self.executable,
            rent_epoch: self.rent_epoch,
            data: Bytes::from(self.data.clone()),
            write_version: self.write_version,
            txn_signature: Some(Signature::try_from(self.signature.as_slice()).unwrap()),
            pre_encoded: OnceLock::new(),
        }
    }

    fn as_shmem_account(&self, slot: u64) -> MessageAccount {
        MessageAccount {
            account: ShmemAccountInfo {
                pubkey: self.pubkey.to_vec(),
                lamports: self.lamports,
                owner: self.owner.to_vec(),
                executable: self.executable,
                rent_epoch: self.rent_epoch,
                data: self.data.clone(),
                write_version: self.write_version,
                txn_signature: Some(self.signature.to_vec()),
            },
            slot,
            is_startup: false,
        }
    }
}

/// Write cost, current path (baseline).
/// `from_geyser` equivalent: one heap alloc for account data, two pubkey copies,
/// one signature copy. Proto encoding is deferred via `OnceLock` and not counted here.
fn bench_current_write_256b(c: &mut Criterion) {
    let raw = FakeGeyserAccount::new(256);
    c.bench_function("current_write_256b", |b| {
        b.iter(|| black_box(&raw).into_dm_account_info())
    });
}

fn bench_current_write_10kb(c: &mut Criterion) {
    let raw = FakeGeyserAccount::new(10 * 1024);
    c.bench_function("current_write_10kb", |b| {
        b.iter(|| black_box(&raw).into_dm_account_info())
    });
}

/// Write cost, shmem path.
/// `DirectCopyCodec::encode_account_into`: zero heap alloc, direct struct copy
/// into pre-claimed mcache pointer. Replaces `from_geyser` on the validator side.
fn bench_shmem_write_256b(c: &mut Criterion) {
    let account = FakeGeyserAccount::new(256).as_shmem_account(300_000_000);
    let size = DirectCopyCodec::account_encoded_size(&account);
    let mut buf = vec![0u8; size];

    c.bench_function("shmem_write_256b", |b| {
        b.iter(|| unsafe {
            DirectCopyCodec::encode_account_into(black_box(&account), buf.as_mut_ptr())
        })
    });
}

fn bench_shmem_write_10kb(c: &mut Criterion) {
    let account = FakeGeyserAccount::new(10 * 1024).as_shmem_account(300_000_000);
    let size = DirectCopyCodec::account_encoded_size(&account);
    let mut buf = vec![0u8; size];

    c.bench_function("shmem_write_10kb", |b| {
        b.iter(|| unsafe {
            DirectCopyCodec::encode_account_into(black_box(&account), buf.as_mut_ptr())
        })
    });
}

/// Read cost, shmem path (no current path equivalent).
/// `decode_account` reads raw bytes back into `GeyserMessage` via direct struct reads.
/// `to_dm_message` converts to the dragons mouth `Message` type consumed by `geyser_loop`.
/// The timestamp is passed in rather than calling `SystemTime::now` per message. In
/// production this is called once per drain batch in `run_shmem_reader`.
fn bench_shmem_read_256b(c: &mut Criterion) {
    let account = FakeGeyserAccount::new(256).as_shmem_account(300_000_000);
    let size = DirectCopyCodec::account_encoded_size(&account);
    let mut buf = vec![0u8; size];
    unsafe { DirectCopyCodec::encode_account_into(&account, buf.as_mut_ptr()) };

    c.bench_function("shmem_read_256b", |b| {
        b.iter(|| {
            let msg = decode_account(black_box(&buf)).unwrap();
            yellowstone_grpc_geyser::plugin::shmem::decoder::ProstShmemDecoder::to_dm_message(
                msg,
                Timestamp::from(SystemTime::now()),
            )
            .unwrap()
        })
    });
}

fn bench_shmem_read_10kb(c: &mut Criterion) {
    let account = FakeGeyserAccount::new(10 * 1024).as_shmem_account(300_000_000);
    let size = DirectCopyCodec::account_encoded_size(&account);
    let mut buf = vec![0u8; size];
    unsafe { DirectCopyCodec::encode_account_into(&account, buf.as_mut_ptr()) };

    c.bench_function("shmem_read_10kb", |b| {
        b.iter(|| {
            let msg = decode_account(black_box(&buf)).unwrap();
            yellowstone_grpc_geyser::plugin::shmem::decoder::ProstShmemDecoder::to_dm_message(
                msg,
                Timestamp::from(SystemTime::now()),
            )
            .unwrap()
        })
    });
}

/// Full round trip, current path.
/// Write cost only: `from_geyser` equivalent. Proto encoding via `OnceLock::get_or_init`
/// (~387 ns) is paid lazily on first subscriber access and is not measured here.
/// Add 387 ns to compare fairly against the shmem round trip below.
fn bench_current_roundtrip_256b(c: &mut Criterion) {
    let raw = FakeGeyserAccount::new(256);
    c.bench_function("current_roundtrip_256b", |b| {
        b.iter(|| {
            let info = black_box(&raw).into_dm_account_info();
            let _ = info.data.len();
            info
        })
    });
}

/// Full round trip, shmem path.
/// Write (`encode_account_into`) + read (`decode_account` + `to_dm_message`).
/// This is the full per-account cost before `geyser_loop` receives the message.
/// Does not include futex wake, which is amortized separately via batch writes.
fn bench_shmem_roundtrip_256b(c: &mut Criterion) {
    let account = FakeGeyserAccount::new(256).as_shmem_account(300_000_000);
    let size = DirectCopyCodec::account_encoded_size(&account);
    let mut buf = vec![0u8; size];
    unsafe { DirectCopyCodec::encode_account_into(&account, buf.as_mut_ptr()) };

    c.bench_function("shmem_roundtrip_256b", |b| {
        b.iter(|| {
            unsafe {
                DirectCopyCodec::encode_account_into(black_box(&account), buf.as_mut_ptr())
            };
            let msg = decode_account(&buf).unwrap();
            yellowstone_grpc_geyser::plugin::shmem::decoder::ProstShmemDecoder::to_dm_message(
                msg,
                Timestamp::from(SystemTime::now()),
            )
            .unwrap()
        })
    });
}

/// Isolates `Arc::new(MessageAccountInfo)` cost within `to_dm_message`.
/// At 57 ns this is ~40% of total shmem read cost. Flagged for object pool
/// optimization in a follow-up.
fn bench_arc_alloc(c: &mut Criterion) {
    use std::sync::Arc;

    let raw = FakeGeyserAccount::new(256);
    c.bench_function("arc_new_account_info_256b", |b| {
        b.iter(|| {
            Arc::new(black_box(MessageAccountInfo {
                pubkey: Pubkey::try_from(raw.pubkey.as_slice()).unwrap(),
                lamports: raw.lamports,
                owner: Pubkey::try_from(raw.owner.as_slice()).unwrap(),
                executable: raw.executable,
                rent_epoch: raw.rent_epoch,
                data: Bytes::from(raw.data.clone()),
                write_version: raw.write_version,
                txn_signature: Some(
                    Signature::try_from(raw.signature.as_slice()).unwrap(),
                ),
                pre_encoded: OnceLock::new(),
            }))
        })
    });
}

criterion_group!(
    benches,
    bench_current_write_256b,
    bench_current_write_10kb,
    bench_shmem_write_256b,
    bench_shmem_write_10kb,
    bench_shmem_read_256b,
    bench_shmem_read_10kb,
    bench_current_roundtrip_256b,
    bench_shmem_roundtrip_256b,
    bench_arc_alloc,
);
criterion_main!(benches);