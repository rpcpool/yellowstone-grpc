use {
    crate::fixture::StreamId,
    bytes::Bytes,
    prost::Message,
    yellowstone_grpc_proto::geyser::{
        subscribe_update::UpdateOneof, subscribe_update_deshred::UpdateOneof as DeshredOneof,
        SubscribeUpdate, SubscribeUpdateDeshred,
    },
};

#[allow(clippy::large_enum_variant)]
pub enum Decoded {
    Main(SubscribeUpdate),
    Deshred(SubscribeUpdateDeshred),
}

/// The fields of a frame that indexing, statistics and checks need.
#[derive(Debug, Clone, Copy, Default)]
pub struct FrameInfo {
    pub kind: &'static str,
    pub slot: Option<u64>,
    pub created_at_ns: Option<u64>,
}

pub fn decode(stream: StreamId, data: &Bytes) -> anyhow::Result<Decoded> {
    Ok(match stream {
        StreamId::Main | StreamId::MainAccounts => {
            Decoded::Main(SubscribeUpdate::decode(data.clone())?)
        }
        StreamId::Deshred => Decoded::Deshred(SubscribeUpdateDeshred::decode(data.clone())?),
    })
}

pub const fn timestamp_ns(ts: &prost_types::Timestamp) -> u64 {
    (ts.seconds as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(ts.nanos as u64)
}

pub fn info(decoded: &Decoded) -> FrameInfo {
    match decoded {
        Decoded::Main(update) => main_info(update),
        Decoded::Deshred(update) => deshred_info(update),
    }
}

pub fn main_info(update: &SubscribeUpdate) -> FrameInfo {
    let created_at_ns = update.created_at.as_ref().map(timestamp_ns);
    let (kind, slot) = match &update.update_oneof {
        Some(UpdateOneof::Account(msg)) => ("account", Some(msg.slot)),
        Some(UpdateOneof::Slot(msg)) => ("slot", Some(msg.slot)),
        Some(UpdateOneof::Transaction(msg)) => ("transaction", Some(msg.slot)),
        Some(UpdateOneof::TransactionStatus(msg)) => ("transaction_status", Some(msg.slot)),
        Some(UpdateOneof::Block(msg)) => ("block", Some(msg.slot)),
        Some(UpdateOneof::Ping(_)) => ("ping", None),
        Some(UpdateOneof::Pong(_)) => ("pong", None),
        Some(UpdateOneof::BlockMeta(msg)) => ("block_meta", Some(msg.slot)),
        Some(UpdateOneof::Entry(msg)) => ("entry", Some(msg.slot)),
        Some(UpdateOneof::BlockFooter(msg)) => ("block_footer", Some(msg.slot)),
        Some(UpdateOneof::EntryUpdateParent(msg)) => ("entry_update_parent", Some(msg.slot)),
        None => ("empty", None),
    };
    FrameInfo {
        kind,
        slot,
        created_at_ns,
    }
}

fn deshred_info(update: &SubscribeUpdateDeshred) -> FrameInfo {
    let created_at_ns = update.created_at.as_ref().map(timestamp_ns);
    let (kind, slot) = match &update.update_oneof {
        Some(DeshredOneof::DeshredTransaction(msg)) => ("deshred_transaction", Some(msg.slot)),
        Some(DeshredOneof::Ping(_)) => ("ping", None),
        Some(DeshredOneof::Pong(_)) => ("pong", None),
        Some(DeshredOneof::Slot(msg)) => ("slot", Some(msg.slot)),
        Some(DeshredOneof::DeshredUpdateParent(msg)) => ("deshred_update_parent", Some(msg.slot)),
        None => ("empty", None),
    };
    FrameInfo {
        kind,
        slot,
        created_at_ns,
    }
}
