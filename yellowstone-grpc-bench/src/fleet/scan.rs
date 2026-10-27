//! Reads the few top-level fields a probe needs from an encoded update, without a full decode.

/// Oneof field numbers of `SubscribeUpdate` and `SubscribeUpdateDeshred`, mapped to one kind space.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u8)]
pub enum Kind {
    Unknown = 0,
    Account = 1,
    Slot = 2,
    Transaction = 3,
    TransactionStatus = 4,
    Block = 5,
    Ping = 6,
    Pong = 7,
    BlockMeta = 8,
    Entry = 9,
    BlockFooter = 10,
    EntryUpdateParent = 11,
    DeshredTransaction = 12,
    DeshredUpdateParent = 13,
}

impl Kind {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Account => "account",
            Self::Slot => "slot",
            Self::Transaction => "transaction",
            Self::TransactionStatus => "transaction_status",
            Self::Block => "block",
            Self::Ping => "ping",
            Self::Pong => "pong",
            Self::BlockMeta => "block_meta",
            Self::Entry => "entry",
            Self::BlockFooter => "block_footer",
            Self::EntryUpdateParent => "entry_update_parent",
            Self::DeshredTransaction => "deshred_transaction",
            Self::DeshredUpdateParent => "deshred_update_parent",
        }
    }

    pub const fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Account,
            2 => Self::Slot,
            3 => Self::Transaction,
            4 => Self::TransactionStatus,
            5 => Self::Block,
            6 => Self::Ping,
            7 => Self::Pong,
            8 => Self::BlockMeta,
            9 => Self::Entry,
            10 => Self::BlockFooter,
            11 => Self::EntryUpdateParent,
            12 => Self::DeshredTransaction,
            13 => Self::DeshredUpdateParent,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Scanned {
    pub kind: Option<Kind>,
    pub created_ns: Option<u64>,
    pub slot: Option<u64>,
    /// Slot status value, for slot updates.
    pub status: Option<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rpc {
    Subscribe,
    Deshred,
}

/// (kind, field number of `slot` in the oneof message, field number of `status`).
const fn subscribe_field(field: u32) -> Option<(Kind, u32, u32)> {
    Some(match field {
        2 => (Kind::Account, 2, 0),
        3 => (Kind::Slot, 1, 3),
        4 => (Kind::Transaction, 2, 0),
        10 => (Kind::TransactionStatus, 1, 0),
        5 => (Kind::Block, 1, 0),
        6 => (Kind::Ping, 0, 0),
        9 => (Kind::Pong, 0, 0),
        7 => (Kind::BlockMeta, 1, 0),
        8 => (Kind::Entry, 1, 0),
        12 => (Kind::BlockFooter, 1, 0),
        13 => (Kind::EntryUpdateParent, 1, 0),
        _ => return None,
    })
}

const fn deshred_field(field: u32) -> Option<(Kind, u32, u32)> {
    Some(match field {
        2 => (Kind::DeshredTransaction, 2, 0),
        3 => (Kind::Ping, 0, 0),
        4 => (Kind::Pong, 0, 0),
        6 => (Kind::Slot, 1, 3),
        7 => (Kind::DeshredUpdateParent, 1, 0),
        _ => return None,
    })
}

pub fn scan(rpc: Rpc, buf: &[u8]) -> Scanned {
    let created_at_field = match rpc {
        Rpc::Subscribe => 11,
        Rpc::Deshred => 5,
    };
    let mut out = Scanned::default();
    let mut pos = 0;
    while pos < buf.len() {
        let Some((field, wire)) = read_tag(buf, &mut pos) else {
            break;
        };
        if wire != 2 {
            if !skip(buf, &mut pos, wire) {
                break;
            }
            continue;
        }
        let Some(body) = read_len(buf, &mut pos) else {
            break;
        };
        if field == created_at_field {
            out.created_ns = timestamp_ns(body);
            continue;
        }
        let mapped = match rpc {
            Rpc::Subscribe => subscribe_field(field),
            Rpc::Deshred => deshred_field(field),
        };
        if let Some((kind, slot_field, status_field)) = mapped {
            out.kind = Some(kind);
            if slot_field != 0 {
                out.slot = varint_field(body, slot_field);
            }
            if status_field != 0 {
                out.status = varint_field(body, status_field).map(|status| status as u8);
            }
        }
    }
    out
}

fn timestamp_ns(body: &[u8]) -> Option<u64> {
    let seconds = varint_field(body, 1).unwrap_or(0);
    let nanos = varint_field(body, 2).unwrap_or(0);
    Some(seconds.saturating_mul(1_000_000_000).saturating_add(nanos))
}

/// First varint value of `field` at the top level of `buf`.
fn varint_field(buf: &[u8], wanted: u32) -> Option<u64> {
    let mut pos = 0;
    while pos < buf.len() {
        let (field, wire) = read_tag(buf, &mut pos)?;
        if field == wanted && wire == 0 {
            return read_varint(buf, &mut pos);
        }
        if !skip(buf, &mut pos, wire) {
            return None;
        }
    }
    None
}

fn read_tag(buf: &[u8], pos: &mut usize) -> Option<(u32, u8)> {
    let tag = read_varint(buf, pos)?;
    Some(((tag >> 3) as u32, (tag & 7) as u8))
}

fn read_len<'a>(buf: &'a [u8], pos: &mut usize) -> Option<&'a [u8]> {
    let len = usize::try_from(read_varint(buf, pos)?).ok()?;
    let end = pos.checked_add(len).filter(|end| *end <= buf.len())?;
    let body = &buf[*pos..end];
    *pos = end;
    Some(body)
}

fn skip(buf: &[u8], pos: &mut usize, wire: u8) -> bool {
    match wire {
        0 => read_varint(buf, pos).is_some(),
        1 => advance(buf, pos, 8),
        2 => read_len(buf, pos).is_some(),
        5 => advance(buf, pos, 4),
        _ => false,
    }
}

const fn advance(buf: &[u8], pos: &mut usize, n: usize) -> bool {
    match pos.checked_add(n) {
        Some(end) if end <= buf.len() => {
            *pos = end;
            true
        }
        _ => false,
    }
}

fn read_varint(buf: &[u8], pos: &mut usize) -> Option<u64> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *buf.get(*pos)?;
        *pos += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some(value);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        prost::Message,
        yellowstone_grpc_proto::geyser::{
            subscribe_update::UpdateOneof, SubscribeUpdate, SubscribeUpdateAccount,
            SubscribeUpdateAccountInfo, SubscribeUpdateSlot,
        },
    };

    #[test]
    fn scans_account_and_slot_updates() {
        let account = SubscribeUpdate {
            filters: vec!["probe".to_owned()],
            update_oneof: Some(UpdateOneof::Account(SubscribeUpdateAccount {
                account: Some(SubscribeUpdateAccountInfo {
                    pubkey: vec![1; 32],
                    data: vec![7u8; 300].into(),
                    ..Default::default()
                }),
                slot: 123_456_789,
                is_startup: false,
                bank_id: Some(42),
            })),
            created_at: Some(prost_types::Timestamp {
                seconds: 1_700_000_000,
                nanos: 123,
            }),
        };
        let scanned = scan(Rpc::Subscribe, &account.encode_to_vec());
        assert_eq!(scanned.kind, Some(Kind::Account));
        assert_eq!(scanned.slot, Some(123_456_789));
        assert_eq!(scanned.created_ns, Some(1_700_000_000_000_000_123));

        let slot = SubscribeUpdate {
            filters: vec![],
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot: 99,
                parent: Some(98),
                status: 5,
                dead_error: None,
                bank_id: Some(3),
            })),
            created_at: None,
        };
        let scanned = scan(Rpc::Subscribe, &slot.encode_to_vec());
        assert_eq!(scanned.kind, Some(Kind::Slot));
        assert_eq!(scanned.slot, Some(99));
        assert_eq!(scanned.status, Some(5));
    }
}
