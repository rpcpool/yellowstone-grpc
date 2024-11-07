use {
    super::{
        filter::FilterName,
        message::{
            MessageAccount, MessageAccountInfo, MessageBlockMeta, MessageEntry, MessageSlot,
            MessageTransaction, MessageTransactionInfo,
        },
    },
    crate::{
        convert_to,
        geyser::{
            subscribe_update::UpdateOneof, CommitmentLevel as CommitmentLevelProto,
            SubscribeUpdate, SubscribeUpdateBlockMeta, SubscribeUpdateEntry, SubscribeUpdatePing,
            SubscribeUpdatePong, SubscribeUpdateSlot,
        },
    },
    bytes::buf::{Buf, BufMut},
    prost::{
        encoding::{
            encode_key, encode_varint, encoded_len_varint, key_len, message, DecodeContext,
            WireType,
        },
        DecodeError,
    },
    smallvec::SmallVec,
    solana_transaction_status::Reward,
    std::{ops::Range, sync::Arc},
};

#[derive(Debug)]
pub struct Message {
    pub filters: MessageFilters,
    pub message: MessageRef,
}

impl From<&Message> for SubscribeUpdate {
    fn from(message: &Message) -> Self {
        SubscribeUpdate {
            filters: message
                .filters
                .iter()
                .map(|f| f.as_ref().to_owned())
                .collect(),
            update_oneof: Some((&message.message).into()),
        }
    }
}

impl prost::Message for Message {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        for name in self.filters.iter().map(|filter| filter.as_ref()) {
            encode_key(1u32, WireType::LengthDelimited, buf);
            encode_varint(name.len() as u64, buf);
            buf.put_slice(name.as_bytes());
        }
        self.message.encode_raw(buf)
    }

    fn encoded_len(&self) -> usize {
        key_len(1u32) * self.filters.len()
            + self
                .filters
                .iter()
                .map(|filter| {
                    encoded_len_varint(filter.as_ref().len() as u64) + filter.as_ref().len()
                })
                .sum::<usize>()
            + self.message.encoded_len()
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: WireType,
        _buf: &mut impl Buf,
        _ctx: DecodeContext,
    ) -> Result<(), DecodeError> {
        unimplemented!()
    }

    fn clear(&mut self) {
        unimplemented!()
    }
}

impl Message {
    pub fn new(filters: MessageFilters, message: MessageRef) -> Self {
        Self { filters, message }
    }
}

pub type MessageFilters = SmallVec<[FilterName; 4]>;

#[derive(Debug)]
pub enum MessageRef {
    Account,                          // 2
    Slot(MessageSlot),                // 3
    Transaction,                      // 4
    TransactionStatus,                // 10
    Block,                            // 5
    Ping,                             // 6
    Pong(MessageRefPong),             // 9
    BlockMeta(Arc<MessageBlockMeta>), // 7
    Entry(Arc<MessageEntry>),         // 8
}

impl From<&MessageRef> for UpdateOneof {
    fn from(message: &MessageRef) -> Self {
        match message {
            MessageRef::Account => todo!(),
            MessageRef::Slot(msg) => Self::Slot(msg.into()),
            MessageRef::Transaction => todo!(),
            MessageRef::TransactionStatus => todo!(),
            MessageRef::Block => todo!(),
            MessageRef::Ping => Self::Ping(SubscribeUpdatePing {}),
            MessageRef::Pong(msg) => Self::Pong(SubscribeUpdatePong { id: msg.id }),
            MessageRef::BlockMeta(msg) => Self::BlockMeta(msg.as_ref().into()),
            MessageRef::Entry(msg) => Self::Entry(msg.as_ref().into()),
        }
    }
}

impl prost::Message for MessageRef {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        match self {
            MessageRef::Account => todo!(),
            MessageRef::Slot(msg) => message::encode(3u32, msg, buf),
            MessageRef::Transaction => todo!(),
            MessageRef::TransactionStatus => todo!(),
            MessageRef::Block => todo!(),
            MessageRef::Ping => {
                encode_key(6u32, WireType::LengthDelimited, buf);
                encode_varint(0, buf);
            }
            MessageRef::Pong(msg) => message::encode(9u32, msg, buf),
            MessageRef::BlockMeta(msg) => message::encode(7u32, msg.as_ref(), buf),
            MessageRef::Entry(msg) => message::encode(8u32, msg.as_ref(), buf),
        }
    }

    fn encoded_len(&self) -> usize {
        match self {
            MessageRef::Account => todo!(),
            MessageRef::Slot(msg) => message::encoded_len(3u32, msg),
            MessageRef::Transaction => todo!(),
            MessageRef::TransactionStatus => todo!(),
            MessageRef::Block => todo!(),
            MessageRef::Ping => 0,
            MessageRef::Pong(msg) => message::encoded_len(9u32, msg),
            MessageRef::BlockMeta(msg) => message::encoded_len(7u32, msg.as_ref()),
            MessageRef::Entry(msg) => message::encoded_len(8u32, msg.as_ref()),
        }
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: WireType,
        _buf: &mut impl Buf,
        _ctx: DecodeContext,
    ) -> Result<(), DecodeError> {
        unimplemented!()
    }

    fn clear(&mut self) {
        unimplemented!()
    }
}

impl MessageRef {
    pub fn account(message: &MessageAccount, accounts_data_slice: Vec<Range<usize>>) -> Self {
        todo!()
    }

    pub const fn slot(message: MessageSlot) -> Self {
        Self::Slot(message)
    }

    pub fn transaction(message: &MessageTransaction) -> Self {
        todo!()
    }

    pub fn transaction_status(message: &MessageTransaction) -> Self {
        todo!()
    }

    pub fn block(message: MessageRefBlock) -> Self {
        todo!()
    }

    pub const fn pong(id: i32) -> Self {
        Self::Pong(MessageRefPong { id })
    }

    pub fn block_meta(message: Arc<MessageBlockMeta>) -> Self {
        Self::BlockMeta(message)
    }

    pub fn entry(message: Arc<MessageEntry>) -> Self {
        Self::Entry(message)
    }
}

impl From<&MessageSlot> for SubscribeUpdateSlot {
    fn from(msg: &MessageSlot) -> Self {
        Self {
            slot: msg.slot,
            parent: msg.parent,
            status: CommitmentLevelProto::from(msg.status) as i32,
        }
    }
}

impl prost::Message for MessageSlot {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        let status = CommitmentLevelProto::from(self.status) as i32;
        if self.slot != 0u64 {
            ::prost::encoding::uint64::encode(1u32, &self.slot, buf);
        }
        if let ::core::option::Option::Some(ref value) = self.parent {
            ::prost::encoding::uint64::encode(2u32, value, buf);
        }
        if status != CommitmentLevelProto::default() as i32 {
            ::prost::encoding::int32::encode(3u32, &status, buf);
        }
    }

    fn encoded_len(&self) -> usize {
        let status = CommitmentLevelProto::from(self.status) as i32;
        (if self.slot != 0u64 {
            ::prost::encoding::uint64::encoded_len(1u32, &self.slot)
        } else {
            0
        }) + self.parent.as_ref().map_or(0, |value| {
            ::prost::encoding::uint64::encoded_len(2u32, value)
        }) + if status != CommitmentLevelProto::default() as i32 {
            ::prost::encoding::int32::encoded_len(3u32, &status)
        } else {
            0
        }
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: WireType,
        _buf: &mut impl Buf,
        _ctx: DecodeContext,
    ) -> Result<(), DecodeError> {
        unimplemented!()
    }

    fn clear(&mut self) {
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct MessageRefBlock {
    pub meta: Arc<MessageBlockMeta>,
    pub transactions: Vec<Arc<MessageTransactionInfo>>,
    pub updated_account_count: u64,
    pub accounts: Vec<Arc<MessageAccountInfo>>,
    pub accounts_data_slice: Vec<Range<usize>>,
    pub entries: Vec<Arc<MessageEntry>>,
}

#[derive(prost::Message)]
pub struct MessageRefPong {
    #[prost(int32, tag = "1")]
    pub id: i32,
}

impl From<&MessageBlockMeta> for SubscribeUpdateBlockMeta {
    fn from(msg: &MessageBlockMeta) -> Self {
        Self {
            slot: msg.slot,
            blockhash: msg.blockhash.clone(),
            rewards: Some(convert_to::create_rewards_obj(
                msg.rewards.as_slice(),
                msg.num_partitions,
            )),
            block_time: msg.block_time.map(convert_to::create_timestamp),
            block_height: msg.block_height.map(convert_to::create_block_height),
            parent_slot: msg.parent_slot,
            parent_blockhash: msg.parent_blockhash.clone(),
            executed_transaction_count: msg.executed_transaction_count,
            entries_count: msg.entries_count,
        }
    }
}

impl prost::Message for MessageBlockMeta {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        // if self.slot != 0u64 {
        //     ::prost::encoding::uint64::encode(1u32, &self.slot, buf);
        // }
        // if self.blockhash != "" {
        //     ::prost::encoding::string::encode(2u32, &self.blockhash, buf);
        // }
        // if let Some(ref msg) = self.rewards {
        //     ::prost::encoding::message::encode(3u32, msg, buf);
        // }
        // if let Some(ref msg) = self.block_time {
        //     ::prost::encoding::message::encode(4u32, msg, buf);
        // }
        // if let Some(ref msg) = self.block_height {
        //     ::prost::encoding::message::encode(5u32, msg, buf);
        // }
        // if self.parent_slot != 0u64 {
        //     ::prost::encoding::uint64::encode(6u32, &self.parent_slot, buf);
        // }
        // if self.parent_blockhash != "" {
        //     ::prost::encoding::string::encode(7u32, &self.parent_blockhash, buf);
        // }
        // if self.executed_transaction_count != 0u64 {
        //     ::prost::encoding::uint64::encode(
        //         8u32,
        //         &self.executed_transaction_count,
        //         buf,
        //     );
        // }
        // if self.entries_count != 0u64 {
        //     ::prost::encoding::uint64::encode(9u32, &self.entries_count, buf);
        // }
        todo!()
    }

    fn encoded_len(&self) -> usize {
        let block_time = self.block_time.map(convert_to::create_timestamp);
        let block_height = self.block_height.map(convert_to::create_block_height);
        (if self.slot != 0u64 {
            ::prost::encoding::uint64::encoded_len(1u32, &self.slot)
        } else {
            0
        }) + if self.blockhash != "" {
            ::prost::encoding::string::encoded_len(2u32, &self.blockhash)
        } else {
            0
        } + {
            let len = key_len(1u32) * self.rewards.len()
                + self.rewards.iter().map(|reward| 0).sum::<usize>()
                + self
                    .num_partitions
                    .as_ref()
                    .map_or(0, |msg| ::prost::encoding::message::encoded_len(2u32, msg));
            key_len(3u32) + encoded_len_varint(len as u64) + len
        } + block_time
            .as_ref()
            .map_or(0, |msg| ::prost::encoding::message::encoded_len(4u32, msg))
            + block_height
                .as_ref()
                .map_or(0, |msg| ::prost::encoding::message::encoded_len(5u32, msg))
            + if self.parent_slot != 0u64 {
                ::prost::encoding::uint64::encoded_len(6u32, &self.parent_slot)
            } else {
                0
            }
            + if self.parent_blockhash != "" {
                ::prost::encoding::string::encoded_len(7u32, &self.parent_blockhash)
            } else {
                0
            }
            + if self.executed_transaction_count != 0u64 {
                ::prost::encoding::uint64::encoded_len(8u32, &self.executed_transaction_count)
            } else {
                0
            }
            + if self.entries_count != 0u64 {
                ::prost::encoding::uint64::encoded_len(9u32, &self.entries_count)
            } else {
                0
            }
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: WireType,
        _buf: &mut impl Buf,
        _ctx: DecodeContext,
    ) -> Result<(), DecodeError> {
        unimplemented!()
    }

    fn clear(&mut self) {
        unimplemented!()
    }
}

impl From<&MessageEntry> for SubscribeUpdateEntry {
    fn from(msg: &MessageEntry) -> Self {
        Self {
            slot: msg.slot,
            index: msg.index as u64,
            num_hashes: msg.num_hashes,
            hash: msg.hash.into(),
            executed_transaction_count: msg.executed_transaction_count,
            starting_transaction_index: msg.starting_transaction_index,
        }
    }
}

impl prost::Message for MessageEntry {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        let index = self.index as u64;
        if self.slot != 0u64 {
            ::prost::encoding::uint64::encode(1u32, &self.slot, buf);
        }
        if index != 0u64 {
            ::prost::encoding::uint64::encode(2u32, &index, buf);
        }
        if self.num_hashes != 0u64 {
            ::prost::encoding::uint64::encode(3u32, &self.num_hashes, buf);
        }
        if !self.hash.is_empty() {
            prost_bytes_encode_raw(4u32, &self.hash, buf);
        }
        if self.executed_transaction_count != 0u64 {
            ::prost::encoding::uint64::encode(5u32, &self.executed_transaction_count, buf);
        }
        if self.starting_transaction_index != 0u64 {
            ::prost::encoding::uint64::encode(6u32, &self.starting_transaction_index, buf);
        }
    }

    fn encoded_len(&self) -> usize {
        let index = self.index as u64;
        (if self.slot != 0u64 {
            ::prost::encoding::uint64::encoded_len(1u32, &self.slot)
        } else {
            0
        }) + if index != 0u64 {
            ::prost::encoding::uint64::encoded_len(2u32, &index)
        } else {
            0
        } + if self.num_hashes != 0u64 {
            ::prost::encoding::uint64::encoded_len(3u32, &self.num_hashes)
        } else {
            0
        } + if !self.hash.is_empty() {
            prost_bytes_encoded_len(4u32, &self.hash)
        } else {
            0
        } + if self.executed_transaction_count != 0u64 {
            ::prost::encoding::uint64::encoded_len(5u32, &self.executed_transaction_count)
        } else {
            0
        } + if self.starting_transaction_index != 0u64 {
            ::prost::encoding::uint64::encoded_len(6u32, &self.starting_transaction_index)
        } else {
            0
        }
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: WireType,
        _buf: &mut impl Buf,
        _ctx: DecodeContext,
    ) -> Result<(), DecodeError> {
        unimplemented!()
    }

    fn clear(&mut self) {
        unimplemented!()
    }
}

#[inline]
fn prost_bytes_encode_raw(tag: u32, value: &[u8], buf: &mut impl BufMut) {
    encode_key(tag, WireType::LengthDelimited, buf);
    encode_varint(value.len() as u64, buf);
    buf.put(value);
}

#[inline]
pub fn prost_bytes_encoded_len(tag: u32, value: &[u8]) -> usize {
    key_len(tag) + encoded_len_varint(value.len() as u64) + value.len()
}

#[cfg(test)]
mod tests {
    use {
        super::{
            FilterName, Message, MessageBlockMeta, MessageEntry, MessageFilters, MessageRef,
            MessageSlot,
        },
        crate::{geyser::SubscribeUpdate, plugin::message::CommitmentLevel},
        prost::Message as _,
        prost_011::Message as _,
        solana_storage_proto::convert::generated,
        solana_transaction_status::ConfirmedBlock,
        std::{fs, sync::Arc},
    };

    fn create_message_filters(names: &[&str]) -> MessageFilters {
        let mut filters = MessageFilters::new();
        for name in names {
            filters.push(FilterName::new(*name));
        }
        filters
    }

    fn create_entries() -> Vec<Arc<MessageEntry>> {
        [
            MessageEntry {
                slot: 299888121,
                index: 42,
                num_hashes: 128,
                hash: [98; 32],
                executed_transaction_count: 32,
                starting_transaction_index: 1000,
            },
            MessageEntry {
                slot: 299888121,
                index: 0,
                num_hashes: 16,
                hash: [42; 32],
                executed_transaction_count: 32,
                starting_transaction_index: 1000,
            },
        ]
        .into_iter()
        .map(Arc::new)
        .collect()
    }

    fn encode_decode_cmp(filters: &[&str], message: MessageRef) {
        let msg = Message {
            filters: create_message_filters(filters),
            message,
        };
        // println!("{:?}", SubscribeUpdate::from(&msg));
        let bytes = msg.encode_to_vec();
        let update = SubscribeUpdate::decode(bytes.as_slice()).expect("failed to decode");
        // println!("{update:?}");
        assert_eq!(update, SubscribeUpdate::from(&msg));
    }

    #[test]
    fn test_message_slot() {
        for slot in [0, 42] {
            for parent in [None, Some(0), Some(42)] {
                for status in [
                    CommitmentLevel::Processed,
                    CommitmentLevel::Confirmed,
                    CommitmentLevel::Finalized,
                ] {
                    encode_decode_cmp(
                        &["123"],
                        MessageRef::slot(MessageSlot {
                            slot,
                            parent,
                            status,
                        }),
                    )
                }
            }
        }
    }

    #[test]
    fn test_message_ping() {
        encode_decode_cmp(&["123"], MessageRef::Ping)
    }

    #[test]
    fn test_message_pong() {
        encode_decode_cmp(&["123"], MessageRef::pong(0));
        encode_decode_cmp(&["123"], MessageRef::pong(42));
    }

    #[test]
    fn test_message_entry() {
        for entry in create_entries() {
            encode_decode_cmp(&["123"], MessageRef::entry(entry));
        }
    }

    #[test]
    fn test_predefined() {
        let location = "./src/plugin/blocks";
        for entry in fs::read_dir(location).expect("failed to read `blocks` dir") {
            let path = entry.expect("failed to read `blocks` dir entry").path();
            let data = fs::read(path).expect("failed to read block");
            let block: ConfirmedBlock = generated::ConfirmedBlock::decode(data.as_slice())
                .expect("failed to decode block")
                .try_into()
                .expect("failed to convert decoded block");

            let block_meta = Arc::new(MessageBlockMeta {
                parent_slot: block.parent_slot,
                slot: block.parent_slot + 1,
                parent_blockhash: block.previous_blockhash,
                blockhash: block.blockhash,
                rewards: block.rewards,
                num_partitions: block.num_partitions,
                block_time: block.block_time,
                block_height: block.block_height,
                executed_transaction_count: block.transactions.len() as u64,
                entries_count: create_entries().len() as u64,
            });
            // encode_decode_cmp(&["123"], MessageRef::block_meta(block_meta));
            let msg = Message {
                filters: create_message_filters(&["123"]),
                message: MessageRef::block_meta(block_meta),
            };
            println!(
                "my {} vs proto {}",
                msg.encoded_len(),
                SubscribeUpdate::from(&msg).encoded_len()
            );
        }

        // use prost::{
        //     encoding::{encoded_len_varint, key_len},
        //     Message as _,
        // };
        // let msg = crate::solana::storage::confirmed_block::BlockHeight { block_height: 42 };
        // let s1 = ::prost::encoding::message::encoded_len(5u32, &msg);
        // println!("{:?}", s1);
        // println!(
        //     "{} {} {}",
        //     key_len(5u32),
        //     encoded_len_varint(msg.encoded_len() as u64),
        //     msg.encoded_len()
        // );
    }
}

// #[derive(Debug, Clone)]
// pub enum FilteredMessage2<'a> {
//     Slot(&'a MessageSlot),
//     Account(&'a MessageAccount),
//     Transaction(&'a MessageTransaction),
//     TransactionStatus(&'a MessageTransaction),
//     Entry(&'a MessageEntry),
//     Block(MessageBlock),
//     BlockMeta(&'a MessageBlockMeta),
// }

// impl<'a> FilteredMessage2<'a> {
//     fn as_proto_account(
//         message: &MessageAccountInfo,
//         accounts_data_slice: &[Range<usize>],
//     ) -> SubscribeUpdateAccountInfo {
//         let data = if accounts_data_slice.is_empty() {
//             message.data.clone()
//         } else {
//             let mut data =
//                 Vec::with_capacity(accounts_data_slice.iter().map(|s| s.end - s.start).sum());
//             for slice in accounts_data_slice {
//                 if message.data.len() >= slice.end {
//                     data.extend_from_slice(&message.data[slice.start..slice.end]);
//                 }
//             }
//             data
//         };
//         SubscribeUpdateAccountInfo {
//             pubkey: message.pubkey.as_ref().into(),
//             lamports: message.lamports,
//             owner: message.owner.as_ref().into(),
//             executable: message.executable,
//             rent_epoch: message.rent_epoch,
//             data,
//             write_version: message.write_version,
//             txn_signature: message.txn_signature.map(|s| s.as_ref().into()),
//         }
//     }

//     fn as_proto_transaction(message: &MessageTransactionInfo) -> SubscribeUpdateTransactionInfo {
//         SubscribeUpdateTransactionInfo {
//             signature: message.signature.as_ref().into(),
//             is_vote: message.is_vote,
//             transaction: Some(convert_to::create_transaction(&message.transaction)),
//             meta: Some(convert_to::create_transaction_meta(&message.meta)),
//             index: message.index as u64,
//         }
//     }

//     fn as_proto_entry(message: &MessageEntry) -> SubscribeUpdateEntry {
//         SubscribeUpdateEntry {
//             slot: message.slot,
//             index: message.index as u64,
//             num_hashes: message.num_hashes,
//             hash: message.hash.into(),
//             executed_transaction_count: message.executed_transaction_count,
//             starting_transaction_index: message.starting_transaction_index,
//         }
//     }

//     pub fn as_proto(&self, accounts_data_slice: &[Range<usize>]) -> UpdateOneof {
//         match self {
//             Self::Slot(message) => UpdateOneof::Slot(SubscribeUpdateSlot {
//                 slot: message.slot,
//                 parent: message.parent,
//                 status: message.status as i32,
//             }),
//             Self::Account(message) => UpdateOneof::Account(SubscribeUpdateAccount {
//                 account: Some(Self::as_proto_account(
//                     message.account.as_ref(),
//                     accounts_data_slice,
//                 )),
//                 slot: message.slot,
//                 is_startup: message.is_startup,
//             }),
//             Self::Transaction(message) => UpdateOneof::Transaction(SubscribeUpdateTransaction {
//                 transaction: Some(Self::as_proto_transaction(message.transaction.as_ref())),
//                 slot: message.slot,
//             }),
//             Self::TransactionStatus(message) => {
//                 UpdateOneof::TransactionStatus(SubscribeUpdateTransactionStatus {
//                     slot: message.slot,
//                     signature: message.transaction.signature.as_ref().into(),
//                     is_vote: message.transaction.is_vote,
//                     index: message.transaction.index as u64,
//                     err: match &message.transaction.meta.status {
//                         Ok(()) => None,
//                         Err(err) => Some(SubscribeUpdateTransactionError {
//                             err: bincode::serialize(&err)
//                                 .expect("transaction error to serialize to bytes"),
//                         }),
//                     },
//                 })
//             }
//             Self::Entry(message) => UpdateOneof::Entry(Self::as_proto_entry(message)),
//             Self::Block(message) => UpdateOneof::Block(SubscribeUpdateBlock {
//                 slot: message.meta.slot,
//                 blockhash: message.meta.blockhash.clone(),
//                 rewards: Some(convert_to::create_rewards_obj(
//                     message.meta.rewards.as_slice(),
//                     message.meta.num_partitions,
//                 )),
//                 block_time: message.meta.block_time.map(convert_to::create_timestamp),
//                 block_height: message
//                     .meta
//                     .block_height
//                     .map(convert_to::create_block_height),
//                 parent_slot: message.meta.parent_slot,
//                 parent_blockhash: message.meta.parent_blockhash.clone(),
//                 executed_transaction_count: message.meta.executed_transaction_count,
//                 transactions: message
//                     .transactions
//                     .iter()
//                     .map(|tx| Self::as_proto_transaction(tx.as_ref()))
//                     .collect(),
//                 updated_account_count: message.updated_account_count,
//                 accounts: message
//                     .accounts
//                     .iter()
//                     .map(|acc| Self::as_proto_account(acc.as_ref(), accounts_data_slice))
//                     .collect(),
//                 entries_count: message.meta.entries_count,
//                 entries: message
//                     .entries
//                     .iter()
//                     .map(|entry| Self::as_proto_entry(entry.as_ref()))
//                     .collect(),
//             }),
//             Self::BlockMeta(message) => UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
//                 slot: message.slot,
//                 blockhash: message.blockhash.clone(),
//                 rewards: Some(convert_to::create_rewards_obj(
//                     message.rewards.as_slice(),
//                     message.num_partitions,
//                 )),
//                 block_time: message.block_time.map(convert_to::create_timestamp),
//                 block_height: message.block_height.map(convert_to::create_block_height),
//                 parent_slot: message.parent_slot,
//                 parent_blockhash: message.parent_blockhash.clone(),
//                 executed_transaction_count: message.executed_transaction_count,
//                 entries_count: message.entries_count,
//             }),
//         }
//     }
// }
