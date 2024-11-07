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
        solana::storage::confirmed_block::RewardType as RewardTypeProto,
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
        if self.slot != 0u64 {
            ::prost::encoding::uint64::encode(1u32, &self.slot, buf);
        }
        if !self.blockhash.is_empty() {
            ::prost::encoding::string::encode(2u32, &self.blockhash, buf);
        }
        self.rewards_encode(buf);
        if let Some(block_time) = self.block_time {
            let msg = convert_to::create_timestamp(block_time);
            ::prost::encoding::message::encode(4u32, &msg, buf);
        }
        if let Some(block_height) = self.block_height {
            let msg = convert_to::create_block_height(block_height);
            ::prost::encoding::message::encode(5u32, &msg, buf);
        }
        if self.parent_slot != 0u64 {
            ::prost::encoding::uint64::encode(6u32, &self.parent_slot, buf);
        }
        if !self.parent_blockhash.is_empty() {
            ::prost::encoding::string::encode(7u32, &self.parent_blockhash, buf);
        }
        if self.executed_transaction_count != 0u64 {
            ::prost::encoding::uint64::encode(8u32, &self.executed_transaction_count, buf);
        }
        if self.entries_count != 0u64 {
            ::prost::encoding::uint64::encode(9u32, &self.entries_count, buf);
        }
    }

    fn encoded_len(&self) -> usize {
        (if self.slot != 0u64 {
            ::prost::encoding::uint64::encoded_len(1u32, &self.slot)
        } else {
            0
        }) + if !self.blockhash.is_empty() {
            ::prost::encoding::string::encoded_len(2u32, &self.blockhash)
        } else {
            0
        } + {
            let len = self.rewards_encoded_len();
            key_len(3u32) + encoded_len_varint(len as u64) + len
        } + self
            .block_time
            .map(convert_to::create_timestamp)
            .map_or(0, |msg| ::prost::encoding::message::encoded_len(4u32, &msg))
            + self
                .block_height
                .map(convert_to::create_block_height)
                .map_or(0, |msg| ::prost::encoding::message::encoded_len(5u32, &msg))
            + if self.parent_slot != 0u64 {
                ::prost::encoding::uint64::encoded_len(6u32, &self.parent_slot)
            } else {
                0
            }
            + if !self.parent_blockhash.is_empty() {
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

impl MessageBlockMeta {
    fn rewards_encode(&self, buf: &mut impl BufMut) {
        encode_key(3u32, WireType::LengthDelimited, buf);
        encode_varint(self.rewards_encoded_len() as u64, buf);
        for reward in &self.rewards {
            Self::reward_encode(reward, buf);
        }
        if let Some(num_partitions) = self.num_partitions {
            let msg = convert_to::create_num_partitions(num_partitions);
            ::prost::encoding::message::encode(2u32, &msg, buf);
        }
    }

    fn reward_encode(reward: &Reward, buf: &mut impl BufMut) {
        encode_key(1u32, WireType::LengthDelimited, buf);
        encode_varint(Self::reward_encoded_len(reward) as u64, buf);

        let reward_type = convert_to::create_reward_type(reward.reward_type) as i32;
        let commission = Self::commission_to_str(reward.commission);

        if !reward.pubkey.is_empty() {
            ::prost::encoding::string::encode(1u32, &reward.pubkey, buf);
        }
        if reward.lamports != 0i64 {
            ::prost::encoding::int64::encode(2u32, &reward.lamports, buf);
        }
        if reward.post_balance != 0u64 {
            ::prost::encoding::uint64::encode(3u32, &reward.post_balance, buf);
        }
        if reward_type != RewardTypeProto::default() as i32 {
            ::prost::encoding::int32::encode(4u32, &reward_type, buf);
        }
        if commission != b"" {
            prost_bytes_encode_raw(5u32, commission, buf);
        }
    }

    fn rewards_encoded_len(&self) -> usize {
        key_len(1u32) * self.rewards.len()
            + self
                .rewards
                .iter()
                .map(Self::reward_encoded_len)
                .map(|len| len + encoded_len_varint(len as u64))
                .sum::<usize>()
            + self
                .num_partitions
                .map(convert_to::create_num_partitions)
                .map_or(0, |msg| ::prost::encoding::message::encoded_len(2u32, &msg))
    }

    fn reward_encoded_len(reward: &Reward) -> usize {
        let reward_type = convert_to::create_reward_type(reward.reward_type) as i32;
        let commission = Self::commission_to_str(reward.commission);
        (if !reward.pubkey.is_empty() {
            ::prost::encoding::string::encoded_len(1u32, &reward.pubkey)
        } else {
            0
        }) + if reward.lamports != 0i64 {
            ::prost::encoding::int64::encoded_len(2u32, &reward.lamports)
        } else {
            0
        } + if reward.post_balance != 0u64 {
            ::prost::encoding::uint64::encoded_len(3u32, &reward.post_balance)
        } else {
            0
        } + if reward_type != RewardTypeProto::default() as i32 {
            ::prost::encoding::int32::encoded_len(4u32, &reward_type)
        } else {
            0
        } + if commission != b"" {
            prost_bytes_encoded_len(5u32, commission)
        } else {
            0
        }
    }

    const fn commission_to_str(commission: Option<u8>) -> &'static [u8] {
        const TABLE: [&[u8]; 256] = [
            b"0", b"1", b"2", b"3", b"4", b"5", b"6", b"7", b"8", b"9", b"10", b"11", b"12", b"13",
            b"14", b"15", b"16", b"17", b"18", b"19", b"20", b"21", b"22", b"23", b"24", b"25",
            b"26", b"27", b"28", b"29", b"30", b"31", b"32", b"33", b"34", b"35", b"36", b"37",
            b"38", b"39", b"40", b"41", b"42", b"43", b"44", b"45", b"46", b"47", b"48", b"49",
            b"50", b"51", b"52", b"53", b"54", b"55", b"56", b"57", b"58", b"59", b"60", b"61",
            b"62", b"63", b"64", b"65", b"66", b"67", b"68", b"69", b"70", b"71", b"72", b"73",
            b"74", b"75", b"76", b"77", b"78", b"79", b"80", b"81", b"82", b"83", b"84", b"85",
            b"86", b"87", b"88", b"89", b"90", b"91", b"92", b"93", b"94", b"95", b"96", b"97",
            b"98", b"99", b"100", b"101", b"102", b"103", b"104", b"105", b"106", b"107", b"108",
            b"109", b"110", b"111", b"112", b"113", b"114", b"115", b"116", b"117", b"118", b"119",
            b"120", b"121", b"122", b"123", b"124", b"125", b"126", b"127", b"128", b"129", b"130",
            b"131", b"132", b"133", b"134", b"135", b"136", b"137", b"138", b"139", b"140", b"141",
            b"142", b"143", b"144", b"145", b"146", b"147", b"148", b"149", b"150", b"151", b"152",
            b"153", b"154", b"155", b"156", b"157", b"158", b"159", b"160", b"161", b"162", b"163",
            b"164", b"165", b"166", b"167", b"168", b"169", b"170", b"171", b"172", b"173", b"174",
            b"175", b"176", b"177", b"178", b"179", b"180", b"181", b"182", b"183", b"184", b"185",
            b"186", b"187", b"188", b"189", b"190", b"191", b"192", b"193", b"194", b"195", b"196",
            b"197", b"198", b"199", b"200", b"201", b"202", b"203", b"204", b"205", b"206", b"207",
            b"208", b"209", b"210", b"211", b"212", b"213", b"214", b"215", b"216", b"217", b"218",
            b"219", b"220", b"221", b"222", b"223", b"224", b"225", b"226", b"227", b"228", b"229",
            b"230", b"231", b"232", b"233", b"234", b"235", b"236", b"237", b"238", b"239", b"240",
            b"241", b"242", b"243", b"244", b"245", b"246", b"247", b"248", b"249", b"250", b"251",
            b"252", b"253", b"254", b"255",
        ];
        if let Some(index) = commission {
            TABLE[index as usize]
        } else {
            &[]
        }
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

            let mut block_meta = MessageBlockMeta {
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
            };

            encode_decode_cmp(
                &["123"],
                MessageRef::block_meta(Arc::new(block_meta.clone())),
            );

            block_meta.num_partitions = Some(42);
            encode_decode_cmp(&["123"], MessageRef::block_meta(Arc::new(block_meta)));
        }
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
