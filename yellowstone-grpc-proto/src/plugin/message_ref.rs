use {
    super::{
        filter::{FilterAccountsDataSlice, FilterName},
        message::{
            MessageAccount, MessageAccountInfo, MessageBlockMeta, MessageEntry, MessageSlot,
            MessageTransaction, MessageTransactionInfo,
        },
    },
    crate::{
        convert_to,
        geyser::{
            subscribe_update::UpdateOneof, CommitmentLevel as CommitmentLevelProto,
            SubscribeUpdate, SubscribeUpdateAccount, SubscribeUpdateAccountInfo,
            SubscribeUpdateBlockMeta, SubscribeUpdateEntry, SubscribeUpdatePing,
            SubscribeUpdatePong, SubscribeUpdateSlot, SubscribeUpdateTransaction,
            SubscribeUpdateTransactionInfo, SubscribeUpdateTransactionStatus,
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
    solana_account_decoder::parse_token::UiTokenAmount,
    solana_sdk::{
        instruction::CompiledInstruction,
        message::{
            v0::{LoadedMessage, MessageAddressTableLookup},
            LegacyMessage, MessageHeader, SanitizedMessage,
        },
        transaction::SanitizedTransaction,
        transaction_context::TransactionReturnData,
    },
    solana_transaction_status::{
        InnerInstruction, InnerInstructions, Reward, TransactionStatusMeta, TransactionTokenBalance,
    },
    std::{borrow::Cow, ops::Range, sync::Arc},
};

#[inline]
fn prost_bytes_encode_raw(tag: u32, value: &[u8], buf: &mut impl BufMut) {
    encode_key(tag, WireType::LengthDelimited, buf);
    encode_varint(value.len() as u64, buf);
    buf.put(value);
}

#[inline]
pub fn prost_field_encoded_len(tag: u32, len: usize) -> usize {
    key_len(tag) + encoded_len_varint(len as u64) + len
}

#[inline]
pub fn prost_bytes_encoded_len(tag: u32, value: &[u8]) -> usize {
    prost_field_encoded_len(tag, value.len())
}

macro_rules! prost_message_repeated_encoded_len {
    ($tag:expr, $values:expr, $get_len:expr) => {{
        key_len($tag) * $values.len()
            + $values
                .iter()
                .map($get_len)
                .map(|len| encoded_len_varint(len as u64) + len)
                .sum::<usize>()
    }};
}

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
        prost_message_repeated_encoded_len!(1u32, self.filters, |filter| filter.as_ref().len())
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
    Account(MessageAccountRef),                     // 2
    Slot(MessageSlot),                              // 3
    Transaction(MessageTransactionRef),             // 4
    TransactionStatus(MessageTransactionStatusRef), // 10
    Block,                                          // 5
    Ping,                                           // 6
    Pong(MessageRefPong),                           // 9
    BlockMeta(Arc<MessageBlockMeta>),               // 7
    Entry(Arc<MessageEntry>),                       // 8
}

impl From<&MessageRef> for UpdateOneof {
    fn from(message: &MessageRef) -> Self {
        match message {
            MessageRef::Account(msg) => Self::Account(msg.into()),
            MessageRef::Slot(msg) => Self::Slot(msg.into()),
            MessageRef::Transaction(msg) => Self::Transaction(msg.into()),
            MessageRef::TransactionStatus(msg) => Self::TransactionStatus(msg.into()),
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
            MessageRef::Account(msg) => message::encode(2u32, msg, buf),
            MessageRef::Slot(msg) => message::encode(3u32, msg, buf),
            MessageRef::Transaction(msg) => message::encode(4u32, msg, buf),
            MessageRef::TransactionStatus(msg) => message::encode(10u32, msg, buf),
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
            MessageRef::Account(msg) => message::encoded_len(2u32, msg),
            MessageRef::Slot(msg) => message::encoded_len(3u32, msg),
            MessageRef::Transaction(msg) => message::encoded_len(4u32, msg),
            MessageRef::TransactionStatus(msg) => message::encoded_len(10u32, msg),
            MessageRef::Block => todo!(),
            MessageRef::Ping => key_len(6u32) + encoded_len_varint(0),
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
    pub fn account(message: &MessageAccount, data_slice: FilterAccountsDataSlice) -> Self {
        Self::Account(MessageAccountRef {
            slot: message.slot,
            account: Arc::clone(&message.account),
            is_startup: message.is_startup,
            data_slice,
        })
    }

    pub const fn slot(message: MessageSlot) -> Self {
        Self::Slot(message)
    }

    pub fn transaction(message: &MessageTransaction) -> Self {
        Self::Transaction(MessageTransactionRef {
            transaction: Arc::clone(&message.transaction),
            slot: message.slot,
        })
    }

    pub fn transaction_status(message: &MessageTransaction) -> Self {
        Self::TransactionStatus(MessageTransactionStatusRef {
            transaction: Arc::clone(&message.transaction),
            slot: message.slot,
        })
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

#[derive(Debug)]
pub struct MessageAccountRef {
    pub account: Arc<MessageAccountInfo>,
    pub slot: u64,
    pub is_startup: bool,
    pub data_slice: FilterAccountsDataSlice,
}

impl From<&MessageAccountRef> for SubscribeUpdateAccount {
    fn from(msg: &MessageAccountRef) -> Self {
        SubscribeUpdateAccount {
            account: Some(SubscribeUpdateAccountInfo {
                pubkey: msg.account.pubkey.as_ref().into(),
                lamports: msg.account.lamports,
                owner: msg.account.owner.as_ref().into(),
                executable: msg.account.executable,
                rent_epoch: msg.account.rent_epoch,
                data: MessageAccountRef::accout_data_slice(&msg.account, &msg.data_slice)
                    .into_owned(),
                write_version: msg.account.write_version,
                txn_signature: msg.account.txn_signature.map(|s| s.as_ref().into()),
            }),
            slot: msg.slot,
            is_startup: msg.is_startup,
        }
    }
}

impl prost::Message for MessageAccountRef {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        Self::account_encode(&self.account, &self.data_slice, 1u32, buf);
        if self.slot != 0u64 {
            ::prost::encoding::uint64::encode(2u32, &self.slot, buf);
        }
        if self.is_startup {
            ::prost::encoding::bool::encode(3u32, &self.is_startup, buf);
        }
    }

    fn encoded_len(&self) -> usize {
        prost_field_encoded_len(
            1u32,
            Self::account_encoded_len(self.account.as_ref(), &self.data_slice),
        ) + if self.slot != 0u64 {
            ::prost::encoding::uint64::encoded_len(2u32, &self.slot)
        } else {
            0
        } + if self.is_startup {
            ::prost::encoding::bool::encoded_len(3u32, &self.is_startup)
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

impl MessageAccountRef {
    fn accout_data_slice<'a>(
        account: &'a MessageAccountInfo,
        data_slice: &'a FilterAccountsDataSlice,
    ) -> Cow<'a, Vec<u8>> {
        if data_slice.is_empty() {
            Cow::Borrowed(&account.data)
        } else {
            let mut data = Vec::with_capacity(data_slice.iter().map(|s| s.end - s.start).sum());
            for slice in data_slice.iter() {
                if account.data.len() >= slice.end {
                    data.extend_from_slice(&account.data[slice.start..slice.end]);
                }
            }
            Cow::Owned(data)
        }
    }

    fn account_data_slice_len(
        account: &MessageAccountInfo,
        data_slice: &FilterAccountsDataSlice,
    ) -> usize {
        if data_slice.is_empty() {
            account.data.len()
        } else {
            let mut len = 0;
            for slice in data_slice.iter() {
                if account.data.len() >= slice.end {
                    len += account.data[slice.start..slice.end].len();
                }
            }
            len
        }
    }

    fn account_encode(
        account: &MessageAccountInfo,
        data_slice: &FilterAccountsDataSlice,
        tag: u32,
        buf: &mut impl BufMut,
    ) {
        encode_key(tag, WireType::LengthDelimited, buf);
        encode_varint(Self::account_encoded_len(account, data_slice) as u64, buf);

        prost_bytes_encode_raw(1u32, account.pubkey.as_ref(), buf);
        if account.lamports != 0u64 {
            ::prost::encoding::uint64::encode(2u32, &account.lamports, buf);
        }
        prost_bytes_encode_raw(3u32, account.owner.as_ref(), buf);
        if account.executable {
            ::prost::encoding::bool::encode(4u32, &account.executable, buf);
        }
        if account.rent_epoch != 0u64 {
            ::prost::encoding::uint64::encode(5u32, &account.rent_epoch, buf);
        }
        let data = Self::accout_data_slice(account, data_slice);
        if !data.is_empty() {
            prost_bytes_encode_raw(6u32, data.as_ref(), buf);
        }
        if account.write_version != 0u64 {
            ::prost::encoding::uint64::encode(7u32, &account.write_version, buf);
        }
        if let Some(value) = &account.txn_signature {
            prost_bytes_encode_raw(8u32, value.as_ref(), buf);
        }
    }

    fn account_encoded_len(
        account: &MessageAccountInfo,
        data_slice: &FilterAccountsDataSlice,
    ) -> usize {
        let data_len = Self::account_data_slice_len(account, data_slice);

        prost_bytes_encoded_len(1u32, account.pubkey.as_ref())
            + if account.lamports != 0u64 {
                ::prost::encoding::uint64::encoded_len(2u32, &account.lamports)
            } else {
                0
            }
            + prost_bytes_encoded_len(3u32, account.owner.as_ref())
            + if account.executable {
                ::prost::encoding::bool::encoded_len(4u32, &account.executable)
            } else {
                0
            }
            + if account.rent_epoch != 0u64 {
                ::prost::encoding::uint64::encoded_len(5u32, &account.rent_epoch)
            } else {
                0
            }
            + if data_len != 0 {
                prost_field_encoded_len(6u32, data_len)
            } else {
                0
            }
            + if account.write_version != 0u64 {
                ::prost::encoding::uint64::encoded_len(7u32, &account.write_version)
            } else {
                0
            }
            + account
                .txn_signature
                .map_or(0, |sig| prost_bytes_encoded_len(8u32, sig.as_ref()))
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
pub struct MessageTransactionRef {
    pub transaction: Arc<MessageTransactionInfo>,
    pub slot: u64,
}

impl From<&MessageTransactionRef> for SubscribeUpdateTransaction {
    fn from(msg: &MessageTransactionRef) -> Self {
        Self {
            transaction: Some(SubscribeUpdateTransactionInfo {
                signature: msg.transaction.signature.as_ref().into(),
                is_vote: msg.transaction.is_vote,
                transaction: Some(convert_to::create_transaction(&msg.transaction.transaction)),
                meta: Some(convert_to::create_transaction_meta(&msg.transaction.meta)),
                index: msg.transaction.index as u64,
            }),
            slot: msg.slot,
        }
    }
}

impl prost::Message for MessageTransactionRef {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        Self::tx_meta_encode_raw(1u32, &self.transaction, buf);
        if self.slot != 0u64 {
            ::prost::encoding::uint64::encode(2u32, &self.slot, buf);
        }
    }

    fn encoded_len(&self) -> usize {
        prost_field_encoded_len(1u32, Self::tx_meta_encoded_len(&self.transaction))
            + if self.slot != 0u64 {
                ::prost::encoding::uint64::encoded_len(2u32, &self.slot)
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

impl MessageTransactionRef {
    fn tx_meta_encode_raw(tag: u32, tx: &MessageTransactionInfo, buf: &mut impl BufMut) {
        encode_key(tag, WireType::LengthDelimited, buf);
        encode_varint(Self::tx_meta_encoded_len(tx) as u64, buf);

        let index = tx.index as u64;

        prost_bytes_encode_raw(1u32, tx.signature.as_ref(), buf);
        if tx.is_vote {
            ::prost::encoding::bool::encode(2u32, &tx.is_vote, buf);
        }
        Self::tx_encode_raw(3u32, &tx.transaction, buf);
        Self::meta_encode_raw(4u32, &tx.meta, buf);
        if index != 0u64 {
            ::prost::encoding::uint64::encode(5u32, &index, buf);
        }
    }

    fn tx_encode_raw(tag: u32, tx: &SanitizedTransaction, buf: &mut impl BufMut) {
        encode_key(tag, WireType::LengthDelimited, buf);
        encode_varint(Self::tx_encoded_len(tx) as u64, buf);

        for sig in tx.signatures() {
            prost_bytes_encode_raw(1u32, sig.as_ref(), buf);
        }
        Self::message_encode_raw(2u32, tx.message(), buf);
    }

    fn message_encode_raw(tag: u32, message: &SanitizedMessage, buf: &mut impl BufMut) {
        encode_key(tag, WireType::LengthDelimited, buf);
        encode_varint(Self::message_encoded_len(message) as u64, buf);

        let (header, account_keys, recent_blockhash, cixs, versioned, atls) = match message {
            SanitizedMessage::Legacy(LegacyMessage { message, .. }) => (
                message.header,
                &message.account_keys,
                &message.recent_blockhash,
                &message.instructions,
                false,
                None,
            ),
            SanitizedMessage::V0(LoadedMessage { message, .. }) => (
                message.header,
                &message.account_keys,
                &message.recent_blockhash,
                &message.instructions,
                true,
                Some(&message.address_table_lookups),
            ),
        };

        Self::header_encode_raw(1u32, header, buf);
        for pubkey in account_keys {
            prost_bytes_encode_raw(2u32, pubkey.as_ref(), buf);
        }
        prost_bytes_encode_raw(3u32, recent_blockhash.as_ref(), buf);
        for cix in cixs {
            Self::cix_encode_raw(4u32, cix, buf);
        }
        if versioned {
            ::prost::encoding::bool::encode(5u32, &versioned, buf);
        }
        if let Some(atls) = atls {
            for atl in atls {
                Self::atl_encode_raw(6u32, atl, buf);
            }
        }
    }

    fn header_encode_raw(tag: u32, header: MessageHeader, buf: &mut impl BufMut) {
        encode_key(tag, WireType::LengthDelimited, buf);
        encode_varint(Self::header_encoded_len(header) as u64, buf);

        let num_required_signatures = header.num_required_signatures as u32;
        let num_readonly_signed_accounts = header.num_readonly_signed_accounts as u32;
        let num_readonly_unsigned_accounts = header.num_readonly_unsigned_accounts as u32;

        if num_required_signatures != 0u32 {
            ::prost::encoding::uint32::encode(1u32, &num_required_signatures, buf);
        }
        if num_readonly_signed_accounts != 0u32 {
            ::prost::encoding::uint32::encode(2u32, &num_readonly_signed_accounts, buf);
        }
        if num_readonly_unsigned_accounts != 0u32 {
            ::prost::encoding::uint32::encode(3u32, &num_readonly_unsigned_accounts, buf);
        }
    }

    fn cix_encode_raw(tag: u32, cix: &CompiledInstruction, buf: &mut impl BufMut) {
        encode_key(tag, WireType::LengthDelimited, buf);
        encode_varint(Self::cix_encoded_len(cix) as u64, buf);

        let program_id_index = cix.program_id_index as u32;

        if program_id_index != 0u32 {
            ::prost::encoding::uint32::encode(1u32, &program_id_index, buf);
        }
        if !cix.accounts.is_empty() {
            ::prost::encoding::bytes::encode(2u32, &cix.accounts, buf);
        }
        if !cix.data.is_empty() {
            ::prost::encoding::bytes::encode(3u32, &cix.data, buf);
        }
    }

    fn atl_encode_raw(tag: u32, atl: &MessageAddressTableLookup, buf: &mut impl BufMut) {
        encode_key(tag, WireType::LengthDelimited, buf);
        encode_varint(Self::atl_encoded_len(atl) as u64, buf);

        prost_bytes_encode_raw(1u32, atl.account_key.as_ref(), buf);
        if !atl.writable_indexes.is_empty() {
            prost_bytes_encode_raw(2u32, atl.writable_indexes.as_ref(), buf);
        }
        if !atl.readonly_indexes.is_empty() {
            prost_bytes_encode_raw(3u32, atl.readonly_indexes.as_ref(), buf);
        }
    }

    fn meta_encode_raw(tag: u32, meta: &TransactionStatusMeta, buf: &mut impl BufMut) {
        encode_key(tag, WireType::LengthDelimited, buf);
        encode_varint(Self::meta_encoded_len(meta) as u64, buf);

        let err = convert_to::create_transaction_error(&meta.status);

        if let Some(msg) = err {
            ::prost::encoding::message::encode(1u32, &msg, buf);
        }
        if meta.fee != 0u64 {
            ::prost::encoding::uint64::encode(2u32, &meta.fee, buf);
        }
        ::prost::encoding::uint64::encode_packed(3u32, &meta.pre_balances, buf);
        ::prost::encoding::uint64::encode_packed(4u32, &meta.post_balances, buf);
        if let Some(vec) = &meta.inner_instructions {
            for ixs in vec {
                Self::ixs_encode_raw(5u32, ixs, buf);
            }
        }
        if let Some(log_messages) = &meta.log_messages {
            ::prost::encoding::string::encode_repeated(6u32, log_messages, buf);
        }
        if let Some(vec) = &meta.pre_token_balances {
            for pre_token_balances in vec {
                Self::token_balance_encode_raw(7u32, pre_token_balances, buf);
            }
        }
        if let Some(vec) = &meta.post_token_balances {
            for post_token_balances in vec {
                Self::token_balance_encode_raw(8u32, post_token_balances, buf);
            }
        }
        if let Some(vec) = &meta.rewards {
            for reward in vec {
                MessageBlockMeta::reward_encode_raw(9u32, reward, buf);
            }
        }
        if meta.inner_instructions.is_none() {
            ::prost::encoding::bool::encode(10u32, &true, buf);
        }
        if meta.log_messages.is_none() {
            ::prost::encoding::bool::encode(11u32, &true, buf);
        }
        for pubkey in meta.loaded_addresses.writable.iter() {
            prost_bytes_encode_raw(12u32, pubkey.as_ref(), buf);
        }
        for pubkey in meta.loaded_addresses.readonly.iter() {
            prost_bytes_encode_raw(13u32, pubkey.as_ref(), buf);
        }
        if let Some(rd) = &meta.return_data {
            Self::return_data_encode_raw(14u32, rd, buf);
        }
        if meta.return_data.is_none() {
            ::prost::encoding::bool::encode(15u32, &true, buf);
        }
        if let Some(value) = &meta.compute_units_consumed {
            ::prost::encoding::uint64::encode(16u32, value, buf);
        }
    }

    fn ixs_encode_raw(tag: u32, ixs: &InnerInstructions, buf: &mut impl BufMut) {
        encode_key(tag, WireType::LengthDelimited, buf);
        encode_varint(Self::ixs_encoded_len(ixs) as u64, buf);

        let index = ixs.index as u32;

        if index != 0u32 {
            ::prost::encoding::uint32::encode(1u32, &index, buf);
        }
        for ix in ixs.instructions.iter() {
            Self::ix_encode_raw(2u32, ix, buf);
        }
    }

    fn ix_encode_raw(tag: u32, ix: &InnerInstruction, buf: &mut impl BufMut) {
        encode_key(tag, WireType::LengthDelimited, buf);
        encode_varint(Self::ix_encoded_len(ix) as u64, buf);

        let program_id_index = ix.instruction.program_id_index as u32;

        if program_id_index != 0u32 {
            ::prost::encoding::uint32::encode(1u32, &program_id_index, buf);
        }
        if !ix.instruction.accounts.is_empty() {
            prost_bytes_encode_raw(2u32, &ix.instruction.accounts, buf);
        }
        if !ix.instruction.data.is_empty() {
            prost_bytes_encode_raw(3u32, &ix.instruction.data, buf);
        }
        if let Some(value) = &ix.stack_height {
            ::prost::encoding::uint32::encode(4u32, value, buf);
        }
    }

    fn token_balance_encode_raw(
        tag: u32,
        balance: &TransactionTokenBalance,
        buf: &mut impl BufMut,
    ) {
        encode_key(tag, WireType::LengthDelimited, buf);
        encode_varint(Self::token_balance_encoded_len(balance) as u64, buf);

        let account_index = balance.account_index as u32;

        if account_index != 0u32 {
            ::prost::encoding::uint32::encode(1u32, &account_index, buf);
        }
        if !balance.mint.is_empty() {
            ::prost::encoding::string::encode(2u32, &balance.mint, buf);
        }
        Self::ui_token_amount_encode_raw(3u32, &balance.ui_token_amount, buf);
        if !balance.owner.is_empty() {
            ::prost::encoding::string::encode(4u32, &balance.owner, buf);
        }
        if !balance.program_id.is_empty() {
            ::prost::encoding::string::encode(5u32, &balance.program_id, buf);
        }
    }

    fn ui_token_amount_encode_raw(tag: u32, amount: &UiTokenAmount, buf: &mut impl BufMut) {
        let ui_amount = amount.ui_amount.unwrap_or_default();
        let decimals = amount.decimals as u32;

        encode_key(tag, WireType::LengthDelimited, buf);
        encode_varint(Self::ui_token_amount_encoded_len(amount) as u64, buf);

        if ui_amount != 0f64 {
            ::prost::encoding::double::encode(1u32, &ui_amount, buf);
        }
        if decimals != 0u32 {
            ::prost::encoding::uint32::encode(2u32, &decimals, buf);
        }
        if !amount.amount.is_empty() {
            ::prost::encoding::string::encode(3u32, &amount.amount, buf);
        }
        if !amount.ui_amount_string.is_empty() {
            ::prost::encoding::string::encode(4u32, &amount.ui_amount_string, buf);
        }
    }

    fn return_data_encode_raw(
        tag: u32,
        return_data: &TransactionReturnData,
        buf: &mut impl BufMut,
    ) {
        encode_key(tag, WireType::LengthDelimited, buf);
        encode_varint(Self::return_data_encoded_len(return_data) as u64, buf);

        prost_bytes_encode_raw(1u32, return_data.program_id.as_ref(), buf);
        if !return_data.data.is_empty() {
            ::prost::encoding::bytes::encode(2u32, &return_data.data, buf);
        }
    }

    fn tx_meta_encoded_len(tx: &MessageTransactionInfo) -> usize {
        let index = tx.index as u64;

        prost_bytes_encoded_len(1u32, tx.signature.as_ref())
            + if tx.is_vote {
                ::prost::encoding::bool::encoded_len(2u32, &tx.is_vote)
            } else {
                0
            }
            + prost_field_encoded_len(3u32, Self::tx_encoded_len(&tx.transaction))
            + prost_field_encoded_len(4u32, Self::meta_encoded_len(&tx.meta))
            + if index != 0u64 {
                ::prost::encoding::uint64::encoded_len(5u32, &index)
            } else {
                0
            }
    }

    fn tx_encoded_len(tx: &SanitizedTransaction) -> usize {
        prost_message_repeated_encoded_len!(1u32, tx.signatures(), |sig| sig.as_ref().len())
            + prost_field_encoded_len(2u32, Self::message_encoded_len(tx.message()))
    }

    fn message_encoded_len(message: &SanitizedMessage) -> usize {
        let (header, account_keys, recent_blockhash, cixs, versioned, atls) = match message {
            SanitizedMessage::Legacy(LegacyMessage { message, .. }) => (
                message.header,
                &message.account_keys,
                &message.recent_blockhash,
                &message.instructions,
                false,
                None,
            ),
            SanitizedMessage::V0(LoadedMessage { message, .. }) => (
                message.header,
                &message.account_keys,
                &message.recent_blockhash,
                &message.instructions,
                true,
                Some(&message.address_table_lookups),
            ),
        };

        prost_field_encoded_len(1u32, Self::header_encoded_len(header))
            + prost_message_repeated_encoded_len!(2u32, account_keys, |key| key.as_ref().len())
            + prost_bytes_encoded_len(3u32, recent_blockhash.as_ref())
            + prost_message_repeated_encoded_len!(4u32, cixs, Self::cix_encoded_len)
            + if versioned {
                ::prost::encoding::bool::encoded_len(5u32, &versioned)
            } else {
                0
            }
            + if let Some(atls) = atls {
                prost_message_repeated_encoded_len!(6u32, atls, Self::atl_encoded_len)
            } else {
                0
            }
    }

    fn header_encoded_len(header: MessageHeader) -> usize {
        let num_required_signatures = header.num_required_signatures as u32;
        let num_readonly_signed_accounts = header.num_readonly_signed_accounts as u32;
        let num_readonly_unsigned_accounts = header.num_readonly_unsigned_accounts as u32;

        (if num_required_signatures != 0u32 {
            ::prost::encoding::uint32::encoded_len(1u32, &num_required_signatures)
        } else {
            0
        }) + if num_readonly_signed_accounts != 0u32 {
            ::prost::encoding::uint32::encoded_len(2u32, &num_readonly_signed_accounts)
        } else {
            0
        } + if num_readonly_unsigned_accounts != 0u32 {
            ::prost::encoding::uint32::encoded_len(3u32, &num_readonly_unsigned_accounts)
        } else {
            0
        }
    }

    fn cix_encoded_len(cix: &CompiledInstruction) -> usize {
        let program_id_index = cix.program_id_index as u32;

        (if program_id_index != 0u32 {
            ::prost::encoding::uint32::encoded_len(1u32, &program_id_index)
        } else {
            0
        }) + if !cix.accounts.is_empty() {
            prost_bytes_encoded_len(2u32, &cix.accounts)
        } else {
            0
        } + if !cix.data.is_empty() {
            prost_bytes_encoded_len(3u32, &cix.data)
        } else {
            0
        }
    }

    fn atl_encoded_len(atl: &MessageAddressTableLookup) -> usize {
        prost_bytes_encoded_len(1u32, atl.account_key.as_ref())
            + if !atl.writable_indexes.is_empty() {
                prost_bytes_encoded_len(2u32, &atl.writable_indexes)
            } else {
                0
            }
            + if !atl.readonly_indexes.is_empty() {
                prost_bytes_encoded_len(3u32, &atl.readonly_indexes)
            } else {
                0
            }
    }

    fn meta_encoded_len(meta: &TransactionStatusMeta) -> usize {
        convert_to::create_transaction_error(&meta.status)
            .map_or(0, |msg| ::prost::encoding::message::encoded_len(1u32, &msg))
            + if meta.fee != 0u64 {
                ::prost::encoding::uint64::encoded_len(2u32, &meta.fee)
            } else {
                0
            }
            + ::prost::encoding::uint64::encoded_len_packed(3u32, &meta.pre_balances)
            + ::prost::encoding::uint64::encoded_len_packed(4u32, &meta.post_balances)
            + if let Some(ixs) = &meta.inner_instructions {
                prost_message_repeated_encoded_len!(5u32, ixs, Self::ixs_encoded_len)
            } else {
                0
            }
            + if let Some(log_messages) = &meta.log_messages {
                ::prost::encoding::string::encoded_len_repeated(6u32, log_messages)
            } else {
                0
            }
            + if let Some(pre_token_balances) = &meta.pre_token_balances {
                prost_message_repeated_encoded_len!(
                    7u32,
                    pre_token_balances,
                    Self::token_balance_encoded_len
                )
            } else {
                0
            }
            + if let Some(post_token_balances) = &meta.post_token_balances {
                prost_message_repeated_encoded_len!(
                    8u32,
                    post_token_balances,
                    Self::token_balance_encoded_len
                )
            } else {
                0
            }
            + if let Some(rewards) = &meta.rewards {
                prost_message_repeated_encoded_len!(
                    9u32,
                    rewards,
                    MessageBlockMeta::reward_encoded_len
                )
            } else {
                0
            }
            + if meta.inner_instructions.is_none() {
                ::prost::encoding::bool::encoded_len(10u32, &true)
            } else {
                0
            }
            + if meta.log_messages.is_none() {
                ::prost::encoding::bool::encoded_len(11u32, &true)
            } else {
                0
            }
            + prost_message_repeated_encoded_len!(12u32, meta.loaded_addresses.writable, |pk| pk
                .as_ref()
                .len())
            + prost_message_repeated_encoded_len!(13u32, meta.loaded_addresses.readonly, |pk| pk
                .as_ref()
                .len())
            + if let Some(rd) = &meta.return_data {
                prost_field_encoded_len(14u32, Self::return_data_encoded_len(rd))
            } else {
                0
            }
            + if meta.return_data.is_none() {
                ::prost::encoding::bool::encoded_len(15u32, &true)
            } else {
                0
            }
            + meta.compute_units_consumed.as_ref().map_or(0, |value| {
                ::prost::encoding::uint64::encoded_len(16u32, value)
            })
    }

    fn ixs_encoded_len(ixs: &InnerInstructions) -> usize {
        let index = ixs.index as u32;

        (if index != 0u32 {
            ::prost::encoding::uint32::encoded_len(1u32, &index)
        } else {
            0
        }) + prost_message_repeated_encoded_len!(2u32, &ixs.instructions, Self::ix_encoded_len)
    }

    fn ix_encoded_len(ix: &InnerInstruction) -> usize {
        let program_id_index = ix.instruction.program_id_index as u32;

        (if program_id_index != 0u32 {
            ::prost::encoding::uint32::encoded_len(1u32, &program_id_index)
        } else {
            0
        }) + if !ix.instruction.accounts.is_empty() {
            prost_bytes_encoded_len(2u32, &ix.instruction.accounts)
        } else {
            0
        } + if !ix.instruction.data.is_empty() {
            prost_bytes_encoded_len(3u32, &ix.instruction.data)
        } else {
            0
        } + ix.stack_height.map_or(0, |value| {
            ::prost::encoding::uint32::encoded_len(4u32, &value)
        })
    }

    fn token_balance_encoded_len(balance: &TransactionTokenBalance) -> usize {
        let account_index = balance.account_index as u32;

        (if account_index != 0u32 {
            ::prost::encoding::uint32::encoded_len(1u32, &account_index)
        } else {
            0
        }) + if !balance.mint.is_empty() {
            ::prost::encoding::string::encoded_len(2u32, &balance.mint)
        } else {
            0
        } + prost_field_encoded_len(
            3u32,
            Self::ui_token_amount_encoded_len(&balance.ui_token_amount),
        ) + if !balance.owner.is_empty() {
            ::prost::encoding::string::encoded_len(4u32, &balance.owner)
        } else {
            0
        } + if !balance.program_id.is_empty() {
            ::prost::encoding::string::encoded_len(5u32, &balance.program_id)
        } else {
            0
        }
    }

    fn ui_token_amount_encoded_len(amount: &UiTokenAmount) -> usize {
        let ui_amount = amount.ui_amount.unwrap_or_default();
        let decimals = amount.decimals as u32;

        (if ui_amount != 0f64 {
            ::prost::encoding::double::encoded_len(1u32, &ui_amount)
        } else {
            0
        }) + if decimals != 0u32 {
            ::prost::encoding::uint32::encoded_len(2u32, &decimals)
        } else {
            0
        } + if !amount.amount.is_empty() {
            ::prost::encoding::string::encoded_len(3u32, &amount.amount)
        } else {
            0
        } + if !amount.ui_amount_string.is_empty() {
            ::prost::encoding::string::encoded_len(4u32, &amount.ui_amount_string)
        } else {
            0
        }
    }

    fn return_data_encoded_len(return_data: &TransactionReturnData) -> usize {
        prost_bytes_encoded_len(1u32, return_data.program_id.as_ref())
            + if !return_data.data.is_empty() {
                ::prost::encoding::bytes::encoded_len(2u32, &return_data.data)
            } else {
                0
            }
    }
}

#[derive(Debug)]
pub struct MessageTransactionStatusRef {
    pub transaction: Arc<MessageTransactionInfo>,
    pub slot: u64,
}

impl From<&MessageTransactionStatusRef> for SubscribeUpdateTransactionStatus {
    fn from(msg: &MessageTransactionStatusRef) -> Self {
        Self {
            slot: msg.slot,
            signature: msg.transaction.signature.as_ref().to_vec(),
            is_vote: msg.transaction.is_vote,
            index: msg.transaction.index as u64,
            err: convert_to::create_transaction_error(&msg.transaction.meta.status),
        }
    }
}

impl prost::Message for MessageTransactionStatusRef {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        if self.slot != 0u64 {
            ::prost::encoding::uint64::encode(1u32, &self.slot, buf);
        }
        let tx = &self.transaction;
        prost_bytes_encode_raw(2u32, tx.signature.as_ref(), buf);
        if tx.is_vote {
            ::prost::encoding::bool::encode(3u32, &tx.is_vote, buf);
        }
        let index = tx.index as u64;
        if index != 0u64 {
            ::prost::encoding::uint64::encode(4u32, &index, buf);
        }
        let err = convert_to::create_transaction_error(&tx.meta.status);
        if let Some(msg) = err {
            ::prost::encoding::message::encode(5u32, &msg, buf);
        }
    }

    fn encoded_len(&self) -> usize {
        let tx = &self.transaction;
        let index = tx.index as u64;

        (if self.slot != 0u64 {
            ::prost::encoding::uint64::encoded_len(1u32, &self.slot)
        } else {
            0
        }) + prost_bytes_encoded_len(2u32, tx.signature.as_ref())
            + if tx.is_vote {
                ::prost::encoding::bool::encoded_len(3u32, &tx.is_vote)
            } else {
                0
            }
            + if index != 0u64 {
                ::prost::encoding::uint64::encoded_len(4u32, &index)
            } else {
                0
            }
            + convert_to::create_transaction_error(&tx.meta.status)
                .map_or(0, |msg| ::prost::encoding::message::encoded_len(5u32, &msg))
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
        self.rewards_encode_raw(3u32, buf);
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
        } + prost_field_encoded_len(3u32, self.rewards_encoded_len())
            + self
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
    fn rewards_encode_raw(&self, tag: u32, buf: &mut impl BufMut) {
        encode_key(tag, WireType::LengthDelimited, buf);
        encode_varint(self.rewards_encoded_len() as u64, buf);

        for reward in &self.rewards {
            Self::reward_encode_raw(1u32, reward, buf);
        }
        if let Some(num_partitions) = self.num_partitions {
            let msg = convert_to::create_num_partitions(num_partitions);
            ::prost::encoding::message::encode(2u32, &msg, buf);
        }
    }

    fn reward_encode_raw(tag: u32, reward: &Reward, buf: &mut impl BufMut) {
        encode_key(tag, WireType::LengthDelimited, buf);
        encode_varint(Self::reward_encoded_len(reward) as u64, buf);

        if !reward.pubkey.is_empty() {
            ::prost::encoding::string::encode(1u32, &reward.pubkey, buf);
        }
        if reward.lamports != 0i64 {
            ::prost::encoding::int64::encode(2u32, &reward.lamports, buf);
        }
        if reward.post_balance != 0u64 {
            ::prost::encoding::uint64::encode(3u32, &reward.post_balance, buf);
        }
        let reward_type = convert_to::create_reward_type(reward.reward_type) as i32;
        if reward_type != RewardTypeProto::default() as i32 {
            ::prost::encoding::int32::encode(4u32, &reward_type, buf);
        }
        let commission = Self::commission_to_str(reward.commission);
        if commission != b"" {
            prost_bytes_encode_raw(5u32, commission, buf);
        }
    }

    fn rewards_encoded_len(&self) -> usize {
        prost_message_repeated_encoded_len!(1u32, self.rewards, Self::reward_encoded_len)
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
        } + if !commission.is_empty() {
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
        prost_bytes_encode_raw(4u32, &self.hash, buf);
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
        } + prost_bytes_encoded_len(4u32, &self.hash)
            + if self.executed_transaction_count != 0u64 {
                ::prost::encoding::uint64::encoded_len(5u32, &self.executed_transaction_count)
            } else {
                0
            }
            + if self.starting_transaction_index != 0u64 {
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

#[cfg(any(test, feature = "plugin-bench"))]
pub mod tests {
    #![cfg_attr(feature = "plugin-bench", allow(dead_code))]
    #![cfg_attr(feature = "plugin-bench", allow(unused_imports))]
    use {
        super::*,
        crate::plugin::message::CommitmentLevel,
        prost::Message as _,
        prost_011::Message as _,
        solana_sdk::{
            message::SimpleAddressLoader,
            pubkey::Pubkey,
            signature::Signature,
            transaction::{MessageHash, SanitizedTransaction},
        },
        solana_storage_proto::convert::generated,
        solana_transaction_status::{ConfirmedBlock, TransactionWithStatusMeta},
        std::{collections::HashSet, fs, str::FromStr},
    };

    pub fn create_message_filters(names: &[&str]) -> MessageFilters {
        let mut filters = MessageFilters::new();
        for name in names {
            filters.push(FilterName::new(*name));
        }
        filters
    }

    fn create_account_data_slice() -> Vec<FilterAccountsDataSlice> {
        let mut data_slice1 = FilterAccountsDataSlice::new();
        data_slice1.push(Range { start: 0, end: 0 });

        let mut data_slice2 = FilterAccountsDataSlice::new();
        data_slice2.push(Range { start: 2, end: 3 });

        let mut data_slice3 = FilterAccountsDataSlice::new();
        data_slice3.push(Range { start: 1, end: 3 });
        data_slice3.push(Range { start: 5, end: 10 });

        vec![
            FilterAccountsDataSlice::new(),
            data_slice1,
            data_slice2,
            data_slice3,
        ]
    }

    pub fn create_accounts() -> Vec<(MessageAccount, FilterAccountsDataSlice)> {
        let pubkey = Pubkey::from_str("28Dncoh8nmzXYEGLUcBA5SUw5WDwDBn15uUCwrWBbyuu").unwrap();
        let owner = Pubkey::from_str("5jrPJWVGrFvQ2V9wRZC3kHEZhxo9pmMir15x73oHT6mn").unwrap();
        let txn_signature = Signature::from_str("4V36qYhukXcLFuvhZaudSoJpPaFNB7d5RqYKjL2xiSKrxaBfEajqqL4X6viZkEvHJ8XcTJsqVjZxFegxhN7EC9V5").unwrap();

        let mut accounts = vec![];
        for lamports in [0, 8123] {
            for executable in [true, false] {
                for rent_epoch in [0, 4242] {
                    for data in [
                        vec![],
                        [42; 165].to_vec(),
                        [42; 1024].to_vec(),
                        [42; 2 * 1024 * 1024].to_vec(),
                    ] {
                        for write_version in [0, 1] {
                            for txn_signature in [None, Some(txn_signature)] {
                                accounts.push(Arc::new(MessageAccountInfo {
                                    pubkey,
                                    lamports,
                                    owner,
                                    executable,
                                    rent_epoch,
                                    data: data.clone(),
                                    write_version,
                                    txn_signature,
                                }));
                            }
                        }
                    }
                }
            }
        }

        let mut vec = vec![];
        for account in accounts {
            for slot in [0, 42] {
                for is_startup in [true, false] {
                    for data_slice in create_account_data_slice() {
                        let msg = MessageAccount {
                            account: Arc::clone(&account),
                            slot,
                            is_startup,
                        };
                        vec.push((msg, data_slice));
                    }
                }
            }
        }
        vec
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
        let update = SubscribeUpdate::from(&msg);
        //
        // let len1 = msg.encoded_len();
        // let len2 = update.encoded_len();
        // if len1 != len2 {
        //     println!("my {len1} vs proto {len2}");
        // }
        assert_eq!(msg.encoded_len(), update.encoded_len());
        assert_eq!(
            SubscribeUpdate::decode(msg.encode_to_vec().as_slice()).expect("failed to decode"),
            update
        );
    }

    #[test]
    fn test_message_account() {
        for (msg, data_slice) in create_accounts() {
            encode_decode_cmp(&["123"], MessageRef::account(&msg, data_slice));
        }
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

            let slot = block.parent_slot + 1;
            let mut block_meta = MessageBlockMeta {
                parent_slot: block.parent_slot,
                slot,
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

            let transactions_info = block
                .transactions
                .iter()
                .enumerate()
                .map(|(index, tx)| {
                    let TransactionWithStatusMeta::Complete(tx) = tx else {
                        panic!("tx with missed meta");
                    };
                    let transaction = SanitizedTransaction::try_create(
                        tx.transaction.clone(),
                        MessageHash::Compute,
                        None,
                        SimpleAddressLoader::Disabled,
                        &HashSet::new(),
                    )
                    .expect("failed to create tx");
                    MessageTransactionInfo {
                        signature: tx.transaction.signatures[0],
                        is_vote: true,
                        transaction,
                        meta: tx.meta.clone(),
                        index,
                    }
                })
                .map(Arc::new)
                .collect::<Vec<_>>();

            for tx in transactions_info.iter() {
                let msg = MessageTransaction {
                    transaction: Arc::clone(tx),
                    slot: 42,
                };
                encode_decode_cmp(&["123"], MessageRef::transaction(&msg));
                encode_decode_cmp(&["123"], MessageRef::transaction_status(&msg));
            }
        }
    }

    // benches
    pub fn build_subscribe_update(
        filters: &MessageFilters,
        update: UpdateOneof,
    ) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: filters.iter().map(|f| f.as_ref().to_owned()).collect(),
            update_oneof: Some(update),
        }
    }

    pub fn build_subscribe_update_account_proto(
        message: &MessageAccountInfo,
        data_slice: &FilterAccountsDataSlice,
    ) -> SubscribeUpdateAccountInfo {
        let data = if data_slice.is_empty() {
            message.data.clone()
        } else {
            let mut data = Vec::with_capacity(data_slice.iter().map(|s| s.end - s.start).sum());
            for slice in data_slice {
                if message.data.len() >= slice.end {
                    data.extend_from_slice(&message.data[slice.start..slice.end]);
                }
            }
            data
        };
        SubscribeUpdateAccountInfo {
            pubkey: message.pubkey.as_ref().into(),
            lamports: message.lamports,
            owner: message.owner.as_ref().into(),
            executable: message.executable,
            rent_epoch: message.rent_epoch,
            data,
            write_version: message.write_version,
            txn_signature: message.txn_signature.map(|s| s.as_ref().into()),
        }
    }

    pub fn build_subscribe_update_account(
        message: &MessageAccount,
        data_slice: &FilterAccountsDataSlice,
    ) -> UpdateOneof {
        UpdateOneof::Account(SubscribeUpdateAccount {
            account: Some(build_subscribe_update_account_proto(
                &message.account,
                data_slice,
            )),
            slot: message.slot,
            is_startup: message.is_startup,
        })
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
