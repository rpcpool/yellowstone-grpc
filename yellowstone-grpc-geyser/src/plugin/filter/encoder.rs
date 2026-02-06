use {
    crate::plugin::{
        filter::message::{prost_bytes_encode_raw, prost_bytes_encoded_len},
        message::{MessageAccountInfo, MessageTransactionInfo},
    },
    bytes::Bytes,
};

pub struct TransactionEncoder;

impl TransactionEncoder {
    pub fn pre_encode(tx: &mut MessageTransactionInfo) {
        let len = Self::encoded_len(tx);
        let mut buf = Vec::with_capacity(len);
        Self::encode_raw(tx, &mut buf);
        tx.pre_encoded = Some(Bytes::from(buf));
    }

    fn encode_raw(tx: &MessageTransactionInfo, buf: &mut impl bytes::BufMut) {
        use prost::encoding::{encode_key, encode_varint, message, WireType};

        let index = tx.index as u64;

        encode_key(1u32, WireType::LengthDelimited, buf);
        encode_varint(tx.signature.as_ref().len() as u64, buf);
        buf.put_slice(tx.signature.as_ref());

        if tx.is_vote {
            prost::encoding::bool::encode(2u32, &tx.is_vote, buf);
        }

        message::encode(3u32, &tx.transaction, buf);
        message::encode(4u32, &tx.meta, buf);

        if index != 0u64 {
            prost::encoding::uint64::encode(5u32, &index, buf);
        }
    }

    pub fn encoded_len(tx: &MessageTransactionInfo) -> usize {
        use prost::encoding::{encoded_len_varint, key_len, message};

        let index = tx.index as u64;
        let sig_len = tx.signature.as_ref().len();

        key_len(1u32)
            + encoded_len_varint(sig_len as u64)
            + sig_len
            + if tx.is_vote {
                prost::encoding::bool::encoded_len(2u32, &tx.is_vote)
            } else {
                0
            }
            + message::encoded_len(3u32, &tx.transaction)
            + message::encoded_len(4u32, &tx.meta)
            + if index != 0u64 {
                prost::encoding::uint64::encoded_len(5u32, &index)
            } else {
                0
            }
    }
}

pub struct AccountEncoder;

impl AccountEncoder {
    pub fn pre_encode(account: &mut MessageAccountInfo) {
        let len = Self::encoded_len(account);
        let mut buf = Vec::with_capacity(len);

        prost_bytes_encode_raw(1u32, account.pubkey.as_ref(), &mut buf);
        if account.lamports != 0u64 {
            ::prost::encoding::uint64::encode(2u32, &account.lamports, &mut buf);
        }
        prost_bytes_encode_raw(3u32, account.owner.as_ref(), &mut buf);
        if account.executable {
            ::prost::encoding::bool::encode(4u32, &account.executable, &mut buf);
        }
        if account.rent_epoch != 0u64 {
            ::prost::encoding::uint64::encode(5u32, &account.rent_epoch, &mut buf);
        }
        if !account.data.is_empty() {
            prost_bytes_encode_raw(6u32, &account.data, &mut buf);
        }
        if account.write_version != 0u64 {
            ::prost::encoding::uint64::encode(7u32, &account.write_version, &mut buf);
        }
        if let Some(value) = &account.txn_signature {
            prost_bytes_encode_raw(8u32, value.as_ref(), &mut buf);
        }

        account.pre_encoded = Some(Bytes::from(buf));
    }

    pub fn encoded_len(account: &MessageAccountInfo) -> usize {
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
            + if !account.data.is_empty() {
                prost_bytes_encoded_len(6u32, &account.data)
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
