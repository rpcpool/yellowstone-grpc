use bytes::Bytes;

use crate::plugin::message::MessageTransactionInfo;

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
