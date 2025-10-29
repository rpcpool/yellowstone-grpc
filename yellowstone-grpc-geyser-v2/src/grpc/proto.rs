use {
    crate::proto::geyser::subscribe_update::UpdateOneof,
    bytes::BufMut,
    prost::{
        encoding::{
            encode_key, encode_varint, encoded_len_varint, key_len, message, DecodeContext,
            WireType,
        },
        Message,
    },
    prost_types::Timestamp,
    std::sync::Arc,
};

///
/// Same layout [`SubscribeUpdate`] except it shares the underyling `UpdateOneof`.
/// This struct is a big hack to circumvent tonic limitations with `Arc<T>` data types.
///
#[derive(Debug, Clone)]
pub struct ZeroCopySubscribeUpdate {
    pub filters: Vec<String>,
    pub created_at: Timestamp,
    pub shared: SharedUpdateOneof,
}

macro_rules! prost_repeated_encoded_len_map {
    ($tag:expr, $values:expr, $get_len:expr) => {{
        key_len($tag) * $values.len()
            + $values
                .iter()
                .map($get_len)
                .map(|len| encoded_len_varint(len as u64) + len)
                .sum::<usize>()
    }};
}

#[derive(Debug, Clone)]
pub struct SharedUpdateOneof(pub Arc<UpdateOneof>);

impl prost::Message for SharedUpdateOneof {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        match &*self.0 {
            UpdateOneof::Account(msg) => message::encode(2u32, msg, buf),
            UpdateOneof::Slot(msg) => message::encode(3u32, msg, buf),
            UpdateOneof::Transaction(msg) => message::encode(4u32, msg, buf),
            UpdateOneof::TransactionStatus(msg) => message::encode(10u32, msg, buf),
            UpdateOneof::Block(msg) => message::encode(5u32, msg, buf),
            UpdateOneof::Ping(_) => {
                encode_key(6u32, WireType::LengthDelimited, buf);
                encode_varint(0, buf);
            }
            UpdateOneof::Pong(msg) => message::encode(9u32, msg, buf),
            UpdateOneof::BlockMeta(msg) => message::encode(7u32, msg, buf),
            UpdateOneof::Entry(msg) => message::encode(8u32, msg, buf),
        }
    }

    fn encoded_len(&self) -> usize {
        match &*self.0 {
            UpdateOneof::Account(msg) => message::encoded_len(2u32, msg),
            UpdateOneof::Slot(msg) => message::encoded_len(3u32, msg),
            UpdateOneof::Transaction(msg) => message::encoded_len(4u32, msg),
            UpdateOneof::TransactionStatus(msg) => message::encoded_len(10u32, msg),
            UpdateOneof::Block(msg) => message::encoded_len(5u32, msg),
            UpdateOneof::Ping(_) => key_len(6u32) + encoded_len_varint(0),
            UpdateOneof::Pong(msg) => message::encoded_len(9u32, msg),
            UpdateOneof::BlockMeta(msg) => message::encoded_len(7u32, msg),
            UpdateOneof::Entry(msg) => message::encoded_len(8u32, msg),
        }
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: WireType,
        _buf: &mut impl bytes::Buf,
        _ctx: DecodeContext,
    ) -> Result<(), prost::DecodeError> {
        unimplemented!();
    }

    fn clear(&mut self) {
        unimplemented!();
    }
}

impl Message for ZeroCopySubscribeUpdate {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        for name in self.filters.iter() {
            encode_key(1u32, WireType::LengthDelimited, buf);
            encode_varint(name.len() as u64, buf);
            buf.put_slice(name.as_bytes());
        }
        self.shared.encode_raw(buf);
        message::encode(11u32, &self.created_at, buf);
    }

    fn encoded_len(&self) -> usize {
        prost_repeated_encoded_len_map!(1u32, self.filters, |filter| filter.len())
            + self.shared.encoded_len()
            + message::encoded_len(11u32, &self.created_at)
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: WireType,
        _buf: &mut impl bytes::Buf,
        _ctx: DecodeContext,
    ) -> Result<(), prost::DecodeError> {
        unimplemented!()
    }

    fn clear(&mut self) {
        unimplemented!()
    }
}
