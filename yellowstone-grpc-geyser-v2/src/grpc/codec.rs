use {
    prost::Message,
    std::{marker::PhantomData, sync::Arc},
    tonic::{
        codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder},
        Status,
    },
};

#[derive(Debug, Clone, Default)]
pub struct ArcProstCodec<T> {
    _marker: std::marker::PhantomData<T>,
}

pub struct ArcProstEncoder<T> {
    _marker: std::marker::PhantomData<T>,
}

impl<T> Default for ArcProstEncoder<T> {
    fn default() -> Self {
        Self {
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T> Encoder for ArcProstEncoder<T>
where
    T: Message + Send + Sync + 'static,
{
    type Item = Arc<T>;
    type Error = Status;

    fn encode(&mut self, item: Arc<T>, dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        item.encode(dst)
            .expect("Only fail if not enough space in dst");
        Ok(())
    }

    fn buffer_settings(&self) -> tonic::codec::BufferSettings {
        tonic::codec::BufferSettings::default()
    }
}

/// A [`Decoder`] that knows how to decode `U`.
#[derive(Debug, Clone, Default)]
pub struct ProstDecoder<U>(PhantomData<U>);

impl<U: Message + Default> Decoder for ProstDecoder<U> {
    type Item = U;
    type Error = Status;

    fn decode(&mut self, buf: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        let item = Message::decode(buf)
            .map(Option::Some)
            .map_err(from_decode_error)?;

        Ok(item)
    }
}

fn from_decode_error(error: prost::DecodeError) -> Status {
    // Map Protobuf parse errors to an INTERNAL status code, as per
    // https://github.com/grpc/grpc/blob/master/doc/statuscodes.md
    Status::new(tonic::Code::Internal, error.to_string())
}

impl<T> Codec for ArcProstCodec<T>
where
    T: Message + Default + Send + Sync + 'static,
{
    type Encode = Arc<T>;
    type Decode = T;
    type Encoder = ArcProstEncoder<T>;
    type Decoder = ProstDecoder<T>;

    fn encoder(&mut self) -> Self::Encoder {
        ArcProstEncoder {
            _marker: std::marker::PhantomData,
        }
    }

    fn decoder(&mut self) -> Self::Decoder {
        ProstDecoder(PhantomData)
    }
}
