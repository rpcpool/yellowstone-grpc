use {
    bytes::{Buf, Bytes},
    std::marker::PhantomData,
    tonic::{
        codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder},
        Status,
    },
};

/// gRPC codec that encodes requests with prost and returns each response message as raw bytes.
pub struct RawCodec<T>(PhantomData<fn() -> T>);

impl<T> Default for RawCodec<T> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T: prost::Message + Send + 'static> Codec for RawCodec<T> {
    type Encode = T;
    type Decode = Bytes;
    type Encoder = ProstEncoder<T>;
    type Decoder = RawDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        ProstEncoder(PhantomData)
    }

    fn decoder(&mut self) -> Self::Decoder {
        RawDecoder
    }
}

pub struct ProstEncoder<T>(PhantomData<fn() -> T>);

impl<T: prost::Message> Encoder for ProstEncoder<T> {
    type Item = T;
    type Error = Status;

    fn encode(&mut self, item: T, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        item.encode(dst)
            .map_err(|error| Status::internal(error.to_string()))
    }
}

pub struct RawDecoder;

impl Decoder for RawDecoder {
    type Item = Bytes;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Bytes>, Status> {
        Ok(Some(src.copy_to_bytes(src.remaining())))
    }
}
