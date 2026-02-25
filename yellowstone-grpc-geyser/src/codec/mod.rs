use std::{convert::Infallible, io, marker::PhantomData};

use bytes::{Bytes, BytesMut};

pub mod encode;


pub trait PreEncoded {
    type Error: From<io::Error>;

    fn pre_encoded(&self) -> Result<Bytes, Self::Error>;
}

/// Result of encoding a single message.
///
/// - `Bytes`: encoder already has a serialized payload and returns it directly.
/// - `Buffer`: encoder wrote serialized payload into the provided `BytesMut`.
#[derive(Debug, Clone)]
pub enum EncodeOutput {
    Bytes(Bytes),
    Buffer,
}

/// Server-side encoding trait that can either serialize into a buffer or hand back existing bytes.
pub trait Encode {
    type Item;
    type Error;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<EncodeOutput, Self::Error>;
}

/// Stream item that can either be already encoded bytes or a custom value that still needs encoding.
#[derive(Debug, Clone)]
pub enum MaybeEncoded<T> {
    Bytes(Bytes),
    Value(T),
}

/// Compatibility encoder for streams whose items already implement `PreEncoded`.
#[derive(Debug, Clone, Copy, Default)]
pub struct PreEncodedEncoder<T>(PhantomData<T>);

impl<T> Encode for PreEncodedEncoder<T>
where
    T: PreEncoded,
{
    type Item = T;
    type Error = T::Error;

    fn encode(&mut self, item: Self::Item, _dst: &mut BytesMut) -> Result<EncodeOutput, Self::Error> {
        Ok(EncodeOutput::Bytes(item.pre_encoded()?))
    }
}

/// Pass-through encoder for streams that already yield encoded bytes.
#[derive(Debug, Clone, Copy, Default)]
pub struct BytesEncoder;

impl Encode for BytesEncoder {
    type Item = Bytes;
    type Error = Infallible;

    fn encode(&mut self, item: Self::Item, _dst: &mut BytesMut) -> Result<EncodeOutput, Self::Error> {
        Ok(EncodeOutput::Bytes(item))
    }
}

/// Adapter that allows mixed streams of pre-encoded bytes and custom items.
#[derive(Debug, Clone, Copy, Default)]
pub struct MaybeEncodedEncoder<E>(pub E);

impl<E> Encode for MaybeEncodedEncoder<E>
where
    E: Encode,
{
    type Item = MaybeEncoded<E::Item>;
    type Error = E::Error;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<EncodeOutput, Self::Error> {
        match item {
            MaybeEncoded::Bytes(bytes) => Ok(EncodeOutput::Bytes(bytes)),
            MaybeEncoded::Value(value) => self.0.encode(value, dst),
        }
    }
}
