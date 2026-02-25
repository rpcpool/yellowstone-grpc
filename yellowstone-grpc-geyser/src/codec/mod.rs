use std::io;

use bytes::Bytes;

pub mod encode;




pub trait PreEncoded {

    type Error: From<io::Error>;

    fn pre_encoded(&self) -> Result<Bytes, Self::Error>;
}