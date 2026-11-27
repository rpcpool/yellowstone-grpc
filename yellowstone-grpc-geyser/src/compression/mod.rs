#![deny(clippy::disallowed_types, clippy::disallowed_methods)]
//! Response compression: whole messages, once per group of streams with the same bytes.

pub mod codec;
pub mod encoding;
pub mod gzip;
pub mod negotiation;
pub mod pool;
pub mod slot;
pub mod status_trailers;
pub mod stream_compressor;
pub mod zstd;

pub use {
    codec::{CompressorContexts, StandardCompressionCodec},
    encoding::{CompressingEncoding, ResponseEncoding},
    gzip::{GzipBackend, ZLIB_RS_GZIP_LEVELS},
    negotiation::{check_request_encoding, negotiate_response_encoding, EnabledEncodings},
    status_trailers::status_trailers,
    stream_compressor::{PooledStreamCompressor, WholeMessageCompressor},
};
