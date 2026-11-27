//! The encoding a response stream sends its messages in.

use tonic::codec::CompressionEncoding;

/// How a response stream sends each message: as it is, as one gzip member or as one zstd frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResponseEncoding {
    Identity,
    Gzip,
    Zstd,
}

impl ResponseEncoding {
    pub const ALL: [Self; 3] = [Self::Identity, Self::Gzip, Self::Zstd];

    /// The name `grpc-encoding` and `grpc-accept-encoding` give this encoding.
    pub const fn encoding_name(self) -> &'static str {
        match self {
            Self::Identity => "identity",
            Self::Gzip => "gzip",
            Self::Zstd => "zstd",
        }
    }

    /// tonic's value for this encoding, where `None` sends messages as they are.
    pub const fn tonic_encoding(self) -> Option<CompressionEncoding> {
        match self {
            Self::Identity => None,
            Self::Gzip => Some(CompressionEncoding::Gzip),
            Self::Zstd => Some(CompressionEncoding::Zstd),
        }
    }
}

/// An encoding that compresses, for a stream whose messages go out compressed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompressingEncoding {
    Gzip,
    Zstd,
}

impl CompressingEncoding {
    pub const ALL: [Self; 2] = [Self::Gzip, Self::Zstd];

    /// The compressing encoding `response_encoding` names, or `None` for identity.
    pub const fn of_response_encoding(response_encoding: ResponseEncoding) -> Option<Self> {
        match response_encoding {
            ResponseEncoding::Identity => None,
            ResponseEncoding::Gzip => Some(Self::Gzip),
            ResponseEncoding::Zstd => Some(Self::Zstd),
        }
    }

    /// The response encoding that names this compressing encoding.
    pub const fn response_encoding(self) -> ResponseEncoding {
        match self {
            Self::Gzip => ResponseEncoding::Gzip,
            Self::Zstd => ResponseEncoding::Zstd,
        }
    }
}
