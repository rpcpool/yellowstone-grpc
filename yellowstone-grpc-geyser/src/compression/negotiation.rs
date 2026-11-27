/*
 *
 * Copyright 2025 gRPC authors.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *
 */

//! The response encoding a stream picks and the request encoding it accepts, ported from tonic
//! 0.14.9 (`src/codec/compression.rs`), which keeps both rules crate-private.

use {
    super::encoding::ResponseEncoding,
    http::HeaderMap,
    std::borrow::Cow,
    tonic::{codec::CompressionEncoding, metadata::MetadataValue, Status},
};

/// The request header that lists the encodings a client accepts, in its order of preference.
pub const ACCEPT_ENCODING_HEADER: &str = "grpc-accept-encoding";

/// The header that names the encoding of the messages that follow it.
pub const ENCODING_HEADER: &str = "grpc-encoding";

/// The compressing encodings a server turns on for one direction, in the order its config lists
/// them. Identity is always allowed and never in the set.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct EnabledEncodings {
    encodings_in_enable_order: [Option<ResponseEncoding>; 2],
}

impl EnabledEncodings {
    /// The set a `compression.send` or `compression.accept` list turns on, built as tonic's
    /// `enable` builds it from the same list: in list order, each encoding once.
    pub fn from_tonic_encodings(tonic_encodings: &[CompressionEncoding]) -> Self {
        tonic_encodings
            .iter()
            .filter_map(|tonic_encoding| response_encoding_of(*tonic_encoding))
            .fold(Self::default(), Self::with_encoding)
    }

    /// This set with `response_encoding` added at the end. Identity, or an encoding the set
    /// already holds, leaves it unchanged.
    pub const fn with_encoding(self, response_encoding: ResponseEncoding) -> Self {
        if matches!(response_encoding, ResponseEncoding::Identity)
            || self.is_enabled(response_encoding)
        {
            return self;
        }
        let encodings_in_enable_order = match self.encodings_in_enable_order {
            [None, _] => [Some(response_encoding), None],
            [first_enabled_encoding, _] => [first_enabled_encoding, Some(response_encoding)],
        };
        Self {
            encodings_in_enable_order,
        }
    }

    /// Whether the set holds `response_encoding`; never true for identity.
    pub const fn is_enabled(self, response_encoding: ResponseEncoding) -> bool {
        let [first_enabled_encoding, second_enabled_encoding] = self.encodings_in_enable_order;
        is_same_compressing_encoding(first_enabled_encoding, response_encoding)
            || is_same_compressing_encoding(second_enabled_encoding, response_encoding)
    }

    /// Whether the set is empty, so its direction uses identity only.
    pub const fn is_empty(self) -> bool {
        self.encodings_in_enable_order[0].is_none()
    }

    /// The `grpc-accept-encoding` value tonic answers an unsupported request encoding with: the
    /// set in order, then `identity`.
    pub const fn grpc_accept_encoding_value(self) -> &'static str {
        match self.encodings_in_enable_order {
            [Some(ResponseEncoding::Gzip), Some(ResponseEncoding::Zstd)] => "gzip,zstd,identity",
            [Some(ResponseEncoding::Zstd), Some(ResponseEncoding::Gzip)] => "zstd,gzip,identity",
            [Some(ResponseEncoding::Gzip), _] => "gzip,identity",
            [Some(ResponseEncoding::Zstd), _] => "zstd,identity",
            _ => "identity",
        }
    }
}

const fn is_same_compressing_encoding(
    enabled_encoding: Option<ResponseEncoding>,
    response_encoding: ResponseEncoding,
) -> bool {
    matches!(
        (enabled_encoding, response_encoding),
        (Some(ResponseEncoding::Gzip), ResponseEncoding::Gzip)
            | (Some(ResponseEncoding::Zstd), ResponseEncoding::Zstd)
    )
}

/// The plugin's encoding for one of tonic's, or `None` for one the config never lists.
const fn response_encoding_of(tonic_encoding: CompressionEncoding) -> Option<ResponseEncoding> {
    match tonic_encoding {
        CompressionEncoding::Gzip => Some(ResponseEncoding::Gzip),
        CompressionEncoding::Zstd => Some(ResponseEncoding::Zstd),
        _ => None,
    }
}

/// The first entry of the request's first `grpc-accept-encoding` field that `send_encodings`
/// holds, zstd before any other with `prefer_zstd`, and identity when no entry matches.
pub fn negotiate_response_encoding(
    request_headers: &HeaderMap,
    send_encodings: EnabledEncodings,
    prefer_zstd: bool,
) -> ResponseEncoding {
    if send_encodings.is_empty() {
        return ResponseEncoding::Identity;
    }
    let Some(accept_encoding_list) = request_headers
        .get(ACCEPT_ENCODING_HEADER)
        .and_then(|accept_encoding_field| accept_encoding_field.to_str().ok())
    else {
        return ResponseEncoding::Identity;
    };
    let zstd_goes_first = prefer_zstd
        && send_encodings.is_enabled(ResponseEncoding::Zstd)
        && accept_encoding_entries(accept_encoding_list).any(|accept_entry| accept_entry == "zstd");
    if zstd_goes_first {
        return ResponseEncoding::Zstd;
    }
    accept_encoding_entries(accept_encoding_list)
        .find_map(|accept_entry| match accept_entry {
            "gzip" if send_encodings.is_enabled(ResponseEncoding::Gzip) => {
                Some(ResponseEncoding::Gzip)
            }
            "zstd" if send_encodings.is_enabled(ResponseEncoding::Zstd) => {
                Some(ResponseEncoding::Zstd)
            }
            _ => None,
        })
        .unwrap_or(ResponseEncoding::Identity)
}

/// The entries of a `grpc-accept-encoding` value in the client's order, each trimmed.
fn accept_encoding_entries(accept_encoding_list: &str) -> impl Iterator<Item = &str> {
    accept_encoding_list.split(',').map(str::trim)
}

/// The encoding of the request's messages: `Ok(None)` when `grpc-encoding` is absent or
/// `identity`, and tonic's UNIMPLEMENTED status when `accept_encodings` does not hold it.
pub fn check_request_encoding(
    request_headers: &HeaderMap,
    accept_encodings: EnabledEncodings,
) -> Result<Option<ResponseEncoding>, Status> {
    let Some(encoding_field) = request_headers.get(ENCODING_HEADER) else {
        return Ok(None);
    };
    match encoding_field.as_bytes() {
        b"gzip" if accept_encodings.is_enabled(ResponseEncoding::Gzip) => {
            Ok(Some(ResponseEncoding::Gzip))
        }
        b"zstd" if accept_encodings.is_enabled(ResponseEncoding::Zstd) => {
            Ok(Some(ResponseEncoding::Zstd))
        }
        b"identity" => Ok(None),
        unsupported_encoding => Err(unsupported_request_encoding_status(
            unsupported_encoding,
            accept_encodings,
        )),
    }
}

fn unsupported_request_encoding_status(
    unsupported_encoding: &[u8],
    accept_encodings: EnabledEncodings,
) -> Status {
    let encoding_text = match std::str::from_utf8(unsupported_encoding) {
        Ok(utf8_encoding) => Cow::Borrowed(utf8_encoding),
        Err(_) => Cow::Owned(format!("{unsupported_encoding:?}")),
    };
    let mut unsupported_status = Status::unimplemented(format!(
        "Content is compressed with `{encoding_text}` which isn't supported"
    ));
    unsupported_status.metadata_mut().insert(
        ACCEPT_ENCODING_HEADER,
        MetadataValue::from_static(accept_encodings.grpc_accept_encoding_value()),
    );
    unsupported_status
}
