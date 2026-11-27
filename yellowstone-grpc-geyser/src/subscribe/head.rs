/*
 * Copyright 2019 gRPC authors.
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
 */

/*
 * Copyright (c) 2014-2026 Sean McArthur
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

//! The head of a Subscribe response, from tonic 0.14.9's `map_response` (`src/server/grpc.rs`,
//! first notice), and hyper 1.12's rules for an HTTP/2 response head (`src/proto/h2/`, second).

use {
    crate::compression::{negotiation::ENCODING_HEADER, ResponseEncoding},
    http::{header::CONTENT_TYPE, response::Parts, HeaderName, HeaderValue, Version},
    tonic::metadata::GRPC_CONTENT_TYPE,
};

/// The metadata names tonic never sends as response headers (`GRPC_RESERVED_HEADERS`).
const GRPC_RESERVED_HEADERS: [HeaderName; 5] = [
    HeaderName::from_static("te"),
    HeaderName::from_static("content-type"),
    HeaderName::from_static("grpc-message"),
    HeaderName::from_static("grpc-message-type"),
    HeaderName::from_static("grpc-status"),
];

/// The head tonic's `map_response` gives a Subscribe response in `response_encoding`, split
/// from the setup's response with its stream. Under hyper, hyper adds its own rules.
pub fn build_response_head<ResponseStream>(
    setup_response: tonic::Response<ResponseStream>,
    response_encoding: ResponseEncoding,
) -> (Parts, ResponseStream) {
    let (response_metadata, response_stream, response_extensions) = setup_response.into_parts();
    let mut response_headers = response_metadata.into_headers();
    for reserved_header in &GRPC_RESERVED_HEADERS {
        response_headers.remove(reserved_header);
    }
    let mut http_response = http::Response::new(());
    *http_response.version_mut() = Version::HTTP_2;
    *http_response.headers_mut() = response_headers;
    *http_response.extensions_mut() = response_extensions;
    let (mut response_parts, ()) = http_response.into_parts();
    response_parts
        .headers
        .insert(CONTENT_TYPE, GRPC_CONTENT_TYPE);
    if response_encoding != ResponseEncoding::Identity {
        response_parts.headers.insert(
            ENCODING_HEADER,
            HeaderValue::from_static(response_encoding.encoding_name()),
        );
    }
    (response_parts, response_stream)
}
