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

//! The trailers that end a response stream with a status, ported from tonic 0.14.9
//! (`Status::to_header_map` in `src/status.rs`), which keeps that function crate-private.

use {http::HeaderMap, tonic::Status};

/// The trailers tonic's encoder ends a stream with for `stream_status`: its metadata less the names
/// gRPC reserves, `grpc-status`, then `grpc-message` and `grpc-status-details-bin` when not empty.
pub fn status_trailers(stream_status: &Status) -> HeaderMap {
    let mut stream_trailers = HeaderMap::with_capacity(3 + stream_status.metadata().len());
    match stream_status.add_header(&mut stream_trailers) {
        Ok(()) => stream_trailers,
        // Not reached: the message is percent-encoded and the details base64-encoded. Here tonic
        // fails the body with this INTERNAL status, whose own trailers always serialize.
        Err(serialize_failure) => status_trailers(&serialize_failure),
    }
}
