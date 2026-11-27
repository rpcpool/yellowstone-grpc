//! One gzip member of a whole message, with the header and trailer flate2's `GzEncoder` writes.

use {
    crate::extension::seams::CompressionError,
    flate2::{Compress, Compression, Crc, FlushCompress, Status as DeflateStatus},
    std::io,
};

/// The gzip levels that deflate on zlib-rs: only levels where its output is not larger than
/// miniz_oxide's for any stream. Every other level runs on miniz_oxide.
pub const ZLIB_RS_GZIP_LEVELS: &[u32] = &[];

/// The highest gzip level; both backends take every level from 0 to it.
pub const MAX_GZIP_LEVEL: u32 = 9;

pub const GZIP_HEADER_BYTES: usize = 10;

pub const GZIP_TRAILER_BYTES: usize = 8;

/// `GzEncoder` reads its input in 32 KiB chunks and tonic reads its output in 8 KiB steps.
/// miniz_oxide's level-1 bytes depend on where a step cuts a block flush short.
const GZ_ENCODER_INPUT_CHUNK_BYTES: usize = 32 * 1024;
const TONIC_READ_STEP_BYTES: usize = 8 * 1024;

/// The deflate implementation behind a gzip member.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GzipBackend {
    /// flate2's default backend, on which tonic's own gzip runs.
    MinizOxide,
    /// The `zlib-rs` crate, called directly.
    ZlibRs,
}

impl GzipBackend {
    /// The backend [`ZLIB_RS_GZIP_LEVELS`] gives `gzip_level`.
    pub fn for_gzip_level(gzip_level: u32) -> Self {
        if ZLIB_RS_GZIP_LEVELS.contains(&gzip_level) {
            Self::ZlibRs
        } else {
            Self::MinizOxide
        }
    }
}

/// The header `GzEncoder` writes at `gzip_level`: no name, no time, extra flags from the level,
/// and operating system 255.
pub const fn gzip_member_header(gzip_level: u32) -> [u8; GZIP_HEADER_BYTES] {
    let extra_flags = if gzip_level >= MAX_GZIP_LEVEL {
        2
    } else if gzip_level <= 1 {
        4
    } else {
        0
    };
    [0x1f, 0x8b, 0x08, 0, 0, 0, 0, 0, extra_flags, 0xff]
}

/// A raw deflate state at one gzip level, reset for each member.
pub struct GzipDeflater {
    gzip_level: u32,
    raw_deflater: RawDeflater,
}

enum RawDeflater {
    MinizOxide(Compress),
    ZlibRs(zlib_rs::Deflate),
}

impl GzipDeflater {
    /// A deflate state for `gzip_level` on `gzip_backend`. Panics when the level is above
    /// [`MAX_GZIP_LEVEL`].
    pub fn new(gzip_backend: GzipBackend, gzip_level: u32) -> Self {
        assert!(
            gzip_level <= MAX_GZIP_LEVEL,
            "gzip level {gzip_level} is above {MAX_GZIP_LEVEL}"
        );
        let raw_deflater = match gzip_backend {
            GzipBackend::MinizOxide => {
                RawDeflater::MinizOxide(Compress::new(Compression::new(gzip_level), false))
            }
            GzipBackend::ZlibRs => {
                RawDeflater::ZlibRs(zlib_rs::Deflate::new(gzip_level as i32, false, 15))
            }
        };
        Self {
            gzip_level,
            raw_deflater,
        }
    }

    /// Appends one gzip member of `message_bytes`. On an error the output keeps its length.
    pub fn write_gzip_member(
        &mut self,
        message_bytes: &[u8],
        compressed_output: &mut Vec<u8>,
    ) -> Result<(), CompressionError> {
        let member_start = compressed_output.len();
        compressed_output.reserve(
            GZIP_HEADER_BYTES
                + zlib_rs::compress_bound(message_bytes.len())
                + GZIP_TRAILER_BYTES
                + TONIC_READ_STEP_BYTES,
        );
        compressed_output.extend_from_slice(&gzip_member_header(self.gzip_level));
        let deflate_result = match &mut self.raw_deflater {
            RawDeflater::MinizOxide(miniz_deflater) => {
                deflate_in_tonic_read_steps(miniz_deflater, message_bytes, compressed_output)
            }
            RawDeflater::ZlibRs(zlib_rs_deflater) => {
                deflate_in_one_call(zlib_rs_deflater, message_bytes, compressed_output)
            }
        };
        if let Err(deflate_error) = deflate_result {
            compressed_output.truncate(member_start);
            return Err(deflate_error);
        }
        let mut message_crc = Crc::new();
        message_crc.update(message_bytes);
        compressed_output.extend_from_slice(&message_crc.sum().to_le_bytes());
        compressed_output.extend_from_slice(&message_crc.amount().to_le_bytes());
        Ok(())
    }
}

fn gzip_member_failed(
    failure_reason: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> CompressionError {
    CompressionError::GzipMemberFailed(io::Error::other(failure_reason))
}

/// Makes the calls `GzEncoder` makes when tonic copies it out in 8 KiB reads: each read step gets
/// a fresh 8 KiB of output space, and each call gets the rest of the current 32 KiB input chunk.
fn deflate_in_tonic_read_steps(
    miniz_deflater: &mut Compress,
    message_bytes: &[u8],
    compressed_output: &mut Vec<u8>,
) -> Result<(), CompressionError> {
    miniz_deflater.reset();
    let mut input_chunk_end = 0;
    loop {
        let step_start = compressed_output.len();
        compressed_output.resize(step_start + TONIC_READ_STEP_BYTES, 0);
        let (step_bytes, stream_ended) = loop {
            let consumed_bytes = miniz_deflater.total_in() as usize;
            if consumed_bytes == input_chunk_end {
                input_chunk_end = message_bytes
                    .len()
                    .min(consumed_bytes + GZ_ENCODER_INPUT_CHUNK_BYTES);
            }
            let chunk_input = &message_bytes[consumed_bytes..input_chunk_end];
            let input_ended = chunk_input.is_empty();
            let deflate_flush = if input_ended {
                FlushCompress::Finish
            } else {
                FlushCompress::None
            };
            let produced_before = miniz_deflater.total_out();
            let deflate_status = miniz_deflater
                .compress(
                    chunk_input,
                    &mut compressed_output[step_start..],
                    deflate_flush,
                )
                .map_err(gzip_member_failed)?;
            let step_bytes = (miniz_deflater.total_out() - produced_before) as usize;
            let consumed_now = miniz_deflater.total_in() as usize - consumed_bytes;
            match deflate_status {
                DeflateStatus::StreamEnd => break (step_bytes, true),
                _ if step_bytes > 0 => break (step_bytes, false),
                _ if !input_ended && consumed_now > 0 => continue,
                _ => {
                    compressed_output.truncate(step_start);
                    return Err(gzip_member_failed("deflate made no progress"));
                }
            }
        };
        compressed_output.truncate(step_start + step_bytes);
        if stream_ended {
            return Ok(());
        }
    }
}

/// One `Finish` call with room for the whole stream.
fn deflate_in_one_call(
    zlib_rs_deflater: &mut zlib_rs::Deflate,
    message_bytes: &[u8],
    compressed_output: &mut Vec<u8>,
) -> Result<(), CompressionError> {
    zlib_rs_deflater.reset();
    let deflate_start = compressed_output.len();
    compressed_output.resize(
        deflate_start + zlib_rs::compress_bound(message_bytes.len()),
        0,
    );
    let deflate_status = zlib_rs_deflater
        .compress(
            message_bytes,
            &mut compressed_output[deflate_start..],
            zlib_rs::DeflateFlush::Finish,
        )
        .map_err(|deflate_error| gzip_member_failed(deflate_error.as_str()));
    compressed_output.truncate(deflate_start + zlib_rs_deflater.total_out() as usize);
    match deflate_status? {
        zlib_rs::Status::StreamEnd => Ok(()),
        zlib_rs::Status::Ok | zlib_rs::Status::BufError => Err(gzip_member_failed(
            "zlib-rs did not finish within the compress bound",
        )),
    }
}
