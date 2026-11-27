//! The standard codec: each message compressed whole, byte for byte as the build's reference
//! encoder writes it.

use {
    super::{
        gzip::{GzipBackend, GzipDeflater, MAX_GZIP_LEVEL},
        zstd::ZstdFrameContext,
    },
    crate::{
        config::{CompressionDefaults, ZstdFrameForm},
        extension::seams::{CompressionCodec, CompressionError, ExtensionError},
    },
    ::zstd::zstd_safe,
};

/// Compresses whole messages: gzip at one level on one backend, and zstd at one level in one
/// frame form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StandardCompressionCodec {
    gzip_level: u32,
    gzip_backend: GzipBackend,
    zstd_level: i32,
    zstd_frame_form: ZstdFrameForm,
}

impl StandardCompressionCodec {
    /// The codec of `compression_defaults`, with gzip on the backend that
    /// [`ZLIB_RS_GZIP_LEVELS`](super::gzip::ZLIB_RS_GZIP_LEVELS) gives its level.
    pub fn new(compression_defaults: &CompressionDefaults) -> Result<Self, ExtensionError> {
        let gzip_backend = GzipBackend::for_gzip_level(compression_defaults.gzip_level);
        Self::with_gzip_backend(compression_defaults, gzip_backend)
    }

    /// The codec of `compression_defaults`, with gzip on `gzip_backend` whatever its level.
    pub fn with_gzip_backend(
        compression_defaults: &CompressionDefaults,
        gzip_backend: GzipBackend,
    ) -> Result<Self, ExtensionError> {
        let CompressionDefaults {
            gzip_level,
            zstd_level,
            zstd_frame_form,
            ..
        } = *compression_defaults;
        if gzip_level > MAX_GZIP_LEVEL {
            return Err(ExtensionError::new(format!(
                "gzip level {gzip_level} is above {MAX_GZIP_LEVEL}"
            )));
        }
        let zstd_levels = zstd_safe::min_c_level()..=zstd_safe::max_c_level();
        if !zstd_levels.contains(&zstd_level) {
            return Err(ExtensionError::new(format!(
                "zstd level {zstd_level} is outside {zstd_levels:?}"
            )));
        }
        Ok(Self {
            gzip_level,
            gzip_backend,
            zstd_level,
            zstd_frame_form,
        })
    }
}

/// What one thread makes once and reuses for every message: a deflate state, a zstd context,
/// and the buffer that joins a message's segments for the forms that need one input.
pub struct CompressorContexts {
    gzip_deflater: GzipDeflater,
    zstd_frame_context: ZstdFrameContext,
    joined_message_bytes: Vec<u8>,
}

impl CompressionCodec for StandardCompressionCodec {
    type CodecThreadState = CompressorContexts;

    fn new_codec_thread_state(&self) -> Self::CodecThreadState {
        CompressorContexts {
            gzip_deflater: GzipDeflater::new(self.gzip_backend, self.gzip_level),
            zstd_frame_context: ZstdFrameContext::new(self.zstd_level),
            joined_message_bytes: Vec::new(),
        }
    }

    fn compress_gzip_member(
        &self,
        codec_thread_state: &mut Self::CodecThreadState,
        uncompressed_segments: &[&[u8]],
        compressed_output: &mut Vec<u8>,
    ) -> Result<(), CompressionError> {
        let CompressorContexts {
            gzip_deflater,
            joined_message_bytes,
            ..
        } = codec_thread_state;
        let message_bytes = join_message_segments(uncompressed_segments, joined_message_bytes);
        gzip_deflater.write_gzip_member(message_bytes, compressed_output)
    }

    fn compress_zstd_frame(
        &self,
        codec_thread_state: &mut Self::CodecThreadState,
        uncompressed_segments: &[&[u8]],
        compressed_output: &mut Vec<u8>,
    ) -> Result<(), CompressionError> {
        let CompressorContexts {
            zstd_frame_context,
            joined_message_bytes,
            ..
        } = codec_thread_state;
        match self.zstd_frame_form {
            ZstdFrameForm::Streaming => {
                zstd_frame_context.write_streaming_frame(uncompressed_segments, compressed_output)
            }
            ZstdFrameForm::OneShot => {
                let message_bytes =
                    join_message_segments(uncompressed_segments, joined_message_bytes);
                zstd_frame_context.write_one_shot_frame(message_bytes, compressed_output)
            }
        }
    }
}

/// The message as one slice: its only non-empty segment, or every segment copied in order into
/// `joined_message_bytes`.
fn join_message_segments<'joined>(
    uncompressed_segments: &[&'joined [u8]],
    joined_message_bytes: &'joined mut Vec<u8>,
) -> &'joined [u8] {
    let mut non_empty_segments = uncompressed_segments
        .iter()
        .filter(|uncompressed_segment| !uncompressed_segment.is_empty());
    match (non_empty_segments.next(), non_empty_segments.next()) {
        (None, _) => &[],
        (Some(only_segment), None) => only_segment,
        (Some(_), Some(_)) => {
            joined_message_bytes.clear();
            for uncompressed_segment in uncompressed_segments {
                joined_message_bytes.extend_from_slice(uncompressed_segment);
            }
            joined_message_bytes
        }
    }
}
