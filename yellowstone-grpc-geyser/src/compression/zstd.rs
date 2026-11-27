//! One zstd frame of a whole message: the streaming form tonic writes, with no content size, or
//! the one-shot form `zstd::bulk::Compressor` writes, with it.

use {
    crate::extension::seams::CompressionError,
    ::zstd::zstd_safe::{self, CCtx, CParameter, ErrorCode, InBuffer, OutBuffer, ResetDirective},
    std::io::{self, Cursor},
};

/// The fewest spare output bytes a streaming call gets, so that each call makes progress.
const MIN_STREAMING_SPARE_BYTES: usize = 64;

/// A zstd context at one level, reset for each frame.
pub struct ZstdFrameContext {
    compression_context: CCtx<'static>,
}

impl ZstdFrameContext {
    /// zstd clamps a level outside its range, so the codec checks the level first.
    pub fn new(zstd_level: i32) -> Self {
        let mut compression_context = CCtx::create();
        compression_context
            .set_parameter(CParameter::CompressionLevel(zstd_level))
            .expect("zstd takes a compression level while no frame is open");
        Self {
            compression_context,
        }
    }

    /// Appends the frame tonic's streaming encoder writes for the segments, in order. On an error
    /// the output keeps its length.
    pub fn write_streaming_frame(
        &mut self,
        uncompressed_segments: &[&[u8]],
        compressed_output: &mut Vec<u8>,
    ) -> Result<(), CompressionError> {
        let frame_start = compressed_output.len();
        let frame_result = self.append_streaming_frame(uncompressed_segments, compressed_output);
        if frame_result.is_err() {
            compressed_output.truncate(frame_start);
        }
        frame_result
    }

    /// Appends the frame `zstd::bulk::Compressor` writes for `message_bytes`, with its content
    /// size. On an error the output keeps its length.
    pub fn write_one_shot_frame(
        &mut self,
        message_bytes: &[u8],
        compressed_output: &mut Vec<u8>,
    ) -> Result<(), CompressionError> {
        let frame_start = compressed_output.len();
        compressed_output.reserve(zstd_safe::compress_bound(message_bytes.len()));
        let mut frame_cursor = Cursor::new(&mut *compressed_output);
        frame_cursor.set_position(frame_start as u64);
        let frame_result = self
            .compression_context
            .compress2(&mut frame_cursor, message_bytes);
        match frame_result {
            Ok(_) => Ok(()),
            Err(error_code) => {
                compressed_output.truncate(frame_start);
                Err(zstd_frame_failed(error_code))
            }
        }
    }

    fn append_streaming_frame(
        &mut self,
        uncompressed_segments: &[&[u8]],
        compressed_output: &mut Vec<u8>,
    ) -> Result<(), CompressionError> {
        self.compression_context
            .reset(ResetDirective::SessionOnly)
            .map_err(zstd_frame_failed)?;
        let message_len = uncompressed_segments
            .iter()
            .map(|uncompressed_segment| uncompressed_segment.len())
            .sum();
        compressed_output.reserve(zstd_safe::compress_bound(message_len));
        // tonic's reader first calls with no input, which opens the frame with no pledged size;
        // a frame opened by its end call would carry the content size.
        self.compress_stream_into(&mut InBuffer::around(&[]), compressed_output)?;
        for &uncompressed_segment in uncompressed_segments {
            let mut segment_input = InBuffer::around(uncompressed_segment);
            while segment_input.pos() < uncompressed_segment.len() {
                self.compress_stream_into(&mut segment_input, compressed_output)?;
            }
        }
        loop {
            keep_streaming_spare_bytes(compressed_output);
            let output_len = compressed_output.len();
            let mut frame_output = OutBuffer::around_pos(compressed_output, output_len);
            let unflushed_bytes = self
                .compression_context
                .end_stream(&mut frame_output)
                .map_err(zstd_frame_failed)?;
            if unflushed_bytes == 0 {
                return Ok(());
            }
        }
    }

    fn compress_stream_into(
        &mut self,
        segment_input: &mut InBuffer<'_>,
        compressed_output: &mut Vec<u8>,
    ) -> Result<(), CompressionError> {
        keep_streaming_spare_bytes(compressed_output);
        let output_len = compressed_output.len();
        let mut frame_output = OutBuffer::around_pos(compressed_output, output_len);
        self.compression_context
            .compress_stream(&mut frame_output, segment_input)
            .map_err(zstd_frame_failed)?;
        Ok(())
    }
}

fn keep_streaming_spare_bytes(compressed_output: &mut Vec<u8>) {
    if compressed_output.capacity() - compressed_output.len() < MIN_STREAMING_SPARE_BYTES {
        compressed_output.reserve(CCtx::out_size());
    }
}

fn zstd_frame_failed(error_code: ErrorCode) -> CompressionError {
    CompressionError::ZstdFrameFailed(io::Error::other(zstd_safe::get_error_name(error_code)))
}
