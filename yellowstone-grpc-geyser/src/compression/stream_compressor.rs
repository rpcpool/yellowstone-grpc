//! Whole-message compression for one stream at a time, with the build's codec. Writers share a
//! pool of codec workspaces, so no stream and no thread keeps one of its own.

use {
    super::encoding::CompressingEncoding,
    crate::extension::seams::{CompressionCodec, CompressionError},
    crossbeam_channel::{Receiver, Sender},
};

/// Idle workspaces the pool keeps; a writer that finds none builds one.
pub const IDLE_WORKSPACE_LIMIT: usize = 64;

/// The capacity a workspace's scratch buffer keeps after a bigger message.
pub const KEPT_SCRATCH_BYTES: usize = 1024 * 1024;

/// Compresses whole messages, each into one gzip member or one zstd frame.
pub trait WholeMessageCompressor: Send + Sync {
    /// Compresses the message `write_message` appends, in `compressing_encoding`, and hands the
    /// compressed bytes to `take_compressed`.
    fn compress_whole_message(
        &self,
        compressing_encoding: CompressingEncoding,
        write_message: &mut dyn FnMut(&mut Vec<u8>),
        take_compressed: &mut dyn FnMut(&mut Vec<u8>),
    ) -> Result<(), CompressionError>;

    /// Compresses the message made of `message_segments`, in order, into the bytes that
    /// `compress_whole_message` gives the same message, and hands them to `take_compressed`.
    fn compress_message_segments(
        &self,
        compressing_encoding: CompressingEncoding,
        message_segments: &[&[u8]],
        take_compressed: &mut dyn FnMut(&mut Vec<u8>),
    ) -> Result<(), CompressionError>;
}

/// A codec's thread state and the message and output buffers one compression uses.
struct CodecWorkspace<CodecThreadState> {
    codec_thread_state: CodecThreadState,
    message_scratch: Vec<u8>,
    compressed_scratch: Vec<u8>,
}

/// The profile's codec with a pool of idle workspaces in a bounded array channel, whose `try`
/// calls take no lock while no one blocks on it.
pub struct PooledStreamCompressor<Codec: CompressionCodec> {
    compression_codec: Codec,
    idle_workspace_tx: Sender<Box<CodecWorkspace<Codec::CodecThreadState>>>,
    idle_workspace_rx: Receiver<Box<CodecWorkspace<Codec::CodecThreadState>>>,
}

impl<Codec: CompressionCodec> PooledStreamCompressor<Codec> {
    pub fn new(compression_codec: Codec) -> Self {
        let (idle_workspace_tx, idle_workspace_rx) =
            crossbeam_channel::bounded(IDLE_WORKSPACE_LIMIT);
        Self {
            compression_codec,
            idle_workspace_tx,
            idle_workspace_rx,
        }
    }

    fn take_workspace(&self) -> Box<CodecWorkspace<Codec::CodecThreadState>> {
        self.idle_workspace_rx.try_recv().unwrap_or_else(|_| {
            Box::new(CodecWorkspace {
                codec_thread_state: self.compression_codec.new_codec_thread_state(),
                message_scratch: Vec::new(),
                compressed_scratch: Vec::new(),
            })
        })
    }
}

impl<Codec: CompressionCodec> WholeMessageCompressor for PooledStreamCompressor<Codec> {
    fn compress_whole_message(
        &self,
        compressing_encoding: CompressingEncoding,
        write_message: &mut dyn FnMut(&mut Vec<u8>),
        take_compressed: &mut dyn FnMut(&mut Vec<u8>),
    ) -> Result<(), CompressionError> {
        let mut codec_workspace = self.take_workspace();
        let CodecWorkspace {
            codec_thread_state,
            message_scratch,
            compressed_scratch,
        } = &mut *codec_workspace;
        message_scratch.clear();
        compressed_scratch.clear();
        write_message(message_scratch);
        let message_segments: [&[u8]; 1] = [message_scratch];
        let compression_result = self.compress_into(
            compressing_encoding,
            codec_thread_state,
            &message_segments,
            compressed_scratch,
        );
        if compression_result.is_ok() {
            take_compressed(compressed_scratch);
        }
        self.return_workspace(codec_workspace);
        compression_result
    }

    fn compress_message_segments(
        &self,
        compressing_encoding: CompressingEncoding,
        message_segments: &[&[u8]],
        take_compressed: &mut dyn FnMut(&mut Vec<u8>),
    ) -> Result<(), CompressionError> {
        let mut codec_workspace = self.take_workspace();
        let CodecWorkspace {
            codec_thread_state,
            compressed_scratch,
            ..
        } = &mut *codec_workspace;
        compressed_scratch.clear();
        let compression_result = self.compress_into(
            compressing_encoding,
            codec_thread_state,
            message_segments,
            compressed_scratch,
        );
        if compression_result.is_ok() {
            take_compressed(compressed_scratch);
        }
        self.return_workspace(codec_workspace);
        compression_result
    }
}

impl<Codec: CompressionCodec> PooledStreamCompressor<Codec> {
    fn compress_into(
        &self,
        compressing_encoding: CompressingEncoding,
        codec_thread_state: &mut Codec::CodecThreadState,
        message_segments: &[&[u8]],
        compressed_output: &mut Vec<u8>,
    ) -> Result<(), CompressionError> {
        match compressing_encoding {
            CompressingEncoding::Gzip => self.compression_codec.compress_gzip_member(
                codec_thread_state,
                message_segments,
                compressed_output,
            ),
            CompressingEncoding::Zstd => self.compression_codec.compress_zstd_frame(
                codec_thread_state,
                message_segments,
                compressed_output,
            ),
        }
    }

    /// Returns `codec_workspace` to the pool, its scratch shrunk after a big message; a full
    /// pool drops it.
    fn return_workspace(&self, mut codec_workspace: Box<CodecWorkspace<Codec::CodecThreadState>>) {
        for scratch_buffer in [
            &mut codec_workspace.message_scratch,
            &mut codec_workspace.compressed_scratch,
        ] {
            if scratch_buffer.capacity() > KEPT_SCRATCH_BYTES {
                *scratch_buffer = Vec::with_capacity(KEPT_SCRATCH_BYTES);
            }
        }
        let _ = self.idle_workspace_tx.try_send(codec_workspace);
    }
}
