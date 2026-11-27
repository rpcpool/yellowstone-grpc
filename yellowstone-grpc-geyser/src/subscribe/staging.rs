//! A stream's staging chunks: frames are written into an active 64 KiB chunk and split off at each
//! cut. A chunk whose frames h2 has dropped is reclaimed, so a stream that keeps up never allocates.

use {
    bytes::{Bytes, BytesMut},
    std::collections::VecDeque,
};

/// The size of each staging chunk.
pub const STAGING_CHUNK_BYTES: usize = 64 * 1024;

/// Retired chunks a stream keeps, whose frames h2 may still hold, to reclaim later.
pub const RETIRED_CHUNK_LIMIT: usize = 3;

/// One stream's staging: the active chunk and the retired ones, all allocated at first use.
#[derive(Debug, Default)]
pub struct FrameStaging {
    active_chunk: BytesMut,
    active_chunk_is_allocated: bool,
    active_chunk_is_oversized: bool,
    retired_chunks: VecDeque<BytesMut>,
}

impl FrameStaging {
    /// The chunk that copied bytes go into.
    pub const fn active_chunk_mut(&mut self) -> &mut BytesMut {
        &mut self.active_chunk
    }

    /// Makes room in the active chunk for `copied_len` more bytes. When the active chunk runs
    /// out, its staged bytes go to `ready_frames` first, so frames keep their order.
    pub fn prepare_room(&mut self, copied_len: usize, ready_frames: &mut VecDeque<Bytes>) {
        if self.active_chunk.capacity() - self.active_chunk.len() >= copied_len
            || self.active_chunk.try_reclaim(copied_len)
        {
            return;
        }
        self.note_a_grown_chunk();
        self.split_staged_frame(ready_frames);
        let keeps_the_active_chunk =
            self.active_chunk_is_allocated && !self.active_chunk_is_oversized;
        let reclaimed_position = self
            .retired_chunks
            .iter_mut()
            .position(|retired_chunk| retired_chunk.try_reclaim(copied_len));
        let retiring_chunk = std::mem::take(&mut self.active_chunk);
        // The retiring chunk takes the reclaimed one's slot, so the ring's order is not by age.
        if let Some(reclaimed_position) = reclaimed_position {
            self.active_chunk = if keeps_the_active_chunk {
                std::mem::replace(&mut self.retired_chunks[reclaimed_position], retiring_chunk)
            } else {
                self.retired_chunks
                    .remove(reclaimed_position)
                    .expect("the position is in the ring")
            };
            self.active_chunk_is_allocated = true;
            self.active_chunk_is_oversized = false;
            return;
        }
        if keeps_the_active_chunk {
            if self.retired_chunks.len() == RETIRED_CHUNK_LIMIT {
                self.retired_chunks.pop_front();
            }
            self.retired_chunks.push_back(retiring_chunk);
        }
        self.active_chunk = BytesMut::with_capacity(copied_len.max(STAGING_CHUNK_BYTES));
        self.active_chunk_is_allocated = true;
        self.active_chunk_is_oversized = copied_len > STAGING_CHUNK_BYTES;
    }

    /// Marks the active chunk oversized when a write copied more than its room and grew it.
    fn note_a_grown_chunk(&mut self) {
        if self.active_chunk.capacity() > STAGING_CHUNK_BYTES {
            self.active_chunk_is_oversized = true;
        }
    }

    /// Splits the staged bytes off as one frame into `ready_frames`; nothing when none are staged.
    pub fn split_staged_frame(&mut self, ready_frames: &mut VecDeque<Bytes>) {
        if !self.active_chunk.is_empty() {
            ready_frames.push_back(self.active_chunk.split().freeze());
        }
    }

    /// Ends a cut: the staged bytes become a frame, and a chunk allocated for one big message is
    /// let go, so the stream keeps no more than its ring of normal chunks.
    pub fn end_cut(&mut self, ready_frames: &mut VecDeque<Bytes>) {
        self.note_a_grown_chunk();
        self.split_staged_frame(ready_frames);
        if self.active_chunk_is_oversized {
            self.active_chunk = BytesMut::new();
            self.active_chunk_is_allocated = false;
            self.active_chunk_is_oversized = false;
        }
    }

    /// Drops the staged bytes of a cut that an encoding error ends.
    pub fn discard_staged(&mut self) {
        self.active_chunk.clear();
    }
}
