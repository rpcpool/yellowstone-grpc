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

//! The plugin's Subscribe response body. It cuts, ends and fails as tonic 0.14.9's `EncodeBody`
//! (`src/codec/encode.rs`), writes identity pieces copied or referenced, and compresses each
//! message once per group of members or per stream.

use {
    super::{
        entry::{
            CopyingContentSink, FrameContentSink, IndexedUpdateRef, ResolvedFrameContent,
            ShapeWait, SubscribeQueueEntry,
        },
        key::FilteredUpdateKey,
        staging::{FrameStaging, STAGING_CHUNK_BYTES},
    },
    crate::{
        body::block_shape::BlockShapeWriteSettings,
        compression::{
            pool::{CompressorPools, PooledGroupJob},
            slot::{
                compress_member, private_compression_outcome, take_back_queued_job,
                BackstopWaitCount, ClaimEndWait, CompressedGroupSlots, CompressedMember,
                CompressionGroupCounters, GroupCompressionContext, GroupCompressionSettings,
                InFlightGroupPolicy, MemberCompression, PrivateCompressionCause, QueuedJobOutcome,
                WaitedSlot,
            },
            status_trailers, CompressingEncoding, WholeMessageCompressor,
        },
        extension::seams::CompressionError,
        grpc::SubscribeResponseStream,
        plugin::{filter::message::FilteredUpdate, message::CommitmentLevel},
        telemetry::data_plane_counts::{
            CompressionGroupEvent, CompressionOutcome, DataPlaneCount, DataPlaneCounts,
            WaitedClaim, DATA_PLANE_COUNTS,
        },
    },
    bytes::{BufMut, Bytes, BytesMut},
    hyper::body::{Body as HttpBody, Frame},
    prost::Message as _,
    std::{
        collections::VecDeque,
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
        time::Duration,
    },
    tokio::time::Sleep,
    tonic::Status,
};

/// tonic's yield threshold: a cut ends once its frames reach 32 KiB.
pub const CUT_YIELD_BYTES: usize = 32 * 1024;

/// The gRPC message prefix: the compressed flag, then the big-endian length.
const FRAME_PREFIX_BYTES: usize = 5;

/// How a stream's messages go out: as they are, or compressed with the build's codec.
pub enum StreamFraming {
    Identity,
    Compressed {
        compressing_encoding: CompressingEncoding,
        stream_compressor: Arc<dyn WholeMessageCompressor>,
        /// The plugin's groups (`compression.mode: shared`); `None` compresses per stream.
        group_sharing: Option<GroupSharingSettings>,
    },
}

/// What a stream needs to share compressions: the plugin's group settings and its pools.
#[derive(Debug, Clone)]
pub struct GroupSharingSettings {
    pub group_settings: Arc<GroupCompressionSettings>,
    pub compressor_pools: CompressorPools,
}

/// The frames of one stream: its staging, the frames of the current cut, and its limits.
struct StreamFrameWriter {
    stream_framing: StreamFraming,
    frame_staging: FrameStaging,
    ready_frames: VecDeque<Bytes>,
    copy_threshold_bytes: usize,
    max_message_bytes: usize,
    compression_counters: CompressionGroupCounters,
    /// `compression_counters` as the last drain folded them into the striped counts.
    folded_counters: CompressionGroupCounters,
    /// The entry that waits for a claim to end, if one does, and the timer its waits use.
    pending_write_state: PendingWriteState,
    /// The plugin's Block shape settings, within which this writer claims and waits.
    block_shape_settings: BlockShapeWriteSettings,
}

/// A stream's pending write, and its waits' backstop timer. Its drop frees a waiter's cell before
/// the pending entry, which keeps the waited table alive, drops.
struct PendingWriteState {
    pending_write: Option<PendingWrite>,
    /// Made with the body, so a wait allocates nothing.
    backstop_timer: Pin<Box<Sleep>>,
}

impl PendingWriteState {
    /// No pending write, and a timer that has not been set; the timer needs the runtime.
    fn new() -> Self {
        Self {
            pending_write: None,
            backstop_timer: Box::pin(tokio::time::sleep(Duration::ZERO)),
        }
    }
}

/// An entry whose claim has not ended. It is written once the claim ends, before any later entry
/// of the stream.
struct PendingWrite {
    queue_entry: SubscribeQueueEntry,
    wait_target: WaitTarget,
    claim_end_wait: ClaimEndWait,
}

/// What a pending write waits on.
enum WaitTarget {
    /// The entry's Block shape, which another writer builds: its slot in the entry's block.
    ShapeSlot { shape_slot_index: u8 },
    /// The entry's compression group, and a processed claimant's own job until the writer takes
    /// it back or finds it started.
    GroupSlot {
        group_slots: Arc<CompressedGroupSlots>,
        group_slot_index: usize,
        queued_pool_job: Option<PooledGroupJob>,
        /// Whether the body returned Pending since the hand-off: the take-back waits for it, so a
        /// pool thread gets one task reschedule to start the job.
        yielded_since_hand_off: bool,
    },
}

/// What a write of one entry did.
enum WriteProgress {
    /// The entry's frame is staged or ready: its framed length.
    Written(usize),
    /// Its claim has not ended: it waits in the stream's pending write.
    Pending,
}

/// What one attempt at an entry did.
enum EntryWrite {
    Written(usize),
    MustWait(WaitTarget),
}

/// An entry's content for one write: a routed member's resolved content, or a filtered update.
#[derive(Clone, Copy)]
enum EntryContent<'resolved, 'entry> {
    Resolved(&'resolved ResolvedFrameContent<'entry>),
    Filtered(&'entry FilteredUpdate),
}

/// What a shared compressed stream did with an entry through its group.
enum GroupWrite {
    Written(usize),
    /// No shared body, or no key: the entry compresses per stream.
    NotEligible,
    /// The group claim has not ended.
    MustWait {
        group_slots: Arc<CompressedGroupSlots>,
        group_slot_index: usize,
        queued_pool_job: Option<PooledGroupJob>,
    },
}

/// Where a compressed frame's message comes from.
#[derive(Clone, Copy)]
enum CompressedFrameSource<'message> {
    /// A group's message, referenced as it is above the copy threshold.
    SharedMessage(&'message Bytes),
    /// A compressor's scratch, copied at its exact size above the copy threshold.
    CompressorScratch(&'message [u8]),
}

/// How a drain of the queue ended.
enum DrainEnd {
    Cut,
    QueueEnded,
    StreamStatus(Status),
    EncodingError(Status),
}

/// The Subscribe response body of `subscribe_body: plugin`.
pub struct SubscribeResponseBody {
    stream_queue: Option<SubscribeResponseStream>,
    frame_writer: StreamFrameWriter,
    stream_ending: Option<Status>,
    trailers_sent: bool,
}

impl SubscribeResponseBody {
    /// The body over `stream_queue`. `copy_threshold_bytes` is clamped to the staging chunk, and
    /// `max_message_bytes` bounds each framed message as tonic's encoding limit does.
    pub fn new(
        stream_queue: SubscribeResponseStream,
        stream_framing: StreamFraming,
        copy_threshold_bytes: usize,
        max_message_bytes: usize,
        block_shape_settings: BlockShapeWriteSettings,
    ) -> Self {
        Self {
            stream_queue: Some(stream_queue),
            frame_writer: StreamFrameWriter {
                stream_framing,
                frame_staging: FrameStaging::default(),
                ready_frames: VecDeque::with_capacity(8),
                copy_threshold_bytes: copy_threshold_bytes.min(STAGING_CHUNK_BYTES),
                max_message_bytes,
                compression_counters: CompressionGroupCounters::default(),
                folded_counters: CompressionGroupCounters::default(),
                pending_write_state: PendingWriteState::new(),
                block_shape_settings,
            },
            stream_ending: None,
            trailers_sent: false,
        }
    }

    /// Takes entries until the cut, a pending write first. Every end that keeps the queue releases
    /// the drained weight once; a status or an encoding error drops the queue and fails senders.
    fn drain_until_cut(&mut self, task_context: &mut Context<'_>) {
        let Some(stream_queue) = self.stream_queue.as_mut() else {
            return;
        };
        let mut cut_bytes = 0;
        let drain_end = 'drain: {
            if self.frame_writer.has_pending_write() {
                match self.frame_writer.resume_pending_write(task_context) {
                    Ok(WriteProgress::Written(frame_bytes)) => {
                        cut_bytes += frame_bytes;
                        if cut_bytes >= CUT_YIELD_BYTES {
                            break 'drain DrainEnd::Cut;
                        }
                    }
                    Ok(WriteProgress::Pending) => break 'drain DrainEnd::Cut,
                    Err(encoding_status) => break 'drain DrainEnd::EncodingError(encoding_status),
                }
            }
            loop {
                match stream_queue.poll_recv_holding_weight(task_context) {
                    Poll::Ready(Some(Ok(queue_entry))) => {
                        if self.frame_writer.compresses_a_big_message(&queue_entry) {
                            stream_queue.release_drained_weight();
                        }
                        match self.frame_writer.write_entry(queue_entry, task_context) {
                            Ok(WriteProgress::Written(frame_bytes)) => {
                                cut_bytes += frame_bytes;
                                if cut_bytes >= CUT_YIELD_BYTES {
                                    break DrainEnd::Cut;
                                }
                            }
                            Ok(WriteProgress::Pending) => break DrainEnd::Cut,
                            Err(encoding_status) => break DrainEnd::EncodingError(encoding_status),
                        }
                    }
                    Poll::Ready(Some(Err(stream_status))) => {
                        break DrainEnd::StreamStatus(stream_status)
                    }
                    Poll::Ready(None) => break DrainEnd::QueueEnded,
                    Poll::Pending => break DrainEnd::Cut,
                }
            }
        };
        match drain_end {
            DrainEnd::Cut => {
                stream_queue.release_drained_weight();
                self.frame_writer.end_cut();
            }
            DrainEnd::QueueEnded => {
                stream_queue.release_drained_weight();
                self.frame_writer.end_cut();
                self.end_stream(Status::ok(""));
            }
            DrainEnd::StreamStatus(stream_status) => {
                self.frame_writer.end_cut();
                self.end_stream(stream_status);
            }
            DrainEnd::EncodingError(encoding_status) => {
                self.frame_writer.discard_cut();
                self.end_stream(encoding_status);
            }
        }
        (self.frame_writer).fold_compression_counts_into(&DATA_PLANE_COUNTS);
    }

    /// Closes the queue and keeps `ending_status` for the trailers after the ready frames.
    fn end_stream(&mut self, ending_status: Status) {
        self.stream_queue = None;
        self.stream_ending = Some(ending_status);
    }
}

impl HttpBody for SubscribeResponseBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        self: Pin<&mut Self>,
        task_context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        let response_body = self.get_mut();
        if let Some(ready_frame) = response_body.frame_writer.ready_frames.pop_front() {
            return Poll::Ready(Some(Ok(Frame::data(ready_frame))));
        }
        if response_body.trailers_sent {
            return Poll::Ready(None);
        }
        if response_body.stream_ending.is_none() {
            response_body.drain_until_cut(task_context);
            if let Some(ready_frame) = response_body.frame_writer.ready_frames.pop_front() {
                return Poll::Ready(Some(Ok(Frame::data(ready_frame))));
            }
        }
        match response_body.stream_ending.take() {
            Some(ending_status) => {
                response_body.trailers_sent = true;
                Poll::Ready(Some(Ok(Frame::trailers(status_trailers(&ending_status)))))
            }
            None => {
                response_body.frame_writer.note_yield();
                Poll::Pending
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.trailers_sent
    }
}

impl StreamFrameWriter {
    /// Whether this compressed stream's next message is big enough to release the held weight
    /// before compressing it. A Block member's counts as big, so its length needs no walk.
    fn compresses_a_big_message(&self, queue_entry: &SubscribeQueueEntry) -> bool {
        matches!(self.stream_framing, StreamFraming::Compressed { .. })
            && (queue_entry.is_block_shape_entry()
                || queue_entry.encoded_len() >= self.copy_threshold_bytes)
    }

    const fn has_pending_write(&self) -> bool {
        self.pending_write_state.pending_write.is_some()
    }

    /// Records that the body returned Pending, so a queued processed job may now be taken back.
    const fn note_yield(&mut self) {
        if let Some(PendingWrite {
            wait_target:
                WaitTarget::GroupSlot {
                    yielded_since_hand_off,
                    ..
                },
            ..
        }) = &mut self.pending_write_state.pending_write
        {
            *yielded_since_hand_off = true;
        }
    }

    /// Writes one entry's frame. An entry whose shape or group claim has not ended waits in the
    /// stream's pending write, already parked or with the task woken.
    fn write_entry(
        &mut self,
        queue_entry: SubscribeQueueEntry,
        task_context: &mut Context<'_>,
    ) -> Result<WriteProgress, Status> {
        match self.try_write_entry(&queue_entry)? {
            EntryWrite::Written(frame_bytes) => Ok(WriteProgress::Written(frame_bytes)),
            EntryWrite::MustWait(wait_target) => {
                self.start_pending_write(queue_entry, wait_target);
                self.resume_pending_write(task_context)
            }
        }
    }

    /// One attempt at an entry: its content resolved once, then its identity, group or
    /// per-stream frame, or the claim it must wait for.
    fn try_write_entry(&mut self, queue_entry: &SubscribeQueueEntry) -> Result<EntryWrite, Status> {
        let resolved_content = match queue_entry {
            SubscribeQueueEntry::Indexed(update_ref) => {
                match update_ref.resolve_for_write(&self.block_shape_settings) {
                    Ok(resolved_content) => Some(resolved_content),
                    Err(ShapeWait { shape_slot_index }) => {
                        let wait_target = WaitTarget::ShapeSlot { shape_slot_index };
                        return Ok(EntryWrite::MustWait(wait_target));
                    }
                }
            }
            SubscribeQueueEntry::Filtered(_) => None,
        };
        let entry_content = match (queue_entry, &resolved_content) {
            (SubscribeQueueEntry::Indexed(_), Some(resolved_content)) => {
                EntryContent::Resolved(resolved_content)
            }
            (SubscribeQueueEntry::Filtered(filtered_update), _) => {
                EntryContent::Filtered(filtered_update)
            }
            (SubscribeQueueEntry::Indexed(_), None) => {
                unreachable!("an indexed entry resolves its content")
            }
        };
        if matches!(self.stream_framing, StreamFraming::Identity) {
            return self
                .write_identity_entry(entry_content)
                .map(EntryWrite::Written);
        }
        if let (
            SubscribeQueueEntry::Indexed(update_ref),
            EntryContent::Resolved(resolved_content),
        ) = (queue_entry, entry_content)
        {
            match self.write_through_group(update_ref, resolved_content)? {
                GroupWrite::Written(frame_bytes) => return Ok(EntryWrite::Written(frame_bytes)),
                GroupWrite::NotEligible => {}
                GroupWrite::MustWait {
                    group_slots,
                    group_slot_index,
                    queued_pool_job,
                } => {
                    return Ok(EntryWrite::MustWait(WaitTarget::GroupSlot {
                        group_slots,
                        group_slot_index,
                        queued_pool_job,
                        yielded_since_hand_off: false,
                    }))
                }
            }
        }
        self.write_per_stream_entry(entry_content)
            .map(EntryWrite::Written)
    }

    fn start_pending_write(&mut self, queue_entry: SubscribeQueueEntry, wait_target: WaitTarget) {
        let pending_write_slot = &mut self.pending_write_state.pending_write;
        debug_assert!(pending_write_slot.is_none(), "one pending write at a time");
        *pending_write_slot = Some(PendingWrite {
            queue_entry,
            wait_target,
            claim_end_wait: ClaimEndWait::default(),
        });
    }

    /// Moves the pending write on: a processed take-back once the body has yielded, then the wait
    /// for the claim's end, then the write, which may wait again. Every Pending has a waker.
    fn resume_pending_write(
        &mut self,
        task_context: &mut Context<'_>,
    ) -> Result<WriteProgress, Status> {
        loop {
            let StreamFrameWriter {
                stream_framing,
                compression_counters,
                pending_write_state,
                block_shape_settings,
                ..
            } = self;
            let PendingWriteState {
                pending_write: pending_write_slot,
                backstop_timer,
            } = pending_write_state;
            let Some(pending_write) = pending_write_slot.as_mut() else {
                return Ok(WriteProgress::Written(0));
            };
            let backstop_timer = backstop_timer.as_mut();
            let waited_slot = pending_write
                .wait_target
                .waited_slot(&pending_write.queue_entry);
            let claim_end = match &mut pending_write.wait_target {
                WaitTarget::ShapeSlot { shape_slot_index } => {
                    let shape_cell = (pending_write.queue_entry.block_shapes())
                        .and_then(|block_shapes| block_shapes.keyed_shape(*shape_slot_index))
                        .map(|keyed_shape| keyed_shape.shape_cell);
                    pending_write.claim_end_wait.poll_claim_end(
                        shape_cell,
                        waited_slot,
                        task_context,
                        backstop_timer,
                        block_shape_settings.waiter_backstop,
                        BackstopWaitCount {
                            stream_backstop_waits: &mut compression_counters.backstop_waits,
                            waited_claim: WaitedClaim::BlockShape,
                        },
                    )
                }
                WaitTarget::GroupSlot {
                    group_slots,
                    group_slot_index,
                    queued_pool_job,
                    yielded_since_hand_off,
                } => {
                    let StreamFraming::Compressed {
                        stream_compressor,
                        group_sharing: Some(group_sharing),
                        ..
                    } = &*stream_framing
                    else {
                        unreachable!("only a shared compressed stream waits for a group");
                    };
                    if let Some(processed_job) = queued_pool_job.as_ref() {
                        if !*yielded_since_hand_off {
                            task_context.waker().wake_by_ref();
                            return Ok(WriteProgress::Pending);
                        }
                        let mut compression_context = GroupCompressionContext {
                            stream_compressor: &**stream_compressor,
                            group_settings: &group_sharing.group_settings,
                            group_counters: compression_counters,
                            compressor_pools: None,
                        };
                        match take_back_queued_job(processed_job, &mut compression_context)
                            .map_err(compression_status)?
                        {
                            QueuedJobOutcome::PublishedInline(published_message) => {
                                let PendingWrite { queue_entry, .. } =
                                    pending_write_slot.take().expect("a pending write");
                                let frame_bytes = self.stage_compressed_frame(
                                    CompressedFrameSource::SharedMessage(
                                        published_message.compressed_bytes(),
                                    ),
                                )?;
                                count_published_body_use(&queue_entry);
                                return Ok(WriteProgress::Written(frame_bytes));
                            }
                            QueuedJobOutcome::RunsOnPool => *queued_pool_job = None,
                        }
                    }
                    pending_write.claim_end_wait.poll_claim_end(
                        group_slots.group_slot(*group_slot_index),
                        waited_slot,
                        task_context,
                        backstop_timer,
                        group_sharing.group_settings.waiter_backstop,
                        BackstopWaitCount {
                            stream_backstop_waits: &mut compression_counters.backstop_waits,
                            waited_claim: WaitedClaim::CompressionGroup,
                        },
                    )
                }
            };
            if claim_end.is_pending() {
                return Ok(WriteProgress::Pending);
            }
            let PendingWrite { queue_entry, .. } =
                pending_write_slot.take().expect("a pending write");
            match self.try_write_entry(&queue_entry)? {
                EntryWrite::Written(frame_bytes) => return Ok(WriteProgress::Written(frame_bytes)),
                EntryWrite::MustWait(wait_target) => {
                    let next_slot = wait_target.waited_slot(&queue_entry);
                    // A cell is claimed once, so an ended wait never names its slot again.
                    debug_assert_ne!(next_slot, waited_slot);
                    self.start_pending_write(queue_entry, wait_target);
                    if next_slot == waited_slot {
                        // Were it to, the writer yields instead of spinning in this poll.
                        task_context.waker().wake_by_ref();
                        return Ok(WriteProgress::Pending);
                    }
                }
            }
        }
    }

    /// Looks the member's group up and writes its frame, or says why it cannot: not a member of
    /// any group, or a claim that has not ended.
    fn write_through_group(
        &mut self,
        update_ref: &IndexedUpdateRef,
        resolved_content: &ResolvedFrameContent<'_>,
    ) -> Result<GroupWrite, Status> {
        let StreamFrameWriter {
            stream_framing,
            frame_staging,
            ready_frames,
            copy_threshold_bytes,
            max_message_bytes,
            compression_counters,
            ..
        } = self;
        let StreamFraming::Compressed {
            compressing_encoding,
            stream_compressor,
            group_sharing: Some(group_sharing),
        } = &*stream_framing
        else {
            return Ok(GroupWrite::NotEligible);
        };
        let Some(shared_body) = resolved_content.shared_body() else {
            return Ok(GroupWrite::NotEligible);
        };
        let matched_update = update_ref.matched_update();
        let member_filter = update_ref.member_filter();
        let Some(filtered_update_key) = FilteredUpdateKey::new(
            matched_update.update_body_variant,
            compressing_encoding.response_encoding(),
            member_filter.filter_generation_names(),
            update_ref.name_kind(),
            matched_update.filter_name_mask,
        ) else {
            return Ok(GroupWrite::NotEligible);
        };
        let in_flight_policy = match member_filter.get_commitment_level() {
            CommitmentLevel::Processed => InFlightGroupPolicy::CompressPrivately,
            CommitmentLevel::Confirmed | CommitmentLevel::Finalized => {
                InFlightGroupPolicy::WaitForClaimant
            }
        };
        let mut private_frame_result = Ok(0);
        let member_compression = compress_member(
            CompressedMember {
                shared_body,
                filtered_update_key: &filtered_update_key,
                compressing_encoding: *compressing_encoding,
                in_flight_policy,
            },
            &mut GroupCompressionContext {
                stream_compressor: &**stream_compressor,
                group_settings: &group_sharing.group_settings,
                group_counters: compression_counters,
                compressor_pools: Some(&group_sharing.compressor_pools),
            },
            &mut |private_copy| {
                private_frame_result = stage_compressed_frame(
                    CompressedFrameSource::CompressorScratch(private_copy),
                    frame_staging,
                    ready_frames,
                    *copy_threshold_bytes,
                    *max_message_bytes,
                );
            },
        )
        .map_err(compression_status)?;
        let group_write = match member_compression {
            MemberCompression::Shared(shared_message) => {
                GroupWrite::Written(stage_compressed_frame(
                    CompressedFrameSource::SharedMessage(shared_message.compressed_bytes()),
                    frame_staging,
                    ready_frames,
                    *copy_threshold_bytes,
                    *max_message_bytes,
                )?)
            }
            MemberCompression::Private(_) => GroupWrite::Written(private_frame_result?),
            MemberCompression::InFlight {
                group_slots,
                group_slot_index,
            } => {
                return Ok(GroupWrite::MustWait {
                    group_slots,
                    group_slot_index,
                    queued_pool_job: None,
                })
            }
            MemberCompression::PoolQueued {
                pooled_job,
                group_slots,
                group_slot_index,
            } => {
                return Ok(GroupWrite::MustWait {
                    group_slots,
                    group_slot_index,
                    queued_pool_job: Some(pooled_job),
                })
            }
        };
        resolved_content.count_published_body_use();
        Ok(group_write)
    }

    /// Writes an identity frame of one entry.
    fn write_identity_entry(
        &mut self,
        entry_content: EntryContent<'_, '_>,
    ) -> Result<usize, Status> {
        let StreamFrameWriter {
            frame_staging,
            ready_frames,
            copy_threshold_bytes,
            max_message_bytes,
            ..
        } = self;
        match entry_content {
            EntryContent::Resolved(resolved_content) => write_resolved_identity_frame(
                resolved_content,
                frame_staging,
                ready_frames,
                *copy_threshold_bytes,
                *max_message_bytes,
            ),
            EntryContent::Filtered(filtered_update) => {
                let filtered_len = filtered_update.encoded_len();
                frame_staging.prepare_room(FRAME_PREFIX_BYTES + filtered_len, ready_frames);
                let active_chunk = frame_staging.active_chunk_mut();
                let prefix_position = active_chunk.len();
                active_chunk.put_slice(&[0; FRAME_PREFIX_BYTES]);
                filtered_update.encode_raw(active_chunk);
                let message_len = active_chunk.len() - prefix_position - FRAME_PREFIX_BYTES;
                check_message_len(message_len, *max_message_bytes)?;
                write_prefix_length(active_chunk, prefix_position, message_len);
                Ok(FRAME_PREFIX_BYTES + message_len)
            }
        }
    }

    /// Compresses one entry for this stream alone, as one gzip member or zstd frame.
    fn write_per_stream_entry(
        &mut self,
        entry_content: EntryContent<'_, '_>,
    ) -> Result<usize, Status> {
        let StreamFrameWriter {
            stream_framing,
            frame_staging,
            ready_frames,
            copy_threshold_bytes,
            max_message_bytes,
            compression_counters,
            ..
        } = self;
        let StreamFraming::Compressed {
            compressing_encoding,
            stream_compressor,
            ..
        } = &*stream_framing
        else {
            unreachable!("only a compressed stream compresses per stream");
        };
        let frame_bytes = write_compressed_frame(
            &mut |message_scratch| match entry_content {
                EntryContent::Resolved(resolved_content) => {
                    resolved_content.write_content(&mut CopyingContentSink::new(message_scratch))
                }
                EntryContent::Filtered(filtered_update) => {
                    filtered_update.encode_raw(message_scratch)
                }
            },
            *compressing_encoding,
            stream_compressor.as_ref(),
            frame_staging,
            ready_frames,
            *copy_threshold_bytes,
            *max_message_bytes,
        )?;
        compression_counters.stream_compressions += 1;
        Ok(frame_bytes)
    }

    fn stage_compressed_frame(
        &mut self,
        frame_source: CompressedFrameSource<'_>,
    ) -> Result<usize, Status> {
        stage_compressed_frame(
            frame_source,
            &mut self.frame_staging,
            &mut self.ready_frames,
            self.copy_threshold_bytes,
            self.max_message_bytes,
        )
    }

    fn end_cut(&mut self) {
        self.frame_staging.end_cut(&mut self.ready_frames);
    }

    /// Adds what this writer counted per member since the last fold to `data_plane_counts`, once
    /// per drain, so a member's write adds to a plain counter of its own only.
    fn fold_compression_counts_into(&mut self, data_plane_counts: &DataPlaneCounts) {
        if self.compression_counters == self.folded_counters {
            return;
        }
        if let StreamFraming::Compressed {
            compressing_encoding,
            ..
        } = &self.stream_framing
        {
            let (counted, folded) = (&self.compression_counters, &self.folded_counters);
            let group_event = |group_event| {
                DataPlaneCount::CompressionGroupEvent(*compressing_encoding, group_event)
            };
            let compression = |compression_outcome| {
                DataPlaneCount::Compression(*compressing_encoding, compression_outcome)
            };
            let event_moves = [
                (
                    group_event(CompressionGroupEvent::SharedRead),
                    counted.shared_group_reads - folded.shared_group_reads,
                ),
                (
                    group_event(CompressionGroupEvent::DuplicateGroup),
                    counted.racing_duplicate_groups - folded.racing_duplicate_groups,
                ),
                (
                    compression(CompressionOutcome::Stream),
                    counted.stream_compressions - folded.stream_compressions,
                ),
            ];
            let private_moves = PrivateCompressionCause::ALL.map(|private_compression_cause| {
                let private_outcome = private_compression_outcome(private_compression_cause);
                let counted_private = counted.private_compressions(private_compression_cause);
                let folded_private = folded.private_compressions(private_compression_cause);
                (
                    compression(private_outcome),
                    counted_private - folded_private,
                )
            });
            for (data_plane_count, count_move) in event_moves.into_iter().chain(private_moves) {
                if count_move > 0 {
                    data_plane_counts.add(data_plane_count, count_move);
                }
            }
        }
        self.folded_counters = self.compression_counters;
    }

    /// Drops this cut's frames, as tonic drops its buffer on an encoding error.
    fn discard_cut(&mut self) {
        self.ready_frames.clear();
        self.frame_staging.discard_staged();
    }
}

impl WaitTarget {
    /// The slot this target names, for `queue_entry`, the entry that waits on it.
    fn waited_slot(&self, queue_entry: &SubscribeQueueEntry) -> WaitedSlot {
        match self {
            Self::ShapeSlot { shape_slot_index } => WaitedSlot::new(
                queue_entry
                    .block_shapes()
                    .expect("a shape wait is a Block member's"),
                usize::from(*shape_slot_index),
            ),
            Self::GroupSlot {
                group_slots,
                group_slot_index,
                ..
            } => WaitedSlot::new(&**group_slots, *group_slot_index),
        }
    }
}

impl Drop for PendingWriteState {
    fn drop(&mut self) {
        let Some(pending_write) = self.pending_write.as_mut() else {
            return;
        };
        let waited_slot = pending_write
            .wait_target
            .waited_slot(&pending_write.queue_entry);
        match &pending_write.wait_target {
            WaitTarget::ShapeSlot { shape_slot_index } => {
                let shape_cell = (pending_write.queue_entry.block_shapes())
                    .and_then(|block_shapes| block_shapes.keyed_shape(*shape_slot_index))
                    .map(|keyed_shape| keyed_shape.shape_cell);
                pending_write
                    .claim_end_wait
                    .cancel_wait(shape_cell, waited_slot);
            }
            WaitTarget::GroupSlot {
                group_slots,
                group_slot_index,
                ..
            } => {
                pending_write
                    .claim_end_wait
                    .cancel_wait(group_slots.group_slot(*group_slot_index), waited_slot);
            }
        }
    }
}

/// Counts a member's write of a published transaction body, as its identity write does.
fn count_published_body_use(queue_entry: &SubscribeQueueEntry) {
    if let SubscribeQueueEntry::Indexed(update_ref) = queue_entry {
        update_ref
            .resolve_frame_content(None)
            .count_published_body_use();
    }
}

fn compression_status(compression_error: CompressionError) -> Status {
    Status::internal(format!("Error compressing: {compression_error}"))
}

/// An identity frame of a routed member: the prefix, then the content, whose big shared
/// segments become frames of their own.
fn write_resolved_identity_frame(
    resolved_content: &ResolvedFrameContent<'_>,
    frame_staging: &mut FrameStaging,
    ready_frames: &mut VecDeque<Bytes>,
    copy_threshold_bytes: usize,
    max_message_bytes: usize,
) -> Result<usize, Status> {
    let content_len = resolved_content.content_len();
    check_message_len(content_len, max_message_bytes)?;
    let copied_len =
        FRAME_PREFIX_BYTES + content_len - resolved_content.referenced_len(copy_threshold_bytes);
    frame_staging.prepare_room(copied_len, ready_frames);
    let active_chunk = frame_staging.active_chunk_mut();
    let prefix_position = active_chunk.len();
    active_chunk.put_u8(0);
    active_chunk.put_u32(content_len as u32);
    let mut staging_sink = StagingContentSink {
        content_start_in_chunk: prefix_position + FRAME_PREFIX_BYTES,
        frame_staging,
        ready_frames,
        copy_threshold_bytes,
        split_content_bytes: 0,
        referenced_bytes: 0,
        split_within_frame: false,
    };
    resolved_content.write_content(&mut staging_sink);
    let written_content_len = staging_sink.written_content_len();
    if !staging_sink.split_within_frame {
        // The prefix is still staged, so it takes the length written, as tonic's does.
        check_message_len(written_content_len, max_message_bytes)?;
        write_prefix_length(
            staging_sink.frame_staging.active_chunk_mut(),
            prefix_position,
            written_content_len,
        );
    } else if written_content_len != content_len {
        return Err(Status::internal(format!(
            "Error encoding: wrote {written_content_len} bytes for a {content_len}-byte message"
        )));
    }
    Ok(FRAME_PREFIX_BYTES + written_content_len)
}

/// A frame of one stream's own compression: the message compressed as one gzip member or one
/// zstd frame, staged under the threshold and a frame of its own above it.
fn write_compressed_frame(
    write_message: &mut dyn FnMut(&mut Vec<u8>),
    compressing_encoding: CompressingEncoding,
    stream_compressor: &dyn WholeMessageCompressor,
    frame_staging: &mut FrameStaging,
    ready_frames: &mut VecDeque<Bytes>,
    copy_threshold_bytes: usize,
    max_message_bytes: usize,
) -> Result<usize, Status> {
    let mut frame_result = Ok(0);
    stream_compressor
        .compress_whole_message(
            compressing_encoding,
            write_message,
            &mut |compressed_scratch| {
                frame_result = stage_compressed_frame(
                    CompressedFrameSource::CompressorScratch(compressed_scratch),
                    frame_staging,
                    ready_frames,
                    copy_threshold_bytes,
                    max_message_bytes,
                );
            },
        )
        .map_err(compression_status)?;
    frame_result
}

/// Stages a compressed frame's prefix, then its message: copied under the threshold, and a
/// frame of its own above it, referenced when shared and copied exactly from a scratch.
fn stage_compressed_frame(
    frame_source: CompressedFrameSource<'_>,
    frame_staging: &mut FrameStaging,
    ready_frames: &mut VecDeque<Bytes>,
    copy_threshold_bytes: usize,
    max_message_bytes: usize,
) -> Result<usize, Status> {
    let compressed_message: &[u8] = match frame_source {
        CompressedFrameSource::SharedMessage(shared_message) => shared_message,
        CompressedFrameSource::CompressorScratch(compressor_scratch) => compressor_scratch,
    };
    let compressed_len = compressed_message.len();
    check_message_len(compressed_len, max_message_bytes)?;
    let copies_message = compressed_len < copy_threshold_bytes;
    let staged_len = FRAME_PREFIX_BYTES + if copies_message { compressed_len } else { 0 };
    frame_staging.prepare_room(staged_len, ready_frames);
    let active_chunk = frame_staging.active_chunk_mut();
    active_chunk.put_u8(1);
    active_chunk.put_u32(compressed_len as u32);
    if copies_message {
        active_chunk.put_slice(compressed_message);
    } else {
        frame_staging.split_staged_frame(ready_frames);
        ready_frames.push_back(match frame_source {
            CompressedFrameSource::SharedMessage(shared_message) => shared_message.clone(),
            // A copy holds the compressed bytes exactly; the scratch's capacity is about the
            // uncompressed size, and stays in the pool.
            CompressedFrameSource::CompressorScratch(compressor_scratch) => {
                Bytes::copy_from_slice(compressor_scratch)
            }
        });
    }
    Ok(FRAME_PREFIX_BYTES + compressed_len)
}

/// tonic's limits on a framed message's length, in its order (`finish_encoding`).
fn check_message_len(message_len: usize, max_message_bytes: usize) -> Result<(), Status> {
    if message_len > max_message_bytes {
        return Err(Status::out_of_range(format!(
            "Error, encoded message length too large: found {message_len} bytes, the limit is: \
             {max_message_bytes} bytes"
        )));
    }
    if message_len > u32::MAX as usize {
        return Err(Status::resource_exhausted(format!(
            "Cannot return body with more than 4GB of data but got {message_len} bytes"
        )));
    }
    Ok(())
}

/// Writes `message_len` into the identity prefix staged at `prefix_position`.
fn write_prefix_length(active_chunk: &mut BytesMut, prefix_position: usize, message_len: usize) {
    active_chunk[prefix_position + 1..prefix_position + FRAME_PREFIX_BYTES]
        .copy_from_slice(&(message_len as u32).to_be_bytes());
}

/// Copies a frame's pieces into the active chunk, and gives each shared segment at or above the
/// threshold a frame of its own, after the staged bytes before it.
struct StagingContentSink<'writer> {
    frame_staging: &'writer mut FrameStaging,
    ready_frames: &'writer mut VecDeque<Bytes>,
    copy_threshold_bytes: usize,
    content_start_in_chunk: usize,
    split_content_bytes: usize,
    referenced_bytes: usize,
    split_within_frame: bool,
}

impl StagingContentSink<'_> {
    /// The content bytes this frame has taken: copied, split off and referenced.
    fn written_content_len(&mut self) -> usize {
        let staged_content_bytes =
            self.frame_staging.active_chunk_mut().len() - self.content_start_in_chunk;
        self.split_content_bytes + staged_content_bytes + self.referenced_bytes
    }
}

impl FrameContentSink for StagingContentSink<'_> {
    type CopyBuffer = BytesMut;

    fn copy_buffer(&mut self) -> &mut BytesMut {
        self.frame_staging.active_chunk_mut()
    }

    fn put_shared_segment(&mut self, shared_segment: &Bytes) {
        if shared_segment.is_empty() {
            return;
        }
        if shared_segment.len() < self.copy_threshold_bytes {
            self.frame_staging
                .active_chunk_mut()
                .put_slice(shared_segment);
            return;
        }
        let staged_len = self.frame_staging.active_chunk_mut().len();
        self.split_content_bytes += staged_len - self.content_start_in_chunk;
        self.content_start_in_chunk = 0;
        self.split_within_frame = true;
        self.frame_staging.split_staged_frame(self.ready_frames);
        self.ready_frames.push_back(shared_segment.clone());
        self.referenced_bytes += shared_segment.len();
    }
}
