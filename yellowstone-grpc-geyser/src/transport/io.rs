//! Coalescing connection IO: a write whose slices are all small is copied into the connection's
//! staging, and the pass owner writes the staging once per pass; any other write goes out at once,
//! staged bytes first, in one vectored write.

use {
    std::{
        io::{self, IoSlice},
        pin::Pin,
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc,
        },
        task::{Context, Poll},
    },
    tokio::io::{AsyncRead, AsyncWrite, ReadBuf},
    tonic::transport::server::Connected,
    yellowstone_grpc_concurrency::single_task_cell::SingleTaskCell,
};

/// Slices at or below this many bytes are copied into staging (arch V12).
pub const DEFAULT_COPY_THRESHOLD_BYTES: usize = 32 * 1024;

/// The most bytes one connection stages before a write has to go out (arch 10.1).
pub const DEFAULT_STAGING_CAP_BYTES: usize = 1024 * 1024;

/// What staging reserves at its first staged write; it doubles up to the cap from there.
const INITIAL_STAGING_BYTES: usize = 64 * 1024;

/// The caller's slices one vectored write carries after the staged bytes; h2 passes at most 64.
const MAX_CALLER_SLICES: usize = 64;

/// Overlapping entries of any connection's IO, which only a bug can cause.
pub static CONNECTION_IO_ENTRY_OVERLAPS: AtomicU64 = AtomicU64::new(0);

static ENTRY_OVERLAP_LOGGED: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoalescingIoSettings {
    pub copy_threshold_bytes: usize,
    pub staging_cap_bytes: usize,
}

impl Default for CoalescingIoSettings {
    fn default() -> Self {
        Self {
            copy_threshold_bytes: DEFAULT_COPY_THRESHOLD_BYTES,
            staging_cap_bytes: DEFAULT_STAGING_CAP_BYTES,
        }
    }
}

impl CoalescingIoSettings {
    /// Why these settings cannot run, if they cannot: the cap must be above 0 and at least the
    /// copy threshold.
    pub const fn problem(&self) -> Option<&'static str> {
        if self.staging_cap_bytes == 0 {
            Some("staging_cap_bytes must be above 0")
        } else if self.copy_threshold_bytes > self.staging_cap_bytes {
            Some("copy_threshold_bytes must not exceed staging_cap_bytes")
        } else {
            None
        }
    }
}

/// A connection's staged bytes, `[read_position, len)` of one buffer that is never compacted: a
/// staging whose tail is full waits for a write that empties it.
#[derive(Debug)]
struct ConnectionStaging {
    staging_buffer: Vec<u8>,
    read_position: usize,
    staging_cap_bytes: usize,
}

impl ConnectionStaging {
    /// Staging that allocates at its first staged write.
    const fn new(staging_cap_bytes: usize) -> Self {
        Self {
            staging_buffer: Vec::new(),
            read_position: 0,
            staging_cap_bytes,
        }
    }

    const fn staged_byte_count(&self) -> usize {
        self.staging_buffer.len() - self.read_position
    }

    fn staged_bytes(&self) -> &[u8] {
        &self.staging_buffer[self.read_position..]
    }

    /// Copies every slice, or none when they do not fit. Grows only while nothing has been
    /// consumed from the front, so a warm connection never allocates here.
    fn try_stage(&mut self, caller_slices: &[IoSlice<'_>], caller_len: usize) -> bool {
        let used_len = self.staging_buffer.len();
        if used_len + caller_len > self.staging_buffer.capacity() {
            if self.read_position != 0 || used_len + caller_len > self.staging_cap_bytes {
                return false;
            }
            let grown_capacity = (self.staging_buffer.capacity() * 2)
                .max(INITIAL_STAGING_BYTES)
                .max(used_len + caller_len)
                .min(self.staging_cap_bytes);
            self.staging_buffer.reserve_exact(grown_capacity - used_len);
        }
        for caller_slice in caller_slices {
            self.staging_buffer.extend_from_slice(caller_slice);
        }
        true
    }

    /// Once every staged byte is written, staging starts again from the front of the buffer.
    fn consume(&mut self, consumed_bytes: usize) {
        self.read_position += consumed_bytes;
        if self.read_position == self.staging_buffer.len() {
            self.staging_buffer.clear();
            self.read_position = 0;
        }
    }
}

/// Calls on the inner IO and bytes moved, per connection. Under TLS a call is not a syscall.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ConnectionWriteCounters {
    pub write_calls: u64,
    pub partial_writes: u64,
    pub copied_bytes: u64,
    pub referenced_bytes: u64,
}

struct ConnectionIoState<InnerIo> {
    inner_io: InnerIo,
    connection_staging: ConnectionStaging,
    copy_threshold_bytes: usize,
    write_through: bool,
    latched_error: Option<io::ErrorKind>,
    write_counters: ConnectionWriteCounters,
}

impl<InnerIo: AsyncWrite + Unpin> ConnectionIoState<InnerIo> {
    fn latch(&mut self, io_error: io::Error) -> io::Error {
        self.latched_error.get_or_insert(io_error.kind());
        io_error
    }

    fn poll_write_slices(
        &mut self,
        task_context: &mut Context<'_>,
        caller_slices: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        if let Some(error_kind) = self.latched_error {
            return Poll::Ready(Err(io::Error::from(error_kind)));
        }
        let leading_empty_slices = caller_slices
            .iter()
            .take_while(|caller_slice| caller_slice.is_empty())
            .count();
        let caller_slices = &caller_slices[leading_empty_slices..];
        let caller_slices = &caller_slices[..caller_slices.len().min(MAX_CALLER_SLICES)];
        let caller_len: usize = caller_slices
            .iter()
            .map(|caller_slice| caller_slice.len())
            .sum();
        if caller_len == 0 {
            return Poll::Ready(Ok(0));
        }
        let all_slices_small = caller_slices
            .iter()
            .all(|caller_slice| caller_slice.len() <= self.copy_threshold_bytes);
        loop {
            let staging_empty = self.connection_staging.staged_byte_count() == 0;
            if all_slices_small
                && !(self.write_through && staging_empty)
                && self.connection_staging.try_stage(caller_slices, caller_len)
            {
                self.write_counters.copied_bytes += caller_len as u64;
                return Poll::Ready(Ok(caller_len));
            }
            let staged_len = self.connection_staging.staged_byte_count();
            let write_result = {
                let mut write_slices = [IoSlice::new(&[]); MAX_CALLER_SLICES + 1];
                let mut slice_count = 0;
                if staged_len > 0 {
                    write_slices[0] = IoSlice::new(self.connection_staging.staged_bytes());
                    slice_count = 1;
                }
                for caller_slice in caller_slices {
                    write_slices[slice_count] = IoSlice::new(caller_slice);
                    slice_count += 1;
                }
                Pin::new(&mut self.inner_io)
                    .poll_write_vectored(task_context, &write_slices[..slice_count])
            };
            let written_len = match write_result {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(io_error)) => return Poll::Ready(Err(self.latch(io_error))),
                Poll::Ready(Ok(0)) => {
                    let write_zero = io::Error::from(io::ErrorKind::WriteZero);
                    return Poll::Ready(Err(self.latch(write_zero)));
                }
                Poll::Ready(Ok(written_len)) => written_len,
            };
            self.write_counters.write_calls += 1;
            if written_len < staged_len + caller_len {
                self.write_counters.partial_writes += 1;
            }
            if written_len <= staged_len {
                self.connection_staging.consume(written_len);
                continue;
            }
            self.connection_staging.consume(staged_len);
            let caller_written = written_len - staged_len;
            self.write_counters.referenced_bytes += caller_written as u64;
            return Poll::Ready(Ok(caller_written));
        }
    }

    /// Writes the staged bytes, then flushes the inner IO, which may hold bytes of its own (TLS).
    fn poll_flush_pass(&mut self, task_context: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Some(error_kind) = self.latched_error {
            return Poll::Ready(Err(io::Error::from(error_kind)));
        }
        while self.connection_staging.staged_byte_count() > 0 {
            let staged_len = self.connection_staging.staged_byte_count();
            let write_result = Pin::new(&mut self.inner_io)
                .poll_write(task_context, self.connection_staging.staged_bytes());
            match write_result {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(io_error)) => return Poll::Ready(Err(self.latch(io_error))),
                Poll::Ready(Ok(0)) => {
                    let write_zero = io::Error::from(io::ErrorKind::WriteZero);
                    return Poll::Ready(Err(self.latch(write_zero)));
                }
                Poll::Ready(Ok(written_len)) => {
                    self.write_counters.write_calls += 1;
                    if written_len < staged_len {
                        self.write_counters.partial_writes += 1;
                    }
                    self.connection_staging.consume(written_len);
                }
            }
        }
        match Pin::new(&mut self.inner_io).poll_flush(task_context) {
            Poll::Ready(Err(io_error)) => Poll::Ready(Err(self.latch(io_error))),
            other_outcome => other_outcome,
        }
    }
}

fn entry_overlap_error() -> io::Error {
    CONNECTION_IO_ENTRY_OVERLAPS.fetch_add(1, Ordering::Relaxed);
    if !ENTRY_OVERLAP_LOGGED.swap(true, Ordering::Relaxed) {
        log::error!("a connection's IO was entered while another use was in progress");
    }
    io::Error::other("connection IO entered twice")
}

/// The IO handed to hyper or h2. Small writes are staged until the pass owner's flush.
pub struct CoalescingConnectionIo<InnerIo: Connected> {
    shared_state: Arc<SingleTaskCell<ConnectionIoState<InnerIo>>>,
    connect_info: InnerIo::ConnectInfo,
}

/// The pass owner's handle: one flush per pass, the counters and the write-through switch.
/// While it lives, the connection's socket stays open, so drop it with the connection.
#[must_use = "staged bytes leave only through this handle's flush pass"]
pub struct ConnectionFlushHandle<InnerIo> {
    shared_state: Arc<SingleTaskCell<ConnectionIoState<InnerIo>>>,
}

/// Wraps each accepted IO into its coalescing IO and the pass owner's flush handle. Wrap the
/// outermost IO only: under TLS, the TLS stream after its handshake.
#[derive(Debug, Clone, Copy, Default)]
pub struct CoalescingIoFactory {
    pub coalescing_io_settings: CoalescingIoSettings,
}

impl CoalescingIoFactory {
    #[must_use = "the connection IO and its flush handle are used together"]
    pub fn wrap<InnerIo: Connected>(
        &self,
        inner_io: InnerIo,
    ) -> (
        CoalescingConnectionIo<InnerIo>,
        ConnectionFlushHandle<InnerIo>,
    ) {
        let connect_info = inner_io.connect_info();
        let shared_state = Arc::new(SingleTaskCell::new(ConnectionIoState {
            inner_io,
            connection_staging: ConnectionStaging::new(
                self.coalescing_io_settings.staging_cap_bytes,
            ),
            copy_threshold_bytes: self.coalescing_io_settings.copy_threshold_bytes,
            write_through: false,
            latched_error: None,
            write_counters: ConnectionWriteCounters::default(),
        }));
        let flush_handle = ConnectionFlushHandle {
            shared_state: Arc::clone(&shared_state),
        };
        let coalescing_io = CoalescingConnectionIo {
            shared_state,
            connect_info,
        };
        (coalescing_io, flush_handle)
    }
}

impl<InnerIo: AsyncWrite + Unpin> ConnectionFlushHandle<InnerIo> {
    /// Writes what the pass staged and flushes the inner IO. Call it after every poll of the
    /// connection, `Ready` included; `Pending` keeps the task registered for the socket.
    pub fn poll_flush_pass(&self, task_context: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.shared_state
            .try_with(|io_state| io_state.poll_flush_pass(task_context))
            .unwrap_or_else(|| Poll::Ready(Err(entry_overlap_error())))
    }

    pub fn staged_byte_count(&self) -> Option<usize> {
        self.shared_state
            .try_with(|io_state| io_state.connection_staging.staged_byte_count())
    }

    pub fn write_counters(&self) -> Option<ConnectionWriteCounters> {
        self.shared_state
            .try_with(|io_state| io_state.write_counters)
    }

    /// With write-through on, a write finding the staging empty goes straight to the inner IO.
    pub fn set_write_through(&self, write_through: bool) -> io::Result<()> {
        self.shared_state
            .try_with(|io_state| io_state.write_through = write_through)
            .ok_or_else(entry_overlap_error)
    }
}

impl<InnerIo: Connected + AsyncRead + Unpin> AsyncRead for CoalescingConnectionIo<InnerIo> {
    fn poll_read(
        self: Pin<&mut Self>,
        task_context: &mut Context<'_>,
        read_buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.shared_state
            .try_with(|io_state| Pin::new(&mut io_state.inner_io).poll_read(task_context, read_buf))
            .unwrap_or_else(|| Poll::Ready(Err(entry_overlap_error())))
    }
}

impl<InnerIo: Connected + AsyncWrite + Unpin> AsyncWrite for CoalescingConnectionIo<InnerIo> {
    fn poll_write(
        self: Pin<&mut Self>,
        task_context: &mut Context<'_>,
        write_buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.shared_state
            .try_with(|io_state| {
                io_state.poll_write_slices(task_context, &[IoSlice::new(write_buf)])
            })
            .unwrap_or_else(|| Poll::Ready(Err(entry_overlap_error())))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        task_context: &mut Context<'_>,
        write_slices: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.shared_state
            .try_with(|io_state| io_state.poll_write_slices(task_context, write_slices))
            .unwrap_or_else(|| Poll::Ready(Err(entry_overlap_error())))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    /// The pass owner flushes; once its handle is gone, this flushes itself. A latched error
    /// still reaches the caller here.
    fn poll_flush(self: Pin<&mut Self>, task_context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let flush_handle_dropped = Arc::strong_count(&self.shared_state) == 1;
        self.shared_state
            .try_with(|io_state| match io_state.latched_error {
                Some(error_kind) => Poll::Ready(Err(io::Error::from(error_kind))),
                None if flush_handle_dropped => io_state.poll_flush_pass(task_context),
                None => Poll::Ready(Ok(())),
            })
            .unwrap_or_else(|| Poll::Ready(Err(entry_overlap_error())))
    }

    fn poll_shutdown(self: Pin<&mut Self>, task_context: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.shared_state
            .try_with(|io_state| match io_state.poll_flush_pass(task_context) {
                Poll::Ready(Ok(())) => Pin::new(&mut io_state.inner_io).poll_shutdown(task_context),
                other_outcome => other_outcome,
            })
            .unwrap_or_else(|| Poll::Ready(Err(entry_overlap_error())))
    }
}

impl<InnerIo: Connected> Connected for CoalescingConnectionIo<InnerIo> {
    type ConnectInfo = InnerIo::ConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.connect_info.clone()
    }
}
