//! Every seam trait an embedding crate implements, and the plain types their signatures name.
//! Slots are a profile's associated types; hooks are trait objects on the builder.

use {
    crate::plugin::message::MessageTransactionInfo,
    bytes::Bytes,
    futures::Stream,
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    std::{borrow::Cow, error::Error, fmt, future::Future, io, ptr::NonNull, time::Duration},
    tokio::io::{AsyncRead, AsyncWrite},
    tokio_util::sync::CancellationToken,
    tonic::{service::Routes, transport::server::Connected},
};

/// Slot: serves the gRPC listeners, from accepting connections to writing response frames.
pub trait SubscribeEngine: Send + Sync + Sized + 'static {
    /// Serves each connection `incoming_connections` yields with `listener_routes`, until
    /// `serve_shutdown` is cancelled.
    fn serve_listener<IncomingConnections, ConnectionIo>(
        &self,
        incoming_connections: IncomingConnections,
        listener_http2_settings: ListenerHttp2Settings,
        listener_routes: Routes,
        serve_shutdown: CancellationToken,
    ) -> impl Future<Output = Result<(), ExtensionError>> + Send + 'static
    where
        IncomingConnections: Stream<Item = io::Result<ConnectionIo>> + Send + 'static,
        ConnectionIo: Connected + AsyncRead + AsyncWrite + Unpin + Send + 'static;

    /// Ends every connection the engine serves and joins every task and thread it started. Open
    /// streams run on until they end, so it may wait; call it off the runtime that serves them.
    fn stop_and_join(&self);
}

/// The HTTP/2 settings of one listener; `None` keeps the transport's default.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ListenerHttp2Settings {
    pub adaptive_window: Option<bool>,
    pub keepalive_interval: Option<Duration>,
    pub keepalive_timeout: Option<Duration>,
    pub initial_connection_window_size: Option<u32>,
    pub initial_stream_window_size: Option<u32>,
}

/// Slot: compresses one whole message into one gzip member or one zstd frame.
pub trait CompressionCodec: Send + Sync + Sized + 'static {
    /// Contexts made once and reused for every call; a pool may move them between threads.
    type CodecThreadState: Send + 'static;

    fn new_codec_thread_state(&self) -> Self::CodecThreadState;

    /// Appends one gzip member of the segments, in order (names, then body).
    fn compress_gzip_member(
        &self,
        codec_thread_state: &mut Self::CodecThreadState,
        uncompressed_segments: &[&[u8]],
        compressed_output: &mut Vec<u8>,
    ) -> Result<(), CompressionError>;

    /// Appends one zstd frame of the segments, in order, in the codec's frame form.
    fn compress_zstd_frame(
        &self,
        codec_thread_state: &mut Self::CodecThreadState,
        uncompressed_segments: &[&[u8]],
        compressed_output: &mut Vec<u8>,
    ) -> Result<(), CompressionError>;
}

/// Why a codec could not compress a message.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum CompressionError {
    #[error("gzip compression failed: {0}")]
    GzipMemberFailed(#[source] io::Error),
    #[error("zstd compression failed: {0}")]
    ZstdFrameFailed(#[source] io::Error),
}

/// Slot: writes a transaction's `TransactionInfo` bytes, once per transaction, behind its
/// claim word. It must not lock, block or unwind.
pub trait TransactionBodyWriter: Send + Sync + Sized + 'static {
    /// Appends `transaction_info` exactly as prost encodes the `TransactionInfo` message.
    fn write_transaction_info(
        &self,
        transaction_info: &MessageTransactionInfo,
        transaction_info_bytes: &mut Vec<u8>,
    );

    /// The length [`Self::write_transaction_info`] appends, computed without writing: a body is
    /// sized from it before the write.
    fn transaction_info_len(&self, transaction_info: &MessageTransactionInfo) -> usize;
}

/// The fields of one account write, as capture reads them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccountFields {
    pub account_pubkey: Pubkey,
    pub account_lamports: u64,
    pub owner_pubkey: Pubkey,
    pub is_executable: bool,
    pub rent_epoch: u64,
    pub write_version: u64,
    pub transaction_signature: Option<Signature>,
}

/// Capture input: one account's data.
pub trait AccountDataSource {
    fn account_data_bytes(&self) -> &[u8];

    /// `Some` keeps the data by reference; the public build copies it into the body.
    fn share_account_data(&self) -> Option<Bytes> {
        None
    }
}

impl AccountDataSource for &[u8] {
    fn account_data_bytes(&self) -> &[u8] {
        self
    }
}

/// Hook: plans each plugin thread's CPUs and policy. The thread's name comes from its role.
pub trait ThreadPlacement: Send + Sync + 'static {
    /// Runs on the spawning thread.
    fn place_thread(&self, thread_role: ThreadRole, thread_ordinal: u16) -> ThreadPlan;

    /// Runs first on the new thread, after its plan is applied.
    fn on_thread_start(&self, _thread_role: ThreadRole, _thread_ordinal: u16) {}
}

/// What a plugin thread does. Each role's thread name, shown here, is a contract.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ThreadRole {
    /// `solGeyserGrpcNN`: the delivery runtime's workers and its blocking pool.
    DeliveryWorker,
    /// `solGrpcFiltNN`: the filter compile lane.
    FilterCompileWorker,
    /// `solGrpcMetrics`: serves and renders `/metrics` on its own runtime, and writes the summary
    /// line.
    MetricsRender,
    /// `solGrpcZipPrNN`: compresses big processed messages.
    ProcessedCompressionWorker,
    /// `solGrpcZipCfNN`: builds block shapes and compresses big confirmed and finalized messages.
    ConfirmedCompressionWorker,
    /// `solGrpcBankDrop`: frees evicted banks and builds bank key indexes.
    BankRelease,
    /// `solGrpcBlockMet`: the block meta storage and its runtime.
    BlockMetaStorage,
}

/// Where and how a plugin thread runs.
#[non_exhaustive]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ThreadPlan {
    /// `None` keeps the process mask read at load.
    pub allowed_cpus: Option<Vec<usize>>,
    pub scheduling_policy: SchedulingPolicy,
}

/// The scheduling policy a thread sets for itself at start; it never inherits one.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SchedulingPolicy {
    /// SCHED_OTHER at nice 0.
    #[default]
    Other,
    Fifo {
        fifo_priority: u8,
    },
}

/// Hook: runs at scrape time on the metrics thread, after the public series.
pub trait TelemetryExporter: Send + Sync + 'static {
    /// Appends the hook's series to `metrics_text`, in Prometheus text format.
    fn export_extra_series(&self, metrics_text: &mut String);
}

/// Hook: reserves memory regions at load, before any data-plane thread starts.
pub trait MemoryBacking: Send + Sync + 'static {
    fn reserve_region(
        &self,
        region_request: &RegionRequest,
    ) -> Result<ReservedRegion, MemoryBackingError>;
}

/// What a reserved region holds.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionPurpose {
    CaptureJournal,
    StagingBuffers,
    CompressedMessages,
    ColdReplayRing,
    IndexMetadata,
}

#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegionRequest {
    pub region_bytes: usize,
    pub region_purpose: RegionPurpose,
    pub numa_node: Option<u32>,
}

impl RegionRequest {
    pub const fn new(region_bytes: usize, region_purpose: RegionPurpose) -> Self {
        Self {
            region_bytes,
            region_purpose,
            numa_node: None,
        }
    }
}

/// A region a [`MemoryBacking`] reserved. Dropping it releases the region, once.
pub struct ReservedRegion {
    region_start: NonNull<u8>,
    region_bytes: usize,
    page_bytes: usize,
    is_locked: bool,
    release_region: Option<Box<dyn FnOnce() + Send>>,
}

// SAFETY: the region is plain memory that this value owns; `release_region` is `Send`.
unsafe impl Send for ReservedRegion {}
// SAFETY: a shared reference only reads the region's description.
unsafe impl Sync for ReservedRegion {}

impl ReservedRegion {
    /// # Safety
    /// The region stays valid for `region_bytes` bytes, and unused by others, until release.
    pub unsafe fn from_raw_parts(
        region_start: NonNull<u8>,
        region_bytes: usize,
        page_bytes: usize,
        is_locked: bool,
        release_region: Box<dyn FnOnce() + Send>,
    ) -> Self {
        Self {
            region_start,
            region_bytes,
            page_bytes,
            is_locked,
            release_region: Some(release_region),
        }
    }

    pub const fn region_start(&self) -> NonNull<u8> {
        self.region_start
    }

    pub const fn region_bytes(&self) -> usize {
        self.region_bytes
    }

    pub const fn page_bytes(&self) -> usize {
        self.page_bytes
    }

    pub const fn is_locked(&self) -> bool {
        self.is_locked
    }
}

impl Drop for ReservedRegion {
    fn drop(&mut self) {
        if let Some(release_region) = self.release_region.take() {
            release_region();
        }
    }
}

impl fmt::Debug for ReservedRegion {
    fn fmt(&self, debug_formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_formatter
            .debug_struct("ReservedRegion")
            .field("region_start", &self.region_start)
            .field("region_bytes", &self.region_bytes)
            .field("page_bytes", &self.page_bytes)
            .field("is_locked", &self.is_locked)
            .finish_non_exhaustive()
    }
}

/// Why a [`MemoryBacking`] reserved no region.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum MemoryBackingError {
    #[error("the backing serves no region of {region_bytes} bytes for {region_purpose:?}")]
    RegionNotServed {
        region_bytes: usize,
        region_purpose: RegionPurpose,
    },
    #[error("reserving {region_bytes} bytes failed: {system_error}")]
    ReservationFailed {
        region_bytes: usize,
        #[source]
        system_error: io::Error,
    },
}

/// An adapter's failure, at load or while it serves.
#[derive(Debug)]
pub struct ExtensionError {
    failure_context: Cow<'static, str>,
    failure_source: Option<Box<dyn Error + Send + Sync + 'static>>,
}

impl ExtensionError {
    pub fn new(failure_context: impl Into<Cow<'static, str>>) -> Self {
        Self {
            failure_context: failure_context.into(),
            failure_source: None,
        }
    }

    pub fn with_source(
        failure_context: impl Into<Cow<'static, str>>,
        failure_source: impl Into<Box<dyn Error + Send + Sync + 'static>>,
    ) -> Self {
        Self {
            failure_context: failure_context.into(),
            failure_source: Some(failure_source.into()),
        }
    }
}

impl fmt::Display for ExtensionError {
    fn fmt(&self, display_formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.failure_source {
            Some(failure_source) => write!(
                display_formatter,
                "{}: {failure_source}",
                self.failure_context
            ),
            None => display_formatter.write_str(&self.failure_context),
        }
    }
}

impl Error for ExtensionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.failure_source
            .as_deref()
            .map(|failure_source| failure_source as &(dyn Error + 'static))
    }
}
