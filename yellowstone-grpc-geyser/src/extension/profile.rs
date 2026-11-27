//! The profile: one type that names the adapter of each hot slot and builds it at load.

use {
    crate::{
        body::StandardTransactionBodyWriter,
        compression::StandardCompressionCodec,
        config::CompressionDefaults,
        extension::{
            engine::StandardSubscribeEngine,
            seams::{CompressionCodec, ExtensionError, SubscribeEngine, TransactionBodyWriter},
        },
    },
    std::fmt,
};

/// The version of the extension API: the profile, the builder, the plugin type and the seams.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExtensionApiVersion {
    pub major_version: u16,
    pub minor_version: u16,
    pub patch_version: u16,
}

impl fmt::Display for ExtensionApiVersion {
    fn fmt(&self, display_formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            display_formatter,
            "{}.{}.{}",
            self.major_version, self.minor_version, self.patch_version
        )
    }
}

/// A new slot, a new required method or a changed signature raises the major version.
pub const EXTENSION_API_VERSION: ExtensionApiVersion = ExtensionApiVersion {
    major_version: 1,
    minor_version: 0,
    patch_version: 0,
};

/// Names the adapter of each hot slot. A library builds the plugin over one profile, so public
/// code calls every slot without dynamic dispatch.
pub trait DataPlaneProfile: fmt::Debug + Send + Sync + Sized + 'static {
    type SubscribeEngine: SubscribeEngine;
    type CompressionCodec: CompressionCodec;
    type TransactionBodyWriter: TransactionBodyWriter;

    /// Runs first at load, with the values in force: each compression key of the config over
    /// the builder's defaults.
    fn new_compression_codec(
        effective_compression_defaults: &CompressionDefaults,
    ) -> Result<Self::CompressionCodec, ExtensionError>;

    /// Runs at load, after the codec is built.
    fn new_transaction_body_writer() -> Result<Self::TransactionBodyWriter, ExtensionError>;

    /// Runs at load, after the transaction writer is built.
    fn new_subscribe_engine() -> Result<Self::SubscribeEngine, ExtensionError>;
}

/// One adapter for each hot slot of profile `P`, built when the plugin loads.
pub struct SlotAdapters<P: DataPlaneProfile> {
    pub subscribe_engine: P::SubscribeEngine,
    pub compression_codec: P::CompressionCodec,
    pub transaction_body_writer: P::TransactionBodyWriter,
}

impl<P: DataPlaneProfile> SlotAdapters<P> {
    /// Builds the codec, then the transaction writer, then the engine. The first error stops
    /// the build, and the adapters built before it drop.
    pub fn build_slot_adapters(
        effective_compression_defaults: &CompressionDefaults,
    ) -> Result<Self, ExtensionError> {
        let compression_codec = P::new_compression_codec(effective_compression_defaults)?;
        let transaction_body_writer = P::new_transaction_body_writer()?;
        let subscribe_engine = P::new_subscribe_engine()?;
        Ok(Self {
            subscribe_engine,
            compression_codec,
            transaction_body_writer,
        })
    }
}

impl<P: DataPlaneProfile> fmt::Debug for SlotAdapters<P> {
    fn fmt(&self, debug_formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_formatter
            .debug_struct("SlotAdapters")
            .finish_non_exhaustive()
    }
}

/// The standard adapters. The library's own `_create_plugin` builds the plugin over this profile.
#[derive(Debug)]
pub enum StandardProfile {}

impl DataPlaneProfile for StandardProfile {
    type SubscribeEngine = StandardSubscribeEngine;
    type CompressionCodec = StandardCompressionCodec;
    type TransactionBodyWriter = StandardTransactionBodyWriter;

    fn new_compression_codec(
        effective_compression_defaults: &CompressionDefaults,
    ) -> Result<Self::CompressionCodec, ExtensionError> {
        StandardCompressionCodec::new(effective_compression_defaults)
    }

    fn new_transaction_body_writer() -> Result<Self::TransactionBodyWriter, ExtensionError> {
        Ok(StandardTransactionBodyWriter)
    }

    fn new_subscribe_engine() -> Result<Self::SubscribeEngine, ExtensionError> {
        Ok(StandardSubscribeEngine::new())
    }
}
