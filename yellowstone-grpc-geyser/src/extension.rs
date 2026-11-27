//! The supported surface for crates that embed the plugin with their own adapters.

pub mod builder;
pub mod engine;
pub mod profile;
pub mod seams;

pub use {
    crate::{
        config::{CompressionDefaults, ZstdFrameForm},
        export_geyser_plugin,
        plugin::entry::GrpcPlugin,
    },
    builder::{HeapMemoryBacking, PluginBuilder, PluginIdentity, PluginParts},
    engine::{StandardSubscribeEngine, TonicServerEngine},
    profile::{
        DataPlaneProfile, ExtensionApiVersion, SlotAdapters, StandardProfile, EXTENSION_API_VERSION,
    },
    seams::{
        AccountDataSource, AccountFields, CompressionCodec, CompressionError, ExtensionError,
        ListenerHttp2Settings, MemoryBacking, MemoryBackingError, RegionPurpose, RegionRequest,
        ReservedRegion, SchedulingPolicy, SubscribeEngine, TelemetryExporter, ThreadPlacement,
        ThreadPlan, ThreadRole, TransactionBodyWriter,
    },
};
