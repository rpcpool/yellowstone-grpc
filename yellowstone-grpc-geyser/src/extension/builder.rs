//! The plugin builder: an embedding crate picks its profile and sets its cold hooks here.

use {
    crate::{
        config::CompressionDefaults,
        extension::{
            profile::{DataPlaneProfile, StandardProfile},
            seams::{
                MemoryBacking, MemoryBackingError, RegionRequest, ReservedRegion,
                TelemetryExporter, ThreadPlacement,
            },
        },
        plugin::entry::GrpcPlugin,
        version::{Version, VERSION},
    },
    std::{
        alloc::{self, Layout},
        fmt, io,
        marker::PhantomData,
        ptr::NonNull,
        sync::Arc,
    },
};

/// The name the plugin reports to the validator and in its load log, and the build version of
/// its version metric. `GetVersion` always answers with the public crate's version.
#[derive(Debug, Clone, Copy)]
pub struct PluginIdentity {
    pub plugin_name: &'static str,
    pub build_version: &'static Version,
}

impl PluginIdentity {
    /// The public crate's name, version and git version, and its [`VERSION`].
    pub const PUBLIC: Self = Self {
        plugin_name: concat!(
            env!("CARGO_PKG_NAME"),
            "-",
            env!("CARGO_PKG_VERSION"),
            "+",
            env!("GIT_VERSION")
        ),
        build_version: &VERSION,
    };
}

/// What a builder hands its plugin: the build's compression defaults and its cold hooks.
pub struct PluginParts {
    /// The build's defaults; each compression key of the config overrides one field.
    pub compression_defaults: CompressionDefaults,
    /// `None` keeps the placement from config.
    pub thread_placement: Option<Arc<dyn ThreadPlacement>>,
    pub plugin_identity: PluginIdentity,
    /// The `grpc.data_plane.extensions` sections this build reads; any other section warns.
    pub claimed_extension_names: Vec<&'static str>,
    /// Hooks that append their own series to `/metrics`, in the order they were set.
    pub telemetry_exporters: Vec<Arc<dyn TelemetryExporter>>,
    /// `None` keeps the standard backing, [`HeapMemoryBacking`].
    pub memory_backing: Option<Arc<dyn MemoryBacking>>,
}

impl fmt::Debug for PluginParts {
    fn fmt(&self, debug_formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_formatter
            .debug_struct("PluginParts")
            .field("compression_defaults", &self.compression_defaults)
            .field("has_thread_placement", &self.thread_placement.is_some())
            .field("plugin_identity", &self.plugin_identity)
            .field("claimed_extension_names", &self.claimed_extension_names)
            .field("telemetry_exporter_count", &self.telemetry_exporters.len())
            .field("has_memory_backing", &self.memory_backing.is_some())
            .finish()
    }
}

/// Builds a [`GrpcPlugin`] over profile `P`. A builder with no hook set builds the public plugin.
pub struct PluginBuilder<P: DataPlaneProfile = StandardProfile> {
    plugin_parts: PluginParts,
    profile_marker: PhantomData<fn() -> P>,
}

impl<P: DataPlaneProfile> PluginBuilder<P> {
    pub const fn new() -> Self {
        Self {
            plugin_parts: PluginParts {
                compression_defaults: CompressionDefaults::PUBLIC,
                thread_placement: None,
                plugin_identity: PluginIdentity::PUBLIC,
                claimed_extension_names: Vec::new(),
                telemetry_exporters: Vec::new(),
                memory_backing: None,
            },
            profile_marker: PhantomData,
        }
    }

    /// Replaces [`CompressionDefaults::PUBLIC`]; the config's compression keys still override
    /// each field.
    pub const fn compression_defaults(mut self, compression_defaults: CompressionDefaults) -> Self {
        self.plugin_parts.compression_defaults = compression_defaults;
        self
    }

    /// Plans each plugin thread's CPUs and policy in place of the placement from config.
    pub fn thread_placement(mut self, thread_placement: impl ThreadPlacement) -> Self {
        self.plugin_parts.thread_placement = Some(Arc::new(thread_placement));
        self
    }

    /// Replaces [`PluginIdentity::PUBLIC`].
    pub const fn plugin_identity(mut self, plugin_identity: PluginIdentity) -> Self {
        self.plugin_parts.plugin_identity = plugin_identity;
        self
    }

    /// Lets the config carry `grpc.data_plane.extensions.<extension_name>` without a warning.
    pub fn claim_extension(mut self, extension_name: &'static str) -> Self {
        if !self
            .plugin_parts
            .claimed_extension_names
            .contains(&extension_name)
        {
            self.plugin_parts
                .claimed_extension_names
                .push(extension_name);
        }
        self
    }

    /// Adds a hook that appends its own series to `/metrics`, after the public series.
    pub fn telemetry_exporter(mut self, telemetry_exporter: impl TelemetryExporter) -> Self {
        self.plugin_parts
            .telemetry_exporters
            .push(Arc::new(telemetry_exporter));
        self
    }

    /// Reserves the plugin's memory regions in place of [`HeapMemoryBacking`].
    pub fn memory_backing(mut self, memory_backing: impl MemoryBacking) -> Self {
        self.plugin_parts.memory_backing = Some(Arc::new(memory_backing));
        self
    }

    pub fn build_plugin(self) -> GrpcPlugin<P> {
        GrpcPlugin::from_plugin_parts(self.plugin_parts)
    }
}

impl<P: DataPlaneProfile> Default for PluginBuilder<P> {
    fn default() -> Self {
        Self::new()
    }
}

impl<P: DataPlaneProfile> fmt::Debug for PluginBuilder<P> {
    fn fmt(&self, debug_formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_formatter
            .debug_struct("PluginBuilder")
            .field("plugin_parts", &self.plugin_parts)
            .finish()
    }
}

/// The standard [`MemoryBacking`]: zeroed heap memory on 4 KiB boundaries, never locked.
#[derive(Debug, Clone, Copy, Default)]
pub struct HeapMemoryBacking;

impl HeapMemoryBacking {
    pub const PAGE_BYTES: usize = 4096;
}

/// One heap region's allocation, which only the region's release touches.
struct HeapRegionAllocation {
    region_start: NonNull<u8>,
    region_layout: Layout,
}

// SAFETY: the allocation is plain memory, and only `release_allocation` uses the pointer.
unsafe impl Send for HeapRegionAllocation {}

impl HeapRegionAllocation {
    fn release_allocation(self) {
        // SAFETY: `alloc_zeroed` returned `region_start` for `region_layout`, and the region
        // runs its release once.
        unsafe { alloc::dealloc(self.region_start.as_ptr(), self.region_layout) }
    }
}

impl MemoryBacking for HeapMemoryBacking {
    fn reserve_region(
        &self,
        region_request: &RegionRequest,
    ) -> Result<ReservedRegion, MemoryBackingError> {
        let region_bytes = region_request.region_bytes;
        let region_layout = Layout::from_size_align(region_bytes.max(1), Self::PAGE_BYTES)
            .map_err(|_| MemoryBackingError::RegionNotServed {
                region_bytes,
                region_purpose: region_request.region_purpose,
            })?;
        // SAFETY: the layout's size is at least 1.
        let region_start = NonNull::new(unsafe { alloc::alloc_zeroed(region_layout) }).ok_or(
            MemoryBackingError::ReservationFailed {
                region_bytes,
                system_error: io::Error::from(io::ErrorKind::OutOfMemory),
            },
        )?;
        let heap_region_allocation = HeapRegionAllocation {
            region_start,
            region_layout,
        };
        let release_region = Box::new(move || heap_region_allocation.release_allocation());
        // SAFETY: the allocation holds `region_bytes` bytes, used by nothing else, until the
        // region's release frees it.
        Ok(unsafe {
            ReservedRegion::from_raw_parts(
                region_start,
                region_bytes,
                Self::PAGE_BYTES,
                false,
                release_region,
            )
        })
    }
}
