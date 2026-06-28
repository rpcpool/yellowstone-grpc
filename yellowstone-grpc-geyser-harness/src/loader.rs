use agave_geyser_plugin_interface::geyser_plugin_interface::GeyserPlugin;
use libloading::{Library, Symbol};
use std::path::Path;

#[derive(Debug)]
pub enum LoadError {
    Dlopen(libloading::Error),
    OnLoad(String),
    ConfigRead(std::io::Error),
}

impl std::fmt::Display for LoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Dlopen(e) => write!(f, "dlopen failed: {e}"),
            Self::OnLoad(e) => write!(f, "on_load failed: {e}"),
            Self::ConfigRead(e) => write!(f, "config read failed: {e}"),
        }
    }
}

impl std::error::Error for LoadError {}

/// Loaded geyser plugin and its backing library.
///
/// Drop order matters: plugin drops first (destructor may call into .so),
/// then _library drops (unmaps .so).
pub struct PluginHandle {
    plugin: Box<dyn GeyserPlugin>,
    _library: Library,
}

impl PluginHandle {
    pub fn load(so_path: &Path, config_path: &Path) -> Result<Self, LoadError> {
        let library = unsafe { Library::new(so_path) }.map_err(LoadError::Dlopen)?;

        #[allow(improper_ctypes_definitions)]
        type CreatePlugin = unsafe extern "C" fn() -> *mut dyn GeyserPlugin;
        let create: Symbol<CreatePlugin> =
            unsafe { library.get(b"_create_plugin") }.map_err(LoadError::Dlopen)?;

        let raw = unsafe { create() };
        assert!(!raw.is_null(), "_create_plugin returned null");
        let mut plugin = unsafe { Box::from_raw(raw) };

        let config_str = config_path
            .to_str()
            .ok_or_else(|| LoadError::ConfigRead(
                std::io::Error::new(std::io::ErrorKind::InvalidData, "non-UTF8 path"),
            ))?;

        plugin
            .on_load(config_str, false)
            .map_err(|e| LoadError::OnLoad(format!("{e:?}")))?;

        Ok(Self {
            plugin,
            _library: library,
        })
    }

    pub fn plugin(&self) -> &dyn GeyserPlugin {
        self.plugin.as_ref()
    }
}