use {
    agave_geyser_plugin_interface::geyser_plugin_interface::GeyserPlugin, anyhow::Context,
    log::info, std::path::Path,
};

#[allow(improper_ctypes_definitions)]
type CreatePlugin = unsafe extern "C" fn() -> *mut dyn GeyserPlugin;

/// Loads a plugin the way agave does and keeps the library mapped for the life of the process.
pub fn load(library: &Path, config: &Path) -> anyhow::Result<&'static dyn GeyserPlugin> {
    // SAFETY: the plugin must come from the same rustc and interface crate versions as ygb;
    // build-plugin.sh builds both in one cargo invocation.
    let plugin = unsafe {
        let library = libloading::Library::new(library)
            .with_context(|| format!("failed to load {}", library.display()))?;
        let create: libloading::Symbol<CreatePlugin> = library
            .get(b"_create_plugin")
            .context("plugin has no _create_plugin symbol")?;
        let plugin = Box::from_raw(create());
        std::mem::forget(library);
        plugin
    };
    let mut plugin = plugin;
    let config = config.to_str().context("config path is not UTF-8")?;
    plugin
        .on_load(config, false)
        .map_err(|error| anyhow::anyhow!("on_load failed: {error}"))?;
    plugin
        .notify_end_of_startup()
        .map_err(|error| anyhow::anyhow!("notify_end_of_startup failed: {error}"))?;
    info!("plugin {} loaded", plugin.name());
    Ok(Box::leak(plugin))
}
