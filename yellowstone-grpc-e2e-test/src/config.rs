use {
    anyhow::Result,
    serde::Deserialize,
    std::{collections::HashMap, path::Path},
};

/// Top-level TOML config loaded from a file and threaded through every scenario run.
///
/// Structure expected in the file:
/// ```toml
/// [scenarios.<module>]
/// field = value          # module-level config, injected into every scenario
///
/// [scenarios.<module>.<scenario-name>]
/// field = value          # per-scenario config, injected only into that scenario
/// ```
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub scenarios: HashMap<String, toml::Value>,
}

impl Config {
    pub fn from_file(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("failed to read config file {}: {e}", path.display()))?;
        toml::from_str(&content)
            .map_err(|e| anyhow::anyhow!("failed to parse config file {}: {e}", path.display()))
    }

    /// Deserialize the TOML section `[scenarios.<module>]` into `T`.
    ///
    /// Returns `T::default()` if the section is absent. Any nested per-scenario
    /// subtables are silently ignored by serde unless the target type uses
    /// `#[serde(deny_unknown_fields)]`.
    pub fn module_config<T: serde::de::DeserializeOwned + Default>(
        &self,
        module: &str,
    ) -> Result<T> {
        let Some(value) = self.scenarios.get(module) else {
            return Ok(T::default());
        };
        T::deserialize(value.clone())
            .map_err(|e| anyhow::anyhow!("failed to deserialize [scenarios.{module}]: {e}"))
    }

    /// Deserialize the TOML section `[scenarios.<module>.<scenario>]` into `T`.
    ///
    /// Returns `T::default()` if the module or scenario section is absent.
    pub fn scenario_config<T: serde::de::DeserializeOwned + Default>(
        &self,
        module: &str,
        scenario: &str,
    ) -> Result<T> {
        let Some(module_value) = self.scenarios.get(module) else {
            return Ok(T::default());
        };
        let Some(value) = module_value.get(scenario) else {
            return Ok(T::default());
        };
        T::deserialize(value.clone()).map_err(|e| {
            anyhow::anyhow!("failed to deserialize [scenarios.{module}.{scenario}]: {e}")
        })
    }
}
