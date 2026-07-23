
# yellowstone-grpc-intg-test

End-to-end (integration) scenario crate for Yellowstone gRPC.  
Scenarios are regular async Rust functions registered with `#[test_helper(...)]` and discovered at runtime through `inventory`.

The CLI binary is `yellowstone-e2e` and supports:
- listing scenarios
- running all scenarios (with filters)
- running one scenario by name

## Crate layout

- `src/scenarios.rs`: shared scenario registry (`Scenario`, `RunConfig`) and module declarations.
- `src/scenarios/default.rs`: default scenario module.
- `src/scenarios/extra.rs`: extra scenario module.
- `src/scenarios/ratelimit.rs`: rate-limit scenarios.
- `src/bin/yellowstone-e2e.rs`: CLI entrypoint.
- `src/config.rs`: TOML config loading for module/scenario-specific config.

## Add a new scenario

1. Pick a module file under `src/scenarios/` (for example `default.rs` or `extra.rs`).
2. Add an async function returning `anyhow::Result<()>`.
3. Add `#[test_helper(name = "stable-scenario-name")]`.
4. Add a doc comment (`/// ...`) for the human-readable description, or set `description = "..."` in the macro.
5. Optional: add tags with `tags = ["tag1", "tag2"]`.
6. Use one of the supported signatures below.

Minimal example:

```rust
use anyhow::Result;
use yellowstone_grpc_e2e_macros::test_helper;

#[test_helper(name = "my-scenario", tags = ["smoke", "accounts"])]
pub async fn my_scenario(config: &crate::scenarios::RunConfig) -> Result<()> {
		let mut client = crate::grpc::new_client(config).await?;
		let _version = client.get_version().await?;
		Ok(())
}
```

## `#[test_helper]` macro usage

Supported attribute arguments:
- `name = "..."` (required)
- `description = "..."` (optional)
- `tags = ["..."]` (optional)
- `config = Type` (optional; required for 3-arg signature)

Supported function signatures:

1. `fn(config: &RunConfig)`
2. `fn(config: &RunConfig, module_cfg: &ModuleConfig)`
3. `fn(config: &RunConfig, cfg: &T)` with `#[test_helper(..., config = T)]`
4. `fn(config: &RunConfig, module_cfg: &ModuleConfig, cfg: &T)` with `#[test_helper(..., config = T)]`

Rules:
- 3-parameter scenarios must provide `config = Type`.
- For the 2-parameter no-`config` case, parameter 2 is module-level `ModuleConfig`.
- Module name is inferred from Rust `module_path!()` (typically the file/module name under `src/scenarios`).

## Module-level config with `module_config!`

If many scenarios in one module share the same config type, declare:

```rust
use serde::Deserialize;
use yellowstone_grpc_intg_test::module_config;

#[derive(Debug, Default, Deserialize)]
pub struct RatelimitConfig {
		pub max_requests: u32,
}

module_config!(RatelimitConfig);
```

Then use it in scenarios:

```rust
use anyhow::Result;
use yellowstone_grpc_e2e_macros::test_helper;

#[test_helper(name = "get-version", config = RatelimitConfig)]
pub async fn test_get_version(
		run: &crate::scenarios::RunConfig,
		module_cfg: &ModuleConfig,
		cfg: &RatelimitConfig,
) -> Result<()> {
		let _ = (module_cfg, cfg);
		let _client = crate::grpc::new_client(run).await?;
		Ok(())
}
```

Notes:
- `module_cfg` is loaded from `[scenarios.<module>]`.
- `cfg` is loaded from `[scenarios.<module>.<scenario-name>]`.

## Config file format (`--config-file`)

Pass a TOML file to inject module/scenario config:

```toml
[scenarios.ratelimit]
max_requests = 1000
window = "10s"

[scenarios.ratelimit.get-version]
max_requests = 250
window = "5s"
```

How deserialization works:
- Module config: `Config::module_config("ratelimit")` -> `[scenarios.ratelimit]`
- Scenario config: `Config::scenario_config("ratelimit", "get-version")` -> `[scenarios.ratelimit.get-version]`
- Missing sections fall back to `Default` for the target type.

## Run the CLI

### List scenarios

```bash
cargo run -p yellowstone-grpc-intg-test --bin yellowstone-e2e -- list
```

Filter by module and/or tag:

```bash
cargo run -p yellowstone-grpc-intg-test --bin yellowstone-e2e -- list --module default
cargo run -p yellowstone-grpc-intg-test --bin yellowstone-e2e -- list --module ratelimit --tag ratelimit
```

### Run all scenarios in a module

```bash
cargo run -p yellowstone-grpc-intg-test --bin yellowstone-e2e -- \
	--endpoint https://127.0.0.1:10000 \
	--x-token test \
	all --module default
```

Run all scenarios matching any of several tags:

```bash
cargo run -p yellowstone-grpc-intg-test --bin yellowstone-e2e -- \
	--endpoint https://127.0.0.1:10000 \
	all --module default --tag accounts --tag filters
```

### Run one scenario

```bash
cargo run -p yellowstone-grpc-intg-test --bin yellowstone-e2e -- \
	--endpoint https://127.0.0.1:10000 \
	run sysvar-account
```

### Use `--dial` override

Use this when TLS/authority endpoint should stay unchanged, but TCP connect target must differ.

```bash
cargo run -p yellowstone-grpc-intg-test --bin yellowstone-e2e -- \
	--endpoint https://localhost:10000 \
	--dial 127.0.0.1:10000 \
	all --module default
```

### Use dotenv and config file

```bash
cargo run -p yellowstone-grpc-intg-test --bin yellowstone-e2e -- \
	--dotenv .env.e2e \
	--config-file yellowstone-grpc-e2e-test/config.toml \
	all --module ratelimit
```

## Endpoint/token/dial resolution precedence

### Endpoint

1. `--endpoint`
2. dotenv: `TEST_ENDPOINT`
3. dotenv: `YELLOWSTONE_GRPC_ENDPOINT`
4. environment: `TEST_ENDPOINT`
5. environment: `YELLOWSTONE_GRPC_ENDPOINT`

`endpoint` is required. If missing, the CLI exits with an error.

### x-token

1. `--x-token`
2. dotenv: `TEST_X_TOKEN`
3. dotenv: `TEST_TOKEN`
4. dotenv: `YELLOWSTONE_GRPC_X_TOKEN`
5. environment: `TEST_X_TOKEN`
6. environment: `TEST_TOKEN`
7. environment: `YELLOWSTONE_GRPC_X_TOKEN`

`x-token` is optional.

### dial target

1. `--dial`
2. dotenv: `TEST_DIAL`
3. dotenv: `YELLOWSTONE_GRPC_DIAL`
4. environment: `TEST_DIAL`
5. environment: `YELLOWSTONE_GRPC_DIAL`

`dial` is optional.

## Common workflow for adding tests

1. Add scenario function with `#[test_helper(name = "...")]` and doc comment.
2. Add tags for logical grouping.
3. If needed, add `Deserialize + Default` config struct and reference it via `config = Type`.
4. If shared config is needed per module, declare `module_config!(Type)` and use `&ModuleConfig` parameter.
5. Confirm registration:

```bash
cargo run -p yellowstone-grpc-intg-test --bin yellowstone-e2e -- \
	--endpoint https://127.0.0.1:10000 \
	list --module <module>
```

6. Execute your scenario:

```bash
cargo run -p yellowstone-grpc-intg-test --bin yellowstone-e2e -- \
	--endpoint https://127.0.0.1:10000 \
	run <scenario-name>
```
