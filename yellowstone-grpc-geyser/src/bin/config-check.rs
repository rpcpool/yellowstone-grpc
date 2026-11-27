use {
    clap::Parser,
    std::{io::Write, process::ExitCode},
    yellowstone_grpc_geyser::config::{Config, DataPlaneConfigWarning},
};

#[derive(Debug, Parser)]
#[clap(author, version, about)]
struct Args {
    #[clap(short, long, default_value_t = String::from("config.json"))]
    /// Path to config
    config: String,
    /// Fail on any warning, and warn about each compression key the config sets
    #[clap(long)]
    strict: bool,
    /// An `extensions` section the plugin build claims; repeat it for each section
    #[clap(long = "claimed-extension", value_name = "NAME")]
    claimed_extensions: Vec<String>,
}

fn main() -> anyhow::Result<ExitCode> {
    let args = Args::parse();
    let config_passed = check_config(&args, &mut std::io::stdout().lock())?;
    Ok(if config_passed {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}

/// Loads the config and prints its warnings. A config with warnings passes unless `--strict`.
fn check_config(command_line_args: &Args, report_output: &mut impl Write) -> anyhow::Result<bool> {
    let loaded_config = Config::load_from_file(&command_line_args.config)?;
    let config_warnings = config_warnings(&loaded_config, command_line_args);
    for config_warning in &config_warnings {
        writeln!(report_output, "warning: {config_warning}")?;
    }
    if command_line_args.strict && !config_warnings.is_empty() {
        writeln!(
            report_output,
            "Config has {} warning(s), and --strict fails on any",
            config_warnings.len()
        )?;
        return Ok(false);
    }
    writeln!(report_output, "Config is OK!")?;
    Ok(true)
}

fn config_warnings(
    loaded_config: &Config,
    command_line_args: &Args,
) -> Vec<DataPlaneConfigWarning> {
    let data_plane = &loaded_config.grpc.data_plane;
    let claimed_extension_names = command_line_args
        .claimed_extensions
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let mut config_warnings = data_plane.load_warnings.clone();
    config_warnings.extend(data_plane.unclaimed_extension_warnings(&claimed_extension_names));
    if command_line_args.strict {
        config_warnings.extend(data_plane.compression_override_warnings());
    }
    config_warnings
}
