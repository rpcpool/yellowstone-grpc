fn main() -> anyhow::Result<()> {
    // Embed build + rustc metadata (VERGEN_BUILD_TIMESTAMP, VERGEN_RUSTC_SEMVER).
    vergen::Emitter::default()
        .add_instructions(&vergen::BuildBuilder::all_build()?)?
        .add_instructions(&vergen::RustcBuilder::all_rustc()?)?
        .emit()?;

    // Embed `git describe` (tag + commit sha) of the source tree this binary
    // was built from, matching the geyser crate's convention.
    println!(
        "cargo:rustc-env=GIT_VERSION={}",
        git_version::git_version!(
            args = ["--always", "--dirty", "--tags", "--long"],
            fallback = "unknown"
        )
    );

    Ok(())
}
