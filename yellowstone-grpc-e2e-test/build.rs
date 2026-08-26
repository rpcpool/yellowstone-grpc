fn main() -> anyhow::Result<()> {
    vergen::Emitter::default()
        .add_instructions(&vergen::BuildBuilder::all_build()?)?
        .add_instructions(&vergen::RustcBuilder::all_rustc()?)?
        .emit()?;

    println!(
        "cargo:rustc-env=GIT_VERSION={}",
        git_version::git_version!(
            args = ["--always", "--dirty", "--tags", "--long"],
            fallback = "unknown"
        )
    );

    Ok(())
}
