use {cargo_lock::Lockfile, std::collections::HashSet};

fn main() -> anyhow::Result<()> {
    let mut envs = vergen::EmitBuilder::builder();
    envs.all_build().all_rustc();
    envs.emit()?;

    // vergen git version does not looks cool
    println!(
        "cargo:rustc-env=GIT_VERSION={}",
        git_version::git_version!()
    );

    // Extract packages version
    let lockfile = Lockfile::load("../Cargo.lock")?;
    println!(
        "cargo:rustc-env=SOLANA_SDK_VERSION={}",
        get_pkg_version(&lockfile, "solana-sdk")
    );
    println!(
        "cargo:rustc-env=YELLOWSTONE_GRPC_PROTO_VERSION={}",
        get_pkg_version(&lockfile, "yellowstone-grpc-proto")
    );

    Ok(())
}

fn get_pkg_version(lockfile: &Lockfile, pkg_name: &str) -> String {
    lockfile
        .packages
        .iter()
        .filter(|pkg| pkg.name.as_str() == pkg_name)
        .map(|pkg| pkg.version.to_string())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
        .join(",")
}
