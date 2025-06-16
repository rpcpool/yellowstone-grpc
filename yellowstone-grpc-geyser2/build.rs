use {
    cargo_lock::Lockfile,
    std::collections::HashSet,
    tonic_build::manual::{Builder, Method, Service},
};

fn main() -> anyhow::Result<()> {
    std::env::set_var("PROTOC", protobuf_src::protoc());

    vergen::Emitter::default()
        .add_instructions(&vergen::BuildBuilder::all_build()?)?
        .add_instructions(&vergen::RustcBuilder::all_rustc()?)?
        .emit()?;

    // vergen git version does not looks cool
    println!(
        "cargo:rustc-env=GIT_VERSION={}",
        git_version::git_version!()
    );

    // Extract packages version
    let lockfile = Lockfile::load("../Cargo.lock")?;
    println!(
        "cargo:rustc-env=SOLANA_SDK_VERSION={}",
        // this is used to set the solana version number
        // this used to refer `solana-sdk`
        // since it was deprecated
        // this now referes `agave-geyser-plugin-interface`
        get_pkg_version(&lockfile, "agave-geyser-plugin-interface")
    );
    println!(
        "cargo:rustc-env=YELLOWSTONE_GRPC_PROTO_VERSION={}",
        get_pkg_version(&lockfile, "yellowstone-grpc-proto")
    );

    tonic_build::configure()
        .bytes(&[".geyser.SubscribeUpdateAccountInfo.data"])
        .build_server(false)
        .build_client(false)
        .compile_protos(&["proto/geyser.proto"], &["proto"])?;

    let geyser_service = Service::builder()
        .name("Geyser")
        .package("geyser")
        .method(
            Method::builder()
                .name("subscribe")
                .route_name("Subscribe")
                .input_type("super::super::geyser::SubscribeRequest")
                .output_type("crate::grpc::proto::ZeroCopySubscribeUpdate")
                .codec_path("tonic::codec::ProstCodec")
                .client_streaming()
                .server_streaming()
                .build(),
        )
        .method(
            Method::builder()
                .name("get_version")
                .route_name("GetVersion")
                .input_type("super::super::geyser::GetVersionRequest")
                .output_type("super::super::geyser::GetVersionResponse")
                .codec_path("tonic::codec::ProstCodec")
                .build(),
        )
        .build();

    Builder::new()
        .build_client(false)
        .compile(&[geyser_service]);

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
