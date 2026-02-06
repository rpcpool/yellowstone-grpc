use {
    cargo_lock::Lockfile,
    std::collections::HashSet,
    tonic_prost_build::manual::{Builder, Method, Service},
};

fn main() -> anyhow::Result<()> {
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

    // build tonic server with custom FilteredUpdate output type
    let geyser_service = Service::builder()
        .name("Geyser")
        .package("geyser")
        .method(
            Method::builder()
                .name("subscribe")
                .route_name("Subscribe")
                .input_type("yellowstone_grpc_proto::geyser::SubscribeRequest")
                .output_type("crate::plugin::filter::message::FilteredUpdate")
                .codec_path("tonic_prost::ProstCodec")
                .client_streaming()
                .server_streaming()
                .build(),
        )
        .method(
            Method::builder()
                .name("subscribe_first_available_slot")
                .route_name("SubscribeReplayInfo")
                .input_type("yellowstone_grpc_proto::geyser::SubscribeReplayInfoRequest")
                .output_type("yellowstone_grpc_proto::geyser::SubscribeReplayInfoResponse")
                .codec_path("tonic_prost::ProstCodec")
                .build(),
        )
        .method(
            Method::builder()
                .name("ping")
                .route_name("Ping")
                .input_type("yellowstone_grpc_proto::geyser::PingRequest")
                .output_type("yellowstone_grpc_proto::geyser::PongResponse")
                .codec_path("tonic_prost::ProstCodec")
                .build(),
        )
        .method(
            Method::builder()
                .name("get_latest_blockhash")
                .route_name("GetLatestBlockhash")
                .input_type("yellowstone_grpc_proto::geyser::GetLatestBlockhashRequest")
                .output_type("yellowstone_grpc_proto::geyser::GetLatestBlockhashResponse")
                .codec_path("tonic_prost::ProstCodec")
                .build(),
        )
        .method(
            Method::builder()
                .name("get_block_height")
                .route_name("GetBlockHeight")
                .input_type("yellowstone_grpc_proto::geyser::GetBlockHeightRequest")
                .output_type("yellowstone_grpc_proto::geyser::GetBlockHeightResponse")
                .codec_path("tonic_prost::ProstCodec")
                .build(),
        )
        .method(
            Method::builder()
                .name("get_slot")
                .route_name("GetSlot")
                .input_type("yellowstone_grpc_proto::geyser::GetSlotRequest")
                .output_type("yellowstone_grpc_proto::geyser::GetSlotResponse")
                .codec_path("tonic_prost::ProstCodec")
                .build(),
        )
        .method(
            Method::builder()
                .name("is_blockhash_valid")
                .route_name("IsBlockhashValid")
                .input_type("yellowstone_grpc_proto::geyser::IsBlockhashValidRequest")
                .output_type("yellowstone_grpc_proto::geyser::IsBlockhashValidResponse")
                .codec_path("tonic_prost::ProstCodec")
                .build(),
        )
        .method(
            Method::builder()
                .name("get_version")
                .route_name("GetVersion")
                .input_type("yellowstone_grpc_proto::geyser::GetVersionRequest")
                .output_type("yellowstone_grpc_proto::geyser::GetVersionResponse")
                .codec_path("tonic_prost::ProstCodec")
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
