use tonic_build::manual::{Builder, Method, Service};

fn main() -> anyhow::Result<()> {
    std::env::set_var("PROTOC", protobuf_src::protoc());
    // build protos
    tonic_build::compile_protos("proto/fumarole.proto")?;

    // build with accepting our custom struct
    let geyser_service = Service::builder()
        .name("Geyser")
        .package("geyser")
        .method(
            Method::builder()
                .name("subscribe")
                .route_name("Subscribe")
                .input_type("crate::geyser::SubscribeRequest")
                // .output_type("crate::geyser::SubscribeUpdate")
                .output_type("crate::plugin::filter::message::FilteredUpdate")
                .codec_path("tonic::codec::ProstCodec")
                // .codec_path("crate::plugin::codec::SubscribeCodec")
                .client_streaming()
                .server_streaming()
                .build(),
        )
        .method(
            Method::builder()
                .name("ping")
                .route_name("Ping")
                .input_type("crate::geyser::PingRequest")
                .output_type("crate::geyser::PongResponse")
                .codec_path("tonic::codec::ProstCodec")
                .build(),
        )
        .method(
            Method::builder()
                .name("get_latest_blockhash")
                .route_name("GetLatestBlockhash")
                .input_type("crate::geyser::GetLatestBlockhashRequest")
                .output_type("crate::geyser::GetLatestBlockhashResponse")
                .codec_path("tonic::codec::ProstCodec")
                .build(),
        )
        .method(
            Method::builder()
                .name("get_block_height")
                .route_name("GetBlockHeight")
                .input_type("crate::geyser::GetBlockHeightRequest")
                .output_type("crate::geyser::GetBlockHeightResponse")
                .codec_path("tonic::codec::ProstCodec")
                .build(),
        )
        .method(
            Method::builder()
                .name("get_slot")
                .route_name("GetSlot")
                .input_type("crate::geyser::GetSlotRequest")
                .output_type("crate::geyser::GetSlotResponse")
                .codec_path("tonic::codec::ProstCodec")
                .build(),
        )
        .method(
            Method::builder()
                .name("is_blockhash_valid")
                .route_name("IsBlockhashValid")
                .input_type("crate::geyser::IsBlockhashValidRequest")
                .output_type("crate::geyser::IsBlockhashValidResponse")
                .codec_path("tonic::codec::ProstCodec")
                .build(),
        )
        .method(
            Method::builder()
                .name("get_version")
                .route_name("GetVersion")
                .input_type("crate::geyser::GetVersionRequest")
                .output_type("crate::geyser::GetVersionResponse")
                .codec_path("tonic::codec::ProstCodec")
                .build(),
        )
        .build();

    Builder::new()
        .build_client(false)
        .compile(&[geyser_service]);

    // patching generated custom struct (if custom Codec is used)
    // let mut location = std::path::PathBuf::from(std::env::var("OUT_DIR")?);
    // location.push("geyser.Geyser.rs");
    // let geyser_rs = std::fs::read_to_string(location.clone())?;
    // let geyser_rs = geyser_rs.replace(
    //     "let codec = crate::plugin::codec::SubscribeCodec::default();",
    //     "let codec = crate::plugin::codec::SubscribeCodec::<crate::plugin::filter::Message, _>::default();",
    // );
    // std::fs::write(location, geyser_rs)?;

    Ok(())
}
