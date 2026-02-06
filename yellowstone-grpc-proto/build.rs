use std::{env, fs, path::Path};

fn main() -> anyhow::Result<()> {
    std::env::set_var("PROTOC", protobuf_src::protoc());

    // build protos
    #[cfg(feature = "account-data-as-bytes")]
    {
        tonic_prost_build::configure()
            .bytes(".geyser.SubscribeUpdateAccountInfo.data")
            .compile_protos(&["proto/geyser.proto"], &["proto"])?;
    }
    #[cfg(not(feature = "account-data-as-bytes"))]
    {
        tonic_prost_build::configure().compile_protos(&["proto/geyser.proto"], &["proto"])?;
    }

    // build protos without tonic (wasm)
    let out_dir = env::var("OUT_DIR").expect("OUT_DIR not found");
    let out_dir_path = Path::new(&out_dir).join("no-tonic");
    fs::create_dir_all(&out_dir_path).expect("failed to create out no-tonic directory");
    let builder = tonic_prost_build::configure()
        .build_client(false)
        .build_server(false)
        .out_dir(out_dir_path);

    builder.compile_protos(&["proto/geyser.proto"], &["proto"])?;

    Ok(())
}
