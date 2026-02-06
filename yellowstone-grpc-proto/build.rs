use std::{env, fs, path::Path};

fn main() -> anyhow::Result<()> {
    // Use vendored protoc to avoid building C++ protobuf via autotools
    let protoc_path = protoc_bin_vendored::protoc_bin_path()?;
    unsafe {
        std::env::set_var("PROTOC", protoc_path);
    }

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
