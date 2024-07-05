fn main() -> anyhow::Result<()> {
    std::env::set_var("PROTOC", protobuf_src::protoc());
    tonic_build::configure().compile(
&[
            "proto/solana-storage.proto",
            "proto/geyser.proto",
            "proto/yellowstone-log.proto"
        ],
        &["proto"]
    )?;
    Ok(())
}
