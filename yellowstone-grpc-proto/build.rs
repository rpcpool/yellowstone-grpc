fn main() -> anyhow::Result<()> {
    std::env::set_var("PROTOC", protobuf_src::protoc());
    tonic_build::compile_protos("proto/geyser.proto")?;
    Ok(())
}
