use std::path::Path;

fn main() -> anyhow::Result<()> {
    std::env::set_var("PROTOC", protobuf_src::protoc());
    tonic_build::configure().compile(
        &[Path::new("proto/proto-b.proto")],
        &[
            Path::new("proto"),
            Path::new("../yellowstone-grpc-proto/proto"),
        ],
    )?;
    Ok(())
}
