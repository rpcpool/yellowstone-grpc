use yellowstone_grpc_geyser::grpc::{GrpcClientId, GrpcMessage, GrpcMessageId, GrpcMessageWithId};

fn main() {
    println!("{}", std::mem::size_of::<GrpcClientId>());
    println!("{}", std::mem::size_of::<GrpcMessageId>());
    println!("{}", std::mem::size_of::<tonic::Status>());
    println!("{}", std::mem::size_of::<GrpcMessage>());
    // println!("{}", std::mem::size_of::<yellowstone_grpc_geyser::grpc::GrpcMessage2>());
    println!("{}", std::mem::size_of::<GrpcMessageWithId>());
    let (tx, rx) = tokio::sync::broadcast::channel::<GrpcMessageWithId>(1_000_000);
    std::thread::sleep(std::time::Duration::from_secs(100000));
    let _ = tx.send(GrpcMessageWithId::new(
        GrpcMessageId::Client(GrpcClientId(0)),
        GrpcMessage::Tonic(Err(tonic::Status::internal(""))),
    ));
}
