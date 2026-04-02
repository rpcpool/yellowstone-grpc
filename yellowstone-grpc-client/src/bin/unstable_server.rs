use {
    clap::Parser,
    std::sync::atomic::{AtomicU32, Ordering},
    tokio::sync::mpsc as tokio_mpsc,
    tokio_stream::wrappers::ReceiverStream,
    tonic::{transport::Server, Request, Response, Status},
    yellowstone_grpc_proto::{
        geyser::geyser_server::{Geyser, GeyserServer},
        prelude::*,
    },
};

#[derive(Debug, Parser)]
#[clap(about = "Unstable gRPC server for testing auto-reconnect")]
struct Args {
    #[clap(long, default_value_t = 10000)]
    port: u16,

    #[clap(long, default_value_t = 50)]
    messages_before_disconnect: u64,

    #[clap(long, default_value_t = 3)]
    disconnect_after_n_connections: u32,
}

struct UnstableGeyser {
    args: Args,
    connections: AtomicU32,
}

#[tonic::async_trait]
impl Geyser for UnstableGeyser {
    type SubscribeStream = ReceiverStream<Result<SubscribeUpdate, Status>>;
    type SubscribeDeshredStream = ReceiverStream<Result<SubscribeUpdateDeshred, Status>>;

    async fn subscribe(
        &self,
        _: Request<tonic::Streaming<SubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let n = self.connections.fetch_add(1, Ordering::SeqCst);
        let should_disconnect = n < self.args.disconnect_after_n_connections;
        let msg_count = self.args.messages_before_disconnect;
        let start_slot = n as u64 * msg_count;

        println!("connection #{n}: will send {msg_count} messages starting at slot {start_slot}");
        if should_disconnect {
            println!("connection #{n}: will disconnect after {msg_count} messages");
        } else {
            println!("connection #{n}: will stream indefinitely");
        }

        let (tx, rx) = tokio_mpsc::channel(100);
        tokio::spawn(async move {
            let mut slot = start_slot;
            loop {
                let msg = SubscribeUpdate {
                    filters: vec![],
                    update_oneof: Some(subscribe_update::UpdateOneof::Slot(
                        SubscribeUpdateSlot {
                            slot,
                            parent: None,
                            status: 0,
                            dead_error: None,
                        },
                    )),
                    created_at: None,
                };

                if tx.send(Ok(msg)).await.is_err() {
                    println!("connection #{n}: client disconnected");
                    break;
                }

                slot += 1;

                if should_disconnect && (slot - start_slot) >= msg_count {
                    println!("connection #{n}: sending disconnect error after {msg_count} messages");
                    let _ = tx.send(Err(Status::unavailable("simulated disconnect"))).await;
                    break;
                }

                tokio::time::sleep(std::time::Duration::from_millis(400)).await;
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn subscribe_deshred(
        &self,
        _: Request<tonic::Streaming<SubscribeDeshredRequest>>,
    ) -> Result<Response<Self::SubscribeDeshredStream>, Status> {
        Err(Status::unimplemented("not supported"))
    }

    async fn ping(&self, _: Request<PingRequest>) -> Result<Response<PongResponse>, Status> {
        Err(Status::unimplemented("not supported"))
    }

    async fn get_latest_blockhash(
        &self,
        _: Request<GetLatestBlockhashRequest>,
    ) -> Result<Response<GetLatestBlockhashResponse>, Status> {
        Err(Status::unimplemented("not supported"))
    }

    async fn get_block_height(
        &self,
        _: Request<GetBlockHeightRequest>,
    ) -> Result<Response<GetBlockHeightResponse>, Status> {
        Err(Status::unimplemented("not supported"))
    }

    async fn get_slot(
        &self,
        _: Request<GetSlotRequest>,
    ) -> Result<Response<GetSlotResponse>, Status> {
        Err(Status::unimplemented("not supported"))
    }

    async fn is_blockhash_valid(
        &self,
        _: Request<IsBlockhashValidRequest>,
    ) -> Result<Response<IsBlockhashValidResponse>, Status> {
        Err(Status::unimplemented("not supported"))
    }

    async fn get_version(
        &self,
        _: Request<GetVersionRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        Err(Status::unimplemented("not supported"))
    }

    async fn subscribe_replay_info(
        &self,
        _: Request<SubscribeReplayInfoRequest>,
    ) -> Result<Response<SubscribeReplayInfoResponse>, Status> {
        Err(Status::unimplemented("not supported"))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let addr = format!("0.0.0.0:{}", args.port).parse()?;

    println!("unstable server on {addr}");
    println!("  messages per connection: {}", args.messages_before_disconnect);
    println!("  disconnect first {} connections", args.disconnect_after_n_connections);

    let server = UnstableGeyser {
        connections: AtomicU32::new(0),
        args,
    };

    Server::builder()
        .add_service(GeyserServer::new(server))
        .serve(addr)
        .await?;

    Ok(())
}