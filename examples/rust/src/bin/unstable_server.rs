use {
    clap::Parser,
    futures::stream::StreamExt,
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

    #[clap(long, default_value_t = 400)]
    message_delay_ms: u64,

    #[clap(long)]
    send_block_meta: bool,
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
        request: Request<tonic::Streaming<SubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let n = self.connections.fetch_add(1, Ordering::SeqCst);
        let should_disconnect = n < self.args.disconnect_after_n_connections;
        let msg_count = self.args.messages_before_disconnect;
        let delay = std::time::Duration::from_millis(self.args.message_delay_ms);
        let send_block_meta = self.args.send_block_meta;

        let mut inbound = request.into_inner();
        let from_slot = match inbound.next().await {
            Some(Ok(req)) => req.from_slot,
            _ => None,
        };

        let start_slot = from_slot.unwrap_or(n as u64 * msg_count);

        println!("connection #{n}: from_slot={from_slot:?} start={start_slot} msgs={msg_count}");
        if should_disconnect {
            println!("connection #{n}: will disconnect after {msg_count} messages");
        } else {
            println!("connection #{n}: will stream indefinitely");
        }

        let (tx, rx) = tokio_mpsc::channel(100);
        tokio::spawn(async move {
            let mut slot = start_slot;
            loop {
                if send_block_meta {
                    let meta = SubscribeUpdate {
                        filters: vec![],
                        update_oneof: Some(subscribe_update::UpdateOneof::BlockMeta(
                            SubscribeUpdateBlockMeta {
                                slot,
                                blockhash: String::new(),
                                rewards: None,
                                block_time: None,
                                block_height: None,
                                parent_slot: slot.saturating_sub(1),
                                parent_blockhash: String::new(),
                                executed_transaction_count: 0,
                                entries_count: 0,
                            },
                        )),
                        created_at: None,
                    };
                    if tx.send(Ok(meta)).await.is_err() {
                        println!("connection #{n}: client disconnected");
                        break;
                    }
                }

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
                    println!("connection #{n}: sending disconnect error");
                    let _ = tx
                        .send(Err(Status::unavailable("simulated disconnect")))
                        .await;
                    break;
                }

                tokio::time::sleep(delay).await;
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn subscribe_deshred(
        &self,
        _: Request<tonic::Streaming<SubscribeDeshredRequest>>,
    ) -> Result<Response<Self::SubscribeDeshredStream>, Status> {
        Err(Status::unimplemented(""))
    }

    async fn ping(&self, _: Request<PingRequest>) -> Result<Response<PongResponse>, Status> {
        Err(Status::unimplemented(""))
    }

    async fn get_latest_blockhash(
        &self,
        _: Request<GetLatestBlockhashRequest>,
    ) -> Result<Response<GetLatestBlockhashResponse>, Status> {
        Err(Status::unimplemented(""))
    }

    async fn get_block_height(
        &self,
        _: Request<GetBlockHeightRequest>,
    ) -> Result<Response<GetBlockHeightResponse>, Status> {
        Err(Status::unimplemented(""))
    }

    async fn get_slot(
        &self,
        _: Request<GetSlotRequest>,
    ) -> Result<Response<GetSlotResponse>, Status> {
        Err(Status::unimplemented(""))
    }

    async fn is_blockhash_valid(
        &self,
        _: Request<IsBlockhashValidRequest>,
    ) -> Result<Response<IsBlockhashValidResponse>, Status> {
        Err(Status::unimplemented(""))
    }

    async fn get_version(
        &self,
        _: Request<GetVersionRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        Err(Status::unimplemented(""))
    }

    async fn subscribe_replay_info(
        &self,
        _: Request<SubscribeReplayInfoRequest>,
    ) -> Result<Response<SubscribeReplayInfoResponse>, Status> {
        Err(Status::unimplemented(""))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let addr = format!("0.0.0.0:{}", args.port).parse()?;

    println!("unstable server on {addr}");
    println!("  messages per connection: {}", args.messages_before_disconnect);
    println!("  disconnect first {} connections", args.disconnect_after_n_connections);
    println!("  message delay: {}ms", args.message_delay_ms);
    println!("  send block meta: {}", args.send_block_meta);

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