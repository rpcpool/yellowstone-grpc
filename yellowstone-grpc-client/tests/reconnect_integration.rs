use {
    futures::stream::StreamExt,
    std::{
        future::Future,
        pin::Pin,
        sync::{
            atomic::{AtomicU32, Ordering},
            Arc,
        },
    },
    tokio::sync::mpsc as tokio_mpsc,
    tokio_stream::wrappers::ReceiverStream,
    tonic::{transport::Server, Request, Response, Status},
    yellowstone_grpc_client::GeyserGrpcClient,
    yellowstone_grpc_proto::{
        geyser::geyser_server::{Geyser, GeyserServer},
        prelude::*,
    },
};

type SubscribeResult = Result<Response<ReceiverStream<Result<SubscribeUpdate, Status>>>, Status>;
type SubscribeHandler =
    Box<dyn Fn() -> Pin<Box<dyn Future<Output = SubscribeResult> + Send>> + Send + Sync>;

struct TestGeyser {
    on_subscribe: SubscribeHandler,
}

impl TestGeyser {
    fn new<F, Fut>(f: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = SubscribeResult> + Send + 'static,
    {
        TestGeyser {
            on_subscribe: Box::new(move || Box::pin(f())),
        }
    }
}

#[tonic::async_trait]
impl Geyser for TestGeyser {
    type SubscribeStream = ReceiverStream<Result<SubscribeUpdate, Status>>;
    type SubscribeDeshredStream = ReceiverStream<Result<SubscribeUpdateDeshred, Status>>;

    async fn subscribe(
        &self,
        _: Request<tonic::Streaming<SubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        (self.on_subscribe)().await
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

fn slot_msg(slot: u64) -> SubscribeUpdate {
    SubscribeUpdate {
        filters: vec![],
        update_oneof: Some(subscribe_update::UpdateOneof::Slot(SubscribeUpdateSlot {
            slot,
            parent: None,
            status: 0,
            dead_error: None,
        })),
        created_at: None,
    }
}

async fn spawn_server(server: TestGeyser) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(GeyserServer::new(server))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    addr
}

async fn connect(addr: std::net::SocketAddr) -> GeyserGrpcClient {
    GeyserGrpcClient::build_from_shared(format!("http://{addr}"))
        .unwrap()
        .auto_reconnect(true)
        .connect()
        .await
        .unwrap()
}

#[tokio::test]
async fn test_client_receives_messages_without_reconnect() {
    let addr = spawn_server(TestGeyser::new(|| async {
        let (tx, rx) = tokio_mpsc::channel(100);
        tokio::spawn(async move {
            for i in 0..5 {
                if tx.send(Ok(slot_msg(i))).await.is_err() {
                    break;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }))
    .await;

    let mut client = connect(addr).await;
    let mut stream = client
        .subscribe_once(SubscribeRequest::default())
        .await
        .unwrap();

    let mut slots = vec![];
    for _ in 0..5 {
        if let Some(Ok(msg)) = stream.next().await {
            if let Some(subscribe_update::UpdateOneof::Slot(s)) = msg.update_oneof {
                slots.push(s.slot);
            }
        }
    }
    assert_eq!(slots, vec![0, 1, 2, 3, 4]);
}

#[tokio::test]
async fn test_client_stops_on_clean_stream_end() {
    let addr = spawn_server(TestGeyser::new(|| async {
        let (tx, rx) = tokio_mpsc::channel(100);
        tokio::spawn(async move {
            for i in 0..3 {
                if tx.send(Ok(slot_msg(i))).await.is_err() {
                    break;
                }
            }
            // tx drops — stream ends, simulates disconnect
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }))
    .await;

    let mut client = connect(addr).await;
    let mut stream = client
        .subscribe_once(SubscribeRequest::default())
        .await
        .unwrap();

    let mut count = 0;
    while let Some(msg) = stream.next().await {
        match msg {
            Ok(_) => count += 1,
            Err(_) => break,
        }
        if count >= 3 {
            break;
        }
    }
    assert_eq!(count, 3);
}

#[tokio::test]
async fn test_client_reconnects_after_server_error() {
    let connections = Arc::new(AtomicU32::new(0));

    let addr = spawn_server(TestGeyser::new({
        let connections = Arc::clone(&connections);
        move || {
            let n = connections.fetch_add(1, Ordering::SeqCst);
            async move {
                let (tx, rx) = tokio_mpsc::channel(100);
                tokio::spawn(async move {
                    if n == 0 {
                        let _ = tx.send(Ok(slot_msg(1))).await;
                        let _ = tx.send(Err(Status::unavailable("server going down"))).await;
                    } else {
                        let _ = tx.send(Ok(slot_msg(100))).await; // different slot
                    }
                });
                Ok(Response::new(ReceiverStream::new(rx)))
            }
        }
    }))
    .await;

    let test = async {
        let mut client = connect(addr).await;
        let mut stream = client
            .subscribe_once(SubscribeRequest::default())
            .await
            .unwrap();

        // Message from connection #1
        let first = stream.next().await.expect("msg").expect("ok");
        eprintln!("first: {first:?}");
        assert!(first.update_oneof.is_some());

        // Unavailable -> reconnect -> message from connection #2
        let second = stream.next().await.expect("msg after reconnect");
        eprintln!("second: {second:?}");

        if let Some(subscribe_update::UpdateOneof::Slot(s)) = second.clone().unwrap().update_oneof {
            assert_eq!(s.slot, 100);
        }

        assert!(second.is_ok());

        // Connection #2 drops tx -> stream ends cleanly
        let third = stream.next().await;
        assert!(third.is_none(), "stream should terminate");

        assert_eq!(connections.load(Ordering::SeqCst), 2);
    };

    tokio::time::timeout(std::time::Duration::from_secs(10), test)
        .await
        .expect("timed out — reconnect may be looping");
}

#[tokio::test]
async fn test_client_stops_on_fatal_error() {
    let addr = spawn_server(TestGeyser::new(|| async {
        Err(Status::permission_denied("bad token"))
    }))
    .await;

    let result = connect(addr)
        .await
        .subscribe_once(SubscribeRequest::default())
        .await;

    assert!(
        result.is_err() || {
            let mut stream = result.unwrap();
            let first = stream.next().await;
            matches!(first, Some(Err(_)) | None)
        }
    );
}
