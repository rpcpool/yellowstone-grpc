//! Benchmark comparing TCP vs Unix Domain Socket transport performance for gRPC.
//!
//! This benchmark measures:
//! - Latency: Round-trip time for unary RPC calls (Ping)
//! - Throughput: Messages per second for streaming RPC

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use futures::StreamExt;
use std::{hint::black_box, time::Duration};
use tokio::{
    net::{TcpListener, UnixListener},
    runtime::Runtime,
    sync::oneshot,
};
use tokio_stream::wrappers::{TcpListenerStream, UnixListenerStream};
use tonic::{transport::Server, Request, Response, Status};
use yellowstone_grpc_proto::prelude::{
    geyser_server::{Geyser, GeyserServer},
    subscribe_update::UpdateOneof,
    GetBlockHeightRequest, GetBlockHeightResponse, GetLatestBlockhashRequest,
    GetLatestBlockhashResponse, GetSlotRequest, GetSlotResponse, GetVersionRequest,
    GetVersionResponse, IsBlockhashValidRequest, IsBlockhashValidResponse, PingRequest,
    PongResponse, SubscribeReplayInfoRequest, SubscribeReplayInfoResponse, SubscribeRequest,
    SubscribeUpdate, SubscribeUpdatePing,
};

/// Mock Geyser service for benchmarking
#[derive(Debug, Default)]
struct MockGeyserService;

#[tonic::async_trait]
impl Geyser for MockGeyserService {
    type SubscribeStream =
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<SubscribeUpdate, Status>> + Send>>;

    async fn subscribe(
        &self,
        request: Request<tonic::Streaming<SubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let mut stream = request.into_inner();

        let output = async_stream::try_stream! {
            while let Some(req) = stream.next().await {
                let _req = req?;
                // Echo back a ping for each request
                yield SubscribeUpdate {
                    filters: vec![],
                    update_oneof: Some(UpdateOneof::Ping(SubscribeUpdatePing {})),
                    created_at: None,
                };
            }
        };

        Ok(Response::new(Box::pin(output)))
    }

    async fn subscribe_replay_info(
        &self,
        _request: Request<SubscribeReplayInfoRequest>,
    ) -> Result<Response<SubscribeReplayInfoResponse>, Status> {
        Ok(Response::new(SubscribeReplayInfoResponse {
            first_available: Some(0),
        }))
    }

    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PongResponse>, Status> {
        Ok(Response::new(PongResponse {
            count: request.into_inner().count,
        }))
    }

    async fn get_latest_blockhash(
        &self,
        _request: Request<GetLatestBlockhashRequest>,
    ) -> Result<Response<GetLatestBlockhashResponse>, Status> {
        Ok(Response::new(GetLatestBlockhashResponse {
            slot: 12345,
            blockhash: "mock_blockhash".to_string(),
            last_valid_block_height: 12345,
        }))
    }

    async fn get_block_height(
        &self,
        _request: Request<GetBlockHeightRequest>,
    ) -> Result<Response<GetBlockHeightResponse>, Status> {
        Ok(Response::new(GetBlockHeightResponse {
            block_height: 12345,
        }))
    }

    async fn get_slot(
        &self,
        _request: Request<GetSlotRequest>,
    ) -> Result<Response<GetSlotResponse>, Status> {
        Ok(Response::new(GetSlotResponse { slot: 12345 }))
    }

    async fn is_blockhash_valid(
        &self,
        _request: Request<IsBlockhashValidRequest>,
    ) -> Result<Response<IsBlockhashValidResponse>, Status> {
        Ok(Response::new(IsBlockhashValidResponse {
            slot: 12345,
            valid: true,
        }))
    }

    async fn get_version(
        &self,
        _request: Request<GetVersionRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        Ok(Response::new(GetVersionResponse {
            version: "mock-1.0.0".to_string(),
        }))
    }
}

/// Start a gRPC server on TCP and return the address
async fn start_tcp_server() -> (String, oneshot::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let endpoint = format!("http://{}", addr);

    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    tokio::spawn(async move {
        Server::builder()
            .add_service(GeyserServer::new(MockGeyserService))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                shutdown_rx.await.ok();
            })
            .await
            .unwrap();
    });

    // Wait for server to be ready
    tokio::time::sleep(Duration::from_millis(50)).await;

    (endpoint, shutdown_tx)
}

/// Start a gRPC server on Unix Domain Socket and return the path
#[cfg(unix)]
async fn start_uds_server() -> (String, oneshot::Sender<()>, tempfile::TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let socket_path = temp_dir.path().join("bench.sock");

    let listener = UnixListener::bind(&socket_path).unwrap();
    let endpoint = format!("unix://{}", socket_path.display());

    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    tokio::spawn(async move {
        Server::builder()
            .add_service(GeyserServer::new(MockGeyserService))
            .serve_with_incoming_shutdown(UnixListenerStream::new(listener), async {
                shutdown_rx.await.ok();
            })
            .await
            .unwrap();
    });

    // Wait for server to be ready
    tokio::time::sleep(Duration::from_millis(50)).await;

    (endpoint, shutdown_tx, temp_dir)
}

/// Benchmark unary RPC latency (Ping)
fn bench_latency(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("transport_latency");
    group.measurement_time(Duration::from_secs(10));

    // TCP benchmark - setup server once
    let (tcp_endpoint, tcp_shutdown) = rt.block_on(start_tcp_server());
    let mut tcp_client = rt
        .block_on(async {
            yellowstone_grpc_client::GeyserGrpcClient::build_from_shared(tcp_endpoint)
                .unwrap()
                .connect()
                .await
        })
        .unwrap();

    group.bench_function(BenchmarkId::new("ping", "tcp"), |b| {
        let mut count = 0i32;
        b.iter(|| {
            count += 1;
            let _ = black_box(rt.block_on(tcp_client.ping(count)));
        });
    });

    tcp_shutdown.send(()).ok();

    // UDS benchmark
    #[cfg(unix)]
    {
        let (uds_endpoint, uds_shutdown, _temp_dir) = rt.block_on(start_uds_server());
        let mut uds_client = rt
            .block_on(async {
                yellowstone_grpc_client::GeyserGrpcClient::build_from_shared(uds_endpoint)
                    .unwrap()
                    .connect()
                    .await
            })
            .unwrap();

        group.bench_function(BenchmarkId::new("ping", "uds"), |b| {
            let mut count = 0i32;
            b.iter(|| {
                count += 1;
                let _ = black_box(rt.block_on(uds_client.ping(count)));
            });
        });

        uds_shutdown.send(()).ok();
    }

    group.finish();
}

/// Benchmark throughput using get_version (simple unary call)
fn bench_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("transport_throughput");
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(1));

    // TCP benchmark - setup server once
    let (tcp_endpoint, tcp_shutdown) = rt.block_on(start_tcp_server());
    let mut tcp_client = rt
        .block_on(async {
            yellowstone_grpc_client::GeyserGrpcClient::build_from_shared(tcp_endpoint)
                .unwrap()
                .connect()
                .await
        })
        .unwrap();

    group.bench_function(BenchmarkId::new("get_version", "tcp"), |b| {
        b.iter(|| {
            let _ = black_box(rt.block_on(tcp_client.get_version()));
        });
    });

    tcp_shutdown.send(()).ok();

    // UDS benchmark
    #[cfg(unix)]
    {
        let (uds_endpoint, uds_shutdown, _temp_dir) = rt.block_on(start_uds_server());
        let mut uds_client = rt
            .block_on(async {
                yellowstone_grpc_client::GeyserGrpcClient::build_from_shared(uds_endpoint)
                    .unwrap()
                    .connect()
                    .await
            })
            .unwrap();

        group.bench_function(BenchmarkId::new("get_version", "uds"), |b| {
            b.iter(|| {
                let _ = black_box(rt.block_on(uds_client.get_version()));
            });
        });

        uds_shutdown.send(()).ok();
    }

    group.finish();
}

criterion_group!(benches, bench_latency, bench_throughput);
criterion_main!(benches);
