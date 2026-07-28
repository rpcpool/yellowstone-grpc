//! Auto-reconnect and dedup over a real gRPC transport.
//!
//! Drives `GeyserGrpcClient` against an in-process Geyser server through forced
//! disconnects. The server replays from the requested `from_slot`, so the client
//! receives real duplicates and must suppress them without dropping anything.
//!
//! Each slot emits: account(wv=1), transaction, block_meta, account(wv=2),
//! slot_status. The account after block_meta models a freeze-time write.

use {
    futures::StreamExt,
    std::{
        collections::HashMap,
        net::SocketAddr,
        pin::Pin,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
        time::Duration,
    },
    tokio::time::timeout,
    tokio_stream::wrappers::TcpListenerStream,
    tonic::{transport::Server, Request, Response, Status, Streaming},
    yellowstone_grpc_client::{Backoff, GeyserGrpcClient, ReconnectConfig},
    yellowstone_grpc_proto::{
        geyser::{
            geyser_server::{Geyser, GeyserServer},
            SubscribeUpdateAccount, SubscribeUpdateAccountInfo,
        },
        prelude::{
            subscribe_update::UpdateOneof, GetBlockHeightRequest, GetBlockHeightResponse,
            GetLatestBlockhashRequest, GetLatestBlockhashResponse, GetSlotRequest, GetSlotResponse,
            GetVersionRequest, GetVersionResponse, IsBlockhashValidRequest,
            IsBlockhashValidResponse, PingRequest, PongResponse, SubscribeDeshredRequest,
            SubscribeReplayInfoRequest, SubscribeReplayInfoResponse, SubscribeRequest,
            SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocksMeta, SubscribeUpdate,
            SubscribeUpdateBlockMeta, SubscribeUpdateDeshred, SubscribeUpdateSlot,
            SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
        },
    },
};

const FIRST_SLOT: u64 = 1_000;
const LAST_SLOT: u64 = 1_040;

type MsgId = (u64, &'static str, u64);

fn identify(u: &SubscribeUpdate) -> Option<MsgId> {
    match u.update_oneof.as_ref()? {
        UpdateOneof::Account(m) => Some((m.slot, "account", m.account.as_ref()?.write_version)),
        UpdateOneof::Transaction(m) => Some((m.slot, "transaction", m.transaction.as_ref()?.index)),
        UpdateOneof::BlockMeta(m) => Some((m.slot, "block_meta", 0)),
        UpdateOneof::Slot(m) => Some((m.slot, "slot_status", m.status as u64)),
        _ => None,
    }
}

fn account(slot: u64, write_version: u64, filters: &[&str]) -> SubscribeUpdate {
    SubscribeUpdate {
        filters: filters.iter().map(|s| (*s).to_string()).collect(),
        update_oneof: Some(UpdateOneof::Account(SubscribeUpdateAccount {
            account: Some(SubscribeUpdateAccountInfo {
                pubkey: vec![(write_version % 251) as u8; 32],
                lamports: 1,
                owner: vec![0; 32],
                executable: false,
                rent_epoch: 0,
                data: vec![].into(),
                write_version,
                txn_signature: None,
            }),
            slot,
            is_startup: false,
        })),
        created_at: None,
    }
}

fn transaction(slot: u64, index: u64, filters: &[&str]) -> SubscribeUpdate {
    SubscribeUpdate {
        filters: filters.iter().map(|s| (*s).to_string()).collect(),
        update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
            transaction: Some(SubscribeUpdateTransactionInfo {
                signature: vec![(slot % 251) as u8; 64],
                is_vote: false,
                transaction: None,
                meta: None,
                index,
            }),
            slot,
        })),
        created_at: None,
    }
}

fn block_meta(slot: u64, filters: &[&str]) -> SubscribeUpdate {
    SubscribeUpdate {
        filters: filters.iter().map(|s| (*s).to_string()).collect(),
        update_oneof: Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
            slot,
            blockhash: format!("hash-{slot}"),
            parent_slot: slot - 1,
            ..Default::default()
        })),
        created_at: None,
    }
}

fn slot_status(slot: u64, filters: &[&str]) -> SubscribeUpdate {
    SubscribeUpdate {
        filters: filters.iter().map(|s| (*s).to_string()).collect(),
        update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
            slot,
            parent: Some(slot - 1),
            status: 0,
            dead_error: None,
        })),
        created_at: None,
    }
}

#[derive(Clone)]
struct Script {
    payload_filter: String,
    control_filter: String,
}

impl Script {
    fn messages_for_slot(&self, slot: u64) -> Vec<SubscribeUpdate> {
        let p = self.payload_filter.as_str();
        let c = self.control_filter.as_str();
        vec![
            account(slot, slot * 10 + 1, &[p]),
            transaction(slot, 0, &[p]),
            block_meta(slot, &[c]),
            account(slot, slot * 10 + 2, &[p]),
            slot_status(slot, &[c]),
        ]
    }
}

struct MockGeyser {
    script: Script,
    abort_after: Vec<Option<usize>>,
    connections: Arc<AtomicUsize>,
    from_slots: Arc<Mutex<Vec<Option<u64>>>>,
}

type UpdateStream = Pin<Box<dyn futures::Stream<Item = Result<SubscribeUpdate, Status>> + Send>>;

#[tonic::async_trait]
impl Geyser for MockGeyser {
    type SubscribeStream = UpdateStream;
    type SubscribeDeshredStream =
        Pin<Box<dyn futures::Stream<Item = Result<SubscribeUpdateDeshred, Status>> + Send>>;

    async fn subscribe(
        &self,
        request: Request<Streaming<SubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let mut inbound = request.into_inner();
        let first = inbound
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("no subscribe request"))??;

        let conn = self.connections.fetch_add(1, Ordering::SeqCst);
        self.from_slots
            .lock()
            .expect("from_slots poisoned")
            .push(first.from_slot);

        let start = first.from_slot.unwrap_or(FIRST_SLOT).max(FIRST_SLOT);
        let abort_after = self.abort_after.get(conn).copied().flatten();
        let script = self.script.clone();

        let (tx, rx) = tokio::sync::mpsc::channel(16);
        tokio::spawn(async move {
            let mut sent = 0usize;
            'outer: for slot in start..=LAST_SLOT {
                for msg in script.messages_for_slot(slot) {
                    if let Some(limit) = abort_after {
                        if sent >= limit {
                            let _ = tx
                                .send(Err(Status::unavailable("simulated disconnect")))
                                .await;
                            return;
                        }
                    }
                    sent += 1;
                    if tx.send(Ok(msg)).await.is_err() {
                        break 'outer;
                    }
                }
            }
            // Keep the stream open without a clean EOF until the client leaves.
            tx.closed().await;
        });

        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    async fn subscribe_deshred(
        &self,
        _r: Request<Streaming<SubscribeDeshredRequest>>,
    ) -> Result<Response<Self::SubscribeDeshredStream>, Status> {
        Err(Status::unimplemented("not used"))
    }
    async fn subscribe_replay_info(
        &self,
        _r: Request<SubscribeReplayInfoRequest>,
    ) -> Result<Response<SubscribeReplayInfoResponse>, Status> {
        Ok(Response::new(SubscribeReplayInfoResponse {
            first_available: Some(FIRST_SLOT),
        }))
    }
    async fn ping(&self, _r: Request<PingRequest>) -> Result<Response<PongResponse>, Status> {
        Ok(Response::new(PongResponse { count: 1 }))
    }
    async fn get_latest_blockhash(
        &self,
        _r: Request<GetLatestBlockhashRequest>,
    ) -> Result<Response<GetLatestBlockhashResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }
    async fn get_block_height(
        &self,
        _r: Request<GetBlockHeightRequest>,
    ) -> Result<Response<GetBlockHeightResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }
    async fn get_slot(
        &self,
        _r: Request<GetSlotRequest>,
    ) -> Result<Response<GetSlotResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }
    async fn is_blockhash_valid(
        &self,
        _r: Request<IsBlockhashValidRequest>,
    ) -> Result<Response<IsBlockhashValidResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }
    async fn get_version(
        &self,
        _r: Request<GetVersionRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        Ok(Response::new(GetVersionResponse {
            version: "test".to_string(),
        }))
    }
}

struct Harness {
    addr: SocketAddr,
    connections: Arc<AtomicUsize>,
    from_slots: Arc<Mutex<Vec<Option<u64>>>>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn spawn_server(script: Script, abort_after: Vec<Option<usize>>) -> Harness {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local addr");

    let connections = Arc::new(AtomicUsize::new(0));
    let from_slots = Arc::new(Mutex::new(Vec::new()));

    let service = MockGeyser {
        script,
        abort_after,
        connections: Arc::clone(&connections),
        from_slots: Arc::clone(&from_slots),
    };

    let server = tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(GeyserServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    Harness {
        addr,
        connections,
        from_slots,
        server,
    }
}

fn expected_ids(script: &Script, user_sees_control: bool) -> Vec<MsgId> {
    let mut out = Vec::new();
    for slot in FIRST_SLOT..=LAST_SLOT {
        for msg in script.messages_for_slot(slot) {
            let is_control = matches!(
                msg.update_oneof.as_ref(),
                Some(UpdateOneof::BlockMeta(_)) | Some(UpdateOneof::Slot(_))
            );
            if is_control && !user_sees_control {
                continue;
            }
            out.push(identify(&msg).expect("identifiable"));
        }
    }
    out
}

async fn drive(addr: SocketAddr, request: SubscribeRequest, stop_at_slot: u64) -> Vec<MsgId> {
    let mut client = GeyserGrpcClient::build_from_shared(format!("http://{addr}"))
        .expect("endpoint")
        .set_reconnect_config(ReconnectConfig::default().with_backoff(Backoff::new(
            Duration::from_millis(1),
            1.0,
            10,
        )))
        .connect()
        .await
        .expect("connect");

    let mut stream = client.subscribe_once(request).await.expect("subscribe");

    let mut got = Vec::new();
    while let Ok(Some(item)) = timeout(Duration::from_secs(10), stream.next()).await {
        let update = item.expect("stream must not surface an error");
        if let Some(id) = identify(&update) {
            let slot = id.0;
            got.push(id);
            if slot >= stop_at_slot {
                break;
            }
        }
    }
    got
}

fn assert_no_loss_no_dupes(got: &[MsgId], expected: &[MsgId], upto_slot: u64) {
    let mut seen: HashMap<MsgId, usize> = HashMap::new();
    for id in got {
        *seen.entry(*id).or_default() += 1;
    }

    let dupes: Vec<_> = seen.iter().filter(|(_, n)| **n > 1).collect();
    assert!(
        dupes.is_empty(),
        "duplicate messages delivered to the user: {dupes:?}"
    );

    let missing: Vec<_> = expected
        .iter()
        .filter(|(slot, _, _)| *slot <= upto_slot)
        .filter(|id| !seen.contains_key(id))
        .collect();
    assert!(
        missing.is_empty(),
        "messages lost across reconnect ({} of them): {:?}",
        missing.len(),
        &missing[..missing.len().min(15)]
    );
}

/// Without blocks_meta the client injects its own filters, so the user sees
/// payloads only.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_reconnect_injected_filters_no_loss_no_dupes() {
    let script = Script {
        payload_filter: "user_payload".to_string(),
        // must match reconnect.rs AUTORECONNECT_FILTER_KEY
        control_filter: "__autoreconnect".to_string(),
    };
    let harness = spawn_server(script.clone(), vec![Some(37), Some(60)]).await;

    let mut accounts = HashMap::new();
    accounts.insert(
        "user_payload".to_string(),
        SubscribeRequestFilterAccounts::default(),
    );
    let request = SubscribeRequest {
        accounts,
        ..Default::default()
    };

    let got = drive(harness.addr, request, LAST_SLOT).await;

    assert!(
        harness.connections.load(Ordering::SeqCst) > 1,
        "expected at least one reconnect, got {} connection(s)",
        harness.connections.load(Ordering::SeqCst)
    );
    println!(
        "connections={} from_slots={:?}",
        harness.connections.load(Ordering::SeqCst),
        harness.from_slots.lock().expect("poisoned")
    );

    let expected = expected_ids(&script, false);
    assert_no_loss_no_dupes(&got, &expected, LAST_SLOT - 1);
}

/// Sweeps the disconnect point across the message timeline so the cut lands
/// before, at and after block_meta, on slot boundaries and mid-slot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_reconnect_at_every_offset_no_loss_no_dupes() {
    let script = Script {
        payload_filter: "user_payload".to_string(),
        control_filter: "user_bm".to_string(),
    };
    let expected = expected_ids(&script, true);

    let mut accounts = HashMap::new();
    accounts.insert(
        "user_payload".to_string(),
        SubscribeRequestFilterAccounts::default(),
    );
    let mut blocks_meta = HashMap::new();
    blocks_meta.insert(
        "user_bm".to_string(),
        SubscribeRequestFilterBlocksMeta::default(),
    );
    let request = SubscribeRequest {
        accounts,
        blocks_meta,
        ..Default::default()
    };

    // Five messages per slot. The second cut exercises nested replay windows.
    for first_cut in 6..=60usize {
        let harness = spawn_server(
            script.clone(),
            vec![Some(first_cut), Some(first_cut + 17), None],
        )
        .await;

        let got = drive(harness.addr, request.clone(), LAST_SLOT).await;

        assert!(
            harness.connections.load(Ordering::SeqCst) > 1,
            "cut at {first_cut}: expected a reconnect"
        );

        let mut seen: HashMap<MsgId, usize> = HashMap::new();
        for id in &got {
            *seen.entry(*id).or_default() += 1;
        }
        let dupes: Vec<_> = seen.iter().filter(|(_, n)| **n > 1).collect();
        assert!(
            dupes.is_empty(),
            "cut at {first_cut} (from_slots={:?}): duplicates {dupes:?}",
            harness.from_slots.lock().expect("poisoned")
        );
        let missing: Vec<_> = expected
            .iter()
            .filter(|(slot, _, _)| *slot < LAST_SLOT)
            .filter(|id| !seen.contains_key(id))
            .collect();
        assert!(
            missing.is_empty(),
            "cut at {first_cut} (from_slots={:?}): lost {} message(s): {:?}",
            harness.from_slots.lock().expect("poisoned"),
            missing.len(),
            &missing[..missing.len().min(10)]
        );
    }
}

/// With blocks_meta subscribed and slots not, nothing is injected and
/// block_meta is the first message seen per slot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_reconnect_user_blocks_meta_no_loss_no_dupes() {
    let script = Script {
        payload_filter: "user_payload".to_string(),
        control_filter: "user_bm".to_string(),
    };
    let harness = spawn_server(script.clone(), vec![Some(43), None]).await;

    let mut accounts = HashMap::new();
    accounts.insert(
        "user_payload".to_string(),
        SubscribeRequestFilterAccounts::default(),
    );
    let mut blocks_meta = HashMap::new();
    blocks_meta.insert(
        "user_bm".to_string(),
        SubscribeRequestFilterBlocksMeta::default(),
    );
    let request = SubscribeRequest {
        accounts,
        blocks_meta,
        ..Default::default()
    };

    let got = drive(harness.addr, request, LAST_SLOT).await;

    assert!(
        harness.connections.load(Ordering::SeqCst) > 1,
        "expected at least one reconnect"
    );
    println!(
        "connections={} from_slots={:?}",
        harness.connections.load(Ordering::SeqCst),
        harness.from_slots.lock().expect("poisoned")
    );

    let expected = expected_ids(&script, true);
    assert_no_loss_no_dupes(&got, &expected, LAST_SLOT - 1);
}
