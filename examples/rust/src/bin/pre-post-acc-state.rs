use {
    futures::StreamExt,
    maplit::hashmap,
    std::time::Duration,
    yellowstone_grpc_client::GeyserGrpcClient,
    yellowstone_grpc_proto::geyser::{
        subscribe_update::UpdateOneof, SubscribeRequest, SubscribeRequestFilterTransactions,
    },
};

#[tokio::main]
async fn main() {
    let endpoint: &str = "http://localhost:10000";
    let builder = GeyserGrpcClient::build_from_shared(endpoint)
        .expect("Failed to build client")
        .http2_adaptive_window(true)
        .tcp_nodelay(true)
        .timeout(Duration::from_secs(30));
    let mut client = builder
        .connect()
        .await
        .expect("Failed to connect to server");
    println!("Connected to the gRPC server");

    let subscribe_request = SubscribeRequest {
        transactions: hashmap! {
            "".to_string() => SubscribeRequestFilterTransactions{
                failed: None,
                vote: Some(false),
                include_pre_post_accounts: Some(true),
                ..Default::default()
            },
        },
        ..Default::default()
    };

    let mut stream = client
        .subscribe_once(subscribe_request)
        .await
        .expect("Failed to subscribe");

    while let Some(message) = stream.next().await {
        let msg = message.expect("Failed to receive message");

        if let Some(UpdateOneof::Transaction(txn)) = msg.update_oneof {
            if let Some(subscribe_update) = &txn.transaction {
                println!(
                    "Received transaction with signature: {}",
                    bs58::encode(subscribe_update.signature.as_slice()).into_string()
                );
                if let Some(pre_post) = &subscribe_update.pre_post_account_states {
                    println!(
                        "Pre-account states: {} accounts",
                        pre_post.pre_account_states.len()
                    );
                    for (idx, account) in pre_post.pre_account_states.iter().enumerate() {
                        println!(
                            "  Account {}: pubkey={}, lamports={}, data_len={}",
                            idx,
                            bs58::encode(&account.pubkey).into_string(),
                            account.lamports,
                            account.data.len()
                        );
                    }
                    println!(
                        "Post-account states: {} accounts",
                        pre_post.post_account_states.len()
                    );
                    for (idx, account) in pre_post.post_account_states.iter().enumerate() {
                        println!(
                            "  Account {}: pubkey={}, lamports={}, data_len={}",
                            idx,
                            bs58::encode(&account.pubkey).into_string(),
                            account.lamports,
                            account.data.len()
                        );
                    }
                } else {
                    println!("No pre/post account states");
                }
            }
        }
    }
}
