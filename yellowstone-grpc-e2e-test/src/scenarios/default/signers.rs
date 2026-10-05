use {
    crate::scenarios::RunConfig,
    anyhow::{bail, ensure, Context, Result},
    solana_pubkey::Pubkey,
    std::collections::HashMap,
    tokio::time::{timeout, Duration},
    tokio_stream::StreamExt,
    yellowstone_grpc_client::{GeyserStream, SubscribeDeshredStream},
    yellowstone_grpc_e2e_macros::test_helper,
    yellowstone_grpc_proto::{
        geyser::{
            subscribe_update::UpdateOneof, subscribe_update_deshred, SubscribeDeshredRequest,
            SubscribeRequest, SubscribeRequestFilterDeshredTransactions,
            SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions,
        },
        solana::storage::confirmed_block::Transaction,
    },
};

// Every vote transaction references the vote program and is signed by the
// validator identity, while the vote program itself never signs. That holds
// on any live cluster, so the scenarios need no fixed accounts.
const VOTE_PROGRAM: &str = "Vote111111111111111111111111111111111111111";
const NEXT_UPDATE_TIMEOUT: Duration = Duration::from_secs(30);
const TRANSACTIONS_TO_CHECK: usize = 5;
const SLOTS_TO_WATCH: usize = 20;

/// The first `num_required_signatures` static keys, as the server defines
/// signers.
fn signers(transaction: Option<&Transaction>) -> Result<Vec<Pubkey>> {
    let message = transaction
        .and_then(|tx| tx.message.as_ref())
        .context("transaction should carry a message")?;
    let num_signers = message
        .header
        .as_ref()
        .context("message should carry a header")?
        .num_required_signatures;
    message
        .account_keys
        .iter()
        .take(usize::try_from(num_signers)?)
        .map(|key| Pubkey::try_from(key.as_slice()).context("account key should be 32 bytes"))
        .collect()
}

/// What a filtered stream delivered next: a transaction's signers or a slot.
enum Next {
    Transaction(Vec<Pubkey>),
    Slot,
}

async fn next(stream: &mut GeyserStream) -> Result<Next> {
    loop {
        let update = timeout(NEXT_UPDATE_TIMEOUT, stream.next())
            .await
            .context("no update before the timeout")?
            .context("stream ended")??;
        match update.update_oneof {
            Some(UpdateOneof::Transaction(update)) => {
                let info = update
                    .transaction
                    .context("transaction update should have transaction field")?;
                return signers(info.transaction.as_ref()).map(Next::Transaction);
            }
            Some(UpdateOneof::Slot(_)) => return Ok(Next::Slot),
            _ => {}
        }
    }
}

async fn next_deshred_signers(stream: &mut SubscribeDeshredStream) -> Result<Vec<Pubkey>> {
    loop {
        let update = timeout(NEXT_UPDATE_TIMEOUT, stream.next())
            .await
            .context("no deshred transaction before the timeout")?
            .context("stream ended")??;
        if let Some(subscribe_update_deshred::UpdateOneof::DeshredTransaction(update)) =
            update.update_oneof
        {
            let info = update
                .transaction
                .context("deshred update should have transaction field")?;
            return signers(info.transaction.as_ref());
        }
    }
}

/// Transactions and slot updates, so a filter that matches nothing still
/// shows the stream is alive.
fn transactions_request(filter: SubscribeRequestFilterTransactions) -> SubscribeRequest {
    SubscribeRequest {
        transactions: HashMap::from([("signers".to_owned(), filter)]),
        slots: HashMap::from([("slots".to_owned(), SubscribeRequestFilterSlots::default())]),
        commitment: Some(0),
        ..Default::default()
    }
}

fn deshred_request(filter: SubscribeRequestFilterDeshredTransactions) -> SubscribeDeshredRequest {
    SubscribeDeshredRequest {
        deshred_transactions: HashMap::from([("signers".to_owned(), filter)]),
        ..Default::default()
    }
}

/// signer_include delivers only transactions the listed account signed, a
/// referenced key that never signs matches nothing, and signer_exclude drops
/// the listed signer.
#[test_helper(name = "filter-transactions-signer")]
pub async fn subscribe_should_filter_transactions_by_signer(config: &RunConfig) -> Result<()> {
    let mut client = crate::grpc::new_client(config).await?;

    let mut votes = client
        .subscribe_once(transactions_request(SubscribeRequestFilterTransactions {
            vote: Some(true),
            ..Default::default()
        }))
        .await
        .context("vote subscription should succeed")?;
    let identity = loop {
        if let Next::Transaction(signers) = next(&mut votes).await? {
            break *signers
                .first()
                .context("a vote transaction has a fee payer")?;
        }
    };
    drop(votes);

    let mut by_identity = client
        .subscribe_once(transactions_request(SubscribeRequestFilterTransactions {
            signer_include: vec![identity.to_string()],
            ..Default::default()
        }))
        .await
        .context("signer_include subscription should succeed")?;
    let mut checked = 0;
    while checked < TRANSACTIONS_TO_CHECK {
        if let Next::Transaction(signers) = next(&mut by_identity).await? {
            ensure!(
                signers.contains(&identity),
                "signer_include delivered a transaction {identity} did not sign"
            );
            checked += 1;
        }
    }
    drop(by_identity);

    let mut by_vote_program = client
        .subscribe_once(transactions_request(SubscribeRequestFilterTransactions {
            signer_include: vec![VOTE_PROGRAM.to_owned()],
            ..Default::default()
        }))
        .await
        .context("signer_include subscription should succeed")?;
    for _ in 0..SLOTS_TO_WATCH {
        if let Next::Transaction(_) = next(&mut by_vote_program).await? {
            bail!("the vote program matched signer_include but never signs");
        }
    }
    drop(by_vote_program);

    // With a single validator nothing is left to deliver, so only the
    // transactions that do arrive are checked.
    let mut without_identity = client
        .subscribe_once(transactions_request(SubscribeRequestFilterTransactions {
            vote: Some(true),
            signer_exclude: vec![identity.to_string()],
            ..Default::default()
        }))
        .await
        .context("signer_exclude subscription should succeed")?;
    for _ in 0..SLOTS_TO_WATCH {
        if let Next::Transaction(signers) = next(&mut without_identity).await? {
            ensure!(
                !signers.contains(&identity),
                "signer_exclude delivered a transaction {identity} signed"
            );
        }
    }
    Ok(())
}

/// The deshred stream applies signer_include the same way, before execution.
#[test_helper(name = "deshred-signer", tags = ["deshred"])]
pub async fn subscribe_deshred_should_filter_by_signer(config: &RunConfig) -> Result<()> {
    let mut client = crate::grpc::new_client(config).await?;

    let mut votes = client
        .subscribe_deshred_once(deshred_request(SubscribeRequestFilterDeshredTransactions {
            vote: Some(true),
            ..Default::default()
        }))
        .await
        .context("vote subscription should succeed")?;
    let identity = *next_deshred_signers(&mut votes)
        .await?
        .first()
        .context("a vote transaction has a fee payer")?;
    drop(votes);

    let mut by_identity = client
        .subscribe_deshred_once(deshred_request(SubscribeRequestFilterDeshredTransactions {
            signer_include: vec![identity.to_string()],
            ..Default::default()
        }))
        .await
        .context("signer_include subscription should succeed")?;
    for _ in 0..TRANSACTIONS_TO_CHECK {
        let signers = next_deshred_signers(&mut by_identity).await?;
        ensure!(
            signers.contains(&identity),
            "signer_include delivered a deshred transaction {identity} did not sign"
        );
    }
    Ok(())
}
