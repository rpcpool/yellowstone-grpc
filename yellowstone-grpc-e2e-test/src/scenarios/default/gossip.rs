use {
    crate::scenarios::RunConfig,
    anyhow::{ensure, Context, Result},
    std::collections::HashSet,
    tokio::time::{timeout, Duration},
    tokio_stream::StreamExt,
    yellowstone_grpc_e2e_macros::test_helper,
    yellowstone_grpc_proto::geyser::subscribe_update_gossip::UpdateOneof,
};

/// A silent cluster would hang the stream reads, so bound every wait.
const RECV_TIMEOUT: Duration = Duration::from_secs(30);

/// Deltas to observe before checking revision continuity.
const DELTAS_TO_OBSERVE: usize = 64;

/// A subscriber must receive the whole gossip table before any delta.
///
/// Agave only replays the table once, at validator start, and dedups every unchanged
/// republish after that. Without the snapshot a client would never learn about a node that
/// does not change, so an empty or missing snapshot is a total failure of the feature.
#[test_helper(name = "gossip-snapshot", tags = ["gossip"])]
pub async fn test_gossip_snapshot(config: &RunConfig) -> Result<()> {
    let mut client = crate::grpc::new_client(config).await?;
    let mut stream = client
        .subscribe_gossip()
        .await
        .context("subscribe_gossip should succeed")?;

    loop {
        let update = timeout(RECV_TIMEOUT, stream.next())
            .await
            .context("timed out waiting for the snapshot")?
            .context("stream ended before the snapshot")?
            .context("stream should yield updates without error")?;

        match update.update_oneof {
            // Keepalives may precede the snapshot; they carry no revision.
            Some(UpdateOneof::Ping(_)) => {
                ensure!(update.seq == 0, "ping should not carry a revision");
                continue;
            }
            Some(UpdateOneof::Snapshot(topology)) => {
                ensure!(
                    !topology.nodes.is_empty(),
                    "snapshot should carry the gossip table, got 0 nodes"
                );
                ensure!(
                    update.seq > 0,
                    "snapshot should carry the revision it was copied at"
                );

                let unique: HashSet<_> = topology.nodes.iter().map(|n| &n.pubkey).collect();
                ensure!(
                    unique.len() == topology.nodes.len(),
                    "snapshot should hold one entry per node, got {} entries for {} pubkeys",
                    topology.nodes.len(),
                    unique.len()
                );
                return Ok(());
            }
            other => anyhow::bail!("expected a snapshot first, got {other:?}"),
        }
    }
}

/// Deltas after the snapshot must be gap-free and strictly newer than it.
///
/// The client applies the snapshot and then every update after it, so a gap leaves its table
/// permanently wrong, and a delta at or below the snapshot revision would re-apply a change
/// the snapshot already contains.
#[test_helper(name = "gossip-sequence", tags = ["gossip"])]
pub async fn test_gossip_sequence(config: &RunConfig) -> Result<()> {
    let mut client = crate::grpc::new_client(config).await?;
    let mut stream = client
        .subscribe_gossip()
        .await
        .context("subscribe_gossip should succeed")?;

    let mut snapshot_seq = None;
    let mut expected = 0u64;
    let mut observed = 0usize;

    while observed < DELTAS_TO_OBSERVE {
        let update = timeout(RECV_TIMEOUT, stream.next())
            .await
            .context("timed out waiting for updates")?
            .context("stream ended early")?
            .context("stream should yield updates without error")?;

        let seq = update.seq;
        match update.update_oneof {
            Some(UpdateOneof::Ping(_)) => continue,
            Some(UpdateOneof::Snapshot(topology)) => {
                ensure!(
                    snapshot_seq.is_none(),
                    "a second snapshot means the server re-synchronised this client"
                );
                log::info!(
                    "gossip snapshot: {} nodes at seq {seq}",
                    topology.nodes.len()
                );
                snapshot_seq = Some(seq);
                expected = seq + 1;
            }
            Some(UpdateOneof::Node(_) | UpdateOneof::Removed(_)) => {
                let snapshot_seq = snapshot_seq.context("delta arrived before the snapshot")?;
                ensure!(
                    seq > snapshot_seq,
                    "delta seq {seq} is not newer than snapshot seq {snapshot_seq}"
                );
                ensure!(
                    seq == expected,
                    "revision gap: expected {expected}, got {seq}"
                );
                expected = seq + 1;
                observed += 1;
            }
            other => anyhow::bail!("unexpected update {other:?}"),
        }
    }

    Ok(())
}
