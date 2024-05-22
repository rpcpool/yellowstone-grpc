use {
    crate::scylladb::types::{
        BlockchainEvent, BlockchainEventType, ProducerId, ShardId, ShardOffset, ShardPeriod,
        SHARD_OFFSET_MODULO,
    },
    core::fmt,
    scylla::{prepared_statement::PreparedStatement, Session},
    std::{collections::VecDeque, sync::Arc},
    tokio::sync::oneshot::{self, error::TryRecvError},
    tracing::warn,
};

const MICRO_BATCH_SIZE: usize = 40;

pub const GET_NEW_TRANSACTION_EVENT: &str = r###"
    SELECT
        shard_id,
        period,
        producer_id,
        offset,
        slot,
        event_type,

        pubkey,
        lamports,
        owner,
        executable,
        rent_epoch,
        write_version,
        data,
        txn_signature,

        signature,
        signatures,
        num_required_signatures,
        num_readonly_signed_accounts,
        num_readonly_unsigned_accounts,
        account_keys,
        recent_blockhash,
        instructions,
        versioned,
        address_table_lookups,
        meta,
        is_vote,
        tx_index
    FROM log
    WHERE producer_id = ? and shard_id = ? and offset > ? and period = ?
    and event_type = 1
    ORDER BY offset ASC
    ALLOW FILTERING
"###;

const GET_LAST_SHARD_PERIOD_COMMIT: &str = r###"
    SELECT
        period
    FROM producer_period_commit_log
    WHERE 
        producer_id = ?
        AND shard_id = ?
    ORDER BY period DESC
    PER PARTITION LIMIT 1
"###;

/// Represents the state of a shard iterator, which is used to manage the iteration
/// and retrieval of blockchain events from a shard.
///
/// The `ShardIteratorState` enum encapsulates different states that the iterator
/// can be in during its lifecycle.
enum ShardIteratorState {
    /// The iterator is initialized and empty.
    Empty(ShardOffset),

    /// The iterator is in the process of loading blockchain events from the shard.
    Loading(ShardOffset, oneshot::Receiver<VecDeque<BlockchainEvent>>),

    /// The iterator has loaded blockchain events and is ready for retrieval.
    Loaded(ShardOffset, VecDeque<BlockchainEvent>),

    /// The iterator is confirming the end of a period in the shard.
    ConfirmingPeriod(ShardOffset, oneshot::Receiver<bool>),

    /// The iterator is actively streaming blockchain events.
    AvailableData(ShardOffset, VecDeque<BlockchainEvent>),

    /// The iterator is waiting for the end of a period in the shard.
    WaitingEndOfPeriod(ShardOffset, oneshot::Receiver<bool>),
}

impl fmt::Debug for ShardIteratorState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty(arg0) => f.debug_tuple("Empty").field(arg0).finish(),
            Self::Loading(arg0, _) => f.debug_tuple("Loading").field(arg0).finish(),
            Self::Loaded(arg0, micro_batch) => f
                .debug_tuple("Loaded")
                .field(arg0)
                .field(&format!("micro_batch({})", micro_batch.len()))
                .finish(),
            Self::ConfirmingPeriod(arg0, _) => {
                f.debug_tuple("ConfirmingPeriod").field(arg0).finish()
            }
            Self::AvailableData(arg0, micro_batch) => f
                .debug_tuple("Available")
                .field(arg0)
                .field(&format!("micro_batch({})", micro_batch.len()))
                .finish(),
            Self::WaitingEndOfPeriod(arg0, _) => f.debug_tuple("EndOfPeriod").field(arg0).finish(),
        }
    }
}

impl ShardIteratorState {
    const fn last_offset(&self) -> ShardOffset {
        match self {
            Self::Empty(offset) => *offset,
            Self::Loading(offset, _) => *offset,
            Self::Loaded(offset, _) => *offset,
            Self::ConfirmingPeriod(offset, _) => *offset,
            Self::AvailableData(offset, _) => *offset,
            Self::WaitingEndOfPeriod(offset, _) => *offset,
        }
    }

    const fn is_empty(&self) -> bool {
        matches!(self, ShardIteratorState::Empty(_))
    }
}

#[derive(Clone, Default)]
pub(crate) struct ShardFilter {
    pub(crate) tx_account_keys: Vec<Vec<u8>>,
    pub(crate) account_owners: Vec<Vec<u8>>,
    pub(crate) account_pubkyes: Vec<Vec<u8>>,
}

pub(crate) struct ShardIterator {
    session: Arc<Session>,
    pub(crate) producer_id: ProducerId,
    pub(crate) shard_id: ShardId,
    inner: ShardIteratorState,
    pub(crate) event_type: BlockchainEventType,
    get_events_prepared_stmt: PreparedStatement,
    get_last_shard_period_commit_prepared_stmt: PreparedStatement,
    last_period_confirmed: ShardPeriod,
    filter: ShardFilter,
}

/// Represents an iterator for fetching and processing blockchain events from a specific shard.
/// The iterator fetch "micro batch" at a time.
impl ShardIterator {
    pub(crate) async fn new(
        session: Arc<Session>,
        producer_id: ProducerId,
        shard_id: ShardId,
        offset: ShardOffset,
        event_type: BlockchainEventType,
        filter: Option<ShardFilter>,
    ) -> anyhow::Result<Self> {
        let get_events_ps = if event_type == BlockchainEventType::AccountUpdate {
            let query_str = forge_account_upadate_event_query(filter.clone().unwrap_or_default());
            session.prepare(query_str).await?
        } else {
            session.prepare(GET_NEW_TRANSACTION_EVENT).await?
        };

        let get_last_shard_period_commit = session.prepare(GET_LAST_SHARD_PERIOD_COMMIT).await?;

        Ok(ShardIterator {
            session,
            producer_id,
            shard_id,
            inner: ShardIteratorState::Empty(offset),
            event_type,
            get_events_prepared_stmt: get_events_ps,
            get_last_shard_period_commit_prepared_stmt: get_last_shard_period_commit,
            last_period_confirmed: (offset / SHARD_OFFSET_MODULO) - 1,
            filter: filter.unwrap_or_default(),
        })
    }

    pub(crate) const fn last_offset(&self) -> ShardOffset {
        self.inner.last_offset()
    }

    /// Warms up the shard iterator by loading the initial micro batch if in the `Empty` state.
    pub(crate) async fn warm(&mut self) -> anyhow::Result<()> {
        if !self.inner.is_empty() {
            return Ok(());
        }
        let last_offset = self.inner.last_offset();

        let micro_batch = self.fetch_micro_batch(last_offset).await?;
        let new_state = ShardIteratorState::AvailableData(last_offset, micro_batch);
        self.inner = new_state;
        Ok(())
    }

    /// Checks if a period is committed based on the given last offset.
    fn is_period_committed(&self, last_offset: ShardOffset) -> oneshot::Receiver<bool> {
        let session = Arc::clone(&self.session);
        let producer_id = self.producer_id;
        let ps = self.get_last_shard_period_commit_prepared_stmt.clone();
        let shard_id = self.shard_id;
        let period = last_offset / SHARD_OFFSET_MODULO;
        let (sender, receiver) = oneshot::channel();
        tokio::spawn(async move {
            let result = session
                .execute(&ps, (producer_id, shard_id))
                .await
                .expect("failed to query period commit state")
                .maybe_first_row_typed::<(ShardPeriod,)>()
                .expect("query not elligible to return rows")
                .map(|row| row.0 >= period)
                .unwrap_or(false);
            sender.send(result).map_err(|_| ()).unwrap_or_else(|_| {
                panic!(
                    "failed to send back period commit status to shard iterator {}",
                    shard_id
                )
            });
        });
        receiver
    }

    /// Fetches a micro batch of blockchain events starting from the given last offset.
    fn fetch_micro_batch(
        &self,
        last_offset: ShardOffset,
    ) -> oneshot::Receiver<VecDeque<BlockchainEvent>> {
        let period = (last_offset + 1) / SHARD_OFFSET_MODULO;
        let producer_id = self.producer_id;
        let ps = self.get_events_prepared_stmt.clone();
        let shard_id = self.shard_id;
        let session = Arc::clone(&self.session);
        let (sender, receiver) = oneshot::channel();
        tokio::spawn(async move {
            let micro_batch = session
                .execute(&ps, (producer_id, shard_id, last_offset, period))
                .await
                .expect("failed to fetch micro batch from scylladb")
                .rows_typed_or_empty::<BlockchainEvent>()
                .collect::<Result<VecDeque<_>, _>>()
                .expect("failed to typed scylladb rows");
            if sender.send(micro_batch).is_err() {
                warn!("Shard iterator {shard_id} was fetching micro batch, but client closed its stream half.")
            }
        });
        receiver
    }

    ///
    /// Apply any filter that cannot be pushed down to the database
    ///
    fn filter_row(&self, row: BlockchainEvent) -> Option<BlockchainEvent> {
        if row.event_type == BlockchainEventType::NewTransaction {
            // Apply transaction filter here
            let elligible_acc_keys = &self.filter.tx_account_keys;
            if !elligible_acc_keys.is_empty() {
                let is_row_elligible = row
                    .account_keys
                    .as_ref()
                    .filter(|actual_keys| {
                        actual_keys
                            .iter()
                            .any(|account_key| elligible_acc_keys.contains(account_key))
                    })
                    .map(|_| true)
                    .unwrap_or(false);
                if !is_row_elligible {
                    return None;
                }
            }
        }

        Some(row)
    }

    /// Attempts to retrieve the next blockchain event from the shard iterator.
    ///
    /// This method asynchronously advances the iterator's state and fetches the next blockchain event
    /// based on its current state.
    ///
    /// It handles different states of the iterator and performs
    /// appropriate actions such as loading, streaming, and period confirmation.
    ///
    /// Returns `Ok(None)` if no event is available or the iterator is waiting for period confirmation.
    pub(crate) async fn try_next(&mut self) -> anyhow::Result<Option<BlockchainEvent>> {
        let last_offset = self.inner.last_offset();
        let current_state =
            std::mem::replace(&mut self.inner, ShardIteratorState::Empty(last_offset));

        let (next_state, maybe_to_return) = match current_state {
            ShardIteratorState::Empty(last_offset) => {
                let receiver = self.fetch_micro_batch(last_offset);
                (ShardIteratorState::Loading(last_offset, receiver), None)
            }
            ShardIteratorState::Loading(last_offset, mut receiver) => {
                let result = receiver.try_recv();
                match result {
                    Err(TryRecvError::Empty) => {
                        (ShardIteratorState::Loading(last_offset, receiver), None)
                    }
                    Err(TryRecvError::Closed) => anyhow::bail!("failed to receive micro batch"),
                    Ok(micro_batch) => (ShardIteratorState::Loaded(last_offset, micro_batch), None),
                }
            }
            ShardIteratorState::Loaded(last_offset, mut micro_batch) => {
                let maybe_row = micro_batch.pop_front();
                if let Some(row) = maybe_row {
                    (
                        ShardIteratorState::AvailableData(row.offset, micro_batch),
                        Some(row),
                    )
                } else {
                    let curr_period = last_offset / SHARD_OFFSET_MODULO;
                    if curr_period <= self.last_period_confirmed {
                        let last_offset_for_curr_period =
                            ((curr_period + 1) * SHARD_OFFSET_MODULO) - 1;
                        (ShardIteratorState::Empty(last_offset_for_curr_period), None)
                    } else {
                        // If a newly loaded row stream is already empty, we must figure out if
                        // its because there no more data in the period or is it because we consume too fast and we should try again later.
                        let receiver = self.is_period_committed(last_offset);
                        (
                            ShardIteratorState::ConfirmingPeriod(last_offset, receiver),
                            None,
                        )
                    }
                }
            }
            ShardIteratorState::ConfirmingPeriod(last_offset, mut rx) => match rx.try_recv() {
                Err(TryRecvError::Empty) => {
                    (ShardIteratorState::ConfirmingPeriod(last_offset, rx), None)
                }
                Err(TryRecvError::Closed) => anyhow::bail!("fail"),
                Ok(period_committed) => {
                    if period_committed {
                        self.last_period_confirmed = last_offset / SHARD_OFFSET_MODULO;
                    }
                    (ShardIteratorState::Empty(last_offset), None)
                }
            },
            ShardIteratorState::AvailableData(last_offset, mut micro_batch) => {
                let maybe_row = micro_batch.pop_front();
                if let Some(row) = maybe_row {
                    (
                        ShardIteratorState::AvailableData(row.offset, micro_batch),
                        Some(row),
                    )
                } else if (last_offset + 1) % SHARD_OFFSET_MODULO == 0 {
                    let receiver = self.is_period_committed(last_offset);
                    (
                        ShardIteratorState::WaitingEndOfPeriod(last_offset, receiver),
                        None,
                    )
                } else {
                    (ShardIteratorState::Empty(last_offset), None)
                }
            }
            ShardIteratorState::WaitingEndOfPeriod(last_offset, mut rx) => {
                match rx.try_recv() {
                    Err(TryRecvError::Empty) => (
                        ShardIteratorState::WaitingEndOfPeriod(last_offset, rx),
                        None,
                    ),
                    Err(TryRecvError::Closed) => anyhow::bail!("fail"),
                    Ok(period_committed) => {
                        if period_committed {
                            self.last_period_confirmed = last_offset / SHARD_OFFSET_MODULO;
                            (ShardIteratorState::Empty(last_offset), None)
                        } else {
                            // Renew the background task
                            let rx2 = self.is_period_committed(last_offset);
                            (
                                ShardIteratorState::WaitingEndOfPeriod(last_offset, rx2),
                                None,
                            )
                        }
                    }
                }
            }
        };
        let _ = std::mem::replace(&mut self.inner, next_state);
        Ok(maybe_to_return.and_then(|row| self.filter_row(row)))
    }
}

const LOG_PRIMARY_KEY_CONDITION: &str = r###"
    producer_id = ? and shard_id = ? and offset > ? and period = ?
"###;

const LOG_PROJECTION: &str = r###"
    shard_id,
    period,
    producer_id,
    offset,
    slot,
    event_type,
    pubkey,
    lamports,
    owner,
    executable,
    rent_epoch,
    write_version,
    data,
    txn_signature,
    signature,
    signatures,
    num_required_signatures,
    num_readonly_signed_accounts,
    num_readonly_unsigned_accounts,
    account_keys,
    recent_blockhash,
    instructions,
    versioned,
    address_table_lookups,
    meta,
    is_vote,
    tx_index
"###;

fn format_as_scylla_hexstring(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        panic!("byte slice is empty")
    }
    let hex = bytes
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<_>>()
        .join("");
    format!("0x{}", hex)
}

fn forge_account_upadate_event_query(filter: ShardFilter) -> String {
    let mut conds = vec![];

    let pubkeys = filter
        .account_pubkyes
        .iter()
        .map(|pubkey| format_as_scylla_hexstring(pubkey.as_slice()))
        .collect::<Vec<_>>();

    let owners = filter
        .account_owners
        .iter()
        .map(|owner| format_as_scylla_hexstring(owner.as_slice()))
        .collect::<Vec<_>>();

    if !pubkeys.is_empty() {
        let cond = format!("AND pubkey IN ({})", pubkeys.join(", "));
        conds.push(cond);
    }
    if !owners.is_empty() {
        let cond = format!("AND owner IN ({})", owners.join(", "));
        conds.push(cond)
    }
    let conds_string = conds.join(" ");

    format!(
        r###"
        SELECT
        {projection}
        FROM log
        WHERE {primary_key_cond}
        AND event_type = 0
        {other_conds}
        ORDER BY offset ASC
        LIMIT {batch_size}
        ALLOW FILTERING
        "###,
        projection = LOG_PROJECTION,
        primary_key_cond = LOG_PRIMARY_KEY_CONDITION,
        other_conds = conds_string,
        batch_size = MICRO_BATCH_SIZE,
    )
}
