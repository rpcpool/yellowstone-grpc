use {
    super::{
        prom::{
            scylladb_batch_request_lag_inc, scylladb_batch_request_lag_sub,
            scylladb_batch_sent_inc, scylladb_batch_size_observe, scylladb_batchitem_sent_inc_by,
        },
        types::{
            AccountUpdate, ProducerId, ShardId, ShardOffset,
            ShardedAccountUpdate, ShardedTransaction, Transaction, SHARD_OFFSET_MODULO,
        },
    }, deepsize::DeepSizeOf, scylla::{
        batch::{Batch, BatchType},
        frame::Compression,
        serialize::{
            row::{RowSerializationContext, SerializeRow},
            RowWriter,
        },
        Session, SessionBuilder,
    }, std::{
        iter::repeat, sync::Arc, time::Duration
    }, tokio::{task::JoinHandle, time::Instant}, tracing::{info, warn}
};

const WARNING_SCYLLADB_LATENCY_THRESHOLD: Duration = Duration::from_millis(50);

const DEFAULT_SHARD_MAX_BUFFER_CAPACITY: usize = 15;

const SHARD_COUNT: usize = 64;

const SCYLLADB_GET_LATEST_SHARD_OFFSET_FOR_PRODUCER_ID: &str = r###"
    SELECT 
        shard_id, 
        max(offset) as max_offset
    FROM shard_max_offset_mv 
    WHERE 
        producer_id = ? 
    GROUP BY producer_id, shard_id 
    ORDER BY shard_id;
"###;

const SCYLLADB_COMMIT_PRODUCER_PERIOD: &str = r###"
    INSERT INTO producer_period_commit_log (
        producer_id,
        shard_id,
        period,
        created_at
    )
    VALUES (?,?,?,currentTimestamp())
"###;

const SCYLLADB_INSERT_ACCOUNT_UPDATE: &str = r###"
    INSERT INTO log (
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
        created_at
    )
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,currentTimestamp())
"###;

const SCYLLADB_INSERT_TRANSACTION: &str = r###"
    INSERT INTO log (
        shard_id, 
        period,
        producer_id,
        offset,
        slot,
        event_type,
        signature,
        signatures,
        num_readonly_signed_accounts, 
        num_readonly_unsigned_accounts,
        num_required_signatures,
        account_keys, 
        recent_blockhash, 
        instructions, 
        versioned,
        address_table_lookups, 
        meta,
        is_vote,
        tx_index,

        created_at
    )
    VALUES (?,?,?,?,?,?, ?,?,?,?,?,?, ?,?,?,?,?, ?,?, currentTimestamp())
"###;

#[derive(Clone, PartialEq, Debug)]
pub struct ScyllaSinkConfig {
    pub producer_id: u8,
    pub batch_len_limit: usize,
    pub batch_size_kb_limit: usize,
    pub linger: Duration,
    pub keyspace: String,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, DeepSizeOf)]
enum ClientCommand {
    // Add other action if necessary...
    InsertAccountUpdate(AccountUpdate),
    InsertTransaction(Transaction),
}

impl ClientCommand {
    pub fn slot(&self) -> i64 {
        match self {
            Self::InsertAccountUpdate(x) => x.slot,
            Self::InsertTransaction(x) => x.slot,
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, DeepSizeOf)]
struct ShardedClientCommand {
    shard_id: ShardId,
    offset: ShardOffset,
    producer_id: ProducerId,
    client_command: ClientCommand,
}

impl SerializeRow for ShardedClientCommand {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter,
    ) -> Result<(), scylla::serialize::SerializationError> {
        //let period = (self.offset / SHARD_OFFSET_MODULO) * SHARD_OFFSET_MODULO;
        match &self.client_command {
            ClientCommand::InsertAccountUpdate(val) => {
                let val: ShardedAccountUpdate = val
                    .clone()
                    .as_blockchain_event(self.shard_id, self.producer_id, self.offset)
                    .into();
                //let serval = SerializedValues::from_serializable(&ctx, &val);
                val.serialize(ctx, writer)
            }
            ClientCommand::InsertTransaction(val) => {
                let val: ShardedTransaction = val
                    .clone()
                    .as_blockchain_event(self.shard_id, self.producer_id, self.offset)
                    .into();
                //let foo = val.serialize(ctx, writer);
                //println!("{:?}", foo);
                //foo
                //let serval = SerializedValues::from_serializable(&ctx, &val);
                val.serialize(ctx, writer)
            }
        }
    }

    fn is_empty(&self) -> bool {
        todo!()
    }
}

impl ClientCommand {
    fn with_shard_info(
        self,
        shard_id: ShardId,
        producer_id: ProducerId,
        offset: ShardOffset,
    ) -> ShardedClientCommand {
        ShardedClientCommand {
            shard_id,
            producer_id,
            offset,
            client_command: self,
        }
    }
}

struct Shard {
    session: Arc<Session>,
    shard_id: ShardId,
    producer_id: ProducerId,
    next_offset: ShardOffset,
    buffer: Vec<ShardedClientCommand>,
    max_buffer_capacity: usize,
    max_buffer_byte_size: usize,
   
    scylla_batch: Batch,
    curr_batch_byte_size: usize,
    buffer_linger: Duration,

    // This variable will hold any background (bg) period commit task
    bg_period_commit_task: Option<JoinHandle<anyhow::Result<()>>>,
}

impl Shard {
    fn new(
        session: Arc<Session>,
        shard_id: ShardId,
        producer_id: ProducerId,
        next_offset: ShardOffset,
        max_buffer_capacity: usize,
        max_buffer_byte_size: usize,
        buffer_linger: Duration,
    ) -> Self {
        Shard {
            session,
            shard_id,
            producer_id,
            next_offset,
            buffer: Vec::with_capacity(max_buffer_capacity),
            max_buffer_capacity,
            max_buffer_byte_size,
             // Since each shard will only batch into a single partition at a time, we can safely disable batch logging
            // without losing atomicity guarantee provided by scylla.
            scylla_batch: Batch::new(BatchType::Unlogged),
            buffer_linger,
            bg_period_commit_task: Default::default(),
            curr_batch_byte_size: 0,
        }
    }

    fn clear_buffer(&mut self) {
        self.buffer.clear();
        self.curr_batch_byte_size = 0;
        self.scylla_batch.statements.clear();
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        let buffer_len = self.buffer.len();
        if buffer_len > 0 {
            let before = Instant::now();
            // We must wait for the batch success to guarantee monotonicity in the shard's timeline.
            self.session.batch(&self.scylla_batch, &self.buffer).await?;
            scylladb_batch_request_lag_sub(buffer_len as i64);
            scylladb_batch_sent_inc();
            scylladb_batch_size_observe(buffer_len);
            scylladb_batchitem_sent_inc_by(buffer_len as u64);
            if before.elapsed() >= WARNING_SCYLLADB_LATENCY_THRESHOLD {
                warn!("sent {} elements in {:?}", buffer_len, before.elapsed());
            }
        }
        self.clear_buffer();
        Ok(())
    }

    fn into_daemon(mut self) -> tokio::sync::mpsc::Sender<ClientCommand> {
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<ClientCommand>(16);
    
        let _handle: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let insert_account_ps = self.session.prepare(SCYLLADB_INSERT_ACCOUNT_UPDATE).await?;
            let insert_tx_ps = self.session.prepare(SCYLLADB_INSERT_TRANSACTION).await?;

            let mut buffering_timeout = Instant::now() + self.buffer_linger;

            loop {
                let shard_id = self.shard_id;
                let producer_id = self.producer_id;
                let offset = self.next_offset;
                let curr_period = offset / SHARD_OFFSET_MODULO;
                self.next_offset += 1;

                let is_end_of_period = (offset + 1) % SHARD_OFFSET_MODULO == 0;

                let msg = receiver.recv().await.ok_or(anyhow::anyhow!("Shard mailbox closed"))?;
                let sharded_msg = msg.with_shard_info(shard_id, producer_id, offset);
                let msg_byte_size = sharded_msg.deep_size_of();

                let need_flush =  self.buffer.len() >= self.max_buffer_capacity 
                    || self.curr_batch_byte_size + msg_byte_size >= self.max_buffer_byte_size
                    || buffering_timeout.elapsed() > Duration::ZERO;

                if need_flush {
                    self.flush().await?;
                    buffering_timeout = Instant::now() + self.buffer_linger;
                }

                let batch_stmt = match &sharded_msg.client_command {
                    ClientCommand::InsertAccountUpdate(_) => insert_account_ps.clone(),
                    ClientCommand::InsertTransaction(_) => insert_tx_ps.clone(),
                };

                self.buffer.push(sharded_msg);
                self.scylla_batch.append_statement(batch_stmt);
                self.curr_batch_byte_size += msg_byte_size;

                // Handle the end of a period
                if is_end_of_period {

                    if let Some(task) = self.bg_period_commit_task.take() {
                        task.await??;
                    }

                    let session = Arc::clone(&self.session);
                    
                    let handle = tokio::spawn(async move {
                        let result = session
                            .query(
                                SCYLLADB_COMMIT_PRODUCER_PERIOD,
                                (producer_id, shard_id, curr_period),
                            )
                            .await
                            .map(|_qr| ())
                            .map_err(anyhow::Error::new);
                        info!("shard={},producer_id={:?} committed period: {}", shard_id, self.producer_id, curr_period);
                        result
                    });
                    // We put the period commit in background so we don't block the next period.
                    // However, we can not commit the next period until the last period was committed.
                    // By the time we finish the next period, the last period commit should have have happen.
                    self.bg_period_commit_task.replace(handle);
                    
                }
            }
        });
        sender
    }

}

pub struct ScyllaSink {
    router_handle: tokio::sync::mpsc::Sender<ClientCommand>,
}

#[derive(Debug)]
pub enum ScyllaSinkError {
    SinkClose,
}

async fn get_shard_offsets_for_producer(
    session: Arc<Session>,
    producer_id: ProducerId,
) -> anyhow::Result<Option<Vec<ShardOffset>>> {

    let shard_bind_markers = (0..SHARD_COUNT)
        .map(|x| format!("{}", x))
        .collect::<Vec<_>>()
        .join(", ");

    let query = format!(
        r###"
        SELECT
            shard_id,
            offset
        FROM shard_max_offset_mv
        WHERE
            producer_id = ?
            AND shard_id IN ({shard_bind_markers})
        ORDER BY offset DESC, period DESC
        PER PARTITION LIMIT 1
        "###,
        shard_bind_markers=shard_bind_markers
    );

    let query_result = session
        .query(
            query,
            (producer_id,),
        )
        .await?;

    let rows = query_result
        .rows_typed_or_empty::<(ShardId, ShardOffset,)>()
        .map(|result| result.map(|typed_row| typed_row.1))
        .collect::<Result<Vec<_>, _>>()
        .map_err(anyhow::Error::new)?;

    if rows.is_empty() {
        info!("producer {:?} offsets don't exists", producer_id);
        Ok(None)
    } else {
        info!("producer {:?} offsets already exists: {:?}", producer_id, rows);
        Ok(Some(rows))
    }
}


fn spawn_round_robin(shard_mailboxes: Vec<tokio::sync::mpsc::Sender<ClientCommand>>) -> tokio::sync::mpsc::Sender<ClientCommand> {

    let (sender, mut receiver) = tokio::sync::mpsc::channel(DEFAULT_SHARD_MAX_BUFFER_CAPACITY);
    let _h: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
        let mut i: usize = 0;
        let total_shards = shard_mailboxes.len();
        loop {
            let msg = receiver.recv().await.ok_or(anyhow::anyhow!("round robin received end is closed"))?;
            let begin = i;
            let maybe_permit = shard_mailboxes
                .iter()
                .enumerate()
                // Cycle forever until you find a destination
                .cycle()
                .skip(begin)
                .take(total_shards)
                .find_map(|(i, dest)| dest.try_reserve().ok().map(|slot| (i, slot)));

            let shard_idx = if let Some((j, permit)) = maybe_permit {
                permit.send(msg);
                j
            } else {
                warn!("failed to find a shard without waiting");
                shard_mailboxes[i].send(msg).await?;
                i
            };

            scylladb_batch_request_lag_inc();
            i = (shard_idx + 1) % total_shards;
        }
    });
    sender
}


impl ScyllaSink {
    pub async fn new(
        config: ScyllaSinkConfig,
        hostname: impl AsRef<str>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> anyhow::Result<Self> {
        let producer_id = [config.producer_id];

        let session: Session = SessionBuilder::new()
            .known_node(hostname)
            .user(username, password)
            .compression(Some(Compression::Lz4))
            .use_keyspace(config.keyspace.clone(), false)
            .build()
            .await?;

        let session = Arc::new(session);

        let shard_count = SHARD_COUNT;

        let mut sharders = vec![];

        info!("Will create {:?} shards", shard_count);
        let maybe_shard_offsets = get_shard_offsets_for_producer(Arc::clone(&session), producer_id).await?;
        let shard_offsets = maybe_shard_offsets.unwrap_or(vec![1; shard_count]);
        for shard_id in 0..shard_count {
            let session = Arc::clone(&session);
            let next_offset = shard_offsets[shard_id];
            let shard = Shard::new(
                session, 
                shard_id as i16, 
                producer_id, 
                next_offset, 
                DEFAULT_SHARD_MAX_BUFFER_CAPACITY, 
                config.batch_size_kb_limit * 1024,
                config.linger,
            );
            let shard_mailbox = shard.into_daemon();
            sharders.push(shard_mailbox);
        }

        let sender = spawn_round_robin(sharders);

        Ok(ScyllaSink {
            router_handle: sender,
        })
    }

    async fn inner_log(&mut self, cmd: ClientCommand) -> anyhow::Result<()> {
        self.router_handle
            .send(cmd)
            .await
            .map_err(|_e| anyhow::anyhow!("failed to route"))
    }

    pub async fn log_account_update(&mut self, update: AccountUpdate) -> anyhow::Result<()> {
        let cmd = ClientCommand::InsertAccountUpdate(update);
        self.inner_log(cmd).await
    }

    pub async fn log_transaction(&mut self, tx: Transaction) -> anyhow::Result<()> {
        let cmd = ClientCommand::InsertTransaction(tx);
        self.inner_log(cmd).await
    }
}
