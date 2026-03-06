use {
    crate::plugin::{
        filter::encoder::{AccountEncoder, TransactionEncoder},
        message::Message,
    },
    rayon::{ThreadPool, ThreadPoolBuilder},
    tokio::sync::{mpsc, oneshot},
};

pub struct ParallelEncoder {
    tx: mpsc::UnboundedSender<EncodeRequest>,
}

struct EncodeRequest {
    batch: Vec<(u64, Message)>,
    response: oneshot::Sender<Vec<(u64, Message)>>,
}

impl ParallelEncoder {
    pub fn new(num_threads: usize) -> (Self, std::thread::JoinHandle<()>) {
        let pool = ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .thread_name(|i| format!("geyser-encoder-{i}"))
            .build()
            .expect("failed to create rayon pool");

        let (tx, rx) = mpsc::unbounded_channel();

        let handle = std::thread::Builder::new()
            .name("geyser-encoder-bridge".into())
            .spawn(move || Self::bridge_loop(rx, pool))
            .expect("failed to spawn encoder bridge");

        (Self { tx }, handle)
    }

    fn bridge_loop(mut rx: mpsc::UnboundedReceiver<EncodeRequest>, pool: ThreadPool) {
        use rayon::prelude::*;

        while let Some(req) = rx.blocking_recv() {
            let EncodeRequest {
                mut batch,
                response,
            } = req;

            pool.install(|| {
                batch.par_iter_mut().for_each(|(_msgid, msg)| {
                    Self::encode_message(msg);
                });
            });

            let _ = response.send(batch);
        }

        log::info!("exiting encoder bridge loop");
    }

    fn encode_message(msg: &Message) {
        match msg {
            Message::Transaction(tx) => {
                if tx.transaction.pre_encoded.get().is_none() {
                    TransactionEncoder::pre_encode(&tx.transaction);
                }
            }
            Message::Account(acc) => {
                if acc.account.pre_encoded.get().is_none() {
                    AccountEncoder::pre_encode(&acc.account);
                }
            }
            _ => {}
        }
    }

    pub async fn encode(&self, batch: Vec<(u64, Message)>) -> Vec<(u64, Message)> {
        if batch.len() < 4 {
            return Self::encode_sync(batch);
        }

        let (tx, rx) = oneshot::channel();

        // move batch, don't clone
        if self
            .tx
            .send(EncodeRequest {
                batch,
                response: tx,
            })
            .is_err()
        {
            // channel closed - this shouldn't happen in normal operation
            panic!("encoder channel closed");
        }

        rx.await.expect("encoder response failed")
    }

    fn encode_sync(mut batch: Vec<(u64, Message)>) -> Vec<(u64, Message)> {
        for (_msgid, msg) in &mut batch {
            Self::encode_message(msg);
        }
        batch
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::plugin::message::{
            MessageAccount, MessageAccountInfo, MessageTransaction, MessageTransactionInfo,
        },
        bytes::Bytes,
        prost_types::Timestamp,
        solana_pubkey::Pubkey,
        solana_signature::Signature,
        std::{
            sync::{Arc, OnceLock},
            time::SystemTime,
        },
    };

    fn create_test_transaction() -> Message {
        let tx_info = MessageTransactionInfo {
            signature: Signature::from([1u8; 64]),
            is_vote: false,
            transaction: Default::default(),
            meta: Default::default(),
            index: 0,
            account_keys: Default::default(),
            pre_encoded: OnceLock::new(),
        };
        Message::Transaction(MessageTransaction {
            transaction: Arc::new(tx_info),
            slot: 100,
            created_at: Timestamp::from(SystemTime::now()),
        })
    }

    fn create_test_account() -> Message {
        let acc_info = MessageAccountInfo {
            pubkey: Pubkey::new_unique(),
            lamports: 1000,
            owner: Pubkey::new_unique(),
            executable: false,
            rent_epoch: 0,
            data: Bytes::from(vec![1, 2, 3]),
            write_version: 1,
            txn_signature: None,
            pre_encoded: OnceLock::new(),
        };
        Message::Account(MessageAccount {
            account: Arc::new(acc_info),
            slot: 100,
            is_startup: false,
            created_at: Timestamp::from(SystemTime::now()),
        })
    }

    #[tokio::test]
    async fn test_parallel_encoder_transactions() {
        let (encoder, _handle) = ParallelEncoder::new(2);

        let batch: Vec<(u64, Message)> = (0..10).map(|i| (i, create_test_transaction())).collect();

        let encoded = encoder.encode(batch).await;

        assert_eq!(encoded.len(), 10);
        for (_msgid, msg) in encoded {
            if let Message::Transaction(tx) = msg {
                assert!(
                    tx.transaction.pre_encoded.get().is_some(),
                    "transaction should be encoded"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_parallel_encoder_accounts() {
        let (encoder, _handle) = ParallelEncoder::new(2);

        let batch: Vec<(u64, Message)> = (0..10).map(|i| (i, create_test_account())).collect();

        let encoded = encoder.encode(batch).await;

        assert_eq!(encoded.len(), 10);
        for (_msgid, msg) in encoded {
            if let Message::Account(acc) = msg {
                assert!(
                    acc.account.pre_encoded.get().is_some(),
                    "account should be encoded"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_small_batch_uses_sync() {
        let (encoder, _handle) = ParallelEncoder::new(2);

        // Small batch < 4 should use sync path
        let batch: Vec<(u64, Message)> = (0..2).map(|i| (i, create_test_transaction())).collect();

        let encoded = encoder.encode(batch).await;

        assert_eq!(encoded.len(), 2);
    }

    #[tokio::test]
    async fn test_mixed_batch() {
        let (encoder, _handle) = ParallelEncoder::new(2);

        let mut batch: Vec<(u64, Message)> = Vec::new();
        for i in 0..5 {
            batch.push((i * 2, create_test_transaction()));
            batch.push((i * 2 + 1, create_test_account()));
        }

        let encoded = encoder.encode(batch).await;

        assert_eq!(encoded.len(), 10);
    }
}
