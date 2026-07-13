use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::SystemTime;

use prost_types::Timestamp;
use yellowstone_shmem_client::{ClientError, ShmemClient};

use crate::plugin::message::Message;
use crate::plugin::shmem::{ProstShmemDecoder, ShmemHealthReporter};
use crate::stream::BatchStream;

pub struct ShmemBatchStream {
    inner: ShmemClient<ProstShmemDecoder>,
    health: ShmemHealthReporter,
    wait_in_flight: bool,
}

impl ShmemBatchStream {
    pub fn new(inner: ShmemClient<ProstShmemDecoder>, health_interval_secs: u64) -> Self {
        Self {
            inner,
            health: ShmemHealthReporter::new(health_interval_secs),
            wait_in_flight: false,
        }
    }
}

impl BatchStream for ShmemBatchStream {
    type Item = Message;

    fn poll_recv_batch(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        batch: &mut Vec<Self::Item>,
    ) -> Poll<Option<usize>> {
        let this = self.get_mut();
        this.wait_in_flight = false;
        let mut count = 0;

        while batch.len() < batch.capacity() {
            match this.inner.try_recv() {
                Some(Ok(gm)) => {
                    match ProstShmemDecoder::to_dm_message(gm, Timestamp::from(SystemTime::now())) {
                        Ok(msg) => {
                            this.health.observe(&msg);
                            batch.push(msg);
                            count += 1;
                        }
                        Err(e) => {
                            log::error!("conversion error: {e}");
                        }
                    }
                }
                Some(Err(ClientError::Lagged(n))) => {
                    this.health.observe_lagged(n);
                    log::warn!("shmem reader lagged, lost {n} entries");
                }
                Some(Err(ClientError::MidWrite)) => {
                    continue;
                }
                Some(Err(e)) => {
                    log::error!("shmem read error: {e}");
                }
                None => break,
            }
        }

        if count > 0 {
            return Poll::Ready(Some(count));
        }

        if !this.inner.check_region() {
            panic!("shmem: region was re-created. Consumer must rejoin.");
        }

        if !this.wait_in_flight {
            this.wait_in_flight = true;
            let waker = cx.waker().clone();
            let wait_handle = this.inner.wait_handle();
            tokio::task::spawn_blocking(move || {
                wait_handle.wait_for_data();
                waker.wake();
            });
        }

        Poll::Pending
    }
}
