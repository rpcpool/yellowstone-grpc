use {
    futures::{Future, FutureExt},
    std::{future::pending, pin::Pin, sync::Arc},
    tokio::{
        sync::{self, mpsc::channel},
        task::JoinHandle,
        time::Instant,
    },
    tonic::async_trait,
    tracing::error,
};

pub type Nothing = ();

#[async_trait]
pub trait Ticker {
    type Input: Send + 'static;

    ///
    /// Optional timeout future that is pulled at the same time as the next message in the message loop.
    ///
    /// Implement this function if you need to flush your Ticker every N unit of time, such as batching.
    ///
    /// If the timeout finish before the next message is pull, then [`Ticker::on_timeout`] is invoked.
    ///
    fn timeout(&self) -> Pin<Box<dyn Future<Output = Nothing> + Send + 'static>> {
        pending().boxed()
    }

    ///
    /// Called if [`Ticker::timeout`] promise returned before the next message pull.
    ///
    async fn on_timeout(&mut self, _now: Instant) -> Result<Nothing, anyhow::Error> {
        Ok(())
    }

    /// Called on each new message received by the message loop
    async fn tick(&mut self, now: Instant, msg: Self::Input) -> Result<Nothing, anyhow::Error>;

    /// This is called if the agent handler must gracefully kill you.
    async fn terminate(&mut self, _now: Instant) -> Result<Nothing, anyhow::Error> {
        Ok(())
    }
}

#[derive(Debug)]
pub enum AgentHandlerError {
    Closed,
    AgentError,
}

#[derive(Clone)]
pub struct AgentHandler<I> {
    sender: sync::mpsc::Sender<I>,
    #[allow(dead_code)]
    handle: Arc<JoinHandle<Result<Nothing, AgentHandlerError>>>,
}

impl<I: Send + 'static> AgentHandler<I> {
    pub async fn send(&self, msg: I) -> Result<(), I> {
        self.sender.send(msg).await.map_err(|err| err.0)
    }
}

pub struct AgentSystem {
    pub agent_buffer_size: usize,
}

impl AgentSystem {
    pub fn spawn<T>(&self, mut ticker: T) -> AgentHandler<T::Input>
    where
        T: Ticker + Send + 'static,
    {
        let (sender, mut receiver) = channel(self.agent_buffer_size);
        let h = tokio::spawn(async move {
            loop {
                let result = tokio::select! {
                    _ = ticker.timeout() => {

                        ticker.on_timeout(Instant::now()).await
                    }
                    opt_msg = receiver.recv() => {
                        match opt_msg {
                            Some(msg) => {
                                let now = Instant::now();
                                ticker.tick(now, msg).await
                            },
                            None => {
                                let now = Instant::now();
                                return Err(
                                    ticker.terminate(now)
                                        .await
                                        .map_err(|_err| AgentHandlerError::AgentError)
                                        .err()
                                        .unwrap_or(AgentHandlerError::Closed)
                                )
                            },
                        }
                    }
                };

                if result.is_err() {
                    error!("{:?}", result.err().unwrap());
                    return Err(AgentHandlerError::AgentError);
                }
            }
        });
        AgentHandler {
            sender,
            handle: Arc::new(h),
        }
    }

    pub const fn new(agent_buffer_size: usize) -> AgentSystem {
        AgentSystem { agent_buffer_size }
    }
}
