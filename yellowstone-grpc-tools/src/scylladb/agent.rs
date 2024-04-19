use {
    futures::{Future, FutureExt, TryFutureExt},
    std::{any, borrow::BorrowMut, fmt, future::pending, pin::Pin, sync::Arc},
    tokio::{
        sync::{self, mpsc::{channel, error::TrySendError, Permit}, oneshot},
        task::JoinHandle,
        time::Instant,
    },
    tonic::async_trait,
    tracing::{error, warn}, tracing_subscriber::fmt::format::Full,
};

pub type Nothing = ();


pub type Watch = oneshot::Receiver<Nothing>;

pub type WatchSignal = oneshot::Sender<Nothing>;


/// Watch to see when a message, previously sent, has been consumed and processed by an agent.

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
    async fn on_timeout(&mut self, _now: Instant) -> anyhow::Result<Nothing> {
        Ok(())
    }

    /// Called on each new message received by the message loop
    async fn tick(&mut self, now: Instant, msg: Self::Input) -> anyhow::Result<Nothing> {
        let (sender, receiver) = oneshot::channel();
        self.tick_with_watch(now, msg, sender).await
    }
    
    async fn tick_with_watch(&mut self, now: Instant, msg: Self::Input, ws: WatchSignal) -> anyhow::Result<Nothing> {
        let result = self.tick(now, msg).await;
        if let Err(_) = ws.send(()) {
            warn!("Failed to notified because endpoint already closed");
        }
        result
    }

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

enum Message<T> {
    FireAndForget(T),
    WithWatch(T, oneshot::Sender<Nothing>),
}


impl<T> Message<T> {

    fn unwrap(self) -> (T, Option<oneshot::Sender<Nothing>>) {
        match self {
            Self::FireAndForget(x) => (x, None),
            Self::WithWatch(x, sender) => (x, Some(sender)),
        }
    }
}


#[derive(Clone)]
pub struct AgentHandler<T> {
    sender: sync::mpsc::Sender<Message<T>>,
    #[allow(dead_code)]
    handle: Arc<JoinHandle<Result<Nothing, AgentHandlerError>>>,
    deadletter_queue: Option<Arc<DeadLetterQueueHandler<T>>>,
}

struct DeadLetterQueueHandler<T> {
    sender: sync::mpsc::Sender<T>,
    handle: Arc<JoinHandle<()>>
}

impl<T> DeadLetterQueueHandler<T> {
    async fn send(&self, msg: T) {
        self.sender.send(msg).await.unwrap();
    }
}

pub struct Slot<'a, T> {
    inner: Permit<'a, Message<T>>,
}

impl<'a, T> Slot<'a, T> {
    fn new(permit: Permit<'a, Message<T>>) -> Self {
        Slot {
            inner: permit
        }
    }

    pub fn send(self, msg: T) {
        self.inner.send(Message::FireAndForget(msg))
    }

    pub fn send_with_watch(self, msg: T) -> oneshot::Receiver<()> {
        let (sender, receiver) = oneshot::channel();
        self.inner.send(Message::WithWatch(msg, sender));
        receiver
    } 
}


impl<T: Send + 'static> AgentHandler<T> {
    pub async fn send(&self, msg: T) -> anyhow::Result<()> {
        let result = self.sender
            .send(Message::FireAndForget(msg))
            .await;

        if let Err(e) = result {
            Err(self.handle_failed_transmission(e.0.unwrap().0).await)
        } else {
            Ok(())
        }
    }

    async fn handle_failed_transmission(&self, msg: T) -> anyhow::Error {
        if let Some(dlq) = self.deadletter_queue.clone() {
            let emsg = "Failed to send message with watch, will reroute to deadletter queue";
            error!(emsg);
            dlq.send(msg).await;
            anyhow::anyhow!(emsg)
        } else {
            error!("Failed to send message with watch, message will be dropped, not deadletter queue is setup");
            anyhow::anyhow!("failed to send message with watch, message is dropped.")
        }
    }

    pub async fn reserve(&self) -> anyhow::Result<Slot<'_, T>> {
        self.sender.reserve()
            .map_err(anyhow::Error::new)
            .await
            .map(Slot::new)
    }

    pub fn try_reserve(&self) -> Result<Slot<'_, T>, TrySendError<()>> {
        self.sender.try_reserve().map(Slot::new)
    }

    pub async fn send_with_watch(&self, msg: T) -> anyhow::Result<oneshot::Receiver<Nothing>> {
        let (sender, receiver) = oneshot::channel();
        let result = self.sender 
            .send(Message::WithWatch(msg, sender)).await;

        if let Err(e) = result {
            Err(self.handle_failed_transmission(e.0.unwrap().0).await)
        } else {
            Ok(receiver)
        }
    }

    pub fn kill(self) {
        self.handle.abort();
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
        let (sender, mut receiver) = channel::<Message<T::Input>>(self.agent_buffer_size);

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
                                let (content, maybe) = msg.unwrap();
                                match maybe {
                                    Some(sender) => ticker.tick_with_watch(now, content, sender).await,
                                    None => ticker.tick(now, content).await,
                                }
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
            deadletter_queue: None,
        }
    }

    pub const fn new(agent_buffer_size: usize) -> AgentSystem {
        AgentSystem { agent_buffer_size }
    }
}
