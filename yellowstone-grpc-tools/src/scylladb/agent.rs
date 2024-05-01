use {
    anyhow::anyhow,
    futures::{Future, FutureExt, TryFutureExt},
    std::{future::pending, pin::Pin, sync::Arc, time::Duration},
    tokio::{
        sync::{
            self,
            mpsc::{channel, error::TrySendError, Permit},
            oneshot,
        },
        task::{AbortHandle, JoinHandle, JoinSet},
        time::Instant,
    },
    tonic::async_trait,
    tracing::{error, warn},
};

pub type Nothing = ();

pub type Callback = oneshot::Receiver<Nothing>;

pub type CallbackSender = oneshot::Sender<Nothing>;

const LOOP_DELAY_WARN_THRESHOLD: std::time::Duration = Duration::from_millis(500);

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

    fn timeout2(&self, _now: Instant) -> bool {
        false
    }

    async fn init(&mut self) -> anyhow::Result<Nothing> {
        Ok(())
    }

    ///
    /// Called if [`Ticker::timeout`] promise returned before the next message pull.
    ///
    async fn on_timeout(&mut self, _now: Instant) -> anyhow::Result<Nothing> {
        Ok(())
    }

    fn is_pull_ready(&self) -> bool {
        true
    }

    /// Called on each new message received by the message loop
    async fn tick(&mut self, now: Instant, msg: Self::Input) -> anyhow::Result<Nothing> {
        let (sender, _receiver) = oneshot::channel();
        self.tick_with_callback_sender(now, msg, vec![sender]).await
    }

    async fn tick_with_callback_sender(
        &mut self,
        now: Instant,
        msg: Self::Input,
        callback_senders: Vec<CallbackSender>,
    ) -> anyhow::Result<Nothing> {
        let result = self.tick(now, msg).await;

        for cb_sender in callback_senders {
            if cb_sender.send(()).is_err() {
                warn!("Failed to notified because endpoint already closed");
            }
        }

        result
    }

    /// This is called if the agent handler must gracefully kill you.
    async fn terminate(&mut self, _now: Instant) -> Result<Nothing, anyhow::Error> {
        warn!("Agent terminated");
        Ok(())
    }
}

#[derive(Debug)]
pub enum AgentHandlerError {
    Closed,
    AgentError,
}

struct Message<T> {
    data: T,
    callbacks: Vec<CallbackSender>,
    //FireAndForget(T),
    //WithCallbacks(T, Vec<oneshot::Sender<Nothing>>),
}

#[derive(Clone)]
pub struct AgentHandler<T> {
    name: String,
    sender: sync::mpsc::Sender<Message<T>>,
    #[allow(dead_code)]
    abort_handle: Arc<AbortHandle>,
    deadletter_queue: Option<Arc<DeadLetterQueueHandler<T>>>,
}

struct DeadLetterQueueHandler<T> {
    sender: sync::mpsc::Sender<T>,
    #[allow(dead_code)]
    handle: Arc<JoinHandle<()>>,
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
        Slot { inner: permit }
    }

    pub fn send(self, msg: T) {
        self.send_with_callback_senders(msg, Vec::new());
    }

    pub fn send_with_callback_senders<IT>(self, msg: T, callback_senders: IT)
    where
        IT: IntoIterator<Item = CallbackSender>,
    {
        self.inner.send(Message {
            data: msg,
            callbacks: callback_senders.into_iter().collect(),
        })
    }

    pub fn send_and_subscribe(self, msg: T) -> oneshot::Receiver<()> {
        let (sender, receiver) = oneshot::channel();
        self.send_with_callback_senders(msg, [sender]);
        receiver
    }
}

impl<T: Send + 'static> AgentHandler<T> {
    pub async fn send(&self, msg: T) -> anyhow::Result<Nothing> {
        self.send_with_callback_senders(msg, Vec::new()).await
    }

    async fn handle_failed_transmission(&self, msg: T) -> anyhow::Error {
        if let Some(dlq) = self.deadletter_queue.clone() {
            let emsg = format!(
                "({:?}) Failed to send message, will reroute to deadletter queue",
                self.name
            );
            error!(emsg);
            dlq.send(msg).await;
            anyhow::anyhow!(emsg)
        } else {
            let emsg = format!("({:?}) Failed to send message, message will be dropped (no deadletter queue detected)", self.name);
            error!(emsg);
            anyhow::anyhow!(emsg)
        }
    }

    pub async fn reserve(&self) -> anyhow::Result<Slot<'_, T>> {
        self.sender
            .reserve()
            .map_err(anyhow::Error::new)
            .await
            .map(Slot::new)
    }

    pub fn try_reserve(&self) -> Result<Slot<'_, T>, TrySendError<()>> {
        self.sender.try_reserve().map(Slot::new)
    }

    pub async fn send_with_callback_senders<IT>(
        &self,
        msg: T,
        callback_senders: IT,
    ) -> anyhow::Result<Nothing>
    where
        IT: IntoIterator<Item = CallbackSender>,
    {
        let now = Instant::now();
        let envelope = Message {
            data: msg,
            callbacks: callback_senders.into_iter().collect(),
        };
        let result = self.sender.send(envelope).await;

        if now.elapsed() > Duration::from_millis(500) {
            warn!(
                "AgentHandler::send slow function detected: {:?}",
                now.elapsed()
            );
        }

        if let Err(e) = result {
            error!("error in send_with_watch");
            Err(self.handle_failed_transmission(e.0.data).await)
        } else {
            Ok(())
        }
    }

    pub async fn send_and_subscribe(&self, msg: T) -> anyhow::Result<oneshot::Receiver<Nothing>> {
        let (sender, receiver) = oneshot::channel();
        self.send_with_callback_senders(msg, vec![sender]).await?;
        Ok(receiver)
    }

    pub fn kill(self) {
        self.abort_handle.abort();
    }
}

pub struct AgentSystem {
    pub default_agent_buffer_capacity: usize,
    handlers: JoinSet<anyhow::Result<Nothing>>,
}

impl AgentSystem {
    pub fn new(default_agent_buffer_capacity: usize) -> Self {
        AgentSystem {
            default_agent_buffer_capacity,
            handlers: JoinSet::new(),
        }
    }

    pub fn spawn<T, N: Into<String>>(&mut self, name: N, ticker: T) -> AgentHandler<T::Input>
    where
        T: Ticker + Send + 'static,
    {
        self.spawn_with_capacity(name, ticker, self.default_agent_buffer_capacity)
    }

    pub fn spawn_with_capacity<T, N: Into<String>>(
        &mut self,
        name: N,
        mut ticker: T,
        buffer: usize,
    ) -> AgentHandler<T::Input>
    where
        T: Ticker + Send + 'static,
    {
        let (sender, mut receiver) = channel::<Message<T::Input>>(buffer);
        let agent_name: String = name.into();
        let inner_agent_name = agent_name.clone();
        let abort_handle = self.handlers.spawn(async move {
            let name = inner_agent_name;

            let init_result = ticker.init().await;
            if init_result.is_err() {
                error!("{:?} error during init: {:?}", name, init_result);
                return init_result;
            }

            loop {
                let before = Instant::now();
                let result = tokio::select! {
                    _ = ticker.timeout() => {
                        ticker.on_timeout(Instant::now()).await
                    }
                    opt_msg = receiver.recv(), if ticker.is_pull_ready() => {
                        match opt_msg {
                            Some(msg) => {
                                let now = Instant::now();
                                if msg.callbacks.is_empty() {
                                    ticker.tick(now, msg.data).await
                                } else {
                                    ticker.tick_with_callback_sender(now, msg.data, msg.callbacks).await
                                }
                            },
                            None => {
                                let now = Instant::now();
                                return Err(
                                    ticker.terminate(now)
                                        .await
                                        .err()
                                        .unwrap_or(anyhow!("Agent is closed"))
                                )
                            },
                        }
                    }
                };

                if let Err(e) = result {
                    let emsg = format!("message loop: {:?}, {:?}", name, e);
                    anyhow::bail!(emsg)
                }

                let iteration_duration = before.elapsed();
                if iteration_duration >= LOOP_DELAY_WARN_THRESHOLD {
                    warn!("{:?} loop iteration took: {:?}", name, iteration_duration);
                }
            }
        });

        AgentHandler {
            name: agent_name,
            sender,
            abort_handle: Arc::new(abort_handle),
            deadletter_queue: None,
        }
    }

    pub async fn until_one_agent_dies(&mut self) -> anyhow::Result<Nothing> {
        self.handlers
            .join_next()
            .map(|inner| inner.unwrap_or(Ok(Ok(()))))
            .map(|join_result| join_result?)
            .await
    }
}
