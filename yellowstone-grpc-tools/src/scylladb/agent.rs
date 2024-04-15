use std::{fmt, future::pending, pin::Pin, sync::Arc, time::Duration};

use futures::{Future, FutureExt};
use tokio::{sync::{self, mpsc::{channel, Receiver}}, task::JoinHandle, time::{self, Instant}};
use tonic::async_trait;
use tracing::error;

pub type Nothing = ();



#[async_trait]
pub trait Ticker {

    type Input: Send + 'static;
    type Error: Send + fmt::Debug + 'static;

    fn timeout(&self) -> Pin<Box<dyn Future<Output = Nothing> + Send + 'static>> {
        pending().boxed()
    }
    
    async fn on_timeout(&mut self, now: Instant) -> Result<Nothing, Self::Error> { 
        Ok(())
    }

    async fn tick(&mut self, now: Instant, msg: Self::Input) -> Result<Nothing, Self::Error>;

    async fn terminate(&mut self, now: Instant) -> Result<Nothing, Self::Error> {
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
    handle: Arc<JoinHandle<Result<Nothing, AgentHandlerError>>>,
}


impl<I> AgentHandler<I> {
    pub async fn send(&self, msg: I) -> Result<(), ()> {
        self.sender.send(msg).await.map_err(|_err| ())
    }
}


pub struct AgentSystem {
    pub agent_buffer_size: usize
}


impl AgentSystem {

    pub fn spawn<T>(&self, mut ticker: T) -> AgentHandler<T::Input>
        where T: Ticker + Send + 'static {
        let (sender, mut receiver) = channel(self.agent_buffer_size);
        let h = tokio::spawn(async move {

            loop {

                let result = tokio::select! {
                    _ = ticker.timeout() => {
                        let res = ticker.on_timeout(Instant::now()).await;
                        res
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
        AgentHandler { sender, handle: Arc::new(h) }
    }


    pub fn new(agent_buffer_size: usize) -> AgentSystem {
        AgentSystem { agent_buffer_size }
    }
}