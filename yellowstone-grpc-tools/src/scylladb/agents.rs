use std::{sync::Arc, time::Duration};

use tokio::{sync::{self, mpsc::channel}, task::JoinHandle, time::{self, Instant}};
use tonic::async_trait;

pub type Nothing = ();


#[async_trait]
pub trait Ticker<I,O,E> 
where I: Send + 'static, O: Send + 'static, E: Send + 'static {
    async fn tick(&mut self, now: Instant, msg: I) -> Result<O, E>;
}


#[async_trait]
pub trait Timer<I, O, E>: Ticker<I, O, E>
where I: Send + 'static, O: Send + 'static, E: Send + 'static {

    async fn timeout(&mut self) -> Result<O, E>;

}


#[derive(Debug)]
pub enum AgentHandlerError {
    Closed,
    AgentError,
}

pub struct AgentHandler<I> {
    sender: sync::mpsc::Sender<I>,
    handle: Arc<JoinHandle<Result<Nothing, AgentHandlerError>>>,
}   


pub struct AgentSystem {
    pub agent_buffer_size: usize
}

impl AgentSystem {

    pub fn spawn<I,O,E,T>(&self, mut ticker: T) -> AgentHandler<I>
        where T: Ticker<I,O,E> + Send + 'static,
                I: Send + 'static,
                O: Send+ 'static,
                E: Send + 'static  {
        let (sender, mut receiver) = channel(self.agent_buffer_size);
        let h = tokio::spawn(async move {
            loop {
                let result = match receiver.recv().await {
                    Some(msg) => {
                        let now = Instant::now();
                        ticker.tick(now, msg).await
                    },
                    None => return Err(AgentHandlerError::Closed),
                };
                if result.is_err() {
                    return Err(AgentHandlerError::AgentError);
                }
            }
        });
        AgentHandler { sender, handle: Arc::new(h) }
    }

    pub fn spawn_timer<I,O,E,T>(&self, mut timer: T, linger: Duration) -> AgentHandler<I>
        where T: Timer<I,O,E> + Send + 'static,
                I: Send + 'static,
                O: Send + 'static,
                E: Send + 'static  {
        let (sender, mut receiver) = channel(self.agent_buffer_size);
        let h = tokio::spawn(async move {

            let mut deadline = Instant::now() + linger;
            loop {

                let result = tokio::select! {
                    _ = time::sleep_until(deadline) => { 
                        let res =  timer.timeout().await;
                        deadline = Instant::now() + linger;
                        res
                    }
                    opt_msg = receiver.recv() => {
                        match receiver.recv().await {
                            Some(msg) => {
                                let now = Instant::now();
                                timer.tick(now, msg).await
                            },
                            None => return Err(AgentHandlerError::Closed),
                        }
                    }
                };
                
                if result.is_err() {
                    return Err(AgentHandlerError::AgentError);
                }
            }
        });
        AgentHandler { sender, handle: Arc::new(h) }
    }
}