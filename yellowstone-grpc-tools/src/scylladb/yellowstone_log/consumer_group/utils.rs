use tokio::{sync::{oneshot, watch::{self, Ref}}, task::JoinHandle};


///
/// Wraps a tokio watch to make it a single-producer/single-consumer channel.
/// It also forces the underlying source thread to provide a termination signal,\
/// so all resources can be cleaned up properly.
/// 
pub struct SpscWatch<T> {
    watch: watch::Receiver<T>,

    #[allow(dead_code)]
    tx_terminate: oneshot::Sender<()>
}


impl<T> SpscWatch<T> {

    pub fn wrap(rx: watch::Receiver<T>, tx: oneshot::Sender<()>) -> Self {
        Self {
            watch: rx,
            tx_terminate: tx,
        }
    }

    pub fn borrow_and_update(&mut self) -> Ref<'_, T> {
        self.watch.borrow_and_update()
    }

    pub async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.watch.changed().await
    }
    
    pub async fn wait_for(&mut self, f: impl FnMut(&T) -> bool) -> Result<Ref<T>, watch::error::RecvError> {
        self.watch.wait_for(f).await
    }

    pub fn mark_changed(&mut self) {
        self.watch.mark_changed();
    }
}


// impl <T> SpscWatch<T> 
// where T: ToOwned + Send + Sync,
//       <T as ToOwned>::Owned: Send {

//     ///
//     /// Allow SpscWatch to be used in a detached context.
//     /// 
//     /// This is useful when you want to wait for a change in the watch, but you don't want to block the current thread.
//     /// 
//     pub fn detached_wait_for(&mut self, f: impl FnMut(&T) -> bool + Send) -> JoinHandle<Result<<T as ToOwned>::Owned, watch::error::RecvError>> {
//         let mut watch = self.watch.clone();
//         watch.mark_changed();
//         tokio::spawn(async move {
//             watch.wait_for(f).await.map(|x| x.to_owned())
//         })
//     }
// }
