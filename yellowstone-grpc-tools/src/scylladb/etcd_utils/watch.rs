use std::sync::Arc;

use chrono::OutOfRange;
use etcd_client::{WatchOptions, WatchResponse, Watcher};
use futures::{Future, TryFuture};
use tokio::{sync::watch::{self, Ref}, task::JoinHandle};





///
/// Wrappers around a tokio watch + etcd watch that drops the etcd watch when no more references point to it.
/// 
pub struct EtcdWatcher<T> {
    watch_handle: Watcher,
    rx: watch::Receiver<T>,
}


impl<T> EtcdWatcher<T> {


    pub fn wrap(watcher: Watcher, rx: watch::Receiver<T>) -> Self {
        Self {
            watch_handle: watcher,
            rx,
        }
    }

    pub fn child_watch(&self) -> watch::Receiver<T> {
        self.rx.clone()
    }
}
