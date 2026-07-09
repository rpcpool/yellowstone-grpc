use futures::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::plugin::message::Message;

pub enum TryRecv<T> {
    Item(T),
    Empty,
    Closed,
}

pub trait PollReceiver {
    type Item;

    fn poll_recv(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>>;
    fn try_recv(&mut self) -> TryRecv<Self::Item>;
}

pub trait BatchInto {
    fn batch_into(self, batch: &mut Vec<Message>);
}

pub struct GeyserStream<R> {
    receiver: R,
    batch_capacity: usize,
}

impl<R> GeyserStream<R> {
    pub fn new(receiver: R, batch_capacity: usize) -> Self {
        Self {
            receiver,
            batch_capacity,
        }
    }
}

impl<R> Stream for GeyserStream<R>
where
    R: PollReceiver + Unpin,
    R::Item: BatchInto,
{
    type Item = Vec<Message>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let message = match Pin::new(&mut self.receiver).poll_recv(cx) {
            Poll::Ready(Some(item)) => item,
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => return Poll::Pending,
        };

        let mut batch_messages = Vec::with_capacity(self.batch_capacity);
        message.batch_into(&mut batch_messages);

        while batch_messages.len() < self.batch_capacity {
            match self.receiver.try_recv() {
                TryRecv::Item(item) => item.batch_into(&mut batch_messages),
                TryRecv::Empty | TryRecv::Closed => break,
            }
        }

        Poll::Ready(Some(batch_messages))
    }
}

impl BatchInto for Message {
    fn batch_into(self, batch: &mut Vec<Message>) {
        batch.push(self);
    }
}

impl<T> PollReceiver for tokio::sync::mpsc::UnboundedReceiver<T> {
    type Item = T;

    fn poll_recv(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        tokio::sync::mpsc::UnboundedReceiver::poll_recv(&mut self, cx)
    }

    fn try_recv(&mut self) -> TryRecv<Self::Item> {
        use tokio::sync::mpsc::error::TryRecvError;
        match tokio::sync::mpsc::UnboundedReceiver::try_recv(self) {
            Ok(item) => TryRecv::Item(item),
            Err(TryRecvError::Empty) => TryRecv::Empty,
            Err(TryRecvError::Disconnected) => TryRecv::Closed,
        }
    }
}

impl<T> PollReceiver for tokio::sync::mpsc::Receiver<T> {
    type Item = T;

    fn poll_recv(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        tokio::sync::mpsc::Receiver::poll_recv(&mut self, cx)
    }

    fn try_recv(&mut self) -> TryRecv<Self::Item> {
        use tokio::sync::mpsc::error::TryRecvError;
        match tokio::sync::mpsc::Receiver::try_recv(self) {
            Ok(item) => TryRecv::Item(item),
            Err(TryRecvError::Empty) => TryRecv::Empty,
            Err(TryRecvError::Disconnected) => TryRecv::Closed,
        }
    }
}
