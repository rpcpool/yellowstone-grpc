//! Merges the HTTP/2 frames of a large update into fewer socket writes.

use {
    std::{
        io::{self, IoSlice},
        num::NonZeroUsize,
        pin::Pin,
        task::{ready, Context, Poll},
    },
    tokio::io::{AsyncRead, AsyncWrite, ReadBuf},
    tonic::transport::server::Connected,
};

/// The bytes since the last flush that mark a full HTTP/2 frame with more frames likely behind it.
const FULL_FRAME_BYTES: usize = 16 * 1024;

/// Wraps a connection and sends runs of full HTTP/2 frames as one larger write.
///
/// h2 flushes after every data frame, and ends each send pass with a flush that follows no write.
/// A flush right after a full frame is held back, and any other flush sends everything held, so an
/// update never waits for data that is not already queued.
///
/// # Examples
///
/// ```ignore
/// let write_coalesce_size = NonZeroUsize::new(64 * 1024);
/// let incoming = incoming.map(move |io| io.map(|io| WriteCoalesceIo::new(io, write_coalesce_size)));
/// server.serve_with_incoming(service, incoming).await?;
/// ```
#[derive(Debug)]
pub struct WriteCoalesceIo<IO> {
    /// The wrapped connection.
    inner: IO,
    /// The largest write to the connection. [`None`] passes every call straight through.
    write_coalesce_size: Option<NonZeroUsize>,
    /// The bytes held for the next write.
    held: Vec<u8>,
    /// How many bytes of `held` the connection has already taken.
    sent: usize,
    /// The bytes accepted since the last flush.
    accepted_since_flush: usize,
}

impl<IO> WriteCoalesceIo<IO> {
    /// Wraps a connection.
    ///
    /// # Arguments
    ///
    /// * `inner` - The connection to wrap.
    /// * `write_coalesce_size` - The largest write to the connection. [`None`] turns merging off.
    ///
    /// # Returns
    ///
    /// The wrapped connection, holding no bytes.
    pub const fn new(inner: IO, write_coalesce_size: Option<NonZeroUsize>) -> Self {
        Self {
            inner,
            write_coalesce_size,
            held: Vec::new(),
            sent: 0,
            accepted_since_flush: 0,
        }
    }

    /// Holds as much of a buffer as fits in the next write.
    ///
    /// # Arguments
    ///
    /// * `buf` - The bytes to hold.
    /// * `write_coalesce_size` - The largest write to the connection.
    ///
    /// # Returns
    ///
    /// The number of bytes held from `buf`.
    fn hold(&mut self, buf: &[u8], write_coalesce_size: usize) -> usize {
        let held_bytes = buf
            .len()
            .min(write_coalesce_size.saturating_sub(self.held.len()));
        self.held.extend_from_slice(&buf[..held_bytes]);
        self.accepted_since_flush = self.accepted_since_flush.saturating_add(held_bytes);
        held_bytes
    }
}

impl<IO: AsyncWrite + Unpin> WriteCoalesceIo<IO> {
    /// Writes every held byte to the connection.
    ///
    /// # Arguments
    ///
    /// * `cx` - The context of the writing task.
    ///
    /// # Returns
    ///
    /// [`Poll::Ready`] once nothing is held, or [`Poll::Pending`] if the connection is full.
    ///
    /// # Errors
    ///
    /// Returns the connection's write error, or [`io::ErrorKind::WriteZero`] if it takes no bytes.
    fn poll_send_held(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while self.sent < self.held.len() {
            let written =
                ready!(Pin::new(&mut self.inner).poll_write(cx, &self.held[self.sent..]))?;
            if written == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }
            self.sent += written;
        }
        self.held.clear();
        self.sent = 0;
        Poll::Ready(Ok(()))
    }
}

impl<IO: AsyncRead + Unpin> AsyncRead for WriteCoalesceIo<IO> {
    /// Reads from the wrapped connection.
    ///
    /// # Arguments
    ///
    /// * `cx` - The context of the reading task.
    /// * `buf` - The buffer to read into.
    ///
    /// # Returns
    ///
    /// The result of the wrapped connection's read.
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl<IO: AsyncWrite + Unpin> AsyncWrite for WriteCoalesceIo<IO> {
    /// Holds bytes for the next write, and sends what is held once the write is full.
    ///
    /// # Arguments
    ///
    /// * `cx` - The context of the writing task.
    /// * `buf` - The bytes to write.
    ///
    /// # Returns
    ///
    /// The number of bytes accepted, or [`Poll::Pending`] if the connection is full.
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let Some(write_coalesce_size) = this.write_coalesce_size else {
            return Pin::new(&mut this.inner).poll_write(cx, buf);
        };
        if this.held.len() >= write_coalesce_size.get() {
            ready!(this.poll_send_held(cx))?;
        }
        Poll::Ready(Ok(this.hold(buf, write_coalesce_size.get())))
    }

    /// Holds several buffers for the next write, and sends what is held once the write is full.
    ///
    /// # Arguments
    ///
    /// * `cx` - The context of the writing task.
    /// * `bufs` - The buffers to write.
    ///
    /// # Returns
    ///
    /// The number of bytes accepted, or [`Poll::Pending`] if the connection is full.
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let Some(write_coalesce_size) = this.write_coalesce_size else {
            return Pin::new(&mut this.inner).poll_write_vectored(cx, bufs);
        };
        if this.held.len() >= write_coalesce_size.get() {
            ready!(this.poll_send_held(cx))?;
        }
        let mut accepted = 0;
        for buf in bufs {
            let held_bytes = this.hold(buf, write_coalesce_size.get());
            accepted += held_bytes;
            if held_bytes < buf.len() {
                break;
            }
        }
        Poll::Ready(Ok(accepted))
    }

    /// Tells if the connection accepts several buffers at once.
    ///
    /// # Returns
    ///
    /// `true` while merging, because holding several buffers costs no extra write.
    fn is_write_vectored(&self) -> bool {
        self.write_coalesce_size.is_some() || self.inner.is_write_vectored()
    }

    /// Sends what is held, unless a full frame just arrived and more frames are likely behind it.
    ///
    /// # Arguments
    ///
    /// * `cx` - The context of the flushing task.
    ///
    /// # Returns
    ///
    /// The result of the flush, or [`Poll::Pending`] if the connection is full.
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let accepted_since_flush = std::mem::take(&mut this.accepted_since_flush);
        if this.write_coalesce_size.is_some() && accepted_since_flush >= FULL_FRAME_BYTES {
            // Safe only because h2 flushes once more after every full data frame before it
            // goes idle. Checked against h2 0.4.20: check again when h2 is upgraded.
            return Poll::Ready(Ok(()));
        }
        ready!(this.poll_send_held(cx))?;
        Pin::new(&mut this.inner).poll_flush(cx)
    }

    /// Sends what is held and shuts down the wrapped connection.
    ///
    /// # Arguments
    ///
    /// * `cx` - The context of the task that shuts down.
    ///
    /// # Returns
    ///
    /// The result of the wrapped connection's shutdown.
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(this.poll_send_held(cx))?;
        Pin::new(&mut this.inner).poll_shutdown(cx)
    }
}

impl<IO: Connected> Connected for WriteCoalesceIo<IO> {
    type ConnectInfo = IO::ConnectInfo;

    /// Reads the connect info of the wrapped connection.
    ///
    /// # Returns
    ///
    /// The connect info of the wrapped connection.
    fn connect_info(&self) -> Self::ConnectInfo {
        self.inner.connect_info()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        bytes::Bytes,
        std::{
            sync::{
                atomic::{AtomicUsize, Ordering},
                Arc,
            },
            task::Waker,
            time::Duration,
        },
        tokio::io::{duplex, AsyncWriteExt, DuplexStream},
    };

    const WRITE_COALESCE_SIZE: NonZeroUsize = NonZeroUsize::new(64 * 1024).unwrap();

    /// Records each write, and takes at most `max_write` bytes of it.
    struct RecordingIo {
        writes: Vec<Vec<u8>>,
        max_write: usize,
    }

    impl AsyncWrite for RecordingIo {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let this = self.get_mut();
            let written = buf.len().min(this.max_write);
            this.writes.push(buf[..written].to_vec());
            Poll::Ready(Ok(written))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// Counts the writes that reach a duplex stream.
    struct CountingIo {
        inner: DuplexStream,
        writes: Arc<AtomicUsize>,
    }

    impl AsyncRead for CountingIo {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for CountingIo {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let this = self.get_mut();
            let written = ready!(Pin::new(&mut this.inner).poll_write(cx, buf))?;
            this.writes.fetch_add(1, Ordering::Relaxed);
            Poll::Ready(Ok(written))
        }

        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.get_mut().inner).poll_flush(cx)
        }

        fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
        }
    }

    fn recording_io(max_write: usize) -> WriteCoalesceIo<RecordingIo> {
        WriteCoalesceIo::new(
            RecordingIo {
                writes: Vec::new(),
                max_write,
            },
            Some(WRITE_COALESCE_SIZE),
        )
    }

    fn flush(io: &mut WriteCoalesceIo<RecordingIo>) {
        let mut cx = Context::from_waker(Waker::noop());
        assert!(matches!(
            Pin::new(io).poll_flush(&mut cx),
            Poll::Ready(Ok(()))
        ));
    }

    #[tokio::test]
    async fn a_small_update_is_sent_at_its_first_flush() {
        let mut io = recording_io(usize::MAX);

        io.write_all(b"small update").await.expect("write");
        flush(&mut io);

        assert_eq!(io.inner.writes, [b"small update".to_vec()]);
    }

    #[tokio::test]
    async fn full_frames_leave_as_one_write_at_the_flush_that_follows_no_write() {
        let frame = vec![7; FULL_FRAME_BYTES];
        let mut io = recording_io(usize::MAX);

        for _ in 0..3 {
            io.write_all(&frame).await.expect("write");
            flush(&mut io);
        }
        assert!(io.inner.writes.is_empty());
        flush(&mut io);

        assert_eq!(io.inner.writes, [vec![7; 3 * FULL_FRAME_BYTES]]);
    }

    #[tokio::test]
    async fn a_short_last_frame_sends_everything_held() {
        let mut io = recording_io(usize::MAX);

        io.write_all(&vec![1; FULL_FRAME_BYTES])
            .await
            .expect("write");
        flush(&mut io);
        io.write_all(&[2; 10]).await.expect("write");
        flush(&mut io);

        assert_eq!(io.inner.writes.len(), 1);
        assert_eq!(io.inner.writes[0].len(), FULL_FRAME_BYTES + 10);
    }

    #[tokio::test]
    async fn a_connection_that_takes_part_of_a_write_gets_every_byte_in_order() {
        let payload: Vec<u8> = (0..300_000u32).map(|byte| byte as u8).collect();
        let mut io = recording_io(1000);

        io.write_all(&payload).await.expect("write");
        io.flush().await.expect("flush");
        io.flush().await.expect("flush");

        assert_eq!(io.inner.writes.concat(), payload);
    }

    #[tokio::test]
    async fn h2_delivers_a_large_update_in_fewer_writes_while_its_stream_stays_open() {
        const PAYLOAD_BYTES: usize = 1024 * 1024;
        let payload: Bytes = (0..PAYLOAD_BYTES).map(|byte| byte as u8).collect();
        let (client_io, server_io) = duplex(4 * PAYLOAD_BYTES);
        let writes = Arc::new(AtomicUsize::new(0));
        let server_io = WriteCoalesceIo::new(
            CountingIo {
                inner: server_io,
                writes: Arc::clone(&writes),
            },
            Some(WRITE_COALESCE_SIZE),
        );

        let server_payload = payload.clone();
        tokio::spawn(async move {
            let mut connection = h2::server::handshake(server_io).await.expect("handshake");
            let (_request, mut respond) = connection
                .accept()
                .await
                .expect("request")
                .expect("request");
            let mut stream = respond
                .send_response(http::Response::new(()), false)
                .expect("response");
            stream.send_data(server_payload, false).expect("data");
            while connection.accept().await.is_some() {}
            drop(stream);
        });

        let (mut sender, connection) = h2::client::Builder::new()
            .initial_window_size(2 * PAYLOAD_BYTES as u32)
            .initial_connection_window_size(2 * PAYLOAD_BYTES as u32)
            .handshake::<_, Bytes>(client_io)
            .await
            .expect("handshake");
        tokio::spawn(connection);
        let request = http::Request::builder()
            .uri("http://localhost/")
            .body(())
            .expect("request");
        let (response, _send_stream) = sender.send_request(request, false).expect("send");
        let mut body = response.await.expect("response").into_body();

        let mut received = Vec::with_capacity(PAYLOAD_BYTES);
        tokio::time::timeout(Duration::from_secs(5), async {
            while received.len() < PAYLOAD_BYTES {
                let chunk = body.data().await.expect("open").expect("data");
                body.flow_control()
                    .release_capacity(chunk.len())
                    .expect("release");
                received.extend_from_slice(&chunk);
            }
        })
        .await
        .expect("every byte should arrive while the stream stays open");

        assert_eq!(received, payload);
        // 64 frames of 16 KB take 16 writes of 64 KB, plus the HTTP/2 handshake.
        assert!(writes.load(Ordering::Relaxed) < 32);
    }
}
