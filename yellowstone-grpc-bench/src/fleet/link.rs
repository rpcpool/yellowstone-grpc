//! User-space link emulation between a subscriber and the plugin: delay, jitter, bandwidth, stalls.
//! Each direction is a delay line with a bounded number of bytes in flight, so a slow or far
//! subscriber pushes back on the plugin through TCP and HTTP/2 flow control, as a real link does.

use {
    serde::{Deserialize, Serialize},
    std::{sync::Arc, time::Duration},
    tokio::{
        io::{AsyncReadExt, AsyncWriteExt, DuplexStream},
        net::{
            tcp::{OwnedReadHalf, OwnedWriteHalf},
            TcpStream,
        },
        sync::{mpsc, Semaphore},
        time::Instant,
    },
};

const CHUNK: usize = 64 * 1024;

/// One direction of a link; both directions share the spec.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LinkSpec {
    /// One-way delay in milliseconds.
    #[serde(default)]
    pub delay_ms: f64,
    /// Uniform jitter added to each chunk's delay, in milliseconds. Order is kept, as on TCP.
    #[serde(default)]
    pub jitter_ms: f64,
    /// Link rate in megabits per second; unset means unlimited.
    #[serde(default)]
    pub bandwidth_mbps: Option<f64>,
    /// Queue allowed beyond the bandwidth-delay product, in KiB.
    #[serde(default = "default_buffer_kib")]
    pub buffer_kib: u64,
    /// Client socket receive buffer, in KiB; bounds what the kernel absorbs before backpressure.
    #[serde(default)]
    pub socket_rcvbuf_kib: Option<u32>,
    /// Every this many seconds the link stops delivering for `stall_ms`.
    #[serde(default)]
    pub stall_every_secs: Option<f64>,
    #[serde(default)]
    pub stall_ms: f64,
}

/// A moderate router queue; with TCP buffers on both ends it bounds what a slow link absorbs.
pub const fn default_buffer_kib() -> u64 {
    256
}

impl Default for LinkSpec {
    fn default() -> Self {
        Self {
            delay_ms: 0.0,
            jitter_ms: 0.0,
            bandwidth_mbps: None,
            buffer_kib: default_buffer_kib(),
            socket_rcvbuf_kib: None,
            stall_every_secs: None,
            stall_ms: 0.0,
        }
    }
}

impl LinkSpec {
    pub fn is_local(&self) -> bool {
        self.delay_ms <= 0.0
            && self.jitter_ms <= 0.0
            && self.bandwidth_mbps.is_none()
            && self.stall_every_secs.is_none()
    }

    /// Bytes the delay line may hold: the bandwidth-delay product plus the configured queue.
    fn in_flight_limit(&self) -> usize {
        let bdp = self
            .bandwidth_mbps
            .map_or(0.0, |mbps| mbps * 1e6 / 8.0 * self.delay_ms / 1e3);
        (bdp as usize + (self.buffer_kib as usize) * 1024).max(CHUNK)
    }
}

/// Connects to `target` through an emulated link and returns the subscriber-side stream.
pub async fn connect(target: &str, spec: &LinkSpec, seed: u64) -> anyhow::Result<LinkedStream> {
    let tcp = connect_tcp(target, spec.socket_rcvbuf_kib).await?;
    if spec.is_local() {
        return Ok(LinkedStream::Direct(tcp));
    }
    let (client_side, link_side) = tokio::io::duplex(CHUNK * 4);
    let (link_read, link_write) = tokio::io::split(link_side);
    let (tcp_read, tcp_write) = tcp.into_split();
    let spec = Arc::new(spec.clone());
    tokio::spawn(downlink(tcp_read, link_write, Arc::clone(&spec), seed));
    tokio::spawn(uplink(link_read, tcp_write, spec, seed ^ 0x9E37_79B9));
    Ok(LinkedStream::Emulated(client_side))
}

async fn connect_tcp(target: &str, rcvbuf_kib: Option<u32>) -> anyhow::Result<TcpStream> {
    let address = tokio::net::lookup_host(target)
        .await?
        .next()
        .ok_or_else(|| anyhow::anyhow!("cannot resolve {target}"))?;
    let socket = if address.is_ipv4() {
        tokio::net::TcpSocket::new_v4()?
    } else {
        tokio::net::TcpSocket::new_v6()?
    };
    if let Some(kib) = rcvbuf_kib {
        socket.set_recv_buffer_size(kib * 1024)?;
    }
    let stream = socket.connect(address).await?;
    stream.set_nodelay(true)?;
    Ok(stream)
}

/// The subscriber's end: the socket itself on a local link, a duplex pipe on an emulated one.
pub enum LinkedStream {
    Direct(TcpStream),
    Emulated(DuplexStream),
}

impl tokio::io::AsyncRead for LinkedStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Direct(stream) => std::pin::Pin::new(stream).poll_read(cx, buf),
            Self::Emulated(stream) => std::pin::Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl tokio::io::AsyncWrite for LinkedStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Direct(stream) => std::pin::Pin::new(stream).poll_write(cx, buf),
            Self::Emulated(stream) => std::pin::Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Direct(stream) => std::pin::Pin::new(stream).poll_flush(cx),
            Self::Emulated(stream) => std::pin::Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Direct(stream) => std::pin::Pin::new(stream).poll_shutdown(cx),
            Self::Emulated(stream) => std::pin::Pin::new(stream).poll_shutdown(cx),
        }
    }
}

/// Departure schedule of one direction: serialization at the link rate, then delay plus jitter.
struct Schedule {
    spec: Arc<LinkSpec>,
    link_free_at: Instant,
    last_release: Instant,
    rng: u64,
    next_stall: Option<Instant>,
}

impl Schedule {
    fn new(spec: Arc<LinkSpec>, seed: u64) -> Self {
        let now = Instant::now();
        let next_stall = spec
            .stall_every_secs
            .filter(|secs| *secs > 0.0)
            .map(|secs| now + Duration::from_secs_f64(secs));
        Self {
            spec,
            link_free_at: now,
            last_release: now,
            rng: seed | 1,
            next_stall,
        }
    }

    fn release_at(&mut self, len: usize) -> Instant {
        let now = Instant::now();
        let mut start = self.link_free_at.max(now);
        if let Some(stall) = self.next_stall {
            if start >= stall {
                start = start.max(stall + Duration::from_secs_f64(self.spec.stall_ms / 1e3));
                let every = self.spec.stall_every_secs.unwrap_or(1.0);
                self.next_stall = Some(stall + Duration::from_secs_f64(every));
            }
        }
        let serialization = self.spec.bandwidth_mbps.map_or(Duration::ZERO, |mbps| {
            Duration::from_secs_f64(len as f64 * 8.0 / (mbps * 1e6))
        });
        self.link_free_at = start + serialization;
        let jitter = if self.spec.jitter_ms > 0.0 {
            self.rng ^= self.rng << 13;
            self.rng ^= self.rng >> 7;
            self.rng ^= self.rng << 17;
            (self.rng >> 11) as f64 / (1u64 << 53) as f64 * self.spec.jitter_ms
        } else {
            0.0
        };
        let release =
            self.link_free_at + Duration::from_secs_f64((self.spec.delay_ms + jitter) / 1e3);
        // TCP delivers in order, so a chunk never overtakes the one before it.
        self.last_release = release.max(self.last_release);
        self.last_release
    }
}

/// Server to subscriber: read only while the delay line has room, so a full line stops TCP.
async fn downlink(
    mut tcp: OwnedReadHalf,
    mut out: tokio::io::WriteHalf<DuplexStream>,
    spec: Arc<LinkSpec>,
    seed: u64,
) {
    let limit = spec.in_flight_limit();
    let room = Arc::new(Semaphore::new(limit));
    let (tx, mut rx) =
        mpsc::unbounded_channel::<(Instant, Vec<u8>, tokio::sync::OwnedSemaphorePermit)>();
    let reader = {
        let room = Arc::clone(&room);
        tokio::spawn(async move {
            let mut schedule = Schedule::new(spec, seed);
            loop {
                let Ok(permit) = Arc::clone(&room).acquire_many_owned(CHUNK as u32).await else {
                    return;
                };
                let mut buf = vec![0u8; CHUNK];
                let n = match tcp.read(&mut buf).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => n,
                };
                buf.truncate(n);
                let mut permit = permit;
                // Keep only the permits for the bytes actually read.
                let _ = permit.split(CHUNK - n);
                if tx.send((schedule.release_at(n), buf, permit)).is_err() {
                    return;
                }
            }
        })
    };
    while let Some((release, buf, permit)) = rx.recv().await {
        tokio::time::sleep_until(release).await;
        if out.write_all(&buf).await.is_err() {
            break;
        }
        drop(permit);
    }
    reader.abort();
}

/// Subscriber to server: requests, window updates and pings arrive one delay later.
async fn uplink(
    mut input: tokio::io::ReadHalf<DuplexStream>,
    mut tcp: OwnedWriteHalf,
    spec: Arc<LinkSpec>,
    seed: u64,
) {
    let (tx, mut rx) = mpsc::unbounded_channel::<(Instant, Vec<u8>)>();
    let reader = tokio::spawn(async move {
        let mut schedule = Schedule::new(spec, seed);
        let mut buf = vec![0u8; CHUNK];
        loop {
            let n = match input.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => n,
            };
            if tx
                .send((schedule.release_at(n), buf[..n].to_vec()))
                .is_err()
            {
                return;
            }
        }
    });
    while let Some((release, buf)) = rx.recv().await {
        tokio::time::sleep_until(release).await;
        if tcp.write_all(&buf).await.is_err() {
            break;
        }
    }
    reader.abort();
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        tokio::io::{AsyncReadExt, AsyncWriteExt},
    };

    #[tokio::test]
    async fn delays_both_directions_and_keeps_order() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 4];
            socket.read_exact(&mut request).await.unwrap();
            for i in 0..50u8 {
                socket.write_all(&[i; 1000]).await.unwrap();
            }
            request
        });
        let spec = LinkSpec {
            delay_ms: 20.0,
            jitter_ms: 5.0,
            bandwidth_mbps: Some(100.0),
            ..Default::default()
        };
        let started = std::time::Instant::now();
        let mut stream = connect(&address, &spec, 7).await.unwrap();
        stream.write_all(b"ping").await.unwrap();
        let mut received = vec![0u8; 50_000];
        stream.read_exact(&mut received).await.unwrap();
        let elapsed = started.elapsed();
        assert_eq!(&server.await.unwrap(), b"ping");
        assert!(
            elapsed >= Duration::from_millis(40),
            "round trip took {elapsed:?}"
        );
        for (i, chunk) in received.chunks(1000).enumerate() {
            assert!(
                chunk.iter().all(|byte| usize::from(*byte) == i),
                "chunk {i} out of order"
            );
        }
    }

    #[test]
    fn bdp_sets_in_flight_limit() {
        let spec = LinkSpec {
            delay_ms: 100.0,
            bandwidth_mbps: Some(80.0),
            buffer_kib: 0,
            ..Default::default()
        };
        assert_eq!(spec.in_flight_limit(), 1_000_000);
    }
}
