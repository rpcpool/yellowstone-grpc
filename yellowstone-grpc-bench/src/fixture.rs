//! Fixture format: `manifest.json` plus `chunks/NNNNN.ygf.zst`.
//! Each chunk is a zstd stream that starts with `YGF1`, followed by frames:
//! stream id (u8: 0 main, 1 deshred, 2 main accounts), receive time (u64 unix ns), payload length (u32),
//! payload. Integers are little-endian.

use {
    anyhow::Context,
    bytes::Bytes,
    serde::{Deserialize, Serialize},
    std::{
        collections::BTreeMap,
        fs::{self, File},
        io::{self, BufReader, BufWriter, Read, Write},
        path::{Path, PathBuf},
    },
};

pub const FORMAT_VERSION: u32 = 1;
const CHUNK_MAGIC: &[u8; 4] = b"YGF1";
const FRAME_HEADER_LEN: usize = 13;
const MANIFEST_FILE: &str = "manifest.json";
const CHUNKS_DIR: &str = "chunks";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum StreamId {
    Main,
    Deshred,
    /// Account updates on their own connection; decoded like `Main`.
    MainAccounts,
}

impl StreamId {
    pub const fn as_u8(self) -> u8 {
        match self {
            Self::Main => 0,
            Self::Deshred => 1,
            Self::MainAccounts => 2,
        }
    }

    pub fn from_u8(value: u8) -> anyhow::Result<Self> {
        match value {
            0 => Ok(Self::Main),
            1 => Ok(Self::Deshred),
            2 => Ok(Self::MainAccounts),
            _ => anyhow::bail!("unknown stream id {value}"),
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Main => "main",
            Self::Deshred => "deshred",
            Self::MainAccounts => "main-accounts",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Frame {
    pub stream: StreamId,
    pub recv_ns: u64,
    pub data: Bytes,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChunkInfo {
    pub file: String,
    pub frames: u64,
    pub bytes: u64,
    pub first_recv_ns: u64,
    pub last_recv_ns: u64,
    pub min_slot: Option<u64>,
    pub max_slot: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Manifest {
    pub format: u32,
    pub endpoint: String,
    pub sni: Option<String>,
    pub server_version: Option<String>,
    pub requests: BTreeMap<String, String>,
    pub started_unix_ns: u64,
    pub finished_unix_ns: u64,
    pub end_reason: String,
    pub first_slot: Option<u64>,
    pub last_slot: Option<u64>,
    pub block_meta_count: u64,
    pub counts: BTreeMap<String, u64>,
    pub bytes: BTreeMap<String, u64>,
    pub notes: BTreeMap<String, String>,
    pub chunks: Vec<ChunkInfo>,
}

impl Manifest {
    pub fn load(dir: &Path) -> anyhow::Result<Self> {
        let path = dir.join(MANIFEST_FILE);
        let data = fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
        serde_json::from_slice(&data).with_context(|| format!("failed to parse {}", path.display()))
    }

    /// Writes the manifest atomically, so a crash never leaves a torn file.
    pub fn save(&self, dir: &Path) -> anyhow::Result<()> {
        let tmp = dir.join(format!("{MANIFEST_FILE}.tmp"));
        fs::write(&tmp, serde_json::to_vec_pretty(self)?)?;
        fs::rename(&tmp, dir.join(MANIFEST_FILE))?;
        Ok(())
    }

    pub fn exists(dir: &Path) -> bool {
        dir.join(MANIFEST_FILE).exists()
    }
}

struct OpenChunk {
    encoder: zstd::stream::write::Encoder<'static, BufWriter<File>>,
    info: ChunkInfo,
}

pub struct FixtureWriter {
    dir: PathBuf,
    chunk_target_bytes: u64,
    level: i32,
    current: Option<OpenChunk>,
    chunks: Vec<ChunkInfo>,
}

impl FixtureWriter {
    pub fn create(dir: &Path, chunk_target_bytes: u64, level: i32) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !Manifest::exists(dir),
            "fixture already exists at {}",
            dir.display()
        );
        fs::create_dir_all(dir.join(CHUNKS_DIR))
            .with_context(|| format!("failed to create {}", dir.display()))?;
        Ok(Self {
            dir: dir.to_path_buf(),
            chunk_target_bytes,
            level,
            current: None,
            chunks: Vec::new(),
        })
    }

    /// Appends one frame. Returns true when a chunk was closed before the frame was written.
    pub fn write(
        &mut self,
        stream: StreamId,
        recv_ns: u64,
        data: &[u8],
        slot: Option<u64>,
    ) -> anyhow::Result<bool> {
        let mut rotated = false;
        if self
            .current
            .as_ref()
            .is_some_and(|chunk| chunk.info.bytes >= self.chunk_target_bytes)
        {
            self.close_chunk()?;
            rotated = true;
        }
        if self.current.is_none() {
            self.open_chunk()?;
        }
        let chunk = self.current.as_mut().expect("chunk is open");
        let len = u32::try_from(data.len()).context("frame larger than 4 GiB")?;
        chunk.encoder.write_all(&[stream.as_u8()])?;
        chunk.encoder.write_all(&recv_ns.to_le_bytes())?;
        chunk.encoder.write_all(&len.to_le_bytes())?;
        chunk.encoder.write_all(data)?;

        let info = &mut chunk.info;
        if info.frames == 0 {
            info.first_recv_ns = recv_ns;
        }
        info.frames += 1;
        info.bytes += (FRAME_HEADER_LEN + data.len()) as u64;
        info.last_recv_ns = recv_ns;
        if let Some(slot) = slot {
            info.min_slot = Some(info.min_slot.map_or(slot, |min| min.min(slot)));
            info.max_slot = Some(info.max_slot.map_or(slot, |max| max.max(slot)));
        }
        Ok(rotated)
    }

    pub fn chunks(&self) -> &[ChunkInfo] {
        &self.chunks
    }

    pub fn finish(mut self) -> anyhow::Result<Vec<ChunkInfo>> {
        self.close_chunk()?;
        Ok(self.chunks)
    }

    fn open_chunk(&mut self) -> anyhow::Result<()> {
        let file = format!("{CHUNKS_DIR}/{:05}.ygf.zst", self.chunks.len());
        let path = self.dir.join(&file);
        let writer = BufWriter::with_capacity(
            1 << 20,
            File::create(&path).with_context(|| format!("failed to create {}", path.display()))?,
        );
        let mut encoder = zstd::stream::write::Encoder::new(writer, self.level)?;
        encoder.write_all(CHUNK_MAGIC)?;
        self.current = Some(OpenChunk {
            encoder,
            info: ChunkInfo {
                file,
                ..Default::default()
            },
        });
        Ok(())
    }

    fn close_chunk(&mut self) -> anyhow::Result<()> {
        if let Some(chunk) = self.current.take() {
            let writer = chunk.encoder.finish()?;
            let file = writer
                .into_inner()
                .map_err(|error| error.into_error())
                .context("failed to flush chunk")?;
            file.sync_all()?;
            self.chunks.push(chunk.info);
        }
        Ok(())
    }
}

type ChunkReader = BufReader<zstd::stream::read::Decoder<'static, BufReader<File>>>;

/// Reads frames across all chunks in order. A truncated last chunk ends the stream with an error.
pub struct FixtureReader {
    dir: PathBuf,
    files: Vec<String>,
    next: usize,
    current: Option<ChunkReader>,
}

impl FixtureReader {
    pub fn open(dir: &Path, chunks: &[ChunkInfo]) -> Self {
        Self {
            dir: dir.to_path_buf(),
            files: chunks.iter().map(|chunk| chunk.file.clone()).collect(),
            next: 0,
            current: None,
        }
    }

    /// Opens at the first chunk that can hold `slot` or later, using each chunk's slot range.
    pub fn open_from(dir: &Path, chunks: &[ChunkInfo], slot: u64) -> Self {
        let skip = chunks
            .iter()
            .take_while(|chunk| chunk.max_slot.is_some_and(|max| max < slot))
            .count();
        Self::open(dir, &chunks[skip..])
    }

    pub fn next_frame(&mut self) -> anyhow::Result<Option<Frame>> {
        loop {
            if self.current.is_none() {
                let Some(file) = self.files.get(self.next) else {
                    return Ok(None);
                };
                let path = self.dir.join(file);
                let decoder = zstd::stream::read::Decoder::new(
                    File::open(&path).with_context(|| format!("failed to open {}", path.display()))?,
                )?;
                let mut reader = BufReader::with_capacity(1 << 20, decoder);
                let mut magic = [0u8; 4];
                reader.read_exact(&mut magic)?;
                anyhow::ensure!(&magic == CHUNK_MAGIC, "bad chunk magic in {}", path.display());
                self.current = Some(reader);
                self.next += 1;
            }

            let reader = self.current.as_mut().expect("chunk is open");
            let mut header = [0u8; FRAME_HEADER_LEN];
            if !read_exact_or_eof(reader, &mut header)? {
                self.current = None;
                continue;
            }
            let stream = StreamId::from_u8(header[0])?;
            let recv_ns = u64::from_le_bytes(header[1..9].try_into().expect("8 bytes"));
            let len = u32::from_le_bytes(header[9..13].try_into().expect("4 bytes")) as usize;
            let mut data = vec![0u8; len];
            reader.read_exact(&mut data)?;
            return Ok(Some(Frame {
                stream,
                recv_ns,
                data: Bytes::from(data),
            }));
        }
    }
}

/// Fills `buf`. Returns false on a clean EOF before the first byte.
fn read_exact_or_eof(reader: &mut impl Read, buf: &mut [u8]) -> io::Result<bool> {
    let mut filled = 0;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..]) {
            Ok(0) if filled == 0 => return Ok(false),
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => filled += n,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_round_trip_across_chunks() {
        let dir = std::env::temp_dir().join(format!("ygb-fixture-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let mut writer = FixtureWriter::create(&dir, 64, 1).unwrap();
        let payloads: Vec<Vec<u8>> = (0..50u8).map(|i| vec![i; usize::from(i) * 3]).collect();
        for (i, payload) in payloads.iter().enumerate() {
            let stream = if i % 2 == 0 { StreamId::Main } else { StreamId::Deshred };
            writer.write(stream, i as u64, payload, Some(i as u64)).unwrap();
        }
        let chunks = writer.finish().unwrap();
        assert!(chunks.len() > 1, "small target must rotate chunks");

        let mut reader = FixtureReader::open(&dir, &chunks);
        for (i, payload) in payloads.iter().enumerate() {
            let frame = reader.next_frame().unwrap().expect("frame");
            assert_eq!(frame.recv_ns, i as u64);
            assert_eq!(frame.data.as_ref(), payload.as_slice());
        }
        assert!(reader.next_frame().unwrap().is_none());
        fs::remove_dir_all(&dir).unwrap();
    }
}
