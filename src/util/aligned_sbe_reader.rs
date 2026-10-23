//! Defines the [`AlignedSbeReader`], which allows us to quickly read SBE
//! messages written by the
//! [`AlignedSbeWriter`](crate::util::aligned_sbe_writer::AlignedSbeWriter).

use std::{fs::File, marker::PhantomData, os::unix::fs::FileExt, path::Path};

use anyhow::Context;
use crossbeam::channel::{Receiver, Sender, bounded};

/// Any SBEs we want to read with the AlignedSbeReader must implement this
/// trait.
pub trait AlignedSbeSchema: 'static {
    const SCHEMA_ID: u16;
    const SCHEMA_VERSION: u16;

    type Message<'buf>
    where
        Self: 'buf;

    fn try_decode<'buf>(buffer: &'buf [u8], offset: usize) -> anyhow::Result<Self::Message<'buf>>;

    fn encoded_length(message: &Self::Message<'_>) -> anyhow::Result<usize>;

    fn expected_block_length(message: &Self::Message<'_>) -> anyhow::Result<usize>;
}

const DEFAULT_CHUNK_SIZE: usize = 64 << 20; // 64MiB

// Every schema in `dtos` uses the standard eight-byte SBE message header.
const HEADER_LENGTH: usize = 8;

/// See [`AlignedSbeReader::create_prefetch_task`] for more details.
pub type PrefetchTask = Box<dyn FnOnce() + Send + 'static>;

/// The [`AlignedSbeReader`] is designed to solve the issue of reading partial
/// messages from SBE flat files. Some background:
///
/// We want to read aeron-spec SBEs from disk at extremely fast speeds -- 100M
/// messages/sec on a single reader thread (with another prefetch thread/task in
/// the background).
///
/// Additionally, we wanted a very simple API to do so. To achieve this, we
/// prefetch our reads, and read in big chunks ([`DEFAULT_CHUNK_SIZE`]). However, when
/// you read in chunks, messages might tear across the chunk boundary. This is
/// undesireable, because now we need to coordinate our prefetch with our
/// reader, in case we need to read the next chunk to finish the currenet
/// message that is truncated on the current chunk.
///
/// This is annoying AF, so to solve the problem, we moved upstream: **Write the
/// encoded SBE messages all-or-nothing, aligned to the chunk size.** If we are
/// writing messages and we find that the next message will tear across two
/// chunks, we simply pad the rest of the current chunk with zeros, and write
/// the message, in full, starting at the next chunk.
///
/// This way the reader can then see the zeros, know the chunk is done, and skip
/// to the next one.
pub struct AlignedSbeReader<T: AlignedSbeSchema> {
    file: Option<File>,
    file_len: u64,
    chunk_size: usize,
    buffer: Vec<u8>,

    file_offset: u64,
    buffer_pos: usize,
    eof: bool,
    ready_chunks: Option<Receiver<std::io::Result<Option<Vec<u8>>>>>,
    recycled_chunks: Option<Sender<Vec<u8>>>,
    _schema: PhantomData<T>,
}

impl<T: AlignedSbeSchema> AlignedSbeReader<T> {
    /// Create a new [`AlignedSbeReader`] from the file at `path`. Chunk size is
    /// set as a constant in this file, [`DEFAULT_CHUNK_SIZE`].
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Self::open_with_chunk_size(path, DEFAULT_CHUNK_SIZE)
    }

    /// Detect the power-of-two chunk size used by an aligned writer and open
    /// the file with that alignment.
    ///
    /// Detection reads the file once and validates every chunk boundary against
    /// the SBE schema. Captures produced by this project are always flushed to
    /// whole chunks, so the file length is an exact multiple of the chunk size.
    pub fn open_auto(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let bytes = std::fs::read(path)?;
        let chunk_size = detect_chunk_size::<T>(&bytes)?;
        Self::open_with_chunk_size(path, chunk_size)
    }

    /// Create a new [`AlignedSbeReader`] from the file at `path`, with
    /// `chunk_size` for chunk size.
    pub fn open_with_chunk_size(path: impl AsRef<Path>, chunk_size: usize) -> anyhow::Result<Self> {
        let file = File::open(path)?;
        let file_len = file.metadata()?.len();
        anyhow::ensure!(chunk_size >= 1024, "chunk size must be at least 1024");
        anyhow::ensure!(
            file_len == 0 || file_len % chunk_size as u64 == 0,
            "file length {file_len} is not a multiple of chunk size {chunk_size}; \
             pass the writer's exact chunk size or use --chunk-size auto"
        );

        Ok(Self {
            file: Some(file),
            file_len,
            chunk_size,
            buffer: vec![0; chunk_size],
            file_offset: 0,
            buffer_pos: 0,
            eof: false,
            ready_chunks: None,
            recycled_chunks: None,
            _schema: PhantomData,
        })
    }

    /// Chunk size in bytes used for aligned reads.
    #[must_use]
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    /// Create a task to be passed up to the owner of this reader, so it can run
    /// chunk prefetching on a separate thread (optionally). When this method is
    /// run, this consumes certain fields of `self` so they may only be used by
    /// the prefetch task.
    pub fn create_prefetch_task(&mut self) -> anyhow::Result<PrefetchTask> {
        anyhow::ensure!(
            self.file_offset == 0 && self.buffer_pos == 0,
            "prefetch must start before reading"
        );

        let file = self.file.take().context("prefetch already started")?;
        let file_len = self.file_len;
        let chunk_size = self.chunk_size;
        let spare_buffers = [std::mem::take(&mut self.buffer), vec![0; self.chunk_size]];
        let (ready_chunks_tx, ready_chunks) = bounded(1);
        let (recycled_chunks, recycled_chunks_rx) = bounded(2);
        self.ready_chunks = Some(ready_chunks);
        self.recycled_chunks = Some(recycled_chunks);

        Ok(Box::new(move || {
            let mut spare_buffers = spare_buffers.into_iter();
            let mut file_offset = 0u64;

            loop {
                let Some(end) = file_offset.checked_add(chunk_size as u64) else {
                    return;
                };
                if end > file_len {
                    let _ = ready_chunks_tx.send(Ok(None));
                    return;
                }

                let mut buffer = match spare_buffers.next() {
                    Some(buffer) => buffer,
                    None => match recycled_chunks_rx.recv() {
                        Ok(buffer) => buffer,
                        Err(_) => return,
                    },
                };
                if let Err(error) = file.read_exact_at(&mut buffer, file_offset) {
                    let _ = ready_chunks_tx.send(Err(error));
                    return;
                }
                if ready_chunks_tx.send(Ok(Some(buffer))).is_err() {
                    return;
                }
                file_offset = end;
            }
        }))
    }

    /// Signal to the prefetch task that it should stop running.
    pub fn signal_stop_prefetch(&mut self) {
        self.ready_chunks = None;
        self.recycled_chunks = None;
    }

    /// Returns whether the next chunk could be loaded or not.
    fn load_chunk(&mut self) -> anyhow::Result<bool> {
        if self.eof {
            return Ok(false);
        }

        if let Some(ready_chunks) = &self.ready_chunks {
            if !self.buffer.is_empty() {
                let buffer = std::mem::take(&mut self.buffer);
                if let Some(recycled_chunks) = &self.recycled_chunks {
                    let _ = recycled_chunks.send(buffer);
                }
            }

            return match ready_chunks
                .recv()
                .context("SBE prefetch thread stopped")??
            {
                Some(buffer) => {
                    self.buffer = buffer;
                    self.buffer_pos = 0;
                    Ok(true)
                }
                None => {
                    self.eof = true;
                    Ok(false)
                }
            };
        }

        let end = self
            .file_offset
            .checked_add(self.chunk_size as u64)
            .context("offset overflow")?;
        if end > self.file_len {
            return Ok(false);
        }

        self.file
            .as_ref()
            .context("file moved to prefetch task")?
            .read_exact_at(&mut self.buffer, self.file_offset)?;

        self.file_offset = end;
        self.buffer_pos = 0;

        Ok(true)
    }

    /// Decode the next message from our chunk buffer.
    ///
    /// Returns `Ok(None)` once we reach the end of the file, and `Err(...)` if
    /// there is an erroring reading the file, decoding the next message, etc.
    pub fn read_msg(&mut self) -> anyhow::Result<Option<T::Message<'_>>> {
        loop {
            let header_end = self
                .buffer_pos
                .checked_add(HEADER_LENGTH)
                .context("pos overflow")?;

            if header_end <= self.buffer.len()
                && self.buffer[self.buffer_pos..header_end] != [0; HEADER_LENGTH]
            {
                break;
            }

            // A chunk sentinel is a run of zero bytes through the end of the
            // chunk. Anything non-zero after the first zero header is malformed
            // and must not be silently skipped while advancing to the next
            // chunk.
            anyhow::ensure!(
                self.buffer[self.buffer_pos..].iter().all(|byte| *byte == 0),
                "non-zero bytes after the aligned SBE chunk sentinel"
            );

            if !self.load_chunk()? {
                return Ok(None);
            }
        }

        // All generated schemas share this header layout:
        // blockLength, templateId, schemaId, version -- all little-endian u16.
        let header_end = self.buffer_pos + HEADER_LENGTH;
        let header = &self.buffer[self.buffer_pos..header_end];
        let block_length = usize::from(u16::from_le_bytes([header[0], header[1]]));
        let schema_id = u16::from_le_bytes([header[4], header[5]]);
        let schema_version = u16::from_le_bytes([header[6], header[7]]);

        anyhow::ensure!(schema_id == T::SCHEMA_ID);
        anyhow::ensure!(schema_version == T::SCHEMA_VERSION);

        let decoded = T::try_decode(&self.buffer, self.buffer_pos)?;

        anyhow::ensure!(block_length == T::expected_block_length(&decoded)?);

        let message_len = T::encoded_length(&decoded)?;
        let end = self
            .buffer_pos
            .checked_add(message_len)
            .context("overflow")?;
        anyhow::ensure!(
            end <= self.buffer.len(),
            "SBE message crosses the aligned chunk boundary"
        );

        self.buffer_pos = end;

        Ok(Some(decoded))
    }
}

/// Scan power-of-two chunk sizes and return the smallest one whose every chunk
/// boundary is a valid SBE stream.
pub fn detect_chunk_size<T: AlignedSbeSchema>(bytes: &[u8]) -> anyhow::Result<usize> {
    if bytes.is_empty() {
        return Ok(DEFAULT_CHUNK_SIZE);
    }

    let mut best: Option<(usize, usize)> = None; // (chunk size, message count)
    let mut chunk = 1024usize;
    while chunk <= bytes.len() {
        if bytes.len().is_multiple_of(chunk) {
            match validate_aligned::<T>(bytes, chunk) {
                Ok(count) if count > 0 && best.is_none_or(|(_, best_count)| count > best_count) => {
                    best = Some((chunk, count));
                }
                _ => {}
            }
        }
        chunk = chunk.saturating_mul(2);
    }

    if let Some((chunk, _)) = best {
        return Ok(chunk);
    }

    anyhow::bail!(
        "could not auto-detect chunk size for {} bytes of SBE data; \
         specify --chunk-size explicitly",
        bytes.len()
    )
}

fn validate_aligned<T: AlignedSbeSchema>(bytes: &[u8], chunk: usize) -> anyhow::Result<usize> {
    let mut total = 0usize;
    for start in (0..bytes.len()).step_by(chunk) {
        let end = start + chunk;
        let mut pos = 0usize;
        while pos < chunk {
            let offset = start + pos;
            let remaining = end - offset;
            if remaining < HEADER_LENGTH {
                anyhow::bail!("truncated header");
            }
            let header = &bytes[offset..offset + HEADER_LENGTH];
            if header == [0; HEADER_LENGTH] {
                anyhow::ensure!(
                    bytes[offset..end].iter().all(|byte| *byte == 0),
                    "non-zero bytes after the chunk sentinel"
                );
                break;
            }
            let block_length = usize::from(u16::from_le_bytes([header[0], header[1]]));
            let schema_id = u16::from_le_bytes([header[4], header[5]]);
            let schema_version = u16::from_le_bytes([header[6], header[7]]);
            anyhow::ensure!(schema_id == T::SCHEMA_ID, "schema id mismatch");
            anyhow::ensure!(
                schema_version == T::SCHEMA_VERSION,
                "schema version mismatch"
            );

            let (message_len, expected_block_length) = {
                let decoded = T::try_decode(bytes, offset)?;
                (
                    T::encoded_length(&decoded)?,
                    T::expected_block_length(&decoded)?,
                )
            };
            anyhow::ensure!(
                block_length == expected_block_length,
                "block length mismatch"
            );
            anyhow::ensure!(message_len >= HEADER_LENGTH, "invalid message length");
            anyhow::ensure!(message_len <= remaining, "message crosses chunk boundary");
            pos += message_len;
            total += 1;
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;
    use crate::util::OrderbookSchema;

    #[test]
    fn rejects_non_zero_bytes_after_a_chunk_sentinel() {
        let path = std::env::temp_dir().join(format!(
            "engineering-challenge-sentinel-{}-{}.sbe",
            std::process::id(),
            crate::time::now_us().unwrap_or(0)
        ));
        let mut bytes = vec![0u8; 1024];
        bytes[100] = 1;
        {
            let mut file = File::create(&path).unwrap();
            file.write_all(&bytes).unwrap();
            file.sync_all().unwrap();
        }
        let mut reader =
            AlignedSbeReader::<OrderbookSchema>::open_with_chunk_size(&path, 1024).unwrap();
        let error = match reader.read_msg() {
            Ok(_) => panic!("expected a sentinel validation error"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("non-zero bytes"), "{error}");
        let _ = std::fs::remove_file(path);
    }
}
