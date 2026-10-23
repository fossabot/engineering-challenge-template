//! [`AlignedSbeWriter`] writes SBE messages in fixed-size, zero-padded chunks.

use std::{
    fs::File,
    io::{BufWriter, Write},
    path::Path,
};

use super::dtos::SbeEncode;

// Every schema in `dtos` uses the standard eight-byte SBE message header.
const HEADER_LENGTH: usize = 8;

/// Writes SBE messages to a flat output in fixed-size, zero-padded chunks.
///
/// See [`AlignedSbeReader`](crate::util::AlignedSbeReader) for more details on
/// why this alignment exists.
///
/// Every chunk reserves enough trailing zeroes for an empty SBE header, so a
/// reader can stop decoding at the sentinel without carrying a frame across a
/// chunk boundary.
///
/// Call [`Self::flush`] to write the final buffered chunk before dropping the
/// writer.
pub struct AlignedSbeWriter<W: Write + Send = File> {
    writer: W,
    buf: Vec<u8>,
    chunk: usize,
    bytes_written: u64,
}

impl AlignedSbeWriter<BufWriter<File>> {
    /// Create an [`AlignedSbeWriter`] writing to `path` with `chunk` chunk
    /// size. Clears the file at `path`, if it exists.
    pub fn create(path: impl AsRef<Path>, chunk: usize) -> anyhow::Result<Self> {
        validate_chunk(chunk)?;
        let file = File::create(path)?;
        Ok(Self::new(BufWriter::with_capacity(chunk, file), chunk))
    }

    /// Flush buffered bytes and ask the operating system to persist the file.
    pub fn sync_all(&mut self) -> anyhow::Result<()> {
        self.flush()?;
        self.writer
            .get_ref()
            .sync_all()
            .map_err(anyhow::Error::from)
    }
}

impl<W: Write + Send> AlignedSbeWriter<W> {
    /// Create an aligned writer over an arbitrary byte sink, such as stdout or
    /// an in-memory buffer.
    pub fn from_writer(writer: W, chunk: usize) -> anyhow::Result<Self> {
        validate_chunk(chunk)?;
        Ok(Self::new(writer, chunk))
    }

    fn new(writer: W, chunk: usize) -> Self {
        Self {
            writer,
            buf: Vec::with_capacity(chunk),
            chunk,
            bytes_written: 0,
        }
    }

    /// Total bytes handed to the underlying writer, including zero padding.
    #[must_use]
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Configured chunk size in bytes.
    #[must_use]
    pub fn chunk_size(&self) -> usize {
        self.chunk
    }

    /// Encodes a complete message directly into the chunk buffer. Rotates to a
    /// new chunk if it does not fit in the existing chunk.
    pub fn write_msg<T: SbeEncode + ?Sized>(&mut self, message: &T) -> anyhow::Result<()> {
        let len = message.encoded_length_with_header()?;
        let payload_limit = self.chunk - HEADER_LENGTH;
        anyhow::ensure!(len <= payload_limit, "SBE message does not fit in a chunk");
        if len > payload_limit - self.buf.len() {
            self.flush()?;
        }

        let start = self.buf.len();
        self.buf.resize(start + len, 0);
        let result = message.encode(&mut self.buf[start..]).and_then(|written| {
            anyhow::ensure!(
                written == len,
                "SBE encoded length mismatch: expected {len} bytes, wrote {written}"
            );
            Ok(())
        });
        if result.is_err() {
            self.buf.truncate(start);
        }
        result
    }

    /// Flush the current buffer to the underlying sink, padding the final
    /// chunk to its configured alignment, and flush that sink.
    pub fn flush(&mut self) -> anyhow::Result<()> {
        if !self.buf.is_empty() {
            self.buf.resize(self.chunk, 0);
            self.writer.write_all(&self.buf)?;
            self.bytes_written += self.chunk as u64;
            self.buf.clear();
        }
        self.writer.flush()?;
        Ok(())
    }
}

fn validate_chunk(chunk: usize) -> anyhow::Result<()> {
    anyhow::ensure!(
        chunk > HEADER_LENGTH,
        "chunk must leave room for SBE data and an SBE header"
    );
    anyhow::ensure!(
        chunk.is_power_of_two(),
        "chunk size must be a power of two so the reader can auto-detect it"
    );
    anyhow::ensure!(chunk >= 1024, "chunk size must be at least 1024 bytes");
    Ok(())
}
