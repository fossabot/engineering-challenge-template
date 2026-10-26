mod aligned_sbe_reader;
mod aligned_sbe_writer;
pub mod dtos;
mod orderbook_schema;

pub use aligned_sbe_reader::{AlignedSbeReader, AlignedSbeSchema, PrefetchTask};
pub use aligned_sbe_writer::AlignedSbeWriter;
pub use orderbook_schema::OrderbookSchema;

/// Lowercase hexadecimal SHA-256 used by catalog and segment manifests.
#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(bytes);
    let mut text = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(text, "{byte:02x}");
    }
    text
}
