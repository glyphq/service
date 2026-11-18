//! Compressed raw tick bundle container.
//!
//! # Container layout
//!
//! A bundle is a single object beginning with a fixed 40-byte little-endian
//! header followed by one Zstandard frame. The frame contains the uncompressed
//! section stream described below. Both the Zstandard frame checksum and the
//! XXH64 digest of the uncompressed body are verified on decode.
//!
//! ```text
//! offset  size  field
//! 0       4     magic, always b"QBB1"
//! 4       2     format version, currently 1
//! 6       2     flags, bit 0 = Zstandard compressed (always set)
//! 8       2     epoch copied from the bundle
//! 10      4     tick copied from the bundle
//! 14      2     number of section records in the body
//! 16      8     uncompressed body length in bytes
//! 24      8     compressed body (Zstandard frame) length in bytes
//! 32      8     XXH64 digest of the uncompressed body, seed 0
//! 40      ...   Zstandard frame
//! ```
//!
//! The uncompressed body is a sequence of section records. Empty sections are
//! omitted, and present sections appear strictly in ascending [`Section`] id
//! order. Every record is:
//!
//! ```text
//! offset  size  field
//! 0       1     section id
//! 1       7     reserved, zero
//! 8       8     payload length in bytes
//! 16      ...   payload
//! ```
//!
//! Section payloads are the exact bytes received for that part of the tick. The
//! container does not reinterpret them. This keeps replay byte-for-byte and
//! lets a future protocol version be archived without changing the container.
//!
//! Encoders and decoders reject unknown section ids, duplicate ids, out-of-order
//! ids, non-zero reserved bytes, trailing bytes, length mismatches, and bodies
//! larger than [`MAX_BUNDLE_BYTES`].
//!
//! # Example
//!
//! ```
//! use archive::bundle::{RawBundle, Section};
//!
//! let mut bundle = RawBundle::new(42, 123);
//! bundle.tick_header = vec![1, 2, 3];
//! bundle.tick_data = vec![0; 16];
//! bundle.set_section(Section::Computors, vec![9; 8]);
//!
//! let encoded = bundle.encode().unwrap();
//! let decoded = RawBundle::decode(&encoded).unwrap();
//! assert_eq!(decoded, bundle);
//! ```

use std::io::Write;

use bytes::Bytes;

use crate::digest::xxh64;
use crate::key::object_key;
use crate::object::RawArchive;
use crate::{Error, Result};

/// Magic bytes at the start of every bundle object.
pub const MAGIC: [u8; 4] = *b"QBB1";
/// Container format version written by [`RawBundle::encode`].
pub const FORMAT_VERSION: u16 = 1;
/// Size of the fixed container header in bytes.
pub const HEADER_LEN: usize = 40;
/// Header flag indicating a Zstandard-compressed body.
pub const FLAG_ZSTD: u16 = 1 << 0;
/// Zstandard compression level used by [`RawBundle::encode`].
pub const COMPRESSION_LEVEL: i32 = 3;
/// Maximum accepted uncompressed body length.
///
/// A tick's raw data, transactions, and logs are far below this bound. The limit
/// exists to reject decompression bombs before allocating.
pub const MAX_BUNDLE_BYTES: usize = 64 * 1024 * 1024;

/// A named part of a raw tick bundle.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Section {
    /// The winning 352-byte tick header.
    TickHeader = 1,
    /// The raw 139,376-byte `TickData` payload.
    TickData = 2,
    /// Concatenated raw 352-byte vote headers.
    Votes = 3,
    /// Concatenated raw transaction payloads.
    Transactions = 4,
    /// Raw log range table.
    LogRanges = 5,
    /// Concatenated raw log records, each with its 26-byte log header.
    Logs = 6,
    /// Raw computor list.
    Computors = 7,
}

impl Section {
    /// All sections in their canonical container order.
    pub const ALL: [Section; 7] = [
        Section::TickHeader,
        Section::TickData,
        Section::Votes,
        Section::Transactions,
        Section::LogRanges,
        Section::Logs,
        Section::Computors,
    ];

    /// Numeric id used in the container.
    pub const fn id(self) -> u8 {
        self as u8
    }

    /// Stable section name for diagnostics and logs.
    pub const fn name(self) -> &'static str {
        match self {
            Section::TickHeader => "tick_header",
            Section::TickData => "tick_data",
            Section::Votes => "votes",
            Section::Transactions => "transactions",
            Section::LogRanges => "log_ranges",
            Section::Logs => "logs",
            Section::Computors => "computors",
        }
    }

    /// Resolves a container id. Unknown ids return `None`.
    pub const fn from_id(id: u8) -> Option<Self> {
        match id {
            1 => Some(Section::TickHeader),
            2 => Some(Section::TickData),
            3 => Some(Section::Votes),
            4 => Some(Section::Transactions),
            5 => Some(Section::LogRanges),
            6 => Some(Section::Logs),
            7 => Some(Section::Computors),
            _ => None,
        }
    }
}

/// Everything the indexer received for one tick.
///
/// Fields are raw byte payloads. An absent or empty payload is omitted from the
/// encoded object. `epoch` and `tick` are also stored in the container header so
/// a caller can identify a bundle before decompressing it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RawBundle {
    /// Epoch the tick belongs to.
    pub epoch: u16,
    /// Tick number.
    pub tick: u32,
    /// The winning 352-byte tick header.
    pub tick_header: Vec<u8>,
    /// The raw 139,376-byte `TickData` payload.
    pub tick_data: Vec<u8>,
    /// Concatenated raw 352-byte vote headers.
    pub votes: Vec<u8>,
    /// Concatenated raw transaction payloads.
    pub transactions: Vec<u8>,
    /// Raw log range table.
    pub log_ranges: Vec<u8>,
    /// Concatenated raw log records.
    pub logs: Vec<u8>,
    /// Raw computor list.
    pub computors: Vec<u8>,
}

impl RawBundle {
    /// Creates an empty bundle for `(epoch, tick)`.
    pub fn new(epoch: u16, tick: u32) -> Self {
        Self {
            epoch,
            tick,
            ..Self::default()
        }
    }

    /// Returns the payload of a section.
    pub fn section(&self, section: Section) -> &[u8] {
        match section {
            Section::TickHeader => &self.tick_header,
            Section::TickData => &self.tick_data,
            Section::Votes => &self.votes,
            Section::Transactions => &self.transactions,
            Section::LogRanges => &self.log_ranges,
            Section::Logs => &self.logs,
            Section::Computors => &self.computors,
        }
    }

    /// Replaces the payload of a section.
    pub fn set_section(&mut self, section: Section, payload: Vec<u8>) {
        match section {
            Section::TickHeader => self.tick_header = payload,
            Section::TickData => self.tick_data = payload,
            Section::Votes => self.votes = payload,
            Section::Transactions => self.transactions = payload,
            Section::LogRanges => self.log_ranges = payload,
            Section::Logs => self.logs = payload,
            Section::Computors => self.computors = payload,
        }
    }

    /// True when every section is empty.
    pub fn is_empty(&self) -> bool {
        Section::ALL
            .iter()
            .all(|section| self.section(*section).is_empty())
    }

    /// Encodes the bundle as a compressed container.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let body = self.encode_body()?;
        let section_count = Section::ALL
            .iter()
            .filter(|section| !self.section(**section).is_empty())
            .count();
        encode_container(self.epoch, self.tick, section_count as u16, &body)
    }

    /// Decodes a bundle produced by [`RawBundle::encode`].
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < HEADER_LEN {
            return Err(Error::InvalidBundle(format!(
                "bundle needs at least {HEADER_LEN} bytes, got {}",
                bytes.len()
            )));
        }
        if bytes[0..4] != MAGIC {
            return Err(Error::InvalidBundle("bad magic, expected QBB1".into()));
        }
        let version = u16::from_le_bytes([bytes[4], bytes[5]]);
        if version != FORMAT_VERSION {
            return Err(Error::InvalidBundle(format!(
                "unsupported bundle version {version}"
            )));
        }
        let flags = u16::from_le_bytes([bytes[6], bytes[7]]);
        if flags & FLAG_ZSTD == 0 || flags & !FLAG_ZSTD != 0 {
            return Err(Error::InvalidBundle(format!(
                "unsupported bundle flags 0x{flags:04x}"
            )));
        }
        let epoch = u16::from_le_bytes([bytes[8], bytes[9]]);
        let tick = u32::from_le_bytes([bytes[10], bytes[11], bytes[12], bytes[13]]);
        let section_count = u16::from_le_bytes([bytes[14], bytes[15]]);
        if section_count as usize > Section::ALL.len() {
            return Err(Error::InvalidBundle(format!(
                "container declares {section_count} sections, maximum is {}",
                Section::ALL.len()
            )));
        }
        let uncompressed_len =
            u64::from_le_bytes(bytes[16..24].try_into().expect("header slice has 8 bytes"));
        let compressed_len =
            u64::from_le_bytes(bytes[24..32].try_into().expect("header slice has 8 bytes"));
        let expected_digest =
            u64::from_le_bytes(bytes[32..40].try_into().expect("header slice has 8 bytes"));
        let body_bytes = &bytes[HEADER_LEN..];
        if compressed_len != body_bytes.len() as u64 {
            return Err(Error::InvalidBundle(format!(
                "header declares {compressed_len} compressed bytes, got {}",
                body_bytes.len()
            )));
        }
        if uncompressed_len > MAX_BUNDLE_BYTES as u64 {
            return Err(Error::InvalidBundle(format!(
                "uncompressed body length {uncompressed_len} exceeds {MAX_BUNDLE_BYTES}"
            )));
        }
        let body = zstd::bulk::decompress(body_bytes, uncompressed_len as usize)?;
        if body.len() != uncompressed_len as usize {
            return Err(Error::InvalidBundle(format!(
                "header declares {uncompressed_len} uncompressed bytes, decoded {}",
                body.len()
            )));
        }
        let actual_digest = xxh64(&body, 0);
        if actual_digest != expected_digest {
            return Err(Error::InvalidBundle(format!(
                "body digest mismatch: header {expected_digest:016x}, computed {actual_digest:016x}"
            )));
        }
        Self::decode_body(epoch, tick, section_count, &body)
    }

    fn encode_body(&self) -> Result<Vec<u8>> {
        let mut total = 0usize;
        for section in Section::ALL {
            let payload = self.section(section);
            if payload.is_empty() {
                continue;
            }
            total = total
                .checked_add(16 + payload.len())
                .ok_or_else(|| Error::InvalidBundle("section stream length overflow".into()))?;
        }
        if total > MAX_BUNDLE_BYTES {
            return Err(Error::InvalidBundle(format!(
                "uncompressed body length {total} exceeds {MAX_BUNDLE_BYTES}"
            )));
        }
        let mut body = Vec::with_capacity(total);
        for section in Section::ALL {
            let payload = self.section(section);
            if payload.is_empty() {
                continue;
            }
            body.push(section.id());
            body.extend_from_slice(&[0; 7]);
            body.extend_from_slice(&(payload.len() as u64).to_le_bytes());
            body.extend_from_slice(payload);
        }
        Ok(body)
    }

    fn decode_body(epoch: u16, tick: u32, section_count: u16, body: &[u8]) -> Result<Self> {
        let mut bundle = Self::new(epoch, tick);
        let mut offset = 0usize;
        let mut seen = 0usize;
        let mut previous_id = 0u8;
        while offset < body.len() {
            if body.len() - offset < 16 {
                return Err(Error::InvalidBundle("truncated section header".into()));
            }
            let id = body[offset];
            let section = Section::from_id(id)
                .ok_or_else(|| Error::InvalidBundle(format!("unknown section id {id}")))?;
            if body[offset + 1..offset + 8].iter().any(|byte| *byte != 0) {
                return Err(Error::InvalidBundle(format!(
                    "section {} has non-zero reserved bytes",
                    section.name()
                )));
            }
            let len = u64::from_le_bytes(
                body[offset + 8..offset + 16]
                    .try_into()
                    .expect("section header slice has 8 bytes"),
            );
            if len > MAX_BUNDLE_BYTES as u64 {
                return Err(Error::InvalidBundle(format!(
                    "section {} length {len} exceeds {MAX_BUNDLE_BYTES}",
                    section.name()
                )));
            }
            let len = len as usize;
            offset += 16;
            let end = offset
                .checked_add(len)
                .ok_or_else(|| Error::InvalidBundle("section length overflow".into()))?;
            if end > body.len() {
                return Err(Error::InvalidBundle(format!(
                    "section {} declares {len} bytes, only {} available",
                    section.name(),
                    body.len() - offset
                )));
            }
            if seen > 0 && id <= previous_id {
                return Err(Error::InvalidBundle(format!(
                    "section {} is duplicate or out of order",
                    section.name()
                )));
            }
            bundle.set_section(section, body[offset..end].to_vec());
            offset = end;
            seen += 1;
            previous_id = id;
        }
        if seen != section_count as usize {
            return Err(Error::InvalidBundle(format!(
                "header declares {section_count} sections, decoded {seen}"
            )));
        }
        Ok(bundle)
    }
}

fn compress(body: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = zstd::Encoder::new(Vec::new(), COMPRESSION_LEVEL)?;
    encoder.include_checksum(true)?;
    encoder.write_all(body)?;
    Ok(encoder.finish()?)
}

fn encode_container(epoch: u16, tick: u32, section_count: u16, body: &[u8]) -> Result<Vec<u8>> {
    let compressed = compress(body)?;
    if compressed.len() > MAX_BUNDLE_BYTES {
        return Err(Error::InvalidBundle(format!(
            "compressed body exceeds {MAX_BUNDLE_BYTES} bytes"
        )));
    }
    let mut out = Vec::with_capacity(HEADER_LEN + compressed.len());
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&FLAG_ZSTD.to_le_bytes());
    out.extend_from_slice(&epoch.to_le_bytes());
    out.extend_from_slice(&tick.to_le_bytes());
    out.extend_from_slice(&section_count.to_le_bytes());
    out.extend_from_slice(&(body.len() as u64).to_le_bytes());
    out.extend_from_slice(&(compressed.len() as u64).to_le_bytes());
    out.extend_from_slice(&xxh64(body, 0).to_le_bytes());
    debug_assert_eq!(out.len(), HEADER_LEN);
    out.extend_from_slice(&compressed);
    Ok(out)
}

/// Encodes `bundle`, writes it at [`object_key`], and returns the key.
pub async fn put_bundle<A: RawArchive + ?Sized>(archive: &A, bundle: &RawBundle) -> Result<String> {
    let key = object_key(bundle.epoch, bundle.tick);
    let encoded = Bytes::from(bundle.encode()?);
    archive.put(&key, encoded).await?;
    Ok(key)
}

/// Reads and decodes the bundle for `(epoch, tick)`.
///
/// A missing object returns `Ok(None)`. Any other storage or decoding failure is
/// returned as an error.
pub async fn get_bundle<A: RawArchive + ?Sized>(
    archive: &A,
    epoch: u16,
    tick: u32,
) -> Result<Option<RawBundle>> {
    let key = object_key(epoch, tick);
    match archive.get(&key).await {
        Ok(bytes) => Ok(Some(RawBundle::decode(&bytes)?)),
        Err(Error::NotFound(_)) => Ok(None),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_bundle() -> RawBundle {
        let mut bundle = RawBundle::new(42, 7_000_001);
        bundle.tick_header = (0..352).map(|value| value as u8).collect();
        bundle.tick_data = vec![0x5a; 4096];
        bundle.votes = (0..352 * 4).map(|value| value as u8).collect();
        bundle.transactions = vec![0x11; 1024];
        bundle.log_ranges = vec![0; 64];
        bundle.logs = vec![0x33; 512];
        bundle.computors = vec![0x44; 256];
        bundle
    }

    #[test]
    fn round_trip_all_sections() {
        let bundle = sample_bundle();
        let encoded = bundle.encode().unwrap();
        assert_eq!(&encoded[0..4], b"QBB1");
        assert_eq!(u16::from_le_bytes([encoded[4], encoded[5]]), FORMAT_VERSION);
        let decoded = RawBundle::decode(&encoded).unwrap();
        assert_eq!(decoded, bundle);
    }

    #[test]
    fn round_trip_omits_empty_sections() {
        let mut bundle = RawBundle::new(3, 9);
        bundle.tick_header = vec![1, 2, 3];
        bundle.logs = vec![4, 5, 6];
        let encoded = bundle.encode().unwrap();
        let decoded = RawBundle::decode(&encoded).unwrap();
        assert_eq!(decoded, bundle);
        assert!(decoded.votes.is_empty());
        assert!(decoded.computors.is_empty());
    }

    #[test]
    fn round_trip_empty_bundle() {
        let bundle = RawBundle::new(1, 2);
        let encoded = bundle.encode().unwrap();
        let decoded = RawBundle::decode(&encoded).unwrap();
        assert_eq!(decoded, bundle);
        assert!(decoded.is_empty());
    }

    #[test]
    fn rejects_bad_magic_and_versions() {
        let encoded = sample_bundle().encode().unwrap();
        let mut bad = encoded.clone();
        bad[0] = b'X';
        assert!(RawBundle::decode(&bad).is_err());

        let mut bad = encoded.clone();
        bad[4] = 9;
        assert!(RawBundle::decode(&bad).is_err());

        let mut bad = encoded;
        bad[6] = 0;
        assert!(RawBundle::decode(&bad).is_err());
    }

    #[test]
    fn rejects_truncated_and_trailing_bytes() {
        let encoded = sample_bundle().encode().unwrap();
        assert!(RawBundle::decode(&encoded[..HEADER_LEN]).is_err());
        assert!(RawBundle::decode(&encoded[..encoded.len() - 1]).is_err());
        let mut trailing = encoded;
        trailing.push(0);
        assert!(RawBundle::decode(&trailing).is_err());
    }

    #[test]
    fn rejects_corrupt_compressed_body() {
        let mut encoded = sample_bundle().encode().unwrap();
        let index = HEADER_LEN + 8;
        encoded[index] ^= 0xff;
        assert!(RawBundle::decode(&encoded).is_err());
    }

    #[test]
    fn rejects_body_digest_mismatch() {
        let mut encoded = sample_bundle().encode().unwrap();
        encoded[32] ^= 0xff;
        assert!(RawBundle::decode(&encoded).is_err());
    }

    #[test]
    fn rejects_unknown_section_id() {
        let mut bundle = RawBundle::new(1, 2);
        bundle.tick_header = vec![1];
        let encoded = bundle.encode().unwrap();
        let mut body = zstd::bulk::decompress(&encoded[HEADER_LEN..], MAX_BUNDLE_BYTES).unwrap();
        body[0] = 99;
        let bad = encode_container(1, 2, 1, &body).unwrap();
        assert!(RawBundle::decode(&bad).is_err());
    }

    #[test]
    fn rejects_non_zero_reserved_bytes() {
        let mut bundle = RawBundle::new(1, 2);
        bundle.tick_header = vec![1];
        let encoded = bundle.encode().unwrap();
        let mut body = zstd::bulk::decompress(&encoded[HEADER_LEN..], MAX_BUNDLE_BYTES).unwrap();
        body[1] = 1;
        let bad = encode_container(1, 2, 1, &body).unwrap();
        assert!(RawBundle::decode(&bad).is_err());
    }

    #[test]
    fn section_ids_are_stable() {
        for section in Section::ALL {
            assert_eq!(Section::from_id(section.id()), Some(section));
        }
        assert_eq!(Section::from_id(0), None);
        assert_eq!(Section::from_id(8), None);
    }
}
