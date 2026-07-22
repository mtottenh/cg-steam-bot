//! Minimal CS2 demo parser that extracts only rank update messages.
//!
//! Parses Source 2 `.dem` files to find `CCSUsrMsg_ServerRankUpdate` protobuf
//! user messages embedded in `CDemoPacket` frames. This avoids a full demo
//! parse — we only care about the per-player rank data.

use std::borrow::Cow;
use tracing::debug;

// =============================================================================
// Public types
// =============================================================================

/// A single player's rank update extracted from a demo.
#[derive(Debug, Clone)]
pub struct RankUpdate {
    /// Steam account ID (Steam32 — add 76561197960265728 for SteamID64).
    pub account_id: u32,
    /// New rank value. CS Rating (0-35000+) for Premier; 1-18 for Comp/Wingman.
    pub rank_id: i32,
    /// Rank type: 6 = Competitive, 7 = Wingman, 11 = Premier.
    pub rank_type_id: u32,
    /// Number of competitive wins.
    pub wins: u32,
    /// Rating change from this match (float delta).
    pub rank_change: f32,
}

const STEAM_ID64_BASE: u64 = 76561197960265728; // 0x0110000100000000

impl RankUpdate {
    /// Convert Steam32 account_id to full Steam64 ID.
    pub fn steam_id64(&self) -> u64 {
        STEAM_ID64_BASE + self.account_id as u64
    }
}

/// Errors that can occur during demo rank extraction.
#[derive(Debug, thiserror::Error)]
pub enum DemoRankError {
    #[error("invalid demo header: expected PBDEMS2 magic")]
    InvalidHeader,
    #[error("demo too short ({0} bytes)")]
    TooShort(usize),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("snappy decompress error: {0}")]
    Snappy(#[from] snap::Error),
}

// =============================================================================
// Source 2 demo format constants
// =============================================================================

const HEADER_MAGIC: &[u8; 8] = b"PBDEMS2\0";
const HEADER_SIZE: usize = 16; // 8 bytes magic + 8 bytes (two i32 offsets)

// Outer demo command types (bit 6 = compressed flag, masked with & !64)
const DEM_FILE_HEADER: u32 = 1; // CDemoFileHeader (first frame, contains map_name)
const DEM_PACKET: u32 = 7; // CDemoPacket
const DEM_FULL_PACKET: u32 = 13; // CDemoFullPacket

// CS2 net message type for rank updates (direct in packet, not wrapped)
const CS_UM_SERVER_RANK_UPDATE: u32 = 352;

// =============================================================================
// Public API
// =============================================================================

/// Extract rank update entries from a decompressed CS2 demo file.
///
/// The demo must be the raw `.dem` bytes (already decompressed from `.bz2`).
/// Returns an empty vec if no rank updates are found (e.g. casual/deathmatch).
pub fn extract_rank_updates(demo_bytes: &[u8]) -> Result<Vec<RankUpdate>, DemoRankError> {
    if demo_bytes.len() < HEADER_SIZE {
        return Err(DemoRankError::TooShort(demo_bytes.len()));
    }

    if &demo_bytes[..8] != HEADER_MAGIC {
        return Err(DemoRankError::InvalidHeader);
    }

    let mut results = Vec::with_capacity(10);
    let mut pos = HEADER_SIZE;
    let mut frame_count: u64 = 0;
    let mut packet_count: u64 = 0;
    let mut rank_msg_count: u64 = 0;
    let mut snappy_decoder = snap::raw::Decoder::new();

    while pos < demo_bytes.len() {
        // Read outer frame: varint command tag, then varint tick, then varint size
        let (cmd_raw, bytes_read) = read_varint(&demo_bytes[pos..])?;
        pos += bytes_read;
        if pos >= demo_bytes.len() {
            break;
        }

        // Skip tick field
        let (_, bytes_read) = read_varint(&demo_bytes[pos..])?;
        pos += bytes_read;
        if pos >= demo_bytes.len() {
            break;
        }

        // Read size
        let (size, bytes_read) = read_varint(&demo_bytes[pos..])?;
        pos += bytes_read;

        let size = size as usize;
        if pos + size > demo_bytes.len() {
            break; // truncated frame, stop
        }

        let frame_data = &demo_bytes[pos..pos + size];
        pos += size;

        // Bit 6 (value 64) indicates compression
        let is_compressed = (cmd_raw & 64) != 0;
        let cmd = (cmd_raw & !64) as u32;

        frame_count += 1;

        if cmd == DEM_PACKET || cmd == DEM_FULL_PACKET {
            packet_count += 1;
            let packet_data: Cow<'_, [u8]> = if is_compressed && !frame_data.is_empty() {
                match snappy_decoder.decompress_vec(frame_data) {
                    Ok(d) => Cow::Owned(d),
                    Err(e) => {
                        debug!("Snappy decompress failed for packet frame: {e}");
                        continue;
                    }
                }
            } else {
                Cow::Borrowed(frame_data)
            };

            if cmd == DEM_FULL_PACKET {
                // CDemoFullPacket has a string_table field (1) and a packet field (2).
                // We only care about the packet field.
                if let Some(inner) = extract_full_packet_data(&packet_data) {
                    scan_packet_for_ranks(inner, &mut results, &mut rank_msg_count)?;
                }
            } else {
                // CDemoPacket: the `data` field (tag 3, wire type 2 = length-delimited)
                if let Some(inner) = extract_demo_packet_data(&packet_data) {
                    scan_packet_for_ranks(inner, &mut results, &mut rank_msg_count)?;
                }
            }
        }
    }

    debug!(
        frame_count,
        packet_count,
        rank_msg_count,
        rank_updates = results.len(),
        "Demo parse complete"
    );

    Ok(results)
}

/// Metadata extracted from a demo file header.
#[derive(Debug, Clone)]
pub struct DemoMetadata {
    /// Map name (e.g. "de_inferno", "de_dust2").
    pub map_name: Option<String>,
}

/// Extract metadata from a decompressed CS2 demo file.
///
/// Reads only the first frame (`CDemoFileHeader`) which contains the map name.
/// This is very fast — no need to scan the entire demo.
pub fn extract_demo_metadata(demo_bytes: &[u8]) -> Result<DemoMetadata, DemoRankError> {
    if demo_bytes.len() < HEADER_SIZE {
        return Err(DemoRankError::TooShort(demo_bytes.len()));
    }
    if &demo_bytes[..8] != HEADER_MAGIC {
        return Err(DemoRankError::InvalidHeader);
    }

    let mut pos = HEADER_SIZE;
    let mut snappy_decoder = snap::raw::Decoder::new();

    // Read the first frame — should be CDemoFileHeader (cmd=1)
    if pos >= demo_bytes.len() {
        return Ok(DemoMetadata { map_name: None });
    }

    let (cmd_raw, bytes_read) = read_varint(&demo_bytes[pos..])?;
    pos += bytes_read;

    // Skip tick
    let (_, bytes_read) = read_varint(&demo_bytes[pos..])?;
    pos += bytes_read;

    // Read size
    let (size, bytes_read) = read_varint(&demo_bytes[pos..])?;
    pos += bytes_read;

    let size = size as usize;
    if pos + size > demo_bytes.len() {
        return Ok(DemoMetadata { map_name: None });
    }

    let frame_data = &demo_bytes[pos..pos + size];
    let is_compressed = (cmd_raw & 64) != 0;
    let cmd = (cmd_raw & !64) as u32;

    if cmd != DEM_FILE_HEADER {
        debug!(cmd, "First frame is not CDemoFileHeader");
        return Ok(DemoMetadata { map_name: None });
    }

    let header_data: Cow<'_, [u8]> = if is_compressed && !frame_data.is_empty() {
        match snappy_decoder.decompress_vec(frame_data) {
            Ok(d) => Cow::Owned(d),
            Err(_) => return Ok(DemoMetadata { map_name: None }),
        }
    } else {
        Cow::Borrowed(frame_data)
    };

    // Parse CDemoFileHeader protobuf — field 5 is map_name (string)
    let map_name = extract_proto_string(&header_data, 5);

    debug!(map_name = ?map_name, "Extracted demo metadata");
    Ok(DemoMetadata { map_name })
}

/// Extract a string field by field number from a protobuf message.
fn extract_proto_string(buf: &[u8], target_field: u32) -> Option<String> {
    let mut cursor = ProtoCursor::new(buf);
    while !cursor.is_eof() {
        let tag = cursor.read_varint32()?;
        let field_number = tag >> 3;
        let wire_type = tag & 0x7;

        if field_number == target_field && wire_type == 2 {
            let data = cursor.read_length_delimited()?;
            return std::str::from_utf8(data).ok().map(|s| s.to_string());
        }
        cursor.skip_field(wire_type)?;
    }
    None
}

// =============================================================================
// Zero-copy protobuf cursor (replaces CodedInputStream)
// =============================================================================

/// Lightweight cursor over a protobuf-encoded byte slice.
/// Returns borrowed slices for length-delimited fields (zero-copy).
struct ProtoCursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> ProtoCursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn is_eof(&self) -> bool {
        self.pos >= self.buf.len()
    }

    fn read_varint32(&mut self) -> Option<u32> {
        let mut value: u32 = 0;
        let mut shift: u32 = 0;
        loop {
            let byte = *self.buf.get(self.pos)?;
            self.pos += 1;
            value |= ((byte & 0x7F) as u32) << shift;
            if byte & 0x80 == 0 {
                return Some(value);
            }
            shift += 7;
            if shift >= 35 {
                return Some(value); // truncate, don't loop forever
            }
        }
    }

    fn read_varint64(&mut self) -> Option<u64> {
        let mut value: u64 = 0;
        let mut shift: u32 = 0;
        loop {
            let byte = *self.buf.get(self.pos)?;
            self.pos += 1;
            value |= ((byte & 0x7F) as u64) << shift;
            if byte & 0x80 == 0 {
                return Some(value);
            }
            shift += 7;
            if shift >= 70 {
                return Some(value);
            }
        }
    }

    fn read_fixed32(&mut self) -> Option<u32> {
        let bytes = self.buf.get(self.pos..self.pos + 4)?;
        self.pos += 4;
        Some(u32::from_le_bytes(bytes.try_into().ok()?))
    }

    fn read_fixed64(&mut self) -> Option<u64> {
        let bytes = self.buf.get(self.pos..self.pos + 8)?;
        self.pos += 8;
        Some(u64::from_le_bytes(bytes.try_into().ok()?))
    }

    fn read_float(&mut self) -> Option<f32> {
        self.read_fixed32().map(f32::from_bits)
    }

    fn read_int32(&mut self) -> Option<i32> {
        self.read_varint32().map(|v| v as i32)
    }

    fn read_length_delimited(&mut self) -> Option<&'a [u8]> {
        let len = self.read_varint32()? as usize;
        let data = self.buf.get(self.pos..self.pos + len)?;
        self.pos += len;
        Some(data)
    }

    fn skip_field(&mut self, wire_type: u32) -> Option<()> {
        match wire_type {
            0 => {
                self.read_varint64()?;
            } // varint
            1 => {
                self.read_fixed64()?;
            } // 64-bit
            2 => {
                self.read_length_delimited()?;
            } // length-delimited
            5 => {
                self.read_fixed32()?;
            } // 32-bit
            _ => {} // unknown, bail
        }
        Some(())
    }
}

// =============================================================================
// Protobuf field extraction (zero-copy)
// =============================================================================

/// Extract the `data` field (field 3) from a CDemoPacket protobuf message.
fn extract_demo_packet_data(buf: &[u8]) -> Option<&[u8]> {
    extract_length_delimited_field(buf, 3)
}

/// Extract the `packet` field (field 2) from a CDemoFullPacket, then extract
/// the `data` field (field 3) from the inner CDemoPacket.
fn extract_full_packet_data(buf: &[u8]) -> Option<&[u8]> {
    let inner_packet = extract_length_delimited_field(buf, 2)?;
    extract_length_delimited_field(inner_packet, 3)
}

/// Generic helper: find a length-delimited field by number in a protobuf message.
fn extract_length_delimited_field(buf: &[u8], target_field: u32) -> Option<&[u8]> {
    let mut cursor = ProtoCursor::new(buf);
    while !cursor.is_eof() {
        let tag = cursor.read_varint32()?;
        let field_number = tag >> 3;
        let wire_type = tag & 0x7;

        if field_number == target_field && wire_type == 2 {
            return cursor.read_length_delimited();
        }

        cursor.skip_field(wire_type)?;
    }
    None
}

// =============================================================================
// Bit reader for Source 2 inner packet data
// =============================================================================

/// Minimal bit reader for Source 2 network packet data.
///
/// Inner packet messages use a bit-oriented format:
///   msg_type = UBitVar (variable-width bit field)
///   size     = varint (byte-aligned within the bit stream)
///   payload  = `size` bytes
struct BitReader<'a> {
    data: &'a [u8],
    /// Current bit position.
    bit_pos: usize,
}

impl<'a> BitReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, bit_pos: 0 }
    }

    fn bits_remaining(&self) -> usize {
        self.data
            .len()
            .saturating_mul(8)
            .saturating_sub(self.bit_pos)
    }

    /// Read `n` bits (up to 32) as a u32, little-endian bit order.
    fn read_bits(&mut self, n: u32) -> Option<u32> {
        if n == 0 {
            return Some(0);
        }
        if self.bits_remaining() < n as usize {
            return None;
        }

        let mut result: u32 = 0;
        let mut bits_read: u32 = 0;

        while bits_read < n {
            let byte_idx = self.bit_pos / 8;
            let bit_offset = self.bit_pos % 8;
            let bits_avail_in_byte = 8 - bit_offset as u32;
            let bits_to_read = (n - bits_read).min(bits_avail_in_byte);

            let byte = *self.data.get(byte_idx)?;
            let mask = ((1u32 << bits_to_read) - 1) as u8;
            let bits = (byte >> bit_offset) & mask;

            result |= (bits as u32) << bits_read;
            bits_read += bits_to_read;
            self.bit_pos += bits_to_read as usize;
        }

        Some(result)
    }

    /// Read a Source 2 UBitVar — variable-width unsigned integer.
    ///
    /// Reads 6 bits. The top 2 bits determine how many extra bits follow:
    ///   00xxxx → 4-bit value (0..15)
    ///   01xxxx → 4 + 4 extra bits (0..255)
    ///   10xxxx → 4 + 8 extra bits (0..4095)
    ///   11xxxx → 4 + 28 extra bits (0..~268M)
    fn read_ubit_var(&mut self) -> Option<u32> {
        let bits = self.read_bits(6)?;
        match bits & 0x30 {
            0x10 => Some((bits & 0x0F) | (self.read_bits(4)? << 4)),
            0x20 => Some((bits & 0x0F) | (self.read_bits(8)? << 4)),
            0x30 => Some((bits & 0x0F) | (self.read_bits(28)? << 4)),
            _ => Some(bits),
        }
    }

    /// Read a varint from the bit stream (reads 8 bits at a time).
    fn read_varint(&mut self) -> Option<u32> {
        let mut result: u32 = 0;
        let mut count: u32 = 0;
        loop {
            if count >= 5 {
                return Some(result);
            }
            let byte = self.read_bits(8)?;
            result |= (byte & 0x7F) << (7 * count);
            count += 1;
            if byte & 0x80 == 0 {
                break;
            }
        }
        Some(result)
    }

    /// Read `n` bytes from the bit stream at the current bit position.
    ///
    /// When byte-aligned, slices directly from the underlying buffer (fast path).
    /// When not byte-aligned (e.g. after a UBitVar that used 10/14/34 bits),
    /// assembles each byte by reading 8 bits across byte boundaries.
    fn read_bytes(&mut self, n: usize) -> Option<Vec<u8>> {
        if self.bit_pos % 8 == 0 {
            // Fast path: byte-aligned, can slice directly
            let byte_pos = self.bit_pos / 8;
            let end = byte_pos.checked_add(n)?;
            if end > self.data.len() {
                return None;
            }
            self.bit_pos = end * 8;
            Some(self.data[byte_pos..end].to_vec())
        } else {
            // Slow path: not byte-aligned, read 8 bits at a time
            let mut result = Vec::with_capacity(n);
            for _ in 0..n {
                result.push(self.read_bits(8)? as u8);
            }
            Some(result)
        }
    }

    /// Advance the bit position by `n` bits without reading. O(1).
    fn skip_bits(&mut self, n: usize) -> bool {
        if self.bits_remaining() >= n {
            self.bit_pos += n;
            true
        } else {
            false
        }
    }
}

/// Scan the raw network packet data for rank update messages.
///
/// Source 2 inner packet format uses bit-oriented encoding:
///   msg_type = UBitVar, size = varint, payload = `size` bytes
fn scan_packet_for_ranks(
    packet_data: &[u8],
    results: &mut Vec<RankUpdate>,
    rank_msg_count: &mut u64,
) -> Result<(), DemoRankError> {
    let mut reader = BitReader::new(packet_data);

    while reader.bits_remaining() > 8 {
        let msg_type = match reader.read_ubit_var() {
            Some(v) => v,
            None => break,
        };
        let size = match reader.read_varint() {
            Some(v) => v as usize,
            None => break,
        };

        if msg_type == CS_UM_SERVER_RANK_UPDATE {
            *rank_msg_count += 1;
            if let Some(payload) = reader.read_bytes(size) {
                debug!(
                    payload_len = payload.len(),
                    "Found CS_UM_ServerRankUpdate message"
                );
                parse_rank_update(&payload, results);
            } else {
                debug!("Failed to read rank update payload of size {size}");
                break;
            }
        } else {
            // Skip payload in O(1) — no need to read bytes
            if !reader.skip_bits(size * 8) {
                break;
            }
        }
    }

    Ok(())
}

/// Parse a CCSUsrMsg_ServerRankUpdate protobuf message.
///
/// CCSUsrMsg_ServerRankUpdate:
///   repeated RankUpdateEntry rank_update = 1;
///
/// RankUpdateEntry:
///   optional int32 account_id = 1;
///   optional int32 rank_old = 2;
///   optional int32 rank_new = 3;
///   optional int32 num_wins = 4;
///   optional float rank_change = 5;
///   optional int32 rank_type_id = 6;
fn parse_rank_update(buf: &[u8], results: &mut Vec<RankUpdate>) {
    let mut cursor = ProtoCursor::new(buf);

    while !cursor.is_eof() {
        let tag = match cursor.read_varint32() {
            Some(t) => t,
            None => break,
        };
        let field_number = tag >> 3;
        let wire_type = tag & 0x7;

        if field_number == 1 && wire_type == 2 {
            // Repeated RankUpdateEntry (length-delimited sub-message)
            let entry_data = match cursor.read_length_delimited() {
                Some(d) => d,
                None => break,
            };
            if let Some(entry) = parse_rank_update_entry(entry_data) {
                debug!(
                    account_id = entry.account_id,
                    rank_id = entry.rank_id,
                    rank_type_id = entry.rank_type_id,
                    wins = entry.wins,
                    rank_change = entry.rank_change,
                    "Found rank update"
                );
                results.push(entry);
            }
        } else {
            cursor.skip_field(wire_type);
        }
    }
}

/// Parse a single RankUpdateEntry sub-message.
fn parse_rank_update_entry(buf: &[u8]) -> Option<RankUpdate> {
    let mut cursor = ProtoCursor::new(buf);
    let mut account_id: u32 = 0;
    let mut rank_new: i32 = 0;
    let mut num_wins: u32 = 0;
    let mut rank_change: f32 = 0.0;
    let mut rank_type_id: u32 = 0;

    while !cursor.is_eof() {
        let tag = match cursor.read_varint32() {
            Some(t) => t,
            None => break,
        };
        let field_number = tag >> 3;
        let wire_type = tag & 0x7;

        match (field_number, wire_type) {
            (1, 0) => account_id = cursor.read_int32().unwrap_or(0) as u32,
            (2, 0) => {
                cursor.read_int32();
            } // rank_old — skip
            (3, 0) => rank_new = cursor.read_int32().unwrap_or(0),
            (4, 0) => num_wins = cursor.read_int32().unwrap_or(0) as u32,
            (5, 5) => rank_change = cursor.read_float().unwrap_or(0.0),
            (6, 0) => rank_type_id = cursor.read_int32().unwrap_or(0) as u32,
            _ => {
                cursor.skip_field(wire_type);
            }
        }
    }

    if account_id == 0 {
        return None;
    }

    Some(RankUpdate {
        account_id,
        rank_id: rank_new,
        rank_type_id,
        wins: num_wins,
        rank_change,
    })
}

// =============================================================================
// Varint helper for outer frame parsing
// =============================================================================

/// Read a protobuf varint from a byte slice. Returns (value, bytes_consumed).
fn read_varint(buf: &[u8]) -> Result<(u64, usize), DemoRankError> {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    for (i, &byte) in buf.iter().enumerate() {
        value |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok((value, i + 1));
        }
        shift += 7;
        if shift >= 64 {
            return Err(DemoRankError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "varint too long",
            )));
        }
    }
    Err(DemoRankError::Io(std::io::Error::new(
        std::io::ErrorKind::UnexpectedEof,
        "unexpected end of varint",
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalid_header() {
        let data = b"NOT_A_DEMO_FILE_HEADER!!";
        let result = extract_rank_updates(data);
        assert!(matches!(result, Err(DemoRankError::InvalidHeader)));
    }

    #[test]
    fn test_too_short() {
        let result = extract_rank_updates(b"short");
        assert!(matches!(result, Err(DemoRankError::TooShort(_))));
    }

    #[test]
    fn test_valid_header_no_frames() {
        // Valid header with two zero i32 offsets, but no frames after
        let mut data = Vec::from(HEADER_MAGIC.as_slice());
        data.extend_from_slice(&[0u8; 8]); // two i32 zero offsets
        let result = extract_rank_updates(&data).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_rank_update_entry_roundtrip() {
        // Manually encode a RankUpdateEntry protobuf
        let mut buf = Vec::new();

        // field 1 (account_id) = 123456, varint wire type 0
        write_varint_field(&mut buf, 1, 0, 123456);
        // field 2 (rank_old) = 15000, varint wire type 0
        write_varint_field(&mut buf, 2, 0, 15000);
        // field 3 (rank_new) = 15250, varint wire type 0
        write_varint_field(&mut buf, 3, 0, 15250);
        // field 4 (num_wins) = 42, varint wire type 0
        write_varint_field(&mut buf, 4, 0, 42);
        // field 5 (rank_change) = 250.0, fixed32 wire type 5
        let tag = (5 << 3) | 5;
        write_raw_varint(&mut buf, tag);
        buf.extend_from_slice(&250.0f32.to_le_bytes());
        // field 6 (rank_type_id) = 11 (Premier), varint wire type 0
        write_varint_field(&mut buf, 6, 0, 11);

        let entry = parse_rank_update_entry(&buf).unwrap();
        assert_eq!(entry.account_id, 123456);
        assert_eq!(entry.rank_id, 15250);
        assert_eq!(entry.rank_type_id, 11);
        assert_eq!(entry.wins, 42);
        assert!((entry.rank_change - 250.0).abs() < 0.01);
    }

    #[test]
    fn test_parse_rank_update_entry_zero_account_id() {
        // account_id = 0 should return None
        let mut buf = Vec::new();
        write_varint_field(&mut buf, 1, 0, 0);
        write_varint_field(&mut buf, 3, 0, 100);
        assert!(parse_rank_update_entry(&buf).is_none());
    }

    #[test]
    fn test_bit_reader_ubit_var() {
        // Test UBitVar encoding for small values (< 16, top 2 bits = 00)
        // Value 7 = 0b000111, 6 bits
        // 6-bit value: 5 = 0b000101
        // In little-endian bit layout, byte[0] bit0 is first bit read
        // So 6 bits for value 5: bits 0-5 = 000101 → byte = 0b??000101
        let data = [0b00_000101u8];
        let mut reader = BitReader::new(&data);
        assert_eq!(reader.read_ubit_var(), Some(5));
    }

    #[test]
    fn test_bit_reader_read_bits() {
        let data = [0xFF, 0x00, 0xAB];
        let mut reader = BitReader::new(&data);

        // Read 8 bits of 0xFF
        assert_eq!(reader.read_bits(8), Some(0xFF));
        // Read 8 bits of 0x00
        assert_eq!(reader.read_bits(8), Some(0x00));
        // Read 4 bits of 0xAB (lower nibble = 0xB = 11)
        assert_eq!(reader.read_bits(4), Some(0x0B));
        // Read 4 more bits (upper nibble = 0xA = 10)
        assert_eq!(reader.read_bits(4), Some(0x0A));
    }

    #[test]
    fn test_bit_reader_read_bytes_non_aligned() {
        // Read 3 bits to get non-aligned, then read_bytes should still work
        let data = [0b1101_0101_u8, 0xAB, 0xCD, 0xEF];
        let mut reader = BitReader::new(&data);

        // Read 5 bits (value = 0b10101 = 21 from lower 5 bits)
        assert_eq!(reader.read_bits(5), Some(0b10101));
        // Now at bit position 5 — not byte-aligned

        // read_bytes(2) should read 16 bits from bit 5 onward
        let bytes = reader.read_bytes(2).unwrap();
        // Verify by computing expected via manual read_bits
        let mut reader2 = BitReader::new(&data);
        reader2.read_bits(5); // skip 5 bits
        let expected0 = reader2.read_bits(8).unwrap() as u8;
        let expected1 = reader2.read_bits(8).unwrap() as u8;
        assert_eq!(bytes, vec![expected0, expected1]);
    }

    #[test]
    fn test_steam_id64() {
        let update = RankUpdate {
            account_id: 123456,
            rank_id: 15000,
            rank_type_id: 11,
            wins: 42,
            rank_change: 100.0,
        };
        assert_eq!(update.steam_id64(), 76561197960265728 + 123456);
    }

    #[test]
    fn test_proto_cursor_varint() {
        let mut buf = Vec::new();
        write_raw_varint(&mut buf, 300);
        let mut cursor = ProtoCursor::new(&buf);
        assert_eq!(cursor.read_varint32(), Some(300));
        assert!(cursor.is_eof());
    }

    #[test]
    fn test_proto_cursor_length_delimited() {
        // Encode: tag (field 3, wire type 2) + length + payload
        let mut buf = Vec::new();
        let tag = (3 << 3) | 2;
        write_raw_varint(&mut buf, tag);
        write_raw_varint(&mut buf, 4); // length = 4
        buf.extend_from_slice(b"test");

        let result = extract_length_delimited_field(&buf, 3);
        assert_eq!(result, Some(b"test".as_slice()));
    }

    #[test]
    fn test_proto_cursor_skip_and_find() {
        // Field 1 (varint) = 42, then field 3 (length-delimited) = "hello"
        let mut buf = Vec::new();
        write_varint_field(&mut buf, 1, 0, 42);
        let tag = (3 << 3) | 2;
        write_raw_varint(&mut buf, tag);
        write_raw_varint(&mut buf, 5);
        buf.extend_from_slice(b"hello");

        let result = extract_length_delimited_field(&buf, 3);
        assert_eq!(result, Some(b"hello".as_slice()));
    }

    #[test]
    fn test_skip_bits() {
        let data = [0xFF, 0x00, 0xAB];
        let mut reader = BitReader::new(&data);

        assert!(reader.skip_bits(16));
        assert_eq!(reader.bits_remaining(), 8);
        assert_eq!(reader.read_bits(8), Some(0xAB));
    }

    // Test helpers for encoding protobuf
    fn write_raw_varint(buf: &mut Vec<u8>, mut value: u64) {
        loop {
            let byte = (value & 0x7F) as u8;
            value >>= 7;
            if value == 0 {
                buf.push(byte);
                break;
            }
            buf.push(byte | 0x80);
        }
    }

    fn write_varint_field(buf: &mut Vec<u8>, field: u64, wire_type: u64, value: u64) {
        let tag = (field << 3) | wire_type;
        write_raw_varint(buf, tag);
        write_raw_varint(buf, value);
    }
}
