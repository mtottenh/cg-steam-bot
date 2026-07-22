# Design: Tail-Only Demo Download for Rank Extraction

## Motivation

CS2 rank updates (`CCSUsrMsg_ServerRankUpdate`) appear in the final frames of a demo file, typically in the last few hundred KB of decompressed data. Currently, the enricher downloads the entire compressed demo (50-200MB), decompresses it to 300-700MB, and parses the full file — all to extract ~1KB of rank data (10 `RankUpdateEntry` messages).

A tail-download approach would:
- **Save bandwidth**: Download ~2-5MB instead of ~100-200MB (~95-98% reduction)
- **Save time**: Decompress 2-3 bzip2 blocks instead of ~250 (~99% reduction)
- **Save memory**: Peak memory drops from ~500MB to ~10-20MB
- **Save cost**: Reduced egress from Valve CDN (matters at scale)

## Background: File Format Layering

The demo files use three nested layers:

```
.dem.bz2 file
  └── bzip2 stream (independently decompressible ~900KB blocks)
       └── Source 2 .dem file
            ├── 16-byte header (PBDEMS2\0 + two i32 offsets)
            └── Sequence of frames:
                 ├── varint cmd_tag (bit 6 = compressed)
                 ├── varint tick
                 ├── varint size
                 └── frame_data[size] (Snappy-compressed if bit 6 set)
                      └── CDemoPacket / CDemoFullPacket
                           └── Inner net messages (bit-oriented):
                                ├── UBitVar msg_type
                                ├── varint msg_size
                                └── msg_payload[msg_size]
```

**Key insight**: bzip2 blocks are independently decompressible. If we download only the last N bytes of the `.bz2` file, we can decompress the bzip2 blocks that fall within that range and get the corresponding tail of the `.dem` file.

## Approach Overview

```
1. HTTP HEAD → get Content-Length
2. HTTP GET with Range: bytes=-N → last N bytes of .bz2
3. Scan for bzip2 block boundaries in suffix
4. Decompress only the tail blocks
5. Scan decompressed data for demo frame boundaries
6. Parse frames looking for rank update messages
7. If not found → fall back to full download
```

## Prerequisites to Verify

Before investing in implementation, these questions must be answered:

### 1. Does the Valve CDN support HTTP Range requests?

Demo URLs look like: `http://replay{N}.valve.net/730/{match_id}_{token}.dem.bz2`

**Test procedure:**
```bash
# Get a real demo URL from an enriched match
DEMO_URL="http://replay123.valve.net/730/003802311790264582805_1309061287.dem.bz2"

# Test HEAD support
curl -I "$DEMO_URL"
# Look for: Accept-Ranges: bytes, Content-Length: NNNNN

# Test Range request (last 5MB)
curl -r -5242880 -o tail.bz2 "$DEMO_URL"
# Verify: HTTP 206 Partial Content, correct size
```

If the CDN returns `Accept-Ranges: none` or ignores the `Range` header, this entire optimization is not viable. **This is the first thing to test.**

### 2. What is the typical bzip2 block size for CS2 demos?

bzip2 blocks are identified by the magic number `0x314159265359` (the digits of pi) at bit-aligned positions. Block size is set at compression time (100KB-900KB compressed, decompressing to 100KB-900KB * compression ratio).

**Test procedure:**
```bash
# Use parallel_bzip2_decoder to count blocks and measure sizes
# (Write a small Rust script that calls scan_blocks on a demo)
cargo run --example measure_blocks -- path/to/demo.dem.bz2
```

This tells us how many bytes of the compressed file we need to download to get the last N MB of decompressed data.

### 3. How far from the end of the demo do rank updates appear?

**Test procedure:**
```bash
# Modify extract_rank_updates to log byte offsets of rank frames
# Run against sample demos and record:
# - Total demo size
# - Byte offset of the rank update frame
# - Distance from EOF
```

If rank updates consistently appear in the last 1MB of a 500MB demo, we only need to download the bzip2 blocks covering that last 1MB.

## Technical Design

### Phase 1: Suffix Decompression

The `parallel_bzip2_decoder::scan_blocks` function scans for bzip2 block magic (`0x314159265359`) at every bit position. It returns `(start_bit, end_bit)` pairs.

For a suffix download:
1. Download the last N bytes of the `.bz2` file
2. The first bzip2 block in our suffix may be truncated (its start is before our range). Skip it.
3. Decompress all complete blocks that start within our suffix.

```rust
fn decompress_suffix(suffix: &[u8], suffix_offset: u64) -> Vec<u8> {
    let blocks: Vec<(u64, u64)> = scan_blocks(suffix).into_iter().collect();

    // Skip the first block if it might be truncated
    // (its magic could be a false positive in a partial block)
    let blocks = if blocks.first().map_or(false, |&(start, _)| start < 48) {
        &blocks[1..]  // skip potentially incomplete first block
    } else {
        &blocks[..]
    };

    // Decompress remaining blocks
    let parts: Vec<Vec<u8>> = blocks.par_iter()
        .map(|&(start, end)| decompress_block(suffix, start, end))
        .collect::<Result<Vec<_>, _>>()
        .unwrap_or_default();

    let total: usize = parts.iter().map(|p| p.len()).sum();
    let mut result = Vec::with_capacity(total);
    for part in parts {
        result.extend_from_slice(&part);
    }
    result
}
```

### Phase 2: Demo Frame Boundary Scanner

The decompressed suffix is a raw byte stream from the middle of a `.dem` file. We need to find valid frame boundaries.

A demo frame has the structure: `varint(cmd) | varint(tick) | varint(size) | data[size]`. To find a valid frame boundary in arbitrary bytes:

```rust
fn find_frame_boundary(data: &[u8]) -> Option<usize> {
    // Try each byte offset as a potential frame start
    for start in 0..data.len().min(4096) {
        if try_parse_frames_from(data, start, 3) {
            return Some(start);
        }
    }
    None
}

fn try_parse_frames_from(data: &[u8], offset: usize, min_valid_frames: usize) -> bool {
    let mut pos = offset;
    let mut valid_count = 0;

    for _ in 0..min_valid_frames {
        // Try reading cmd varint
        let (cmd_raw, n) = match read_varint_at(data, pos) {
            Some(v) => v,
            None => return false,
        };
        pos += n;

        let cmd = (cmd_raw & !64) as u32;
        // Validate cmd is a known demo command type (0-15 range)
        if cmd > 15 { return false; }

        // Skip tick varint
        let (_, n) = match read_varint_at(data, pos) {
            Some(v) => v,
            None => return false,
        };
        pos += n;

        // Read size varint
        let (size, n) = match read_varint_at(data, pos) {
            Some(v) => v,
            None => return false,
        };
        pos += n;

        let size = size as usize;
        // Sanity check: frame size should be reasonable (< 1MB)
        if size > 1_000_000 { return false; }

        if pos + size > data.len() { return false; }
        pos += size;

        valid_count += 1;
    }

    valid_count >= min_valid_frames
}
```

The heuristic: try offsets 0..4096 and accept the first one where we can successfully parse 3 consecutive frames. This is robust because:
- Valid varints + valid cmd values + correct frame sizes spanning multiple consecutive frames is extremely unlikely to occur by chance in arbitrary data
- 3 consecutive valid frames is a strong signal

### Phase 3: New Parser Entry Point

```rust
/// Extract rank updates from a demo suffix (no header required).
///
/// Scans for valid frame boundaries, then parses frames looking for
/// rank update messages. Returns Ok(vec![]) if no frames or ranks found.
pub fn extract_rank_updates_from_suffix(
    suffix_bytes: &[u8],
) -> Result<Vec<RankUpdate>, DemoRankError> {
    let start = match find_frame_boundary(suffix_bytes) {
        Some(s) => s,
        None => return Ok(vec![]),
    };

    // Parse frames from the discovered boundary
    // (reuse the existing frame parsing loop from extract_rank_updates,
    //  but skip the header check)
    parse_frames(&suffix_bytes[start..])
}
```

### Phase 4: Enricher Integration

```rust
async fn download_and_extract_ranks(
    http: &Client,
    demo_url: &str,
) -> Result<Vec<RankUpdate>, Box<dyn std::error::Error>> {
    // Step 1: Try tail-only download
    let head = http.head(demo_url)
        .timeout(Duration::from_secs(10))
        .send().await?;

    let supports_range = head.headers()
        .get("accept-ranges")
        .map_or(false, |v| v == "bytes");

    let content_length = head.headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());

    if supports_range {
        if let Some(total_size) = content_length {
            let tail_size = 5 * 1024 * 1024; // 5MB
            let range_start = total_size.saturating_sub(tail_size);

            let suffix = http.get(demo_url)
                .header("Range", format!("bytes={range_start}-"))
                .timeout(Duration::from_secs(30))
                .send().await?
                .bytes().await?;

            let decompressed = decompress_suffix(&suffix, range_start);
            let ranks = cs2_demo_rank::extract_rank_updates_from_suffix(&decompressed)?;

            if !ranks.is_empty() {
                info!(rank_count = ranks.len(), "Rank extraction from tail succeeded");
                return Ok(ranks);
            }

            info!("No ranks in tail, falling back to full download");
        }
    }

    // Fallback: full download (existing logic)
    download_and_extract_ranks_full(http, demo_url).await
}
```

## Edge Cases

1. **Rank message spans bzip2 block boundary**: The rank update message is tiny (~200 bytes) and could theoretically straddle two bzip2 blocks. If we miss the first half, the frame parser won't find it. Mitigation: download enough suffix to get several blocks past the expected rank position.

2. **Demo has no rank updates**: Casual/deathmatch demos don't have rank updates. The suffix parse returns empty, we fall back to full download which also returns empty. Wasteful but correct. Could add a check: if the match is known to be unranked (from GC data), skip demo download entirely.

3. **CDN doesn't support Range**: Fall back to full download. No behavior change.

4. **False positive frame boundary**: The heuristic could find a byte sequence that looks like 3 valid frames but isn't. The rank parser would then either find no ranks (correct) or find garbage that looks like a rank update (extremely unlikely — would need valid protobuf structure inside a valid net message inside a valid Snappy-compressed frame). Mitigation: validate parsed rank data (e.g., account_id should be non-zero, rank_type_id should be 6/7/11).

5. **Compressed block straddles the range boundary**: The first bzip2 block in our suffix is likely truncated. `scan_blocks` will still find its magic number but `decompress_block` will fail. Solution: catch the error and skip to the next block.

## Implementation Order

1. **Verify CDN support** (30 min): Manual `curl` tests against real demo URLs to confirm Range request support. If no → stop here.

2. **Measure rank position** (1 hour): Add byte offset logging to the parser, run against 10+ sample demos. Determine how far from EOF ranks typically appear.

3. **Measure bzip2 block sizes** (30 min): Use `scan_blocks` on sample demos to determine typical block sizes and how much compressed data covers the last N MB of decompressed data.

4. **Implement suffix decompression** (2 hours): `decompress_suffix()` function with error handling for truncated first block.

5. **Implement frame boundary scanner** (2 hours): `find_frame_boundary()` with the consecutive-valid-frames heuristic.

6. **Add `extract_rank_updates_from_suffix`** to cs2-demo-rank (1 hour): New public API entry point.

7. **Integrate into enricher** (1 hour): HEAD → Range → suffix parse → fallback path.

8. **Test with real demos** (2 hours): Validate against 20+ demos, compare results with full-download path.

## Success Criteria

- Rank extraction produces identical results to full-download for 100% of test demos
- Tail download size is <10MB for typical demos
- End-to-end latency for rank extraction drops from ~30s to <5s
- Graceful fallback to full download when tail-only fails
