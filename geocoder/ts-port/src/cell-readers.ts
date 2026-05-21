// Cell-index readers — port of for_each_entry + lookup_admin_cell from
// geocoder/server/src/main.rs.
//
// The on-disk format (built by builder/src/cell_index.cpp):
//
//   admin_cells.bin: a sorted array of (cell_id u64, entries_offset u32, _pad u32)
//     20 bytes per record. Binary-searched by cell_id to find the entries offset.
//
//   admin_entries.bin: at the offset returned above, layout is
//     u16 count, then `count` u32 polygon IDs (high bit = INTERIOR_FLAG).
//
//   geo_cells.bin (street/addr/interp combined): same shape, returns
//     three offsets (street/addr/interp) per cell. Not used in the
//     admin-only port but the binary search machinery is shared.

import { ByteSource } from "./byte-source.js";

// admin_cells.bin stride: u64 cell_id + u32 entries_offset, no padding.
// Matches builder/src/cell_index.cpp:174-180.
const ADMIN_CELL_RECORD_SIZE = 12;

// Binary search admin_cells.bin for a u64 cell_id. Returns the entries
// offset (uint32) or NO_DATA if not found. We split the u64 into hi/lo
// uint32s for the comparison since JS numbers don't represent u64
// precisely above 2^53.
export function lookupAdminCell(cellsBuf: Buffer, cellId: bigint): number {
  const recordCount = Math.floor(cellsBuf.length / ADMIN_CELL_RECORD_SIZE);
  let lo = 0;
  let hi = recordCount;
  // Compare via BigInt — slower than uint32 split but trivial code.  Hot
  // path could split if profiling demands.
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    const off = mid * ADMIN_CELL_RECORD_SIZE;
    const midId = cellsBuf.readBigUInt64LE(off);
    if (midId < cellId) lo = mid + 1;
    else if (midId > cellId) hi = mid;
    else {
      return cellsBuf.readUInt32LE(off + 8);
    }
  }
  return 0xFFFFFFFF;
}

// Iterate IDs in an entries blob starting at the given offset.
// Mirrors Rust's for_each_entry: u16 count, then count u32 IDs.
// Caller's callback receives the raw u32 (high bit may be INTERIOR_FLAG).
//
// Accepts either Buffer or ByteSource so it works on chunked sources
// (planet street_entries / addr_entries / interp_entries are >600 MiB
// each — they'd be loaded as ChunkedFile via index-loader).  For
// ByteSource we batch-read the entries into a single buffer copy first
// so we don't pay a syscall per ID.
export function forEachEntry(
  entriesBuf: ByteSource,
  offset: number,
  cb: (id: number) => void,
): void {
  if (offset === 0xFFFFFFFF) return;
  if (offset + 2 > entriesBuf.length) return;
  const count = entriesBuf.readUInt16LE(offset);
  if (count === 0) return;
  // Batch read: pull the full ID list once.  ChunkedFile.subarray()
  // returns a contiguous Buffer; Buffer.subarray() is zero-copy.
  const blob = entriesBuf.subarray(offset + 2, offset + 2 + count * 4);
  if (blob.length < count * 4) return;
  for (let i = 0; i < count; i++) {
    cb(blob.readUInt32LE(i * 4));
  }
}
