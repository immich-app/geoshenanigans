//! The address files as the builder writes them (builder/src/addr_files.h, which lays the format
//! out), three files: addr_points.bin, the records in blocks of BLOCK_RECORDS, each block with
//! the dictionary of its records' streets and parent ways; addr_vertices.bin, a blob for each
//! record with a footprint; and addr_index.bin, where each block and its records' first blob
//! start, a row for each superblock of SUPERBLOCK_BLOCKS blocks. A record's id is its place in
//! the blocks. A record is read from its block in place, and its footprint's vertices are
//! decoded from its blob. geocoder/testdata/addr_files.tsv holds blobs and files the builder's
//! encoder and this reader must agree on.

use crate::coord_keys::{biased, bit_length, bits_at, from_hybrid_key, from_order_key, is_hybrid_key, order_key, unzigzag16, varint};
use crate::{FileBytes, NodeCoord};

pub const POINTS_FILE: &str = "addr_points.bin";
pub const VERTICES_FILE: &str = "addr_vertices.bin";
pub const INDEX_FILE: &str = "addr_index.bin";

/// The records of a block, and the blocks of a superblock.
pub const BLOCK_RECORDS: usize = 16;
const SUPERBLOCK_BLOCKS: usize = 64;
// A block counts its dictionary's entries, one a record at the most, in a byte.
const _: () = assert!(BLOCK_RECORDS >= 1 && BLOCK_RECORDS <= 255);

const BLOCK_HEAD_BYTES: usize = 19;
const POINTS_FOOTER_BYTES: usize = 8;
const VERTICES_FOOTER_BYTES: usize = 16;
const INDEX_FOOTER_BYTES: usize = 8;
const INDEX_HEAD_BYTES: usize = 16;
const INDEX_ENTRY_BYTES: usize = 6;
const INDEX_ROW_BYTES: usize = INDEX_HEAD_BYTES + SUPERBLOCK_BLOCKS * INDEX_ENTRY_BYTES;
/// The most records the files hold: a patch indexes them in a u32 below NO_DATA.
const MAX_RECORDS: u64 = u32::MAX as u64 - 1;
/// The bytes a logical record's vertex_offset counts by, which every logical blob is a whole
/// number of, and the most bytes of the footprints' logical blobs: as many of them as a u32
/// below NO_DATA counts.
pub const VERTEX_OFFSET_UNIT: u64 = 1;
pub const MAX_LOGICAL_VERTEX_BYTES: u64 = VERTEX_OFFSET_UNIT * (u32::MAX as u64 - 1);

const FLAG_LAT_ORDER: u8 = 1;
const FLAG_LNG_ORDER: u8 = 2;
const FLAG_SPLIT: u8 = 4;
/// A record's fields, in the order a block holds them and its head their widths.
const W_LAT: usize = 0;
const W_LNG: usize = 1;
const W_HOUSENUMBER: usize = 2;
const W_INDEX: usize = 3;
const W_PARENT: usize = 4;
const W_LEN: usize = 5;
const WIDTHS: usize = 6;

/// A logical blob's bytes before its vertices: the encoding, a zero byte and the bbox minimum.
const LOGICAL_HEADER: usize = 10;
/// The most vertices of a footprint whose blob is no escape.
const MAX_BLOB_VERTICES: u64 = 65535;
const ESCAPE_TAG: u8 = 0x80;
const TAG_CLOSED: u8 = 1 << 2;
const TAG_SMALL: u8 = 1 << 3;
const TAG_DELTA: u8 = 1 << 4;
/// The degrees of a vertex's step in encodings 0 to 3.
const SCALE: [f64; 4] = [1e-5, 1e-4, 1e-6, 1e-7];

#[inline(always)]
fn u16_at(b: &[u8], at: usize) -> Option<u16> {
    Some(u16::from_le_bytes(*b.get(at..)?.first_chunk::<2>()?))
}

#[inline(always)]
fn u32_at(b: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_le_bytes(*b.get(at..)?.first_chunk::<4>()?))
}

#[inline(always)]
fn u64_at(b: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_le_bytes(*b.get(at..)?.first_chunk::<8>()?))
}

/// An address as its block holds it: its point, the strings of its house number and its street,
/// the street way it is under (NO_DATA: none), and its footprint's blob, `blob_len` bytes at
/// `blob_at` of addr_vertices.bin. A point has no footprint, and 0 for both.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AddrRecord {
    pub lat: f32,
    pub lng: f32,
    pub housenumber_id: u32,
    pub street_id: u32,
    pub parent_way_id: u32,
    pub blob_at: u64,
    pub blob_len: u32,
}

impl AddrRecord {
    /// Whether the address has a footprint: a building's, not a point's.
    pub fn is_building(&self) -> bool {
        self.blob_len != 0
    }
}

/// A block's head and where its parts are: what its records are read with.
struct Block<'a> {
    lat_order: bool,
    lng_order: bool,
    split: bool,
    entries: usize,
    /// The bits of each of a record's fields, and where each starts in the record; the last
    /// start is a record's bits.
    w: [u32; WIDTHS],
    field_at: [usize; WIDTHS + 1],
    lat_base: u32,
    lng_base: u32,
    housenumber_base: u32,
    parent_base: u32,
    /// The entries: a (street, parent) each in a paired block; a street each in a split one, and
    /// the parents' base after them.
    dictionary: &'a [u8],
    /// From the records' fields to the end of the file: the footer after the blocks holds the 8
    /// bytes a field's load takes past the block.
    fields: &'a [u8],
    /// Where the first blob of the block's records starts in addr_vertices.bin, and where their
    /// last one ends at the latest: where the next block's first starts, or the blobs end.
    blobs_at: u64,
    blobs_end: u64,
}

impl Block<'_> {
    /// Field `k` of record `j`, which is one of the block's.
    #[inline(always)]
    fn field(&self, j: usize, k: usize) -> Option<u32> {
        bits_at(self.fields, j * self.field_at[WIDTHS] + self.field_at[k], self.w[k])
    }

    /// Record `j`'s lat or lng (`axis` W_LAT or W_LNG), as f32 bits; None for a key past 32 bits
    /// and a hybrid key no coordinate has.
    #[inline]
    fn coordinate(&self, j: usize, axis: usize) -> Option<u32> {
        let (base, order) = if axis == W_LAT { (self.lat_base, self.lat_order) } else { (self.lng_base, self.lng_order) };
        let key = base.checked_add(self.field(j, axis)?)?;
        if order {
            return Some(from_order_key(key));
        }
        // A hybrid key is held with its sign bit flipped.
        let key = biased(key);
        is_hybrid_key(key).then(|| from_hybrid_key(key))
    }

    /// Record `j`'s street and parent way; None for an index past the dictionary and a parent
    /// past 32 bits.
    #[inline]
    fn street_and_parent(&self, j: usize) -> Option<(u32, u32)> {
        let entry = self.field(j, W_INDEX)? as usize;
        if entry >= self.entries {
            return None;
        }
        if self.split {
            Some((u32_at(self.dictionary, 4 * entry)?, self.parent_base.checked_add(self.field(j, W_PARENT)?)?))
        } else {
            Some((u32_at(self.dictionary, 8 * entry)?, u32_at(self.dictionary, 8 * entry + 4)?))
        }
    }
}

/// What a blob's bytes before its values say of it.
struct BlobHead {
    /// The footprint's encoding, and whether the blob is its logical blob as it is.
    enc: u8,
    escape: bool,
    closed: bool,
    small: bool,
    delta: bool,
    /// The vertices the blob holds: all of the footprint's but a closed ring's last.
    stored: u32,
    /// Where the bbox minimum starts, and where the values do: an escape's vertices.
    bbox_at: usize,
    values_at: usize,
    /// The bits of a lat value and of a lng value; 0 of an escape.
    wl: u32,
    wg: u32,
}

impl BlobHead {
    /// The footprint's vertices.
    fn vertex_count(&self) -> u32 {
        self.stored + u32::from(self.closed)
    }
}

/// The head of the blob of `record`'s footprint, given all of the blob's bytes; None for a blob
/// no builder writes: a tag or a count none has, an escape of a footprint a blob holds, bytes
/// that are not the ones its count and its widths take or that end in bits past its last value,
/// two residuals at a point that is not finite, a residual of -128.
fn blob_head(blob: &[u8], record: &AddrRecord) -> Option<BlobHead> {
    let tag = *blob.first()?;
    let mut at = 1;
    if tag == ESCAPE_TAG {
        let count = varint(blob, &mut at, 5)?;
        let enc = *blob.get(at)?;
        if count == 0 || enc > 3 || *blob.get(at + 1)? != 0 {
            return None;
        }
        // An escape is of a footprint in encoding 3, or of more vertices than a blob holds.
        if enc != 3 && count <= MAX_BLOB_VERTICES {
            return None;
        }
        let vertex_bytes = if enc == 3 { 8 } else { 4 };
        if blob.len() as u64 != (at + LOGICAL_HEADER) as u64 + count * vertex_bytes {
            return None;
        }
        return Some(BlobHead {
            enc,
            escape: true,
            closed: false,
            small: false,
            delta: false,
            stored: u32::try_from(count).ok()?,
            bbox_at: at + 2,
            values_at: at + LOGICAL_HEADER,
            wl: 0,
            wg: 0,
        });
    }
    if tag & 0xE0 != 0 || tag & 3 == 3 {
        return None;
    }
    let (closed, small) = (tag & TAG_CLOSED != 0, tag & TAG_SMALL != 0);
    let stored = varint(blob, &mut at, 3)?;
    if stored == 0 || stored + u64::from(closed) > MAX_BLOB_VERTICES {
        return None;
    }
    if small {
        // Two residuals are a finite point's, whose predictions then are finite: the mean of a
        // footprint's steps is below 7 degrees. Each is within 127 floats: a byte's -128 is none.
        let finite = record.lat.is_finite() && record.lng.is_finite();
        if !finite || *blob.get(at)? == 0x80 || *blob.get(at + 1)? == 0x80 {
            return None;
        }
    }
    // Two residuals, or the bbox minimum in full, and then the widths' byte.
    let values_at = at + if small { 2 } else { 8 } + 1;
    let widths = *blob.get(values_at - 1)?;
    let bits = |nibble: u8| if nibble == 15 { 16 } else { u32::from(nibble) };
    let (wl, wg) = (bits(widths & 15), bits(widths >> 4));
    // The values, which end in zero bits to the blob's last byte's end.
    let values = stored * u64::from(wl + wg);
    if blob.len() as u64 != values_at as u64 + values.div_ceil(8) {
        return None;
    }
    if values % 8 != 0 && *blob.last()? >> (values % 8) != 0 {
        return None;
    }
    Some(BlobHead { enc: tag & 3, escape: false, closed, small, delta: tag & TAG_DELTA != 0, stored: stored as u32, bbox_at: at, values_at, wl, wg })
}

/// Where one axis of a footprint's bbox minimum is predicted to be, as f32 bits: its record's
/// point less the mean of the axis's steps, whose sum over the footprint's vertices is `sum`.
/// The product comes before the quotient, as in the builder.
fn predicted(point: u32, sum: u64, vertex_count: u32, scale: f64) -> u32 {
    ((f64::from(f32::from_bits(point)) - (sum as f64 * scale) / f64::from(vertex_count)) as f32).to_bits()
}

/// Calls `vertex` with the lat and lng steps of each vertex of a blob that is no escape, in the
/// footprint's order, a closed ring's first again as its last. `values` is the file from the
/// blob's values on. The steps' sums over the vertices, lat and lng; None where a value's load
/// passes the file.
#[inline(always)]
fn for_steps(head: &BlobHead, values: &[u8], mut vertex: impl FnMut(u16, u16)) -> Option<(u64, u64)> {
    // The lat values, then the lng values.
    let (stored, wl, wg) = (head.stored as usize, head.wl as usize, head.wg as usize);
    let lng_at = stored * wl;
    let (mut first, mut step) = ((0u16, 0u16), (0u16, 0u16));
    let (mut sum_lat, mut sum_lng) = (0u64, 0u64);
    for k in 0..stored {
        // A value of 16 bits at the most: a step, or as a delta its step from the one before.
        let (vl, vg) = (bits_at(values, k * wl, head.wl)? as u16, bits_at(values, lng_at + k * wg, head.wg)? as u16);
        step = if head.delta && k > 0 { (step.0.wrapping_add(unzigzag16(vl)), step.1.wrapping_add(unzigzag16(vg))) } else { (vl, vg) };
        if k == 0 {
            first = step;
        }
        sum_lat += u64::from(step.0);
        sum_lng += u64::from(step.1);
        vertex(step.0, step.1);
    }
    if head.closed {
        // A closed ring's first vertex is its last too, and counts twice in the sums.
        sum_lat += u64::from(first.0);
        sum_lng += u64::from(first.1);
        vertex(first.0, first.1);
    }
    Some((sum_lat, sum_lng))
}

/// The bbox minimum of a blob that is no escape, lat and lng as f32 bits: the bits the blob
/// holds in full, or where its two residuals put it from the prediction the record's point and
/// the steps' `sums` give.
#[inline]
fn bbox_minimum(head: &BlobHead, blob: &[u8], record: &AddrRecord, sums: (u64, u64)) -> Option<(u32, u32)> {
    if !head.small {
        return Some((u32_at(blob, head.bbox_at)?, u32_at(blob, head.bbox_at + 4)?));
    }
    let scale = SCALE[usize::from(head.enc)];
    let near = |point: f32, sum: u64, residual: u8| {
        let key = order_key(predicted(point.to_bits(), sum, head.vertex_count(), scale));
        from_order_key(key.wrapping_add_signed((residual as i8).into()))
    };
    Some((near(record.lat, sums.0, *blob.get(head.bbox_at)?), near(record.lng, sums.1, *blob.get(head.bbox_at + 1)?)))
}

/// The three files in place: a record's fields read from its block as they sit, its footprint
/// decoded from its blob.
pub struct AddrFiles {
    points: FileBytes,
    vertices: FileBytes,
    index: FileBytes,
    records: u32,
    /// Where the blocks end and addr_points.bin's footer starts, and the same of the blobs and
    /// addr_vertices.bin's.
    blocks_end: usize,
    blobs_end: usize,
}

/// The count a file's footer holds before the magic it ends in; an error naming the file where
/// its last `len` bytes are no such footer.
fn footer_count(bytes: &[u8], len: usize, magic: &str, file: &str) -> Result<u32, String> {
    bytes
        .len()
        .checked_sub(len)
        .map(|at| &bytes[at..])
        .filter(|footer| footer[len - 4..] == *magic.as_bytes())
        .and_then(|footer| u32_at(footer, len - 8))
        .ok_or_else(|| format!("{file} has no {magic} footer"))
}

impl AddrFiles {
    /// The files of a dir, mapped, each None where the dir has no such file: None of a dir with
    /// none of the three, which has no addresses. An error for files no builder writes: some of
    /// the three without the others, a footer without its magic, more records or bytes of
    /// logical blobs than a patch indexes, an index that counts other records than the blocks'
    /// footer or is not their rows, more blobs than records. What a block or a blob holds is
    /// not checked here: a record or a footprint that can't be read reads as none.
    pub fn open(points: Option<FileBytes>, vertices: Option<FileBytes>, index: Option<FileBytes>) -> Result<Option<AddrFiles>, String> {
        let (points, vertices, index) = match (points, vertices, index) {
            (Some(points), Some(vertices), Some(index)) => (points, vertices, index),
            (None, None, None) => return Ok(None),
            (points, vertices, index) => {
                let files = [(POINTS_FILE, points.is_some()), (VERTICES_FILE, vertices.is_some()), (INDEX_FILE, index.is_some())];
                let named = |there: bool| files.iter().filter(|f| f.1 == there).map(|f| f.0).collect::<Vec<_>>().join(" and ");
                return Err(format!("{} without {}", named(true), named(false)));
            }
        };
        let (p, v, x) = (points.as_slice(), vertices.as_slice(), index.as_slice());
        let records = footer_count(p, POINTS_FOOTER_BYTES, "ADB1", POINTS_FILE)?;
        let blobs = footer_count(v, VERTICES_FOOTER_BYTES, "ADF1", VERTICES_FILE)?;
        let indexed = footer_count(x, INDEX_FOOTER_BYTES, "ADX1", INDEX_FILE)?;
        // The bytes of the logical blobs, which addr_vertices.bin's footer starts with.
        let logical_bytes = u64_at(v, v.len() - VERTICES_FOOTER_BYTES).unwrap_or(0);
        if logical_bytes > MAX_LOGICAL_VERTEX_BYTES {
            return Err(format!("a logical {VERTICES_FILE} of {logical_bytes} bytes is more than the {MAX_LOGICAL_VERTEX_BYTES} a patch can index"));
        }
        if u64::from(records) > MAX_RECORDS {
            return Err(format!("{records} records in {POINTS_FILE} are more than the {MAX_RECORDS} a patch can index"));
        }
        if indexed != records {
            return Err(format!("{POINTS_FILE} and {INDEX_FILE} count different records"));
        }
        // A row's head a superblock and an entry a block, the last row as short as its blocks.
        let blocks = u64::from(records).div_ceil(BLOCK_RECORDS as u64);
        let rows_bytes = blocks.div_ceil(SUPERBLOCK_BLOCKS as u64) * INDEX_HEAD_BYTES as u64 + blocks * INDEX_ENTRY_BYTES as u64;
        if (x.len() - INDEX_FOOTER_BYTES) as u64 != rows_bytes {
            return Err(format!("{INDEX_FILE} is not the rows of its {records} records"));
        }
        if blobs > records {
            return Err(format!("{VERTICES_FILE} counts more blobs than {POINTS_FILE} has records"));
        }
        let (blocks_end, blobs_end) = (p.len() - POINTS_FOOTER_BYTES, v.len() - VERTICES_FOOTER_BYTES);
        Ok(Some(AddrFiles { points, vertices, index, records, blocks_end, blobs_end }))
    }

    /// The records: ids below this.
    pub fn count(&self) -> u32 {
        self.records
    }

    /// Record `id`; None past the last record and for one that can't be read: a block no builder
    /// writes or one that is not the bytes up to the next block's start, a key or an id past 32
    /// bits, an index past the block's dictionary, a blob placed past its block's blobs' end.
    pub fn record(&self, id: u32) -> Option<AddrRecord> {
        Run::new(self).record(id, None)
    }

    /// Calls `f` with each of `ids` that reads (`record`) and its record, in their order. A
    /// block's head is read once for the ids next to each other that are in it, and each of its
    /// records' blob lengths once where the ids ascend, as a cell's list does.
    pub fn for_ids(&self, ids: impl IntoIterator<Item = u32>, mut f: impl FnMut(u32, AddrRecord)) {
        let mut run = Run::new(self);
        for id in ids {
            if let Some(record) = run.record(id, None) {
                f(id, record);
            }
        }
    }

    /// `for_ids` over the records under the parent way `parent_way_id` alone: of a record under
    /// another way, only its street and parent are read.
    pub fn for_ids_on_way(&self, ids: impl IntoIterator<Item = u32>, parent_way_id: u32, mut f: impl FnMut(u32, AddrRecord)) {
        let mut run = Run::new(self);
        for id in ids {
            if let Some(record) = run.record(id, Some(parent_way_id)) {
                f(id, record);
            }
        }
    }

    /// The vertices of a record's footprint, from its blob's first bytes: 0 for a point and for
    /// a blob that can't be read (`footprint`).
    pub fn vertex_count(&self, record: &AddrRecord) -> u32 {
        self.blob(record).and_then(|(blob, _)| blob_head(blob, record)).map_or(0, |head| head.vertex_count())
    }

    /// A record's footprint into `vertices`, which is cleared first: a closed ring's first
    /// vertex is also its last. False, and no vertices, for a point and for a blob that can't be
    /// read: one placed past the blobs' end, a head no builder writes for a footprint at the
    /// record's point, bytes that are not the ones its count and its widths take.
    pub fn footprint(&self, record: &AddrRecord, vertices: &mut Vec<NodeCoord>) -> bool {
        vertices.clear();
        let read = self.read_footprint(record, vertices).is_some();
        if !read {
            vertices.clear();
        }
        read
    }

    /// A record's footprint into `logical`, which is cleared first, as its logical blob, the
    /// bytes it has in the logical addr_vertices.bin the files hold: the encoding, a zero byte,
    /// the bbox minimum's lat and lng (f32) and each vertex's lat and lng steps from it (u16
    /// each, u32 each in encoding 3). False, and no bytes, where `footprint` is false.
    pub fn logical_blob(&self, record: &AddrRecord, logical: &mut Vec<u8>) -> bool {
        logical.clear();
        let read = self.read_logical_blob(record, logical).is_some();
        if !read {
            logical.clear();
        }
        read
    }

    /// Block `b`, one of the blocks; None for one that can't be read: one that is not the bytes
    /// from where the index places it to where it places the next (the blocks' end for the
    /// last), a superblock's first that is not where the superblock starts or the first of all
    /// that does not start the file, a head no builder writes.
    fn block(&self, b: usize) -> Option<Block<'_>> {
        let left = self.records as usize - b * BLOCK_RECORDS;
        let records = BLOCK_RECORDS.min(left);
        // The superblock's row: where its first block and its first blob start, and how far
        // after them this block's do.
        let row = self.index.as_slice().get(b / SUPERBLOCK_BLOCKS * INDEX_ROW_BYTES..)?;
        let entry = INDEX_HEAD_BYTES + b % SUPERBLOCK_BLOCKS * INDEX_ENTRY_BYTES;
        let (row_at, row_blobs_at) = (u64_at(row, 0)?, u64_at(row, 8)?);
        let after_row = u16_at(row, entry)?;
        let at = usize::try_from(row_at.checked_add(after_row.into())?).ok()?;
        let blobs_at = row_blobs_at.checked_add(u32_at(row, entry + 2)?.into())?;
        // The block and its records' blobs end where the next block and its blobs start: at the
        // files' footers for the last block, where the next row says for a superblock's last,
        // else where the row's next entry says.
        let next = entry + INDEX_ENTRY_BYTES;
        let (end, blobs_end) = if left <= BLOCK_RECORDS {
            (self.blocks_end as u64, self.blobs_end as u64)
        } else if next == INDEX_ROW_BYTES {
            (u64_at(row, next)?, u64_at(row, next + 8)?)
        } else {
            (row_at.checked_add(u16_at(row, next)?.into())?, row_blobs_at.checked_add(u32_at(row, next + 2)?.into())?)
        };
        // A superblock's first block starts where its row says the superblock does, and the
        // first superblock where the file does.
        if entry == INDEX_HEAD_BYTES && (after_row != 0 || (b == 0 && row_at != 0)) {
            return None;
        }
        let points = self.points.as_slice();
        let block = points.get(..self.blocks_end)?.get(at..usize::try_from(end).ok()?)?;
        let head = block.first_chunk::<BLOCK_HEAD_BYTES>()?;
        let (flags, entries) = (head[0], usize::from(head[1]));
        if flags & !(FLAG_LAT_ORDER | FLAG_LNG_ORDER | FLAG_SPLIT) != 0 || entries == 0 || entries > records {
            return None;
        }
        // Six widths of 6 bits in 5 bytes, the 4 bits after them 0. A width past 32 bits gives
        // no field.
        let widths = head[2..7].iter().rev().fold(0u64, |all, &byte| all << 8 | u64::from(byte));
        if widths >> (6 * WIDTHS) != 0 {
            return None;
        }
        let (mut w, mut field_at) = ([0u32; WIDTHS], [0usize; WIDTHS + 1]);
        for k in 0..WIDTHS {
            w[k] = (widths >> (6 * k)) as u32 & 63;
            field_at[k + 1] = field_at[k] + w[k] as usize;
        }
        let split = flags & FLAG_SPLIT != 0;
        // An index is the bits the last entry's takes. Only a split block's records have a
        // parent field: a paired block's entries hold theirs.
        if w[W_INDEX] != bit_length(entries as u32 - 1) || (!split && w[W_PARENT] != 0) {
            return None;
        }
        // The block is its head, its dictionary and its records' fields to the byte.
        let fields_at = BLOCK_HEAD_BYTES + if split { 4 * entries + 4 } else { 8 * entries };
        if fields_at + (records * field_at[WIDTHS]).div_ceil(8) != block.len() {
            return None;
        }
        Some(Block {
            lat_order: flags & FLAG_LAT_ORDER != 0,
            lng_order: flags & FLAG_LNG_ORDER != 0,
            split,
            entries,
            w,
            field_at,
            lat_base: u32_at(head, 7)?,
            lng_base: u32_at(head, 11)?,
            housenumber_base: u32_at(head, 15)?,
            parent_base: if split { u32_at(block, BLOCK_HEAD_BYTES + 4 * entries)? } else { 0 },
            dictionary: block.get(BLOCK_HEAD_BYTES..fields_at)?,
            fields: points.get(at + fields_at..)?,
            blobs_at,
            blobs_end: blobs_end.min(self.blobs_end as u64),
        })
    }

    /// The blob of a record's footprint, and the file from the blob's first byte on: the footer
    /// after the blobs holds the 8 bytes a value's load takes past the blob. None for a point
    /// and a blob that ends past the blobs' end.
    fn blob(&self, record: &AddrRecord) -> Option<(&[u8], &[u8])> {
        if record.blob_len == 0 {
            return None;
        }
        let at = usize::try_from(record.blob_at).ok()?;
        let len = usize::try_from(record.blob_len).ok()?;
        if at.checked_add(len)? > self.blobs_end {
            return None;
        }
        let from = self.vertices.as_slice().get(at..)?;
        Some((from.get(..len)?, from))
    }

    /// A record's footprint appended to `out`; None where it has none (`footprint`), with `out`
    /// left as it is or longer.
    fn read_footprint(&self, record: &AddrRecord, out: &mut Vec<NodeCoord>) -> Option<()> {
        let (blob, from) = self.blob(record)?;
        let head = blob_head(blob, record)?;
        let scale = SCALE[usize::from(head.enc)];
        let stored = head.stored as usize;
        // A footprint whose vertices no buffer holds reads as none, not as a failed allocation.
        out.try_reserve(stored + usize::from(head.closed)).ok()?;
        let bmin = |lat: u32, lng: u32| (f64::from(f32::from_bits(lat)), f64::from(f32::from_bits(lng)));
        if head.escape {
            // The logical blob: the bbox minimum, then each vertex's steps from it.
            let (bmin_lat, bmin_lng) = bmin(u32_at(blob, head.bbox_at)?, u32_at(blob, head.bbox_at + 4)?);
            let vertex = |lat: f64, lng: f64| NodeCoord { lat: (bmin_lat + lat * scale) as f32, lng: (bmin_lng + lng * scale) as f32 };
            let steps = blob.get(head.values_at..)?;
            if head.enc == 3 {
                out.extend(steps.chunks_exact(8).map(|q| {
                    vertex(u32::from_le_bytes([q[0], q[1], q[2], q[3]]).into(), u32::from_le_bytes([q[4], q[5], q[6], q[7]]).into())
                }));
            } else {
                out.extend(steps.chunks_exact(4).map(|q| vertex(u16::from_le_bytes([q[0], q[1]]).into(), u16::from_le_bytes([q[2], q[3]]).into())));
            }
            return Some(());
        }
        // Each vertex's steps are kept in its place until the bbox minimum they are counted from
        // is known: of two residuals, it is their sums'.
        let from_here = out.len();
        let sums = for_steps(&head, from.get(head.values_at..)?, |lat, lng| out.push(NodeCoord { lat: lat.into(), lng: lng.into() }))?;
        let (bmin_lat, bmin_lng) = bbox_minimum(&head, blob, record, sums)?;
        let (bmin_lat, bmin_lng) = bmin(bmin_lat, bmin_lng);
        for vertex in &mut out[from_here..] {
            vertex.lat = (bmin_lat + f64::from(vertex.lat) * scale) as f32;
            vertex.lng = (bmin_lng + f64::from(vertex.lng) * scale) as f32;
        }
        Some(())
    }

    /// A record's footprint appended to `out` as its logical blob (`logical_blob`); None where
    /// it has none, with `out` left as it is or longer.
    fn read_logical_blob(&self, record: &AddrRecord, out: &mut Vec<u8>) -> Option<()> {
        let (blob, from) = self.blob(record)?;
        let head = blob_head(blob, record)?;
        if head.escape {
            // The logical blob as it is, after the escape's tag and count.
            let logical = blob.get(head.values_at - LOGICAL_HEADER..)?;
            out.try_reserve(logical.len()).ok()?;
            out.extend_from_slice(logical);
            return Some(());
        }
        out.try_reserve(LOGICAL_HEADER + 4 * head.vertex_count() as usize).ok()?;
        // The bbox minimum is known after the steps: of two residuals, it is their sums'.
        let header_at = out.len();
        out.extend_from_slice(&[head.enc, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        let sums = for_steps(&head, from.get(head.values_at..)?, |lat, lng| {
            out.extend_from_slice(&lat.to_le_bytes());
            out.extend_from_slice(&lng.to_le_bytes());
        })?;
        let (bmin_lat, bmin_lng) = bbox_minimum(&head, blob, record, sums)?;
        out[header_at + 2..header_at + 6].copy_from_slice(&bmin_lat.to_le_bytes());
        out[header_at + 6..header_at + LOGICAL_HEADER].copy_from_slice(&bmin_lng.to_le_bytes());
        Some(())
    }
}

/// Where a run of reads is: the block of the id before, its head read once, and how far into it
/// the records' blobs are summed.
struct Run<'a> {
    files: &'a AddrFiles,
    /// Which block is at hand, and the block: None where it can't be read.
    at: usize,
    block: Option<Block<'a>>,
    /// The block's first records whose blobs' bytes are summed, and the sum.
    summed: usize,
    blob_bytes: u64,
}

impl<'a> Run<'a> {
    fn new(files: &'a AddrFiles) -> Run<'a> {
        Run { files, at: usize::MAX, block: None, summed: 0, blob_bytes: 0 }
    }

    /// Record `id` (`AddrFiles::record`). With `on_way`, None also for a record under another
    /// parent way, of which no more than its street and parent is read.
    fn record(&mut self, id: u32, on_way: Option<u32>) -> Option<AddrRecord> {
        if id >= self.files.records {
            return None;
        }
        let (b, j) = (id as usize / BLOCK_RECORDS, id as usize % BLOCK_RECORDS);
        if b != self.at {
            (self.at, self.block, self.summed, self.blob_bytes) = (b, self.files.block(b), 0, 0);
        }
        let block = self.block.as_ref()?;
        let (street_id, parent_way_id) = block.street_and_parent(j)?;
        if on_way.is_some_and(|way| way != parent_way_id) {
            return None;
        }
        let (lat, lng) = (block.coordinate(j, W_LAT)?, block.coordinate(j, W_LNG)?);
        let housenumber_id = block.housenumber_base.checked_add(block.field(j, W_HOUSENUMBER)?)?;
        let blob_len = block.field(j, W_LEN)?;
        let mut blob_at = 0;
        if blob_len != 0 {
            // The blob starts where the block's first does plus the blobs of the records before
            // this one: summed on from the record a run's last sum stopped at, where that is no
            // later one.
            if j < self.summed {
                (self.summed, self.blob_bytes) = (0, 0);
            }
            while self.summed < j {
                self.blob_bytes += u64::from(block.field(self.summed, W_LEN)?);
                self.summed += 1;
            }
            blob_at = block.blobs_at.checked_add(self.blob_bytes)?;
            if blob_at.checked_add(blob_len.into())? > block.blobs_end {
                return None;
            }
        }
        Some(AddrRecord { lat: f32::from_bits(lat), lng: f32::from_bits(lng), housenumber_id, street_id, parent_way_id, blob_at, blob_len })
    }
}

/// The vertex_offset of a footprint with `bytes_before` bytes of logical blobs before it: none
/// of bytes that are not whole units, or are more than the logical blobs may be.
pub fn vertex_offset_of(bytes_before: u64) -> Option<u32> {
    if bytes_before % VERTEX_OFFSET_UNIT != 0 || bytes_before > MAX_LOGICAL_VERTEX_BYTES {
        return None;
    }
    u32::try_from(bytes_before / VERTEX_OFFSET_UNIT).ok()
}

/// The three files (fixtures and tests).
#[cfg(any(test, feature = "fixtures"))]
pub struct AddrFileBytes {
    pub points: Vec<u8>,
    pub vertices: Vec<u8>,
    pub index: Vec<u8>,
}

/// The files of a logical addr_points.bin and addr_vertices.bin, as the builder's encoder writes
/// them (fixtures and tests). A logical record is seven u32: lat and lng (f32 bits),
/// housenumber_id, street_id, parent_way_id, vertex_offset and vertex_count; a point's
/// vertex_offset is NO_DATA and its vertex_count 0. A footprint's logical blob sits at its
/// record's vertex_offset, the bytes of the footprints before it: the encoding (0 to 3), a zero
/// byte, the bbox minimum's lat and lng (f32) and vertex_count vertices, each its lat and lng
/// steps from the bbox minimum (u16 each, u32 each in encoding 3). Panics on a pair the files
/// can't hold.
#[cfg(any(test, feature = "fixtures"))]
pub fn encode(points: &[u8], vertices: &[u8]) -> AddrFileBytes {
    const STRIDE: usize = 28;
    assert_eq!(points.len() % STRIDE, 0, "a logical record is {STRIDE} bytes");
    let records = points.len() / STRIDE;
    assert!(records < u32::MAX as usize, "fewer than 2^32 - 1 records");
    let field = |i: usize, k: usize| u32::from_le_bytes(points[i * STRIDE + 4 * k..][..4].try_into().unwrap());
    let mut out = AddrFileBytes { points: Vec::new(), vertices: Vec::new(), index: Vec::new() };
    // The bytes of the logical blobs so far, and the blobs.
    let (mut logical, mut blobs) = (0usize, 0u32);
    let (mut row_blocks_at, mut row_blobs_at) = (0, 0);
    for b in 0..records.div_ceil(BLOCK_RECORDS) {
        let ids = b * BLOCK_RECORDS..records.min((b + 1) * BLOCK_RECORDS);
        if b % SUPERBLOCK_BLOCKS == 0 {
            (row_blocks_at, row_blobs_at) = (out.points.len(), out.vertices.len());
            out.index.extend((row_blocks_at as u64).to_le_bytes());
            out.index.extend((row_blobs_at as u64).to_le_bytes());
        }
        out.index.extend(u16::try_from(out.points.len() - row_blocks_at).expect("a superblock's blocks in 64 KiB").to_le_bytes());
        out.index.extend(u32::try_from(out.vertices.len() - row_blobs_at).expect("a superblock's blobs below 4 GiB").to_le_bytes());
        let lens: Vec<u32> = ids
            .clone()
            .map(|i| {
                let (offset, count) = (field(i, 5), field(i, 6) as usize);
                if count == 0 {
                    assert_eq!(offset, crate::NO_DATA, "record {i} is a point with a vertex_offset");
                    return 0;
                }
                assert_eq!(Some(offset), vertex_offset_of(logical as u64), "record {i}'s vertex_offset is the bytes of the footprints before it");
                assert!(logical < vertices.len() && vertices[logical] <= 3, "record {i}'s footprint is in an encoding of 0 to 3");
                let bytes = LOGICAL_HEADER + count * if vertices[logical] == 3 { 8 } else { 4 };
                assert!(bytes <= vertices.len() - logical, "record {i}'s footprint ends in the logical addr_vertices.bin");
                let blob = encode_blob(&vertices[logical..logical + bytes], field(i, 0), field(i, 1));
                logical += bytes;
                blobs += 1;
                out.vertices.extend(&blob);
                u32::try_from(blob.len()).expect("a blob below 4 GiB")
            })
            .collect();
        let column = |k: usize| ids.clone().map(|i| field(i, k)).collect::<Vec<u32>>();
        encode_block([&column(0), &column(1), &column(2), &column(3), &column(4)], &lens, &mut out.points);
    }
    assert_eq!(logical, vertices.len(), "the logical addr_vertices.bin is its records' footprints");
    out.points.extend((records as u32).to_le_bytes());
    out.points.extend(b"ADB1");
    out.vertices.extend((logical as u64).to_le_bytes());
    out.vertices.extend(blobs.to_le_bytes());
    out.vertices.extend(b"ADF1");
    out.index.extend((records as u32).to_le_bytes());
    out.index.extend(b"ADX1");
    out
}

/// The block of the records whose lats, lngs (f32 bits), housenumbers, streets and parents are
/// `columns` and whose blobs are `len` bytes, appended to `out`.
#[cfg(any(test, feature = "fixtures"))]
fn encode_block(columns: [&[u32]; 5], len: &[u32], out: &mut Vec<u8>) {
    use crate::coord_keys::{bit_length, hybrid_key, BitWriter};
    let [lat, lng, housenumber, street, parent] = columns;
    let n = len.len();
    // An axis's keys, and whether they are order keys: each coordinate's hybrid key with its
    // sign bit flipped where every one has a hybrid key, else each one's order key.
    let keys = |bits: &[u32]| match bits.iter().map(|&b| hybrid_key(b).map(biased)).collect::<Option<Vec<u32>>>() {
        Some(keys) => (keys, false),
        None => (bits.iter().map(|&b| order_key(b)).collect(), true),
    };
    // The least of the values, and the bits of the largest less it.
    let spread = |values: &[u32]| {
        let least = *values.iter().min().unwrap();
        (least, bit_length(values.iter().max().unwrap() - least))
    };
    // A value's place among the ones before it, the last place where it is new.
    fn place<T: PartialEq>(known: &mut Vec<T>, value: T) -> u32 {
        let at = known.iter().position(|k| *k == value).unwrap_or(known.len());
        if at == known.len() {
            known.push(value);
        }
        at as u32
    }
    let ((lat, lat_order), (lng, lng_order)) = (keys(lat), keys(lng));
    let ((lat_base, lat_bits), (lng_base, lng_bits)) = (spread(&lat), spread(&lng));
    let (housenumber_base, housenumber_bits) = spread(housenumber);
    let (parent_base, parent_bits) = spread(parent);
    // The distinct (street, parent) and the distinct streets, in the order the records first
    // have them, and each record's entry among either.
    let (mut pairs, mut streets) = (Vec::new(), Vec::new());
    let pair_of: Vec<u32> = (0..n).map(|j| place(&mut pairs, (street[j], parent[j]))).collect();
    let street_of: Vec<u32> = (0..n).map(|j| place(&mut streets, street[j])).collect();
    let (pair_bits, street_bits) = (bit_length(pairs.len() as u32 - 1), bit_length(streets.len() as u32 - 1));
    // Split exactly when that takes fewer bits than pairs: the streets, the parents' base and
    // an index and a parent a record, against the pairs and an index a record.
    let split_bits = 32 * streets.len() + 32 + n * (street_bits + parent_bits) as usize;
    let split = split_bits < 64 * pairs.len() + n * pair_bits as usize;
    let (entries, index_bits, parent_bits) = if split { (streets.len(), street_bits, parent_bits) } else { (pairs.len(), pair_bits, 0) };
    let w = [lat_bits, lng_bits, housenumber_bits, index_bits, parent_bits, bit_length(*len.iter().max().unwrap())];

    out.push(u8::from(lat_order) * FLAG_LAT_ORDER | u8::from(lng_order) * FLAG_LNG_ORDER | u8::from(split) * FLAG_SPLIT);
    out.push(entries as u8);
    let widths = w.iter().rev().fold(0u64, |all, &bits| all << 6 | u64::from(bits));
    out.extend(&widths.to_le_bytes()[..5]);
    for base in [lat_base, lng_base, housenumber_base] {
        out.extend(base.to_le_bytes());
    }
    if split {
        streets.iter().for_each(|street| out.extend(street.to_le_bytes()));
        out.extend(parent_base.to_le_bytes());
    } else {
        for (street, parent) in &pairs {
            out.extend(street.to_le_bytes());
            out.extend(parent.to_le_bytes());
        }
    }
    let mut fields = BitWriter::default();
    for j in 0..n {
        fields.put(lat[j] - lat_base, w[W_LAT]);
        fields.put(lng[j] - lng_base, w[W_LNG]);
        fields.put(housenumber[j] - housenumber_base, w[W_HOUSENUMBER]);
        fields.put(if split { street_of[j] } else { pair_of[j] }, w[W_INDEX]);
        fields.put(if split { parent[j] - parent_base } else { 0 }, w[W_PARENT]);
        fields.put(len[j], w[W_LEN]);
    }
    out.extend(fields.finish());
}

/// The blob of a footprint, given its logical blob and its record's lat and lng (f32 bits), as
/// the builder's encoder writes it. Panics on a logical blob no footprint has.
#[cfg(any(test, feature = "fixtures"))]
fn encode_blob(logical: &[u8], lat: u32, lng: u32) -> Vec<u8> {
    use crate::coord_keys::{bit_length, put_varint, zigzag16, BitWriter};
    const MAX_RESIDUAL: i64 = 127;
    let enc = logical[0];
    let vertex_bytes = if enc == 3 { 8 } else { 4 };
    let n = (logical.len() - LOGICAL_HEADER) / vertex_bytes;
    assert!(enc <= 3 && logical[1] == 0, "a footprint in an encoding of 0 to 3, the byte after it 0");
    assert!(n >= 1 && logical.len() == LOGICAL_HEADER + n * vertex_bytes, "a footprint of whole vertices, one at the least");
    let count = u32::try_from(n).expect("a footprint of fewer than 2^32 vertices");
    let mut out = Vec::new();
    if enc == 3 || u64::from(count) > MAX_BLOB_VERTICES {
        out.push(ESCAPE_TAG);
        put_varint(&mut out, count.into());
        out.extend(logical);
        return out;
    }
    let q: Vec<(u16, u16)> =
        logical[LOGICAL_HEADER..].chunks_exact(4).map(|v| (u16::from_le_bytes([v[0], v[1]]), u16::from_le_bytes([v[2], v[3]]))).collect();
    // A ring that ends where it starts is held without its last vertex.
    let closed = n >= 2 && q[n - 1] == q[0];
    let plain = &q[..n - usize::from(closed)];
    // The same vertices as deltas: the first as it is, each later one its step from the one before.
    let step = |k: usize| (zigzag16(plain[k].0.wrapping_sub(plain[k - 1].0)), zigzag16(plain[k].1.wrapping_sub(plain[k - 1].1)));
    let deltas: Vec<(u16, u16)> = (0..plain.len()).map(|k| if k == 0 { plain[0] } else { step(k) }).collect();
    // The bits of each axis's values: those of all of them ORed together, 16 where that is 15.
    let widths = |values: &[(u16, u16)]| {
        let any = values.iter().fold((0, 0), |any, v| (any.0 | v.0, any.1 | v.1));
        [any.0, any.1].map(|any| if bit_length(any.into()) == 15 { 16 } else { bit_length(any.into()) })
    };
    let ([pl, pg], [dl, dg]) = (widths(plain), widths(&deltas));
    // Deltas exactly when they make the two widths' sum smaller.
    let delta = dl + dg < pl + pg;
    let (values, wl, wg) = if delta { (&deltas[..], dl, dg) } else { (plain, pl, pg) };
    // Small exactly when the point and both predictions are finite and the bbox minimum is
    // within MAX_RESIDUAL floats of each prediction.
    let (sum_lat, sum_lng) = q.iter().fold((0u64, 0u64), |sum, v| (sum.0 + u64::from(v.0), sum.1 + u64::from(v.1)));
    let (bmin_lat, bmin_lng) = (u32_at(logical, 2).unwrap(), u32_at(logical, 6).unwrap());
    let scale = SCALE[usize::from(enc)];
    let (predicted_lat, predicted_lng) = (predicted(lat, sum_lat, count, scale), predicted(lng, sum_lng, count, scale));
    let residual = |bmin: u32, predicted: u32| i64::from(order_key(bmin)) - i64::from(order_key(predicted));
    let (rl, rg) = (residual(bmin_lat, predicted_lat), residual(bmin_lng, predicted_lng));
    let finite = [lat, lng, predicted_lat, predicted_lng].iter().all(|&bits| f32::from_bits(bits).is_finite());
    let small = finite && rl.abs() <= MAX_RESIDUAL && rg.abs() <= MAX_RESIDUAL;

    out.push(enc | u8::from(closed) * TAG_CLOSED | u8::from(small) * TAG_SMALL | u8::from(delta) * TAG_DELTA);
    put_varint(&mut out, plain.len() as u64);
    if small {
        out.extend([rl as i8 as u8, rg as i8 as u8]);
    } else {
        out.extend(bmin_lat.to_le_bytes());
        out.extend(bmin_lng.to_le_bytes());
    }
    let nibble = |bits: u32| if bits == 16 { 15 } else { bits as u8 };
    out.push(nibble(wl) | nibble(wg) << 4);
    let mut packed = BitWriter::default();
    values.iter().for_each(|v| packed.put(v.0.into(), wl));
    values.iter().for_each(|v| packed.put(v.1.into(), wg));
    out.extend(packed.finish());
    out
}

#[cfg(test)]
mod tests;
