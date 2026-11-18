//! The postcode centroid files as the builder writes them (builder/src/postcode_files.h, which
//! lays the format out): postcode_centroids.bin, the centroids in cell order, Rice-coded steps
//! in blocks of BLOCK_ROWS rows, a table of the blocks with each one's first row and box, and a
//! footer; and postcode_centroid_cells.bin, a ranges index of each admin cell's rows. A row is
//! read from its block's first; a query's cells give their blocks, nearest box first.
//!
//! The reader checks the footer, the table's size and the cells' ranges at open, and reads a
//! block without trusting it: a malformed block reads as no rows or wrong ones, never a panic.

use crate::cell_pairs::CellPairs;
use crate::coord_keys::{bits_at, dec4_round, from_dec4_key, from_order_key, get_rice, order_key, unzigzag32};
use crate::FileBytes;

pub const CENTROIDS_FILE: &str = "postcode_centroids.bin";
pub const CELLS_FILE: &str = "postcode_centroid_cells.bin";
const MAGIC: &[u8; 4] = b"PCC1";
pub const BLOCK_ROWS: usize = 32;
const ENTRY_BYTES: usize = 29;
const FOOTER_BYTES: usize = 8;
/// A box's units, 2^-14 degrees; ±BOX_OPEN is an open side.
pub const BOX_UNIT: f64 = 1.0 / 16384.0;
const BOX_OPEN: i16 = 32767;
const FLAG_MIXED: u32 = 1;
const FLAG_KIND1: u32 = 2;
const FLAG_ONE_COUNTRY: u32 = 4;

#[cfg(test)]
thread_local! {
    /// The blocks `rows_of` has decoded on this thread, for tests that count a lookup's reads.
    pub static BLOCK_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// A centroid as a block gives it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Centroid {
    pub lat: f32,
    pub lng: f32,
    pub postcode_id: u32,
    pub country_code: u16,
}

/// A centroid as the patch tools diff it (postcode_files.h's LogicalCentroid, its pad left out):
/// its cell's key is the admin cell id >> 32.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LogicalCentroid {
    pub lat: f32,
    pub lng: f32,
    pub postcode_id: u32,
    pub country_code: u16,
    pub cell_key: u32,
}

/// A block holding rows of a query's cells: the rows of it they hold (bit k: the block's row k),
/// and the least distance (`distance`) from the query of a point in the block's box, which no row
/// of the block is nearer than.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct NearBlock {
    pub block: u32,
    pub rows: u32,
    pub d: f64,
}

/// resolve_postcode's distance between the query (lat, lng; `cos_lat` the cosine of its lat) and a
/// point, in squared radians: monotone in each of |lat - plat| and |lng - plng| (each step of it
/// is), so a point's distance is at least that of any point of a box nearer the query on both.
#[inline(always)]
pub fn distance(lat: f64, lng: f64, cos_lat: f64, plat: f64, plng: f64) -> f64 {
    let dlat = (lat - plat).to_radians();
    let dlng = (lng - plng).to_radians();
    dlat * dlat + dlng * dlng * cos_lat * cos_lat
}

#[inline(always)]
fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().unwrap())
}

/// A block's table entry.
#[derive(Clone, Copy)]
struct Entry {
    lat: u32,
    lng: u32,
    postcode_id: u32,
    offset: usize,
    country_code: u16,
    k: [u32; 4],
    flags: u32,
    sides: [i16; 4],
}

impl Entry {
    fn of(e: &[u8]) -> Entry {
        let packed = e[18] as u32 | (e[19] as u32) << 8 | (e[20] as u32) << 16;
        let side = |at: usize| i16::from_le_bytes([e[at], e[at + 1]]);
        Entry {
            lat: u32_at(e, 0),
            lng: u32_at(e, 4),
            postcode_id: u32_at(e, 8),
            offset: u32_at(e, 12) as usize,
            country_code: u16::from_le_bytes([e[16], e[17]]),
            k: [packed & 31, packed >> 5 & 31, packed >> 10 & 31, packed >> 15 & 31],
            flags: packed >> 20 & 7,
            sides: [side(21), side(23), side(25), side(27)],
        }
    }

    /// The box: lat_lo, lat_hi, lng_lo, lng_hi as f64s, an open side infinite (a side past the
    /// ones the builder writes read as open).
    fn bounds(&self) -> [f64; 4] {
        let (lat, lng) = (f32::from_bits(self.lat) as f64, f32::from_bits(self.lng) as f64);
        let side = |first: f64, n: i16, open: f64| if n.unsigned_abs() >= BOX_OPEN as u16 { open } else { first + n as f64 * BOX_UNIT };
        [
            side(lat, self.sides[0], f64::NEG_INFINITY),
            side(lat, self.sides[1], f64::INFINITY),
            side(lng, self.sides[2], f64::NEG_INFINITY),
            side(lng, self.sides[3], f64::INFINITY),
        ]
    }
}

/// The two files in place.
pub struct PostcodeFiles {
    centroids: FileBytes,
    cells: CellPairs,
    rows: u32,
    table_at: usize,
}

impl PostcodeFiles {
    /// The files of a dir, mapped, at its admin cell level; an error for files no builder writes
    /// (a bad footer, a table past the file, a cells file of another kind or level or whose
    /// ranges don't tile the rows), which a block's bytes are not checked for.
    pub fn open(centroids: FileBytes, cells: FileBytes, level: u64) -> Result<PostcodeFiles, String> {
        let c = centroids.as_slice();
        if c.len() < FOOTER_BYTES || &c[c.len() - 4..] != MAGIC {
            return Err(format!("{CENTROIDS_FILE} has no PCC1 footer"));
        }
        let rows = u32_at(c, c.len() - FOOTER_BYTES);
        let blocks = (rows as usize).div_ceil(BLOCK_ROWS);
        let table_at = (c.len() - FOOTER_BYTES)
            .checked_sub(blocks * ENTRY_BYTES)
            .ok_or_else(|| format!("{CENTROIDS_FILE} is shorter than its block table"))?;
        let cells = CellPairs::open_ranges(cells, level, rows).map_err(|e| format!("{CELLS_FILE}: {e}"))?;
        Ok(PostcodeFiles { centroids, cells, rows, table_at })
    }

    /// The centroids: rows below this.
    pub fn rows(&self) -> u32 {
        self.rows
    }

    /// The rows of the admin cell `cell`: [first, end), empty when it holds none.
    pub fn cell_rows(&self, cell: u64) -> (u32, u32) {
        self.cells.range(cell)
    }

    /// Calls `f` with each admin cell holding centroids and its rows, [first, end), in cell
    /// order.
    pub fn for_each_cell(&self, f: impl FnMut(u64, u32, u32)) {
        self.cells.for_each_range(f)
    }

    /// The blocks: rows below BLOCK_ROWS times this.
    pub fn blocks(&self) -> u32 {
        self.rows.div_ceil(BLOCK_ROWS as u32)
    }

    fn entry(&self, b: u32) -> Option<Entry> {
        if b >= self.blocks() {
            return None;
        }
        let at = self.table_at + b as usize * ENTRY_BYTES;
        self.centroids.as_slice().get(at..at + ENTRY_BYTES).map(Entry::of)
    }

    /// Block b's box (lat_lo, lat_hi, lng_lo, lng_hi), an open side infinite.
    pub fn block_box(&self, b: u32) -> Option<[f64; 4]> {
        self.entry(b).map(|e| e.bounds())
    }

    /// The blocks holding rows of `cells` (a query's cell and its neighbours), each once with the
    /// rows of it they hold, and the least distance of its box from (lat, lng), those at
    /// `max_d` or farther left out; into `out` (cleared), the nearest first and the others in
    /// block order. A walk that reads a block only while its d is at most the best so far reads
    /// every row that can be nearest, as no row is nearer than its box.
    pub fn blocks_near(&self, cells: &[u64], lat: f64, lng: f64, max_d: f64, out: &mut Vec<NearBlock>) {
        out.clear();
        let mut few = [(0u32, 0u32); 16];
        let mut many = Vec::new();
        let ranges: &mut [(u32, u32)] = if cells.len() <= few.len() {
            for (r, &c) in few.iter_mut().zip(cells) {
                *r = self.cells.range(c);
            }
            &mut few[..cells.len()]
        } else {
            many.extend(cells.iter().map(|&c| self.cells.range(c)));
            &mut many
        };
        ranges.sort_unstable();
        let cos_lat = lat.to_radians().cos();
        // The block a range's rows were last added to, and whether it is in out; a cell given twice
        // is read once.
        let mut last: Option<(u32, bool)> = None;
        let mut seen = None;
        for &(first, end) in ranges.iter() {
            if seen == Some((first, end)) {
                continue;
            }
            seen = Some((first, end));
            let mut row = first;
            while row < end {
                let b = row / BLOCK_ROWS as u32;
                let stop = ((b as u64 + 1) * BLOCK_ROWS as u64).min(end as u64) as u32;
                let lo = row % BLOCK_ROWS as u32;
                let hi = stop - b * BLOCK_ROWS as u32;
                let mask = (((1u64 << hi) - 1) & !((1u64 << lo) - 1)) as u32;
                match last {
                    Some((lb, kept)) if lb == b => {
                        if kept {
                            out.last_mut().unwrap().rows |= mask;
                        }
                    }
                    _ => {
                        let kept = match self.entry(b) {
                            Some(e) => {
                                let [la, lah, ga, gah] = e.bounds();
                                let d = distance(lat, lng, cos_lat, lat.max(la).min(lah), lng.max(ga).min(gah));
                                (d < max_d).then(|| out.push(NearBlock { block: b, rows: mask, d })).is_some()
                            }
                            None => false,
                        };
                        last = Some((b, kept));
                    }
                }
                row = stop;
            }
        }
        // The nearest to the front, the ones before it one place on: the others stay in block order.
        if let Some(m) = (0..out.len()).min_by(|&i, &j| out[i].d.total_cmp(&out[j].d)) {
            out[..=m].rotate_right(1);
        }
    }

    /// Calls `f` with each row of block `block` that `rows` holds (bit k: the block's row k) and
    /// its centroid, in row order. A row whose codes run past the block's end, and every row after
    /// it, is left out.
    pub fn rows_of(&self, block: u32, rows: u32, mut f: impl FnMut(u32, Centroid)) {
        let Some(e) = self.entry(block) else { return };
        let first = block * BLOCK_ROWS as u32;
        let n = (self.rows - first).min(BLOCK_ROWS as u32);
        let rows = rows & if n == 32 { u32::MAX } else { (1 << n) - 1 };
        if rows == 0 || e.offset > self.table_at {
            return;
        }
        #[cfg(test)]
        BLOCK_READS.with(|n| n.set(n.get() + 1));
        // The block's bytes, and its bits from the block's first byte: a bit offset in the file
        // would pass a 32-bit usize past 512 MiB. The bytes after the block (the next blocks, then
        // the table) stay in the slice for the 8-byte loads.
        let len = match self.entry(block + 1) {
            Some(next) => next.offset.clamp(e.offset, self.table_at),
            None => self.table_at,
        } - e.offset;
        let s = &self.centroids.as_slice()[e.offset..];
        let mut c = Centroid { lat: f32::from_bits(e.lat), lng: f32::from_bits(e.lng), postcode_id: e.postcode_id, country_code: e.country_code };
        if rows & 1 != 0 {
            f(first, c);
        }
        let last = 31 - rows.leading_zeros();
        let (mixed, kind1, one_country) = (e.flags & FLAG_MIXED != 0, e.flags & FLAG_KIND1 != 0, e.flags & FLAG_ONE_COUNTRY != 0);
        let [kl, kg, kp, kc] = e.k;
        let mut bit = 0;
        // The previous row's decimal keys, when it was of kind 1: its rounds.
        let mut keys: Option<(i32, i32)> = None;
        for i in 1..=last {
            let kind = if mixed {
                let Some(k) = bits_at(s, bit, 1) else { return };
                bit += 1;
                k == 1
            } else {
                kind1
            };
            let (Some(zl), Some(zg), Some(zp)) = (get_rice(s, &mut bit, kl), get_rice(s, &mut bit, kg), get_rice(s, &mut bit, kp)) else {
                return;
            };
            let zc = if one_country {
                0
            } else {
                let Some(z) = get_rice(s, &mut bit, kc) else { return };
                z
            };
            if bit.div_ceil(8) > len {
                return;
            }
            let (dl, dg) = (unzigzag32(zl), unzigzag32(zg));
            if kind {
                let (pl, pg) = keys.unwrap_or_else(|| (dec4_round(c.lat.to_bits()), dec4_round(c.lng.to_bits())));
                let (kl, kg) = (pl.wrapping_add(dl as i32), pg.wrapping_add(dg as i32));
                c.lat = f32::from_bits(from_dec4_key(kl));
                c.lng = f32::from_bits(from_dec4_key(kg));
                keys = Some((kl, kg));
            } else {
                c.lat = f32::from_bits(from_order_key(order_key(c.lat.to_bits()).wrapping_add(dl)));
                c.lng = f32::from_bits(from_order_key(order_key(c.lng.to_bits()).wrapping_add(dg)));
                keys = None;
            }
            c.postcode_id = c.postcode_id.wrapping_add(unzigzag32(zp));
            c.country_code = c.country_code.wrapping_add(unzigzag32(zc) as u16);
            if rows >> i & 1 != 0 {
                f(first + i, c);
            }
        }
    }

    /// Calls `f` with each row in [first, end) and its centroid, in row order.
    pub fn for_rows(&self, first: u32, end: u32, mut f: impl FnMut(u32, Centroid)) {
        let end = end.min(self.rows);
        let mut row = first;
        while row < end {
            let b = row / BLOCK_ROWS as u32;
            let stop = ((b as u64 + 1) * BLOCK_ROWS as u64).min(end as u64) as u32;
            let (lo, hi) = (row % BLOCK_ROWS as u32, stop - b * BLOCK_ROWS as u32);
            self.rows_of(b, (((1u64 << hi) - 1) & !((1u64 << lo) - 1)) as u32, &mut f);
            row = stop;
        }
    }
}

/// The two files' bytes for logical rows in their order (cell key, postcode_id, country_code
/// ascending), as the builder's encoder writes them (fixtures and tests): (postcode_centroids.bin,
/// postcode_centroid_cells.bin). Panics on rows it refuses.
#[cfg(any(test, feature = "fixtures"))]
pub fn encode(rows: &[LogicalCentroid]) -> (Vec<u8>, Vec<u8>) {
    use crate::coord_keys::{best_rice_k, dec4_key, put_rice, zigzag32, BitWriter};
    for w in rows.windows(2) {
        let key = |r: &LogicalCentroid| (r.cell_key, r.postcode_id, r.country_code);
        assert!(key(&w[0]) < key(&w[1]), "rows out of order, or a postcode of one country twice in a cell");
    }
    assert!(rows.iter().all(|r| r.lat.is_finite() && r.lng.is_finite() && r.postcode_id != crate::NO_DATA));
    let mut ranges: Vec<(u64, u32)> = Vec::new();
    for r in rows {
        match ranges.last_mut() {
            Some(last) if last.0 == (r.cell_key as u64) << 32 => last.1 += 1,
            _ => ranges.push(((r.cell_key as u64) << 32, 1)),
        }
    }
    let kind1 = |r: &LogicalCentroid| dec4_key(r.lat.to_bits()).is_some() && dec4_key(r.lng.to_bits()).is_some();
    let step = |v: f32, prev: f32, k1: bool| -> u32 {
        if k1 { (dec4_key(v.to_bits()).unwrap() as u32).wrapping_sub(dec4_round(prev.to_bits()) as u32) } else { order_key(v.to_bits()).wrapping_sub(order_key(prev.to_bits())) }
    };
    // The largest n of -32766..=32766 at which first + n units is at most v, -32767 for none; and
    // the least at which it is at least v, 32767 for none: first + n units grows with n.
    let side = |first: f64, n: i32| first + n as f64 * BOX_UNIT;
    let least_n = |ok: &dyn Fn(i32) -> bool| {
        let (mut a, mut b) = (-32766, 32767);
        while a < b {
            let m = a + (b - a) / 2;
            if ok(m) { b = m } else { a = m + 1 }
        }
        a
    };
    let lo = |first: f64, v: f64| (least_n(&|n| side(first, n) > v) - 1).max(-32767) as i16;
    let hi = |first: f64, v: f64| least_n(&|n| side(first, n) >= v) as i16;
    let (mut blocks, mut table) = (Vec::new(), Vec::new());
    for chunk in rows.chunks(BLOCK_ROWS) {
        let r0 = chunk[0];
        let steps = &chunk[1..];
        let kinds: Vec<bool> = steps.iter().map(kind1).collect();
        let n1 = kinds.iter().filter(|k| **k).count();
        let mixed = n1 > 0 && n1 < steps.len();
        let one_country = chunk.iter().all(|r| r.country_code == r0.country_code);
        let z: [Vec<u32>; 4] = [
            chunk.windows(2).zip(&kinds).map(|(w, &k)| zigzag32(step(w[1].lat, w[0].lat, k))).collect(),
            chunk.windows(2).zip(&kinds).map(|(w, &k)| zigzag32(step(w[1].lng, w[0].lng, k))).collect(),
            chunk.windows(2).map(|w| zigzag32(w[1].postcode_id.wrapping_sub(w[0].postcode_id))).collect(),
            chunk.windows(2).map(|w| zigzag32((w[1].country_code as i32 - w[0].country_code as i32) as u32)).collect(),
        ];
        let k = [best_rice_k(&z[0]), best_rice_k(&z[1]), best_rice_k(&z[2]), if one_country { 0 } else { best_rice_k(&z[3]) }];
        let mut w = BitWriter::default();
        let fields = if one_country { 3 } else { 4 };
        for s in 0..steps.len() {
            if mixed {
                w.put(kinds[s] as u32, 1);
            }
            for f in 0..fields {
                put_rice(&mut w, z[f][s], k[f]);
            }
        }
        let flags = mixed as u32 | ((!steps.is_empty() && n1 == steps.len()) as u32) << 1 | (one_country as u32) << 2;
        table.extend(r0.lat.to_bits().to_le_bytes());
        table.extend(r0.lng.to_bits().to_le_bytes());
        table.extend(r0.postcode_id.to_le_bytes());
        table.extend((blocks.len() as u32).to_le_bytes());
        table.extend(r0.country_code.to_le_bytes());
        let packed = k[0] | k[1] << 5 | k[2] << 10 | k[3] << 15 | flags << 20;
        table.extend(&packed.to_le_bytes()[..3]);
        let (lat0, lng0) = (r0.lat as f64, r0.lng as f64);
        let least = |f: fn(&LogicalCentroid) -> f32| chunk.iter().map(|r| f(r) as f64).fold(f64::INFINITY, f64::min);
        let most = |f: fn(&LogicalCentroid) -> f32| chunk.iter().map(|r| f(r) as f64).fold(f64::NEG_INFINITY, f64::max);
        for s in [lo(lat0, least(|r| r.lat)), hi(lat0, most(|r| r.lat)), lo(lng0, least(|r| r.lng)), hi(lng0, most(|r| r.lng))] {
            table.extend(s.to_le_bytes());
        }
        blocks.extend(w.finish());
    }
    blocks.extend(table);
    blocks.extend((rows.len() as u32).to_le_bytes());
    blocks.extend(MAGIC);
    (blocks, crate::cell_pairs::write_ranges(&ranges))
}

/// The files of centroids anywhere, each with its admin cell at `level` and put in cell order, as
/// the builder writes them (fixtures and tests). The level is at most 14: a cell's key is its id
/// >> 32, which keeps the whole id only where its low 32 bits are zero, as they are at level 14
/// and coarser; the builder refuses a finer cell (postcode_files.h's cell_key).
#[cfg(any(test, feature = "fixtures"))]
pub fn encode_points(points: &[Centroid], level: u64) -> (Vec<u8>, Vec<u8>) {
    assert!(level <= 14, "postcode centroid cells of level {level}: their keys hold levels up to 14");
    let mut rows: Vec<LogicalCentroid> = points
        .iter()
        .map(|c| LogicalCentroid {
            lat: c.lat,
            lng: c.lng,
            postcode_id: c.postcode_id,
            country_code: c.country_code,
            cell_key: (crate::cell_id_at_level(c.lat as f64, c.lng as f64, level) >> 32) as u32,
        })
        .collect();
    rows.sort_by_key(|r| (r.cell_key, r.postcode_id, r.country_code));
    encode(&rows)
}

#[cfg(test)]
mod tests;
