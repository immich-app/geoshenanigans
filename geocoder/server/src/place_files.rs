//! The place node files as the builder writes them (builder/src/place_files.h, which lays the
//! format out): place_cells.bin, a row per admin cell holding visible nodes (its key, the cell id
//! >> 32, and its first row); place_nodes.bin, the visible nodes in cell order, bit-packed in
//! blocks of BLOCK_ROWS rows, with a table of the blocks and a footer after them; and
//! place_hidden.bin, a row per node a boundary hides. A node's id is its row: a visible node's,
//! or a hidden one's after the visible rows.

use crate::{FileBytes, PlaceNode, NO_DATA, PLACE_FLAG_HIDDEN, PLACE_FLAG_LABEL_OF};
pub const CELLS_FILE: &str = "place_cells.bin";
pub const NODES_FILE: &str = "place_nodes.bin";
pub const HIDDEN_FILE: &str = "place_hidden.bin";
const MAGIC: &[u8; 4] = b"PNL3";
pub const BLOCK_ROWS: usize = 64;
const TABLE_ENTRY_BYTES: usize = 16;
const FOOTER_BYTES: usize = 8;
const CELL_ROW_BYTES: usize = 8;
const HIDDEN_ROW_BYTES: usize = 16;
const TYPE_BITS: u32 = 3;
const HIDDEN_NAME_BITS: u32 = 28;

#[inline(always)]
fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().unwrap())
}

/// An f32 bit pattern's place in the floats' order (-0 before +0), and back.
#[inline]
pub fn order_key(bits: u32) -> u32 {
    if bits & 0x8000_0000 != 0 { !bits } else { bits | 0x8000_0000 }
}
#[inline]
pub fn from_order_key(key: u32) -> u32 {
    if key & 0x8000_0000 != 0 { key & 0x7FFF_FFFF } else { !key }
}

/// A block's table entry: its bases, where its bits start, and its widths.
#[derive(Clone, Copy)]
struct Block {
    lat_base: u32,
    lng_base: u32,
    offset: usize,
    wl: u32,
    wg: u32,
    wn: u32,
}

impl Block {
    fn row_bits(&self) -> usize {
        (self.wl + self.wg + TYPE_BITS + self.wn) as usize
    }
}

/// The three files in place: the cell table and the hidden rows read as they sit, a visible row
/// from its block.
pub struct PlaceFiles {
    cells: FileBytes,
    nodes: FileBytes,
    hidden: FileBytes,
    visible: u32,
    hidden_rows: u32,
    table_at: usize,
}

impl PlaceFiles {
    /// The files of a dir, mapped; an error for files no builder writes (a bad footer, a table
    /// past the file, rows not whole), which a block's bytes are not checked for.
    pub fn open(cells: FileBytes, nodes: FileBytes, hidden: FileBytes) -> Result<PlaceFiles, String> {
        let n = nodes.as_slice();
        if n.len() < FOOTER_BYTES || &n[n.len() - 4..] != MAGIC {
            return Err(format!("{NODES_FILE} has no PNL3 footer"));
        }
        let visible = u32_at(n, n.len() - FOOTER_BYTES);
        let blocks = (visible as usize).div_ceil(BLOCK_ROWS);
        let table_at = (n.len() - FOOTER_BYTES)
            .checked_sub(blocks * TABLE_ENTRY_BYTES)
            .ok_or_else(|| format!("{NODES_FILE} is shorter than its block table"))?;
        if cells.len() % CELL_ROW_BYTES as u64 != 0 {
            return Err(format!("{CELLS_FILE} is not whole rows"));
        }
        if hidden.len() % HIDDEN_ROW_BYTES as u64 != 0 {
            return Err(format!("{HIDDEN_FILE} is not whole rows"));
        }
        let hidden_rows = u32::try_from(hidden.len() / HIDDEN_ROW_BYTES as u64)
            .ok()
            .filter(|h| h.checked_add(visible).is_some())
            .ok_or_else(|| "more than 2^32 - 1 place nodes".to_string())?;
        Ok(PlaceFiles { cells, nodes, hidden, visible, hidden_rows, table_at })
    }

    /// The visible nodes: ids below this.
    pub fn visible(&self) -> u32 {
        self.visible
    }

    /// Every node, visible and hidden.
    pub fn count(&self) -> u32 {
        self.visible + self.hidden_rows
    }

    /// The visible rows of the admin cell `cell` (an id ending in 32 zero bits): [first, end),
    /// empty when it holds none.
    pub fn cell_rows(&self, cell: u64) -> (u32, u32) {
        if cell & 0xFFFF_FFFF != 0 {
            return (0, 0);
        }
        let key = (cell >> 32) as u32;
        let c = self.cells.as_slice();
        let n = c.len() / CELL_ROW_BYTES;
        let key_at = |k: usize| u32_at(c, k * CELL_ROW_BYTES);
        let first_at = |k: usize| u32_at(c, k * CELL_ROW_BYTES + 4);
        let (mut lo, mut hi) = (0usize, n);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match key_at(mid).cmp(&key) {
                std::cmp::Ordering::Equal => {
                    let end = if mid + 1 < n { first_at(mid + 1) } else { self.visible };
                    let first = first_at(mid);
                    return if first < end && end <= self.visible { (first, end) } else { (0, 0) };
                }
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        (0, 0)
    }

    fn block(&self, b: usize) -> Option<Block> {
        let at = self.table_at + b * TABLE_ENTRY_BYTES;
        let e = self.nodes.as_slice().get(at..at + TABLE_ENTRY_BYTES)?;
        let block = Block {
            lat_base: u32_at(e, 0),
            lng_base: u32_at(e, 4),
            offset: u32_at(e, 8) as usize,
            wl: e[12] as u32,
            wg: e[13] as u32,
            wn: e[14] as u32,
        };
        (block.wl <= 32 && block.wg <= 32 && block.wn <= 32 && block.offset <= self.table_at).then_some(block)
    }

    /// Calls `f` with each visible row in [first, end) and its node, each block's entry read
    /// once; a row of a block the table can't place is left out.
    pub fn for_rows(&self, first: u32, end: u32, mut f: impl FnMut(u32, PlaceNode)) {
        let n = self.nodes.as_slice();
        let end = end.min(self.visible);
        let mut row = first;
        while row < end {
            let b = row as usize / BLOCK_ROWS;
            let block_end = block_end(b, end);
            let Some(e) = self.block(b) else {
                row = block_end;
                continue;
            };
            let rec = e.row_bits();
            while row < block_end {
                let mut bit = e.offset * 8 + (row as usize % BLOCK_ROWS) * rec;
                let mut take = |w: u32| -> Option<u32> {
                    let v = bits_at(n, bit, w, self.table_at)?;
                    bit += w as usize;
                    Some(v)
                };
                let (Some(dl), Some(dg), Some(t), Some(name)) = (take(e.wl), take(e.wg), take(TYPE_BITS), take(e.wn)) else {
                    break;
                };
                f(row, PlaceNode {
                    lat: f32::from_bits(from_order_key(e.lat_base.wrapping_add(dl))),
                    lng: f32::from_bits(from_order_key(e.lng_base.wrapping_add(dg))),
                    name_id: name,
                    place_type: t as u8,
                    _pad1: 0,
                    _pad2: 0,
                    parent_poly_id: NO_DATA,
                });
                row += 1;
            }
            row = block_end;
        }
    }

    /// Node `id`, visible or hidden; None past the last.
    pub fn node(&self, id: u32) -> Option<PlaceNode> {
        if id < self.visible {
            let mut out = None;
            self.for_rows(id, id + 1, |_, pn| out = Some(pn));
            return out;
        }
        let at = (id - self.visible) as usize * HIDDEN_ROW_BYTES;
        let r = self.hidden.as_slice().get(at..at + HIDDEN_ROW_BYTES)?;
        let w = u32_at(r, 8);
        Some(PlaceNode {
            lat: f32::from_bits(u32_at(r, 0)),
            lng: f32::from_bits(u32_at(r, 4)),
            name_id: w & ((1 << HIDDEN_NAME_BITS) - 1),
            place_type: (w >> HIDDEN_NAME_BITS & ((1 << TYPE_BITS) - 1)) as u8,
            _pad1: PLACE_FLAG_HIDDEN | (w >> 31) as u8 * PLACE_FLAG_LABEL_OF,
            _pad2: 0,
            parent_poly_id: u32_at(r, 12),
        })
    }
}

/// The row after block b's last that a read to `end` takes: the block's end, clamped to `end` before it
/// is narrowed (the last block of 2^32 - 1 rows ends at 2^32).
#[inline]
fn block_end(b: usize, end: u32) -> u32 {
    ((b as u64 + 1) * BLOCK_ROWS as u64).min(u64::from(end)) as u32
}

/// The w bits (at most 32) at bit `bit` of the blocks, which end at `blocks_end`; the file runs
/// on past them (the table and footer), and a load stops at its end. None past the blocks.
#[inline]
fn bits_at(file: &[u8], bit: usize, w: u32, blocks_end: usize) -> Option<u32> {
    if w == 0 {
        return Some(0);
    }
    if bit + w as usize > blocks_end * 8 {
        return None;
    }
    let at = bit >> 3;
    let mut word = [0u8; 8];
    let k = (file.len() - at).min(8);
    word[..k].copy_from_slice(&file[at..at + k]);
    Some(((u64::from_le_bytes(word) >> (bit & 7)) & ((1u64 << w) - 1)) as u32)
}

/// The three files' bytes for `nodes` (fixtures and tests), the builder's encoding: the visible
/// nodes stably sorted by their admin cell at `cell_level`, then the hidden ones in their order.
/// Returns the files and each given node's id.
#[cfg(any(test, feature = "fixtures"))]
pub fn encode(nodes: &[PlaceNode], cell_level: u64) -> (PlaceFileBytes, Vec<u32>) {
    let visible = |pn: &PlaceNode| pn._pad1 == 0 && pn.parent_poly_id == NO_DATA;
    let key = |pn: &PlaceNode| (crate::cell_id_at_level(pn.lat as f64, pn.lng as f64, cell_level) >> 32) as u32;
    let mut order: Vec<usize> = (0..nodes.len()).filter(|&i| visible(&nodes[i])).collect();
    order.sort_by_key(|&i| key(&nodes[i]));
    let n_visible = order.len();
    order.extend((0..nodes.len()).filter(|&i| !visible(&nodes[i])));
    let mut ids = vec![0u32; nodes.len()];
    for (id, &i) in order.iter().enumerate() {
        ids[i] = id as u32;
    }
    let rows: Vec<(PlaceNode, u32)> = order.iter().map(|&i| (nodes[i], if visible(&nodes[i]) { key(&nodes[i]) } else { NO_DATA })).collect();
    let files = encode_logical(&rows);
    assert_eq!(files.visible, n_visible as u32);
    (files, ids)
}

/// The three files of a place set.
#[cfg(any(test, feature = "fixtures"))]
pub struct PlaceFileBytes {
    pub cells: Vec<u8>,
    pub nodes: Vec<u8>,
    pub hidden: Vec<u8>,
    pub visible: u32,
}

/// The files of nodes in their logical order with their cell keys (NO_DATA for a hidden node), as
/// the builder's encoder writes them. Panics on a set it refuses.
#[cfg(any(test, feature = "fixtures"))]
pub fn encode_logical(rows: &[(PlaceNode, u32)]) -> PlaceFileBytes {
    let width = |v: u32| 32 - v.leading_zeros();
    let (mut cells, mut blocks, mut table, mut hidden) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let vis: Vec<&(PlaceNode, u32)> = rows.iter().filter(|(pn, _)| pn._pad1 == 0 && pn.parent_poly_id == NO_DATA).collect();
    assert!(rows[..vis.len()].iter().all(|(pn, _)| pn._pad1 == 0 && pn.parent_poly_id == NO_DATA), "visible nodes first");
    for (row, (_, key)) in vis.iter().enumerate() {
        assert_ne!(*key, NO_DATA);
        if row == 0 || vis[row - 1].1 != *key {
            assert!(row == 0 || vis[row - 1].1 < *key, "cell order");
            cells.extend_from_slice(&key.to_le_bytes());
            cells.extend_from_slice(&(row as u32).to_le_bytes());
        }
    }
    for chunk in vis.chunks(BLOCK_ROWS) {
        let lk: Vec<u32> = chunk.iter().map(|(pn, _)| order_key(pn.lat.to_bits())).collect();
        let gk: Vec<u32> = chunk.iter().map(|(pn, _)| order_key(pn.lng.to_bits())).collect();
        let (lb, gb) = (*lk.iter().min().unwrap(), *gk.iter().min().unwrap());
        let wl = width(lk.iter().max().unwrap() - lb);
        let wg = width(gk.iter().max().unwrap() - gb);
        let wn = width(chunk.iter().map(|(pn, _)| pn.name_id).max().unwrap());
        table.extend_from_slice(&lb.to_le_bytes());
        table.extend_from_slice(&gb.to_le_bytes());
        table.extend_from_slice(&(blocks.len() as u32).to_le_bytes());
        table.extend_from_slice(&[wl as u8, wg as u8, wn as u8, 0]);
        let (mut acc, mut n) = (0u64, 0u32);
        let mut put = |v: u32, w: u32| {
            if w == 0 {
                return;
            }
            acc |= (v as u64) << n;
            n += w;
            while n >= 8 {
                blocks.push(acc as u8);
                acc >>= 8;
                n -= 8;
            }
        };
        for (j, (pn, _)) in chunk.iter().enumerate() {
            assert!(pn.place_type < 1 << TYPE_BITS);
            put(lk[j] - lb, wl);
            put(gk[j] - gb, wg);
            put(pn.place_type as u32, TYPE_BITS);
            put(pn.name_id, wn);
        }
        if n > 0 {
            blocks.push(acc as u8);
        }
    }
    for (pn, key) in &rows[vis.len()..] {
        assert_eq!(*key, NO_DATA);
        assert!(pn._pad1 & PLACE_FLAG_HIDDEN != 0 && pn.name_id < 1 << HIDDEN_NAME_BITS && pn.place_type < 1 << TYPE_BITS);
        let label = pn._pad1 & PLACE_FLAG_LABEL_OF != 0;
        assert_eq!(label, pn.parent_poly_id != NO_DATA);
        hidden.extend_from_slice(&pn.lat.to_bits().to_le_bytes());
        hidden.extend_from_slice(&pn.lng.to_bits().to_le_bytes());
        hidden.extend_from_slice(&(pn.name_id | (pn.place_type as u32) << HIDDEN_NAME_BITS | (label as u32) << 31).to_le_bytes());
        hidden.extend_from_slice(&pn.parent_poly_id.to_le_bytes());
    }
    let visible = vis.len() as u32;
    blocks.extend_from_slice(&table);
    blocks.extend_from_slice(&visible.to_le_bytes());
    blocks.extend_from_slice(MAGIC);
    PlaceFileBytes { cells, nodes: blocks, hidden, visible }
}

#[cfg(test)]
mod tests;
