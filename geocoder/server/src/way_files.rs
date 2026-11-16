//! The street and interpolation way files as the builder writes them (builder/src/way_files.h,
//! which lays the format out), a pair of files a kind: the rows file, a row per group of
//! GROUP_WAYS ways holding where the group's node blocks start, its anchor and each way's node
//! count, code, name and, for an interpolation, numbers; and the nodes file, the bit-packed node
//! blocks of the ways with nodes, back to back. A way's id is its place in the rows.
//! geocoder/testdata/way_files.tsv holds pairs the builder's encoder and this reader must agree
//! on.

use crate::coord_keys::{bits_at, byte_length, from_hybrid_key, from_order_key, hybrid_key, is_hybrid_key, order_key, unzigzag32};
use crate::{FileBytes, NodeCoord};

pub const GROUP_WAYS: usize = 16;
const ROW_HEAD_BYTES: usize = 12;
const WAYS_FOOTER_BYTES: usize = 8;
const NODES_FOOTER_BYTES: usize = 16;
/// The most nodes a way has, the bytes a node fills at the most, and the most bytes a nodes
/// file is: a row's off is a u32.
const MAX_WAY_NODES: u64 = u16::MAX as u64;
const NODE_BYTES: u64 = 8;
const MAX_NODES_FILE_BYTES: u64 = u32::MAX as u64;

#[inline(always)]
fn u16_at(b: &[u8], at: usize) -> u16 {
    u16::from_le_bytes(b[at..at + 2].try_into().unwrap())
}

#[inline(always)]
fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().unwrap())
}

/// The two kinds of way, each a pair of files.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Streets,
    Interps,
}

impl Kind {
    pub fn ways_file(self) -> &'static str {
        match self {
            Kind::Streets => "street_ways.bin",
            Kind::Interps => "interp_ways.bin",
        }
    }

    pub fn nodes_file(self) -> &'static str {
        match self {
            Kind::Streets => "street_nodes.bin",
            Kind::Interps => "interp_nodes.bin",
        }
    }

    fn ways_magic(self) -> &'static str {
        match self {
            Kind::Streets => "SWY1",
            Kind::Interps => "IWY1",
        }
    }

    fn nodes_magic(self) -> &'static str {
        match self {
            Kind::Streets => "SWN1",
            Kind::Interps => "IWN1",
        }
    }

    /// A way's bytes in its group's row.
    fn row_way_bytes(self) -> usize {
        match self {
            Kind::Streets => 8,
            Kind::Interps => 17,
        }
    }
}

/// What a way's row holds of it: its node count, its name (a street's name_id, an
/// interpolation's street_id) and an interpolation's numbers, which are 0 of a street.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WayRow {
    pub node_count: u16,
    pub name: u32,
    pub start_number: u32,
    pub end_number: u32,
    pub interpolation: u8,
}

/// The bytes of the block of a way of `cnt` nodes packed as `code` says: none of a way without
/// nodes. None for a code no way of that many nodes has: any but 0 without nodes, else a width
/// past 32 bits or a first node in more than 4 bytes an axis.
#[inline]
fn block_bytes(cnt: u16, code: u16) -> Option<u64> {
    if cnt == 0 {
        return (code == 0).then_some(0);
    }
    let (wl, wg, wf) = (u64::from(code & 63), u64::from(code >> 6 & 63), u64::from(code >> 13));
    (wl <= 32 && wg <= 32 && wf <= 4).then(|| 2 * wf + ((u64::from(cnt) - 1) * (wl + wg)).div_ceil(8))
}

/// A way's node block: its first node, and what its other nodes are read with.
struct Block<'a> {
    cnt: usize,
    lat: u32,
    lng: u32,
    /// Whether the way's key is the hybrid key, not the order key; and its first node's keys.
    hybrid: bool,
    key_lat: u32,
    key_lng: u32,
    wl: u32,
    wg: u32,
    /// The values' bits, with the 8 bytes after them a field's load takes.
    bits: &'a [u8],
}

/// The two files of a kind in place: a way's fields read from its row as they sit, its nodes
/// decoded from its block.
pub struct WayFiles {
    kind: Kind,
    rows: FileBytes,
    nodes: FileBytes,
    ways: u32,
    /// Where the node blocks end and the nodes file's footer starts.
    blocks_end: usize,
}

impl WayFiles {
    /// The files of a dir, mapped; an error for files no builder writes: a footer without its
    /// magic, footers that count different ways or 2^32 - 1 of them, a rows file that is not
    /// the rows of its ways, a nodes file of 4 GiB or more, more nodes than the ways can have,
    /// blocks longer than the nodes can fill. What a row or a block holds is not checked here:
    /// a way that can't be read reads as none.
    pub fn open(kind: Kind, ways: FileBytes, nodes: FileBytes) -> Result<WayFiles, String> {
        let (r, n) = (ways.as_slice(), nodes.as_slice());
        let (ways_file, nodes_file) = (kind.ways_file(), kind.nodes_file());
        if r.len() < WAYS_FOOTER_BYTES || &r[r.len() - 4..] != kind.ways_magic().as_bytes() {
            return Err(format!("{ways_file} has no {} footer", kind.ways_magic()));
        }
        if n.len() < NODES_FOOTER_BYTES || &n[n.len() - 4..] != kind.nodes_magic().as_bytes() {
            return Err(format!("{nodes_file} has no {} footer", kind.nodes_magic()));
        }
        let count = u32_at(r, r.len() - WAYS_FOOTER_BYTES);
        if u32_at(n, n.len() - 8) != count {
            return Err(format!("{ways_file} and {nodes_file} count different ways"));
        }
        if count == u32::MAX {
            return Err(format!("{ways_file} holds 2^32 - 1 ways or more: NO_DATA is no way's id"));
        }
        let groups = u64::from(count).div_ceil(GROUP_WAYS as u64);
        let rows_bytes = groups * ROW_HEAD_BYTES as u64 + u64::from(count) * kind.row_way_bytes() as u64;
        if (r.len() - WAYS_FOOTER_BYTES) as u64 != rows_bytes {
            return Err(format!("{ways_file} is not the rows of its {count} ways"));
        }
        if n.len() as u64 > MAX_NODES_FILE_BYTES {
            return Err(format!("{nodes_file} is 4 GiB or more"));
        }
        let blocks_end = n.len() - NODES_FOOTER_BYTES;
        let node_count = u64::from_le_bytes(n[blocks_end..blocks_end + 8].try_into().unwrap());
        if node_count > u64::from(count) * MAX_WAY_NODES {
            return Err(format!("{nodes_file} counts more nodes than its ways can have"));
        }
        if blocks_end as u64 > node_count * NODE_BYTES {
            return Err(format!("{nodes_file} is longer than its nodes can fill"));
        }
        Ok(WayFiles { kind, rows: ways, nodes, ways: count, blocks_end })
    }

    /// The ways: ids below this.
    pub fn count(&self) -> u32 {
        self.ways
    }

    /// The row of way `id`'s group, the group's ways and the way's place among them; None past
    /// the last way.
    #[inline]
    fn group(&self, id: u32) -> Option<(&[u8], usize, usize)> {
        if id >= self.ways {
            return None;
        }
        let (g, j) = (id as usize / GROUP_WAYS, id as usize % GROUP_WAYS);
        let n = GROUP_WAYS.min(self.ways as usize - g * GROUP_WAYS);
        let at = g * (ROW_HEAD_BYTES + GROUP_WAYS * self.kind.row_way_bytes());
        let row = self.rows.as_slice().get(at..at + ROW_HEAD_BYTES + n * self.kind.row_way_bytes())?;
        Some((row, n, j))
    }

    /// Way `id`'s row fields, without its nodes; None past the last way.
    pub fn row(&self, id: u32) -> Option<WayRow> {
        let (row, n, j) = self.group(id)?;
        let mut at = ROW_HEAD_BYTES + 4 * n;
        let (mut start_number, mut end_number, mut interpolation) = (0, 0, 0);
        if self.kind == Kind::Interps {
            let numbers = at + 9 * j;
            start_number = u32_at(row, numbers);
            end_number = start_number.wrapping_add(u32_at(row, numbers + 4));
            interpolation = row[numbers + 8];
            at += 9 * n;
        }
        Some(WayRow { node_count: u16_at(row, ROW_HEAD_BYTES + 4 * j), name: u32_at(row, at + 4 * j), start_number, end_number, interpolation })
    }

    /// The block of way `j` of a group's row, the file's first group where `first_group`: None
    /// for a way without nodes, and for a row or a block no builder writes: a code that the
    /// way, or a way before it in the group, can't have, a block past the blocks' end, a first
    /// group whose blocks do not start the nodes file, a group whose first way with nodes is
    /// not at its anchor, a first node in more bytes than it needs or without the hybrid key
    /// its way is under, bits set past the last value.
    #[inline]
    fn block(&self, row: &[u8], j: usize, first_group: bool) -> Option<Block<'_>> {
        let way = |k: usize| (u16_at(row, ROW_HEAD_BYTES + 4 * k), u16_at(row, ROW_HEAD_BYTES + 4 * k + 2));
        let (cnt, code) = way(j);
        if cnt == 0 {
            return None;
        }
        let bytes = block_bytes(cnt, code)?;
        let (wl, wg, wf) = (u32::from(code & 63), u32::from(code >> 6 & 63), usize::from(code >> 13));
        // The group's blocks start at its off, this way's after those of the ways before it.
        // The first of the group's ways with nodes is at its anchor: its first node takes no
        // bytes.
        let (mut before, mut anchored) = (0u64, false);
        for k in 0..j {
            let (cnt, code) = way(k);
            before += block_bytes(cnt, code)?;
            if cnt != 0 && !anchored {
                if code >> 13 != 0 {
                    return None;
                }
                anchored = true;
            }
        }
        // The first group's blocks start where the nodes file does.
        let off = u32_at(row, 0);
        if (!anchored && wf != 0) || (first_group && off != 0) {
            return None;
        }
        let at = usize::try_from(u64::from(off) + before).ok()?;
        let end = at.checked_add(usize::try_from(bytes).ok()?)?;
        if end > self.blocks_end {
            return None;
        }
        // The footer after the blocks holds the 8 bytes a field's load takes past the block.
        let block = self.nodes.as_slice().get(at..end + 8)?;
        // The first node's steps from the anchor in the order keys, as zigzags, in the fewest
        // bytes that hold both.
        let first = |from: usize| (0..wf).fold(0u32, |z, b| z | u32::from(block[from + b]) << (8 * b));
        let (zl, zg) = (first(0), first(wf));
        if byte_length(zl).max(byte_length(zg)) as usize != wf {
            return None;
        }
        // The values end in zero bits to their last byte's end.
        let values = (usize::from(cnt) - 1) * (wl + wg) as usize;
        if values % 8 != 0 && block[2 * wf + values / 8] >> (values % 8) != 0 {
            return None;
        }
        let (order_lat, order_lng) = (order_key(u32_at(row, 4)).wrapping_add(unzigzag32(zl)), order_key(u32_at(row, 8)).wrapping_add(unzigzag32(zg)));
        let (lat, lng) = (from_order_key(order_lat), from_order_key(order_lng));
        // The way's key: the hybrid key, which its first node then has, or the order key.
        let hybrid = code >> 12 & 1 != 0;
        let (key_lat, key_lng) = if hybrid { (hybrid_key(lat)?, hybrid_key(lng)?) } else { (order_lat, order_lng) };
        Some(Block { cnt: usize::from(cnt), lat, lng, hybrid, key_lat, key_lng, wl, wg, bits: &block[2 * wf..] })
    }

    /// Way `id`'s first node; None for a way without nodes or one whose block can't be read.
    /// Only the first node is read: a way that `way` gives no nodes of for a later node's key
    /// has a first node here.
    pub fn first_node(&self, id: u32) -> Option<NodeCoord> {
        let (row, _, j) = self.group(id)?;
        let b = self.block(row, j, id < GROUP_WAYS as u32)?;
        Some(NodeCoord { lat: f32::from_bits(b.lat), lng: f32::from_bits(b.lng) })
    }

    /// Way `id`'s nodes into `nodes`, which is cleared first: none for a way without nodes.
    /// False, and no nodes, past the last way and for a way that can't be read.
    pub fn way(&self, id: u32, nodes: &mut Vec<NodeCoord>) -> bool {
        nodes.clear();
        let read = self.read_nodes(id, nodes).is_some();
        if !read {
            nodes.clear();
        }
        read
    }

    fn read_nodes(&self, id: u32, nodes: &mut Vec<NodeCoord>) -> Option<()> {
        let (row, _, j) = self.group(id)?;
        if u16_at(row, ROW_HEAD_BYTES + 4 * j) == 0 {
            // A way without nodes has no block, and no code.
            return (u16_at(row, ROW_HEAD_BYTES + 4 * j + 2) == 0).then_some(());
        }
        let b = self.block(row, j, id < GROUP_WAYS as u32)?;
        nodes.reserve(b.cnt);
        nodes.push(NodeCoord { lat: f32::from_bits(b.lat), lng: f32::from_bits(b.lng) });
        let (mut lat, mut lng) = (b.key_lat, b.key_lng);
        let (wl, wg) = (b.wl as usize, b.wg as usize);
        let lng_at = (b.cnt - 1) * wl;
        let (mut step_lat, mut step_lng) = (0u32, 0u32);
        for k in 0..b.cnt - 1 {
            step_lat = step_lat.wrapping_add(unzigzag32(bits_at(b.bits, k * wl, b.wl)?));
            step_lng = step_lng.wrapping_add(unzigzag32(bits_at(b.bits, lng_at + k * wg, b.wg)?));
            lat = lat.wrapping_add(step_lat);
            lng = lng.wrapping_add(step_lng);
            let (lat_bits, lng_bits) = if b.hybrid {
                // A hybrid key past the largest is no coordinate's.
                if !is_hybrid_key(lat) || !is_hybrid_key(lng) {
                    return None;
                }
                (from_hybrid_key(lat), from_hybrid_key(lng))
            } else {
                (from_order_key(lat), from_order_key(lng))
            };
            nodes.push(NodeCoord { lat: f32::from_bits(lat_bits), lng: f32::from_bits(lng_bits) });
        }
        Some(())
    }
}

/// The two files of a kind (fixtures and tests).
#[cfg(any(test, feature = "fixtures"))]
pub struct WayFileBytes {
    pub ways: Vec<u8>,
    pub nodes: Vec<u8>,
}

/// The files of ways in id order, each with its nodes, as the builder's encoder writes them
/// (fixtures and tests). Panics on a way whose node count is not its nodes', and on more nodes
/// than a way or a nodes file holds.
#[cfg(any(test, feature = "fixtures"))]
pub fn encode(kind: Kind, ways: &[(WayRow, Vec<NodeCoord>)]) -> WayFileBytes {
    let (mut rows, mut blocks) = (Vec::new(), Vec::new());
    let mut total = 0u64;
    for group in ways.chunks(GROUP_WAYS) {
        // The first node of the group's first way with nodes.
        let anchor = group.iter().find_map(|(_, nodes)| nodes.first()).map_or((0, 0), |c| (c.lat.to_bits(), c.lng.to_bits()));
        rows.extend_from_slice(&u32::try_from(blocks.len()).expect("a nodes file below 4 GiB").to_le_bytes());
        rows.extend_from_slice(&anchor.0.to_le_bytes());
        rows.extend_from_slice(&anchor.1.to_le_bytes());
        for (row, nodes) in group {
            assert_eq!(usize::from(row.node_count), nodes.len(), "a way's node count is its nodes'");
            let code = if nodes.is_empty() { 0 } else { encode_block(nodes, anchor, &mut blocks) };
            rows.extend_from_slice(&row.node_count.to_le_bytes());
            rows.extend_from_slice(&code.to_le_bytes());
            total += nodes.len() as u64;
        }
        if kind == Kind::Interps {
            for (row, _) in group {
                rows.extend_from_slice(&row.start_number.to_le_bytes());
                rows.extend_from_slice(&row.end_number.wrapping_sub(row.start_number).to_le_bytes());
                rows.push(row.interpolation);
            }
        }
        for (row, _) in group {
            rows.extend_from_slice(&row.name.to_le_bytes());
        }
    }
    let count = u32::try_from(ways.len()).expect("fewer than 2^32 ways");
    rows.extend_from_slice(&count.to_le_bytes());
    rows.extend_from_slice(kind.ways_magic().as_bytes());
    blocks.extend_from_slice(&total.to_le_bytes());
    blocks.extend_from_slice(&count.to_le_bytes());
    blocks.extend_from_slice(kind.nodes_magic().as_bytes());
    WayFileBytes { ways: rows, nodes: blocks }
}

/// A way's node block under its group's anchor, appended to `out`; its code.
#[cfg(any(test, feature = "fixtures"))]
fn encode_block(nodes: &[NodeCoord], anchor: (u32, u32), out: &mut Vec<u8>) -> u16 {
    use crate::coord_keys::{bit_length, zigzag32, BitWriter};
    let bits = |c: &NodeCoord| (c.lat.to_bits(), c.lng.to_bits());
    let (lat0, lng0) = bits(&nodes[0]);
    let zl = zigzag32(order_key(lat0).wrapping_sub(order_key(anchor.0)));
    let zg = zigzag32(order_key(lng0).wrapping_sub(order_key(anchor.1)));
    let wf = byte_length(zl).max(byte_length(zg));
    out.extend_from_slice(&zl.to_le_bytes()[..wf as usize]);
    out.extend_from_slice(&zg.to_le_bytes()[..wf as usize]);
    // The hybrid key when every coordinate of the way has one, else the order key.
    let hybrid_keys: Option<Vec<(u32, u32)>> = nodes.iter().map(|c| Some((hybrid_key(bits(c).0)?, hybrid_key(bits(c).1)?))).collect();
    let hybrid = hybrid_keys.is_some();
    let keys = hybrid_keys.unwrap_or_else(|| nodes.iter().map(|c| (order_key(bits(c).0), order_key(bits(c).1))).collect());
    // Node 1's step, then each later node's step less the one before it.
    let (mut step_lat, mut step_lng) = (0u32, 0u32);
    let values: Vec<(u32, u32)> = keys
        .windows(2)
        .map(|w| {
            let (dl, dg) = (w[1].0.wrapping_sub(w[0].0), w[1].1.wrapping_sub(w[0].1));
            let v = (zigzag32(dl.wrapping_sub(step_lat)), zigzag32(dg.wrapping_sub(step_lng)));
            (step_lat, step_lng) = (dl, dg);
            v
        })
        .collect();
    let wl = bit_length(values.iter().fold(0, |any, v| any | v.0));
    let wg = bit_length(values.iter().fold(0, |any, v| any | v.1));
    let mut packed = BitWriter::default();
    values.iter().for_each(|v| packed.put(v.0, wl));
    values.iter().for_each(|v| packed.put(v.1, wg));
    out.extend_from_slice(&packed.finish());
    (wl | wg << 6 | u32::from(hybrid) << 12 | wf << 13) as u16
}

#[cfg(test)]
mod tests;
