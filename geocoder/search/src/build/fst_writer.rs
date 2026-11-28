//! A streaming writer for the fst crate's map format. The general-purpose builder retains
//! the largest transition array ever seen in each cache cell. The core shares its device
//! budget with mapped record tables and merge buffers, so it caches only small states.
use crate::{
    constants::*,
    index::invalid,
    keys::{COMMON_INPUTS, TRANS_INDEX_THRESHOLD, VERSION},
};
use std::io::{self, Write};

#[derive(Clone, Copy, Default, PartialEq, Eq)]
struct Edge {
    byte: u8,
    addr: u64,
    output: u64,
}

#[derive(Default)]
struct Node {
    value: Option<u64>,
    edges: Vec<Edge>,
}

#[derive(Clone, Copy)]
struct Cached {
    addr: u64,
    value: Option<u64>,
    edges: [Edge; CORE_FST_CACHE_ARCS],
    count: usize,
}

const COMMON_CODES: [u8; FST_ALPHABET] = {
    let mut codes = [0; FST_ALPHABET];
    let mut i = 0;
    while i < COMMON_INPUTS.len() {
        codes[COMMON_INPUTS[i] as usize] = i as u8 + 1;
        i += 1;
    }
    codes
};
const CRC_TABLE: [u32; FST_ALPHABET] = {
    let mut table = [0; FST_ALPHABET];
    let mut i = 0;
    while i < table.len() {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < u8::BITS {
            crc = (crc >> 1) ^ if crc & 1 != 0 { CRC32C_REVERSED } else { 0 };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
};

struct NodeWriter<W> {
    out: W,
    position: u64,
    crc: u32,
    buf: Vec<u8>,
    cache: Vec<Option<Cached>>,
}
impl<W: Write> NodeWriter<W> {
    fn write_buf(&mut self) -> io::Result<()> {
        self.out.write_all(&self.buf)?;
        for &byte in &self.buf {
            self.crc = (self.crc >> 8) ^ CRC_TABLE[usize::from(self.crc as u8 ^ byte)];
        }
        self.position += self.buf.len() as u64;
        self.buf.clear();
        Ok(())
    }
    fn node(&mut self, node: &Node, root: bool) -> io::Result<u64> {
        if node.value == Some(0) && node.edges.is_empty() {
            return Ok(0);
        }
        let mut saved = None;
        if !root && node.edges.len() <= CORE_FST_CACHE_ARCS {
            let mut hash = node.value.unwrap_or_default();
            for edge in &node.edges {
                for field in [u64::from(edge.byte), edge.addr, edge.output] {
                    hash = (hash ^ field).wrapping_mul(FST_HASH_PRIME);
                }
            }
            let bucket = (hash ^ (hash >> u32::BITS)) as usize % self.cache.len();
            if let Some(hit) = &self.cache[bucket] {
                if hit.value == node.value && hit.edges[..hit.count] == node.edges {
                    return Ok(hit.addr);
                }
            }
            saved = Some(bucket);
        }
        self.encode_node(node);
        self.write_buf()?;
        let addr = self.position - 1;
        if let Some(bucket) = saved {
            let mut edges = [Edge::default(); CORE_FST_CACHE_ARCS];
            edges[..node.edges.len()].copy_from_slice(&node.edges);
            self.cache[bucket] = Some(Cached { addr, value: node.value, edges, count: node.edges.len() });
        }
        Ok(addr)
    }
    fn encode_node(&mut self, node: &Node) {
        fn width(value: u64) -> usize {
            (u64::BITS - value.leading_zeros()).div_ceil(u8::BITS) as usize
        }
        let start = self.position;
        let distance = |addr: u64| if addr == 0 { 0 } else { start - addr };
        if let (None, [e]) = (node.value, node.edges.as_slice()) {
            let adjacent = e.addr != 0 && e.addr + 1 == start && e.output == 0;
            if !adjacent {
                let delta = distance(e.addr);
                let delta_bytes = width(delta).max(1);
                let output_bytes = width(e.output);
                self.buf.extend_from_slice(&e.output.to_le_bytes()[..output_bytes]);
                self.buf.extend_from_slice(&delta.to_le_bytes()[..delta_bytes]);
                self.buf.push(((delta_bytes << FST_SIZE_SHIFT) | output_bytes) as u8);
            }
            let code = COMMON_CODES[e.byte as usize];
            if code == 0 {
                self.buf.push(e.byte);
            }
            self.buf.push((if adjacent { FST_NEXT_FLAG } else { FST_ONE_FLAG }) | code);
            return;
        }
        // Multi-edge nodes are read backwards from their state byte. Large alphabets
        // include a byte-to-transition index directly below that state.
        let value = node.value.unwrap_or_default();
        let max_output = node.edges.iter().map(|e| e.output).max().unwrap_or(0).max(value);
        let output_bytes = width(max_output);
        let delta_bytes = node.edges.iter().map(|e| width(distance(e.addr)).max(1)).max().unwrap_or(0);
        if node.value.is_some() {
            self.buf.extend_from_slice(&value.to_le_bytes()[..output_bytes]);
        }
        for e in node.edges.iter().rev() {
            self.buf.extend_from_slice(&e.output.to_le_bytes()[..output_bytes]);
        }
        for e in node.edges.iter().rev() {
            self.buf.extend_from_slice(&distance(e.addr).to_le_bytes()[..delta_bytes]);
        }
        self.buf.extend(node.edges.iter().rev().map(|e| e.byte));
        let n = node.edges.len();
        if n > TRANS_INDEX_THRESHOLD {
            let mut index = [u8::MAX; FST_ALPHABET];
            for (i, e) in node.edges.iter().enumerate() {
                index[e.byte as usize] = i as u8;
            }
            self.buf.extend_from_slice(&index);
        }
        self.buf.push(((delta_bytes << FST_SIZE_SHIFT) | output_bytes) as u8);
        let count = if n == 0 || n > usize::from(FST_STATE_COUNT_MASK) {
            self.buf.push(if n == FST_ALPHABET { 1 } else { n as u8 });
            0
        } else {
            n as u8
        };
        self.buf.push(count | if node.value.is_some() { FST_FINAL_FLAG } else { 0 });
    }
}

pub(super) struct FstWriter<W> {
    wire: NodeWriter<W>,
    path: Vec<Node>,
    last: Vec<u8>,
    keys: u64,
}
impl<W: Write> FstWriter<W> {
    pub fn new(out: W) -> io::Result<Self> {
        let mut wire =
            NodeWriter { out, position: 0, crc: u32::MAX, cache: vec![None; CORE_FST_CACHE_SLOTS], buf: Vec::new() };
        wire.buf.extend_from_slice(&VERSION.to_le_bytes());
        wire.buf.extend_from_slice(&0u64.to_le_bytes());
        wire.write_buf()?;
        Ok(Self { wire, path: vec![Node::default()], last: Vec::new(), keys: 0 })
    }
    pub fn insert(&mut self, key: impl AsRef<[u8]>, value: u64) -> io::Result<()> {
        let key = key.as_ref();
        if self.keys > 0 && key <= self.last.as_slice() {
            return Err(invalid("out-of-order core fst key"));
        }
        let common = self.last.iter().zip(key).take_while(|(a, b)| a == b).count();
        self.close(common)?;
        if self.path.len() <= key.len() {
            self.path.resize_with(key.len() + 1, Node::default);
        }
        self.path[key.len()].value = Some(value);
        self.last.clear();
        self.last.extend_from_slice(key);
        self.keys += 1;
        Ok(())
    }
    // Pull the smallest output of a closed subtree up to its incoming edge.
    // Equal residual suffixes can then point to the same node.
    fn close(&mut self, keep: usize) -> io::Result<()> {
        for depth in (keep + 1..=self.last.len()).rev() {
            let node = &mut self.path[depth];
            let base = node.value.into_iter().chain(node.edges.iter().map(|e| e.output)).min().unwrap();
            if let Some(value) = &mut node.value {
                *value -= base;
            }
            for edge in &mut node.edges {
                edge.output -= base;
            }
            let addr = self.wire.node(node, false)?;
            node.value = None;
            node.edges.clear();
            self.path[depth - 1].edges.push(Edge { byte: self.last[depth - 1], addr, output: base });
        }
        Ok(())
    }
    pub fn finish(mut self) -> io::Result<W> {
        self.close(0)?;
        let root = self.wire.node(&self.path[0], true)?;
        self.wire.buf.extend_from_slice(&self.keys.to_le_bytes());
        self.wire.buf.extend_from_slice(&root.to_le_bytes());
        self.wire.write_buf()?;
        let sum = (!self.wire.crc).rotate_right(SNAPPY_CRC_ROTATION).wrapping_add(SNAPPY_CRC_MASK);
        self.wire.out.write_all(&sum.to_le_bytes())?;
        self.wire.out.flush()?;
        Ok(self.wire.out)
    }
}

#[cfg(test)]
mod tests {
    use super::FstWriter;
    use fst::Streamer;
    use std::collections::BTreeMap;

    #[test]
    fn a_key_cannot_reopen_an_already_written_path() {
        let mut output = FstWriter::new(Vec::new()).unwrap();
        output.insert(b"town", 0).unwrap();
        assert!(output.insert(b"town", 7).is_err());
        assert!(output.insert(b"to", 1).is_err());
        output.insert(b"township", 2).unwrap();
        let map = fst::Map::new(output.finish().unwrap()).unwrap();
        assert_eq!((map.get("town"), map.get("township")), (Some(0), Some(2)));
    }

    #[test]
    fn a_bounded_writer_produces_verified_standard_maps() {
        let mut entries = BTreeMap::from([(vec![], 17), (b"prefix".to_vec(), u64::MAX)]);
        for byte in 0..=255u8 {
            entries.insert(vec![b'p', byte], u64::from(byte));
            entries.insert(vec![b'p', byte, b'z'], 10000 - u64::from(byte));
        }
        for n in 0..80_000u32 {
            entries.insert([b"term".as_slice(), &n.to_be_bytes()].concat(), u64::from(n.reverse_bits()));
        }
        for rows in [BTreeMap::new(), BTreeMap::from([(vec![], 0)]), entries] {
            let mut output = FstWriter::new(Vec::new()).unwrap();
            for (key, &value) in &rows {
                output.insert(key, value).unwrap();
            }
            let map = fst::Map::new(output.finish().unwrap()).unwrap();
            map.as_fst().verify().unwrap();
            assert_eq!(map.len(), rows.len());
            let mut stream = map.stream();
            for (key, value) in rows {
                assert_eq!(stream.next(), Some((key.as_slice(), value)));
            }
            assert_eq!(stream.next(), None);
        }
    }
}
