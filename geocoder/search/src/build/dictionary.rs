use super::{fst_writer::FstWriter, write::writer};
use crate::{constants::*, varint};
use std::{
    collections::VecDeque,
    fs::File,
    io::{self, BufWriter, Write},
    path::Path,
};

/// The FST routes a search to a small run of adjacent terms. Keeping their suffixes
/// together lets compression share spelling bytes across source words and variants.
pub(super) struct TermBlocks {
    fst: FstWriter<BufWriter<File>>,
    pending: VecDeque<(Vec<u8>, u64)>,
    bytes: usize,
    previous: Vec<u8>,
    encoded: Vec<u8>,
}

fn shared_prefix(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(a, b)| a == b).count()
}

impl TermBlocks {
    pub(super) fn new(out: &Path) -> io::Result<Self> {
        Ok(Self {
            fst: FstWriter::new(writer(&out.join("core.fst"))?)?,
            pending: Default::default(),
            bytes: 0,
            previous: Vec::new(),
            encoded: Vec::new(),
        })
    }
    pub(super) fn add(&mut self, key: &[u8], posting: u64, out: &mut impl Write, pos: &mut u64) -> io::Result<()> {
        while !self.pending.is_empty()
            && (self.pending.len() == TERM_BLOCK_ROWS || self.bytes + key.len() > TERM_BLOCK_BYTES)
        {
            self.flush_block(key, out, pos)?;
        }
        self.pending.push_back((key.to_vec(), posting));
        self.bytes += key.len();
        Ok(())
    }
    fn flush_block(&mut self, next: &[u8], out: &mut impl Write, pos: &mut u64) -> io::Result<()> {
        let first = &self.pending.front().unwrap().0;
        // Take the shortest prefix distinguishing this group from its two neighbours.
        let len = (1 + shared_prefix(first, next).max(shared_prefix(first, &self.previous))).min(first.len());
        let route = &first[..len];
        let n = self.pending.iter().take_while(|(key, _)| key.starts_with(route)).count();
        if n == 1 {
            let (key, posting) = self.pending.pop_front().unwrap();
            self.fst.insert(&key, posting << 1)?;
            self.bytes -= key.len();
            self.previous = key;
        } else {
            self.fst.insert(route, (*pos << 1) | TERM_BLOCK)?;
            self.previous.clear();
            self.previous.extend_from_slice(route);
            self.encoded.clear();
            varint::put(&mut self.encoded, n as u64);
            for (key, posting) in self.pending.drain(..n) {
                let shared = shared_prefix(&self.previous, &key);
                varint::put(&mut self.encoded, (shared - len) as u64);
                varint::put(&mut self.encoded, (key.len() - shared) as u64);
                self.encoded.extend_from_slice(&key[shared..]);
                let value = if posting & INLINE_POSTING != 0 { posting } else { (*pos - (posting >> 1)) << 1 };
                varint::put(&mut self.encoded, value);
                self.bytes -= key.len();
                self.previous = key;
            }
            out.write_all(&self.encoded)?;
            *pos += self.encoded.len() as u64;
        }
        Ok(())
    }
    pub(super) fn finish(mut self, out: &mut impl Write, pos: &mut u64) -> io::Result<()> {
        while !self.pending.is_empty() {
            self.flush_block(&[], out, pos)?;
        }
        self.fst.finish()?;
        Ok(())
    }
}
