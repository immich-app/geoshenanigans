//! A side file as the builder writes it (builder/src/side_blocks.h, which lays the format out):
//! a u32 for each record of the record file it sits beside (way_parents.bin, way_postcodes.bin,
//! interp_postcodes.bin, addr_postcodes.bin, addr_cities.bin), in blocks of BLOCK_VALUES values,
//! each FOR or a dictionary, with a table of the blocks' offsets and a footer after them. A
//! value's id is its record's, and it is read from its block in place. NO_DATA is a value like
//! any other in SDB1. An STG1 tag column stores only the present rows of each block.

use crate::coord_keys::{bit_length, bits_at};
use crate::FileBytes;

pub const BLOCK_VALUES: usize = 64;
const TABLE_ENTRY_BYTES: usize = 4;
const FOOTER_BYTES: usize = 8;
const FOR_HEAD_BYTES: usize = 6;
/// FOR at 32 bits a value; a dictionary is written only where it is smaller.
const BLOCK_MAX_BYTES: usize = FOR_HEAD_BYTES + BLOCK_VALUES * 4;
const MODE_FOR: u8 = 0;
const MODE_DICTIONARY: u8 = 1;
const MAGIC: &[u8; 4] = b"SDB1";
const TAG_MAGIC: &[u8; 4] = b"STG1";
const MODE_PRESENT: u8 = 2;
const PRESENCE_BYTES: usize = 1 + std::mem::size_of::<u64>();

#[inline(always)]
fn u32_at(b: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_le_bytes(*b.get(at..)?.first_chunk::<4>()?))
}

/// The file in place: a value read from its block through the block table.
pub struct SideBlocks {
    bytes: FileBytes,
    count: u32,
    table_at: usize,
    tags: bool,
}

enum Base<'a> {
    Frame(u32),
    Dictionary(&'a [u8]),
}
struct Block<'a> {
    base: Base<'a>,
    fields: &'a [u8],
    width: u32,
    presence: Option<u64>,
    tags: bool,
}
impl Block<'_> {
    fn get(&self, row: usize) -> Option<u32> {
        let row = if let Some(mask) = self.presence {
            let bit = 1u64 << row;
            if mask & bit == 0 {
                return None;
            }
            (mask & (bit - 1)).count_ones() as usize
        } else {
            row
        };
        let value = bits_at(self.fields, row * self.width as usize, self.width)?;
        match &self.base {
            Base::Frame(base) => base.checked_add(value),
            Base::Dictionary(entries) => u32_at(entries, (value as usize).checked_mul(size_of::<u32>())?),
        }
        .filter(|&value| !self.tags || value != crate::NO_DATA)
    }
}

impl SideBlocks {
    /// The file `name` of a dir, mapped; an error that names it for one no builder writes (a bad
    /// footer, a table past the file, blocks of a size no blocks of that many values have), which
    /// a block's bytes are not checked for.
    pub fn open(bytes: FileBytes, name: &str) -> Result<SideBlocks, String> {
        Self::open_column(bytes, name, false)
    }

    pub fn open_tags(bytes: FileBytes, name: &str) -> Result<Self, String> {
        Self::open_column(bytes, name, true)
    }

    fn open_column(bytes: FileBytes, name: &str, tags: bool) -> Result<Self, String> {
        let b = bytes.as_slice();
        let (magic, min_block, max_block) = if tags {
            (TAG_MAGIC, 0, BLOCK_MAX_BYTES + PRESENCE_BYTES)
        } else {
            (MAGIC, FOR_HEAD_BYTES, BLOCK_MAX_BYTES)
        };
        let no_footer = || format!("{name} has no {} footer", if tags { "STG1" } else { "SDB1" });
        let footer_at = b.len().checked_sub(FOOTER_BYTES).ok_or_else(no_footer)?;
        let count = u32_at(b, footer_at).filter(|_| &b[footer_at + 4..] == magic).ok_or_else(no_footer)?;
        let blocks = (count as usize).div_ceil(BLOCK_VALUES);
        let table_at = footer_at
            .checked_sub(blocks * TABLE_ENTRY_BYTES)
            .ok_or_else(|| format!("{name} is shorter than its block table"))?;
        // A dense block needs its FOR header. An empty tag block uses no bytes.
        if table_at < blocks * min_block || table_at as u64 > blocks as u64 * max_block as u64 {
            return Err(format!("{name} has blocks of a size its values cannot take"));
        }
        Ok(SideBlocks { bytes, count, table_at, tags })
    }

    /// The values: ids below this.
    pub fn count(&self) -> u32 {
        self.count
    }

    /// Value `id`; None past the last, and where the table or its block gives none (bytes no
    /// builder writes): a block that is not the bytes from its start to the next block's, a
    /// mode or a width no block has, a value past 32 bits, an index past the dictionary.
    pub fn get(&self, id: u32) -> Option<u32> {
        if id >= self.count {
            return None;
        }
        self.block(id as usize / BLOCK_VALUES)?.get(id as usize % BLOCK_VALUES)
    }

    /// Values in record order. A damaged value occupies its original row.
    pub fn values(&self) -> impl Iterator<Item = Option<u32>> + '_ {
        let mut current = None;
        (0..self.count as usize).map(move |row| {
            let j = row % BLOCK_VALUES;
            if j == 0 {
                current = self.block(row / BLOCK_VALUES);
            }
            current.as_ref()?.get(j)
        })
    }

    fn block(&self, b: usize) -> Option<Block<'_>> {
        let file = self.bytes.as_slice();
        let entry = self.table_at + b * TABLE_ENTRY_BYTES;
        let at = u32_at(file, entry)? as usize;
        // The block's values, and where it ends: BLOCK_VALUES and the next block's start, or
        // for the last block the values left and the table's start.
        let left = self.count as usize - b * BLOCK_VALUES;
        let (c, end) = if left > BLOCK_VALUES { (BLOCK_VALUES, u32_at(file, entry + TABLE_ENTRY_BYTES)? as usize) } else { (left, self.table_at) };
        // The first block starts where the file does.
        if b == 0 && at != 0 {
            return None;
        }
        // The blocks end where the table starts: a block is none of the table's bytes.
        let mut block = file.get(..self.table_at)?.get(at..end)?;
        let (mut at, mut c) = (at, c);
        let presence = if self.tags && *block.first()? == MODE_PRESENT {
            let mask = u64::from_le_bytes(*block.get(1..)?.first_chunk::<8>()?);
            let present = mask.count_ones() as usize;
            if present == 0 || present >= c || (c < BLOCK_VALUES && mask >> c != 0) {
                return None;
            }
            c = present;
            at += PRESENCE_BYTES;
            block = block.get(PRESENCE_BYTES..)?;
            Some(mask)
        } else {
            None
        };
        let (base, head, width) = match *block.first()? {
            MODE_FOR => (Base::Frame(u32_at(block, 1)?), FOR_HEAD_BYTES, *block.get(FOR_HEAD_BYTES - 1)? as u32),
            MODE_DICTIONARY => {
                let k = *block.get(1)? as usize;
                if k == 0 || k > c {
                    return None;
                }
                let head = 3 + size_of::<u32>() * k;
                let width = *block.get(head - 1)? as u32;
                if width != bit_length(k as u32 - 1) {
                    return None;
                }
                (Base::Dictionary(&block[2..head - 1]), head, width)
            }
            _ => return None,
        };
        if width > u32::BITS || block.len() != head + (c * width as usize).div_ceil(8) {
            return None;
        }
        // bits_at takes an eight-byte load. Following blocks and the footer supply the padding.
        Some(Block { base, fields: file.get(at + head..)?, width, presence, tags: self.tags })
    }
}

/// The file's bytes for `values` (fixtures and tests), the builder's encoding.
#[cfg(any(test, feature = "fixtures"))]
pub fn encode(values: &[u32]) -> Vec<u8> {
    encode_column(values, false)
}

#[cfg(any(test, feature = "fixtures"))]
pub fn encode_tags(values: &[u32]) -> Vec<u8> {
    encode_column(values, true)
}

#[cfg(any(test, feature = "fixtures"))]
fn encode_column(values: &[u32], tags: bool) -> Vec<u8> {
    use crate::coord_keys::BitWriter;
    let (mut blocks, mut table) = (Vec::new(), Vec::new());
    for chunk in values.chunks(BLOCK_VALUES) {
        table.extend((blocks.len() as u32).to_le_bytes());
        let packed;
        let chunk = if tags {
            let mut mask = 0u64;
            packed = chunk.iter().copied().enumerate().filter_map(|(i, v)| {
                (v != crate::NO_DATA).then(|| { mask |= 1 << i; v })
            }).collect::<Vec<_>>();
            if packed.is_empty() {
                continue;
            }
            if packed.len() != chunk.len() {
                blocks.push(MODE_PRESENT);
                blocks.extend(mask.to_le_bytes());
            }
            packed.as_slice()
        } else {
            chunk
        };
        let least = *chunk.iter().min().unwrap();
        let wf = bit_length(chunk.iter().max().unwrap() - least);
        // The distinct values in the order they first come, and each value's index among them.
        let mut entries: Vec<u32> = Vec::new();
        let indexes: Vec<u32> = chunk
            .iter()
            .map(|v| {
                let known = entries.iter().position(|e| e == v);
                known.unwrap_or_else(|| {
                    entries.push(*v);
                    entries.len() - 1
                }) as u32
            })
            .collect();
        let wd = bit_length(entries.len() as u32 - 1);
        let dictionary_bits = (3 + 4 * entries.len()) * 8 + chunk.len() * wd as usize;
        let for_bits = FOR_HEAD_BYTES * 8 + chunk.len() * wf as usize;
        let mut bits = BitWriter::default();
        if dictionary_bits < for_bits {
            blocks.extend([MODE_DICTIONARY, entries.len() as u8]);
            for e in &entries {
                blocks.extend(e.to_le_bytes());
            }
            blocks.push(wd as u8);
            for x in indexes {
                bits.put(x, wd);
            }
        } else {
            blocks.push(MODE_FOR);
            blocks.extend(least.to_le_bytes());
            blocks.push(wf as u8);
            for v in chunk {
                bits.put(v - least, wf);
            }
        }
        blocks.extend(bits.finish());
    }
    blocks.extend(table);
    blocks.extend((values.len() as u32).to_le_bytes());
    blocks.extend(if tags { TAG_MAGIC } else { MAGIC });
    blocks
}

#[cfg(test)]
mod tests;
