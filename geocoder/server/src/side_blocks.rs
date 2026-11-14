//! A side file as the builder writes it (builder/src/side_blocks.h, which lays the format out):
//! a u32 for each record of the record file it sits beside (way_parents.bin, way_postcodes.bin,
//! interp_postcodes.bin, addr_postcodes.bin), in blocks of BLOCK_VALUES values, each FOR or a
//! dictionary, with a table of the blocks' offsets and a footer after them. A value's id is its
//! record's, and it is read from its block in place. NO_DATA is a value like any other.

use crate::coord_keys::bits_at;
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

#[inline(always)]
fn u32_at(b: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_le_bytes(*b.get(at..)?.first_chunk::<4>()?))
}

/// The file in place: a value read from its block through the block table.
pub struct SideBlocks {
    bytes: FileBytes,
    count: u32,
    table_at: usize,
}

impl SideBlocks {
    /// The file `name` of a dir, mapped; an error that names it for one no builder writes (a bad
    /// footer, a table past the file, blocks of a size no blocks of that many values have), which
    /// a block's bytes are not checked for.
    pub fn open(bytes: FileBytes, name: &str) -> Result<SideBlocks, String> {
        let b = bytes.as_slice();
        let no_footer = || format!("{name} has no SDB1 footer");
        let footer_at = b.len().checked_sub(FOOTER_BYTES).ok_or_else(no_footer)?;
        let count = u32_at(b, footer_at).filter(|_| &b[footer_at + 4..] == MAGIC).ok_or_else(no_footer)?;
        let blocks = (count as usize).div_ceil(BLOCK_VALUES);
        let table_at = footer_at
            .checked_sub(blocks * TABLE_ENTRY_BYTES)
            .ok_or_else(|| format!("{name} is shorter than its block table"))?;
        // A block is its FOR head at the least. 2^26 blocks of 262 bytes pass 32 bits.
        if table_at < blocks * FOR_HEAD_BYTES || table_at as u64 > blocks as u64 * BLOCK_MAX_BYTES as u64 {
            return Err(format!("{name} has blocks of a size its values cannot take"));
        }
        Ok(SideBlocks { bytes, count, table_at })
    }

    /// The values: ids below this.
    pub fn count(&self) -> u32 {
        self.count
    }

    /// Value `id`; None past the last, and where the table or its block gives none (bytes no
    /// builder writes).
    pub fn get(&self, id: u32) -> Option<u32> {
        if id >= self.count {
            return None;
        }
        let file = self.bytes.as_slice();
        let (b, j) = (id as usize / BLOCK_VALUES, id as usize % BLOCK_VALUES);
        let at = u32_at(file, self.table_at + b * TABLE_ENTRY_BYTES)? as usize;
        // From the block's first byte to the blocks' end: its own end is not stored.
        let block = file.get(at..self.table_at)?;
        // Field j of the w-bit fields that start `head` bytes into the block: one 8-byte load,
        // which the table and the footer after the blocks leave room for. None for a field past
        // the blocks, and for a width past 32 bits.
        let field = |head: usize, w: u32| {
            let bit = j * w as usize;
            if head + (bit + w as usize).div_ceil(8) > block.len() {
                return None;
            }
            bits_at(file.get(at + head..)?, bit, w)
        };
        match *block.first()? {
            MODE_FOR => {
                let w = *block.get(FOR_HEAD_BYTES - 1)? as u32;
                u32_at(block, 1)?.checked_add(field(FOR_HEAD_BYTES, w)?)
            }
            MODE_DICTIONARY => {
                let k = *block.get(1)? as usize;
                let head = 3 + 4 * k;
                let w = *block.get(head - 1)? as u32;
                let x = field(head, w)? as usize;
                if x >= k {
                    return None;
                }
                u32_at(block, 2 + 4 * x)
            }
            _ => None,
        }
    }
}

/// The file's bytes for `values` (fixtures and tests), the builder's encoding.
#[cfg(any(test, feature = "fixtures"))]
pub fn encode(values: &[u32]) -> Vec<u8> {
    use crate::coord_keys::{bit_length, BitWriter};
    let (mut blocks, mut table) = (Vec::new(), Vec::new());
    for chunk in values.chunks(BLOCK_VALUES) {
        table.extend((blocks.len() as u32).to_le_bytes());
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
    blocks.extend(MAGIC);
    blocks
}

#[cfg(test)]
mod tests;
