//! The postal cities as the builder writes them (builder/src/postal_cities.h, which lays the format
//! out; builder/src/postal_cities_build.h says how each name is chosen): postal_cities.bin, per
//! postcode of the countries it covers the place name mail to it is addressed with, as rows of
//! country, code and name ascending by (country, code), in blocks of at most BLOCK_ROWS rows of one
//! country with a table of each block's country, rows and offset, and a footer.
//!
//! The reader checks the footer and the table at open, and reads a block without trusting it: a
//! malformed block reads as no name or a wrong one, never a panic, and a block of more bytes than
//! its rows can hold, or a name its block does not end, reads as none, as the patch tools refuse
//! them.

use crate::FileBytes;

pub const FILE: &str = "postal_cities.bin";
const MAGIC: &[u8; 4] = b"PCY1";
pub const BLOCK_ROWS: usize = 64;
const ENTRY_BYTES: usize = 8;
const FOOTER_BYTES: usize = 12;
/// The longest code and the longest name.
pub const CODE_BYTES: usize = 14;
pub const NAME_BYTES: usize = 48;

/// The most bytes a block of `rows` rows can have (postal_cities.h's max_block_bytes): a byte, a
/// code and an index a row, and a name of NAME_BYTES and its 0 a row.
const fn max_block_bytes(rows: usize) -> usize {
    rows * (2 + CODE_BYTES + NAME_BYTES + 1)
}

/// A postcode's row as the patch tools diff it (postal_cities.h's LogicalRow): its country (two
/// upper-case letters), its code and its name.
#[derive(Clone, Debug, PartialEq)]
pub struct LogicalRow {
    pub country_code: u16,
    pub code: String,
    pub name: String,
}

#[inline(always)]
fn u16_at(b: &[u8], at: usize) -> u16 {
    u16::from_le_bytes(b[at..at + 2].try_into().unwrap())
}

#[inline(always)]
fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().unwrap())
}

/// The file in place.
pub struct PostalCities {
    file: FileBytes,
    rows: u32,
    blocks: u32,
    table_at: usize,
}

/// A block's rows as they sit: each row's code (its bytes in a buffer of CODE_BYTES and its
/// length) and the index of its name, and where its names start.
struct Block<'a> {
    country: u16,
    codes: [([u8; CODE_BYTES], usize); BLOCK_ROWS],
    names_of: [u8; BLOCK_ROWS],
    rows: usize,
    names: &'a [u8],
}

impl<'a> Block<'a> {
    fn code(&self, i: usize) -> &[u8] {
        &self.codes[i].0[..self.codes[i].1]
    }

    /// Row i's name: the block's names' entry of its index, None where the block has no such
    /// name ended by its 0 byte, or it is no UTF-8.
    fn name(&self, i: usize) -> Option<&'a str> {
        let mut rest = self.names;
        for _ in 0..self.names_of[i] {
            rest = &rest[rest.iter().position(|&b| b == 0)? + 1..];
        }
        let name = &rest[..rest.iter().position(|&b| b == 0)?];
        if name.is_empty() || name.len() > NAME_BYTES {
            return None;
        }
        std::str::from_utf8(name).ok()
    }
}

impl PostalCities {
    /// The file of a dir, mapped; an error for one no builder writes (a bad footer, a table past
    /// the file or out of order), which a block's bytes are not checked for.
    pub fn open(file: FileBytes) -> Result<PostalCities, String> {
        let b = file.as_slice();
        if b.len() < FOOTER_BYTES || &b[b.len() - 4..] != MAGIC {
            return Err(format!("{FILE} has no PCY1 footer"));
        }
        let f = &b[b.len() - FOOTER_BYTES..];
        let (rows, blocks) = (u32_at(f, 0), u32_at(f, 4));
        if rows == u32::MAX || blocks > rows || (blocks as u64) * (BLOCK_ROWS as u64) < rows as u64 {
            return Err(format!("{FILE}'s footer is not one the builder writes"));
        }
        let table_at = (b.len() - FOOTER_BYTES)
            .checked_sub(blocks as usize * ENTRY_BYTES)
            .ok_or_else(|| format!("{FILE} is shorter than its block table"))?;
        let (mut held, mut last_offset, mut last_country) = (0u64, 0usize, 0u16);
        for k in 0..blocks as usize {
            let e = &b[table_at + k * ENTRY_BYTES..table_at + (k + 1) * ENTRY_BYTES];
            let (country, n, offset) = (u16_at(e, 0), e[2] as usize, u32_at(e, 4) as usize);
            if n == 0 || n > BLOCK_ROWS || e[3] != 0 || offset < last_offset || offset > table_at || country < last_country {
                return Err(format!("{FILE}'s block table is not one the builder writes"));
            }
            (held, last_offset, last_country) = (held + n as u64, offset, country);
        }
        if held != rows as u64 || (rows == 0 && table_at != 0) {
            return Err(format!("{FILE}'s blocks hold other than its footer's rows"));
        }
        Ok(PostalCities { file, rows, blocks, table_at })
    }

    /// The rows: postcodes the table names.
    pub fn rows(&self) -> u32 {
        self.rows
    }

    /// Block b's country, its rows and its bytes.
    fn entry(&self, b: u32) -> Option<(u16, usize, &[u8])> {
        if b >= self.blocks {
            return None;
        }
        let f = self.file.as_slice();
        let at = self.table_at + b as usize * ENTRY_BYTES;
        let start = u32_at(f, at + 4) as usize;
        let end = if b + 1 < self.blocks { u32_at(f, at + ENTRY_BYTES + 4) as usize } else { self.table_at };
        Some((u16_at(f, at), f[at + 2] as usize, f.get(start..end.min(self.table_at))?))
    }

    /// Block b's rows; None where it reads as malformed, or has more bytes than its rows can.
    fn block(&self, b: u32) -> Option<Block<'_>> {
        let (country, rows, bytes) = self.entry(b)?;
        if rows == 0 || rows > BLOCK_ROWS || bytes.len() > max_block_bytes(rows) {
            return None;
        }
        let mut block = Block { country, codes: [([0; CODE_BYTES], 0); BLOCK_ROWS], names_of: [0; BLOCK_ROWS], rows, names: &[] };
        let mut at = 0usize;
        for i in 0..rows {
            let head = *bytes.get(at)?;
            at += 1;
            let (shared, suffix) = ((head >> 4) as usize, (head & 0x0F) as usize);
            if (i == 0 && shared > 0) || suffix == 0 || shared + suffix > CODE_BYTES || (i > 0 && shared > block.codes[i - 1].1) {
                return None;
            }
            let mut code = if i > 0 { block.codes[i - 1].0 } else { [0; CODE_BYTES] };
            code[shared..shared + suffix].copy_from_slice(bytes.get(at..at + suffix)?);
            at += suffix;
            block.codes[i] = (code, shared + suffix);
            block.names_of[i] = *bytes.get(at)?;
            at += 1;
        }
        block.names = &bytes[at..];
        Some(block)
    }

    /// The name of the postcode `code` of the country `country` (its two upper-case letters):
    /// None where the table has no row of it, or the row's block reads as malformed.
    pub fn name(&self, country: [u8; 2], code: &str) -> Option<&str> {
        let (country, code) = (u16::from_be_bytes(country), code.as_bytes());
        // The last block whose first row is at or before the postcode.
        let (mut lo, mut hi) = (0u32, self.blocks);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let (c, _, bytes) = self.entry(mid)?;
            let first = bytes.first().and_then(|&h| bytes.get(1..1 + (h & 0x0F) as usize)).unwrap_or(&[]);
            if (c, first) <= (country, code) { lo = mid + 1 } else { hi = mid }
        }
        let block = self.block(lo.checked_sub(1)?)?;
        if block.country != country {
            return None;
        }
        let i = (0..block.rows).find(|&i| block.code(i) == code)?;
        block.name(i)
    }

    /// Calls `f` with each row in order, a block read whole; false where a block reads as
    /// malformed (its rows before that given).
    pub fn for_rows(&self, mut f: impl FnMut(LogicalRow)) -> bool {
        (0..self.blocks).all(|b| {
            let Some(block) = self.block(b) else { return false };
            (0..block.rows).all(|i| {
                let (Ok(code), Some(name)) = (std::str::from_utf8(block.code(i)), block.name(i)) else { return false };
                f(LogicalRow { country_code: block.country, code: code.to_string(), name: name.to_string() });
                true
            })
        })
    }
}

/// The code a postcode of the country `country` (upper-case) is a row of, as the builder takes it
/// (postcode_grid_build.h's grid_postcode, Nominatim's pattern and output): for the United States
/// the five digits of `ddddd` or `ddddd-dddd`, after an optional "US" and a space or '-', not all
/// of 0 and '-'. None for every other country and postcode.
pub fn postal_key<'p>(country: [u8; 2], postcode: &'p str) -> Option<&'p str> {
    if country != *b"US" {
        return None;
    }
    let s = postcode.trim();
    let zip = |g: &'p str| -> Option<&'p str> {
        let b = g.as_bytes();
        let digits = |r: std::ops::Range<usize>| b[r].iter().all(u8::is_ascii_digit);
        let five = b.len() == 5 && digits(0..5);
        let nine = b.len() == 10 && digits(0..5) && b[5] == b'-' && digits(6..10);
        ((five || nine) && b.iter().any(|&c| c != b'0' && c != b'-')).then(|| &g[..5])
    };
    if let Some(z) = zip(s) {
        return Some(z);
    }
    let rest = s.get(..2).filter(|p| p.eq_ignore_ascii_case("US")).map(|_| &s[2..])?;
    zip(rest).or_else(|| rest.strip_prefix([' ', '-']).and_then(zip))
}

/// The file the builder's encoder writes for `rows` (postal_cities.h), for the fixtures: rows
/// ascending by (country, code).
#[cfg(any(test, feature = "fixtures"))]
pub fn encode(rows: &[LogicalRow]) -> Vec<u8> {
    let (mut out, mut table, mut blocks) = (Vec::new(), Vec::new(), 0u32);
    let mut start = 0;
    while start < rows.len() {
        let mut end = start + 1;
        while end < rows.len() && end - start < BLOCK_ROWS && rows[end].country_code == rows[start].country_code {
            end += 1;
        }
        let block = &rows[start..end];
        table.extend_from_slice(&block[0].country_code.to_le_bytes());
        table.extend_from_slice(&[block.len() as u8, 0]);
        table.extend_from_slice(&(out.len() as u32).to_le_bytes());
        let mut names: Vec<&str> = block.iter().map(|r| r.name.as_str()).collect();
        names.sort();
        names.dedup();
        for (i, r) in block.iter().enumerate() {
            let code = r.code.as_bytes();
            let shared = if i == 0 {
                0
            } else {
                block[i - 1].code.as_bytes().iter().zip(code).take_while(|(a, b)| a == b).count()
            };
            out.push((shared << 4 | (code.len() - shared)) as u8);
            out.extend_from_slice(&code[shared..]);
            out.push(names.binary_search(&r.name.as_str()).unwrap() as u8);
        }
        for name in &names {
            out.extend_from_slice(name.as_bytes());
            out.push(0);
        }
        blocks += 1;
        start = end;
    }
    out.extend_from_slice(&table);
    out.extend_from_slice(&(rows.len() as u32).to_le_bytes());
    out.extend_from_slice(&blocks.to_le_bytes());
    out.extend_from_slice(MAGIC);
    out
}

#[cfg(test)]
mod tests;
