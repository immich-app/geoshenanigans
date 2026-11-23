//! Fixed-width rows split into columns: the cols kind's files, and the row sections of the other
//! kinds (place_cells.bin, place_hidden.bin and R2's rows), one column a field.

use super::container::{col, invalid, read_tail, Chunk, ColSpec, Input, Out, Section, Source, B, D, R, RAW_D, S};
use std::io::{self, Read, Write};

/// The files a cols unit holds.
pub const COLS_FILES: [&str; 4] = ["admin_polygons.bin", "street_ways.bin", "interp_ways.bin", "addr_index.bin"];

/// A row's fields in order, each its width and the column holding it, and the columns: a column
/// holds one field, or a field the row repeats (addr_index.bin's 64 block starts), its values in
/// row order.
pub(super) struct Layout {
    fields: Vec<(usize, usize)>,
    cols: Vec<ColSpec>,
}

impl Layout {
    /// One column a field.
    pub fn of(specs: &[ColSpec]) -> Layout {
        Layout { fields: specs.iter().enumerate().map(|(c, s)| (s.w, c)).collect(), cols: specs.to_vec() }
    }

    /// The fields as a unit's header gives them: each one's width and column.
    pub fn fields(&self) -> Vec<(u8, u16)> {
        self.fields.iter().map(|&(w, c)| (w as u8, c as u16)).collect()
    }

    /// The layout of a header's fields (read_head checks them), its columns' encodings the
    /// stream's.
    pub fn from_fields(fields: &[(u8, u16)]) -> Layout {
        let mut cols = Vec::new();
        for &(w, c) in fields {
            if c as usize == cols.len() {
                cols.push(col(w as usize, R));
            }
        }
        Layout { fields: fields.iter().map(|&(w, c)| (w as usize, c as usize)).collect(), cols }
    }

    /// `n` times the fields of `group`, after the fields so far, each field of the group in a
    /// column of its own across the n.
    fn repeat(&mut self, n: usize, group: &[ColSpec]) {
        let first = self.cols.len();
        self.cols.extend_from_slice(group);
        for _ in 0..n {
            self.fields.extend(group.iter().enumerate().map(|(k, s)| (s.w, first + k)));
        }
    }

    fn stride(&self) -> usize {
        self.fields.iter().map(|f| f.0).sum()
    }
}

/// A cols file's name as COLS_FILES holds it and its row's layout: admin_polygons.bin's ring
/// offset, name, area, country code, level and override, own place type, a column each; a way
/// group's off and anchor lat and lng (u32), a column each, then a column for each field its 16
/// ways repeat: their counts and codes (u16), an interpolation's starts, spans (u32) and
/// interpolations (u8), their names (u32); addr_index.bin's superblock starts (u64), a column
/// each, then a column for its 64 blocks' starts (u16) and one for their blob starts (u32).
pub(super) fn layout_of(file: &str) -> io::Result<(&'static str, Layout)> {
    let name = COLS_FILES.iter().find(|&&f| f == file).ok_or_else(|| invalid(format!("{file} is no cols file")))?;
    let layout = match *name {
        "admin_polygons.bin" => Layout::of(&[col(4, S), col(4, S), col(4, B), col(2, R), col(1, R), col(1, R)]),
        "street_ways.bin" | "interp_ways.bin" => {
            let mut l = Layout::of(&[col(4, D), col(4, D), col(4, D)]);
            l.repeat(16, &[col(2, B), col(2, B)]);
            if *name == "interp_ways.bin" {
                l.repeat(16, &[col(4, B), col(4, B), col(1, R)]);
            }
            l.repeat(16, &[col(4, R)]);
            l
        }
        _ => {
            let mut l = Layout::of(&[col(8, S), col(8, S)]);
            l.repeat(64, &[col(2, RAW_D), col(4, D)]);
            l
        }
    };
    Ok((name, layout))
}

/// A section of `rows` rows of `bytes` in `layout`'s columns, then `tail`.
pub(super) fn encode_rows(w: &mut impl Write, bytes: &[u8], rows: usize, layout: &Layout, tail: &[u8]) -> io::Result<()> {
    let stride = layout.stride();
    let mut sec = Section::new(&layout.cols);
    for row in bytes[..rows * stride].chunks_exact(stride) {
        sec.unit(w, stride)?;
        let mut at = 0;
        for &(fw, c) in &layout.fields {
            let mut v = [0u8; 8];
            v[..fw].copy_from_slice(&row[at..at + fw]);
            sec.push(c, u64::from_le_bytes(v));
            at += fw;
        }
    }
    sec.finish(w, tail)
}

/// A section of rows in `layout`'s columns into `out`, then its tail.
pub(super) fn decode_rows<R: Read, W: Write>(r: &mut Input<R>, chunk: &mut Chunk, out: &mut Out<W>, layout: &Layout) -> io::Result<()> {
    let mut per_row = vec![0u64; layout.cols.len()];
    for &(_, c) in &layout.fields {
        per_row[c] += 1;
    }
    let mut cols = Vec::with_capacity(layout.cols.len());
    while let Some(units) = chunk.read(r, &layout.cols)? {
        cols.clear();
        for (c, &n) in per_row.iter().enumerate() {
            chunk.expect(c, u64::from(units) * n, "a row's field")?;
            cols.push(chunk.col(c));
        }
        for _ in 0..units {
            for &(fw, c) in &layout.fields {
                out.bytes(&cols[c].next(&chunk.buf).to_le_bytes()[..fw])?;
            }
        }
    }
    read_tail(r, out, &mut chunk.buf)
}

/// A cols file's unit: its whole rows, then the bytes after them.
pub(super) fn encode(w: &mut impl Write, file: &Source, layout: Layout) -> io::Result<()> {
    let rows = file.len() / layout.stride();
    encode_rows(w, file.bytes(), rows, &layout, &file.bytes()[rows * layout.stride()..])
}

pub(super) fn decode<R: Read, W: Write>(r: &mut Input<R>, chunk: &mut Chunk, out: &mut Out<W>, fields: &[(u8, u16)]) -> io::Result<()> {
    decode_rows(r, chunk, out, &Layout::from_fields(fields))
}
