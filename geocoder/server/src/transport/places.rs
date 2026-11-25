//! The places unit: place_cells.bin and place_hidden.bin as rows, place_nodes.bin's L3 blocks
//! unpacked (place_files.rs lays the files out). Decode packs each block's rows at its entry's
//! widths, puts the entry, its offset where the blocks before it end, in a scratch file as it
//! goes, and appends the table and the footer after the last block.

use super::cols::{decode_rows, encode_rows, Layout};
use super::container::{bits_at, col, invalid, read_tail, Chunk, ColSpec, Input, Out, Pack, Packed, Section, Source, B, D, R, S};
use crate::place_files::{BLOCK_ROWS, CELLS_FILE, CELL_ROW_BYTES, FOOTER_BYTES, HIDDEN_FILE, HIDDEN_ROW_BYTES, MAGIC, NODES_FILE, TABLE_ENTRY_BYTES, TYPE_BITS};
use std::fs::File;
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub(super) const MEMBERS: [&str; 3] = [CELLS_FILE, NODES_FILE, HIDDEN_FILE];

const CELLS: [ColSpec; 2] = [col(4, S), col(4, D)];
const HIDDEN: [ColSpec; 4] = [col(4, D), col(4, B), col(4, S), col(4, D)];
/// A block's entry (lat base, lng base, wl, wg, wn), then its rows' fields (lat and lng less the
/// bases, type, name).
const NODES: [ColSpec; 9] = [col(4, D), col(4, D), col(1, D), col(1, D), col(1, R), col(4, D), col(4, D), col(1, R), col(4, S)];
const LAT_BASE: usize = 0;
const LNG_BASE: usize = 1;
const WL: usize = 2;
const WG: usize = 3;
const WN: usize = 4;
const DL: usize = 5;
const DG: usize = 6;
const TYPE: usize = 7;
const NAME: usize = 8;
/// The bytes of columns a block's entry and a row take.
const ENTRY_COLS: usize = 4 + 4 + 1 + 1 + 1;
const ROW_COLS: usize = 4 + 4 + 1 + 4;

fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([b[at], b[at + 1], b[at + 2], b[at + 3]])
}

/// A block's rows at widths wl, wg and wn, `row` giving each row's lat and lng less the bases,
/// type and name.
fn pack_block(p: &mut impl Pack, rows: usize, (wl, wg, wn): (u32, u32, u32), mut row: impl FnMut() -> [u64; 4]) -> io::Result<()> {
    for _ in 0..rows {
        let [dl, dg, t, name] = row();
        p.bits(dl, wl)?;
        p.bits(dg, wg)?;
        p.bits(t, TYPE_BITS)?;
        p.bits(name, wn)?;
    }
    p.pad()
}

pub(super) fn encode(w: &mut impl Write, cells: &Source, nodes: &Source, hidden: &Source) -> io::Result<()> {
    let c = cells.bytes();
    let rows = c.len() / CELL_ROW_BYTES;
    encode_rows(w, c, rows, &Layout::of(&CELLS), &c[rows * CELL_ROW_BYTES..])?;
    encode_nodes(w, nodes)?;
    let h = hidden.bytes();
    let rows = h.len() / HIDDEN_ROW_BYTES;
    encode_rows(w, h, rows, &Layout::of(&HIDDEN), &h[rows * HIDDEN_ROW_BYTES..])
}

fn encode_nodes(w: &mut impl Write, nodes: &Source) -> io::Result<()> {
    let n = nodes.bytes();
    if n.len() < FOOTER_BYTES || &n[n.len() - 4..] != MAGIC {
        return Err(nodes.refuse("no PNL3 footer"));
    }
    let rows = u32_at(n, n.len() - FOOTER_BYTES) as usize;
    let blocks = rows.div_ceil(BLOCK_ROWS);
    let table_at = (n.len() - FOOTER_BYTES).checked_sub(blocks * TABLE_ENTRY_BYTES).ok_or_else(|| nodes.refuse("shorter than its block table"))?;
    let mut sec = Section::new(&NODES);
    let (mut at, mut packed, mut fields) = (0usize, Packed::default(), Vec::with_capacity(BLOCK_ROWS));
    for b in 0..blocks {
        let e = &n[table_at + b * TABLE_ENTRY_BYTES..table_at + (b + 1) * TABLE_ENTRY_BYTES];
        let (wl, wg, wn) = (u32::from(e[12]), u32::from(e[13]), u32::from(e[14]));
        if e[15] != 0 || wl > 32 || wg > 32 || wn > 32 {
            return Err(nodes.refuse(format!("block {b}'s table entry is out of range")));
        }
        if u32_at(e, 8) as usize != at {
            return Err(nodes.refuse(format!("block {b} is not where the blocks before it end")));
        }
        let rows_b = BLOCK_ROWS.min(rows - b * BLOCK_ROWS);
        let bytes = (rows_b * (wl + wg + TYPE_BITS + wn) as usize).div_ceil(8);
        if bytes > table_at - at {
            return Err(nodes.refuse(format!("block {b} runs into the block table")));
        }
        let block = &n[at..at + bytes];
        fields.clear();
        let mut bit = 0;
        for _ in 0..rows_b {
            let mut f = [0u64; 4];
            for (v, width) in f.iter_mut().zip([wl, wg, TYPE_BITS, wn]) {
                *v = bits_at(block, bit, width);
                bit += width as usize;
            }
            fields.push(f);
        }
        packed.clear();
        let mut next = fields.iter();
        pack_block(&mut packed, rows_b, (wl, wg, wn), || *next.next().unwrap_or(&[0; 4]))?;
        if packed.bytes != block {
            return Err(nodes.refuse(format!("block {b} has bits set past its last row")));
        }
        sec.unit(w, ENTRY_COLS + rows_b * ROW_COLS)?;
        for (c, v) in [(LAT_BASE, u32_at(e, 0)), (LNG_BASE, u32_at(e, 4)), (WL, wl), (WG, wg), (WN, wn)] {
            sec.push(c, u64::from(v));
        }
        for f in &fields {
            for (c, &v) in [DL, DG, TYPE, NAME].into_iter().zip(f) {
                sec.push(c, v);
            }
        }
        at += bytes;
    }
    if at != table_at {
        return Err(nodes.refuse("bytes between its blocks and its table"));
    }
    sec.finish(w, &n[n.len() - FOOTER_BYTES..])
}

pub(super) fn decode<R: Read, W: Write>(r: &mut Input<R>, chunk: &mut Chunk, outs: &mut [Out<W>], scratch: &Path) -> io::Result<()> {
    let [cells, nodes, hidden] = outs else { return Err(invalid("a places unit without its three members")) };
    decode_rows(r, chunk, cells, &Layout::of(&CELLS))?;
    decode_nodes(r, chunk, nodes, scratch)?;
    decode_rows(r, chunk, hidden, &Layout::of(&HIDDEN))
}

fn decode_nodes<R: Read, W: Write>(r: &mut Input<R>, chunk: &mut Chunk, out: &mut Out<W>, scratch: &Path) -> io::Result<()> {
    let mut table = Table::create(scratch)?;
    let mut ended = false;
    while let Some(units) = chunk.read(r, &NODES)? {
        if ended {
            return Err(invalid("place blocks after a short one"));
        }
        let blocks = u64::from(units);
        for c in [LAT_BASE, LNG_BASE, WL, WG, WN] {
            chunk.expect(c, blocks, "a place block's entry")?;
        }
        let rows = chunk.col(DL).len() as u64;
        for c in [DL, DG, TYPE, NAME] {
            chunk.expect(c, rows, "a place row's field")?;
        }
        let full = BLOCK_ROWS as u64;
        if rows > blocks * full || rows <= (blocks - 1) * full {
            return Err(invalid(format!("{rows} place rows in {blocks} blocks of {full}")));
        }
        ended = rows < blocks * full;
        let buf = &chunk.buf;
        let mut c: [_; 9] = std::array::from_fn(|i| chunk.col(i));
        for b in 0..blocks {
            let rows_b = if b + 1 < blocks { full } else { rows - b * full };
            let (lat_base, lng_base) = (c[LAT_BASE].next(buf), c[LNG_BASE].next(buf));
            let (wl, wg, wn) = (c[WL].next(buf), c[WG].next(buf), c[WN].next(buf));
            if wl > 32 || wg > 32 || wn > 32 {
                return Err(invalid("a place block's width past 32 bits"));
            }
            let offset = u32::try_from(out.at()).map_err(|_| invalid("place_nodes.bin's blocks past 4 GiB"))?;
            let mut entry = [0u8; TABLE_ENTRY_BYTES];
            entry[0..4].copy_from_slice(&(lat_base as u32).to_le_bytes());
            entry[4..8].copy_from_slice(&(lng_base as u32).to_le_bytes());
            entry[8..12].copy_from_slice(&offset.to_le_bytes());
            entry[12..15].copy_from_slice(&[wl as u8, wg as u8, wn as u8]);
            table.put(&entry)?;
            let [dl, dg, t, name] = c.get_disjoint_mut([DL, DG, TYPE, NAME]).map_err(|_| invalid("place row columns"))?;
            pack_block(out, rows_b as usize, (wl as u32, wg as u32, wn as u32), || [dl.next(buf), dg.next(buf), t.next(buf), name.next(buf)])?;
        }
    }
    table.copy_into(out, &mut chunk.buf)?;
    read_tail(r, out, &mut chunk.buf)
}

static TABLES: AtomicU64 = AtomicU64::new(0);

/// The block table of the place_nodes.bin being decoded, in a file of its own in the scratch dir
/// until the last block is written; removed when dropped.
struct Table {
    path: PathBuf,
    file: BufWriter<File>,
}

impl Table {
    fn create(dir: &Path) -> io::Result<Table> {
        loop {
            let n = TABLES.fetch_add(1, Ordering::Relaxed);
            let path = dir.join(format!(".{NODES_FILE}.table.{}.{n}", std::process::id()));
            match File::options().read(true).write(true).create_new(true).open(&path) {
                Ok(f) => return Ok(Table { path, file: BufWriter::with_capacity(super::container::OUT_BUFFER, f) }),
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(io::Error::new(e.kind(), format!("{}: {e}", path.display()))),
            }
        }
    }

    fn put(&mut self, entry: &[u8]) -> io::Result<()> {
        self.file.write_all(entry)
    }

    /// The table into `out`, read back through `buf`.
    fn copy_into<W: Write>(mut self, out: &mut Out<W>, buf: &mut Vec<u8>) -> io::Result<()> {
        self.file.flush()?;
        let f = self.file.get_mut();
        f.seek(SeekFrom::Start(0))?;
        buf.clear();
        buf.resize(super::container::OUT_BUFFER, 0);
        loop {
            let k = f.read(buf)?;
            if k == 0 {
                return Ok(());
            }
            out.bytes(&buf[..k])?;
        }
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}
