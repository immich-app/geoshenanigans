//! The download-only form of the packed files: a unit carries member files of one dir as
//! byte-aligned columns, which compress better under zstd than the packed bytes, and decode writes
//! the members back byte for byte. Every encoder choice (a place block's bases and widths, a
//! polygon's tag and widths, an arc's flags and group headers) travels in the stream, so decode
//! only packs values at the widths it is given. What it computes is a field's definition, never a
//! choice: a place block's offset (where the blocks before it end), an arc slot's offset and an
//! arc's box (the extent of its own vertices). The publisher compresses a unit with zstd and
//! checks it decodes to its members; a client checks each member's sha256.
//!
//! The kinds and their members, in order:
//! - places: place_cells.bin, place_nodes.bin (its L3 blocks unpacked), place_hidden.bin;
//! - pois: poi_records.bin (R2's rows), poi_vertices.bin (its V2 blocks unpacked);
//! - arcs: arc_index.bin and arc_vertices.bin (the arc blocks unpacked, the index computed);
//! - cols: one fixed-width file (COLS_FILES), its rows split into their fields.
//!
//! A unit's stream, integers little-endian:
//! - "GTT1", the transport version (u32, TRANSPORT_VERSION) and the kind (u8: 1 places, 2 pois,
//!   3 arcs, 4 cols);
//! - the members: their count (u8), then each one's name (a u8 length and its bytes) and size
//!   (u64), in the kind's order;
//! - cols only: the row's fields, their count (u16), then each one's width in bytes (u8: 1, 2, 4
//!   or 8) and its column (u16): a column an earlier field of its width has, or the next one;
//! - the kind's sections, in order. A section is its chunks, a zero u32, then its tail: the bytes
//!   its member ends with that no column holds, a u32 length and the bytes.
//!
//! A chunk is its units (u32, at least 1: rows, place blocks, polygons or arc slots), then each
//! of its section's columns' encoding (u8) and length in bytes (u32), in the section's order, then
//! the columns' bytes back to back. A chunk's columns take at most CHUNK_BYTES, but for a chunk of
//! one unit, which may take up to MAX_CHUNK_BYTES. A chunk decodes alone: its deltas start from 0.
//!
//! A column holds values of one width w (1, 2, 4 or 8 bytes) in order, mod 2^(8w), under one of
//! these encodings:
//! - R: the values; B: their bytes in planes, every value's first byte, then every value's second,
//!   and so on;
//! - D, d: each value's zigzagged difference from the one before it (0 before the first), as B,
//!   as R;
//! - S, s: each value's difference from the last one before it that is not all ones (0 before the
//!   first), an all-ones value as it is, as B, as R; a chunk in which a difference is all ones
//!   holds the column as D or d;
//! - P, p: as D and d, but the value before each unit's first is 0 (the POI vertex columns).
//!
//! The sections and their columns, each at the encoding the encoder writes it in:
//! - places: cells, a row a unit: key S, first row D. nodes, an L3 block a unit: lat base D, lng
//!   base D, wl D, wg D, wn R, then the blocks' rows (BLOCK_ROWS a block, the file's last block
//!   fewer): lat less the base D, lng less the base D, type R, name S; decode writes each block's
//!   table entry to a scratch file as it goes, and the table after the blocks, then the tail (the
//!   footer). hidden, a row a unit: lat D, lng B, word S, parent D.
//! - pois: records, an R2 row a unit: lat D, lng B, voff S, name S, street R, the three bytes of
//!   poly D R R, postcode R, combo B; the tail is the dictionaries and the footer. vertices, a V2
//!   block a unit: tag R, m B, the bbox varints' values B B (u64), wl R, wg R, then the blocks'
//!   stored vertices, q_lat and q_lng as u16 P P (encodings 0 to 2) or as u32 p s (encoding 3);
//!   the tail is the pad.
//! - arcs: an arc slot a unit: its vertex count B (0: an empty block); then each arc's flags R and
//!   its first vertex as the zigzagged differences from the last vertex of the arc before it in
//!   the chunk ((0, 0) for the first) B B (u64); then the arcs' group header bytes R; then their
//!   deltas zl, zg B B (u64), as their blocks hold them.
//! - cols: a row a unit, each field in its column, a column holding one field or one the row
//!   repeats (cols.rs gives each file's columns and their encodings); the tail is the bytes after
//!   the last whole row.
//!
//! Decode holds one chunk and a small buffer for each member it writes, at most
//! max(CHUNK_BYTES, the largest unit's columns) + DECODE_OVERHEAD bytes of heap. It refuses a
//! malformed stream with an error, never a panic, and writes nothing to a member past the error
//! or past the member's size.

mod arcs;
mod cols;
mod container;
mod places;
mod pois;

use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc;

pub use cols::COLS_FILES;
pub use container::{CHUNK_BYTES, DECODE_OVERHEAD, MAGIC, MAX_CHUNK_BYTES, TRANSPORT_VERSION};

/// What a unit holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Places,
    Pois,
    Arcs,
    Cols,
}

impl Kind {
    pub const ALL: [Kind; 4] = [Kind::Places, Kind::Pois, Kind::Arcs, Kind::Cols];

    pub fn name(self) -> &'static str {
        match self {
            Kind::Places => "places",
            Kind::Pois => "pois",
            Kind::Arcs => "arcs",
            Kind::Cols => "cols",
        }
    }

    pub fn from_name(name: &str) -> Option<Kind> {
        Kind::ALL.into_iter().find(|k| k.name() == name)
    }

    fn code(self) -> u8 {
        match self {
            Kind::Places => 1,
            Kind::Pois => 2,
            Kind::Arcs => 3,
            Kind::Cols => 4,
        }
    }

    fn of_code(code: u8) -> Option<Kind> {
        Kind::ALL.into_iter().find(|k| k.code() == code)
    }

    /// The members of a unit of this kind, in its order; empty for cols, whose one member is its
    /// file.
    pub fn members(self) -> &'static [&'static str] {
        match self {
            Kind::Places => &places::MEMBERS,
            Kind::Pois => &pois::MEMBERS,
            Kind::Arcs => &arcs::MEMBERS,
            Kind::Cols => &[],
        }
    }
}

/// A member file as a unit's header gives it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Member {
    pub name: String,
    pub size: u64,
}

/// What decode wrote: the unit's kind, and each member with its writer, flushed, in the kind's
/// order.
pub struct Decoded<W> {
    pub kind: Kind,
    pub members: Vec<(Member, W)>,
}

/// Writes the unit of `kind` for the member files in `dir` to `out`: the stream before zstd. A
/// cols unit is of `file` (one of COLS_FILES); the other kinds take none. An error for a file
/// the stream can't give back byte for byte.
pub fn encode(kind: Kind, dir: &Path, file: Option<&str>, out: impl Write) -> io::Result<()> {
    let names: Vec<&str> = match (kind, file) {
        (Kind::Cols, Some(f)) => vec![cols::layout_of(f)?.0],
        (Kind::Cols, None) => return Err(container::invalid("a cols unit needs its file")),
        (_, None) => kind.members().to_vec(),
        (_, Some(f)) => return Err(container::invalid(format!("a {} unit takes no file ({f})", kind.name()))),
    };
    let files = names.iter().map(|n| container::Source::open(&dir.join(n))).collect::<io::Result<Vec<_>>>()?;
    let mut w = io::BufWriter::with_capacity(container::OUT_BUFFER, out);
    let members: Vec<Member> = names.iter().zip(&files).map(|(n, f)| Member { name: n.to_string(), size: f.len() as u64 }).collect();
    match kind {
        Kind::Places => {
            container::write_head(&mut w, kind, &members, None)?;
            places::encode(&mut w, &files[0], &files[1], &files[2])?
        }
        Kind::Pois => {
            container::write_head(&mut w, kind, &members, None)?;
            pois::encode(&mut w, &files[0], &files[1])?
        }
        Kind::Arcs => {
            container::write_head(&mut w, kind, &members, None)?;
            arcs::encode(&mut w, &files[0], &files[1])?
        }
        Kind::Cols => {
            let layout = cols::layout_of(names[0])?.1;
            container::write_head(&mut w, kind, &members, Some(&layout.fields()))?;
            cols::encode(&mut w, &files[0], layout)?
        }
    }
    w.flush()
}

/// Decodes the unit `input` (the stream after zstd) into its members, each written front to back
/// through the writer `member` gives for it; `member` is asked for every member, in order, once
/// the header is read, and may refuse one. `scratch` is a dir decode may put a file of its own in
/// while it runs (a place unit's block table), which it removes.
pub fn decode<W: Write>(input: impl Read, scratch: &Path, mut member: impl FnMut(&Member) -> io::Result<W>) -> io::Result<Decoded<W>> {
    let mut r = container::Input::new(input);
    let head = container::read_head(&mut r)?;
    let mut outs = Vec::with_capacity(head.members.len());
    for m in &head.members {
        outs.push(container::Out::new(member(m)?, m));
    }
    let mut chunk = container::Chunk::default();
    match head.kind {
        Kind::Places => places::decode(&mut r, &mut chunk, &mut outs, scratch)?,
        Kind::Pois => pois::decode(&mut r, &mut chunk, &mut outs)?,
        Kind::Arcs => arcs::decode(&mut r, &mut chunk, &mut outs)?,
        Kind::Cols => cols::decode(&mut r, &mut chunk, &mut outs[0], head.fields.as_deref().unwrap_or(&[]))?,
    }
    r.end()?;
    let mut members = Vec::with_capacity(outs.len());
    for (m, o) in head.members.into_iter().zip(outs) {
        members.push((m, o.finish()?));
    }
    Ok(Decoded { kind: head.kind, members })
}

/// A member file being written and hashed.
struct Hashed {
    file: File,
    sha: Sha256,
}

impl Write for Hashed {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.file.write_all(b)?;
        self.sha.update(b);
        Ok(b.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

/// A file a unit is to decode to: its name, its sha256 (lowercase hex) and, where known, its
/// size.
#[derive(Clone, Debug)]
pub struct Expected {
    pub name: String,
    pub sha256: String,
    pub size: Option<u64>,
}

/// Decodes `unit`, a unit as published (through zstd), into its members in `dir`: each one to
/// <name>.tmp there, all of them renamed into place once the whole unit has decoded. With
/// `expect`, the members must be those files, in any order, each of its sha256 and size;
/// without, any. On an error no .tmp file stays and no member is renamed, so `dir` holds what it
/// held. A unit's members lie in `dir` together for a while: it needs their size free there.
/// Returns the unit's kind and members.
pub fn decode_into_dir(unit: impl Read, dir: &Path, expect: Option<&[Expected]>) -> io::Result<(Kind, Vec<Member>)> {
    let mut tmps: Vec<PathBuf> = Vec::new();
    let mut wanted: Vec<Option<&Expected>> = Vec::new();
    let decoded = zstd::stream::read::Decoder::new(unit).and_then(|input| {
        decode(input, dir, |m| {
            let e = match expect {
                None => None,
                Some(files) => {
                    let e = files.iter().find(|e| e.name == m.name).ok_or_else(|| container::invalid(format!("{} is no file this unit is to hold", m.name)))?;
                    if let Some(size) = e.size.filter(|&size| size != m.size) {
                        return Err(container::invalid(format!("{} is {} bytes in the unit, not {size}", m.name, m.size)));
                    }
                    Some(e)
                }
            };
            let tmp = dir.join(format!("{}.tmp", m.name));
            let file = File::create(&tmp).map_err(|e| io::Error::new(e.kind(), format!("{}: {e}", tmp.display())))?;
            tmps.push(tmp);
            wanted.push(e);
            Ok(Hashed { file, sha: Sha256::new() })
        })
    });
    let checked = decoded.and_then(|d| {
        if let Some(missing) = expect.unwrap_or(&[]).iter().find(|e| !d.members.iter().any(|(m, _)| m.name == e.name)) {
            return Err(container::invalid(format!("the unit lacks {}", missing.name)));
        }
        let mut members = Vec::with_capacity(d.members.len());
        for ((m, w), e) in d.members.into_iter().zip(&wanted) {
            let got = format!("{:x}", w.sha.finalize());
            if let Some(e) = e.filter(|e| e.sha256 != got) {
                return Err(container::invalid(format!("{}'s sha256 is {got}, not {}", m.name, e.sha256)));
            }
            members.push(m);
        }
        Ok((d.kind, members))
    });
    let (kind, members) = match checked {
        Ok(done) => done,
        Err(e) => {
            for tmp in &tmps {
                let _ = std::fs::remove_file(tmp);
            }
            return Err(e);
        }
    };
    for (i, (m, tmp)) in members.iter().zip(&tmps).enumerate() {
        if let Err(e) = std::fs::rename(tmp, dir.join(&m.name)) {
            for tmp in &tmps[i..] {
                let _ = std::fs::remove_file(tmp);
            }
            return Err(io::Error::new(e.kind(), format!("{}: {e}", tmp.display())));
        }
    }
    Ok((kind, members))
}

/// The bytes of a channel's chunks, in order, as one stream, which ends once the channel's
/// senders are gone: for a decode on a thread of its own, fed as the unit arrives.
pub struct ChunkReader<B> {
    rx: mpsc::Receiver<B>,
    chunk: Option<B>,
    at: usize,
}

impl<B> ChunkReader<B> {
    pub fn new(rx: mpsc::Receiver<B>) -> Self {
        ChunkReader { rx, chunk: None, at: 0 }
    }
}

impl<B: AsRef<[u8]>> Read for ChunkReader<B> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            if let Some(c) = &self.chunk {
                let c = &c.as_ref()[self.at..];
                if !c.is_empty() {
                    let n = c.len().min(buf.len());
                    buf[..n].copy_from_slice(&c[..n]);
                    self.at += n;
                    return Ok(n);
                }
            }
            match self.rx.recv() {
                Ok(c) => {
                    self.chunk = Some(c);
                    self.at = 0;
                }
                Err(_) => return Ok(0),
            }
        }
    }
}

#[cfg(test)]
mod tests;
