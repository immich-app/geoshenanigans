//! The stream's framing (transport.rs lays it out): its header, chunks of columns under their
//! encodings, the section ends and tails, and the members decode writes.

use super::{Kind, Member};
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::Path;

pub const MAGIC: [u8; 4] = *b"GTT1";
/// The stream's layout: a reader refuses a unit of any other version.
pub const TRANSPORT_VERSION: u32 = 2;
/// The most bytes of columns a chunk of more than one unit holds.
pub const CHUNK_BYTES: usize = 1 << 20;
/// The most bytes of columns a chunk holds: a unit past CHUNK_BYTES goes alone in its chunk.
pub const MAX_CHUNK_BYTES: usize = 64 << 20;
/// The heap decode holds beside its chunk: a buffer for each member and the place table's
/// scratch file, and its bookkeeping.
pub const DECODE_OVERHEAD: usize = 1 << 20;
/// The buffer a member is written through, and the encoder's output buffer.
pub(crate) const OUT_BUFFER: usize = 64 << 10;
/// The most fields a cols row has.
const MAX_FIELDS: usize = 1024;

pub(crate) fn invalid(msg: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("transport: {msg}"))
}

/// A column's encodings (transport.rs).
pub(crate) const R: u8 = b'R';
pub(crate) const B: u8 = b'B';
pub(crate) const D: u8 = b'D';
pub(crate) const S: u8 = b'S';
pub(crate) const P: u8 = b'P';
pub(crate) const RAW_D: u8 = b'd';
pub(crate) const RAW_S: u8 = b's';
pub(crate) const RAW_P: u8 = b'p';

fn planes(enc: u8) -> bool {
    matches!(enc, B | D | S | P)
}

/// A column of a section: its values' width in bytes, the encoding the encoder writes it in,
/// and whether its values fall in units that P restarts at.
#[derive(Clone, Copy)]
pub(crate) struct ColSpec {
    pub w: usize,
    pub enc: u8,
    pub per_unit: bool,
}

pub(crate) const fn col(w: usize, enc: u8) -> ColSpec {
    ColSpec { w, enc, per_unit: false }
}

pub(crate) const fn unit_col(w: usize, enc: u8) -> ColSpec {
    ColSpec { w, enc, per_unit: true }
}

#[inline(always)]
fn mask(w: usize) -> u64 {
    if w >= 8 { u64::MAX } else { (1u64 << (8 * w)) - 1 }
}

/// A difference mod 2^(8w), zigzagged, and back.
#[inline(always)]
fn zz(d: u64, w: usize) -> u64 {
    let sign = (d >> (8 * w - 1)) & 1;
    ((d << 1) ^ 0u64.wrapping_sub(sign)) & mask(w)
}

#[inline(always)]
fn unzz(z: u64, w: usize) -> u64 {
    ((z >> 1) ^ 0u64.wrapping_sub(z & 1)) & mask(w)
}

// ------------------------------------------------------------------ encoding

/// A member file as the encoder reads it: mapped, or no bytes.
pub(crate) struct Source {
    map: Option<memmap2::Mmap>,
    pub name: String,
}

impl Source {
    pub fn open(path: &Path) -> io::Result<Source> {
        let name = path.file_name().map_or_else(String::new, |n| n.to_string_lossy().into_owned());
        let at = |e: io::Error| io::Error::new(e.kind(), format!("{}: {e}", path.display()));
        let f = File::open(path).map_err(at)?;
        let map = if f.metadata().map_err(at)?.len() == 0 {
            None
        } else {
            // SAFETY: the publisher maps files no one writes while it reads them.
            let m = unsafe { memmap2::Mmap::map(&f) }.map_err(at)?;
            let _ = m.advise(memmap2::Advice::Sequential);
            Some(m)
        };
        Ok(Source { map, name })
    }

    pub fn bytes(&self) -> &[u8] {
        self.map.as_deref().unwrap_or(&[])
    }

    pub fn len(&self) -> usize {
        self.bytes().len()
    }

    /// An error naming this file.
    pub fn refuse(&self, what: impl std::fmt::Display) -> io::Error {
        invalid(format!("{}: {what}", self.name))
    }
}

fn put_u32(w: &mut impl Write, v: u32) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
}

pub(crate) fn write_head(w: &mut impl Write, kind: Kind, members: &[Member], fields: Option<&[(u8, u16)]>) -> io::Result<()> {
    w.write_all(&MAGIC)?;
    put_u32(w, TRANSPORT_VERSION)?;
    w.write_all(&[kind.code(), members.len() as u8])?;
    for m in members {
        w.write_all(&[m.name.len() as u8])?;
        w.write_all(m.name.as_bytes())?;
        w.write_all(&m.size.to_le_bytes())?;
    }
    if let Some(fields) = fields {
        w.write_all(&(fields.len() as u16).to_le_bytes())?;
        for &(width, c) in fields {
            w.write_all(&[width])?;
            w.write_all(&c.to_le_bytes())?;
        }
    }
    Ok(())
}

/// The columns of one section's chunk being gathered, and the chunks written as they fill.
pub(crate) struct Section<'a> {
    specs: &'a [ColSpec],
    cols: Vec<Vec<u64>>,
    /// Per column, where each unit's values start (the P columns').
    starts: Vec<Vec<usize>>,
    per_unit: Vec<usize>,
    units: u32,
    raw: usize,
    bytes: Vec<Vec<u8>>,
}

impl<'a> Section<'a> {
    pub fn new(specs: &'a [ColSpec]) -> Self {
        let n = specs.len();
        let per_unit = (0..n).filter(|&c| specs[c].per_unit).collect();
        Section { specs, cols: vec![Vec::new(); n], starts: vec![Vec::new(); n], per_unit, units: 0, raw: 0, bytes: vec![Vec::new(); n] }
    }

    /// Starts a unit whose values take `raw` bytes of columns: the chunk so far is written first
    /// when the unit would take it past CHUNK_BYTES. Whether the unit is its chunk's first.
    pub fn unit(&mut self, w: &mut impl Write, raw: usize) -> io::Result<bool> {
        if raw > MAX_CHUNK_BYTES {
            return Err(invalid(format!("a unit of {raw} bytes of columns, past the {MAX_CHUNK_BYTES} a chunk holds")));
        }
        if self.units > 0 && self.raw + raw > CHUNK_BYTES {
            self.flush(w)?;
        }
        self.units += 1;
        self.raw += raw;
        for &c in &self.per_unit {
            self.starts[c].push(self.cols[c].len());
        }
        Ok(self.units == 1)
    }

    #[inline(always)]
    pub fn push(&mut self, c: usize, v: u64) {
        self.cols[c].push(v);
    }

    fn flush(&mut self, w: &mut impl Write) -> io::Result<()> {
        let raw: usize = self.cols.iter().zip(self.specs).map(|(c, s)| c.len() * s.w).sum();
        if raw != self.raw {
            return Err(io::Error::other(format!("transport: a chunk's columns take {raw} bytes where its units gave {}", self.raw)));
        }
        put_u32(w, self.units)?;
        for c in 0..self.specs.len() {
            let enc = encode_column(&self.cols[c], self.specs[c], &self.starts[c], &mut self.bytes[c]);
            w.write_all(&[enc])?;
            put_u32(w, self.bytes[c].len() as u32)?;
        }
        for c in 0..self.specs.len() {
            w.write_all(&self.bytes[c])?;
            self.cols[c].clear();
            self.starts[c].clear();
        }
        self.units = 0;
        self.raw = 0;
        Ok(())
    }

    /// Writes the last chunk, the section's end and its tail.
    pub fn finish(mut self, w: &mut impl Write, tail: &[u8]) -> io::Result<()> {
        if self.units > 0 {
            self.flush(w)?;
        }
        let len = u32::try_from(tail.len()).map_err(|_| invalid(format!("a tail of {} bytes, past 4 GiB", tail.len())))?;
        put_u32(w, 0)?;
        put_u32(w, len)?;
        w.write_all(tail)
    }
}

/// A column's values under its spec's encoding into `out`; the encoding written (S falls back to
/// D where a difference is all ones).
pub(super) fn encode_column(vals: &[u64], spec: ColSpec, starts: &[usize], out: &mut Vec<u8>) -> u8 {
    let (w, m) = (spec.w, mask(spec.w));
    let mut enc = spec.enc;
    if matches!(enc, S | RAW_S) {
        let mut prev = 0u64;
        for &x in vals {
            if x == m {
                continue;
            }
            if x.wrapping_sub(prev) & m == m {
                enc = if enc == S { D } else { RAW_D };
                break;
            }
            prev = x;
        }
    }
    let t: Vec<u64> = match enc {
        D | RAW_D => {
            let mut prev = 0u64;
            vals.iter().map(|&x| {
                let d = zz(x.wrapping_sub(prev) & m, w);
                prev = x;
                d
            }).collect()
        }
        P | RAW_P => {
            let (mut prev, mut next) = (0u64, starts.iter().peekable());
            vals.iter().enumerate().map(|(i, &x)| {
                while next.peek().is_some_and(|&&s| s <= i) {
                    next.next();
                    prev = 0;
                }
                let d = zz(x.wrapping_sub(prev) & m, w);
                prev = x;
                d
            }).collect()
        }
        S | RAW_S => {
            let mut prev = 0u64;
            vals.iter().map(|&x| {
                if x == m {
                    return x;
                }
                let d = x.wrapping_sub(prev) & m;
                prev = x;
                d
            }).collect()
        }
        _ => vals.to_vec(),
    };
    out.clear();
    if planes(enc) && w > 1 {
        out.resize(t.len() * w, 0);
        for (i, v) in t.iter().enumerate() {
            for k in 0..w {
                out[k * t.len() + i] = (v >> (8 * k)) as u8;
            }
        }
    } else {
        for v in &t {
            out.extend_from_slice(&v.to_le_bytes()[..w]);
        }
    }
    enc
}

// ------------------------------------------------------------------ decoding

/// The stream being decoded, and how far into it.
pub(crate) struct Input<R> {
    r: R,
    at: u64,
}

impl<R: Read> Input<R> {
    pub fn new(r: R) -> Self {
        Input { r, at: 0 }
    }

    pub fn exact(&mut self, b: &mut [u8]) -> io::Result<()> {
        self.r.read_exact(b).map_err(|e| {
            if e.kind() == io::ErrorKind::UnexpectedEof { invalid(format!("the unit ends early, past byte {}", self.at)) } else { e }
        })?;
        self.at += b.len() as u64;
        Ok(())
    }

    pub fn u8(&mut self) -> io::Result<u8> {
        let mut b = [0u8; 1];
        self.exact(&mut b)?;
        Ok(b[0])
    }

    pub fn u16(&mut self) -> io::Result<u16> {
        let mut b = [0u8; 2];
        self.exact(&mut b)?;
        Ok(u16::from_le_bytes(b))
    }

    pub fn u32(&mut self) -> io::Result<u32> {
        let mut b = [0u8; 4];
        self.exact(&mut b)?;
        Ok(u32::from_le_bytes(b))
    }

    pub fn u64(&mut self) -> io::Result<u64> {
        let mut b = [0u8; 8];
        self.exact(&mut b)?;
        Ok(u64::from_le_bytes(b))
    }

    /// The bytes read so far.
    #[cfg(test)]
    pub fn pos(&self) -> u64 {
        self.at
    }

    /// After the last section: the stream ends.
    pub fn end(&mut self) -> io::Result<()> {
        let mut b = [0u8; 1];
        loop {
            match self.r.read(&mut b) {
                Ok(0) => return Ok(()),
                Ok(_) => return Err(invalid(format!("bytes after the last section, at byte {}", self.at))),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
    }
}

/// A unit's header: its kind, its members and, for cols, its fields' widths and columns.
pub(crate) struct Head {
    pub kind: Kind,
    pub members: Vec<Member>,
    pub fields: Option<Vec<(u8, u16)>>,
}

/// A member's name a unit may give: a plain file name.
fn plain_name(n: &str) -> bool {
    !n.is_empty() && !n.starts_with('.') && n.bytes().all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'_' | b'-'))
}

pub(crate) fn read_head<R: Read>(r: &mut Input<R>) -> io::Result<Head> {
    let mut magic = [0u8; 4];
    r.exact(&mut magic)?;
    if magic != MAGIC {
        return Err(invalid("not a transport unit (no GTT1)"));
    }
    let version = r.u32()?;
    if version != TRANSPORT_VERSION {
        return Err(invalid(format!("a unit of transport version {version}: this build reads version {TRANSPORT_VERSION} only")));
    }
    let code = r.u8()?;
    let kind = Kind::of_code(code).ok_or_else(|| invalid(format!("a unit of kind {code}, which this build doesn't know")))?;
    let count = r.u8()? as usize;
    let want = match kind {
        Kind::Cols => 1,
        _ => kind.members().len(),
    };
    if count != want {
        return Err(invalid(format!("a {} unit of {count} members, not {want}", kind.name())));
    }
    let mut members = Vec::with_capacity(count);
    for i in 0..count {
        let mut name = vec![0u8; r.u8()? as usize];
        r.exact(&mut name)?;
        let name = String::from_utf8(name).ok().filter(|n| plain_name(n)).ok_or_else(|| invalid("a member's name that is no plain file name"))?;
        if kind != Kind::Cols && name != kind.members()[i] {
            return Err(invalid(format!("a {} unit's member {name} where {} goes", kind.name(), kind.members()[i])));
        }
        members.push(Member { name, size: r.u64()? });
    }
    let fields = if kind == Kind::Cols {
        let n = r.u16()? as usize;
        if n == 0 || n > MAX_FIELDS {
            return Err(invalid(format!("a row of {n} fields")));
        }
        let (mut fields, mut widths) = (Vec::with_capacity(n), Vec::new());
        for _ in 0..n {
            let (w, c) = (r.u8()?, r.u16()?);
            if !matches!(w, 1 | 2 | 4 | 8) {
                return Err(invalid(format!("a field of {w} bytes")));
            }
            match widths.get(c as usize) {
                Some(&cw) if cw != w => return Err(invalid(format!("a field of {w} bytes in a column of {cw}-byte values"))),
                Some(_) => {}
                None if c as usize == widths.len() => widths.push(w),
                None => return Err(invalid(format!("a field in column {c} where the next is {}", widths.len()))),
            }
            fields.push((w, c));
        }
        Some(fields)
    } else {
        None
    };
    Ok(Head { kind, members, fields })
}

/// One chunk's columns, held whole while its units are written; reused from chunk to chunk.
#[derive(Default)]
pub(crate) struct Chunk {
    pub buf: Vec<u8>,
    cols: Vec<Col>,
}

impl Chunk {
    /// Reads the next chunk of a section of columns `specs`: its units, or None at the section's
    /// end. Refuses an encoding the column can't hold, a column of part of a value, and a chunk
    /// past CHUNK_BYTES (MAX_CHUNK_BYTES for one unit), before it holds any of its bytes.
    pub fn read<R: Read>(&mut self, r: &mut Input<R>, specs: &[ColSpec]) -> io::Result<Option<u32>> {
        let units = r.u32()?;
        if units == 0 {
            return Ok(None);
        }
        let limit = if units == 1 { MAX_CHUNK_BYTES } else { CHUNK_BYTES };
        self.cols.clear();
        let mut total = 0usize;
        for s in specs {
            let enc = r.u8()?;
            let known = matches!(enc, R | B | D | RAW_D | S | RAW_S) || (s.per_unit && matches!(enc, P | RAW_P));
            if !known {
                return Err(invalid(format!("a column encoded {:?}, which it can't be", enc as char)));
            }
            let len = r.u32()? as usize;
            if len % s.w != 0 {
                return Err(invalid(format!("a column of {len} bytes of {}-byte values", s.w)));
            }
            if len > limit - total {
                return Err(invalid(format!("a chunk of {units} units past {limit} bytes")));
            }
            self.cols.push(Col { at: total, n: len / s.w, w: s.w, enc, i: 0, prev: 0 });
            total += len;
        }
        if self.buf.capacity() < total {
            // The old buffer goes before the new one comes: one chunk held at a time.
            self.buf = Vec::new();
            self.buf.reserve_exact(total.max(CHUNK_BYTES));
        }
        self.buf.clear();
        self.buf.resize(total, 0);
        r.exact(&mut self.buf)?;
        Ok(Some(units))
    }

    /// Column c, at its first value.
    pub fn col(&self, c: usize) -> Col {
        self.cols[c]
    }

    /// Checks column c holds n values.
    pub fn expect(&self, c: usize, n: u64, what: &str) -> io::Result<()> {
        let got = self.cols[c].n as u64;
        if got != n {
            return Err(invalid(format!("{what}: {got} values where the chunk's units have {n}")));
        }
        Ok(())
    }
}

/// A cursor over a column's values in a chunk's bytes.
#[derive(Clone, Copy)]
pub(crate) struct Col {
    at: usize,
    n: usize,
    w: usize,
    enc: u8,
    i: usize,
    prev: u64,
}

impl Col {
    pub fn len(&self) -> usize {
        self.n
    }

    /// The next value; 0 past the last (the sections check a column's count before they read it).
    #[inline(always)]
    pub fn next(&mut self, buf: &[u8]) -> u64 {
        let (i, w) = (self.i, self.w);
        if i >= self.n {
            return 0;
        }
        self.i += 1;
        let raw = if planes(self.enc) && w > 1 {
            let mut v = 0u64;
            for k in 0..w {
                v |= u64::from(buf.get(self.at + k * self.n + i).copied().unwrap_or(0)) << (8 * k);
            }
            v
        } else {
            let mut b = [0u8; 8];
            let at = self.at + i * w;
            if let Some(s) = buf.get(at..at + w) {
                b[..w].copy_from_slice(s);
            }
            u64::from_le_bytes(b)
        };
        let m = mask(w);
        match self.enc {
            R | B => raw,
            S | RAW_S => {
                if raw == m {
                    return raw;
                }
                self.prev = self.prev.wrapping_add(raw) & m;
                self.prev
            }
            _ => {
                self.prev = self.prev.wrapping_add(unzz(raw, w)) & m;
                self.prev
            }
        }
    }

    /// At a unit's first value: P's difference is from 0.
    #[inline(always)]
    pub fn unit_start(&mut self) {
        if matches!(self.enc, P | RAW_P) {
            self.prev = 0;
        }
    }
}

/// A member being written front to back through a small buffer, never past its size, and the
/// bits a packer has not yet made a byte of (Pack).
pub(crate) struct Out<W> {
    w: W,
    buf: Vec<u8>,
    written: u64,
    size: u64,
    name: String,
    acc: u64,
    bits: u32,
}

impl<W: Write> Out<W> {
    pub fn new(w: W, m: &Member) -> Self {
        Out { w, buf: Vec::with_capacity(OUT_BUFFER), written: 0, size: m.size, name: m.name.clone(), acc: 0, bits: 0 }
    }

    fn flush_buf(&mut self) -> io::Result<()> {
        let n = self.buf.len() as u64;
        if n > self.size - self.written {
            return Err(invalid(format!("{} runs past its {} bytes", self.name, self.size)));
        }
        self.w.write_all(&self.buf)?;
        self.written += n;
        self.buf.clear();
        Ok(())
    }

    #[inline(always)]
    pub fn byte(&mut self, b: u8) -> io::Result<()> {
        self.buf.push(b);
        if self.buf.len() >= OUT_BUFFER {
            self.flush_buf()?;
        }
        Ok(())
    }

    pub fn bytes(&mut self, mut b: &[u8]) -> io::Result<()> {
        while !b.is_empty() {
            let k = b.len().min(OUT_BUFFER - self.buf.len());
            self.buf.extend_from_slice(&b[..k]);
            b = &b[k..];
            if self.buf.len() >= OUT_BUFFER {
                self.flush_buf()?;
            }
        }
        Ok(())
    }

    /// The bytes written so far.
    pub fn at(&self) -> u64 {
        self.written + self.buf.len() as u64
    }

    /// The member's writer, once the member is its size.
    pub fn finish(mut self) -> io::Result<W> {
        self.flush_buf()?;
        if self.written != self.size {
            return Err(invalid(format!("{} is {} bytes, its unit gives {}", self.name, self.written, self.size)));
        }
        self.w.flush()?;
        Ok(self.w)
    }
}

/// What the packers write to: a member (decode), or the bytes the encoder checks a unit's
/// against.
pub(crate) trait Pack {
    fn byte(&mut self, b: u8) -> io::Result<()>;
    /// The low `w` bits of v (w at most 56) after the bits so far, LSB first; an error for a v
    /// of more bits.
    fn bits(&mut self, v: u64, w: u32) -> io::Result<()>;
    /// The bits so far, their last byte filled with zero bits.
    fn pad(&mut self) -> io::Result<()>;

    fn bytes(&mut self, b: &[u8]) -> io::Result<()> {
        b.iter().try_for_each(|&x| self.byte(x))
    }

    /// v as a LEB128 varint in its fewest bytes.
    fn varint(&mut self, mut v: u64) -> io::Result<()> {
        while v >= 0x80 {
            self.byte(v as u8 | 0x80)?;
            v >>= 7;
        }
        self.byte(v as u8)
    }
}

fn too_wide(v: u64, w: u32) -> io::Error {
    invalid(format!("a value of {v} in a field of {w} bits"))
}

impl<W: Write> Pack for Out<W> {
    #[inline(always)]
    fn byte(&mut self, b: u8) -> io::Result<()> {
        Out::byte(self, b)
    }

    #[inline(always)]
    fn bits(&mut self, v: u64, w: u32) -> io::Result<()> {
        if w < 64 && v >> w != 0 {
            return Err(too_wide(v, w));
        }
        if w == 0 {
            return Ok(());
        }
        self.acc |= v << self.bits;
        self.bits += w;
        while self.bits >= 8 {
            Out::byte(self, self.acc as u8)?;
            self.acc >>= 8;
            self.bits -= 8;
        }
        Ok(())
    }

    fn pad(&mut self) -> io::Result<()> {
        if self.bits > 0 {
            Out::byte(self, self.acc as u8)?;
        }
        self.acc = 0;
        self.bits = 0;
        Ok(())
    }

    fn bytes(&mut self, b: &[u8]) -> io::Result<()> {
        Out::bytes(self, b)
    }
}

/// Bytes packed in memory: what decode would write for a unit, which the encoder compares with
/// the file's.
#[derive(Default)]
pub(crate) struct Packed {
    pub bytes: Vec<u8>,
    acc: u64,
    bits: u32,
}

impl Packed {
    pub fn clear(&mut self) {
        self.bytes.clear();
        self.acc = 0;
        self.bits = 0;
    }
}

impl Pack for Packed {
    fn byte(&mut self, b: u8) -> io::Result<()> {
        self.bytes.push(b);
        Ok(())
    }

    fn bits(&mut self, v: u64, w: u32) -> io::Result<()> {
        if w < 64 && v >> w != 0 {
            return Err(too_wide(v, w));
        }
        if w == 0 {
            return Ok(());
        }
        self.acc |= v << self.bits;
        self.bits += w;
        while self.bits >= 8 {
            self.bytes.push(self.acc as u8);
            self.acc >>= 8;
            self.bits -= 8;
        }
        Ok(())
    }

    fn pad(&mut self) -> io::Result<()> {
        if self.bits > 0 {
            self.bytes.push(self.acc as u8);
        }
        self.acc = 0;
        self.bits = 0;
        Ok(())
    }
}

/// The `w` bits (at most 56) at bit `bit` of `b`, LSB first; bits past its end read as 0.
#[inline]
pub(crate) fn bits_at(b: &[u8], bit: usize, w: u32) -> u64 {
    if w == 0 {
        return 0;
    }
    let at = bit >> 3;
    let mut word = [0u8; 8];
    if at < b.len() {
        let end = (at + 8).min(b.len());
        word[..end - at].copy_from_slice(&b[at..end]);
    }
    (u64::from_le_bytes(word) >> (bit & 7)) & ((1u64 << w) - 1)
}

pub(crate) fn zigzag(v: i64) -> u64 {
    ((v as u64) << 1) ^ ((v >> 63) as u64)
}

pub(crate) fn unzigzag(z: u64) -> i64 {
    (z >> 1) as i64 ^ -((z & 1) as i64)
}

/// A section's tail, after its end, into `out` through `buf`.
pub(crate) fn read_tail<R: Read, W: Write>(r: &mut Input<R>, out: &mut Out<W>, buf: &mut Vec<u8>) -> io::Result<()> {
    let mut left = r.u32()? as usize;
    while left > 0 {
        let k = left.min(OUT_BUFFER);
        buf.clear();
        buf.resize(k, 0);
        r.exact(buf)?;
        out.bytes(buf)?;
        left -= k;
    }
    Ok(())
}
