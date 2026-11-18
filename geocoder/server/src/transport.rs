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

use crate::lower_hex;
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
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

/// Decodes `unit`, a unit as published (through zstd), into its members in `dir`, each under its
/// name, all of them put in place once the whole unit has decoded. With `expect`, the members
/// must be those files, in any order, each of its sha256 and size; without, any. Returns the
/// unit's kind and members.
///
/// The decode makes a dir of its own in `dir` (a hidden name, which no member has and no other
/// decode takes) and writes the members there; then it moves them into place one by one, the
/// file each one replaces moved into its dir first, and once all are in it marks its dir so
/// (INSTALLED) and removes those files and its dir.
///
/// Besides those it touches only what decodes into `dir` that stopped before they could clean up
/// (their process killed, say) left. On unix a decode holds a shared lock on `dir` while it runs,
/// and one that finds no other decode holding it first finishes each of those decodes, by its
/// dir (finish_leftover): one that had put all its members in place loses the files they
/// replaced, and its dir; one that had not is undone, each file it replaced put back in its
/// place, over the member that went in or where it had left none, and its dir removed. A member
/// such a decode put in place that replaced no file stays: it is complete, and checked as that
/// decode checks its members. A dir that can't be read, or holding a file that can't go back (a
/// directory in its place, say), stays, as do the files still in it, for a later decode to
/// finish. None is finished while another decode runs in `dir`, on a file system without locks,
/// or on platforms other than unix. Nothing else: a `<member>.tmp` there, say, stays as it is.
///
/// On an error `dir` is as it was once leftovers were finished: the members moved in so far are
/// taken out again, the files they replaced put back, and nothing of the decode is left (put_back
/// tells what it couldn't undo). A member's path that is a directory is an error, as is, on unix,
/// a `dir` the decode can't open to lock, or a lock that fails but on a file system without
/// locks.
///
/// Decodes of a unit into one dir at once each put complete, checked members in place, the last
/// to move a member in winning; one that fails while another is putting its members in may undo
/// some of that one's. One killed while others run in the dir is finished only once none does:
/// undone then, it puts back the files it replaced over any member a decode put in place since.
///
/// A unit's members lie in `dir` beside the files they replace for a while: it needs their size
/// free there.
pub fn decode_into_dir(unit: impl Read, dir: &Path, expect: Option<&[Expected]>) -> io::Result<(Kind, Vec<Member>)> {
    let mut staging = Staging::new_in(dir)?;
    let scratch = staging.path.clone();
    let mut wanted: Vec<Option<&Expected>> = Vec::new();
    let decoded = decode(zstd::stream::read::Decoder::new(unit)?, &scratch, |m| {
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
        let file = staging.file(&m.name)?;
        wanted.push(e);
        Ok(Hashed { file, sha: Sha256::new() })
    })?;
    if let Some(missing) = expect.unwrap_or(&[]).iter().find(|e| !decoded.members.iter().any(|(m, _)| m.name == e.name)) {
        return Err(container::invalid(format!("the unit lacks {}", missing.name)));
    }
    let kind = decoded.kind;
    let mut members = Vec::with_capacity(decoded.members.len());
    for ((m, w), e) in decoded.members.into_iter().zip(&wanted) {
        let got = lower_hex(&w.sha.finalize());
        if let Some(e) = e.filter(|e| e.sha256 != got) {
            return Err(container::invalid(format!("{}'s sha256 is {got}, not {}", m.name, e.sha256)));
        }
        members.push(m);
    }
    staging.put_in_place(dir, &members)?;
    Ok((kind, members))
}

/// An io error of `path`.
fn at(path: &Path, e: io::Error) -> io::Error {
    io::Error::new(e.kind(), format!("{}: {e}", path.display()))
}

/// How many dirs this process's decodes into a dir have made, for their names.
static STAGINGS: AtomicU64 = AtomicU64::new(0);

/// The dir of a decode into a dir, in that dir: it holds the members as they decode (and a place
/// unit's block table), then the files they replace while they go in place, and INSTALLED once
/// all are in until those files are gone. Dropped, it removes the members it still holds, then
/// itself if empty, and only then lets go of its hold on the dir.
struct Staging {
    path: PathBuf,
    /// The members' files made in it.
    made: Vec<PathBuf>,
    /// Dropped after the fields above and Drop::drop.
    _hold: Hold,
}

/// A decode's shared lock on the dir it decodes into, held from before its own dir is made until
/// after that is removed (none on a file system without locks).
struct Hold {
    _lock: Option<File>,
}

/// Takes a hold on `dir`; if no other decode holds one, first finishes what the decodes that
/// stopped without removing their dirs left (finish_leftovers): with every live decode holding
/// its lock, those are all the decodes' dirs in `dir`. A dir that can't be opened is an error. On
/// a file system without locks the decode goes without a hold, and finishes none: there no decode
/// can tell another's dir from a leftover.
#[cfg(unix)]
fn hold(dir: &Path) -> io::Result<Hold> {
    let lock = File::open(dir).map_err(|e| at(dir, e))?;
    match lock.try_lock() {
        Ok(()) => {
            finish_leftovers(dir);
            lock.unlock().map_err(|e| at(dir, e))?;
        }
        Err(std::fs::TryLockError::WouldBlock) => {}
        Err(std::fs::TryLockError::Error(e)) if no_locks_here(&e) => return Ok(Hold { _lock: None }),
        Err(std::fs::TryLockError::Error(e)) => return Err(at(dir, e)),
    }
    // Waits while another decode finishes leftovers; the dir this one makes comes after.
    lock.lock_shared().map_err(|e| at(dir, e))?;
    Ok(Hold { _lock: Some(lock) })
}

#[cfg(not(unix))]
fn hold(_dir: &Path) -> io::Result<Hold> {
    Ok(Hold { _lock: None })
}

/// Whether `e`, an error taking a lock, says the file system has no locks.
#[cfg(unix)]
fn no_locks_here(e: &io::Error) -> bool {
    e.kind() == io::ErrorKind::Unsupported || e.raw_os_error() == Some(libc::ENOLCK)
}

/// Whether `name` is a decode's dir's, `.transport.<pid>.<n>`, as Staging::new_in writes it:
/// both numbers in decimal, without a sign or leading zeros.
#[cfg(unix)]
fn is_staging_name(name: &std::ffi::OsStr) -> bool {
    let Some(rest) = name.to_str().and_then(|n| n.strip_prefix(".transport.")) else { return false };
    let Some((pid, n)) = rest.split_once('.') else { return false };
    pid.parse::<u32>().is_ok_and(|p| p.to_string() == pid) && n.parse::<u64>().is_ok_and(|v| v.to_string() == n)
}

/// The empty file a decode's dir holds once all its members are in place, until the files they
/// replaced are removed: a hidden name, which no member has.
const INSTALLED: &str = ".installed";

/// Finishes what each decode into `dir` that stopped before it could clean up left: its dir
/// (finish_leftover); what can't be read stays.
#[cfg(unix)]
fn finish_leftovers(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for e in entries.flatten() {
        if is_staging_name(&e.file_name()) && e.file_type().is_ok_and(|t| t.is_dir()) {
            let _ = finish_leftover(dir, &e.path());
        }
    }
}

/// Finishes the decode into `dir` whose dir is `leftover`. If all its members went in (it holds
/// INSTALLED), the files they replaced are stale and go. If not, it is undone as put_back would
/// have: each file it replaced goes back to its place in `dir`, over the member that went in or
/// into the place left empty; a member that replaced no file stays, complete and checked. Then
/// the dir goes, with what else it holds. An error reading it, or a file that can't go
/// back, keeps the dir, and the files still in it, for a later decode to finish.
#[cfg(unix)]
fn finish_leftover(dir: &Path, leftover: &Path) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let installed = match std::fs::symlink_metadata(leftover.join(INSTALLED)) {
        Ok(_) => true,
        Err(e) if e.kind() == io::ErrorKind::NotFound => false,
        Err(e) => return Err(e),
    };
    let entries = std::fs::read_dir(leftover)?.collect::<io::Result<Vec<_>>>()?;
    let mut kept = Ok(());
    for e in entries {
        let name = e.file_name();
        let Some(name) = name.as_bytes().strip_prefix(b".earlier.") else { continue };
        let gone = if installed { std::fs::remove_file(e.path()) } else { std::fs::rename(e.path(), dir.join(std::ffi::OsStr::from_bytes(name))) };
        if gone.is_err() {
            kept = gone;
        }
    }
    kept?;
    std::fs::remove_dir_all(leftover)
}

/// A member moved into place (once `installed`), the file it replaced moved to `earlier`.
struct Moved {
    dest: PathBuf,
    earlier: Option<PathBuf>,
    installed: bool,
}

impl Staging {
    /// A new dir in `dir`, under a name no member has, once it holds `dir`: one of that name
    /// already there (an earlier process of this pid's, say) is skipped, never taken.
    fn new_in(dir: &Path) -> io::Result<Staging> {
        let hold = hold(dir)?;
        loop {
            let path = dir.join(format!(".transport.{}.{}", std::process::id(), STAGINGS.fetch_add(1, Ordering::Relaxed)));
            match std::fs::create_dir(&path) {
                Ok(()) => return Ok(Staging { path, made: Vec::new(), _hold: hold }),
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(at(&path, e)),
            }
        }
    }

    /// The new file the member `name` decodes to.
    fn file(&mut self, name: &str) -> io::Result<File> {
        let path = self.path.join(name);
        let file = File::options().write(true).create_new(true).open(&path).map_err(|e| at(&path, e))?;
        self.made.push(path);
        Ok(file)
    }

    /// Moves `members`, all decoded here, into place in `dir`, in order, the file each replaces
    /// moved here first; once all are in, puts INSTALLED here, so that a decode that finds this
    /// dir left behind knows those files are stale, then removes them, and INSTALLED once they
    /// are gone. On an error, INSTALLED not yet made, puts back what it moved.
    fn put_in_place(&self, dir: &Path, members: &[Member]) -> io::Result<()> {
        let mut moved = Vec::with_capacity(members.len());
        for m in members {
            if let Err(e) = self.move_in(dir, &m.name, &mut moved) {
                return Err(put_back(moved, e));
            }
        }
        let installed = self.path.join(INSTALLED);
        if let Err(e) = File::create(&installed) {
            return Err(put_back(moved, at(&installed, e)));
        }
        let mut removed = true;
        for earlier in moved.iter().filter_map(|m| m.earlier.as_ref()) {
            removed &= std::fs::remove_file(earlier).is_ok();
        }
        if removed {
            let _ = std::fs::remove_file(&installed);
        }
        Ok(())
    }

    /// Moves the member `name` into place in `dir`, the file there first moved here, and adds it
    /// to `moved` as soon as anything of it has moved.
    fn move_in(&self, dir: &Path, name: &str, moved: &mut Vec<Moved>) -> io::Result<()> {
        let dest = dir.join(name);
        let earlier = match std::fs::symlink_metadata(&dest) {
            Ok(meta) if meta.is_dir() => return Err(io::Error::other(format!("{}: is a directory", dest.display()))),
            Ok(_) => {
                // A hidden name, which no member's file here has.
                let earlier = self.path.join(format!(".earlier.{name}"));
                match std::fs::rename(&dest, &earlier) {
                    Ok(()) => Some(earlier),
                    // Moved away since, by another decode of this dir.
                    Err(e) if e.kind() == io::ErrorKind::NotFound => None,
                    Err(e) => return Err(at(&dest, e)),
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => return Err(at(&dest, e)),
        };
        moved.push(Moved { dest, earlier, installed: false });
        let m = moved.last_mut().expect("just pushed");
        std::fs::rename(self.path.join(name), &m.dest).map_err(|e| at(&m.dest, e))?;
        m.installed = true;
        Ok(())
    }
}

/// Undoes `moved`, the last first, after the error `e`: each replaced file back in its place,
/// a member that replaced none removed. Returns `e`, naming what it couldn't undo. A replaced
/// file it couldn't put back stays in the decode's dir, which then stays too; on unix a later
/// decode into the dir puts it back (finish_leftover).
fn put_back(moved: Vec<Moved>, e: io::Error) -> io::Error {
    let later = if cfg!(unix) { ", for a later decode into the dir to put back" } else { "" };
    let mut left = Vec::new();
    for m in moved.into_iter().rev() {
        let undone = match (&m.earlier, m.installed) {
            (Some(earlier), _) => std::fs::rename(earlier, &m.dest).map_err(|re| format!("the file {} replaced stays at {}{later}: {re}", m.dest.display(), earlier.display())),
            (None, true) => std::fs::remove_file(&m.dest).map_err(|re| format!("{}, this decode's, stays: {re}", m.dest.display())),
            (None, false) => Ok(()),
        };
        left.extend(undone.err());
    }
    if left.is_empty() {
        e
    } else {
        io::Error::new(e.kind(), format!("{e}; {}", left.join("; ")))
    }
}

impl Drop for Staging {
    fn drop(&mut self) {
        for f in &self.made {
            let _ = std::fs::remove_file(f);
        }
        let _ = std::fs::remove_dir(&self.path);
    }
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
