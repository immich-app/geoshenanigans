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
/// (their process killed, say) left. On unix decodes take two locks, both flock's, exclusive:
/// the install lock, on `dir`, and each decode's own lock, on its own dir, held from just after
/// it makes that dir until after it removes it. A decode takes the install lock, finishes what
/// decodes that are gone left (below), makes its dir and takes its lock, and lets go of the
/// install lock; it decodes its members, as other decodes do theirs at the same time; then it
/// takes the install lock again, finishes what decodes that died meanwhile left, puts its
/// members in place, removes its dir and only then lets go of the install lock. So installs go
/// one at a time, and one killed part-way is finished before any later one goes in, never after
/// it. Whether decodes take locks is told by a decode's first lock: once it took that one, a
/// later one that fails fails the decode, whatever the error, before a member goes in.
///
/// To finish leftovers a decode tries the lock of each decode's dir in `dir`: one held is a
/// running decode's, decoding into it, and stays; one it takes is a dead decode's, and is
/// finished (finish_leftover). One that had put all its members in place loses the files they
/// replaced, and its dir; one that had not is undone, each file it replaced put back in its
/// place, over the member that went in or where it had left none, and its dir removed. A member
/// such a decode put in place that replaced no file stays: it is complete, and checked as that
/// decode checks its members. A dir that can't be read, or holding a file that can't go back (a
/// directory in its place, say), stays, as do the files still in it, for a later decode to
/// finish. While one stays that would put back a file over one of a decode's members once
/// finished, or that can't be read, opened or locked, so that none can tell, that decode fails
/// before it puts a member in place, naming the dir: finished after the install, the dir would
/// undo it. Nothing else: a `<member>.tmp` there, say, stays as it is. On a file system without
/// locks, or on platforms other than unix, decodes take no locks at all: they finish no
/// leftovers, which stay until removed by hand, and their installs are not serialised.
///
/// On an error `dir` is as it was once leftovers were finished: the members moved in so far are
/// taken out again, the files they replaced put back, and nothing of the decode is left (put_back
/// tells what it couldn't undo). A member's path that is a directory is an error, as is, on unix,
/// a `dir` the decode can't open to lock or read, or a lock that fails but the first on a file
/// system without locks.
///
/// Decodes of a unit into one dir at once each put complete, checked members in place, the last
/// to install winning; with installs one at a time, one that fails undoes only its own. Where
/// decodes take no locks, the last to move a member in wins, and one that fails while another is
/// putting its members in may undo some of that one's.
///
/// All of this holds for the decodes of one machine. On a network file system (NFS, SMB) a lock
/// on a dir is seen only by the machine that takes it: a decode on another machine, into a dir
/// both share, takes the install lock while this one holds it and finds this one's dir unlocked,
/// so it may finish that dir while this decode runs, leaving older files in place of its members
/// or failing it. Such a dir is to be decoded into from one machine at a time.
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
    staging.install(dir, &members)?;
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
/// itself if empty, and only then lets go of its own lock and, after that, of the install lock.
struct Staging {
    path: PathBuf,
    /// The members' files made in it.
    made: Vec<PathBuf>,
    /// Whether decodes into its dataset dir take locks: not on a file system without them, nor
    /// on platforms other than unix.
    locks: bool,
    /// Its own lock, an exclusive flock on it, from just after it is made: a sweep that finds it
    /// held leaves the dir alone. Dropped after Drop::drop.
    _own: Option<File>,
    /// The install lock, from install() on. Dropped after the own lock.
    installing: Option<File>,
}

/// Opens `path` and takes an exclusive flock on it, waiting while another holds one.
#[cfg(unix)]
fn locked(path: &Path) -> io::Result<File> {
    let file = File::open(path)?;
    loop {
        match file.lock() {
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            taken => return taken.map(|()| file),
        }
    }
}

/// The install lock of the dataset dir `dir`: an exclusive flock on it, waiting while another
/// decode holds it. A decode holds it while it finishes leftovers and makes its own dir, and
/// again (install_lock_again) while it finishes leftovers and puts its members in place. None
/// on a file system without locks, which this first taking tells: the decode then takes no
/// locks. A dir that can't be opened, or a lock that fails otherwise, is an error.
#[cfg(unix)]
fn install_lock(dir: &Path) -> io::Result<Option<File>> {
    match locked(dir) {
        Ok(lock) => Ok(Some(lock)),
        Err(e) if no_locks_here(&e) => Ok(None),
        Err(e) => Err(at(dir, e)),
    }
}

#[cfg(not(unix))]
fn install_lock(_dir: &Path) -> io::Result<Option<File>> {
    Ok(None)
}

/// The install lock of `dir` taken again, for a decode's install, once the decode took it to make
/// its dir (new_in): whether decodes take locks is told there, by the first, and here any error
/// is an error, ENOLCK too (flock gives it also when the kernel is out of lock records). Going on
/// without the lock, the install could go in beside another, or before a killed decode's leftover
/// is finished.
#[cfg(unix)]
fn install_lock_again(dir: &Path) -> io::Result<File> {
    install_lock_fault().map_or_else(|| locked(dir), Err).map_err(|e| at(dir, e))
}

#[cfg(not(unix))]
fn install_lock_again(_dir: &Path) -> io::Result<File> {
    // Never taken: there decodes take no locks.
    Err(io::Error::from(io::ErrorKind::Unsupported))
}

/// Whether `e`, an error taking a lock, says the file system has no locks.
#[cfg(unix)]
pub(crate) fn no_locks_here(e: &io::Error) -> bool {
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

/// A decode's dir in a dataset dir that a sweep couldn't finish: why, and the names of the files
/// it would put back once finished (none once its decode's members were all in, INSTALLED), or
/// None where the sweep couldn't tell (it couldn't open, lock or read the dir).
#[cfg_attr(not(unix), allow(dead_code))]
struct Unfinished {
    path: PathBuf,
    why: io::Error,
    puts_back: Option<Vec<std::ffi::OsString>>,
}

/// Finishes what each decode into `dir` that is gone left, its dir (finish_leftover); called with
/// the install lock held. A decode's dir whose lock it takes is one whose decode is gone (a
/// running decode holds its dir's lock from just after it made it until after it removed it):
/// it is finished, the lock held meanwhile. One whose lock is held (WouldBlock) is a running
/// decode's, decoding into it, and stays; one gone before it is opened (NotFound) is skipped, and
/// one gone after, once its decode removed it and let go of its lock, holds nothing to finish
/// (finish_leftover). Returns the dirs it couldn't finish, one that can't be opened or locked
/// among them, which stay. An error reading `dir` is an error.
#[cfg(unix)]
fn finish_leftovers(dir: &Path) -> io::Result<Vec<Unfinished>> {
    let mut unfinished = Vec::new();
    for e in std::fs::read_dir(dir).map_err(|e| at(dir, e))? {
        let e = e.map_err(|e| at(dir, e))?;
        if !is_staging_name(&e.file_name()) {
            continue;
        }
        let path = e.path();
        let lock = match e.file_type().and_then(|t| if t.is_dir() { File::open(&path).map(Some) } else { Ok(None) }) {
            Ok(Some(lock)) => lock,
            Ok(None) => continue,
            Err(why) if why.kind() == io::ErrorKind::NotFound => continue,
            Err(why) => {
                unfinished.push(Unfinished { path, why, puts_back: None });
                continue;
            }
        };
        seam(Step::Opened);
        match lock.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => continue,
            Err(std::fs::TryLockError::Error(why)) => {
                unfinished.push(Unfinished { path, why, puts_back: None });
                continue;
            }
        }
        if let Err((why, puts_back)) = finish_leftover(dir, &path) {
            unfinished.push(Unfinished { path, why, puts_back });
        }
    }
    Ok(unfinished)
}

#[cfg(not(unix))]
fn finish_leftovers(_dir: &Path) -> io::Result<Vec<Unfinished>> {
    Ok(Vec::new())
}

/// Finishes what decodes into `dir` that are gone left (finish_leftovers), under the install
/// lock, as a decode does before it makes its dir: for a caller about to read what `dir` holds,
/// which such a dir, finished later by a decode, would change, putting back the files its decode
/// replaced (the downloader, before it reads its ledger and lists the files to remove). A
/// running decode's dir stays. One that can't be finished and would put back a file once
/// finished, or of which none can tell, is an error naming it, as is a `dir` that can't be
/// opened, locked or read. Nothing where decodes take no locks (decode_into_dir).
pub fn finish_leftovers_in(dir: &Path) -> io::Result<()> {
    let Some(_install) = install_lock(dir)? else { return Ok(()) };
    for u in finish_leftovers(dir)? {
        let what = match &u.puts_back {
            Some(names) if names.is_empty() => continue,
            Some(names) => format!("it would put back {}", names.iter().map(|n| n.to_string_lossy()).collect::<Vec<_>>().join(", ")),
            None => "it may hold a file a decode replaced, which would go back".to_string(),
        };
        return Err(io::Error::new(u.why.kind(), format!("{} can't be finished ({}): {what}", u.path.display(), u.why)));
    }
    Ok(())
}

/// Finishes the decode into `dir` whose dir is `leftover`. If all its members went in (it holds
/// INSTALLED), the files they replaced are stale and go. If not, it is undone as put_back would
/// have: each file it replaced goes back to its place in `dir`, over the member that went in or
/// into the place left empty; a member that replaced no file stays, complete and checked. Then
/// the dir goes, with what else it holds. An error reading it, or a file that can't go back,
/// keeps the dir, and the files still in it, for a later decode to finish: the error, with the
/// names of the files still to go back (None where it couldn't read the dir). Called with the
/// lock of `leftover` held, taken once a sweep opened it: a `leftover` gone by then was removed
/// by its decode, which removes its dir only once it is empty, and before it lets go of its
/// lock. Nothing of it is left to finish.
#[cfg(unix)]
fn finish_leftover(dir: &Path, leftover: &Path) -> Result<(), (io::Error, Option<Vec<std::ffi::OsString>>)> {
    use std::os::unix::ffi::OsStrExt;
    let installed = match std::fs::symlink_metadata(leftover.join(INSTALLED)) {
        Ok(_) => true,
        Err(e) if e.kind() == io::ErrorKind::NotFound => false,
        Err(e) => return Err((e, None)),
    };
    let entries = match std::fs::read_dir(leftover).and_then(|d| d.collect::<io::Result<Vec<_>>>()) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err((e, None)),
    };
    let mut kept = Ok(());
    let mut to_go_back = Vec::new();
    for e in entries {
        let name = e.file_name();
        let Some(name) = name.as_bytes().strip_prefix(b".earlier.") else { continue };
        let name = std::ffi::OsStr::from_bytes(name);
        let gone = if installed { std::fs::remove_file(e.path()) } else { std::fs::rename(e.path(), dir.join(name)) };
        if let Err(why) = gone {
            kept = Err(why);
            if !installed {
                to_go_back.push(name.to_owned());
            }
        }
    }
    kept.map_err(|why| (why, Some(to_go_back)))?;
    std::fs::remove_dir_all(leftover).map_err(|why| (why, Some(Vec::new())))
}

/// Where a decode into a dir, or a sweep of the dir (finish_leftovers), is, as it tells the
/// tests' seam (which outside tests does nothing).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Step {
    /// A decode's dir opened by a sweep, before the sweep tries its lock.
    Opened,
    /// Its own dir made, the install lock held, before it locks the dir.
    Made,
    /// Its members decoded, before it takes the install lock to put them in place.
    WaitsToInstall,
    /// The install lock taken, before it finishes leftovers.
    Installs,
    /// A file a member replaces moved into its dir, before the member goes in.
    MovedAway,
    /// All its members in place and INSTALLED made, before it removes the files they replaced.
    AllIn,
    /// Its dir removed, the install lock taken for its install still held, before it lets go
    /// of its locks.
    Done,
}

#[cfg(not(test))]
fn seam(_: Step) {}

#[cfg(all(unix, not(test)))]
fn install_lock_fault() -> Option<io::Error> {
    None
}

/// What a test has the decodes and sweeps on this thread do at each Step.
#[cfg(test)]
#[derive(Default)]
struct Seam {
    /// Where each step is told, with whose decode it is.
    tell: Option<(&'static str, mpsc::Sender<(&'static str, Step)>)>,
    /// The step at which the next decode, or sweep, waits for a word, once.
    hold: Option<(Step, mpsc::Receiver<()>)>,
    /// The step at which the next decode stops, as one killed there would: it panics, and its
    /// dir stays with what it holds but the members it still made there.
    stop: Option<Step>,
    /// The error (an errno) the next decode's taking of the install lock for its install fails
    /// with, once (install_lock_again); its first, to make its dir, goes as it would.
    #[cfg_attr(not(unix), allow(dead_code))]
    fail_install_lock: Option<i32>,
}

#[cfg(test)]
thread_local! {
    static SEAM: std::cell::RefCell<Seam> = std::cell::RefCell::new(Seam::default());
}

#[cfg(all(unix, test))]
fn install_lock_fault() -> Option<io::Error> {
    SEAM.with_borrow_mut(|s| s.fail_install_lock.take()).map(io::Error::from_raw_os_error)
}

#[cfg(test)]
fn seam(step: Step) {
    let stop = SEAM.with_borrow_mut(|s| {
        if let Some((who, tell)) = &s.tell {
            let _ = tell.send((who, step));
        }
        if let Some((_, go)) = s.hold.take_if(|(at, _)| *at == step) {
            let _ = go.recv();
        }
        s.stop.take_if(|at| *at == step).is_some()
    });
    if stop {
        panic!("the decode stops here, as one killed here would");
    }
}

/// A member moved into place (once `installed`), the file it replaced moved to `earlier`.
struct Moved {
    dest: PathBuf,
    earlier: Option<PathBuf>,
    installed: bool,
}

impl Staging {
    /// A new dir in `dir`, under a name no member has, once it finished what decodes that are
    /// gone left there (finish_leftovers). It takes the install lock for that, and lets go of it
    /// only once its dir is made and locked: no sweep finds the dir before its lock is held. One
    /// of that name already there (an earlier process of this pid's, say) is skipped, never
    /// taken. A leftover it can't finish, or a `dir` it can't read, stays for install() to find.
    fn new_in(dir: &Path) -> io::Result<Staging> {
        let install = install_lock(dir)?;
        let locks = install.is_some();
        if locks {
            // Nothing goes in place here: install() sweeps again, and fails on what matters.
            let _ = finish_leftovers(dir);
        }
        #[cfg_attr(not(unix), allow(unused_mut))]
        let mut staging = loop {
            let path = dir.join(format!(".transport.{}.{}", std::process::id(), STAGINGS.fetch_add(1, Ordering::Relaxed)));
            match std::fs::create_dir(&path) {
                Ok(()) => break Staging { path, made: Vec::new(), locks, _own: None, installing: None },
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(at(&path, e)),
            }
        };
        seam(Step::Made);
        #[cfg(unix)]
        if locks {
            staging._own = Some(locked(&staging.path).map_err(|e| at(&staging.path, e))?);
        }
        drop(install);
        Ok(staging)
    }

    /// Takes the install lock on `dir` again, waiting while another decode holds it, and finishes
    /// what decodes that are gone left (finish_leftovers), before its members go in place: a
    /// decode killed while it put its members in is finished before any later decode puts its
    /// own in, never after. A lock that fails fails it, whatever the error (install_lock_again):
    /// no member goes in. So does a leftover it can't finish that would put back a file over one
    /// of `members` once finished (or of which it can't tell), or a `dir` it can't read. The lock
    /// is held until this is dropped. Nothing where decodes take no locks.
    fn install(&mut self, dir: &Path, members: &[Member]) -> io::Result<()> {
        if !self.locks {
            return Ok(());
        }
        seam(Step::WaitsToInstall);
        self.installing = Some(install_lock_again(dir)?);
        seam(Step::Installs);
        for u in finish_leftovers(dir)? {
            let what = match &u.puts_back {
                None => "it may hold a file a member replaced, which would go back over this decode's".to_string(),
                Some(names) => match members.iter().find(|m| names.iter().any(|n| *n == *m.name)) {
                    Some(m) => format!("it holds the file {} replaced, which would go back over this decode's", m.name),
                    None => continue,
                },
            };
            return Err(io::Error::new(u.why.kind(), format!("{} can't be finished ({}): {what}", u.path.display(), u.why)));
        }
        Ok(())
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
                return Err(put_back(moved, e, self.locks));
            }
        }
        let installed = self.path.join(INSTALLED);
        if let Err(e) = File::create(&installed) {
            return Err(put_back(moved, at(&installed, e), self.locks));
        }
        seam(Step::AllIn);
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
                    Ok(()) => {
                        seam(Step::MovedAway);
                        Some(earlier)
                    }
                    // Moved away since, by another decode of this dir installing at the same time
                    // (where decodes take no locks).
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
/// file it couldn't put back stays in the decode's dir, which then stays too: where decodes
/// take locks (`locks`) a later decode into the dir puts it back (finish_leftover); where they
/// take none, none does, and it is to be moved back by hand.
fn put_back(moved: Vec<Moved>, e: io::Error, locks: bool) -> io::Error {
    let later = if locks { ", for a later decode into the dir to put back" } else { ", to be moved back by hand" };
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
        if self.installing.is_some() {
            seam(Step::Done);
        }
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
