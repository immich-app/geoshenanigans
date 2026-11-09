//! The place index: the visible place nodes (`PlaceNode::is_visible`, the ones reverse geocoding
//! reads) of each admin cell (the cell, at the admin cell level, of a node's point), laid out as
//! the builder laid out its cell indexes before the cell pairs (lib.rs `lookup_place_cell_fb`): a
//! cells file of rows, ascending by cell (u64 cell id, u32 offset of its list in the entries
//! file), and an entries file of the lists (u16 count, then the node ids, ascending).
//!
//! Datasets don't ship it. `open` derives it from place_nodes.bin the first time a dataset loads,
//! into DERIVED_DIR beside the dataset's files (or, where those can't be written, into a private
//! dir of the temp dir kept for this user and that dataset), and maps it from there while it was
//! derived from the same place nodes.

use crate::{cell_id_at_level, mmap_opened, FileBytes, FileStamp, PlaceNode, NO_DATA};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::cell::OnceCell;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// The dir beside a dataset's files that holds what loading it derives from them.
pub const DERIVED_DIR: &str = "derived";
// Not `.bin`: compress_index.py publishes every .bin file of a build's output tree.
pub const CELLS_FILE: &str = "place_cells.idx";
pub const ENTRIES_FILE: &str = "place_entries.idx";
/// What the index there was derived from: the derivation (FORMAT), the cell level, and the place
/// nodes (place_nodes.bin's stamp and sha256). Written last, so it names only a complete index.
pub const SOURCE_FILE: &str = "place_index.json";
/// The derivation's version: an index another version derived is derived again.
const FORMAT: u64 = 2;
/// Sort memory: the (cell, id) pairs past this many bytes go to sorted runs on disk, merged at
/// the end, so a derivation's heap stays within a few MiB on planet.
pub const RUN_BYTES: usize = 4 << 20;
const NODE_BYTES: usize = std::mem::size_of::<PlaceNode>();
/// Place nodes read at once.
const READ_NODES: usize = 1 << 14;
/// The buffer of each sorted run while the runs merge, and of each file written.
const IO_BUFFER: usize = 64 << 10;
/// A pair in a sorted run on disk: u64 cell, u32 id.
const RUN_PAIR_BYTES: usize = 12;

pub struct PlaceIndex {
    pub cells: FileBytes,
    pub entries: FileBytes,
}

/// Why a place index couldn't be opened in a dir.
#[derive(Debug)]
pub(crate) enum Failure {
    /// The dir can't hold one (it can't be made, locked or written): another dir may.
    Unusable(String),
    /// No dir would change it (place_nodes.bin can't be read, or its nodes don't fit the layout).
    Failed(String),
}

impl Failure {
    fn message(self) -> String {
        match self {
            Failure::Unusable(m) | Failure::Failed(m) => m,
        }
    }

    /// The failure with the dir it happened in named, when the dir is what failed.
    fn in_dir(self, dir: &Path) -> Failure {
        match self {
            Failure::Unusable(m) => Failure::Unusable(format!("{}: {m}", dir.display())),
            failed => failed,
        }
    }
}

fn unusable(e: std::io::Error) -> Failure {
    Failure::Unusable(e.to_string())
}

/// How `open_with` derives: the sort memory, and the temp dir a dataset dir that can't be
/// written has its index in.
pub(crate) struct Options {
    pub run_bytes: usize,
    pub temp_root: PathBuf,
}

impl Default for Options {
    fn default() -> Self {
        Options { run_bytes: RUN_BYTES, temp_root: std::env::temp_dir() }
    }
}

/// The place index of `nodes`, the place_nodes.bin of the dataset in `dataset` as Index::load
/// opened it, at `cell_level`: the one in the dataset's DERIVED_DIR when it was derived from these
/// nodes at this level, else one derived there first. A dataset dir that can't hold one (read
/// only, or no room) has it in a private dir of the temp dir instead (`temp_dir_for`), found or
/// derived the same way.
pub fn open(dataset: &Path, nodes: &File, cell_level: u64) -> Result<PlaceIndex, String> {
    open_with(dataset, nodes, cell_level, &Options::default())
}

pub(crate) fn open_with(dataset: &Path, nodes: &File, cell_level: u64, opts: &Options) -> Result<PlaceIndex, String> {
    let stamp = nodes.metadata().map_err(|e| format!("{}/place_nodes.bin: {e}", dataset.display()))?;
    let nodes = Nodes { file: nodes, stamp: FileStamp::of(&stamp), sha256: OnceCell::new() };
    let in_dataset = match open_in(&dataset.join(DERIVED_DIR), &nodes, cell_level, opts.run_bytes, Access::Shared) {
        Ok(index) => return Ok(index),
        Err(Failure::Failed(m)) => return Err(m),
        Err(Failure::Unusable(m)) => m,
    };
    open_in(&temp_dir_for(dataset, &opts.temp_root), &nodes, cell_level, opts.run_bytes, Access::Private)
        .map_err(|f| format!("{in_dataset}; {}", f.message()))
}

/// The dir of `temp_root` that holds this user's place index of the dataset in `dataset` when the
/// dataset's own dir can't: one for each user and dataset dir (by its canonical path), so users
/// sharing a temp dir never share one.
pub(crate) fn temp_dir_for(dataset: &Path, temp_root: &Path) -> PathBuf {
    let path = std::fs::canonicalize(dataset).unwrap_or_else(|_| dataset.to_path_buf());
    let key = Sha256::digest(path.as_os_str().as_encoded_bytes());
    temp_root.join(format!("geocoder-place-index-{}-{key:x}", user_id()))
}

#[cfg(unix)]
fn user_id() -> u32 {
    // SAFETY: geteuid has no preconditions and cannot fail.
    unsafe { libc::geteuid() }
}

#[cfg(not(unix))]
fn user_id() -> u32 {
    0
}

/// place_nodes.bin as `open` read it: the file Index::load mapped, its stamp then, and its
/// sha256 once taken.
struct Nodes<'a> {
    file: &'a File,
    stamp: FileStamp,
    sha256: OnceCell<String>,
}

impl Nodes<'_> {
    /// The file from its first byte.
    fn from_start(&self) -> Result<&File, Failure> {
        let mut file = self.file;
        file.seek(SeekFrom::Start(0)).map_err(|e| Failure::Failed(format!("place_nodes.bin: {e}")))?;
        Ok(file)
    }

    fn sha256(&self) -> Result<&str, Failure> {
        if self.sha256.get().is_none() {
            let mut sha = Sha256::new();
            read_nodes(self.from_start()?, |bytes| sha.update(bytes))?;
            let _ = self.sha256.set(format!("{:x}", sha.finalize()));
        }
        Ok(self.sha256.get().unwrap())
    }
}

/// Who a dir `open_in` makes is for: every user (the dataset's own, beside its files) or this
/// one alone (one in the shared temp dir, which must be this user's own and open to no other:
/// another user could plant an index there).
#[derive(Clone, Copy, PartialEq)]
enum Access {
    Shared,
    Private,
}

/// The index in `dir`, made with `access` when missing, as `open` finds or derives it there.
fn open_in(dir: &Path, nodes: &Nodes, cell_level: u64, run_bytes: usize, access: Access) -> Result<PlaceIndex, Failure> {
    let held = make_dir(dir, access).and_then(|()| hold(dir)).map_err(|e| unusable(e).in_dir(dir))?;
    let index = match current(dir, nodes, cell_level)? {
        Some(index) => Ok(index),
        None => derive_in(dir, &held, nodes, cell_level, run_bytes).map_err(|f| f.in_dir(dir)),
    };
    drop(held);
    index
}

fn make_dir(dir: &Path, access: Access) -> std::io::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    if access == Access::Private {
        std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
    }
    match builder.create(dir) {
        Err(e) if e.kind() == ErrorKind::AlreadyExists => {}
        made => made?,
    }
    let made = if access == Access::Private { std::fs::symlink_metadata(dir)? } else { std::fs::metadata(dir)? };
    if !made.is_dir() {
        return Err(std::io::Error::other("in the way of the place index dir"));
    }
    #[cfg(unix)]
    if access == Access::Private {
        use std::os::unix::fs::MetadataExt;
        // The temp dir's sticky bit keeps others from moving a dir of this user's.
        if made.uid() != user_id() || made.mode() & 0o077 != 0 {
            return Err(std::io::Error::other("not a dir of this user's alone"));
        }
    }
    Ok(())
}

/// An exclusive lock on `dir`, released when the file drops: opens of one dir wait while another
/// derives there, then find its index current, and no derivation replaces files another reads.
/// The file also syncs the dir's entries (`derive_in`).
fn hold(dir: &Path) -> std::io::Result<File> {
    let held = File::open(dir)?;
    held.lock()?;
    Ok(held)
}

/// The index in `dir` when it was derived from `nodes` at `cell_level`, else None: the nodes'
/// stamp is the one recorded, or their sha256 is, as for a dataset copied with its derived dir
/// (into an image, say), whose copies have stamps of their own.
fn current(dir: &Path, nodes: &Nodes, cell_level: u64) -> Result<Option<PlaceIndex>, Failure> {
    let Some(source) = std::fs::read_to_string(dir.join(SOURCE_FILE)).ok().and_then(|s| serde_json::from_str::<Value>(&s).ok())
    else {
        return Ok(None);
    };
    if source["format"].as_u64() != Some(FORMAT) || source["cell_level"].as_u64() != Some(cell_level) {
        return Ok(None);
    }
    let recorded = &source["place_nodes"];
    let same_stamp = FileStamp::from_json(recorded) == Some(nodes.stamp);
    if !same_stamp && recorded["sha256"].as_str() != Some(nodes.sha256()?) {
        return Ok(None);
    }
    let (Ok(cells), Ok(entries)) = (map(&dir.join(CELLS_FILE)), map(&dir.join(ENTRIES_FILE))) else {
        return Ok(None);
    };
    if !same_stamp {
        // So the next open needn't hash the nodes. A dir that can't be written (in a read-only
        // image) hashes them at each open instead.
        let _ = write_source(dir, nodes.stamp, nodes.sha256()?, cell_level);
    }
    Ok(Some(PlaceIndex { cells, entries }))
}

fn map(path: &Path) -> Result<FileBytes, String> {
    let file = File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
    mmap_opened(&file, &path.display().to_string())
}

/// Derives the index of `nodes` at `cell_level` into `dir`, held as `held`. The dir's entries are
/// synced after each step, so after a crash its SOURCE_FILE names only an index on disk whole.
fn derive_in(dir: &Path, held: &File, nodes: &Nodes, cell_level: u64, run_bytes: usize) -> Result<PlaceIndex, Failure> {
    // First: until the new index is complete, none in `dir` is current.
    remove_if_present(&dir.join(SOURCE_FILE)).and_then(|()| held.sync_all()).map_err(unusable)?;
    let mut cells = Staged::create(dir.join(CELLS_FILE)).map_err(unusable)?;
    let mut entries = Staged::create(dir.join(ENTRIES_FILE)).map_err(unusable)?;
    let sha256 = derive(nodes.from_start()?, cell_level, run_bytes, dir, &mut cells.file, &mut entries.file)?;
    cells.publish().and_then(|()| entries.publish()).and_then(|()| held.sync_all()).map_err(unusable)?;
    write_source(dir, nodes.stamp, &sha256, cell_level).and_then(|()| held.sync_all()).map_err(unusable)?;
    let mapped = |name: &str| map(&dir.join(name)).map_err(Failure::Unusable);
    Ok(PlaceIndex { cells: mapped(CELLS_FILE)?, entries: mapped(ENTRIES_FILE)? })
}

fn write_source(dir: &Path, stamp: FileStamp, sha256: &str, cell_level: u64) -> std::io::Result<()> {
    let mut place_nodes = stamp.to_json();
    place_nodes["sha256"] = json!(sha256);
    let source = json!({"format": FORMAT, "cell_level": cell_level, "place_nodes": place_nodes});
    let mut staged = Staged::create(dir.join(SOURCE_FILE))?;
    staged.file.write_all(serde_json::to_string_pretty(&source).unwrap().as_bytes())?;
    staged.publish()
}

fn remove_if_present(path: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
        removed => removed,
    }
}

/// A file written under a temporary name and renamed into place once complete and on disk, so a
/// reader never maps part of one; the temporary file goes when it isn't.
struct Staged {
    path: PathBuf,
    tmp: PathBuf,
    file: BufWriter<File>,
}

impl Staged {
    fn create(path: PathBuf) -> std::io::Result<Staged> {
        let mut tmp = path.clone().into_os_string();
        tmp.push(".tmp");
        let tmp = PathBuf::from(tmp);
        // A dead derivation's (the dir is held). Never written through: it could be a symlink.
        remove_if_present(&tmp)?;
        let file = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
        Ok(Staged { path, tmp, file: BufWriter::with_capacity(IO_BUFFER, file) })
    }

    fn publish(mut self) -> std::io::Result<()> {
        self.file.flush()?;
        self.file.get_ref().sync_all()?;
        std::fs::rename(&self.tmp, &self.path)
    }
}

impl Drop for Staged {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.tmp);
    }
}

/// Calls `f` with the bytes of `nodes` (place_nodes.bin) in pieces of whole place nodes; refuses
/// a file that ends inside one.
fn read_nodes(mut nodes: impl Read, mut f: impl FnMut(&[u8])) -> Result<(), Failure> {
    let failed = |e: std::io::Error| Failure::Failed(format!("place_nodes.bin: {e}"));
    let mut buf = vec![0u8; READ_NODES * NODE_BYTES];
    loop {
        let mut n = 0;
        while n < buf.len() {
            match nodes.read(&mut buf[n..]) {
                Ok(0) => break,
                Ok(k) => n += k,
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Err(failed(e)),
            }
        }
        if n % NODE_BYTES != 0 {
            return Err(Failure::Failed("place_nodes.bin ends inside a place node".to_string()));
        }
        f(&buf[..n]);
        if n < buf.len() {
            return Ok(());
        }
    }
}

/// The place node of its NODE_BYTES in place_nodes.bin.
fn place_node(bytes: &[u8]) -> PlaceNode {
    let u32_at = |at: usize| u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap());
    PlaceNode {
        lat: f32::from_bits(u32_at(0)),
        lng: f32::from_bits(u32_at(4)),
        name_id: u32_at(8),
        place_type: bytes[12],
        _pad1: bytes[13],
        _pad2: u16::from_le_bytes([bytes[14], bytes[15]]),
        parent_poly_id: u32_at(16),
    }
}

/// Derives the place index of the place nodes `nodes` reads (place_nodes.bin's bytes) at
/// `cell_level` into `cells` and `entries`: a (cell, id) pair for each visible node (the others,
/// tombstones and hidden nodes, Index::place_node reads as none), sorted in runs of at most
/// `run_bytes`; with more than one run, each goes to `spill_dir` and the runs merge. Returns the
/// sha256 of the nodes read.
pub(crate) fn derive(nodes: impl Read, cell_level: u64, run_bytes: usize, spill_dir: &Path, cells: impl Write,
                     entries: impl Write) -> Result<String, Failure> {
    let run_pairs = (run_bytes / std::mem::size_of::<(u64, u32)>()).max(1);
    let mut sha = Sha256::new();
    let mut run: Vec<(u64, u32)> = Vec::with_capacity(run_pairs);
    let mut runs: Vec<Run> = Vec::new();
    let mut next_id: u64 = 0;
    let mut spill_error = None;
    read_nodes(nodes, |bytes| {
        sha.update(bytes);
        for node in bytes.chunks_exact(NODE_BYTES).map(place_node) {
            let id = next_id;
            next_id += 1;
            if spill_error.is_some() || !node.is_visible() {
                continue;
            }
            run.push((cell_id_at_level(node.lat as f64, node.lng as f64, cell_level), id as u32));
            if run.len() == run_pairs {
                match Run::spill(&mut run, spill_dir, runs.len()) {
                    Ok(spilled) => runs.push(spilled),
                    Err(e) => spill_error = Some(e),
                }
            }
        }
    })?;
    if let Some(e) = spill_error {
        return Err(unusable(e));
    }
    if next_id > u64::from(NO_DATA) {
        return Err(Failure::Failed(format!("place_nodes.bin holds {next_id} place nodes, more than a u32 id names")));
    }
    let mut out = CellWriter::new(cells, entries);
    if runs.is_empty() {
        run.sort_unstable();
        for &(cell, id) in &run {
            out.push(cell, id)?;
        }
    } else {
        if !run.is_empty() {
            runs.push(Run::spill(&mut run, spill_dir, runs.len()).map_err(unusable)?);
        }
        // Freed before the runs' buffers are taken.
        drop(run);
        merge(runs, &mut out)?;
    }
    out.finish()?;
    Ok(format!("{:x}", sha.finalize()))
}

/// A sorted run of pairs on disk, unlinked as soon as it is made where the system allows it.
struct Run {
    file: BufReader<File>,
    path: PathBuf,
    pairs: usize,
}

impl Run {
    /// Sorts `run` into the `k`th run file of `dir` and empties it.
    fn spill(run: &mut Vec<(u64, u32)>, dir: &Path, k: usize) -> std::io::Result<Run> {
        run.sort_unstable();
        let path = dir.join(format!("{CELLS_FILE}.run{k}.tmp"));
        remove_if_present(&path)?;
        let mut file = OpenOptions::new().read(true).write(true).create_new(true).open(&path)?;
        let _ = std::fs::remove_file(&path);
        {
            let mut out = BufWriter::with_capacity(IO_BUFFER, &mut file);
            for &(cell, id) in run.iter() {
                out.write_all(&cell.to_le_bytes())?;
                out.write_all(&id.to_le_bytes())?;
            }
            out.flush()?;
        }
        file.seek(SeekFrom::Start(0))?;
        let pairs = run.len();
        run.clear();
        Ok(Run { file: BufReader::with_capacity(IO_BUFFER, file), path, pairs })
    }

    fn next(&mut self) -> std::io::Result<Option<(u64, u32)>> {
        if self.pairs == 0 {
            return Ok(None);
        }
        self.pairs -= 1;
        let mut pair = [0u8; RUN_PAIR_BYTES];
        self.file.read_exact(&mut pair)?;
        Ok(Some((u64::from_le_bytes(pair[0..8].try_into().unwrap()), u32::from_le_bytes(pair[8..12].try_into().unwrap()))))
    }
}

impl Drop for Run {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// The pairs of `runs` into `out`, ascending.
fn merge<C: Write, E: Write>(mut runs: Vec<Run>, out: &mut CellWriter<C, E>) -> Result<(), Failure> {
    let mut heads = BinaryHeap::with_capacity(runs.len());
    for (k, run) in runs.iter_mut().enumerate() {
        if let Some(pair) = run.next().map_err(unusable)? {
            heads.push(Reverse((pair, k)));
        }
    }
    while let Some(Reverse(((cell, id), k))) = heads.pop() {
        out.push(cell, id)?;
        if let Some(pair) = runs[k].next().map_err(unusable)? {
            heads.push(Reverse((pair, k)));
        }
    }
    Ok(())
}

/// Writes (cell, id) pairs, given ascending, as a cell index: a row in `cells` for each cell and
/// its list in `entries`.
struct CellWriter<C: Write, E: Write> {
    cells: C,
    entries: E,
    entries_written: u64,
    cell: u64,
    ids: Vec<u32>,
}

impl<C: Write, E: Write> CellWriter<C, E> {
    fn new(cells: C, entries: E) -> Self {
        CellWriter { cells, entries, entries_written: 0, cell: 0, ids: Vec::new() }
    }

    fn push(&mut self, cell: u64, id: u32) -> Result<(), Failure> {
        if !self.ids.is_empty() && cell != self.cell {
            self.end_cell()?;
        }
        if self.ids.len() == usize::from(u16::MAX) {
            return Err(Failure::Failed(format!("cell {cell:#x} holds more place nodes than a list counts ({})", u16::MAX)));
        }
        self.cell = cell;
        self.ids.push(id);
        Ok(())
    }

    fn end_cell(&mut self) -> Result<(), Failure> {
        let offset = u32::try_from(self.entries_written)
            .map_err(|_| Failure::Failed("the place entries pass the 4 GiB a cell row's offset reaches".to_string()))?;
        self.write_cell(offset).map_err(unusable)?;
        self.entries_written += 2 + 4 * self.ids.len() as u64;
        self.ids.clear();
        Ok(())
    }

    fn write_cell(&mut self, offset: u32) -> std::io::Result<()> {
        self.cells.write_all(&self.cell.to_le_bytes())?;
        self.cells.write_all(&offset.to_le_bytes())?;
        self.entries.write_all(&(self.ids.len() as u16).to_le_bytes())?;
        for id in &self.ids {
            self.entries.write_all(&id.to_le_bytes())?;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<(), Failure> {
        if !self.ids.is_empty() {
            self.end_cell()?;
        }
        self.cells.flush().and_then(|()| self.entries.flush()).map_err(unusable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scratch_dir::ScratchDir;
    use crate::DEFAULT_ADMIN_CELL_LEVEL;
    use std::collections::BTreeMap;

    /// place_nodes.bin of `n` named nodes spread over about 300 admin cells in an order unrelated
    /// to their cells, every seventh node a tombstone.
    fn nodes(n: u32) -> Vec<u8> {
        let mut out = Vec::new();
        for i in 0..n {
            let spot = i.wrapping_mul(2_654_435_761) % 300;
            let lat = 10.0 + (spot / 20) as f32 * 0.2 + (i % 13) as f32 * 1e-3;
            let lng = 20.0 + (spot % 20) as f32 * 0.2 + (i % 11) as f32 * 1e-3;
            let name_id = if i % 7 == 3 { NO_DATA } else { i * 4 };
            for word in [lat.to_bits(), lng.to_bits(), name_id, 1, NO_DATA] {
                out.extend_from_slice(&word.to_le_bytes());
            }
        }
        out
    }

    /// The index of `nodes` as the builder writes one: each named node's id in its cell's list.
    fn builders_index(nodes: &[u8]) -> (Vec<u8>, Vec<u8>) {
        let mut lists: BTreeMap<u64, Vec<u32>> = BTreeMap::new();
        for (id, node) in nodes.chunks_exact(NODE_BYTES).map(place_node).enumerate() {
            if node.name_id != NO_DATA {
                let cell = cell_id_at_level(node.lat as f64, node.lng as f64, DEFAULT_ADMIN_CELL_LEVEL);
                lists.entry(cell).or_default().push(id as u32);
            }
        }
        let (mut cells, mut entries) = (Vec::new(), Vec::new());
        for (cell, ids) in lists {
            cells.extend_from_slice(&cell.to_le_bytes());
            cells.extend_from_slice(&(entries.len() as u32).to_le_bytes());
            entries.extend_from_slice(&(ids.len() as u16).to_le_bytes());
            ids.iter().for_each(|id| entries.extend_from_slice(&id.to_le_bytes()));
        }
        (cells, entries)
    }

    fn derived(nodes: &[u8], run_bytes: usize, spill_dir: &Path) -> (Vec<u8>, Vec<u8>, String) {
        let (mut cells, mut entries) = (Vec::new(), Vec::new());
        let sha = derive(nodes, DEFAULT_ADMIN_CELL_LEVEL, run_bytes, spill_dir, &mut cells, &mut entries).unwrap();
        (cells, entries, sha)
    }

    fn file_bytes(f: &FileBytes) -> Vec<u8> {
        f.read_chunk(0, f.len() as usize).into_owned()
    }

    #[test]
    fn a_derivation_spilled_in_runs_of_64_kib_gives_the_same_bytes() {
        let spill = ScratchDir::new();
        let nodes = nodes(40_000);

        let (cells, entries, sha) = derived(&nodes, 64 << 10, spill.path());

        // 34,286 named nodes, 4,096 pairs a run: nine runs, each spread over every cell.
        assert_eq!((cells.clone(), entries.clone()), builders_index(&nodes));
        let (in_memory_cells, in_memory_entries, in_memory_sha) = derived(&nodes, RUN_BYTES, spill.path());
        assert_eq!((cells, entries, sha), (in_memory_cells, in_memory_entries, in_memory_sha));
        assert_eq!(std::fs::read_dir(spill.path()).unwrap().count(), 0, "a run file was left");
    }

    #[test]
    fn a_file_ending_inside_a_place_node_is_refused() {
        let spill = ScratchDir::new();
        let mut nodes = nodes(3);
        nodes.pop();
        let (mut cells, mut entries) = (Vec::new(), Vec::new());

        let refused = derive(&nodes[..], DEFAULT_ADMIN_CELL_LEVEL, RUN_BYTES, spill.path(), &mut cells, &mut entries);

        assert_eq!(refused.unwrap_err().message(), "place_nodes.bin ends inside a place node");
    }

    /// A dataset dir holding only `nodes` as place_nodes.bin.
    fn dataset(nodes: &[u8]) -> ScratchDir {
        let dir = ScratchDir::new();
        std::fs::write(dir.path().join("place_nodes.bin"), nodes).unwrap();
        dir
    }

    fn open_in_temp(dataset: &Path, temp_root: &Path) -> Result<PlaceIndex, String> {
        let nodes = File::open(dataset.join("place_nodes.bin")).unwrap();
        open_with(dataset, &nodes, DEFAULT_ADMIN_CELL_LEVEL, &Options { run_bytes: RUN_BYTES, temp_root: temp_root.to_path_buf() })
    }

    fn ino(path: &Path) -> u64 {
        FileStamp::of(&std::fs::metadata(path).unwrap()).ino
    }

    #[test]
    fn a_dataset_dir_that_cant_hold_the_index_has_it_in_a_temp_dir_of_its_own() {
        let temp = ScratchDir::new();
        let (a, b) = (dataset(&nodes(50)), dataset(&nodes(60)));
        for ds in [&a, &b] {
            // Where the index would go, a file nobody (not even root) can make a dir of.
            std::fs::write(ds.path().join(DERIVED_DIR), b"").unwrap();
        }

        let index = open_in_temp(a.path(), temp.path()).unwrap();
        open_in_temp(b.path(), temp.path()).unwrap();

        let a_dir = temp_dir_for(a.path(), temp.path());
        assert!(std::fs::metadata(a.path().join(DERIVED_DIR)).unwrap().is_file());
        assert_eq!((file_bytes(&index.cells), file_bytes(&index.entries)), builders_index(&nodes(50)));
        assert_ne!(a_dir, temp_dir_for(b.path(), temp.path()));
        for dir in [&a_dir, &temp_dir_for(b.path(), temp.path())] {
            assert!(dir.join(SOURCE_FILE).is_file(), "{}", dir.display());
        }
        // Opened again, it is current there.
        let cells = ino(&a_dir.join(CELLS_FILE));
        open_in_temp(a.path(), temp.path()).unwrap();
        assert_eq!(ino(&a_dir.join(CELLS_FILE)), cells);
    }

    #[cfg(unix)]
    #[test]
    fn a_temp_index_dir_other_users_can_open_is_refused() {
        use std::os::unix::fs::PermissionsExt;
        let temp = ScratchDir::new();
        let ds = dataset(&nodes(5));
        std::fs::write(ds.path().join(DERIVED_DIR), b"").unwrap();
        let planted = temp_dir_for(ds.path(), temp.path());
        std::fs::create_dir(&planted).unwrap();
        std::fs::set_permissions(&planted, std::fs::Permissions::from_mode(0o755)).unwrap();

        let refused = open_in_temp(ds.path(), temp.path());

        assert!(refused.is_err_and(|e| e.contains("not a dir of this user's alone")));
        assert_eq!(std::fs::read_dir(&planted).unwrap().count(), 0);
    }

    #[test]
    fn an_index_left_without_its_source_file_is_derived_again() {
        // As a derivation cut off before it wrote the source file leaves one.
        let temp = ScratchDir::new();
        let ds = dataset(&nodes(50));
        open_in_temp(ds.path(), temp.path()).unwrap();
        let derived = ds.path().join(DERIVED_DIR);
        std::fs::remove_file(derived.join(SOURCE_FILE)).unwrap();
        std::fs::write(derived.join(CELLS_FILE), b"part of an index").unwrap();

        let index = open_in_temp(ds.path(), temp.path()).unwrap();

        assert_eq!((file_bytes(&index.cells), file_bytes(&index.entries)), builders_index(&nodes(50)));
        assert!(derived.join(SOURCE_FILE).is_file());
    }

    #[cfg(unix)]
    #[test]
    fn a_symlink_in_place_of_a_temp_index_dir_is_refused() {
        let temp = ScratchDir::new();
        let ds = dataset(&nodes(5));
        std::fs::write(ds.path().join(DERIVED_DIR), b"").unwrap();
        let elsewhere = ScratchDir::new();
        std::os::unix::fs::symlink(elsewhere.path(), temp_dir_for(ds.path(), temp.path())).unwrap();

        let refused = open_in_temp(ds.path(), temp.path());

        assert!(refused.is_err_and(|e| e.contains("in the way of the place index dir")));
        assert_eq!(std::fs::read_dir(elsewhere.path()).unwrap().count(), 0);
    }
}
