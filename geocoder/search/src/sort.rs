//! The index build's external sort, in bounded memory: entries (a byte key and a fixed-size
//! record) go into a run of a fixed budget, which is sorted and spilled to a file whenever it
//! fills; the runs are then merged back in order, first in groups of MERGE_FAN_IN into longer
//! runs while more than MERGE_FAN_IN are left.
use crate::constants::*;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

/// The fixed-size part of a sort entry, ordered after the entry's key. `order` must tell apart
/// any two records that `put` writes differently, so the sorted entries do not depend on how
/// they were cut into runs (by the budget, or among threads).
pub trait SortRecord: Copy {
    /// Bytes of a record in a run file.
    const BYTES: usize;
    /// Writes the record into `out`, BYTES long.
    fn put(&self, out: &mut [u8]);
    /// The record `put` wrote into `b`.
    fn get(b: &[u8]) -> Self;
    fn order(&self, other: &Self) -> Ordering;
}

/// A fixed-capacity array in its own anonymous mapping. Dropping it unmaps it, so a phase's
/// buffers go back to the system before the next phase: glibc keeps freed heap memory for reuse
/// once a block this large has been freed, and the next phase's buffers came on top of it.
pub(crate) struct MapVec<T: Copy> {
    map: memmap2::MmapMut,
    len: usize,
    cap: usize,
    _t: PhantomData<T>,
}

impl<T: Copy> MapVec<T> {
    pub(crate) fn with_capacity(cap: usize) -> std::io::Result<Self> {
        const { assert!(std::mem::align_of::<T>() <= MIN_PAGE_BYTES, "an element aligned past a page") };
        let map = memmap2::MmapOptions::new().len((cap * std::mem::size_of::<T>()).max(1)).map_anon()?;
        Ok(MapVec { map, len: 0, cap, _t: PhantomData })
    }

    pub(crate) fn capacity(&self) -> usize {
        self.cap
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(crate) fn clear(&mut self) {
        self.len = 0;
    }

    pub(crate) fn push(&mut self, v: T) {
        assert!(self.len < self.cap, "MapVec full");
        // SAFETY: in bounds of the mapping, which is page-aligned (so aligned for T).
        unsafe { (self.map.as_mut_ptr() as *mut T).add(self.len).write(v) };
        self.len += 1;
    }

    pub(crate) fn extend_from_slice(&mut self, s: &[T]) {
        assert!(self.len + s.len() <= self.cap, "MapVec full");
        // SAFETY: in bounds (checked above); a fresh mapping never overlaps `s`.
        unsafe { std::ptr::copy_nonoverlapping(s.as_ptr(), (self.map.as_mut_ptr() as *mut T).add(self.len), s.len()) };
        self.len += s.len();
    }

    pub(crate) fn as_slice(&self) -> &[T] {
        // SAFETY: the first `len` elements were written by push.
        unsafe { std::slice::from_raw_parts(self.map.as_ptr() as *const T, self.len) }
    }

    pub(crate) fn as_mut_slice(&mut self) -> &mut [T] {
        // SAFETY: as in as_slice.
        unsafe { std::slice::from_raw_parts_mut(self.map.as_mut_ptr() as *mut T, self.len) }
    }
}

/// One entry of a run in memory: its key in the run's arena, and its record.
#[derive(Clone, Copy)]
struct Item<R> {
    off: u32,
    len: u16,
    rec: R,
}

/// Sort run files, removed when dropped.
#[derive(Default)]
struct RunFiles(Vec<PathBuf>);

impl RunFiles {
    /// The first `n` runs, no longer held here.
    fn take_first(&mut self, n: usize) -> RunFiles {
        RunFiles(self.0.drain(..n).collect())
    }
}

impl Drop for RunFiles {
    fn drop(&mut self) {
        for f in &self.0 {
            let _ = std::fs::remove_file(f);
        }
    }
}

/// Entries in sorted runs of a fixed memory budget: `push` each, then `finish` for the runs. A
/// failed `push` or `finish` leaves it to be dropped, its runs with it. Its runs are named after
/// its tag, which no other sort in the same directory may share.
pub struct ExternalSort<R: SortRecord> {
    arena: MapVec<u8>,
    items: MapVec<Item<R>>,
    runs: SortedRuns<R>,
    dir: PathBuf,
    tag: String,
    scratch: Vec<u8>,
}

impl<R: SortRecord> ExternalSort<R> {
    /// A sort in `budget` bytes of run buffers, whose runs go to `dir` as `<tag>-<n>.run`.
    /// An error if the budget cannot hold one entry.
    pub fn new(budget: usize, dir: &Path, tag: String) -> std::io::Result<Self> {
        Self::with_arena_limit(budget, MAX_RUN_ARENA_BYTES, dir, tag)
    }

    /// `new` with at most `arena_limit` key bytes per run.
    fn with_arena_limit(budget: usize, arena_limit: usize, dir: &Path, tag: String) -> std::io::Result<Self> {
        let (arena_cap, items_cap) = run_capacities::<R>(budget, arena_limit);
        if items_cap == 0 {
            let e = format!("a sort budget of {budget} bytes holds no entry");
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, e));
        }

        Ok(ExternalSort {
            arena: MapVec::with_capacity(arena_cap)?,
            items: MapVec::with_capacity(items_cap)?,
            runs: SortedRuns::default(),
            dir: dir.to_path_buf(),
            tag,
            scratch: vec![0; R::BYTES],
        })
    }

    /// Adds one entry; an error for a key longer than a run holds: MAX_KEY_BYTES (a run entry's
    /// u16 length), or the run's key arena when that is smaller.
    pub fn push(&mut self, key: &[u8], rec: R) -> std::io::Result<()> {
        let fits = MAX_KEY_BYTES.min(self.arena.capacity());
        if key.len() > fits {
            let e = format!("a key of {} bytes, over the {fits} bytes a sort run holds", key.len());
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, e));
        }

        if self.arena.len() + key.len() > self.arena.capacity() || self.items.len() == self.items.capacity() {
            self.spill()?;
        }
        let off = self.arena.len() as u32;
        self.arena.extend_from_slice(key);
        self.items.push(Item { off, len: key.len() as u16, rec });
        Ok(())
    }

    /// Spills the last run and frees the buffers.
    pub fn finish(mut self) -> std::io::Result<SortedRuns<R>> {
        self.spill()?;
        Ok(self.runs)
    }

    fn spill(&mut self) -> std::io::Result<()> {
        if self.items.is_empty() {
            return Ok(());
        }
        let arena = self.arena.as_slice();
        let key = |it: &Item<R>| &arena[it.off as usize..it.off as usize + it.len as usize];
        self.items.as_mut_slice().sort_unstable_by(|a, b| key(a).cmp(key(b)).then_with(|| a.rec.order(&b.rec)));
        let path = self.dir.join(format!("{}-{}.run", self.tag, self.runs.len()));
        let file = File::create(&path)?;
        // Held from here: a run whose write fails goes with the others.
        self.runs.files.0.push(path);
        let mut w = BufWriter::with_capacity(WRITE_BUFFER, file);
        for it in self.items.as_slice() {
            self.runs.spilled += write_entry(&mut w, key(it), &it.rec, &mut self.scratch)?;
        }
        w.flush()?;
        self.items.clear();
        self.arena.clear();
        Ok(())
    }
}

/// Sorted runs on disk, removed when dropped. A run is held from its file's creation until the
/// merge that reads it is done with it, so a build that fails anywhere leaves none behind.
pub struct SortedRuns<R> {
    files: RunFiles,
    spilled: u64,
    _rec: PhantomData<R>,
}

impl<R> Default for SortedRuns<R> {
    fn default() -> Self {
        SortedRuns { files: RunFiles::default(), spilled: 0, _rec: PhantomData }
    }
}

impl<R: SortRecord> SortedRuns<R> {
    pub fn len(&self) -> usize {
        self.files.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.files.0.is_empty()
    }

    /// Bytes the runs were spilled as.
    pub fn spilled_bytes(&self) -> u64 {
        self.spilled
    }

    /// Takes over `other`'s runs, after this one's.
    pub fn append(&mut self, mut other: SortedRuns<R>) {
        self.files.0.append(&mut other.files.0);
        self.spilled += other.spilled;
    }

    /// Every run's entries in order: the runs are merged in groups of MERGE_FAN_IN into longer
    /// runs in `dir` (named after `tag`) until at most MERGE_FAN_IN are left, which the merge
    /// then reads.
    pub fn merge(self, dir: &Path, tag: &str) -> std::io::Result<Merged<R>> {
        let (files, rewritten) = reduce_runs::<R>(self.files, dir, tag)?;
        let mut m = Merged::open(files)?;
        m.rewritten = rewritten;
        Ok(m)
    }
}

/// The entries of sorted runs in (key, record, run) order. It holds the runs until dropped.
pub struct Merged<R> {
    readers: Vec<RunReader>,
    heap: BinaryHeap<Head<R>>,
    files: RunFiles,
    rewritten: u64,
    error: Option<(usize, std::io::Error)>,
}

impl<R: SortRecord> Merged<R> {
    fn open(files: RunFiles) -> std::io::Result<Self> {
        let mut readers = Vec::with_capacity(files.0.len());
        for f in &files.0 {
            readers.push(RunReader::open(f).map_err(|e| run_error(f, &e))?);
        }
        let mut heap = BinaryHeap::with_capacity(readers.len());
        for (run, r) in readers.iter_mut().enumerate() {
            let mut key = Vec::new();
            if let Some(rec) = r.read_into(&mut key).map_err(|e| run_error(&files.0[run], &e))? {
                heap.push(Head { key, rec, run });
            }
        }
        Ok(Merged { readers, heap, files, rewritten: 0, error: None })
    }

    /// Bytes the runs over MERGE_FAN_IN were merged into longer runs as.
    pub fn rewritten_bytes(&self) -> u64 {
        self.rewritten
    }

    /// The next entry's record and its key in `key`, or None at the end. A run read failure
    /// returns the same error on this and all subsequent calls.
    pub fn next(&mut self, key: &mut Vec<u8>) -> std::io::Result<Option<R>> {
        if let Some((run, e)) = &self.error {
            return Err(run_error(&self.files.0[*run], e));
        }

        let Some(mut top) = self.heap.peek_mut() else { return Ok(None) };
        key.clear();
        key.extend_from_slice(&top.key);
        let rec = top.rec;
        let run = top.run;
        match self.readers[run].read_into(&mut top.key) {
            Ok(Some(next)) => top.rec = next,
            Ok(None) => {
                std::collections::binary_heap::PeekMut::pop(top);
            }
            Err(e) => {
                // The read can replace part of the head's key before it fails.
                let err = run_error(&self.files.0[run], &e);
                self.error = Some((run, e));
                return Err(err);
            }
        }
        Ok(Some(rec))
    }
}

/// A sort run's open or read error, naming the run.
fn run_error(path: &Path, e: &std::io::Error) -> std::io::Error {
    std::io::Error::new(e.kind(), format!("sort run {}: {e}", path.display()))
}

/// Merges groups of at most MERGE_FAN_IN consecutive runs into longer runs until at most
/// MERGE_FAN_IN are left (consecutive, so entries that compare equal keep their run order).
/// Each pass merges every run while more than MERGE_FAN_IN² are left, then only as many of the
/// last runs as it takes to leave MERGE_FAN_IN. Returns the runs left and the bytes rewritten.
fn reduce_runs<R: SortRecord>(mut files: RunFiles, dir: &Path, tag: &str) -> std::io::Result<(RunFiles, u64)> {
    let mut rewritten = 0u64;
    let mut suffix = 0;
    let mut key = Vec::new();
    let mut scratch = vec![0; R::BYTES];
    while files.0.len() > MERGE_FAN_IN {
        let n = files.0.len();
        let excess = n - MERGE_FAN_IN;
        let merged = if n > MERGE_FAN_IN * MERGE_FAN_IN { n } else { excess + excess.div_ceil(MERGE_FAN_IN - 1) };
        // The runs left as they are, then each group's merged run; `files` holds those not merged
        // yet, so every run has one holder until its group's merge has removed it.
        let mut next = files.take_first(n - merged);
        while !files.0.is_empty() {
            let mut group = files.take_first(MERGE_FAN_IN.min(files.0.len()));
            if group.0.len() == 1 {
                next.0.append(&mut group.0);
                continue;
            }
            let (path, file) = loop {
                let path = dir.join(format!("{tag}-{suffix}.merge"));
                suffix += 1;
                match File::create_new(&path) {
                    Ok(file) => break (path, file),
                    Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                    Err(e) => return Err(e),
                }
            };
            next.0.push(path);
            let mut w = BufWriter::with_capacity(WRITE_BUFFER, file);
            let mut m = Merged::<R>::open(group)?;
            while let Some(rec) = m.next(&mut key)? {
                rewritten += write_entry(&mut w, &key, &rec, &mut scratch)?;
            }
            w.flush()?;
        }
        files = next;
    }
    Ok((files, rewritten))
}

/// One run entry: u16 key length, key, record (`scratch` holds its bytes). Returns the bytes
/// written.
fn write_entry<R: SortRecord>(w: &mut impl Write, key: &[u8], rec: &R, scratch: &mut Vec<u8>) -> std::io::Result<u64> {
    scratch.resize(R::BYTES, 0);
    rec.put(scratch);
    w.write_all(&(key.len() as u16).to_le_bytes())?;
    w.write_all(key)?;
    w.write_all(scratch)?;
    Ok((2 + key.len() + R::BYTES) as u64)
}

/// A run's next entry at the head of a merge.
struct Head<R> {
    key: Vec<u8>,
    rec: R,
    run: usize,
}

impl<R: SortRecord> PartialEq for Head<R> {
    fn eq(&self, o: &Self) -> bool {
        self.cmp(o) == Ordering::Equal
    }
}
impl<R: SortRecord> Eq for Head<R> {}
impl<R: SortRecord> PartialOrd for Head<R> {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl<R: SortRecord> Ord for Head<R> {
    // reversed: BinaryHeap pops the smallest entry first
    fn cmp(&self, o: &Self) -> Ordering {
        o.key.cmp(&self.key).then_with(|| o.rec.order(&self.rec)).then(o.run.cmp(&self.run))
    }
}

struct RunReader {
    r: BufReader<File>,
    rec: Vec<u8>,
}

impl RunReader {
    fn open(path: &Path) -> std::io::Result<Self> {
        Ok(RunReader { r: BufReader::with_capacity(RUN_READ_BUFFER, File::open(path)?), rec: Vec::new() })
    }

    /// The run's next entry, its key into `key`: None at the run's end (nothing left before an
    /// entry), an error for a failed read or an entry cut short.
    fn read_into<R: SortRecord>(&mut self, key: &mut Vec<u8>) -> std::io::Result<Option<R>> {
        if self.r.fill_buf()?.is_empty() {
            return Ok(None);
        }
        let mut len = [0u8; 2];
        self.r.read_exact(&mut len)?;
        key.resize(u16::from_le_bytes(len) as usize, 0);
        self.r.read_exact(key)?;
        self.rec.resize(R::BYTES, 0);
        self.r.read_exact(&mut self.rec)?;
        Ok(Some(R::get(&self.rec)))
    }
}

/// Sort-run memory of each of `threads` workers sharing `mem_mb`; an error below
/// MIN_RUN_BUDGET_BYTES, where the longest key would not fit a run.
pub fn run_budget(mem_mb: usize, threads: usize) -> Result<usize, String> {
    let budget = (mem_mb << 20) / threads;
    if budget < MIN_RUN_BUDGET_BYTES {
        return Err(format!("Sort budget too small: {budget} bytes per thread, at least {MIN_RUN_BUDGET_BYTES}"));
    }

    Ok(budget)
}

/// (key bytes, entries) one sort run of `budget` bytes holds. The arena takes up to
/// RUN_ARENA_PERCENT of the budget, leaving room for at least one entry if it fits.
fn run_capacities<R>(budget: usize, arena_limit: usize) -> (usize, usize) {
    let item_bytes = std::mem::size_of::<Item<R>>();
    let arena_share = (budget as u128 * RUN_ARENA_PERCENT as u128 / 100) as usize;
    let arena_cap = arena_share.min(arena_limit).min(budget.saturating_sub(item_bytes));
    (arena_cap, (budget - arena_cap) / item_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::{Rng, TempDir};

    /// A test record: a number and a byte, ordered as the pair.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    struct Rec(u32, u8);

    impl SortRecord for Rec {
        const BYTES: usize = 5;

        fn put(&self, out: &mut [u8]) {
            out[..4].copy_from_slice(&self.0.to_le_bytes());
            out[4] = self.1;
        }

        fn get(b: &[u8]) -> Self {
            Rec(u32::from_le_bytes(b[..4].try_into().unwrap()), b[4])
        }

        fn order(&self, other: &Self) -> Ordering {
            self.cmp(other)
        }
    }

    #[derive(Clone, Copy)]
    struct Wide([u8; Wide::BYTES]);

    impl SortRecord for Wide {
        const BYTES: usize = 1024;

        fn put(&self, out: &mut [u8]) {
            out.copy_from_slice(&self.0);
        }

        fn get(b: &[u8]) -> Self {
            Wide(b.try_into().unwrap())
        }

        fn order(&self, other: &Self) -> Ordering {
            self.0.cmp(&other.0)
        }
    }

    type Entry = (Vec<u8>, Rec);

    fn in_memory(mut entries: Vec<Entry>) -> Vec<Entry> {
        entries.sort();
        entries
    }

    /// Writes `entries` as one run, sorted, and returns its path.
    fn write_run(dir: &Path, name: &str, entries: &[Entry]) -> PathBuf {
        let path = dir.join(name);
        let mut w = BufWriter::new(File::create(&path).unwrap());
        let mut scratch = Vec::new();
        for (k, r) in in_memory(entries.to_vec()) {
            write_entry(&mut w, &k, &r, &mut scratch).unwrap();
        }
        w.flush().unwrap();
        path
    }

    fn read_all(m: std::io::Result<Merged<Rec>>) -> Vec<Entry> {
        let mut m = m.unwrap();
        let mut key = Vec::new();
        let mut out = Vec::new();
        while let Some(r) = m.next(&mut key).unwrap() {
            out.push((key.clone(), r));
        }
        out
    }

    fn sorted(sut: ExternalSort<Rec>, dir: &Path) -> Vec<Entry> {
        read_all(sut.finish().unwrap().merge(dir, "m"))
    }

    fn runs_of(files: Vec<PathBuf>) -> SortedRuns<Rec> {
        SortedRuns { files: RunFiles(files), spilled: 0, _rec: PhantomData }
    }

    /// `n` runs of random entries; returns the run files and every entry.
    fn random_runs(dir: &Path, n: usize, rng: &mut Rng) -> (Vec<PathBuf>, Vec<Entry>) {
        let mut all = Vec::new();
        let mut files = Vec::new();
        for r in 0..n {
            let run: Vec<Entry> = (0..(1 + rng.next() % 40))
                .map(|_| (format!("k{}", rng.next() % 50).into_bytes(), Rec((rng.next() % 1000) as u32, (rng.next() % 4) as u8)))
                .collect();
            files.push(write_run(dir, &format!("t-{r}.run"), &run));
            all.extend(run);
        }
        (files, in_memory(all))
    }

    /// `n` random entries: short keys over a small alphabet (so keys repeat and prefix each
    /// other, the empty key among them), records that repeat, and entries that repeat whole.
    fn random_entries(n: usize, rng: &mut Rng) -> Vec<Entry> {
        let mut out: Vec<Entry> = Vec::with_capacity(n);
        for _ in 0..n {
            if !out.is_empty() && rng.next().is_multiple_of(10) {
                let again = out[(rng.next() % out.len() as u64) as usize].clone();
                out.push(again);
                continue;
            }
            let key = (0..rng.next() % 5).map(|_| b"ab\x00\xff"[(rng.next() % 4) as usize]).collect();
            out.push((key, Rec((rng.next() % 8) as u32, (rng.next() % 3) as u8)));
        }
        out
    }

    #[test]
    fn map_vec_holds_what_was_pushed() {
        let mut v: MapVec<u32> = MapVec::with_capacity(4).unwrap();
        v.push(3);
        v.extend_from_slice(&[1, 2]);
        v.as_mut_slice().sort_unstable();
        assert_eq!(v.as_slice(), [1, 2, 3]);
        v.clear();
        assert!(v.is_empty());
        assert_eq!(v.capacity(), 4);
    }

    #[test]
    #[should_panic(expected = "MapVec full")]
    fn map_vec_refuses_to_grow() {
        let mut v: MapVec<u8> = MapVec::with_capacity(2).unwrap();
        v.extend_from_slice(b"abc");
    }

    #[test]
    fn run_budget_refuses_less_than_the_longest_key_needs() {
        for (mem_mb, threads, want) in
            [(0, 1, None), (1, 16, None), (1, 9, Some((1 << 20) / 9)), (DEFAULT_RUN_BUDGET_MB, 1, Some(DEFAULT_RUN_BUDGET_MB << 20))]
        {
            assert_eq!(run_budget(mem_mb, threads).ok(), want, "{mem_mb} MiB, {threads} threads");
        }
        assert_eq!(run_budget(0, 1), Err(format!("Sort budget too small: 0 bytes per thread, at least {MIN_RUN_BUDGET_BYTES}")));
    }

    #[test]
    fn a_sort_holds_the_longest_key_at_the_least_budget() {
        let dir = TempDir::new("sort-longest");
        let longest = vec![b'z'; MAX_KEY_BYTES];
        let mut sut = ExternalSort::new(MIN_RUN_BUDGET_BYTES, &dir.0, "t".into()).unwrap();
        sut.push(b"a", Rec(1, 0)).unwrap();
        sut.push(&longest, Rec(2, 0)).unwrap();
        sut.push(b"b", Rec(3, 0)).unwrap();

        let got = sorted(sut, &dir.0);

        assert_eq!(got, vec![(b"a".to_vec(), Rec(1, 0)), (b"b".to_vec(), Rec(3, 0)), (longest, Rec(2, 0))]);
    }

    #[test]
    fn run_keys_stay_addressable_at_any_budget() {
        // An entry addresses its key by a u32 offset into the run's arena.
        for budget in [DEFAULT_RUN_BUDGET_MB << 20, MAX_RUN_ARENA_BYTES, MAX_RUN_ARENA_BYTES.saturating_mul(2), usize::MAX] {
            let (arena, items) = run_capacities::<Rec>(budget, MAX_RUN_ARENA_BYTES);
            assert!(arena <= u32::MAX as usize, "{budget} bytes: {arena} key bytes");
            assert_eq!(items, (budget - arena) / std::mem::size_of::<Item<Rec>>());
        }
    }

    #[test]
    fn a_sort_spills_a_run_at_the_arena_limit() {
        let dir = TempDir::new("sort-arena-limit");
        let mut sut = ExternalSort::with_arena_limit(1 << 20, 64, &dir.0, "t".into()).unwrap();
        let keys: Vec<Vec<u8>> = (0..40).rev().map(|k: u32| format!("key{k:03}").into_bytes()).collect();
        for (n, key) in keys.iter().enumerate() {
            sut.push(key, Rec(n as u32, 0)).unwrap();
        }

        let runs = sut.finish().unwrap();

        // 6-byte keys, 10 to a 64-byte arena.
        assert_eq!(runs.len(), 4);
        let mut want = keys.clone();
        want.sort();
        assert_eq!(read_all(runs.merge(&dir.0, "m")).into_iter().map(|(k, _)| k).collect::<Vec<_>>(), want);
    }

    /// Bytes a sort's run buffers take.
    fn run_bytes<R: SortRecord>(sut: &ExternalSort<R>) -> usize {
        sut.arena.capacity() + sut.items.capacity() * std::mem::size_of::<Item<R>>()
    }

    #[test]
    fn a_sort_run_fits_its_budget_whatever_its_record() {
        let dir = TempDir::new("sort-run-bytes");
        for budget in [MIN_RUN_BUDGET_BYTES, 1 << 20] {
            let narrow = ExternalSort::<Rec>::new(budget, &dir.0, "n".into()).unwrap();
            let wide = ExternalSort::<Wide>::new(budget, &dir.0, "w".into()).unwrap();

            assert!(run_bytes(&narrow) <= budget, "{budget} bytes: narrow runs take {}", run_bytes(&narrow));
            assert!(run_bytes(&wide) <= budget, "{budget} bytes: wide runs take {}", run_bytes(&wide));
        }
    }

    #[test]
    fn a_sort_refuses_a_budget_that_holds_no_entry() {
        let dir = TempDir::new("sort-no-entry");
        for budget in [0, std::mem::size_of::<Item<Wide>>() - 1] {
            let err = ExternalSort::<Wide>::new(budget, &dir.0, "t".into()).err().map(|e| (e.kind(), e.to_string()));

            assert_eq!(err, Some((std::io::ErrorKind::InvalidInput, format!("a sort budget of {budget} bytes holds no entry"))));
        }
    }

    #[test]
    fn a_sort_spills_when_the_budget_holds_only_one_entry() {
        for key in [&b""[..], b"x"] {
            let dir = TempDir::new("sort-one-entry");
            let budget = std::mem::size_of::<Item<Wide>>() + key.len();
            let mut sut = ExternalSort::<Wide>::new(budget, &dir.0, "t".into()).unwrap();
            assert!(run_bytes(&sut) <= budget);
            let high = Wide([2; Wide::BYTES]);
            let low = Wide([1; Wide::BYTES]);
            sut.push(key, high).unwrap();
            sut.push(key, low).unwrap();

            let runs = sut.finish().unwrap();

            assert_eq!(runs.len(), 2);
            let mut merged = runs.merge(&dir.0, "m").unwrap();
            let mut got_key = Vec::new();
            assert_eq!(merged.next(&mut got_key).unwrap().unwrap().0, low.0);
            assert_eq!(got_key, key);
            assert_eq!(merged.next(&mut got_key).unwrap().unwrap().0, high.0);
            assert_eq!(got_key, key);
            assert!(merged.next(&mut got_key).unwrap().is_none());
        }
    }

    #[test]
    fn a_sort_refuses_a_key_it_cannot_hold() {
        for (budget, key_len) in [(MIN_RUN_BUDGET_BYTES - 1, MAX_KEY_BYTES), (2 * MIN_RUN_BUDGET_BYTES, MAX_KEY_BYTES + 1)] {
            let dir = TempDir::new("sort-refuse");
            let mut sut = ExternalSort::new(budget, &dir.0, "t".into()).unwrap();

            let err = sut.push(&vec![b'z'; key_len], Rec(1, 0)).unwrap_err();

            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput, "budget {budget}, key {key_len}");
            assert!(err.to_string().contains(&format!("a key of {key_len} bytes")), "{err}");
            assert!(sut.finish().unwrap().is_empty());
        }
    }

    #[test]
    fn a_sort_refuses_keys_over_the_length_limit() {
        let dir = TempDir::new("sort-long-key");
        let mut sut = ExternalSort::new(1 << 20, &dir.0, "t".into()).unwrap();
        let longest = vec![b'a'; u16::MAX as usize];
        assert!(sut.push(&longest, Rec(1, 9)).is_ok());

        // One byte over: ASCII, and a two-byte character across the limit.
        for key in [vec![b'a'; u16::MAX as usize + 1], [&longest[1..], "ж".as_bytes()].concat()] {
            assert_eq!(sut.push(&key, Rec(2, 9)).map_err(|e| e.kind()), Err(std::io::ErrorKind::InvalidInput), "{} bytes", key.len());
        }
        assert_eq!(sorted(sut, &dir.0), [(longest, Rec(1, 9))]);
    }

    #[test]
    fn a_merge_streams_runs_in_key_then_record_order() {
        let dir = TempDir::new("sort-merge");
        let (files, all) = random_runs(&dir.0, 5, &mut Rng(7));

        let got = read_all(runs_of(files).merge(&dir.0, "m"));

        assert_eq!(got, all);
    }

    #[test]
    fn a_merge_rewrites_only_the_runs_over_the_fan_in() {
        let dir = TempDir::new("sort-reduce-excess");
        let (files, all) = random_runs(&dir.0, MERGE_FAN_IN + 1, &mut Rng(11));
        let last_two: u64 = files[MERGE_FAN_IN - 1..].iter().map(|f| std::fs::metadata(f).unwrap().len()).sum();

        let (left, rewritten) = reduce_runs::<Rec>(RunFiles(files.clone()), &dir.0, "t").unwrap();

        assert_eq!(left.0.len(), MERGE_FAN_IN);
        assert_eq!(left.0[..MERGE_FAN_IN - 1], files[..MERGE_FAN_IN - 1]);
        assert_eq!(rewritten, last_two);
        assert!(!files[MERGE_FAN_IN].exists());
        assert_eq!(read_all(Merged::open(left)), all);
    }

    #[test]
    fn a_merge_takes_passes_until_the_fan_in_is_left() {
        for n in [MERGE_FAN_IN, 3 * MERGE_FAN_IN + 5, MERGE_FAN_IN * MERGE_FAN_IN + 3] {
            let dir = TempDir::new("sort-reduce");
            let (files, all) = random_runs(&dir.0, n, &mut Rng(n as u64));

            let (left, rewritten) = reduce_runs::<Rec>(RunFiles(files), &dir.0, "t").unwrap();

            assert!(left.0.len() <= MERGE_FAN_IN);
            assert_eq!(rewritten == 0, n <= MERGE_FAN_IN);
            assert_eq!(read_all(Merged::open(left)), all);
        }
    }

    #[test]
    fn a_sort_gives_what_sorting_in_memory_gives_whatever_its_budget_and_split() {
        // Arena limits that hold every entry in one run, a few runs, and more runs than one
        // reducing pass leaves at the fan-in; the entries cut among 1, 3 and 7 sorts as threads
        // cut them.
        for seed in 1..=4u64 {
            let entries = random_entries(2_500, &mut Rng(seed));
            let want = in_memory(entries.clone());
            for arena_limit in [1 << 20, 1_000, 8] {
                for sorts in [1, 3, 7] {
                    let dir = TempDir::new("sort-in-memory");
                    let mut runs = SortedRuns::default();
                    for t in 0..sorts {
                        let (lo, hi) = (entries.len() * t / sorts, entries.len() * (t + 1) / sorts);
                        let mut sut = ExternalSort::with_arena_limit(1 << 20, arena_limit, &dir.0, format!("t{t}")).unwrap();
                        for (k, r) in &entries[lo..hi] {
                            sut.push(k, *r).unwrap();
                        }
                        runs.append(sut.finish().unwrap());
                    }

                    let got = read_all(runs.merge(&dir.0, "m"));

                    assert_eq!(got, want, "seed {seed}, arena {arena_limit}, {sorts} sorts");
                }
            }
        }
    }

    #[test]
    fn a_sort_of_nothing_has_no_runs_and_merges_to_nothing() {
        let dir = TempDir::new("sort-nothing");
        let sut: ExternalSort<Rec> = ExternalSort::new(1 << 20, &dir.0, "t".into()).unwrap();

        let runs = sut.finish().unwrap();

        assert!(runs.is_empty());
        assert_eq!(runs.spilled_bytes(), 0);
        assert!(read_all(runs.merge(&dir.0, "m")).is_empty());
    }

    #[test]
    fn a_sort_counts_the_bytes_it_spills_and_rewrites() {
        // 20 runs of one 3-byte key each: 2 + 3 + Rec::BYTES bytes a run; the 5 over the fan-in and
        // the one before them are merged into one run.
        let dir = TempDir::new("sort-bytes");
        let mut runs = SortedRuns::default();
        for t in 0..MERGE_FAN_IN + 4 {
            let mut sut = ExternalSort::new(1 << 20, &dir.0, format!("t{t}")).unwrap();
            sut.push(b"abc", Rec(t as u32, 0)).unwrap();
            runs.append(sut.finish().unwrap());
        }
        let entry = (2 + 3 + Rec::BYTES) as u64;
        assert_eq!((runs.len(), runs.spilled_bytes()), (MERGE_FAN_IN + 4, (MERGE_FAN_IN as u64 + 4) * entry));

        let m = runs.merge(&dir.0, "m").unwrap();

        assert_eq!(m.rewritten_bytes(), 5 * entry);
        assert_eq!(read_all(Ok(m)).len(), MERGE_FAN_IN + 4);
    }

    #[test]
    fn a_merge_leaves_no_run_once_dropped_whether_read_or_not() {
        for read in [0, 1, usize::MAX] {
            let dir = TempDir::new("sort-dropped");
            let mut runs = SortedRuns::default();
            for t in 0..MERGE_FAN_IN + 2 {
                let mut sut = ExternalSort::with_arena_limit(1 << 20, 8, &dir.0, format!("t{t}")).unwrap();
                for k in 0..5u32 {
                    sut.push(&k.to_be_bytes(), Rec(t as u32, 0)).unwrap();
                }
                runs.append(sut.finish().unwrap());
            }
            let mut m = runs.merge(&dir.0, "m").unwrap();
            let mut key = Vec::new();
            for _ in 0..read {
                if m.next(&mut key).unwrap().is_none() {
                    break;
                }
            }

            drop(m);

            let left: Vec<_> = std::fs::read_dir(&dir.0).unwrap().map(|e| e.unwrap().file_name()).collect();
            assert!(left.is_empty(), "read {read}: {left:?}");
        }
    }

    #[test]
    fn a_merge_preserves_runs_from_a_sort_with_an_overlapping_tag() {
        let dir = TempDir::new("sort-names");
        let mut sut = ExternalSort::with_arena_limit(1 << 20, 4, &dir.0, "m-m0".into()).unwrap();
        let entries: Vec<Entry> = (0..=MERGE_FAN_IN as u32).rev().map(|k| (k.to_be_bytes().to_vec(), Rec(k, 0))).collect();
        for (k, r) in &entries {
            sut.push(k, *r).unwrap();
        }
        let runs = sut.finish().unwrap();
        assert_eq!(runs.len(), MERGE_FAN_IN + 1);

        let got = read_all(runs.merge(&dir.0, "m"));

        assert_eq!(got, in_memory(entries));
    }

    #[test]
    fn a_merge_leaves_an_existing_output_file_alone() {
        let dir = TempDir::new("sort-existing-output");
        let (files, all) = random_runs(&dir.0, MERGE_FAN_IN + 1, &mut Rng(7));
        let occupied = dir.0.join("m-0.merge");
        std::fs::write(&occupied, b"other merge").unwrap();

        let merged = runs_of(files).merge(&dir.0, "m").unwrap();

        assert_eq!(merged.files.0.last().unwrap(), &dir.0.join("m-1.merge"));
        assert_eq!(read_all(Ok(merged)), all);
        assert_eq!(std::fs::read(&occupied).unwrap(), b"other merge");
        assert_eq!(std::fs::read_dir(&dir.0).unwrap().count(), 1);
    }

    #[test]
    fn a_merge_removes_input_runs_if_its_output_cannot_be_created() {
        let dir = TempDir::new("sort-output-error");
        let (files, _) = random_runs(&dir.0, MERGE_FAN_IN + 1, &mut Rng(7));

        let err = runs_of(files).merge(&dir.0.join("missing"), "m").err().unwrap();

        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
        assert_eq!(std::fs::read_dir(&dir.0).unwrap().count(), 0);
    }

    #[test]
    fn a_merge_fails_every_read_after_a_run_cut_short() {
        // The first run is cut inside its second key, which the first read reaches.
        let dir = TempDir::new("sort-merge-cut");
        let mut bytes = Vec::new();
        for (k, r) in [(&b"a"[..], Rec(1, 0)), (b"ab", Rec(3, 0))] {
            write_entry(&mut bytes, k, &r, &mut Vec::new()).unwrap();
        }
        bytes.truncate(bytes.len() - Rec::BYTES - 1);
        let cut = dir.0.join("cut.run");
        std::fs::write(&cut, &bytes).unwrap();
        let whole = write_run(&dir.0, "whole.run", &[(b"b".to_vec(), Rec(2, 0))]);
        let mut sut = runs_of(vec![cut.clone(), whole]).merge(&dir.0, "m").unwrap();
        let run_name = cut.display().to_string();
        let mut key = Vec::new();

        let got: Vec<_> = (0..3)
            .map(|_| {
                let next = sut.next(&mut key);
                next.map(|rec| rec.map(|rec| (key.clone(), rec))).map_err(|e| (e.kind(), e.to_string()))
            })
            .collect();

        assert!(got.iter().all(|r| matches!(r, Err((std::io::ErrorKind::UnexpectedEof, message)) if message.contains(&run_name))), "{got:?}");
        assert!(got.windows(2).all(|pair| pair[0] == pair[1]), "{got:?}");
    }

    #[test]
    fn a_run_reader_tells_the_end_from_a_cut_record() {
        // u16 key length, key, record.
        let record = [&2u16.to_le_bytes()[..], b"ab", &7u32.to_le_bytes(), &[9]].concat();
        let dir = TempDir::new("sort-run-cut");
        let path = dir.0.join("cut.run");
        let read_all = |bytes: &[u8]| -> std::io::Result<Vec<(Vec<u8>, Rec)>> {
            std::fs::write(&path, bytes).unwrap();
            let mut r = RunReader::open(&path)?;
            let mut key = Vec::new();
            let mut out = Vec::new();
            while let Some(rec) = r.read_into::<Rec>(&mut key)? {
                out.push((key.clone(), rec));
            }
            Ok(out)
        };
        assert!(read_all(&[]).unwrap().is_empty());
        assert_eq!(read_all(&record).unwrap(), vec![(b"ab".to_vec(), Rec(7, 9))]);
        // The next record cut in its length, its key and its record.
        for cut in [1, 3, record.len() - 1] {
            let bytes = [&record[..], &record[..cut]].concat();
            assert_eq!(read_all(&bytes).map_err(|e| e.kind()), Err(std::io::ErrorKind::UnexpectedEof), "cut at {cut}");
        }
    }
}
