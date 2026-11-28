use super::fst_writer::FstWriter;
use super::{
    counts::Coverage,
    sources::{CoreRecords, SourceRow},
    write::{invalid_size, writer},
    BuildOpts,
};
use crate::{
    constants::*,
    index::{areas::AreaRanges, entities::EntityRef, invalid, map, number, terms::TermKind},
    sort::{ExternalSort, SortedRuns},
    text::{
        normalize::{name_normalized, transliterated},
        sanitize::Place,
        variants::{Spelling, Variants},
    },
    varint,
};
use std::{
    cmp::Reverse,
    collections::{BTreeSet, BinaryHeap},
    fs::File,
    hash::{DefaultHasher, Hash, Hasher},
    io::{self, BufReader, BufWriter, Seek, Write},
    path::{Path, PathBuf},
    time::Instant,
};

fn key(kind: TermKind, token: &str, word: &str, lookup: &str, cc: Option<&str>) -> Vec<u8> {
    let mut b = vec![kind as u8];
    b.extend_from_slice(token.as_bytes());
    b.push(0);
    if matches!(kind, TermKind::Word | TermKind::Country) {
        // The middle tag elides the overwhelmingly common word identical to its token,
        // without moving it ahead of alphabetically earlier variant sources.
        b.push(match word.cmp(token) {
            std::cmp::Ordering::Less => WORD_BEFORE,
            std::cmp::Ordering::Equal => WORD_SAME,
            std::cmp::Ordering::Greater => WORD_AFTER,
        });
        if word != token {
            let shared = word.bytes().zip(token.bytes()).take_while(|(a, b)| a == b).count().min(u16::MAX as usize);
            let length = if word < token { shared as u16 } else { u16::MAX - shared as u16 };
            b.extend_from_slice(&length.to_be_bytes());
            b.extend_from_slice(&word.as_bytes()[shared..]);
        }
    }
    let default = word.split(TERM_WORD_LANGUAGE_MARK).next().unwrap();
    if lookup != default || cc.is_some() {
        b.push(0);
        if lookup != default {
            if lookup == token {
                b.push(LOOKUP_IS_TOKEN)
            } else {
                b.extend_from_slice(lookup.as_bytes())
            }
        }
    }
    if let Some(cc) = cc {
        b.push(0);
        b.extend_from_slice(cc.as_bytes())
    }
    b
}

struct CachedName {
    name: String,
    language: Option<&'static str>,
    spellings: Vec<Spelling>,
}

/// Most repeated brands reuse a small set of names and analyses. Each slot has a
/// byte ceiling, so one multilingual name cannot evict the sort's memory budget.
struct SpellingCache {
    slots: Vec<Option<CachedName>>,
}
impl SpellingCache {
    fn new(slots: usize) -> Self {
        Self { slots: (0..slots).map(|_| None).collect() }
    }
    fn with(&mut self, name: &str, language: Option<&'static str>, mut use_spelling: impl FnMut(&Spelling)) {
        let mut h = DefaultHasher::new();
        (name, language).hash(&mut h);
        let Some(i) = (h.finish() as usize).checked_rem(self.slots.len()) else {
            Variants::get().spellings(&name_normalized(name), language).iter().for_each(use_spelling);
            return;
        };
        if let Some(hit) = self.slots[i].as_ref().filter(|c| c.language == language && c.name == name) {
            hit.spellings.iter().for_each(use_spelling);
            return;
        }
        let values = Variants::get().spellings(&name_normalized(name), language);
        values.iter().for_each(&mut use_spelling);
        let bytes = size_of::<CachedName>()
            + name.len()
            + values.capacity() * size_of::<Spelling>()
            + values.iter().map(|s| s.token.capacity() + s.word.capacity() + s.lookup.capacity()).sum::<usize>();
        if bytes <= SPELLING_SLOT_BYTES {
            self.slots[i] = Some(CachedName { name: name.into(), language, spellings: values });
        }
    }
}

fn name_keys(
    records: &CoreRecords,
    e: EntityRef,
    row: SourceRow,
    country: &str,
    cache: &mut SpellingCache,
) -> Vec<Vec<u8>> {
    let bundle = records.tags(e);
    let name = records.geo.get_string(row.name_id());
    let own = if matches!(row, SourceRow::Area(_)) {
        records.geo.admin_own_name(e.record()).map(|id| records.geo.get_string(id))
    } else {
        None
    };
    let mut tags: Vec<_> = bundle.iter().flat_map(|b| b.iter()).collect();
    for s in std::iter::once(&*name).chain(own.as_deref()) {
        if !s.is_empty() && !tags.iter().any(|&(_, v)| v == s) {
            tags.push(("name", s))
        }
    }
    let mut keys = Vec::new();
    for n in Place::from_tags(tags).sanitized(country).names {
        cache.with(&n.name, n.language, |s| {
            keys.push(key(TermKind::Word, &s.token, &s.word, &s.lookup, None));
            for w in s.token.split_whitespace() {
                keys.push(key(TermKind::Partial, w, w, w, None));
            }
        });
        if row.is_country() && !country.is_empty() {
            let word = name_normalized(&n.name);
            let token = transliterated(&word);
            if !word.is_empty() && !token.is_empty() {
                keys.push(key(TermKind::Country, &token, &word, &word, Some(country)));
            }
        }
    }
    if row.is_country() && !country.is_empty() {
        keys.push(key(TermKind::Country, country, country, country, Some(country)))
    }
    keys.sort_unstable();
    keys.dedup();
    keys
}

// A name posting and an address interval are interleaved by start, so both statistics
// are final when the last row of a key leaves the merge.
type Occurrence = [u32; 3];

/// Checking polygon label points can leave the linked-node tables resident. Deduct
/// their actual allocations once across the workers before sizing the sort runs.
fn worker_buffers(per_worker: usize, threads: usize, resident: usize) -> (usize, usize) {
    let budget = per_worker.saturating_sub(resident.div_ceil(threads)).max(MIN_RUN_BUDGET_BYTES);
    let cache_bytes = (budget / SPELLING_BUDGET_DIVISOR).min(budget - MIN_RUN_BUDGET_BYTES);
    let slots = (cache_bytes / SPELLING_SLOT_BYTES).min(MAX_SPELLING_SLOTS);
    (budget - slots * SPELLING_SLOT_BYTES, slots)
}

fn table_bytes<T>(table: &std::sync::OnceLock<Vec<T>>) -> usize {
    table.get().map_or(0, |rows| rows.capacity() * size_of::<T>())
}

fn emit(
    records: &CoreRecords,
    areas: &AreaRanges,
    rows: &[u8],
    buffers: (usize, usize),
    tmp: &Path,
    worker: usize,
) -> io::Result<SortedRuns<Occurrence>> {
    let (budget, slots) = buffers;
    let mut cache = SpellingCache::new(slots);
    let mut sort = ExternalSort::new(budget, tmp, format!("names-{worker}"))?;
    for b in rows.chunks_exact(CORE_RECORD_BYTES) {
        let e = EntityRef::unpack(number(b, 0).unwrap()).unwrap();
        let id = number(b, 4).unwrap();
        let Some(row) = records.row(e) else { continue };
        let cc = row.country().or_else(|| query_server::country_code_bytes(number(b, 8).unwrap() as u16));
        let upper = cc.unwrap_or(*b"  ").map(|v| v.to_ascii_uppercase());
        let country = std::str::from_utf8(&upper).unwrap_or("").trim();
        let imp = records.importance(e, row, country);
        let is_area = matches!(row, SourceRow::Area(_));
        for k in name_keys(records, e, row, &country.to_ascii_lowercase(), &mut cache) {
            sort.push(&k, [id, NAME_OCCURRENCE, imp])?;
            if is_area && k[0] != TermKind::Country as u8 {
                for r in areas.ranges(e.record()) {
                    sort.push(&k, [r.start, AREA_MEMBERS, r.end])?;
                }
            }
        }
    }
    sort.finish()
}

/// Short lists never touch a scratch file. Each long list reuses the same file,
/// with a fixed-size buffer even for a term covering the whole planet.
struct Spool {
    buf: Vec<u8>,
    file: BufWriter<File>,
    path: PathBuf,
    len: u32,
    spilled: bool,
}
impl Spool {
    fn new(path: PathBuf) -> io::Result<Self> {
        Ok(Self { buf: Vec::new(), file: writer(&path)?, path, len: 0, spilled: false })
    }
    fn add(&mut self, b: &[u8]) -> io::Result<()> {
        self.len = self
            .len
            .checked_add(u32::try_from(b.len()).map_err(invalid_size)?)
            .ok_or_else(|| invalid("posting list exceeds 4 GiB"))?;
        if self.buf.len() + b.len() > POSTING_BUFFER_BYTES {
            self.file.write_all(&self.buf)?;
            self.buf.clear();
            self.spilled = true;
        }
        self.buf.extend_from_slice(b);
        Ok(())
    }
    fn drain(&mut self, out: &mut impl Write) -> io::Result<()> {
        if self.spilled {
            self.file.flush()?;
            io::copy(
                &mut std::io::Read::take(File::open(&self.path)?, u64::from(self.len) - self.buf.len() as u64),
                out,
            )?;
            self.file.rewind()?;
            self.spilled = false;
        }
        out.write_all(&self.buf)?;
        self.buf.clear();
        self.len = 0;
        Ok(())
    }
}

type Ranked = (u32, Reverse<u32>);
struct ListWriter {
    deltas: Spool,
    skips: Spool,
    head: BinaryHeap<Reverse<Ranked>>,
    df: u32,
    last: u32,
    coverage: Coverage,
    scratch: Vec<u8>,
}
impl ListWriter {
    fn new(tmp: &Path) -> io::Result<Self> {
        Ok(Self {
            deltas: Spool::new(tmp.join("deltas"))?,
            skips: Spool::new(tmp.join("skips"))?,
            head: BinaryHeap::new(),
            df: 0,
            last: 0,
            coverage: Coverage::default(),
            scratch: Vec::new(),
        })
    }
    fn add(&mut self, [id, kind, v]: Occurrence) -> io::Result<()> {
        if kind == AREA_MEMBERS {
            self.coverage.include(id, v);
            return Ok(());
        }
        if self.df > 0 && self.last == id {
            return Ok(());
        }
        self.scratch.clear();
        varint::put(&mut self.scratch, u64::from(id - self.last));
        self.deltas.add(&self.scratch)?;
        self.df += 1;
        self.last = id;
        if self.df.is_multiple_of(SKIP_EVERY) {
            self.skips.add(&id.to_le_bytes())?;
            self.skips.add(&self.deltas.len.to_le_bytes())?;
        }
        let rank = Reverse((v, Reverse(id)));
        if self.head.len() < TOPK_PER_TERM {
            self.head.push(rank)
        } else if let Some(mut worst) = self.head.peek_mut() {
            if rank < *worst {
                *worst = rank
            }
        }
        Ok(())
    }
    fn finish(&mut self, out: &mut impl Write, pos: &mut u64, top: &mut Vec<Ranked>) -> io::Result<u64> {
        top.clear();
        top.extend(self.head.drain().map(|Reverse(r)| r));
        top.sort_unstable_by(|a, b| b.cmp(a));
        let value = if self.df == 1 && self.coverage.count == 0 {
            self.deltas.drain(&mut io::sink())?;
            (u64::from(self.last) << 1) | INLINE_POSTING
        } else {
            let value = *pos << 1;
            self.scratch.clear();
            for v in [self.df, self.coverage.count, self.df, self.df / SKIP_EVERY, self.deltas.len] {
                varint::put(&mut self.scratch, v.into());
            }
            *pos += self.scratch.len() as u64 + u64::from(self.deltas.len) + u64::from(self.skips.len);
            out.write_all(&self.scratch)?;
            self.skips.drain(out)?;
            self.deltas.drain(out)?;
            if self.df >= TOPK_MIN_DF {
                for &(_, Reverse(id)) in top.iter() {
                    out.write_all(&id.to_le_bytes())?;
                }
                *pos += (4 * top.len()) as u64;
            }
            value
        };
        self.df = 0;
        self.last = 0;
        self.coverage = Coverage::default();
        Ok(value)
    }
}

struct Prefix {
    key: String,
    best: BTreeSet<Ranked>,
    file: BufWriter<File>,
    path: PathBuf,
}
impl Prefix {
    fn new(tmp: &Path, i: usize) -> io::Result<Self> {
        let path = tmp.join(format!("prefix-{i}"));
        Ok(Self { key: String::new(), best: BTreeSet::new(), file: writer(&path)?, path })
    }
    fn add(&mut self, rank: Ranked) {
        if self.best.len() == PREFIX_TABLE_TOP && self.best.first().is_some_and(|r| *r >= rank) {
            return;
        }
        self.best.insert(rank);
        if self.best.len() > PREFIX_TABLE_TOP {
            self.best.pop_first();
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        if self.key.is_empty() {
            return Ok(());
        }
        self.file.write_all(&(self.key.len() as u16).to_le_bytes())?;
        self.file.write_all(self.key.as_bytes())?;
        self.file.write_all(&((self.best.len() * 8) as u32).to_le_bytes())?;
        for (imp, Reverse(id)) in std::mem::take(&mut self.best) {
            self.file.write_all(&imp.to_le_bytes())?;
            self.file.write_all(&id.to_le_bytes())?;
        }
        self.key.clear();
        Ok(())
    }
    fn reader(mut self) -> io::Result<BufReader<File>> {
        self.flush()?;
        self.file.flush()?;
        Ok(BufReader::new(File::open(self.path)?))
    }
}

/// For each name kind and character length, prefixes left the term merge in byte order.
/// At most one row of each of these streams is needed beside the prefix FST builder.
fn prefix_files(prefixes: Vec<Prefix>, out: &Path) -> io::Result<()> {
    let mut readers = prefixes.into_iter().map(Prefix::reader).collect::<io::Result<Vec<_>>>()?;
    let mut heads = readers.iter_mut().map(super::read_prefix).collect::<io::Result<Vec<_>>>()?;
    let mut fst = FstWriter::new(writer(&out.join("core.pfx.fst"))?)?;
    let mut data = writer(&out.join("core.pfx"))?;
    let (mut best, mut buf, mut pos) = (Vec::new(), Vec::new(), 0u64);
    while let Some(first) = heads.iter().flatten().map(|(k, _)| k).min() {
        let key = first.clone();
        best.clear();
        for (head, reader) in heads.iter_mut().zip(&mut readers) {
            if let Some((_, bytes)) = head.as_ref().filter(|(k, _)| k == &key) {
                best.extend(bytes.chunks_exact(8).map(|b| (Reverse(number(b, 0).unwrap()), number(b, 4).unwrap())));
                *head = super::read_prefix(reader)?;
            }
        }
        best.sort_unstable();
        best.dedup();
        best.truncate(PREFIX_TABLE_TOP);
        buf.clear();
        varint::put(&mut buf, best.len() as u64);
        for &(_, id) in &best {
            varint::put(&mut buf, id.into());
        }
        fst.insert(key, pos)?;
        data.write_all(&buf)?;
        pos += buf.len() as u64;
    }
    fst.finish()?;
    data.flush()
}
#[derive(Default)]
pub(super) struct TermStats {
    pub terms: u64,
    pub postings: u64,
    pub emit_s: f64,
    pub sort_s: f64,
    pub merge_s: f64,
    pub fst_s: f64,
    pub prefix_s: f64,
    pub sort_runs: usize,
    pub merge_passes: usize,
}

pub(super) fn build(
    records: CoreRecords,
    out: &Path,
    tmp: &Path,
    opts: &BuildOpts,
    n: u32,
    profile: crate::profile::Profile,
) -> io::Result<TermStats> {
    let start = Instant::now();
    let rows = map(&tmp.join("records"))?;
    let areas = AreaRanges::open(out, n, profile)?;
    let budget = crate::sort::run_budget(opts.mem_mb, opts.threads).map_err(invalid)?;
    let resident = table_bytes(&records.geo.linked_centres) + table_bytes(&records.geo.linked_names);
    let buffers = worker_buffers(budget, opts.threads, resident);
    let mut names = SortedRuns::default();
    std::thread::scope(|scope| -> io::Result<()> {
        let areas = &areas;
        let records = &records;
        let mut workers = Vec::new();
        let chunk = (n as usize).div_ceil(opts.threads).max(1) * CORE_RECORD_BYTES;
        for (worker, rows) in rows.chunks(chunk).enumerate() {
            workers.push(scope.spawn(move || emit(records, areas, rows, buffers, tmp, worker)));
        }
        for w in workers {
            names.append(w.join().map_err(|_| invalid("name worker failed"))??);
        }
        Ok(())
    })?;
    drop(rows);
    drop(areas);
    drop(records);
    let emit_s = start.elapsed().as_secs_f64();
    let mut stats = merge(names, out, tmp)?;
    stats.emit_s = emit_s;
    Ok(stats)
}

fn merge(names: SortedRuns<Occurrence>, out: &Path, tmp: &Path) -> io::Result<TermStats> {
    let start = Instant::now();
    let mut stats =
        TermStats { sort_runs: names.len(), sort_s: names.spill_time().as_secs_f64(), ..TermStats::default() };
    let mut rows = names.merge(tmp, "name-merge")?;
    stats.merge_s = start.elapsed().as_secs_f64();
    stats.merge_passes = rows.passes();
    // The final merge streams directly into the posting and FST writers.
    let start = Instant::now();
    let mut list = ListWriter::new(tmp)?;
    let mut prefixes = (0..2 * PREFIX_TABLE_MAX_CHARS).map(|i| Prefix::new(tmp, i)).collect::<io::Result<Vec<_>>>()?;
    let mut fst = FstWriter::new(writer(&out.join("core.fst"))?)?;
    let mut post = writer(&out.join("core.post"))?;
    let (mut prev, mut top, mut pos, mut terms, mut postings) = (Vec::new(), Vec::new(), 0u64, 0u64, 0u64);
    let mut flush = |k: &[u8], list: &mut ListWriter| -> io::Result<()> {
        if k.is_empty() {
            return Ok(());
        }
        postings += u64::from(list.df);
        terms += 1;
        let value = list.finish(&mut post, &mut pos, &mut top)?;
        fst.insert(k, value)?;
        if k[0] == TermKind::Word as u8 || k[0] == TermKind::Partial as u8 {
            let token = std::str::from_utf8(k[1..].split(|&b| b == 0).next().unwrap()).map_err(io::Error::other)?;
            let first = if k[0] == TermKind::Word as u8 { 0 } else { PREFIX_TABLE_MAX_CHARS };
            for ((i, c), slot) in token.char_indices().zip(&mut prefixes[first..first + PREFIX_TABLE_MAX_CHARS]) {
                let prefix = &token[..i + c.len_utf8()];
                if slot.key != prefix {
                    slot.flush()?;
                    slot.key.push_str(prefix);
                }
                for &r in top.iter().take(PREFIX_TABLE_TOP) {
                    slot.add(r);
                }
            }
        }
        Ok(())
    };
    // A bounded batch separates the final merge time from the writer without reading
    // the clock for every occurrence or materializing another sorted run.
    let mut batch: [(Vec<u8>, Occurrence); TERM_MERGE_BATCH] = Default::default();
    let mut final_read = std::time::Duration::ZERO;
    loop {
        let read_start = Instant::now();
        let mut count = 0;
        for (key, occurrence) in &mut batch {
            let Some(row) = rows.next(key)? else { break };
            *occurrence = row;
            count += 1;
        }
        final_read += read_start.elapsed();
        for (key, row) in &batch[..count] {
            if *key != prev {
                flush(&prev, &mut list)?;
                prev.clone_from(key);
            }
            list.add(*row)?;
        }
        if count < batch.len() {
            break;
        }
    }
    flush(&prev, &mut list)?;
    fst.finish()?;
    post.flush()?;
    drop(rows);
    stats.fst_s = (start.elapsed() - final_read).as_secs_f64();
    stats.merge_s += final_read.as_secs_f64();
    let start = Instant::now();
    prefix_files(prefixes, out)?;
    stats.prefix_s = start.elapsed().as_secs_f64();
    stats.terms = terms;
    stats.postings = postings;
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        fixture::TempDir,
        index::{terms::TermSource, Dictionary},
    };
    #[test]
    fn term_workers_share_the_budget_with_resident_label_tables() {
        let mib = 1 << 20;
        let (run, slots) = worker_buffers(16 * mib, 1, 7 * mib);
        assert_eq!(run + slots * SPELLING_SLOT_BYTES, 9 * mib);
        let (run, slots) = worker_buffers(4 * mib, 4, 7 * mib);
        assert_eq!(4 * (run + slots * SPELLING_SLOT_BYTES), 9 * mib);
        assert_eq!(worker_buffers(mib, 1, 2 * mib), (MIN_RUN_BUDGET_BYTES, 0));
        let mut uncached = SpellingCache::new(0);
        let mut spellings = Vec::new();
        uncached.with("Airport", None, |s| spellings.push(s.token.clone()));
        assert_eq!(spellings, ["airport"]);
    }

    #[test]
    fn three_million_postings_share_one_term_in_the_device_budget() {
        const N: u32 = 3_000_000;
        let dir = TempDir::new("long-posting");
        let budget = DEFAULT_RUN_BUDGET_MB << 20;
        let mut sort = ExternalSort::new(budget, &dir.0, "input".into()).unwrap();
        let k = key(TermKind::Partial, "de", "de", "de", None);
        for i in (0..N).rev() {
            sort.push(&k, [i, NAME_OCCURRENCE, i]).unwrap()
        }
        let empty = key(TermKind::Partial, "z", "z", "z", None);
        sort.push(&empty, [2, AREA_MEMBERS, 19]).unwrap();
        let stats = merge(sort.finish().unwrap(), &dir.0, &dir.0).unwrap();
        assert_eq!((stats.terms, stats.postings), (2, N.into()));
        let terms = Dictionary::open(&dir.0, N).unwrap();
        let term = &terms.exact(TermKind::Partial, "de")[0];
        let list = terms.posting(term.postings[0]).unwrap();
        assert_eq!(list.df, N);
        assert_eq!(list.seek(N - 2).collect::<Vec<_>>(), [N - 2, N - 1]);
        assert_eq!(list.head().next(), Some(N - 1));
        let zero = &terms.exact(TermKind::Partial, "z")[0];
        assert_eq!((zero.count, zero.addr_count), (0, 17));
        assert_eq!(terms.posting(zero.postings[0]).unwrap().ids().next(), None);
    }
}
