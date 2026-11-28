use super::{chains, sources::CoreRecords, terms, BuildOpts, Contended, HeldIndexDir};
use crate::{
    constants::*,
    dataset::{self, HashCache, Stamps},
    index::invalid,
    profile::Profile,
    sort::SortRecord,
};
use query_server::Index;
use std::{
    cmp::Ordering,
    fs::{self, File},
    io::{self, BufWriter, Seek, Write},
    path::Path,
    time::Instant,
};

impl<const N: usize> SortRecord for [u32; N] {
    const BYTES: usize = N * 4;
    fn put(&self, buf: &mut [u8]) {
        for (b, n) in buf.chunks_exact_mut(4).zip(self) {
            b.copy_from_slice(&n.to_le_bytes())
        }
    }
    fn get(buf: &[u8]) -> Self {
        std::array::from_fn(|i| crate::index::number(buf, i * 4).unwrap())
    }
    fn order(&self, other: &Self) -> Ordering {
        self.cmp(other)
    }
}
pub(super) fn writer(path: &Path) -> io::Result<BufWriter<File>> {
    File::create(path).map(|f| BufWriter::with_capacity(WRITE_BUFFER, f))
}

/// The row directory precedes its variable-size slices, so the blob is spooled separately.
pub(super) struct TableWriter {
    rows: BufWriter<File>,
    data: BufWriter<File>,
    count: u32,
    size: u32,
}
impl TableWriter {
    pub fn new(dir: &Path, name: &str, tmp: &Path) -> io::Result<Self> {
        let mut rows = writer(&dir.join(name))?;
        rows.write_all(&0u32.to_le_bytes())?;
        let data = writer(&tmp.join(format!("{name}.blob")))?;
        Ok(Self { rows, data, count: 0, size: 0 })
    }
    pub fn begin(&mut self, fields: &[u32]) -> io::Result<()> {
        for field in fields {
            self.rows.write_all(&field.to_le_bytes())?
        }
        self.rows.write_all(&self.size.to_le_bytes())?;
        self.count = self.count.checked_add(1).ok_or_else(|| invalid("too many table rows"))?;
        Ok(())
    }
    pub fn append(&mut self, b: &[u8]) -> io::Result<()> {
        self.size = self
            .size
            .checked_add(u32::try_from(b.len()).map_err(invalid_size)?)
            .ok_or_else(|| invalid("table blob exceeds 4 GiB"))?;
        self.data.write_all(b)
    }
    pub fn finish(mut self, last: &[u32], blob: &Path) -> io::Result<()> {
        let count = self.count;
        self.begin(last)?;
        self.data.flush()?;
        io::copy(&mut File::open(blob)?, &mut self.rows)?;
        self.rows.rewind()?;
        self.rows.write_all(&count.to_le_bytes())?;
        self.rows.flush()
    }
}
pub(super) fn invalid_size(e: std::num::TryFromIntError) -> io::Error {
    invalid(e.to_string())
}

/// Builds the core under the same directory lock used by readers and the shipped place search.
pub fn build_core(
    geo: &Index,
    loaded: &Stamps,
    set: &Path,
    out: &Path,
    profile: Profile,
    opts: &BuildOpts,
) -> Result<(), String> {
    let lock = HeldIndexDir::take(out, Contended::Fail)?;
    build_core_held(geo, loaded, set, &lock, profile, opts, &mut HashCache::default())
}

pub(crate) fn build_core_held(
    geo: &Index,
    loaded: &Stamps,
    set: &Path,
    lock: &HeldIndexDir,
    profile: Profile,
    opts: &BuildOpts,
    hashes: &mut HashCache,
) -> Result<(), String> {
    let out = lock.path.as_path();
    if opts.threads == 0 || opts.mem_mb.checked_mul(1 << 20).is_none() {
        return Err("invalid core sort budget or thread count".into());
    }
    crate::sort::run_budget(opts.mem_mb, opts.threads)?;
    let interrupted = super::has_unfinished_publish(out);
    let stage = out.join(if interrupted { CORE_RECOVERY_DIR } else { BUILD_STAGE_DIR });
    // A core build replaces its whole file set. An interrupted publish can be completed
    // by rebuilding, even when it left a mixture of generations.
    let result = (|| -> io::Result<()> {
        if stage.try_exists()? {
            fs::remove_dir_all(&stage)?
        }
        fs::create_dir_all(&stage)?;
        let tmp = (0u64..)
            .find_map(|attempt| {
                if let Err(e) = fs::create_dir_all(&opts.tmp) {
                    return Some(Err(e));
                }
                let path = opts.tmp.join(format!("core-{}-{attempt}", std::process::id()));
                match fs::create_dir(&path) {
                    Ok(()) => Some(Ok(path)),
                    Err(e) if matches!(e.kind(), io::ErrorKind::AlreadyExists | io::ErrorKind::NotFound) => None,
                    Err(e) => Some(Err(e)),
                }
            })
            .expect("scratch directory number overflow")?;
        let built = (|| -> io::Result<()> {
            let start = Instant::now();
            let records = CoreRecords::open(geo, set)?;
            let n = chains::build(&records, set, &stage, &tmp, opts, profile)?;
            let chain_s = start.elapsed().as_secs_f64();
            let (terms, postings) = terms::build(&records, &stage, &tmp, opts, n)?;
            let dataset = dataset::core_fingerprint(set, loaded, hashes).map_err(invalid)?;
            let mut bytes = std::collections::BTreeMap::new();
            for &name in CORE_FILES {
                bytes.insert(name, fs::metadata(stage.join(name))?.len());
            }
            let meta = serde_json::json!({
                "format": INDEX_FORMAT,
                "profile": profile.as_str(),
                "poi_max_tier": opts.poi_max_tier,
                "core": {
                    "entities": n,
                    "terms": terms,
                    "postings": postings,
                    "bytes": bytes,
                    "chain_s": chain_s,
                    "wall_s": start.elapsed().as_secs_f64(),
                    "vm_hwm_kb": super::vm_hwm_kb(),
                    "dataset": dataset
                }
            });
            fs::write(stage.join(META_FILE), serde_json::to_vec_pretty(&meta)?)?;
            super::publish(&stage, out, |a, b| fs::rename(a, b))
        })();
        let _ = fs::remove_dir_all(tmp);
        if opts.tmp == out.join("tmp") {
            let _ = fs::remove_dir(&opts.tmp);
        }
        built
    })();
    if result.is_ok() && interrupted {
        fs::remove_dir_all(out.join(BUILD_STAGE_DIR)).map_err(|e| e.to_string())?;
    }
    super::clear_stage(&stage);
    result.map_err(|e| format!("build core in {}: {e}", out.display()))
}
