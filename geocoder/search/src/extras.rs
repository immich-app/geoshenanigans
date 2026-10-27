//! The optional extra-names file: names the shipped records do not hold (other languages,
//! alternate / old / official names), keyed by the existing record ids. Shipped zstd-compressed,
//! kept decompressed on disk and memory-mapped (the index build streams it; queries read the
//! matched name for display).
//!
//! Layout (little-endian):
//!   b"PSXN0001"
//!   u32 tag-key count, then per key: u8 length + UTF-8 bytes ("name:de", "alt_name", ...)
//!   u32 n, then n x u32 record refs (kind << 30 | record index), ascending
//!   (n + 1) x u32 offsets into the blob
//!   blob: per record: varint count, then per name: varint tag-key index, varint length, bytes
use crate::model::class_of;
use crate::varint;
use memmap2::Mmap;
use std::collections::HashMap;
use std::io::{BufRead, Write};

pub const MAGIC: &[u8; 8] = b"PSXN0001";
pub const KIND_PLACE_NODE: u32 = 0;
pub const KIND_POLYGON: u32 = 1;
pub const KIND_POI: u32 = 2;

pub fn record_ref(kind: u32, rec: u32) -> u32 {
    (kind << 30) | rec
}

pub struct Extras {
    map: Mmap,
    /// (tag key, class)
    pub keys: Vec<(String, u8)>,
    n: usize,
    refs_at: usize,
    offs_at: usize,
    blob_at: usize,
}

fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().unwrap())
}

/// The u32 at `*at`, moving past it; None past the end of `b`.
fn next_u32(b: &[u8], at: &mut usize) -> Option<u32> {
    let v = u32::from_le_bytes(b.get(*at..*at + 4)?.try_into().unwrap());
    *at += 4;
    Some(v)
}

pub struct ExtraName<'a> {
    pub key: &'a str,
    pub class: u8,
    pub name: &'a str,
}

impl Extras {
    /// Opens and checks the file: its header, key table and record tables must fit it, and its
    /// offsets must end at its end (a record's own offsets are checked when it is read).
    pub fn open(path: &str) -> Result<Self, String> {
        let f = std::fs::File::open(path).map_err(|e| format!("{path}: {e}"))?;
        let map = unsafe { Mmap::map(&f) }.map_err(|e| format!("{path}: {e}"))?;
        let _ = map.advise(memmap2::Advice::Random);
        if map.get(..MAGIC.len()) != Some(&MAGIC[..]) {
            return Err(format!("{path}: not an extra-names file"));
        }
        let cut = |part: &str| format!("{path}: extra-names file cut short in its {part}");
        let mut at = MAGIC.len();
        let nk = next_u32(&map, &mut at).ok_or_else(|| cut("key count"))?;
        let mut keys = Vec::new();
        for _ in 0..nk {
            let l = *map.get(at).ok_or_else(|| cut("key table"))? as usize;
            let k = map.get(at + 1..at + 1 + l).ok_or_else(|| cut("key table"))?;
            let k = std::str::from_utf8(k).unwrap_or("").to_string();
            at += 1 + l;
            let c = class_of(&k);
            keys.push((k, c));
        }
        let n = next_u32(&map, &mut at).ok_or_else(|| cut("record count"))? as usize;
        let refs_at = at;
        // n refs, then n + 1 offsets.
        let blob_at = n.checked_mul(8).and_then(|t| t.checked_add(refs_at + 4)).filter(|&b| b <= map.len());
        let blob_at = blob_at.ok_or_else(|| cut("record tables"))?;
        let offs_at = refs_at + 4 * n;
        let x = Extras { map, keys, n, refs_at, offs_at, blob_at };
        let blob_len = x.map.len() - blob_at;
        if x.offset(n) != blob_len {
            return Err(format!("{path}: extra-names offsets end at {} of a {blob_len}-byte blob", x.offset(n)));
        }
        Ok(x)
    }

    /// Blob offset `i` (record `i`'s start, record `i - 1`'s end).
    fn offset(&self, i: usize) -> usize {
        u32_at(&self.map, self.offs_at + 4 * i) as usize
    }

    pub fn len(&self) -> usize {
        self.n
    }

    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    pub fn ref_at(&self, row: usize) -> u32 {
        u32_at(&self.map, self.refs_at + 4 * row)
    }

    /// Row of a record ref (binary search).
    pub fn find(&self, rref: u32) -> Option<usize> {
        let (mut lo, mut hi) = (0usize, self.n);
        while lo < hi {
            let mid = (lo + hi) / 2;
            let v = self.ref_at(mid);
            if v == rref {
                return Some(mid);
            }
            if v < rref {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        None
    }

    /// First row whose ref is >= rref.
    pub fn lower_bound(&self, rref: u32) -> usize {
        let (mut lo, mut hi) = (0usize, self.n);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.ref_at(mid) < rref {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// The names of row `row`, read within its record: none for a record that starts past its end
    /// or the blob's, and only those before a name cut short by the record's end.
    pub fn names(&self, row: usize) -> Vec<ExtraName<'_>> {
        let Some(rec) = self.map[self.blob_at..].get(self.offset(row)..self.offset(row + 1)) else { return Vec::new() };
        let mut pos = 0;
        let mut out = Vec::new();
        let Some(count) = varint::try_get(rec, &mut pos) else { return out };
        for _ in 0..count {
            let (Some(k), Some(l)) = (varint::try_get(rec, &mut pos), varint::try_get(rec, &mut pos)) else { break };
            let Some(bytes) = rec.get(pos..).and_then(|r| r.get(..usize::try_from(l).ok()?)) else { break };
            pos += bytes.len();
            let name = std::str::from_utf8(bytes).unwrap_or("");
            let (key, class) = self.keys.get(k as usize).map(|(s, c)| (s.as_str(), *c)).unwrap_or(("", 5));
            out.push(ExtraName { key, class, name });
        }
        out
    }

    /// Extra names of an entity's records in order (a labelled boundary's polygon, then its label
    /// node): a posting's ordinal k is the k-th of them.
    pub fn names_of(&self, rrefs: &[u32]) -> Vec<ExtraName<'_>> {
        rrefs.iter().filter_map(|&r| self.find(r)).flat_map(|row| self.names(row)).collect()
    }

    /// The `ord`-th (1-based) extra name of an entity's records.
    pub fn name_at(&self, rrefs: &[u32], ord: u8) -> Option<(&str, u8)> {
        self.names_of(rrefs).into_iter().nth(ord as usize - 1).map(|x| (x.name, x.class))
    }
}

/// extra_names.tsv (kind letter p/b/o, record index, class, tag key, name; any order) -> the
/// binary file. Server-side tool: holds the rows in memory.
pub fn pack(tsv: &str, out: &str) -> Result<(usize, usize), String> {
    let f = std::fs::File::open(tsv).map_err(|e| format!("{tsv}: {e}"))?;
    let mut rows: Vec<(u32, u32, String)> = Vec::new();
    let mut key_ids: HashMap<String, u32> = HashMap::new();
    let mut keys: Vec<String> = Vec::new();
    for line in std::io::BufReader::new(f).lines() {
        let line = line.map_err(|e| e.to_string())?;
        let c: Vec<&str> = line.splitn(5, '\t').collect();
        if c.len() < 5 || c[4].is_empty() {
            continue;
        }
        let kind = match c[0] {
            "p" => KIND_PLACE_NODE,
            "b" => KIND_POLYGON,
            "o" => KIND_POI,
            _ => continue,
        };
        let rec: u32 = c[1].parse().map_err(|_| format!("bad record {}", c[1]))?;
        let k = *key_ids.entry(c[3].to_string()).or_insert_with(|| {
            keys.push(c[3].to_string());
            keys.len() as u32 - 1
        });
        rows.push((record_ref(kind, rec), k, c[4].to_string()));
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    let mut refs: Vec<u32> = Vec::new();
    let mut offs: Vec<u32> = vec![];
    let mut blob: Vec<u8> = Vec::new();
    let mut i = 0;
    while i < rows.len() {
        let r = rows[i].0;
        let mut j = i;
        while j < rows.len() && rows[j].0 == r {
            j += 1;
        }
        refs.push(r);
        offs.push(blob.len() as u32);
        varint::put(&mut blob, (j - i) as u64);
        for (_, k, name) in &rows[i..j] {
            varint::put(&mut blob, *k as u64);
            varint::put(&mut blob, name.len() as u64);
            blob.extend_from_slice(name.as_bytes());
        }
        i = j;
    }
    offs.push(blob.len() as u32);
    let mut w = std::io::BufWriter::new(std::fs::File::create(out).map_err(|e| format!("{out}: {e}"))?);
    let mut put = |b: &[u8]| w.write_all(b).map_err(|e| e.to_string());
    put(MAGIC)?;
    put(&(keys.len() as u32).to_le_bytes())?;
    for k in &keys {
        let b = &k.as_bytes()[..k.len().min(255)];
        put(&[b.len() as u8])?;
        put(b)?;
    }
    put(&(refs.len() as u32).to_le_bytes())?;
    for r in &refs {
        put(&r.to_le_bytes())?;
    }
    for o in &offs {
        put(&o.to_le_bytes())?;
    }
    put(&blob)?;
    w.flush().map_err(|e| e.to_string())?;
    Ok((refs.len(), rows.len()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The packed extra-names file of two records, and a path to write variants of it to.
    fn packed(tag: &str) -> (Vec<u8>, std::path::PathBuf) {
        let base = std::env::temp_dir().join(format!("ps-extra-{tag}-{}", std::process::id()));
        let (tsv, bin) = (base.with_extension("tsv"), base.with_extension("bin"));
        std::fs::write(&tsv, "p\t1\t0\tname:de\tBerlin\np\t1\t0\tname:fr\tBerlin\no\t7\t1\talt_name\tTor\n").unwrap();
        pack(tsv.to_str().unwrap(), bin.to_str().unwrap()).unwrap();
        let bytes = std::fs::read(&bin).unwrap();
        std::fs::remove_file(tsv).unwrap();
        (bytes, bin)
    }

    fn open_bytes(path: &std::path::Path, bytes: &[u8]) -> Result<Extras, String> {
        std::fs::write(path, bytes).unwrap();
        Extras::open(path.to_str().unwrap())
    }

    fn all_names(x: &Extras) -> Vec<Vec<(String, String)>> {
        (0..x.len()).map(|row| x.names(row).iter().map(|n| (n.key.to_string(), n.name.to_string())).collect()).collect()
    }

    #[test]
    fn a_cut_extra_names_file_fails_the_open() {
        let (bytes, path) = packed("cut");
        assert_eq!(all_names(&open_bytes(&path, &bytes).unwrap()).len(), 2);

        // Cut in its magic, key count, key table, record count, ref and offset tables, and blob.
        for cut in 0..bytes.len() {
            let err = open_bytes(&path, &bytes[..cut]).err();
            assert!(err.as_deref().is_some_and(|e| e.contains(path.to_str().unwrap())), "cut at {cut}: {err:?}");
        }
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn a_record_is_read_within_its_offsets() {
        let (bytes, path) = packed("offsets");
        let x = open_bytes(&path, &bytes).unwrap();
        let (offs_at, blob_at, names) = (x.offs_at, x.blob_at, all_names(&x));
        let with = |at: usize, v: u32| [&bytes[..at], &v.to_le_bytes(), &bytes[at + 4..]].concat();
        let blob_len = (bytes.len() - blob_at) as u32;

        // The offsets end past the blob: the file is cut short.
        assert!(open_bytes(&path, &with(offs_at + 8, blob_len + 1)).is_err());
        // Record 0 (17 bytes) starts past its end, or past the blob: no names, record 1 still reads.
        for start in [18, blob_len + 1] {
            let x = open_bytes(&path, &with(offs_at, start)).unwrap();
            assert_eq!(all_names(&x), [vec![], names[1].clone()], "record 0 from {start}");
        }
        // Record 0's last name is longer than the record: its names before it, record 1 as it was.
        let long = [&bytes[..blob_at], &[2, 0, 6], b"Berlin", &[1, 9], b"Berlin", &bytes[blob_at + 17..]].concat();
        let x = open_bytes(&path, &long).unwrap();
        assert_eq!(all_names(&x), [names[0][..1].to_vec(), names[1].clone()]);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn a_corrupt_extra_names_file_opens_or_fails_without_a_panic() {
        let (bytes, path) = packed("corrupt");
        for at in 0..bytes.len() {
            for v in [0x00, 0x01, 0x7F, 0x80, 0xFF] {
                let b = [&bytes[..at], &[v], &bytes[at + 1..]].concat();
                if let Ok(x) = open_bytes(&path, &b) {
                    all_names(&x);
                }
            }
        }
        std::fs::remove_file(path).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn pack_reports_final_flush_failure() {
        let tsv = std::env::temp_dir().join(format!("ps-extra-flush-{}.tsv", std::process::id()));
        std::fs::write(&tsv, "p\t1\t0\tname:de\tBerlin\no\t7\t1\talt_name\tTor\n").unwrap();
        let result = pack(tsv.to_str().unwrap(), "/dev/full");
        std::fs::remove_file(tsv).unwrap();
        assert!(result.is_err());
    }
}
