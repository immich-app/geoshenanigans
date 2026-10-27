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

pub struct ExtraName<'a> {
    pub key: &'a str,
    pub class: u8,
    pub name: &'a str,
}

impl Extras {
    pub fn open(path: &str) -> Result<Self, String> {
        let f = std::fs::File::open(path).map_err(|e| format!("{path}: {e}"))?;
        let map = unsafe { Mmap::map(&f) }.map_err(|e| format!("{path}: {e}"))?;
        let _ = map.advise(memmap2::Advice::Random);
        if map.len() < 16 || &map[..8] != MAGIC {
            return Err(format!("{path}: not an extra-names file"));
        }
        let mut at = 8;
        let nk = u32_at(&map, at) as usize;
        at += 4;
        let mut keys = Vec::with_capacity(nk);
        for _ in 0..nk {
            let l = map[at] as usize;
            let k = std::str::from_utf8(&map[at + 1..at + 1 + l]).unwrap_or("").to_string();
            at += 1 + l;
            let c = class_of(&k);
            keys.push((k, c));
        }
        let n = u32_at(&map, at) as usize;
        at += 4;
        let refs_at = at;
        let offs_at = refs_at + 4 * n;
        let blob_at = offs_at + 4 * (n + 1);
        Ok(Extras { map, keys, n, refs_at, offs_at, blob_at })
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

    pub fn names(&self, row: usize) -> Vec<ExtraName<'_>> {
        let start = self.blob_at + u32_at(&self.map, self.offs_at + 4 * row) as usize;
        let b = &self.map[..];
        let mut pos = start;
        let count = varint::get(b, &mut pos) as usize;
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            let k = varint::get(b, &mut pos) as usize;
            let l = varint::get(b, &mut pos) as usize;
            let name = std::str::from_utf8(&b[pos..pos + l]).unwrap_or("");
            pos += l;
            let (key, class) = self.keys.get(k).map(|(s, c)| (s.as_str(), *c)).unwrap_or(("", 5));
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

#[cfg(all(test, target_os = "linux"))]
mod tests {
    #[test]
    fn pack_reports_final_flush_failure() {
        let tsv = std::env::temp_dir().join(format!("ps-extra-flush-{}.tsv", std::process::id()));
        std::fs::write(&tsv, "p\t1\t0\tname:de\tBerlin\no\t7\t1\talt_name\tTor\n").unwrap();
        let result = super::pack(tsv.to_str().unwrap(), "/dev/full");
        std::fs::remove_file(tsv).unwrap();
        assert!(result.is_err());
    }
}
