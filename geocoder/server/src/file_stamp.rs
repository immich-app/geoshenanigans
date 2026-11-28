//! A file's stat: its size, modification time and inode. Writing or replacing a file changes it
//! (a patch or a download renames a new file over the old one, which gives it a new inode), so
//! whatever was computed from a file is reused while the file keeps its stamp.

use std::collections::BTreeMap;
use std::io::ErrorKind;
use std::path::Path;

use serde_json::{json, Value};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileStamp {
    pub size: u64,
    pub mtime_ns: u128,
    pub ino: u64,
}

impl FileStamp {
    pub fn of(m: &std::fs::Metadata) -> FileStamp {
        let mtime_ns = m.modified().ok().and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok()).map_or(0, |d| d.as_nanos());
        #[cfg(unix)]
        let ino = std::os::unix::fs::MetadataExt::ino(m);
        #[cfg(not(unix))]
        let ino = 0;
        FileStamp { size: m.len(), mtime_ns, ino }
    }

    /// {"size", "mtime_ns", "ino"}, the time as a string: a JSON number can't hold every u128.
    pub fn to_json(&self) -> Value {
        json!({"size": self.size, "mtime_ns": self.mtime_ns.to_string(), "ino": self.ino})
    }

    /// The stamp `to_json` wrote into `v`; None when `v` holds none.
    pub fn from_json(v: &Value) -> Option<FileStamp> {
        Some(FileStamp { size: v["size"].as_u64()?, mtime_ns: v["mtime_ns"].as_str()?.parse().ok()?, ino: v["ino"].as_u64()? })
    }
}

/// The geocoder files an index is derived from: the records its postings point at, the string
/// tiers of their names (the polygons' own names beside them among them) and the boundary
/// geometry its label points come from, the boundary tier's own arcs among it (the records and
/// rings are the same in every tier).
pub const DATASET_FILES: [&str; 20] = [
    "admin_own_names.bin",
    "admin_polygons.bin",
    "admin_rings.bin",
    "arc_index.bin",
    "arc_vertices.bin",
    "place_cells.bin",
    "place_hidden.bin",
    "place_nodes.bin",
    "poi_records.bin",
    "strings_core.bin",
    "strings_layout.json",
    "strings_poi.bin",
    "strings_tags_core.bin",
    "strings_tags_poi.bin",
    "place_tags.bin",
    "admin_tags.bin",
    "poi_tags.bin",
    "place_imp.bin",
    "admin_imp.bin",
    "poi_imp.bin",
];

/// The membership inputs used by the chain-sorted core.
const CHAIN_INPUTS: [&str; 6] = [
    "admin_cells.bin",
    "admin_entries.bin",
    "search_chains.bin",
    "place_chains.bin",
    "admin_chains.bin",
    "poi_chains.bin",
];

/// The stat of each dataset file present, by name. A file's hash is reused while it keeps its
/// stamp: a patched file is always hashed again.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Stamps(pub BTreeMap<String, FileStamp>);

impl Stamps {
    /// The stamps of the dataset files in `dir`.
    pub fn of(dir: &Path) -> std::io::Result<Stamps> {
        let mut files = stamps_of(dir, &DATASET_FILES)?;
        files.extend(stamps_of(dir, &CHAIN_INPUTS)?);
        Ok(Stamps(files))
    }
}

/// The stat of each of the files `names` present in `dir`, by name; an error names the file it
/// is of.
pub fn stamps_of(dir: &Path, names: &[&str]) -> std::io::Result<BTreeMap<String, FileStamp>> {
    let mut out = BTreeMap::new();
    for name in names {
        let path = dir.join(name);
        match std::fs::metadata(&path) {
            Ok(m) => {
                out.insert(name.to_string(), FileStamp::of(&m));
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            Err(e) => return Err(std::io::Error::new(e.kind(), format!("{}: {e}", path.display()))),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stamp_reads_back_from_its_json() {
        let stamp = FileStamp { size: 82_487_980, mtime_ns: u128::from(u64::MAX) * 3, ino: 1 << 40 };

        assert_eq!(FileStamp::from_json(&stamp.to_json()), Some(stamp));
        assert_eq!(FileStamp::from_json(&json!({"size": 1, "ino": 2})), None);
    }
}
