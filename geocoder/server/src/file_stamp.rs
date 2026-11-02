//! A file's stat: its size, modification time and inode. Writing or replacing a file changes it
//! (a patch or a download renames a new file over the old one, which gives it a new inode), so
//! whatever was computed from a file is reused while the file keeps its stamp.

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
