use super::{invalid, small, BlobTable};
use std::{io, path::Path};
pub struct ChainTable(BlobTable);
impl ChainTable {
    pub(crate) fn open(dir: &Path, entities: u32) -> io::Result<Self> {
        let table = BlobTable::open(&dir.join("core.chn"), 8)?;
        if table.field(table.rows, 0) != Some(entities) {
            return Err(invalid("bad final chain boundary"));
        }
        Ok(Self(table))
    }
    pub fn of_entity(&self, id: u32) -> Option<Vec<u32>> {
        let t = &self.0;
        let (mut lo, mut hi) = (0, t.rows);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if t.field(mid, 0)? <= id {
                lo = mid + 1
            } else {
                hi = mid
            }
        }
        if id >= t.field(lo, 0)? {
            return None;
        }
        let b = t.slice(lo.checked_sub(1)?)?;
        let mut at = 0;
        let mut slots = Vec::new();
        while at < b.len() {
            slots.push(small(b, &mut at)?);
        }
        Some(slots)
    }
}
