use super::{small, BlobTable};
use std::{io, ops::Range, path::Path};
pub struct AreaRanges {
    table: BlobTable,
    entities: u32,
}
impl AreaRanges {
    pub(crate) fn open(dir: &Path, entities: u32) -> io::Result<Self> {
        Ok(Self { table: BlobTable::open(&dir.join("core.area"), 4)?, entities })
    }
    /// Disjoint half-open ranges, including any sampled intersections.
    pub fn ranges(&self, slot: u32) -> impl Iterator<Item = Range<u32>> + '_ {
        let bytes = self.table.slice(slot as usize).unwrap_or_default();
        let (mut at, mut end) = (0, 0u32);
        std::iter::from_fn(move || {
            let start = end.checked_add(small(bytes, &mut at)?)?;
            end = start.checked_add(small(bytes, &mut at)?)?;
            (end > start && end <= self.entities).then_some(start..end)
        })
        .fuse()
    }
}
