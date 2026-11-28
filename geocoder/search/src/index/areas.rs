use super::{entities::BBox, small, BlobTable};
use crate::{constants::BBOX_BYTES, profile::Profile};
use std::{io, ops::Range, path::Path};
pub struct AreaRanges {
    table: BlobTable,
    entities: u32,
    pub(crate) profile: Profile,
}
impl AreaRanges {
    pub(crate) fn open(dir: &Path, entities: u32, profile: Profile) -> io::Result<Self> {
        let stride = size_of::<u32>() + if profile == Profile::Full { BBOX_BYTES } else { 0 };
        Ok(Self { table: BlobTable::open(&dir.join("core.area"), stride)?, entities, profile })
    }

    pub fn bbox(&self, slot: u32) -> Option<BBox> {
        let row = slot as usize;
        if self.profile != Profile::Full || row >= self.table.rows { return None; }
        let coord = |i| self.table.field(row, i * size_of::<i32>()).map(|n| i64::from(n as i32));
        let (min_lat, min_lon) = query_server::PointE7 { lat: coord(0)?, lng: coord(1)? }.degrees();
        let (max_lat, max_lon) = query_server::PointE7 { lat: coord(2)?, lng: coord(3)? }.degrees();
        (min_lat <= max_lat && min_lon <= max_lon).then_some(BBox { min_lat, min_lon, max_lat, max_lon })
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
