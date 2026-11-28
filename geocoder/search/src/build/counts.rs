use super::write::TableWriter;
use crate::{index::invalid, sort::ExternalSort, varint};
use std::{io, ops::Range, path::Path};

/// The input intervals arrive in start order. Only their rightmost covered point
/// is needed to count the union, even for a word shared by nested polygons.
#[derive(Default)]
pub(super) struct Coverage {
    pub count: u32,
    end: u32,
}
impl Coverage {
    pub fn include(&mut self, start: u32, end: u32) {
        self.count += end.saturating_sub(start.max(self.end));
        self.end = self.end.max(end);
    }
}
pub(super) fn write(sort: ExternalSort<[u32; 3]>, out: &Path, tmp: &Path, slots: u32) -> io::Result<()> {
    let mut rows = sort.finish()?.merge(tmp, "area-merge")?;
    let mut key = Vec::new();
    let mut next = rows.next(&mut key)?;
    let mut table = TableWriter::new(out, "core.area", tmp)?;
    let mut bytes = Vec::new();
    for slot in 0..slots {
        table.begin(&[])?;
        let mut active: Option<Range<u32>> = None;
        let mut last = 0;
        let mut flush = |r: Range<u32>| -> io::Result<()> {
            bytes.clear();
            varint::put(&mut bytes, u64::from(r.start - last));
            varint::put(&mut bytes, u64::from(r.end - r.start));
            last = r.end;
            table.append(&bytes)
        };
        while let Some([s, start, end]) = next {
            if s != slot {
                break;
            }
            match &mut active {
                Some(r) if start <= r.end => r.end = r.end.max(end),
                _ => {
                    if let Some(r) = active.replace(start..end) {
                        flush(r)?
                    }
                }
            }
            next = rows.next(&mut key)?;
        }
        if let Some(r) = active {
            flush(r)?
        }
    }
    if next.is_some() {
        return Err(invalid("chain contains an unknown polygon"));
    }
    table.finish(&[], &tmp.join("core.area.blob"))
}
