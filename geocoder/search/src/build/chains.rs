use super::{
    counts,
    sources::{side, CoreRecords, SourceRow},
    write::{writer, TableWriter},
    BuildOpts,
};
use crate::{
    constants::*,
    index::{
        entities::{EntityKind, EntityRef},
        invalid, map, small,
    },
    profile::Profile,
    sort::ExternalSort,
    varint,
};
use query_server::{Index, NO_DATA};
use std::{
    io::{self, Seek, SeekFrom, Write},
    path::Path,
};

fn at(geo: &Index, lat: f64, lon: f64) -> Vec<u32> {
    let mut hits = geo.find_admin(lat, lon).ranked;
    hits.retain(|p| {
        p.poly.name_id != NO_DATA
            && ((COUNTRY_LEVEL..=ADMIN_LEVEL_MAX).contains(&p.poly.admin_level)
                || p.poly.admin_level == PLACE_AREA_LEVEL)
    });
    hits.sort_unstable_by_key(|p| (p.poly.admin_level, p.slot));
    // Zero separates levels. Real slots start at one, so a missing parent sorts
    // before a present parent even when the next level's slot number is larger.
    let mut tuple = Vec::new();
    let mut level = COUNTRY_LEVEL;
    for hit in hits {
        tuple.resize(tuple.len() + usize::from(hit.poly.admin_level - level), 0);
        tuple.push(hit.slot + 1);
        level = hit.poly.admin_level;
    }
    tuple
}
/// Inside an admin cell containing only interior entries the membership cannot change.
struct PointChains<'a> {
    geo: &'a Index,
    cells: std::collections::VecDeque<(u64, Vec<u32>)>,
}
impl<'a> PointChains<'a> {
    fn get(&mut self, lat: f64, lon: f64) -> Vec<u32> {
        let (lat, lon) = query_server::AdminPoint::new(lat, lon).degrees();
        let id = query_server::cell_id_at_level(lat, lon, self.geo.admin_cell_level);
        if let Some((_, v)) = self.cells.iter().find(|(cell, _)| *cell == id) {
            return v.clone();
        }
        let slots = at(self.geo, lat, lon);
        let mut interior = true;
        self.geo.admin_index.with_ids(id, |ids| interior = ids.iter().all(|v| v & query_server::INTERIOR_FLAG != 0));
        if interior {
            if self.cells.len() == CHAIN_CACHE_CELLS {
                self.cells.pop_front();
            }
            self.cells.push_back((id, slots.clone()));
        }
        slots
    }
}
/// Shipped chain ids are in tuple order. Once records have been sorted by id, the
/// dictionary is consumed front to back, including chains unused by this selection.
struct Shipped {
    bytes: memmap2::Mmap,
    at: usize,
    next: u32,
    slots: Vec<u32>,
}
impl Shipped {
    fn through(&mut self, id: u32) -> io::Result<&[u32]> {
        while self.next <= id {
            self.slots.clear();
            let n = small(&self.bytes, &mut self.at).ok_or_else(|| invalid("missing shipped chain"))?;
            for _ in 0..n {
                self.slots.push(small(&self.bytes, &mut self.at).ok_or_else(|| invalid("short shipped chain"))?);
            }
            self.next = self.next.checked_add(1).ok_or_else(|| invalid("chain id overflow"))?;
        }
        Ok(&self.slots)
    }
}
pub(super) fn build(
    records: &CoreRecords,
    set: &Path,
    out: &Path,
    tmp: &Path,
    opts: &BuildOpts,
    profile: Profile,
) -> io::Result<u32> {
    let mut points = PointChains { geo: records.geo, cells: Default::default() };
    let budget = opts.mem_mb << 20;
    let mut sort = ExternalSort::new(budget, tmp, "chain".into())?;
    let dict = set.join(CHAIN_DICTIONARY);
    let mut shipped =
        if dict.try_exists()? { Some(Shipped { bytes: map(&dict)?, at: 0, next: 0, slots: Vec::new() }) } else { None };
    let mut key = Vec::new();
    for kind in CORE_KINDS {
        let n = records.sizes[kind as usize];
        let ids = side(&set.join(CORE_CHAIN_FILES[kind as usize]), n)?;
        if n > 0 && shipped.is_some() != ids.is_some() {
            return Err(invalid("incomplete shipped chain files"));
        }
        for i in 0..n {
            let e = EntityRef::new(kind, i).ok_or_else(|| invalid("record exceeds entity row"))?;
            key.clear();
            if let Some(ids) = &ids {
                let id = ids.get(i).ok_or_else(|| invalid("bad record chain id"))?;
                if id == NO_DATA {
                    continue;
                }
                if kind == EntityKind::Poi
                    && opts.poi_max_tier < crate::constants::MAX_CORE_POI_TIER
                    && records.geo.poi_record(i).is_none_or(|p| p.tier > opts.poi_max_tier)
                {
                    continue;
                }
                key.extend_from_slice(&id.to_be_bytes());
            } else {
                let Some(row) = records.row(e) else { continue };
                if matches!(row,SourceRow::Poi(p) if p.tier>opts.poi_max_tier) {
                    continue;
                }
                let Some((lat, lon)) = row.point(records.geo) else { continue };
                for field in points.get(lat, lon) {
                    key.extend_from_slice(&field.to_be_bytes())
                }
            }
            sort.push(&key, [e.packed()])?;
        }
    }
    let mut input = sort.finish()?.merge(tmp, "chain-merge")?;
    let mut ents = writer(&out.join("core.ent"))?;
    ents.write_all(ENTITY_MAGIC)?;
    ents.write_all(&0u32.to_le_bytes())?;
    let mut chains = TableWriter::new(out, "core.chn", tmp)?;
    let mut by_record = ExternalSort::new(budget / 2, tmp, "record".into())?;
    let mut ranges = ExternalSort::new(budget / 4, tmp, "ranges".into())?;
    let mut prev: Option<Vec<u8>> = None;
    let mut slots = Vec::new();
    let mut first = 0;
    let mut n = 0u32;
    let mut cc = 0;
    let mut blob = Vec::new();
    while let Some([packed]) = input.next(&mut key)? {
        if prev.as_ref() != Some(&key) {
            for &s in &slots {
                ranges.push(&[], [s, first, n])?;
            }
            first = n;
            slots.clear();
            if let Some(dict) = &mut shipped {
                slots.extend_from_slice(dict.through(u32::from_be_bytes(key[..].try_into().unwrap()))?);
            } else {
                slots.extend(
                    key.chunks_exact(4).filter_map(|b| u32::from_be_bytes(b.try_into().unwrap()).checked_sub(1)),
                );
            }
            blob.clear();
            for &s in &slots {
                varint::put(&mut blob, s.into());
            }
            chains.begin(&[n])?;
            chains.append(&blob)?;
            cc = slots
                .first()
                .and_then(|&s| records.geo.admin_polygon(s))
                .filter(|p| p.admin_level == COUNTRY_LEVEL)
                .map_or(0, |p| p.country_code as u32);
            prev.get_or_insert_with(Vec::new).clone_from(&key);
        }
        ents.write_all(&packed.to_le_bytes())?;
        by_record.push(&[], [packed, n, cc])?;
        let end = n.checked_add(1).ok_or_else(|| invalid("too many entities"))?;
        if profile == Profile::Full {
            let e = EntityRef::unpack(packed).unwrap();
            if e.kind() == EntityKind::Area {
                let poly =
                    records.geo.admin_polygon(e.record()).ok_or_else(|| invalid("missing polygon of shipped chain"))?;
                let mut last = Vec::new();
                let mut added = Ok(());
                let read = records.geo.admin_geometry.for_each_vertex(&poly, |v| {
                    if added.is_err() {
                        return;
                    }
                    let (lat, lon) = v.degrees();
                    let parents = points.get(lat, lon);
                    if parents != last {
                        for s in parents.iter().filter_map(|v| v.checked_sub(1)).filter(|s| !slots.contains(s)) {
                            if let Err(e) = ranges.push(&[], [s, n, end]) {
                                added = Err(e);
                                break;
                            }
                        }
                        last = parents;
                    }
                });
                added?;
                read.ok_or_else(|| invalid("unreadable area outline"))?;
            }
        }
        n = end;
    }
    for s in slots {
        ranges.push(&[], [s, first, n])?;
    }
    ents.seek(SeekFrom::Start(4))?;
    ents.write_all(&n.to_le_bytes())?;
    ents.flush()?;
    chains.finish(&[n], &tmp.join("core.chn.blob"))?;
    let runs = by_record.finish()?;
    counts::write(ranges, out, tmp, records.geo.admin_polygon_count())?;
    let mut rows = runs.merge(tmp, "record-merge")?;
    let mut sorted = writer(&tmp.join("records"))?;
    while let Some(row) = rows.next(&mut key)? {
        for v in row {
            sorted.write_all(&v.to_le_bytes())?
        }
    }
    sorted.flush()?;
    Ok(n)
}
