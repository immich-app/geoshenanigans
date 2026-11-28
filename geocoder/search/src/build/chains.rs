//! Shipped chain ids let devices skip point-in-polygon tests. Each record family has
//! an SDB1 column in its original row order, with NO_DATA for excluded records.
//! Ids index search_chains.bin in lexicographic level-tuple order. Each tuple is
//! a varint slot count followed by that many absolute varint polygon slots, outer
//! levels first. Sorting records by id makes this table a single forward read.
use super::{
    counts,
    sources::{side, CoreRecords, SourceRow},
    write::{writer, EntityWriter, TableWriter},
    BuildOpts,
};
use crate::{
    constants::*,
    index::{
        entities::{EntityKind, EntityRef},
        invalid,
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
    reader: std::io::BufReader<std::fs::File>,
    next: u32,
    slots: Vec<u32>,
}
impl Shipped {
    fn through(&mut self, id: u32) -> io::Result<&[u32]> {
        while self.next <= id {
            self.slots.clear();
            let n = varint::read_u32(&mut self.reader)?;
            for _ in 0..n {
                self.slots.push(varint::read_u32(&mut self.reader)?);
            }
            self.next = self.next.checked_add(1).ok_or_else(|| invalid("chain id overflow"))?;
        }
        Ok(&self.slots)
    }
}
/// Each polygon has an open interval only while the chain's entities can take its rank.
struct Parent {
    slot: u32,
    rank: u8,
    start: Option<u32>,
}

fn transition(parents: &mut [Parent], max_rank: u8, at: u32, ranges: &mut ExternalSort<[u32; 3]>) -> io::Result<()> {
    for p in parents {
        if p.rank != 0 && p.rank < max_rank {
            p.start.get_or_insert(at);
        } else if let Some(start) = p.start.take() {
            ranges.push(&[], [p.slot, start, at])?;
        }
    }
    Ok(())
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
    let dict = set.join(CHAIN_DICTIONARY);
    let mut shipped = if dict.try_exists()? {
        Some(Shipped { reader: std::io::BufReader::new(std::fs::File::open(dict)?), next: 0, slots: Vec::new() })
    } else {
        None
    };
    let mut sort = if shipped.is_some() {
        ExternalSort::fixed(budget / 2, tmp, "chain".into())?
    } else {
        ExternalSort::new(budget, tmp, "chain".into())?
    };
    let mut key = Vec::new();
    for kind in CORE_KINDS {
        let n = records.sizes[kind as usize];
        let ids = side(&set.join(CORE_CHAIN_FILES[kind as usize]), n)?;
        if n > 0 && shipped.is_some() != ids.is_some() {
            return Err(invalid("incomplete shipped chain files"));
        }
        let mut column = ids.as_ref().map(|ids| ids.values());
        // The rest of the device build needs these pages for strings and sort runs.
        let labels = std::sync::OnceLock::new();
        for i in 0..n {
            let chain = column.as_mut().and_then(|values| values.next()).flatten();
            let e = EntityRef::new(kind, i).ok_or_else(|| invalid("record exceeds entity row"))?;
            let Some(row) = records.row(e) else { continue };
            if matches!(row, SourceRow::Poi(p) if p.tier > opts.poi_max_tier) {
                continue;
            }
            let point = match row {
                SourceRow::Area(a) => records
                    .geo
                    .area_centre_with(&a, &labels)
                    .filter(|&(lat, lon)| query_server::coords_in_range(lat, lon)),
                _ => row.point(records.geo),
            };
            let Some((lat, lon)) = point else { continue };
            key.clear();
            let chain_id = if shipped.is_some() {
                chain.filter(|&id| id != NO_DATA).ok_or_else(|| invalid("incomplete shipped chain files"))?
            } else {
                for field in points.get(lat, lon) {
                    key.extend_from_slice(&field.to_be_bytes())
                }
                0
            };
            sort.push(&key, [chain_id, e.packed(), u32::from(row.max_rank(records.geo, (lat, lon)))])?;
        }
    }
    let mut input = sort.finish()?.merge(tmp, "chain-merge")?;
    let mut ents = EntityWriter::new(out, tmp)?;
    let mut chains = TableWriter::new(out, "core.chn", tmp)?;
    let mut by_record = ExternalSort::fixed(budget / 2, tmp, "record".into())?;
    let mut ranges = ExternalSort::fixed(budget / 4, tmp, "ranges".into())?;
    let mut prev: Option<Vec<u8>> = None;
    let mut last_id = 0;
    let mut slots = Vec::new();
    let mut parents = Vec::new();
    let mut previous_rank = 0;
    let mut n = 0u32;
    let mut cc = 0;
    let mut blob = Vec::new();
    // Ring chunk boxes omit the southern edge. Keep the exact extrema from the vertex pass.
    let mut bounds = if profile == Profile::Full {
        let file = std::fs::File::options().read(true).write(true).create_new(true).open(tmp.join("bounds"))?;
        file.set_len(u64::from(records.geo.admin_polygon_count()) * BBOX_BYTES as u64)?;
        Some(file)
    } else {
        None
    };
    while let Some([id, packed, rank]) = input.next(&mut key)? {
        if id != last_id || prev.as_ref() != Some(&key) {
            last_id = id;
            transition(&mut parents, 0, n, &mut ranges)?;
            parents.clear();
            previous_rank = 0;
            slots.clear();
            if let Some(dict) = &mut shipped {
                slots.extend_from_slice(dict.through(id)?);
            } else {
                slots.extend(
                    key.chunks_exact(4).filter_map(|b| u32::from_be_bytes(b.try_into().unwrap()).checked_sub(1)),
                );
            }
            blob.clear();
            for &s in &slots {
                let poly = records.geo.admin_polygon(s).ok_or_else(|| invalid("chain contains an unknown polygon"))?;
                parents.push(Parent { slot: s, rank: poly.rank_address, start: None });
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
        let e = EntityRef::unpack(packed).unwrap();
        let max_rank = rank as u8;
        if previous_rank != max_rank {
            transition(&mut parents, max_rank, n, &mut ranges)?;
            previous_rank = max_rank;
        }
        ents.push(n, e)?;
        by_record.push(&[], [packed, n, cc])?;
        let end = n.checked_add(1).ok_or_else(|| invalid("too many entities"))?;
        if let Some(file) = &mut bounds {
            if e.kind() == EntityKind::Area {
                let poly =
                    records.geo.admin_polygon(e.record()).ok_or_else(|| invalid("missing polygon of shipped chain"))?;
                let outline = poly.rank_address == 0 && max_rank > OUTLINE_ADDRESS_MIN_RANK;
                let mut bbox = [i32::MAX, i32::MAX, i32::MIN, i32::MIN];
                let mut last = Vec::new();
                let mut added = Ok(());
                let read = records.geo.admin_geometry.for_each_vertex(&poly, |v| {
                    if added.is_err() {
                        return;
                    }
                    bbox[0] = bbox[0].min(v.lat as i32);
                    bbox[1] = bbox[1].min(v.lng as i32);
                    bbox[2] = bbox[2].max(v.lat as i32);
                    bbox[3] = bbox[3].max(v.lng as i32);
                    if !outline {
                        return;
                    }
                    let (lat, lon) = v.degrees();
                    let parents = points.get(lat, lon);
                    if parents != last {
                        for s in parents.iter().filter_map(|v| v.checked_sub(1)).filter(|s| !slots.contains(s)) {
                            if !records
                                .geo
                                .admin_polygon(s)
                                .is_some_and(|p| p.rank_address != 0 && p.rank_address < max_rank)
                            {
                                continue;
                            }
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
                file.seek(SeekFrom::Start(u64::from(e.record()) * BBOX_BYTES as u64))?;
                let mut row = [0; BBOX_BYTES];
                for (dst, coord) in row.chunks_exact_mut(size_of::<i32>()).zip(bbox) {
                    dst.copy_from_slice(&coord.to_le_bytes());
                }
                file.write_all(&row)?;
            }
        }
        n = end;
    }
    transition(&mut parents, 0, n, &mut ranges)?;
    ents.finish(n, tmp)?;
    chains.finish(&[n], &tmp.join("core.chn.blob"))?;
    let runs = by_record.finish()?;
    counts::write(ranges, out, tmp, records.geo.admin_polygon_count(), bounds)?;
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
