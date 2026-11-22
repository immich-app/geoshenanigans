//! A dataset's water layer: the named bodies of water of one of the builder's water dirs
//! (`{region}/water/{all,major}/{simplified,exact}/`, builder/src/water_layer.h), which a client
//! puts in `<dataset>/water/` (PATCHING.md). Its files have the admin geometry's names and
//! formats but are a set of their own, pointing into nothing else:
//! - admin_polygons.bin and admin_rings.bin: a record per polygon, an OSM object's outer ring
//!   with its holes or a piece of it cut along a grid, carrying the object's name, its type code
//!   in the country code's place and its area, all its outer rings' in square degrees;
//! - arc_index.bin and arc_vertices.bin: the arcs of those rings;
//! - admin_cells.bin and admin_entries.bin: each L10 cell's polygons, flagged interior where
//!   their rings hold the whole cell;
//! - strings_core.bin and strings_layout.json: the names, in a core tier of their own;
//! - water_meta.json: each type code's class, type and Nominatim rank.
//!
//! The water at a point is the smallest object whose rings hold it, holes honoured, then the one
//! of the higher rank, the lower type code and the smaller name. It answers beside the address,
//! never changing it: an extension of Nominatim's answer (NOMINATIM_REFERENCE.md).

use crate::{
    cell_id_at_level, coords_in_range, mmap_file, polygon_record, AdminGeometry, AdminPoint, AdminPolygon,
    CellPairs, FileBytes, PipMemo, StringPool, ADMIN_POLYGON_RECORD_BYTES, ID_MASK, INTERIOR_FLAG, NO_DATA,
};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::cmp::Ordering;

/// The dir of a dataset that holds its water layer.
pub const WATER_DIR: &str = "water";

/// The files a water layer loads from, by name: a dir holds all of them or none.
pub const WATER_FILES: [&str; 9] = [
    "admin_cells.bin",
    "admin_entries.bin",
    "admin_polygons.bin",
    "admin_rings.bin",
    "arc_index.bin",
    "arc_vertices.bin",
    "strings_core.bin",
    "strings_layout.json",
    "water_meta.json",
];

/// A type code's kind, as water_meta.json lists it: Nominatim's class and type, and its
/// rank_search (settings/address-levels.json).
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct WaterKind {
    pub code: u16,
    pub class: String,
    pub r#type: String,
    pub rank: u8,
}

/// The kinds of a water dir's type codes (water_meta.json), by code.
#[derive(Debug, Default)]
pub struct WaterMeta {
    kinds: Vec<WaterKind>,
}

impl WaterMeta {
    /// The kinds water_meta.json's text lists; an error for text that isn't such a list, or
    /// that gives a code twice.
    pub fn from_json(text: &str) -> Result<WaterMeta, String> {
        #[derive(Deserialize)]
        struct Listed {
            types: Vec<WaterKind>,
        }
        let mut kinds = serde_json::from_str::<Listed>(text).map_err(|e| e.to_string())?.types;
        kinds.sort_by_key(|k| k.code);
        if let Some(twice) = kinds.windows(2).find(|w| w[0].code == w[1].code) {
            return Err(format!("type code {} listed twice", twice[0].code));
        }
        Ok(WaterMeta { kinds })
    }

    /// The kind of type code `code`; none for a code it doesn't list.
    pub fn kind(&self, code: u16) -> Option<&WaterKind> {
        self.kinds.binary_search_by_key(&code, |k| k.code).ok().map(|i| &self.kinds[i])
    }
}

/// The water an answer names: the object's name, and its kind's class and type, which are the
/// `category` and `type` Nominatim gives it (`type` the key its `address` names it by).
#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
pub struct WaterDetail<'a> {
    pub name: Cow<'a, str>,
    pub class: &'a str,
    pub r#type: &'a str,
}

/// A polygon of the point's cell that may hold it, with its kind.
struct Candidate<'a> {
    poly: AdminPolygon,
    interior: bool,
    kind: &'a WaterKind,
}

impl Candidate<'_> {
    /// The order in which candidates answer, but for the name: the smaller area, then the higher
    /// rank, then the lower type code.
    fn order(&self, other: &Self) -> Ordering {
        self.poly.area.total_cmp(&other.poly.area)
            .then(other.kind.rank.cmp(&self.kind.rank))
            .then(self.poly.country_code.cmp(&other.poly.country_code))
    }
}

/// A water dir, mapped.
pub struct WaterLayer {
    index: CellPairs,
    polygons: FileBytes,
    geometry: AdminGeometry,
    strings: StringPool,
    meta: WaterMeta,
    cell_level: u64,
}

impl WaterLayer {
    /// The layer of these files; `index` is read at `cell_level`.
    pub fn new(index: CellPairs, polygons: FileBytes, geometry: AdminGeometry, strings: StringPool, meta: WaterMeta, cell_level: u64) -> Self {
        WaterLayer { index, polygons, geometry, strings, meta, cell_level }
    }

    /// The water layer in `dir` (a dataset's water dir), its cells at `cell_level`: none where
    /// the dir isn't there or holds none of WATER_FILES; an error where it holds some of them and
    /// not all, or one that doesn't load, and where one of them can't be looked up or isn't a
    /// file, naming it.
    pub fn open(dir: &str, cell_level: u64) -> Result<Option<WaterLayer>, String> {
        let held = WATER_FILES.iter().map(|name| {
            let path = format!("{dir}/{name}");
            match std::fs::metadata(&path) {
                Ok(m) if m.is_file() => Ok(true),
                Ok(_) => Err(format!("{path}: not a file")),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
                Err(e) => Err(format!("{path}: {e}")),
            }
        }).collect::<Result<Vec<bool>, String>>()?;
        if !held.contains(&true) {
            return Ok(None);
        }
        if held.contains(&false) {
            let missing: Vec<&str> = WATER_FILES.iter().zip(&held).filter(|(_, &h)| !h).map(|(name, _)| *name).collect();
            return Err(format!("Partial water dir {dir}: missing [{}]", missing.join(", ")));
        }
        let index = CellPairs::open(mmap_file(&format!("{dir}/admin_cells.bin"))?, mmap_file(&format!("{dir}/admin_entries.bin"))?,
                                    crate::cell_pairs::ADMIN, cell_level)
            .map_err(|e| format!("{dir}/admin_cells.bin: {e}"))?;
        let meta_path = format!("{dir}/water_meta.json");
        let meta = std::fs::read_to_string(&meta_path).map_err(|e| format!("Failed to read {meta_path}: {e}"))?;
        let meta = WaterMeta::from_json(&meta).map_err(|e| format!("{meta_path}: {e}"))?;
        Ok(Some(WaterLayer {
            index,
            polygons: mmap_file(&format!("{dir}/admin_polygons.bin"))?,
            geometry: AdminGeometry::load(dir)?,
            strings: StringPool::load(dir)?,
            meta,
            cell_level,
        }))
    }

    /// The water holding the point (`lat`, `lng`), if any: off the globe, none.
    pub fn at(&self, lat: f64, lng: f64) -> Option<WaterDetail<'_>> {
        self.at_with(lat, lng, &mut PipMemo::new())
    }

    /// `at`, its ring tests' answers kept in `memo`, which sees only this point.
    fn at_with(&self, lat: f64, lng: f64, memo: &mut PipMemo) -> Option<WaterDetail<'_>> {
        if !coords_in_range(lat, lng) {
            return None;
        }
        // Rounded once, as every ring test takes the point; its cell is the rounded point's.
        let point = AdminPoint::new(lat, lng);
        let (lat, lng) = point.degrees();
        let mut candidates: Vec<Candidate<'_>> = self.index.with_ids(cell_id_at_level(lat, lng, self.cell_level), |ids| {
            ids.iter().for_each(|&id| self.polygons.prefetch(u64::from(id & ID_MASK) * ADMIN_POLYGON_RECORD_BYTES));
            ids.iter()
                .filter_map(|&id| {
                    let poly = polygon_record(&self.polygons, self.geometry.rings_len(), id & ID_MASK)?;
                    // A tombstone, a polygon without rings and a code the dir lists no kind for
                    // answer nothing.
                    if poly.name_id == NO_DATA || !self.geometry.has_rings(&poly) {
                        return None;
                    }
                    let kind = self.meta.kind(poly.country_code)?;
                    Some(Candidate { poly, interior: id & INTERIOR_FLAG != 0, kind })
                })
                .collect()
        });
        candidates.sort_by(Candidate::order);
        for c in &candidates {
            self.geometry.prefetch(&c.poly);
        }
        // In answering order, so the first candidate that holds the point answers, but where
        // another of its area, rank and code (another object's) holds it too: the smaller name
        // answers. A polygon of the name already held (another piece of its object) is not
        // tested. An interior entry holds every point of the cell.
        let mut best: Option<&Candidate<'_>> = None;
        for c in &candidates {
            if let Some(b) = best {
                if c.order(b) != Ordering::Equal {
                    break;
                }
                if c.poly.name_id == b.poly.name_id {
                    continue;
                }
            }
            if !c.interior && !self.geometry.contains(&c.poly, point, memo) {
                continue;
            }
            best = match best {
                Some(b) if self.strings.get(b.poly.name_id) <= self.strings.get(c.poly.name_id) => Some(b),
                _ => Some(c),
            };
        }
        best.map(|b| WaterDetail { name: self.strings.get(b.poly.name_id), class: &b.kind.class, r#type: &b.kind.r#type })
    }
}

/// Water dirs as the builder writes them, for the tests: rectangular areas with rectangular
/// holes, each ring one arc.
#[cfg(test)]
pub(crate) mod fixture {
    use super::*;
    use crate::arc_fixture::{self, ArcFixture};
    use crate::{cell_pairs, strings, DEFAULT_ADMIN_CELL_LEVEL, STR_TIER_COUNT};
    use s2::cellid::CellID;
    use s2::rect::Rect;
    use s2::region::RegionCoverer;
    use std::collections::BTreeMap;

    /// (south, west, north, east) in degrees.
    pub type Span = (f64, f64, f64, f64);

    /// The kinds the fixtures use, as water_meta.json lists the builder's fixed codes.
    pub const META: &str = r#"{"types": [
  {"code": 1, "class": "natural", "type": "water", "rank": 22},
  {"code": 2, "class": "water", "type": "lake", "rank": 20},
  {"code": 4, "class": "water", "type": "pond", "rank": 24},
  {"code": 21, "class": "natural", "type": "bay", "rank": 22},
  {"code": 23, "class": "natural", "type": "wetland", "rank": 22},
  {"code": 25, "class": "place", "type": "sea", "rank": 2},
  {"code": 29, "class": "waterway", "type": "river", "rank": 19}
]}
"#;
    pub const NATURAL_WATER: u16 = 1;
    pub const LAKE: u16 = 2;
    pub const POND: u16 = 4;
    pub const BAY: u16 = 21;
    pub const WETLAND: u16 = 23;
    pub const SEA: u16 = 25;
    pub const RIVER: u16 = 29;

    struct Area {
        name: String,
        code: u16,
        area: Option<f32>,
        outer: Span,
        holes: Vec<Span>,
    }

    /// The polygons of a water dir, in slot order.
    #[derive(Default)]
    pub struct WaterFixture {
        areas: Vec<Area>,
        meta: Option<String>,
    }

    fn ring((s, w, n, e): Span) -> Vec<crate::PointE7> {
        [(s, w), (s, e), (n, e), (n, w), (s, w)].iter().map(|&(lat, lng)| arc_fixture::e7(lat, lng)).collect()
    }

    /// The L10 cells meeting the span.
    fn cells((s, w, n, e): Span) -> Vec<u64> {
        let rect = Rect::from_degrees(s, w, n, e);
        let coverer = RegionCoverer { min_level: DEFAULT_ADMIN_CELL_LEVEL as u8, max_level: DEFAULT_ADMIN_CELL_LEVEL as u8, level_mod: 1, max_cells: 1 << 16 };
        coverer.covering(&rect).0.iter().map(|c| c.0).collect()
    }

    /// Whether the cell lies inside the span.
    fn inside(cell: u64, (s, w, n, e): Span) -> bool {
        let b = s2::cell::Cell::from(CellID(cell)).rect_bound();
        b.lat_lo().deg() > s && b.lat_hi().deg() < n && b.lng_lo().deg() > w && b.lng_hi().deg() < e
    }

    /// Whether the cell meets the span.
    fn meets(cell: u64, (s, w, n, e): Span) -> bool {
        let b = s2::cell::Cell::from(CellID(cell)).rect_bound();
        b.lat_lo().deg() < n && b.lat_hi().deg() > s && b.lng_lo().deg() < e && b.lng_hi().deg() > w
    }

    impl WaterFixture {
        pub fn new() -> Self {
            Self::default()
        }

        /// An area of its own: its record's area that of its rings.
        pub fn area(&mut self, name: &str, code: u16, outer: Span, holes: &[Span]) -> &mut Self {
            self.areas.push(Area { name: name.into(), code, area: None, outer, holes: holes.to_vec() });
            self
        }

        /// A polygon of an object of `area` square degrees (a piece of one the builder cut, or
        /// one of its outer rings): its record's area the object's.
        pub fn piece(&mut self, name: &str, code: u16, area: f32, outer: Span) -> &mut Self {
            self.areas.push(Area { name: name.into(), code, area: Some(area), outer, holes: Vec::new() });
            self
        }

        /// water_meta.json's text, in place of META.
        pub fn meta(&mut self, text: &str) -> &mut Self {
            self.meta = Some(text.into());
            self
        }

        /// The files, by name. Each polygon is listed in every cell meeting its outer ring,
        /// flagged interior where the cell lies inside it and meets none of its holes; the
        /// names are the strings of a core tier.
        pub fn files(&self) -> Vec<(&'static str, Vec<u8>)> {
            let (mut pool, mut offsets) = (Vec::new(), BTreeMap::new());
            for a in &self.areas {
                offsets.entry(a.name.clone()).or_insert_with(|| {
                    let at = pool.len() as u32;
                    pool.extend_from_slice(a.name.as_bytes());
                    pool.push(0);
                    at
                });
            }
            let mut arcs = ArcFixture::new();
            let mut listed: BTreeMap<u64, Vec<u32>> = BTreeMap::new();
            for (id, a) in self.areas.iter().enumerate() {
                let rings: Vec<Vec<crate::PointE7>> = std::iter::once(a.outer).chain(a.holes.iter().copied()).map(ring).collect();
                let area = a.area.unwrap_or_else(|| arc_fixture::exact_area(&rings));
                let record = AdminPolygon { country_code: a.code, ..arc_fixture::record(offsets[&a.name], 0, area) };
                arcs.ring_polygon(record, 0, &rings);
                for cell in cells(a.outer) {
                    let interior = inside(cell, a.outer) && !a.holes.iter().any(|&h| meets(cell, h));
                    listed.entry(cell).or_default().push(id as u32 | if interior { INTERIOR_FLAG } else { 0 });
                }
            }
            let (cells, entries) = cell_pairs::write(&listed.into_iter().collect::<Vec<_>>(), cell_pairs::ADMIN);
            let end = pool.len();
            let tiers: Vec<String> = (0..STR_TIER_COUNT).map(|t| format!("{{\"start\": {}, \"end\": {end}}}", if t == 0 { 0 } else { end })).collect();
            let mut files: Vec<(&'static str, Vec<u8>)> = arcs.files().into();
            files.extend([
                ("admin_cells.bin", cells),
                ("admin_entries.bin", entries),
                ("strings_core.bin", strings::encode(&pool)),
                ("strings_layout.json", format!("{{\"tiers\": [{}]}}", tiers.join(", ")).into_bytes()),
                ("water_meta.json", self.meta.clone().unwrap_or_else(|| META.into()).into_bytes()),
            ]);
            files
        }

        /// Writes the files into `dir`, made where it isn't there.
        pub fn write(&self, dir: &std::path::Path) {
            std::fs::create_dir_all(dir).unwrap();
            for (name, bytes) in self.files() {
                std::fs::write(dir.join(name), bytes).unwrap();
            }
        }

        /// The layer of the files, held in memory.
        pub fn layer(&self) -> WaterLayer {
            let files: BTreeMap<&str, Vec<u8>> = self.files().into_iter().collect();
            let file = |name: &str| FileBytes::Owned(files[name].clone());
            let layout: serde_json::Value = serde_json::from_slice(&files["strings_layout.json"]).unwrap();
            let end = layout["tiers"][0]["end"].as_u64().unwrap() as u32;
            let core = strings::Tier::open(file("strings_core.bin")).unwrap();
            let strings = StringPool::from_parts([Some(core), None, None, None, None], [0, end, end, end, end, end]);
            WaterLayer::new(
                CellPairs::open(file("admin_cells.bin"), file("admin_entries.bin"), cell_pairs::ADMIN, DEFAULT_ADMIN_CELL_LEVEL).unwrap(),
                file("admin_polygons.bin"),
                AdminGeometry::new(file("admin_rings.bin"), file("arc_index.bin"), file("arc_vertices.bin")),
                strings,
                WaterMeta::from_json(std::str::from_utf8(&files["water_meta.json"]).unwrap()).unwrap(),
                DEFAULT_ADMIN_CELL_LEVEL,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fixture::*;
    use super::*;

    /// The water at the point, as JSON.
    fn water(layer: &WaterLayer, lat: f64, lng: f64) -> serde_json::Value {
        serde_json::to_value(layer.at(lat, lng)).unwrap()
    }

    fn named(name: &str, class: &str, r#type: &str) -> serde_json::Value {
        serde_json::json!({"name": name, "class": class, "type": r#type})
    }

    #[test]
    fn the_water_holding_a_point_answers_its_name_class_and_type() {
        let layer = WaterFixture::new().area("Le Léman", LAKE, (46.2, 6.1, 46.5, 6.9), &[]).layer();

        assert_eq!(water(&layer, 46.4, 6.5), named("Le Léman", "water", "lake"));
        assert_eq!(serde_json::to_string(&layer.at(46.4, 6.5)).unwrap(), r#"{"name":"Le Léman","class":"water","type":"lake"}"#);
        for (lat, lng) in [(46.6, 6.5), (46.4, 7.0), (10.0, 20.0)] {
            assert_eq!(layer.at(lat, lng), None, "({lat}, {lng})");
        }
    }

    #[test]
    fn a_point_off_the_globe_is_in_no_water() {
        // A sea over the cell (10, 20), onto which S2 would wrap (10, 380).
        let layer = WaterFixture::new().area("Testsee", SEA, (9.0, 19.0, 11.0, 21.0), &[]).layer();

        assert_eq!(water(&layer, 10.0, 20.0), named("Testsee", "place", "sea"));
        for (lat, lng) in [(10.0, 380.0), (90.5, 20.0), (f64::NAN, 20.0), (10.0, f64::INFINITY)] {
            assert_eq!(layer.at(lat, lng), None, "({lat}, {lng})");
        }
    }

    #[test]
    fn the_smallest_area_holding_the_point_answers() {
        // A lake in a wetland in a bay: Nominatim's rank order (wetland and bay 22 above the
        // lake's 20) would give the wetland.
        let layer = WaterFixture::new()
            .area("Bay", BAY, (0.0, 0.0, 3.0, 3.0), &[])
            .area("Wetland", WETLAND, (1.0, 1.0, 2.5, 2.5), &[])
            .area("Lake", LAKE, (1.5, 1.5, 2.0, 2.0), &[])
            .layer();

        assert_eq!(water(&layer, 1.7, 1.7), named("Lake", "water", "lake"));
        assert_eq!(water(&layer, 1.2, 1.2), named("Wetland", "natural", "wetland"));
        assert_eq!(water(&layer, 0.5, 0.5), named("Bay", "natural", "bay"));
    }

    #[test]
    fn a_point_in_a_hole_is_not_in_its_polygon() {
        // An island of the lake (a hole), in which a pond lies; the bay holds the lake.
        let island = (1.2, 1.2, 1.8, 1.8);
        let layer = WaterFixture::new()
            .area("Lake", LAKE, (1.0, 1.0, 2.0, 2.0), &[island])
            .area("Pond", POND, (1.4, 1.4, 1.5, 1.5), &[])
            .layer();
        let in_bay = WaterFixture::new()
            .area("Lake", LAKE, (1.0, 1.0, 2.0, 2.0), &[island])
            .area("Bay", BAY, (0.0, 0.0, 3.0, 3.0), &[])
            .layer();

        assert_eq!(water(&layer, 1.1, 1.1), named("Lake", "water", "lake"));
        assert_eq!(layer.at(1.7, 1.7), None);
        assert_eq!(water(&layer, 1.45, 1.45), named("Pond", "water", "pond"));
        assert_eq!(water(&in_bay, 1.7, 1.7), named("Bay", "natural", "bay"));
        assert_eq!(water(&in_bay, 1.1, 1.1), named("Lake", "water", "lake"));
    }

    #[test]
    fn a_polygon_answers_by_its_objects_area() {
        // A piece of a large wetland (its object's 8 square degrees), smaller than the lake
        // around it: the lake, the smaller object, answers there.
        let layer = WaterFixture::new()
            .area("Lake", LAKE, (0.0, 0.0, 1.0, 1.0), &[])
            .piece("Wetland", WETLAND, 8.0, (0.4, 0.4, 0.6, 0.6))
            .piece("Wetland", WETLAND, 8.0, (2.0, 2.0, 2.2, 2.2))
            .layer();

        assert_eq!(water(&layer, 0.5, 0.5), named("Lake", "water", "lake"));
        assert_eq!(water(&layer, 2.1, 2.1), named("Wetland", "natural", "wetland"));
    }

    #[test]
    fn areas_of_one_size_answer_by_rank_then_type_code_then_name() {
        let span = (0.0, 0.0, 1.0, 1.0);
        let answer = |areas: &[(&str, u16)]| {
            let mut fixture = WaterFixture::new();
            for &(name, code) in areas {
                fixture.piece(name, code, 1.0, span);
            }
            water(&fixture.layer(), 0.5, 0.5)
        };

        // The pond (rank 24) above the lake (20); the river (19) last.
        assert_eq!(answer(&[("Lake", LAKE), ("Pond", POND), ("River", RIVER)]), named("Pond", "water", "pond"));
        assert_eq!(answer(&[("River", RIVER), ("Lake", LAKE)]), named("Lake", "water", "lake"));
        // Of one rank (22), the lower code: natural/water (1) before the bay (21) and wetland (23).
        assert_eq!(answer(&[("Wetland", WETLAND), ("Bay", BAY), ("Water", NATURAL_WATER)]), named("Water", "natural", "water"));
        // The Venice lagoon's tie, between objects of one kind: the smaller name.
        for areas in [[("Laguna Sud", WETLAND), ("Laguna di Venezia", WETLAND)], [("Laguna di Venezia", WETLAND), ("Laguna Sud", WETLAND)]] {
            assert_eq!(answer(&areas), named("Laguna Sud", "natural", "wetland"));
        }
    }

    #[test]
    fn a_tombstone_or_a_code_without_a_kind_answers_nothing() {
        let meta = r#"{"types": [{"code": 2, "class": "water", "type": "lake", "rank": 20}]}"#;
        let layer = WaterFixture::new()
            .area("Unlisted", POND, (0.0, 0.0, 0.5, 0.5), &[])
            .area("Lake", LAKE, (0.0, 0.0, 1.0, 1.0), &[])
            .meta(meta)
            .layer();

        assert_eq!(water(&layer, 0.25, 0.25), named("Lake", "water", "lake"));
    }

    #[test]
    fn a_lookup_tests_no_larger_area_once_a_smaller_one_holds_the_point() {
        // A lake in a sea, neither interior at the point: the sea's arc is never read there.
        let fixture = |sea: bool| {
            let mut f = WaterFixture::new();
            if sea {
                f.area("Sea", SEA, (0.0, 0.0, 0.3, 0.3), &[]);
            }
            f.area("Lake", LAKE, (0.1, 0.1, 0.2, 0.2), &[]);
            f.layer()
        };
        let arcs_read = |layer: &WaterLayer, lat: f64, lng: f64| {
            let mut memo = PipMemo::new();
            let got = layer.at_with(lat, lng, &mut memo).map(|w| w.name.into_owned());
            (got, memo.arcs_read())
        };

        assert_eq!(arcs_read(&fixture(true), 0.15, 0.15), arcs_read(&fixture(false), 0.15, 0.15));
        assert_eq!(arcs_read(&fixture(true), 0.15, 0.15).0.as_deref(), Some("Lake"));
        assert_eq!(arcs_read(&fixture(true), 0.25, 0.25).0.as_deref(), Some("Sea"));
    }

    // geocoder/testdata/water_dir holds the files the builder's water layer writes on three days
    // (builder/tests/s2/test_water_layer.cpp): a lake relation with an island, a pond, a river
    // area its waterway names and one none names, a lake of a water=* value of its own, and from
    // day 3 a salt pond of another and a marine sea. Each answers as the builder made it.
    #[test]
    fn the_builders_water_dirs_answer_as_it_wrote_them() {
        let day = |d: &str| {
            let dir = format!("{}/../testdata/water_dir/{d}", env!("CARGO_MANIFEST_DIR"));
            WaterLayer::open(&dir, crate::DEFAULT_ADMIN_CELL_LEVEL).unwrap().unwrap()
        };
        let (one, three) = (day("day1"), day("day3"));
        let at = |layer: &WaterLayer, lat: f64, lng: f64| layer.at(lat, lng).map(|w| [w.name.into_owned(), w.class.to_string(), w.r#type.to_string()]);
        let kind = |name: &str, class: &str, r#type: &str| Some([name, class, r#type].map(str::to_string));

        for layer in [&one, &three] {
            assert_eq!(at(layer, 47.005, 8.002), kind("Test Lake", "water", "lake"));
            assert_eq!(at(layer, 47.005, 8.005), None, "the island");
            assert_eq!(at(layer, 47.02005, 8.00005), kind("Pond", "water", "pond"));
            assert_eq!(at(layer, 47.0305, 8.0005), kind("Test River", "waterway", "river"));
            assert_eq!(at(layer, 47.0405, 8.0005), None, "the river area no waterway names");
            assert_eq!(at(layer, 47.05005, 8.00005), kind("Ganga", "water", "ganga_river"));
        }
        assert_eq!((at(&one, 47.06005, 8.00005), at(&one, 45.001, 8.001)), (None, None));
        assert_eq!(at(&three, 47.06005, 8.00005), kind("Salzweiher", "water", "salt_pond"));
        assert_eq!(at(&three, 45.001, 8.001), kind("North Sea", "place", "sea"));
    }

    #[test]
    fn water_meta_lists_each_code_once() {
        let meta = WaterMeta::from_json(META).unwrap();
        assert_eq!(meta.kind(LAKE).map(|k| (k.class.as_str(), k.r#type.as_str(), k.rank)), Some(("water", "lake", 20)));
        assert_eq!(meta.kind(3), None);

        let twice = r#"{"types": [{"code": 2, "class": "water", "type": "lake", "rank": 20}, {"code": 2, "class": "water", "type": "pond", "rank": 24}]}"#;
        assert_eq!(WaterMeta::from_json(twice).err(), Some("type code 2 listed twice".to_string()));
        assert!(WaterMeta::from_json("{}").is_err());
    }

    #[test]
    fn a_dir_holds_all_of_its_files_or_none() {
        let dir = crate::scratch_dir::ScratchDir::new();
        let water = dir.path().join(WATER_DIR);
        let open = || WaterLayer::open(water.to_str().unwrap(), crate::DEFAULT_ADMIN_CELL_LEVEL).map(|w| w.map(|w| w.at(0.5, 0.5).map(|d| d.name.into_owned())));

        assert_eq!(open(), Ok(None));
        std::fs::create_dir(&water).unwrap();
        assert_eq!(open(), Ok(None));
        WaterFixture::new().area("Lake", LAKE, (0.0, 0.0, 1.0, 1.0), &[]).write(&water);
        assert_eq!(open(), Ok(Some(Some("Lake".to_string()))));
        std::fs::remove_file(water.join("water_meta.json")).unwrap();
        std::fs::remove_file(water.join("arc_index.bin")).unwrap();
        assert_eq!(open(), Err(format!("Partial water dir {}: missing [arc_index.bin, water_meta.json]", water.display())));
        std::fs::write(water.join("water_meta.json"), "{\"types\": 1}").unwrap();
        std::fs::write(water.join("arc_index.bin"), []).unwrap();
        assert!(open().unwrap_err().starts_with(&format!("{}/water_meta.json: ", water.display())));
    }

    #[test]
    fn a_water_path_that_cant_be_read_fails_the_open_naming_it() {
        // Taken for no water, either would leave a dataset dry that holds water: a water dir
        // that is a file (its files can't be looked up, as in a dir that can't be searched),
        // and a name of the files that is a dir.
        let dir = crate::scratch_dir::ScratchDir::new();
        let water = dir.path().join(WATER_DIR);
        let open = || WaterLayer::open(water.to_str().unwrap(), crate::DEFAULT_ADMIN_CELL_LEVEL).map(|w| w.is_some());

        std::fs::write(&water, "not a dir").unwrap();
        let got = open();
        assert!(got.as_ref().is_err_and(|e| e.starts_with(&format!("{}/admin_cells.bin: ", water.display()))), "{got:?}");
        std::fs::remove_file(&water).unwrap();
        WaterFixture::new().area("Lake", LAKE, (0.0, 0.0, 1.0, 1.0), &[]).write(&water);
        for name in ["admin_cells.bin", "water_meta.json"] {
            std::fs::remove_file(water.join(name)).unwrap();
            std::fs::create_dir(water.join(name)).unwrap();
        }
        assert_eq!(open(), Err(format!("{}/admin_cells.bin: not a file", water.display())));
    }
}
