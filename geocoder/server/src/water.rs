//! A dataset's water layer: the named bodies of water of one of the builder's water dirs
//! (`{region}/water/{all,major}/{simplified,exact}/`, builder/src/water_layer.h), which a client
//! puts in `<dataset>/water/` (PATCHING.md). Its files have the admin geometry's names and
//! formats but are a set of their own, pointing into nothing else:
//! - admin_polygons.bin and admin_rings.bin: a record per polygon, an OSM object's outer ring
//!   with its holes or a piece of it cut along a grid, carrying the object's name, its type code
//!   in the country code's place, its area (that of all its outer rings, their holes taken off,
//!   in square degrees) and in byte 14 its flags (WEAK_NAME);
//! - arc_index.bin and arc_vertices.bin: the arcs of those rings;
//! - water_cut_arcs.bin: which arc slots are cuts, a bit per slot (CutArcs);
//! - admin_cells.bin and admin_entries.bin: each L10 cell's polygons, flagged interior where
//!   their rings hold the whole cell;
//! - strings_core.bin and strings_layout.json: the names, in a core tier of their own;
//! - water_meta.json: each type code's class, type and Nominatim rank.
//!
//! The water at a point is the smallest object whose rings hold it, holes honoured, then the one
//! of the higher rank, the lower type code and the smaller name. It answers beside the address,
//! never changing it: an extension of Nominatim's answer (NOMINATIM_REFERENCE.md). Where the
//! location line names it in its landmark's place, it says so (`WaterDetail::takes_landmark`).

use crate::{
    cell_id_at_level, cell_neighbors_at_level, coords_in_range, mmap_file, poi_category_to_osm_class, polygon_record,
    AdminGeometry, AdminPoint, AdminPolygon, CellPairs, FileBytes, PipMemo, PointE7, StringPool, ADMIN_POLYGON_RECORD_BYTES,
    ID_MASK, INTERIOR_FLAG, NO_DATA,
};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::cmp::Ordering;
use unicode_normalization::{char::canonical_combining_class, UnicodeNormalization};

/// The dir of a dataset that holds its water layer.
pub const WATER_DIR: &str = "water";

/// The files a water layer loads from, by name: a dir holds all of them or none.
pub const WATER_FILES: [&str; 10] = [
    "admin_cells.bin",
    "admin_entries.bin",
    "admin_polygons.bin",
    "admin_rings.bin",
    "arc_index.bin",
    "arc_vertices.bin",
    "strings_core.bin",
    "strings_layout.json",
    "water_cut_arcs.bin",
    "water_meta.json",
];

/// A water record's flag, in byte 14 (a boundary's level and place type): its name is that of a
/// short waterway that runs only a short way into it, an unnamed river or canal area that a
/// creek or tributary named (builder/src/water_names.h), which the location line takes for no
/// name of note.
pub const WEAK_NAME: u8 = 1;

/// How far from the point the shore of the water the location line names must lie at least, in
/// metres: past a phone's error in town, the simplified tier's tolerance and a quay at the
/// water's edge.
pub const LANDMARK_SHORE_M: f64 = 40.0;

/// Square kilometres in a square degree at the equator.
const KM2_PER_DEG2: f64 = 111.319_49 * 111.319_49;

/// A kind's group, as the location line reads it (`WaterDetail::takes_landmark`): the named
/// natural water people name, and the kinds it never names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WaterGroup {
    /// The seas and oceans: place/sea and place/ocean.
    Sea,
    /// Bays, straits, coves, fjords, sounds, seas, lagoons and harbours.
    Coastal,
    /// Lakes, reservoirs, ponds, oxbows and natural/water.
    Still,
    /// River and canal areas.
    River,
    Glacier,
    Wetland,
    /// Every other kind: basins, wastewater, fountains, fishponds, locks, salt pools, moats and
    /// every water=* value of its own. The water field names them; the line never does.
    #[default]
    Other,
}

impl WaterGroup {
    /// The group of Nominatim's class and type.
    pub fn of(class: &str, r#type: &str) -> WaterGroup {
        match (class, r#type) {
            ("place", "sea" | "ocean") => WaterGroup::Sea,
            ("natural", "bay" | "strait")
            | ("water", "bay" | "cove" | "fjord" | "sound" | "strait" | "sea" | "lagoon" | "harbour") => WaterGroup::Coastal,
            ("water", "lake" | "reservoir" | "pond" | "oxbow") | ("landuse", "reservoir") | ("natural", "water") => WaterGroup::Still,
            ("waterway" | "water", "river" | "canal") => WaterGroup::River,
            ("natural", "glacier") => WaterGroup::Glacier,
            ("natural", "wetland") => WaterGroup::Wetland,
            _ => WaterGroup::Other,
        }
    }
}

/// How well a body of water is known, from its group and its area: a minor one never names the
/// line, a notable one or larger takes the place of a park that holds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Prominence {
    Minor,
    Local,
    Notable,
    Major,
}

impl Prominence {
    /// A body of `group` of `km2` square kilometres: under 1 ha still water and wetland are minor;
    /// coastal water, glaciers and still water notable from 0.1 km² and major from 10 km²; a
    /// river or canal area notable at any size, major from 10 km²; a sea or ocean local, as is a
    /// wetland from 1 ha. Every other kind is minor.
    fn of(group: WaterGroup, km2: f64) -> Prominence {
        let by_area = |floor: Prominence| {
            if km2 >= 10.0 {
                Prominence::Major
            } else if km2 >= 0.1 {
                Prominence::Notable
            } else {
                floor
            }
        };
        match group {
            WaterGroup::Sea => Prominence::Local,
            WaterGroup::Coastal | WaterGroup::Glacier => by_area(Prominence::Local),
            WaterGroup::Still if km2 < 0.01 => Prominence::Minor,
            WaterGroup::Still => by_area(Prominence::Local),
            WaterGroup::River => by_area(Prominence::Notable),
            WaterGroup::Wetland if km2 < 0.01 => Prominence::Minor,
            WaterGroup::Wetland => Prominence::Local,
            WaterGroup::Other => Prominence::Minor,
        }
    }
}

/// A type code's kind, as water_meta.json lists it: Nominatim's class and type, and its
/// rank_search (settings/address-levels.json); and its group, from the class and type.
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct WaterKind {
    pub code: u16,
    pub class: String,
    pub r#type: String,
    pub rank: u8,
    #[serde(skip)]
    pub group: WaterGroup,
}

/// The kinds of a water dir's type codes (water_meta.json), by code.
#[derive(Debug, Default)]
pub struct WaterMeta {
    kinds: Vec<WaterKind>,
}

impl WaterMeta {
    /// The kinds water_meta.json's text lists, each with its group; an error for text that isn't
    /// such a list, or that gives a code twice.
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
        for k in &mut kinds {
            k.group = WaterGroup::of(&k.class, &k.r#type);
        }
        Ok(WaterMeta { kinds })
    }

    /// The kind of type code `code`; none for a code it doesn't list.
    pub fn kind(&self, code: u16) -> Option<&WaterKind> {
        self.kinds.binary_search_by_key(&code, |k| k.code).ok().map(|i| &self.kinds[i])
    }
}

/// water_cut_arcs.bin: which arc slots of a water dir are cuts, an arc only one object's polygons
/// use, two of them along it (the cut between two pieces of a lake split along the grid, or two
/// tiles of a marine area), with that object's water on both sides. Bit s % 32 of the
/// little-endian u32 at 4 × (s / 32) for slot s: as many words as the slots take.
pub struct CutArcs {
    words: FileBytes,
}

impl CutArcs {
    /// The cut arcs of `words` beside an arc index of `slots` slots; an error unless it holds just
    /// the words those take.
    pub fn new(words: FileBytes, slots: usize) -> Result<CutArcs, String> {
        let want = slots.div_ceil(32) as u64 * 4;
        if words.len() != want {
            return Err(format!("{} bytes for {slots} arc slots, not {want}", words.len()));
        }
        Ok(CutArcs { words })
    }

    /// Whether arc slot `slot` is a cut.
    pub fn is_cut(&self, slot: u32) -> bool {
        let at = (slot / 32) as usize * 4;
        self.words.as_slice().get(at..at + 4).is_some_and(|w| u32::from_le_bytes([w[0], w[1], w[2], w[3]]) >> (slot % 32) & 1 == 1)
    }
}

/// The water an answer names: the object's name, and its kind's class and type, which are the
/// `category` and `type` Nominatim gives it (`type` the key its `address` names it by); and,
/// last, whether the location line names it in its landmark's place (`takes_landmark`), there
/// only where it does.
#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
pub struct WaterDetail<'a> {
    pub name: Cow<'a, str>,
    pub class: &'a str,
    pub r#type: &'a str,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub landmark: bool,
    /// Where its object comes in the order in which water answers, which no answer shows.
    #[serde(skip)]
    precedence: Precedence,
    /// The polygon that answers, for the location line.
    #[serde(skip)]
    source: Source<'a>,
}

impl WaterDetail<'_> {
    /// The order in which water answers, of one water dir or of several (the continents that
    /// hold a point): by Precedence, then the smaller name.
    pub(crate) fn order(&self, other: &Self) -> Ordering {
        self.precedence.cmp(&other.precedence).then_with(|| self.name.cmp(&other.name))
    }

    /// Whether the location line beside this water at (lat, lng), whose address `line` gives,
    /// names it in its landmark's place. Each rule says no in turn, the cheapest first:
    /// 1. a kind the line never names (WaterGroup::Other);
    /// 2. a catalogue name: no letter in it, or a number after "No", "Nr", "#", "№", "lake",
    ///    "pond", "pool", "tank", "basin", "reservoir" or "glacier" alone ("Lake 938");
    /// 3. a minor body (Prominence::of at the point's latitude), or one of a weak name
    ///    (WEAK_NAME);
    /// 4. a name the line has: the city, state or country it shows, folded (`fold`), or one the
    ///    landmark names (`same_name`);
    /// 5. a landmark that stays: every one but a nature reserve, national park or protected area
    ///    (a UNESCO zone among them), which gives way to a notable body smaller than its polygon,
    ///    and a natural feature, which gives way to a smaller body;
    /// 6. a shore of its object within LANDMARK_SHORE_M of the point, in the tier's own geometry
    ///    (`WaterLayer::shore_within`).
    pub(crate) fn takes_landmark(&self, lat: f64, lng: f64, line: &LocationLine<'_>) -> bool {
        let Source { layer, poly, group, weak } = self.source;
        if group == WaterGroup::Other || catalogue_name(&self.name) {
            return false;
        }
        let km2 = f64::from(self.precedence.area) * KM2_PER_DEG2 * lat.to_radians().cos();
        let prominence = if weak { Prominence::Minor } else { Prominence::of(group, km2) };
        if prominence == Prominence::Minor {
            return false;
        }
        let name = fold(&self.name);
        if line.shown.iter().flatten().any(|s| fold(s) == name) {
            return false;
        }
        if let Some((landmark, poi)) = line.landmark {
            if same_name(landmark, &self.name) {
                return false;
            }
            let smaller = poi.is_some_and(|p| f64::from(self.precedence.area) < p.area);
            let gives_way = match poi.map(|p| p.category) {
                Some(61 | 130 | 131) => prominence >= Prominence::Notable && smaller,
                Some(c) if poi_category_to_osm_class(c) == Some("natural") => smaller,
                _ => false,
            };
            if !gives_way {
                return false;
            }
        }
        !layer.shore_within(&poly, group == WaterGroup::Sea, lat, lng, LANDMARK_SHORE_M)
    }
}

/// What the location line shows beside the water: the city, state and country Immich takes from
/// the answer's address (none without a country), and its landmark with that POI's category and
/// polygon area where known.
#[derive(Debug, Default)]
pub(crate) struct LocationLine<'s> {
    pub shown: [Option<&'s str>; 3],
    pub landmark: Option<(&'s str, Option<LandmarkPoi>)>,
}

/// The POI an answer's landmark names: its category (builder/src/types.h) and its polygon's
/// area, in square degrees as a water record's.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LandmarkPoi {
    pub category: u8,
    pub area: f64,
}

/// A name as the line compares names: its compatibility decomposition (NFKD) without its
/// combining marks, in lower case, letters and digits alone ("Lágoa D'água" and "Lagoa D'Agua"
/// alike).
fn fold(name: &str) -> String {
    name.nfkd()
        .filter(|&c| canonical_combining_class(c) == 0)
        .flat_map(char::to_lowercase)
        .filter(|c| c.is_alphanumeric())
        .collect()
}

/// Whether two names name one thing: folded (`fold`), equal, or the shorter, of 4 characters or
/// more, inside the longer ("Lake Titicaca (Peruvian sector)" and "Lake Titicaca").
fn same_name(a: &str, b: &str) -> bool {
    let (a, b) = (fold(a), fold(b));
    if a.is_empty() || b.is_empty() {
        return false;
    }
    let (short, long) = if a.chars().count() <= b.chars().count() { (&a, &b) } else { (&b, &a) };
    short == long || (short.chars().count() >= 4 && long.contains(short.as_str()))
}

/// Whether a name is a catalogue number, not a name: no letter in it ("49,0"), or "No", "Nr",
/// "#" or "№" ("No." and "Nr." too), or "lake", "pond", "pool", "tank", "basin", "reservoir" or
/// "glacier", in any case, then a number and at most one letter or digit after it, alone
/// ("№30", "No. 116", "Lake 938", "Glacier 5b").
fn catalogue_name(name: &str) -> bool {
    let name = name.trim();
    if !name.chars().any(char::is_alphabetic) {
        return true;
    }
    let lower = name.to_lowercase();
    let words = ["№", "no.", "no", "nr.", "nr", "#", "lake", "pond", "pool", "tank", "basin", "reservoir", "glacier"];
    words.iter().any(|w| {
        lower.strip_prefix(w).is_some_and(|rest| {
            let rest = rest.trim_start();
            let digits = rest.chars().take_while(|c| c.is_numeric()).count();
            let after: Vec<char> = rest.chars().skip(digits).collect();
            digits > 0 && (after.is_empty() || (after.len() == 1 && (after[0].is_alphanumeric() || after[0] == '_')))
        })
    })
}

/// The polygon a water answer comes from: its layer and record, its kind's group and whether its
/// name is weak.
#[derive(Clone, Copy)]
struct Source<'a> {
    layer: &'a WaterLayer,
    poly: AdminPolygon,
    group: WaterGroup,
    weak: bool,
}

impl PartialEq for Source<'_> {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self.layer, other.layer) && same_polygon(&self.poly, &other.poly)
    }
}

impl Eq for Source<'_> {}

impl std::fmt::Debug for Source<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Source {{ ring_offset: {}, group: {:?}, weak: {} }}", self.poly.ring_offset, self.group, self.weak)
    }
}

/// An object's place in the order in which water answers, but for its name: the smaller area,
/// then the higher rank, then the lower type code.
#[derive(Debug, Clone, Copy)]
struct Precedence {
    area: f32,
    rank: u8,
    code: u16,
}

impl Ord for Precedence {
    fn cmp(&self, other: &Self) -> Ordering {
        self.area.total_cmp(&other.area).then(other.rank.cmp(&self.rank)).then(self.code.cmp(&other.code))
    }
}

impl PartialOrd for Precedence {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Precedence {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Precedence {}

/// A polygon of the point's cell that may hold it, with its kind.
struct Candidate<'a> {
    poly: AdminPolygon,
    interior: bool,
    kind: &'a WaterKind,
}

impl Candidate<'_> {
    /// Its object's place in the order in which water answers.
    fn precedence(&self) -> Precedence {
        Precedence { area: self.poly.area, rank: self.kind.rank, code: self.poly.country_code }
    }

    /// The order in which candidates answer, but for the name.
    fn order(&self, other: &Self) -> Ordering {
        self.precedence().cmp(&other.precedence())
    }
}

/// A water dir, mapped.
pub struct WaterLayer {
    index: CellPairs,
    polygons: FileBytes,
    geometry: AdminGeometry,
    cuts: CutArcs,
    strings: StringPool,
    meta: WaterMeta,
    cell_level: u64,
}

impl WaterLayer {
    /// The layer of these files; `index` is read at `cell_level`.
    pub fn new(index: CellPairs, polygons: FileBytes, geometry: AdminGeometry, cuts: CutArcs, strings: StringPool, meta: WaterMeta,
               cell_level: u64) -> Self {
        WaterLayer { index, polygons, geometry, cuts, strings, meta, cell_level }
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
        let geometry = AdminGeometry::load(dir)?;
        let cuts_path = format!("{dir}/water_cut_arcs.bin");
        let cuts = CutArcs::new(mmap_file(&cuts_path)?, geometry.arc_slots()).map_err(|e| format!("{cuts_path}: {e}"))?;
        Ok(Some(WaterLayer {
            index,
            polygons: mmap_file(&format!("{dir}/admin_polygons.bin"))?,
            geometry,
            cuts,
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
        best.map(|b| WaterDetail {
            name: self.strings.get(b.poly.name_id),
            class: &b.kind.class,
            r#type: &b.kind.r#type,
            landmark: false,
            precedence: b.precedence(),
            source: Source {
                layer: self,
                poly: b.poly,
                group: b.kind.group,
                weak: b.poly.place_type_override & WEAK_NAME != 0,
            },
        })
    }

    /// Whether a shore of the object of `poly` (a polygon holding (lat, lng)) lies nearer than
    /// `metres` to the point: an edge of its rings, or of the object's polygons its water runs on
    /// into there, but one along which it runs on, which is no shore:
    /// - a cut arc's (CutArcs), which the polygon on its other side uses too;
    /// - for a sea or ocean (`marine`), one along a line of a tenth of a degree of latitude or
    ///   longitude with sea either side of it (`seas_beside`), where the marine areas are cut
    ///   into tiles, their sides along it not always of the same vertices.
    ///
    /// Only the polygons the water so runs on into within the distance are read beyond `poly`:
    /// the object's polygons across those tile lines, and those of the cells around the point
    /// (`object_near`) that use a cut arc met.
    fn shore_within(&self, poly: &AdminPolygon, marine: bool, lat: f64, lng: f64, metres: f64) -> bool {
        // A tenth of a degree in e7 units.
        const TILE_E7: i64 = 1_000_000;
        let p = AdminPoint::new(lat, lng);
        let tile_line = |a: PointE7, b: PointE7| (a.lat == b.lat && a.lat % TILE_E7 == 0) || (a.lng == b.lng && a.lng % TILE_E7 == 0);
        // The polygons walked and to walk, the cut arcs met, and the object's polygons near.
        let (mut walked, mut todo, mut cuts) = (Vec::new(), vec![*poly], Vec::new());
        let mut near: Option<Vec<AdminPolygon>> = None;
        while let Some(q) = todo.pop() {
            walked.push(q);
            let met = cuts.len();
            let mut across = Vec::new();
            let shore = self.geometry.edge_within(&q, p, metres, |slot, a, b| {
                if self.cuts.is_cut(slot) {
                    if !cuts.contains(&slot) {
                        cuts.push(slot);
                    }
                    return false;
                }
                if !(marine && tile_line(a, b)) {
                    return true;
                }
                let Some(seas) = self.seas_beside(p.e7(), a, b) else { return true };
                across.extend(seas.into_iter().filter(|sea| same_object(sea, poly)));
                false
            });
            if shore {
                return true;
            }
            if cuts.len() > met {
                let near = near.get_or_insert_with(|| self.object_near(poly, p));
                across.extend(near.iter().filter(|r| self.geometry.uses_arc_near(r, p, metres, &cuts[met..])));
            }
            for r in across {
                if !walked.iter().chain(&todo).any(|w| same_polygon(w, &r)) {
                    todo.push(r);
                }
            }
        }
        false
    }

    /// The polygons of the seas or oceans either side of the edge a-b, which runs along a
    /// parallel or a meridian: those that hold the points an e7 unit off the line each way, at
    /// the point of the edge nearest `p` (inside its ends); none unless both sides are sea.
    fn seas_beside(&self, p: PointE7, a: PointE7, b: PointE7) -> Option<[AdminPolygon; 2]> {
        let along = |at: i64, x: i64, y: i64| if (x - y).abs() >= 2 { at.clamp(x.min(y) + 1, x.max(y) - 1) } else { x };
        let sides = if a.lat == b.lat {
            let lng = along(p.lng, a.lng, b.lng);
            [PointE7 { lat: a.lat - 1, lng }, PointE7 { lat: a.lat + 1, lng }]
        } else {
            let lat = along(p.lat, a.lat, b.lat);
            [PointE7 { lat, lng: a.lng - 1 }, PointE7 { lat, lng: a.lng + 1 }]
        };
        Some([self.sea_at(sides[0])?, self.sea_at(sides[1])?])
    }

    /// The polygon of a sea or ocean of the layer that holds the point, if any.
    fn sea_at(&self, point: PointE7) -> Option<AdminPolygon> {
        let (lat, lng) = point.degrees();
        let point = AdminPoint::new(lat, lng);
        let mut memo = PipMemo::new();
        self.index.with_ids(cell_id_at_level(lat, lng, self.cell_level), |ids| {
            ids.iter().find_map(|&id| {
                let poly = polygon_record(&self.polygons, self.geometry.rings_len(), id & ID_MASK)?;
                let sea = poly.name_id != NO_DATA
                    && self.geometry.has_rings(&poly)
                    && self.meta.kind(poly.country_code).is_some_and(|k| k.group == WaterGroup::Sea)
                    && (id & INTERIOR_FLAG != 0 || self.geometry.contains(&poly, point, &mut memo));
                sea.then_some(poly)
            })
        })
    }

    /// The polygons of the object of `poly` (its name, type code and area) that the cells around
    /// `p` list: those of the cell holding it and of the cells beside that one, which hold every
    /// point within a cell's width of it. `poly` among them.
    fn object_near(&self, poly: &AdminPolygon, p: AdminPoint) -> Vec<AdminPolygon> {
        let (lat, lng) = p.degrees();
        let cell = cell_id_at_level(lat, lng, self.cell_level);
        let mut ids = Vec::new();
        for c in std::iter::once(cell).chain(cell_neighbors_at_level(cell, self.cell_level)) {
            self.index.with_ids(c, |listed| ids.extend(listed.iter().map(|&id| id & ID_MASK)));
        }
        ids.sort_unstable();
        ids.dedup();
        ids.into_iter()
            .filter_map(|id| polygon_record(&self.polygons, self.geometry.rings_len(), id))
            .filter(|q| same_object(q, poly) && self.geometry.has_rings(q))
            .collect()
    }
}

/// Whether two records are one polygon: their ring blocks the same.
fn same_polygon(a: &AdminPolygon, b: &AdminPolygon) -> bool {
    (a.ring_offset, a.ring_bytes) == (b.ring_offset, b.ring_bytes)
}

/// Whether two records are polygons of one object: of its name, type code and area.
fn same_object(a: &AdminPolygon, b: &AdminPolygon) -> bool {
    (a.name_id, a.country_code, a.area.to_bits()) == (b.name_id, b.country_code, b.area.to_bits())
}

/// Water dirs as the builder writes them, for the tests: rectangular areas with rectangular
/// holes, each ring one arc, and objects of two pieces either side of a cut along a meridian.
#[cfg(test)]
pub(crate) mod fixture {
    use super::*;
    use crate::arc_fixture::{self, ArcFixture, Ref};
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
  {"code": 10, "class": "water", "type": "harbour", "rank": 22},
  {"code": 12, "class": "water", "type": "fishpond", "rank": 22},
  {"code": 21, "class": "natural", "type": "bay", "rank": 22},
  {"code": 23, "class": "natural", "type": "wetland", "rank": 22},
  {"code": 24, "class": "natural", "type": "glacier", "rank": 22},
  {"code": 25, "class": "place", "type": "sea", "rank": 2},
  {"code": 26, "class": "place", "type": "ocean", "rank": 22},
  {"code": 29, "class": "waterway", "type": "river", "rank": 19},
  {"code": 30, "class": "waterway", "type": "canal", "rank": 20},
  {"code": 32768, "class": "water", "type": "ganga_river", "rank": 22}
]}
"#;
    pub const NATURAL_WATER: u16 = 1;
    pub const LAKE: u16 = 2;
    pub const POND: u16 = 4;
    pub const HARBOUR: u16 = 10;
    pub const FISHPOND: u16 = 12;
    pub const BAY: u16 = 21;
    pub const WETLAND: u16 = 23;
    pub const GLACIER: u16 = 24;
    pub const SEA: u16 = 25;
    pub const OCEAN: u16 = 26;
    pub const RIVER: u16 = 29;
    pub const CANAL: u16 = 30;
    pub const VERBATIM: u16 = 32768;

    /// How an area's polygons lie: one, `outer` with its holes; or two pieces either side of a
    /// cut along a meridian (its south end, then its north end), sharing the arc along it, which
    /// water_cut_arcs.bin marks where `marked`. `sides` hold the west piece's other vertices from
    /// the cut's north end round to its south end and the east piece's from its south end round
    /// to its north end, `holes` each piece's holes; a cell inside a piece is interior only
    /// where the piece is a rectangle (`rect`).
    enum Shape {
        Whole { outer: Span, holes: Vec<Span> },
        Pieces { cut: [(f64, f64); 2], sides: [Vec<(f64, f64)>; 2], holes: [Vec<Span>; 2], marked: bool, rect: bool },
    }

    struct Area {
        name: String,
        code: u16,
        area: Option<f32>,
        flags: u8,
        shape: Shape,
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

    fn line(points: &[(f64, f64)]) -> Vec<crate::PointE7> {
        points.iter().map(|&(lat, lng)| arc_fixture::e7(lat, lng)).collect()
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

    /// The span of the points.
    fn span_of(points: &[(f64, f64)]) -> Span {
        let start = (f64::MAX, f64::MAX, f64::MIN, f64::MIN);
        points.iter().fold(start, |(s, w, n, e), &(lat, lng)| (s.min(lat), w.min(lng), n.max(lat), e.max(lng)))
    }

    impl WaterFixture {
        pub fn new() -> Self {
            Self::default()
        }

        /// An area of its own: its record's area that of its rings.
        pub fn area(&mut self, name: &str, code: u16, outer: Span, holes: &[Span]) -> &mut Self {
            self.areas.push(Area { name: name.into(), code, area: None, flags: 0, shape: Shape::Whole { outer, holes: holes.to_vec() } });
            self
        }

        /// A polygon of an object of `area` square degrees (a piece of one the builder cut, or
        /// one of its outer rings): its record's area the object's.
        pub fn piece(&mut self, name: &str, code: u16, area: f32, outer: Span) -> &mut Self {
            self.areas.push(Area { name: name.into(), code, area: Some(area), flags: 0, shape: Shape::Whole { outer, holes: Vec::new() } });
            self
        }

        /// An object of two polygons, `outer` cut along the meridian `at`, the west piece first,
        /// each record's area that of the whole; the arc along the cut is marked a cut where
        /// `marked`.
        pub fn split(&mut self, name: &str, code: u16, (s, w, n, e): Span, at: f64, marked: bool) -> &mut Self {
            let sides = [vec![(n, w), (s, w)], vec![(s, e), (n, e)]];
            let shape = Shape::Pieces { cut: [(s, at), (n, at)], sides, holes: [Vec::new(), Vec::new()], marked, rect: true };
            self.areas.push(Area { name: name.into(), code, area: None, flags: 0, shape });
            self
        }

        /// An object of two polygons either side of the cut along a meridian from `cut[0]` north
        /// to `cut[1]`, the west piece first: `west` its other vertices from the cut's north end
        /// round to its south end, `east` the east piece's from its south end round to its north
        /// end; each record's area that of the whole, and the cut marked where `marked`.
        pub fn pieces(&mut self, name: &str, code: u16, cut: [(f64, f64); 2], west: &[(f64, f64)], east: &[(f64, f64)], marked: bool)
                      -> &mut Self {
            assert_eq!(cut[0].1, cut[1].1, "a cut along a meridian");
            let shape = Shape::Pieces { cut, sides: [west.to_vec(), east.to_vec()], holes: [Vec::new(), Vec::new()], marked, rect: false };
            self.areas.push(Area { name: name.into(), code, area: None, flags: 0, shape });
            self
        }

        /// A hole in the last area: in the piece on its side of the cut, where it has two.
        pub fn island(&mut self, hole: Span) -> &mut Self {
            match &mut self.areas.last_mut().expect("an area for the island").shape {
                Shape::Whole { holes, .. } => holes.push(hole),
                Shape::Pieces { cut, holes, .. } => {
                    let (_, w, _, e) = hole;
                    assert!(e < cut[0].1 || w > cut[0].1, "an island on one side of the cut");
                    holes[usize::from(w > cut[0].1)].push(hole);
                }
            }
            self
        }

        /// The last area's records carry `flags` in byte 14.
        pub fn flags(&mut self, flags: u8) -> &mut Self {
            self.areas.last_mut().expect("an area to flag").flags = flags;
            self
        }

        /// water_meta.json's text, in place of META.
        pub fn meta(&mut self, text: &str) -> &mut Self {
            self.meta = Some(text.into());
            self
        }

        /// The files, by name. Each polygon is listed in every cell meeting its outer ring's
        /// span, flagged interior where the cell lies inside a rectangle of it and meets none of
        /// its holes; the names are the strings of a core tier.
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
            let mut cut = Vec::new();
            let mut listed: BTreeMap<u64, Vec<u32>> = BTreeMap::new();
            let mut list = |id: u32, span: Span, holes: &[Span], rect: bool| {
                for cell in cells(span) {
                    let interior = rect && inside(cell, span) && !holes.iter().any(|&h| meets(cell, h));
                    listed.entry(cell).or_default().push(id | if interior { INTERIOR_FLAG } else { 0 });
                }
            };
            for a in &self.areas {
                let record = |area: f32| AdminPolygon {
                    country_code: a.code,
                    place_type_override: a.flags,
                    ..arc_fixture::record(offsets[&a.name], 0, area)
                };
                match &a.shape {
                    Shape::Whole { outer, holes } => {
                        let rings: Vec<Vec<crate::PointE7>> = std::iter::once(*outer).chain(holes.iter().copied()).map(ring).collect();
                        let id = arcs.ring_polygon(record(a.area.unwrap_or_else(|| arc_fixture::exact_area(&rings))), 0, &rings);
                        list(id, *outer, holes, true);
                    }
                    Shape::Pieces { cut: [south, north], sides, holes, marked, rect } => {
                        // Each piece's outer ring, from the cut's end it leaves round to the other.
                        let outers = [[&[*north][..], &sides[0], &[*south]].concat(), [&[*south][..], &sides[1], &[*north]].concat()];
                        let rings = |k: usize| -> Vec<Vec<crate::PointE7>> {
                            let closed = [&outers[k][..], &outers[k][..1]].concat();
                            std::iter::once(line(&closed)).chain(holes[k].iter().map(|&h| ring(h))).collect()
                        };
                        let area = a.area.unwrap_or_else(|| arc_fixture::exact_area(&rings(0)) + arc_fixture::exact_area(&rings(1)));
                        let along = arcs.arc(0, &line(&[*south, *north]));
                        if *marked {
                            cut.push(along);
                        }
                        for (k, way) in [(0, Ref::fwd(along)), (1, Ref::rev(along))] {
                            let side = arcs.arc(0, &line(&outers[k]));
                            let mut refs = vec![vec![Ref::fwd(side), way]];
                            refs.extend(holes[k].iter().map(|&h| vec![Ref::fwd(arcs.arc(0, &ring(h)))]));
                            let id = arcs.polygon(record(area), &refs);
                            list(id, span_of(&outers[k]), &holes[k], *rect);
                        }
                    }
                }
            }
            let (cells, entries) = cell_pairs::write(&listed.into_iter().collect::<Vec<_>>(), cell_pairs::ADMIN);
            let end = pool.len();
            let tiers: Vec<String> = (0..STR_TIER_COUNT).map(|t| format!("{{\"start\": {}, \"end\": {end}}}", if t == 0 { 0 } else { end })).collect();
            let mut words = vec![0u32; (arcs.arc_index().len() / 4).div_ceil(32)];
            for slot in cut {
                words[slot as usize / 32] |= 1 << (slot % 32);
            }
            let mut files: Vec<(&'static str, Vec<u8>)> = arcs.files().into();
            files.extend([
                ("admin_cells.bin", cells),
                ("admin_entries.bin", entries),
                ("strings_core.bin", strings::encode(&pool)),
                ("strings_layout.json", format!("{{\"tiers\": [{}]}}", tiers.join(", ")).into_bytes()),
                ("water_cut_arcs.bin", words.iter().flat_map(|w| w.to_le_bytes()).collect()),
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
            let geometry = AdminGeometry::new(file("admin_rings.bin"), file("arc_index.bin"), file("arc_vertices.bin"));
            let cuts = CutArcs::new(file("water_cut_arcs.bin"), geometry.arc_slots()).unwrap();
            WaterLayer::new(
                CellPairs::open(file("admin_cells.bin"), file("admin_entries.bin"), cell_pairs::ADMIN, DEFAULT_ADMIN_CELL_LEVEL).unwrap(),
                file("admin_polygons.bin"),
                geometry,
                cuts,
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
    fn a_code_without_a_kind_answers_nothing() {
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
        // A lake in a sea, neither interior in the cell of (0.15, 0.15), which the sea's east
        // edge crosses: the sea alone is tested against its arc there, beside the lake never.
        let fixture = |sea: bool, lake: bool| {
            let mut f = WaterFixture::new();
            if sea {
                f.area("Sea", SEA, (0.0, 0.0, 0.3, 0.21), &[]);
            }
            if lake {
                f.area("Lake", LAKE, (0.1, 0.1, 0.2, 0.2), &[]);
            }
            f.layer()
        };
        let arcs_read = |layer: &WaterLayer, lat: f64, lng: f64| {
            let mut memo = PipMemo::new();
            let got = layer.at_with(lat, lng, &mut memo).map(|w| w.name.into_owned());
            (got, memo.arcs_read())
        };

        let sea_alone = arcs_read(&fixture(true, false), 0.15, 0.15);
        assert!(sea_alone.0.as_deref() == Some("Sea") && sea_alone.1 > 0, "{sea_alone:?}");
        assert_eq!(arcs_read(&fixture(true, true), 0.15, 0.15), arcs_read(&fixture(false, true), 0.15, 0.15));
        assert_eq!(arcs_read(&fixture(true, true), 0.15, 0.15).0.as_deref(), Some("Lake"));
        assert_eq!(arcs_read(&fixture(true, true), 0.25, 0.18).0.as_deref(), Some("Sea"));
    }

    // geocoder/testdata/water_dir holds the files the builder's water layer writes on four days
    // (builder/tests/s2/test_water_layer.cpp): a lake relation with an island, a pond, a river
    // area its waterway names and one none names, a lake of a water=* value of its own, from
    // day 3 a salt pond of another and a marine sea of two tiles, and on day 4 a lake of many
    // islands in pieces. Each answers as the builder made it, and the side the tiles share is a
    // cut, no shore.
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
        // 16 m from the side the tiles share, 110 m and more from every other; 11 m from their
        // north side, and from their south side, on a line of a tenth of a degree with no sea
        // across it.
        let line = |(lat, lng): (f64, f64)| three.at(lat, lng).unwrap().takes_landmark(lat, lng, &LocationLine::default());
        assert!(line((45.001, 8.0018)));
        assert!(!line((45.0001, 8.001)));
        assert!(!line((45.0019, 8.001)));
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

    /// Whether the location line beside the water at the point, `line` its address, names it.
    fn takes(layer: &WaterLayer, (lat, lng): (f64, f64), line: &LocationLine<'_>) -> bool {
        layer.at(lat, lng).expect("water at the point").takes_landmark(lat, lng, line)
    }

    /// A lake of 0.01 degrees at the equator (1.2 km², notable), its south side at latitude 0.
    const SQUARE: Span = (0.0, 0.0, 0.01, 0.01);
    const MIDDLE: (f64, f64) = (0.005, 0.005);

    /// A line without a landmark, city, state or country.
    fn bare() -> LocationLine<'static> {
        LocationLine::default()
    }

    /// A line whose landmark is a POI of `category` and polygon area `area`.
    fn under(name: &str, category: u8, area: f64) -> LocationLine<'_> {
        LocationLine { landmark: Some((name, Some(LandmarkPoi { category, area }))), ..LocationLine::default() }
    }

    #[test]
    fn a_lake_takes_the_landmark_40_m_or_more_inside_its_shore() {
        // 0.0003 degrees is 33.4 m at the equator, 0.0004 degrees 44.5 m.
        let lake = WaterFixture::new().area("Lac Léman", LAKE, SQUARE, &[]).layer();

        assert!(takes(&lake, MIDDLE, &bare()));
        assert!(takes(&lake, (0.0004, 0.005), &bare()));
        for p in [(0.0003, 0.005), (0.005, 0.0097), (0.0097, 0.0097), (0.00001, 0.00001)] {
            assert!(!takes(&lake, p, &bare()), "{p:?}");
        }
        // An island's shore is a shore.
        let island = WaterFixture::new().area("Lac Léman", LAKE, SQUARE, &[(0.004, 0.004, 0.006, 0.006)]).layer();
        assert!(takes(&island, (0.003, 0.005), &bare()));
        assert!(!takes(&island, (0.0037, 0.005), &bare()));
    }

    #[test]
    fn kinds_the_line_never_names_never_take_the_landmark() {
        let named = |code: u16| takes(&WaterFixture::new().area("Testwasser", code, SQUARE, &[]).layer(), MIDDLE, &bare());

        for code in [NATURAL_WATER, LAKE, POND, HARBOUR, BAY, WETLAND, GLACIER, SEA, OCEAN, RIVER, CANAL] {
            assert!(named(code), "code {code}");
        }
        for code in [FISHPOND, VERBATIM] {
            assert!(!named(code), "code {code}");
        }
        assert_eq!([("water", "moat"), ("landuse", "basin"), ("water", "wastewater"), ("water", "river")].map(|(c, t)| WaterGroup::of(c, t)),
                   [WaterGroup::Other, WaterGroup::Other, WaterGroup::Other, WaterGroup::River]);
    }

    #[test]
    fn a_catalogue_number_is_no_name() {
        for name in ["№30", "No. 116", "no 5", "Nr.12", "#4", "Lake 938", "lake938", "Glacier 5b", "Pond 12", "49,0", "1-2", "Tank 3"] {
            assert!(catalogue_name(name), "{name}");
        }
        for name in ["Lake Geneva", "Lake 938 North", "Lakes 5", "Nolan 5", "North Lake", "Pond 12 Upper", "Lake 938ab", "Moat"] {
            assert!(!catalogue_name(name), "{name}");
        }
        let lake = |name: &str| takes(&WaterFixture::new().area(name, LAKE, SQUARE, &[]).layer(), MIDDLE, &bare());
        assert!(!lake("Lake 938"));
        assert!(lake("Lake Geneva"));
    }

    #[test]
    fn still_water_and_wetland_under_a_hectare_never_take_the_landmark_rivers_of_any_size_do() {
        // A hectare is 8.07e-7 square degrees at the equator and twice that at 60 degrees north;
        // the record's area is the object's, whatever its polygon here.
        let of = |code: u16, area: f32, span: Span, p: (f64, f64)| takes(&WaterFixture::new().piece("Testwasser", code, area, span).layer(), p, &bare());

        assert!(!of(POND, 7.0e-7, SQUARE, MIDDLE));
        assert!(of(POND, 9.0e-7, SQUARE, MIDDLE));
        assert!(!of(WETLAND, 7.0e-7, SQUARE, MIDDLE));
        assert!(of(WETLAND, 9.0e-7, SQUARE, MIDDLE));
        assert!(of(RIVER, 1.0e-8, SQUARE, MIDDLE));
        assert!(of(CANAL, 1.0e-8, SQUARE, MIDDLE));
        let north = (60.0, 0.0, 60.01, 0.02);
        assert!(!of(LAKE, 1.5e-6, north, (60.005, 0.01)));
        assert!(of(LAKE, 1.7e-6, north, (60.005, 0.01)));
    }

    #[test]
    fn a_weak_name_never_takes_the_landmark() {
        let river = |flags: u8| takes(&WaterFixture::new().area("Newtown Creek", RIVER, SQUARE, &[]).flags(flags).layer(), MIDDLE, &bare());

        assert!(river(0));
        assert!(!river(WEAK_NAME));
    }

    #[test]
    fn a_name_the_line_has_already_never_takes_the_landmark() {
        let lake = |name: &str, line: &LocationLine<'_>| takes(&WaterFixture::new().area(name, LAKE, SQUARE, &[]).layer(), MIDDLE, line);
        let shown = |city, state, country| LocationLine { shown: [city, state, country], ..LocationLine::default() };

        assert!(!lake("Jämijärvi", &shown(Some("Jämijärvi"), Some("Satakunta"), Some("Finland"))));
        assert!(!lake("Lake-Geneva", &shown(None, Some("lake geneva"), Some("Switzerland"))));
        assert!(!lake("Aral", &shown(None, None, Some("ARAL"))));
        assert!(lake("Jämijärvi", &shown(Some("Jämijärvi town"), Some("Satakunta"), Some("Finland"))));
        // Within the landmark's name, or the landmark within the water's, from 4 characters.
        let reserve = |name: &'static str| under(name, 131, 1.0);
        assert!(!lake("Mill Pond", &reserve("Setauket Mill Pond")));
        assert!(!lake("Lake Titicaca", &reserve("Lake Titicaca (Peruvian sector)")));
        assert!(!lake("Lake Titicaca", &reserve("lake titicaca")));
        assert!(!lake("Milford Sound", &reserve("Piopiotahi (Milford Sound) Marine Reserve")));
        assert!(lake("Mill Pond", &reserve("Mil")));
        assert!(lake("Tonle Sap Lake", &reserve("Boeung Tonle Sap Protected Area")));
        // Accents aside (NFKD, its marks dropped), and compatibility forms too.
        assert!(!lake("Lagoa D'água", &shown(Some("Lágoa D'água"), Some("Bahia"), Some("Brazil"))));
        assert!(!lake("Jamijarvi", &shown(Some("Jämijärvi"), None, Some("Finland"))));
        assert!(!lake("Lake Mahinapua", &reserve("Lake Māhinapua Scenic Reserve")));
        assert!(!lake("Ｌａｋｅ Ｂｌｅｄ", &shown(None, Some("Lake Bled"), Some("Slovenia"))));
        assert!(lake("Lagoa D'água", &shown(Some("Lagoa Dourada"), Some("Bahia"), Some("Brazil"))));
        assert_eq!(fold("Lágoa D'Água"), "lagoadagua");
        assert_eq!(fold("Ångström-Sjön №5"), "angstromsjonno5");
    }

    #[test]
    fn a_landmark_stays_but_for_a_park_under_notable_smaller_water_and_a_natural_one_under_smaller_water() {
        // The square's object is 1.0e-4 square degrees.
        let water = |code: u16, line: &LocationLine<'_>| takes(&WaterFixture::new().area("Testwasser", code, SQUARE, &[]).layer(), MIDDLE, line);
        let small = |line: &LocationLine<'_>| takes(&WaterFixture::new().piece("Testwasser", LAKE, 4.0e-6, SQUARE).layer(), MIDDLE, line);

        // A nature reserve (61), national park (130) or protected area (131) larger than a
        // notable lake, glacier, bay or river gives way to it.
        for category in [61, 130, 131] {
            for code in [LAKE, GLACIER, BAY, RIVER] {
                assert!(water(code, &under("Testpark", category, 1.0)), "{category} {code}");
                assert!(!water(code, &under("Testpark", category, 5.0e-5)), "{category} {code}: a smaller park");
            }
            // A sea, an ocean and a wetland never; nor a lake of under 0.1 km².
            for code in [SEA, OCEAN, WETLAND] {
                assert!(!water(code, &under("Testpark", category, 1.0)), "{category} {code}");
            }
            assert!(!small(&under("Testpark", category, 1.0)));
        }
        // A natural feature (a beach, 82; a glacier, 86) gives way to smaller water, whatever its
        // size; a sea too.
        for category in [82, 86] {
            assert!(water(LAKE, &under("Testfeature", category, 1.0)));
            assert!(small(&under("Testfeature", category, 1.0)));
            assert!(water(SEA, &under("Testfeature", category, 1.0)));
            assert!(!water(LAKE, &under("Testfeature", category, 5.0e-5)));
        }
        // Every other landmark stays: a bridge (113), a park (60), an attraction (1), a marina
        // (66); and one whose POI the answer doesn't know.
        for category in [113, 60, 1, 66] {
            assert!(!water(LAKE, &under("Testlandmark", category, 1.0)), "{category}");
        }
        assert!(!water(LAKE, &LocationLine { landmark: Some(("Testlandmark", None)), ..LocationLine::default() }));
    }

    #[test]
    fn a_cut_between_two_pieces_of_one_object_is_no_shore() {
        // A lake cut along the meridian 0.01 into two pieces; the points 22 m either side of it.
        let split = |marked: bool| WaterFixture::new().split("Testsee", LAKE, (0.0, 0.0, 0.01, 0.02), 0.01, marked).layer();
        let (west, east) = ((0.005, 0.0098), (0.005, 0.0102));

        assert!(takes(&split(true), west, &bare()));
        assert!(takes(&split(true), east, &bare()));
        assert!(!takes(&split(false), west, &bare()));
        assert!(!takes(&split(false), east, &bare()));
        // Its other shores are shores still.
        assert!(!takes(&split(true), (0.0002, 0.0098), &bare()));
    }

    #[test]
    fn a_shore_of_another_piece_of_the_object_past_a_cut_is_a_shore() {
        // A lake cut along the meridian 0.01, an island in its east piece 22 m past the cut: 11 m
        // west of the cut, the point is 33 m from the island's shore, and 550 m and more from
        // every shore of the piece that holds it.
        let island =
            WaterFixture::new().split("Testsee", LAKE, (0.0, 0.0, 0.01, 0.02), 0.01, true).island((0.0045, 0.0102, 0.0055, 0.0105)).layer();
        assert!(!takes(&island, (0.005, 0.0099), &bare()));
        assert!(takes(&island, (0.005, 0.0095), &bare()));

        // The lake's north shore runs north-west from the cut's north end in the west piece and
        // falls steeply to the south-east from it in the east piece: 20 m west of the cut and
        // 40 m south of its end, the point is 45 m from the end and 36 m from the east piece's
        // shore; 60 m south of the end, 45 m from it.
        let m = 1.0 / crate::METRES_PER_DEGREE;
        let bank = WaterFixture::new()
            .pieces("Testsee", LAKE, [(0.0, 0.01), (0.01, 0.01)], &[(0.011, 0.009), (0.011, 0.0), (0.0, 0.0)], &[(0.0, 0.015)], true)
            .layer();
        assert!(!takes(&bank, (0.01 - 40.0 * m, 0.01 - 20.0 * m), &bare()));
        assert!(takes(&bank, (0.01 - 60.0 * m, 0.01 - 20.0 * m), &bare()));
    }

    #[test]
    fn a_seas_line_of_a_tenth_of_a_degree_is_no_shore_where_sea_lies_across_it() {
        // Two tiles of a sea either side of the meridian 0.1, each a ring of its own (no arc
        // shared, none marked a cut), as the marine areas are cut along those lines: the line
        // between them is no shore. Their south sides lie on the parallel 0, with no sea south
        // of it: a coast, as is a sea's edge off those lines.
        let tiles = |east: &str| {
            WaterFixture::new().piece("North Sea", SEA, 0.02, (0.0, 0.0, 0.1, 0.1)).piece(east, SEA, 0.02, (0.0, 0.1, 0.1, 0.2)).layer()
        };
        let (seam, south) = ((0.05, 0.0998), (0.0002, 0.05));
        assert!(takes(&tiles("North Sea"), seam, &bare()));
        assert!(!takes(&tiles("North Sea"), south, &bare()));
        // A tile of another sea across the line.
        assert!(takes(&tiles("Norwegian Sea"), seam, &bare()));
        // Past the line, the shore of the sea's tile there is its shore: an island 22 m east of
        // it, 33 m from the point.
        let island = WaterFixture::new()
            .piece("North Sea", SEA, 0.02, (0.0, 0.0, 0.1, 0.1))
            .piece("North Sea", SEA, 0.02, (0.0, 0.1, 0.1, 0.2))
            .island((0.0495, 0.1002, 0.0505, 0.1005))
            .layer();
        assert!(!takes(&island, (0.05, 0.0999), &bare()));
        assert!(takes(&island, (0.05, 0.0995), &bare()));
        // A sea alone, its sides on those lines but its east one: each a coast.
        let sea = WaterFixture::new().area("North Sea", SEA, (0.0, 0.0, 0.1, 0.10005), &[]).layer();
        for p in [(0.0002, 0.05), (0.05, 0.0002), (0.05, 0.1)] {
            assert!(!takes(&sea, p, &bare()), "{p:?}");
        }
        // A lake's edges on them are shores, whatever lies across.
        let lakes =
            WaterFixture::new().piece("Testsee", LAKE, 0.02, (0.0, 0.0, 0.1, 0.1)).piece("Testsee", LAKE, 0.02, (0.0, 0.1, 0.1, 0.2)).layer();
        assert!(!takes(&lakes, seam, &bare()));
    }

    #[test]
    fn a_water_answer_carries_landmark_last_and_only_when_set() {
        let lake = WaterFixture::new().area("Le Léman", LAKE, SQUARE, &[]).layer();
        let mut water = lake.at(MIDDLE.0, MIDDLE.1).unwrap();

        assert_eq!(serde_json::to_string(&water).unwrap(), r#"{"name":"Le Léman","class":"water","type":"lake"}"#);
        water.landmark = true;
        assert_eq!(serde_json::to_string(&water).unwrap(), r#"{"name":"Le Léman","class":"water","type":"lake","landmark":true}"#);
    }

    #[test]
    fn a_cut_arc_file_holds_a_bit_per_arc_slot() {
        let words = |bytes: &[u8], slots: usize| CutArcs::new(FileBytes::Owned(bytes.to_vec()), slots);

        let cuts = words(&[0b10, 0, 0, 0x80, 1, 0, 0, 0], 33).unwrap();
        assert_eq!((0..34).filter(|&s| cuts.is_cut(s)).collect::<Vec<u32>>(), [1, 31, 32]);
        assert!(words(&[], 0).is_ok());
        assert_eq!(words(&[0; 4], 33).err(), Some("4 bytes for 33 arc slots, not 8".to_string()));
        assert_eq!(words(&[0; 8], 32).err(), Some("8 bytes for 32 arc slots, not 4".to_string()));
    }
}
