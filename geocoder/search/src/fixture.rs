//! Test fixture: a small geocoder index (boundaries, place nodes, POIs, streets, address points,
//! interpolation lines, postcode centroids, their side files and the tag bundles of places, areas
//! and POIs) in the builder's file layout, through query_server's fixture encoders (the
//! boundaries' rings as arcs, query_server's arc fixture), and the place-search index built from
//! it the way `place-search build` builds one.
use crate::build::{build_all, held_subs, index_meta, BuildOpts};
use crate::constants::{ADMIN_TAGS_FILE, PLACE_TAGS_FILE, POI_TAGS_FILE, PLACES_SUB_INDEXES};
use crate::model::PLACE_FLAG_LABEL_OF;
use crate::profile::{Profile, SearchRequest};
use crate::constants::TAG_TIERS;
use crate::tags;
use query_server::arc_fixture::{self, ArcFixture};
use query_server::{
    addr_files, cell_id_at_level, cell_pairs, geo_index, place_files, poi_files, postcode_files, side_blocks, strings, way_files, AdminPolygon,
    Index, NodeCoord, PlaceNode, PointE7, PoiRecord, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE, DEFAULT_STREET_CELL_LEVEL, NO_DATA,
    PLACE_FLAG_HIDDEN, STR_TIER_COUNT,
};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Grid steps per side of a boundary's bounding box whose points list it in their admin cells.
const GRID: u32 = 32;
/// The step in degrees between the points along a way's edge whose street cells list the way.
const EDGE_STEP_DEG: f32 = 1e-4;

/// xorshift: deterministic test values without a dependency.
pub struct Rng(pub u64);

impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

/// A fresh directory under the temp dir, removed on drop.
pub struct TempDir(pub PathBuf);

impl TempDir {
    pub fn new(tag: &str) -> Self {
        static DIRS: AtomicUsize = AtomicUsize::new(0);
        let n = DIRS.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("ps-{tag}-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        TempDir(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A way of street_ways.bin or interp_ways.bin: its name (a street's own, an interpolation's
/// street's), its numbers (start, end, interpolation; none for a street) and its nodes, (lat, lng).
struct Way {
    name_id: u32,
    numbers: (u32, u32, u8),
    nodes: Vec<(f32, f32)>,
}

/// An address point of addr_points.bin: its point, house number, addr:street and parent way.
struct Addr {
    lat: f32,
    lng: f32,
    housenumber_id: u32,
    street_id: u32,
    parent_way_id: u32,
}

/// A side file: a u32 a record of the file it sits beside.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Side {
    WayParents,
    WayPostcodes,
    AddrPostcodes,
    AddrCities,
    InterpPostcodes,
    PlaceTags,
    AdminTags,
    PoiTags,
}

impl Side {
    fn file(self) -> &'static str {
        match self {
            Side::WayParents => "way_parents.bin",
            Side::WayPostcodes => "way_postcodes.bin",
            Side::AddrPostcodes => "addr_postcodes.bin",
            Side::AddrCities => "addr_cities.bin",
            Side::InterpPostcodes => "interp_postcodes.bin",
            Side::PlaceTags => PLACE_TAGS_FILE,
            Side::AdminTags => ADMIN_TAGS_FILE,
            Side::PoiTags => POI_TAGS_FILE,
        }
    }
}

#[derive(Default)]
pub struct Geo {
    strings: Vec<u8>,
    tag_strings: [Vec<u8>; 2],
    /// (record, closed rings as (lat, lng) in degrees, the outer one first)
    polygons: Vec<(AdminPolygon, Vec<Vec<(f32, f32)>>)>,
    nodes: Vec<PlaceNode>,
    pois: Vec<PoiRecord>,
    /// admin_own_names.bin: (polygon, own name) of each polygon that goes by its place node's
    /// English name.
    own_names: Vec<(u32, u32)>,
    streets: Vec<Way>,
    interps: Vec<Way>,
    addrs: Vec<Addr>,
    centroids: Vec<postcode_files::Centroid>,
    /// The values each side file holds other than NO_DATA, as (record, value).
    sides: BTreeMap<Side, Vec<(u32, u32)>>,
}

impl Geo {
    fn string(&mut self, s: &str) -> u32 {
        let id = self.strings.len() as u32;
        self.strings.extend_from_slice(s.as_bytes());
        self.strings.push(0);
        id
    }

    /// A rectangular boundary ([south, west, north, east]); returns its polygon id.
    pub fn boundary(&mut self, name: &str, admin_level: u8, rect: [f32; 4]) -> u32 {
        let [s, w, n, e] = rect;
        self.shaped_boundary(name, admin_level, &[(s, w), (s, e), (n, e), (n, w), (s, w)], &[])
    }

    /// A boundary of closed (lat, lng) rings, `outer` around `holes`, each one arc; returns its
    /// polygon id.
    pub fn shaped_boundary(&mut self, name: &str, admin_level: u8, outer: &[(f32, f32)], holes: &[&[(f32, f32)]]) -> u32 {
        let rings: Vec<Vec<(f32, f32)>> = std::iter::once(outer).chain(holes.iter().copied()).map(|r| r.to_vec()).collect();
        let area = arc_fixture::exact_area(&rings.iter().map(|r| e7_ring(r)).collect::<Vec<_>>());
        let poly = arc_fixture::record(self.string(name), admin_level, area);
        self.polygons.push((poly, rings));
        self.polygons.len() as u32 - 1
    }

    /// A rectangular place area ([south, west, north, east]) of place type `place_type`
    /// (query_server::place_type_to_field's), as the builder writes a place=* way or relation
    /// (admin_level 15) at its type's rank; returns its polygon id.
    pub fn place_area(&mut self, name: &str, place_type: u8, rect: [f32; 4]) -> u32 {
        let id = self.boundary(name, 15, rect);
        let poly = &mut self.polygons[id as usize].0;
        poly.place_type_override = place_type;
        poly.rank_address = arc_fixture::default_rank(15, place_type);
        id
    }

    /// A place node of PlaceNode type `place_type` (0 city .. 6 quarter); returns its index.
    pub fn place(&mut self, name: &str, place_type: u8, lat: f32, lng: f32) -> u32 {
        let name_id = self.string(name);
        let rank_address = query_server::PLACE_NODE_RANK[place_type as usize];
        self.nodes.push(PlaceNode { lat, lng, name_id, place_type, _pad1: 0, rank_address, parent_poly_id: NO_DATA });
        self.nodes.len() as u32 - 1
    }

    /// Boundary `poly`'s own name, where it goes by the English name of the place node it links.
    pub fn own_name(&mut self, poly: u32, name: &str) {
        let name_id = self.string(name);
        self.own_names.push((poly, name_id));
    }

    /// Node `node` hidden as the label of boundary `poly`, as the builder flags a claimed node.
    pub fn claim(&mut self, poly: u32, node: u32) {
        let pn = &mut self.nodes[node as usize];
        pn._pad1 |= PLACE_FLAG_LABEL_OF | PLACE_FLAG_HIDDEN;
        pn.rank_address = 0;
        pn.parent_poly_id = poly;
    }

    /// A point POI with the builder's importance byte; returns its record index.
    pub fn poi(&mut self, name: &str, lat: f32, lng: f32, importance: u8) -> u32 {
        let name_id = self.string(name);
        self.pois.push(PoiRecord {
            lat, lng, vertex_offset: NO_DATA, name_id, category: 0, tier: 1, flags: 0, importance,
            parent_street_id: NO_DATA, parent_postcode_id: NO_DATA, parent_poly_id: NO_DATA,
        });
        self.pois.len() as u32 - 1
    }

    /// POI `poi` of `category` and tier `tier`, its flags byte `flags`.
    pub fn classify_poi(&mut self, poi: u32, category: u8, tier: u8, flags: u8) {
        let p = &mut self.pois[poi as usize];
        (p.category, p.tier, p.flags) = (category, tier, flags);
    }

    /// A street named `name` through `nodes`, (lat, lng); returns its way index.
    pub fn street(&mut self, name: &str, nodes: &[(f32, f32)]) -> u32 {
        let name_id = self.string(name);
        self.streets.push(Way { name_id, numbers: (0, 0, 0), nodes: nodes.to_vec() });
        self.streets.len() as u32 - 1
    }

    /// Polygon `poly` as the parent of street `way`.
    pub fn way_parent(&mut self, way: u32, poly: u32) {
        self.side(Side::WayParents, way, poly);
    }

    /// Boundary `poly` as the country `code` (ISO 3166-1 alpha-2, either case).
    pub fn country(&mut self, poly: u32, code: [u8; 2]) {
        self.polygons[poly as usize].0.country_code = packed_country(code);
    }

    /// The postcode of street `way`.
    pub fn way_postcode(&mut self, way: u32, postcode: &str) {
        let postcode_id = self.string(postcode);
        self.side(Side::WayPostcodes, way, postcode_id);
    }

    /// An address point at (`lat`, `lng`) numbered `housenumber`, with its addr:street and its
    /// parent way where it has them; returns its record index.
    pub fn address(&mut self, housenumber: &str, lat: f32, lng: f32, street: Option<&str>, way: Option<u32>) -> u32 {
        let housenumber_id = self.string(housenumber);
        let street_id = street.map_or(NO_DATA, |s| self.string(s));
        self.addrs.push(Addr { lat, lng, housenumber_id, street_id, parent_way_id: way.unwrap_or(NO_DATA) });
        self.addrs.len() as u32 - 1
    }

    /// The postcode of address point `addr`.
    pub fn addr_postcode(&mut self, addr: u32, postcode: &str) {
        let postcode_id = self.string(postcode);
        self.side(Side::AddrPostcodes, addr, postcode_id);
    }

    /// The addr:city of address point `addr`.
    pub fn addr_city(&mut self, addr: u32, city: &str) {
        let city_id = self.string(city);
        self.side(Side::AddrCities, addr, city_id);
    }

    /// An interpolation line of the street named `street` through `nodes`, (lat, lng), numbered
    /// from `start` at its first node to `end` at its last: every number for `interpolation` 0,
    /// the even ones for 1, the odd ones for 2. Returns its index.
    pub fn interpolation(&mut self, street: &str, (start, end, interpolation): (u32, u32, u8), nodes: &[(f32, f32)]) -> u32 {
        let name_id = self.string(street);
        self.interps.push(Way { name_id, numbers: (start, end, interpolation), nodes: nodes.to_vec() });
        self.interps.len() as u32 - 1
    }

    /// The postcode of interpolation line `interp`.
    pub fn interp_postcode(&mut self, interp: u32, postcode: &str) {
        let postcode_id = self.string(postcode);
        self.side(Side::InterpPostcodes, interp, postcode_id);
    }

    /// A postcode centroid of `country` (ISO 3166-1 alpha-2, either case) at (`lat`, `lng`).
    pub fn postcode_centroid(&mut self, postcode: &str, country: [u8; 2], lat: f32, lng: f32) {
        let postcode_id = self.string(postcode);
        self.centroids.push(postcode_files::Centroid { lat, lng, postcode_id, country_code: packed_country(country) });
    }

    /// The tags of place node `node`, as the bundle place_tags.bin points at.
    pub fn place_tags(&mut self, node: u32, tags: &[(&str, &str)]) {
        self.bundle(Side::PlaceTags, node, tags);
    }

    /// The tags of polygon `poly`, as the bundle admin_tags.bin points at.
    pub fn area_tags(&mut self, poly: u32, tags: &[(&str, &str)]) {
        self.bundle(Side::AdminTags, poly, tags);
    }

    /// The tags of POI `poi`, as the bundle poi_tags.bin points at.
    pub fn poi_tags(&mut self, poi: u32, tags: &[(&str, &str)]) {
        self.bundle(Side::PoiTags, poi, tags);
    }

    /// Record `record`'s bundle of `tags`, a string of the pool's, in the side file `side`.
    fn bundle(&mut self, side: Side, record: u32, tags: &[(&str, &str)]) {
        let bundle = tags::encode(tags.iter().copied()).expect("tags a bundle holds");
        let pool = &mut self.tag_strings[usize::from(side == Side::PoiTags)];
        let bundle_id = pool.len() as u32;
        pool.extend_from_slice(bundle.as_bytes());
        pool.push(0);
        self.side(side, record, bundle_id);
    }

    /// Record `record`'s value in the side file `side`.
    fn side(&mut self, side: Side, record: u32, value: u32) {
        self.sides.entry(side).or_default().push((record, value));
    }

    /// The files as the builder writes them, by name. A boundary is listed in the admin cells of
    /// a grid of points over its bounding box and of each point of a record inside the box; the
    /// own names beside the polygons, empty without any.
    fn files(&self) -> Vec<(&'static str, Vec<u8>)> {
        let cell = |lat: f32, lng: f32| cell_id_at_level(lat as f64, lng as f64, DEFAULT_ADMIN_CELL_LEVEL);
        let points: Vec<(f32, f32)> = self.nodes.iter().map(|p| (p.lat, p.lng))
            .chain(self.pois.iter().map(|p| (p.lat, p.lng)))
            .chain(self.streets.iter().chain(&self.interps).flat_map(|w| w.nodes.iter().copied()))
            .chain(self.addrs.iter().map(|a| (a.lat, a.lng)))
            .chain(self.centroids.iter().map(|c| (c.lat, c.lng)))
            .collect();
        let (mut arcs, mut admin) = (ArcFixture::new(), Vec::new());
        for (id, (poly, rings)) in self.polygons.iter().enumerate() {
            arcs.ring_polygon(*poly, 0, &rings.iter().map(|r| e7_ring(r)).collect::<Vec<_>>());
            let outer = &rings[0];
            let min = |f: fn(&(f32, f32)) -> f32| outer.iter().map(f).fold(f32::INFINITY, f32::min);
            let max = |f: fn(&(f32, f32)) -> f32| outer.iter().map(f).fold(f32::NEG_INFINITY, f32::max);
            let ([s, w], [n, e]) = ([min(|v| v.0), min(|v| v.1)], [max(|v| v.0), max(|v| v.1)]);
            let grid = (0..=GRID).flat_map(|i| {
                (0..=GRID).map(move |j| (s + (n - s) * i as f32 / GRID as f32, w + (e - w) * j as f32 / GRID as f32))
            });
            for (lat, lng) in points.iter().copied().chain(grid) {
                if (s..=n).contains(&lat) && (w..=e).contains(&lng) {
                    admin.push((cell(lat, lng), id as u32));
                }
            }
        }
        admin.sort_unstable();
        admin.dedup();
        let (admin_cells, admin_entries) = cell_pairs::write_pairs(admin, cell_pairs::ADMIN);
        let (places, place_rows) = place_files::encode(&self.nodes, DEFAULT_ADMIN_CELL_LEVEL);
        let end = self.strings.len();
        let mut tiers: Vec<String> = (0..STR_TIER_COUNT)
            .map(|t| format!("{{\"start\": {}, \"end\": {end}}}", if t == 0 { 0 } else { end }))
            .collect();
        let mut base = end;
        for (name, pool) in TAG_TIERS.iter().zip(&self.tag_strings) {
            let start = base;
            base += pool.len();
            tiers.push(format!(r#"{{"file":"{name}","start":{start},"end":{base}}}"#));
        }
        let mut own_names = self.own_names.clone();
        own_names.sort_unstable();
        let own_names = own_names.iter().flat_map(|&(poly, name)| [poly, name]).flat_map(u32::to_le_bytes).collect();
        let mut files: Vec<(&'static str, Vec<u8>)> = arcs.files().into();
        files.extend([
            ("admin_cells.bin", admin_cells), ("admin_entries.bin", admin_entries),
            (query_server::ADMIN_OWN_NAMES_FILE, own_names),
            (place_files::CELLS_FILE, places.cells), (place_files::NODES_FILE, places.nodes),
            (place_files::HIDDEN_FILE, places.hidden),
            ("strings_core.bin", strings::encode(&self.strings)),
            ("strings_layout.json", format!("{{\"tiers\": [{}]}}", tiers.join(", ")).into_bytes()),
        ]);
        if !self.pois.is_empty() {
            let (poi_cells, poi_entries) = cell_pairs::write_pairs(
                self.pois.iter().enumerate().map(|(i, p)| (cell(p.lat, p.lng), i as u32)),
                cell_pairs::POI,
            );
            let (records, vertices) = poi_files::encode(&self.pois.iter().map(|&p| (p, None)).collect::<Vec<_>>());
            files.extend([
                ("poi_records.bin", records),
                ("poi_cells.bin", poi_cells), ("poi_entries.bin", poi_entries), ("poi_vertices.bin", vertices),
            ]);
        }
        if !(self.streets.is_empty() && self.interps.is_empty() && self.addrs.is_empty()) {
            let (geo_cells, geo_entries) = geo_index::write(&self.geo_cells());
            files.extend([("geo_cells.bin", geo_cells), ("geo_entries.bin", geo_entries)]);
            files.extend(way_pair(way_files::Kind::Streets, &self.streets));
            if !self.interps.is_empty() {
                files.extend(way_pair(way_files::Kind::Interps, &self.interps));
            }
            if !self.addrs.is_empty() {
                let fields = |a: &Addr| [a.lat.to_bits(), a.lng.to_bits(), a.housenumber_id, a.street_id, a.parent_way_id, NO_DATA, 0];
                let logical: Vec<u8> = self.addrs.iter().flat_map(fields).flat_map(u32::to_le_bytes).collect();
                let addrs = addr_files::encode(&logical, &[]);
                files.extend([
                    (addr_files::POINTS_FILE, addrs.points), (addr_files::VERTICES_FILE, addrs.vertices), (addr_files::INDEX_FILE, addrs.index),
                ]);
            }
        }
        if !self.centroids.is_empty() {
            let (centroids, cells) = postcode_files::encode_points(&self.centroids, DEFAULT_ADMIN_CELL_LEVEL);
            files.extend([(postcode_files::CENTROIDS_FILE, centroids), (postcode_files::CELLS_FILE, cells)]);
        }
        for (name, pool) in TAG_TIERS.into_iter().zip(&self.tag_strings) {
            if !pool.is_empty() { files.push((name, strings::encode(pool))); }
        }
        for (&side, set) in &self.sides {
            let mut values = vec![NO_DATA; self.records_beside(side)];
            for &(record, value) in set {
                // The place files order their nodes by cell (place_files::encode); every other
                // file keeps its records in the order they were added.
                let row = if side == Side::PlaceTags { place_rows[record as usize] } else { record };
                values[row as usize] = value + match side {
                    Side::PlaceTags | Side::AdminTags => end as u32,
                    Side::PoiTags => (end + self.tag_strings[0].len()) as u32,
                    _ => 0,
                };
            }
            files.push((side.file(), side_blocks::encode(&values)));
        }
        files
    }

    /// The records of the file `side` sits beside.
    fn records_beside(&self, side: Side) -> usize {
        match side {
            Side::WayParents | Side::WayPostcodes => self.streets.len(),
            Side::AddrPostcodes | Side::AddrCities => self.addrs.len(),
            Side::InterpPostcodes => self.interps.len(),
            Side::PlaceTags => self.nodes.len(),
            Side::AdminTags => self.polygons.len(),
            Side::PoiTags => self.pois.len(),
        }
    }

    /// The street cells the ways and address points are listed in: a way in those of its nodes
    /// and of points EDGE_STEP_DEG apart along each edge, an address point in its point's.
    fn geo_cells(&self) -> Vec<(u64, [Vec<u32>; geo_index::TABLES])> {
        let cell = |(lat, lng): (f32, f32)| cell_id_at_level(lat as f64, lng as f64, DEFAULT_STREET_CELL_LEVEL);
        let mut cells: BTreeMap<u64, [Vec<u32>; geo_index::TABLES]> = BTreeMap::new();
        for (table, ways) in [(geo_index::STREETS, &self.streets), (geo_index::INTERPOLATIONS, &self.interps)] {
            for (id, way) in ways.iter().enumerate() {
                for edge in way.nodes.windows(2) {
                    let ((a_lat, a_lng), (b_lat, b_lng)) = (edge[0], edge[1]);
                    let steps = ((b_lat - a_lat).hypot(b_lng - a_lng) / EDGE_STEP_DEG).ceil().max(1.0) as u32;
                    for k in 0..=steps {
                        let t = k as f32 / steps as f32;
                        cells.entry(cell((a_lat + (b_lat - a_lat) * t, a_lng + (b_lng - a_lng) * t))).or_default()[table].push(id as u32);
                    }
                }
                for &node in &way.nodes {
                    cells.entry(cell(node)).or_default()[table].push(id as u32);
                }
            }
        }
        for (id, a) in self.addrs.iter().enumerate() {
            cells.entry(cell((a.lat, a.lng))).or_default()[geo_index::ADDRESSES].push(id as u32);
        }
        cells
            .into_iter()
            .map(|(cell, mut lists)| {
                for ids in &mut lists {
                    ids.sort_unstable();
                    ids.dedup();
                }
                (cell, lists)
            })
            .collect()
    }

    /// Writes the files into `dir` the way a patch replaces one: a new file renamed over it.
    pub fn write(&self, dir: &Path) {
        std::fs::create_dir_all(dir).unwrap();
        for (name, bytes) in self.files() {
            let new = dir.join(format!("{name}.new"));
            std::fs::write(&new, bytes).unwrap();
            std::fs::rename(&new, dir.join(name)).unwrap();
        }
    }

    /// Writes the files into `dir` and loads them.
    pub fn load(&self, dir: &Path) -> Index {
        self.write(dir);
        Index::load(dir.to_str().unwrap(), DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE)
            .unwrap()
    }
}

/// The two files of `ways`, of street_ways.bin and street_nodes.bin or of interp_ways.bin and
/// interp_nodes.bin.
fn way_pair(kind: way_files::Kind, ways: &[Way]) -> [(&'static str, Vec<u8>); 2] {
    let rows: Vec<(way_files::WayRow, Vec<NodeCoord>)> = ways
        .iter()
        .map(|w| {
            let (start_number, end_number, interpolation) = w.numbers;
            let node_count = u16::try_from(w.nodes.len()).expect("a way of fewer than 2^16 nodes");
            let nodes = w.nodes.iter().map(|&(lat, lng)| NodeCoord { lat, lng }).collect();
            (way_files::WayRow { node_count, name: w.name_id, start_number, end_number, interpolation }, nodes)
        })
        .collect();
    let files = way_files::encode(kind, &rows);
    [(kind.ways_file(), files.ways), (kind.nodes_file(), files.nodes)]
}

/// Builds every sub-index of `geo` into `out` with the default options and `extras`, and the
/// search_meta.json fields `SearchIndex::open` reads.
pub fn build_search(geo: &Index, out: &Path, extras: Option<&Path>) {
    let opts = BuildOpts { extras: extras.map(|p| p.to_str().unwrap().to_string()), ..BuildOpts::new(out.join("tmp")) };
    let meta = index_meta(geo, &opts).into();
    build_all(geo, &held_subs(geo, PLACES_SUB_INDEXES), out, &opts, || Ok(meta)).unwrap();
}

/// A country code as the builder packs it: upper case, the first letter the high byte.
fn packed_country(code: [u8; 2]) -> u16 {
    u16::from_be_bytes(code.map(|b| b.to_ascii_uppercase()))
}

/// A ring of (lat, lng) degrees on the e7 grid.
fn e7_ring(ring: &[(f32, f32)]) -> Vec<PointE7> {
    ring.iter().map(|&(lat, lng)| arc_fixture::e7(lat as f64, lng as f64)).collect()
}



/// A request of the immich profile for `q`, at most `limit` results.
pub fn new_request(q: &str, limit: usize) -> SearchRequest {
    SearchRequest { limit, ..SearchRequest::new(q, Profile::Immich) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::SearchFiles;
    use crate::model::Kind;
    use query_server::side_blocks::SideBlocks;

    /// Austin, a street in it with an address point and an interpolation line, and a centroid.
    fn austin() -> Geo {
        let mut g = Geo::default();
        let city = g.boundary("Austin", 8, [30.0, -98.0, 30.6, -97.4]);
        let way = g.street("Liberty Park Drive", &[(30.3001, -97.8001), (30.3001, -97.7951), (30.3051, -97.7951)]);
        g.way_parent(way, city);
        g.way_postcode(way, "78746");
        let addr = g.address("1044", 30.3002, -97.7980, Some("Liberty Park Drive"), Some(way));
        g.addr_postcode(addr, "78746");
        g.addr_city(addr, "Austin");
        g.address("8a", 30.3101, -97.7101, None, None);
        let interp = g.interpolation("Liberty Park Drive", (1000, 1098, 1), &[(30.3000, -97.8001), (30.3000, -97.7951)]);
        g.interp_postcode(interp, "78746");
        g.postcode_centroid("78746", *b"us", 30.3, -97.8);
        g
    }

    #[test]
    fn streets_addresses_and_interpolations_load_with_their_side_files() {
        let dir = TempDir::new("fixture-streets");

        let geo = austin().load(&dir.0);

        let string = |id: Option<u32>| id.map(|id| geo.get_string(id).into_owned());
        let streets = geo.streets.as_ref().unwrap();
        let row = streets.row(0).unwrap();
        let mut nodes = Vec::new();
        assert!(streets.way(0, &mut nodes));
        assert_eq!((streets.count(), string(Some(row.name)).unwrap(), nodes.len()), (1, "Liberty Park Drive".to_string(), 3));
        let addrs = geo.addrs.as_ref().unwrap();
        let (first, second) = (addrs.record(0).unwrap(), addrs.record(1).unwrap());
        assert_eq!((string(Some(first.housenumber_id)), string(Some(first.street_id)), first.parent_way_id), (Some("1044".into()), Some("Liberty Park Drive".into()), 0));
        assert_eq!((string(Some(second.housenumber_id)), second.street_id, second.parent_way_id), (Some("8a".into()), NO_DATA, NO_DATA));
        let interp = geo.interps.as_ref().unwrap().row(0).unwrap();
        assert_eq!((interp.start_number, interp.end_number, interp.interpolation), (1000, 1098, 1));
        assert_eq!(geo.way_parents.as_ref().and_then(|s| s.get(0)), Some(0));
        let side = |s: &Option<SideBlocks>, id: u32| string(s.as_ref().and_then(|s| s.get(id)));
        assert_eq!((side(&geo.way_postcodes, 0), side(&geo.interp_postcodes, 0)), (Some("78746".into()), Some("78746".into())));
        assert_eq!((side(&geo.addr_postcodes, 0), side(&geo.addr_cities, 0)), (Some("78746".into()), Some("Austin".into())));
        assert_eq!((geo.addr_postcodes.as_ref().and_then(|s| s.get(1)), geo.addr_cities.as_ref().and_then(|s| s.get(1))), (Some(NO_DATA), Some(NO_DATA)));
    }

    #[test]
    fn a_street_and_its_address_point_answer_a_reverse_lookup_there() {
        let dir = TempDir::new("fixture-reverse");

        let geo = austin().load(&dir.0);

        let answer = serde_json::to_string(&geo.query(30.3002, -97.7980)).unwrap();
        assert!(answer.contains(r#""road":"Liberty Park Drive""#) && answer.contains(r#""house_number":"1044""#), "{answer}");
    }

    #[test]
    fn a_postcode_centroid_loads_with_its_country() {
        let dir = TempDir::new("fixture-centroids");

        let geo = austin().load(&dir.0);

        let centroids = geo.postcode_centroids.as_ref().unwrap();
        let mut rows = Vec::new();
        centroids.rows_of(0, 1, |row, c| rows.push((row, geo.get_string(c.postcode_id).into_owned(), c.country_code.to_be_bytes())));
        assert_eq!((centroids.rows(), rows), (1, vec![(0, "78746".to_string(), *b"US")]));
    }

    #[test]
    fn a_postcode_centroid_answers_a_reverse_lookup_in_its_country() {
        let mut g = Geo::default();
        let us = g.boundary("United States", 2, [25.0, -106.0, 36.0, -93.0]);
        g.country(us, *b"us");
        g.postcode_centroid("78746", *b"us", 30.3, -97.8);
        let dir = TempDir::new("fixture-centroid-country");

        let geo = g.load(&dir.0);

        let answer = serde_json::to_string(&geo.query(30.3, -97.8)).unwrap();
        assert!(answer.contains(r#""postcode":"78746""#), "{answer}");
    }

    #[test]
    fn places_areas_and_pois_have_their_tag_bundles_beside_them() {
        let mut g = Geo::default();
        let land = g.boundary("Bayern", 4, [47.0, 9.0, 50.0, 14.0]);
        let city = g.place("Munich", 0, 48.137, 11.575);
        g.place("Garching", 2, 48.25, 11.65);
        let cafe = g.poi("Café Luitpold", 48.143, 11.574, 10);
        g.place_tags(city, &[("name", "München"), ("name:en", "Munich"), ("name:it", "Monaco di Baviera")]);
        g.area_tags(land, &[("name", "Bayern"), ("ISO3166-2", "DE-BY")]);
        g.poi_tags(cafe, &[("name", "Café Luitpold"), ("amenity", "cafe")]);
        let dir = TempDir::new("fixture-tags");

        let geo = g.load(&dir.0);

        let files = SearchFiles::open(&dir.0, &geo).unwrap();
        let tag = |kind, record, key: &str| files.bundle(kind, record).and_then(|b| b.get(key).map(str::to_owned));
        let place_row = |name: &str| (0..geo.place_node_count()).find(|&row| geo.get_string(geo.place_node_record(row).unwrap().name_id) == name).unwrap();
        assert_eq!(tag(Kind::Place, place_row("Munich"), "name:it").as_deref(), Some("Monaco di Baviera"));
        assert_eq!(tag(Kind::Place, place_row("Garching"), "name"), None);
        assert_eq!(tag(Kind::Area, land, "ISO3166-2").as_deref(), Some("DE-BY"));
        assert_eq!(tag(Kind::Poi, cafe, "amenity").as_deref(), Some("cafe"));
    }

    #[test]
    fn a_place_tag_bundle_sits_beside_its_places_row() {
        // A hidden node goes after the visible ones, so the first place added is the last row.
        let mut g = Geo::default();
        let land = g.boundary("Bayern", 4, [47.0, 9.0, 50.0, 14.0]);
        let label = g.place("Bayern label", 3, 48.9, 11.4);
        g.claim(land, label);
        let city = g.place("Munich", 0, 48.137, 11.575);
        g.place_tags(label, &[("name", "Bayern label")]);
        g.place_tags(city, &[("name", "Munich")]);
        let dir = TempDir::new("fixture-place-rows");

        let geo = g.load(&dir.0);

        let files = SearchFiles::open(&dir.0, &geo).unwrap();
        let rows: Vec<(String, String)> = (0..geo.place_node_count())
            .map(|row| {
                let bundle = files.bundle(Kind::Place, row).unwrap();
                (geo.get_string(geo.place_node_record(row).unwrap().name_id).into_owned(), bundle.get("name").unwrap().to_string())
            })
            .collect();
        assert_eq!(rows, [("Munich".into(), "Munich".into()), ("Bayern label".into(), "Bayern label".into())]);
    }
}
