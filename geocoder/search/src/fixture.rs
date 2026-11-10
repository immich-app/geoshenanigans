//! Test fixture: a small geocoder index (rectangular boundaries, place nodes, point POIs) in the
//! builder's file layout (the boundaries' rings as arcs, query_server's arc fixture), and the
//! place-search index built from it the way `place-search build` builds one.
use crate::build::{build_all, held_subs, index_meta, BuildOpts};
use crate::constants::SUB_INDEXES;
use crate::model::PLACE_FLAG_LABEL_OF;
use query_server::arc_fixture::{self, ArcFixture};
use query_server::{
    cell_id_at_level, cell_pairs, AdminPolygon, Index, PlaceNode, PointE7, PoiRecord, DEFAULT_ADMIN_CELL_LEVEL,
    DEFAULT_SEARCH_DISTANCE, DEFAULT_STREET_CELL_LEVEL, NO_DATA, PLACE_FLAG_HIDDEN, STR_TIER_COUNT,
};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Grid steps per side of a boundary's bounding box whose points list it in their admin cells.
const GRID: u32 = 32;

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

#[derive(Default)]
pub struct Geo {
    strings: Vec<u8>,
    /// (record, closed rings as (lat, lng) in degrees, the outer one first)
    polygons: Vec<(AdminPolygon, Vec<Vec<(f32, f32)>>)>,
    nodes: Vec<PlaceNode>,
    pois: Vec<PoiRecord>,
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

    /// A place node of PlaceNode type `place_type` (0 city .. 6 quarter); returns its index.
    pub fn place(&mut self, name: &str, place_type: u8, lat: f32, lng: f32) -> u32 {
        let name_id = self.string(name);
        self.nodes.push(PlaceNode { lat, lng, name_id, place_type, _pad1: 0, _pad2: 0, parent_poly_id: NO_DATA });
        self.nodes.len() as u32 - 1
    }

    /// Node `node` hidden as the label of boundary `poly`, as the builder flags a claimed node.
    pub fn claim(&mut self, poly: u32, node: u32) {
        let pn = &mut self.nodes[node as usize];
        pn._pad1 |= PLACE_FLAG_LABEL_OF | PLACE_FLAG_HIDDEN;
        pn.parent_poly_id = poly;
    }

    /// A point POI with the builder's importance byte; returns its record index.
    pub fn poi(&mut self, name: &str, lat: f32, lng: f32, importance: u8) -> u32 {
        let name_id = self.string(name);
        self.pois.push(PoiRecord {
            lat, lng, vertex_offset: NO_DATA, vertex_count: 0, name_id, category: 0, tier: 1, flags: 0, importance,
            parent_street_id: NO_DATA, parent_postcode_id: NO_DATA, parent_poly_id: NO_DATA,
        });
        self.pois.len() as u32 - 1
    }

    /// The files as the builder writes them, by name. A boundary is listed in the admin cells of
    /// a grid of points over its bounding box and of each place node and POI inside the box.
    fn files(&self) -> Vec<(&'static str, Vec<u8>)> {
        let cell = |lat: f32, lng: f32| cell_id_at_level(lat as f64, lng as f64, DEFAULT_ADMIN_CELL_LEVEL);
        let points: Vec<(f32, f32)> =
            self.nodes.iter().map(|p| (p.lat, p.lng)).chain(self.pois.iter().map(|p| (p.lat, p.lng))).collect();
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
        let nodes: Vec<u8> = self.nodes.iter().flat_map(node_bytes).collect();
        let end = self.strings.len();
        let tiers: Vec<String> = (0..STR_TIER_COUNT)
            .map(|t| format!("{{\"start\": {}, \"end\": {end}}}", if t == 0 { 0 } else { end }))
            .collect();
        let mut files: Vec<(&'static str, Vec<u8>)> = arcs.files().into();
        files.extend([
            ("admin_cells.bin", admin_cells), ("admin_entries.bin", admin_entries),
            ("place_nodes.bin", nodes),
            ("strings_core.bin", self.strings.clone()),
            ("strings_layout.json", format!("{{\"tiers\": [{}]}}", tiers.join(", ")).into_bytes()),
        ]);
        if !self.pois.is_empty() {
            let (poi_cells, poi_entries) = cell_pairs::write_pairs(
                self.pois.iter().enumerate().map(|(i, p)| (cell(p.lat, p.lng), i as u32)),
                cell_pairs::POI,
            );
            files.extend([
                ("poi_records.bin", self.pois.iter().flat_map(poi_bytes).collect()),
                ("poi_cells.bin", poi_cells), ("poi_entries.bin", poi_entries), ("poi_vertices.bin", Vec::new()),
            ]);
        }
        files
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

/// Builds every sub-index of `geo` into `out` with the default options and `extras`, and the
/// search_meta.json fields `SearchIndex::open` reads.
pub fn build_search(geo: &Index, out: &Path, extras: Option<&Path>) {
    let opts = BuildOpts { extras: extras.map(|p| p.to_str().unwrap().to_string()), ..BuildOpts::new(out.join("tmp")) };
    let meta = index_meta(geo, &opts).into();
    build_all(geo, &held_subs(geo, SUB_INDEXES), out, &opts, || Ok(meta)).unwrap();
}

/// A ring of (lat, lng) degrees on the e7 grid.
fn e7_ring(ring: &[(f32, f32)]) -> Vec<PointE7> {
    ring.iter().map(|&(lat, lng)| arc_fixture::e7(lat as f64, lng as f64)).collect()
}

fn node_bytes(p: &PlaceNode) -> Vec<u8> {
    let b = [
        &p.lat.to_le_bytes()[..], &p.lng.to_le_bytes(), &p.name_id.to_le_bytes(), &[p.place_type, p._pad1],
        &p._pad2.to_le_bytes(), &p.parent_poly_id.to_le_bytes(),
    ]
    .concat();
    assert_eq!(b.len(), std::mem::size_of::<PlaceNode>());
    b
}

fn poi_bytes(p: &PoiRecord) -> Vec<u8> {
    let b = [
        &p.lat.to_le_bytes()[..], &p.lng.to_le_bytes(), &p.vertex_offset.to_le_bytes(), &p.vertex_count.to_le_bytes(),
        &p.name_id.to_le_bytes(), &[p.category, p.tier, p.flags, p.importance], &p.parent_street_id.to_le_bytes(),
        &p.parent_postcode_id.to_le_bytes(), &p.parent_poly_id.to_le_bytes(),
    ]
    .concat();
    assert_eq!(b.len(), std::mem::size_of::<PoiRecord>());
    b
}
