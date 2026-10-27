//! Test fixture: a small geocoder index (rectangular boundaries, place nodes, point POIs) in the
//! builder's file layout, and the place-search index built from it the way `place-search build`
//! builds one.
use crate::build::{build, BuildOpts};
use crate::constants::DEFAULT_RUN_BUDGET_MB;
use crate::model::{PLACE_FLAG_HIDDEN, PLACE_FLAG_LABEL_OF};
use query_server::{
    cell_id_at_level, AdminPolygon, Index, PlaceNode, PoiRecord, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE,
    DEFAULT_STREET_CELL_LEVEL, NO_DATA, STR_TIER_COUNT,
};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Raw f32 vertices (VertexEncoding in builder/src/types.h).
const RAW_F32_VERTICES: u8 = 4;

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
    /// (record, [south, west, north, east] in degrees)
    polygons: Vec<(AdminPolygon, [f32; 4])>,
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
        let poly = AdminPolygon {
            vertex_offset: 0, vertex_count: 5, name_id: self.string(name), admin_level, place_type_override: 0,
            _pad2: 0, _pad3: 0, area: (n - s) * (e - w), country_code: 0, hole_count: 0,
        };
        self.polygons.push((poly, rect));
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

    /// The files as the builder writes them, by name. A boundary is listed in the admin cell of
    /// its centre and of each place node and POI inside it.
    fn files(&self) -> Vec<(&'static str, Vec<u8>)> {
        let cell = |lat: f32, lng: f32| cell_id_at_level(lat as f64, lng as f64, DEFAULT_ADMIN_CELL_LEVEL);
        let points: Vec<(f32, f32)> =
            self.nodes.iter().map(|p| (p.lat, p.lng)).chain(self.pois.iter().map(|p| (p.lat, p.lng))).collect();
        let (mut polygons, mut vertices, mut admin) = (Vec::new(), Vec::new(), Vec::new());
        for (id, (poly, [s, w, n, e])) in self.polygons.iter().enumerate() {
            polygons.extend(polygon_bytes(&AdminPolygon { vertex_offset: vertices.len() as u32, ..*poly }));
            vertices.extend([RAW_F32_VERTICES, 0]);
            for v in [s, w, s, w, s, e, n, e, n, w, s, w] {
                vertices.extend(v.to_le_bytes());
            }
            let centre = ((s + n) / 2.0, (w + e) / 2.0);
            for (lat, lng) in points.iter().copied().chain([centre]) {
                if (*s..=*n).contains(&lat) && (*w..=*e).contains(&lng) {
                    admin.push((cell(lat, lng), id as u32));
                }
            }
        }
        let (admin_cells, admin_entries) = cell_files(admin);
        let nodes: Vec<u8> = self.nodes.iter().flat_map(node_bytes).collect();
        let (place_cells, place_entries) =
            cell_files(self.nodes.iter().enumerate().map(|(i, p)| (cell(p.lat, p.lng), i as u32)).collect());
        let end = self.strings.len();
        let tiers: Vec<String> = (0..STR_TIER_COUNT)
            .map(|t| format!("{{\"start\": {}, \"end\": {end}}}", if t == 0 { 0 } else { end }))
            .collect();
        let mut files = vec![
            ("admin_polygons.bin", polygons), ("admin_vertices.bin", vertices),
            ("admin_cells.bin", admin_cells), ("admin_entries.bin", admin_entries),
            ("place_nodes.bin", nodes), ("place_cells.bin", place_cells), ("place_entries.bin", place_entries),
            ("strings_core.bin", self.strings.clone()),
            ("strings_layout.json", format!("{{\"tiers\": [{}]}}", tiers.join(", ")).into_bytes()),
        ];
        if !self.pois.is_empty() {
            let (poi_cells, poi_entries) =
                cell_files(self.pois.iter().enumerate().map(|(i, p)| (cell(p.lat, p.lng), i as u32)).collect());
            files.extend([
                ("poi_records.bin", self.pois.iter().flat_map(poi_bytes).collect()),
                ("poi_cells.bin", poi_cells), ("poi_entries.bin", poi_entries), ("poi_vertices.bin", Vec::new()),
            ]);
        }
        files
    }

    /// Writes the files into `dir` and loads them.
    pub fn load(&self, dir: &Path) -> Index {
        std::fs::create_dir_all(dir).unwrap();
        for (name, bytes) in self.files() {
            std::fs::write(dir.join(name), bytes).unwrap();
        }
        Index::load(dir.to_str().unwrap(), DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE)
            .unwrap()
    }
}

/// Builds every sub-index of `geo` into `out` with the default options and `extras`, and the
/// search_meta.json fields `SearchIndex::open` reads.
pub fn build_search(geo: &Index, out: &Path, extras: Option<&Path>) {
    let opts = BuildOpts {
        threads: 1, mem_mb: DEFAULT_RUN_BUDGET_MB, tmp: out.join("tmp"), inline: true,
        extras: extras.map(|p| p.to_str().unwrap().to_string()), imp_dir: None, area_proxy: false, poi_max_tier: 3,
    };
    let names: &[&str] = if geo.poi_records.is_some() { &["places", "poi"] } else { &["places"] };
    build(geo, names, out, &opts).unwrap();
    let n_place_nodes = geo.place_nodes.as_ref().map_or(0, |m| m.len() / std::mem::size_of::<PlaceNode>() as u64);
    let meta = serde_json::json!({"n_place_nodes": n_place_nodes, "extras": extras.is_some()});
    std::fs::write(out.join("search_meta.json"), meta.to_string()).unwrap();
}

/// A `*_cells.bin` (u64 cell, u32 entry offset; sorted) and its `*_entries.bin` (u16 count, then
/// the u32 ids) of (cell, id) pairs.
fn cell_files(mut pairs: Vec<(u64, u32)>) -> (Vec<u8>, Vec<u8>) {
    pairs.sort();
    pairs.dedup();
    let (mut cells, mut entries) = (Vec::new(), Vec::new());
    for group in pairs.chunk_by(|a, b| a.0 == b.0) {
        cells.extend(group[0].0.to_le_bytes());
        cells.extend((entries.len() as u32).to_le_bytes());
        entries.extend((group.len() as u16).to_le_bytes());
        for (_, id) in group {
            entries.extend(id.to_le_bytes());
        }
    }
    (cells, entries)
}

fn polygon_bytes(p: &AdminPolygon) -> Vec<u8> {
    let b = [
        &p.vertex_offset.to_le_bytes()[..], &p.vertex_count.to_le_bytes(), &p.name_id.to_le_bytes(),
        &[p.admin_level, p.place_type_override, p._pad2, p._pad3], &p.area.to_le_bytes(),
        &p.country_code.to_le_bytes(), &p.hole_count.to_le_bytes(),
    ]
    .concat();
    assert_eq!(b.len(), std::mem::size_of::<AdminPolygon>());
    b
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
