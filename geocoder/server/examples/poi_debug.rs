// One-off: dump cell-entry flags + stored geometry for POIs matching a name
// substring at a query point. Usage: poi_debug <index_dir> <lat> <lng> <name_substr>
use query_server::*;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = &args[1];
    let lat: f64 = args[2].parse().unwrap();
    let lng: f64 = args[3].parse().unwrap();
    let needle = &args[4];
    let idx = Index::load(dir, 17, 10, 200.0).unwrap();

    let poi_cells = idx.poi_cells.as_ref().unwrap();
    let poi_entries = idx.poi_entries.as_ref().unwrap();
    let cell = cell_id_at_level(lat, lng, idx.admin_cell_level);
    let neighbors = cell_neighbors_at_level(cell, idx.admin_cell_level);

    for c in std::iter::once(cell).chain(neighbors.into_iter()) {
        Index::for_each_entry_fb(poi_entries, Index::lookup_admin_cell_fb(poi_cells, c), |id| {
            let is_interior = (id & INTERIOR_FLAG) != 0;
            let poi_id = id & ID_MASK;
            let Some(poi) = idx.poi_record(poi_id) else { return; };
            if poi.name_id == NO_DATA { return; }
            let name = idx.get_string(poi.name_id);
            if !name.contains(needle.as_str()) { return; }
            println!("POI id={} name={:?}", poi_id, name);
            println!("  cell={:x} is_interior={}", c, is_interior);
            println!("  centroid=({}, {}) category={} tier={} flags={:#x} importance={}",
                     poi.lat, poi.lng, poi.category, poi.tier, poi.flags, poi.importance);
            println!("  vertex_offset={} vertex_count={}", poi.vertex_offset, poi.vertex_count);
            if poi.vertex_count > 0 && poi.vertex_offset != NO_DATA {
                if let Some(verts) = idx.poi_verts(&poi) {
                    let (mut mnla, mut mxla, mut mnlo, mut mxlo) = (f32::MAX, f32::MIN, f32::MAX, f32::MIN);
                    for v in verts.iter() {
                        mnla = mnla.min(v.lat); mxla = mxla.max(v.lat);
                        mnlo = mnlo.min(v.lng); mxlo = mxlo.max(v.lng);
                    }
                    let pip = point_in_polygon(lat as f32, lng as f32, &verts);
                    println!("  decoded_verts={} bbox=({:.5},{:.5})..({:.5},{:.5}) pip={}",
                             verts.len(), mnla, mnlo, mxla, mxlo, pip);
                    // closing-edge sanity: distance between first and last vertex
                    let f = verts[0]; let l = verts[verts.len()-1];
                    println!("  first=({:.5},{:.5}) last=({:.5},{:.5}) closed={}",
                             f.lat, f.lng, l.lat, l.lng,
                             (f.lat - l.lat).abs() < 1e-6 && (f.lng - l.lng).abs() < 1e-6);
                    // count large jumps between consecutive vertices (ring concatenation smell)
                    let mut jumps = 0;
                    for i in 1..verts.len() {
                        let d = ((verts[i].lat - verts[i-1].lat).powi(2) + (verts[i].lng - verts[i-1].lng).powi(2)).sqrt();
                        if d > 0.05 { jumps += 1; }
                    }
                    println!("  large_jumps(>0.05deg)={}", jumps);
                }
            }
        });
    }
}
