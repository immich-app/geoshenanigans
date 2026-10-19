// Debug probe for admin resolution: for each "lat lng" line on stdin,
// prints one JSON line with
//   pip:   every admin polygon (cell + neighbours) whose ring contains the
//          point, as find_admin sees them (id, level, name, cc, area, verts)
//   street: nearest street way (index, name) and its way_parents start id
//   chain: the admin_parents walk from that start (id, level, name, cc)
//
//   admin_probe <flat-dir> [osm_ids sidecar] < coords
use query_server::{
    cell_id_at_level, cell_neighbors_at_level, point_in_polygon, Index, DEFAULT_ADMIN_CELL_LEVEL,
    DEFAULT_SEARCH_DISTANCE, DEFAULT_STREET_CELL_LEVEL, ID_MASK, NO_DATA,
};
use std::io::{BufRead, BufWriter, Write};

fn cc_str(cc: u16) -> String {
    if cc == 0 {
        return String::new();
    }
    format!("{}{}", (cc >> 8) as u8 as char, (cc & 0xFF) as u8 as char)
}

fn main() {
    let dir = std::env::args().nth(1).expect("usage: admin_probe <dir> [sidecar]");
    let sidecar: Option<Vec<u8>> = std::env::args().nth(2).map(|p| std::fs::read(p).expect("sidecar"));
    let osm_id = |pid: u32| -> String {
        // Sidecar: 12-byte header (magic, version, count) then 12-byte
        // slots {u8 type, u8 flags, u8 tier, u8 reserved, u64 stable_id}.
        let Some(s) = sidecar.as_ref() else { return String::new() };
        let off = 12 + pid as usize * 12;
        if off + 12 > s.len() {
            return String::new();
        }
        let ty = s[off] as u64;
        let stable = u64::from_le_bytes(s[off + 4..off + 12].try_into().unwrap());
        if ty == 3 {
            format!("r{}/{}", stable >> 16, stable & 0xFFFF)
        } else {
            format!("t{}:{}", ty, stable)
        }
    };
    let idx = Index::load(&dir, DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE)
        .expect("load");
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    let poly_json = |pid: u32| -> serde_json::Value {
        match idx.admin_polygon(pid) {
            Some(p) => serde_json::json!({
                "id": pid, "osm": osm_id(pid), "level": p.admin_level, "name": idx.get_string(p.name_id),
                "cc": cc_str(p.country_code), "area": p.area, "nv": p.vertex_count,
                "pto": p.place_type_override, "holes": p.hole_count,
            }),
            None => serde_json::json!({"id": pid, "tombstone": true}),
        }
    };
    for line in stdin.lock().lines() {
        let line = line.unwrap();
        let mut it = line.split_whitespace();
        let (Some(a), Some(b)) = (it.next(), it.next()) else { continue };
        let (Ok(lat), Ok(lng)) = (a.parse::<f64>(), b.parse::<f64>()) else { continue };
        let cell = cell_id_at_level(lat, lng, DEFAULT_ADMIN_CELL_LEVEL);
        let mut seen = std::collections::BTreeSet::new();
        let mut pip = Vec::new();
        let mut own_cell = Vec::new();
        let mut in_hole = Vec::new();
        for c in std::iter::once(cell).chain(cell_neighbors_at_level(cell, DEFAULT_ADMIN_CELL_LEVEL)) {
            Index::for_each_entry_fb(&idx.admin_entries, Index::lookup_admin_cell_fb(&idx.admin_cells, c), |id| {
                let pid = id & ID_MASK;
                if c == cell {
                    own_cell.push(pid);
                }
                if !seen.insert(pid) {
                    return;
                }
                let Some(p) = idx.admin_polygon(pid) else { return };
                let Some(v) = idx.admin_verts(&p) else { return };
                if point_in_polygon(lat as f32, lng as f32, &v) {
                    pip.push(poly_json(pid));
                } else if p.hole_count > 0 {
                    // Outer ring alone: it ends at the first repeat of v0.
                    let n = (3..v.len()).find(|&i| v[i].lat == v[0].lat && v[i].lng == v[0].lng).map_or(v.len(), |i| i + 1);
                    if point_in_polygon(lat as f32, lng as f32, &v[..n]) {
                        in_hole.push(poly_json(pid));
                    }
                }
            });
        }
        let (_, _, street) = idx.query_geo(lat, lng);
        let mut street_json = serde_json::Value::Null;
        let mut chain = Vec::new();
        if let Some((d, w, wi)) = street {
            let start = idx
                .way_parents
                .as_ref()
                .and_then(|wp| {
                    let off = wi as u64 * 4;
                    if off + 4 > wp.len() {
                        None
                    } else {
                        Some(wp.read_at::<u32>(wi as u64))
                    }
                })
                .unwrap_or(NO_DATA);
            street_json = serde_json::json!({"way": wi, "d2": d, "name": idx.get_string(w.name_id), "parent": start});
            let mut cur = start;
            for _ in 0..15 {
                if cur == NO_DATA {
                    break;
                }
                chain.push(poly_json(cur));
                let Some(ap) = idx.admin_parents.as_ref() else { break };
                if cur as u64 * 4 + 4 > ap.len() {
                    break;
                }
                cur = ap.read_at::<u32>(cur as u64);
            }
        }
        let j = serde_json::json!({"lat": lat, "lng": lng, "pip": pip, "in_hole": in_hole, "own_cell_n": own_cell.len(),
                                   "street": street_json, "chain": chain});
        writeln!(out, "{}", j).unwrap();
    }
}
