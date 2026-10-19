// Cell-scan diagnosis for find_admin. For each "lat lng" on stdin, gathers the admin polygons
// listed in the query's level-10 cell and its 8 neighbours (the cells main's find_admin scans)
// and tests each once with Index::admin_pip.
//
//   scan_diag <dir> [--lost] < points > report
//
// Default: one JSON summary line over all points:
//   own_* / nb_*: polygons listed in the own cell / only in a neighbour (find_admin's filters:
//     level < 12 or level 15, area > 0, vertices present, not an ISO-less level 2)
//   nb_pip_true: neighbour-only polygons the point is inside (what own-cell-only scanning loses)
//   nb_need: neighbour-only polygons main must test (smaller than the own-cell winner at their
//     level, no own-cell winner, or level 15) and the share a bbox header test rejects
// --lost: one line per (point, neighbour-only polygon containing it):
//   lat lng poly_id level area vertex_count cell_mask(bit0 = own, bit k = k-th neighbour) name
use query_server::{cell_id_at_level, cell_neighbors_at_level, Index, ID_MASK, INTERIOR_FLAG, NO_DATA, POLY_HEADER_BYTES, vertex_scale, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE, DEFAULT_STREET_CELL_LEVEL};
use std::collections::BTreeMap;
use std::io::{BufRead, BufWriter, Write};

// Header-only rejection: true when the encoded ring cannot contain (lat, lng).
fn bbox_rejects(idx: &Index, off: u32, lat: f32, lng: f32) -> bool {
    let h = idx.admin_vertices.read_chunk(off as u64, POLY_HEADER_BYTES as usize);
    let enc = h[0];
    let bmin_lat = f32::from_le_bytes(h[2..6].try_into().unwrap());
    let bmin_lng = f32::from_le_bytes(h[6..10].try_into().unwrap());
    if lng < bmin_lng || lat < bmin_lat - 1e-3 { return true; }
    if enc <= 2 {
        let s = vertex_scale(enc);
        let max_lat = (bmin_lat as f64 + 65535.0 * s) as f32;
        let max_lng = (bmin_lng as f64 + 65535.0 * s) as f32;
        if lng > max_lng || lat > max_lat + 1e-3 { return true; }
    }
    false
}

// --wide: rings that contain the point but are listed in none of the 3x3 cells main scans, only
// further out (up to 4 cells away): one line per (point, ring) with the ring ring-distance in cells
// and whether it is smaller than the 3x3 winner at its level (so a wider scan would change
// find_admin's answer):  lat lng poly_id level area cells_away beats_3x3 name
fn wide(idx: &Index, out: &mut impl Write) {
    let lvl = DEFAULT_ADMIN_CELL_LEVEL;
    for line in std::io::stdin().lock().lines() {
        let line = line.unwrap();
        let mut it = line.split_whitespace();
        let (Some(Ok(lat)), Some(Ok(lng))) = (it.next().map(str::parse::<f64>), it.next().map(str::parse::<f64>)) else { continue };
        let own = cell_id_at_level(lat, lng, lvl);
        let mut dist: BTreeMap<u64, u32> = BTreeMap::new();
        dist.insert(own, 0);
        let mut frontier = vec![own];
        for d in 1..=4 {
            let mut next = Vec::new();
            for &c in &frontier {
                for n in cell_neighbors_at_level(c, lvl) {
                    if !dist.contains_key(&n) { dist.insert(n, d); next.push(n); }
                }
            }
            frontier = next;
        }
        let mut near: BTreeMap<u32, u32> = BTreeMap::new();
        for (&c, &d) in &dist {
            Index::for_each_entry_fb(&idx.admin_entries, Index::lookup_admin_cell_fb(&idx.admin_cells, c), |id| {
                let e = near.entry(id & ID_MASK).or_insert(d);
                if d < *e { *e = d; }
            });
        }
        let ok = |p: &query_server::AdminPolygon| {
            let l = p.admin_level;
            (l < 12 || l == 15) && p.area > 0.0 && p.vertex_count > 0 && p.vertex_offset != NO_DATA && !(l == 2 && p.country_code == 0)
        };
        let mut best3 = [f32::INFINITY; 16];
        for (&pid, &d) in &near {
            let Some(p) = idx.admin_polygon(pid) else { continue };
            if d <= 1 && ok(&p) && idx.admin_pip(&p, lat, lng) && p.area < best3[p.admin_level as usize] { best3[p.admin_level as usize] = p.area; }
        }
        for (&pid, &d) in &near {
            if d <= 1 { continue }
            let Some(p) = idx.admin_polygon(pid) else { continue };
            if !ok(&p) || !idx.admin_pip(&p, lat, lng) { continue }
            let beats = p.admin_level == 15 || p.area < best3[p.admin_level as usize];
            writeln!(out, "{lat:.7} {lng:.7} {pid} {} {} {d} {} {}", p.admin_level, p.area, beats as u8, idx.get_string(p.name_id)).unwrap();
        }
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = args.next().expect("usage: scan_diag <dir> [--lost|--wide]");
    let flag = args.next();
    let lost = flag.as_deref() == Some("--lost");
    let idx = Index::load(&dir, DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE).expect("load");
    let lvl = DEFAULT_ADMIN_CELL_LEVEL;
    let out = std::io::stdout();
    let mut out = BufWriter::new(out.lock());
    if flag.as_deref() == Some("--wide") {
        wide(&idx, &mut out);
        return;
    }
    let mut n_pts = 0u64;
    let mut c: BTreeMap<&str, u64> = BTreeMap::new();
    for line in std::io::stdin().lock().lines() {
        let line = line.unwrap();
        let mut it = line.split_whitespace();
        let (Some(Ok(lat)), Some(Ok(lng))) = (it.next().map(str::parse::<f64>), it.next().map(str::parse::<f64>)) else { continue };
        n_pts += 1;
        let cell = cell_id_at_level(lat, lng, lvl);
        let cells: Vec<u64> = std::iter::once(cell).chain(cell_neighbors_at_level(cell, lvl)).collect();
        // poly_id -> (cell mask, interior-in-own)
        let mut seen: BTreeMap<u32, (u32, bool)> = BTreeMap::new();
        for (k, &cid) in cells.iter().enumerate() {
            Index::for_each_entry_fb(&idx.admin_entries, Index::lookup_admin_cell_fb(&idx.admin_cells, cid), |id| {
                let e = seen.entry(id & ID_MASK).or_insert((0, false));
                e.0 |= 1 << k;
                if k == 0 && id & INTERIOR_FLAG != 0 { e.1 = true; }
            });
        }
        let mut own_best = [f32::INFINITY; 12];
        let mut nb: Vec<(u32, u8, f32, u32, u32, bool, u32)> = Vec::new();
        for (&pid, &(mask, _interior)) in &seen {
            let Some(p) = idx.admin_polygon(pid) else { continue };
            let l = p.admin_level;
            if !(l < 12 || l == 15) || p.area <= 0.0 || p.vertex_count == 0 || p.vertex_offset == NO_DATA { continue }
            if l == 2 && p.country_code == 0 { continue }
            let inside = idx.admin_pip(&p, lat, lng);
            if mask & 1 != 0 {
                *c.entry("own_polys").or_default() += 1;
                *c.entry("own_vertices").or_default() += p.vertex_count as u64;
                if inside && l < 12 && p.area < own_best[l as usize] { own_best[l as usize] = p.area; }
                continue;
            }
            *c.entry("nb_polys").or_default() += 1;
            *c.entry("nb_vertices").or_default() += p.vertex_count as u64;
            nb.push((pid, l, p.area, p.vertex_count, mask, inside, p.vertex_offset));
            if inside {
                *c.entry("nb_pip_true").or_default() += 1;
                if lost {
                    writeln!(out, "{lat:.7} {lng:.7} {pid} {l} {} {} {mask:#x} {}", p.area, p.vertex_count, idx.get_string(p.name_id)).unwrap();
                }
            }
        }
        for &(_pid, l, area, vc, _mask, inside, off) in &nb {
            let need = l == 15 || area < own_best[l as usize];
            if !need { continue }
            *c.entry("nb_need").or_default() += 1;
            *c.entry("nb_need_vertices").or_default() += vc as u64;
            if bbox_rejects(&idx, off, lat as f32, lng as f32) {
                assert!(!inside, "bbox test rejected a containing ring");
                *c.entry("nb_need_bbox_rejected").or_default() += 1;
                *c.entry("nb_need_bbox_rejected_vertices").or_default() += vc as u64;
            }
        }
    }
    if !lost {
        let mut s = format!("{{\"points\":{n_pts}");
        for (k, v) in &c {
            s += &format!(",\"{k}\":{:.3}", *v as f64 / n_pts as f64);
        }
        writeln!(out, "{s}}}").unwrap();
    }
}
