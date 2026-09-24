// Dump postcode-resolution internals at a point: way_postcodes for the
// nearest street, and tier-2 centroid candidates with distances/gates.
use query_server::*;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = &args[1];
    let lat: f64 = args[2].parse().unwrap();
    let lng: f64 = args[3].parse().unwrap();
    let idx = Index::load(dir, 17, 10, 200.0).unwrap();

    let (addr, _interp, street) = idx.query_geo(lat, lng);
    println!("addr: {:?}", addr.as_ref().map(|(d, _, id)| (d.sqrt() * 6_371_000.0, *id)));
    if let Some((d, w, wid)) = &street {
        println!("street: idx={} dist={:.1}m name={:?}", wid, d.sqrt() * 6_371_000.0,
                 if w.name_id != NO_DATA { idx.get_string(w.name_id) } else { "-" });
        if let Some(wpc) = idx.way_postcodes.as_ref() {
            let off = (*wid as u64) * 4;
            if off + 4 <= wpc.len() {
                let b = wpc.read_chunk(off, 4);
                let pid = u32::from_le_bytes(b[..].try_into().unwrap());
                println!("way_postcodes[{}] = {}", wid,
                         if pid == NO_DATA { "NO_DATA".into() } else { format!("{:?}", idx.get_string(pid)) });
            }
        } else { println!("way_postcodes: FILE ABSENT"); }
    }
    // tier-2 centroids near the point
    println!("centroid files: centroids={} cells={} entries={}",
             idx.postcode_centroids.is_some(), idx.postcode_centroid_cells.is_some(),
             idx.postcode_centroid_entries.is_some());
    if let (Some(cells), Some(entries)) = (idx.postcode_centroid_cells.as_ref(), idx.postcode_centroid_entries.as_ref()) {
        let cell = cell_id_at_level(lat, lng, idx.admin_cell_level);
        let neighbors = cell_neighbors_at_level(cell, idx.admin_cell_level);
        let cos_lat = lat.to_radians().cos();
        let mut n = 0;
        for c in std::iter::once(cell).chain(neighbors.into_iter()) {
            Index::for_each_entry_fb(entries, Index::lookup_admin_cell_fb(cells, c), |id| {
                let Some(pc) = idx.postcode_centroid(id) else { return; };
                let dlat = (lat - pc.lat as f64).to_radians();
                let dlng = (lng - pc.lng as f64).to_radians();
                let d = (dlat * dlat + dlng * dlng * cos_lat * cos_lat).sqrt() * 6_371_000.0;
                if d < 3000.0 && n < 12 {
                    let cc = [(pc.country_code >> 8) as u8, (pc.country_code & 0xff) as u8];
                    println!("  centroid id={} pc={:?} cc={:?}({:#06x}) dist={:.0}m ok={}",
                             id, idx.get_string(pc.postcode_id),
                             String::from_utf8_lossy(&cc), pc.country_code, d,
                             centroid_postcode_ok(idx.get_string(pc.postcode_id)));
                    n += 1;
                }
            });
        }
        if n == 0 { println!("  (no centroids within 3km in cell+neighbors)"); }
    }
}
