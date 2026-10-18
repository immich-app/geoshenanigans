// Load-time rebuild of place_cells.bin + place_entries.bin from place_nodes.bin with the
// server's s2 crate (lib.rs cell_id_at_level, level 10), single thread. Copy of
// places-strings-index/rbench with an output dir. Mode "live" skips name_id == NO_DATA
// slots (V11 zeroes hidden slots and leaves them out of the lists); mode "exact" skips only
// lat=lng=0 tombstones with name_id NO_DATA (what the builder does for the shipped set).
// usage: rbench <nodes_dir> live|exact <out_dir>
use s2::cellid::CellID;
use s2::latlng::LatLng;
use std::time::Instant;
fn main() {
    let a: Vec<String> = std::env::args().collect();
    let dir = &a[1];
    let live_only = a[2] == "live";
    let out = &a[3];
    let t_read = Instant::now();
    let nodes = std::fs::read(format!("{}/place_nodes.bin", dir)).unwrap();
    let read_s = t_read.elapsed().as_secs_f64();
    let n = nodes.len() / 20;
    let t0 = Instant::now();
    let mut pairs: Vec<(u64, u32)> = Vec::with_capacity(n);
    for i in 0..n {
        let o = i * 20;
        let lat = f32::from_le_bytes(nodes[o..o + 4].try_into().unwrap());
        let lng = f32::from_le_bytes(nodes[o + 4..o + 8].try_into().unwrap());
        let name = u32::from_le_bytes(nodes[o + 8..o + 12].try_into().unwrap());
        if name == u32::MAX && (live_only || (lat == 0.0 && lng == 0.0)) { continue; }
        let c = CellID::from(LatLng::from_degrees(lat as f64, lng as f64)).parent(10).0;
        pairs.push((c, i as u32));
    }
    let t1 = t0.elapsed().as_secs_f64();
    pairs.sort_unstable();
    let t2 = t0.elapsed().as_secs_f64();
    let mut cells: Vec<u8> = Vec::new();
    let mut entries: Vec<u8> = Vec::new();
    let m = pairs.len();
    let mut s = 0;
    while s < m {
        let c = pairs[s].0;
        let mut e = s;
        while e < m && pairs[e].0 == c { e += 1; }
        cells.extend_from_slice(&c.to_le_bytes());
        cells.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        entries.extend_from_slice(&((e - s) as u16).to_le_bytes());
        for k in s..e { entries.extend_from_slice(&pairs[k].1.to_le_bytes()); }
        s = e;
    }
    let t3 = t0.elapsed().as_secs_f64();
    std::fs::create_dir_all(out).unwrap();
    std::fs::write(format!("{}/place_cells.bin", out), &cells).unwrap();
    std::fs::write(format!("{}/place_entries.bin", out), &entries).unwrap();
    println!("records {} indexed {} | read {:.3}s cell ids {:.3}s sort {:.3}s build {:.3}s total(after read) {:.3}s | out cells {} B entries {} B | heap pairs {} MiB",
        n, m, read_s, t1, t2 - t1, t3 - t2, t3, cells.len(), entries.len(), m * 16 / 1048576);
}
