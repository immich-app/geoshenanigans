"""tiers3: count the admin vertices a reader touches per query, a machine-independent measure of
point-in-polygon work: calls and vertices of admin_verts (decode into memory; main's find_admin
decodes every candidate) and of admin_pip (feat/place-search-prod: ray cast over the encoded ring,
one vertex at a time). grid_query prints the totals to stderr at exit. No behaviour change.
usage: count_patch3.py <lib.rs> <grid_query.rs>"""
import sys

lib, gq = sys.argv[1:3]
s = open(lib).read()
old = """    pub fn admin_verts(&self, poly: &AdminPolygon) -> Option<std::borrow::Cow<'_, [NodeCoord]>> {
        decode_polygon_verts(&self.admin_vertices, poly.vertex_offset, poly.vertex_count)
    }
"""
assert s.count(old) == 1
s = s.replace(old, """    pub fn admin_verts(&self, poly: &AdminPolygon) -> Option<std::borrow::Cow<'_, [NodeCoord]>> {
        ADMIN_DECODES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        ADMIN_DECODED_VERTS.fetch_add(poly.vertex_count as u64, std::sync::atomic::Ordering::Relaxed);
        decode_polygon_verts(&self.admin_vertices, poly.vertex_offset, poly.vertex_count)
    }
""")
old_pip = """    pub fn admin_pip(&self, poly: &AdminPolygon, lat: f64, lng: f64) -> bool {
"""
if old_pip in s:
    s = s.replace(old_pip, old_pip + """        ADMIN_PIPS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        ADMIN_PIP_VERTS.fetch_add(poly.vertex_count as u64, std::sync::atomic::Ordering::Relaxed);
""")
s = s.replace("pub const INTERIOR_FLAG: u32 = 0x80000000;", """pub const INTERIOR_FLAG: u32 = 0x80000000;
/// tiers3 study counters: admin_verts / admin_pip calls and the vertices they touch.
pub static ADMIN_DECODES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static ADMIN_DECODED_VERTS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static ADMIN_PIPS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static ADMIN_PIP_VERTS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);""", 1)
open(lib, "w").write(s)
g = open(gq).read()
old_g = """        writeln!(out, "{:.6} {:.6} {}", lat, lng, idx.query_json(lat, lng)).unwrap();
    }
"""
assert g.count(old_g) == 1
g = g.replace(old_g, old_g + """    let ld = |a: &std::sync::atomic::AtomicU64| a.load(std::sync::atomic::Ordering::Relaxed);
    eprintln!("admin_decodes {} admin_decoded_verts {} admin_pips {} admin_pip_verts {}",
              ld(&query_server::ADMIN_DECODES), ld(&query_server::ADMIN_DECODED_VERTS),
              ld(&query_server::ADMIN_PIPS), ld(&query_server::ADMIN_PIP_VERTS));
""")
open(gq, "w").write(g)
print("patched")
