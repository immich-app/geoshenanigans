"""instrument.py <lib.rs>: count admin ring reads (rings, vertices) in Index::admin_verts and,
where it exists, Index::admin_pip. Scratch copies only; the counters cost one relaxed add."""
import sys

p = sys.argv[1]
s = open(p).read()
s = s.replace(
    "// --- S2 helpers ---",
    "pub static RG_RING_CALLS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);\n"
    "pub static RG_RING_VERTS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);\n"
    "fn rg_count(n: u32) {\n"
    "    RG_RING_CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);\n"
    "    RG_RING_VERTS.fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);\n"
    "}\n\n// --- S2 helpers ---",
    1,
)
old = "    pub fn admin_verts(&self, poly: &AdminPolygon) -> Option<std::borrow::Cow<'_, [NodeCoord]>> {\n"
assert s.count(old) == 1
s = s.replace(old, old + "        rg_count(poly.vertex_count);\n")
old = "    pub fn admin_pip(&self, poly: &AdminPolygon, lat: f64, lng: f64) -> bool {\n"
if old in s:
    s = s.replace(old, old + "        rg_count(poly.vertex_count);\n")
open(p, "w").write(s)
print("ok", "admin_pip" if old in s else "no admin_pip")
