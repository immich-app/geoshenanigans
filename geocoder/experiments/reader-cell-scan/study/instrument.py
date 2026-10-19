"""instrument.py <lib.rs>: scratch-copy counters for the admin read path (one relaxed add each):
cell index lookups, admin polygon records read, admin rings whose vertices are read, and those
vertices (admin_verts decodes and point_in_encoded_polygon scans after its header test)."""
import sys

p = sys.argv[1]
s = open(p).read()
COUNTERS = ["RG_LOOKUPS", "RG_RECORDS", "RG_RINGS", "RG_VERTS"]
decl = "".join(f"pub static {c}: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);\n" for c in COUNTERS)
decl += "#[inline(always)] fn rg_add(c: &std::sync::atomic::AtomicU64, n: u64) { c.fetch_add(n, std::sync::atomic::Ordering::Relaxed); }\n\n"
s = s.replace("// --- S2 helpers ---", decl + "// --- S2 helpers ---", 1)


def inject(after, code, required=True):
    global s
    n = s.count(after)
    if n != 1:
        assert not required, (after, n)
        return False
    s = s.replace(after, after + code)
    return True


inject("    pub fn lookup_admin_cell_fb(cells: &FileBytes, cell_id: u64) -> u32 {\n", "        rg_add(&RG_LOOKUPS, 1);\n")
inject("    pub fn lookup_admin_cells_fb(cells: &FileBytes, ids: &[u64], out: &mut [u32]) {\n", "        rg_add(&RG_LOOKUPS, ids.len() as u64);\n", False)
inject("    pub fn admin_polygon(&self, idx: u32) -> Option<AdminPolygon> {\n", "        rg_add(&RG_RECORDS, 1);\n")
inject("    pub fn admin_verts(&self, poly: &AdminPolygon) -> Option<std::borrow::Cow<'_, [NodeCoord]>> {\n",
       "        rg_add(&RG_RINGS, 1);\n        rg_add(&RG_VERTS, poly.vertex_count as u64);\n")
# point_in_encoded_polygon: count where it reads the vertices (after any header test)
inject("    let bytes = src.read_chunk(verts_off, n * stride);\n    let scale = vertex_scale(encoding);\n",
       "    rg_add(&RG_RINGS, 1);\n    rg_add(&RG_VERTS, n as u64);\n", False)
open(p, "w").write(s)
print("instrumented", p)
