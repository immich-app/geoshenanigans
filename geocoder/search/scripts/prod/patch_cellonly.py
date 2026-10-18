"""One-off patch: find_admin scans only the query's own admin cell."""
import sys

p = sys.argv[1]
s = open(p).read()
a = s.index("    pub fn find_admin(&self, lat: f64, lng: f64) -> AdminResult<'_> {")
b = s.index("        let mut result = AdminResult::default();", a)
seg = s[a:b]
old1 = """        let cell = cell_id_at_level(lat, lng, self.admin_cell_level);
        let neighbors = cell_neighbors_at_level(cell, self.admin_cell_level);
"""
new1 = """        // A polygon containing the point always lists the point's own cell in its covering, so
        // only that cell's polygons can contain it (the neighbours used to be scanned too).
        let cell = cell_id_at_level(lat, lng, self.admin_cell_level);
"""
assert seg.count(old1) == 1
seg = seg.replace(old1, new1)
assert seg.count("for c in std::iter::once(cell).chain(neighbors.into_iter()) {") == 1
seg = seg.replace("for c in std::iter::once(cell).chain(neighbors.into_iter()) {", "for c in std::iter::once(cell) {")
old2 = """        let neighbors2 = cell_neighbors_at_level(cell, self.admin_cell_level);
        for c in std::iter::once(cell).chain(neighbors2.into_iter()) {"""
assert seg.count(old2) == 1
seg = seg.replace(old2, """        for c in std::iter::once(cell) {""")
s = s[:a] + seg + s[b:]
open(p, "w").write(s)
print("ok")
