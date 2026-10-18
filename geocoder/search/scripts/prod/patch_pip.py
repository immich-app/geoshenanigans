"""One-off patch (server lib clone + search query.rs): point-in-polygon over the encoded vertices,
decoded one at a time, for find_admin (no ring allocation)."""
import sys

LIB, Q = sys.argv[1], sys.argv[2]
s = open(LIB).read()


def rep(old, new, count=1):
    global s
    assert s.count(old) == count, (s.count(old), old[:100])
    s = s.replace(old, new)


rep('''// --- API types ---

pub struct PoiMatch<'a> {''', '''/// `point_in_polygon` over a polygon's encoded vertices (the layout `decode_polygon_verts`
/// reads), decoding one vertex at a time to the same f32 values: no allocation, one pass. None
/// exactly where `decode_polygon_verts` is None.
pub fn point_in_encoded_polygon(src: &FileBytes, vertex_offset: u32, vertex_count: u32, lat: f32, lng: f32) -> Option<bool> {
    if vertex_count == 0 || vertex_offset == NO_DATA { return None; }
    let off = vertex_offset as u64;
    if off + POLY_HEADER_BYTES > src.len() { return None; }
    let header = src.read_chunk(off, POLY_HEADER_BYTES as usize);
    let encoding = header[0];
    let bmin_lat = f32::from_le_bytes(header[2..6].try_into().unwrap()) as f64;
    let bmin_lng = f32::from_le_bytes(header[6..10].try_into().unwrap()) as f64;
    drop(header);
    let stride = vertex_stride(encoding);
    let n = vertex_count as usize;
    let verts_off = off + POLY_HEADER_BYTES;
    if verts_off + (n * stride) as u64 > src.len() { return None; }
    let bytes = src.read_chunk(verts_off, n * stride);
    let scale = vertex_scale(encoding);
    let at = |i: usize| -> (f32, f32) {
        let b = &bytes[i * stride..i * stride + stride];
        match encoding {
            4 => (f32::from_le_bytes(b[0..4].try_into().unwrap()), f32::from_le_bytes(b[4..8].try_into().unwrap())),
            3 => (
                (bmin_lat + u32::from_le_bytes(b[0..4].try_into().unwrap()) as f64 * scale) as f32,
                (bmin_lng + u32::from_le_bytes(b[4..8].try_into().unwrap()) as f64 * scale) as f32,
            ),
            _ => (
                (bmin_lat + u16::from_le_bytes(b[0..2].try_into().unwrap()) as f64 * scale) as f32,
                (bmin_lng + u16::from_le_bytes(b[2..4].try_into().unwrap()) as f64 * scale) as f32,
            ),
        }
    };
    let mut inside = false;
    let (mut vj_lat, mut vj_lng) = at(n - 1);
    for i in 0..n {
        let (vi_lat, vi_lng) = at(i);
        if ((vi_lng > lng) != (vj_lng > lng))
            && (lat < (vj_lat - vi_lat) * (lng - vi_lng) / (vj_lng - vi_lng) + vi_lat)
        {
            inside = !inside;
        }
        vj_lat = vi_lat;
        vj_lng = vi_lng;
    }
    Some(inside)
}

// --- API types ---

pub struct PoiMatch<'a> {''')

rep('''    pub fn admin_verts(&self, poly: &AdminPolygon) -> Option<std::borrow::Cow<'_, [NodeCoord]>> {''', '''    /// Point-in-polygon on an admin ring without decoding it into memory (false where
    /// `admin_verts` is None).
    pub fn admin_pip(&self, poly: &AdminPolygon, lat: f64, lng: f64) -> bool {
        point_in_encoded_polygon(&self.admin_vertices, poly.vertex_offset, poly.vertex_count, lat as f32, lng as f32).unwrap_or(false)
    }

    pub fn admin_verts(&self, poly: &AdminPolygon) -> Option<std::borrow::Cow<'_, [NodeCoord]>> {''')

rep('''                let pip_now = || self.admin_verts(&poly)
                    .map(|v| point_in_polygon(lat as f32, lng as f32, &v))
                    .unwrap_or(false);''', '''                let pip_now = || self.admin_pip(&poly, lat, lng);''')
rep('''                            let bverts = self.admin_verts(&best_poly);
                            let best_inside = bverts.as_deref().map(|b| point_in_polygon(lat as f32, lng as f32, b)).unwrap_or(false);''',
    '''                            let best_inside = self.admin_pip(&best_poly, lat, lng);''')
rep('''                let inside = self.admin_verts(&poly)
                    .map(|v| point_in_polygon(lat as f32, lng as f32, &v))
                    .unwrap_or(false);
                if inside {
                    best_by_level[level] = Some((area, poly, true));''', '''                let inside = self.admin_pip(&poly, lat, lng);
                if inside {
                    best_by_level[level] = Some((area, poly, true));''')
rep('''                if poly.admin_level != 15 { return; }
                if poly.area <= 0.0 { return; }
                let Some(verts) = self.admin_verts(&poly) else { return; };
                if !point_in_polygon(lat as f32, lng as f32, &verts) { return; }''', '''                if poly.admin_level != 15 { return; }
                if poly.area <= 0.0 { return; }
                if !self.admin_pip(&poly, lat, lng) { return; }''', count=2)
open(LIB, "w").write(s)

q = open(Q).read()
old = q[q.index("    fn inside(&self, st: &mut State<'g>"):q.index("    /// Boundaries covering a point's admin cell")]
q = q.replace(old, '''    fn inside(&self, ap: &AdminPolygon, lat: f32, lng: f32) -> bool {
        self.idx.geo.admin_pip(ap, lat as f64, lng as f64)
    }

''')
q = q.replace("interior || self.inside(st, poly, &ap, lat, lng)", "interior || self.inside(&ap, lat, lng)")
q = q.replace("for (poly, ap, interior) in self.cell_polygons(lat, lng)", "for (_, ap, interior) in self.cell_polygons(lat, lng)")
q = q.replace('''    rings: HashMap<u32, Rc<Vec<NodeCoord>>>,
    ring_vertices: usize,
    stats: QueryStats,''', '''    stats: QueryStats,''')
q = q.replace('''        let mut st = State { ctx: HashMap::new(), rings: HashMap::new(), ring_vertices: 0, stats: QueryStats::default() };''',
              '''        let mut st = State { ctx: HashMap::new(), stats: QueryStats::default() };''')
q = q.replace('''/// Per-search caches: one reverse lookup per entity at most, one ring decode per polygon.''',
              '''/// Per-search caches: one reverse lookup per entity at most.''')
q = q.replace("use query_server::{cell_id_at_level, point_in_polygon, AdminPolygon, Index, NodeCoord, ID_MASK, INTERIOR_FLAG, NO_DATA};",
              "use query_server::{cell_id_at_level, AdminPolygon, Index, ID_MASK, INTERIOR_FLAG, NO_DATA};")
open(Q, "w").write(q)
print("ok")
