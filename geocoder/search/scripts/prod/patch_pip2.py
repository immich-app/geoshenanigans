"""One-off patch: the encoded point-in-polygon loop specialised per vertex encoding (one
monomorphised pass, no per-vertex match)."""
import sys

LIB = sys.argv[1]
s = open(LIB).read()
a = s.index("pub fn point_in_encoded_polygon(")
b = s.index("// --- API types ---", a)
s = s[:a] + '''pub fn point_in_encoded_polygon(src: &FileBytes, vertex_offset: u32, vertex_count: u32, lat: f32, lng: f32) -> Option<bool> {
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
    let u16_at = |b: &[u8], i: usize| u16::from_le_bytes([b[i], b[i + 1]]) as f64;
    let u32_at = |b: &[u8], i: usize| u32::from_le_bytes([b[i], b[i + 1], b[i + 2], b[i + 3]]);
    Some(match encoding {
        4 => pip_vertices(&bytes, 8, lat, lng, |b| (f32::from_bits(u32_at(b, 0)), f32::from_bits(u32_at(b, 4)))),
        3 => pip_vertices(&bytes, 8, lat, lng, |b| {
            ((bmin_lat + u32_at(b, 0) as f64 * scale) as f32, (bmin_lng + u32_at(b, 4) as f64 * scale) as f32)
        }),
        _ => pip_vertices(&bytes, 4, lat, lng, |b| {
            ((bmin_lat + u16_at(b, 0) * scale) as f32, (bmin_lng + u16_at(b, 2) * scale) as f32)
        }),
    })
}

/// The `point_in_polygon` ray cast over packed vertices of one encoding (`at` decodes one).
#[inline]
fn pip_vertices(bytes: &[u8], stride: usize, lat: f32, lng: f32, at: impl Fn(&[u8]) -> (f32, f32)) -> bool {
    let n = bytes.len() / stride;
    let mut inside = false;
    let (mut vj_lat, mut vj_lng) = at(&bytes[(n - 1) * stride..]);
    for c in bytes.chunks_exact(stride) {
        let (vi_lat, vi_lng) = at(c);
        if ((vi_lng > lng) != (vj_lng > lng))
            && (lat < (vj_lat - vi_lat) * (lng - vi_lng) / (vj_lng - vi_lng) + vi_lat)
        {
            inside = !inside;
        }
        vj_lat = vi_lat;
        vj_lng = vi_lng;
    }
    inside
}

''' + s[b:]
open(LIB, "w").write(s)
print("ok")
