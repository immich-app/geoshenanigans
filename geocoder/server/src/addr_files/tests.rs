use super::*;
use crate::coord_keys::hybrid_key;
use crate::test_rng::Rng;
use crate::{vertex_scale, NO_DATA};

const SUPERBLOCK_RECORDS: usize = SUPERBLOCK_BLOCKS * BLOCK_RECORDS;
/// The kinds of records `random_records` draws.
const KINDS: u64 = 4;

/// A logical record: its point (f32 bits), its ids, and its footprint's logical blob (the
/// encoding, a zero byte, the bbox minimum and the vertices), empty for a point.
#[derive(Clone, Debug, PartialEq)]
struct Logical {
    lat: u32,
    lng: u32,
    housenumber: u32,
    street: u32,
    parent: u32,
    footprint: Vec<u8>,
}

/// The fill of a dead slot: a point at 0, 0 with string 0 for its housenumber and its street, and
/// no parent way.
fn tombstone() -> Logical {
    Logical { lat: 0, lng: 0, housenumber: 0, street: 0, parent: NO_DATA, footprint: Vec::new() }
}

/// The vertices of a logical blob: none of a point's.
fn count_of(logical: &[u8]) -> u32 {
    if logical.is_empty() {
        return 0;
    }
    ((logical.len() - 10) / if logical[0] == 3 { 8 } else { 4 }) as u32
}

/// The logical pair of the records: addr_points.bin, each footprint's vertex_offset the bytes of
/// the ones before it, and addr_vertices.bin.
fn logical_pair(records: &[Logical]) -> (Vec<u8>, Vec<u8>) {
    let (mut points, mut vertices) = (Vec::new(), Vec::new());
    for r in records {
        let offset = if r.footprint.is_empty() { NO_DATA } else { vertices.len() as u32 };
        for field in [r.lat, r.lng, r.housenumber, r.street, r.parent, offset, count_of(&r.footprint)] {
            points.extend(field.to_le_bytes());
        }
        vertices.extend(&r.footprint);
    }
    (points, vertices)
}

/// The records of a logical pair.
fn logical_records(points: &[u8], vertices: &[u8]) -> Vec<Logical> {
    points
        .chunks_exact(28)
        .map(|r| {
            let field = |k: usize| u32::from_le_bytes(r[4 * k..4 * k + 4].try_into().unwrap());
            let (offset, count) = (field(5) as usize, field(6) as usize);
            let footprint = if count == 0 {
                Vec::new()
            } else {
                vertices[offset..offset + 10 + count * if vertices[offset] == 3 { 8 } else { 4 }].to_vec()
            };
            Logical { lat: field(0), lng: field(1), housenumber: field(2), street: field(3), parent: field(4), footprint }
        })
        .collect()
}

fn open(points: &[u8], vertices: &[u8], index: &[u8]) -> Result<Option<AddrFiles>, String> {
    let owned = |bytes: &[u8]| Some(FileBytes::Owned(bytes.to_vec()));
    AddrFiles::open(owned(points), owned(vertices), owned(index))
}

fn files(b: &AddrFileBytes) -> AddrFiles {
    open(&b.points, &b.vertices, &b.index).unwrap_or_else(|e| panic!("{e}")).expect("the three files")
}

fn encoded(records: &[Logical]) -> AddrFileBytes {
    let (points, vertices) = logical_pair(records);
    encode(&points, &vertices)
}

fn bits(vertices: &[NodeCoord]) -> Vec<(u32, u32)> {
    vertices.iter().map(|c| (c.lat.to_bits(), c.lng.to_bits())).collect()
}

/// A record's fields, a float as its bits: lat, lng, housenumber, street, parent, and its blob's
/// place and bytes.
type Fields = (u32, u32, u32, u32, u32, u64, u32);

fn fields(r: &AddrRecord) -> Fields {
    (r.lat.to_bits(), r.lng.to_bits(), r.housenumber_id, r.street_id, r.parent_way_id, r.blob_at, r.blob_len)
}

/// A logical blob's header, as a polygon's in a stream of logical blobs:
///   byte 0     : vertex encoding tag (VertexEncoding in builder/src/types.h)
///   byte 1     : padding
///   bytes 2..6 : bbox_min_lat (f32)
///   bytes 6..10: bbox_min_lng (f32)
/// Vertices follow at byte 10, packed at the encoding's stride.
const POLY_HEADER_BYTES: u64 = 10;

#[inline]
fn vertex_stride(enc: u8) -> usize {
    match enc {
        0 | 1 | 2 => 4, // u16 dlat + u16 dlng
        3 | 4     => 8, // u32 dlat + u32 dlng OR f32 + f32
        _ => 8,
    }
}

/// A polygon's encoded vertices as `decode_polygon_verts` reads them: the inline 10-byte header
/// (encoding tag, bbox minimum) at `vertex_offset` and where its `vertex_count` vertices start.
struct EncodedPolygon {
    encoding: u8,
    bmin_lat: f32,
    bmin_lng: f32,
    verts_off: u64,
}

/// The polygon at `vertex_offset`, or None where it has none: no vertices, or the header or
/// vertices past the end of `src`.
fn encoded_polygon(src: &FileBytes, vertex_offset: u32, vertex_count: u32) -> Option<EncodedPolygon> {
    if vertex_count == 0 || vertex_offset == NO_DATA { return None; }
    let off = vertex_offset as u64;
    if off + POLY_HEADER_BYTES > src.len() { return None; }
    let header = src.read_chunk(off, POLY_HEADER_BYTES as usize);
    let encoding = header[0];
    let bmin_lat = f32::from_le_bytes(header[2..6].try_into().unwrap());
    let bmin_lng = f32::from_le_bytes(header[6..10].try_into().unwrap());
    let verts_off = off + POLY_HEADER_BYTES;
    if verts_off + vertex_count as u64 * vertex_stride(encoding) as u64 > src.len() { return None; }
    Some(EncodedPolygon { encoding, bmin_lat, bmin_lng, verts_off })
}

/// Decode a polygon's quantized vertices into NodeCoord values.
/// Reads the inline 10-byte polygon header (encoding tag + bbox_min)
/// at vertex_offset, then `vertex_count` vertices that follow. The
/// decoder of logical blobs: a footprint's vertices are the ones it
/// gives for the footprint's logical blob (`logical_vertices`).
fn decode_polygon_verts<'a>(
    src: &'a FileBytes,
    vertex_offset: u32,
    vertex_count: u32,
) -> Option<std::borrow::Cow<'a, [NodeCoord]>> {
    let EncodedPolygon { encoding, bmin_lat: bbox_min_lat, bmin_lng: bbox_min_lng, verts_off } =
        encoded_polygon(src, vertex_offset, vertex_count)?;
    let total_bytes = (vertex_count as u64) * vertex_stride(encoding) as u64;
    let bytes = src.read_chunk(verts_off, total_bytes as usize);

    if encoding == 4 {
        // Legacy raw F32 lat/lng (8 bytes/vertex). Vertices start at
        // POLY_HEADER_BYTES (byte 10), which is only 2-aligned, so a direct
        // `bytes.as_ptr() as *const NodeCoord` cast is undefined behaviour
        // (NodeCoord needs 4-byte alignment). Decode each field with an
        // unaligned little-endian read instead: this yields bit-identical
        // f32 values to the cast on little-endian targets without the
        // alignment hazard.
        let count = vertex_count as usize;
        let mut out = Vec::<NodeCoord>::with_capacity(count);
        for i in 0..count {
            let lat = f32::from_le_bytes(bytes[i*8..i*8+4].try_into().unwrap());
            let lng = f32::from_le_bytes(bytes[i*8+4..i*8+8].try_into().unwrap());
            out.push(NodeCoord { lat, lng });
        }
        Some(std::borrow::Cow::Owned(out))
    } else {
        // Delta-encoded.  Decode each vertex into a fresh NodeCoord.
        let scale = vertex_scale(encoding);
        let bmin_lat = bbox_min_lat as f64;
        let bmin_lng = bbox_min_lng as f64;
        let count = vertex_count as usize;
        let mut out = Vec::<NodeCoord>::with_capacity(count);
        if encoding == 3 {
            // u32 deltas (8 bytes/vertex)
            for i in 0..count {
                let dlat = u32::from_le_bytes(bytes[i*8..i*8+4].try_into().unwrap()) as f64;
                let dlng = u32::from_le_bytes(bytes[i*8+4..i*8+8].try_into().unwrap()) as f64;
                out.push(NodeCoord {
                    lat: (bmin_lat + dlat * scale) as f32,
                    lng: (bmin_lng + dlng * scale) as f32,
                });
            }
        } else {
            // u16 deltas (4 bytes/vertex) — encodings 0, 1, 2
            for i in 0..count {
                let dlat = u16::from_le_bytes(bytes[i*4..i*4+2].try_into().unwrap()) as f64;
                let dlng = u16::from_le_bytes(bytes[i*4+2..i*4+4].try_into().unwrap()) as f64;
                out.push(NodeCoord {
                    lat: (bmin_lat + dlat * scale) as f32,
                    lng: (bmin_lng + dlng * scale) as f32,
                });
            }
        }
        Some(std::borrow::Cow::Owned(out))
    }
}

/// The vertices of a logical blob, as the decoder of logical blobs gives them: each the bbox
/// minimum plus its steps.
fn logical_vertices(logical: &[u8]) -> Vec<(u32, u32)> {
    let src = FileBytes::Owned(logical.to_vec());
    bits(&decode_polygon_verts(&src, 0, count_of(logical)).expect("a polygon"))
}

/// Record `id` as the reader gives it against its logical record: its fields, its vertex count,
/// and its footprint's vertices against the logical blob's.
fn assert_reads(f: &AddrFiles, id: u32, want: &Logical, buf: &mut Vec<NodeCoord>, what: &str) {
    let r = f.record(id).unwrap_or_else(|| panic!("{what}: record {id} does not read"));
    assert_eq!(
        (r.lat.to_bits(), r.lng.to_bits(), r.housenumber_id, r.street_id, r.parent_way_id),
        (want.lat, want.lng, want.housenumber, want.street, want.parent),
        "{what}: record {id}"
    );
    assert_eq!(r.is_building(), !want.footprint.is_empty(), "{what}: record {id}");
    assert_eq!(f.vertex_count(&r), count_of(&want.footprint), "{what}: record {id}");
    assert_eq!(f.footprint(&r, buf), r.is_building(), "{what}: record {id}");
    // A buffer that holds another footprint's logical blob.
    let mut logical = vec![0xAA; 7];
    assert_eq!(f.logical_blob(&r, &mut logical), r.is_building(), "{what}: record {id}");
    assert!(logical == want.footprint, "{what}: record {id}'s logical blob");
    if r.is_building() {
        assert!(bits(buf) == logical_vertices(&want.footprint), "{what}: record {id}'s footprint");
    } else {
        // A point has no blob, and leaves nothing in the buffer.
        assert!(buf.is_empty() && (r.blob_at, r.blob_len) == (0, 0), "{what}: record {id}");
    }
}

/// Whether record `r` has a footprint, read into `buf`: the vertices its blob's head counts,
/// which are its logical blob's. Without one it has no vertices, no count and no logical blob.
fn draws(f: &AddrFiles, r: &AddrRecord, buf: &mut Vec<NodeCoord>) -> bool {
    let count = f.vertex_count(r);
    let drawn = f.footprint(r, buf);
    assert_eq!((drawn, buf.len()), (count > 0, count as usize));
    // A buffer that holds another footprint's logical blob.
    let mut logical = vec![0xAA; 7];
    assert_eq!(f.logical_blob(r, &mut logical), drawn);
    if drawn {
        assert!(count_of(&logical) == count && logical_vertices(&logical) == bits(buf), "a footprint of {count} vertices");
    } else {
        assert!(logical.is_empty());
    }
    drawn
}

fn logical_blob(enc: u8, bmin_lat: u32, bmin_lng: u32, q: &[(u32, u32)]) -> Vec<u8> {
    let mut out = vec![enc, 0];
    out.extend(bmin_lat.to_le_bytes());
    out.extend(bmin_lng.to_le_bytes());
    for &(lat, lng) in q {
        if enc == 3 {
            out.extend(lat.to_le_bytes());
            out.extend(lng.to_le_bytes());
        } else {
            out.extend((lat as u16).to_le_bytes());
            out.extend((lng as u16).to_le_bytes());
        }
    }
    out
}

/// Where one axis of a footprint's bbox minimum is when its point is its vertices' mean, the
/// format's rule written apart from the reader: the product before the quotient.
fn bbox_minimum_under(point: u32, sum: u64, vertices: usize, enc: u8) -> u32 {
    let scale = [1e-5, 1e-4, 1e-6][enc as usize];
    ((f32::from_bits(point) as f64 - (sum as f64 * scale) / vertices as f64) as f32).to_bits()
}

/// The float `residual` floats after `bits` in the floats' order.
fn floats_after(bits: u32, residual: i64) -> u32 {
    from_order_key((order_key(bits) as i64 + residual) as u32)
}

/// A footprint at the point (lat, lng) in encoding `enc` (0 to 2), its bbox minimum `rl` and
/// `rg` floats after where the point puts it.
fn footprint_at(enc: u8, lat: u32, lng: u32, q: &[(u32, u32)], rl: i64, rg: i64) -> Vec<u8> {
    let (sum_lat, sum_lng) = q.iter().fold((0u64, 0u64), |s, v| (s.0 + v.0 as u64, s.1 + v.1 as u64));
    let bmin_lat = floats_after(bbox_minimum_under(lat, sum_lat, q.len(), enc), rl);
    let bmin_lng = floats_after(bbox_minimum_under(lng, sum_lng, q.len(), enc), rg);
    logical_blob(enc, bmin_lat, bmin_lng, q)
}

/// A footprint at a record's point, of one of six kinds: 0, a building (a closed ring of a few
/// corners near each other, its bbox minimum near where the point puts it); 1, an open path in
/// encoding 0, 1 or 2, its bbox minimum up to 300 floats off; 2, a long outline in small steps
/// from beside the bbox minimum, up from it or around it (a step below it is taken mod 2^16),
/// closed or open; 3, any 16 bits a value, the bbox minimum any 32; 4, one vertex, or two that
/// are equal; 5, a footprint in encoding 3.
fn random_footprint(rng: &mut Rng, lat: u32, lng: u32, kind: u64) -> Vec<u8> {
    let mut below = |n: u32| (rng.next() % n as u64) as u32;
    match kind {
        0 => {
            let mut ring: Vec<(u32, u32)> = (0..3 + below(6)).map(|_| (below(600), below(600))).collect();
            ring.push(ring[0]);
            footprint_at(2, lat, lng, &ring, below(7) as i64 - 3, below(7) as i64 - 3)
        }
        1 => {
            let reach = 1 << (1 + below(16));
            let q: Vec<(u32, u32)> = (0..2 + below(40)).map(|_| (below(reach), below(reach))).collect();
            footprint_at(below(3) as u8, lat, lng, &q, below(601) as i64 - 300, below(601) as i64 - 300)
        }
        2 => {
            let reach = 1 << (2 + below(8));
            let back = if below(2) == 0 { 0 } else { reach / 2 };
            let (mut a, mut b) = (below(reach), below(reach));
            let mut q = Vec::new();
            for _ in 0..20 + below(60) {
                q.push((a, b));
                a = (a + below(reach)).wrapping_sub(back) & 0xFFFF;
                b = (b + below(reach)).wrapping_sub(back) & 0xFFFF;
            }
            if below(2) == 0 {
                q.push(q[0]);
            }
            footprint_at(below(3) as u8, lat, lng, &q, 124 + below(8) as i64, -124 - below(8) as i64)
        }
        3 => {
            let q: Vec<(u32, u32)> = (0..1 + below(12)).map(|_| (below(65536), below(65536))).collect();
            logical_blob(below(3) as u8, below(u32::MAX), below(u32::MAX), &q)
        }
        4 => {
            let v = (below(65536), below(65536));
            let q = if below(2) == 0 { vec![v] } else { vec![v, v] };
            footprint_at(2, lat, lng, &q, 0, 0)
        }
        _ => {
            let q: Vec<(u32, u32)> = (0..1 + below(6)).map(|_| (below(u32::MAX), below(u32::MAX))).collect();
            logical_blob(3, below(u32::MAX), below(u32::MAX), &q)
        }
    }
}

/// The bits of the f32 nearest `v / unit`.
fn float_bits(v: i64, unit: f64) -> u32 {
    ((v as f64 / unit) as f32).to_bits()
}

/// `n` records of one of KINDS kinds: 0, a town (points a few metres apart, streets in runs,
/// each under its own parent way for 96 records and then all under ways near each other for as
/// many, housenumbers near each other); 1, points within a degree of the equator, where few
/// floats have a hybrid key, with runs of ones that do and -0 among them; 2, any 32 bits a
/// field; 3, a town with dead slots among its records. With `footprints`, about half the
/// records that are no dead slot have one, of every kind.
fn random_records(rng: &mut Rng, n: usize, kind: u64, footprints: bool) -> Vec<Logical> {
    let lat0 = (rng.next() % 160_000_001) as i64 - 80_000_000;
    let lng0 = (rng.next() % 340_000_001) as i64 - 170_000_000;
    let mut street = (rng.next() % 1_000_000) as u32;
    let parent = (rng.next() % 1_000_000) as u32;
    let housenumber = (rng.next() % 1_000_000) as u32;
    (0..n)
        .map(|i| {
            let dice = rng.next();
            if dice % 8 == 0 {
                street += 1 + ((dice >> 8) % 40) as u32;
            }
            if kind == 3 && (dice >> 40) % 5 == 0 {
                return tombstone();
            }
            let (lat, lng) = match kind {
                // A run of coordinates of seven decimals, each with a hybrid key, then two runs of
                // floats between them.
                1 if i / 24 % 3 == 0 => {
                    (float_bits((rng.next() % 2_000_001) as i64 - 1_000_000, 1e7), float_bits((rng.next() % 2_000_001) as i64 - 1_000_000, 1e7))
                }
                1 if (dice >> 4) % 16 == 0 => (0x8000_0000, 0x8000_0000),
                1 => (0.3f32.to_bits() + (rng.next() % 100_000) as u32, (0.6f32.to_bits() + (rng.next() % 100_000) as u32) | 0x8000_0000),
                2 => (rng.next() as u32, rng.next() as u32),
                _ => (float_bits(lat0 + (rng.next() % 4001) as i64 - 2000, 1e6), float_bits(lng0 + (rng.next() % 4001) as i64 - 2000, 1e6)),
            };
            let mut r = Logical {
                lat,
                lng,
                housenumber: housenumber + ((dice >> 16) % 300) as u32,
                street,
                parent: if i / 96 % 2 == 0 { street.wrapping_mul(2_654_435_761) } else { parent + street % 7 + ((dice >> 28) % 4) as u32 },
                footprint: Vec::new(),
            };
            if kind == 2 {
                (r.housenumber, r.street, r.parent) = (rng.next() as u32, rng.next() as u32, rng.next() as u32);
            }
            if footprints && (dice >> 44) % 2 == 0 {
                r.footprint = random_footprint(rng, lat, lng, (dice >> 48) % 6);
            }
            r
        })
        .collect()
}

fn le(bytes: &[u8], at: usize, n: usize) -> u64 {
    (0..n).fold(0, |v, k| v | (bytes[at + k] as u64) << (8 * k))
}

/// The `w` bits at bit `at` of `bytes`, a bit at a time, LSB first.
fn slow_bits(bytes: &[u8], at: usize, w: usize) -> u32 {
    (0..w).fold(0, |v, k| v | ((bytes[(at + k) / 8] >> ((at + k) % 8) & 1) as u32) << k)
}

/// The logical blob of the blob of a footprint at the point (lat, lng), as the format lays a
/// blob out, its values a bit at a time.
fn logical_of_blob(blob: &[u8], lat: u32, lng: u32) -> Vec<u8> {
    let (mut at, mut count) = (1, 0usize);
    loop {
        count |= ((blob[at] & 0x7F) as usize) << (7 * (at - 1));
        at += 1;
        if blob[at - 1] & 0x80 == 0 {
            break;
        }
    }
    if blob[0] == 0x80 {
        // An escape: the logical blob as it is.
        assert_eq!(count_of(&blob[at..]) as usize, count);
        return blob[at..].to_vec();
    }
    assert_eq!(blob[0] >> 5, 0);
    let (enc, closed, small, delta) = (blob[0] & 3, blob[0] >> 2 & 1 == 1, blob[0] >> 3 & 1 == 1, blob[0] >> 4 & 1 == 1);
    let bbox_at = at;
    at += if small { 2 } else { 8 };
    let width = |nibble: u8| if nibble == 15 { 16 } else { nibble as usize };
    let (wl, wg) = (width(blob[at] & 15), width(blob[at] >> 4));
    at += 1;
    assert_eq!(blob.len(), at + (count * (wl + wg)).div_ceil(8), "a blob is the bytes its count and widths take");
    let mut q: Vec<(u32, u32)> = Vec::new();
    for k in 0..count {
        let (vl, vg) = (slow_bits(blob, 8 * at + k * wl, wl) as u16, slow_bits(blob, 8 * at + count * wl + k * wg, wg) as u16);
        // A value is its vertex's step, or as a delta the step's own step from the one before it.
        let stepped = |last: u32, v: u16| (last as u16).wrapping_add(unzigzag16(v)) as u32;
        q.push(match q.last() {
            Some(&(last_lat, last_lng)) if delta => (stepped(last_lat, vl), stepped(last_lng, vg)),
            _ => (vl as u32, vg as u32),
        });
    }
    if closed {
        q.push(q[0]);
    }
    let (bmin_lat, bmin_lng) = if small {
        let (sum_lat, sum_lng) = q.iter().fold((0u64, 0u64), |s, v| (s.0 + v.0 as u64, s.1 + v.1 as u64));
        (
            floats_after(bbox_minimum_under(lat, sum_lat, q.len(), enc), (blob[bbox_at] as i8).into()),
            floats_after(bbox_minimum_under(lng, sum_lng, q.len(), enc), (blob[bbox_at + 1] as i8).into()),
        )
    } else {
        (le(blob, bbox_at, 4) as u32, le(blob, bbox_at + 4, 4) as u32)
    };
    logical_blob(enc, bmin_lat, bmin_lng, &q)
}

/// What a reader that takes the three files a bit at a time finds: every record, with its blob's
/// place; every footprint's logical blob, none of a point; and every block's flags.
struct BitByBit {
    records: Vec<AddrRecord>,
    footprints: Vec<Vec<u8>>,
    flags: Vec<u8>,
}

/// The three files read a bit at a time, in record order, as the format lays them out. Panics
/// where the index is not the offsets the blocks and the blobs give, and on bytes between the
/// last block, blob or row and its file's footer.
fn read_bit_by_bit(points: &[u8], vertices: &[u8], index: &[u8]) -> BitByBit {
    let count = le(points, points.len() - 8, 4) as usize;
    assert_eq!(le(index, index.len() - 8, 4) as usize, count);
    assert_eq!((&points[points.len() - 4..], &vertices[vertices.len() - 4..], &index[index.len() - 4..]), (&b"ADB1"[..], &b"ADF1"[..], &b"ADX1"[..]));
    let mut out = BitByBit { records: Vec::new(), footprints: Vec::new(), flags: Vec::new() };
    let (mut at, mut blob_at, mut row_at) = (0usize, 0usize, 0usize);
    let (mut row_blocks_at, mut row_blobs_at) = (0, 0);
    for b in 0..count.div_ceil(BLOCK_RECORDS) {
        let n = BLOCK_RECORDS.min(count - b * BLOCK_RECORDS);
        if b % SUPERBLOCK_BLOCKS == 0 {
            assert_eq!((le(index, row_at, 8) as usize, le(index, row_at + 8, 8) as usize), (at, blob_at), "superblock {}", b / SUPERBLOCK_BLOCKS);
            (row_blocks_at, row_blobs_at) = (at, blob_at);
            row_at += 16;
        }
        assert_eq!((le(index, row_at, 2) as usize, le(index, row_at + 2, 4) as usize), (at - row_blocks_at, blob_at - row_blobs_at), "block {b}");
        row_at += 6;
        let (flags, entries) = (points[at], points[at + 1] as usize);
        let w: Vec<usize> = (0..6).map(|k| slow_bits(points, 8 * (at + 2) + 6 * k, 6) as usize).collect();
        assert_eq!(slow_bits(points, 8 * (at + 2) + 36, 4), 0);
        let base = |k: usize| le(points, at + 7 + 4 * k, 4) as u32;
        let split = flags & 4 != 0;
        let dictionary_at = at + 19;
        let fields_at = dictionary_at + if split { 4 * entries + 4 } else { 8 * entries };
        let record_bits: usize = w.iter().sum();
        for j in 0..n {
            let mut bit = 8 * fields_at + j * record_bits;
            let mut field = |k: usize| {
                let v = slow_bits(points, bit, w[k]);
                bit += w[k];
                v
            };
            let (lat, lng, housenumber, x, parent, len) = (field(0), field(1), field(2), field(3) as usize, field(4), field(5));
            let coordinate = |key: u32, order: bool| if order { from_order_key(key) } else { from_hybrid_key(key ^ 0x8000_0000) };
            let (lat, lng) = (coordinate(base(0) + lat, flags & 1 != 0), coordinate(base(1) + lng, flags & 2 != 0));
            let entry = |at: usize| le(points, dictionary_at + at, 4) as u32;
            let (street_id, parent_way_id) = if split { (entry(4 * x), entry(4 * entries) + parent) } else { (entry(8 * x), entry(8 * x + 4)) };
            out.records.push(AddrRecord {
                lat: f32::from_bits(lat),
                lng: f32::from_bits(lng),
                housenumber_id: base(2) + housenumber,
                street_id,
                parent_way_id,
                blob_at: if len == 0 { 0 } else { blob_at as u64 },
                blob_len: len,
            });
            out.footprints.push(if len == 0 { Vec::new() } else { logical_of_blob(&vertices[blob_at..blob_at + len as usize], lat, lng) });
            blob_at += len as usize;
        }
        out.flags.push(flags);
        at = fields_at + (n * record_bits).div_ceil(8);
    }
    assert_eq!((at + 8, blob_at + 16, row_at + 8), (points.len(), vertices.len(), index.len()));
    out
}

/// The logical records a bit-at-a-time read of the files gives.
fn logical_of(read: &BitByBit) -> Vec<Logical> {
    read.records
        .iter()
        .zip(&read.footprints)
        .map(|(r, footprint)| Logical {
            lat: r.lat.to_bits(),
            lng: r.lng.to_bits(),
            housenumber: r.housenumber_id,
            street: r.street_id,
            parent: r.parent_way_id,
            footprint: footprint.clone(),
        })
        .collect()
}

#[test]
fn every_record_reads_back_at_its_id_whatever_its_place_in_a_block_or_a_superblock() {
    let mut rng = Rng::new(20261114);
    let mut buf = Vec::new();
    for n in [0, 1, BLOCK_RECORDS - 1, BLOCK_RECORDS, BLOCK_RECORDS + 1, SUPERBLOCK_RECORDS, SUPERBLOCK_RECORDS + 1, 3000] {
        for kind in 0..KINDS {
            for footprints in [false, true] {
                let records = random_records(&mut rng, n, kind, footprints);
                let bytes = encoded(&records);
                let what = format!("{n} records of kind {kind}, footprints {footprints}");
                // A row a superblock and an entry a block, the last row shorter, and the footers:
                // all there is of no records, and all of addr_vertices.bin without a footprint.
                assert_eq!(bytes.index.len(), 16 * n.div_ceil(SUPERBLOCK_RECORDS) + 6 * n.div_ceil(BLOCK_RECORDS) + 8, "{what}");
                assert_eq!(bytes.points.len() == 8, n == 0, "{what}");
                assert!(bytes.vertices.len() >= 16 && (footprints || bytes.vertices.len() == 16), "{what}");
                let f = files(&bytes);
                assert_eq!(f.count() as usize, n, "{what}");
                for (id, want) in records.iter().enumerate() {
                    assert_reads(&f, id as u32, want, &mut buf, &what);
                }
                // Past the last record there is none, alone or in a run.
                let past = [n, n + 1, n + BLOCK_RECORDS, n + SUPERBLOCK_RECORDS, NO_DATA as usize].map(|id| id as u32);
                assert!(past.iter().all(|&id| f.record(id).is_none()), "{what}");
                f.for_ids(past, |id, _| panic!("{what}: record {id} reads"));
            }
        }
    }
}

#[test]
fn a_dead_slots_record_reads_as_a_record_like_any_other() {
    let mut rng = Rng::new(11);
    let mut buf = Vec::new();
    let mut town = random_records(&mut rng, 3 * BLOCK_RECORDS + 3, 0, true);
    // A block's first and last record, a block of nothing else, and the last record of all.
    for id in [0, BLOCK_RECORDS - 1, 3 * BLOCK_RECORDS + 2].into_iter().chain(BLOCK_RECORDS..2 * BLOCK_RECORDS) {
        town[id] = tombstone();
    }
    for records in [vec![tombstone()], vec![tombstone(); BLOCK_RECORDS], town] {
        let f = files(&encoded(&records));
        for (id, want) in records.iter().enumerate() {
            assert_reads(&f, id as u32, want, &mut buf, "dead slots");
            if *want == tombstone() {
                assert_eq!(fields(&f.record(id as u32).unwrap()), (0, 0, 0, 0, NO_DATA, 0, 0));
            }
        }
    }
}

#[test]
fn a_dir_has_all_three_files_or_none() {
    let good = encoded(&random_records(&mut Rng::new(3), 40, 0, true));
    let file = |bytes: &Vec<u8>, there: bool| there.then(|| FileBytes::Owned(bytes.clone()));
    let said = |points: bool, vertices: bool, index: bool| {
        AddrFiles::open(file(&good.points, points), file(&good.vertices, vertices), file(&good.index, index)).map(|f| f.map(|f| f.count()))
    };
    assert_eq!(said(true, true, true), Ok(Some(40)));
    // A dir without address files has no addresses.
    assert_eq!(said(false, false, false), Ok(None));
    let refusals = [
        (said(false, true, true), "addr_vertices.bin and addr_index.bin without addr_points.bin"),
        (said(true, false, true), "addr_points.bin and addr_index.bin without addr_vertices.bin"),
        (said(true, true, false), "addr_points.bin and addr_vertices.bin without addr_index.bin"),
        (said(true, false, false), "addr_points.bin without addr_vertices.bin and addr_index.bin"),
        (said(false, true, false), "addr_vertices.bin without addr_points.bin and addr_index.bin"),
        (said(false, false, true), "addr_index.bin without addr_points.bin and addr_vertices.bin"),
    ];
    for (got, want) in refusals {
        assert_eq!(got, Err(want.to_string()));
    }
}

#[test]
fn files_no_builder_writes_are_refused_at_open_by_name() {
    let said = |points: &[u8], vertices: &[u8], index: &[u8]| open(points, vertices, index).err().unwrap_or_default();
    // Two blocks and half of one more.
    let n = 2 * BLOCK_RECORDS + BLOCK_RECORDS / 2;
    let good = encoded(&random_records(&mut Rng::new(7), n, 0, true));
    let (p, v, x) = (&good.points[..], &good.vertices[..], &good.index[..]);
    assert_eq!(said(p, v, x), "");

    // A footer cut, or with another file's magic.
    assert_eq!(said(&p[..p.len() - 1], v, x), "addr_points.bin has no ADB1 footer");
    assert_eq!(said(&p[..7], v, x), "addr_points.bin has no ADB1 footer");
    assert_eq!(said(&[], v, x), "addr_points.bin has no ADB1 footer");
    assert_eq!(said(x, v, x), "addr_points.bin has no ADB1 footer");
    assert_eq!(said(p, &v[..v.len() - 1], x), "addr_vertices.bin has no ADF1 footer");
    assert_eq!(said(p, &v[..15], x), "addr_vertices.bin has no ADF1 footer");
    assert_eq!(said(p, &[], x), "addr_vertices.bin has no ADF1 footer");
    assert_eq!(said(p, p, x), "addr_vertices.bin has no ADF1 footer");
    assert_eq!(said(p, v, &x[..x.len() - 1]), "addr_index.bin has no ADX1 footer");
    assert_eq!(said(p, v, &x[..7]), "addr_index.bin has no ADX1 footer");
    assert_eq!(said(p, v, &[]), "addr_index.bin has no ADX1 footer");
    assert_eq!(said(p, v, p), "addr_index.bin has no ADX1 footer");
    // Open reads the footers alone: blocks and blobs cut to their footers open.
    assert_eq!(said(&p[p.len() - 8..], &v[v.len() - 16..], x), "");

    // Footers that count different records.
    let counting = |bytes: &[u8], count: usize| {
        let mut b = bytes.to_vec();
        let at = b.len() - 8;
        b[at..at + 4].copy_from_slice(&(count as u32).to_le_bytes());
        b
    };
    let different = "addr_points.bin and addr_index.bin count different records";
    assert_eq!(said(&counting(p, n + 1), v, x), different);
    assert_eq!(said(p, v, &counting(x, n - 1)), different);
    // An index that is not the rows of the records both count: a block more or fewer, a byte
    // more or fewer, a count no file holds.
    let rows = |count: usize| format!("addr_index.bin is not the rows of its {count} records");
    for count in [n + BLOCK_RECORDS, n - BLOCK_RECORDS, 0, u32::MAX as usize] {
        assert_eq!(said(&counting(p, count), v, &counting(x, count)), rows(count));
    }
    assert_eq!(said(p, v, &x[1..]), rows(n));
    let mut longer = vec![0u8];
    longer.extend_from_slice(x);
    assert_eq!(said(p, v, &longer), rows(n));
    // The rows are a block's and a superblock's, not a record's: a record more in the last
    // block has the same rows.
    assert_eq!(said(&counting(p, n + 1), v, &counting(x, n + 1)), "");

    // More blobs than records.
    assert_eq!(said(p, &counting(v, n), x), "");
    assert_eq!(said(p, &counting(v, n + 1), x), "addr_vertices.bin counts more blobs than addr_points.bin has records");
}

/// A copy of the files, for an edit.
fn copy_of(b: &AddrFileBytes) -> AddrFileBytes {
    AddrFileBytes { points: b.points.clone(), vertices: b.vertices.clone(), index: b.index.clone() }
}

#[test]
fn a_block_or_a_blob_no_builder_writes_reads_as_none_and_the_records_beside_it_still_read() {
    let here = (52.37f32.to_bits(), 4.89f32.to_bits());
    let ring = [(0, 40), (0, 310), (120, 310), (120, 180), (260, 180), (260, 40), (0, 40)];
    // One block of four records on three streets under parent ways 32 bits apart, so the block
    // is paired: a building whose bbox minimum is two residuals, a point, a building whose bbox
    // minimum is in full, and a footprint in encoding 3.
    let records = vec![
        Logical { lat: here.0, lng: here.1, housenumber: 100, street: 7, parent: 40, footprint: footprint_at(2, here.0, here.1, &ring, 1, -2) },
        Logical { lat: here.0 + 5, lng: here.1 - 3, housenumber: 103, street: 9, parent: 0x4000_0000, footprint: Vec::new() },
        Logical {
            lat: here.0 + 2,
            lng: here.1 + 12,
            housenumber: 101,
            street: 11,
            parent: 0x8000_0000,
            footprint: logical_blob(0, 10.0f32.to_bits(), (-20.0f32).to_bits(), &[(100, 0), (130, 1), (100, 2)]),
        },
        Logical { lat: here.0 + 9, lng: here.1, housenumber: 100, street: 7, parent: 40, footprint: logical_blob(3, 1, 2, &[(70000, 5)]) },
    ];
    let good = encoded(&records);
    let whole = files(&good);
    let was: Vec<AddrRecord> = (0..4).map(|id| whole.record(id).unwrap()).collect();
    let mut buf = Vec::new();
    for (id, want) in records.iter().enumerate() {
        assert_reads(&whole, id as u32, want, &mut buf, "whole");
    }
    // The block is paired, under hybrid keys, with three entries of 8 bytes after its head and
    // two bits an index.
    assert_eq!((good.points[0], good.points[1]), (0, 3));
    let widths: Vec<usize> = (0..6).map(|k| slow_bits(&good.points, 16 + 6 * k, 6) as usize).collect();
    assert_eq!((widths[3], widths[4]), (2, 0));
    let fields_at = BLOCK_HEAD_BYTES + 3 * 8;

    // The ids that read, alone and in a run, of the files with an edit.
    let reading = |edit: &dyn Fn(&mut AddrFileBytes)| {
        let mut b = copy_of(&good);
        edit(&mut b);
        let f = files(&b);
        let alone: Vec<u32> = (0..4).filter(|&id| f.record(id).is_some()).collect();
        let mut run = Vec::new();
        f.for_ids(0..4, |id, _| run.push(id));
        assert_eq!(run, alone);
        alone
    };
    let none: Vec<u32> = Vec::new();
    assert_eq!(reading(&|_| {}), [0, 1, 2, 3]);
    // A flag no block has; a dictionary of no entries, and of more than the block's records.
    assert_eq!(reading(&|b| b.points[0] |= 8), none);
    assert_eq!(reading(&|b| b.points[1] = 0), none);
    assert_eq!(reading(&|b| b.points[1] = 5), none);
    // A width past 32 bits, bits set past the widths, and a parent width in a paired block.
    assert_eq!(reading(&|b| b.points[2] = b.points[2] & !63 | 33), none);
    assert_eq!(reading(&|b| b.points[6] |= 0x10), none);
    assert_eq!(reading(&|b| b.points[5] |= 1), none);
    // A base and a value that pass 32 bits: the housenumbers at the base are left.
    assert_eq!(reading(&|b| b.points[15..19].copy_from_slice(&u32::MAX.to_le_bytes())), [0, 3]);
    // A hybrid key no coordinate has: the lats' base at 0 is the key furthest below zero, and
    // at the most a base is, the record at the base is left with the key furthest above it.
    assert!(hybrid_key(here.0).is_some());
    assert_eq!(reading(&|b| b.points[7..11].copy_from_slice(&[0; 4])), none);
    assert_eq!(reading(&|b| b.points[7..11].copy_from_slice(&u32::MAX.to_le_bytes())), none);
    // A block that starts at the blocks' end, or past it, and blocks cut to their footer.
    let blocks_end = good.points.len() - 8;
    assert_eq!(reading(&|b| b.index[..8].copy_from_slice(&(blocks_end as u64).to_le_bytes())), none);
    assert_eq!(reading(&|b| b.index[..8].copy_from_slice(&u64::MAX.to_le_bytes())), none);
    assert_eq!(reading(&|b| b.index[16..18].copy_from_slice(&u16::MAX.to_le_bytes())), none);
    assert_eq!(reading(&|b| b.points = b.points.split_off(blocks_end)), none);
    // A block that ends a byte past the blocks' end: none of its records read.
    assert_eq!(reading(&|b| b.points = [&b.points[..blocks_end - 1], &b.points[blocks_end..]].concat()), none);
    // Blobs that start past the blobs' end, by the superblock's offset or the block's, and blobs
    // cut to their footer: the point still reads.
    assert_eq!(reading(&|b| b.index[8..16].copy_from_slice(&u64::MAX.to_le_bytes())), [1]);
    assert_eq!(reading(&|b| b.index[18..22].copy_from_slice(&u32::MAX.to_le_bytes())), [1]);
    assert_eq!(reading(&|b| b.vertices = b.vertices.split_off(b.vertices.len() - 16)), [1]);
    // A blob that ends a byte past the blobs' end: the last record's.
    let blobs_end = good.vertices.len() - 16;
    assert_eq!(reading(&|b| b.vertices = [&b.vertices[..blobs_end - 1], &b.vertices[blobs_end..]].concat()), [0, 1, 2]);

    // The same keys as order keys are floats, each of them; a base and a value that pass 32 bits
    // are none.
    let mut b = copy_of(&good);
    b.points[0] |= 1;
    b.points[7..11].copy_from_slice(&[0; 4]);
    let lats: Vec<Option<u32>> = (0..4).map(|id| files(&b).record(id).map(|r| r.lat.to_bits())).collect();
    assert_eq!(lats, [0u32, 5, 2, 9].map(|key| Some(from_order_key(key))));
    b.points[7..11].copy_from_slice(&u32::MAX.to_le_bytes());
    let lats: Vec<Option<u32>> = (0..4).map(|id| files(&b).record(id).map(|r| r.lat.to_bits())).collect();
    assert_eq!(lats, [Some(from_order_key(u32::MAX)), None, None, None]);

    // An index past the dictionary: record 0's two bits set to 3 where its entry is 0. The
    // records after it read, their blobs after its own.
    let index_at = 8 * fields_at + widths[..3].iter().sum::<usize>();
    let mut b = copy_of(&good);
    b.points[index_at / 8] |= 3 << (index_at % 8);
    b.points[index_at / 8 + 1] |= (3u16 << (index_at % 8) >> 8) as u8;
    let f = files(&b);
    assert!(f.record(0).is_none());
    for id in 1..4 {
        assert_eq!(fields(&f.record(id).unwrap()), fields(&was[id as usize]));
    }

    // A split block whose parents' base and a record's value pass 32 bits: two records of one
    // street under no parent way and under way 0.
    let on = |street: u32, parent: u32| Logical { lat: here.0, lng: here.1, housenumber: 1, street, parent, footprint: Vec::new() };
    let mut split = encoded(&[on(7, NO_DATA), on(7, 0)]);
    assert_eq!((split.points[0], split.points[1]), (4, 1));
    assert_eq!(files(&split).record(0).map(|r| r.parent_way_id), Some(NO_DATA));
    split.points[BLOCK_HEAD_BYTES + 4] = 1;
    let f = files(&split);
    assert!(f.record(0).is_none());
    assert_eq!(f.record(1).map(|r| r.parent_way_id), Some(1));
    // A split block of three streets, record 0's index set to 3: the parents' base after the
    // streets is no street.
    let mut split = encoded(&[on(7, 40), on(9, 41), on(11, 42), on(7, 40)]);
    assert_eq!((split.points[0], split.points[1]), (4, 3));
    let index_at = 8 * (BLOCK_HEAD_BYTES + 3 * 4 + 4) + (0..3).map(|k| slow_bits(&split.points, 16 + 6 * k, 6) as usize).sum::<usize>();
    split.points[index_at / 8] |= 3 << (index_at % 8);
    split.points[index_at / 8 + 1] |= (3u16 << (index_at % 8) >> 8) as u8;
    let f = files(&split);
    let streets: Vec<Option<(u32, u32)>> = (0..4).map(|id| f.record(id).map(|r| (r.street_id, r.parent_way_id))).collect();
    assert_eq!(streets, [None, Some((9, 41)), Some((11, 42)), Some((7, 40))]);

    // A dictionary of an entry more than the block's records, in a block with another after
    // it: a full block of as many streets under parent ways 32 bits apart, which is paired.
    let streets: Vec<Logical> = (0..BLOCK_RECORDS as u32 + 4).map(|i| on(100 + i, i.wrapping_mul(0x1111_1111))).collect();
    let mut more = encoded(&streets);
    assert_eq!((more.points[0], more.points[1] as usize), (0, BLOCK_RECORDS));
    more.points[1] += 1;
    let f = files(&more);
    let read: Vec<u32> = (0..streets.len() as u32).filter(|&id| f.record(id).is_some()).collect();
    assert_eq!(read, (BLOCK_RECORDS as u32..streets.len() as u32).collect::<Vec<u32>>());

    // Whether record `id` has a footprint, its blob edited: either way the record reads, a
    // footprint is the vertices its blob's head counts, and the footprints beside it read.
    let footprint_of = |id: usize, edit: &dyn Fn(&mut [u8])| {
        let mut b = copy_of(&good);
        let at = was[id].blob_at as usize;
        edit(&mut b.vertices[at..at + was[id].blob_len as usize]);
        let f = files(&b);
        let r = f.record(id as u32).unwrap();
        assert_eq!(fields(&r), fields(&was[id]));
        // A buffer that holds another footprint's vertices.
        let mut buf = vec![NodeCoord { lat: 1.0, lng: 2.0 }];
        let drawn = draws(&f, &r, &mut buf);
        for other in (0..4).filter(|&other| other != id) {
            assert_reads(&f, other as u32, &records[other], &mut buf, "beside an edited blob");
        }
        drawn
    };
    for id in [0, 2, 3] {
        assert!(footprint_of(id, &|_| {}));
    }
    // The first building's blob: a closed ring of six stored vertices, two residuals, and the
    // widths in its fifth byte.
    assert_eq!((good.vertices[0], good.vertices[1], was[0].blob_at), (2 | 4 | 8, 6, 0));
    // A tag none has: a bit past the flags, and encoding 3 where the blob is no escape.
    assert!(!footprint_of(0, &|blob| blob[0] |= 0x20));
    assert!(!footprint_of(0, &|blob| blob[0] |= 1));
    // No vertices; a count that is not in its fewest bytes; a ring of more vertices than a blob
    // holds; a count, or widths, the blob's bytes are not those of.
    assert!(!footprint_of(0, &|blob| blob[1] = 0));
    assert!(!footprint_of(0, &|blob| blob[1..3].copy_from_slice(&[0x86, 0])));
    assert!(!footprint_of(0, &|blob| blob[1..4].copy_from_slice(&[0xFF, 0xFF, 0x03])));
    assert!(!footprint_of(0, &|blob| blob[1] = 7));
    assert!(!footprint_of(0, &|blob| blob[4] ^= 1));
    // A bbox minimum that is two residuals where the blob has it in full, and the reverse.
    assert!(!footprint_of(2, &|blob| blob[0] |= 8));
    assert!(!footprint_of(0, &|blob| blob[0] &= !8));
    // An escape: an encoding past 3, its zero byte set, no vertices, and a count its bytes are
    // not those of.
    assert_eq!((good.vertices[was[3].blob_at as usize], was[3].blob_len), (0x80, 2 + 10 + 8));
    assert!(!footprint_of(3, &|blob| blob[2] = 4));
    assert!(!footprint_of(3, &|blob| blob[3] = 1));
    assert!(!footprint_of(3, &|blob| blob[1] = 0));
    assert!(!footprint_of(3, &|blob| blob[1] = 2));
    // Other values are other vertices, as many.
    assert!(footprint_of(0, &|blob| *blob.last_mut().unwrap() ^= 1));
    assert!(footprint_of(3, &|blob| *blob.last_mut().unwrap() ^= 1));
}

/// The files of one block of points at 52.37, 4.89, record j's blob `blobs[j]`, whatever its
/// bytes.
fn files_of_blobs(blobs: &[Vec<u8>]) -> AddrFileBytes {
    assert!(!blobs.is_empty() && blobs.len() <= BLOCK_RECORDS);
    let column = |value: u32| vec![value; blobs.len()];
    let lens: Vec<u32> = blobs.iter().map(|blob| blob.len() as u32).collect();
    // The block and its blobs start where their files do: a row of zeros.
    let mut b = AddrFileBytes { points: Vec::new(), vertices: blobs.concat(), index: vec![0; INDEX_HEAD_BYTES + INDEX_ENTRY_BYTES] };
    encode_block([&column(52.37f32.to_bits()), &column(4.89f32.to_bits()), &column(1), &column(2), &column(3)], &lens, &mut b.points);
    b.vertices.extend([0; 8]);
    for (file, magic) in [(&mut b.points, b"ADB1"), (&mut b.vertices, b"ADF1"), (&mut b.index, b"ADX1")] {
        file.extend((blobs.len() as u32).to_le_bytes());
        file.extend(magic);
    }
    b
}

#[test]
fn a_blob_written_by_hand_reads_only_where_its_head_is_one_a_blob_has() {
    let here = (52.37f32.to_bits(), 4.89f32.to_bits());
    // The vertices a blob holds, none where it is no footprint's: read from the files of a
    // record at `here` and the points beside it.
    let read = |blob: &[u8]| {
        let f = files(&files_of_blobs(&[vec![0x80], blob.to_vec(), vec![0x80]]));
        let r = f.record(1).unwrap();
        assert_eq!((r.blob_at, r.blob_len as usize), (1, blob.len()));
        // A buffer that holds another footprint's vertices.
        let mut buf = vec![NodeCoord { lat: 1.0, lng: 2.0 }];
        draws(&f, &r, &mut buf).then(|| bits(&buf))
    };
    let at_the_point = |vertices: usize| Some(vec![here; vertices]);
    // Two residuals of 0 and values of no bits: vertices where the point is, as many as the
    // blob counts, and a closed ring's first again. The most are 65,535.
    let small = 2 | 8;
    assert_eq!(read(&[small, 1, 0, 0, 0]), at_the_point(1));
    assert_eq!(read(&[small, 100, 0, 0, 0]), at_the_point(100));
    assert_eq!(read(&[small | 4, 100, 0, 0, 0]), at_the_point(101));
    assert_eq!(read(&[small, 0xFF, 0xFF, 0x03, 0, 0, 0]), at_the_point(65_535));
    assert_eq!(read(&[small | 4, 0xFE, 0xFF, 0x03, 0, 0, 0]), at_the_point(65_535));
    // No vertices; more than a blob holds, open, as a closed ring and in a count of four bytes.
    assert_eq!(read(&[small, 0, 0, 0, 0]), None);
    assert_eq!(read(&[small | 4, 0, 0, 0, 0]), None);
    assert_eq!(read(&[small, 0x80, 0x80, 0x04, 0, 0, 0]), None);
    assert_eq!(read(&[small | 4, 0xFF, 0xFF, 0x03, 0, 0, 0]), None);
    assert_eq!(read(&[small, 0x80, 0x80, 0x80, 0x01, 0, 0, 0]), None);
    // A blob that ends before its widths.
    assert_eq!(read(&[small]), None);
    assert_eq!(read(&[small, 1]), None);
    assert_eq!(read(&[small, 1, 0, 0]), None);

    // An escape: its count, the logical blob's encoding and zero byte, a bbox minimum at
    // `here`, and steps of 0.
    let escape = |count: &[u8], enc: u8, zero: u8, steps: usize| {
        let mut blob = vec![0x80];
        blob.extend(count);
        blob.extend([enc, zero]);
        blob.extend(here.0.to_le_bytes());
        blob.extend(here.1.to_le_bytes());
        blob.extend(vec![0; steps]);
        blob
    };
    // In any encoding up to 3, of as many vertices as its bytes hold: the reader takes an
    // escape of a footprint the builder writes a blob for.
    assert_eq!(read(&escape(&[1], 3, 0, 8)), at_the_point(1));
    assert_eq!(read(&escape(&[3], 3, 0, 24)), at_the_point(3));
    assert_eq!(read(&escape(&[2], 2, 0, 8)), at_the_point(2));
    assert_eq!(read(&escape(&[0x80, 0x01], 0, 0, 4 * 128)), at_the_point(128));
    // No vertices; an encoding past 3; the zero byte set; bytes that are not its count's; a
    // count that is not in its fewest bytes, and one of six bytes; an escape that ends before
    // its steps.
    assert_eq!(read(&escape(&[0], 3, 0, 0)), None);
    assert_eq!(read(&escape(&[0], 2, 0, 0)), None);
    for enc in [4, 5, 128, 255] {
        assert_eq!(read(&escape(&[1], enc, 0, 4)), None);
        assert_eq!(read(&escape(&[1], enc, 0, 8)), None);
    }
    assert_eq!(read(&escape(&[1], 3, 1, 8)), None);
    assert_eq!(read(&escape(&[1], 3, 0, 7)), None);
    assert_eq!(read(&escape(&[1], 3, 0, 9)), None);
    assert_eq!(read(&escape(&[2], 3, 0, 8)), None);
    assert_eq!(read(&escape(&[0x81, 0], 3, 0, 8)), None);
    assert_eq!(read(&escape(&[0x81, 0x80, 0x80, 0x80, 0x80, 0], 3, 0, 8)), None);
    assert_eq!(read(&[0x80]), None);
    assert_eq!(read(&[0x80, 1]), None);
    assert_eq!(read(&[0x80, 1, 3]), None);
    assert_eq!(read(&escape(&[1], 3, 0, 0)), None);

    // A record its caller made up, its blob across the blobs' end or past it: the footer's
    // bytes are no blob's, though the first three after these two would make them one.
    let f = files(&files_of_blobs(&[vec![small, 1]]));
    let r = f.record(0).unwrap();
    let mut buf = Vec::new();
    for (blob_at, blob_len) in [(0, 5), (2, 5), (1, 1), (u64::MAX, 5), (0, u32::MAX)] {
        let made_up = AddrRecord { blob_at, blob_len, ..r };
        assert!(!draws(&f, &made_up, &mut buf), "a blob of {blob_len} bytes at {blob_at}");
    }
}

#[test]
fn damaged_files_read_without_a_panic_and_what_no_byte_changed_in_still_reads() {
    let mut rng = Rng::new(99);
    let n = SUPERBLOCK_RECORDS + 3 * BLOCK_RECORDS + 5;
    let (mut read, mut unread, mut drawn, mut undrawn) = (0usize, 0usize, 0usize, 0usize);
    let mut buf = Vec::new();
    for kind in 0..KINDS {
        let good = encoded(&random_records(&mut rng, n, kind, true));
        let f = files(&good);
        // Every record and its footprint's vertices, undamaged.
        let whole: Vec<(AddrRecord, Vec<(u32, u32)>)> = (0..n as u32)
            .map(|id| {
                let r = f.record(id).unwrap();
                f.footprint(&r, &mut buf);
                (r, bits(&buf))
            })
            .collect();
        // Where each block starts in addr_points.bin, as addr_index.bin has it.
        let starts: Vec<usize> = (0..n.div_ceil(BLOCK_RECORDS))
            .map(|b| {
                let row = b / SUPERBLOCK_BLOCKS * INDEX_ROW_BYTES;
                (le(&good.index, row, 8) + le(&good.index, row + INDEX_HEAD_BYTES + b % SUPERBLOCK_BLOCKS * INDEX_ENTRY_BYTES, 2)) as usize
            })
            .collect();
        for round in 0..500 {
            let (mut points, mut vertices, mut index) = (good.points.clone(), good.vertices.clone(), good.index.clone());
            // The blocks a byte changed in or under, and the bytes that changed in the blobs.
            let mut hit = vec![false; starts.len()];
            let mut blobs_hit: Vec<u64> = Vec::new();
            for _ in 0..1 + rng.next() % 4 {
                // Keep the footers, so the files open.
                match rng.next() % 3 {
                    0 => {
                        let at = (rng.next() % (points.len() as u64 - 8)) as usize;
                        points[at] = rng.next() as u8;
                        hit[starts.partition_point(|&start| start <= at) - 1] = true;
                    }
                    1 => {
                        let at = (rng.next() % (index.len() as u64 - 8)) as usize;
                        index[at] = rng.next() as u8;
                        let first = at / INDEX_ROW_BYTES * SUPERBLOCK_BLOCKS;
                        hit.iter_mut().skip(first).take(SUPERBLOCK_BLOCKS).for_each(|h| *h = true);
                    }
                    _ => {
                        let at = rng.next() % (vertices.len() as u64 - 16);
                        vertices[at as usize] = rng.next() as u8;
                        blobs_hit.push(at);
                    }
                }
            }
            // And files that lost their first bytes: every block, or every blob, is other bytes.
            let cut = round % 10 == 9;
            if cut && round % 20 == 9 {
                points.drain(..1 + (rng.next() % (points.len() as u64 - 8)) as usize);
            } else if cut {
                vertices.drain(..1 + (rng.next() % (vertices.len() as u64 - 16)) as usize);
            }
            let f = open(&points, &vertices, &index).unwrap_or_else(|e| panic!("{e}")).unwrap();
            let mut alone = Vec::new();
            for id in 0..f.count() + 2 {
                let block_whole = !cut && (id as usize) < n && !hit[id as usize / BLOCK_RECORDS];
                let Some(r) = f.record(id) else {
                    assert!(!block_whole, "record {id}");
                    unread += 1;
                    continue;
                };
                read += 1;
                alone.push((id, fields(&r)));
                let (was, was_vertices) = &whole[id as usize];
                if block_whole {
                    assert_eq!(fields(&r), fields(was), "record {id}");
                }
                // A footprint is the vertices its blob's head counts and its logical blob has,
                // or none.
                let has_footprint = draws(&f, &r, &mut buf);
                drawn += has_footprint as usize;
                undrawn += (!has_footprint && r.is_building()) as usize;
                if block_whole && !blobs_hit.iter().any(|&at| at >= r.blob_at && at < r.blob_at + r.blob_len as u64) {
                    assert!(bits(&buf) == *was_vertices, "record {id}'s footprint");
                }
            }
            // A run reads what each id reads alone, of every street and of one.
            let mut run = Vec::new();
            f.for_ids(0..f.count() + 2, |id, r| run.push((id, fields(&r))));
            assert!(run == alone, "round {round}");
            let way = whole[(rng.next() % n as u64) as usize].0.parent_way_id;
            run.clear();
            f.for_ids_on_way(0..f.count() + 2, way, |id, r| run.push((id, fields(&r))));
            alone.retain(|(_, r)| r.4 == way);
            assert!(run == alone, "round {round}");
        }
    }
    assert!(read > 1_000_000 && unread > 10_000 && drawn > 300_000 && undrawn > 1_000, "{read} {unread} {drawn} {undrawn}");
}

#[test]
fn the_reader_gives_what_a_bit_at_a_time_reader_does() {
    let mut rng = Rng::new(5);
    let mut buf = Vec::new();
    let (mut split, mut paired, mut order, mut hybrid) = (0, 0, 0, 0);
    for n in [1, BLOCK_RECORDS, 2 * BLOCK_RECORDS + 1, SUPERBLOCK_RECORDS + 500] {
        for kind in 0..KINDS {
            let records = random_records(&mut rng, n, kind, true);
            let bytes = encoded(&records);
            let slow = read_bit_by_bit(&bytes.points, &bytes.vertices, &bytes.index);
            assert!(logical_of(&slow) == records, "{n} records of kind {kind}");
            let f = files(&bytes);
            assert_eq!(f.count() as usize, slow.records.len());
            for (id, want) in slow.records.iter().enumerate() {
                let r = f.record(id as u32).unwrap_or_else(|| panic!("record {id} of {n} of kind {kind}"));
                assert_eq!(fields(&r), fields(want), "record {id} of {n} of kind {kind}");
                assert_eq!(f.vertex_count(&r), count_of(&slow.footprints[id]));
                if f.footprint(&r, &mut buf) {
                    assert!(bits(&buf) == logical_vertices(&slow.footprints[id]), "record {id}'s footprint");
                }
            }
            for flags in slow.flags {
                split += (flags & 4 != 0) as usize;
                paired += (flags & 4 == 0) as usize;
                order += (flags & 1 != 0) as usize + (flags & 2 != 0) as usize;
                hybrid += (flags & 1 == 0) as usize + (flags & 2 == 0) as usize;
            }
        }
    }
    assert!(split > 50 && paired > 50 && order > 50 && hybrid > 50, "{split} {paired} {order} {hybrid}");
}

#[test]
fn a_run_of_ids_reads_what_each_id_reads_alone_in_any_order() {
    let mut rng = Rng::new(17);
    let n = SUPERBLOCK_RECORDS + 200;
    for kind in 0..KINDS {
        let f = files(&encoded(&random_records(&mut rng, n, kind, true)));
        let all: Vec<u32> = (0..n as u32).collect();
        let cell = |rng: &mut Rng, keep: u64| all.iter().copied().filter(|_| rng.next() % keep == 0).collect::<Vec<u32>>();
        let mut shuffled = cell(&mut rng, 2);
        for i in (1..shuffled.len()).rev() {
            shuffled.swap(i, (rng.next() % (i as u64 + 1)) as usize);
        }
        // Every id; a cell's ascending ids, dense and sparse; ids past the last among them; an
        // id twice over; ids that descend; ids in no order.
        let lists: Vec<Vec<u32>> = vec![
            all.clone(),
            cell(&mut rng, 2),
            cell(&mut rng, 40),
            vec![3, n as u32, n as u32 - 1, NO_DATA, 0],
            all.iter().flat_map(|&id| [id, id]).collect(),
            all.iter().rev().copied().collect(),
            shuffled,
            Vec::new(),
        ];
        for ids in &lists {
            let alone: Vec<(u32, Fields)> = ids.iter().filter_map(|&id| f.record(id).map(|r| (id, fields(&r)))).collect();
            assert_eq!(alone.len(), ids.iter().filter(|&&id| (id as usize) < n).count());
            let mut run = Vec::new();
            f.for_ids(ids.iter().copied(), |id, r| run.push((id, fields(&r))));
            assert!(run == alone, "kind {kind}, {} ids", ids.len());
            // The records under one parent way: those of the run that are.
            for way in [alone.first().map_or(0, |(_, r)| r.4), alone.last().map_or(0, |(_, r)| r.4), NO_DATA, 12345] {
                let on_way: Vec<(u32, Fields)> = alone.iter().filter(|(_, r)| r.4 == way).copied().collect();
                run.clear();
                f.for_ids_on_way(ids.iter().copied(), way, |id, r| run.push((id, fields(&r))));
                assert!(run == on_way, "kind {kind}, {} ids, way {way}", ids.len());
            }
        }
    }
}

#[test]
fn a_footprints_vertices_are_its_logical_blobs_in_every_encoding() {
    let mut rng = Rng::new(7);
    let here = (52.37f32.to_bits(), 4.89f32.to_bits());
    let at = |lat: u32, lng: u32, footprint: Vec<u8>| Logical { lat, lng, housenumber: 1, street: 2, parent: 3, footprint };
    let mut records = Vec::new();
    // Footprints of every kind at points on land, either side of zero and at any 32 bits.
    for i in 0..6000u64 {
        let (lat, lng) = match i % 4 {
            0 | 1 => (float_bits((rng.next() % 1_700_001) as i64 - 850_000, 1e4), float_bits((rng.next() % 3_500_001) as i64 - 1_750_000, 1e4)),
            2 => (float_bits((rng.next() % 2001) as i64 - 1000, 1e6), float_bits((rng.next() % 2001) as i64 - 1000, 1e6)),
            _ => (rng.next() as u32, rng.next() as u32),
        };
        records.push(at(lat, lng, random_footprint(&mut rng, lat, lng, i % 6)));
    }
    // Values of 13 to 16 bits, in every encoding a blob holds.
    for top in [0x1FFF, 0x2000, 0x3FFF, 0x4000, 0x7FFF, 0x8000, 0xFFFF] {
        for enc in 0..3 {
            records.push(at(here.0, here.1, footprint_at(enc, here.0, here.1, &[(top, 0), (top >> 1, top), (0, top >> 2)], 0, 0)));
        }
    }
    // The most vertices a blob holds, open and as a closed ring, and one more: the logical blob
    // as it is, in an encoding below 3.
    for enc in 0..3 {
        let outline = |n: u32| (0..n).map(|k| (k * 7 % 5000, k % 3000)).collect::<Vec<(u32, u32)>>();
        let mut ring = outline(65_534);
        ring.push(ring[0]);
        for q in [outline(65_535), ring, outline(65_536)] {
            records.push(at(here.0, here.1, footprint_at(enc, here.0, here.1, &q, 3, -3)));
        }
    }
    let bytes = encoded(&records);
    let f = files(&bytes);
    let mut buf = Vec::new();
    // What the blobs' heads say: escapes; rings, residuals and deltas, and blobs without each;
    // and each of the three encodings.
    let (mut escapes, mut flags, mut encodings, mut wide) = (0, [[0usize; 2]; 3], [0usize; 3], 0);
    for (id, want) in records.iter().enumerate() {
        assert_reads(&f, id as u32, want, &mut buf, "every encoding");
        let r = f.record(id as u32).unwrap();
        let tag = bytes.vertices[r.blob_at as usize];
        if tag == 0x80 {
            escapes += 1;
            continue;
        }
        encodings[(tag & 3) as usize] += 1;
        for (k, flag) in flags.iter_mut().enumerate() {
            flag[(tag >> (2 + k) & 1) as usize] += 1;
        }
        // The widths' byte after a count of one byte and two residuals: a nibble of 15 is 16 bits.
        wide += (tag & 8 != 0 && bytes.vertices[r.blob_at as usize + 1] < 0x80 && bytes.vertices[r.blob_at as usize + 4] & 15 == 15) as usize;
    }
    let every = encodings.iter().chain(flags.iter().flatten()).all(|&n| n > 500);
    assert!(escapes > 1000 && every && wide > 20, "{escapes} {encodings:?} {flags:?} {wide}");
    // The longest blob is the most vertices at the widths their steps take.
    assert_eq!(f.record(records.len() as u32 - 3).map(|r| f.vertex_count(&r)), Some(65_535));
    assert_eq!(f.record(records.len() as u32 - 1).map(|r| (f.vertex_count(&r), r.blob_len)), Some((65_536, 4 + 10 + 4 * 65_536)));
}

#[test]
fn a_footprints_vertex_is_its_bbox_minimum_plus_its_steps_on_its_encodings_grid() {
    // One vertex, its steps north and east of the bbox minimum, in each encoding; encoding 3's
    // blob is an escape.
    let cases: [(u8, f64, (f32, f32), (u32, u32)); 5] = [
        (0, 1e-5, (50.0, 5.0), (10, 20)),
        (1, 1e-4, (50.0, 5.0), (10, 20)),
        (2, 1e-6, (50.0, 5.0), (10, 20)),
        (3, 1e-7, (50.0, 5.0), (10, 20)),
        (3, 1e-7, (-10.0, 100.0), (1_000_000, 2_000_000)),
    ];
    for (enc, scale, (bmin_lat, bmin_lng), (north, east)) in cases {
        let footprint = logical_blob(enc, bmin_lat.to_bits(), bmin_lng.to_bits(), &[(north, east)]);
        let record = Logical { lat: bmin_lat.to_bits(), lng: bmin_lng.to_bits(), housenumber: 1, street: 2, parent: 3, footprint };
        let f = files(&encoded(&[record]));
        let mut vertices = Vec::new();
        assert!(f.footprint(&f.record(0).expect("a record"), &mut vertices), "encoding {enc}");
        let want = NodeCoord {
            lat: (f64::from(bmin_lat) + f64::from(north) * scale) as f32,
            lng: (f64::from(bmin_lng) + f64::from(east) * scale) as f32,
        };
        assert_eq!(bits(&vertices), bits(&[want]), "encoding {enc}");
    }
}

#[test]
fn a_footprint_is_decoded_into_the_callers_buffer() {
    let mut rng = Rng::new(23);
    let f = files(&encoded(&random_records(&mut rng, 500, 0, true)));
    let mut buf: Vec<NodeCoord> = Vec::with_capacity(128);
    let (start, mut buildings) = (buf.as_ptr(), 0);
    let mut logical: Vec<u8> = Vec::with_capacity(1024);
    let logical_start = logical.as_ptr();
    for id in 0..f.count() {
        let r = f.record(id).unwrap();
        // A buffer that holds another footprint's vertices.
        buf.resize(3, NodeCoord { lat: 1.0, lng: 2.0 });
        let drawn = f.footprint(&r, &mut buf);
        assert_eq!((drawn, buf.len()), (r.is_building(), f.vertex_count(&r) as usize));
        assert!(buf.len() <= 128 && buf.as_ptr() == start && buf.capacity() == 128, "record {id}");
        // Its logical blob, into a buffer that holds another's.
        logical.resize(3, 0xAA);
        assert_eq!((f.logical_blob(&r, &mut logical), count_of(&logical) as usize), (drawn, buf.len()), "record {id}");
        assert!(logical.is_empty() != drawn && logical.as_ptr() == logical_start && logical.capacity() == 1024, "record {id}");
        buildings += drawn as usize;
    }
    assert!(buildings > 100);
}

fn hex(s: &str) -> Vec<u8> {
    if s == "-" {
        return Vec::new();
    }
    (0..s.len() / 2).map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap()).collect()
}

#[test]
fn footprint_blobs_read_as_the_shared_vectors_say_and_the_test_encoder_writes_them() {
    let rows = crate::exact_pip::tests::testdata_rows("addr_files.tsv");
    let mut buf = Vec::new();
    let (mut blobs, mut escapes, mut small, mut full) = (0, 0, 0, 0);
    for r in rows.iter().filter(|r| r[0] == "blob") {
        assert_eq!(r.len(), 5, "{}", r[1]);
        let (lat, lng) = r[2].split_once(':').unwrap();
        let (lat, lng) = (u32::from_str_radix(lat, 16).unwrap(), u32::from_str_radix(lng, 16).unwrap());
        let (logical, blob) = (hex(&r[3]), hex(&r[4]));
        assert_eq!(encode_blob(&logical, lat, lng), blob, "{}", r[1]);
        assert_eq!(logical_of_blob(&blob, lat, lng), logical, "{}", r[1]);
        // The blob as the files of its one record hold it, and the footprint read from them.
        let record = Logical { lat, lng, housenumber: 1, street: 2, parent: 3, footprint: logical };
        let bytes = encoded(std::slice::from_ref(&record));
        assert_eq!(bytes.vertices[..bytes.vertices.len() - 16], blob[..], "{}", r[1]);
        let f = files(&bytes);
        assert_reads(&f, 0, &record, &mut buf, &r[1]);
        assert_eq!(f.record(0).map(|r| (r.blob_at, r.blob_len as usize)), Some((0, blob.len())), "{}", r[1]);
        blobs += 1;
        escapes += (blob[0] == 0x80) as usize;
        small += (blob[0] != 0x80 && blob[0] & 8 != 0) as usize;
        full += (blob[0] != 0x80 && blob[0] & 8 == 0) as usize;
    }
    assert!(blobs >= 63 && escapes >= 2 && small >= 20 && full >= 20, "{blobs} {escapes} {small} {full}");
}

#[test]
fn address_files_read_as_the_shared_vectors_say_and_the_test_encoder_writes_them() {
    let rows = crate::exact_pip::tests::testdata_rows("addr_files.tsv");
    let mut buf = Vec::new();
    let (mut cases, mut records, mut footprints, mut dead, mut second_superblocks) = (0, 0, 0, 0, 0);
    let (mut split, mut paired, mut order, mut hybrid) = (0, 0, 0, 0);
    for r in rows.iter().filter(|r| r[0] == "files") {
        assert_eq!(r.len(), 7, "{}", r[1]);
        let (logical_points, logical_vertices) = (hex(&r[2]), hex(&r[3]));
        let (points, vertices, index) = (hex(&r[4]), hex(&r[5]), hex(&r[6]));
        let ours = encode(&logical_points, &logical_vertices);
        assert!(ours.points == points && ours.vertices == vertices && ours.index == index, "{}", r[1]);
        let want = logical_records(&logical_points, &logical_vertices);
        let f = open(&points, &vertices, &index).unwrap_or_else(|e| panic!("{}: {e}", r[1])).unwrap();
        assert_eq!(f.count() as usize, want.len(), "{}", r[1]);
        for (id, record) in want.iter().enumerate() {
            assert_reads(&f, id as u32, record, &mut buf, &r[1]);
        }
        assert!(f.record(want.len() as u32).is_none(), "{}", r[1]);
        let slow = read_bit_by_bit(&points, &vertices, &index);
        assert!(logical_of(&slow) == want, "{}", r[1]);
        cases += 1;
        records += want.len();
        footprints += want.iter().filter(|record| !record.footprint.is_empty()).count();
        dead += want.iter().filter(|record| **record == tombstone()).count();
        second_superblocks += (want.len() > SUPERBLOCK_RECORDS) as usize;
        for flags in slow.flags {
            split += (flags & 4 != 0) as usize;
            paired += (flags & 4 == 0) as usize;
            order += (flags & 1 != 0) as usize + (flags & 2 != 0) as usize;
            hybrid += (flags & 1 == 0) as usize + (flags & 2 == 0) as usize;
        }
    }
    let counts = (cases, records, footprints, dead, second_superblocks);
    assert!(cases >= 38 && records >= 1400 && footprints >= 200 && dead >= 20 && second_superblocks >= 1, "{counts:?}");
    assert!(split >= 10 && paired >= 10 && order >= 5 && hybrid >= 50, "{split} {paired} {order} {hybrid}");
}
