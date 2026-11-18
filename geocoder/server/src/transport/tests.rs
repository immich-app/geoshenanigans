use super::container::{self, col, unit_col, Chunk, ColSpec, Input, B, D, P, R, RAW_D, RAW_P, RAW_S, S};
use super::*;
use crate::arc_blocks::{arc_vertices, ArcFiles};
use crate::arc_fixture::{arc_block_of_deltas, ArcFixture, RawBox};
use crate::exact_pip::tests::testdata_rows;
use crate::poi_files::{PoiPolygon, PoiRecords};
use crate::scratch_dir::ScratchDir;
use crate::test_heap as heap;
use crate::test_rng::Rng;
use crate::{AdminPolygon, FileBytes, PlaceNode, PoiRecord, PointE7, NO_DATA, PLACE_FLAG_HIDDEN, PLACE_FLAG_LABEL_OF};
use std::cell::RefCell;
use std::io::{self, Read, Write};
use std::ops::Range;
use std::path::Path;
use std::rc::Rc;

fn hex(s: &str) -> Vec<u8> {
    if s == "-" {
        return Vec::new();
    }
    (0..s.len() / 2).map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap()).collect()
}

fn to_hex(b: &[u8]) -> String {
    if b.is_empty() {
        return "-".into();
    }
    b.iter().map(|x| format!("{x:02x}")).collect()
}

type Files = Vec<(&'static str, Vec<u8>)>;

/// A scratch dir holding `files`.
fn dir_of(files: &[(&str, Vec<u8>)]) -> ScratchDir {
    let d = ScratchDir::new();
    for (name, bytes) in files {
        std::fs::write(d.path().join(name), bytes).unwrap();
    }
    d
}

fn unit_of(kind: Kind, dir: &Path, file: Option<&str>) -> Vec<u8> {
    let mut unit = Vec::new();
    encode(kind, dir, file, &mut unit).unwrap_or_else(|e| panic!("encode {}: {e}", kind.name()));
    unit
}

/// A writer whose bytes the test keeps once decode has dropped it.
#[derive(Clone, Default)]
struct Shared(Rc<RefCell<Vec<u8>>>);

impl Write for Shared {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.0.borrow_mut().extend_from_slice(b);
        Ok(b.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

type Decode = Result<(Kind, Vec<(String, Vec<u8>)>), (io::Error, Vec<Vec<u8>>)>;

/// `unit` decoded in memory: its kind and members, or its error and what each member got before
/// it. Decode leaves nothing in its scratch dir either way.
fn decoded(unit: &[u8]) -> Decode {
    let scratch = ScratchDir::new();
    let mut got: Vec<Shared> = Vec::new();
    let r = decode(unit, scratch.path(), |_| {
        let s = Shared::default();
        got.push(s.clone());
        Ok(s)
    });
    assert!(std::fs::read_dir(scratch.path()).unwrap().next().is_none(), "decode left a file in its scratch dir");
    match r {
        Ok(d) => Ok((d.kind, d.members.into_iter().map(|(m, s)| (m.name, s.0.take())).collect())),
        Err(e) => Err((e, got.iter().map(|s| s.0.borrow().clone()).collect())),
    }
}

fn refusal(unit: &[u8]) -> String {
    match decoded(unit) {
        Ok(_) => panic!("decoded a unit it should refuse"),
        Err((e, _)) => e.to_string(),
    }
}

/// The unit of kind (of `file`, for cols) holding `files`, whose members decode as they went.
fn round_trip(kind: Kind, file: Option<&str>, files: &[(&str, Vec<u8>)]) -> Vec<u8> {
    let dir = dir_of(files);
    let unit = unit_of(kind, dir.path(), file);
    let (k, members) = decoded(&unit).unwrap_or_else(|(e, _)| panic!("decode {}: {e}", kind.name()));
    assert_eq!(k, kind);
    let names: Vec<&str> = members.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names, files.iter().map(|(n, _)| *n).collect::<Vec<_>>());
    for ((name, got), (_, want)) in members.iter().zip(files) {
        let first = got.iter().zip(want).position(|(a, b)| a != b);
        assert!(got == want, "{name}: {} bytes back for {}, the first differing at {first:?}", got.len(), want.len());
    }
    unit
}

fn members_of(kind: Kind, bytes: Vec<Vec<u8>>) -> Files {
    kind.members().iter().copied().zip(bytes).collect()
}

fn arc_files(fx: &ArcFixture) -> Files {
    members_of(Kind::Arcs, vec![fx.arc_index(), fx.arc_vertices()])
}

/// A unit's sections: each one's chunks, as the units they hold and the bytes their columns take
/// and where they lie in the unit, and where its tail lies.
struct Sections(Vec<(Vec<(u32, usize, Range<usize>)>, Range<usize>)>);

fn sections_of(unit: &[u8]) -> Sections {
    let mut r = Input::new(unit);
    let head = container::read_head(&mut r).unwrap();
    let at = |r: &Input<&[u8]>| r.pos() as usize;
    let columns: Vec<usize> = match head.kind {
        Kind::Places => vec![2, 9, 4],
        Kind::Pois => vec![10, 10],
        Kind::Arcs => vec![7],
        Kind::Cols => vec![head.fields.unwrap().iter().map(|f| f.1 as usize + 1).max().unwrap()],
    };
    let mut out = Vec::new();
    for n in columns {
        let mut chunks = Vec::new();
        loop {
            let start = at(&r);
            let units = r.u32().unwrap();
            if units == 0 {
                break;
            }
            let mut bytes = 0;
            for _ in 0..n {
                r.u8().unwrap();
                bytes += r.u32().unwrap() as usize;
            }
            r.exact(&mut vec![0; bytes]).unwrap();
            chunks.push((units, bytes, start..at(&r)));
        }
        let tail = r.u32().unwrap() as usize;
        let start = at(&r);
        r.exact(&mut vec![0; tail]).unwrap();
        out.push((chunks, start..start + tail));
    }
    Sections(out)
}

/// A stream written by hand, for units no encoder writes.
struct Stream(Vec<u8>);

impl Stream {
    fn head(kind: Kind, members: &[(&str, u64)], fields: Option<&[(u8, u16)]>) -> Stream {
        let members: Vec<Member> = members.iter().map(|&(n, size)| Member { name: n.into(), size }).collect();
        let mut s = Vec::new();
        container::write_head(&mut s, kind, &members, fields).unwrap();
        Stream(s)
    }

    /// A chunk of `units` and these columns, each its encoding and its bytes.
    fn chunk(mut self, units: u32, cols: &[(u8, Vec<u8>)]) -> Stream {
        self.0.extend(units.to_le_bytes());
        for (enc, bytes) in cols {
            self.0.push(*enc);
            self.0.extend((bytes.len() as u32).to_le_bytes());
        }
        for (_, bytes) in cols {
            self.0.extend(bytes);
        }
        self
    }

    fn end(mut self, tail: &[u8]) -> Stream {
        self.0.extend(0u32.to_le_bytes());
        self.0.extend((tail.len() as u32).to_le_bytes());
        self.0.extend(tail);
        self
    }
}

/// A column of `w`-byte values as they are (R).
fn raw(w: usize, vals: &[u64]) -> (u8, Vec<u8>) {
    (R, vals.iter().flat_map(|v| v.to_le_bytes()[..w].to_vec()).collect())
}

// ------------------------------------------------------------------ the column encodings

/// `vals` as a column of one chunk under `spec`, read back: the encoding written and the values.
fn column_back(vals: &[u64], spec: ColSpec, starts: &[usize]) -> (u8, Vec<u64>) {
    let mut bytes = Vec::new();
    let enc = container::encode_column(vals, spec, starts, &mut bytes);
    let s = Stream(Vec::new()).chunk(1, &[(enc, bytes)]);
    let mut chunk = Chunk::default();
    assert_eq!(chunk.read(&mut Input::new(&s.0[..]), &[spec]).unwrap(), Some(1));
    let mut c = chunk.col(0);
    let mut next = starts.iter().peekable();
    let back = (0..vals.len())
        .map(|i| {
            while next.peek().is_some_and(|&&s| s <= i) {
                next.next();
                c.unit_start();
            }
            c.next(&chunk.buf)
        })
        .collect();
    (enc, back)
}

#[test]
fn every_encoding_gives_back_its_values_at_every_width() {
    let mut rng = Rng::new(17);
    for w in [1usize, 2, 4, 8] {
        let m = if w == 8 { u64::MAX } else { (1u64 << (8 * w)) - 1 };
        // Both ends, all-ones values among them, runs, wraps past 0 and past the top, and a step
        // of all ones (5 to 4).
        let mut vals = vec![0, 1, m, m - 1, 2, 0, m, 5, 5, 4, 7, m / 2, m / 2 + 1, 3];
        vals.extend((0..300).map(|_| rng.next() & m));
        let starts = [0, 4, 4, 9, 200];
        for enc in [R, B, D, RAW_D, S, RAW_S, P, RAW_P] {
            let spec = if matches!(enc, P | RAW_P) { unit_col(w, enc) } else { col(w, enc) };
            let (used, back) = column_back(&vals, spec, &starts);
            assert_eq!(back, vals, "width {w}, encoding {}", enc as char);
            // The step of all ones makes S a delta.
            let want = match enc {
                S => D,
                RAW_S => RAW_D,
                e => e,
            };
            assert_eq!(used as char, want as char, "width {w}");
        }
        // Without a step of all ones S stays S.
        let steps: Vec<u64> = (0..60u64).map(|i| if i % 9 == 0 { m } else { i * 3 }).collect();
        assert_eq!(column_back(&steps, col(w, S), &[]), (S, steps.clone()));
    }
}

// ------------------------------------------------------------------ the shared vectors

#[test]
fn place_files_of_the_shared_vectors_come_back_byte_for_byte() {
    let rows = testdata_rows("place_files.tsv");
    for r in &rows {
        round_trip(Kind::Places, None, &members_of(Kind::Places, vec![hex(&r[2]), hex(&r[3]), hex(&r[4])]));
    }
    assert!(rows.len() >= 9);
}

/// The bits of the widest zz delta of `k`-grid vertices, first- or second-order.
fn widest_delta(k: u8, v: &[(i64, i64)], second: bool) -> u32 {
    let step = 10i64.pow(k as u32);
    let mut prev = (0i64, 0i64);
    let mut widest = 0;
    for e in v.windows(2) {
        let d = ((e[1].0 - e[0].0) / step, (e[1].1 - e[0].1) / step);
        for (x, p) in [(d.0, prev.0), (d.1, prev.1)] {
            let z = container::zigzag(if second { x - p } else { x });
            widest = widest.max(64 - z.leading_zeros());
        }
        prev = d;
    }
    widest
}

#[test]
fn arc_blocks_of_the_shared_vectors_come_back_byte_for_byte() {
    let rows = testdata_rows("arc_blocks.tsv");
    let (mut fx, mut widest) = (ArcFixture::new(), [0u32; 2]);
    for (i, r) in rows.iter().enumerate() {
        let block = hex(&r[3]);
        let mut one = ArcFixture::new();
        one.arc_block(block.clone(), RawBox::EMPTY);
        round_trip(Kind::Arcs, None, &arc_files(&one));
        if i % 7 == 3 {
            fx.dead_arc();
        }
        fx.arc_block(block.clone(), RawBox::EMPTY);
        let k: u8 = r[0].parse().unwrap();
        let v: Vec<(i64, i64)> = r[2].split(';').map(|p| p.split_once(',').unwrap()).map(|(a, b)| (a.parse().unwrap(), b.parse().unwrap())).collect();
        let second = block[0] & crate::arc_blocks::SECOND_ORDER != 0;
        let w = &mut widest[second as usize];
        *w = (*w).max(widest_delta(k, &v, second));
    }
    round_trip(Kind::Arcs, None, &arc_files(&fx));
    // Deltas no u32 holds: a first-order one of 33 bits, a second-order one of 34.
    assert_eq!(widest, [33, 34]);
}

#[test]
fn a_45_bit_delta_comes_back() {
    // The widest delta a group holds, at base width 30 and 15 more: out to 2^44 - 1 and back. No
    // reader takes its vertices, which leave i32, but its block packs as any other.
    let big = (1i64 << 44) - 1;
    let block = arc_block_of_deltas(0, (5, -5), &[(big, 3), (-big, -3), (1, 1)]);
    assert_eq!((block[0] >> 3 & 15, block[9] >> 4), (15, 15));
    let mut fx = ArcFixture::new();
    fx.arc_block(block, RawBox::EMPTY);
    fx.arc(0, &[PointE7 { lat: 7, lng: 8 }, PointE7 { lat: 9, lng: 10 }]);
    round_trip(Kind::Arcs, None, &arc_files(&fx));
}

#[test]
fn poi_files_of_the_shared_vectors_come_back_byte_for_byte() {
    let rows = testdata_rows("poi_files.tsv");
    let mut alone = 0;
    for r in &rows {
        let unit = round_trip(Kind::Pois, None, &members_of(Kind::Pois, vec![hex(&r[3]), hex(&r[4])]));
        let s = sections_of(&unit);
        for &(units, bytes, _) in &s.0[1].0 {
            assert!(bytes <= CHUNK_BYTES || units == 1, "{}: a chunk of {units} polygons in {bytes} bytes", r[0]);
            alone += (bytes > CHUNK_BYTES) as usize;
        }
    }
    assert!(rows.len() >= 6);
    // The polygon of 140,000 vertices.
    assert_eq!(alone, 1);
}

#[test]
fn way_rows_and_address_indexes_of_the_shared_vectors_come_back_byte_for_byte() {
    let rows = testdata_rows("way_files.tsv");
    for r in &rows {
        let file = if r[1] == "street" { "street_ways.bin" } else { "interp_ways.bin" };
        round_trip(Kind::Cols, Some(file), &[(file, hex(&r[3]))]);
    }
    assert!(rows.len() > 20);
    let files: Vec<_> = testdata_rows("addr_files.tsv").into_iter().filter(|r| r[0] == "files").collect();
    for r in &files {
        round_trip(Kind::Cols, Some("addr_index.bin"), &[("addr_index.bin", hex(&r[6]))]);
    }
    assert!(files.len() > 10);
}

// ------------------------------------------------------------------ fixture datasets

fn grid_walk(rng: &mut Rng, k: u8, n: usize, reach: u64) -> Vec<PointE7> {
    let step = 10i64.pow(k as u32);
    let on_grid = |c: i64, lim: i64| c.clamp(-lim, lim) / step * step;
    let (mut lat, mut lng) = (on_grid((rng.next() % 1_600_000_000) as i64 - 800_000_000, 900_000_000), on_grid((rng.next() % 3_400_000_000) as i64 - 1_700_000_000, 1_800_000_000));
    let mut v = Vec::with_capacity(n);
    for _ in 0..n {
        v.push(PointE7 { lat, lng });
        lat = on_grid(lat + ((rng.next() % (2 * reach + 1)) as i64 - reach as i64) * step, 900_000_000);
        lng = on_grid(lng + ((rng.next() % (2 * reach + 1)) as i64 - reach as i64) * step, 1_800_000_000);
    }
    v
}

fn poi(rng: &mut Rng, name_id: u32) -> PoiRecord {
    PoiRecord {
        lat: rng.range(-80.0, 80.0) as f32,
        lng: rng.range(-179.0, 179.0) as f32,
        vertex_offset: NO_DATA,
        name_id,
        category: (rng.next() % 40) as u8,
        tier: 1 + (rng.next() % 3) as u8,
        flags: (rng.next() % 4) as u8,
        importance: (rng.next() % 256) as u8,
        parent_street_id: if rng.next() % 3 == 0 { NO_DATA } else { (rng.next() % 1_000_000) as u32 },
        parent_postcode_id: if rng.next() % 4 == 0 { NO_DATA } else { (rng.next() % 300) as u32 * 11 },
        parent_poly_id: if rng.next() % 5 == 0 { NO_DATA } else { (rng.next() % 0xFF_FFFF) as u32 },
    }
}

/// A polygon of `n` vertices of `encoding` whose offsets are below 2^bits, closed if it says.
fn polygon(rng: &mut Rng, encoding: u8, n: usize, bits: u32, closed: bool) -> PoiPolygon {
    let mut q: Vec<(u32, u32)> = (0..n).map(|_| ((rng.next() % (1u64 << bits)) as u32, (rng.next() % (1u64 << bits)) as u32)).collect();
    if closed {
        q.push(q[0]);
    }
    PoiPolygon { encoding, bmin_lat: rng.range(-60.0, 60.0) as f32, bmin_lng: rng.range(-170.0, 170.0) as f32, q }
}

#[test]
fn fixture_datasets_come_back_and_the_readers_read_them_as_before() {
    let mut rng = Rng::new(20261117);
    // Places: visible nodes over the globe, hidden ones and labels.
    let nodes: Vec<PlaceNode> = (0..3000u32)
        .map(|i| {
            let (lat, lng) = (rng.range(-85.0, 85.0) as f32, rng.range(-180.0, 180.0) as f32);
            let hidden = i % 9 == 4;
            let label = hidden && i % 2 == 0;
            PlaceNode {
                lat,
                lng,
                name_id: (rng.next() % (1 << 27)) as u32 >> (i % 20),
                place_type: (rng.next() % 7) as u8,
                _pad1: if hidden { PLACE_FLAG_HIDDEN | if label { PLACE_FLAG_LABEL_OF } else { 0 } } else { 0 },
                _pad2: 0,
                parent_poly_id: if label { i * 3 } else { NO_DATA },
            }
        })
        .collect();
    let (files, ids) = crate::place_files::encode(&nodes, crate::DEFAULT_ADMIN_CELL_LEVEL);
    let unit = round_trip(Kind::Places, None, &members_of(Kind::Places, vec![files.cells, files.nodes, files.hidden]));
    let (_, back) = decoded(&unit).unwrap_or_else(|(e, _)| panic!("{e}"));
    let mut back = back.into_iter().map(|(_, b)| FileBytes::Owned(b));
    let places = crate::PlaceFiles::open(back.next().unwrap(), back.next().unwrap(), back.next().unwrap()).unwrap();
    for (n, &id) in nodes.iter().zip(&ids) {
        let got = places.node(id).unwrap();
        let fields = |p: &PlaceNode| (p.lat.to_bits(), p.lng.to_bits(), p.name_id, p.place_type, p._pad1, p.parent_poly_id);
        assert_eq!(fields(&got), fields(n), "node {id}");
    }
    // POIs: points and polygons of every encoding, open and closed.
    let pois: Vec<(PoiRecord, Option<PoiPolygon>)> = (0..2000u32)
        .map(|i| {
            let r = poi(&mut rng, i * 7);
            let p = (i % 3 == 1).then(|| {
                let encoding = (i / 3 % 4) as u8;
                let bits = if encoding == 3 { 1 + (i % 32) } else { 1 + (i % 16) };
                polygon(&mut rng, encoding, 1 + (i % 50) as usize, bits, i % 2 == 0)
            });
            (r, p)
        })
        .collect();
    let (r2, v2) = crate::poi_files::encode(&pois);
    let unit = round_trip(Kind::Pois, None, &members_of(Kind::Pois, vec![r2.clone(), v2.clone()]));
    let (_, back) = decoded(&unit).unwrap_or_else(|(e, _)| panic!("{e}"));
    let (was, now) = (PoiRecords::open(FileBytes::Owned(r2)).unwrap(), PoiRecords::open(FileBytes::Owned(back[0].1.clone())).unwrap());
    let (v_was, v_now) = (FileBytes::Owned(v2), FileBytes::Owned(back[1].1.clone()));
    let coords = |p: Option<Vec<crate::NodeCoord>>| p.map(|v| v.iter().map(|c| (c.lat, c.lng)).collect::<Vec<_>>());
    for i in 0..pois.len() as u32 {
        let (a, b) = (was.record(i).unwrap(), now.record(i).unwrap());
        assert_eq!(a, b);
        assert_eq!(coords(now.polygon(&v_now, &b)), coords(was.polygon(&v_was, &a)), "record {i}");
        assert_eq!(a.is_polygon(), i % 3 == 1);
    }
    // Arcs at every grid, boxed from 16 vertices, dead slots among them; the polygons of their
    // rings as a cols unit.
    let mut fx = ArcFixture::new();
    let mut arcs = Vec::new();
    for i in 0..400usize {
        if i % 37 == 5 {
            fx.dead_arc();
            arcs.push(None);
            continue;
        }
        let k = [0u8, 2, 3][i % 3];
        let v = grid_walk(&mut rng, k, 1 + i % 60, [5, 2_000, 2_000_000][i % 3 / 2 + i % 2]);
        fx.arc(k, &v);
        arcs.push(Some(v));
    }
    for i in 0..40u32 {
        let refs = [crate::arc_fixture::Ref::fwd(i * 3), crate::arc_fixture::Ref::rev(i * 3 + 1)];
        fx.polygon(crate::arc_fixture::record(i, 8, 1.5), &[refs.to_vec()]);
    }
    fx.tombstone();
    let unit = round_trip(Kind::Arcs, None, &arc_files(&fx));
    let (_, back) = decoded(&unit).unwrap_or_else(|(e, _)| panic!("{e}"));
    let files = ArcFiles { index: &back[0].1, vertices: &back[1].1 };
    for (slot, want) in arcs.iter().enumerate() {
        let got = files.block(slot as u32).map(|b| {
            let mut v = Vec::new();
            arc_vertices(b, |p| v.push(p)).unwrap();
            v
        });
        assert_eq!(got.as_ref(), want.as_ref(), "slot {slot}");
    }
    let polygons = fx.files()[0].1.clone();
    round_trip(Kind::Cols, Some("admin_polygons.bin"), &[("admin_polygons.bin", polygons)]);
}

#[test]
fn empty_members_of_every_kind_come_back() {
    let (places, _) = crate::place_files::encode(&[], crate::DEFAULT_ADMIN_CELL_LEVEL);
    round_trip(Kind::Places, None, &members_of(Kind::Places, vec![places.cells, places.nodes, places.hidden]));
    let (r2, v2) = crate::poi_files::encode(&[]);
    round_trip(Kind::Pois, None, &members_of(Kind::Pois, vec![r2, v2]));
    round_trip(Kind::Arcs, None, &members_of(Kind::Arcs, vec![Vec::new(), Vec::new()]));
    for f in COLS_FILES {
        round_trip(Kind::Cols, Some(f), &[(f, Vec::new())]);
    }
    // A cols file of part of a row: all tail.
    round_trip(Kind::Cols, Some("addr_index.bin"), &[("addr_index.bin", vec![7; 399])]);
}

#[test]
fn a_polygon_and_an_arc_past_chunk_bytes_each_go_alone_in_a_chunk() {
    let mut rng = Rng::new(5);
    // 150,000 u32 vertices: 1,200,023 bytes of columns, between POIs that share chunks.
    let mut pois: Vec<(PoiRecord, Option<PoiPolygon>)> = (0..40_000).map(|i| (poi(&mut rng, i), Some(polygon(&mut rng, (i % 3) as u8, 3, 12, false)))).collect();
    pois[20_000].1 = Some(polygon(&mut rng, 3, 150_000, 18, true));
    let (r2, v2) = crate::poi_files::encode(&pois);
    let unit = round_trip(Kind::Pois, None, &members_of(Kind::Pois, vec![r2, v2]));
    // 80,000 deltas: 1,285,021 bytes of columns.
    let mut fx = ArcFixture::new();
    for i in 0..3000 {
        let n = if i == 1500 { 80_001 } else { 2 + i % 40 };
        fx.arc(0, &grid_walk(&mut rng, 0, n, 3_000));
    }
    let arcs = round_trip(Kind::Arcs, None, &arc_files(&fx));
    for (unit, section) in [(&unit, 1), (&arcs, 0)] {
        let chunks = &sections_of(unit).0[section].0;
        let alone: Vec<_> = chunks.iter().filter(|c| c.1 > CHUNK_BYTES).collect();
        assert_eq!(alone.len(), 1);
        assert_eq!(alone[0].0, 1);
        assert!(chunks.len() >= 3 && chunks.iter().all(|c| c.1 <= CHUNK_BYTES || c.0 == 1));
    }
}

#[test]
fn a_dictionary_of_65535_entries_and_a_block_past_256_kib_come_back() {
    let mut rng = Rng::new(65535);
    let mut pois: Vec<(PoiRecord, Option<PoiPolygon>)> = (0..65_535u32)
        .map(|i| {
            let r = PoiRecord { importance: i as u8, flags: (i >> 8) as u8, parent_postcode_id: i * 4, ..poi(&mut rng, i) };
            (r, None)
        })
        .collect();
    // 70,000 vertices of 32 bits each: a block of 280,009 bytes.
    pois[100].1 = Some(polygon(&mut rng, 1, 70_000, 16, false));
    let (r2, v2) = crate::poi_files::encode(&pois);
    assert!(v2.len() > 256 << 10);
    let unit = round_trip(Kind::Pois, None, &members_of(Kind::Pois, vec![r2, v2]));
    // The dictionaries and the footer: R2's tail.
    let tail = sections_of(&unit).0[0].1.len();
    assert_eq!(tail, 65_535 * 5 + 65_535 * 4 + 24);
}

// ------------------------------------------------------------------ the golden units

/// The sets the golden units hold: a case, its kind, its cols file, its members.
fn golden_sets() -> Vec<(&'static str, Kind, Option<&'static str>, Files)> {
    let place = testdata_rows("place_files.tsv").into_iter().find(|r| r[0] == "two cells, a label and a hidden node").unwrap();
    let pois = testdata_rows("poi_files.tsv").into_iter().find(|r| r[0] == "the widths' and varints' edges").unwrap();
    let mut fx = ArcFixture::new();
    fx.arc(0, &[PointE7 { lat: 10, lng: -20 }, PointE7 { lat: 13, lng: -18 }, PointE7 { lat: -7, lng: 2 }]);
    fx.dead_arc();
    let line: Vec<PointE7> = (0..17).map(|i| PointE7 { lat: 450_000_000 + 300 * i, lng: 80_000_000 - 500 * i + 100 * (i % 3) }).collect();
    fx.arc(2, &line);
    fx.arc(3, &[PointE7 { lat: -1000, lng: 2000 }]);
    let mut polygons = Vec::new();
    for (ring_offset, name_id, area, country_code, admin_level) in [(0, 7, 0.5f32, 528u16, 2u8), (96, 11, 0.0625, 528, 8), (160, 12, 3.25, 36, 4)] {
        let p = AdminPolygon { ring_offset, ring_bytes: 0, name_id, admin_level, place_type_override: (admin_level == 8) as u8, area, country_code };
        polygons.extend(p.record_bytes());
    }
    polygons.extend([1, 2, 3, 4, 5]);
    vec![
        ("two cells, a label and a hidden node", Kind::Places, None, members_of(Kind::Places, vec![hex(&place[2]), hex(&place[3]), hex(&place[4])])),
        ("the widths' and varints' edges", Kind::Pois, None, members_of(Kind::Pois, vec![hex(&pois[3]), hex(&pois[4])])),
        ("three arcs, a dead slot, a box", Kind::Arcs, None, arc_files(&fx)),
        ("three polygons and part of a row", Kind::Cols, Some("admin_polygons.bin"), vec![("admin_polygons.bin", polygons)]),
    ]
}

const GOLDEN: &str = "transport_units.tsv";

/// Writes geocoder/testdata/transport_units.tsv: cargo test --release write_golden_units -- --ignored.
#[test]
#[ignore]
fn write_golden_units() {
    let mut out = String::from(
        "# Transport units of a few hundred bytes, a kind each: what the case covers, its kind, its cols\n\
         # file (- for the other kinds), its members (name:hex, joined by ;) and the unit as hex (the\n\
         # stream before zstd, transport.rs). The server's transport tests decode each unit to its\n\
         # members and encode the members back to it, which pins the stream: a change to these is a\n\
         # change of TRANSPORT_VERSION. Written by the transport tests' write_golden_units.\n\
         case\tkind\tfile\tmembers\tunit\n",
    );
    for (case, kind, file, members) in golden_sets() {
        let dir = dir_of(&members);
        let unit = unit_of(kind, dir.path(), file);
        let m: Vec<String> = members.iter().map(|(n, b)| format!("{n}:{}", to_hex(b))).collect();
        out += &format!("{case}\t{}\t{}\t{}\t{}\n", kind.name(), file.unwrap_or("-"), m.join(";"), to_hex(&unit));
    }
    std::fs::write(format!("{}/../testdata/{GOLDEN}", env!("CARGO_MANIFEST_DIR")), out).unwrap();
}

/// The golden units, as (kind, file, members, unit).
fn golden_units() -> Vec<(Kind, Option<String>, Files, Vec<u8>)> {
    testdata_rows(GOLDEN)
        .into_iter()
        .map(|r| {
            let kind = Kind::from_name(&r[1]).unwrap();
            let file = (r[2] != "-").then(|| r[2].clone());
            let members: Files = r[3]
                .split(';')
                .map(|m| {
                    let (name, bytes) = m.split_once(':').unwrap();
                    let name = Kind::ALL.iter().flat_map(|k| k.members()).chain(&COLS_FILES).find(|&&n| n == name).unwrap();
                    (*name, hex(bytes))
                })
                .collect();
            (kind, file, members, hex(&r[4]))
        })
        .collect()
}

#[test]
fn the_golden_units_decode_to_their_members_and_encode_back() {
    let units = golden_units();
    assert_eq!(units.iter().map(|u| u.0).collect::<Vec<_>>(), Kind::ALL);
    for (kind, file, members, unit) in &units {
        assert!(unit.len() < 1024, "{}: {} bytes", kind.name(), unit.len());
        let (k, got) = decoded(unit).unwrap_or_else(|(e, _)| panic!("{e}"));
        assert_eq!(k, *kind);
        assert_eq!(got, members.iter().map(|(n, b)| (n.to_string(), b.clone())).collect::<Vec<_>>());
        let dir = dir_of(members);
        assert!(unit_of(*kind, dir.path(), file.as_deref()) == *unit, "{} encodes to another unit", kind.name());
    }
    // The sets they hold are the sets the golden writer takes.
    let sets = golden_sets();
    for ((_, _, members, _), (case, _, _, want)) in units.iter().zip(&sets) {
        assert!(members == want, "{case}");
    }
}

// ------------------------------------------------------------------ refusals

#[test]
fn a_truncated_unit_is_refused_and_its_members_get_only_what_came_before() {
    for (kind, _, members, unit) in golden_units() {
        for len in 0..unit.len() {
            let (e, got) = decoded(&unit[..len]).err().unwrap_or_else(|| panic!("{} decoded from {len} bytes", kind.name()));
            assert!(e.to_string().contains("ends early"), "{}: {e}", kind.name());
            for (g, (name, want)) in got.iter().zip(&members) {
                assert!(want.starts_with(g), "{name} from {len} bytes of the unit");
            }
        }
    }
}

#[test]
fn a_unit_with_bytes_past_its_end_or_another_version_or_kind_is_refused() {
    let units = golden_units();
    let unit = &units[0].3;
    assert!(refusal(&[unit.as_slice(), &[0]].concat()).contains("bytes after the last section"));
    let mut v1 = unit.clone();
    v1[4] = 1;
    assert_eq!(refusal(&v1), "transport: a unit of transport version 1: this build reads version 2 only");
    let mut k9 = unit.clone();
    k9[8] = 9;
    assert_eq!(refusal(&k9), "transport: a unit of kind 9, which this build doesn't know");
    assert_eq!(refusal(b"GTT2\x01\x00\x00\x00"), "transport: not a transport unit (no GTT1)");
    // A places unit naming its members out of order, a cols file named as a path.
    let out_of_order = Stream::head(Kind::Places, &[("place_nodes.bin", 8), ("place_cells.bin", 0), ("place_hidden.bin", 0)], None);
    assert!(refusal(&out_of_order.0).contains("member place_nodes.bin where place_cells.bin goes"));
    let path = Stream::head(Kind::Cols, &[("../x.bin", 0)], Some(&[(4, 0)]));
    assert!(refusal(&path.0).contains("no plain file name"));
    let width = Stream::head(Kind::Cols, &[("x.bin", 0)], Some(&[(3, 0)]));
    assert!(refusal(&width.0).contains("a field of 3 bytes"));
    let mixed = Stream::head(Kind::Cols, &[("x.bin", 0)], Some(&[(4, 0), (2, 0)]));
    assert!(refusal(&mixed.0).contains("a field of 2 bytes in a column of 4-byte values"));
    let skipped = Stream::head(Kind::Cols, &[("x.bin", 0)], Some(&[(4, 0), (4, 2)]));
    assert!(refusal(&skipped.0).contains("a field in column 2 where the next is 1"));
}

#[test]
fn an_encoding_a_column_cant_hold_is_refused() {
    let cols = |enc: u8| Stream::head(Kind::Cols, &[("x.bin", 4)], Some(&[(4, 0)])).chunk(1, &[(enc, 7u32.to_le_bytes().to_vec())]).end(&[]).0;
    assert_eq!(decoded(&cols(R)).map(|d| d.1).ok(), Some(vec![("x.bin".to_string(), 7u32.to_le_bytes().to_vec())]));
    assert!(refusal(&cols(b'X')).contains("a column encoded 'X'"));
    // P restarts at a polygon's first vertex: a row's field has none.
    assert!(refusal(&cols(P)).contains("a column encoded 'P'"));
    let part = Stream::head(Kind::Cols, &[("x.bin", 4)], Some(&[(4, 0)])).chunk(1, &[(R, vec![1, 2, 3])]).end(&[]);
    assert!(refusal(&part.0).contains("a column of 3 bytes of 4-byte values"));
}

/// A reader counting what it hands out.
struct Counted<'a>(&'a [u8], Rc<RefCell<usize>>);

impl Read for Counted<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.0.read(buf)?;
        *self.1.borrow_mut() += n;
        Ok(n)
    }
}

#[test]
fn a_chunk_past_chunk_bytes_is_refused_before_its_bytes_are_read() {
    let head = || Stream::head(Kind::Cols, &[("x.bin", 1 << 30)], Some(&[(1, 0)]));
    // Two rows that claim a column of CHUNK_BYTES + 1, one that claims MAX_CHUNK_BYTES + 1.
    for (units, len) in [(2u32, CHUNK_BYTES + 1), (1, MAX_CHUNK_BYTES + 1)] {
        let mut s = head().0;
        s.extend(units.to_le_bytes());
        s.push(R);
        s.extend((len as u32).to_le_bytes());
        s.extend(vec![0; 64]);
        let read = Rc::new(RefCell::new(0));
        let scratch = ScratchDir::new();
        let e = decode(Counted(&s, read.clone()), scratch.path(), |_| Ok(io::sink())).err().unwrap();
        assert!(e.to_string().contains(&format!("a chunk of {units} units past")), "{e}");
        assert_eq!(*read.borrow(), s.len() - 64);
    }
}

/// A places unit of no cells and no hidden nodes whose one block has entry `entry` (lat base,
/// lng base, wl, wg, wn) and the rows `rows`.
fn place_block(entry: [u64; 5], rows: &[[u64; 4]], nodes_size: u64) -> Vec<u8> {
    let field = |i: usize, w: usize| raw(w, &rows.iter().map(|r| r[i]).collect::<Vec<_>>());
    let cols = [raw(4, &[entry[0]]), raw(4, &[entry[1]]), raw(1, &[entry[2]]), raw(1, &[entry[3]]), raw(1, &[entry[4]]), field(0, 4), field(1, 4), field(2, 1), field(3, 4)];
    Stream::head(Kind::Places, &[("place_cells.bin", 0), ("place_nodes.bin", nodes_size), ("place_hidden.bin", 0)], None)
        .end(&[])
        .chunk(1, &cols)
        .end(&[1, 0, 0, 0, b'P', b'N', b'L', b'3'])
        .end(&[])
        .0
}

#[test]
fn a_width_past_its_familys_most_or_a_value_past_its_width_is_refused() {
    // One row of a 7-bit name: a byte of bits, the 16-byte table entry, the footer.
    let ok = place_block([5, 6, 0, 0, 3], &[[0, 0, 2, 7]], 1 + 16 + 8);
    let (_, m) = decoded(&ok).unwrap_or_else(|(e, _)| panic!("{e}"));
    assert_eq!(m[1].1, [&[0x3a][..], &5u32.to_le_bytes(), &6u32.to_le_bytes(), &[0, 0, 0, 0, 0, 0, 3, 0], &[1, 0, 0, 0], b"PNL3"].concat());
    assert_eq!(refusal(&place_block([5, 6, 33, 0, 3], &[[0, 0, 2, 7]], 25)), "transport: a place block's width past 32 bits");
    assert_eq!(refusal(&place_block([5, 6, 0, 0, 2], &[[0, 0, 2, 7]], 25)), "transport: a value of 7 in a field of 2 bits");
    assert_eq!(refusal(&place_block([5, 6, 0, 0, 3], &[[0, 0, 8, 7]], 25)), "transport: a value of 8 in a field of 3 bits");
    // A polygon of encoding 0 at 17 bits a latitude: u16 vertices are 16 bits at most.
    // One vertex (1, 1): a 6-byte header, its bits, the pad.
    let polygon = |tag: u64, wl: u64| {
        let narrow = if tag & 3 == 3 { vec![] } else { vec![1] };
        let wide = if tag & 3 == 3 { vec![1] } else { vec![] };
        let cols = [raw(1, &[tag]), raw(4, &[1]), raw(8, &[0]), raw(8, &[0]), raw(1, &[wl]), raw(1, &[1]), raw(2, &narrow), raw(2, &narrow), raw(4, &wide), raw(4, &wide)];
        let size = 6 + (wl + 1).div_ceil(8) + 8;
        Stream::head(Kind::Pois, &[("poi_records.bin", 0), ("poi_vertices.bin", size)], None).end(&[]).chunk(1, &cols).end(&[0; 8]).0
    };
    assert!(decoded(&polygon(0, 16)).is_ok());
    assert_eq!(refusal(&polygon(0, 17)), "transport: a polygon's vertex width past its encoding's 16 bits");
    assert!(decoded(&polygon(3, 32)).is_ok());
    assert_eq!(refusal(&polygon(3, 33)), "transport: a polygon's vertex width past its encoding's 32 bits");
}

/// An arcs unit of one chunk of these slots' vertex counts, flags, first vertices, group header
/// bytes and deltas.
fn arc_chunk(counts: &[u64], flags: &[u64], groups: &[u64], deltas: &[(u64, u64)], vertices: u64) -> Vec<u8> {
    let firsts = vec![0; flags.len()];
    let cols = [
        raw(4, counts),
        raw(1, flags),
        raw(8, &firsts),
        raw(8, &firsts),
        raw(1, groups),
        raw(8, &deltas.iter().map(|d| d.0).collect::<Vec<_>>()),
        raw(8, &deltas.iter().map(|d| d.1).collect::<Vec<_>>()),
    ];
    Stream::head(Kind::Arcs, &[("arc_index.bin", 4 * counts.len() as u64), ("arc_vertices.bin", vertices)], None).chunk(counts.len() as u32, &cols).end(&[]).0
}

#[test]
fn an_arc_whose_groups_or_deltas_overrun_its_count_is_refused() {
    // Three vertices at grid 0: flags, the first vertex, one group of two deltas at 4 bits each.
    let ok = arc_chunk(&[3, 0], &[0], &[0x44], &[(1, 2), (3, 4)], 9 + 1 + 2);
    let (_, m) = decoded(&ok).unwrap_or_else(|(e, _)| panic!("{e}"));
    assert_eq!(m[0].1, [0u32.to_le_bytes(), 12u32.to_le_bytes()].concat());
    assert_eq!(m[1].1, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0x44, 0x31, 0x42]);
    assert!(refusal(&arc_chunk(&[3, 0], &[0], &[0x44, 0x44], &[(1, 2), (3, 4)], 12)).contains("the arcs' groups: 2 values where the chunk's units have 1"));
    assert!(refusal(&arc_chunk(&[3, 0], &[0], &[0x44], &[(1, 2), (3, 4), (5, 6)], 12)).contains("the arcs' deltas: 3 values where the chunk's units have 2"));
    assert!(refusal(&arc_chunk(&[3, 2], &[0], &[0x44], &[(1, 2), (3, 4)], 12)).contains("an arc's flags: 1 values where the chunk's units have 2"));
    assert_eq!(refusal(&arc_chunk(&[3, 0], &[0], &[0x44], &[(16, 2), (3, 4)], 12)), "transport: a value of 16 in a field of 4 bits");
    // A first vertex past i32.
    let mut far = arc_chunk(&[1], &[0], &[], &[], 9);
    let at = far.len() - 8 - 8 - 4 - 4;
    far[at..at + 8].copy_from_slice(&container::zigzag(1 << 31).to_le_bytes());
    assert_eq!(refusal(&far), "transport: an arc's first vertex past 32 bits");
}

#[test]
fn a_member_longer_or_shorter_than_its_header_says_is_refused() {
    let unit = |size: u64| Stream::head(Kind::Cols, &[("x.bin", size)], Some(&[(4, 0)])).chunk(3, &[raw(4, &[1, 2, 3])]).end(&[9]).0;
    assert!(decoded(&unit(13)).is_ok());
    let (e, got) = decoded(&unit(12)).err().unwrap();
    assert_eq!(e.to_string(), "transport: x.bin runs past its 12 bytes");
    assert!(got[0].is_empty());
    let (e, _) = decoded(&unit(14)).err().unwrap();
    assert_eq!(e.to_string(), "transport: x.bin is 13 bytes, its unit gives 14");
    // A member past its size is never written past it, however far it runs.
    let rows: Vec<u64> = (0..200_000).collect();
    let long = Stream::head(Kind::Cols, &[("x.bin", 100_000)], Some(&[(4, 0)])).chunk(200_000, &[raw(4, &rows)]).end(&[]).0;
    let (e, got) = decoded(&long).err().unwrap();
    assert!(e.to_string().contains("runs past its 100000 bytes"));
    assert!(got[0].len() <= 100_000 && got[0].len() % 4 == 0);
    assert!(rows.iter().flat_map(|r| (*r as u32).to_le_bytes()).take(got[0].len()).eq(got[0].iter().copied()));
}

#[test]
fn corrupt_units_are_refused_or_decoded_never_a_panic() {
    let mut rng = Rng::new(99);
    let mut units: Vec<Vec<u8>> = golden_units().into_iter().map(|u| u.3).collect();
    let mut fx = ArcFixture::new();
    for i in 0..30 {
        fx.arc([0, 2, 3][i % 3], &grid_walk(&mut rng, [0, 2, 3][i % 3], 1 + i * 3, 50));
    }
    let dir = dir_of(&arc_files(&fx));
    units.push(unit_of(Kind::Arcs, dir.path(), None));
    let (mut refused, mut decodes) = (0, 0);
    for unit in &units {
        for _ in 0..3000 {
            let mut bad = unit.clone();
            for _ in 0..1 + rng.next() % 3 {
                let at = (rng.next() % bad.len() as u64) as usize;
                bad[at] = rng.next() as u8;
            }
            match decoded(&bad) {
                Ok(_) => decodes += 1,
                Err(_) => refused += 1,
            }
        }
    }
    assert!(refused > 1000 && decodes > 100, "{refused} refused, {decodes} decoded");
}

#[test]
fn files_the_stream_cant_give_back_are_refused_by_encode() {
    let refused = |kind: Kind, files: &[(&str, Vec<u8>)]| {
        let dir = dir_of(files);
        let e = encode(kind, dir.path(), None, io::sink()).err().unwrap_or_else(|| panic!("encoded {}", kind.name()));
        e.to_string()
    };
    // A place block with a bit set past its last row.
    let (p, _) = crate::place_files::encode(&[PlaceNode { lat: 1.0, lng: 2.0, name_id: 7, place_type: 1, _pad1: 0, _pad2: 0, parent_poly_id: NO_DATA }], 10);
    let mut nodes = p.nodes.clone();
    nodes[0] |= 0x80;
    assert_eq!(refused(Kind::Places, &members_of(Kind::Places, vec![p.cells.clone(), nodes, p.hidden.clone()])), "transport: place_nodes.bin: block 0 has bits set past its last row");
    let mut nodes = p.nodes.clone();
    nodes.truncate(nodes.len() - 1);
    assert!(refused(Kind::Places, &members_of(Kind::Places, vec![p.cells.clone(), nodes, p.hidden.clone()])).contains("no PNL3 footer"));
    // An arc block whose box is not its vertices', and slots out of order.
    let mut fx = ArcFixture::new();
    let line: Vec<PointE7> = (0..20).map(|i| PointE7 { lat: 100 * i, lng: -50 * i }).collect();
    fx.arc(0, &line);
    fx.arc(0, &line[..3]);
    let [index, mut vertices] = [fx.arc_index(), fx.arc_vertices()];
    vertices[9] ^= 1;
    assert!(refused(Kind::Arcs, &members_of(Kind::Arcs, vec![index.clone(), vertices])).contains("arc_vertices.bin: slot 0's block is not as decode writes it back"));
    let swapped = [&index[4..8], &index[0..4]].concat();
    assert_eq!(refused(Kind::Arcs, &members_of(Kind::Arcs, vec![swapped, fx.arc_vertices()])), "transport: arc_index.bin: slot 0's offset is not where the blocks before it end");
    // A V2 block's m in two bytes where one does.
    let row = testdata_rows("poi_files.tsv").into_iter().find(|r| r[0] == "the widths' and varints' edges").unwrap();
    let mut v2 = hex(&row[4]);
    assert_eq!(v2[1], 1);
    v2.splice(1..2, [0x81, 0x00]);
    assert!(refused(Kind::Pois, &members_of(Kind::Pois, vec![hex(&row[3]), v2])).contains("poi_vertices.bin: the block at 0: not as decode writes it back"));
    // 2^32 - 1 vertices of no bits in 9 bytes, and an arc of 300,000 groups of 16 deltas of no
    // bits: refused before they are unpacked.
    let none = [&hex("00 ffffffff0f 00 00 00 00".replace(' ', "").as_str())[..], &[0; 8]].concat();
    assert_eq!(refused(Kind::Pois, &members_of(Kind::Pois, vec![hex(&row[3]), none])), "transport: poi_vertices.bin: the block at 0: more vertices than a chunk holds");
    let groups = members_of(Kind::Arcs, vec![vec![0; 4], vec![0; 9 + 300_000]]);
    assert_eq!(refused(Kind::Arcs, &groups), "transport: arc_vertices.bin: slot 0's block: more deltas than a chunk holds");
    // A kind given a file, cols given none or one that is no cols file: the packed postcode
    // centroids are none, and go as their .zst.
    let dir = ScratchDir::new();
    assert!(encode(Kind::Places, dir.path(), Some("x.bin"), io::sink()).is_err());
    assert!(encode(Kind::Cols, dir.path(), None, io::sink()).is_err());
    for f in ["poi_records.bin", "postcode_centroids.bin"] {
        assert_eq!(encode(Kind::Cols, dir.path(), Some(f), io::sink()).err().unwrap().to_string(), format!("transport: {f} is no cols file"));
    }
}

#[test]
fn a_places_unit_refused_part_way_leaves_no_scratch_file() {
    // decoded() checks the scratch dir is empty after an error too: a unit cut in its nodes.
    let (_, _, _, unit) = golden_units().into_iter().next().unwrap();
    let s = sections_of(&unit);
    let cut = s.0[1].0[0].2.start + 20;
    assert!(decoded(&unit[..cut]).is_err());
}

// ------------------------------------------------------------------ the heap decode holds

/// A unit of a head, `copies` of one chunk, then the bytes after them, read as it goes.
struct Repeated {
    parts: [Vec<u8>; 3],
    copies: u64,
    part: usize,
    at: usize,
}

impl Read for Repeated {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        while self.part < 3 {
            let p = &self.parts[self.part];
            if self.at < p.len() {
                let n = buf.len().min(p.len() - self.at);
                buf[..n].copy_from_slice(&p[self.at..self.at + n]);
                self.at += n;
                return Ok(n);
            }
            self.at = 0;
            if self.part == 1 && self.copies > 1 {
                self.copies -= 1;
            } else {
                self.part += 1;
            }
        }
        Ok(0)
    }
}

/// The most heap decode held beyond what it started with.
fn decode_peak(unit: impl Read) -> i64 {
    let scratch = ScratchDir::new();
    let held = heap::held();
    heap::reset_peak();
    decode(unit, scratch.path(), |_| Ok(io::sink())).unwrap_or_else(|e| panic!("{e}"));
    heap::peak_since(held)
}

#[test]
fn decode_holds_one_chunk_however_many_polygons_the_unit_has() {
    // One chunk of 31,000 polygons of two vertices (1,023,000 bytes of columns), made by the
    // encoder; ten times planet poi/all's 8,618,581 polygons are 2,781 copies of it.
    let mut rng = Rng::new(3);
    let pois: Vec<(PoiRecord, Option<PoiPolygon>)> = (0..31_000).map(|i| (poi(&mut rng, i), Some(polygon(&mut rng, (i % 4) as u8, 2, 9, false)))).collect();
    let (r2, v2) = crate::poi_files::encode(&pois);
    let dir = dir_of(&members_of(Kind::Pois, vec![r2, v2.clone()]));
    let unit = unit_of(Kind::Pois, dir.path(), None);
    let s = sections_of(&unit);
    assert_eq!(s.0[1].0.len(), 1);
    let chunk = unit[s.0[1].0[0].2.clone()].to_vec();
    let polygons = |copies: u64| {
        let blocks = (v2.len() - 8) as u64;
        let head = Stream::head(Kind::Pois, &[("poi_records.bin", 24), ("poi_vertices.bin", copies * blocks + 8)], None).end(&[0; 24]).0;
        Repeated { parts: [head, chunk.clone(), Stream(Vec::new()).end(&[0; 8]).0], copies, part: 0, at: 0 }
    };
    let few = decode_peak(polygons(10));
    let many = decode_peak(polygons(2_781));
    assert!(few <= (CHUNK_BYTES + DECODE_OVERHEAD) as i64, "{few} bytes of heap");
    assert!(many <= few, "{many} bytes of heap for 2,781 chunks, {few} for 10");
}

#[test]
fn decode_holds_a_unit_past_chunk_bytes_and_no_more() {
    let mut rng = Rng::new(4);
    // 200,000 deltas: 3,212,521 bytes of columns.
    let mut fx = ArcFixture::new();
    fx.arc(0, &grid_walk(&mut rng, 0, 30, 100));
    fx.arc(0, &grid_walk(&mut rng, 0, 200_001, 100));
    let dir = dir_of(&arc_files(&fx));
    let arcs = unit_of(Kind::Arcs, dir.path(), None);
    // 300,000 u32 vertices: 2,400,023 bytes of columns.
    let pois = vec![(poi(&mut rng, 1), Some(polygon(&mut rng, 3, 300_000, 20, false)))];
    let (r2, v2) = crate::poi_files::encode(&pois);
    let dir = dir_of(&members_of(Kind::Pois, vec![r2, v2]));
    let polygon = unit_of(Kind::Pois, dir.path(), None);
    for (unit, section, largest) in [(&arcs, 0, 3_212_521usize), (&polygon, 1, 2_400_023)] {
        let biggest = sections_of(unit).0[section].0.iter().map(|c| c.1).max().unwrap();
        assert_eq!(biggest, largest);
        let peak = decode_peak(&unit[..]);
        assert!(peak >= largest as i64 && peak <= (largest + DECODE_OVERHEAD) as i64, "{peak} bytes of heap for a unit of {largest}");
    }
}

// ------------------------------------------------------------------ the template's units

/// configurations.template.json's transport block: the units a dir publishes when it holds their
/// files (compress_index.py), at this module's version. A unit of each kind but cols, named after
/// it, holds the kind's members in order; a cols unit, named after its file, each cols file.
#[test]
fn the_templates_units_are_this_versions_kinds_and_their_members() {
    let template: serde_json::Value = serde_json::from_str(include_str!("../../../builder/configurations.template.json")).unwrap();
    let transport = &template["transport"];
    assert_eq!(transport["version"], TRANSPORT_VERSION);
    let got: Vec<(String, String, Vec<String>)> = transport["units"]
        .as_array()
        .unwrap()
        .iter()
        .map(|u| {
            let files = u["files"].as_array().unwrap().iter().map(|f| f.as_str().unwrap().to_string()).collect();
            (u["unit"].as_str().unwrap().to_string(), u["kind"].as_str().unwrap().to_string(), files)
        })
        .collect();
    let mut want: Vec<(String, String, Vec<String>)> = Kind::ALL
        .iter()
        .filter(|&&k| k != Kind::Cols)
        .map(|k| (format!("{}.tt.zst", k.name()), k.name().to_string(), k.members().iter().map(|m| m.to_string()).collect()))
        .collect();
    for f in COLS_FILES {
        want.push((format!("{}.tt.zst", f.strip_suffix(".bin").unwrap()), "cols".to_string(), vec![f.to_string()]));
    }
    assert_eq!(got, want);
}

// ------------------------------------------------------------------ decoding into a dir

fn sha256_hex(b: &[u8]) -> String {
    use sha2::Digest;
    crate::lower_hex(&sha2::Sha256::digest(b))
}

/// The names in `dir` and their bytes.
fn dir_files(dir: &Path) -> Vec<(String, Vec<u8>)> {
    let mut v: Vec<(String, Vec<u8>)> =
        std::fs::read_dir(dir).unwrap().map(|e| e.unwrap()).map(|e| (e.file_name().into_string().unwrap(), std::fs::read(e.path()).unwrap())).collect();
    v.sort();
    v
}

/// The golden places unit as published, its members, and a dir holding other bytes under their
/// names.
fn published_places() -> (Vec<u8>, Files, ScratchDir) {
    let (_, _, members, unit) = golden_units().into_iter().next().unwrap();
    let dir = dir_of(&members.iter().map(|(n, _)| (*n, b"an older build's".to_vec())).collect::<Vec<_>>());
    (zstd::encode_all(&unit[..], 3).unwrap(), members, dir)
}

fn expected(members: &Files) -> Vec<Expected> {
    members.iter().map(|(n, b)| Expected { name: n.to_string(), sha256: sha256_hex(b), size: Some(b.len() as u64) }).collect()
}

#[test]
fn a_unit_decodes_into_its_files_in_a_dir_once_they_are_the_files_expected() {
    let (unit, members, dir) = published_places();
    let mut expect = expected(&members);
    expect.reverse();

    let (kind, got) = decode_into_dir(&unit[..], dir.path(), Some(&expect)).unwrap();

    assert_eq!(kind, Kind::Places);
    let sizes: Vec<(String, u64)> = members.iter().map(|(n, b)| (n.to_string(), b.len() as u64)).collect();
    assert_eq!(got.into_iter().map(|m| (m.name, m.size)).collect::<Vec<_>>(), sizes);
    let mut want: Vec<(String, Vec<u8>)> = members.iter().map(|(n, b)| (n.to_string(), b.clone())).collect();
    want.sort();
    assert_eq!(dir_files(dir.path()), want);
    // Of no size, as a caller that knows only the hashes gives them; of any files, as the CLI
    // decodes.
    let other = ScratchDir::new();
    let no_sizes: Vec<Expected> = expect.into_iter().map(|e| Expected { size: None, ..e }).collect();
    decode_into_dir(&unit[..], other.path(), Some(&no_sizes)).unwrap();
    assert_eq!(dir_files(other.path()), want);
    let any = ScratchDir::new();
    decode_into_dir(&unit[..], any.path(), None).unwrap();
    assert_eq!(dir_files(any.path()), want);
}

#[test]
fn a_unit_refused_or_not_of_the_files_expected_leaves_the_dir_as_it_was() {
    let (unit, members, dir) = published_places();
    let before = dir_files(dir.path());
    let expect = expected(&members);
    let with = |unit: &[u8], expect: &[Expected]| decode_into_dir(unit, dir.path(), Some(expect)).map(|_| ()).map_err(|e| e.to_string());
    let changed = |i: usize, f: &dyn Fn(&mut Expected)| {
        let mut e = expect.clone();
        f(&mut e[i]);
        e
    };

    let zero = "0".repeat(64);
    assert_eq!(with(&unit, &changed(1, &|e| e.sha256 = zero.clone())), Err(format!("transport: place_nodes.bin's sha256 is {}, not {zero}", expect[1].sha256)));
    assert_eq!(dir_files(dir.path()), before);
    assert_eq!(with(&unit, &changed(2, &|e| e.size = Some(1))), Err(format!("transport: place_hidden.bin is {} bytes in the unit, not 1", members[2].1.len())));
    assert_eq!(dir_files(dir.path()), before);
    assert_eq!(with(&unit, &changed(0, &|e| e.name = "place_cell.bin".into())), Err("transport: place_cells.bin is no file this unit is to hold".to_string()));
    assert_eq!(dir_files(dir.path()), before);
    let mut more = expect.clone();
    more.push(Expected { name: "poi_records.bin".into(), sha256: zero.clone(), size: None });
    assert_eq!(with(&unit, &more), Err("transport: the unit lacks poi_records.bin".to_string()));
    assert_eq!(dir_files(dir.path()), before);
    let cut = with(&unit[..unit.len() - 9], &expect).unwrap_err();
    assert!(cut.contains("ends early") || cut.contains("incomplete"), "{cut}");
    assert_eq!(dir_files(dir.path()), before);
    assert!(with(b"not zstd", &expect).is_err());
    assert_eq!(dir_files(dir.path()), before);
}

/// A unit's bytes, which once all are read tell `at_end` and wait for `go` before they end: a
/// decode held with its members written, before it puts any in place.
struct HeldAtEnd {
    bytes: Vec<u8>,
    at: usize,
    at_end: Option<std::sync::mpsc::Sender<()>>,
    go: std::sync::mpsc::Receiver<()>,
}

impl Read for HeldAtEnd {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.at == self.bytes.len() {
            if let Some(at_end) = self.at_end.take() {
                at_end.send(()).unwrap();
                self.go.recv().unwrap();
            }
            return Ok(0);
        }
        let n = buf.len().min(self.bytes.len() - self.at);
        buf[..n].copy_from_slice(&self.bytes[self.at..self.at + n]);
        self.at += n;
        Ok(n)
    }
}

#[test]
fn two_decodes_of_a_unit_into_one_dir_at_once_each_put_its_files_in_place() {
    // Both decodes write all their members before either puts one in place; the first then puts
    // its members in, and the second after it.
    let (unit, members, dir) = published_places();
    let expect = expected(&members);
    let (path, expect) = (dir.path(), &expect);

    let got: Vec<Result<(), String>> = std::thread::scope(|s| {
        let decodes: Vec<_> = (0..2)
            .map(|_| {
                let (at_end, held) = std::sync::mpsc::channel();
                let (go, wait) = std::sync::mpsc::channel();
                let unit = HeldAtEnd { bytes: unit.clone(), at: 0, at_end: Some(at_end), go: wait };
                let decode = s.spawn(move || decode_into_dir(unit, path, Some(expect)).map(|_| ()).map_err(|e| e.to_string()));
                held.recv().unwrap();
                (go, decode)
            })
            .collect();
        decodes
            .into_iter()
            .map(|(go, decode)| {
                go.send(()).unwrap();
                decode.join().unwrap()
            })
            .collect()
    });

    assert_eq!(got, vec![Ok(()), Ok(())]);
    let mut want: Vec<(String, Vec<u8>)> = members.iter().map(|(n, b)| (n.to_string(), b.clone())).collect();
    want.sort();
    assert_eq!(dir_files(dir.path()), want);
}

#[test]
fn a_decode_leaves_the_files_named_as_its_members_and_tmp_alone() {
    let (unit, members, dir) = published_places();
    let theirs: Vec<(String, Vec<u8>)> = members.iter().map(|(n, _)| (format!("{n}.tmp"), b"not the decode's".to_vec())).collect();
    for (name, bytes) in &theirs {
        std::fs::write(dir.path().join(name), bytes).unwrap();
    }
    let before = dir_files(dir.path());
    let mut wrong = expected(&members);
    wrong[2].sha256 = "0".repeat(64);

    assert!(decode_into_dir(&unit[..], dir.path(), Some(&wrong)).is_err());
    assert_eq!(dir_files(dir.path()), before);
    decode_into_dir(&unit[..], dir.path(), Some(&expected(&members))).unwrap();
    let mut want: Vec<(String, Vec<u8>)> = members.iter().map(|(n, b)| (n.to_string(), b.clone())).chain(theirs).collect();
    want.sort();
    assert_eq!(dir_files(dir.path()), want);
}

/// Every path under `dir`, relative to it, with a file's bytes; none for a dir.
fn tree(dir: &Path) -> Vec<(String, Option<Vec<u8>>)> {
    let mut paths = Vec::new();
    let mut dirs = vec![dir.to_path_buf()];
    while let Some(d) = dirs.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            let rel = p.strip_prefix(dir).unwrap().to_string_lossy().into_owned();
            if p.is_dir() {
                paths.push((rel, None));
                dirs.push(p);
            } else {
                paths.push((rel, Some(std::fs::read(&p).unwrap())));
            }
        }
    }
    paths.sort();
    paths
}

#[test]
fn a_member_that_cant_go_in_place_puts_back_the_files_of_the_members_before_it() {
    // A directory where a member goes, the second or the last: the members before it have gone
    // in place by then, and are taken out again, the files they replaced put back.
    for (blocked, earlier_files) in [(1, true), (1, false), (2, true)] {
        let (unit, members, dir) = published_places();
        if !earlier_files {
            for (name, _) in &members[..blocked] {
                std::fs::remove_file(dir.path().join(name)).unwrap();
            }
        }
        let name = members[blocked].0;
        std::fs::remove_file(dir.path().join(name)).unwrap();
        std::fs::create_dir(dir.path().join(name)).unwrap();
        std::fs::write(dir.path().join(name).join("inside"), b"a file of the dir's").unwrap();
        let before = tree(dir.path());

        let e = decode_into_dir(&unit[..], dir.path(), Some(&expected(&members))).unwrap_err().to_string();

        assert_eq!(tree(dir.path()), before, "{blocked} {earlier_files}");
        assert!(e.contains(&format!("{name}: is a directory")), "{blocked} {earlier_files}: {e}");
    }
}

#[test]
fn a_chunk_reader_reads_a_channels_chunks_as_one_stream() {
    let (tx, rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(2);
    let bytes: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
    let chunks: Vec<Vec<u8>> = bytes.chunks(7_777).map(|c| c.to_vec()).chain([Vec::new()]).collect();
    let feeder = std::thread::spawn(move || {
        for c in chunks {
            tx.send(c).unwrap();
        }
    });
    let mut got = Vec::new();
    let mut r = ChunkReader::new(rx);
    let mut buf = [0u8; 1000];
    loop {
        match r.read(&mut buf).unwrap() {
            0 => break,
            n => got.extend_from_slice(&buf[..n]),
        }
    }
    feeder.join().unwrap();
    assert!(got == bytes, "{} bytes read of {}", got.len(), bytes.len());
}
