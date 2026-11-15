use super::*;
use crate::arc_fixture::{arc_block_of_deltas, encode_arc, encode_ring, mix64, put_varint, zigzag, ArcFixture, RawBox, Ref, CHUNK_BOX_MIN_REFS, REF_CHUNK_MIN};
use crate::test_rng::Rng;

/// A random walk of `n` vertices on the 10^k grid from `start` (e7, on that grid), steps of up to
/// `reach` grid units.
pub(crate) fn random_arc(rng: &mut Rng, k: u8, n: usize, start: PointE7, reach: i64) -> Vec<PointE7> {
    let step = 10i64.pow(k as u32);
    let mut v = vec![start];
    while v.len() < n {
        let last = *v.last().unwrap();
        let d = |rng: &mut Rng| (rng.next() % (2 * reach as u64 + 1)) as i64 - reach;
        v.push(PointE7 { lat: last.lat + d(rng) * step, lng: last.lng + d(rng) * step });
    }
    v
}

/// Points to test against `vertices`: each vertex and its neighbours one e7 unit off, the grid
/// points half-way along each edge and beside them, and `random` points over their box.
pub(crate) fn probes(rng: &mut Rng, vertices: &[PointE7], random: usize) -> Vec<PointE7> {
    let mut out = Vec::new();
    for e in vertices.windows(2) {
        for (dlat, dlng) in [(0, 0), (1, 0), (-1, 0), (0, 1), (0, -1), (1, 1), (-1, -1)] {
            out.push(PointE7 { lat: e[0].lat + dlat, lng: e[0].lng + dlng });
        }
        let (slat, slng) = (e[0].lat + e[1].lat, e[0].lng + e[1].lng);
        if slat % 2 == 0 && slng % 2 == 0 {
            let mid = PointE7 { lat: slat / 2, lng: slng / 2 };
            out.extend([mid, PointE7 { lat: mid.lat + 1, ..mid }, PointE7 { lat: mid.lat - 1, ..mid }]);
        }
    }
    let lo = |f: fn(&PointE7) -> i64| vertices.iter().map(f).min().unwrap() - 10;
    let hi = |f: fn(&PointE7) -> i64| vertices.iter().map(f).max().unwrap() + 10;
    let (lat0, lat1, lng0, lng1) = (lo(|v| v.lat), hi(|v| v.lat), lo(|v| v.lng), hi(|v| v.lng));
    for _ in 0..random {
        out.push(PointE7 { lat: lat0 + (rng.next() % (lat1 - lat0) as u64) as i64, lng: lng0 + (rng.next() % (lng1 - lng0) as u64) as i64 });
    }
    out
}

/// Whether the ray from `p` crosses the polyline's edges an odd number of times.
pub(crate) fn odd_crossings(vertices: &[PointE7], p: PointE7) -> bool {
    vertices.windows(2).filter(|e| edge_crosses(e[0], e[1], p)).count() % 2 == 1
}

fn decoded(block: &[u8]) -> Option<Vec<PointE7>> {
    let mut v = Vec::new();
    let n = arc_vertices(block, |p| v.push(p))?;
    assert_eq!(n as usize, v.len());
    Some(v)
}

#[test]
fn arc_blocks_decode_at_every_grid_with_and_without_a_box() {
    let mut rng = Rng::new(20261103);
    let mut boxed_reads = 0;
    for k in [0u8, 2, 3] {
        let step = 10i64.pow(k as u32);
        for n in [1, 2, 15, 16, 17, 200] {
            for boxed in [false, true] {
                let start = PointE7 { lat: 471_234 * step, lng: -1_224_194 * step / 10 * 10 };
                let v = random_arc(&mut rng, k, n, start, 300);
                let block = encode_arc(k, &v, boxed);
                assert_eq!(block[0] & (GRID_MASK | HAS_BOX), k | if boxed { HAS_BOX } else { 0 });
                assert_eq!(decoded(&block).as_deref(), Some(&v[..]), "k {k} n {n} boxed {boxed}");
                for p in probes(&mut rng, &v, 200) {
                    let want = odd_crossings(&v, p);
                    match arc_crossings(&block, p).unwrap() {
                        ArcCrossings::Boxed => {
                            assert!(boxed && !want, "k {k} n {n} {p:?}");
                            boxed_reads += 1;
                        }
                        ArcCrossings::Read { odd, vertices } => {
                            assert_eq!(odd, want, "k {k} n {n} boxed {boxed} {p:?}");
                            assert_eq!(vertices as usize, n);
                        }
                    }
                }
            }
        }
    }
    assert!(boxed_reads > 100);
}

#[test]
fn arc_blocks_decode_to_the_shared_vectors_and_the_test_encoder_writes_them() {
    // geocoder/testdata/arc_blocks.tsv, written by the builder's encode_arc.
    let rows = crate::exact_pip::tests::testdata_rows("arc_blocks.tsv");
    assert!(rows.len() > 100);
    for row in &rows {
        let k: u8 = row[0].parse().unwrap();
        let v: Vec<PointE7> = row[2]
            .split(';')
            .map(|pair| {
                let (lat, lng) = pair.split_once(',').unwrap();
                PointE7 { lat: lat.parse().unwrap(), lng: lng.parse().unwrap() }
            })
            .collect();
        let block: Vec<u8> = (0..row[3].len()).step_by(2).map(|i| u8::from_str_radix(&row[3][i..i + 2], 16).unwrap()).collect();
        assert_eq!(decoded(&block).as_deref(), Some(&v[..]), "{} {}", row[0], row[1]);
        assert_eq!(encode_arc(k, &v, v.len() >= 16), block, "{} {}", row[0], row[1]);
    }
}

#[test]
fn an_arcs_box_skips_exactly_the_points_its_edges_cannot_cross() {
    // A box's three sides at grid 3: points on the west side and south of the top are read, points
    // east of the east side or at its longitude, or at or north of the top, are not.
    let v = [PointE7 { lat: 0, lng: 0 }, PointE7 { lat: 5000, lng: 3000 }, PointE7 { lat: 1000, lng: 6000 }];
    let block = encode_arc(3, &v, true);
    let read = |lat, lng| matches!(arc_crossings(&block, PointE7 { lat, lng }), Some(ArcCrossings::Read { .. }));
    assert!(read(-10, 0) && read(4999, 5999) && read(-10, 1));
    assert!(!read(-10, -1) && !read(-10, 6000) && !read(5000, 3000) && !read(1 << 40, 10));
}

#[test]
fn truncated_overlong_and_empty_arc_blocks_never_panic() {
    let mut rng = Rng::new(7);
    let v = random_arc(&mut rng, 0, 40, PointE7 { lat: 100, lng: -100 }, 100_000);
    let block = encode_arc(0, &v, true);
    let p = PointE7 { lat: 0, lng: 0 };
    for len in 0..block.len() {
        // A prefix reads as some shorter arc or as malformed, never past its end.
        let prefix = &block[..len];
        if let Some(got) = decoded(prefix) {
            assert!(got.len() < v.len() && got[0] == v[0], "len {len}");
        }
        let _ = arc_crossings(prefix, p);
    }
    assert_eq!(decoded(&[]), None);
    assert_eq!(arc_crossings(&[], p), None);
    // A short group whose widths sum to under 8 bits can't say its count, and one too short for a
    // vertex says none.
    let head = encode_arc(0, &v[..1], false);
    for group in [&[0x21u8, 0x05][..], &[0x55, 0x00]] {
        let bad = [&head[..], group].concat();
        assert_eq!(decoded(&bad), None, "{group:?}");
        assert_eq!(arc_crossings(&bad, p), None, "{group:?}");
    }
}

#[test]
fn arc_blocks_past_i32_e7_are_malformed_as_the_builder_reads_them() {
    let p = PointE7 { lat: 0, lng: 5 };
    // A closed arc whose second vertex is one unit past i32, at the first grid and the coarsest.
    for (k, past) in [(0u8, i32::MAX as i64 + 1), (3, i32::MAX as i64 / 1000 + 1)] {
        let edge = |lat: i64| arc_block_of_deltas(k, (0, 0), &[(lat, 10), (-lat, 10), (0, -20)]);
        assert!(decoded(&edge(past - 1)).is_some() && arc_crossings(&edge(past - 1), p).is_some(), "k {k}");
        assert_eq!(decoded(&edge(past)), None, "k {k}");
        assert_eq!(arc_crossings(&edge(past), p), None, "k {k}");
    }
    // Vertices that climb to the edge of the range or past it and come back, in the middle of the
    // arc and across groups, at every grid and on every side.
    for k in [0u8, 1, 2, 3] {
        let units = GRID_UNITS[k as usize];
        let (lo, hi) = (units.lo, units.lo + units.span);
        let walk = |lat0: i64, lng0: i64, d_lat: i64, d_lng: i64, steps: usize| {
            let deltas: Vec<(i64, i64)> = std::iter::repeat((d_lat, d_lng)).take(steps).chain(std::iter::repeat((-d_lat, -d_lng)).take(steps)).collect();
            arc_block_of_deltas(k, (lat0 as i32, lng0 as i32), &deltas)
        };
        for steps in [1i64, 2, 5, 40] {
            for (at_edge, past, d_lat, d_lng) in [
                ((hi - steps * 8191, 0), (hi - steps * 8191 + 1, 0), 8191, 0),
                ((lo + steps * 8192, 0), (lo + steps * 8192 - 1, 0), -8192, 0),
                ((0, hi - steps * 8191), (0, hi - steps * 8191 + 1), 0, 8191),
                ((0, lo + steps * 8192), (0, lo + steps * 8192 - 1), 0, -8192),
            ] {
                let fits = walk(at_edge.0, at_edge.1, d_lat, d_lng, steps as usize);
                assert!(decoded(&fits).is_some() && arc_crossings(&fits, p).is_some(), "k {k} steps {steps} {at_edge:?}");
                let over = walk(past.0, past.1, d_lat, d_lng, steps as usize);
                assert_eq!(decoded(&over), None, "k {k} steps {steps} {past:?}");
                assert_eq!(arc_crossings(&over, p), None, "k {k} steps {steps} {past:?}");
            }
        }
        // A long delta past the range and straight back, from well inside it.
        let out_and_back = arc_block_of_deltas(k, (0, 0), &[(hi + 1, 0), (-(hi + 1), 0), (1, 0)]);
        assert_eq!(decoded(&out_and_back), None, "k {k}");
        assert_eq!(arc_crossings(&out_and_back, p), None, "k {k}");
    }
    // The widest group: deltas of 45 bits, which no grid holds.
    let mut widest = arc_block_of_deltas(0, (0, 0), &[]);
    widest[0] |= 15 << BASE_WIDTH_SHIFT;
    widest.extend([0xff; 13]);
    assert_eq!((decoded(&widest), arc_crossings(&widest, p)), (None, None));
    // A first vertex or a box past i32 once scaled by the grid.
    let head = |k: u8, lat: i32, lng: i32| [&[k][..], &lat.to_le_bytes(), &lng.to_le_bytes()].concat();
    let far = i32::MAX / 1000 + 1;
    assert_eq!(decoded(&head(3, far, 0)), None);
    assert_eq!(decoded(&head(3, 0, -far)), None);
    assert!(decoded(&head(3, far - 1, 1 - far)).is_some());
    for b in [[far, 0, 0], [0, far, 0], [0, 0, far]] {
        let boxed = [&[3 | HAS_BOX][..], &0i32.to_le_bytes(), &0i32.to_le_bytes(), &b[0].to_le_bytes(), &b[1].to_le_bytes(), &b[2].to_le_bytes()].concat();
        assert_eq!(decoded(&boxed), None, "{b:?}");
        assert_eq!(arc_crossings(&boxed, p), None, "{b:?}");
    }
}

#[test]
fn arc_slots_resolve_only_to_blocks_inside_the_file() {
    let mut fx = ArcFixture::new();
    let a = fx.arc(0, &[PointE7 { lat: 0, lng: 0 }, PointE7 { lat: 1, lng: 1 }]);
    let dead = fx.dead_arc();
    let b = fx.arc(0, &[PointE7 { lat: 5, lng: 5 }]);
    let (index, vertices) = (fx.arc_index(), fx.arc_vertices());
    let arcs = ArcFiles { index: &index, vertices: &vertices };
    assert_eq!(arcs.slots(), 3);
    assert_eq!(arcs.block(a).map(|b| b.len()), Some(11));
    assert_eq!(arcs.block(dead), None);
    // The last block runs to the end of the file.
    assert_eq!(arcs.block(b).map(|b| b.len()), Some(9));
    for slot in [3, u32::MAX] {
        assert_eq!(arcs.block(slot), None);
    }
    // Offsets past the end of the file, or going backwards.
    let mut past = index.clone();
    past[8..12].copy_from_slice(&1000u32.to_le_bytes());
    assert_eq!(ArcFiles { index: &past, vertices: &vertices }.block(b), None);
    let mut backwards = index.clone();
    backwards[4..8].copy_from_slice(&2u32.to_le_bytes());
    backwards[8..12].copy_from_slice(&1u32.to_le_bytes());
    assert_eq!(ArcFiles { index: &backwards, vertices: &vertices }.block(dead), None);
    // A torn last index entry is no slot.
    assert_eq!(ArcFiles { index: &index[..10], vertices: &vertices }.slots(), 2);
}

/// Everything `walk_rings` hands its visitor.
#[derive(Default, Debug, PartialEq)]
struct Walk {
    /// Each chunk read: its box, if any, and its refs.
    chunks: Vec<(Option<ChunkBox>, Vec<ArcRef>)>,
    ring_ends: Vec<usize>,
    /// Whether to read boxed chunks.
    skip_boxed: bool,
    open: bool,
}

impl RingVisitor for Walk {
    fn wants_chunk(&mut self, bbox: &ChunkBox) -> bool {
        if !self.skip_boxed {
            self.chunks.push((Some(*bbox), Vec::new()));
            self.open = true;
        }
        !self.skip_boxed
    }

    fn arc(&mut self, r: ArcRef) -> Option<()> {
        if !self.open {
            self.chunks.push((None, Vec::new()));
            self.open = true;
        }
        self.chunks.last_mut().unwrap().1.push(r);
        Some(())
    }

    fn ring_end(&mut self) -> Option<()> {
        self.ring_ends.push(self.chunks.len());
        self.open = false;
        Some(())
    }
}

impl Walk {
    fn of(block: &[u8]) -> Option<Walk> {
        let mut w = Walk::default();
        walk_rings(block, &mut w)?;
        Some(w)
    }
}

/// Refs to slots 0.. with every third reversed.
fn refs(n: u32) -> Vec<Ref> {
    (0..n).map(|s| Ref { slot: s * 7 % 1000, reversed: s % 3 == 0 }).collect()
}

fn arc_refs(refs: &[Ref]) -> Vec<ArcRef> {
    refs.iter().map(|r| ArcRef { slot: r.slot, reversed: r.reversed }).collect()
}

fn slot_box(slot: u32) -> RawBox {
    let c = slot as i32 * 100;
    RawBox { min_lng: c - 10, max_lng: c + 10, max_lat: c }
}

#[test]
fn a_ring_of_up_to_eight_refs_is_one_chunk_without_a_box() {
    for n in 1..=CHUNK_BOX_MIN_REFS as u32 {
        let r = refs(n);
        let block = encode_ring(&r, slot_box);
        assert_eq!(block[0], (n as u8) << 2 | 1, "{n} refs");
        let w = Walk::of(&block).unwrap();
        assert_eq!(w.chunks, vec![(None, arc_refs(&r))]);
        assert_eq!(w.ring_ends, [1]);
    }
}

#[test]
fn a_longer_ring_is_cut_into_boxed_chunks_where_its_slots_say() {
    for n in [CHUNK_BOX_MIN_REFS as u32 + 1, 60, 400] {
        let r = refs(n);
        let w = Walk::of(&encode_ring(&r, slot_box)).unwrap();
        assert_eq!(w.ring_ends, [w.chunks.len()]);
        let mut at = 0;
        for (i, (bbox, chunk)) in w.chunks.iter().enumerate() {
            assert_eq!(chunk[..], arc_refs(&r[at..at + chunk.len()])[..]);
            at += chunk.len();
            let last = i + 1 == w.chunks.len();
            assert!((1..=REF_CHUNK_MAX as usize).contains(&chunk.len()));
            // A chunk ends where its last slot cuts, at 16 refs, or at the ring's end.
            let cut = |s: u32| mix64(s as u64) & 7 == 7;
            assert!(last || chunk.len() == 16 || (chunk.len() >= REF_CHUNK_MIN && cut(chunk.last().unwrap().slot)), "{n} refs chunk {i}");
            assert!(chunk[..chunk.len() - 1].iter().enumerate().all(|(j, r)| j + 1 < REF_CHUNK_MIN || !cut(r.slot)));
            let mut want = RawBox::EMPTY;
            chunk.iter().for_each(|r| want.add(slot_box(r.slot)));
            assert_eq!(*bbox, Some(ChunkBox { min_lng: want.min_lng, max_lng: want.max_lng, max_lat: want.max_lat }));
        }
        assert_eq!(at, n as usize);
        // Skipped, a boxed chunk's refs are jumped over, and the ring still ends.
        let skipped = { let mut w = Walk { skip_boxed: true, ..Walk::default() }; walk_rings(&encode_ring(&r, slot_box), &mut w).unwrap(); w };
        assert!(skipped.chunks.is_empty() && skipped.ring_ends == [0]);
    }
}

#[test]
fn a_block_of_several_rings_walks_them_in_order() {
    let (outer, hole) = (refs(40), refs(3));
    let block = [encode_ring(&outer, slot_box), encode_ring(&hole, slot_box)].concat();
    let w = Walk::of(&block).unwrap();
    let walked: Vec<ArcRef> = w.chunks.iter().flat_map(|c| c.1.iter().copied()).collect();
    assert_eq!(walked, [arc_refs(&outer), arc_refs(&hole)].concat());
    assert_eq!(w.ring_ends.len(), 2);
    assert_eq!(w.chunks.last().unwrap(), &(None, arc_refs(&hole)));
    // No rings is a block of none.
    assert_eq!(Walk::of(&[]), Some(Walk::default()));
}

#[test]
fn truncated_and_malformed_ring_blocks_never_panic() {
    let block = [encode_ring(&refs(40), slot_box), encode_ring(&refs(3), slot_box)].concat();
    let ends: Vec<usize> = { let first = encode_ring(&refs(40), slot_box).len(); vec![first, block.len()] };
    let (index, vertices) = (vec![0u8; 4000], vec![0u8; 9]);
    let arcs = ArcFiles { index: &index, vertices: &vertices };
    for len in 0..block.len() {
        let prefix = &block[..len];
        // Only a prefix ending a ring is well formed.
        assert_eq!(Walk::of(prefix).is_some(), len == 0 || ends.contains(&len), "len {len}");
        let _ = ring_block_contains(prefix, arcs, PointE7 { lat: 0, lng: 0 }, Some(&mut ArcMemo::default()));
    }
    let chunk = |head: u64, rest: &[u8]| {
        let mut b = Vec::new();
        put_varint(head, &mut b);
        b.extend(rest);
        b
    };
    let ref_bytes = |delta: i64| { let mut b = Vec::new(); put_varint(zigzag(delta) << 1, &mut b); b };
    let boxed = |refs: u64, body: u64, refs_bytes: &[u8]| {
        let mut rest = [1i32, 2, 3].iter().flat_map(|c| c.to_le_bytes()).collect::<Vec<u8>>();
        put_varint(body, &mut rest);
        rest.extend(refs_bytes);
        chunk(refs << 2 | 3, &rest)
    };
    for bad in [
        chunk(1, &[0]),                       // a chunk of no refs
        chunk(17 << 2 | 1, &[0; 17]),         // more than REF_CHUNK_MAX
        chunk(1 << 2, &[0]),                  // ends no ring
        chunk(1 << 2 | 1, &ref_bytes(-1)),    // a slot below 0
        chunk(2 << 2 | 1, &[ref_bytes(u32::MAX as i64), ref_bytes(1)].concat()), // past u32
        boxed(1, 5, &[0]),                    // a body past the block
        boxed(1, 2, &[0, 0]),                 // a body longer than its refs
        boxed(2, 1, &[0, 0]),                 // a body shorter than its refs
        chunk(1 << 2 | 1, &[0x80; 11]),       // an overlong ref
        chunk(u64::MAX, &[]),                 // a head past every count
    ] {
        assert_eq!(Walk::of(&bad), None, "{bad:?}");
        // A point inside the boxed chunks' box, so they are read.
        assert_eq!(ring_block_contains(&bad, arcs, PointE7 { lat: 0, lng: 1 }, None), None, "{bad:?}");
    }
    // The largest slot is a slot, but none the arc files hold.
    let top = chunk(1 << 2 | 1, &ref_bytes(u32::MAX as i64));
    assert_eq!(Walk::of(&top).unwrap().chunks[0].1, [ArcRef { slot: u32::MAX, reversed: false }]);
    assert_eq!(ring_block_contains(&top, arcs, PointE7 { lat: 0, lng: 0 }, Some(&mut ArcMemo::default())), None);
}

/// A square ring of `per_side` arcs a side, each of `vertices` vertices on the 10^k grid along
/// it, as arcs of `fx`: its refs (every other one reversed) and its vertices in ring order.
pub(crate) fn square_of_arcs(fx: &mut ArcFixture, k: u8, side: i64, per_side: i64, vertices: i64) -> (Vec<Ref>, Vec<PointE7>) {
    let step = 10i64.pow(k as u32);
    let corners = [(0, 0), (0, side), (side, side), (side, 0), (0, 0)];
    let (mut refs, mut ring) = (Vec::new(), vec![PointE7 { lat: 0, lng: 0 }]);
    for c in corners.windows(2) {
        let ((lat0, lng0), (lat1, lng1)) = (c[0], c[1]);
        for a in 0..per_side {
            let along = |t: i64| {
                let f = a * (vertices - 1) + t;
                let at = |x0: i64, x1: i64| (x0 + (x1 - x0) * f / (per_side * (vertices - 1))) / step * step;
                PointE7 { lat: at(lat0, lat1), lng: at(lng0, lng1) }
            };
            let v: Vec<PointE7> = (0..vertices).map(along).collect();
            ring.extend(&v[1..]);
            let reversed = refs.len() % 2 == 1;
            let stored: Vec<PointE7> = if reversed { v.iter().rev().copied().collect() } else { v };
            refs.push(Ref { slot: fx.arc(k, &stored), reversed });
        }
    }
    (refs, ring)
}

#[test]
fn reversed_refs_cross_as_their_arcs_do() {
    let mut rng = Rng::new(11);
    let mut fx = ArcFixture::new();
    let (r, ring) = square_of_arcs(&mut fx, 0, 1_000_000, 15, 5);
    let block = encode_ring(&r, |s| RawBox::of(&ring[s as usize * 4..s as usize * 4 + 5]));
    let (index, vertices) = (fx.arc_index(), fx.arc_vertices());
    let arcs = ArcFiles { index: &index, vertices: &vertices };
    assert!(r.iter().any(|r| r.reversed));
    for p in probes(&mut rng, &ring, 2000) {
        assert_eq!(ring_block_contains(&block, arcs, p, None), Some(odd_crossings(&ring, p)), "{p:?}");
    }
}

#[test]
fn a_point_outside_a_chunks_box_and_slack_reads_none_of_its_arcs() {
    let mut fx = ArcFixture::new();
    // 60 arcs of five vertices: below kArcBboxMinVertices, so every arc of a chunk read is read.
    let (r, ring) = square_of_arcs(&mut fx, 0, 1_000_000, 15, 5);
    let raw = |s: u32| RawBox::of(&ring[s as usize * 4..s as usize * 4 + 5]);
    let block = encode_ring(&r, raw);
    let (index, vertices) = (fx.arc_index(), fx.arc_vertices());
    let arcs = ArcFiles { index: &index, vertices: &vertices };
    let w = Walk::of(&block).unwrap();
    assert!(w.chunks.len() > 4);
    let h = CHUNK_BOX_SLACK_E7;
    let mut skipped = 0;
    for (bbox, chunk) in &w.chunks {
        let b = bbox.unwrap();
        let (west, east, top) = (b.min_lng as i64 - h, b.max_lng as i64 + h, b.max_lat as i64 + h);
        let mid_lng = (b.min_lng as i64 + b.max_lng as i64) / 2;
        // (point, whether the chunk's box rules it out)
        for (p, out) in [
            (PointE7 { lat: -5, lng: west - 1 }, true), (PointE7 { lat: -5, lng: west }, false),
            (PointE7 { lat: -5, lng: east }, true), (PointE7 { lat: -5, lng: east - 1 }, false),
            (PointE7 { lat: top, lng: mid_lng }, true), (PointE7 { lat: top - 1, lng: mid_lng }, false),
        ] {
            let mut memo = ArcMemo::default();
            assert_eq!(ring_block_contains(&block, arcs, p, Some(&mut memo)), Some(odd_crossings(&ring, p)), "{p:?}");
            for a in chunk {
                assert_eq!(memo.get(a.slot).is_some(), !out, "{p:?} slot {}", a.slot);
            }
            assert!(memo.arcs_read() as usize <= r.len() - if out { chunk.len() } else { 0 });
            skipped += out as usize;
        }
    }
    assert_eq!(skipped, 3 * w.chunks.len());
}

#[test]
fn the_slack_covers_stored_vertices_off_their_raw_box() {
    // The builder stores a vertex up to 500 e7 units off the raw one its chunk's box spans (half
    // the coarsest grid a tier may take, 1e-4 degrees): answers at the box's sides are still the
    // stored arcs'.
    const BUILDER_SLACK_E7: i32 = 500;
    let mut rng = Rng::new(5);
    let mut fx = ArcFixture::new();
    let (r, ring) = square_of_arcs(&mut fx, 2, 1_000_000, 15, 5);
    let h = BUILDER_SLACK_E7;
    let shrunk = |s: u32| {
        let b = RawBox::of(&ring[s as usize * 4..s as usize * 4 + 5]);
        RawBox { min_lng: b.min_lng + h, max_lng: b.max_lng - h, max_lat: b.max_lat - h }
    };
    let block = encode_ring(&r, shrunk);
    let (index, vertices) = (fx.arc_index(), fx.arc_vertices());
    let arcs = ArcFiles { index: &index, vertices: &vertices };
    for p in probes(&mut rng, &ring, 2000) {
        assert_eq!(ring_block_contains(&block, arcs, p, None), Some(odd_crossings(&ring, p)), "{p:?}");
    }
}

#[test]
fn the_memo_remembers_up_to_its_capacity_and_then_nothing_more() {
    let mut memo = ArcMemo::default();
    for slot in 0..ARC_MEMO_CAPACITY as u32 + 10 {
        assert_eq!(memo.get(slot * 64), None);
        memo.arc_read(slot * 64, slot % 3 == 0, 2);
    }
    assert_eq!(memo.len(), ARC_MEMO_CAPACITY);
    for slot in 0..ARC_MEMO_CAPACITY as u32 + 10 {
        let want = (slot < ARC_MEMO_CAPACITY as u32).then_some(slot % 3 == 0);
        assert_eq!(memo.get(slot * 64), want, "slot {}", slot * 64);
    }
    assert_eq!((memo.arcs_read(), memo.vertices_read()), (ARC_MEMO_CAPACITY as u32 + 10, 2 * (ARC_MEMO_CAPACITY as u64 + 10)));
    // The free marker is never remembered.
    let mut memo = ArcMemo::default();
    memo.arc_read(u32::MAX, true, 1);
    assert_eq!((memo.get(u32::MAX), memo.len()), (None, 0));
}

#[test]
fn a_full_memo_ends_a_probe_at_a_free_entry_nearby() {
    // Full, it holds as many free entries as taken ones, so a probe for an arc it doesn't hold
    // stops at a free one instead of visiting every entry; the longest run of taken entries
    // bounds every probe.
    let mut rng = Rng::new(9);
    let mut memo = ArcMemo::default();
    while memo.len() < ARC_MEMO_CAPACITY {
        let slot = (rng.next() % 4_000_000) as u32;
        if memo.get(slot).is_none() {
            memo.arc_read(slot, slot % 2 == 0, 3);
        }
    }
    let free = memo.slots.iter().filter(|&&s| s == ArcMemo::FREE).count();
    assert!(free >= ARC_MEMO_CAPACITY, "{free} free entries");
    let longest_run = (0..memo.slots.len())
        .map(|start| (0..memo.slots.len()).take_while(|k| memo.slots[(start + k) % memo.slots.len()] != ArcMemo::FREE).count())
        .max()
        .unwrap();
    assert!(longest_run < ARC_MEMO_CAPACITY / 2, "a run of {longest_run} taken entries");
    for k in 0..10_000u32 {
        let slot = 5_000_000 + k;
        assert_eq!(memo.get(slot), None);
    }
}

/// An arc block's vertices with every delta read a bit at a time: what `arc_vertices` must give.
fn vertices_bitwise(block: &[u8]) -> Option<Vec<PointE7>> {
    let head = arc_head(block)?;
    let e7 = |lat: i64, lng: i64| PointE7 { lat: lat * head.step, lng: lng * head.step };
    let (mut lat, mut lng) = (head.lat, head.lng);
    let mut out = vec![e7(lat, lng)];
    let (mut at, mut prev) = (head.deltas, (0i64, 0i64));
    while at < block.len() {
        let h = u32::from(block[at]);
        at += 1;
        let (wl, wg) = (head.base_width + (h >> 4), head.base_width + (h & 15));
        let w = (wl + wg) as usize;
        let rem = block.len() - at;
        let c = if rem >= 2 * w {
            ARC_GROUP
        } else if w < 8 {
            return None;
        } else {
            rem * 8 / w
        };
        let used = (c * w).div_ceil(8);
        if c == 0 || (c < ARC_GROUP && used != rem) {
            return None;
        }
        let bit = |i: usize| u64::from(block[at + i / 8] >> (i % 8) & 1);
        let read = |from: usize, width: u32| (0..width as usize).fold(0u64, |v, j| v | bit(from + j) << j);
        for i in 0..c {
            let (mut dl, mut dg) = (unzigzag(read(i * wl as usize, wl)), unzigzag(read(c * wl as usize + i * wg as usize, wg)));
            if head.second_order {
                (dl, dg) = (dl + prev.0, dg + prev.1);
            }
            prev = (dl, dg);
            (lat, lng) = (lat + dl, lng + dg);
            if !(head.units.hold(lat) && head.units.hold(lng)) {
                return None;
            }
            out.push(e7(lat, lng));
        }
        at += used;
    }
    Some(out)
}

#[test]
fn the_vertex_loops_read_what_a_bit_at_a_time_reads() {
    // Blocks of random groups (narrow and wide widths, second-order deltas, base widths up to 30),
    // values that leave the grid's i32 range, every prefix and single flipped bits: the vertices,
    // the count and the crossing parity are the bit-at-a-time reader's, malformed blocks included.
    let mut rng = Rng::new(20261113);
    let (mut whole, mut malformed) = (0, 0);
    for case in 0..2000 {
        let k = [0u8, 2, 3][case % 3];
        let units = GRID_UNITS[k as usize];
        let boxed = case % 5 == 0;
        let base = if rng.next() % 6 == 0 { 2 * (rng.next() % 16) as u32 } else { 0 };
        let second = rng.next() % 3 == 0;
        let coordinate = |rng: &mut Rng| (rng.next() % units.span as u64) as i64 + units.lo;
        let mut block = vec![k | if second { SECOND_ORDER } else { 0 } | ((base / 2) as u8) << BASE_WIDTH_SHIFT | if boxed { HAS_BOX } else { 0 }];
        let (lat0, lng0) = (coordinate(&mut rng) / 2, coordinate(&mut rng) / 2);
        block.extend((lat0 as i32).to_le_bytes());
        block.extend((lng0 as i32).to_le_bytes());
        if boxed {
            for _ in 0..3 {
                block.extend((coordinate(&mut rng) as i32).to_le_bytes());
            }
        }
        let groups = rng.next() % 5;
        for g in 0..groups {
            // Mostly narrow widths, so most blocks stay in range.
            let nibble = |rng: &mut Rng| if rng.next() % 4 == 0 { (rng.next() % 16) as u32 } else { (rng.next() % 7) as u32 };
            let (dl, dg) = (nibble(&mut rng), nibble(&mut rng));
            let w = (2 * base + dl + dg) as usize;
            block.push((dl << 4 | dg) as u8);
            let c = if g + 1 < groups || w < 8 { ARC_GROUP } else { 1 + (rng.next() % ARC_GROUP as u64) as usize };
            for _ in 0..(c * w).div_ceil(8) {
                // Small bytes most often: deltas near 0.
                block.push(if rng.next() % 3 == 0 { rng.next() as u8 } else { (rng.next() % 4) as u8 });
            }
        }
        let p = PointE7 { lat: lat0 * 10i64.pow(k as u32), lng: lng0 * 10i64.pow(k as u32) + 1 };
        let mut check = |b: &[u8]| {
            let want = vertices_bitwise(b);
            assert_eq!(decoded(b), want, "case {case} {} bytes", b.len());
            let got = arc_crossings(b, p);
            // Bytes after the block, the next one's in the file, change nothing.
            for after in [&[0xff; 7][..], &[0x00; 7], &[0x80, 0xff], &[0xff], &[0x83, 0x00, 0xff, 0x12, 0x34, 0x56, 0x78]] {
                assert_eq!(arc_crossings_in(&[b, after].concat(), b.len(), p), got, "case {case} {} bytes, then {after:?}", b.len());
            }
            match (&want, got) {
                (None, got) => {
                    assert!(got.is_none() || got == Some(ArcCrossings::Boxed), "case {case}");
                    malformed += 1;
                }
                (Some(v), Some(ArcCrossings::Read { odd, vertices })) => {
                    assert_eq!((odd, vertices as usize), (odd_crossings(v, p), v.len()), "case {case}");
                    whole += 1;
                }
                (Some(_), got) => assert!(got == Some(ArcCrossings::Boxed), "case {case}"),
            }
        };
        check(&block);
        for len in (0..block.len()).step_by(1 + block.len() / 40) {
            check(&block[..len]);
        }
        for _ in 0..8 {
            let mut bad = block.clone();
            let at = (rng.next() % bad.len() as u64) as usize;
            bad[at] ^= 1 << (rng.next() % 8);
            check(&bad);
        }
    }
    assert!(whole > 1000 && malformed > 1000, "{whole} read whole, {malformed} malformed");
}
