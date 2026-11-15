use super::*;
use crate::arc_blocks::tests::{odd_crossings, probes, square_of_arcs};
use crate::arc_blocks::ARC_MEMO_CAPACITY;
use crate::arc_fixture::{e7, exact_area, record, ArcFixture, Ref};
use crate::test_rng::Rng;
use crate::exact_pip::tests::testdata_rows;
use crate::scratch_dir::ScratchDir;
use crate::{cell_id_at_level, AdminScan, Index, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE, DEFAULT_STREET_CELL_LEVEL, STR_TIER_COUNT};

const fn pt(lat: i64, lng: i64) -> PointE7 {
    PointE7 { lat, lng }
}

/// Polygons whose rings are refs to arcs, and the rings they were made from: the truth every
/// answer is checked against.
#[derive(Default)]
struct Shapes {
    fx: ArcFixture,
    /// Each polygon's rings as given, outer first, each closed.
    rings: Vec<Vec<Vec<PointE7>>>,
}

impl Shapes {
    /// A polygon whose rings are `refs`, made from the closed vertex rings `rings`.
    fn polygon(&mut self, level: u8, refs: &[Vec<Ref>], rings: Vec<Vec<PointE7>>) -> u32 {
        let id = self.fx.polygon(record(0, level, exact_area(&rings)), refs);
        self.rings.push(rings);
        id
    }

    fn geometry(&self) -> (AdminGeometry, Vec<AdminPolygon>) {
        let [_, (_, rings), (_, index), (_, vertices)] = self.fx.files();
        (AdminGeometry::new(FileBytes::Owned(rings), FileBytes::Owned(index), FileBytes::Owned(vertices)), self.fx.records())
    }
}

/// Whether the ray from `p` crosses `rings`' edges an odd number of times: the brute-force
/// integer ray cast over every ring as given.
fn brute(rings: &[Vec<PointE7>], p: PointE7) -> bool {
    rings.iter().fold(false, |inside, ring| inside ^ odd_crossings(ring, p))
}

/// A lattice edge from one junction to the next: its vertices and the refs of the arcs it was
/// cut into, both in that direction.
#[derive(Clone)]
struct Edge {
    line: Vec<PointE7>,
    refs: Vec<Ref>,
}

impl Edge {
    fn back(&self) -> Edge {
        Edge {
            line: self.line.iter().rev().copied().collect(),
            refs: self.refs.iter().rev().map(|r| Ref { reversed: !r.reversed, ..*r }).collect(),
        }
    }
}

/// The refs and closed vertex ring of edges joined end to end.
fn ring_of(edges: &[Edge]) -> (Vec<Ref>, Vec<PointE7>) {
    let mut line = vec![edges[0].line[0]];
    for e in edges {
        assert_eq!(line.last(), e.line.first());
        line.extend(&e.line[1..]);
    }
    assert_eq!(line.first(), line.last());
    (edges.iter().flat_map(|e| e.refs.iter().copied()).collect(), line)
}

/// A rows × cols lattice of cells between jittered junctions, each lattice edge a line wiggling
/// across its straight one (`vertices` at most between its junctions) on the 10^k grid of a k
/// picked from `grids`, cut into one to three arcs stored either way round. Cells
/// (level 8) share their edges' arcs with their neighbours, with each 2 × 2 block of them
/// (level 6) and with the lattice (level 2). With `holes`, every third cell has a star-shaped
/// hole, one or two arcs, which an island (level 8) fills.
struct Lattice {
    shapes: Shapes,
    cells: Vec<u32>,
    islands: Vec<u32>,
    blocks: Vec<u32>,
    whole: u32,
}

const LATTICE_ORIGIN: PointE7 = pt(-339_000_000, -706_000_000);

fn lattice(rng: &mut Rng, (rows, cols): (usize, usize), side: i64, vertices: usize, grids: &[u8], holes: bool) -> Lattice {
    let mut shapes = Shapes::default();
    let snap = |c: i64, step: i64| c.div_euclid(step) * step;
    let grid = |rng: &mut Rng| grids[(rng.next() % grids.len() as u64) as usize];
    // Junctions on the coarsest grid, which every finer one holds.
    let junction_step = 10i64.pow(*grids.iter().max().unwrap() as u32);
    let jitter = side / 40;
    let junction: Vec<Vec<PointE7>> = (0..=rows)
        .map(|i| (0..=cols).map(|j| {
            let d = |rng: &mut Rng| rng.range(-jitter as f64, jitter as f64) as i64;
            pt(snap(LATTICE_ORIGIN.lat + i as i64 * side + d(rng), junction_step), snap(LATTICE_ORIGIN.lng + j as i64 * side + d(rng), junction_step))
        }).collect())
        .collect();
    let edge = |rng: &mut Rng, fx: &mut ArcFixture, a: PointE7, b: PointE7, across_lat: bool| {
        let k = grid(rng);
        let step = 10i64.pow(k as u32);
        let n = (rng.next() % (vertices as u64 + 1)) as usize;
        let mut ts: Vec<f64> = (0..n).map(|_| rng.range(0.25, 0.75)).collect();
        ts.sort_by(f64::total_cmp);
        let mut line = vec![a];
        for t in ts {
            let off = rng.range(-(jitter as f64), jitter as f64) as i64;
            let at = |x: i64, y: i64| x + ((y - x) as f64 * t) as i64;
            let (lat, lng) = (at(a.lat, b.lat), at(a.lng, b.lng));
            line.push(if across_lat { pt(snap(lat + off, step), snap(lng, step)) } else { pt(snap(lat, step), snap(lng + off, step)) });
        }
        line.push(b);
        line.dedup();
        let mut cuts: Vec<usize> = (0..rng.next() % 3).map(|_| 1 + (rng.next() % (line.len() as u64 - 1).max(1)) as usize).filter(|&c| c < line.len() - 1).collect();
        cuts.extend([0, line.len() - 1]);
        cuts.sort_unstable();
        cuts.dedup();
        let refs = cuts.windows(2).map(|w| {
            let piece = &line[w[0]..=w[1]];
            let reversed = rng.next() % 2 == 1;
            let stored: Vec<PointE7> = if reversed { piece.iter().rev().copied().collect() } else { piece.to_vec() };
            Ref { slot: fx.arc(k, &stored), reversed }
        }).collect();
        Edge { line, refs }
    };
    let fx = &mut shapes.fx;
    // along[i][j]: junction (i, j) to (i, j + 1); up[i][j]: (i, j) to (i + 1, j).
    let along: Vec<Vec<Edge>> = (0..=rows).map(|i| (0..cols).map(|j| edge(rng, fx, junction[i][j], junction[i][j + 1], true)).collect()).collect();
    let up: Vec<Vec<Edge>> = (0..rows).map(|i| (0..=cols).map(|j| edge(rng, fx, junction[i][j], junction[i + 1][j], false)).collect()).collect();

    let around = |i0: usize, j0: usize, h: usize, w: usize| -> Vec<Edge> {
        let mut e: Vec<Edge> = (j0..j0 + w).map(|j| along[i0][j].clone()).collect();
        e.extend((i0..i0 + h).map(|i| up[i][j0 + w].clone()));
        e.extend((j0..j0 + w).rev().map(|j| along[i0 + h][j].back()));
        e.extend((i0..i0 + h).rev().map(|i| up[i][j0].back()));
        e
    };
    let (mut cells, mut islands, mut blocks) = (Vec::new(), Vec::new(), Vec::new());
    for i in 0..rows {
        for j in 0..cols {
            let (refs, ring) = ring_of(&around(i, j, 1, 1));
            if !holes || (i + j) % 3 != 0 {
                cells.push(shapes.polygon(8, &[refs], vec![ring]));
                continue;
            }
            let k = grid(rng);
            let step = 10i64.pow(k as u32);
            let c = [junction[i][j], junction[i + 1][j + 1]];
            let centre = pt((c[0].lat + c[1].lat) / 2, (c[0].lng + c[1].lng) / 2);
            let m = 6 + (rng.next() % 25) as usize;
            let mut angles: Vec<f64> = (0..m).map(|_| rng.range(0.0, std::f64::consts::TAU)).collect();
            angles.sort_by(f64::total_cmp);
            let mut hole: Vec<PointE7> = angles.iter().map(|a| {
                let r = side as f64 / 10.0 * rng.range(0.5, 1.0);
                pt(snap(centre.lat + (r * a.sin()) as i64, step), snap(centre.lng + (r * a.cos()) as i64, step))
            }).collect();
            hole.dedup();
            if hole.first() == hole.last() {
                hole.pop();
            }
            hole.push(hole[0]);
            let hole_refs: Vec<Ref> = if rng.next() % 2 == 0 {
                vec![Ref::fwd(shapes.fx.arc(k, &hole))]
            } else {
                let half = hole.len() / 2;
                vec![Ref::fwd(shapes.fx.arc(k, &hole[..=half])), Ref::rev(shapes.fx.arc(k, &hole[half..].iter().rev().copied().collect::<Vec<_>>()))]
            };
            cells.push(shapes.polygon(8, &[refs, hole_refs.clone()], vec![ring, hole.clone()]));
            let island_refs: Vec<Ref> = hole_refs.iter().rev().map(|r| Ref { reversed: !r.reversed, ..*r }).collect();
            islands.push(shapes.polygon(8, &[island_refs], vec![hole.iter().rev().copied().collect()]));
        }
    }
    for i in (0..rows.saturating_sub(1)).step_by(2) {
        for j in (0..cols.saturating_sub(1)).step_by(2) {
            let (refs, ring) = ring_of(&around(i, j, 2, 2));
            blocks.push(shapes.polygon(6, &[refs], vec![ring]));
        }
    }
    let (refs, ring) = ring_of(&around(0, 0, rows, cols));
    let whole = shapes.polygon(2, &[refs], vec![ring]);
    Lattice { shapes, cells, islands, blocks, whole }
}

impl Lattice {
    /// Every polygon's ids.
    fn polygons(&self) -> impl Iterator<Item = u32> + '_ {
        self.cells.iter().chain(&self.islands).chain(&self.blocks).copied().chain([self.whole])
    }

    /// Points along every ring of the lattice, and `random` more over it.
    fn probes(&self, rng: &mut Rng, random: usize) -> Vec<PointE7> {
        let mut out: Vec<PointE7> = self.shapes.rings.iter().flatten().flat_map(|r| probes(rng, r, 0)).collect();
        out.extend(probes(rng, &self.shapes.rings[self.whole as usize][0], random));
        out
    }
}

#[test]
fn admin_points_round_as_the_shared_vectors_say() {
    // Every row: a table that lost them would check nothing.
    let rows = testdata_rows("point_e7.tsv");
    assert!(rows.len() > 60);
    for r in rows {
        let deg = if r[1] == "f32" { r[0].parse::<f32>().unwrap() as f64 } else { r[0].parse::<f64>().unwrap() };
        let want: i64 = r[2].parse().unwrap();
        assert_eq!(AdminPoint::new(deg, 0.0).e7(), pt(want, 0), "{r:?}");
        assert_eq!(AdminPoint::new(0.0, deg).e7(), pt(0, want), "{r:?}");
    }
    // Off the globe, and NaN, is beyond every ring; the globe itself is kept.
    let limit = AdminPoint::LIMIT_E7;
    assert_eq!(AdminPoint::new(1e12, -f64::INFINITY).e7(), pt(limit, -limit));
    assert_eq!(AdminPoint::new(f64::NAN, f64::NAN).e7(), pt(limit, limit));
    assert_eq!(AdminPoint::new(-90.0, 180.0).e7(), pt(-900_000_000, 1_800_000_000));
    assert_eq!(AdminPoint::new(47.1234567, 8.5).degrees(), (47.1234567, 8.5));
}

#[test]
fn contains_is_the_ray_cast_over_the_input_rings() {
    let mut rng = Rng::new(20261103);
    for (dims, vertices, holes) in [((4, 4), 40, true), ((2, 3), 3, false), ((1, 1), 60, true)] {
        let l = lattice(&mut rng, dims, 200_000, vertices, &[0], holes);
        let (geom, polys) = l.shapes.geometry();
        let probes = l.probes(&mut rng, 4000);
        let mut inside = 0;
        for &p in &probes {
            let (point, mut memo) = (AdminPoint(p), PipMemo::new());
            for id in l.polygons() {
                let want = brute(&l.shapes.rings[id as usize], p);
                inside += want as usize;
                assert_eq!(geom.contains_once(&polys[id as usize], point), want, "{dims:?} polygon {id} {p:?}");
                assert_eq!(geom.contains(&polys[id as usize], point, &mut memo), want, "{dims:?} polygon {id} {p:?}");
            }
        }
        assert!(inside > probes.len() / 2, "{dims:?}");
    }
}

/// Every e7 point within `reach` of an edge of `line`, on its main axis's every unit.
fn band(line: &[PointE7], reach: i64) -> Vec<PointE7> {
    let mut out = Vec::new();
    for e in line.windows(2) {
        let (a, b) = (e[0], e[1]);
        let (dlat, dlng) = (b.lat - a.lat, b.lng - a.lng);
        if dlng.abs() >= dlat.abs() && dlng != 0 {
            for lng in a.lng.min(b.lng)..=a.lng.max(b.lng) {
                let lat = a.lat + ((lng - a.lng) * dlat) / dlng;
                out.extend((-reach..=reach).map(|d| pt(lat + d, lng)));
            }
        } else if dlat != 0 {
            for lat in a.lat.min(b.lat)..=a.lat.max(b.lat) {
                let lng = a.lng + ((lat - a.lat) * dlng) / dlat;
                out.extend((-reach..=reach).map(|d| pt(lat, lng + d)));
            }
        }
    }
    out
}

#[test]
fn a_point_along_a_shared_arc_is_in_exactly_one_neighbour() {
    let mut rng = Rng::new(3);
    // Two neighbours, then a lattice of nine with enclaves: its cells and islands tile it.
    for (dims, side, holes) in [((1, 2), 3000, false), ((3, 3), 1500, true)] {
        let l = lattice(&mut rng, dims, side, 12, &[0], holes);
        let (geom, polys) = l.shapes.geometry();
        let tiles: Vec<u32> = l.cells.iter().chain(&l.islands).copied().collect();
        let (mut on_edges, mut points) = (0, 0);
        for ring in l.shapes.rings.iter().flatten() {
            for p in band(ring, 3) {
                let holding = tiles.iter().filter(|&&id| geom.contains_once(&polys[id as usize], AdminPoint(p))).count();
                let in_lattice = brute(&l.shapes.rings[l.whole as usize], p);
                assert_eq!(holding, in_lattice as usize, "{dims:?} {p:?}");
                on_edges += ring.windows(2).any(|e| on_edge(e[0], e[1], p)) as usize;
                points += 1;
            }
        }
        assert!(on_edges > 100, "{dims:?}: {on_edges} of {points} on an edge");
    }
}

#[test]
fn a_point_in_a_hole_is_outside_and_the_island_filling_it_inside() {
    let mut s = Shapes::default();
    let square = |lat0: i64, lng0: i64, lat1: i64, lng1: i64| vec![pt(lat0, lng0), pt(lat0, lng1), pt(lat1, lng1), pt(lat1, lng0), pt(lat0, lng0)];
    let outer = square(0, 0, 1000, 1000);
    let hole = square(300, 300, 700, 600);
    let o = s.fx.arc(0, &outer);
    // The hole is two arcs, its island the same two the other way round.
    let (h0, h1) = (s.fx.arc(0, &hole[..=2]), s.fx.arc(0, &hole[2..]));
    let holed = s.polygon(8, &[vec![Ref::fwd(o)], vec![Ref::fwd(h0), Ref::fwd(h1)]], vec![outer.clone(), hole.clone()]);
    let island = s.polygon(8, &[vec![Ref::rev(h1), Ref::rev(h0)]], vec![hole.iter().rev().copied().collect()]);
    let (geom, polys) = s.geometry();
    let at = |id: u32, lat, lng| geom.contains_once(&polys[id as usize], AdminPoint(pt(lat, lng)));
    assert!(!at(holed, 500, 450) && at(island, 500, 450));
    assert!(at(holed, 100, 100) && !at(island, 100, 100));
    for p in band(&hole, 2) {
        assert!(at(holed, p.lat, p.lng) != at(island, p.lat, p.lng), "{p:?}");
    }
    assert_eq!(polys[holed as usize].hole_count, 1);
}

#[test]
fn a_polygon_of_mixed_grids_answers_like_its_e7_rings() {
    let mut rng = Rng::new(31);
    let l = lattice(&mut rng, (3, 3), 200_000, 30, &[2, 3], true);
    let (geom, polys) = l.shapes.geometry();
    let grids = |id: u32| -> Vec<u8> {
        let mut flags = Vec::new();
        let block = geom.ring_block(&polys[id as usize]).unwrap();
        struct Grids<'a, 'f>(ArcFiles<'a>, &'f mut Vec<u8>);
        impl RingVisitor for Grids<'_, '_> {
            fn wants_chunk(&mut self, _: &ChunkBox) -> bool { true }
            fn arc(&mut self, r: ArcRef) -> Option<()> {
                self.1.push(self.0.block(r.slot)?[0] & crate::arc_blocks::GRID_MASK);
                Some(())
            }
        }
        walk_rings(block, &mut Grids(geom.arcs(), &mut flags)).unwrap();
        flags
    };
    assert!(l.cells.iter().any(|&c| { let g = grids(c); g.contains(&2) && g.contains(&3) }));
    for p in l.probes(&mut rng, 3000) {
        for id in l.polygons() {
            assert_eq!(geom.contains_once(&polys[id as usize], AdminPoint(p)), brute(&l.shapes.rings[id as usize], p), "polygon {id} {p:?}");
        }
    }
}

#[test]
fn the_memo_reads_an_arc_its_levels_share_once_per_point() {
    let mut rng = Rng::new(17);
    let l = lattice(&mut rng, (4, 4), 200_000, 10, &[0], false);
    let (geom, polys) = l.shapes.geometry();
    let (mut shared_reads, mut separate_reads) = (0, 0);
    for p in l.probes(&mut rng, 500) {
        // A scan's polygons: those whose rings come near the point, every level.
        let near: Vec<u32> = l.polygons().filter(|&id| {
            let r = &l.shapes.rings[id as usize][0];
            let span = |f: fn(&PointE7) -> i64| (r.iter().map(f).min().unwrap(), r.iter().map(f).max().unwrap());
            let ((lat0, lat1), (lng0, lng1)) = (span(|v| v.lat), span(|v| v.lng));
            (lat0..=lat1).contains(&p.lat) && (lng0..=lng1).contains(&p.lng)
        }).collect();
        let mut memo = PipMemo::new();
        let mut read = std::collections::BTreeSet::new();
        for &id in &near {
            let poly = &polys[id as usize];
            assert_eq!(geom.contains(poly, AdminPoint(p), &mut memo), geom.contains_once(poly, AdminPoint(p)), "{id} {p:?}");
            let mut alone = PipMemo::new();
            geom.contains(poly, AdminPoint(p), &mut alone);
            separate_reads += alone.arcs_read();
            read.extend((0..geom.arcs().slots() as u32).filter(|&s| alone.arcs.get(s).is_some()));
        }
        assert!(memo.arcs.len() < ARC_MEMO_CAPACITY);
        assert_eq!(memo.arcs_read() as usize, read.len(), "{p:?}");
        shared_reads += memo.arcs_read();
    }
    assert!(shared_reads < separate_reads, "{shared_reads} shared, {separate_reads} separate");
}

#[test]
fn past_its_capacity_the_memo_reads_arcs_again_and_answers_stay_equal() {
    // Twelve squares round the origin, each of eight arcs of their own: 96 arcs, all read at the
    // origin. Then the same rings again under twelve other records.
    let mut s = Shapes::default();
    let mut refs = Vec::new();
    for i in 1..=12 {
        let r = 1000 * i;
        let ring = vec![pt(-r, -r), pt(-r, 0), pt(-r, r), pt(0, r), pt(r, r), pt(r, 0), pt(r, -r), pt(0, -r), pt(-r, -r)];
        let arcs: Vec<Ref> = ring.windows(2).map(|e| Ref::fwd(s.fx.arc(0, e))).collect();
        refs.push((arcs, ring));
    }
    for (arcs, ring) in refs.iter().chain(refs.iter()) {
        s.polygon(8, &[arcs.clone()], vec![ring.clone()]);
    }
    let (geom, polys) = s.geometry();
    let mut memo = PipMemo::new();
    for p in [pt(0, 0), pt(1, 1), pt(-999, 5)] {
        for (i, poly) in polys.iter().enumerate() {
            assert_eq!(geom.contains(poly, AdminPoint(p), &mut memo), geom.contains_once(poly, AdminPoint(p)), "{i} {p:?}");
            assert!(geom.contains_once(poly, AdminPoint(p)));
            if p == pt(0, 0) && i == 11 {
                assert_eq!((memo.arcs_read(), memo.arcs.len()), (96, ARC_MEMO_CAPACITY));
            }
        }
        if p == pt(0, 0) {
            // The second twelve read again the 32 arcs the memo had no room for.
            assert_eq!(memo.arcs_read(), 96 + 32);
        }
        memo = PipMemo::new();
    }
}

mod heap {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;

    thread_local! {
        static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
    }

    /// The system allocator, counting each thread's allocations.
    pub struct Counting;

    fn count() {
        let _ = ALLOCATIONS.try_with(|n| n.set(n.get() + 1));
    }

    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            count();
            System.alloc(layout)
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            count();
            System.alloc_zeroed(layout)
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            count();
            System.realloc(ptr, layout, new_size)
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            System.dealloc(ptr, layout)
        }
    }

    #[global_allocator]
    static COUNTING: Counting = Counting;

    /// This thread's allocations so far.
    pub fn allocations() -> u64 {
        ALLOCATIONS.with(|n| n.get())
    }
}

#[test]
fn reading_arcs_allocates_nothing_beyond_the_scans_own_memo() {
    // Long exact borders: up to 400 vertices a lattice edge, so arcs carry boxes and rings chunks.
    let mut rng = Rng::new(23);
    let l = lattice(&mut rng, (4, 4), 2_000_000, 400, &[0], true);
    let (geom, polys) = l.shapes.geometry();
    let ids: Vec<u32> = l.polygons().collect();
    assert!(ids.len() * 2 <= 64, "more polygons than the block memo holds before it grows");
    let mut points: Vec<PointE7> = l.shapes.rings.iter().flatten().flat_map(|r| probes(&mut rng, r, 0)).collect();
    points.truncate(10_000);
    assert_eq!(points.len(), 10_000);
    let (mut memo_tables, mut arcs_read) = (0, 0);
    let before = heap::allocations();
    for &p in &points {
        let mut memo = PipMemo::new();
        for &id in &ids {
            geom.contains(&polys[id as usize], AdminPoint(p), &mut memo);
        }
        memo_tables += 1;
        arcs_read += memo.arcs_read() as u64;
    }
    // One allocation a point: the block memo's table, which main's scans allocate too.
    assert_eq!(heap::allocations() - before, memo_tables);
    let before = heap::allocations();
    let mut inside = 0;
    for &p in &points {
        for &id in &ids {
            inside += geom.contains_once(&polys[id as usize], AdminPoint(p)) as u64;
        }
    }
    assert_eq!(heap::allocations() - before, 0);
    assert!(inside > 0 && arcs_read > 10 * points.len() as u64, "{arcs_read} arcs read");
}

#[test]
fn rings_are_the_outer_then_the_holes_closed_each_junction_once() {
    let mut s = Shapes::default();
    // An outer ring of three arcs, the middle one stored backwards, round a hole of two.
    let a = s.fx.arc(0, &[pt(0, 0), pt(0, 50), pt(0, 100)]);
    let b = s.fx.arc(0, &[pt(100, 100), pt(50, 110), pt(0, 100)]);
    let c = s.fx.arc(0, &[pt(100, 100), pt(100, 0), pt(0, 0)]);
    let (h0, h1) = (s.fx.arc(0, &[pt(20, 20), pt(20, 40), pt(40, 40)]), s.fx.arc(0, &[pt(40, 40), pt(40, 20), pt(20, 20)]));
    let outer = vec![pt(0, 0), pt(0, 50), pt(0, 100), pt(50, 110), pt(100, 100), pt(100, 0), pt(0, 0)];
    let hole = vec![pt(20, 20), pt(20, 40), pt(40, 40), pt(40, 20), pt(20, 20)];
    let id = s.polygon(8, &[vec![Ref::fwd(a), Ref::rev(b), Ref::fwd(c)], vec![Ref::fwd(h0), Ref::fwd(h1)]], vec![outer.clone(), hole.clone()]);
    // A ring of one closed arc, walked backwards.
    let loop_ = s.fx.arc(0, &hole);
    let backwards = s.polygon(8, &[vec![Ref::rev(loop_)]], vec![hole.iter().rev().copied().collect()]);
    let (geom, polys) = s.geometry();
    let lng_lat = |r: &[PointE7]| r.iter().map(|v| [v.lng as f64 / 1e7, v.lat as f64 / 1e7]).collect::<Vec<_>>();
    assert_eq!(geom.rings(&polys[id as usize]), vec![lng_lat(&outer), lng_lat(&hole)]);
    assert_eq!(geom.rings(&polys[backwards as usize]), vec![lng_lat(&s.rings[backwards as usize][0])]);
    let mut rng = Rng::new(9);
    let l = lattice(&mut rng, (3, 3), 200_000, 30, &[0, 2, 3], true);
    let (geom, polys) = l.shapes.geometry();
    for id in l.polygons() {
        let rings = geom.rings(&polys[id as usize]);
        let want: Vec<_> = l.shapes.rings[id as usize].iter().map(|r| lng_lat(r)).collect();
        assert_eq!(rings, want, "polygon {id}");
        assert!(rings.iter().all(|r| r.first() == r.last() && r.windows(2).all(|e| e[0] != e[1])), "polygon {id}");
    }
}

/// The builder's interior_point (polygon_rings.h) over rings as given, in degrees.
fn interior_point(rings: &[Vec<PointE7>]) -> (f64, f64) {
    let deg = |c: i64| c as f64 / 1e7;
    let outer = &rings[0];
    let (mut lo, mut hi) = (outer.iter().map(|v| deg(v.lat)).fold(f64::INFINITY, f64::min), outer.iter().map(|v| deg(v.lat)).fold(f64::NEG_INFINITY, f64::max));
    let centre = (lo + hi) / 2.0;
    for v in rings.iter().flatten() {
        let y = deg(v.lat);
        if y <= centre {
            if y > lo { lo = y; }
        } else if y < hi {
            hi = y;
        }
    }
    let scan = (lo + hi) / 2.0;
    let mut crossings = Vec::new();
    for r in rings {
        for e in r.windows(2) {
            let (y0, y1, x0, x1) = (deg(e[0].lat), deg(e[1].lat), deg(e[0].lng), deg(e[1].lng));
            if (y0 > scan && y1 > scan) || (y0 < scan && y1 < scan) || y0 == y1 { continue; }
            if (y0 == scan && y1 < scan) || (y1 == scan && y0 < scan) { continue; }
            crossings.push(if x0 == x1 { x0 } else { x0 + (scan - y0) / ((y1 - y0) / (x1 - x0)) });
        }
    }
    crossings.sort_by(f64::total_cmp);
    let mut best = (deg(outer[0].lat), deg(outer[0].lng));
    let mut width = 0.0;
    for run in crossings.chunks_exact(2) {
        if run[1] - run[0] > width {
            width = run[1] - run[0];
            best = (scan, (run[0] + run[1]) / 2.0);
        }
    }
    best
}

/// A closed ring of (lat, lng) degree vertices.
fn ring_deg(v: &[(f64, f64)]) -> Vec<PointE7> {
    v.iter().map(|&(lat, lng)| e7(lat, lng)).collect()
}

fn box_deg(lat0: f64, lng0: f64, lat1: f64, lng1: f64) -> Vec<PointE7> {
    ring_deg(&[(lat0, lng0), (lat0, lng1), (lat1, lng1), (lat1, lng0), (lat0, lng0)])
}

/// A polygon of rings, each one arc.
fn ring_polygon(s: &mut Shapes, rings: Vec<Vec<PointE7>>) -> u32 {
    let refs: Vec<Vec<Ref>> = rings.iter().map(|r| vec![Ref::fwd(s.fx.arc(0, r))]).collect();
    s.polygon(8, &refs, rings)
}

#[test]
fn point_on_surface_is_in_the_widest_run_inside() {
    let mut s = Shapes::default();
    // A square: its middle. An L whose box's middle is outside it: on the line between the
    // latitudes round the middle (1 and 4, so 2.5), the middle of the run inside (lng 0 to 2).
    let square = ring_polygon(&mut s, vec![box_deg(0.0, 0.0, 2.0, 2.0)]);
    let l = ring_polygon(&mut s, vec![ring_deg(&[(0.0, 0.0), (0.0, 8.0), (1.0, 8.0), (1.0, 2.0), (4.0, 2.0), (4.0, 0.0), (0.0, 0.0)])]);
    // Holes split the runs: at lat 5 the run is 0 to 10 past the holes, and 6 to 10 right of a
    // hole from lng 1 to 6. A hole from lat 4 to 8 moves the line to 6.
    let outer = box_deg(0.0, 0.0, 10.0, 10.0);
    let two_holes = ring_polygon(&mut s, vec![outer.clone(), box_deg(2.0, 2.0, 4.0, 4.0), box_deg(6.0, 5.0, 7.0, 8.0)]);
    let wide_hole = ring_polygon(&mut s, vec![outer.clone(), box_deg(2.0, 1.0, 8.0, 6.0)]);
    let high_hole = ring_polygon(&mut s, vec![outer.clone(), box_deg(4.0, 1.0, 8.0, 6.0)]);
    // An enclave: its ring is the hole's arc of the polygon round it, so its extreme latitudes
    // are the hole's.
    let hole = box_deg(2.0, 1.0, 8.0, 6.0);
    let (o, h) = (s.fx.arc(0, &outer), s.fx.arc(0, &hole));
    let around = s.polygon(8, &[vec![Ref::fwd(o)], vec![Ref::fwd(h)]], vec![outer.clone(), hole.clone()]);
    let enclave = s.polygon(8, &[vec![Ref::rev(h)]], vec![hole.iter().rev().copied().collect()]);
    // A comb whose scan line at lat 4 crosses its three teeth: the widest, lng 3 to 5, wins.
    let comb = ring_polygon(&mut s, vec![ring_deg(&[
        (0.0, 0.0), (0.0, 9.0), (6.0, 9.0), (6.0, 8.0), (2.0, 8.0), (2.0, 5.0), (6.0, 5.0), (6.0, 3.0), (2.0, 3.0),
        (2.0, 1.0), (6.0, 1.0), (6.0, 0.0), (0.0, 0.0),
    ])]);
    // Rings that don't nest, as raw data's may not: the outer ring's latitude range alone picks
    // the line, 1.5 between its 1 and 2, not the middle of both.
    let unnested = ring_polygon(&mut s, vec![box_deg(0.0, 0.0, 2.0, 2.0), box_deg(1.0, 1.0, 10.0, 3.0)]);
    // A ring with no width gives its first vertex.
    let flat = ring_polygon(&mut s, vec![ring_deg(&[(1.0, 0.0), (1.0, 2.0), (1.0, 1.0), (1.0, 0.0)])]);
    let ringless = s.polygon(8, &[], vec![]);
    let (geom, polys) = s.geometry();
    let at = |id: u32| geom.point_on_surface(&polys[id as usize]);
    assert_eq!(at(square), Some((1.0, 1.0)));
    assert_eq!(at(l), Some((2.5, 1.0)));
    assert_eq!(at(two_holes), Some((5.0, 5.0)));
    assert_eq!(at(wide_hole), Some((5.0, 8.0)));
    assert_eq!(at(high_hole), Some((6.0, 8.0)));
    assert_eq!(at(around), Some((5.0, 8.0)));
    assert_eq!(at(enclave), Some((5.0, 3.5)));
    assert_eq!(at(comb), Some((4.0, 4.0)));
    assert_eq!(at(unnested), Some((1.5, 0.5)));
    assert_eq!(at(flat), Some((1.0, 0.0)));
    assert_eq!(at(ringless), None);
    for id in [square, l, two_holes, wide_hole, high_hole, around, enclave, comb, unnested, flat] {
        let (lat, lng) = interior_point(&s.rings[id as usize]);
        assert_eq!(at(id), Some((lat as f32, lng as f32)), "{id}");
    }
    // Each has its point on surface inside it, and the enclave's is in the hole round it.
    let within = |a: u32, b: u32| geom.point_on_surface_in(&polys[a as usize], &polys[b as usize]);
    assert!([square, l, two_holes, wide_hole, high_hole, around, enclave, comb].iter().all(|&id| within(id, id)));
    assert!(!within(enclave, around) && !within(around, enclave));
    // The square's point (1, 1) is inside the 0..10 square, but not on the top edge of a hole
    // there, which the ray cast alone counts as inside.
    let big = ring_polygon(&mut s, vec![outer.clone()]);
    let low_hole = ring_polygon(&mut s, vec![outer.clone(), box_deg(0.5, 0.5, 1.0, 4.0)]);
    let (geom, polys) = s.geometry();
    let within = |a: u32, b: u32| geom.point_on_surface_in(&polys[a as usize], &polys[b as usize]);
    assert!(within(square, big));
    assert!(geom.contains_once(&polys[low_hole as usize], AdminPoint::new(1.0, 1.0)));
    assert!(!within(square, low_hole));
    assert!(!within(ringless, big) && !within(big, ringless));
}

#[test]
fn point_on_surface_streams_what_the_builder_finds_over_the_rings() {
    // Wiggling edges cross a scan line many times, and in a lattice of mixed grids arcs of
    // different grids meet at their shared junctions.
    let mut rng = Rng::new(41);
    for grids in [&[0u8][..], &[2], &[3], &[0, 2, 3]] {
        for round in 0..4 {
            let l = lattice(&mut rng, (3, 3), 200_000, 40, grids, true);
            let (geom, polys) = l.shapes.geometry();
            let mut crossings = Vec::new();
            for id in l.polygons() {
                let (lat, lng) = interior_point(&l.shapes.rings[id as usize]);
                let got = geom.point_on_surface_with(&polys[id as usize], &mut crossings);
                assert_eq!(got, Some((lat as f32, lng as f32)), "grids {grids:?} round {round} polygon {id}");
                assert!(geom.contains_once(&polys[id as usize], AdminPoint::new(lat, lng)), "grids {grids:?} round {round} polygon {id}");
            }
        }
    }
}

#[test]
fn current_boundaries_test_the_point_on_surface_of_larger_and_equal_boundaries() {
    // A city square and rank 20 boundaries round it, beside it, beside it at the same area, and
    // with their point on surface on the city's edge: only the one round it has its point on
    // surface inside the city.
    let mut s = Shapes::default();
    let city = ring_polygon(&mut s, vec![box_deg(0.0, 0.0, 2.0, 2.0)]);
    let others = [
        (ring_polygon(&mut s, vec![box_deg(-1.0, -1.0, 3.0, 3.0)]), true),
        (ring_polygon(&mut s, vec![box_deg(1.5, 1.5, 3.5, 9.5)]), false),
        (ring_polygon(&mut s, vec![box_deg(1.5, 1.5, 3.5, 3.5)]), false),
        // Its point on surface (0, 1) is on the city's edge.
        (ring_polygon(&mut s, vec![box_deg(-1.0, -1.0, 1.0, 3.0)]), false),
    ];
    let (geom, polys) = s.geometry();
    assert_eq!(geom.point_on_surface(&polys[others[3].0 as usize]), Some((0.0, 1.0)));
    for (id, inside) in others {
        assert_eq!(geom.point_on_surface_in(&polys[id as usize], &polys[city as usize]), inside, "{id}");
    }
}

/// An index of `fx`'s polygons, each listed in the admin cells `cells` gives (cell, entry),
/// written as files into `dir`, with `names` its strings.
fn write_index(dir: &std::path::Path, fx: &ArcFixture, cells: &[(u64, u32)], names: &[u8]) {
    let (index, entries) = crate::cell_pairs::write_pairs(cells.iter().copied(), crate::cell_pairs::ADMIN);
    let end = names.len();
    let tiers: Vec<String> = (0..STR_TIER_COUNT).map(|t| format!("{{\"start\": {}, \"end\": {end}}}", if t == 0 { 0 } else { end })).collect();
    let mut files: Vec<(&str, Vec<u8>)> = fx.files().into();
    files.extend([
        ("admin_cells.bin", index), ("admin_entries.bin", entries), ("strings_core.bin", crate::strings::encode(names)),
        ("strings_layout.json", format!("{{\"tiers\": [{}]}}", tiers.join(", ")).into_bytes()),
    ]);
    for (name, bytes) in files {
        std::fs::write(dir.join(name), bytes).unwrap();
    }
}

fn load(dir: &std::path::Path) -> Result<Index, String> {
    Index::load(dir.to_str().unwrap(), DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE)
}

/// Testland: one level-2 square round (10, 20), 1° a side, in the admin cell of its centre.
fn testland() -> (ArcFixture, Vec<(u64, u32)>, Vec<u8>) {
    let mut fx = ArcFixture::new();
    let mut country = record(1, 2, 1.0);
    country.country_code = u16::from_be_bytes(*b"tl");
    let id = fx.ring_polygon(country, 0, &[box_deg(9.5, 19.5, 10.5, 20.5)]);
    (fx, vec![(cell_id_at_level(10.0, 20.0, DEFAULT_ADMIN_CELL_LEVEL), id)], b"\0Testland\0".to_vec())
}

#[test]
fn index_load_reads_the_arc_files_and_names_a_missing_one() {
    let (fx, cells, names) = testland();
    let dir = ScratchDir::new();
    write_index(dir.path(), &fx, &cells, &names);
    assert_eq!(load(dir.path()).unwrap().find_admin(10.0, 20.0).country.as_deref(), Some("Testland"));
    for name in ADMIN_GEOMETRY_FILES {
        let dir = ScratchDir::new();
        write_index(dir.path(), &fx, &cells, &names);
        std::fs::remove_file(dir.path().join(name)).unwrap();
        let Err(e) = load(dir.path()) else { panic!("loaded without {name}") };
        assert!(e.contains(name), "{e}");
    }
}

#[test]
fn an_admin_scan_looks_up_the_cell_of_the_rounded_point() {
    // A point within half an e7 unit of a level-10 cell's edge, on the other side of it from the
    // e7 point it rounds to.
    let lat = 22.002;
    let cell = |lng: f64| cell_id_at_level(lat, lng, DEFAULT_ADMIN_CELL_LEVEL);
    let (mut lo, mut hi) = (275_700_000i64, 277_700_000i64);
    assert_ne!(cell(lo as f64 / 1e7), cell(hi as f64 / 1e7));
    while hi - lo > 1 {
        let mid = (lo + hi) / 2;
        if cell(mid as f64 / 1e7) == cell(lo as f64 / 1e7) { lo = mid } else { hi = mid }
    }
    let (mut a, mut b) = (lo as f64 / 1e7, hi as f64 / 1e7);
    for _ in 0..100 {
        let mid = (a + b) / 2.0;
        if cell(mid) == cell(lo as f64 / 1e7) { a = mid } else { b = mid }
    }
    // a and b straddle the edge, a hair apart: the one rounding to the far side's e7 point.
    let raw = if crate::to_e7(b) == lo { b } else { a };
    let rounded = AdminPoint::new(lat, raw);
    assert_ne!(cell(raw), cell(rounded.degrees().1), "{raw} rounds to {:?}", rounded.e7());

    let mut fx = ArcFixture::new();
    let id = fx.ring_polygon(record(1, 8, 1.0), 0, &[box_deg(21.9, 27.5, 22.1, 27.8)]);
    let dir = ScratchDir::new();
    write_index(dir.path(), &fx, &[(cell(rounded.degrees().1), id)], b"\0Rounded\0");
    let idx = load(dir.path()).unwrap();
    let scan = AdminScan::new(&idx, lat, raw);
    assert_eq!(scan.pip.point, rounded);
    assert_eq!(scan.rings.iter().map(|(entry, _)| *entry).collect::<Vec<_>>(), [id]);
}

#[test]
fn has_rings_is_false_for_tombstones_empty_and_out_of_file_blocks() {
    let (fx, _, _) = testland();
    let [_, (_, rings), (_, index), (_, vertices)] = fx.files();
    let len = rings.len() as u32;
    let geom = AdminGeometry::new(FileBytes::Owned(rings), FileBytes::Owned(index), FileBytes::Owned(vertices));
    let live = fx.records()[0];
    let p = AdminPoint::new(10.0, 20.0);
    assert!(geom.has_rings(&live) && geom.contains_once(&live, p));
    for (what, poly) in [
        ("tombstone", AdminPolygon { ring_offset: NO_DATA, ring_bytes: 0, name_id: NO_DATA, ..live }),
        ("tombstone with bytes", AdminPolygon { ring_offset: NO_DATA, ..live }),
        ("empty", AdminPolygon { ring_bytes: 0, ..live }),
        ("past the end", AdminPolygon { ring_offset: len, ..live }),
        ("running past the end", AdminPolygon { ring_bytes: len + 1, ..live }),
        ("wrapping", AdminPolygon { ring_offset: u32::MAX - 1, ring_bytes: 4, ..live }),
    ] {
        assert!(!geom.has_rings(&poly), "{what}");
        assert!(!geom.contains_once(&poly, p) && !geom.contains(&poly, p, &mut PipMemo::new()), "{what}");
        assert!(geom.rings(&poly).is_empty(), "{what}");
        assert_eq!(geom.point_on_surface(&poly), None, "{what}");
        assert!(!geom.point_on_surface_in(&poly, &live) && !geom.point_on_surface_in(&live, &poly), "{what}");
        geom.prefetch(&poly);
    }
}

#[test]
fn malformed_blocks_answer_false_and_no_rings() {
    let mut s = Shapes::default();
    let good = ring_polygon(&mut s, vec![box_deg(0.0, 0.0, 1.0, 1.0)]);
    let dead = s.fx.dead_arc();
    let to_dead = s.polygon(8, &[vec![Ref::fwd(dead)]], vec![box_deg(0.0, 0.0, 1.0, 1.0)]);
    let to_none = s.polygon(8, &[vec![Ref::fwd(1000)]], vec![box_deg(0.0, 0.0, 1.0, 1.0)]);
    let torn = s.fx.arc_block(vec![0, 1, 2], crate::arc_fixture::RawBox::EMPTY);
    let to_torn = s.polygon(8, &[vec![Ref::fwd(torn)]], vec![box_deg(0.0, 0.0, 1.0, 1.0)]);
    let (geom, polys) = s.geometry();
    let p = AdminPoint::new(0.5, 0.5);
    assert!(geom.contains_once(&polys[good as usize], p));
    for id in [to_dead, to_none, to_torn] {
        let poly = &polys[id as usize];
        assert!(geom.has_rings(poly));
        assert!(!geom.contains_once(poly, p) && !geom.contains(poly, p, &mut PipMemo::new()), "{id}");
        assert!(geom.rings(poly).is_empty() && geom.point_on_surface(poly).is_none(), "{id}");
    }
    // A ring block cut short.
    let cut = AdminPolygon { ring_bytes: polys[good as usize].ring_bytes - 1, ..polys[good as usize] };
    assert!(!geom.contains_once(&cut, p) && geom.rings(&cut).is_empty());
}

#[test]
fn the_block_memo_answers_each_ring_block_once() {
    let mut memo = BlockMemo::default();
    assert!(memo.slots.is_empty());
    assert!(memo.get_or_insert_with(7, || true));
    // Asked again, the first answer stands: nothing is tested.
    assert!(memo.get_or_insert_with(7, || panic!("tested twice")));
    assert!(!memo.get_or_insert_with(8, || false));
    assert_eq!(memo.len, 2);
}

#[test]
fn the_block_memo_keeps_its_answers_as_it_grows() {
    // 300 squares, every third holding the point: the memo grows from 64 slots past 512.
    let mut s = Shapes::default();
    for i in 0..300 {
        let lat0 = if i % 3 == 0 { -5000 } else { 5000 } + i;
        ring_polygon(&mut s, vec![vec![pt(lat0, -5000), pt(lat0, 5000), pt(lat0 + 10_000, 5000), pt(lat0 + 10_000, -5000), pt(lat0, -5000)]]);
    }
    let (geom, polys) = s.geometry();
    let p = AdminPoint(pt(2500, 0));
    let mut memo = PipMemo::new();
    for pass in 0..2 {
        for (i, poly) in polys.iter().enumerate() {
            assert_eq!(geom.contains(poly, p, &mut memo), i % 3 == 0, "pass {pass} ring {i}");
        }
    }
    assert_eq!(memo.blocks.len, 300);
    assert!(memo.blocks.slots.len() >= 600);
}

#[test]
fn a_long_ring_answers_through_its_chunk_boxes() {
    // 100 arcs round a square: chunks with boxes, most skipped from a point near one side.
    let mut s = Shapes::default();
    let (refs, ring) = square_of_arcs(&mut s.fx, 0, 1_000_000, 25, 5);
    let id = s.polygon(8, &[refs], vec![ring.clone()]);
    let (geom, polys) = s.geometry();
    let mut rng = Rng::new(2);
    let points = probes(&mut rng, &ring, 3000);
    let mut read = 0;
    for &q in &points {
        let mut memo = PipMemo::new();
        assert_eq!(geom.contains(&polys[id as usize], AdminPoint(q), &mut memo), brute(&[ring.clone()], q), "{q:?}");
        read += memo.arcs_read() as usize;
    }
    assert!(read < points.len() * 40, "{read} arcs read at {} points", points.len());
}

#[test]
fn point_on_surface_in_answers_from_its_memo_as_from_the_passes() {
    let mut rng = Rng::new(43);
    let l = lattice(&mut rng, (3, 3), 200_000, 40, &[0, 2, 3], true);
    let (geom, polys) = l.shapes.geometry();
    let ids: Vec<u32> = l.polygons().collect();
    let passes = |id: u32| geom.surface_point(geom.ring_block(&polys[id as usize]).unwrap(), &mut Vec::new());
    let inside = |id: u32, other: u32| {
        let Some((lat, lng)) = passes(id) else { return false };
        let mut test = InsideOffEdges { arcs: geom.arcs(), p: AdminPoint::new(lat, lng).e7(), odd: false, on_edge: false };
        walk_rings(geom.ring_block(&polys[other as usize]).unwrap(), &mut test).is_some() && test.odd && !test.on_edge
    };
    // Asked twice, the second time from the memo.
    let mut held = 0;
    for round in 0..2 {
        for &a in &ids {
            for &b in &ids {
                assert_eq!(geom.point_on_surface_in(&polys[a as usize], &polys[b as usize]), inside(a, b), "round {round} {a} in {b}");
            }
        }
    }
    let memo = geom.surfaces.lock().unwrap();
    for &a in &ids {
        if let Some(point) = memo.get(AdminGeometry::block_key(&polys[a as usize])) {
            assert_eq!(point, passes(a), "{a}");
            held += 1;
        }
    }
    assert!(held > ids.len() / 2, "{held} of {} points on surface held", ids.len());
    let (insides, mut held) = (geom.insides.lock().unwrap(), 0);
    for &a in &ids {
        for &b in &ids {
            let key = |id: u32| AdminGeometry::block_key(&polys[id as usize]);
            if let Some(answer) = insides.get(key(a), key(b)) {
                assert_eq!(answer, inside(a, b), "{a} in {b}");
                held += 1;
            }
        }
    }
    assert!(held > ids.len() * ids.len() / 4, "{held} of {} answers held", ids.len() * ids.len());
}

#[test]
fn the_surface_memo_keeps_the_last_answer_of_each_slot() {
    let mut memo = SurfaceMemo::default();
    assert_eq!(memo.get(5), None);
    assert!(memo.slots.is_empty());
    memo.insert(5, Some((1.0, 2.0)));
    memo.insert(6, None);
    assert_eq!((memo.get(5), memo.get(6), memo.get(7)), (Some(Some((1.0, 2.0))), Some(None), None));
    assert_eq!(memo.slots.len(), SurfaceMemo::SLOTS);
    // A key of 5's slot takes it over.
    let other = (6..).find(|&k| SurfaceMemo::slot(k) == SurfaceMemo::slot(5)).unwrap();
    memo.insert(other, Some((3.0, 4.0)));
    assert_eq!((memo.get(5), memo.get(other)), (None, Some(Some((3.0, 4.0)))));
}

#[test]
fn the_inside_memo_keeps_the_last_answer_of_each_slot() {
    let mut memo = InsideMemo::default();
    assert_eq!(memo.get(5, 6), None);
    assert!(memo.slots.is_empty());
    memo.insert(5, 6, true);
    memo.insert(6, 5, false);
    assert_eq!((memo.get(5, 6), memo.get(6, 5), memo.get(5, 5)), (Some(true), Some(false), None));
    assert_eq!(memo.slots.len(), InsideMemo::SLOTS);
    // A pair of (5, 6)'s slot takes it over.
    let other = (7..).find(|&k| InsideMemo::slot(k, 6) == InsideMemo::slot(5, 6)).unwrap();
    memo.insert(other, 6, false);
    assert_eq!((memo.get(5, 6), memo.get(other, 6)), (None, Some(false)));
}
