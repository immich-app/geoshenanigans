use super::*;
use crate::arc_blocks::tests::{odd_crossings, probes, square_of_arcs};
use crate::arc_blocks::ARC_MEMO_CAPACITY;
use crate::arc_fixture::{e7, exact_area, record, ArcFixture, Ref};
use crate::test_rng::Rng;
use crate::exact_pip::tests::testdata_rows;
use crate::scratch_dir::ScratchDir;
use crate::{cell_id_at_level, AdminScan, Index, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE, DEFAULT_STREET_CELL_LEVEL, NO_DATA, STR_TIER_COUNT};

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
    assert_eq!(geom.rings(&polys[holed as usize]).len(), 2);
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

use crate::test_heap as heap;

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
fn a_centroid_is_the_outer_ring_s_less_its_holes_whichever_way_the_arcs_run() {
    // ST_Centroid of a 10 by 10 degree square less a hole from 6 to 8: the square's centre (5, 5)
    // weighted by its area, 100, less the hole's (7, 7) by its 4, over 96. Each side an arc of
    // its own, and once with a side of each ring stored the other way round, which the ring
    // walks backwards.
    let want = (100.0 * 5.0 - 4.0 * 7.0) / 96.0;
    let corners = |lo: f64, hi: f64| [(lo, lo), (lo, hi), (hi, hi), (hi, lo), (lo, lo)];
    for reversed in [false, true] {
        let mut s = Shapes::default();
        let sides = |fx: &mut ArcFixture, ring: [(f64, f64); 5], back: usize| -> Vec<Ref> {
            (0..4).map(|k| {
                if reversed && k == back {
                    Ref::rev(fx.arc(0, &ring_deg(&[ring[k + 1], ring[k]])))
                } else {
                    Ref::fwd(fx.arc(0, &ring_deg(&[ring[k], ring[k + 1]])))
                }
            }).collect()
        };
        let (outer, hole) = (corners(0.0, 10.0), corners(6.0, 8.0));
        let refs = [sides(&mut s.fx, outer, 1), sides(&mut s.fx, hole, 2)];
        let holed = s.polygon(8, &refs, vec![ring_deg(&outer), ring_deg(&hole)]);
        let refs = [sides(&mut s.fx, outer, 3)];
        let square = s.polygon(8, &refs, vec![ring_deg(&outer)]);
        let (geom, polys) = s.geometry();
        let (lat, lng) = geom.centroid(&polys[holed as usize]).unwrap();
        assert!((lat - want).abs() < 1e-9 && (lng - want).abs() < 1e-9, "reversed {reversed}: ({lat}, {lng})");
        let (lat, lng) = geom.centroid(&polys[square as usize]).unwrap();
        assert!((lat - 5.0).abs() < 1e-9 && (lng - 5.0).abs() < 1e-9, "reversed {reversed}: ({lat}, {lng})");
    }
}

#[test]
fn one_outline_is_the_same_whichever_vertex_and_way_round_its_ring_starts() {
    // A rectangle, the same written from another vertex the other way round, and rectangles of
    // its size beside it, a hair off and turned round its centre, each with the area the builder
    // stores: only the first two have one outline.
    let mut s = Shapes::default();
    let square = ring_polygon(&mut s, vec![box_deg(47.1, 8.3, 47.4, 8.7)]);
    let again = ring_polygon(&mut s, vec![ring_deg(&[(47.4, 8.7), (47.1, 8.7), (47.1, 8.3), (47.4, 8.3), (47.4, 8.7)])]);
    let beside = ring_polygon(&mut s, vec![box_deg(47.2, 8.3, 47.5, 8.7)]);
    let off = ring_polygon(&mut s, vec![box_deg(47.1, 8.3000001, 47.4, 8.7000001)]);
    let turned = ring_polygon(&mut s, vec![box_deg(47.05, 8.35, 47.45, 8.65)]);
    // The rectangle with a vertex more halfway along its southern edge, in two arcs.
    let (south, rest) = (ring_deg(&[(47.1, 8.3), (47.1, 8.5), (47.1, 8.7)]), ring_deg(&[(47.1, 8.7), (47.4, 8.7), (47.4, 8.3), (47.1, 8.3)]));
    let cut = {
        let refs = vec![Ref::fwd(s.fx.arc(0, &south)), Ref::fwd(s.fx.arc(0, &rest))];
        let ring: Vec<PointE7> = south.iter().chain(&rest[1..]).copied().collect();
        s.polygon(8, &[refs], vec![ring])
    };
    let ringless = s.polygon(8, &[], vec![]);
    let (geom, mut polys) = s.geometry();
    for id in [beside, off] {
        polys[id as usize].area = polys[square as usize].area;
    }
    assert_eq!(polys[turned as usize].area, polys[square as usize].area);
    let same = |a: u32, b: u32| geom.same_outline(&polys[a as usize], &polys[b as usize]);
    assert_eq!(polys[cut as usize].area, polys[square as usize].area);
    assert!(same(square, square) && same(square, again) && same(again, square) && same(square, cut) && same(cut, again));
    assert!(!same(square, beside) && !same(square, off) && !same(square, turned) && !same(ringless, ringless));
    // Of another area, the same ring is another outline.
    polys[again as usize].area *= 2.0;
    assert!(!geom.same_outline(&polys[square as usize], &polys[again as usize]));
}

#[test]
fn the_memoised_point_on_surface_is_the_builder_s_at_full_precision() {
    // A rank's areas are taken by their centres' distances: a box 3e-7 degrees wide at lng 100
    // has its middle between two f32 values, which two such boxes side by side would share.
    let mut s = Shapes::default();
    let narrow = ring_polygon(&mut s, vec![box_deg(10.0, 100.000_000_1, 10.2, 100.000_000_4)]);
    let (geom, polys) = s.geometry();
    let exact = interior_point(&s.rings[narrow as usize]);
    assert_ne!(f64::from(exact.1 as f32), exact.1);
    assert_eq!(geom.point_on_surface_memo(&polys[narrow as usize]), Some(exact));
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

/// Whether the edges a-b and c-d cross at a point inside both: the ends of each on either side
/// of the other's line, by the signs of their cross products.
fn cross_inside_brute(a: PointE7, b: PointE7, c: PointE7, d: PointE7) -> bool {
    let cross = |p: PointE7, q: PointE7, r: PointE7| (q.lng - p.lng) as i128 * (r.lat - p.lat) as i128 - (q.lat - p.lat) as i128 * (r.lng - p.lng) as i128;
    let apart = |x: i128, y: i128| (x < 0 && y > 0) || (x > 0 && y < 0);
    apart(cross(a, b, c), cross(a, b, d)) && apart(cross(c, d, a), cross(c, d, b))
}

/// A point at twice the coordinates whose longitude is a fraction: (lat, num, den), the
/// longitude num / den, den > 0.
type Exact2 = (i64, i128, i128);

/// The point on surface of `rings` as given, exact at twice the coordinates, from `surface2`,
/// surface_point's there: the middle of the widest run between the rings' crossings of the line
/// of latitude `surface2.lat`, the first of equal widths; `surface2` where a vertex lies on the
/// line or no run has width.
fn exact_surface_brute(rings: &[Vec<PointE7>], surface2: PointE7) -> Exact2 {
    let (scan, on_grid) = (surface2.lat, (surface2.lat, surface2.lng as i128, 1));
    let mut crossings: Vec<(i128, i128)> = Vec::new();
    for e in rings.iter().flat_map(|r| r.windows(2)) {
        let (a, b) = (pt(2 * e[0].lat, 2 * e[0].lng), pt(2 * e[1].lat, 2 * e[1].lng));
        if a.lat == scan || b.lat == scan {
            return on_grid;
        }
        if (a.lat < scan) != (b.lat < scan) {
            // a.lng + (scan - a.lat) * (b.lng - a.lng) / (b.lat - a.lat), over a positive divisor.
            let (n, d) = (a.lng as i128 * (b.lat - a.lat) as i128 + (scan - a.lat) as i128 * (b.lng - a.lng) as i128, (b.lat - a.lat) as i128);
            crossings.push(if d < 0 { (-n, -d) } else { (n, d) });
        }
    }
    crossings.sort_by(|x, y| (x.0 * y.1).cmp(&(y.0 * x.1)));
    let mut best = (0.0, on_grid);
    for run in crossings.chunks_exact(2) {
        let ((n1, d1), (n2, d2)) = (run[0], run[1]);
        let width = (n2 * d1 - n1 * d2) as f64 / (d1 * d2) as f64;
        if width > best.0 {
            best = (width, (scan, n1 * d2 + n2 * d1, 2 * d1 * d2));
        }
    }
    best.1
}

/// Whether the point `p` lies inside the rings `doubled` off their edges, both at twice the
/// coordinates: the ray cast towards +lat of `brute` and `on_edge`, its products in fractions
/// over `p`'s divisor, each checked.
fn exact_inside_off_edges(doubled: &[Vec<PointE7>], (lat, num, den): Exact2) -> bool {
    let m = |x: i128, y: i128| x.checked_mul(y).expect("the product fits in i128");
    let mut odd = false;
    for e in doubled.iter().flat_map(|r| r.windows(2)) {
        let (a, b) = (e[0], e[1]);
        // (lat - a.lat) * (b.lng - a.lng) - (b.lat - a.lat) * (num / den - a.lng), times den.
        let side = m(m((lat - a.lat) as i128, (b.lng - a.lng) as i128), den) - m((b.lat - a.lat) as i128, num - m(a.lng as i128, den));
        let (lng0, lng1) = (a.lng.min(b.lng) as i128, a.lng.max(b.lng) as i128);
        if side == 0 && (a.lat.min(b.lat)..=a.lat.max(b.lat)).contains(&lat) && m(lng0, den) <= num && num <= m(lng1, den) {
            return false;
        }
        let east = |c: i64| m(c as i128, den) > num;
        odd ^= east(a.lng) != east(b.lng) && side != 0 && ((side < 0) != (b.lng < a.lng));
    }
    odd
}

/// polygon_within over rings as given, every edge against every edge: each vertex of `rings`
/// inside `other` or on its edges, no edge of theirs crossing one of `other`'s inside both, no
/// point of `other`'s edges inside them off their edges, and `surface`, their exact point on
/// surface at twice the coordinates, inside `other` off its edges. An edge of `other`, cut at the
/// vertices of `rings` on it, meets their edges only at the ends of its pieces, so each piece is
/// inside them where its middle is: tested at twice the coordinates, where the middle of two e7
/// points is on the grid.
fn within_brute(rings: &[Vec<PointE7>], other: &[Vec<PointE7>], surface: Exact2) -> bool {
    let edges = |rings: &[Vec<PointE7>]| -> Vec<(PointE7, PointE7)> { rings.iter().flat_map(|r| r.windows(2).map(|e| (e[0], e[1]))).collect() };
    let (own, others) = (edges(rings), edges(other));
    let on = |edges: &[(PointE7, PointE7)], p: PointE7| edges.iter().any(|&(a, b)| on_edge(a, b, p));
    let twice = |p: PointE7| pt(2 * p.lat, 2 * p.lng);
    let doubled = |rings: &[Vec<PointE7>]| -> Vec<Vec<PointE7>> { rings.iter().map(|r| r.iter().map(|&v| twice(v)).collect()).collect() };
    let (doubled, doubled_other) = (doubled(rings), doubled(other));
    let doubled_edges = edges(&doubled);
    let inside_off_edges = |p2: PointE7| brute(&doubled, p2) && !on(&doubled_edges, p2);
    let enters = |&(a, b): &(PointE7, PointE7)| {
        let along = |v: &PointE7| (v.lat - a.lat) as i128 * (b.lat - a.lat) as i128 + (v.lng - a.lng) as i128 * (b.lng - a.lng) as i128;
        let mut cuts: Vec<PointE7> = rings.iter().flatten().copied().filter(|&v| on_edge(a, b, v)).chain([a, b]).collect();
        cuts.sort_by_key(along);
        cuts.windows(2).any(|w| inside_off_edges(pt(w[0].lat + w[1].lat, w[0].lng + w[1].lng)))
    };
    rings.iter().flatten().all(|&v| brute(other, v) || on(&others, v))
        && !own.iter().any(|&(a, b)| others.iter().any(|&(c, d)| cross_inside_brute(a, b, c, d)))
        && !other.iter().flatten().any(|&v| inside_off_edges(twice(v)))
        && !others.iter().any(enters)
        && exact_inside_off_edges(&doubled_other, surface)
}

#[test]
fn a_polygon_is_within_another_only_where_no_part_of_it_is_outside() {
    // ST_Contains: no point of the polygon outside the other. Round a 0..10 square in degrees:
    // the square, with a hole from 4 to 6, and notched from its top edge down to (5, 5).
    let mut s = Shapes::default();
    let outer = box_deg(0.0, 0.0, 10.0, 10.0);
    let square = ring_polygon(&mut s, vec![outer.clone()]);
    let holed = ring_polygon(&mut s, vec![outer.clone(), box_deg(4.0, 4.0, 6.0, 6.0)]);
    let notched = ring_polygon(&mut s, vec![ring_deg(&[(0.0, 0.0), (0.0, 10.0), (10.0, 10.0), (10.0, 6.0), (5.0, 5.0), (10.0, 4.0), (10.0, 0.0), (0.0, 0.0)])]);
    // Round the hole and the notch's tip; beside both.
    let inner = ring_polygon(&mut s, vec![box_deg(2.0, 2.0, 8.0, 8.0)]);
    let corner = ring_polygon(&mut s, vec![box_deg(1.0, 1.0, 3.0, 3.0)]);
    // Across the notch above its tip: every vertex inside the notched square and none of its
    // inside this, but the edges cross.
    let across = ring_polygon(&mut s, vec![box_deg(7.0, 1.0, 9.0, 9.0)]);
    // Along the square's edges, the western half; the hole's outline, which fills it.
    let half = ring_polygon(&mut s, vec![box_deg(0.0, 0.0, 10.0, 5.0)]);
    let enclave = ring_polygon(&mut s, vec![box_deg(4.0, 4.0, 6.0, 6.0)]);
    let past = ring_polygon(&mut s, vec![box_deg(5.0, 5.0, 15.0, 15.0)]);
    // A larger square with a triangular hole whose corners all lie on the 0..10 square's edges:
    // the hole is inside that square, its edges run through it from edge to edge. And one whose
    // hole touches the square's southern edge from outside.
    let larger = box_deg(-5.0, -5.0, 15.0, 15.0);
    let chords = ring_polygon(&mut s, vec![larger.clone(), ring_deg(&[(0.0, 2.0), (0.0, 4.0), (2.0, 0.0), (0.0, 2.0)])]);
    let touching = ring_polygon(&mut s, vec![larger.clone(), ring_deg(&[(0.0, 2.0), (0.0, 4.0), (-2.0, 3.0), (0.0, 2.0)])]);
    let ringless = s.polygon(8, &[], vec![]);
    let (geom, polys) = s.geometry();
    let within = |a: u32, b: u32| geom.polygon_within(&polys[a as usize], &polys[b as usize]);
    for (a, b, want) in [
        (inner, square, true), (inner, holed, false), (inner, notched, false),
        (corner, square, true), (corner, holed, true), (corner, notched, true),
        (across, square, true), (across, notched, false),
        (half, square, true), (half, holed, false), (half, notched, false),
        (enclave, square, true), (enclave, holed, false),
        (holed, square, true), (square, holed, false), (notched, square, true), (square, notched, false),
        (square, square, true), (past, square, false), (square, inner, false),
        (square, chords, false), (square, touching, true), (corner, chords, false), (inner, chords, true),
        (ringless, square, false), (square, ringless, false),
    ] {
        assert_eq!(within(a, b), want, "{a} in {b}");
        let surface = geom.ring_block(&polys[a as usize]).and_then(|block| geom.surface_point(block, &mut Vec::new()));
        if let Some((lat, lng)) = surface.filter(|_| b != ringless) {
            let surface = exact_surface_brute(&s.rings[a as usize], twice_surface(lat, lng));
            assert_eq!(within_brute(&s.rings[a as usize], &s.rings[b as usize], surface), want, "{a} in {b}");
        }
    }
}

#[test]
fn a_polygon_is_within_one_along_its_edge_by_its_point_on_surface_off_the_grid() {
    // A triangle a few e7 units across, and one larger along its edge from (0, 2) to (3, 0):
    // its point on surface, the middle of its widest run at latitude 1.5, is (1.5, 0.5), which
    // the grid would round to (2, 1), past that edge.
    let mut s = Shapes::default();
    let triangle = ring_polygon(&mut s, vec![vec![pt(0, 0), pt(0, 2), pt(3, 0), pt(0, 0)]]);
    let larger = ring_polygon(&mut s, vec![vec![pt(0, -1), pt(0, 2), pt(3, 0), pt(3, -1), pt(0, -1)]]);
    let (geom, polys) = s.geometry();
    let within = |a: u32, b: u32| geom.polygon_within(&polys[a as usize], &polys[b as usize]);
    assert!(within(triangle, larger) && within(triangle, triangle) && within(larger, larger));
    assert!(!within(larger, triangle));
}

#[test]
fn a_sliver_is_within_one_along_both_its_sides_by_its_exact_point_on_surface() {
    // Triangles whose run at the scan line is half an e7 unit wide or less, each inside a larger
    // one along both its sloped sides: at twice the coordinates the middle of the run still
    // rounds onto one of those sides. A triangle 50000 units tall, its run at latitude 25000 from
    // 1 to 1.5; one 1000 units tall, its run at 500.5 from 501 to 501.0005. Each near the origin,
    // and near the poles and the antimeridian, where the products are largest.
    for (lat0, lng0) in [(0, 0), (899_000_000, 1_799_000_000), (-899_000_000, -1_799_000_000)] {
        let ring = |v: &[(i64, i64)]| -> Vec<PointE7> { v.iter().map(|&(lat, lng)| pt(lat0 + lat, lng0 + lng)).collect() };
        let mut s = Shapes::default();
        let sliver = ring(&[(0, 0), (1, 1), (1000, 1001), (0, 0)]);
        let pairs = [
            (ring_polygon(&mut s, vec![ring(&[(0, 0), (0, 1), (50000, 2), (0, 0)])]), ring_polygon(&mut s, vec![ring(&[(-50000, -2), (-50000, 0), (50000, 2), (-50000, -2)])])),
            (ring_polygon(&mut s, vec![sliver.clone()]), ring_polygon(&mut s, vec![ring(&[(-1000, -1001), (-998, -999), (1000, 1001), (-1000, -1001)])])),
        ];
        // A square with the sliver as its hole, which the sliver fills.
        let square = ring(&[(-5000, -5000), (-5000, 5000), (5000, 5000), (5000, -5000), (-5000, -5000)]);
        let holed = ring_polygon(&mut s, vec![square.clone(), sliver.iter().rev().copied().collect()]);
        let whole = ring_polygon(&mut s, vec![square]);
        let (geom, polys) = s.geometry();
        let within = |a: u32, b: u32| geom.polygon_within(&polys[a as usize], &polys[b as usize]);
        for (child, parent) in pairs {
            assert!(within(child, parent) && within(child, child), "({lat0}, {lng0}): {child} in {parent}");
            assert!(!within(parent, child), "({lat0}, {lng0}): {parent} in {child}");
        }
        let sliver = pairs[1].0;
        assert!(within(sliver, whole) && within(holed, whole), "({lat0}, {lng0})");
        assert!(!within(sliver, holed) && !within(holed, sliver), "({lat0}, {lng0})");
        // So do the rings as given, every edge against every edge.
        for (a, b, want) in [(pairs[0].0, pairs[0].1, true), (sliver, pairs[1].1, true), (sliver, holed, false)] {
            let (lat, lng) = geom.surface_point(geom.ring_block(&polys[a as usize]).unwrap(), &mut Vec::new()).unwrap();
            let surface = exact_surface_brute(&s.rings[a as usize], twice_surface(lat, lng));
            assert_eq!(within_brute(&s.rings[a as usize], &s.rings[b as usize], surface), want, "({lat0}, {lng0}): {a} in {b}");
        }
    }
}

#[test]
fn polygon_within_answers_as_testing_every_edge_against_every_edge() {
    // Lattices of cells sharing their edges' arcs with their blocks and the whole, some cells
    // with a hole an island fills, edges of every grid read either way round.
    let mut rng = Rng::new(20261123);
    for (dims, vertices, grids) in [((3, 3), 40, &[0u8][..]), ((2, 2), 3, &[0, 2, 3][..]), ((4, 4), 12, &[2][..])] {
        let l = lattice(&mut rng, dims, 200_000, vertices, grids, true);
        let (geom, polys) = l.shapes.geometry();
        let ids: Vec<u32> = l.polygons().collect();
        let within = |a: u32, b: u32| geom.polygon_within(&polys[a as usize], &polys[b as usize]);
        let mut held = 0;
        for &a in &ids {
            let (lat, lng) = geom.surface_point(geom.ring_block(&polys[a as usize]).unwrap(), &mut Vec::new()).unwrap();
            let rings = &l.shapes.rings[a as usize];
            let surface = exact_surface_brute(rings, twice_surface(lat, lng));
            let doubled: Vec<Vec<PointE7>> = rings.iter().map(|r| r.iter().map(|v| pt(2 * v.lat, 2 * v.lng)).collect()).collect();
            assert!(exact_inside_off_edges(&doubled, surface), "{dims:?} {grids:?} {a}");
            for &b in &ids {
                let want = within_brute(rings, &l.shapes.rings[b as usize], surface);
                held += want as usize;
                assert_eq!(within(a, b), want, "{dims:?} {grids:?} {a} in {b}");
            }
        }
        // Each within itself and the whole, the cells of a block within it, and no island
        // within a cell, as the one round it has the island's outline as a hole.
        assert!(ids.iter().all(|&a| within(a, a) && within(a, l.whole)), "{dims:?}");
        let cols = dims.1;
        let corners = (0..dims.0.saturating_sub(1)).step_by(2).flat_map(|i| (0..cols.saturating_sub(1)).step_by(2).map(move |j| (i, j)));
        for (&block, (i, j)) in l.blocks.iter().zip(corners) {
            for (di, dj) in [(0, 0), (0, 1), (1, 0), (1, 1)] {
                assert!(within(l.cells[(i + di) * cols + j + dj], block), "{dims:?} cell ({}, {}) in its block", i + di, j + dj);
            }
        }
        assert!(l.islands.iter().all(|&island| l.cells.iter().all(|&cell| !within(island, cell))), "{dims:?}");
        assert!(!l.islands.is_empty() && held > ids.len() * 2, "{dims:?}");
    }
}

#[test]
fn polygon_within_reads_each_arc_once_then_answers_from_its_memo() {
    // A square of 8 arcs and 393 vertices inside a square of one arc: its arcs are read for its
    // rings and its point on surface, the other's once, not once a vertex.
    let mut s = Shapes::default();
    let (refs, ring) = square_of_arcs(&mut s.fx, 0, 1_000_000, 2, 50);
    let inner = s.polygon(8, &[refs], vec![ring]);
    let outer = ring_polygon(&mut s, vec![ring_deg(&[(-0.01, -0.01), (-0.01, 0.11), (0.11, 0.11), (0.11, -0.01), (-0.01, -0.01)])]);
    let (geom, polys) = s.geometry();
    let decodes = || crate::arc_blocks::ARC_DECODES.with(|n| n.get());
    let before = decodes();
    assert!(geom.polygon_within(&polys[inner as usize], &polys[outer as usize]));
    assert!(decodes() - before <= 3 * 8 + 1, "{} arcs read", decodes() - before);
    let before = decodes();
    assert!(geom.polygon_within(&polys[inner as usize], &polys[outer as usize]));
    assert_eq!(decodes(), before);
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
    assert_eq!(load(dir.path()).unwrap().find_admin(10.0, 20.0).row("country").map(|row| row.name.to_string()).as_deref(), Some("Testland"));
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
        ("tombstone", AdminPolygon { ring_bytes: 0, name_id: NO_DATA, ..live }),
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
