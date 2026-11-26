//! geocoder-transport against a fixture dir of every kind's members.
use query_server::arc_fixture::ArcFixture;
use query_server::poi_files::PoiPolygon;
use query_server::transport::Kind;
use query_server::{place_files, poi_files, PlaceNode, PoiRecord, PointE7, DEFAULT_ADMIN_CELL_LEVEL, NO_DATA, PLACE_FLAG_HIDDEN};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};

/// A fresh directory under the temp dir, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        static DIRS: AtomicUsize = AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!("gt-{tag}-{}-{}", std::process::id(), DIRS.fetch_add(1, Ordering::Relaxed)));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        TempDir(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn run(args: &[&str], stdin: Option<&[u8]>) -> Output {
    let mut c = Command::new(env!("CARGO_BIN_EXE_geocoder-transport"));
    c.args(args).stdin(if stdin.is_some() { Stdio::piped() } else { Stdio::null() }).stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = c.spawn().expect("run geocoder-transport");
    if let Some(b) = stdin {
        child.stdin.take().unwrap().write_all(b).unwrap();
    }
    child.wait_with_output().unwrap()
}

/// A dir holding a member set of every kind: places, POIs with polygons, arcs with a dead slot,
/// admin polygons.
fn fixture_dir() -> TempDir {
    let d = TempDir::new("dir");
    let nodes: Vec<PlaceNode> = (0..200u32)
        .map(|i| PlaceNode {
            lat: -60.0 + i as f32 * 0.6,
            lng: 170.0 - i as f32 * 1.7,
            name_id: i * 5,
            place_type: (i % 7) as u8,
            _pad1: if i % 10 == 3 { PLACE_FLAG_HIDDEN } else { 0 },
            rank_address: if i % 10 == 3 { 0 } else { query_server::PLACE_NODE_RANK[(i % 7) as usize] },
            parent_poly_id: NO_DATA,
        })
        .collect();
    let (places, _) = place_files::encode(&nodes, DEFAULT_ADMIN_CELL_LEVEL);
    let poi = |i: u32| PoiRecord {
        lat: 47.0 + i as f32 * 0.01,
        lng: 8.0 - i as f32 * 0.01,
        vertex_offset: NO_DATA,
        name_id: i,
        category: (i % 5) as u8,
        tier: 1,
        flags: 0,
        importance: (i % 3) as u8,
        parent_street_id: NO_DATA,
        parent_postcode_id: if i % 2 == 0 { 40 } else { NO_DATA },
        parent_poly_id: i,
    };
    let pois: Vec<(PoiRecord, Option<PoiPolygon>)> = (0..300u32)
        .map(|i| {
            let polygon = (i % 4 == 1).then(|| PoiPolygon {
                encoding: (i / 4 % 4) as u8,
                bmin_lat: 46.9,
                bmin_lng: 7.9,
                q: (0..3 + i % 20).map(|k| (k * 37 % 1000, k * 91 % 700)).collect(),
            });
            (poi(i), polygon)
        })
        .collect();
    let (records, vertices) = poi_files::encode(&pois);
    let mut fx = ArcFixture::new();
    for i in 0..50i64 {
        if i % 9 == 4 {
            fx.dead_arc();
        }
        let v: Vec<PointE7> = (0..2 + i * 3).map(|k| PointE7 { lat: 1000 * i + 7 * k, lng: -500 * i + 13 * k * (k % 3) }).collect();
        fx.arc(0, &v);
    }
    let polygons: Vec<u8> = (0..100u32).flat_map(|i| [i * 48, i * 3, (0.25 * i as f32).to_bits(), 528 | 8 << 20].into_iter().flat_map(u32::to_le_bytes)).collect();
    for (name, bytes) in [
        ("place_cells.bin", places.cells),
        ("place_nodes.bin", places.nodes),
        ("place_hidden.bin", places.hidden),
        ("poi_records.bin", records),
        ("poi_vertices.bin", vertices),
        ("arc_index.bin", fx.arc_index()),
        ("arc_vertices.bin", fx.arc_vertices()),
        ("admin_polygons.bin", polygons),
    ] {
        std::fs::write(d.0.join(name), bytes).unwrap();
    }
    d
}

fn units() -> Vec<(Kind, Option<&'static str>)> {
    vec![(Kind::Places, None), (Kind::Pois, None), (Kind::Arcs, None), (Kind::Cols, Some("admin_polygons.bin"))]
}

fn members(kind: Kind, file: Option<&'static str>) -> Vec<&'static str> {
    file.map_or_else(|| kind.members().to_vec(), |f| vec![f])
}

/// The unit `encode` writes for kind (of `file`), zstd-compressed.
fn unit(dir: &Path, kind: Kind, file: Option<&str>) -> Vec<u8> {
    let mut args = vec!["encode", kind.name(), dir.to_str().unwrap()];
    args.extend(file);
    let out = run(&args, None);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    assert_eq!(&out.stdout[..4], b"GTT1");
    zstd::encode_all(&out.stdout[..], 3).unwrap()
}

#[test]
fn a_unit_decodes_from_a_file_and_from_stdin_to_its_members_and_verifies() {
    let dir = fixture_dir();
    for (kind, file) in units() {
        let zst = unit(&dir.0, kind, file);
        let path = dir.0.join(format!("{}.tt.zst", kind.name()));
        std::fs::write(&path, &zst).unwrap();
        for (source, stdin) in [(path.to_str().unwrap(), None), ("-", Some(&zst[..]))] {
            let out_dir = TempDir::new("out");
            let out = run(&["decode", source, out_dir.0.to_str().unwrap()], stdin);
            assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
            assert!(String::from_utf8_lossy(&out.stdout).starts_with(&format!("decoded {} unit: ", kind.name())));
            let mut written: Vec<String> = std::fs::read_dir(&out_dir.0).unwrap().map(|e| e.unwrap().file_name().into_string().unwrap()).collect();
            written.sort();
            let mut want: Vec<String> = members(kind, file).iter().map(|m| m.to_string()).collect();
            want.sort();
            assert_eq!(written, want);
            for m in members(kind, file) {
                assert!(std::fs::read(out_dir.0.join(m)).unwrap() == std::fs::read(dir.0.join(m)).unwrap(), "{m}");
            }
        }
        // verify writes nothing to the dir it reads, a places unit's block table included.
        let scratch = TempDir::new("verify-tmp");
        let listing = |d: &Path| {
            let mut n: Vec<_> = std::fs::read_dir(d).unwrap().map(|e| e.unwrap().file_name()).collect();
            n.sort();
            n
        };
        let before = listing(&dir.0);
        let out = Command::new(env!("CARGO_BIN_EXE_geocoder-transport")).args(["verify", path.to_str().unwrap(), dir.0.to_str().unwrap()]).env("TMPDIR", &scratch.0).output().unwrap();
        assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
        assert!(String::from_utf8_lossy(&out.stdout).starts_with(&format!("verified {} unit: ", kind.name())));
        assert_eq!(listing(&dir.0), before);
        assert!(listing(&scratch.0).is_empty());
    }
}

#[test]
fn verify_fails_on_a_member_that_differs_and_writes_nothing() {
    let dir = fixture_dir();
    let zst = unit(&dir.0, Kind::Pois, None);
    let path = dir.0.join("pois.tt.zst");
    std::fs::write(&path, &zst).unwrap();
    let mut vertices = std::fs::read(dir.0.join("poi_vertices.bin")).unwrap();
    vertices[5] ^= 0x10;
    std::fs::write(dir.0.join("poi_vertices.bin"), &vertices).unwrap();
    let names = || {
        let mut n: Vec<_> = std::fs::read_dir(&dir.0).unwrap().map(|e| e.unwrap().file_name()).collect();
        n.sort();
        n
    };
    let before = names();
    let out = run(&["verify", path.to_str().unwrap(), dir.0.to_str().unwrap()], None);
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(String::from_utf8_lossy(&out.stderr), "geocoder-transport: poi_vertices.bin differs from the unit's at byte 5\n");
    vertices.push(0);
    std::fs::write(dir.0.join("poi_vertices.bin"), &vertices).unwrap();
    let out = run(&["verify", path.to_str().unwrap(), dir.0.to_str().unwrap()], None);
    assert_eq!(out.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&out.stderr).contains(&format!("poi_vertices.bin is {} bytes, the unit's {}", vertices.len(), vertices.len() - 1)));
    assert_eq!(names(), before);
}

#[test]
fn decode_of_a_unit_cut_short_leaves_no_member() {
    let dir = fixture_dir();
    for (kind, file) in units() {
        let mut args = vec!["encode", kind.name(), dir.0.to_str().unwrap()];
        args.extend(file);
        let raw = run(&args, None).stdout;
        let cut = zstd::encode_all(&raw[..raw.len() * 2 / 3], 3).unwrap();
        let out_dir = TempDir::new("cut");
        let out = run(&["decode", "-", out_dir.0.to_str().unwrap()], Some(&cut));
        assert_eq!(out.status.code(), Some(1), "{}", kind.name());
        assert!(String::from_utf8_lossy(&out.stderr).contains("the unit ends early"), "{}", String::from_utf8_lossy(&out.stderr));
        assert_eq!(std::fs::read_dir(&out_dir.0).unwrap().count(), 0, "{}", kind.name());
    }
}

#[test]
fn a_missing_file_a_wrong_kind_and_bad_arguments_are_refused() {
    let dir = fixture_dir();
    let d = dir.0.to_str().unwrap();
    let out = run(&["encode", "streets", d], None);
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(String::from_utf8_lossy(&out.stderr), "geocoder-transport: streets is no kind: places, pois, arcs or cols\n");
    let out = run(&["encode", "cols", d, "street_ways.bin"], None);
    assert_eq!(out.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&out.stderr).contains("street_ways.bin"));
    assert!(out.stdout.is_empty());
    for args in [&["decode"][..], &["encode", "places"], &["verify", "a", "b", "c"], &[]] {
        assert_eq!(run(args, None).status.code(), Some(2), "{args:?}");
    }
}
