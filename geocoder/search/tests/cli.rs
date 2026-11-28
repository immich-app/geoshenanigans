use place_search::dataset::{Origin, PlaceSearch, Stamps};
use place_search::profile::{Profile, SearchRequest};
use place_search::query::{debug_line, results_json};
use query_server::{
    arc_fixture, cell_id_at_level, cell_pairs, place_files, strings, AdminPolygon, Index, PlaceNode, PointE7,
    DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE, DEFAULT_STREET_CELL_LEVEL, NO_DATA, STR_TIER_COUNT,
};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// A fresh directory under the temp dir, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        static DIRS: AtomicUsize = AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!("ps-cli-{tag}-{}-{}", std::process::id(), DIRS.fetch_add(1, Ordering::Relaxed)));
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

/// Writes into `dir` a dataset of the country Testland (lat 10-11, lng 20-21) and its towns
/// Testville and Testerby.
fn write_dataset(dir: &Path) {
    let mut pool = Vec::new();
    let mut string = |s: &str| {
        let at = pool.len() as u32;
        pool.extend_from_slice(s.as_bytes());
        pool.push(0);
        at
    };
    let (country, towns) = (string("Testland"), [(string("Testville"), 10.5, 20.5), (string("Testerby"), 10.25, 20.75)]);
    let ring: Vec<PointE7> =
        [(10.0, 20.0), (10.0, 21.0), (11.0, 21.0), (11.0, 20.0), (10.0, 20.0)].into_iter().map(|(lat, lng)| arc_fixture::e7(lat, lng)).collect();
    let mut arcs = arc_fixture::ArcFixture::new();
    arcs.ring_polygon(AdminPolygon { country_code: u16::from_le_bytes(*b"tl"), ..arc_fixture::record(country, 2, 1.0) }, 0, &[ring]);
    let mut cells: Vec<(u64, Vec<u32>)> = towns.iter().map(|&(_, lat, lng)| (cell_id_at_level(lat, lng, DEFAULT_ADMIN_CELL_LEVEL), vec![0])).collect();
    cells.sort();
    cells.dedup();
    let (admin_cells, admin_entries) = cell_pairs::write(&cells, cell_pairs::ADMIN);
    let nodes: Vec<PlaceNode> = towns.iter()
        .map(|&(name_id, lat, lng)| PlaceNode { lat: lat as f32, lng: lng as f32, name_id, place_type: 1, _pad1: 0, rank_address: 16, parent_poly_id: NO_DATA })
        .collect();
    let (places, _) = place_files::encode(&nodes, DEFAULT_ADMIN_CELL_LEVEL);
    let tiers: Vec<String> =
        (0..STR_TIER_COUNT).map(|t| format!("{{\"start\": {}, \"end\": {}}}", if t == 0 { 0 } else { pool.len() }, pool.len())).collect();
    let mut files: Vec<(&str, Vec<u8>)> = arcs.files().into();
    files.extend([
        ("admin_cells.bin", admin_cells),
        ("admin_entries.bin", admin_entries),
        (place_files::CELLS_FILE, places.cells),
        (place_files::NODES_FILE, places.nodes),
        (place_files::HIDDEN_FILE, places.hidden),
        ("strings_layout.json", format!("{{\"tiers\": [{}]}}", tiers.join(", ")).into_bytes()),
        ("strings_core.bin", strings::encode(&pool)),
    ]);
    std::fs::create_dir_all(dir).unwrap();
    for (name, bytes) in files {
        std::fs::write(dir.join(name), bytes).unwrap();
    }
}

/// place-search with `args`, `stdin` its input: its stdout, once it exits 0.
fn place_search(args: &[&str], stdin: &str) -> String {
    let mut child = Command::new(env!("CARGO_BIN_EXE_place-search"))
        .args(args).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped())
        .spawn().expect("run place-search");
    child.stdin.take().unwrap().write_all(stdin.as_bytes()).unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success(), "{args:?}: {}", String::from_utf8_lossy(&out.stderr));
    String::from_utf8(out.stdout).unwrap()
}

// `place-search query` prints for each query the line query::debug_line makes of the search of a
// place search opened over the same index (the node binding's and the test portal's), byte for
// byte but the time it took; its results are the binding's answer (query::results_json), each
// with how it was found besides. The index is the one `place-search build` built, which the open
// finds built from these files.
#[test]
fn the_cli_answers_as_the_binding_and_the_portal_do() {
    let dir = TempDir::new("answers");
    let (set, index) = (dir.0.join("set"), dir.0.join("index"));
    write_dataset(&set);
    let (set_arg, index_arg) = (set.to_str().unwrap(), index.to_str().unwrap());
    place_search(&["build", set_arg, index_arg], "");
    let geo = Index::load(set_arg, DEFAULT_STREET_CELL_LEVEL, DEFAULT_ADMIN_CELL_LEVEL, DEFAULT_SEARCH_DISTANCE).unwrap();
    let (search, origin) = PlaceSearch::open(Arc::new(geo), &Stamps::of(&set).unwrap(), &set, &index, &Default::default()).unwrap();
    assert_eq!(origin, Origin::Current);
    let queries = ["testville", "Testerby", "testvile", "testland", "test", "testville, testland", "nowhere"];

    for limit in [1, 20] {
        let printed = place_search(&["query", set_arg, index_arg, "--limit", &limit.to_string()], &(queries.join("\n") + "\n"));

        let lines: Vec<&str> = printed.lines().collect();
        assert_eq!(lines.len(), queries.len());
        // A line but its time, its last key.
        let timeless = |line: &str| line[..line.rfind(",\"us\":").unwrap()].to_string();
        let mut found = 0;
        for (q, line) in queries.into_iter().zip(lines) {
            let (results, stats) = search.search(&SearchRequest { limit, ..SearchRequest::new(q, Profile::Immich) }).unwrap();

            assert_eq!(timeless(line), timeless(&debug_line(q, 0.0, &results, &stats).to_string()), "{q}");
            let mut answer = serde_json::from_str::<serde_json::Value>(line).unwrap()["results"].clone();
            for r in answer.as_array_mut().unwrap() {
                for key in ["kind", "score", "text", "context", "class", "entity"] {
                    r.as_object_mut().unwrap().remove(key).unwrap();
                }
            }
            // Both read back as JSON: of the same digits, the same numbers.
            assert_eq!(answer, serde_json::from_str::<serde_json::Value>(&results_json(&results)).unwrap(), "{q}");
            found += results.len();
        }
        assert!(found > 0);
    }
}

#[test]
fn a_flag_given_last_without_its_value_is_a_usage_error() {
    for args in [["build", "set", "index", "--sub"], ["query", "set", "index", "--limit"]] {
        let out = Command::new(env!("CARGO_BIN_EXE_place-search")).args(args).output().expect("run place-search");
        assert_eq!(out.status.code(), Some(2), "{args:?}");
        assert_eq!(String::from_utf8_lossy(&out.stderr), format!("{} needs a value\n", args[3]), "{args:?}");
    }
}
