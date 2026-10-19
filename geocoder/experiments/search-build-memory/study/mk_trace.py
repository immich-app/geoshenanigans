"""mk_trace.py <geocoder/search dir>: insert memtrace marks into an experiment copy of the search crate."""
import sys

d = sys.argv[1]


def sub(path, pairs):
    s = open(path).read()
    for a, b in pairs:
        assert s.count(a) == 1, (path, a)
        s = s.replace(a, b)
    open(path, "w").write(s)


sub(d + "/src/lib.rs", [("pub mod build;", "pub mod build;\npub mod memtrace;")])
open(d + "/src/memtrace.rs", "w").write(open("/ssd/searchbuild/memtrace.rs").read() + """
pub static MAXG: AtomicUsize = AtomicUsize::new(0);
pub fn maxg(n: usize) { MAXG.fetch_max(n, Relaxed); }
""")
mk = 'crate::memtrace::mark(&format!("{name} %s"));'
sub(d + "/src/build.rs", [
    ("        None => None,\n    };\n    let src = if", "        None => None,\n    };\n    " + mk % "start" + "\n    let src = if"),
    ("    let t_src = t0.elapsed();\n", "    let t_src = t0.elapsed();\n    " + mk % "source" + "\n"),
    ("    let t_keys = t0.elapsed() - t_src;\n", "    let t_keys = t0.elapsed() - t_src;\n    " + mk % "runs" + "\n"),
    ("    drop(m);\n    for f in &files", "    " + mk % "merge" + "\n    drop(m);\n    for f in &files"),
    ("    let t_merge = t0.elapsed();\n", "    let t_merge = t0.elapsed();\n    " + mk % "prefix" +
     "\n    eprintln!(\"MAXG {}\", crate::memtrace::MAXG.swap(0, std::sync::atomic::Ordering::Relaxed));\n"),
    ("        if let Some(l) = self.last.take() {\n",
     "        crate::memtrace::maxg(self.whole.len() + 1);\n        if let Some(l) = self.last.take() {\n"),
])
sub(d + "/src/main.rs", [
    ("    let geo = load_geo(set);\n    let sub = opt", "    let geo = load_geo(set);\n    place_search::memtrace::mark(\"load\");\n    let sub = opt"),
    ("    eprintln!(\"build memory: {}\", mem_json());", "    place_search::memtrace::mark(\"end\");\n    eprintln!(\"build memory: {}\", mem_json());"),
])
print("patched")
