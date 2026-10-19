"""mk_adv.py <search crate dir>: experiment patch (not committed). Before each key phase, re-advise
the mapped files the phase streams, chosen by environment: ADV_REC for the record files it scans in
order (poi_records, or place_nodes + admin_polygons), ADV_STR for the string tiers, ADV_VERT for
admin_vertices; values seq | normal | willneed (unset: keep the reader's MADV_RANDOM)."""
import sys

p = sys.argv[1] + "/src/build.rs"
s = open(p).read()


def rep(a, b):
    global s
    assert s.count(a) == 1, a
    s = s.replace(a, b)


rep("use query_server::{AdminPolygon, Index, NodeCoord, PlaceNode, PoiRecord, NO_DATA};",
    "use query_server::{AdminPolygon, FileBytes, Index, NodeCoord, PlaceNode, PoiRecord, NO_DATA};")
rep("""    let src = if name == "poi" {
        Source::Poi(""", """    exp_advise(geo, name);
    let src = if name == "poi" {
        Source::Poi(""")
s += '''
fn exp_advise(geo: &Index, name: &str) {
    use memmap2::Advice;
    let pick = |var: &str| match std::env::var(var).as_deref() {
        Ok("seq") => Some(Advice::Sequential),
        Ok("normal") => Some(Advice::Normal),
        Ok("willneed") => Some(Advice::WillNeed),
        _ => None,
    };
    let adv = |f: Option<&FileBytes>, a: Option<Advice>| {
        if let (Some(FileBytes::Mmap(m)), Some(a)) = (f, a) {
            m.advise(a).expect("madvise");
        }
    };
    let (rec, st, vert) = (pick("ADV_REC"), pick("ADV_STR"), pick("ADV_VERT"));
    if name == "poi" {
        adv(geo.poi_records.as_ref(), rec);
    } else {
        adv(geo.place_nodes.as_ref(), rec);
        adv(Some(&geo.admin_polygons), rec);
        adv(Some(&geo.admin_vertices), vert);
    }
    for t in &geo.strings.tiers {
        adv(t.as_ref(), st);
    }
    eprintln!("advice {name}: rec {rec:?} strings {st:?} vertices {vert:?}");
}
'''
open(p, "w").write(s)
print("patched", p)
