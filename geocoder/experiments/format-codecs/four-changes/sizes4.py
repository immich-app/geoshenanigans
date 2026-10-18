"""Stage totals per preset from the manifest (published zstd -18 -T1 sizes) and whole-file
zstd 1.5.7 -18 -T1 measurements of every changed file (work/zst_T1.tsv, by path, = compress_index.py).
A file not in a stage's override map keeps its published size. Prints markdown; writes work/sizes4.json.
1 MiB = 1,048,576 B."""
import csv, json
from common4 import *

PUB = json.load(open(f"{W}/work/published.json"))
T1 = {r[0]: (int(r[1]), int(r[2])) for r in csv.reader(open(f"{W}/work/zst_T1.tsv"), delimiter="\t")}
S = f"{W}/sets"
B = f"{W}/work/B"
CH1 = ["addr_points.bin", "addr_postcodes.bin", "admin_cells.bin", "admin_entries.bin", "admin_polygons.bin",
       "interp_postcodes.bin", "interp_ways.bin", "place_cells.bin", "place_entries.bin", "place_nodes.bin",
       "poi_records.bin", "postal_polygons.bin", "postcode_centroids.bin", "street_ways.bin", "strings_core.bin",
       "strings_layout.json", "way_postcodes.bin"]
DROP = None  # marker: file not shipped


INODE = {(os.stat(p).st_dev, os.stat(p).st_ino): v for p, v in T1.items() if os.path.exists(p)}
SAME = {}  # path -> measured path with byte-identical content (logged)


def m(path):
    """Measured (raw, zst) of path: by realpath, then by inode (hard link), then by a byte-identical
    measured file of the same name and size (filecmp, full compare)."""
    r = os.path.realpath(path)
    if r in T1:
        return T1[r]
    st = os.stat(r)
    if (st.st_dev, st.st_ino) in INODE:
        return INODE[(st.st_dev, st.st_ino)]
    import filecmp
    for p, v in T1.items():
        if os.path.basename(p) == os.path.basename(r) and v[0] == st.st_size and filecmp.cmp(p, r, shallow=False):
            SAME[r] = p
            return v
    return None


def stage(base, over):
    """base: {file: (raw, zst)}; over: {file: path | DROP}. Returns new dict (missing measurement -> None)."""
    out = dict(base)
    for f, p in over.items():
        if p is DROP:
            out.pop(f, None)
        else:
            out[f] = m(p)
    return out


def tot(st):
    if any(v is None for v in st.values()):
        miss = [f for f, v in st.items() if v is None]
        return None, None, miss
    return sum(v[0] for v in st.values()), sum(v[1] for v in st.values()), []


res = {}
for preset in ("max", "withaddr"):
    s1 = f"{S}/{preset}1"
    base = {f: (v["raw"], v["zst"]) for f, v in PUB[preset].items()}
    st = {"today (published)": base}
    st["1 dead weight"] = stage(base, {f: f"{s1}/{f}" for f in CH1})
    st["1+2 place index rebuilt at load"] = stage(st["1 dead weight"], {"place_cells.bin": DROP, "place_entries.bin": DROP})
    st["1+2+3 layout B (lossless)"] = stage(st["1+2 place index rebuilt at load"], {"admin_vertices.bin": f"{B}/{preset}_B.bin"})
    lossless = st["1+2+3 layout B (lossless)"]
    st["1+2+3 + postal dedup P1 (drop postal_*)"] = stage(lossless, {"postal_polygons.bin": DROP, "postal_vertices.bin": DROP})
    p2 = f"{S}/{preset}1p2"
    st["1+2+3 + postal dedup P2 (drop admin L11 copy)"] = stage(lossless, {
        "admin_polygons.bin": f"{p2}/admin_polygons.bin", "admin_cells.bin": f"{p2}/admin_cells.bin",
        "admin_entries.bin": f"{p2}/admin_entries.bin", "admin_vertices.bin": f"{B}/{preset}1p2_B.bin"})
    if preset == "withaddr":
        s4 = f"{S}/withaddr1s"
        st["1+2+3+4 11 m grid (LOSSY)"] = stage(lossless, {"admin_polygons.bin": f"{s4}/admin_polygons.bin",
                                                            "admin_vertices.bin": f"{B}/withaddr1s_B.bin"})
        st["1+2+3+4 + postal dedup P1 (LOSSY)"] = stage(st["1+2+3+4 11 m grid (LOSSY)"], {"postal_polygons.bin": DROP, "postal_vertices.bin": DROP})
        p2s = f"{S}/withaddr1sp2"
        st["1+2+3+4 + postal dedup P2 (LOSSY)"] = stage(st["1+2+3+4 11 m grid (LOSSY)"], {
            "admin_polygons.bin": f"{p2s}/admin_polygons.bin", "admin_cells.bin": f"{p2s}/admin_cells.bin",
            "admin_entries.bin": f"{p2s}/admin_entries.bin", "admin_vertices.bin": f"{B}/withaddr1sp2_B.bin"})
        st["(today's format) 1+4 11 m grid, reader-loadable"] = stage(st["1 dead weight"], {
            "admin_polygons.bin": f"{s4}/admin_polygons.bin", "admin_vertices.bin": f"{s4}/admin_vertices.bin"})
    b_raw, b_zst, _ = tot(base)
    print(f"\n## {preset}\n| stage | download MiB (zstd -18) | Δ MiB | raw on disk MiB | Δ MiB |\n|---|---:|---:|---:|---:|")
    res[preset] = {}
    for name, d in st.items():
        raw, z, miss = tot(d)
        res[preset][name] = {"raw": raw, "zst": z, "missing": miss,
                             "files": {f: v for f, v in d.items()}}
        if z is None:
            print(f"| {name} | (missing: {', '.join(miss)}) | | | |")
            continue
        print(f"| {name} | {z / MiB:,.1f} | {(z - b_zst) / MiB:+,.1f} | {raw / MiB:,.1f} | {(raw - b_raw) / MiB:+,.1f} |")
    # per-file table: every file some stage changes
    names = list(st)
    files = sorted({f for n in names for f in set(base) | set(st[n]) if st[n].get(f) != base.get(f)},
                   key=lambda f: -base.get(f, (0, 0))[1])
    print(f"\n| file ({preset}) | " + " | ".join(names) + " |\n|---|" + "---:|" * len(names))
    for f in files:
        cells = []
        for n in names:
            v = st[n].get(f, "drop")
            cells.append("dropped" if v == "drop" else "?" if v is None else f"{v[1] / MiB:,.2f}")
        print(f"| {f} | " + " | ".join(cells) + " |")
json.dump(res, open(f"{W}/work/sizes4.json", "w"), indent=1)
print("\nsizes taken from a byte-identical measured file (full filecmp):")
for a, b in sorted(SAME.items()):
    print(f"  {a} == {b}")
