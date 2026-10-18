"""Download / Docker layer / raw sizes of the baseline, each stacked stage and the final set.
download = sum of per-file `zstd 1.5.7 -18 -T1 -c <file>` (publish setting, compress_index.py)
layer    = `tar -ch --sort=name --owner=0 --group=0 --mtime=@0 . | gzip -6` of the set dir
raw      = sum of file sizes
Set dirs are written as symlink farms under work/sets/ (tar -h stores the target bytes).
Writes work/sizes.json and prints markdown tables. 1 MiB = 1,048,576 B."""
import json, os, shutil, subprocess
from concurrent.futures import ThreadPoolExecutor

S = "/ssd/shrink/stack"
SM = "/ssd/shrink/idx/smallest"
V11 = f"{S}/work/v11"
TF = f"{S}/todays-format"
L = f"{S}/work/layouts"
ZSTD = "/ssd/shrink/zstd157/bin/zstd"
MiB = 1048576
PLACE_IDX = ["place_cells.bin", "place_entries.bin"]
ALL = ["admin_cells.bin", "admin_entries.bin", "admin_polygons.bin", "admin_vertices.bin", "place_cells.bin",
       "place_entries.bin", "place_nodes.bin", "strings_core.bin", "strings_layout.json"]


def drop(d, names):
    return {k: v for k, v in d.items() if k not in names}


base = {f: f"{SM}/{f}" for f in ALL}
s1 = {f: os.path.realpath(f"{V11}/{f}") for f in ALL}
s12 = drop(s1, PLACE_IDX)
s123 = drop({f: f"{TF}/{f}" for f in ALL}, PLACE_IDX)
SETS = {
    "baseline": base,
    "1 dead weight (V11)": s1,
    "1+2 no place index": s12,
    "1+2+3 11 m grid, today's format": s123,
    "1+2+3+4 layout B = FINAL": {**s123, "admin_vertices.bin": f"{L}/B.bin"},
    "1+2+3 + layout A (global grid) instead of B": {**s123, "admin_vertices.bin": f"{L}/A.bin"},
    "1+2+3 + layout A-native instead of B": {**s123, "admin_vertices.bin": f"{L}/An.bin"},
    "1+2+3 + layout B with divisor (extra)": {**s123, "admin_vertices.bin": f"{L}/Bg.bin"},
    "1+3 today's format (reader-loadable stack)": {f: f"{TF}/{f}" for f in ALL},
}

SD = f"{S}/work/sets"
shutil.rmtree(SD, ignore_errors=True)
dirs = {}
for i, (name, files) in enumerate(SETS.items()):
    d = f"{SD}/{i}"
    os.makedirs(d)
    for f, src in files.items():
        os.symlink(src, f"{d}/{f}")
    dirs[name] = d

srcs = sorted({os.path.realpath(p) for files in SETS.values() for p in files.values()})


def zst(p):
    return len(subprocess.run([ZSTD, "-18", "-T1", "-q", "-c", p], stdout=subprocess.PIPE, check=True).stdout)


def layer(d):
    tar = subprocess.Popen(["tar", "-ch", "--sort=name", "--owner=0", "--group=0", "--numeric-owner", "--mtime=@0",
                            "-C", d, "-f", "-", "."], stdout=subprocess.PIPE)
    gz = subprocess.run(["gzip", "-6", "-n"], stdin=tar.stdout, stdout=subprocess.PIPE, check=True)
    assert tar.wait() == 0
    return len(gz.stdout)


with ThreadPoolExecutor(16) as ex:
    zf = {p: ex.submit(zst, p) for p in srcs}
    lf = {n: ex.submit(layer, d) for n, d in dirs.items()}
    zsz = {p: f.result() for p, f in zf.items()}
    lsz = {n: f.result() for n, f in lf.items()}

res = {}
for name, files in SETS.items():
    per = {f: {"raw": os.path.getsize(os.path.realpath(p)), "zst": zsz[os.path.realpath(p)], "src": os.path.realpath(p)}
           for f, p in files.items()}
    res[name] = {"files": per, "zst": sum(v["zst"] for v in per.values()), "raw": sum(v["raw"] for v in per.values()),
                 "layer_gzip6": lsz[name]}
json.dump(res, open(f"{S}/work/sizes.json", "w"), indent=1)

b = res["baseline"]
print("| set | download zstd-18 MiB | Δ vs baseline | Docker layer gzip-6 MiB | Δ | raw on disk MiB | Δ |")
print("|---|---:|---:|---:|---:|---:|---:|")
for name, r in res.items():
    print(f"| {name} | {r['zst'] / MiB:.2f} | {(r['zst'] - b['zst']) / MiB:+.2f} | {r['layer_gzip6'] / MiB:.2f} | "
          f"{(r['layer_gzip6'] - b['layer_gzip6']) / MiB:+.2f} | {r['raw'] / MiB:.2f} | {(r['raw'] - b['raw']) / MiB:+.2f} |")
print()
fin = res["1+2+3+4 layout B = FINAL"]["files"]
print("| file | baseline zst | final zst | baseline raw | final raw |")
print("|---|---:|---:|---:|---:|")
for f in ALL:
    bz, br = b["files"][f]["zst"], b["files"][f]["raw"]
    fz = fin.get(f, {}).get("zst")
    fr = fin.get(f, {}).get("raw")
    print(f"| {f} | {bz / MiB:.2f} | {'-' if fz is None else f'{fz / MiB:.2f}'} | {br / MiB:.2f} | {'-' if fr is None else f'{fr / MiB:.2f}'} |")
for name in SETS:
    if "layout" in name or "today" in name:
        print(f"admin_vertices.bin in '{name}': zst {res[name]['files']['admin_vertices.bin']['zst']:,} B "
              f"({res[name]['files']['admin_vertices.bin']['zst'] / MiB:.2f} MiB), raw {res[name]['files']['admin_vertices.bin']['raw']:,} B")
pi = sum(os.path.getsize(f"{TF}/{f}") for f in PLACE_IDX)
print(f"place index rebuilt + cached at load: {pi:,} B ({pi / MiB:.2f} MiB) raw; "
      f"zst it would have cost {sum(res['1 dead weight (V11)']['files'][f]['zst'] for f in PLACE_IDX) / MiB:.2f} MiB")
