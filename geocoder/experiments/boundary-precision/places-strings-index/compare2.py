"""grid_query over work/points.txt (gen_points2.py: [0,70k) POI +-300 m, [70k,140k) place node
+-300 m, [140k,200k) place node +-3 km) on the shipped admin-minimal set (V0) and the named
variants; counts answers that differ: full JSON and the three fields Immich stores
(immich-geocoder server/src/repositories/geocoding.repository.ts:67-77:
city = city ?? town ?? village ?? hamlet ?? municipality, state = state ?? province, country from code).
usage: compare2.py V7 V9 ...   (re-uses work/q_<V>.jsonl if present and newer than the variant dir)"""
import json, os, subprocess, sys, time
from concurrent.futures import ThreadPoolExecutor
from common import *
GQ = "/ssd/shrink/target/release/examples/grid_query"
PTS = f"{WORK}/points.txt"
SUBSETS = [("POI+-300m", 0, 70000), ("place+-300m", 70000, 140000), ("place+-3km", 140000, 200000)]
dirs = {"V0": SM}
for v in sys.argv[1:]:
    dirs[v] = f"{WORK}/variants/{v}"
def run(item):
    name, d = item
    out = f"{WORK}/q_{name}.jsonl"
    t = time.time()
    with open(PTS) as fi, open(out, "w") as fo:
        subprocess.run([GQ, d], stdin=fi, stdout=fo, check=True)
    return name, time.time() - t
with ThreadPoolExecutor(min(8, len(dirs))) as ex:
    for name, secs in ex.map(run, dirs.items()):
        print(f"{name}: {secs:.1f}s", flush=True)
def immich(js):
    a = json.loads(js).get("address", {})
    g = lambda *k: next((a[x] for x in k if a.get(x) is not None), None)
    return (g("city", "town", "village", "hamlet", "municipality"), g("state", "province"), a.get("country_code"))
def rows(name):
    with open(f"{WORK}/q_{name}.jsonl") as f:
        return [l.split(" ", 2)[2] for l in f]
base = rows("V0")
bi = [immich(j) for j in base]
for name in dirs:
    if name == "V0": continue
    other = rows(name)
    assert len(other) == len(base)
    oi = [immich(j) for j in other]
    out = [f"{name}: full-JSON diffs {sum(a != b for a, b in zip(base, other)):,}/{len(base):,}"]
    for lab, s, e in SUBSETS:
        n = e - s
        c = sum(bi[i][0] != oi[i][0] for i in range(s, e))
        st = sum(bi[i][1] != oi[i][1] for i in range(s, e))
        co = sum(bi[i][2] != oi[i][2] for i in range(s, e))
        cn = sum(bi[i][0] is not None and oi[i][0] is None for i in range(s, e))
        out.append(f"  {lab}: Immich city changed {c:,} ({c / n:.2%}; of which now empty {cn:,}), state {st:,}, country {co:,}")
    print("\n".join(out), flush=True)
    ex = [(i, bi[i], oi[i]) for i in range(len(base)) if bi[i] != oi[i]][:3]
    for e in ex: print("     ", e)
