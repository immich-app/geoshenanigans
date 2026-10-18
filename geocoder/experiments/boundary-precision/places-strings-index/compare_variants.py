"""Run grid_query over work/points.txt on the shipped admin-minimal set and every variant,
then count answers that differ (full JSON, and the three fields Immich stores).

Immich mapping (immich feat/geoshenanigans-geocoder, server/src/repositories/geocoding.repository.ts:77):
  city  = city ?? town ?? village ?? hamlet ?? municipality
  state = state ?? province
  country via country_code
"""
import json, subprocess, sys, time
from concurrent.futures import ThreadPoolExecutor
from common import *

GQ = "/ssd/shrink/target/release/examples/grid_query"
PTS = f"{WORK}/points.txt"
dirs = {"V0": SM}
for v in sys.argv[1:] or ["V1", "V2", "V3", "V4", "V5", "V6"]:
    dirs[v] = f"{WORK}/variants/{v}"


def run(item):
    name, d = item
    t = time.time()
    out = f"{WORK}/q_{name}.jsonl"
    with open(PTS) as fi, open(out, "w") as fo:
        subprocess.run([GQ, d], stdin=fi, stdout=fo, check=True)
    return name, time.time() - t


with ThreadPoolExecutor(len(dirs)) as ex:
    for name, secs in ex.map(run, dirs.items()):
        print(f"{name}: {secs:.1f}s", flush=True)


def immich(a):
    a = a.get("address", {})
    return (a.get("city") or a.get("town") or a.get("village") or a.get("hamlet") or a.get("municipality"),
            a.get("state") or a.get("province"), a.get("country_code"))


def rows(name):
    with open(f"{WORK}/q_{name}.jsonl") as f:
        for line in f:
            lat, lng, js = line.split(" ", 2)
            yield (lat, lng), js


base = list(rows("V0"))
for name in dirs:
    if name == "V0":
        continue
    other = list(rows(name))
    assert len(other) == len(base)
    full = 0
    im = [0, 0, 0]
    examples = []
    for (k0, j0), (k1, j1) in zip(base, other):
        assert k0 == k1
        if j0 == j1:
            continue
        full += 1
        a, b = immich(json.loads(j0)), immich(json.loads(j1))
        for i in range(3):
            if a[i] != b[i]:
                im[i] += 1
        if a != b and len(examples) < 4:
            examples.append((k0, a, b))
    n = len(base)
    print(f"{name}: full-JSON diffs {full:,}/{n:,} ({full / n:.3%}); Immich city {im[0]:,} ({im[0] / n:.3%}), state {im[1]:,}, country {im[2]:,}")
    for e in examples:
        print("    ", e)
