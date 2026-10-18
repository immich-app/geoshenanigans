"""poi_records: gc's lossless R3 (offsets into poi_vertices.bopen, no field zeroed) against the index
track's R3_aos32 (offsets into today's poi_vertices.bin, R1 zeroing: polygon POIs' lat/lng and dead
records' unread fields zeroed). Builds all four combinations from today's poi_records.bin and gc's
file, checks the gc one is byte-identical to gc's output, zstd -18 -T16 each (streamed).
usage: poi_exp.py"""
import subprocess
import numpy as np

D = "/ssd/shrink/sortpatch/sort/days/2026-10-13/planet/poi/all"
E = "/ssd/shrink/sortpatch/codecs/try/enc/2026-10-13/planet/poi/all"
Z = "/ssd/shrink/zstd157/bin/zstd"
NO = np.uint32(0xFFFFFFFF)
T36 = np.dtype([("lat", "<u4"), ("lng", "<u4"), ("vo", "<u4"), ("vc", "<u4"), ("name", "<u4"), ("cat", "u1"), ("tier", "u1"),
                ("flags", "u1"), ("imp", "u1"), ("ps", "<u4"), ("pc", "<u4"), ("pp", "<u4")])
R32 = np.dtype([("lat", "<u4"), ("lng", "<u4"), ("vo", "<u4"), ("name", "<u4"), ("cat", "u1"), ("tier", "u1"), ("flags", "u1"),
                ("imp", "u1"), ("ps", "<u4"), ("pc", "<u4"), ("pp", "<u4")])
p = np.fromfile(f"{D}/poi_records.bin", T36)
g = np.fromfile(f"{E}/poi_records.r3", R32)
n = len(p)
poly = (p["vc"] > 0) & (p["vo"] != NO)
dead = (p["name"] == NO) & (p["vo"] == NO)


def mono(off, total):
    nxt = np.minimum.accumulate(np.where(poly, off.astype(np.int64), total)[::-1])[::-1]
    return np.where(poly, off.astype(np.int64), np.append(nxt[1:], total)).astype(np.uint32)


bopen_total = 682133214
today_total = 716634818
ours = g["vo"]
# B-open offsets recomputed from gc's record offsets are what gc wrote; today's offsets from today's file
variants = {}
for basis, off in (("bopen offsets", ours), ("today's offsets", mono(p["vo"], today_total))):
    for zero in (False, True):
        r = np.zeros(n, R32)
        for f in R32.names:
            r[f] = off if f == "vo" else p[f]
        if zero:
            r["lat"][poly] = 0
            r["lng"][poly] = 0
            for f in ("lat", "lng", "cat", "tier", "flags", "imp", "ps", "pc", "pp"):
                r[f][dead] = 0
        variants[(basis, zero)] = r
assert variants[("bopen offsets", False)].tobytes() == g.tobytes(), "gc's poi_records.r3 differs from the rebuild"
print("gc poi_records.r3 == rebuild from today's records + B-open offsets: True")
print(f"records {n:,} polygons {int(poly.sum()):,} dead {int(dead.sum()):,}")
print("\n| offsets | fields | raw | zstd -18 -T16 |")
print("|---|---|---:|---:|")
for (basis, zero), r in variants.items():
    b = r.tobytes()
    z = len(subprocess.run([Z, "-18", "-T16", "-q", "-c"], input=b, stdout=subprocess.PIPE, check=True).stdout)
    print(f"| {basis} | {'R1 zeroed (lossy)' if zero else 'all kept (lossless)'} | {len(b):,} | {z:,} |", flush=True)
