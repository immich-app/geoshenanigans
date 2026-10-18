#!/usr/bin/env python3
"""gen_pts.py PUB_PLANET_DIR OUT SEED: a fresh query mix drawn from the PUBLISHED day.

30k points: addresses (+-30 m and exact), streets (random node +-30 m, first
nodes exact), interpolation lines (random point on a random segment, end
nodes), POIs (exact and +-30 m), places (exact and +-30 m). Tombstones skipped.
"""
import sys
import numpy as np

pub, out, seed = sys.argv[1], sys.argv[2], int(sys.argv[3])
rng = np.random.default_rng(seed)
F = pub + "/full/"
R = 6371008.8
NO = 0xFFFFFFFF


def jitter(lat, lng, metres):
    r = metres * np.sqrt(rng.random(len(lat)))
    th = rng.random(len(lat)) * 2 * np.pi
    dlat = r * np.cos(th) / (R * np.pi / 180)
    dlng = r * np.sin(th) / (R * np.pi / 180 * np.maximum(np.cos(np.radians(lat)), 0.01))
    return lat + dlat, np.clip(lng + dlng, -180, 180)


def pick(n_total, k, ok):
    """k random indices i < n_total with ok(i) True (rejection sampling)."""
    got = np.empty(0, dtype=np.int64)
    while len(got) < k:
        cand = np.unique(rng.integers(0, n_total, size=2 * (k - len(got)) + 16))
        cand = cand[ok(cand)]
        got = np.unique(np.concatenate([got, cand]))
    return rng.permutation(got)[:k]


pts = []
tags = []


def add(tag, lat, lng):
    pts.append(np.stack([np.asarray(lat, np.float64), np.asarray(lng, np.float64)], 1))
    tags.extend([tag] * len(lat))


# addresses
ad = np.dtype([("lat", "<f4"), ("lng", "<f4"), ("hn", "<u4"), ("st", "<u4"), ("par", "<u4"), ("vo", "<u4"), ("vc", "<u4")])
A = np.memmap(F + "addr_points.bin", dtype=ad, mode="r")
live_a = lambda i: ~((A["lat"][i] == 0) & (A["lng"][i] == 0) & (A["hn"][i] == 0) & (A["st"][i] == 0))
i = pick(len(A), 9000, live_a)
lat, lng = A["lat"][i].astype(np.float64), A["lng"][i].astype(np.float64)
add("addr30", *jitter(lat[:7000], lng[:7000], 30))
add("addr0", lat[7000:], lng[7000:])

# streets
wd = np.dtype([("off", "<u4"), ("cnt", "<u2"), ("pad", "<u2"), ("name", "<u4")])
W = np.memmap(F + "street_ways.bin", dtype=wd, mode="r")
WN = np.memmap(F + "street_nodes.bin", dtype="<f4", mode="r").reshape(-1, 2)
i = pick(len(W), 7500, lambda i: W["cnt"][i] > 0)
off, cnt = W["off"][i].astype(np.int64), W["cnt"][i].astype(np.int64)
j = off + (rng.random(len(i)) * cnt).astype(np.int64)
lat, lng = WN[j, 0].astype(np.float64), WN[j, 1].astype(np.float64)
add("street30", *jitter(lat[:6000], lng[:6000], 30))
add("street_first", WN[off[6000:], 0].astype(np.float64), WN[off[6000:], 1].astype(np.float64))

# interpolation lines
idt = np.dtype([("off", "<u4"), ("cnt", "<u2"), ("p1", "<u2"), ("st", "<u4"), ("s", "<u4"), ("e", "<u4"), ("it", "u1"), ("p", "u1", 3)])
IW = np.memmap(F + "interp_ways.bin", dtype=idt, mode="r")
IN = np.memmap(F + "interp_nodes.bin", dtype="<f4", mode="r").reshape(-1, 2)
i = pick(len(IW), 5500, lambda i: IW["cnt"][i] >= 2)
off, cnt = IW["off"][i].astype(np.int64), IW["cnt"][i].astype(np.int64)
seg = off + (rng.random(len(i)) * (cnt - 1)).astype(np.int64)
t = rng.random(len(i))
lat = IN[seg, 0] * (1 - t) + IN[seg + 1, 0] * t
lng = IN[seg, 1] * (1 - t) + IN[seg + 1, 1] * t
add("interp_mid", *jitter(lat[:4000].astype(np.float64), lng[:4000].astype(np.float64), 10))
endn = np.where(rng.random(1500) < 0.5, off[4000:], off[4000:] + cnt[4000:] - 1)
add("interp_end", IN[endn, 0].astype(np.float64), IN[endn, 1].astype(np.float64))

# POIs
pd = np.dtype([("lat", "<f4"), ("lng", "<f4"), ("vo", "<u4"), ("vc", "<u4"), ("name", "<u4"), ("cat", "u1"), ("tier", "u1"), ("fl", "u1"),
               ("imp", "u1"), ("pst", "<u4"), ("ppc", "<u4"), ("ppoly", "<u4")])
P = np.memmap(pub + "/poi/all/poi_records.bin", dtype=pd, mode="r")
i = pick(len(P), 4000, lambda i: P["name"][i] != NO)
lat, lng = P["lat"][i].astype(np.float64), P["lng"][i].astype(np.float64)
add("poi0", lat[:2000], lng[:2000])
add("poi30", *jitter(lat[2000:], lng[2000:], 30))

# places
pld = np.dtype([("lat", "<f4"), ("lng", "<f4"), ("name", "<u4"), ("pt", "u1"), ("p1", "u1"), ("p2", "<u2"), ("par", "<u4")])
PL = np.memmap(F + "place_nodes.bin", dtype=pld, mode="r")
i = pick(len(PL), 4000, lambda i: PL["name"][i] != NO)
lat, lng = PL["lat"][i].astype(np.float64), PL["lng"][i].astype(np.float64)
add("place0", lat[:2000], lng[:2000])
add("place30", *jitter(lat[2000:], lng[2000:], 30))

allp = np.concatenate(pts)
assert len(allp) == 30000, len(allp)
with open(out, "w") as f:
    for (la, ln) in allp:
        f.write(f"{la:.8f} {ln:.8f}\n")
with open(out + ".tags", "w") as f:
    f.write("\n".join(tags) + "\n")
from collections import Counter
print(Counter(tags), len(allp))
