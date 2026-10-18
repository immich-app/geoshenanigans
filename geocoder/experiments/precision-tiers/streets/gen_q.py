#!/usr/bin/env python3
"""gen_q.py FULL_DIR OUT SEED: query points near streets, addresses and interpolation lines.

Drawn from the build's own records (builder order, f32): addresses (+-30 m, +-5 m, exact), street nodes
(+-30 m, +-5 m), points on random street segments (+-3 m), points on interpolation segments (+-10 m) and
interpolation end nodes. Writes "lat lng" lines and OUT.tags (one class name per line).
"""
import sys
import numpy as np

full, out, seed = sys.argv[1], sys.argv[2], int(sys.argv[3])
rng = np.random.default_rng(seed)
R = 6371008.8


def jitter(lat, lng, metres):
    r = metres * np.sqrt(rng.random(len(lat)))
    th = rng.random(len(lat)) * 2 * np.pi
    dlat = r * np.cos(th) / (R * np.pi / 180)
    dlng = r * np.sin(th) / (R * np.pi / 180 * np.maximum(np.cos(np.radians(lat)), 0.01))
    return lat + dlat, np.clip(lng + dlng, -180, 180)


pts, tags = [], []


def add(tag, lat, lng):
    pts.append(np.stack([np.asarray(lat, np.float64), np.asarray(lng, np.float64)], 1))
    tags.extend([tag] * len(lat))


ad = np.dtype([("lat", "<f4"), ("lng", "<f4"), ("hn", "<u4"), ("st", "<u4"), ("par", "<u4"), ("vo", "<u4"), ("vc", "<u4")])
A = np.memmap(full + "/addr_points.bin", dtype=ad, mode="r")
i = rng.integers(0, len(A), 350000)
lat, lng = A["lat"][i].astype(np.float64), A["lng"][i].astype(np.float64)
add("addr30", *jitter(lat[:200000], lng[:200000], 30))
add("addr5", *jitter(lat[200000:300000], lng[200000:300000], 5))
add("addr0", lat[300000:], lng[300000:])

wd = np.dtype([("off", "<u4"), ("cnt", "<u2"), ("pad", "<u2"), ("name", "<u4")])
W = np.memmap(full + "/street_ways.bin", dtype=wd, mode="r")
WN = np.memmap(full + "/street_nodes.bin", dtype="<f4", mode="r").reshape(-1, 2)
i = rng.integers(0, len(W), 400000)
off, cnt = W["off"][i].astype(np.int64), W["cnt"][i].astype(np.int64)
j = off + (rng.random(len(i)) * cnt).astype(np.int64)
lat, lng = WN[j, 0].astype(np.float64), WN[j, 1].astype(np.float64)
add("street30", *jitter(lat[:200000], lng[:200000], 30))
add("street5", *jitter(lat[200000:300000], lng[200000:300000], 5))
seg = off[300000:] + (rng.random(100000) * (cnt[300000:] - 1)).astype(np.int64)
t = rng.random(100000)
lat = WN[seg, 0] * (1 - t) + WN[seg + 1, 0] * t
lng = WN[seg, 1] * (1 - t) + WN[seg + 1, 1] * t
add("street_seg3", *jitter(lat.astype(np.float64), lng.astype(np.float64), 3))

idt = np.dtype([("off", "<u4"), ("cnt", "<u2"), ("p1", "<u2"), ("st", "<u4"), ("s", "<u4"), ("e", "<u4"), ("it", "u1"), ("p", "u1", 3)])
IW = np.memmap(full + "/interp_ways.bin", dtype=idt, mode="r")
IN = np.memmap(full + "/interp_nodes.bin", dtype="<f4", mode="r").reshape(-1, 2)
i = rng.integers(0, len(IW), 125000)
off, cnt = IW["off"][i].astype(np.int64), IW["cnt"][i].astype(np.int64)
seg = off[:100000] + (rng.random(100000) * (cnt[:100000] - 1)).astype(np.int64)
t = rng.random(100000)
lat = IN[seg, 0] * (1 - t) + IN[seg + 1, 0] * t
lng = IN[seg, 1] * (1 - t) + IN[seg + 1, 1] * t
add("interp_mid10", *jitter(lat.astype(np.float64), lng.astype(np.float64), 10))
endn = np.where(rng.random(25000) < 0.5, off[100000:], off[100000:] + cnt[100000:] - 1)
add("interp_end", IN[endn, 0].astype(np.float64), IN[endn, 1].astype(np.float64))

P = np.concatenate(pts)
with open(out, "w") as f:
    for la, ln in P:
        f.write("%.8f %.8f\n" % (la, ln))
with open(out + ".tags", "w") as f:
    f.write("\n".join(tags) + "\n")
print("points", len(P))
