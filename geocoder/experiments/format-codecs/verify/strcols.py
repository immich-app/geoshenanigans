"""Effect of the string purges (all tiers; index track's keep mask = strings referenced by any shipped
file, which equals the four-changes core mask + the addresses street mask) on the addr_points
housenumber and street columns in the Tier B (cell) record order. Writes the two columns before and
after the remap as separate files for whole-file zstd. usage: strcols.py <out_dir>"""
import json
import os
import sys
import numpy as np

I = "/ssd/shrink/idx/max"
T = "/ssd/shrink/max/index-poi-strings/work"
out = sys.argv[1]
os.makedirs(out, exist_ok=True)
NO = 0xFFFFFFFF
lay = json.load(open(f"{I}/strings_layout.json"))["tiers"]
starts = np.load(f"{T}/str_starts.npy").astype(np.int64)
keep = np.load(f"{T}/str_ref_any.npy")
new_start_of = np.full(len(starts), -1, np.int64)
base = 0
for t in lay:
    s = (starts >= t["start"]) & (starts < t["end"])
    st = starts[s] - t["start"]
    en = np.concatenate([st[1:], [t["end"] - t["start"]]])
    k = keep[s]
    kl = (en - st)[k]
    new_local = np.concatenate([[0], np.cumsum(kl)[:-1]])
    new_start_of[np.nonzero(s)[0][k]] = base + new_local
    base += int(kl.sum())
ap = np.memmap(f"{I}/addr_points.bin", np.uint32, mode="r").reshape(-1, 7)
old = np.load("/ssd/shrink/max/verify/ad/sorted_old_ids.npy").astype(np.int64)
for name, c in (("hn", 2), ("st", 3)):
    v = np.asarray(ap[:, c])[old]
    v.astype("<u4").tofile(f"{out}/{name}_tierA.bin")
    msk = v != NO
    i = np.searchsorted(starts, v[msk].astype(np.int64))
    assert np.array_equal(starts[i], v[msk].astype(np.int64)) and (new_start_of[i] >= 0).all()
    nv = v.copy()
    nv[msk] = new_start_of[i].astype(np.uint32)
    nv.astype("<u4").tofile(f"{out}/{name}_tierA_purged.bin")
    print(name, "records", len(v), "refs", int(msk.sum()))
