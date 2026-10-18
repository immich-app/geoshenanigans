"""Strings purge: every string no shipped file of the maximum preset references (strings_refs.py
proved each tier's unreferenced set; no reference points inside a string) is dropped, tiers keep
their order, every reference is remapped. Writes a today's-format set in work/v_strpurge/
(purged strings_*.bin + strings_layout.json + every file holding string offsets).
usage: strings_purge.py"""
import json
import os
import numpy as np
from lists import *
from strings_fields import FIELDS

out = f"{W}/v_strpurge"
os.makedirs(out, exist_ok=True)
lay = json.load(open(f"{D}/strings_layout.json"))
starts = np.load(f"{W}/str_starts.npy")
keep = np.load(f"{W}/str_ref_any.npy")
ends = np.empty_like(starts)
new_tiers = []
new_start_of = np.full(len(starts), -1, np.int64)
base = 0
for t in lay["tiers"]:
    b = np.fromfile(f"{D}/{t['file']}", np.uint8)
    s = (starts >= t["start"]) & (starts < t["end"])
    st = starts[s] - t["start"]
    en = np.concatenate([st[1:], [len(b)]])
    k = keep[s]
    ln = en - st
    kept_ln = ln[k]
    new_local = np.concatenate([[0], np.cumsum(kept_ln)[:-1]]).astype(np.int64)
    idx = np.nonzero(s)[0]
    new_start_of[idx[k]] = base + new_local
    # gather bytes of kept strings
    pos = np.repeat(st[k], kept_ln) + (np.arange(int(kept_ln.sum())) - np.repeat(new_local, kept_ln))
    nb = b[pos]
    nb.tofile(f"{out}/{t['file']}")
    new_tiers.append({"name": t["name"], "file": t["file"], "start": int(base), "end": int(base + len(nb))})
    print(f"{t['file']:22s} {len(b):>12,d} -> {len(nb):>12,d} B ({int((~k).sum()):,} strings dropped)", flush=True)
    base += len(nb)
lay2 = dict(lay)
lay2["tiers"] = new_tiers
json.dump(lay2, open(f"{out}/strings_layout.json", "w"))
# poi_records: start from the R1 variant (dead lat/lng zeroed, poi_rec.py) so the sets stack
SRC = {"poi_records.bin": f"{W}/v_poi_r1"}
old_tiers = {t["name"]: np.memmap(f"{D}/{t['file']}", np.uint8, "r") for t in lay["tiers"]}
new_tiers_b = {t["name"]: np.fromfile(f"{out}/{t['file']}", np.uint8) for t in new_tiers}


def tier_of(v, tiers):
    return np.searchsorted([t["start"] for t in tiers], v, side="right") - 1


def check_same(old, new):
    """Every reference resolves to the same bytes before and after (exhaustive)."""
    to = tier_of(old, lay["tiers"])
    tn = tier_of(new, new_tiers)
    assert np.array_equal(to, tn)
    bad = 0
    for k, t in enumerate(lay["tiers"]):
        m = to == k
        if not m.any():
            continue
        ob, nb = old_tiers[t["name"]], new_tiers_b[t["name"]]
        lo = old[m] - t["start"]
        ln_ = new[m] - new_tiers[k]["start"]
        # compare string bytes position by position until both hit NUL
        o, n_ = lo.copy(), ln_.copy()
        alive = np.ones(len(o), bool)
        while alive.any():
            a_, b_ = ob[o[alive]], nb[n_[alive]]
            bad += int((a_ != b_).sum())
            idx = np.nonzero(alive)[0]
            done = (a_ == 0) | (a_ != b_)
            alive[idx[done]] = False
            o[idx[~done]] += 1
            n_[idx[~done]] += 1
    return bad


for fn, (dt, flds, _) in FIELDS.items():
    a = np.fromfile(f"{SRC.get(fn, D)}/{fn}", dt)
    for f in flds:
        v = a[f].astype(np.int64)
        m = v != NO
        i = np.searchsorted(starts, v[m])
        assert np.all(starts[i] == v[m]) and np.all(new_start_of[i] >= 0)
        nv = v.copy()
        nv[m] = new_start_of[i]
        uo, ui = np.unique(v[m], return_index=True)   # the remap is a function: check each distinct ref once
        bad = check_same(uo, nv[m][ui])
        assert bad == 0, (fn, f, bad)
        a[f] = nv.astype(np.uint32)
    a.tofile(f"{out}/{fn}")
    print(f"remapped {fn} (every reference resolves to the same bytes: checked)", flush=True)
