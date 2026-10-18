"""Every string reference in the maximum-quality set, which strings are never referenced,
and whether any reference lands inside a string (suffix sharing)."""
import json
import numpy as np
from lists import *

lay = json.load(open(f"{D}/strings_layout.json"))["tiers"]
from strings_fields import FIELDS

# tier string starts
tiers = []
for t in lay:
    b = np.fromfile(f"{D}/{t['file']}", np.uint8)
    assert len(b) == t["end"] - t["start"], t
    nul = np.nonzero(b == 0)[0]
    assert len(b) == 0 or b[-1] == 0
    starts = np.concatenate([[0], nul[:-1] + 1]).astype(np.int64) + t["start"]
    tiers.append((t["name"], t["start"], t["end"], starts, nul + t["start"]))
    print(f"tier {t['name']:8s} bytes {len(b):>12,d} strings {len(starts):>11,d}")
all_starts = np.concatenate([x[3] for x in tiers])
all_ends = np.concatenate([x[4] for x in tiers])  # position of NUL
assert np.all(np.diff(all_starts) > 0)
nstr = len(all_starts)
ref_any = np.zeros(nstr, bool)
ref_reader = np.zeros(nstr, bool)
per_source = {}
for fn, (dt, flds, read) in FIELDS.items():
    a = np.fromfile(f"{D}/{fn}", dt)
    for f in flds:
        v = a[f].astype(np.int64)
        v = v[v != NO]
        i = np.searchsorted(all_starts, v, side="right") - 1
        ok = (i >= 0) & (v <= all_ends[np.clip(i, 0, None)])
        exact = all_starts[i] == v
        out_of_pool = int((~ok).sum())
        mid = int((ok & ~exact).sum())
        m = np.zeros(nstr, bool)
        m[i[ok]] = True
        ref_any |= m
        if read:
            ref_reader |= m
        tn = np.searchsorted([x[1] for x in tiers], v, side="right") - 1
        tc = np.bincount(tn, minlength=5)
        per_source[f"{fn}:{f}"] = m
        print(f"{fn:24s} {f:20s} refs {len(v):>12,d} distinct strings {int(m.sum()):>11,d} mid-string refs {mid} "
              f"outside pool {out_of_pool} tiers {dict((tiers[k][0], int(c)) for k, c in enumerate(tc) if c)}", flush=True)
lens = (all_ends - all_starts + 1)
for name, lo, hi, st, _ in tiers:
    s = (all_starts >= lo) & (all_starts < hi)
    print(f"tier {name:8s}: unreferenced by any shipped file {int((s & ~ref_any).sum()):>10,d} strings {int(lens[s & ~ref_any].sum()):>12,d} bytes"
          f" | unreferenced by fields the reader reads {int((s & ~ref_reader).sum()):>10,d} strings {int(lens[s & ~ref_reader].sum()):>12,d} bytes")
np.save(f"{W}/str_starts.npy", all_starts)
np.save(f"{W}/str_ref_any.npy", ref_any)
np.save(f"{W}/str_ref_reader.npy", ref_reader)
# cross-tier duplicates (same bytes in two tiers) among referenced strings
