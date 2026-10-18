"""Who references strings_core.bin in the max / withaddr presets.
For every string reference field the reader follows (common4.STRING_REFS), count references into
the core tier [0, core_len), check each lands on a string start, and mark the referenced strings.
Also counts references into every other tier (sanity) and the hidden place slots.
Writes work/core_keep_<preset>.npy (bool per core string) and work/core_starts.npy."""
import sys
from common4 import *

core = np.fromfile(f"{MAX}/strings_core.bin", np.uint8)
CL = len(core)
nul = np.flatnonzero(core == 0)
assert nul[-1] == CL - 1
starts = np.concatenate([[0], nul[:-1] + 1]).astype(np.int64)
ns = len(starts)
idx_of = np.full(CL, -1, np.int32)
idx_of[starts] = np.arange(ns, dtype=np.int32)
lens = nul - starts + 1
np.save(f"{W}/work/core_starts.npy", starts)
print(f"strings_core: {CL:,} B, {ns:,} strings")
lay = layout(MAX)["tiers"]
print("tiers:", [(t["name"], t["start"], t["end"]) for t in lay])
assert lay[0]["end"] == CL


def tier_of(v):
    t = np.full(len(v), -1, np.int64)
    for k, x in enumerate(lay):
        t[(v >= x["start"]) & (v < x["end"])] = k
    return t


for preset, d in (("max", MAX), ("withaddr", WA)):
    keep = np.zeros(ns, bool)
    print(f"\n== preset {preset} ({d})")
    for f, stride, fields in STRING_REFS:
        p = f"{d}/{f}"
        if not os.path.exists(p):
            print(f"  {f}: absent")
            continue
        for name, off in fields:
            v = np.asarray(u32col(p, stride, off))
            nd = int((v == NO_DATA).sum())
            vv = v[v != NO_DATA].astype(np.int64)
            t = tier_of(vv)
            per = {lay[k]["name"]: int((t == k).sum()) for k in range(len(lay))}
            bad = int((t == -1).sum())
            c = vv[t == 0]
            si = idx_of[c]
            mid = int((si < 0).sum())
            keep[si[si >= 0]] = True
            print(f"  {f}.{name}: {len(v):,} refs, NO_DATA {nd:,}, by tier {per}, out of range {bad}, "
                  f"core refs not at a string start {mid}, distinct core strings {len(np.unique(si[si >= 0])):,}")
    print(f"  UNION referenced core strings {int(keep.sum()):,} of {ns:,}; bytes kept {int(lens[keep].sum()):,} of {CL:,} "
          f"({lens[keep].sum() / CL:.2%}); unreferenced {int((~keep).sum()):,} strings, {int(lens[~keep].sum()):,} B")
    np.save(f"{W}/work/core_keep_{preset}.npy", keep)
    pn = np.fromfile(f"{d}/place_nodes.bin", PLACE_DT)
    live = pn["name_id"] != NO_DATA
    print(f"  place_nodes: {len(pn):,} slots, live {int(live.sum()):,}, hidden {int((~live).sum()):,}, "
          f"parent_poly_id NO_DATA {int((pn['parent_poly_id'] == NO_DATA).sum()):,}")
    rng = np.random.default_rng(1)
    un = np.flatnonzero(~keep)
    if len(un):
        smp = rng.choice(un, min(15, len(un)), replace=False)
        print("  sample unreferenced:", [bytes(core[starts[i]:starts[i] + lens[i] - 1]).decode('utf-8', 'replace') for i in smp])
