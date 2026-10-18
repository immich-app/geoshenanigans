#!/usr/bin/env python3
"""zstd -18 (the publish level, builder/tools/compress_index.py:40) of content slices of
the admin-minimal set, to attribute each file's download size to its contents.

Usage: zstd_shares.py <minimal dir> <strcls.npy from inventory.py> <work dir> <out json>
"""
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
from inventory import POLY_DT, PLACE_DT, NO_DATA, load_polys, load_strings, entries_per_cell  # noqa: E402

LEVEL = "-18"


def zsize(path):
    r = subprocess.run([os.environ.get("ZSTD", "zstd"), LEVEL, "-T1", "-c", "-q", path], check=True, stdout=subprocess.PIPE)
    return len(r.stdout)


def main():
    mdir, clsf, work, out = sys.argv[1:5]
    os.makedirs(work, exist_ok=True)
    jobs = {}  # name -> (path, raw bytes)

    def emit(name, data):
        path = f"{work}/{name}.bin"
        with open(path, "wb") as f:
            f.write(data)
        jobs[name] = (path, len(data))

    # --- vertices by level, and L8 split by bbox class ---
    p, v, live, has, enc, nbytes = load_polys(mdir)
    vbytes = v.tobytes()
    for L in range(2, 9):
        idx = np.flatnonzero(has & (p["level"] == L))
        idx = idx[np.argsort(p["voff"][idx])]
        emit(f"vert_L{L}", b"".join(vbytes[int(p["voff"][i]):int(p["voff"][i]) + int(nbytes[i])] for i in idx))
    for e, nm in [(2, "tiny"), (0, "u16_1m"), (1, "u16_11m"), (3, "u32")]:
        idx = np.flatnonzero(has & (enc == e))
        idx = idx[np.argsort(p["voff"][idx])]
        emit(f"vert_enc_{nm}", b"".join(vbytes[int(p["voff"][i]):int(p["voff"][i]) + int(nbytes[i])] for i in idx))
    emit("vert_all", vbytes)

    # --- admin_polygons records by level (24 B each, slot order) ---
    raw = np.fromfile(f"{mdir}/admin_polygons.bin", dtype=np.uint8).reshape(-1, 24)
    for L in range(2, 9):
        emit(f"polyrec_L{L}", raw[live & (p["level"] == L)].tobytes())
    emit("polyrec_all", raw.tobytes())

    # --- strings by attribution class ---
    cls = np.load(clsf)
    sb, starts, lens = load_strings(f"{mdir}/strings_core.bin")
    sbytes = sb.tobytes()

    def strings_of(mask):
        idx = np.flatnonzero(mask)
        return b"".join(sbytes[int(starts[i]):int(starts[i]) + int(lens[i]) + 1] for i in idx)

    used = (cls & 3) != 0
    emit("str_used_min", strings_of(used))
    emit("str_unused_min", strings_of(~used))
    emit("str_admin_only", strings_of((cls & 3) == 1))
    emit("str_place_only", strings_of((cls & 3) == 2))
    emit("str_both", strings_of((cls & 3) == 3))
    emit("str_all", sbytes)

    # --- place nodes: columns and subsets ---
    pn = np.fromfile(f"{mdir}/place_nodes.bin", dtype=PLACE_DT)
    prow = np.fromfile(f"{mdir}/place_nodes.bin", dtype=np.uint8).reshape(-1, 20)
    emit("pn_all", prow.tobytes())
    emit("pn_col_latlng", np.ascontiguousarray(prow[:, 0:8]).tobytes())
    emit("pn_col_name", np.ascontiguousarray(prow[:, 8:12]).tobytes())
    emit("pn_col_type_pad", np.ascontiguousarray(prow[:, 12:16]).tobytes())
    emit("pn_col_parent", np.ascontiguousarray(prow[:, 16:20]).tobytes())
    named = pn["name"] != NO_DATA
    emit("pn_named", prow[named].tobytes())
    for t, nm in [(0, "city"), (1, "town"), (2, "village"), (4, "hamlet")]:
        emit(f"pn_type_{nm}", prow[pn["pt"] == t].tobytes())
    # 13-byte packed form (lat, lng, name, type) for named nodes only — what the reader needs
    emit("pn_named_13B", np.ascontiguousarray(prow[named][:, 0:13]).tobytes())

    # --- entries files: per-level / per-type id streams (approximate attribution) ---
    rows, counts, ids = entries_per_cell(mdir, "admin")
    lvl = p["level"][(ids & 0x7FFFFFFF).astype(np.int64)]
    for L in range(2, 9):
        emit(f"aent_L{L}", ids[lvl == L].tobytes())
    emit("aent_all_ids", ids.tobytes())
    emit("admin_entries", np.fromfile(f"{mdir}/admin_entries.bin", dtype=np.uint8).tobytes())
    prow_c, pcounts, pids = entries_per_cell(mdir, "place")
    for t, nm in [(0, "city"), (1, "town"), (2, "village"), (4, "hamlet")]:
        emit(f"pent_{nm}", pids[pn["pt"][pids] == t].tobytes())

    with ThreadPoolExecutor(max_workers=16) as ex:
        sizes = dict(zip(jobs, ex.map(lambda k: zsize(jobs[k][0]), jobs)))
    R = {k: {"raw": jobs[k][1], "zst": sizes[k]} for k in jobs}
    for k in jobs:
        os.remove(jobs[k][0])
    json.dump(R, open(out, "w"), indent=1)
    for k, r in R.items():
        print(f"{k:24s} raw {r['raw']:>12,d}  zst {r['zst']:>12,d}  ratio {r['zst'] / max(r['raw'], 1):.3f}")


if __name__ == "__main__":
    main()
