#!/usr/bin/env python3
"""Size (raw and zstd -18, the publish level: builder/tools/compress_index.py:40) of the
admin-minimal file set after content-level pruning, by writing the pruned files in the current
on-disk formats (dense ids in slot order, cell index rebuilt from the surviving (cell, id) pairs,
strings kept in sorted order) and compressing them. Encodings are unchanged.

Scenarios (cumulative unless noted):
  0  repack as-is (checks the rewriter: should match the published sizes)
  A  drop records the reader can never return (place nodes with name_id = NO_DATA: linked/hidden
     nodes + strategy-2 tombstones; admin tombstones) and core strings nothing left references.
     Reader output identical by construction (lib.rs:1080-1115).
  B  A + zero PlaceNode.parent_poly_id (the reader never reads it: only PoiRecord's is used,
     lib.rs:2334-2335).
  C  B + drop admin_level 3 rings
  D  B + drop rings whose output field (default rank table + place_type_override) Immich
     doesn't read
  E  D + drop place=hamlet nodes
Measured Immich output change for C/D/E: see ablate3.py.

Usage: prune2.py <minimal dir> <work dir> <out json>
"""
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
from inventory import POLY_DT, PLACE_DT, NO_DATA, load_polys, load_strings, entries_per_cell  # noqa: E402
from ablate3 import OVERRIDE, DEFAULT_FIELD, IMMICH_FIELDS  # noqa: E402

ROW = np.dtype([("cell", "<u8"), ("off", "<u4")])
FILES = ["admin_cells.bin", "admin_entries.bin", "admin_polygons.bin", "admin_vertices.bin",
         "place_cells.bin", "place_entries.bin", "place_nodes.bin", "strings_core.bin"]


def zsize(path):
    r = subprocess.run([os.environ.get("ZSTD", "zstd"), "-18", "-T1", "-c", "-q", path], check=True, stdout=subprocess.PIPE)
    return len(r.stdout)


def write_index(path_cells, path_entries, cell_of_id, ids):
    order = np.lexsort((ids, cell_of_id))
    cells, ids = cell_of_id[order], ids[order]
    uniq, start, counts = np.unique(cells, return_index=True, return_counts=True)
    rows = np.zeros(len(uniq), dtype=ROW)
    rows["cell"] = uniq
    sizes = 2 + 4 * counts.astype(np.int64)
    offs = np.zeros(len(uniq), dtype=np.int64)
    offs[1:] = np.cumsum(sizes)[:-1]
    rows["off"] = offs.astype(np.uint32)
    buf = np.zeros(int(sizes.sum()), dtype=np.uint8)
    buf[offs] = (counts & 0xFF).astype(np.uint8)
    buf[offs + 1] = (counts >> 8).astype(np.uint8)
    rep = np.repeat(offs + 2, counts) + 4 * (np.arange(len(ids)) - np.repeat(start, counts))
    for b in range(4):
        buf[rep + b] = ((ids >> (8 * b)) & 0xFF).astype(np.uint8)
    rows.tofile(path_cells)
    buf.tofile(path_entries)


def main():
    mdir, work, out = sys.argv[1:4]
    os.makedirs(work, exist_ok=True)
    p, v, live, has, enc, nbytes = load_polys(mdir)
    pn = np.fromfile(f"{mdir}/place_nodes.bin", dtype=PLACE_DT)
    sb, sstarts, slens = load_strings(f"{mdir}/strings_core.bin")
    sbytes = sb.tobytes()
    vb = v.tobytes()
    arows, acounts, aids = entries_per_cell(mdir, "admin")
    acell = np.repeat(arows["cell"], acounts)
    prows, pcounts, pids = entries_per_cell(mdir, "place")
    pcell = np.repeat(prows["cell"], pcounts)

    field = [OVERRIDE.get(int(o)) if o else DEFAULT_FIELD.get(int(L)) for L, o in zip(p["level"], p["pto"])]
    unused = live & np.array([f not in IMMICH_FIELDS for f in field])
    l3 = live & (p["level"] == 3)

    scen = {
        "0": dict(keep_dead=True),
        "A": dict(),
        "B": dict(zero_parent=True),
        "C": dict(zero_parent=True, drop=l3),
        "D": dict(zero_parent=True, drop=unused),
        "E": dict(zero_parent=True, drop=unused, drop_ptypes=[4]),
    }
    R = {}
    for name, cfg in scen.items():
        d = f"{work}/{name}"
        os.makedirs(d, exist_ok=True)
        keep_dead = cfg.get("keep_dead", False)
        keep_poly = (live & has) | (keep_dead & ~(live & has))
        if "drop" in cfg:
            keep_poly = keep_poly & ~cfg["drop"]
        keep_place = (pn["name"] != NO_DATA) | keep_dead
        if cfg.get("drop_ptypes"):
            keep_place = keep_place & ~np.isin(pn["pt"], cfg["drop_ptypes"])
        kidx = np.flatnonzero(keep_poly)
        remap = np.full(len(p), NO_DATA, dtype=np.uint32)
        remap[kidx] = np.arange(len(kidx), dtype=np.uint32)
        newp = p[kidx].copy()
        chunks, at = [], 0
        for j, i in enumerate(kidx):
            if not has[i]:
                continue
            o, n = int(p["voff"][i]), int(nbytes[i])
            chunks.append(vb[o:o + n])
            newp["voff"][j] = at
            at += n
        used = np.zeros(len(sstarts), dtype=bool)
        if keep_dead:
            used[:] = True
        else:
            for offs in (newp["name"], pn["name"][keep_place]):
                used[np.searchsorted(sstarts, offs[offs != NO_DATA])] = True
        new_off = np.zeros(len(sstarts), dtype=np.int64)
        new_off[used] = np.concatenate(([0], np.cumsum(slens[used] + 1)[:-1]))
        nm = newp["name"].astype(np.int64)
        ok = nm != NO_DATA
        newp["name"][ok] = new_off[np.searchsorted(sstarts, nm[ok])].astype(np.uint32)
        with open(f"{d}/admin_vertices.bin", "wb") as f:
            f.write(b"".join(chunks))
        newp.tofile(f"{d}/admin_polygons.bin")
        with open(f"{d}/strings_core.bin", "wb") as f:
            f.write(b"".join(sbytes[int(sstarts[i]):int(sstarts[i]) + int(slens[i]) + 1]
                             for i in np.flatnonzero(used)))
        pid = (aids & 0x7FFFFFFF).astype(np.int64)
        flag = aids & 0x80000000
        k = keep_poly[pid]
        write_index(f"{d}/admin_cells.bin", f"{d}/admin_entries.bin", acell[k], remap[pid[k]] | flag[k])
        pk = np.flatnonzero(keep_place)
        premap = np.full(len(pn), NO_DATA, dtype=np.uint32)
        premap[pk] = np.arange(len(pk), dtype=np.uint32)
        newpn = pn[pk].copy()
        nm = newpn["name"].astype(np.int64)
        okn = nm != NO_DATA
        newpn["name"][okn] = new_off[np.searchsorted(sstarts, nm[okn])].astype(np.uint32)
        if cfg.get("zero_parent"):
            newpn["parent"] = 0
        else:
            par = newpn["parent"].astype(np.int64)
            okp = par != NO_DATA
            newpn["parent"][okp] = remap[par[okp]]
        newpn.tofile(f"{d}/place_nodes.bin")
        kk = keep_place[pids]
        write_index(f"{d}/place_cells.bin", f"{d}/place_entries.bin", pcell[kk], premap[pids[kk]])
        with ThreadPoolExecutor(max_workers=8) as ex:
            zs = list(ex.map(lambda fn: zsize(f"{d}/{fn}"), FILES))
        R[name] = {fn: {"raw": os.path.getsize(f"{d}/{fn}"), "zst": z} for fn, z in zip(FILES, zs)}
        tr = sum(x["raw"] for x in R[name].values()) + 460
        tz = sum(x["zst"] for x in R[name].values()) + 190
        R[name]["total"] = {"raw": tr, "zst": tz}
        R[name]["kept"] = {"rings": int(keep_poly.sum()), "places": int(keep_place.sum()), "strings": int(used.sum())}
        print(name, f"raw {tr / 2**20:.1f} MiB  zst {tz / 2**20:.2f} MiB", R[name]["kept"],
              {fn: round(x["zst"] / 2**20, 2) for fn, x in R[name].items() if fn in FILES})
        for fn in FILES:
            os.remove(f"{d}/{fn}")
    json.dump(R, open(out, "w"), indent=1)


if __name__ == "__main__":
    main()
