#!/usr/bin/env python3
"""Which admin-minimal contents feed Immich's (country, state, city)?

Hides admin levels / place types in copies of the admin-minimal dir (name_id = NO_DATA, which the
reader's admin_polygon()/place_node() treat as absent: server/src/lib.rs:1080-1115), queries every
variant with the real read path (grid_query) and maps each answer with Immich's prototype mapper
(immich-geocoder server/src/repositories/geocoding.repository.ts:66-80):
  country <- country_code (i18n name), state <- state ?? province,
  city <- city ?? town ?? village ?? hamlet ?? municipality.
Also compares admin-minimal against the admin-mode recommended set (all levels, q2.5) on the
same points.

Sample points are a proxy for photo locations (no real photo data):
  urban    city/town nodes of the set, jittered +-0.03 deg
  village  village nodes, +-0.02 deg
  hamlet   hamlet nodes, +-0.01 deg
  land     uniform on the sphere, kept where the base answer has a country (area-weighted)

Usage: ablate3.py <minimal dir> <recommended dir> <grid_query> <work dir> <out json>
"""
import json
import os
import subprocess
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import numpy as np

NO_DATA = 0xFFFFFFFF
POLY_DT = np.dtype([("voff", "<u4"), ("vcnt", "<u4"), ("name", "<u4"), ("level", "u1"), ("pto", "u1"),
                    ("pad", "<u2"), ("area", "<f4"), ("cc", "<u2"), ("pad2", "<u2")])
PLACE_DT = np.dtype([("lat", "<f4"), ("lng", "<f4"), ("name", "<u4"), ("pt", "u1"), ("pad1", "u1"),
                     ("pad2", "<u2"), ("parent", "<u4")])
FILES = ["admin_cells.bin", "admin_entries.bin", "admin_polygons.bin", "admin_vertices.bin", "place_cells.bin",
         "place_entries.bin", "place_nodes.bin", "strings_core.bin", "strings_layout.json"]
# AdminPlaceType (builder/src/types.h:63-79) -> field (server place_type_to_field, lib.rs:427-445)
OVERRIDE = {1: "city", 2: "town", 3: "village", 4: "suburb", 5: "neighbourhood", 6: "quarter", 7: "state",
            8: "province", 9: "region", 10: "county", 11: "district", 12: "borough", 13: "hamlet",
            14: "municipality"}
# server/admin_levels.json "default" rank -> Nominatim ADMIN_LABELS (rank // 2), lib.rs:447-451
DEFAULT_FIELD = {2: "country", 3: "region", 4: "state", 5: "state_district", 6: "county", 7: "municipality",
                 8: "city"}
IMMICH_FIELDS = {"state", "province", "city", "town", "village", "hamlet", "municipality", "country"}

VARIANTS = {
    "-L3": dict(levels=[3]),
    "-L4": dict(levels=[4]),
    "-L5": dict(levels=[5]),
    "-L6": dict(levels=[6]),
    "-L7": dict(levels=[7]),
    "-L8": dict(levels=[8]),
    "-L5,6": dict(levels=[5, 6]),
    "-L3,5,6": dict(levels=[3, 5, 6]),
    "-L3,5,6,7": dict(levels=[3, 5, 6, 7]),
    "-city nodes": dict(ptypes=[0]),
    "-town nodes": dict(ptypes=[1]),
    "-village nodes": dict(ptypes=[2]),
    "-hamlet nodes": dict(ptypes=[4]),
    "-village+hamlet nodes": dict(ptypes=[2, 4]),
    "-rings w/o Immich field": dict(static_unused=True),
    "-rings w/o Immich field, -hamlet": dict(static_unused=True, ptypes=[4]),
    "-L3,5,6, -hamlet": dict(levels=[3, 5, 6], ptypes=[4]),
    "-all place nodes": dict(ptypes=[0, 1, 2, 4]),
    "-L5,6,7,8": dict(levels=[5, 6, 7, 8]),
}


def immich(a):
    cc = a.get("country_code")
    state = a.get("state") or a.get("province")
    city = next((a[k] for k in ("city", "town", "village", "hamlet", "municipality") if a.get(k)), None)
    return (cc.upper() if cc else None, state, city)


def src(a):
    s = next((k for k in ("state", "province") if a.get(k)), "none")
    c = next((k for k in ("city", "town", "village", "hamlet", "municipality") if a.get(k)), "none")
    return s, c


def run(gq, d, coords):
    r = subprocess.run([gq, d], input=coords.encode(), stdout=subprocess.PIPE, check=True)
    return [json.loads(line.split(" ", 2)[2]).get("address", {}) for line in r.stdout.decode().splitlines()]


def fmt(pts):
    return "".join(f"{la:.6f} {ln:.6f}\n" for la, ln in pts)


def main():
    mdir, rdir, gq, work, out = sys.argv[1:6]
    os.makedirs(work, exist_ok=True)
    polys = np.fromfile(f"{mdir}/admin_polygons.bin", dtype=POLY_DT)
    places = np.fromfile(f"{mdir}/place_nodes.bin", dtype=PLACE_DT)
    rng = np.random.default_rng(20261013)

    strata = {}
    named = places["name"] != NO_DATA
    for name, types, n, jit in [("urban", [0, 1], 10000, 0.03), ("village", [2], 7500, 0.02),
                                ("hamlet", [4], 7500, 0.01)]:
        idx = np.flatnonzero(named & np.isin(places["pt"], types))
        pick = rng.choice(idx, size=n, replace=False)
        lat = places["lat"][pick].astype(np.float64) + rng.uniform(-jit, jit, n)
        lng = places["lng"][pick].astype(np.float64) + rng.uniform(-jit, jit, n)
        strata[name] = list(zip(lat, lng))
    # uniform on the sphere, kept where base has a country
    u = rng.uniform(-1, 1, 60000)
    cand = list(zip(np.degrees(np.arcsin(u)), rng.uniform(-180, 180, 60000)))
    res = run(gq, mdir, fmt(cand))
    strata["land"] = [p for p, a in zip(cand, res) if a.get("country_code")][:10000]
    land_frac = sum(1 for a in res if a.get("country_code")) / len(res)

    order, pts = [], []
    for s, p in strata.items():
        order += [s] * len(p)
        pts += p
    coords = fmt(pts)
    open(f"{work}/coords.txt", "w").write(coords)
    order = np.array(order)

    live = polys["name"] != NO_DATA
    field = np.array([OVERRIDE.get(int(o)) if o else DEFAULT_FIELD.get(int(L)) for L, o in
                      zip(polys["level"], polys["pto"])], dtype=object)
    static_unused = live & np.array([f not in IMMICH_FIELDS for f in field])

    def build(vname):
        cfg = VARIANTS[vname]
        d = f"{work}/v_" + "".join(ch if ch.isalnum() else "_" for ch in vname)
        os.makedirs(d, exist_ok=True)
        hide = np.isin(polys["level"], cfg.get("levels", []))
        if cfg.get("static_unused"):
            hide |= static_unused
        for fn in FILES:
            dst = f"{d}/{fn}"
            if os.path.lexists(dst):
                os.remove(dst)
            if fn == "admin_polygons.bin" and hide.any():
                p = polys.copy()
                p["name"][hide] = NO_DATA
                p.tofile(dst)
            elif fn == "place_nodes.bin" and cfg.get("ptypes"):
                q = places.copy()
                q["name"][np.isin(q["pt"], cfg["ptypes"])] = NO_DATA
                q.tofile(dst)
            else:
                os.symlink(os.path.abspath(f"{mdir}/{fn}"), dst)
        return vname, run(gq, d, coords)

    with ThreadPoolExecutor(max_workers=8) as ex:
        results = dict(ex.map(build, VARIANTS))
    base = run(gq, mdir, coords)
    results["admin mode (recommended set) vs minimal"] = run(gq, rdir, coords)

    R = {"n": {s: int((order == s).sum()) for s in strata}, "land_fraction_of_sphere_with_country": land_frac,
         "static_unused_rings": int(static_unused.sum()), "base": {}, "variants": {}}
    for s in strata:
        ids = np.flatnonzero(order == s)
        sc = Counter(src(base[i]) for i in ids)
        R["base"][s] = {
            "country": float(np.mean([bool(base[i].get("country_code")) for i in ids])),
            "state_from": dict(Counter(k[0] for k in sc.elements())),
            "city_from": dict(Counter(k[1] for k in sc.elements())),
        }
    for vname, res in results.items():
        R["variants"][vname] = {}
        for s in strata:
            ids = np.flatnonzero(order == s)
            c = Counter()
            for i in ids:
                a, b = immich(base[i]), immich(res[i])
                c["country"] += a[0] != b[0]
                c["state"] += a[1] != b[1]
                c["state_lost"] += a[1] is not None and b[1] is None
                c["city"] += a[2] != b[2]
                c["city_lost"] += a[2] is not None and b[2] is None
                c["city_gained"] += a[2] is None and b[2] is not None
                c["any"] += a != b
            R["variants"][vname][s] = {k: c[k] / len(ids) for k in
                                       ("country", "state", "state_lost", "city", "city_lost", "city_gained", "any")}
        # top countries for city changes (all strata)
        cc = Counter(immich(base[i])[0] for i in range(len(pts)) if immich(base[i])[2] != immich(res[i])[2])
        R["variants"][vname]["city_change_top_cc"] = cc.most_common(8)
    json.dump(R, open(out, "w"), indent=1)

    print("n:", R["n"], "land frac", round(land_frac, 3), "static-unused rings", R["static_unused_rings"])
    for s, v in R["base"].items():
        print("base", s, json.dumps(v))
    hdr = "variant".ljust(42) + "".join(f"| {s:>7s} any/state/city/lost " for s in strata)
    print(hdr)
    for vname, per in R["variants"].items():
        row = vname.ljust(42)
        for s in strata:
            m = per[s]
            row += f"| {m['any']*100:5.2f} {m['state']*100:5.2f} {m['city']*100:5.2f} {m['city_lost']*100:5.2f} "
        print(row, per["city_change_top_cc"][:5])


if __name__ == "__main__":
    main()
