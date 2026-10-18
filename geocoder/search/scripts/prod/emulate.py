#!/usr/bin/env python3
"""Search-prod inputs from the planet-261006 instrumented build.

1. Flat test sets (symlinks to the shipped files) for minimal / recommended / max:
     minimal      = admin-minimal
     recommended  = admin + quality/q2.5 + poi/major
     max          = full + quality/uncapped + poi/all
2. Zero-byte builder changes, emulated from the osm_ids sidecars and the names study's
   links.tsv, written as modified copies of three record files (sizes compared after):
     admin_polygons.bin  _pad2 bit0  = the largest ring of its boundary (one search entity per boundary)
     place_nodes.bin     hidden node: _pad1 bit0 = parent_poly_id names the boundary that claims
                         (hides) the node, not the containing polygon (the label point of that boundary)
     poi_records.bin     flags 0x08  = a secondary ring of a multi-ring POI relation
3. Shipped extras (each measured separately):
     imp_places.u8 / imp_polys.u8 / imp_poi.u8   population + sitelinks + capital (places) or
                         sitelinks (POIs) part of the prototype's importance, one byte per record slot
     iso_codes.tsv       poly_idx -> ISO 3166-1 alpha-2/alpha-3 / ISO 3166-2 subdivision codes
     extra_names.tsv     kind record_idx class name: every searchable name the record's own string
                         does not already hold (the prototype corpus' name lists)
Inputs: /ssd/search/data/{corpus.tsv,iso3166.opl}, /ssd/precision/names/run-links/links.tsv."""
import collections
import os
import re
import sys
import time

import numpy as np

P = "/ssd/precision/build/out-nocap/planet/"
OUT = "/ssd/search-prod/sets/"
EXT = "/ssd/search-prod/extras/"
CORPUS = "/ssd/search/data/corpus.tsv"
NO = 0xFFFFFFFF
T0 = time.time()

pn_dt = np.dtype([("lat", "<f4"), ("lng", "<f4"), ("name_id", "<u4"), ("pt", "u1"), ("p1", "u1"), ("p2", "<u2"), ("parent", "<u4")])
ap_dt = np.dtype([("voff", "<u4"), ("vcnt", "<u4"), ("name_id", "<u4"), ("level", "u1"), ("ovr", "u1"), ("p2", "u1"), ("p3", "u1"), ("area", "<f4"), ("cc", "<u2"), ("p4", "<u2")])
slot_dt = np.dtype([("type", "u1"), ("flags", "u1"), ("tier", "u1"), ("res", "u1"), ("id", "<u8")])
rec_dt = np.dtype([("lat", "<f4"), ("lng", "<f4"), ("voff", "<u4"), ("vcnt", "<u4"), ("name", "<u4"), ("cat", "u1"), ("tier", "u1"), ("flags", "u1"), ("imp", "u1"), ("ps", "<u4"), ("pp", "<u4"), ("ppoly", "<u4")])
TYPE_LETTER = {1: "n", 2: "w", 3: "r"}
METHOD_RANK = {"rel-label": 0, "rel-wikidata": 1, "way-wikidata": 1, "name": 2}

SETS = {
    "minimal": {"mode": "admin-minimal", "quality": None, "poi": None},
    "recommended": {"mode": "admin", "quality": "q2.5", "poi": "major"},
    "max": {"mode": "full", "quality": "uncapped", "poi": "all"},
}


def log(*a):
    print(f"[{time.time() - T0:7.1f}s]", *a, file=sys.stderr, flush=True)


def slots(path):
    raw = np.fromfile(path, dtype=np.uint8)
    return np.frombuffer(raw[12:].tobytes(), dtype=slot_dt)


def osm_keys(sl):
    """'r123' style keys per slot; relation slots carry (id << 16 | ring)."""
    ids = np.where(sl["type"] == 3, sl["id"] >> np.uint64(16), sl["id"])
    return [TYPE_LETTER.get(int(t), "?") + str(int(i)) for t, i in zip(sl["type"].tolist(), ids.tolist())]


def primary_rings(keys, vcnt):
    """Index of the ring with the most vertices per key (first on ties)."""
    best = {}
    for i, (k, v) in enumerate(zip(keys, vcnt.tolist())):
        if k not in best or v > best[k][0]:
            best[k] = (v, i)
    return {k: i for k, (_, i) in best.items()}


# ------------------------------------------------------------------ importance (prototype formula)
W_TYPE, W_POPULATION, W_SITELINKS = 0.40, 0.30, 0.30
POP_FULL, SL_FULL = 7.0, 2.5
CAP_NAT, CAP_REG, AREA_POP = 0.12, 0.05, 0.5
W_POI_SITELINKS = 0.45
EXTRA_MAX = 0.75  # byte 255


def log_score(v, full):
    return min(max(np.log10(max(v, 0.0) + 1.0) / full, 0.0), 1.0)


def extra_importance(c):
    """The part of the prototype importance the shipped records cannot derive."""
    kind = c[0]
    sl = float(c[6] or 0)
    if kind == "O":
        return W_POI_SITELINKS * log_score(sl, SL_FULL)
    pop = float(c[5] or 0)
    cap = {"yes": CAP_NAT, "2": CAP_NAT, "3": CAP_REG, "4": CAP_REG}.get(c[7], 0.0)
    area = kind == "A" or c[2] in ("country", "state", "province", "region", "county")
    pw = W_POPULATION * AREA_POP if area else W_POPULATION
    return pw * log_score(pop, POP_FULL) + W_SITELINKS * log_score(sl, SL_FULL) + cap


def qbyte(x):
    return int(round(min(max(x / EXTRA_MAX, 0.0), 1.0) * 255))


# ------------------------------------------------------------------ name classes (prototype class_of)
MAJOR = set("en de fr es it pt nl ru uk pl cs sk sv da no nb nn fi el tr ar he fa hi bn zh ja ko vi th id ms hu ro bg hr sr ca eu gl et".split())


def class_of(key):
    base, _, suf = key.partition(":")
    lang = re.split(r"[-_]", suf)[0] if suf else ""
    if suf and suf != "en" and lang not in MAJOR:
        return 5
    if base == "name":
        return 0 if not suf else (1 if suf == "en" else 2)
    if base == "old_name":
        return 4
    return 3


def main():
    os.makedirs(OUT, exist_ok=True)
    os.makedirs(EXT, exist_ok=True)

    # ---------------- sidecar maps (admin mode = the full place-node list with osm ids)
    pn_admin = np.fromfile(P + "admin/place_nodes.bin", dtype=pn_dt)
    pn_keys = osm_keys(slots(P + "admin/place_nodes.osm_ids"))
    node_idx_admin = {k: i for i, k in enumerate(pn_keys)}
    log("admin place nodes", len(pn_admin))

    pn_min = np.fromfile(P + "admin-minimal/place_nodes.bin", dtype=pn_dt)
    # minimal nodes are admin's city/town/village/hamlet nodes; join on exact fields
    akey = {}
    for i, r in enumerate(pn_admin.tolist()):
        akey.setdefault((r[0], r[1], r[2], r[3]), i)
    min_to_admin = np.array([akey.get((r[0], r[1], r[2], r[3]), -1) for r in pn_min.tolist()], dtype=np.int64)
    log("minimal nodes", len(pn_min), "unmatched", int((min_to_admin < 0).sum()))
    admin_to_min = {int(a): i for i, a in enumerate(min_to_admin.tolist()) if a >= 0}

    polys = {}
    for name, qdir in (("minimal", "admin-minimal"), ("q", "quality/q2.5")):
        ap = np.fromfile(P + qdir + "/admin_polygons.bin", dtype=ap_dt)
        keys = osm_keys(slots(P + qdir + "/admin_polygons.osm_ids"))
        polys[name] = (ap, keys, primary_rings(keys, ap["vcnt"]))
        log("polygons", name, len(ap), "boundaries", len(polys[name][2]))

    # claims: boundary key -> label node (admin idx), first by method rank then node id
    claims = collections.defaultdict(list)
    for line in open("/ssd/precision/names/run-links/links.tsv"):
        m, t, bid, nid, _ = line.rstrip("\n").split("\t")
        claims[t + bid].append((METHOD_RANK.get(m, 9), int(nid)))
    label_node = {}
    for bk, lst in claims.items():
        for _, nid in sorted(lst):
            i = node_idx_admin.get("n" + str(nid))
            if i is not None and pn_admin["name_id"][i] == NO:
                label_node[bk] = i
                break
    log("claims", len(claims), "with a hidden shipped label node", len(label_node))

    # POI slots (all tier sidecar; major = tier <= 1 slots in order, notable = tier <= 2)
    psl = slots(P + "poi/all/poi_records.osm_ids")
    pkeys_all = osm_keys(psl)
    ptier = psl["tier"]
    del psl

    # ---------------- corpus rows -> record ids
    rows = collections.defaultdict(list)  # (kind, key) -> row cols
    for line in open(CORPUS, encoding="utf-8", errors="surrogateescape"):
        c = line.rstrip("\n").split("\t")
        rows[c[0]].append(c)
    log("corpus kinds", {k: len(v) for k, v in rows.items()})

    strings_core = open(P + "admin/strings_core.bin", "rb").read()
    strings_poi = open(P + "poi/all/strings_poi.bin", "rb").read()
    import json
    lay = json.load(open(P + "poi/all/strings_layout.json"))["tiers"]
    poi_base = next(t["start"] for t in lay if t["name"] == "poi")

    def sget(buf, off, base=0):
        if off == NO:
            return ""
        o = off - base
        e = buf.index(b"\0", o)
        return buf[o:e].decode("utf-8", "surrogateescape")

    stats = collections.Counter()
    for set_name, cfg in SETS.items():
        d = OUT + set_name + "/"
        e = EXT + set_name + "/"
        os.makedirs(d, exist_ok=True)
        os.makedirs(e, exist_ok=True)
        mode = cfg["mode"]
        srcs = [P + mode]
        if cfg["quality"]:
            srcs.append(P + "quality/" + cfg["quality"])
        if cfg["poi"]:
            srcs.append(P + "poi/" + cfg["poi"])
        for s in srcs:
            for f in os.listdir(s):
                if f.endswith(".osm_ids") or f in ("patch.gcpatch",):
                    continue
                dst = d + f
                if os.path.lexists(dst):
                    os.remove(dst)
                os.symlink(s + "/" + f, dst)

        is_min = set_name == "minimal"
        pn = (pn_min if is_min else pn_admin).copy()
        ap, pkeys, prim = polys["minimal" if is_min else "q"]
        ap = ap.copy()
        if not is_min and cfg["quality"] == "uncapped":
            ap_unc = np.fromfile(P + "quality/uncapped/admin_polygons.bin", dtype=ap_dt)
            assert (ap_unc["name_id"] == ap["name_id"]).all()
            ap = ap_unc.copy()

        # E1 primary ring flag
        prim_set = np.zeros(len(ap), dtype=bool)
        prim_set[list(prim.values())] = True
        ap["p2"] = np.where(prim_set, ap["p2"] | 1, ap["p2"] & 0xFE)
        # E2 hidden node -> claiming boundary (primary ring)
        linked = 0
        for bk, ai in label_node.items():
            pi = prim.get(bk)
            if pi is None:
                continue
            ni = ai if not is_min else admin_to_min.get(ai)
            if ni is None:
                continue
            pn["parent"][ni] = pi
            pn["p1"][ni] = pn["p1"][ni] | 1
            linked += 1
        stats[set_name + " linked hidden nodes"] = linked
        for f, arr in (("place_nodes.bin", pn), ("admin_polygons.bin", ap)):
            os.remove(d + f)
            arr.tofile(d + f)

        # E3 POI secondary rings
        poi_idx = {}
        pr = None
        if cfg["poi"]:
            lim = {"major": 1, "notable": 2, "all": 3}[cfg["poi"]]
            sel = np.nonzero(ptier <= lim)[0]
            pr = np.fromfile(P + "poi/" + cfg["poi"] + "/poi_records.bin", dtype=rec_dt)
            assert len(sel) == len(pr), (len(sel), len(pr))
            keys = [pkeys_all[j] for j in sel.tolist()]
            pprim = primary_rings(keys, pr["vcnt"])
            sec = np.ones(len(pr), dtype=bool)
            sec[list(pprim.values())] = False
            pr["flags"] = np.where(sec, pr["flags"] | 8, pr["flags"] & 0xF7)
            stats[set_name + " poi secondary rings"] = int(sec.sum())
            os.remove(d + "poi_records.bin")
            pr.tofile(d + "poi_records.bin")
            poi_idx = pprim
            del keys

        # ---------------- extras
        node_idx = node_idx_admin if not is_min else {k: admin_to_min[i] for k, i in node_idx_admin.items() if i in admin_to_min}
        imp_pl = np.zeros(len(pn), dtype=np.uint8)
        imp_po = np.zeros(len(ap), dtype=np.uint8)
        imp_poi = np.zeros(0 if pr is None else len(pr), dtype=np.uint8)
        names_out = open(e + "extra_names.tsv", "w", encoding="utf-8", errors="surrogateescape")
        n_extra = collections.Counter()
        for kind in ("P", "M", "B", "A", "O"):
            for c in rows.get(kind, []):
                key = c[1]
                if kind in "PM":
                    rec = node_idx.get(key)
                    if rec is None:
                        continue
                    base = sget(strings_core, int(pn["name_id"][rec]))
                    imp_pl[rec] = qbyte(extra_importance(c))
                    k = "p"
                elif kind in "BA":
                    rec = prim.get(key)
                    if rec is None:
                        continue
                    base = sget(strings_core, int(ap["name_id"][rec]))
                    imp_po[rec] = qbyte(extra_importance(c))
                    k = "b"
                else:
                    rec = poi_idx.get(key)
                    if rec is None:
                        continue
                    base = sget(strings_poi, int(pr["name"][rec]), poi_base)
                    imp_poi[rec] = qbyte(extra_importance(c))
                    k = "o"
                stats[f"{set_name} rows {kind} matched"] += 1
                seen = set()
                for kv in c[11].split("\x1f"):
                    kk, _, v = kv.partition("=")
                    if not v or v in seen:
                        continue
                    seen.add(v)
                    if base is not None and v == base:
                        n_extra["base_equal"] += 1
                        continue
                    names_out.write(f"{k}\t{rec}\t{class_of(kk)}\t{kk}\t{v}\n")
                    n_extra[k] += 1
        names_out.close()
        imp_pl.tofile(e + "imp_places.u8")
        imp_po.tofile(e + "imp_polys.u8")
        if len(imp_poi):
            imp_poi.tofile(e + "imp_poi.u8")
        stats[set_name + " extra names"] = dict(n_extra)
        log(set_name, "done")

    # ---------------- ISO codes (country alpha-2/alpha-3, subdivision codes) for each polygon set
    ESC = re.compile(r"%([0-9a-f]+)%")
    iso = {}
    for line in open("/ssd/search/data/iso3166.opl", encoding="utf-8"):
        parts = line.rstrip("\n").split(" ")
        rid = parts[0][1:]
        t = next((p for p in parts if p.startswith("T")), "T")
        tags = {}
        if len(t) > 1:
            for kv in t[1:].split(","):
                kk, _, v = kv.partition("=")
                tags[ESC.sub(lambda m: chr(int(m.group(1), 16)), kk)] = ESC.sub(lambda m: chr(int(m.group(1), 16)), v)
        codes = set()
        a2 = tags.get("ISO3166-1:alpha2") or tags.get("ISO3166-1", "")
        if tags.get("admin_level") == "2" and len(a2) == 2:
            codes.add(a2.lower())
            if tags.get("ISO3166-1:alpha3"):
                codes.add(tags["ISO3166-1:alpha3"].lower())
        iso2 = tags.get("ISO3166-2", "")
        if "-" in iso2:
            sub = iso2.split("-", 1)[1]
            if sub.isalpha() and len(sub) >= 2:
                codes.add(sub.lower())
        if codes:
            iso["r" + rid] = codes
    for set_name in SETS:
        ap, pkeys, prim = polys["minimal" if set_name == "minimal" else "q"]
        n = 0
        with open(EXT + set_name + "/iso_codes.tsv", "w") as o:
            for bk, codes in sorted(iso.items()):
                pi = prim.get(bk)
                if pi is not None:
                    o.write(f"{pi}\t{';'.join(sorted(codes))}\n")
                    n += 1
        stats[set_name + " iso polygons"] = n
    for k, v in stats.items():
        print(k, v)


if __name__ == "__main__":
    main()
