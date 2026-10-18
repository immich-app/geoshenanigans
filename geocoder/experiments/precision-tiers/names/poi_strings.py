#!/usr/bin/env python3
"""Step "names": POI levels for the search-file size measurement.

Tier per POI from the shipped sidecar /ssd/precision/build/out-nocap/planet/poi/all/
poi_records.osm_ids (12-byte slots: type, flags, tier, reserved, u64 id; every POI
record of the build, tiers 1-3 ship); names from names.tsv POI rows.
Writes files/{fm,dict}_<set>_L<tier>.txt (tier 1 major, 2 notable, 3 rest of
"all"), out/levels_poi.tsv, out/poi_coverage.tsv, out/poi_signals.tsv."""
import collections
import re
import subprocess
import sys
import time

import numpy as np

B = "/ssd/precision/build/"
N = "/ssd/precision/names/"
OUT = N + "out/"
FILES = N + "files/"
T0 = time.time()


def log(*a):
    print(f"[{time.time() - T0:7.1f}s]", *a, file=sys.stderr, flush=True)


ALT_BASES = {"int_name", "alt_name", "old_name", "official_name", "short_name", "loc_name", "reg_name",
             "nat_name", "sorting_name"}
BASE_SET = ("name", "name:en", "int_name", "alt_name", "official_name", "old_name")
LANG_RE = re.compile(r"^[a-z]{2,3}(?:[-_][A-Za-z0-9]+)*$")
top10 = open(N + "top10.txt").read().split()
TOP10_KEYS = set(BASE_SET) | {"name:" + l for l in top10}
TYPE = {"n": 1, "w": 2, "r": 3}


def is_lang(s):
    return bool(LANG_RE.match(s))


def in_all(k):
    if k == "name":
        return True
    if k.startswith("name:"):
        return is_lang(k[5:])
    base, _, suf = k.partition(":")
    return base in ALT_BASES and (suf == "" or is_lang(suf))


def unescape(s):
    if "\\" not in s:
        return s
    out, i = [], 0
    while i < len(s):
        c = s[i]
        if c == "\\" and i + 1 < len(s):
            d = s[i + 1]
            out.append({"t": "\t", "n": "\n", "r": "\r"}.get(d, d))
            i += 2
        else:
            out.append(c)
            i += 1
    return "".join(out)


def clean(s):
    return s.replace("\n", " ").replace("\r", " ").replace("\t", " ").replace("\x1f", " ")


def distinct(tags, keep):
    seen, out = set(), []
    for k, v in tags:
        if not keep(k):
            continue
        for p in v.split(";"):
            p = p.strip(" ")
            if p and p not in seen:
                seen.add(p)
                out.append(p)
    return out


# ---------------------------------------------------------------- tiers
slot_dt = np.dtype([("type", "u1"), ("flags", "u1"), ("tier", "u1"), ("res", "u1"), ("id", "<u8")])
raw = np.fromfile(B + "out-nocap/planet/poi/all/poi_records.osm_ids", dtype=np.uint8)
slots = np.frombuffer(raw[12:].tobytes(), dtype=slot_dt)
live = (slots["flags"] & 1) == 0
# A relation POI's slots carry (relation id << 16 | ring index), one per ring
# (like admin polygons); names.tsv keys the relation by its id, so fold rings.
ids = slots["id"].copy()
is_rel = slots["type"] == 3
ids[is_rel] >>= np.uint64(16)
keys = (slots["type"].astype(np.uint64) << np.uint64(56)) | ids
keys, tiers = keys[live], slots["tier"][live]
log("relation POI ring records", int(is_rel.sum()), "relations", len(np.unique(ids[is_rel])))
o = np.lexsort((tiers, keys))
keys, tiers = keys[o], tiers[o]
first = np.ones(len(keys), bool)
first[1:] = keys[1:] != keys[:-1]
log("sidecar slots", len(slots), "live", int(live.sum()), "distinct features", int(first.sum()),
    "features with several records (relation rings)", int(len(keys) - first.sum()))
keys, tiers = keys[first], tiers[first]  # lowest (best) tier per osm id
log("tiers of distinct ids", {int(t): int(c) for t, c in zip(*np.unique(tiers, return_counts=True))})

# ---------------------------------------------------------------- pass 1: row keys
log("pass 1")
row_keys = []
with open(B + "names/names.tsv", "rb") as f:
    for line in f:
        if not line.startswith(b"poi\t"):
            continue
        p = line.split(b"\t", 4)
        row_keys.append((TYPE[p[1].decode()] << 56) | int(p[2]))
row_keys = np.array(row_keys, dtype=np.uint64)
i = np.searchsorted(keys, row_keys)
i[i >= len(keys)] = 0
row_tier = np.where(keys[i] == row_keys, tiers[i], 255).astype(np.uint8)
log("POI rows", len(row_keys), "tiers", {int(t): int(c) for t, c in zip(*np.unique(row_tier, return_counts=True))})

# ---------------------------------------------------------------- pass 2: strings
SETS = ("base", "top10", "all")
KEEP = {"base": lambda k: k in BASE_SET, "top10": lambda k: k in TOP10_KEYS, "all": in_all}
fh = {}
for s in SETS:
    for t in (1, 2, 3):
        fh[("fm", s, t)] = open(FILES + f"fm_{s}_L{t}.unsorted", "w", encoding="utf-8", errors="surrogateescape")
        fh[("dict", s, t)] = open(FILES + f"dict_{s}_L{t}.txt", "w", encoding="utf-8", errors="surrogateescape")
stats = collections.defaultdict(lambda: [0, 0, 0])
cov = collections.defaultdict(collections.Counter)
nstr = collections.defaultdict(list)
shipped_mismatch = 0
seen = set()
log("pass 2")
with open(B + "names/names.tsv", encoding="utf-8", errors="surrogateescape") as f:
    r = -1
    for line in f:
        if not line.startswith("poi\t"):
            continue
        r += 1
        t = int(row_tier[r])
        p = line.rstrip("\n").split("\t")
        if (p[4] == "1") != (t <= 3):
            shipped_mismatch += 1
        if t > 3:
            continue
        k = int(row_keys[r])
        if k in seen:
            continue
        seen.add(k)
        kv = p[5:]
        tags = [(kv[j], unescape(kv[j + 1])) for j in range(0, len(kv) - 1, 2)]
        keyset = {x for x, _ in tags}
        langs = {x[5:] for x in keyset if x.startswith("name:") and is_lang(x[5:])}
        c = cov[t]
        c["features"] += 1
        c["name:en"] += "name:en" in keyset
        c["int_name"] += "int_name" in keyset
        c["alt_name"] += "alt_name" in keyset
        c["lang_not_en"] += bool(langs - {"en"})
        c["langs>=2"] += len(langs) >= 2
        c["langs>=5"] += len(langs) >= 5
        c["langs>=10"] += len(langs) >= 10
        for s in SETS:
            strs = [clean(x) for x in distinct(tags, KEEP[s])]
            nstr[(s, t)].append(len(strs))
            if not strs:
                continue
            fh[("fm", s, t)].write(f"{k}\t" + "\x1f".join(strs) + "\n")
            fh[("dict", s, t)].write("\n".join(strs) + "\n")
            st = stats[(s, t)]
            st[0] += 1
            st[1] += len(strs)
            st[2] += sum(len(x.encode("utf-8", "surrogateescape")) for x in strs)
for h in fh.values():
    h.close()
log("pass 2 done; names.tsv shipped flag vs sidecar tier mismatches", shipped_mismatch)

# Feature-major files in osm-key order (the place files are in osm order too).
procs = []
for s in SETS:
    for t in (1, 2, 3):
        src, dst = FILES + f"fm_{s}_L{t}.unsorted", FILES + f"fm_{s}_L{t}.txt"
        procs.append(subprocess.Popen(
            f"LC_ALL=C sort -n -k1,1 -S 8G -T /ssd/precision/tmp {src} | cut -f2- > {dst} && rm {src}", shell=True))
for pr in procs:
    assert pr.wait() == 0

with open(OUT + "levels_poi.tsv", "w") as f:
    f.write("set\tlevel\tfeatures\tpostings\tstring_bytes\n")
    for (s, t), (a, b_, c_) in sorted(stats.items()):
        f.write(f"{s}\tL{t}\t{a}\t{b_}\t{c_}\n")
with open(OUT + "poi_coverage.tsv", "w") as f:
    cols = ["name:en", "int_name", "alt_name", "lang_not_en", "langs>=2", "langs>=5", "langs>=10"]
    f.write("tier\tnamed_pois\t" + "\t".join(c + "%" for c in cols) +
            "\tbase_mean\tall_mean\tall_p90\tall_p99\n")
    for t in (1, 2, 3):
        c = cov[t]
        n = c["features"]
        f.write("\t".join([str(t), str(n)] + [f"{100.0 * c[x] / n:.2f}" for x in cols] +
                          [f"{np.mean(nstr[('base', t)]):.2f}", f"{np.mean(nstr[('all', t)]):.2f}",
                           f"{np.percentile(nstr[('all', t)], 90):.0f}",
                           f"{np.percentile(nstr[('all', t)], 99):.0f}"]) + "\n")

# POI ranking signals from the shipped records (tiers 1-3).
rec_dt = np.dtype([("lat", "<f4"), ("lng", "<f4"), ("voff", "<u4"), ("vcnt", "<u4"), ("name_id", "<u4"),
                   ("cat", "u1"), ("tier", "u1"), ("flags", "u1"), ("imp", "u1"), ("street", "<u4"),
                   ("postcode", "<u4"), ("poly", "<u4")])
rec = np.fromfile(B + "out-nocap/planet/poi/all/poi_records.bin", dtype=rec_dt)
with open(OUT + "poi_signals.tsv", "w") as f:
    f.write("tier\trecords\tnamed%\twikipedia_flag%\twikidata_flag%\timportance>0%\timportance_p50(>0)\t"
            "importance_p90(>0)\n")
    for t in (1, 2, 3):
        m = rec["tier"] == t
        n = int(m.sum())
        named = rec["name_id"][m] != 0xFFFFFFFF
        imp = rec["imp"][m]
        nz = imp[imp > 0]
        f.write("\t".join([str(t), str(n), f"{100.0 * named.mean():.2f}",
                           f"{100.0 * ((rec['flags'][m] & 1) > 0).mean():.2f}",
                           f"{100.0 * ((rec['flags'][m] & 2) > 0).mean():.2f}",
                           f"{100.0 * len(nz) / n:.2f}",
                           f"{np.percentile(nz, 50):.0f}" if len(nz) else "-",
                           f"{np.percentile(nz, 90):.0f}" if len(nz) else "-"]) + "\n")
log("done")
