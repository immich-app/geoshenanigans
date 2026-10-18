#!/usr/bin/env python3
"""Zero-byte builder change E4, emulated on the test sets' place_nodes.bin copies: a hidden
(boundary-claimed) place node keeps its name_id and is marked hidden by PlaceNode._pad1 bit 1
instead of name_id = NO_DATA. The name is the builder's choice (name:en, else name, else ref)
from the names study's dump; it is set only when strings_core already holds that string (the
rest are counted: their bytes are what keeping them would add to strings_core)."""
import collections
import sys

import numpy as np

P = "/ssd/precision/build/out-nocap/planet/"
S = "/ssd/search-prod/sets/"
NO = 0xFFFFFFFF
pn_dt = np.dtype([("lat", "<f4"), ("lng", "<f4"), ("name_id", "<u4"), ("pt", "u1"), ("p1", "u1"), ("p2", "<u2"), ("parent", "<u4")])
slot_dt = np.dtype([("type", "u1"), ("flags", "u1"), ("tier", "u1"), ("res", "u1"), ("id", "<u8")])
HIDDEN = 2


def unescape(s):
    if "\\" not in s:
        return s
    out, i = [], 0
    while i < len(s):
        if s[i] == "\\" and i + 1 < len(s):
            out.append({"t": "\t", "n": "\n", "r": "\r"}.get(s[i + 1], s[i + 1]))
            i += 2
        else:
            out.append(s[i])
            i += 1
    return "".join(out)


core = open(P + "admin/strings_core.bin", "rb").read()
offs = {}
start = 0
while start < len(core):
    end = core.index(b"\0", start)
    offs.setdefault(core[start:end], start)
    start = end + 1
print("strings_core strings", len(offs), file=sys.stderr)

pn_admin = np.fromfile(P + "admin/place_nodes.bin", dtype=pn_dt)
raw = np.fromfile(P + "admin/place_nodes.osm_ids", dtype=np.uint8)
ids = np.frombuffer(raw[12:].tobytes(), dtype=slot_dt)["id"]
hidden_ids = {int(ids[i]): i for i in np.nonzero(pn_admin["name_id"] == NO)[0].tolist()}
print("hidden nodes (admin)", len(hidden_ids), file=sys.stderr)

names = {}
with open(P.replace("out-nocap/planet/", "names/names.tsv"), encoding="utf-8", errors="surrogateescape") as f:
    for line in f:
        if not line.startswith("place\tn\t"):
            continue
        p = line.rstrip("\n").split("\t")
        oid = int(p[2])
        if oid not in hidden_ids:
            continue
        kv = dict((p[i], unescape(p[i + 1])) for i in range(5, len(p) - 1, 2))
        nm = kv.get("name:en") or kv.get("name") or kv.get("ref")
        if nm:
            names[hidden_ids[oid]] = nm
print("hidden nodes with a name in the dump", len(names), file=sys.stderr)

found, missing, missing_bytes, missing_distinct = 0, 0, 0, set()
name_off = {}
for i, nm in names.items():
    b = nm.encode("utf-8", "surrogateescape")
    o = offs.get(b)
    if o is None:
        missing += 1
        if b not in missing_distinct:
            missing_distinct.add(b)
            missing_bytes += len(b) + 1
    else:
        found += 1
        name_off[i] = o
print(f"names already in strings_core: {found}; missing: {missing} ({len(missing_distinct)} distinct, {missing_bytes} bytes)")

# admin -> minimal node index (same join as emulate.py)
pn_min_orig = np.fromfile(P + "admin-minimal/place_nodes.bin", dtype=pn_dt)
akey = {}
for i, r in enumerate(pn_admin.tolist()):
    akey.setdefault((r[0], r[1], r[2], r[3]), i)
min_to_admin = [akey.get((r[0], r[1], r[2], r[3]), -1) for r in pn_min_orig.tolist()]

for s in ("minimal", "recommended", "max"):
    path = S + s + "/place_nodes.bin"
    pn = np.fromfile(path, dtype=pn_dt)
    n = 0
    for i in range(len(pn)):
        if pn["name_id"][i] != NO:
            continue
        a = min_to_admin[i] if s == "minimal" else i
        o = name_off.get(a)
        pn["p1"][i] |= HIDDEN
        if o is not None:
            pn["name_id"][i] = o
            n += 1
    pn.tofile(path)
    print(s, "hidden nodes given their name back:", n, "of", int((pn["p1"] & HIDDEN != 0).sum()))
