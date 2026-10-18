#!/usr/bin/env python3
"""vident.py PREV CUR: does a published slot live on both days hold the same OSM entity?

Reads the published .bin files (tombstone test on record content) and the
*.osm_ids sidecars (12-byte header: magic 0xD0510EAD, version 1, count; then
12 bytes per slot: type u8, flags u8, tier u8, reserved u8, osm id u64).
"""
import sys
import numpy as np

DAYS = "/ssd/shrink/days"
SC = "/ssd/shrink/sortpatch/sort/sidecars"
NO = np.uint32(0xFFFFFFFF)
SLOT = np.dtype([("type", "u1"), ("flags", "u1"), ("tier", "u1"), ("res", "u1"), ("id", "<u8")])
WAY = np.dtype([("off", "<u4"), ("cnt", "<u2"), ("pad", "<u2"), ("name", "<u4")])
IW = np.dtype([("off", "<u4"), ("cnt", "<u2"), ("pad1", "<u2"), ("street", "<u4"), ("start", "<u4"), ("end", "<u4"), ("interp", "u1"), ("p", "u1", 3)])
ADDR = np.dtype([("lat", "<u4"), ("lng", "<u4"), ("hn", "<u4"), ("street", "<u4"), ("parent", "<u4"), ("voff", "<u4"), ("vcnt", "<u4")])

prev, cur = sys.argv[1], sys.argv[2]


def sidecar(date, fam):
    raw = np.fromfile(f"{SC}/{date}/{fam}.osm_ids", np.uint8)
    hdr = raw[:12].view("<u4")
    assert hdr[0] == 0xD0510EAD and hdr[1] == 1, hdr
    s = raw[12:].view(SLOT)
    assert len(s) == hdr[2]
    return s


def tombs(date, fam):
    d = f"{DAYS}/{date}/planet/full"
    if fam == "street_ways":
        r = np.fromfile(f"{d}/street_ways.bin", WAY)
        return (r["cnt"] == 0) & (r["off"] == 0) & (r["name"] == NO) & (r["pad"] == 0)
    if fam == "interp_ways":
        r = np.fromfile(f"{d}/interp_ways.bin", IW)
        return (r["cnt"] == 0) & (r["off"] == 0) & (r["street"] == NO) & (r["start"] == 0) & (r["end"] == 0) & (r["interp"] == 0)
    r = np.fromfile(f"{d}/addr_points.bin", ADDR)
    return (r["lat"] == 0) & (r["lng"] == 0) & (r["hn"] == 0) & (r["street"] == 0) & (r["parent"] == NO) & (r["voff"] == NO) & (r["vcnt"] == 0)


for fam in ("street_ways", "interp_ways", "addr_points"):
    tp, tc = tombs(prev, fam), tombs(cur, fam)
    sp, sc = sidecar(prev, fam), sidecar(cur, fam)
    assert len(sp) == len(tp) and len(sc) == len(tc), (fam, len(sp), len(tp), len(sc), len(tc))
    n = min(len(tp), len(tc))
    both = ~tp[:n] & ~tc[:n]
    same = both & (sp["type"][:n] == sc["type"][:n]) & (sp["id"][:n] == sc["id"][:n])
    reused = (tp[:n] & ~tc[:n]).sum() + 0
    appended = (~tc[n:]).sum()
    # tombstone flag of the sidecar: type NONE (0) on a dead slot?
    dead_named = int(((sc["type"] != 0) & tc).sum())
    live = ~tc
    key = (sc["type"][live].astype(np.uint64) << np.uint64(56)) | sc["id"][live]
    dup = len(key) - len(np.unique(key))
    print(f"{fam} {prev}->{cur}: live on both {int(both.sum())}, same osm entity {int(same.sum())}, differ {int(both.sum() - same.sum())}; "
          f"tombstone->live reused {int(reused)}, appended live {int(appended)}; dead slots still naming an entity {dead_named}; "
          f"duplicate live entity keys {dup}", flush=True)
