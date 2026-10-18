#!/usr/bin/env python3
"""Vertex counts per admin level for the boundary tiers: a (shipped quality/uncapped), b (1e-5 snap of
the DP rings, duplicates dropped), c (exact DP rings), d (exact raw rings). Rings are aligned record for
record with quality/uncapped/admin_polygons.bin."""
import numpy as np

SH = "/ssd/precision/build/out-nocap/planet/quality/uncapped"
W = "/ssd/precision/tiers/work/admin"
PDT = np.dtype([("off", "<u4"), ("cnt", "<u4"), ("name", "<u4"), ("lvl", "u1"), ("pto", "u1"), ("p2", "u1"),
                ("p3", "u1"), ("area", "<f4"), ("cc", "<u2"), ("p4", "<u2")])
p = np.fromfile(f"{SH}/admin_polygons.bin", PDT)
cnt = {"a": p["cnt"].astype(np.int64)}
for v in "bcd":
    cnt[v] = np.diff(np.fromfile(f"{W}/{v}.e7.idx", "<u8").astype(np.int64))
print("| admin_level | rings | a today | b Maximum | c Original 0.1 m DP | d Original raw |")
print("|---:|---:|---:|---:|---:|---:|")
for L in np.unique(p["lvl"]):
    s = p["lvl"] == L
    print(f"| {L} | {int(s.sum()):,} | " + " | ".join(f"{int(cnt[v][s].sum()):,}" for v in "abcd") + " |")
print(f"| all | {len(p):,} | " + " | ".join(f"{int(cnt[v].sum()):,}" for v in "abcd") + " |")
