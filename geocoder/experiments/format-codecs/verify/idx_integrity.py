"""Integrity of the shared input dirs after all tracks ran: sha256 of every file in idx/max and
idx/withaddr against the published manifest (sha256 of the decompressed file), 8 files at a time."""
import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor

M = json.load(open("/ssd/shrink/data/configurations-2026-10-13.json"))["files"]
SETS = {"max": ["planet/full", "planet/quality/uncapped", "planet/poi/all"],
        "withaddr": ["planet/full", "planet/quality/q2.5", "planet/poi/all"]}


def sha(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        while b := f.read(1 << 24):
            h.update(b)
    return h.hexdigest()


jobs = []
for s, prefixes in SETS.items():
    d = f"/ssd/shrink/idx/{s}"
    for fn in sorted(os.listdir(d)):
        want = [M[f"{p}/{fn}"]["sha256"] for p in prefixes if f"{p}/{fn}" in M]
        jobs.append((s, fn, f"{d}/{fn}", want))
with ThreadPoolExecutor(8) as ex:
    res = list(ex.map(lambda j: (j, sha(j[2])), jobs))
bad = 0
for (s, fn, p, want), h in res:
    ok = h in want
    bad += not ok
    if not ok:
        print(f"MISMATCH {s}/{fn}: {h} not in {want}")
print(f"{len(res)} files checked, {bad} mismatches")
