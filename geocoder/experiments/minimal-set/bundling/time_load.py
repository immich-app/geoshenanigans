#!/usr/bin/env python3
"""Startup + query cost of the minimal set via grid_query (process start + mmap load + queries),
cold (the set's pages evicted with posix_fadvise DONTNEED, touching nothing else) and warm."""
import json
import os
import resource
import subprocess
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
GQ = "/ssd/shrink/target/release/examples/grid_query"
DS = "/ssd/shrink/idx/smallest"
rows = [json.loads(line) for line in open(HERE / "comparison.jsonl", encoding="utf-8")]
pts = "".join(f"{r['lat']} {r['lon']}\n" for r in rows)
one = "51.5007 -0.1246\n"


def evict():
    for f in os.listdir(DS):
        fd = os.open(os.path.join(DS, f), os.O_RDONLY)
        os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        os.close(fd)


def run(label, inp):
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    t = time.perf_counter()
    p = subprocess.run(["/usr/bin/time", "-f", "%M %F %R", GQ, DS], input=inp, capture_output=True, text=True, check=True)
    dt = time.perf_counter() - t
    maxrss_kb, major, minor = p.stderr.strip().splitlines()[-1].split()
    n = inp.count("\n")
    print(f"{label}: {n} queries, wall {dt * 1000:.1f} ms ({dt * 1000 / n:.2f} ms/query), max RSS {int(maxrss_kb) / 1024:.1f} MiB, "
          f"major faults {major}, minor faults {minor}")


evict()
run("cold, 1 query", one)
run("warm, 1 query", one)
evict()
run(f"cold, {len(rows)} queries", pts)
run(f"warm, {len(rows)} queries", pts)
