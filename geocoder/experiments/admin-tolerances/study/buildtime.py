#!/usr/bin/env python3
"""buildtime.py <build name>... : builder time per build (/ssd/tiers3/build/<name>): wall from run.txt,
peak memory (cgroup memory.peak), and the planet phases a tier scheme touches: quality/q2.5 simplify
and pack + write, planet admin-minimal (log_phase lines: wall seconds and average busy cores), plus the
sum over every quality/q* simplify phase of the planet (the planet's quality dirs are simplified
three at a time beside each other)."""
import re
import sys

PH = re.compile(r"\[(?P<s>[\d.]+)s (?P<c>[\d.]+)/64cores [\d.]+% \d+MiB\]\s+(?P<label>.*)$")
for name in sys.argv[1:]:
    d = f"/ssd/tiers3/build/{name}"
    wall = re.search(r"wall=([\d.]+)", open(f"{d}/run.txt").read())
    peak = int(open(f"{d}/memory.peak").read()) / 2**30
    lines = open(f"{d}/build.log", errors="replace").read().splitlines()
    # the planet run's phases come after the continents' (planet/admin: total marks it)
    start = max(i for i, line in enumerate(lines) if "planet/admin: total" in line) if any("planet/admin: total" in x for x in lines) else 0
    got = {}
    for line in lines[start:]:
        m = PH.search(line)
        if not m:
            continue
        lab = m.group("label").strip()
        for key in ("quality/q2.5: simplify", "quality/q2.5: pack + write", "planet: admin-minimal", "planet: total"):
            if lab == key:
                got[key] = (float(m.group("s")), float(m.group("c")))
    print(f"{name:<10} wall {float(wall.group(1)) if wall else float('nan'):7.1f} s  peak {peak:5.1f} GiB  " +
          "  ".join(f"{k} {v[0]:.1f} s x {v[1]:.1f} cores" for k, v in got.items()))
