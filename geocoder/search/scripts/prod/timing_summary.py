#!/usr/bin/env python3
"""Summarise results/timing: build wall/peak memory, query latency percentiles (all 10k, and the
first 1,000 of a cold run), anonymous memory and VmHWM."""
import glob
import json
import os

O = "/ssd/search-prod/results/timing/"


def pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round((len(xs) - 1) * q)))]


print("| build | wall s | peak RssAnon MiB | peak VmHWM MiB | exit |")
print("|---|---:|---:|---:|---:|")
for f in sorted(glob.glob(O + "build-*.json")):
    d = json.load(open(f))
    k = d["peak_kB"]
    print(f"| {os.path.basename(f)[6:-5]} | {d['wall_s']} | {k['RssAnon'] / 1024:.1f} | {k['VmHWM'] / 1024:.1f} | {d['exit']} |")
print()
print("| queries | n | p50 ms | p90 ms | p99 ms | max ms | first 1k p50 / p99 ms | lookups/q | peak RssAnon MiB | peak VmHWM MiB | exit |")
print("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
for f in sorted(glob.glob(O + "q-*.mem.json")):
    tag = os.path.basename(f)[2:-9]
    m = json.load(open(f))
    try:
        b = json.loads(open(O + f"q-{tag}.json").read().strip().splitlines()[-1])
        lat = [float(x) for x in open(O + f"q-{tag}.lat")]
    except (FileNotFoundError, IndexError, ValueError):
        print(f"| {tag} | - | | | | | | | {m['peak_kB']['RssAnon'] / 1024:.1f} | {m['peak_kB']['VmHWM'] / 1024:.1f} | {m['exit']} |")
        continue
    first = lat[:1000]
    k = m["peak_kB"]
    print(f"| {tag} | {len(lat)} | {pct(lat, .5) / 1e3:.2f} | {pct(lat, .9) / 1e3:.2f} | {pct(lat, .99) / 1e3:.2f} | {max(lat) / 1e3:.1f} | "
          f"{pct(first, .5) / 1e3:.2f} / {pct(first, .99) / 1e3:.1f} | {b['reverse_lookups_per_query']:.1f} | "
          f"{k['RssAnon'] / 1024:.1f} | {k['VmHWM'] / 1024:.1f} | {m['exit']} |")
