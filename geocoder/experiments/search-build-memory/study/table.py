"""table.py <runs dir>: one markdown row per run: binary, config, limit, sort budget, result,
files identical, peak RssAnon, peak RSS, wall, peak temp disk, key / merge phase seconds."""
import json
import os
import re
import sys

d = sys.argv[1]
rows = []
for r in sorted(os.listdir(d)):
    p = os.path.join(d, r)
    if not os.path.exists(os.path.join(p, "mem.json")):
        continue
    m = json.load(open(os.path.join(p, "mem.json")))
    summ = open(os.path.join(p, "summary.txt")).read() if os.path.exists(os.path.join(p, "summary.txt")) else ""
    same = re.search(r"same=(\d+) diff=(\d+) missing=(\d+)", summ)
    log = open(os.path.join(p, "build.log")).read() if os.path.exists(os.path.join(p, "build.log")) else ""
    keys = sum(float(x) for x in re.findall(r"time (?:sources|keys\+runs) s: ([\d.]+)", log))
    merge = sum(float(x) for x in re.findall(r"time merge\+write s: ([\d.]+)", log))
    mm = re.search(r"--mem-mb (\d+)", open(os.path.join(p, "inner.sh")).read())
    budget = mm.group(1) if mm else "16 (default)"
    cg = m.get("cgroup", {})
    oom = "OOM-killed" if m["exit"] != 0 else "ok"
    rows.append((r, budget, oom, f"{same.group(1)}/{int(same.group(1)) + int(same.group(2)) + int(same.group(3))}" if same else "?",
                 m["peak_kB"].get("RssAnon", 0) / 1024, m["peak_kB"].get("VmHWM", 0) / 1024, m["wall_s"],
                 m["tmp_peak_bytes"] / 2**20, keys, merge, cg.get("max_events", "-")))
print("| run | sort MiB | result | files = ref | peak anon MiB | peak RSS MiB | wall s | peak tmp MiB | key s | merge s | memory.max events |")
print("|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|")
for x in rows:
    print(f"| {x[0]} | {x[1]} | {x[2]} | {x[3]} | {x[4]:.1f} | {x[5]:.1f} | {x[6]:.1f} | {x[7]:.0f} | {x[8]:.1f} | {x[9]:.1f} | {x[10]} |")
