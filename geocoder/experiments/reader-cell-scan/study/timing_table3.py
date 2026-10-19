"""timing_table3.py [dir]: markdown table of timing3.sh results, mean / p99 µs per reverse lookup,
one row per (set, pool, call, warm|cold), one column per reader, plus tip / head and tip / main
ratios of the means."""
import json
import os
import sys

D = sys.argv[1] if len(sys.argv) > 1 else "/ssd/reader/timing3"
READERS = ["main", "pip2", "head", "fix5", "fix6", "tip"]
SET_ORDER = ["b-smallest", "b-recommended", "b-max", "r1-smallest", "r1-recommended", "u-smallest", "u-recommended"]
rows = {}
for f in sorted(os.listdir(D)):
    if not f.endswith(".json"):
        continue
    set_, pool, mode, temp, reader, _ = f.split(".")
    try:
        j = json.load(open(os.path.join(D, f)))
    except json.JSONDecodeError:
        continue
    rows.setdefault((set_, pool, mode, temp), {})[reader] = j
present = [r for r in READERS if any(r in v for v in rows.values())]
print("| set | pool | call | cache | " + " | ".join(f"{r} mean / p99" for r in present) + " | tip / head | tip / main |")
print("|---|---|---|---|" + "---:|" * (len(present) + 2))
for key in sorted(rows, key=lambda k: (SET_ORDER.index(k[0]) if k[0] in SET_ORDER else 99, k[1], k[2] != "query", k[3] != "warm")):
    v = rows[key]
    cells = [f"{v[r]['mean_us']:.1f} / {v[r]['p99_us']:.0f}" if r in v else "–" for r in present]
    ratio = lambda a, b: f"{v[a]['mean_us'] / v[b]['mean_us']:.2f}" if a in v and b in v else "–"
    call = "query_json" if key[2] == "query" else "find_admin"
    print(f"| {key[0]} | {key[1]} | {call} | {key[3]} | " + " | ".join(cells) + f" | {ratio('tip', 'head')} | {ratio('tip', 'main')} |")
