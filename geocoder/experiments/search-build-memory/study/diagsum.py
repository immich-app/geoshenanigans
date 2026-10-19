"""diagsum.py <diag.jsonl>...: per phase of each diag.py run: wall seconds, CPU seconds, major faults,
disk bytes read / written, cgroup refaults and reclaim scans, memory / io pressure (full) seconds,
and the most frequent wait channels (share of samples)."""
import collections
import json
import sys

for path in sys.argv[1:]:
    rows = [json.loads(l) for l in open(path)]
    rows = [r for r in rows if "cpu_s" in r]
    print(f"== {path} ({len(rows)} samples)")
    print("| phase | wall s | cpu s | majflt | read MiB | write MiB | refault file | pgscan | mem full s | io full s | top waits |")
    print("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|")
    order = []
    by = collections.defaultdict(list)
    for r in rows:
        if r["phase"] not in by:
            order.append(r["phase"])
        by[r["phase"]].append(r)
    prev = rows[0]
    for ph in order:
        rs = by[ph]
        a, b = prev, rs[-1]
        d = lambda k: b.get(k, 0) - a.get(k, 0)
        dc = lambda k: b.get("cg", {}).get(k, 0) - a.get("cg", {}).get(k, 0)
        waits = collections.Counter(r["wchan"] if r.get("state") != "R" else "running" for r in rs)
        top = ", ".join(f"{w} {100 * n // len(rs)}%" for w, n in waits.most_common(4))
        print(f"| {ph} | {d('t'):.1f} | {d('cpu_s'):.1f} | {d('majflt')} | {d('read_bytes') / 2**20:.0f} | "
              f"{d('write_bytes') / 2**20:.0f} | {dc('workingset_refault_file')} | {dc('pgscan')} | "
              f"{d('memory_full_us') / 1e6:.1f} | {d('io_full_us') / 1e6:.1f} | {top} |")
        prev = rs[-1]
