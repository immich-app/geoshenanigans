"""Timing tables from logs/time_<label>.{log,res} (gc phase lines + scope_run result lines) and the
per-family memory runs (logs/time_perfam.log). usage: timing_table.py"""
import os
import re

L = "/ssd/shrink/sortpatch/codecs/logs"
FAMS = ["streets", "interps", "way_parents", "way_postcodes", "interp_postcodes", "gxc", "addr", "admin", "poi"]


def phases(label):
    p = f"{L}/time_{label}.log"
    out = {}
    if not os.path.exists(p):
        return out
    for line in open(p):
        m = re.match(r"phase (\S+)\s+wall=([\d.]+) cpu=([\d.]+)(?: in=(\d+) out=(\d+))?", line)
        if m and m.group(1) in FAMS:
            out[m.group(1)] = (float(m.group(2)), float(m.group(3)), int(m.group(4) or 0), int(m.group(5) or 0))
    return out


def res(label):
    p = f"{L}/time_{label}.res"
    if not os.path.exists(p):
        return {}
    return dict(kv.split("=", 1) for kv in open(p).read().split())


for d in ("enc", "dec"):
    labels = [f"{d}_warm_1013", f"{d}_cold_1013", f"{d}_small_1013"]
    P = [phases(x) for x in labels]
    print(f"\n### {'encode' if d == 'enc' else 'decode'} 10-13, per family (wall s / CPU s, gc phase timers)\n")
    print("| family | in (MB) | out (MB) | warm | cold | small device |")
    print("|---|---:|---:|---:|---:|---:|")
    for f in FAMS:
        if f not in P[0]:
            continue
        cells = []
        for ph in P:
            cells.append(f"{ph[f][0]:.1f} / {ph[f][1]:.1f}" if f in ph else "-")
        print(f"| {f} | {P[0][f][2] / 1e6:,.0f} | {P[0][f][3] / 1e6:,.0f} | " + " | ".join(cells) + " |")
    R = [res(x) for x in labels]
    print("| **whole day (process, /usr/bin/time)** | | | " + " | ".join(
        f"**{r.get('wall_s', '-')} / {r.get('cpu_s', '-')}**" for r in R) + " |")
    print("\n| run | wall s | user s | sys s | max RSS MiB | anon peak MiB | scope peak MiB | read GB | written GB (exit / after fsync) | fsync s | OOM |")
    print("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for x, r in zip(labels, R):
        if r:
            print(f"| {x} | {r['wall_s']} | {r['user_s']} | {r['sys_s']} | {r['maxrss_mib']} | {r['anon_peak_mib']} | {r['cgroup_peak_mib']} | "
                  f"{r['read_gb']} | {r['written_gb_at_exit']} / {r['written_gb_after_fsync']} | {r['fsync_s']} | {r['oom_kills']} |")

p = f"{L}/time_perfam.log"
if os.path.exists(p):
    print("\n### per family, one process each (warm, 1 core): wall s / CPU s / max RSS MiB\n")
    print("| family | encode | decode |")
    print("|---|---:|---:|")
    cur, row = None, {}
    for line in open(p):
        m = re.match(r"## (encode|decode) (\S+)", line)
        if m:
            cur = (m.group(1), m.group(2))
            continue
        m = re.match(r"phase (\S+)\s+wall=([\d.]+) cpu=([\d.]+)", line)
        if m and cur and m.group(1) == cur[1]:
            row.setdefault(cur[1], {})[cur[0]] = [float(m.group(2)), float(m.group(3)), None]
        m = re.match(r"phase total .* maxrss_kib=(\d+)", line)
        if m and cur:
            row.setdefault(cur[1], {}).setdefault(cur[0], [0, 0, None])[2] = int(m.group(1)) / 1024
    for f in FAMS:
        if f in row:
            e, dd = row[f].get("encode"), row[f].get("decode")
            fmt = lambda v: f"{v[0]:.1f} / {v[1]:.1f} / {v[2]:.0f}" if v else "-"
            print(f"| {f} | {fmt(e)} | {fmt(dd)} |")
