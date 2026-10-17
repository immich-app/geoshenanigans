"""memsample.py CGROUP_DIR OUT_PREFIX [INTERVAL_S]

Samples a cgroup until a stop file (OUT_PREFIX.stop) appears or the cgroup disappears:
  - every process: RssAnon (and its role: comm + the first distinguishing argv words)
  - the cgroup's memory.stat anon / kernel / file and memory.current
Writes OUT_PREFIX.series (t, sum of tool RssAnon KiB, cgroup anon, kernel, file KiB) and
OUT_PREFIX.procs (per process: peak RssAnon KiB, VmHWM KiB, role), and prints the peaks.
Run it outside the measured scope, on a core of its own."""
import os
import sys
import time

cg, out = sys.argv[1], sys.argv[2]
iv = float(sys.argv[3]) if len(sys.argv) > 3 else 0.1
TOOLS = ("gcx", "geocoder-patch", "zstd", "gcpatchm", "gcxm")


def role(pid):
    try:
        comm = open(f"/proc/{pid}/comm").read().strip()
        argv = open(f"/proc/{pid}/cmdline", "rb").read().split(b"\0")
    except OSError:
        return None, None
    a = [x.decode(errors="replace") for x in argv if x]
    r = comm
    if comm.startswith("gcx"):
        # gcx decode|encode SRC OUT --only fam ... --files f,f
        mode = a[1] if len(a) > 1 else "?"
        fam = a[a.index("--only") + 1] if "--only" in a else "?"
        files = a[a.index("--files") + 1] if "--files" in a else ""
        r = f"{comm} {mode} {fam} {files}"
    elif comm.startswith("geocoder-patch") or comm.startswith("gcpatch"):
        r = f"{comm} {a[1].rstrip('/').split('/')[-1] if len(a) > 1 else ''}"
        # variant from the patch path
        for x in a:
            if x.endswith(".gcpatch"):
                r = f"{comm} {x.split('/')[-2]}"
    return comm, r


def status(pid):
    d = {}
    try:
        for line in open(f"/proc/{pid}/status"):
            k, _, v = line.partition(":")
            if k in ("RssAnon", "VmHWM", "RssFile"):
                d[k] = int(v.split()[0])
    except OSError:
        return None
    return d


def mstat():
    d = {}
    try:
        for line in open(f"{cg}/memory.stat"):
            k, v = line.split()
            if k in ("anon", "kernel", "file", "sock", "pagetables", "kernel_stack", "slab"):
                d[k] = int(v) // 1024
        d["current"] = int(open(f"{cg}/memory.current").read()) // 1024
    except OSError:
        return None
    return d


procs = {}  # pid -> [peak_anon, hwm, role, comm, first_t, last_t]
series = open(out + ".series", "w")
series.write("t tool_anon_kib cg_anon_kib cg_kernel_kib cg_file_kib cg_current_kib nproc\n")
t0 = time.time()
peak = {"tool": 0, "anon": 0, "kernel": 0, "anon+kernel": 0, "current": 0}
seen_cg = False
while not os.path.exists(out + ".stop"):
    if not os.path.isdir(cg):
        if seen_cg:
            break
        time.sleep(iv)
        continue
    seen_cg = True
    t = time.time() - t0
    try:
        pids = [int(x) for x in open(f"{cg}/cgroup.procs").read().split()]
    except OSError:
        pids = []
    tool = 0
    for p in pids:
        s = status(p)
        if not s:
            continue
        e = procs.get(p)
        if e is None:
            comm, r = role(p)
            if comm is None:
                continue
            e = procs[p] = [0, 0, r, comm, t, t]
        e[0] = max(e[0], s.get("RssAnon", 0))
        e[1] = max(e[1], s.get("VmHWM", 0))
        e[5] = t
        if e[3].startswith(TOOLS):
            tool += s.get("RssAnon", 0)
    m = mstat() or {}
    series.write(f"{t:.2f} {tool} {m.get('anon', 0)} {m.get('kernel', 0)} {m.get('file', 0)} {m.get('current', 0)} {len(pids)}\n")
    peak["tool"] = max(peak["tool"], tool)
    peak["anon"] = max(peak["anon"], m.get("anon", 0))
    peak["kernel"] = max(peak["kernel"], m.get("kernel", 0))
    peak["anon+kernel"] = max(peak["anon+kernel"], m.get("anon", 0) + m.get("kernel", 0))
    peak["current"] = max(peak["current"], m.get("current", 0))
    time.sleep(iv)
series.close()
with open(out + ".procs", "w") as f:
    f.write("pid peak_anon_kib hwm_kib first_s last_s role\n")
    for p, e in sorted(procs.items(), key=lambda kv: -kv[1][0]):
        f.write(f"{p} {e[0]} {e[1]} {e[4]:.1f} {e[5]:.1f} {e[2]}\n")
with open(out + ".peaks", "w") as f:
    f.write(" ".join(f"{k}_kib={v}" for k, v in peak.items()) + "\n")
print(" ".join(f"{k}_mib={v / 1024:.1f}" for k, v in peak.items()))
