"""diag.py <out.jsonl> <tmp dir> <idx dir> -- <command...>: run a place-search build and every 100 ms
record where it is: phase (from the temp/run files and index files present), state and wchan of the
place-search process, its cumulative CPU time, faults and I/O bytes, and (inside a systemd scope)
its cgroup's memory.stat counters and memory/io pressure totals. One JSON line per sample.
Diagnostic only: answers where the wall time of a 32 MiB cgroup build goes."""
import json
import os
import subprocess
import sys
import time

out, tmpdir, idxdir = sys.argv[1], sys.argv[2], sys.argv[3]
cmd = sys.argv[sys.argv.index("--") + 1:]
CG_KEYS = ("anon", "file", "kernel", "file_dirty", "file_writeback", "file_mapped", "pgmajfault",
           "workingset_refault_file", "workingset_refault_anon", "pgscan", "pgsteal", "pgactivate")


def children(pid):
    res = []
    try:
        for t in os.listdir(f"/proc/{pid}/task"):
            with open(f"/proc/{pid}/task/{t}/children") as f:
                res += [int(x) for x in f.read().split()]
    except OSError:
        pass
    return res


def find_build(root):
    todo = [root]
    while todo:
        x = todo.pop()
        try:
            if open(f"/proc/{x}/comm").read().startswith("place-search"):
                return x
        except OSError:
            pass
        todo += children(x)
    return None


def phase():
    try:
        names = os.listdir(tmpdir)
    except OSError:
        names = []
    idx = {}
    try:
        for e in os.scandir(idxdir):
            idx[e.name] = e.stat().st_size
    except OSError:
        pass
    runs = [n for n in names if n.endswith(".run")]
    if "poi.pfx" in idx or "poi.pfx.fst" in idx:
        return "poi-prefix"
    if "poi.fst" in idx:
        return "poi-merge"
    if any(n.startswith("poi-m") for n in runs):
        return "poi-reduce"
    if "places.pfx" in idx:
        return "between"
    if "places.fst" in idx:
        return "places-merge"
    if any(n.startswith("places-m") for n in runs):
        return "places-reduce"
    if any(n.startswith("poi-") for n in runs):
        return "poi-keys"
    if any(n.startswith("places-") for n in runs):
        return "places-keys"
    if "places.bnd" in idx:
        return "places-source"
    return "load"


def read_kv(path):
    try:
        return dict(l.split()[:2] for l in open(path))
    except OSError:
        return {}


t0 = time.time()
p = subprocess.Popen(cmd)
f = open(out, "w")
while p.poll() is None:
    b = find_build(p.pid)
    s = {"t": round(time.time() - t0, 2), "phase": phase()}
    if b:
        try:
            st = open(f"/proc/{b}/stat").read().rsplit(")", 1)[1].split()
            s["minflt"], s["majflt"] = int(st[7]), int(st[9])
            s["cpu_s"] = (int(st[11]) + int(st[12])) / os.sysconf("SC_CLK_TCK")
            # the thread doing the work: any not parked on a futex (the key phase runs in a worker)
            waits = []
            for t in os.listdir(f"/proc/{b}/task"):
                ts = open(f"/proc/{b}/task/{t}/stat").read().rsplit(")", 1)[1].split()[0]
                waits.append((ts, open(f"/proc/{b}/task/{t}/wchan").read()))
            act = [w for w in waits if w[1] != "futex_wait_queue"] or waits
            s["state"], s["wchan"] = act[0]
            io = read_kv(f"/proc/{b}/io")
            for k in ("read_bytes", "write_bytes", "rchar", "wchar"):
                s[k] = int(io.get(k + ":", 0))
            rel = open(f"/proc/{b}/cgroup").read().split("::", 1)[1].strip()
            if rel.endswith(".scope") and "/run-" in rel:
                base = "/sys/fs/cgroup" + rel
                ms = read_kv(base + "/memory.stat")
                s["cg"] = {k: int(ms[k]) for k in CG_KEYS if k in ms}
                for kind in ("memory", "io"):
                    for line in open(f"{base}/{kind}.pressure"):
                        w = line.split()
                        s[f"{kind}_{w[0]}_us"] = int(w[4].split("=")[1])
        except (OSError, IndexError, ValueError):
            pass
    f.write(json.dumps(s) + "\n")
    time.sleep(0.1)
f.close()
print(json.dumps({"exit": p.returncode, "wall_s": round(time.time() - t0, 2)}))
