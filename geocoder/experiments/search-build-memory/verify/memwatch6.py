"""memwatch6.py <report.json> <tmp dir> -- <command...>

verify6's own sampler. Runs the command; every 20 ms reads /proc/<pid>/status of each descendant
whose name starts with "place-search" (peaks of RssAnon, RssFile, VmHWM in kB) and, while it runs,
remembers its cgroup; every 10th sample also sums the bytes under <tmp dir>. On exit reads the
cgroup's memory.peak and memory.events (oom_kill) when it was a systemd-run scope. Writes JSON."""
import json
import os
import subprocess
import sys
import time

rep, tmpdir = sys.argv[1], sys.argv[2]
cmd = sys.argv[sys.argv.index("--") + 1:]


def kids(pid):
    out = []
    try:
        for t in os.listdir(f"/proc/{pid}/task"):
            with open(f"/proc/{pid}/task/{t}/children") as f:
                out += [int(x) for x in f.read().split()]
    except OSError:
        pass
    return out


def tree_bytes(d):
    total = 0
    for root, _, files in os.walk(d):
        for f in files:
            try:
                total += os.lstat(os.path.join(root, f)).st_size
            except OSError:
                pass
    return total


t0 = time.time()
p = subprocess.Popen(cmd)
peak, cgpath, n, tmp_peak, last_cg = {}, None, 0, 0, {}
while p.poll() is None:
    todo, seen = [p.pid], []
    while todo:
        x = todo.pop()
        seen.append(x)
        todo += kids(x)
    for x in seen:
        try:
            st = {}
            name = ""
            for line in open(f"/proc/{x}/status"):
                k = line.split(":")[0]
                if k == "Name":
                    name = line.split()[1]
                elif k in ("RssAnon", "RssFile", "VmHWM"):
                    st[k] = int(line.split()[1])
            if not name.startswith("place-search"):
                continue
            for k, v in st.items():
                peak[k] = max(peak.get(k, 0), v)
            rel = open(f"/proc/{x}/cgroup").read().split("::", 1)[1].strip()
            if rel.endswith(".scope"):
                cgpath = "/sys/fs/cgroup" + rel
                last_cg["memory.peak"] = int(open(cgpath + "/memory.peak").read())
                ev = dict(l.split() for l in open(cgpath + "/memory.events"))
                last_cg["oom_kill"] = int(ev.get("oom_kill", 0))
                last_cg["max"] = int(ev.get("max", 0))
        except (OSError, ValueError, IndexError):
            pass
    if n % 10 == 0:
        tmp_peak = max(tmp_peak, tree_bytes(tmpdir))
    n += 1
    time.sleep(0.02)
wall = time.time() - t0
json.dump({"cmd": " ".join(cmd), "exit": p.returncode, "wall_s": round(wall, 2), "samples": n,
           "peak_kB": peak, "tmp_peak_MiB": round(tmp_peak / 2**20, 1), "cgroup": cgpath, "cg": last_cg},
          open(rep, "w"), indent=1)
sys.exit(p.returncode)
