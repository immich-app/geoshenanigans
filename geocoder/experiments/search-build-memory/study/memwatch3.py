"""memwatch3.py <report.json> <tmp dir> -- <command...>

Run a command; every 20 ms sample /proc/<pid>/status of every place-search process below it (peak
RssAnon / RssFile / VmRSS / VmHWM, kB), the total size of the files under <tmp dir> (peak temporary
disk, bytes) and, when the process sits in its own cgroup (systemd-run --scope), that cgroup's
memory.peak / memory.events. Writes exit code, wall time and the peaks as JSON."""
import json
import os
import subprocess
import sys
import time


def children(pid):
    out = []
    try:
        for t in os.listdir(f"/proc/{pid}/task"):
            with open(f"/proc/{pid}/task/{t}/children") as f:
                out += [int(x) for x in f.read().split()]
    except (FileNotFoundError, ProcessLookupError):
        pass
    return out


def dir_bytes(d):
    total = 0
    try:
        for e in os.scandir(d):
            try:
                total += e.stat(follow_symlinks=False).st_size
            except FileNotFoundError:
                pass
    except FileNotFoundError:
        pass
    return total


rep, tmpdir = sys.argv[1], sys.argv[2]
cmd = sys.argv[sys.argv.index("--") + 1:]
t0 = time.time()
p = subprocess.Popen(cmd)
peak = {}
cg = {}
tmp_peak = 0
samples = 0
while p.poll() is None:
    todo, seen = [p.pid], []
    while todo:
        x = todo.pop()
        seen.append(x)
        todo += children(x)
    for x in seen:
        try:
            st = {}
            name = ""
            for line in open(f"/proc/{x}/status"):
                k = line.split(":")[0]
                if k == "Name":
                    name = line.split()[1]
                if k in ("RssAnon", "RssFile", "VmRSS", "VmHWM"):
                    st[k] = int(line.split()[1])
            if not name.startswith("place-search"):
                continue
            for k, v in st.items():
                peak[k] = max(peak.get(k, 0), v)
            rel = open(f"/proc/{x}/cgroup").read().split("::", 1)[1].strip()
            base = "/sys/fs/cgroup" + rel
            if "/run-" in rel and rel.endswith(".scope"):
                cg["path"] = rel
                cg["memory.peak"] = int(open(base + "/memory.peak").read())
                ev = dict(l.split() for l in open(base + "/memory.events"))
                cg["oom_kill"] = int(ev.get("oom_kill", 0))
                cg["max_events"] = int(ev.get("max", 0))
        except (FileNotFoundError, ProcessLookupError, ValueError, IndexError):
            pass
    if samples % 10 == 0:
        tmp_peak = max(tmp_peak, dir_bytes(tmpdir))
    samples += 1
    time.sleep(0.02)
json.dump({"cmd": " ".join(cmd), "exit": p.returncode, "wall_s": round(time.time() - t0, 2), "samples": samples,
           "peak_kB": peak, "tmp_peak_bytes": tmp_peak, "cgroup": cg}, open(rep, "w"), indent=1)
sys.exit(p.returncode)
