"""rsswatch.py <seconds> <log> -- <command...>: evict the max set, run the command (a 32 MiB
cgroup build), and every 2 s log the build's RssFile/RssAnon, its cgroup's memory.stat
(anon, file, kernel) and the page cache residency of the set files it reads; then stop it.
Answers: are file pages the build maps charged to its cgroup, or resident outside it?"""
import ctypes
import json
import mmap
import os
import subprocess
import sys
import time

libc = ctypes.CDLL("libc.so.6")
secs, log = float(sys.argv[1]), sys.argv[2]
cmd = sys.argv[sys.argv.index("--") + 1:]
S = "/ssd/searchbuild/sets/max/"
files = {n: os.path.realpath(S + n) for n in ["place_nodes.bin", "admin_polygons.bin", "admin_vertices.bin",
                                              "strings_core.bin", "poi_records.bin", "strings_poi.bin"]}


def resident(path):
    size = os.path.getsize(path)
    with open(path, "rb") as f:
        m = mmap.mmap(f.fileno(), size, access=mmap.ACCESS_COPY)
        vec = (ctypes.c_ubyte * ((size + 4095) // 4096))()
        a = ctypes.c_char.from_buffer(m)
        libc.mincore(ctypes.c_void_p(ctypes.addressof(a)), ctypes.c_size_t(size), vec)
        del a
        m.close()
    return round(sum(v & 1 for v in bytes(vec)) * 4096 / 2**20, 1)


subprocess.run(["python3", "-I", "/ssd/searchbuild/evict.py", "/ssd/searchbuild/sets/max", "/ssd/searchbuild/extras/max"])
p = subprocess.Popen(cmd)
t0 = time.time()
out = open(log, "w")
while time.time() - t0 < secs and p.poll() is None:
    time.sleep(2)
    row = {"t": round(time.time() - t0, 1)}
    try:
        pid = int(subprocess.run(["pgrep", "-f", "^/ssd/searchbuild/bin/place-search"], capture_output=True, text=True).stdout.split()[0])
        for l in open(f"/proc/{pid}/status"):
            if l.startswith(("RssAnon", "RssFile")):
                row[l.split(":")[0]] = int(l.split()[1]) // 1024
        cg = "/sys/fs/cgroup" + open(f"/proc/{pid}/cgroup").read().split("::", 1)[1].strip()
        st = dict(l.split() for l in open(cg + "/memory.stat"))
        for k in ("anon", "file", "kernel", "file_mapped"):
            row["cg_" + k] = int(st[k]) // 2**20
    except (IndexError, FileNotFoundError, ValueError):
        pass
    row["resident"] = {k: resident(v) for k, v in files.items()}
    out.write(json.dumps(row) + "\n")
    out.flush()
p.terminate()
subprocess.run(["pkill", "-f", "^/ssd/searchbuild/bin/place-search"])
