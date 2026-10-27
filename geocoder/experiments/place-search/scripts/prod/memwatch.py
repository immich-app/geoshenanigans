#!/usr/bin/env python3
"""memwatch.py <report.json> -- <command...>: run a command (stdin/stdout passed through),
sample its /proc status every 50 ms, write peak RssAnon / RssFile / VmRSS / VmHWM (kB) and the
wall time to the report."""
import json
import subprocess
import sys
import time

rep = sys.argv[1]
cmd = sys.argv[sys.argv.index("--") + 1:]
t0 = time.time()
p = subprocess.Popen(cmd)
peak = {"RssAnon": 0, "RssFile": 0, "VmRSS": 0, "VmHWM": 0}
samples = 0
while p.poll() is None:
    try:
        for line in open(f"/proc/{p.pid}/status"):
            k = line.split(":")[0]
            if k in peak:
                peak[k] = max(peak[k], int(line.split()[1]))
        samples += 1
    except (FileNotFoundError, ProcessLookupError, ValueError):
        pass
    time.sleep(0.05)
json.dump({"cmd": " ".join(cmd), "exit": p.returncode, "wall_s": round(time.time() - t0, 2), "samples": samples,
           "peak_kB": peak}, open(rep, "w"), indent=1)
sys.exit(p.returncode)
