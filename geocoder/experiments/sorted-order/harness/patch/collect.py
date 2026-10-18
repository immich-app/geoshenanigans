"""Tabulates this step's measurements (out/<tool>/<date>/<variant>/*.result,
smoke/<tool>-smoke/...) next to the baseline step's (published patches).

Usage: collect.py"""
import glob
import os
import re

B = "/ssd/shrink/sortpatch/patch"
BASE = "/ssd/shrink/sortpatch/baseline/out"


def kv(line):
    # The timed (cold) run's fields; the warm instrumented run follows "| warm-sec:".
    return dict(re.findall(r"(\w+)=(\"[^\"]*\"|\S+)", line.split("| warm-sec:")[0]))


def read(p):
    try:
        return open(p).read().strip().splitlines()[-1]
    except OSError:
        return ""


rows = []
for new in ("2026-10-13", "2026-10-12"):
    for safe in ("planet_full", "planet_quality_uncapped", "planet_poi_all"):
        print(f"\n## {new} {safe}")
        for tool, d in (("published", f"{BASE}/{new}/{safe}"), ("v7", f"{B}/out/v7/{new}/{safe}"), ("v8", f"{B}/out/v8/{new}/{safe}"), ("v8s", f"{B}/out/v8s/{new}/{safe}")):
            df = kv(read(f"{d}/diff.result"))
            size = df.get("ours_zst") or df.get("zst") or "-"
            print(f"  {tool:10} diff: zst={size} raw={df.get('ours_raw') or df.get('raw')} wall={df.get('wall')} cpu={df.get('cpu')} "
                  f"maxrss_mib={df.get('maxrss_mib')} fs_in_gb={df.get('fs_in_gb')} sib={df.get('sib_busy')}")
            for prof in ("p8", "p1", "small"):
                a = kv(read(f"{d}/apply-{prof}.result"))
                if not a:
                    continue
                print(f"  {tool:10} apply {prof:5}: wall={a.get('wall')} cpu={a.get('cpu')} maxrss={a.get('maxrss_mib')} "
                      f"anon={a.get('anon_peak_mib')} read={a.get('read_gb')} written={a.get('written_gb_after_fsync')} "
                      f"disk={a.get('peak_disk_gb')} sib={a.get('sib_busy')} verify={a.get('verify')}")
print("\n## smoke (untimed, warm; diff 16 cores 32-47, apply core 48)")
for p in sorted(glob.glob(f"{B}/smoke/*-smoke/*/*/")):
    tool = p.split("/")[-4]
    new = p.split("/")[-3]
    log = open(p + "../../../../logs/" + "x", "a") if False else None
    zst = os.path.getsize(p + "patch.gcpatch") if os.path.exists(p + "patch.gcpatch") else "-"
    ver = read(p + "verify.txt")
    print(f"  {tool:10} {new} zst={zst} verify={ver}")
