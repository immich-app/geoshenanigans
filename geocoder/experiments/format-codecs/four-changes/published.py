"""Published per-file sizes (manifest size_zst / size_raw) of the max-quality and with-addresses
presets: component files + optional files of mode.full, quality.<q>, poi_tier.all. Writes
work/published.json {preset: {basename: {zst, raw, path}}} and prints the totals."""
import json
from common4 import *

m = json.load(open("/ssd/shrink/data/configurations-2026-10-13.json"))
F = m["files"]
out = {}
for preset, q in (("max", "uncapped"), ("withaddr", "q2.5")):
    files = {}
    for comp in ("mode.full", f"quality.{q}", "poi_tier.all"):
        c = m["components"][comp]
        for p in c["files"] + c.get("optional_files", []):
            p = p.replace("{region}", "planet").replace("{quality}", q)
            b = os.path.basename(p)
            assert b not in files or F[files[b]["path"]]["sha256"] == F[p]["sha256"], (b, p)
            if b in files:
                continue
            files[b] = {"zst": F[p]["size_zst"], "raw": F[p]["size_raw"], "path": p}
    out[preset] = files
    z, r = sum(v["zst"] for v in files.values()), sum(v["raw"] for v in files.values())
    d = {"max": MAX, "withaddr": WA}[preset]
    local = set(os.listdir(d))
    print(f"{preset}: {len(files)} files, {z / MiB:,.1f} MiB zst, {r / MiB:,.1f} MiB raw; "
          f"manifest-not-local {sorted(set(files) - local)} local-not-manifest {sorted(local - set(files))}")
    bad = [b for b in files if b in local and os.path.getsize(f"{d}/{b}") != files[b]["raw"]]
    print("   raw size mismatches vs local:", bad)
json.dump(out, open(f"{W}/work/published.json", "w"), indent=1)
