"""Assemble the three preset flat dirs (smallest / recommended / max) from a builder output
the way a client would (fetch_preset.py's file selection from the 2026-10-13 manifest), as
symlinks into the build output.

usage: python -I assemble_sets.py <build out dir> <dest dir>
"""
import json
import os
import sys

MANIFEST = "/ssd/shrink/data/configurations-2026-10-13.json"
PRESETS = {
    "smallest": ("admin-minimal", "q2.5", "none"),
    "recommended": ("admin", "q2.5", "major"),
    "max": ("full", "uncapped", "all"),
}

out, dest = sys.argv[1:3]
c = json.load(open(MANIFEST))
for name, (mode, quality, poi) in PRESETS.items():
    def sub(p):
        return (p.replace("{region}", "planet").replace("{mode}", mode)
                .replace("{quality}", quality).replace("{poi_tier}", poi))
    files, rep = [], set()
    for cid in [f"mode.{mode}", f"quality.{quality}", f"poi_tier.{poi}"]:
        cp = c["components"][cid]
        files += [sub(f) for f in cp.get("files", [])]
        files += [sub(f) for f in cp.get("optional_files", []) if os.path.exists(os.path.join(out, sub(f)))]
        rep |= {sub(r) for r in cp.get("replaces", [])}
    files = sorted(set(f for f in files if f not in rep))
    d = os.path.join(dest, name)
    os.makedirs(d, exist_ok=True)
    for f in files:
        src = os.path.join(out, f)
        if not os.path.exists(src):
            sys.exit(f"missing {src}")
        link = os.path.join(d, os.path.basename(f))
        if os.path.lexists(link):
            os.remove(link)
        os.symlink(src, link)
    print(name, len(files), "files")
