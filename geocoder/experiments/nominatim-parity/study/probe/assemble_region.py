"""assemble_region.py <build out dir> <region> <dest dir> : assemble_sets.py for one region of the
build (the three preset flat dirs, symlinks into the build output, the 2026-10-13 manifest's file
selection). Exits non-zero naming the first missing file a client would ask for."""
import json
import os
import sys

MANIFEST = "/ssd/shrink/data/configurations-2026-10-13.json"
PRESETS = {
    "smallest": ("admin-minimal", "q2.5", "none"),
    "recommended": ("admin", "q2.5", "major"),
    "max": ("full", "uncapped", "all"),
}

out, region, dest = sys.argv[1:4]
c = json.load(open(MANIFEST))
missing = []
for name, (mode, quality, poi) in PRESETS.items():
    def sub(p):
        return (p.replace("{region}", region).replace("{mode}", mode)
                .replace("{quality}", quality).replace("{poi_tier}", poi))
    files, rep = [], set()
    for cid in [f"mode.{mode}", f"quality.{quality}", f"poi_tier.{poi}"]:
        cp = c["components"][cid]
        files += [sub(f) for f in cp.get("files", [])]
        files += [sub(f) for f in cp.get("optional_files", []) if os.path.exists(os.path.join(out, sub(f)))]
        rep |= {sub(r) for r in cp.get("replaces", [])}
    files = sorted(set(f for f in files if f not in rep))
    gone = [f for f in files if not os.path.exists(os.path.join(out, f))]
    if gone:
        missing += [(name, f) for f in gone]
        print(name, "MISSING", len(gone), gone[:4])
        continue
    d = os.path.join(dest, name)
    os.makedirs(d, exist_ok=True)
    for f in files:
        link = os.path.join(d, os.path.basename(f))
        if os.path.lexists(link):
            os.remove(link)
        os.symlink(os.path.join(out, f), link)
    print(name, len(files), "files")
print("missing", len(missing))
sys.exit(1 if missing else 0)
