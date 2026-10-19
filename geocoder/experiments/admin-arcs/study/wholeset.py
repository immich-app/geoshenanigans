#!/usr/bin/env python3
"""wholeset.py <name> ... : raw and zstd (1.5.7 -18 -T1, cached) of every file a flat set ships.
Per-polygon sets: sets/<name>/<set>; arc sets: sets/<name>a/<set> without the decoded
admin_vertices.bin / postal_vertices.bin (the arc reader does not read them) and with poly_arcs.bin.
admin_polygons.osm_ids (a build sidecar) is left out of both. MB = 1e6 bytes."""
import json
import os
import sys

sys.path.insert(0, "/ssd/arcs")
from sizes import cache, zst, CACHE  # noqa: E402

A = "/ssd/arcs"
SKIP = {"admin_polygons.osm_ids", "arc_meta.json"}
for name in sys.argv[1:]:
    line = f"WHOLE {name}"
    for st in ("smallest", "recommended"):
        arc = os.path.isdir(f"{A}/sets/{name}a")
        d = f"{A}/sets/{name}a/{st}" if arc else f"{A}/sets/{name}/{st}"
        raw = z = 0
        for f in sorted(os.listdir(d)):
            if f in SKIP or (arc and f in ("admin_vertices.bin", "postal_vertices.bin")):
                continue
            p = os.path.realpath(f"{d}/{f}")
            raw += os.path.getsize(p)
            z += zst(p)
        line += f" | {st}: raw {raw / 1e6:.1f} zst {z / 1e6:.1f}"
    print(line, flush=True)
json.dump(cache, open(CACHE, "w"))
