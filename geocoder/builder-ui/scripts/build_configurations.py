#!/usr/bin/env python3
"""
Merge the static configurations template with a build's manifest.json
to produce the final per-build configurations.json that the UI consumes.

Inputs:
  - configurations.template.json  (this directory)
  - manifest.json                 (per-build, fetched from Tigris)

Output: configurations.json with `build`, `latest_preset`, and `files`
populated from the manifest's per-file size + sha256 records.
"""
import json
import sys
from pathlib import Path

if len(sys.argv) != 4:
    print("usage: build_configurations.py <template.json> <manifest.json> <out.json>", file=sys.stderr)
    sys.exit(1)

template = json.loads(Path(sys.argv[1]).read_text())
manifest = json.loads(Path(sys.argv[2]).read_text())

template["build"] = {
    "date":          manifest.get("date"),
    "version":       manifest.get("build_version"),
    "patch_version": manifest.get("patch_version"),
    "wikidata_date": manifest.get("wikidata_date"),
    "built_at":      manifest.get("built_at"),
}
template["files"] = {
    f["path"]: {
        "size_zst": f["compressed_size"],
        "size_raw": f["raw_size"],
        "sha256":   f["sha256"],
    }
    for f in manifest.get("files", [])
}

Path(sys.argv[3]).write_text(json.dumps(template, indent=2))
print(f"wrote {sys.argv[3]}: {len(template['files'])} files, {Path(sys.argv[3]).stat().st_size} bytes")
