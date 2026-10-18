#!/usr/bin/env python3
"""Download the 'COPY /build/ /build/' layer of immich-server:release linux/amd64 and list it."""
import json
import subprocess
import urllib.request
from registry_sizes import token, get

REPO = "immich-app/immich-server"
tok = token(REPO)
idx = get(REPO, "release", tok)
dig = next(m["digest"] for m in idx["manifests"] if m["platform"].get("architecture") == "amd64")
man = get(REPO, dig, tok)
cfg = get(REPO, man["config"]["digest"], tok, accept="*/*", kind="blobs")
hist = [h for h in cfg["history"] if not h.get("empty_layer")]
i = next(i for i, h in enumerate(hist) if h["created_by"].startswith("COPY /build/ /build/"))
layer = man["layers"][i]
req = urllib.request.Request(f"https://ghcr.io/v2/{REPO}/blobs/{layer['digest']}",
                             headers={"Authorization": f"Bearer {tok}"})
with urllib.request.urlopen(req) as r, open("build_layer.tar.gz", "wb") as f:
    while chunk := r.read(1 << 20):
        f.write(chunk)
print("layer", i, layer["digest"], layer["size"])
print(subprocess.run(["tar", "-tzvf", "build_layer.tar.gz"], capture_output=True, text=True).stdout)
