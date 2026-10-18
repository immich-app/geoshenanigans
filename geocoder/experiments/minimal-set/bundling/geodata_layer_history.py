#!/usr/bin/env python3
"""For recent immich-server release tags (linux/amd64), print the digest and size of the
'COPY /build/ /build/' layer (the geodata), to see how often users re-download it."""
from registry_sizes import get, token

REPO = "immich-app/immich-server"
TAGS = ["v3.3.1", "v3.3.0", "v3.2.4", "v3.2.2", "v3.2.1", "v3.2.0", "v3.1.0", "v3.0.3", "v3.0.2"]
tok = token(REPO)
for tag in TAGS:
    try:
        idx = get(REPO, tag, tok)
    except Exception as e:  # noqa: BLE001 - report and continue
        print(tag, "error", e)
        continue
    dig = next(m["digest"] for m in idx["manifests"] if m["platform"].get("architecture") == "amd64")
    man = get(REPO, dig, tok)
    cfg = get(REPO, man["config"]["digest"], tok, accept="*/*", kind="blobs")
    hist = [h for h in cfg["history"] if not h.get("empty_layer")]
    i = next(i for i, h in enumerate(hist) if h["created_by"].startswith("COPY /build/ /build/"))
    layer = man["layers"][i]
    total = sum(x["size"] for x in man["layers"])
    print(f"{tag} created {cfg.get('created', '?')[:10]} build-layer {layer['digest'][7:19]} {layer['size']} B; image {total} B")
