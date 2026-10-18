#!/usr/bin/env python3
"""Anonymously read ghcr.io manifests for immich-server:release (linux/amd64 + arm64)
and the base-server-prod image, print per-layer compressed sizes and the config history
so the /build/geodata layer can be identified."""
import json
import sys
import urllib.request

ACCEPT = ", ".join([
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
])


def token(repo):
    url = f"https://ghcr.io/token?scope=repository:{repo}:pull"
    return json.load(urllib.request.urlopen(url))["token"]


def get(repo, ref, tok, accept=ACCEPT, kind="manifests"):
    req = urllib.request.Request(f"https://ghcr.io/v2/{repo}/{kind}/{ref}",
                                 headers={"Authorization": f"Bearer {tok}", "Accept": accept})
    with urllib.request.urlopen(req) as r:
        return json.load(r)


def show(repo, tag, arch="amd64"):
    tok = token(repo)
    idx = get(repo, tag, tok)
    if "manifests" in idx:
        plats = [(m["platform"].get("architecture"), m["platform"].get("os"), m["digest"]) for m in idx["manifests"]]
        print(f"{repo}:{tag} index platforms: {[(a, o) for a, o, _ in plats]}")
        dig = next(d for a, o, d in plats if a == arch and o == "linux")
        man = get(repo, dig, tok)
    else:
        man = idx
    cfg = get(repo, man["config"]["digest"], tok, accept="*/*", kind="blobs")
    hist = [h for h in cfg.get("history", []) if not h.get("empty_layer")]
    layers = man["layers"]
    total = sum(l["size"] for l in layers)
    print(f"{repo}:{tag} linux/{arch}: {len(layers)} layers, compressed total {total} B = {total/2**20:.1f} MiB")
    for i, l in enumerate(layers):
        cmd = hist[i]["created_by"][:110] if i < len(hist) else "?"
        print(f"  {i:2d} {l['size']:>11} {l['size']/2**20:8.2f} MiB {l['mediaType'].rsplit('.',1)[-1]:6s} {cmd}")
    labels = cfg.get("config", {}).get("Labels") or {}
    for k in sorted(labels):
        if "version" in k or "created" in k or "revision" in k:
            print(f"  label {k}={labels[k]}")
    return man


if __name__ == "__main__":
    for arch in ("amd64", "arm64"):
        show("immich-app/immich-server", "release", arch)
        print()
