#!/usr/bin/env python3
"""Cost of the extra names two ways (recommended set: places + major POIs).

A. the separate optional file (extra_names.bin, measured elsewhere)
B. inside the existing string tiers: the names the pool does not hold yet as a new tier
   (deduplicated against strings_core + strings_poi), plus per-record references
   (count, then per name: tag-key byte + u32 string offset), zstd-19 each.
C. POI all tier (max): extra names of every shipped POI from the names-study dump, packed
   like extra_names.bin (count, then per name: key index, length, bytes), zstd-19."""
import collections
import re
import subprocess

P = "/ssd/precision/build/out-nocap/planet/"
Z = "/ssd/shrink/zstd157/bin/zstd"


def z(b):
    return len(subprocess.run([Z, "-19", "-T16", "-c", "-q", "-"], input=b, capture_output=True, check=True).stdout)


def varint(v, out):
    while v >= 0x80:
        out.append((v & 0x7F) | 0x80)
        v >>= 7
    out.append(v)


def pool(path):
    b = open(path, "rb").read()
    return set(b.split(b"\0"))


existing = pool(P + "admin/strings_core.bin") | pool(P + "poi/all/strings_poi.bin")
print("existing strings", len(existing), flush=True)

rows = collections.defaultdict(list)
keys = {}
for line in open("/ssd/search-prod/extras/recommended/extra_names.tsv", encoding="utf-8", errors="surrogateescape"):
    k, rec, cls, key, name = line.rstrip("\n").split("\t", 4)
    rows[(k, int(rec))].append((keys.setdefault(key, len(keys)), name.encode("utf-8", "surrogateescape")))
new = {}
for lst in rows.values():
    for _, n in lst:
        if n not in existing and n not in new:
            new[n] = len(new)
tier = b"\0".join(new.keys()) + b"\0"
refs = bytearray()
for rk in sorted(rows):
    lst = rows[rk]
    varint(len(lst), refs)
    for ki, n in lst:
        refs.append(min(ki, 255))
        refs += (new.get(n, 0)).to_bytes(4, "little")
n_names = sum(len(v) for v in rows.values())
print(f"B: names {n_names}, already in the pool {n_names - sum(1 for v in rows.values() for _, n in v if n in new)}, "
      f"new distinct {len(new)}; new tier raw {len(tier)} zstd {z(bytes(tier))}; refs raw {len(refs)} zstd {z(bytes(refs))}", flush=True)

# ---------------- C: POI all tier
ALT = {"int_name", "alt_name", "old_name", "official_name", "short_name", "loc_name", "reg_name", "nat_name"}
LANG = re.compile(r"^[a-z]{2,3}(?:[-_][A-Za-z0-9]+)*$")


def searchable(k):
    if k == "name":
        return True
    if k.startswith("name:"):
        return bool(LANG.match(k[5:]))
    base, _, suf = k.partition(":")
    return base in ALT and (suf == "" or bool(LANG.match(suf)))


packed = bytearray()
pkeys = {}
n_poi = n_with = n_extra = 0
with open(P.replace("out-nocap/planet/", "names/names.tsv"), encoding="utf-8", errors="surrogateescape") as f:
    for line in f:
        if not line.startswith("poi\t"):
            continue
        p = line.rstrip("\n").split("\t")
        if p[4] != "1":
            continue
        n_poi += 1
        kv = [(p[i], p[i + 1]) for i in range(5, len(p) - 1, 2) if searchable(p[i])]
        d = dict(kv)
        base = d.get("name:en") or d.get("name")
        seen, extra = {base}, []
        for k, v in kv:
            for part in v.split(";"):
                part = part.strip()
                if part and part not in seen:
                    seen.add(part)
                    extra.append((pkeys.setdefault(k, len(pkeys)), part.encode("utf-8", "surrogateescape")))
        if extra:
            n_with += 1
            n_extra += len(extra)
            varint(len(extra), packed)
            for ki, b in extra:
                varint(ki, packed)
                varint(len(b), packed)
                packed += b
print(f"C: shipped POIs {n_poi}, with extra names {n_with}, extra names {n_extra}; packed raw {len(packed)} "
      f"zstd {z(bytes(packed))} (+ 4 B record ref per POI with extras before compression: {4 * n_with})")
