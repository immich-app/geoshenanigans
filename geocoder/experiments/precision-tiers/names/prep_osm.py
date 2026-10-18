#!/usr/bin/env python3
"""places.opl (osmium tags-filter -R of planet-261006: n/place,
r/boundary=administrative, r/place, w/place, w/boundary=administrative) ->
  osm/nodes.tsv  id lat lon place population wikidata wikipedia(0/1) capital
  osm/bnd.tsv    type id boundary admin_level place linked_place wikidata
                 wikipedia(0/1) population label_node admin_centre_node
Coordinates are OSM's own 1e-7 values as osmium prints them."""
import re
import sys

N = "/ssd/precision/names/osm/"
ESC = re.compile(r"%([0-9a-f]+)%")


def unesc(s):
    return ESC.sub(lambda m: chr(int(m.group(1), 16)), s) if "%" in s else s


def tags_of(field):
    out = {}
    if len(field) <= 1:
        return out
    for kv in field[1:].split(","):
        k, _, v = kv.partition("=")
        out[unesc(k)] = unesc(v)
    return out


def clean(v):
    return (v or "").replace("\t", " ").replace("\n", " ")


nodes = open(N + "nodes.tsv", "w")
bnd = open(N + "bnd.tsv", "w")
counts = {"n": 0, "w": 0, "r": 0}
for line in open(N + "places.opl", encoding="utf-8"):
    parts = line.rstrip("\n").split(" ")
    kind = parts[0][0]
    oid = parts[0][1:]
    fields = {p[0]: p for p in parts[1:] if p}
    t = tags_of(fields.get("T", "T"))
    counts[kind] += 1
    if kind == "n":
        if "place" not in t:
            continue
        x = fields.get("x", "x")[1:]
        y = fields.get("y", "y")[1:]
        nodes.write("\t".join([oid, y, x, clean(t.get("place")), clean(t.get("population")),
                               clean(t.get("wikidata")), "1" if "wikipedia" in t else "0",
                               clean(t.get("capital"))]) + "\n")
        continue
    label = centre = ""
    if kind == "r" and "M" in fields and len(fields["M"]) > 1:
        for m in fields["M"][1:].split(","):
            ref, _, role = m.partition("@")
            if ref[:1] != "n":
                continue
            role = unesc(role)
            if role == "label" and not label:
                label = ref[1:]
            elif role == "admin_centre" and not centre:
                centre = ref[1:]
    bnd.write("\t".join([kind, oid, clean(t.get("boundary")), clean(t.get("admin_level")), clean(t.get("place")),
                         clean(t.get("linked_place")), clean(t.get("wikidata")), "1" if "wikipedia" in t else "0",
                         clean(t.get("population")), label, centre]) + "\n")
print(counts, file=sys.stderr)
