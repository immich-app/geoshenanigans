"""osm6.py ids | check : OSM tags behind the builder step's polygon changes (mech6.py output).
ids  : writes /ssd/verify6/osm/ids.txt (r<id> / w<id> lines for osmium getid -i) from
       /ssd/verify6/cmp/mech_<preset>_ids.txt.
check: reads /ssd/verify6/osm/rels.opl (osmium cat -f opl of those objects, planet-261018b) and
       classifies every dropped / added polygon by the rule that should explain it:
  dropped: no_area_type   (relation type not multipolygon/boundary: fff988c)
           postal_no_code (boundary=postal_code relation without postal_code/postcode/addr:postcode: 0094397)
           postal_way     (closed way with boundary=postal_code: 0094397)
           place_rank0    (place=state/province/region, not an admin boundary: f7096b0)
  added:   new_place_area (type boundary/multipolygon, ranked place=*, boundary not administrative /
                           postal_code / place, so bc2e758 skipped it: fff988c)
           minimal_rank16 (smallest only: place area of city/town/village/municipality/county/district
                           that bc2e758 dropped from admin-minimal: 253eb9a)
  anything else: UNEXPLAINED, printed with its tags."""
import re
import sys
from collections import Counter, defaultdict

V = "/ssd/verify6/"
RANKED = {"city", "town", "village", "hamlet", "borough", "suburb", "neighbourhood", "quarter", "county", "district",
          "municipality"}
MIN16 = {"city", "town", "village", "municipality", "county", "district"}


def osm_key(o):
    m = re.match(r"r(\d+)/", o)
    if m:
        return "r" + m.group(1)
    m = re.match(r"t2:(\d+)", o)
    return "w" + m.group(1) if m else None


rows = []
for p in ("smallest", "recommended", "max"):
    for line in open(f"{V}cmp/mech_{p}_ids.txt"):
        kind, osm, lvl, n = line.rstrip("\n").split("\t")
        rows.append((p, kind, osm, int(lvl), int(n)))
if sys.argv[1] == "ids":
    keys = sorted({osm_key(o) for _, k, o, _, _ in rows if k in ("dropped", "added") and osm_key(o)})
    open(V + "osm/ids.txt", "w").write("".join(k + "\n" for k in keys))
    print(len(keys), "ids")
    sys.exit(0)

tags = {}
for line in open(V + "osm/rels.opl", encoding="utf-8"):
    f = line.split(" ")
    oid = f[0]
    t = next((x[1:] for x in f if x.startswith("T")), "")
    d = {}
    for kv in t.split(","):
        if "=" in kv:
            k, v = kv.split("=", 1)
            d[re.sub(r"%([0-9a-f]+)%", lambda m: chr(int(m.group(1), 16)), k)] = re.sub(r"%([0-9a-f]+)%", lambda m: chr(int(m.group(1), 16)), v.strip())
    tags[oid] = d


def classify(preset, kind, osm):
    k = osm_key(osm)
    if k is None:
        return "no_osm_id"
    t = tags.get(k)
    if t is None:
        return "missing_in_planet"
    if k.startswith("w"):
        return "postal_way" if kind == "dropped" and t.get("boundary") == "postal_code" else "UNEXPLAINED"
    typ, bnd, place = t.get("type"), t.get("boundary"), t.get("place")
    if kind == "dropped":
        if typ not in ("multipolygon", "boundary"):
            return "no_area_type"
        if bnd == "postal_code" and not any(t.get(x) for x in ("postal_code", "postcode", "addr:postcode")):
            return "postal_no_code"
        if place in ("state", "province", "region") and bnd != "administrative":
            return "place_rank0"
        return "UNEXPLAINED"
    if typ in ("multipolygon", "boundary") and place in RANKED and bnd not in ("administrative", "postal_code", "place", None):
        return "new_place_area"
    if preset == "smallest" and place in MIN16 and typ in ("multipolygon", "boundary"):
        return "minimal_rank16"
    return "UNEXPLAINED"


cnt = Counter()
ex = defaultdict(list)
for p, kind, osm, lvl, n in rows:
    if kind not in ("dropped", "added"):
        continue
    c = classify(p, kind, osm)
    cnt[(p, kind, c)] += 1
    if len(ex[(p, kind, c)]) < 4:
        k = osm_key(osm)
        t = tags.get(k, {})
        ex[(p, kind, c)].append(f"{osm} L{lvl} type={t.get('type')} boundary={t.get('boundary')} place={t.get('place')} admin_level={t.get('admin_level')} name={t.get('name')}")
print("| preset | kind | class | distinct polygons |\n|---|---|---|---:|")
for (p, kind, c), n in sorted(cnt.items()):
    print(f"| {p} | {kind} | {c} | {n} |")
for key, v in sorted(ex.items()):
    print(key, *v, sep="\n   ")
