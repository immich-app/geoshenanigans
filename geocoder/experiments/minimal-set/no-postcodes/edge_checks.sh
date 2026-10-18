#!/usr/bin/env bash
# Edge checks for the no-postcode variants:
#  1. partial postcode groups: does the reader refuse them (lib.rs:927-953)?
#  2. the 112 POI major records whose parent_street string lives in the
#     postcode tier: query at each POI, recommended vs rec_nopc (road field).
#  3. Immich-field breakdown admin_nopc_nopoi vs smallest (22k set).
set -uo pipefail
W=/ssd/shrink/no-postcodes
Q=/ssd/shrink/target/release/examples/grid_query
P=/ssd/shrink/venv/bin/python
REC=/ssd/shrink/idx/recommended
cd "$W"

echo "== 1. partial groups"
for drop in postcode_centroids.bin postal_vertices.bin; do
  rm -rf dirs/partial; mkdir -p dirs/partial
  for f in "$REC"/*; do [ "$(basename "$f")" = "$drop" ] || ln "$f" dirs/partial/; done
  printf "48.8584 2.2945\n" | "$Q" dirs/partial > /dev/null 2> out/partial.err
  echo "  without only $drop: rc=$? $(grep -o 'Partial index group.*' out/partial.err | head -1)"
done
rm -rf dirs/partial

echo "== 2. POIs whose parent_street is a postcode-tier string"
$P - > out/pc_street_points.txt <<'PY'
import json, mmap, struct
REC = "/ssd/shrink/idx/recommended"
POI = struct.Struct("<ffIIIBBBBIII")
t = {x["name"]: x for x in json.load(open(f"{REC}/strings_layout.json"))["tiers"]}["postcode"]
fh = open(f"{REC}/poi_records.bin", "rb")
m = mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ)
for i in range(len(m) // POI.size):
    r = POI.unpack_from(m, i * POI.size)
    if t["start"] <= r[9] < t["end"]:
        print(f"{r[0]:.6f} {r[1]:.6f}")
PY
wc -l < out/pc_street_points.txt
"$Q" "$REC" < out/pc_street_points.txt > out/recommended.pcstreet.jsonl
"$Q" dirs/rec_nopc < out/pc_street_points.txt > out/rec_nopc.pcstreet.jsonl
$P compare.py out/recommended.pcstreet.jsonl out/rec_nopc.pcstreet.jsonl --show 3
echo "  road values in rec_nopc output that are empty strings: $(grep -c '"road":""' out/rec_nopc.pcstreet.jsonl)"

echo "== 3. admin_nopc_nopoi vs smallest, Immich fields (22k)"
$P - <<'PY'
import json
from collections import Counter
def load(p):
    return {tuple(l.split(" ", 2)[:2]): json.loads(l.split(" ", 2)[2]) for l in open(p)}
a, b = load("out/admin_nopc_nopoi.p22k.jsonl"), load("out/smallest.p22k.jsonl")
def city(x):
    for k in ("city", "town", "village", "hamlet", "municipality"):
        if x.get(k):
            return x[k]
c = Counter(); ex = []
for k in a:
    x, y = a[k].get("address", {}), b[k].get("address", {})
    for f, g in (("city", city), ("state", lambda z: z.get("state") or z.get("province")), ("country", lambda z: z.get("country_code"))):
        u, v = g(x), g(y)
        if u != v:
            kind = f"{f}: " + ("lost in minimal" if v is None else "gained in minimal" if u is None else "changed")
            c[kind] += 1
            if f == "city" and len(ex) < 8:
                ex.append((" ".join(k), u, v))
print(dict(c.most_common()))
for e in ex:
    print("   ", e)
PY
