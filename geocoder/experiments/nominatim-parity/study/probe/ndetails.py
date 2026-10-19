"""ndetails.py <out.json> <T><id> [...] : public Nominatim /details (addressdetails, keywords off,
English) for each OSM object (e.g. W4805756, R17140517, N123), 2.1 s apart."""
import json
import sys
import time
import urllib.parse
import urllib.request

UA = "geoshenanigans-parity/1.0 (+https://github.com/immich-app/geoshenanigans)"
out = {}
for obj in sys.argv[2:]:
    params = {"osmtype": obj[0].upper(), "osmid": obj[1:], "addressdetails": 1, "format": "json",
              "polygon_geojson": 0}
    url = "https://nominatim.openstreetmap.org/details?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept-Language": "en"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            res = json.loads(r.read().decode())
    except Exception as e:  # noqa: BLE001 - record the HTTP error (404 = not in Nominatim)
        res = {"error": str(e)}
    out[obj] = {"time": time.strftime("%Y-%m-%dT%H:%M:%S"), "r": res}
    a = res.get("address") or []
    print(obj, res.get("category"), res.get("type"), "rank", res.get("rank_address"), "centroid",
          (res.get("centroid") or {}).get("coordinates"), res.get("error", ""))
    for line in a:
        print("   ", line.get("isaddress"), line.get("rank_address"), line.get("class"), line.get("type"),
              line.get("osm_type"), line.get("osm_id"), line.get("localname"), line.get("distance"))
    time.sleep(2.1)
json.dump(out, open(sys.argv[1], "w"), ensure_ascii=False, indent=1)
