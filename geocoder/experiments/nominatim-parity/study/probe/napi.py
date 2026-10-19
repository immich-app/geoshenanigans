"""napi.py <points> <out.jsonl> : public Nominatim reverse (jsonv2, addressdetails, zoom 18,
English) for each "name lat lng" line, 2.1 s apart; appends {"name","lat","lng","time","r"} lines.
Points already in <out.jsonl> (same name) are skipped, so a rerun resumes.
"""
import json
import os
import sys
import time
import urllib.parse
import urllib.request

UA = "geoshenanigans-parity/1.0 (+https://github.com/immich-app/geoshenanigans)"
pts_path, out_path = sys.argv[1:3]
done = set()
if os.path.exists(out_path):
    done = {json.loads(l)["name"] for l in open(out_path, encoding="utf-8")}
with open(out_path, "a", encoding="utf-8") as out:
    for line in open(pts_path, encoding="utf-8"):
        parts = line.split()
        if len(parts) < 3 or parts[0] in done:
            continue
        name, lat, lng = parts[0], float(parts[1]), float(parts[2])
        params = {"lat": lat, "lon": lng, "format": "jsonv2", "addressdetails": 1, "zoom": 18}
        url = "https://nominatim.openstreetmap.org/reverse?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept-Language": "en"})
        t = time.strftime("%Y-%m-%dT%H:%M:%S")
        with urllib.request.urlopen(req, timeout=30) as r:
            res = json.loads(r.read().decode())
        out.write(json.dumps({"name": name, "lat": lat, "lng": lng, "time": t, "r": res}, ensure_ascii=False) + "\n")
        out.flush()
        print(name, res.get("osm_type"), res.get("osm_id"), json.dumps(res.get("address"), ensure_ascii=False), flush=True)
        time.sleep(2.1)
