"""napi6.py <points> <out.jsonl> : public Nominatim reverse (jsonv2, addressdetails=1, zoom=18,
Accept-Language en) for each "name lat lng" line, >= 2.1 s between calls; appends
{"name","lat","lng","time","r"} lines; names already in <out.jsonl> are skipped (resumable)."""
import json
import os
import sys
import time
import urllib.parse
import urllib.request

UA = "geoshenanigans-verify/1.0 (+https://github.com/immich-app/geoshenanigans)"
pts, outp = sys.argv[1:3]
done = set()
if os.path.exists(outp):
    done = {json.loads(l)["name"] for l in open(outp, encoding="utf-8")}
with open(outp, "a", encoding="utf-8") as out:
    for line in open(pts, encoding="utf-8"):
        p = line.split()
        if len(p) < 3 or p[0] in done:
            continue
        name, lat, lng = p[0], float(p[1]), float(p[2])
        q = urllib.parse.urlencode({"lat": lat, "lon": lng, "format": "jsonv2", "addressdetails": 1, "zoom": 18})
        req = urllib.request.Request("https://nominatim.openstreetmap.org/reverse?" + q,
                                     headers={"User-Agent": UA, "Accept-Language": "en"})
        t = time.strftime("%Y-%m-%dT%H:%M:%S")
        for attempt in range(3):
            try:
                with urllib.request.urlopen(req, timeout=30) as r:
                    res = json.loads(r.read().decode())
                break
            except Exception as e:  # noqa: BLE001 - retry, then record the failure
                res = {"error": str(e)}
                time.sleep(10)
        out.write(json.dumps({"name": name, "lat": lat, "lng": lng, "time": t, "r": res}, ensure_ascii=False) + "\n")
        out.flush()
        print(name, json.dumps(res.get("address"), ensure_ascii=False), flush=True)
        time.sleep(2.1)
