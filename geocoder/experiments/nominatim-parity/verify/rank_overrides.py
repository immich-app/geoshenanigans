"""rank_overrides.py : place areas (admin level 15) in verify6's base and tip recommended sets whose
place type has a country-specific rank_address in Nominatim's address-levels.json (es/sa
province > 0; ca/de county rank 0; de region rank 0; id/ru/sa municipality 18), by country code."""
from collections import Counter

import numpy as np

PDT = np.dtype([("off", "<u4"), ("cnt", "<u4"), ("name", "<u4"), ("lvl", "u1"), ("pto", "u1"), ("p2", "u1"),
                ("p3", "u1"), ("area", "<f4"), ("cc", "<u2"), ("p4", "<u2")])
PT = {7: "state", 8: "province", 9: "region", 10: "county", 11: "district", 14: "municipality"}


def cc(v):
    return chr(v >> 8) + chr(v & 0xFF) if v else "--"


for data in ("base", "tip"):
    p = np.fromfile(f"/ssd/verify6/sets/{data}/recommended/admin_polygons.bin", PDT)
    m = p["lvl"] == 15
    c = Counter((PT.get(int(t)), cc(int(x))) for t, x in zip(p["pto"][m], p["cc"][m]) if int(t) in PT)
    print(data, "L15 place areas", int(m.sum()))
    for k in ("province", "state", "region"):
        tot = sum(v for (a, _), v in c.items() if a == k)
        print(f"  {k}: {tot}; ES {c.get((k, 'ES'), 0)}, SA {c.get((k, 'SA'), 0)}, DE {c.get((k, 'DE'), 0)}")
    print(f"  county: CA {c.get(('county', 'CA'), 0)}, DE {c.get(('county', 'DE'), 0)}; municipality: ID {c.get(('municipality', 'ID'), 0)}, "
          f"RU {c.get(('municipality', 'RU'), 0)}, SA {c.get(('municipality', 'SA'), 0)}")
