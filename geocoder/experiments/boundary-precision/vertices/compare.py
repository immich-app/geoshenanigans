#!/usr/bin/env python3
"""Flip rates of each variant vs the original minimal set, per point pool.
Immich fields follow the prototype mapping (immich feat/geoshenanigans-geocoder
server/src/repositories/geocoding.repository.ts:67-77):
  country = country_code, state = state ?? province,
  city = city ?? town ?? village ?? hamlet ?? municipality."""
import json, sys, os
VARS = sys.argv[1:] or ['identity', 'snap_1e-6', 'snap_1e-5', 'snap_1e-4', 'floor_75', 'scale_2', 'floor_150']
POOLS = ["random", "places", "L2", "L4", "L8", "all"]
def load(path):
    out = []
    for line in open(path):
        a, b, js = line.split(' ', 2)
        out.append(json.loads(js).get('address', {}))
    return out
def immich(ad):
    return (ad.get('country_code'), ad.get('state') or ad.get('province'),
            ad.get('city') or ad.get('town') or ad.get('village') or ad.get('hamlet') or ad.get('municipality'))
print(f"{'variant':10s} {'pool':6s} {'n':>6s} {'any field':>9s} {'immich any':>10s} {'country':>8s} {'state':>7s} {'city':>7s}")
base = {p: load(f'q/orig.{p}.jsonl') for p in POOLS}
for v in VARS:
    for p in POOLS:
        f = f'q/{v}.{p}.jsonl'
        if not os.path.exists(f): continue
        r = load(f); b = base[p]; n = len(b)
        assert len(r) == n, (v, p)
        anyf = sum(x != y for x, y in zip(b, r))
        ib, ir = [immich(x) for x in b], [immich(x) for x in r]
        imm = sum(x != y for x, y in zip(ib, ir))
        c = [sum(x[k] != y[k] for x, y in zip(ib, ir)) for k in range(3)]
        print(f'{v:10s} {p:6s} {n:6d} {anyf/n:9.2%} {imm/n:10.2%} {c[0]/n:8.2%} {c[1]/n:7.2%} {c[2]/n:7.2%}')
