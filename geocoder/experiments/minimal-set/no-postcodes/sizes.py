#!/usr/bin/env python3
"""Size table for recommended / no-postcode variants, read from the 2026-10-13 manifest."""
import json

M = "/ssd/shrink/data/configurations-2026-10-13.json"
m = json.load(open(M))
F = m['files']
MiB = 1024 * 1024


def comp(name, region='planet', quality='q2.5'):
    c = m['components'][name]
    sub = lambda p: p.replace('{region}', region).replace('{quality}', quality)
    return ([sub(p) for p in c['files']],
            [sub(p) for p in c.get('optional_files', [])],
            [sub(p) for p in c.get('replaces', [])])


def total(paths):
    return (sum(F[p]['size_zst'] for p in paths), sum(F[p]['size_raw'] for p in paths))


def show(title, paths):
    z, r = total(paths)
    print(f'## {title}: {len(paths)} files, {z/MiB:.1f} MiB zst / {r/MiB:.1f} MiB raw')
    for p in sorted(paths, key=lambda p: -F[p]['size_zst']):
        print(f'   {p:60s} zst {F[p]["size_zst"]/MiB:8.2f}  raw {F[p]["size_raw"]/MiB:8.2f}')
    return z, r


adm_f, adm_o, _ = comp('mode.admin')
q_f, q_o, _ = comp('quality.q2.5')
poi_f, _, _ = comp('poi_tier.major')
min_f, _, _ = comp('mode.admin-minimal')

postcode_only = [p for p in adm_f + adm_o + q_o if 'postcode' in p or 'postal' in p]
rec = adm_f + adm_o + q_f + q_o + poi_f
rec_nopc = [p for p in rec if p not in postcode_only]
show('postcode-only files (admin mode + q2.5)', postcode_only)
show('immich-recommended (admin, q2.5, POI major) incl. optional', rec)
show('recommended WITHOUT postcodes', rec_nopc)
show('admin, q2.5, no POI, no postcodes', [p for p in adm_f + q_f if p not in postcode_only])
show('admin, q2.5, no POI, with postcodes', adm_f + adm_o + q_f + q_o)
show('admin-minimal (planet)', min_f)
show('admin-minimal + POI major', min_f + poi_f)
show('POI major', poi_f)
print('## admin vs admin-minimal: same sha256?')
for p in min_f:
    base = p.split('/')[-1]
    for cand in [f'planet/admin/{base}', f'planet/quality/q2.5/{base}']:
        if cand in F:
            print(f'   {base:24s} admin-min {F[p]["size_raw"]:>11d} zst {F[p]["size_zst"]:>11d} | {cand.split("/")[1]:8s} {F[cand]["size_raw"]:>11d} zst {F[cand]["size_zst"]:>11d} same={F[p]["sha256"]==F[cand]["sha256"]}')
