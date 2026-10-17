#!/usr/bin/env python3
"""Evaluation: Immich's GeoNames search (exact SQL of search.repository.ts:403-427 against
geodata_places in Immich's postgres image) vs the place-search prototype, on
queries_manual.tsv + queries_villages.tsv.

  run_eval.py <ours.jsonl> <out_dir> <query tsv>...
ours.jsonl: place-search output for queries.txt (one JSON per query, same order).
A query is a hit at rank r when result r lies within tol_km of the expected point or of an
alternate. Latency: baseline per query = client-side execute+fetch, median of REPEATS after one
warm-up; ours comes from place-search --bench (separate run)."""
import collections
import json
import math
import statistics
import sys
import time

import psycopg

Q = "/ssd/search/qset/"
REPEATS = 5
# %% escapes the pg_trgm %>> operator for psycopg placeholders.
SQL = """select * from "geodata_places" where
  f_unaccent (name) %%>> f_unaccent (%(q)s)
  or f_unaccent ("admin2Name") %%>> f_unaccent (%(q)s)
  or f_unaccent ("admin1Name") %%>> f_unaccent (%(q)s)
  or f_unaccent ("alternateNames") %%>> f_unaccent (%(q)s)
order by
  coalesce(f_unaccent (name) <->>> f_unaccent (%(q)s), 0.1)
  + coalesce(f_unaccent ("admin2Name") <->>> f_unaccent (%(q)s), 0.1)
  + coalesce(f_unaccent ("admin1Name") <->>> f_unaccent (%(q)s), 0.1)
  + coalesce(f_unaccent ("alternateNames") <->>> f_unaccent (%(q)s), 0.1)
limit %(limit)s"""


def km(a, b, c, d):
    p = math.pi / 180
    h = math.sin((c - a) * p / 2) ** 2 + math.cos(a * p) * math.cos(c * p) * math.sin((d - b) * p / 2) ** 2
    return 12742 * math.asin(math.sqrt(min(1.0, h)))


def load_queries(files):
    rows = []
    for fn in files:
        for line in open(fn, encoding="utf-8"):
            if line.startswith("#") or not line.strip():
                continue
            c = line.rstrip("\n").split("\t")
            targets = [(float(c[3]), float(c[4]), float(c[5]))]
            if len(c) > 6 and c[6]:
                for alt in c[6].split(";"):
                    a, b, t = alt.split(",")
                    targets.append((float(a), float(b), float(t)))
            rows.append(dict(cat=c[0], q=c[1], expected=c[2], targets=targets))
    return rows


def rank_of(results, targets):
    for i, r in enumerate(results):
        if any(km(r["lat"], r["lon"], a, b) <= t for a, b, t in targets):
            return i + 1
    return None


def main():
    ours_path, out_dir = sys.argv[1], sys.argv[2]
    qs = load_queries(sys.argv[3:])
    ours = [json.loads(l) for l in open(ours_path, encoding="utf-8")]
    assert len(ours) == len(qs), (len(ours), len(qs))

    base, base_ms = [], []
    with psycopg.connect("host=127.0.0.1 port=55432 user=postgres password=search dbname=immich") as conn:
        for row in qs:
            params = dict(q=row["q"], limit=20)
            conn.execute(SQL, params).fetchall()
            times = []
            for _ in range(REPEATS):
                t = time.perf_counter()
                res = conn.execute(SQL, params).fetchall()
                times.append((time.perf_counter() - t) * 1e3)
            base_ms.append(statistics.median(times))
            base.append([dict(name=r[1], lat=r[3], lon=r[2], admin1=r[8], admin2=r[9], cc=r[4]) for r in res])

    per = []
    for row, o, b, bms in zip(qs, ours, base, base_ms):
        assert o["q"] == row["q"], (o["q"], row["q"])
        orr = [dict(name=r["name"], lat=r["latitude"], lon=r["longitude"], admin1=r["admin1name"],
                    admin2=r["country"], kind=r["kind"], type=r["type"]) for r in o["results"]]
        per.append(dict(row, ours=orr[:5], base=b[:5], ours_rank=rank_of(orr, row["targets"]),
                        base_rank=rank_of(b, row["targets"]), ours_us=o["us"], base_ms=bms,
                        ours_n=len(orr), base_n=len(b)))
    json.dump(per, open(out_dir + "/per_query.json", "w"), ensure_ascii=False, indent=1)

    cats = collections.OrderedDict()
    for p in per:
        cats.setdefault(p["cat"], []).append(p)
    cats["ALL"] = per
    lines = ["| category | n | GeoNames top-1 | GeoNames top-5 | ours top-1 | ours top-5 | GeoNames p50 / p99 ms | ours (single run) p50 / p99 ms |",
             "|---|---:|---:|---:|---:|---:|---:|---:|"]

    def pct(xs, q):
        xs = sorted(xs)
        return xs[min(len(xs) - 1, int(round((len(xs) - 1) * q)))]

    for c, ps in cats.items():
        n = len(ps)
        f = lambda key, k: sum(1 for p in ps if p[key] is not None and p[key] <= k)
        bl = [p["base_ms"] for p in ps]
        ol = [p["ours_us"] / 1e3 for p in ps]
        lines.append(f"| {c} | {n} | {f('base_rank', 1)} ({100 * f('base_rank', 1) / n:.0f}%) | "
                     f"{f('base_rank', 5)} ({100 * f('base_rank', 5) / n:.0f}%) | "
                     f"{f('ours_rank', 1)} ({100 * f('ours_rank', 1) / n:.0f}%) | "
                     f"{f('ours_rank', 5)} ({100 * f('ours_rank', 5) / n:.0f}%) | "
                     f"{pct(bl, .5):.1f} / {pct(bl, .99):.1f} | {pct(ol, .5):.2f} / {pct(ol, .99):.2f} |")
    summary = "\n".join(lines)

    def fmt(rs):
        return "; ".join(f"{r['name']} ({r.get('admin1') or ''}{', ' + r['admin2'] if r.get('admin2') else ''})" for r in rs[:3]) or "(no result)"

    ours_wins = [p for p in per if p["ours_rank"] == 1 and p["base_rank"] != 1]
    base_wins = [p for p in per if p["base_rank"] == 1 and p["ours_rank"] != 1]
    both_miss = [p for p in per if p["base_rank"] != 1 and p["ours_rank"] != 1]
    ex = ["", f"ours top-1 and GeoNames not: {len(ours_wins)}; GeoNames top-1 and ours not: {len(base_wins)}; "
              f"both miss top-1: {len(both_miss)}", ""]
    for title, group in (("GeoNames wins", base_wins), ("both miss", both_miss), ("ours wins (sample)", ours_wins[:60])):
        ex.append(f"### {title}")
        for p in group:
            ex.append(f"- [{p['cat']}] `{p['q']}` (want {p['expected']}): ours r={p['ours_rank']} -> {fmt(p['ours'])} | "
                      f"GeoNames r={p['base_rank']} -> {fmt(p['base'])}")
        ex.append("")
    open(out_dir + "/summary.md", "w").write(summary + "\n" + "\n".join(ex))
    print(summary)
    print("\n".join(ex[:3]))


if __name__ == "__main__":
    main()
