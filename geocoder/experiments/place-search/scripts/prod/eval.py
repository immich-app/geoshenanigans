#!/usr/bin/env python3
"""Score place-search output against the prototype's evaluation sets.

  eval.py <ours.jsonl> <out_dir> <query tsv>...
A query is a hit at rank r when result r lies within tol_km of the expected point or an
alternate (same rule as /ssd/search/qset/run_eval.py). The prototype's and GeoNames' ranks come
from the prototype's final run (/ssd/search/results/final/per_query.json, same queries)."""
import collections
import json
import math
import os
import sys

PROTO = "/ssd/search/results/final/per_query.json"


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
        if any(km(r["latitude"], r["longitude"], a, b) <= t for a, b, t in targets):
            return i + 1
    return None


def group(cat):
    return {"heldout_big": "h1 name", "heldout_ctx": "h1 name+state", "h2_heldout_big": "h2 name", "h2_heldout_ctx": "h2 name+state",
            "h3_heldout_big": "h3 name", "h3_heldout_ctx": "h3 name+state"}.get(cat, cat)


def main():
    ours_path, out_dir = sys.argv[1], sys.argv[2]
    qs = load_queries(sys.argv[3:])
    ours = [json.loads(l) for l in open(ours_path, encoding="utf-8")]
    assert len(ours) == len(qs), (len(ours), len(qs))
    proto = {(p["cat"], p["q"]): p for p in json.load(open(PROTO))}
    per = []
    for row, o in zip(qs, ours):
        assert o["q"] == row["q"], (o["q"], row["q"])
        p = proto.get((row["cat"], row["q"]), {})
        per.append(dict(cat=row["cat"], grp=group(row["cat"]), q=row["q"], expected=row["expected"],
                        rank=rank_of(o["results"], row["targets"]), proto_rank=p.get("ours_rank"),
                        base_rank=p.get("base_rank"), us=o["us"], lookups=o.get("reverse_lookups"),
                        checks=o.get("context_checks"),
                        top=[(r["name"], r.get("admin1name", ""), r.get("country", ""), r["kind"], r["type"]) for r in o["results"][:3]],
                        proto_top=[(r["name"], r.get("admin1", ""), r.get("admin2", "")) for r in p.get("ours", [])[:3]]))
    os.makedirs(out_dir, exist_ok=True)
    json.dump(per, open(out_dir + "/per_query.json", "w"), ensure_ascii=False, indent=1)
    cats = collections.OrderedDict()
    for p in per:
        cats.setdefault(p["grp"], []).append(p)
    cats["ALL"] = per
    lines = ["| set | n | GeoNames top-1 / top-5 | prototype top-1 / top-5 | this top-1 / top-5 |", "|---|---:|---:|---:|---:|"]
    for c, ps in cats.items():
        n = len(ps)
        f = lambda key, k: sum(1 for p in ps if p[key] is not None and p[key] <= k)
        pc = lambda x: f"{100 * x / n:.0f}%"
        lines.append(f"| {c} | {n} | {pc(f('base_rank', 1))} / {pc(f('base_rank', 5))} | {pc(f('proto_rank', 1))} / {pc(f('proto_rank', 5))} | "
                     f"{pc(f('rank', 1))} / {pc(f('rank', 5))} |")
    lost = [p for p in per if p["proto_rank"] == 1 and p["rank"] != 1]
    won = [p for p in per if p["proto_rank"] != 1 and p["rank"] == 1]
    ex = ["", f"top-1 lost vs prototype: {len(lost)}; gained: {len(won)}", "", "### lost"]
    for p in lost:
        ex.append(f"- [{p['cat']}] `{p['q']}` (want {p['expected']}): rank {p['rank']} -> {p['top']} | proto {p['proto_top']}")
    ex += ["", "### gained"]
    for p in won:
        ex.append(f"- [{p['cat']}] `{p['q']}` (want {p['expected']}): rank {p['rank']} | proto r={p['proto_rank']}")
    open(out_dir + "/summary.md", "w").write("\n".join(lines) + "\n" + "\n".join(ex) + "\n")
    print("\n".join(lines))
    print(ex[1])


if __name__ == "__main__":
    main()
