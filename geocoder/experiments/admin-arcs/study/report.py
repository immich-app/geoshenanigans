#!/usr/bin/env python3
"""report.py <name> ... : markdown tables of sizes, accuracy, enclaves, loc-to-city, gaps/overlaps
for the named candidates (cmp/<n>.sizes.json, .score.json, .cover.json, l2c/<n>/eval.json,
var/<n>/{min,q}.json). Deltas against the first name (the per-polygon q2.5 baseline)."""
import json
import os
import sys

A = "/ssd/arcs"


def j(p):
    try:
        return json.load(open(p))
    except (OSError, ValueError):
        return None


def pct(r, f):
    return 100 * r.get(f, 0) / r["n"] if r and r.get("n") else float("nan")


def stored(n, part):
    s = j(f"{A}/var/{n}/{part}.json")
    if not s:
        return None
    if "stored_vertices" in s:
        return s["stored_vertices"]
    return sum(v[1] for v in s["levels"].values())


names = sys.argv[1:]
base = names[0]
for part, st, title in (("min", "smallest", "admin-minimal (L2-8)"), ("q", "recommended", "recommended quality dir")):
    print(f"\n### {title}\n")
    print("| candidate | format | raw MB (Δ) | zstd MB (Δ) | vs planned zstd (Δ) | stored vertices | photo any (city/state/country) % | "
          "L2 country % | L4 state % | L8 city % | fresh L7 / L8 city % | enclaves | l2c all_pass | gap / overlap L2·L4·L8 (per 10k) |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    b = j(f"{A}/cmp/{base}.sizes.json")[part]["total"]
    for n in names:
        sz = j(f"{A}/cmp/{n}.sizes.json")
        sc = j(f"{A}/cmp/{n}.score.json")
        cv = j(f"{A}/cmp/{n}.cover.json")
        l2 = j(f"{A}/l2c/{n}/eval.json")
        t = sz[part]["total"] if sz else None
        if t is None:
            continue
        kind = sz["kind"]
        raw, zst = t["raw"], t["zst"]
        planned = t.get("planned_zst", zst)
        cells = [n, "per-polygon" if kind == "pp" else "arcs",
                 f"{raw / 1e6:.1f} ({100 * (raw / b['raw'] - 1):+.1f}%)",
                 f"{zst / 1e6:.1f} ({100 * (zst / b['zst'] - 1):+.1f}%)",
                 f"{planned / 1e6:.1f} ({100 * (planned / b['planned_zst'] - 1):+.1f}%)",
                 f"{(stored(n, part) or 0) / 1e6:.2f}M"]
        if sc:
            a = sc[st]["acc"]
            ph = {k: a["p200k"].get(k, 0) + a["places"].get(k, 0) for k in ("n", "any", "city", "state", "country")}
            g = sc[st]["grids"]["sum8"]
            cells += [f"{pct(ph, 'any'):.2f} ({pct(ph, 'city'):.2f}/{pct(ph, 'state'):.2f}/{pct(ph, 'country'):.2f})",
                      f"{pct(a['L2'], 'country'):.2f}", f"{pct(a['L4'], 'state'):.2f}", f"{pct(a['L8'], 'city'):.2f}",
                      f"{pct(a['border_L7'], 'city'):.2f} / {pct(a['border_L8'], 'city'):.2f}",
                      f"{g.get('enc_right', 0)}/{g.get('n_enc', 0)}"]
        else:
            cells += ["truth"] + [""] * 5
        cells.append(str(l2["summary"][st]["all_pass"]) if l2 else "")
        if cv and part == "min":
            lv = cv["levels"]
            cells.append(" · ".join(f"{1e4 * lv[k]['gap'] / max(1, lv[k]['truth_one']):.1f}/"
                                    f"{1e4 * lv[k]['overlap'] / max(1, lv[k]['truth_one']):.1f}" for k in ("2", "4", "8")))
        else:
            cells.append("")
        print("| " + " | ".join(cells) + " |")
