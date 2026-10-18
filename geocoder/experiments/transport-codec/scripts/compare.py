"""Per-file table: published zstd size vs one or more run_cfg results.

compare.py <published: files.tsv | manifest.json:prefix1,prefix2,...> <label=results.json> ...
Sizes in MiB. 'seven' = the files the description reports on.
"""
import json, os, sys

sys.path.insert(0, "/ssd/transport/scripts")
import specs  # noqa: E402

pub_arg = sys.argv[1]
pub = {}
if pub_arg.endswith(".tsv"):
    for line in open(pub_arg):
        f, raw, z, *_ = line.rstrip("\n").split("\t")
        pub[os.path.basename(f)] = int(z)
else:
    man, prefixes = pub_arg.split(":", 1)
    F = json.load(open(man))["files"]
    for p in prefixes.split(","):
        for k, v in F.items():
            if k.startswith(p) and "/" not in k[len(p):]:
                pub[os.path.basename(k)] = pub.get(os.path.basename(k), 0) + v["size_zst"]
runs = []
for arg in sys.argv[2:]:
    label, path = arg.split("=", 1)
    j = json.load(open(path))
    rr = {r["file"]: r for r in j["results"]}
    if j["cfg"] == "raw":  # measured plain zstd: keep its own numbers
        for r in rr.values():
            r["kind"] = "plain"
    runs.append((label, rr))
M = 1048576
names = sorted(pub, key=lambda n: -pub[n])
hdr = "| file | published | " + " | ".join(f"{l} | Δ" for l, _ in runs) + " |"
print(hdr)
print("|---|---:|" + "---:|---:|" * len(runs))
tot = {"all": [0] * (1 + len(runs)), "seven": [0] * (1 + len(runs))}
for n in names:
    row = [pub[n]]
    cells = []
    for l, r in runs:
        z = r[n]["zst"] if n in r else pub[n]
        # files left raw keep the published size (the tt header only adds bytes)
        if n in r and r[n]["kind"] == "r":
            z = pub[n]
        row.append(z)
        cells.append(f"{z / M:,.2f} | {100 * (z - pub[n]) / pub[n]:+.0f}%")
    for k in tot:
        if k == "all" or n in specs.SEVEN:
            tot[k] = [a + b for a, b in zip(tot[k], row)]
    if pub[n] >= 0.005 * M:
        print(f"| {n} | {pub[n] / M:,.2f} | " + " | ".join(cells) + " |")
for k, row in tot.items():
    cells = [f"**{z / M:,.2f}** | {100 * (z - row[0]) / row[0]:+.1f}%" for z in row[1:]]
    print(f"| **total {k}** | **{row[0] / M:,.2f}** | " + " | ".join(cells) + " |")
bad = [n for _, r in runs for n, x in r.items() if x.get("ok") is False]
print("roundtrip failures:", bad or "none")
