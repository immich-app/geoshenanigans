"""results.json of BASE with the files of OVER replaced (unchanged files reuse BASE's run).

merge_results.py <out.json> <base results.json> <over results.json>
Also merges colsearch JSONs ({file: {"spec":..}}) when given those instead.
"""
import json, sys

out, base, over = sys.argv[1:4]
b, o = json.load(open(base)), json.load(open(over))
if "results" in b:
    keep = {r["file"]: r for r in b["results"]}
    for r in o["results"]:
        keep[r["file"]] = r
    b["results"] = list(keep.values())
    b["src"] = f"{b['src']} + {o['src']}"
else:
    b.update(o)
json.dump(b, open(out, "w"), indent=1)
