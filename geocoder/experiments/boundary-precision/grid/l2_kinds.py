"""L2 border pool: how many truth answers have no country (sea side of a maritime border), and how the
country flips of each configuration split into land (country A -> country B) vs sea (country <-> none).
usage: l2_kinds.py name ..."""
import json, sys
def cc(line):
    return json.loads(line.split(" ", 2)[2]).get("address", {}).get("country_code")
T = [cc(l) for l in open("/ssd/shrink/grid/q/T.L2.jsonl")]
n = len(T)
print(f"L2 pool: {n:,} points; truth has no country for {sum(c is None for c in T):,} ({sum(c is None for c in T) / n:.1%})")
for name in sys.argv[1:]:
    O = [cc(l) for l in open(f"/ssd/shrink/grid/q/{name}.L2.jsonl")]
    land = sum(a is not None and b is not None and a != b for a, b in zip(T, O))
    lost = sum(a is not None and b is None for a, b in zip(T, O))
    gain = sum(a is None and b is not None for a, b in zip(T, O))
    print(f"{name:12s} country flips {land + lost + gain:6,} ({(land + lost + gain) / n:.2%}): land A->B {land:,} ({land / n:.2%}), "
          f"country->none {lost:,} ({lost / n:.2%}), none->country {gain:,} ({gain / n:.2%})")
