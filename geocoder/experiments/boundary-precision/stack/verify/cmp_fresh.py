"""usage: cmp_fresh.py <a.jsonl> <b.jsonl> [n_places]  -- answer differences, split places / land half.
Immich mapper: city = city ?? town ?? village ?? hamlet ?? municipality; state = state ?? province;
country = country_code."""
import sys, json


def co(a, *ks):
    for k in ks:
        if a.get(k) is not None:
            return a[k]
    return None


def imm(js):
    a = json.loads(js).get("address", {})
    return (co(a, "city", "town", "village", "hamlet", "municipality"), co(a, "state", "province"), a.get("country_code"))


A = [l.rstrip("\n").split(" ", 2) for l in open(sys.argv[1])]
Bv = [l.rstrip("\n").split(" ", 2) for l in open(sys.argv[2])]
assert len(A) == len(Bv) and all(x[:2] == y[:2] for x, y in zip(A, Bv))
npl = int(sys.argv[3]) if len(sys.argv) > 3 else 10000
ex = []
for name, rng in (("places+-2km", range(0, npl)), ("uniform land", range(npl, len(A))), ("all", range(len(A)))):
    n = len(rng)
    full = [i for i in rng if A[i][2] != Bv[i][2]]
    im = [i for i in full if imm(A[i][2]) != imm(Bv[i][2])]
    per = [sum(imm(A[i][2])[k] != imm(Bv[i][2])[k] for i in im) for k in range(3)]
    empty = sum(1 for i in rng if json.loads(A[i][2]).get("address") is None)
    print(f"| {name} | {n:,} | {len(full)} ({len(full) / n:.3%}) | {len(im)} ({len(im) / n:.3%}) | {per[0]} | {per[1]} | {per[2]} |")
    if name != "all":
        ex += [f"  {name} {A[i][0]} {A[i][1]}: {imm(A[i][2])} -> {imm(Bv[i][2])}" for i in im[:4]]
print("examples:")
print("\n".join(ex))
