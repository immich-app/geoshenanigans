#!/usr/bin/env python3
"""names_check.py sample|check: spot-check the names step's per-place name sets and coverage
against tags read straight from the planet PBF (osmium getid), not the builder's names.tsv.

sample: 2,000 random search places (uniform) + 400 extra boundary places from places.pkl;
        writes ids.txt (the places and, for boundaries, every node links.tsv says they claim).
check:  reads osmium's OPL, rebuilds each place's tag list (boundary: own tags, then claimed
        nodes' tags not already present, nodes by link-method rank then id, as places.py), and
        compares the base / all distinct-string sets and the coverage flags."""
import collections
import pickle
import random
import re
import sys

V = '/ssd/precision/verify/names'
N = '/ssd/precision/names/'
ALT_BASES = {"int_name", "alt_name", "old_name", "official_name", "short_name", "loc_name", "reg_name",
             "nat_name", "sorting_name"}
BASE_SET = ("name", "name:en", "int_name", "alt_name", "official_name", "old_name")
LANG_RE = re.compile(r"^[a-z]{2,3}(?:[-_][A-Za-z0-9]+)*$")
METHOD_RANK = {"rel-label": 0, "rel-wikidata": 1, "way-wikidata": 1, "name": 2}


def is_lang(s):
    return bool(LANG_RE.match(s))


def in_all(k):
    if k == "name":
        return True
    if k.startswith("name:"):
        return is_lang(k[5:])
    base, _, suf = k.partition(":")
    return base in ALT_BASES and (suf == "" or is_lang(suf))


def split_values(v):
    return [p.strip(" ") for p in v.split(";") if p.strip(" ")]


def distinct(tags, keep):
    seen = set()
    for k, v in tags:
        if keep(k):
            for p in split_values(v):
                seen.add(p)
    return seen


def sample():
    d = pickle.load(open(N + 'places.pkl', 'rb'))
    pl = [c for c in d['cand'] if c[0] == 'places']
    print('search places in pkl (with coordinates)', len(pl), 'boundaries', sum(1 for c in pl if c[1] == 'bnd'))
    rnd = random.Random(20261014)
    s = rnd.sample(pl, 2000)
    have = {(c[2], c[3]) for c in s}
    bnds = [c for c in pl if c[1] == 'bnd' and (c[2], c[3]) not in have]
    s += rnd.sample(bnds, 400)
    want = {(c[2], c[3]) for c in s if c[1] == 'bnd'}
    claims = collections.defaultdict(dict)
    for line in open(N + 'run-links/links.tsv'):
        m, t, bid, nid, _ = line.rstrip('\n').split('\t')
        if (t, int(bid)) in want:
            claims[(t, int(bid))].setdefault(int(nid), m)
    out = []
    for c in s:
        out.append(dict(kind=c[1], t=c[2], id=c[3], ptype=c[4], base=set(c[9]), all=set(c[11]),
                        claims=claims.get((c[2], c[3]), {}), uniform=len(out) < 2000))
    pickle.dump(out, open(V + '/sample.pkl', 'wb'))
    ids = set()
    for r in out:
        ids.add(f"{r['t']}{r['id']}")
        for n in r['claims']:
            ids.add(f'n{n}')
    open(V + '/ids.txt', 'w').write('\n'.join(sorted(ids)) + '\n')
    print('sampled', len(out), 'ids', len(ids))


def opl_unescape(s):
    # OPL escapes: %XXXX% hex code points
    return re.sub(r'%([0-9a-fA-F]+)%', lambda m: chr(int(m.group(1), 16)), s)


def check():
    S = pickle.load(open(V + '/sample.pkl', 'rb'))
    tags = {}
    for line in open(V + '/sample.opl', encoding='utf-8'):
        f = line.rstrip('\n').split(' ')
        T = next((x for x in f if x.startswith('T')), 'T')[1:]
        kv = []
        if T:
            for pair in T.split(','):
                k, _, v = pair.partition('=')
                kv.append((opl_unescape(k), opl_unescape(v)))
        tags[(f[0][0], int(f[0][1:]))] = kv
    base_eq = all_eq = missing = 0
    flags = collections.Counter()
    nb = na = 0
    diffs = []
    U = [r for r in S if r['uniform']]
    for r in S:
        own = tags.get((r['t'], r['id']))
        if own is None:
            missing += 1
            continue
        tl = list(own)
        if r['kind'] == 'bnd':
            have = set(tl)
            for nid, _m in sorted(r['claims'].items(), key=lambda kv: (METHOD_RANK.get(kv[1], 9), kv[0])):
                for kv in tags.get(('n', nid), []):
                    if kv not in have:
                        have.add(kv)
                        tl.append(kv)
        b = distinct(tl, lambda k: k in BASE_SET)
        a = distinct(tl, in_all)
        base_eq += b == r['base']
        all_eq += a == r['all']
        if a != r['all'] or b != r['base']:
            diffs.append((r['t'], r['id'], sorted(a ^ r['all'])[:4], sorted(b ^ r['base'])[:4]))
        if r['uniform']:
            keys = {k for k, _ in tl}
            langs = {k[5:] for k in keys if k.startswith('name:') and is_lang(k[5:])}
            flags['name:en'] += 'name:en' in keys
            flags['int_name'] += 'int_name' in keys
            flags['alt_name'] += 'alt_name' in keys
            flags['lang_not_en'] += len(langs - {'en'}) >= 1
            flags['langs>=2'] += len(langs) >= 2
            flags['bnd'] += r['kind'] == 'bnd'
            nb += len(b)
            na += len(a)
    n = len(S) - missing
    print(f'places checked {n} (missing from PBF {missing}); base set equal {base_eq}, all set equal {all_eq}')
    for d in diffs[:10]:
        print('DIFF', d)
    u = len(U)
    print(f'uniform sample n={u}: ' + ', '.join(f'{k} {100 * v / u:.1f}%' for k, v in flags.items()) +
          f'; mean distinct strings base {nb / u:.2f} all {na / u:.2f}')


if __name__ == '__main__':
    if sys.argv[1] in ("sample", "check"): {"sample": sample, "check": check}[sys.argv[1]]()


def unescape_tsv(s):
    if "\\" not in s:
        return s
    out, i = [], 0
    while i < len(s):
        c = s[i]
        if c == "\\" and i + 1 < len(s):
            out.append({"t": "\t", "n": "\n", "r": "\r"}.get(s[i + 1], s[i + 1]))
            i += 2
        else:
            out.append(c)
            i += 1
    return "".join(out)


def flags_of(tl):
    keys = {k for k, _ in tl}
    langs = {k[5:] for k in keys if k.startswith("name:") and is_lang(k[5:])}
    return ("name:en" in keys, "int_name" in keys, "alt_name" in keys, len(langs - {"en"}) >= 1, len(langs) >= 2)


def merged(own, claims, lookup):
    tl = list(own)
    have = set(tl)
    for nid, _m in sorted(claims.items(), key=lambda kv: (METHOD_RANK.get(kv[1], 9), kv[0])):
        for kv in lookup(nid):
            if kv not in have:
                have.add(kv)
                tl.append(kv)
    return tl


def flags():
    S = pickle.load(open(V + "/sample.pkl", "rb"))
    want_n, want_a = set(), set()
    for r in S:
        if r["kind"] == "node":
            want_n.add(r["id"])
        else:
            want_a.add((r["t"], r["id"]))
            want_n.update(r["claims"])
    pn, an = {}, {}
    with open("/ssd/precision/build/names/names.tsv", encoding="utf-8", errors="surrogateescape") as f:
        for line in f:
            if line.startswith("poi\t"):
                continue
            p = line.rstrip("\n").split("\t")
            cls, t, oid = p[0], p[1], int(p[2])
            kv = p[5:]
            tags = [(kv[i], unescape_tsv(kv[i + 1])) for i in range(0, len(kv) - 1, 2)]
            if cls == "place" and oid in want_n:
                pn.setdefault(oid, tags)
            elif cls != "place" and (t, oid) in want_a:
                an.setdefault((t, oid), tags)
    opl = {}
    for line in open(V + "/sample.opl", encoding="utf-8"):
        f = line.rstrip("\n").split(" ")
        T = next((x for x in f if x.startswith("T")), "T")[1:]
        kv = []
        if T:
            for pair in T.split(","):
                k, _, v = pair.partition("=")
                kv.append((opl_unescape(k), opl_unescape(v)))
        opl[(f[0][0], int(f[0][1:]))] = kv
    same = 0
    diff = []
    agg_t = collections.Counter()
    U = 0
    for r in S:
        if r["kind"] == "node":
            mine = opl[("n", r["id"])]
            theirs = pn.get(r["id"], [])
        else:
            mine = merged(opl[(r["t"], r["id"])], r["claims"], lambda n: opl.get(("n", n), []))
            theirs = merged(an.get((r["t"], r["id"]), []), r["claims"], lambda n: pn.get(n, []))
        a, b = flags_of(mine), flags_of(theirs)
        same += a == b
        if a != b:
            diff.append((r["t"], r["id"], a, b))
        if r["uniform"]:
            U += 1
            for i, k in enumerate(("name:en", "int_name", "alt_name", "lang_not_en", "langs>=2")):
                agg_t[k] += b[i]
    print(f"per-place coverage flags equal (PBF vs names.tsv path): {same} of {len(S)}")
    for d in diff[:10]:
        print("FLAGDIFF", d)
    print("uniform sample via names.tsv path: " + ", ".join(f"{k} {100 * v / U:.1f}%" for k, v in agg_t.items()))


if __name__ == "__main__" and sys.argv[1] == "flags":
    flags()
