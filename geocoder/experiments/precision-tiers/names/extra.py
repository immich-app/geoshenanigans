#!/usr/bin/env python3
"""Step "names": a base search file plus an optional extra file.

For each split (base set -> +top10 languages, base set -> +all languages,
+top10 -> +all) writes the extra file's feature-major lines: one line per
feature of the base file (same order, empty when nothing is added), holding the
strings the larger set adds. Levels: L0 search places (from places.pkl, in the
fm files' osm order), L1 major and L2 notable POIs (line-aligned fm files).
Also the extra dictionary: the extra strings not already in the base
dictionary. Output: scopes/extra_*.txt, compressed by compress_extra.sh."""
import pickle

N = "/ssd/precision/names/"
F = N + "files/"
S = N + "scopes/"

SPLITS = (("base", "top10"), ("base", "all"), ("top10", "all"))
IDX = {"base": 9, "top10": 10, "all": 11}


def clean(s):
    return s.replace("\n", " ").replace("\r", " ").replace("\t", " ").replace("\x1f", " ")


d = pickle.load(open(N + "places.pkl", "rb"))
places = sorted((c for c in d["cand"] if c[0] == "places"), key=lambda c: (c[1], c[2], c[3]))


def lines_l0(small, big):
    for c in places:
        a = [clean(x) for x in c[IDX[small]]]
        b = [clean(x) for x in c[IDX[big]]]
        if not a:
            continue  # the base file has no line for this feature
        have = set(a)
        yield a, [x for x in b if x not in have]


def lines_aligned(small, big, lv):
    with open(F + f"fm_{small}_{lv}.txt", encoding="utf-8", errors="surrogateescape") as fa, \
            open(F + f"fm_{big}_{lv}.txt", encoding="utf-8", errors="surrogateescape") as fb:
        for la, lb in zip(fa, fb):
            a = la.rstrip("\n").split("\x1f")
            b = lb.rstrip("\n").split("\x1f")
            assert set(a) <= set(b), (lv, a, b)  # same feature: the larger set holds the smaller
            have = set(a)
            yield a, [x for x in b if x not in have]


for small, big in SPLITS:
    for lv in ("L0", "L1", "L2"):
        gen = lines_l0(small, big) if lv == "L0" else lines_aligned(small, big, lv)
        n = added = 0
        with open(S + f"xfm_{big}_over_{small}_{lv}.txt", "w", encoding="utf-8", errors="surrogateescape") as fm, \
                open(S + f"xstr_{big}_over_{small}_{lv}.txt", "w", encoding="utf-8", errors="surrogateescape") as st:
            for a, extra in gen:
                n += 1
                added += bool(extra)
                fm.write("\x1f".join(extra) + "\n")
                if extra:
                    st.write("\n".join(extra) + "\n")
        print(f"{big} over {small} {lv}: {n} base features, {added} with extra strings", flush=True)
