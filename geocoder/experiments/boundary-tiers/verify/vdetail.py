#!/usr/bin/env python3
"""vdetail.py : per-slice detail of cmp/vscore.json (accuracy per border level / photo kind, enclave
points right per grid) plus paired comparisons (points one scheme gets right and the other wrong)."""
import json
import sys

V = "/ssd/tiers2-verify"
R = json.load(open(f"{V}/cmp/vscore.json"))
G8 = ["vatican", "campione", "busingen", "baarle", "llivia", "jungholz", "san_marino", "lesotho"]
SL = ["border_L2", "border_L3", "border_L4", "border_L5", "border_L6", "border_L7", "border_L8",
      "photo_poi", "photo_place", "photo_rural", "photo", "border", "all"]
schemes = sys.argv[1:] or ["q25", "cap10", "rec", "small"]
for st in ("smallest", "recommended"):
    print("==", st)
    print("slice n | " + " | ".join(schemes) + "   (any / city / state / country / cc_wrong, %)")
    for sl in SL:
        cells = []
        for s in schemes:
            r = R[f"{s}_{st}"]["acc"][sl]
            n = r["n"]
            cells.append("/".join(f"{100 * r.get(f, 0) / n:.2f}" for f in ("any", "city", "state", "country", "cc_wrong")))
        print(f"{sl} {R[f'{schemes[0]}_{st}']['acc'][sl]['n']} | " + " | ".join(cells))
    for s in ["unc"] + schemes:
        g = R[f"{s}_{st}"]["grids"]
        print(f"  enclave pts right {s}: " + " ".join(f"{x} {g[x].get('enc_right', 0)}/{g[x].get('n_enc', 0)}" for x in G8)
              + f" | sum {g['sum8'].get('enc_right', 0)} | leaks " + " ".join(str(g[x].get('leak', 0)) for x in G8))
