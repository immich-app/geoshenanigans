"""Fresh verifier point pool (seed 918273, not used by any earlier pool).
stage 'cand': 10k points jittered +-2 km (uniform square) around random LIVE baseline place nodes
              -> fresh_places.txt ; 60k area-uniform sphere points -> fresh_cand.txt
stage 'land': keep the first 10k candidates whose BASELINE answer has a country_code
              (q/base.cand.jsonl) -> fresh_land.txt ; fresh.txt = places + land (20k)."""
import sys, json, numpy as np

V = "/ssd/shrink/stack/verify"
PLACE_DT = np.dtype([('lat', '<f4'), ('lng', '<f4'), ('name_id', '<u4'), ('place_type', 'u1'),
                     ('pad1', 'u1'), ('pad2', '<u2'), ('parent_poly_id', '<u4')])
rng = np.random.default_rng(918273)
if sys.argv[1] == "cand":
    p = np.fromfile("/ssd/shrink/idx/smallest/place_nodes.bin", PLACE_DT)
    live = np.flatnonzero(p["name_id"] != 0xFFFFFFFF)
    pick = rng.choice(live, 10000, replace=False)
    dy, dx = rng.uniform(-2000, 2000, 10000), rng.uniform(-2000, 2000, 10000)
    la = p["lat"][pick].astype(np.float64) + dy / 111320.0
    lo = p["lng"][pick].astype(np.float64) + dx / (111320.0 * np.cos(np.radians(p["lat"][pick].astype(np.float64))))
    la = np.clip(la, -89.999, 89.999)
    lo = (lo + 180.0) % 360.0 - 180.0
    np.savetxt(f"{V}/fresh_places.txt", np.stack([la, lo], 1), fmt="%.7f")
    print("place types picked:", np.bincount(p["place_type"][pick]).tolist())
    u = rng.uniform(-1, 1, 60000)
    c = np.stack([np.degrees(np.arcsin(u)), rng.uniform(-180, 180, 60000)], 1)
    np.savetxt(f"{V}/fresh_cand.txt", c, fmt="%.7f")
else:
    keep = []
    for line in open(f"{V}/q/base.cand.jsonl"):
        la, lo, js = line.rstrip("\n").split(" ", 2)
        if json.loads(js).get("address", {}).get("country_code"):
            keep.append(f"{la} {lo}")
        if len(keep) == 10000:
            break
    print("land candidates kept", len(keep))
    open(f"{V}/fresh_land.txt", "w").write("\n".join(keep) + "\n")
    open(f"{V}/fresh.txt", "w").write(open(f"{V}/fresh_places.txt").read() + "\n".join(keep) + "\n")
