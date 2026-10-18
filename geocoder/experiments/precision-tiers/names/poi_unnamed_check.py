# Which shipped tier-1..3 POIs carry a name but no name-like tag row in names.tsv?
import numpy as np, collections, json
B="/ssd/precision/build/out-nocap/planet/"
slot_dt=np.dtype([("type","u1"),("flags","u1"),("tier","u1"),("res","u1"),("id","<u8")])
raw=np.fromfile(B+"poi/all/poi_records.osm_ids",dtype=np.uint8)
s=np.frombuffer(raw[12:].tobytes(),dtype=slot_dt)
sel=np.nonzero(s["tier"]<=3)[0]
rec_dt=np.dtype([("lat","<f4"),("lng","<f4"),("voff","<u4"),("vcnt","<u4"),("name_id","<u4"),("cat","u1"),("tier","u1"),("flags","u1"),("imp","u1"),("street","<u4"),("postcode","<u4"),("poly","<u4")])
rec=np.fromfile(B+"poi/all/poi_records.bin",dtype=rec_dt)
assert len(sel)==len(rec), (len(sel),len(rec))
assert (s["tier"][sel]==rec["tier"]).all()
keys=(s["type"][sel].astype(np.uint64)<<np.uint64(56))|s["id"][sel]
T={b"n":1,b"w":2,b"r":3}
nk=[]
with open("/ssd/precision/build/names/names.tsv","rb") as f:
    for line in f:
        if line.startswith(b"poi\t"):
            p=line.split(b"\t",4); nk.append((T[p[1]]<<56)|int(p[2]))
nk=np.unique(np.array(nk,dtype=np.uint64))
has=np.isin(keys,nk)
lay=json.load(open(B+"poi/all/strings_layout.json"))["tiers"]
blobs={t["name"]:(t["start"],open(B+"poi/all/"+t["file"] if t["name"]=="poi" else B+"full/"+t["file"],"rb").read()) for t in lay}
def name(nid):
    for t in lay:
        if t["start"]<=nid<t["end"]:
            st,b=blobs[t["name"]]; o=nid-st; e=b.index(b"\0",o); return b[o:e].decode("utf-8","replace")
    return None
named=rec["name_id"]!=0xFFFFFFFF
for t in (1,2,3):
    m=(rec["tier"]==t)&named
    miss=m&~has
    print("tier",t,"named records",int(m.sum()),"without a names.tsv row",int(miss.sum()), "by osm type", {int(k):int(v) for k,v in zip(*np.unique(s["type"][sel][miss],return_counts=True))})
    cats=collections.Counter(rec["cat"][miss].tolist()).most_common(8); print("  categories",cats)
    idx=np.nonzero(miss)[0][:: max(1,int(miss.sum())//8)][:8]
    for i in idx: print("   ", "nwr"[int(s["type"][sel][i])-1], int(s["id"][sel][i]), repr(name(int(rec["name_id"][i]))))
