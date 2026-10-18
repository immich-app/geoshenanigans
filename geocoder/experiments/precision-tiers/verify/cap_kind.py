import numpy as np
B="/ssd/precision/build"
AP=np.dtype([("lat","<f4"),("lng","<f4"),("hn","<u4"),("st","<u4"),("pw","<u4"),("voff","<u4"),("vcnt","<u4")])
SC=np.dtype([("ot","u1"),("fl","u1"),("tier","u1"),("res","u1"),("sid","<u8")])
ap=np.fromfile(f"{B}/out-base/planet/full/addr_points.bin",dtype=AP)
sc=np.fromfile(f"{B}/out-base/planet/full/addr_points.osm_ids",dtype=SC,offset=12)
v=ap["vcnt"]
m=v>20
print("capped >20:", int(m.sum()), "by object type:", {int(t):int(c) for t,c in zip(*np.unique(sc["ot"][m],return_counts=True))})
print("outlines by object type:", {int(t):int(c) for t,c in zip(*np.unique(sc["ot"][v>0],return_counts=True))})
print("max vcnt for way-keyed:", int(v[(sc["ot"]==2)].max()), " relation-keyed:", int(v[(sc["ot"]==3)].max()) if (sc["ot"]==3).any() else None)
