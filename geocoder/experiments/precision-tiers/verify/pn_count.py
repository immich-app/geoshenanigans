import numpy as np
B="/ssd/precision/build/out-nocap/planet/full/"
dt=np.dtype([("lat","<f4"),("lng","<f4"),("name_id","<u4"),("pt","u1"),("p1","u1"),("p2","<u2"),("parent","<u4")])
pn=np.fromfile(B+"place_nodes.bin",dtype=dt)
vis=pn["name_id"]!=0xFFFFFFFF
names={0:"city",1:"town",2:"village",3:"suburb",4:"hamlet",5:"neighbourhood",6:"quarter"}
print("place_nodes",len(pn),"hidden",int((~vis).sum()))
for k,v in names.items(): print(v, int(((pn["pt"]==k)&vis).sum()))
print("visible city+town+village+hamlet", int((vis & np.isin(pn["pt"],[0,1,2,4])).sum()))
