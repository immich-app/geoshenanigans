import numpy as np, collections
NO=0xFFFFFFFF
ADMIN_DT = np.dtype([("vertex_offset","<u4"),("vertex_count","<u4"),("name_id","<u4"),("admin_level","u1"),("pto","u1"),("pad2","u1"),("pad3","u1"),("area","<f4"),("cc","<u2"),("pad4","<u2")])
B="/ssd/shrink/idx/smallest"; F="/ssd/shrink/stack/final"
b=np.fromfile(f"{B}/admin_polygons.bin",ADMIN_DT); f=np.fromfile(f"{F}/admin_polygons.bin",ADMIN_DT)
ob=np.fromfile(f"{B}/strings_core.bin",np.uint8); nul=np.flatnonzero(ob==0); st=np.concatenate([[0],nul[:-1]+1])
def nm(i):
    j=np.searchsorted(st,i); return bytes(ob[st[j]:nul[j]]).decode()
gone=np.load("area_gone.npy")
m2=b["area"][gone].astype(np.float64)*111320.0**2
print("collapsed slots", len(gone), "| baseline area m2 (approx deg2*111320^2, no cos): median %.1f p90 %.1f p99 %.1f max %.1f"%(np.median(m2),np.percentile(m2,90),np.percentile(m2,99),m2.max()))
print("vertex_count of collapsed: ", np.bincount(np.minimum(b["vertex_count"][gone],20)).tolist())
print("by admin_level:", dict(sorted(collections.Counter(b["admin_level"][gone].tolist()).items())))
print("with place_type_override:", int((b["pto"][gone]!=0).sum()))
key=lambda a,i:(int(a["name_id"][i]),int(a["admin_level"][i]),int(a["cc"][i]))
alive=collections.Counter(); allc=collections.Counter()
for i in range(len(b)):
    if b["name_id"][i]==NO or b["area"][i]<=0: continue
    k=key(b,i); allc[k]+=1
    if f["area"][i]>0: alive[k]+=1
lost=[k for k in allc if alive[k]==0]
print("named (name,level,cc) groups with area>0 in baseline:", len(allc), "| groups with NO ring left (area>0) after snap:", len(lost))
lv=collections.Counter(k[1] for k in lost); print("lost groups by level:", dict(sorted(lv.items())))
# biggest lost groups by baseline area
ga={}
for i in gone:
    k=key(b,i)
    if k in set(lost): ga[k]=ga.get(k,0)+float(b["area"][i])
top=sorted(ga.items(),key=lambda t:-t[1])[:12]
for k,a in top: print("  lost: L%d %r cc=%d baseline area %.2e deg2 (~%.0f m2)"%(k[1],nm(k[0]),k[2],a,a*111320**2))
