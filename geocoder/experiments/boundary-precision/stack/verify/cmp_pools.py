import sys, json
def co(a,*ks):
    for k in ks:
        if a.get(k) is not None: return a[k]
def imm(js):
    a=json.loads(js).get("address",{}); return (co(a,"city","town","village","hamlet","municipality"), co(a,"state","province"), a.get("country_code"))
A,Bn=sys.argv[1],sys.argv[2]
tot=[0,0,0]
for p in ["places","random","L2","L4","L8","all","p200k","ablate3"]:
    a=[l.rstrip("\n").split(" ",2) for l in open(f"qp/{A}.{p}.jsonl")]; b=[l.rstrip("\n").split(" ",2) for l in open(f"qp/{Bn}.{p}.jsonl")]
    assert len(a)==len(b) and all(x[:2]==y[:2] for x,y in zip(a,b))
    full=[i for i in range(len(a)) if a[i][2]!=b[i][2]]; im=[i for i in full if imm(a[i][2])!=imm(b[i][2])]
    per=[sum(imm(a[i][2])[k]!=imm(b[i][2])[k] for i in im) for k in range(3)]
    n=len(a); tot[0]+=n; tot[1]+=len(full); tot[2]+=len(im)
    print(f"| {p} | {n:,} | {len(full)} ({len(full)/n:.3%}) | {len(im)} ({len(im)/n:.3%}) | {per[0]} | {per[1]} | {per[2]} |")
print(f"| total | {tot[0]:,} | {tot[1]} | {tot[2]} |")
