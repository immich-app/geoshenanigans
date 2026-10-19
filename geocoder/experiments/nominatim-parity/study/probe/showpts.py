"""showpts.py <qroot> <pool> <label,label,...> <lat> <lng> [<lat> <lng> ...]
Prints the address of each point in <qroot>/<label>/<pool>.txt (grid_query output lines)."""
import json
import sys

qroot, pool, labels = sys.argv[1], sys.argv[2], sys.argv[3].split(",")
pts = [(f"{float(a):.6f}", f"{float(b):.6f}") for a, b in zip(sys.argv[4::2], sys.argv[5::2])]
want = set(pts)
for lab in labels:
    for line in open(f"{qroot}/{lab}/{pool}.txt", encoding="utf-8"):
        a, b, j = line.split(" ", 2)
        if (a, b) in want:
            print(lab, a, b, json.dumps(json.loads(j).get("address"), ensure_ascii=False))
