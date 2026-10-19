import json, sys
sys.path.insert(0, "/ssd/verify5/tmp")
from patchcmp_v5 import compare_pp
# arcs track idealised per-polygon payload for the quality dir, on the real q2.5+cap builds D -> D1
r = compare_pp("/ssd/tiers3/build/out-D/planet/quality/q2.5", "/ssd/tiers3/build/out-D1/planet/quality/q2.5", "/ssd/tiers3/dumpD", "/ssd/tiers3/dumpD1", "q")
print(json.dumps(r))
