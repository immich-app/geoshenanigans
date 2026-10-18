"""Combined geo index ("GXC"): the index track's block index + the streets track's spatial way order
+ the addresses track's cell-ordered address records, measured together (none of the tracks did).

Per row (level-17 cell of today's geo_cells.bin, same rows, same order):
  street list  = today's list, repeated ids dropped, ids mapped old->new (streets remap), sorted
  interp list  = same with the interp remap
  addr         = number of distinct ids in today's list (addresses Tier A renumbers records into cell
                 order, so a row's addresses are the next `count` record ids)
Layout (random access: binary search gx_top, decode <= B rows of one block):
  gxc_top    per block: u64 first cell | u32 rows offset | u32 street stream offset of the list in
             effect at block start (NO_DATA if none) | u32 interp stream offset (same) | u32 first addr id
             of the block | [delta mode: u32 previous new street list first id | u32 same for interp]
  gxc_rows   per row: header byte s | a<<2 | i<<4 | d<<6 (s/i: 0 absent, 1 next stored list, 2 same list
             as this table's previous present row; a: 0/1; d: cell step-1 in level-17 units, 3 = escape),
             LEB128(step-4) if d == 3, LEB128(addr count) if a == 1 [acnt mode 'inline']
  gxc_acnt   LEB128(addr count) per row with addresses [acnt mode 'sep'; top gets u32 offset]
  gxc_<t>    stored lists back to back: LEB128(count-1), first id (LEB128, or zigzag LEB128 delta from
             the previous stored list's first id in 'delta' mode), LEB128(gap-1) for the rest
usage: gxc.py <out_dir> <B> <first: abs|delta> <acnt: inline|sep>
Writes lists_<t>.npz caches in out_dir on first run (cleaned+remapped+sorted lists, addr counts)."""
import os
import sys
import time
import numpy as np

I = "/ssd/shrink/idx/max"
# the streets track's tables (the verifier's re-run reproduced them byte for byte); env overrides
# allow other way orders
REMAP = {"s": os.environ.get("GXC_REMAP_S", "/ssd/shrink/max/streets/reo/street/remap_street.u32"),
         "i": os.environ.get("GXC_REMAP_I", "/ssd/shrink/max/streets/reo/interp/remap_interp.u32")}
NO = 0xFFFFFFFF
GEO_DT = np.dtype([("cell", "<u8"), ("s", "<u4"), ("a", "<u4"), ("i", "<u4")])
out, B, first_mode, acnt_mode = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
os.makedirs(out, exist_ok=True)
t0 = time.time()
log = lambda *a: print(f"[{time.time() - t0:7.1f}s]", *a, flush=True)


def varint(v):
    v = np.asarray(v).astype(np.uint64)
    ln = np.ones(len(v), np.int64)
    t = v >> np.uint64(7)
    while np.any(t):
        ln += (t > 0)
        t >>= np.uint64(7)
    L = int(ln.max()) if len(v) else 1
    o = np.empty(int(ln.sum()), np.uint8)
    pos = np.concatenate([[0], np.cumsum(ln)[:-1]])
    for k in range(L):
        s = ln > k
        b = ((v[s] >> np.uint64(7 * k)) & np.uint64(0x7F)).astype(np.uint8)
        b |= np.where(ln[s] > k + 1, 0x80, 0).astype(np.uint8)
        o[pos[s] + k] = b
    return o, ln


def zz(x):
    x = np.asarray(x, np.int64)
    return np.where(x >= 0, 2 * x, -2 * x - 1).astype(np.uint64)


g = np.fromfile(f"{I}/geo_cells.bin", GEO_DT)
R = len(g)
cell = g["cell"].copy()
cols = {k: g[k].copy() for k in "sai"}
del g
log("rows", R)


def load_lists(fn, offs):
    e = np.fromfile(f"{I}/{fn}", np.uint8)
    o = offs.astype(np.int64)
    cnt = e[o].astype(np.int64) | (e[o + 1].astype(np.int64) << 8)
    tot = int(cnt.sum())
    st = np.concatenate([[0], np.cumsum(cnt)[:-1]])
    lid = np.repeat(np.arange(len(cnt), dtype=np.int64), cnt)
    pos = o[lid] + 2 + 4 * (np.arange(tot, dtype=np.int64) - st[lid])
    ids = (e[pos].astype(np.uint32) | (e[pos + 1].astype(np.uint32) << 8) | (e[pos + 2].astype(np.uint32) << 16)
           | (e[pos + 3].astype(np.uint32) << 24))
    return cnt, ids, lid


def cleaned(key, fn):
    cache = f"{out}/lists_{key}.npz"
    if os.path.exists(cache):
        z = np.load(cache)
        return z["cnt"], z["ids"]
    rows = np.nonzero(cols[key] != NO)[0]
    cnt, ids, lid = load_lists(fn, cols[key][rows])
    if key in REMAP:
        rm = np.fromfile(REMAP[key], "<u4")
        ids = rm[ids]
    k = (lid.astype(np.uint64) << np.uint64(32)) | ids.astype(np.uint64)
    del lid, ids
    k.sort()
    keep = np.ones(len(k), bool)
    keep[1:] = k[1:] != k[:-1]
    k = k[keep]
    lid = (k >> np.uint64(32)).astype(np.int64)
    ids = (k & np.uint64(0xFFFFFFFF)).astype(np.uint32)
    del k
    cnt2 = np.bincount(lid, minlength=len(cnt)).astype(np.int64)
    assert cnt2.min() >= 1
    log(f"{key}: {len(cnt):,} lists, ids {int(cnt.sum()):,} -> {len(ids):,} after dropping repeats")
    np.savez(cache, cnt=cnt2, ids=ids)
    return cnt2, ids


state = {}
streams = {}
cur_at = {}
prevfirst_at = {}
blk = np.arange(0, R, B)
for key, fn in (("s", "street_entries.bin"), ("i", "interp_entries.bin")):
    cnt, ids = cleaned(key, fn)
    J = len(cnt)
    st = np.concatenate([[0], np.cumsum(cnt)[:-1]])
    # same as the previous present row's list?
    same = np.zeros(J, bool)
    cand = np.nonzero(cnt[1:] == cnt[:-1])[0] + 1
    c = cnt[cand]
    rep = np.repeat(np.arange(len(cand)), c)
    kk = np.arange(int(c.sum())) - np.repeat(np.concatenate([[0], np.cumsum(c)[:-1]]), c)
    mism = ids[st[cand][rep] + kk] != ids[st[cand - 1][rep] + kk]
    bad = np.bincount(rep, weights=mism, minlength=len(cand))
    same[cand[bad == 0]] = True
    del rep, kk, mism
    new = ~same
    order = np.nonzero(new)[0]
    log(f"{key}: present rows {J:,}, stored lists {len(order):,}")
    # stored list values: count-1, first, gaps-1
    nc = cnt[order]
    fst = ids[st[order]].astype(np.int64)
    if first_mode == "delta":
        fcode = zz(np.diff(fst, prepend=0))
    else:
        fcode = fst.astype(np.uint64)
    nvals = nc + 1
    vstart = np.concatenate([[0], np.cumsum(nvals)[:-1]])
    vals = np.empty(int(nvals.sum()), np.uint64)
    vals[vstart] = (nc - 1).astype(np.uint64)
    vals[vstart + 1] = fcode
    # gaps: for each stored list, ids[st+1 .. st+c-1] - previous - 1
    gl = nc - 1
    m = gl > 0
    rep = np.repeat(np.nonzero(m)[0], gl[m])
    kk = np.arange(int(gl.sum())) - np.repeat(np.concatenate([[0], np.cumsum(gl[m])[:-1]]), gl[m])
    src = st[order][rep] + 1 + kk
    vals[vstart[rep] + 2 + kk] = (ids[src].astype(np.int64) - ids[src - 1].astype(np.int64) - 1).astype(np.uint64)
    del rep, kk, src
    b, ln = varint(vals)
    lsz = np.add.reduceat(ln, vstart)
    loff = np.concatenate([[0], np.cumsum(lsz)[:-1]])
    assert int(lsz.sum()) == len(b) and len(b) < 2 ** 32
    streams[key] = b
    # per present row: offset of its list
    sidx = np.cumsum(new) - 1
    row_off = loff[sidx]
    rows = np.nonzero(cols[key] != NO)[0]
    s_full = np.zeros(R, np.uint8)
    s_full[rows] = np.where(new, 1, 2)
    state[key] = s_full
    j_at = np.searchsorted(rows, blk)          # first present row at/after block start
    prev = j_at - 1
    cur_at[key] = np.where(prev >= 0, row_off[np.maximum(prev, 0)], NO).astype(np.uint32)
    # first id of the last stored list before the block (delta base)
    fst_full = fst[sidx]
    prevfirst_at[key] = np.where(prev >= 0, fst_full[np.maximum(prev, 0)], 0).astype(np.uint32)
    del cnt, ids, st, same, new, order, vals, b, ln, sidx, row_off, rows, fst_full

# addr counts (distinct ids per row)
cache = f"{out}/lists_a.npz"
if os.path.exists(cache):
    acnt = np.load(cache)["cnt"]
else:
    rows = np.nonzero(cols["a"] != NO)[0]
    cnt, ids, lid = load_lists("addr_entries.bin", cols["a"][rows])
    k = (lid.astype(np.uint64) << np.uint64(32)) | ids.astype(np.uint64)
    del lid, ids
    k.sort()
    keep = np.ones(len(k), bool)
    keep[1:] = k[1:] != k[:-1]
    acnt = np.bincount((k[keep] >> np.uint64(32)).astype(np.int64), minlength=len(cnt)).astype(np.int64)
    del k, keep
    log(f"a: {len(cnt):,} lists, ids {int(cnt.sum()):,} -> {int(acnt.sum()):,} distinct")
    np.savez(cache, cnt=acnt)
apres = cols["a"] != NO
arow = np.zeros(R, np.int64)
arow[apres] = acnt
afirst_at = (np.cumsum(arow) - arow)[blk].astype(np.uint32)

# rows stream
du = np.zeros(R, np.int64)
du[1:] = ((cell[1:] - cell[:-1]) >> np.uint64(27)).astype(np.int64)
assert np.all(du[1:] >= 1)
fob = np.zeros(R, bool)
fob[blk] = True
dcode = np.where(fob, 0, np.minimum(du - 1, 3)).astype(np.uint8)
hdr = (state["s"] | (apres.astype(np.uint8) << 2) | (state["i"] << 4) | (dcode << 6)).astype(np.uint8)
esc = dcode == 3
parts = [(esc, du - 4)]
if acnt_mode == "inline":
    parts.append((apres, arow))
rowlen = np.ones(R, np.int64)
enc = []
for msk, v in parts:
    b, ln = varint(v[msk])
    full = np.zeros(R, np.int64)
    full[msk] = ln
    enc.append((msk, b, full))
    rowlen += full
rowpos = np.concatenate([[0], np.cumsum(rowlen)[:-1]])
buf = np.empty(int(rowlen.sum()), np.uint8)
buf[rowpos] = hdr
at = rowpos + 1
for msk, b, full in enc:
    ln = full[msk]
    p0 = at[msk]
    pos = np.repeat(p0, ln) + (np.arange(int(ln.sum())) - np.repeat(np.concatenate([[0], np.cumsum(ln)[:-1]]), ln))
    buf[pos] = b
    at = at + full
buf.tofile(f"{out}/gxc_rows.bin")
fields = [("cell", "<u8"), ("roff", "<u4"), ("s", "<u4"), ("i", "<u4"), ("afirst", "<u4")]
if acnt_mode == "sep":
    ab, aln = varint(acnt)
    ab.tofile(f"{out}/gxc_acnt.bin")
    a_off = np.concatenate([[0], np.cumsum(aln)])            # stream offset before each present row
    aj = np.searchsorted(np.nonzero(apres)[0], blk)
    fields.append(("aoff", "<u4"))
if first_mode == "delta":
    fields += [("sprev", "<u4"), ("iprev", "<u4")]
top = np.zeros(len(blk), np.dtype(fields))
top["cell"] = cell[blk]
assert rowpos[blk].max() < 2 ** 32
top["roff"] = rowpos[blk]
top["s"], top["i"], top["afirst"] = cur_at["s"], cur_at["i"], afirst_at
if acnt_mode == "sep":
    top["aoff"] = a_off[aj]
if first_mode == "delta":
    top["sprev"], top["iprev"] = prevfirst_at["s"], prevfirst_at["i"]
top.tofile(f"{out}/gxc_top.bin")
streams["s"].tofile(f"{out}/gxc_street.bin")
streams["i"].tofile(f"{out}/gxc_interp.bin")
log(f"B {B} first {first_mode} acnt {acnt_mode}: rows {len(buf):,} B, top {top.nbytes:,} B, street {len(streams['s']):,} B, "
    f"interp {len(streams['i']):,} B, delta codes {np.bincount(dcode, minlength=4).tolist()}")
