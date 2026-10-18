# Checks structural assumptions the transport transform relies on.
import numpy as np, sys, os
d = sys.argv[1]
NO = 0xFFFFFFFF


def recs(name, stride, voff_field, vcnt_field, vname):
    p = os.path.join(d, name); v = os.path.join(d, vname)
    if not os.path.exists(p) or not os.path.exists(v):
        return
    a = np.memmap(p, dtype=np.uint32, mode='r').reshape(-1, stride // 4)
    off = a[:, voff_field].astype(np.int64); cnt = a[:, vcnt_field].astype(np.int64)
    vb = np.memmap(v, dtype=np.uint8, mode='r')
    has = cnt > 0
    nodata_with_cnt = int(((off == NO) & has).sum())
    o = off[has]; c = cnt[has]
    tags = vb[o]
    stride_v = np.where(tags == 3, 8, 4)
    size = 10 + c * stride_v
    mono = bool(np.all(np.diff(o) > 0)) if len(o) > 1 else True
    order = np.argsort(o, kind='stable')
    so = o[order]; ss = size[order]
    dup = int((np.diff(so) == 0).sum())
    gaps = so[1:] - (so[:-1] + ss[:-1])
    print(f"{name}: recs={len(a)} poly={len(o)} nodata_with_cnt={nodata_with_cnt} "
          f"cnt0_offset_not_nodata={int(((off != NO) & ~has).sum())} monotone_in_record_order={mono} "
          f"dup_offsets={dup} first_off={so[0] if len(so) else None} gaps>0={int((gaps > 0).sum())} "
          f"overlaps={int((gaps < 0).sum())} gapbytes={int(gaps[gaps > 0].sum())} "
          f"tail={len(vb) - (so[-1] + ss[-1]) if len(so) else len(vb)} vfile={len(vb)} "
          f"tags={np.bincount(tags, minlength=4).tolist()}", flush=True)


recs("addr_points.bin", 28, 5, 6, "addr_vertices.bin")
recs("poi_records.bin", 36, 2, 3, "poi_vertices.bin")
recs("admin_polygons.bin", 24, 0, 1, "admin_vertices.bin")
recs("postal_polygons.bin", 24, 0, 1, "postal_vertices.bin")


def cellfile(cname, enames, row):
    p = os.path.join(d, cname)
    if not os.path.exists(p):
        return
    raw = np.memmap(p, dtype=np.uint8, mode='r')
    nrow = len(raw) // row
    ids = np.ndarray((nrow,), dtype='<u8', buffer=raw, offset=0, strides=(row,))
    print(f"{cname}: rows={nrow} rem={len(raw) % row} ids_sorted={bool(np.all(np.diff(ids) > 0))}", flush=True)
    for k, en in enumerate(enames):
        offs = np.ndarray((nrow,), dtype='<u4', buffer=raw, offset=8 + 4 * k, strides=(row,)).astype(np.int64)
        if not os.path.exists(os.path.join(d, en)):
            print("  no", en, "nodata=", int((offs == NO).sum())); continue
        eb = np.memmap(os.path.join(d, en), dtype=np.uint8, mode='r')
        valid = offs != NO
        vo = offs[valid]
        # offsets are 32-bit and wrap on files > 4 GiB; detect that
        wraps = int((np.diff(vo) < 0).sum())
        if wraps:
            vo = vo + np.concatenate([[0], np.cumsum(np.diff(vo) < 0)]) * (1 << 32)
        cnt = eb[vo].astype(np.int64) | (eb[vo + 1].astype(np.int64) << 8)
        ends = vo + 2 + 4 * cnt
        contiguous = bool(np.all(vo[1:] == ends[:-1])) and vo[0] == 0 and ends[-1] == len(eb)
        print(f"  col{k} {en}: valid={int(valid.sum())} nodata={int((~valid).sum())} wraps={wraps} "
              f"first={vo[0]} contiguous={contiguous} lastend={ends[-1]} esize={len(eb)} maxcnt={cnt.max()}", flush=True)


cellfile("geo_cells.bin", ["street_entries.bin", "addr_entries.bin", "interp_entries.bin"], 20)
for c, e in [("admin_cells.bin", "admin_entries.bin"), ("place_cells.bin", "place_entries.bin"),
             ("poi_cells.bin", "poi_entries.bin"), ("postcode_centroid_cells.bin", "postcode_centroid_entries.bin")]:
    cellfile(c, [e], 12)


def ways(name, stride, nodes):
    p = os.path.join(d, name)
    if not os.path.exists(p):
        return
    raw = np.memmap(p, dtype=np.uint8, mode='r'); n = len(raw) // stride
    off = np.ndarray((n,), dtype='<u4', buffer=raw, offset=0, strides=(stride,)).astype(np.int64)
    cnt = np.ndarray((n,), dtype='<u2', buffer=raw, offset=4, strides=(stride,)).astype(np.int64)
    nn = os.path.getsize(os.path.join(d, nodes)) // 8
    has = cnt > 0
    print(f"{name}: n={n} cnt0={int((~has).sum())} offNO={int((off == NO).sum())} "
          f"monotone={bool(np.all(np.diff(off[has]) > 0))} "
          f"contiguous={bool(np.all(off[has][1:] == off[has][:-1] + cnt[has][:-1]))} nodes={nn} "
          f"last_end={off[has][-1] + cnt[has][-1]}", flush=True)


ways("street_ways.bin", 12, "street_nodes.bin")
ways("interp_ways.bin", 24, "interp_nodes.bin")
