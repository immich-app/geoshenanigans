#!/usr/bin/env python3
"""Writes the builder's marine input (build-index --marine): the seas and oceans of the IHO's sea areas, as
Marine Regions draws them, cut to the water off OpenStreetMap's land.

Usage:
  marine_import.py build --eez-iho EEZ_IHO --land LAND -o marine_areas.bin [--stats STATS.json]
                         [--workers N] [--bbox WEST,SOUTH,EAST,NORTH]
  marine_import.py stats marine_areas.bin

EEZ_IHO is Marine Regions' intersect of the Exclusive Economic Zones and IHO sea areas, version 5 (CC BY 4.0),
as its WFS serves it (typeNames=MarineRegions:eez_iho, GeoJSON). LAND is osmdata.openstreetmap.de's land
polygons, made from OpenStreetMap's coastline, split, in WGS84 (land-polygons-split-4326.zip, read inside the
zip; ODbL). Both are pinned: the nightly never runs this, it downloads what a run of it published
(geocoder/builder/data/marine.json), so the marine areas don't change from one day to the next.

Each IHO sea area (iho_mrgid and iho_sea: its features in every EEZ together) is one marine object, an ocean
when its name ends in " Ocean" and a sea otherwise. Its polygons are its water, one 1-degree tile at a time:
  - the tile's water is the tile less the land polygons in it;
  - each sea area takes the water inside it;
  - the water no sea area covers (the sea areas' shores are coarser than OpenStreetMap's: the strip along a
    coast, the water round islands they leave out, the heads of fjords) goes to the one sea area within a
    tile of the tile when there is just one. With more, it is cut along a 0.1-degree grid and each bit goes
    to the sea area nearest it (of those touching it, the one it shares the most border with, then the
    lowest MRGID);
  - water no sea area comes within a tile of stays out: the Caspian, which the coastline holds and no sea
    area does.
Overlays run on the e7 grid (GEOS' fixed precision), so every vertex is an e7 coordinate as the builder reads
it, and a cut along a tile line falls alike in the tiles either side. A tile's pieces are its polygons: no
polygon spans more than a tile, and the builder's cells and lookups stay local.

Output, little-endian:
  "GCMARINE", u32 version (1), u32 object count; then each object by id: u64 id (the sea area's MRGID), u8
  kind (1 sea, 2 ocean), u16 name bytes, the name (UTF-8), u32 polygon count; each polygon: u32 ring count
  (its outer ring, then its holes), each ring: u32 vertex count (at least 3, the first not repeated at the
  end), then its vertices as i32 latitude and i32 longitude in e7 units, the first as they are and each next
  as its difference from the one before (which compresses to well under half). An object's polygons go by tile,
  south to north and then west to east, a tile's by their smallest vertex (latitude, then longitude); outer
  rings run counterclockwise, holes clockwise.

`stats` prints a file's objects, polygons, rings and vertices, one line each object and a total.
"""
import argparse
import json
import os
import struct
import sys
import time

MAGIC = b"GCMARINE"
VERSION = 1
KIND_SEA, KIND_OCEAN = 1, 2
KIND_NAMES = {KIND_SEA: "sea", KIND_OCEAN: "ocean"}

# The overlays' grid: e7 units, in degrees.
GRID = 1e-7
# The grid water no sea area covers is cut along, where more than one is near (degrees per side).
GAP_CELL_DIVISIONS = 10

E7_LIMITS = {"lat": 900000000, "lon": 1800000000}

HEADER = struct.Struct("<8sII")
OBJECT_HEAD = struct.Struct("<QBH")
COUNT = struct.Struct("<I")


def kind_of(name):
    """An IHO sea area's kind: the oceans are the areas named "... Ocean"."""
    return KIND_OCEAN if name.endswith(" Ocean") else KIND_SEA


def _ring_bytes(ring, oid):
    """A ring's vertices as the file holds them: (lat, lon) pairs, i32 each, the first as it is and each next
    as its difference from the one before."""
    if hasattr(ring, "tobytes"):
        import numpy as np
        d = ring.astype(np.int64)
        d[1:] -= ring[:-1]
        if abs(d).max() >= 1 << 31:
            raise ValueError(f"object {oid}: an edge too long for its step")
        return len(ring), d.astype("<i4").tobytes()
    flat = list(ring[0])
    for (a, b), (c, d) in zip(ring, ring[1:]):
        flat += [c - a, d - b]
    if any(abs(v) >= 1 << 31 for v in flat):
        raise ValueError(f"object {oid}: an edge too long for its step")
    return len(ring), struct.pack(f"<{len(flat)}i", *flat)


def _check_ring(ring, oid):
    n = len(ring)
    if n < 3:
        raise ValueError(f"object {oid}: a ring of {n} vertices")
    if hasattr(ring, "tolist"):
        lat, lon = abs(ring[:, 0]).max(), abs(ring[:, 1]).max()
        if lat <= E7_LIMITS["lat"] and lon <= E7_LIMITS["lon"]:
            return
        ring = ring.tolist()
    for lat, lon in ring:
        if abs(lat) > E7_LIMITS["lat"] or abs(lon) > E7_LIMITS["lon"]:
            raise ValueError(f"object {oid}: vertex ({lat}, {lon}) outside the world")


def write_marine(path, objects):
    """Writes `objects`, (id, kind, name, polygons), each polygon a list of rings (outer first), each ring
    (lat_e7, lon_e7) pairs (a list of tuples, or an int array of shape (n, 2)), to `path` by id: first to
    path + ".part", renamed when whole."""
    ordered = sorted(objects, key=lambda o: o[0])
    for a, b in zip(ordered, ordered[1:]):
        if a[0] == b[0]:
            raise ValueError(f"object {a[0]} twice")
    tmp = path + ".part"
    with open(tmp, "wb") as f:
        f.write(HEADER.pack(MAGIC, VERSION, len(ordered)))
        for oid, kind, name, polygons in ordered:
            if kind not in KIND_NAMES:
                raise ValueError(f"object {oid}: kind {kind}")
            encoded = name.encode("utf-8")
            if not encoded or len(encoded) > 0xFFFF:
                raise ValueError(f"object {oid}: a name of {len(encoded)} bytes")
            f.write(OBJECT_HEAD.pack(oid, kind, len(encoded)) + encoded + COUNT.pack(len(polygons)))
            for rings in polygons:
                if not rings:
                    raise ValueError(f"object {oid}: a polygon of no rings")
                f.write(COUNT.pack(len(rings)))
                for ring in rings:
                    _check_ring(ring, oid)
                    n, data = _ring_bytes(ring, oid)
                    f.write(COUNT.pack(n) + data)
    os.replace(tmp, path)


def read_marine(path):
    """A file write_marine wrote: its objects, (id, kind, name, polygons), each ring a list of (lat, lon)."""
    with open(path, "rb") as f:
        data = f.read()
    at = 0

    def take(n):
        nonlocal at
        if at + n > len(data):
            raise ValueError(f"{path}: cut short at byte {at}")
        piece = data[at:at + n]
        at += n
        return piece

    magic, version, count = HEADER.unpack(take(HEADER.size))
    if magic != MAGIC or version != VERSION:
        raise ValueError(f"{path}: not a marine areas file of version {VERSION}")
    objects = []
    for _ in range(count):
        oid, kind, name_len = OBJECT_HEAD.unpack(take(OBJECT_HEAD.size))
        name = take(name_len).decode("utf-8")
        polygons = []
        for _ in range(COUNT.unpack(take(4))[0]):
            rings = []
            for _ in range(COUNT.unpack(take(4))[0]):
                n = COUNT.unpack(take(4))[0]
                flat = struct.unpack(f"<{2 * n}i", take(8 * n))
                ring, lat, lon = [], 0, 0
                for k in range(n):
                    lat, lon = (flat[0], flat[1]) if k == 0 else (lat + flat[2 * k], lon + flat[2 * k + 1])
                    ring.append((lat, lon))
                rings.append(ring)
            polygons.append(rings)
        objects.append((oid, kind, name, polygons))
    if at != len(data):
        raise ValueError(f"{path}: {len(data) - at} bytes past its last object")
    return objects


def stats(path):
    total = [0, 0, 0, 0]
    for oid, kind, name, polygons in read_marine(path):
        rings = sum(len(p) for p in polygons)
        vertices = sum(len(r) for p in polygons for r in p)
        print(f"{oid}\t{KIND_NAMES[kind]}\t{name}\t{len(polygons)} polygons\t{rings} rings\t{vertices} vertices")
        for k, v in enumerate((1, len(polygons), rings, vertices)):
            total[k] += v
    print(f"total\t{total[0]} objects\t{total[1]} polygons\t{total[2]} rings\t{total[3]} vertices")


# --- The geometry (shapely, numpy and pyogrio, which only `build` needs) ---

# What the forked tile workers read: set before the pool starts.
_LAND = None
_LAND_TREE = None
_SEA_TILES = None


def _polygon_parts(geoms):
    """The polygons of any area in a geometry or an array of them, collections in collections (as GEOS'
    repair gives: a multipolygon beside the lines it cut off) taken apart to the last."""
    import shapely
    parts = shapely.get_parts(geoms)
    while (shapely.get_type_id(parts) >= 4).any():
        parts = shapely.get_parts(parts)
    return [p for p in parts if shapely.get_type_id(p) == 3 and p.area > 0]


def _polygons_of(geom):
    """The polygons of an overlay's result, as one (Multi)Polygon, or None for none of any area."""
    import shapely
    parts = _polygon_parts(geom)
    if not parts:
        return None
    return parts[0] if len(parts) == 1 else shapely.MultiPolygon(parts)


def _split_into_tiles(item):
    """A sea area part, (mrgid, wkb): its pieces in each 1-degree tile it reaches, (tile, mrgid, wkb), cut
    by halving its box along whole degrees."""
    import shapely
    mrgid, wkb = item
    geom = shapely.from_wkb(wkb)
    x0, y0, x1, y1 = shapely.bounds(geom)
    box = (int(x0 // 1), int(y0 // 1), int(-(-x1 // 1)), int(-(-y1 // 1)))
    box = (box[0], box[1], max(box[2], box[0] + 1), max(box[3], box[1] + 1))
    out = []
    todo = [(geom, box)]
    while todo:
        g, (bx0, by0, bx1, by1) = todo.pop()
        if bx1 - bx0 == 1 and by1 - by0 == 1:
            out.append(((bx0, by0), mrgid, shapely.to_wkb(g)))
            continue
        if bx1 - bx0 >= by1 - by0:
            m = (bx0 + bx1) // 2
            halves = [(bx0, by0, m, by1), (m, by0, bx1, by1)]
        else:
            m = (by0 + by1) // 2
            halves = [(bx0, by0, bx1, m), (bx0, m, bx1, by1)]
        gx0, gy0, gx1, gy1 = shapely.bounds(g)
        for h in halves:
            if gx0 >= h[0] and gy0 >= h[1] and gx1 <= h[2] and gy1 <= h[3]:
                todo.append((g, h))
                continue
            if gx1 <= h[0] or gy1 <= h[1] or gx0 >= h[2] or gy0 >= h[3]:
                continue
            piece = _polygons_of(shapely.intersection(g, shapely.box(*h), grid_size=GRID))
            if piece is not None:
                todo.append((piece, h))
    return out


def _neighbours(key):
    """The tiles around `key` (and it), each with the shift in longitude that puts it beside `key` across
    the antimeridian."""
    x, y = key
    for dy in (-1, 0, 1):
        if not -90 <= y + dy < 90:
            continue
        for dx in (-1, 0, 1):
            nx, shift = x + dx, 0
            if nx < -180:
                nx, shift = nx + 360, -360
            elif nx >= 180:
                nx, shift = nx - 360, 360
            yield (nx, y + dy), shift


def _rings_e7(poly):
    """A polygon's rings as the file holds them, outer counterclockwise and holes clockwise, the closing
    vertex and repeated vertices dropped; None when its outer ring has fewer than 3 left. A hole of fewer
    goes."""
    import numpy as np
    import shapely
    poly = shapely.orient_polygons(poly)
    out = []
    for k, ring in enumerate([poly.exterior] + list(poly.interiors)):
        xy = np.rint(shapely.get_coordinates(ring) * 1e7).astype(np.int64)[:-1]
        if len(xy):
            xy = xy[np.any(xy != np.roll(xy, 1, axis=0), axis=1)]
        if len(xy) < 3:
            if k == 0:
                return None
            continue
        out.append(np.stack([xy[:, 1], xy[:, 0]], axis=1).astype("<i4"))
    return out


def _smallest_vertex(rings):
    import numpy as np
    outer = rings[0]
    i = np.lexsort((outer[:, 1], outer[:, 0]))[0]
    return int(outer[i, 0]), int(outer[i, 1])


def _tile_job(key):
    """A tile's marine polygons, (mrgid, tile, smallest vertex, rings), and its counts."""
    import numpy as np
    import shapely
    x, y = key
    counts = {"tiles": 1, "gap_km2": 0.0, "gap_dropped_km2": 0.0, "gap_bits": 0}
    near = {}  # mrgid -> its pieces in the tiles around, shifted beside this one
    for k, shift in _neighbours(key):
        for mrgid, pieces in _SEA_TILES.get(k, {}).items():
            near.setdefault(mrgid, []).extend(
                pieces if shift == 0 else [shapely.transform(p, lambda c, s=shift: c + [s, 0]) for p in pieces])
    if not near:
        return [], counts
    tile = shapely.box(x, y, x + 1, y + 1)
    found = _LAND_TREE.query(tile, predicate="intersects")
    water = tile
    # The land polygons' areas in the tile (a polygon touching it along an edge gives a line).
    land = _polygon_parts(shapely.intersection(_LAND[found], tile, grid_size=GRID))
    if land:
        land = shapely.union_all(land, grid_size=GRID)
        water = _polygons_of(shapely.difference(tile, land, grid_size=GRID))
    if water is None:
        return [], counts
    here = {m: shapely.union_all(p, grid_size=GRID) for m, p in _SEA_TILES.get(key, {}).items()}
    scale = 111.32 * 111.32 * np.cos(np.radians(y + 0.5))
    if len(near) == 1:
        (only,) = near
        taken = {only: water}
        if only in here:
            gap = _polygons_of(shapely.difference(water, here[only], grid_size=GRID))
            counts["gap_km2"] += 0 if gap is None else gap.area * scale
        else:
            counts["gap_km2"] += water.area * scale
    else:
        taken = {}
        for m, g in here.items():
            inside = _polygons_of(shapely.intersection(water, g, grid_size=GRID))
            if inside is not None:
                taken[m] = [inside]
        covered = shapely.union_all(list(here.values()), grid_size=GRID) if here else None
        gap = water if covered is None else _polygons_of(shapely.difference(water, covered, grid_size=GRID))
        if gap is not None:
            step = 1 / GAP_CELL_DIVISIONS
            cells = [shapely.box(x + i * step, y + j * step, x + (i + 1) * step, y + (j + 1) * step)
                     for j in range(GAP_CELL_DIVISIONS) for i in range(GAP_CELL_DIVISIONS)]
            bits = _polygon_parts(shapely.intersection(gap, cells, grid_size=GRID))
            pieces = [p for m in sorted(near) for p in near[m]]
            owner = [m for m in sorted(near) for _ in near[m]]
            # Each bit's nearest pieces, every one at that distance.
            pairs, dist = shapely.STRtree(pieces).query_nearest(bits, all_matches=True, return_distance=True)
            tied = {}
            for i, j, d in zip(pairs[0], pairs[1], dist):
                tied.setdefault(int(i), (d, set()))[1].add(owner[j])
            # Each sea area's border round its pieces together, so a stretch two of them share counts once.
            borders = {}
            for i, bit in enumerate(bits):
                d, owners = tied[i]
                owners = sorted(owners)
                best = owners[0]
                if len(owners) > 1 and d == 0:
                    edge = shapely.boundary(bit)
                    for m in owners:
                        if m not in borders:
                            borders[m] = shapely.boundary(shapely.union_all(near[m], grid_size=GRID))
                    shared = [shapely.length(shapely.intersection(edge, borders[m], grid_size=GRID)) for m in owners]
                    best = owners[int(np.argmax(shared))]
                taken.setdefault(best, []).append(bit)
                counts["gap_bits"] += 1
                counts["gap_km2"] += bit.area * scale
        taken = {m: shapely.union_all(g, grid_size=GRID) for m, g in taken.items()}
    out = []
    for m, g in taken.items():
        for poly in _polygon_parts(g):
            rings = _rings_e7(poly)
            if rings is not None:
                out.append((m, key, _smallest_vertex(rings), rings))
    return out, counts


def _valid_parts(item):
    """A sea area feature, (mrgid, wkb): its polygons made valid on the e7 grid, (mrgid, wkb) each."""
    import shapely
    mrgid, wkb = item
    g = _polygons_of(shapely.set_precision(shapely.make_valid(shapely.from_wkb(wkb)), GRID))
    return [] if g is None else [(mrgid, shapely.to_wkb(p)) for p in shapely.get_parts(g)]


def _read_seas(path, pool):
    """The sea areas: each MRGID's name, and its parts as (mrgid, wkb), valid and on the e7 grid."""
    import pyogrio
    # The fields come in the source's order, whatever the order asked for: each is taken by its name.
    meta, _, geoms, fields = pyogrio.raw.read(path, columns=["iho_mrgid", "iho_sea"])
    field = dict(zip(meta["fields"], fields))
    names, features = {}, []
    for wkb, mrgid, name in zip(geoms, field["iho_mrgid"], field["iho_sea"]):
        if wkb is None or not name:
            continue
        mrgid = int(mrgid)
        if names.setdefault(mrgid, name) != name:
            raise ValueError(f"MRGID {mrgid} is named both {names[mrgid]!r} and {name!r}")
        features.append((mrgid, wkb))
    # The biggest first, so the last one doesn't run alone; the parts in the features' order.
    order = sorted(range(len(features)), key=lambda i: -len(features[i][1]))
    made = dict(zip(order, pool.map(_valid_parts, [features[i] for i in order], chunksize=1)))
    return names, [p for i in range(len(features)) for p in made[i]]


def _land_path(path):
    """GDAL's path of the land polygons' shapefile, inside their zip."""
    if not path.endswith(".zip"):
        return path
    import zipfile
    with zipfile.ZipFile(path) as z:
        shp = [n for n in z.namelist() if n.endswith(".shp")]
    if len(shp) != 1:
        raise ValueError(f"{path}: {len(shp)} shapefiles")
    return f"/vsizip/{path}/{shp[0]}"


def build(opts):
    import multiprocessing
    import numpy as np
    import pyogrio
    import shapely
    global _LAND, _LAND_TREE, _SEA_TILES
    started = time.time()

    def log(message):
        print(f"[{time.time() - started:7.1f}s] {message}", flush=True)

    if opts.stats and os.path.realpath(opts.stats) == os.path.realpath(opts.output):
        raise ValueError(f"--stats {opts.stats} is the output, -o {opts.output}")
    bbox = tuple(float(v) for v in opts.bbox.split(",")) if opts.bbox else None
    ctx = multiprocessing.get_context("fork")
    tiles = {}
    with ctx.Pool(opts.workers) as pool:
        names, parts = _read_seas(opts.eez_iho, pool)
        log(f"{len(names)} sea areas in {len(parts)} parts")
        # Every part, --bbox or not: a tile's water goes by the sea areas in the tiles around it, across the
        # antimeridian too. The costliest first, so the last one doesn't run alone.
        order = sorted(parts, key=lambda p: -len(p[1]))
        for pieces in pool.imap_unordered(_split_into_tiles, order):
            for key, mrgid, wkb in pieces:
                tiles.setdefault(key, {}).setdefault(mrgid, []).append(wkb)
    _SEA_TILES = {k: {m: list(shapely.from_wkb(w)) for m, w in v.items()} for k, v in tiles.items()}
    log(f"sea areas cut into {sum(len(v) for v in _SEA_TILES.values())} pieces in {len(_SEA_TILES)} tiles")

    _, _, geoms, _ = pyogrio.raw.read(_land_path(opts.land), columns=[],
                                      bbox=(bbox[0] - 1, bbox[1] - 1, bbox[2] + 1, bbox[3] + 1) if bbox else None)
    _LAND = shapely.from_wkb(geoms)
    del geoms
    _LAND_TREE = shapely.STRtree(_LAND)
    log(f"{len(_LAND)} land polygons, {shapely.get_num_coordinates(_LAND).sum()} vertices")

    keys = set()
    for k in _SEA_TILES:
        keys.update(n for n, _ in _neighbours(k))
    if bbox:
        keys = {k for k in keys if bbox[0] <= k[0] + 0.5 <= bbox[2] and bbox[1] <= k[1] + 0.5 <= bbox[3]}
    # The tiles with the most land first, then the rest.
    keys = sorted(keys, key=lambda k: (-len(_LAND_TREE.query(shapely.box(k[0], k[1], k[0] + 1, k[1] + 1))), k))
    log(f"{len(keys)} tiles with a sea area within a tile")
    found, counts = [], {}
    with ctx.Pool(opts.workers) as pool:
        for i, (polygons, c) in enumerate(pool.imap_unordered(_tile_job, keys, chunksize=4)):
            found.extend(polygons)
            for k, v in c.items():
                counts[k] = counts.get(k, 0) + v
            if (i + 1) % 5000 == 0:
                log(f"{i + 1} of {len(keys)} tiles")
    by_object = {}
    for mrgid, (tx, ty), smallest, rings in found:
        by_object.setdefault(mrgid, []).append(((ty, tx), smallest, rings))
    objects = []
    for mrgid, polygons in by_object.items():
        polygons.sort(key=lambda p: (p[0], p[1]))
        objects.append((mrgid, kind_of(names[mrgid]), names[mrgid], [p[2] for p in polygons]))
    write_marine(opts.output, objects)
    vertices = int(sum(len(r) for _, _, _, rings in found for r in rings))
    summary = {
        "objects": len(objects),
        "oceans": sum(1 for o in objects if o[1] == KIND_OCEAN),
        "polygons": len(found),
        "rings": int(sum(len(rings) for _, _, _, rings in found)),
        "vertices": vertices,
        "bytes": os.path.getsize(opts.output),
        "tiles": counts.get("tiles", 0),
        "gap_bits": counts.get("gap_bits", 0),
        "gap_km2": round(counts.get("gap_km2", 0.0)),
        "sea_areas_without_water": sorted(set(names) - set(by_object)) if not bbox else [],
        "seconds": round(time.time() - started),
    }
    log(json.dumps(summary))
    if opts.stats:
        with open(opts.stats, "w") as f:
            json.dump(summary, f, indent=2)
            f.write("\n")
    return 0


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="command", required=True)
    b = sub.add_parser("build")
    b.add_argument("--eez-iho", required=True)
    b.add_argument("--land", required=True)
    b.add_argument("-o", "--output", required=True)
    b.add_argument("--stats", default="")
    b.add_argument("--workers", type=int, default=os.cpu_count())
    b.add_argument("--bbox", default="", help="only the tiles whose centres are in WEST,SOUTH,EAST,NORTH")
    s = sub.add_parser("stats")
    s.add_argument("path")
    opts = p.parse_args(argv)
    if opts.command == "stats":
        stats(opts.path)
        return 0
    return build(opts)


if __name__ == "__main__":
    sys.exit(main())
