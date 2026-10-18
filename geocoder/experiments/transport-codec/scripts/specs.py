"""Per-file layout (tt KIND), side records and column encodings.

Column order per kind:
  s:...  struct fields in order
  e      counts(u16), items(u32)
  v      tag, pad, bbox_min_lat, bbox_min_lng, lat16, lng16, lat32, lng32
"""
import json, os

ZSTD = "/ssd/shrink/zstd157/bin/zstd"
TT = "/ssd/transport/bin/tt"

ADMIN_POLY = "s:4,4,4,1,1,1,1,4,2,2"
KIND = {
    "street_nodes.bin": "s:4,4",
    "interp_nodes.bin": "s:4,4",
    "street_ways.bin": "s:4,2,2,4",
    "interp_ways.bin": "s:4,2,2,4,4,4,1,1,1,1",
    "geo_cells.bin": "s:8,4,4,4",
    "admin_cells.bin": "s:8,4",
    "place_cells.bin": "s:8,4",
    "poi_cells.bin": "s:8,4",
    "postcode_centroid_cells.bin": "s:8,4",
    "street_entries.bin": "e",
    "addr_entries.bin": "e",
    "interp_entries.bin": "e",
    "admin_entries.bin": "e",
    "place_entries.bin": "e",
    "poi_entries.bin": "e",
    "postcode_centroid_entries.bin": "e",
    "addr_points.bin": "s:4,4,4,4,4,4,4",
    "poi_records.bin": "s:4,4,4,4,4,1,1,1,1,4,4,4",
    "admin_polygons.bin": ADMIN_POLY,
    "postal_polygons.bin": ADMIN_POLY,
    "place_nodes.bin": "s:4,4,4,1,1,2,4",
    "postcode_centroids.bin": "s:4,4,4,2,2",
    "way_parents.bin": "s:4",
    "way_postcodes.bin": "s:4",
    "addr_postcodes.bin": "s:4",
    "interp_postcodes.bin": "s:4",
    "admin_parents.bin": "s:4",
    "addr_vertices.bin": "v",
    "poi_vertices.bin": "v",
    "admin_vertices.bin": "v",
    "postal_vertices.bin": "v",
}

# records file, stride, byte offset of the u32 vertex count
SIDE = {
    "addr_vertices.bin": ("addr_points.bin", 28, 24),
    "poi_vertices.bin": ("poi_records.bin", 36, 12),
    "admin_vertices.bin": ("admin_polygons.bin", 24, 4),
    "postal_vertices.bin": ("postal_polygons.bin", 24, 4),
}

COLNAMES = {
    "street_nodes.bin": ["lat", "lng"],
    "interp_nodes.bin": ["lat", "lng"],
    "street_ways.bin": ["node_offset", "node_count", "pad", "name_id"],
    "interp_ways.bin": ["node_offset", "node_count", "pad", "street_id", "start", "end", "interp", "pad4", "pad5", "pad6"],
    "geo_cells.bin": ["cell_id", "street_off", "addr_off", "interp_off"],
    "addr_points.bin": ["lat", "lng", "housenumber_id", "street_id", "parent_way_id", "vertex_offset", "vertex_count"],
    "poi_records.bin": ["lat", "lng", "vertex_offset", "vertex_count", "name_id", "category", "tier", "flags",
                        "importance", "parent_street_id", "parent_postcode_id", "parent_poly_id"],
    "admin_polygons.bin": ["vertex_offset", "vertex_count", "name_id", "admin_level", "place_type", "pad2", "pad3",
                           "area", "country_code", "pad4"],
    "place_nodes.bin": ["lat", "lng", "name_id", "place_type", "pad1", "pad2", "parent_poly_id"],
    "postcode_centroids.bin": ["lat", "lng", "postcode_id", "country_code", "pad"],
}
COLNAMES["postal_polygons.bin"] = COLNAMES["admin_polygons.bin"]
for f in ["admin_cells.bin", "place_cells.bin", "poi_cells.bin", "postcode_centroid_cells.bin"]:
    COLNAMES[f] = ["cell_id", "entry_off"]
for f in ["way_parents.bin", "way_postcodes.bin", "addr_postcodes.bin", "interp_postcodes.bin", "admin_parents.bin"]:
    COLNAMES[f] = ["value"]
for f, k in KIND.items():
    if k == "e":
        COLNAMES[f] = ["count", "items"]
    elif k == "v":
        COLNAMES[f] = ["tag", "pad", "bbox_min_lat", "bbox_min_lng", "lat16", "lng16", "lat32", "lng32"]

# The experiment as described: coordinates, sorted cell ids, way offsets and
# name ids delta+BSS; geo_cells entry offsets as steps from the previous valid
# offset (NO_DATA kept); string ids and street_entries items raw; every other
# column BSS. Files the description does not cover follow the same rules
# (string ids raw, coordinates/cell ids delta, record offsets steps, rest BSS).
THEIRS = {
    "street_nodes.bin": "DD",
    "interp_nodes.bin": "DD",
    "street_ways.bin": "DBBD",
    "interp_ways.bin": "DBBRBBBBBB",
    "geo_cells.bin": "DSSS",
    "admin_cells.bin": "DS",
    "place_cells.bin": "DS",
    "poi_cells.bin": "DS",
    "postcode_centroid_cells.bin": "DS",
    "street_entries.bin": "BR",
    "interp_entries.bin": "BR",
    "addr_entries.bin": "BB",
    "admin_entries.bin": "BB",
    "place_entries.bin": "BB",
    "poi_entries.bin": "BB",
    "postcode_centroid_entries.bin": "BB",
    "addr_points.bin": "DDRRBSB",
    "poi_records.bin": "DDSBRBBBBRRB",
    "admin_polygons.bin": "SBRBBBBBBB",
    "postal_polygons.bin": "SBRBBBBBBB",
    "place_nodes.bin": "DDRBBBB",
    "postcode_centroids.bin": "DDRBB",
    "way_parents.bin": "B",
    "admin_parents.bin": "B",
    "way_postcodes.bin": "R",
    "addr_postcodes.bin": "R",
    "interp_postcodes.bin": "R",
    "addr_vertices.bin": "BBDDDDDD",
    "poi_vertices.bin": "BBDDDDDD",
    "admin_vertices.bin": "BBDDDDDD",
    "postal_vertices.bin": "BBDDDDDD",
}

# The seven files the description reports on.
SEVEN = ["geo_cells.bin", "street_ways.bin", "street_nodes.bin", "addr_entries.bin", "addr_points.bin",
         "addr_vertices.bin", "street_entries.bin"]


def load_cfg(name):
    if name == "theirs":
        return dict(THEIRS)
    if name == "raw":
        return {}
    # a JSON file written by colsearch.py: {file: {"spec": "..."}}
    j = json.load(open(name))
    return {f: v["spec"] for f, v in j.items()}


def zstd_threads(path):
    # compress_index.py: one thread per 256 MiB, 1..16
    return max(1, min(16, os.path.getsize(path) // (256 << 20)))


def side_arg(src_dir, name):
    if name not in SIDE:
        return []
    rec, stride, off = SIDE[name]
    return ["--side", f"{os.path.join(src_dir, rec)}:{stride}:{off}"]
