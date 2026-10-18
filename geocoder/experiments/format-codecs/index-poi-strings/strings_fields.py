"""String-offset fields of every file in the maximum preset: file -> (dtype, fields, read by the reader at query time?)."""
import numpy as np

U4 = "<u4"
FIELDS = {  # file: (dtype, [string-offset fields], read by the reader at query time?)
    "admin_polygons.bin": (np.dtype([("vo", U4), ("vc", U4), ("name_id", U4), ("al", "u1"), ("pto", "u1"), ("p2", "u1"),
                                     ("p3", "u1"), ("area", "<f4"), ("cc", "<u2"), ("p4", "<u2")]), ["name_id"], True),
    "postal_polygons.bin": (np.dtype([("vo", U4), ("vc", U4), ("name_id", U4), ("al", "u1"), ("pto", "u1"), ("p2", "u1"),
                                      ("p3", "u1"), ("area", "<f4"), ("cc", "<u2"), ("p4", "<u2")]), ["name_id"], False),
    "place_nodes.bin": (np.dtype([("lat", "<f4"), ("lng", "<f4"), ("name_id", U4), ("t", "u1"), ("p1", "u1"), ("p2", "<u2"),
                                  ("pp", U4)]), ["name_id"], True),
    "street_ways.bin": (np.dtype([("no", U4), ("nc", "<u2"), ("p", "<u2"), ("name_id", U4)]), ["name_id"], True),
    "addr_points.bin": (np.dtype([("lat", "<f4"), ("lng", "<f4"), ("housenumber_id", U4), ("street_id", U4),
                                  ("pw", U4), ("vo", U4), ("vc", U4)]), ["housenumber_id", "street_id"], True),
    "interp_ways.bin": (np.dtype([("no", U4), ("nc", "<u2"), ("p", "<u2"), ("street_id", U4), ("s", U4), ("e", U4),
                                  ("i", "u1"), ("p3", "V3")]), ["street_id"], True),
    "poi_records.bin": (np.dtype([("lat", "<f4"), ("lng", "<f4"), ("vo", U4), ("vc", U4), ("name_id", U4), ("c", "u1"),
                                  ("t", "u1"), ("f", "u1"), ("imp", "u1"), ("parent_street_id", U4),
                                  ("parent_postcode_id", U4), ("pp", U4)]), ["name_id", "parent_street_id", "parent_postcode_id"], True),
    "postcode_centroids.bin": (np.dtype([("lat", "<f4"), ("lng", "<f4"), ("postcode_id", U4), ("cc", "<u2"), ("p", "<u2")]),
                               ["postcode_id"], True),
    "addr_postcodes.bin": (np.dtype([("v", U4)]), ["v"], True),
    "way_postcodes.bin": (np.dtype([("v", U4)]), ["v"], True),
    "interp_postcodes.bin": (np.dtype([("v", U4)]), ["v"], True),
}
