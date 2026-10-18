
### max

| file | today | four changes | best lossless on-disk | best download-only | raw on disk today -> best (MB) | random access |
|---|---:|---:|---:|---:|---:|---|
| street_nodes + street_ways | 2,424.7 | 2,424.6 | 1,508.6 | 1,411.4 | 5,691 -> 1,966 | per way (group table + <=15 header hops) |
| interp_nodes + interp_ways | 824.5 | 824.5 | 576.9 | 535.7 | 2,620 -> 1,081 | per way |
| way_parents | 75.2 | 75.2 | 7.6 | 7.6 | 222 -> 167 | per way (u24) |
| way_postcodes | 17.0 | 16.9 | 0.9 | 0.9 | 222 -> 111 | per way (u16 + table) |
| interp_postcodes | 28.9 | 28.9 | 2.0 | 2.0 | 136 -> 68 | per way (u16 + table) |
| geo index: geo_cells + street/interp/addr entries | 2,328.8 | 2,328.8 | 517.8 | 480.1 | 12,321 -> 1,246 | per cell (block of 128 rows) |
| addr_points | 1,528.5 | 1,522.6 | 874.9 | 755.3 | 5,033 -> 4,313 | per record (byte planes) |
| addr_vertices | 1,731.7 | 1,731.7 | 1,014.9 | 870.2 | 3,690 -> 1,932 | per outline |
| addr_postcodes | 72.9 | 72.8 | 16.6 | 16.6 | 719 -> 719 | per record |
| admin_vertices | 1,330.7 | 725.8 | 725.8 | 684.7 | 1,625 -> 1,625 | per polygon (same block) |
| admin_polygons | 11.7 | 11.8 | 11.8 | 11.8 | 26 -> 26 | per record |
| admin_cells + admin_entries | 15.0 | 10.2 | 7.3 | 7.3 | 84 -> 33 | per cell (bitmap + rank) |
| admin_parents | 2.1 | 2.1 | 2.1 | 2.1 | 4 -> 4 | unchanged |
| postal_vertices + postal_polygons | 83.3 | 0.0 | 0.0 | 0.0 | 98 -> 0 | dropped (never read) |
| place_nodes | 44.4 | 30.5 | 30.5 | 30.5 | 82 -> 82 | per record |
| place_cells + place_entries | 13.1 | 0.0 | 0.0 | 0.0 | 24 -> 0 | rebuilt at load (+0.5-0.8 s) |
| strings_core | 12.8 | 11.2 | 11.2 | 11.2 | 49 -> 44 | by offset |
| strings_street | 46.9 | 46.9 | 45.8 | 45.8 | 242 -> 234 | by offset |
| strings_addr | 3.5 | 3.5 | 3.5 | 2.3 | 16 -> 16 | by offset |
| strings_postcode | 3.0 | 3.0 | 3.0 | 3.0 | 23 -> 23 |  |
| strings_poi | 82.5 | 82.5 | 82.5 | 82.5 | 341 -> 341 |  |
| poi_records | 387.6 | 387.5 | 315.7 | 267.1 | 881 -> 783 | per record (32 B) |
| poi_vertices | 523.8 | 523.8 | 433.1 | 395.0 | 717 -> 682 | per polygon |
| poi_cells + poi_entries | 74.8 | 74.8 | 72.3 | 72.3 | 118 -> 107 | per cell (bitmap + rank) |
| postcode_centroids + cells + entries | 22.2 | 22.2 | 22.2 | 22.2 | 58 -> 58 | unchanged |
| poi_meta.json + strings_layout.json | 0.0 | 0.0 | 0.0 | 0.0 | 0 -> 0 | unchanged |
| **total** | **11,689.5** | **10,961.9** | **6,286.7** | **5,717.5** | 35,043 -> 15,662 | |

### withaddr

| file | today | four changes | best lossless on-disk | best download-only | raw on disk today -> best (MB) | random access |
|---|---:|---:|---:|---:|---:|---|
| street_nodes + street_ways | 2,424.7 | 2,424.6 | 1,508.6 | 1,411.4 | 5,691 -> 1,966 | per way (group table + <=15 header hops) |
| interp_nodes + interp_ways | 824.5 | 824.5 | 576.9 | 535.7 | 2,620 -> 1,081 | per way |
| way_parents | 75.2 | 75.2 | 7.6 | 7.6 | 222 -> 167 | per way (u24) |
| way_postcodes | 17.0 | 16.9 | 0.9 | 0.9 | 222 -> 111 | per way (u16 + table) |
| interp_postcodes | 28.9 | 28.9 | 2.0 | 2.0 | 136 -> 68 | per way (u16 + table) |
| geo index: geo_cells + street/interp/addr entries | 2,328.8 | 2,328.8 | 517.8 | 480.1 | 12,321 -> 1,246 | per cell (block of 128 rows) |
| addr_points | 1,528.5 | 1,522.6 | 874.9 | 755.3 | 5,033 -> 4,313 | per record (byte planes) |
| addr_vertices | 1,731.7 | 1,731.7 | 1,014.9 | 870.2 | 3,690 -> 1,932 | per outline |
| addr_postcodes | 72.9 | 72.8 | 16.6 | 16.6 | 719 -> 719 | per record |
| admin_vertices | 212.6 | 166.4 | 166.4 | 154.4 | 240 -> 240 | per polygon (same block) |
| admin_polygons | 10.5 | 10.5 | 10.5 | 10.5 | 26 -> 26 | per record |
| admin_cells + admin_entries | 15.0 | 10.2 | 7.3 | 7.3 | 84 -> 33 | per cell (bitmap + rank) |
| admin_parents | 2.1 | 2.1 | 2.1 | 2.1 | 4 -> 4 | unchanged |
| postal_vertices + postal_polygons | 13.1 | 0.0 | 0.0 | 0.0 | 15 -> 0 | dropped (never read) |
| place_nodes | 44.4 | 30.5 | 30.5 | 30.5 | 82 -> 82 | per record |
| place_cells + place_entries | 13.1 | 0.0 | 0.0 | 0.0 | 24 -> 0 | rebuilt at load (+0.5-0.8 s) |
| strings_core | 12.8 | 11.2 | 11.2 | 11.2 | 49 -> 44 | by offset |
| strings_street | 46.9 | 46.9 | 45.8 | 45.8 | 242 -> 234 | by offset |
| strings_addr | 3.5 | 3.5 | 3.5 | 2.3 | 16 -> 16 | by offset |
| strings_postcode | 3.0 | 3.0 | 3.0 | 3.0 | 23 -> 23 |  |
| strings_poi | 82.5 | 82.5 | 82.5 | 82.5 | 341 -> 341 |  |
| poi_records | 387.6 | 387.5 | 315.7 | 267.1 | 881 -> 783 | per record (32 B) |
| poi_vertices | 523.8 | 523.8 | 433.1 | 395.0 | 717 -> 682 | per polygon |
| poi_cells + poi_entries | 74.8 | 74.8 | 72.3 | 72.3 | 118 -> 107 | per cell (bitmap + rank) |
| postcode_centroids + cells + entries | 22.2 | 22.2 | 22.2 | 22.2 | 58 -> 58 | unchanged |
| poi_meta.json + strings_layout.json | 0.0 | 0.0 | 0.0 | 0.0 | 0 -> 0 | unchanged |
| **total** | **10,500.0** | **10,401.1** | **5,726.0** | **5,185.8** | 33,575 -> 14,276 | |
| **+ 11 m grid on q2.5 outlines (LOSSY)** | | **10,388.9** | **5,672.3** | **5,144.1** | | admin_vertices 113.0 (B + divisor), admin_polygons 10.3 |
