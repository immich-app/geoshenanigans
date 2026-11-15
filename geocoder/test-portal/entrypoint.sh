#!/bin/sh
set -e

DATA_DIR="${DATA_DIR:-/data}"
INDEX_DIR="$DATA_DIR/index"
TIGRIS_BASE="https://geoshenanigans-reverse-geocoding.t3.tigrisfiles.io/geocoder/builds"
# Boundary tier: simplified (the default: ~37 m boundaries, shared by
# neighbours) or exact (every OSM point, the largest download).
QUALITY="${QUALITY:-simplified}"
# POI tier: major (8.6k records, 30 MB), notable (45k, 100 MB), or
# all (340k, 670 MB).  All by default.
POI_TIER="${POI_TIER:-all}"
# Minimal mode: admin-only with simplified boundaries + place nodes
# (city/town/village/hamlet/...).  Skips streets, addresses,
# interpolation, postcodes, postcode areas, POI data, and all string
# tiers except strings_core (which covers admin + place names).
# Smallest possible deployable that resolves admin hierarchy + GeoNames
# cities500-equivalent detail.
MINIMAL="${MINIMAL:-0}"

# The planet files this server downloads, one "<dir> <file>" a line: those
# of the template's selection (mode full or admin-minimal, $QUALITY,
# $POI_TIER), each from the dir a client takes it from
# (builder/tools/test_build_search_indexes.py checks them against it).
selection_files() {
    if [ "$MINIMAL" = "1" ]; then
        # Everything comes from the admin-minimal/ subdir (filtered: admin
        # polygons of levels 2-8, place types in {city,town,village,hamlet}):
        # its own polygons on the minimal arc set, cell index, place_nodes,
        # core strings and layout. The server derives the place index from
        # place_nodes.bin at its first load.
        for f in admin_cells.bin admin_entries.bin \
                 admin_polygons.bin admin_rings.bin arc_index.bin arc_vertices.bin \
                 place_nodes.bin strings_core.bin strings_layout.json; do
            echo "admin-minimal $f"
        done
        return
    fi
    for f in geo_cells.bin geo_entries.bin \
             street_ways.bin street_nodes.bin \
             addr_points.bin addr_vertices.bin addr_postcodes.bin \
             interp_ways.bin interp_nodes.bin interp_postcodes.bin \
             admin_cells.bin admin_entries.bin admin_parents.bin \
             place_nodes.bin \
             postcode_centroids.bin postcode_centroid_cells.bin postcode_centroid_entries.bin \
             way_parents.bin way_postcodes.bin \
             strings_core.bin strings_street.bin strings_addr.bin strings_postcode.bin; do
        echo "full $f"
    done
    # The boundaries of the chosen tier: the admin polygons (postcode areas
    # among them) and the arcs their rings share.
    for f in admin_polygons.bin admin_rings.bin arc_index.bin arc_vertices.bin; do
        echo "quality/$QUALITY $f"
    done
    # The POI tier's records and strings; its string layout is the one every
    # dir sharing the strings holds.
    for f in poi_records.bin poi_vertices.bin poi_cells.bin poi_entries.bin strings_poi.bin \
             strings_layout.json poi_meta.json; do
        echo "poi/$POI_TIER $f"
    done
}

if [ "${1:-}" = "--list-files" ]; then
    selection_files
    exit 0
fi

# Files in S3 are zstd-compressed with .zst suffix.  Helper fetches
# either the .zst (preferred) or plain version, decompresses if needed.
# A file the build lacks (404 for both) is a miss; any other failure, a
# transfer that broke off among them, fails the fetch and leaves nothing.
fetch() {
    src_path="$1"; dest="$2"; zst=; plain=
    if zst=$(curl -fsSL -w '%{http_code}' "$src_path.zst" -o "$dest.zst" 2>/dev/null); then
        zstd -dq --rm "$dest.zst" && return
    elif plain=$(curl -fsSL -w '%{http_code}' "$src_path" -o "$dest" 2>/dev/null); then
        rm -f "$dest.zst"
        return
    elif [ "$zst" = 404 ] && [ "$plain" = 404 ]; then
        echo "  miss: $src_path"
        return
    fi
    echo "  failed: $src_path"
    rm -f "$dest" "$dest.zst"
    return 1
}

# Files a region may lack (interp_postcodes.bin outside TIGER regions, the
# postcode centroids): the template's optional files of mode full.
optional_file() {
    case "$1" in
        interp_postcodes.bin|postcode_centroids.bin|postcode_centroid_cells.bin|postcode_centroid_entries.bin)
            return 0 ;;
    esac
    return 1
}

# A finished download leaves the "<dir> <file>" lines of its selection here.
MARKER="$INDEX_DIR/.selection"

# The selection's files missing from $INDEX_DIR that a region can't lack,
# one a line.
missing_files() {
    selection_files | while read -r dir f; do
        optional_file "$f" || [ -f "$INDEX_DIR/$f" ] || echo "$dir/$f"
    done
}

# Whether $INDEX_DIR holds a finished download of this selection: its marker
# names this selection's files and none it needs is missing. An interrupted
# download, or a dataset of another selection or build, fails it.
dataset_complete() {
    [ -f "$MARKER" ] && [ "$(cat "$MARKER")" = "$(selection_files)" ] && [ -z "$(missing_files)" ]
}

# Sentinel file differs by mode — minimal doesn't download geo_cells.
SENTINEL="geo_cells.bin"
if [ "$MINIMAL" = "1" ]; then SENTINEL="admin_cells.bin"; fi

if dataset_complete; then
    echo "Index data found at $INDEX_DIR (delete to force re-download)"
else
    # Download the whole selection afresh, never into what is there: its
    # cells and strings may be another build's. Only the accounts stay.
    mkdir -p "$INDEX_DIR"
    if find "$INDEX_DIR" -mindepth 1 -maxdepth 1 ! -name geocoder.json | grep -q .; then
        echo "Index data at $INDEX_DIR is not a finished download of this selection — removing it"
        rm -f "$MARKER"
        find "$INDEX_DIR" -mindepth 1 -maxdepth 1 ! -name geocoder.json -exec rm -rf {} +
    fi
    echo "Downloading index data from Tigris (quality=$QUALITY, poi=$POI_TIER, minimal=$MINIMAL)..."

    LATEST_JSON=$(curl -sL "$TIGRIS_BASE/latest.json")
    # latest.json is multi-line pretty-printed JSON; flatten with tr
    # before extracting "latest":"..." with sed.
    LATEST=$(printf '%s' "$LATEST_JSON" | tr -d '\n\r' | sed 's/.*"latest"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/')
    if [ -z "$LATEST" ] || [ "$LATEST" = "$LATEST_JSON" ]; then
        echo "Error: could not parse latest.json"; echo "$LATEST_JSON"; exit 1
    fi
    echo "Latest build: $LATEST"
    BASE="$TIGRIS_BASE/$LATEST"

    # An optional file a region lacks (interp_postcodes.bin outside TIGER
    # regions, the postcode centroids) is a miss, not an error. A fetch that
    # failed fails the start before the marker vouches for the download.
    if ! selection_files | {
        pids=
        while read -r dir f; do
            echo "  $dir/$f"
            fetch "$BASE/planet/$dir/$f" "$INDEX_DIR/$f" &
            pids="$pids $!"
        done
        ok=1
        for pid in $pids; do wait "$pid" || ok=; done
        [ -n "$ok" ]
    }; then
        echo "Error: a file failed to download (named above); the next start downloads afresh"
        exit 1
    fi

    echo "Download complete."
    MISSING=$(missing_files)
    if [ -n "$MISSING" ]; then
        echo "Error: the download lacks files the server needs:"
        echo "$MISSING"
        exit 1
    fi
    SENTINEL_SIZE=$(stat -c%s "$INDEX_DIR/$SENTINEL" 2>/dev/null || echo 0)
    if [ "$SENTINEL_SIZE" -lt 1000 ]; then
        echo "Error: $SENTINEL is only ${SENTINEL_SIZE} bytes — download likely failed"
        exit 1
    fi
    echo "Verified: $SENTINEL is $((SENTINEL_SIZE / 1024 / 1024)) MiB"
    echo "Total index size: $(du -sh $INDEX_DIR | cut -f1)"
    selection_files > "$MARKER"
fi

# Test auth config
if [ ! -f "$INDEX_DIR/geocoder.json" ]; then
    echo "Creating test auth config..."
    cat > "$INDEX_DIR/geocoder.json" << 'AUTHEOF'
{
  "users": {
    "test": {
      "password_hash": "",
      "admin": true,
      "rate_per_second": 100,
      "rate_per_day": 1000000,
      "rate_by_ip": false
    }
  },
  "tokens": {
    "test": "test"
  }
}
AUTHEOF
fi

echo "Starting server (quality=$QUALITY, poi=$POI_TIER, minimal=$MINIMAL)..."
exec query-server "$INDEX_DIR" 0.0.0.0:3000
