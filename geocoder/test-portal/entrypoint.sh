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
# The build version of the files this server reads, the build.version of
# configurations.template.json (builder/tools/test_build_search_indexes.py
# holds it to that).
BUILD_VERSION=24

# The planet files this server downloads, one "<dir> <file>" a line: those
# of the template's selection (mode full or admin-minimal, $QUALITY,
# $POI_TIER), each from the dir a client takes it from
# (builder/tools/test_build_search_indexes.py checks them against it).
selection_files() {
    if [ "$MINIMAL" = "1" ]; then
        # Everything comes from the admin-minimal/ subdir (filtered: admin
        # polygons of levels 2-8, place types in {city,town,village,hamlet}):
        # its own polygons on the minimal arc set, cell index, place nodes,
        # core strings and layout, and the postcode grid.
        for f in admin_cells.bin admin_entries.bin \
                 admin_polygons.bin admin_rings.bin arc_index.bin arc_vertices.bin \
                 place_cells.bin place_hidden.bin place_nodes.bin strings_core.bin strings_layout.json \
                 postcode_grid.bin; do
            echo "admin-minimal $f"
        done
        return
    fi
    for f in geo_cells.bin geo_entries.bin \
             street_ways.bin street_nodes.bin \
             addr_points.bin addr_vertices.bin addr_index.bin addr_postcodes.bin \
             interp_ways.bin interp_nodes.bin interp_postcodes.bin \
             admin_cells.bin admin_entries.bin admin_parents.bin \
             place_cells.bin place_hidden.bin place_nodes.bin \
             postcode_centroids.bin postcode_centroid_cells.bin postcode_grid.bin \
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

# The transport unit that holds a file in its dir, the only published form
# of it (configurations.template.json's transport block;
# builder/tools/test_build_search_indexes.py checks them against it), or
# nothing for a file published as its own .zst.
unit_of() {
    case "$1" in
        place_cells.bin|place_nodes.bin|place_hidden.bin) echo places.tt.zst ;;
        poi_records.bin|poi_vertices.bin) echo pois.tt.zst ;;
        arc_index.bin|arc_vertices.bin) echo arcs.tt.zst ;;
        admin_polygons.bin|street_ways.bin|interp_ways.bin|addr_index.bin) echo "${1%.bin}.tt.zst" ;;
    esac
}

if [ "${1:-}" = "--list-files" ]; then
    selection_files
    exit 0
fi
if [ "${1:-}" = "--unit-of" ]; then
    unit_of "$2"
    exit 0
fi
if [ "${1:-}" = "--build-version" ]; then
    echo "$BUILD_VERSION"
    exit 0
fi

# Files in S3 are zstd-compressed with .zst suffix, but those a transport
# unit holds (fetch_unit).  Helper fetches either the .zst (preferred) or
# plain version to <dest>, decompresses if needed.
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

# A transport unit, into <dir>: fetched whole to <tmp>, then decoded into its
# files with geocoder-transport (all of them or, failing, none). A unit the
# build lacks (404) is a miss, as a file is; any other failure fails the fetch
# and leaves none of its files.
fetch_unit() {
    src_path="$1"; tmp="$2"; into="$3"; code=
    if code=$(curl -fsSL -w '%{http_code}' "$src_path" -o "$tmp" 2>/dev/null) \
            && geocoder-transport decode "$tmp" "$into" > /dev/null; then
        rm -f "$tmp"
        return
    fi
    rm -f "$tmp"
    if [ "$code" = 404 ]; then
        echo "  miss: $src_path"
        return
    fi
    echo "  failed: $src_path"
    return 1
}

# Files a region may lack (interp_postcodes.bin outside TIGER regions, the
# postcode centroids and grid): the template's optional files of mode full
# and admin-minimal.
optional_file() {
    case "$1" in
        interp_postcodes.bin|postcode_centroids.bin|postcode_centroid_cells.bin|postcode_grid.bin)
            return 0 ;;
    esac
    return 1
}

# A finished download leaves the "<dir> <file>" lines of its selection here.
MARKER="$INDEX_DIR/.selection"
# A download goes here, beside the index, whose files it replaces once it is
# whole.
STAGING="$DATA_DIR/.index-download"

# The selection's files missing from <dir> ($INDEX_DIR by default) that a
# region can't lack, one a line.
missing_files() {
    in="${1:-$INDEX_DIR}"
    selection_files | while read -r dir f; do
        optional_file "$f" || [ -f "$in/$f" ] || echo "$dir/$f"
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
    # cells and strings may be another build's. What is there stays until
    # the download is whole, and only the accounts stay after it.
    echo "No finished download of this selection at $INDEX_DIR: downloading it (quality=$QUALITY, poi=$POI_TIER, minimal=$MINIMAL)..."

    LATEST_JSON=$(curl -sL "$TIGRIS_BASE/latest.json")
    # latest.json is multi-line pretty-printed JSON; flatten with tr
    # before extracting "latest":"..." and "build_version":N with sed.
    FLAT=$(printf '%s' "$LATEST_JSON" | tr -d '\n\r')
    LATEST=$(printf '%s' "$FLAT" | sed 's/.*"latest"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/')
    if [ -z "$LATEST" ] || [ "$LATEST" = "$FLAT" ]; then
        echo "Error: could not parse latest.json"; echo "$LATEST_JSON"; exit 1
    fi
    # A build of another version holds files this server can't read: the
    # index stays as it is, for a server of that version, or for this one
    # once a build of its version is out.
    LATEST_VERSION=$(printf '%s' "$FLAT" | sed -n 's/.*"build_version"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p')
    if [ "$LATEST_VERSION" != "$BUILD_VERSION" ]; then
        echo "Error: the latest build ($LATEST) is of build_version ${LATEST_VERSION:-none}; this server reads build_version $BUILD_VERSION."
        echo "The index at $INDEX_DIR is left as it is."
        exit 1
    fi
    echo "Latest build: $LATEST"
    BASE="$TIGRIS_BASE/$LATEST"
    rm -rf "$STAGING"
    mkdir -p "$STAGING"

    # An optional file a region lacks (interp_postcodes.bin outside TIGER
    # regions, the postcode centroids) is a miss, not an error. A fetch that
    # failed fails the start before the download replaces anything. The
    # files a transport unit holds come through it, once.
    if ! selection_files | {
        pids=; units=
        while read -r dir f; do
            unit=$(unit_of "$f")
            if [ -z "$unit" ]; then
                echo "  $dir/$f"
                fetch "$BASE/planet/$dir/$f" "$STAGING/$f" &
            else
                case " $units " in *" $dir/$unit "*) continue ;; esac
                units="$units $dir/$unit"
                echo "  $dir/$unit"
                fetch_unit "$BASE/planet/$dir/$unit" "$STAGING/.$unit" "$STAGING" &
            fi
            pids="$pids $!"
        done
        ok=1
        for pid in $pids; do wait "$pid" || ok=; done
        [ -n "$ok" ]
    }; then
        rm -rf "$STAGING"
        echo "Error: a file failed to download (named above); the next start downloads afresh"
        exit 1
    fi

    echo "Download complete."
    MISSING=$(missing_files "$STAGING")
    if [ -n "$MISSING" ]; then
        rm -rf "$STAGING"
        echo "Error: the download lacks files the server needs:"
        echo "$MISSING"
        exit 1
    fi
    SENTINEL_SIZE=$(stat -c%s "$STAGING/$SENTINEL" 2>/dev/null || echo 0)
    if [ "$SENTINEL_SIZE" -lt 1000 ]; then
        rm -rf "$STAGING"
        echo "Error: $SENTINEL is only ${SENTINEL_SIZE} bytes — download likely failed"
        exit 1
    fi
    echo "Verified: $SENTINEL is $((SENTINEL_SIZE / 1024 / 1024)) MiB"

    # The download replaces the index's files but the accounts. The marker
    # goes first and comes back last, so a start cut short in between
    # downloads afresh.
    mkdir -p "$INDEX_DIR"
    rm -f "$MARKER"
    find "$INDEX_DIR" -mindepth 1 -maxdepth 1 ! -name geocoder.json -exec rm -rf {} +
    find "$STAGING" -mindepth 1 -maxdepth 1 -exec mv {} "$INDEX_DIR"/ \;
    rmdir "$STAGING"
    selection_files > "$MARKER"
    echo "Total index size: $(du -sh "$INDEX_DIR" | cut -f1)"
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
