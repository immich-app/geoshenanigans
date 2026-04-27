#!/bin/sh
set -e

DATA_DIR="${DATA_DIR:-/data}"
INDEX_DIR="$DATA_DIR/index"
TIGRIS_BASE="https://geoshenanigans-reverse-geocoding.t3.tigrisfiles.io/geocoder/builds"
# Quality variant for admin polygons.  q2.5 is the default — most
# aggressive simplification (~37 m boundary precision at L8), smallest
# disk footprint (~280 MB saved vs uncapped on planet).  Override
# with QUALITY=uncapped|q1|q1.5|... for finer precision.
QUALITY="${QUALITY:-q2.5}"
# POI tier: major (8.6k records, 30 MB), notable (45k, 100 MB), or
# all (340k, 670 MB).  All by default.
POI_TIER="${POI_TIER:-all}"

# Files in S3 are zstd-compressed with .zst suffix.  Helper fetches
# either the .zst (preferred) or plain version, decompresses if needed.
fetch() {
    src_path="$1"; dest="$2"
    if curl -fsSL "$src_path.zst" -o "$dest.zst" 2>/dev/null; then
        zstd -dq --rm "$dest.zst"
    elif curl -fsSL "$src_path" -o "$dest" 2>/dev/null; then
        :
    else
        echo "  miss: $src_path"
        return 1
    fi
}

if [ -f "$INDEX_DIR/geo_cells.bin" ]; then
    echo "Index data found at $INDEX_DIR (delete to force re-download)"
else
    echo "No index data found — downloading from Tigris (quality=$QUALITY, poi=$POI_TIER)..."
    mkdir -p "$INDEX_DIR"
    apt-get update -qq && apt-get install -y -qq zstd >/dev/null 2>&1 || true

    LATEST_JSON=$(curl -sL "$TIGRIS_BASE/latest.json")
    LATEST=$(echo "$LATEST_JSON" | sed 's/.*"latest":"\([^"]*\)".*/\1/')
    if [ -z "$LATEST" ] || [ "$LATEST" = "$LATEST_JSON" ]; then
        echo "Error: could not parse latest.json"; exit 1
    fi
    echo "Latest build: $LATEST"
    BASE="$TIGRIS_BASE/$LATEST"

    # Full-mode files (geo + addresses + interpolation + cells/entries
    # + parents + postcodes).  v15 uses tiered string files instead of
    # monolithic strings.bin, plus extra parent / postcode / postal files.
    full_files="geo_cells.bin
                street_entries.bin street_ways.bin street_nodes.bin
                addr_entries.bin addr_points.bin addr_vertices.bin addr_postcodes.bin
                interp_entries.bin interp_ways.bin interp_nodes.bin
                admin_cells.bin admin_entries.bin admin_parents.bin
                place_cells.bin place_entries.bin place_nodes.bin
                postcode_centroids.bin postcode_centroid_cells.bin postcode_centroid_entries.bin
                postal_polygons.bin postal_vertices.bin
                way_parents.bin way_postcodes.bin
                strings_core.bin strings_street.bin strings_addr.bin
                strings_postcode.bin strings_poi.bin"
    echo "Downloading full mode files..."
    for f in $full_files; do
        echo "  $f"
        fetch "$BASE/planet/full/$f" "$INDEX_DIR/$f" &
    done
    wait

    # Layout JSON and POI meta JSON (small, plain text)
    curl -sL -o "$INDEX_DIR/strings_layout.json" "$BASE/planet/full/strings_layout.json"
    curl -sL -o "$INDEX_DIR/poi_meta.json" "$BASE/planet/poi/$POI_TIER/poi_meta.json"

    # Admin polygons from the selected quality variant.
    echo "Downloading admin polygons (quality=$QUALITY)..."
    for f in admin_polygons.bin admin_vertices.bin; do
        echo "  $f"
        fetch "$BASE/planet/quality/$QUALITY/$f" "$INDEX_DIR/$f" &
    done
    wait

    # POI tier
    echo "Downloading POI data (tier=$POI_TIER)..."
    for f in poi_records.bin poi_vertices.bin poi_cells.bin poi_entries.bin; do
        echo "  $f"
        fetch "$BASE/planet/poi/$POI_TIER/$f" "$INDEX_DIR/$f" &
    done
    wait

    echo "Download complete."
    GEO_SIZE=$(stat -c%s "$INDEX_DIR/geo_cells.bin" 2>/dev/null || echo 0)
    if [ "$GEO_SIZE" -lt 1000 ]; then
        echo "Error: geo_cells.bin is only ${GEO_SIZE} bytes — download likely failed"
        exit 1
    fi
    echo "Verified: geo_cells.bin is $((GEO_SIZE / 1024 / 1024)) MiB"
    echo "Total index size: $(du -sh $INDEX_DIR | cut -f1)"
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

echo "Starting server (quality=$QUALITY, poi=$POI_TIER)..."
exec query-server "$INDEX_DIR" 0.0.0.0:3000
