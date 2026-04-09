#!/bin/sh
set -e

DATA_DIR="${DATA_DIR:-/data}"
INDEX_DIR="$DATA_DIR/index"
TIGRIS_BASE="https://geoshenanigans-reverse-geocoding.t3.tigrisfiles.io/geocoder/builds"

# Check if index data already exists
if [ -f "$INDEX_DIR/geo_cells.bin" ]; then
    echo "Index data found at $INDEX_DIR"
else
    echo "No index data found — downloading from Tigris..."
    mkdir -p "$INDEX_DIR"

    # Get latest build date (parse without jq — minimal image)
    LATEST_JSON=$(curl -sL "$TIGRIS_BASE/latest.json")
    echo "latest.json: $LATEST_JSON"
    LATEST=$(echo "$LATEST_JSON" | sed 's/.*"latest":"\([^"]*\)".*/\1/')
    if [ -z "$LATEST" ] || [ "$LATEST" = "$LATEST_JSON" ]; then
        echo "Error: could not parse latest.json"
        exit 1
    fi
    echo "Latest build: $LATEST"
    BASE="$TIGRIS_BASE/$LATEST"

    # Download full mode files
    echo "Downloading full mode index..."
    for f in geo_cells.bin street_entries.bin street_ways.bin street_nodes.bin \
             addr_entries.bin addr_points.bin interp_entries.bin interp_ways.bin \
             interp_nodes.bin admin_cells.bin admin_entries.bin strings.bin; do
        echo "  $f"
        curl -sL -o "$INDEX_DIR/$f" "$BASE/planet/full/$f" &
    done
    wait

    # Download admin polygons from quality/uncapped
    echo "Downloading admin polygons..."
    for f in admin_polygons.bin admin_vertices.bin; do
        echo "  $f"
        curl -sL -o "$INDEX_DIR/$f" "$BASE/planet/quality/uncapped/$f" &
    done
    wait

    # Download POI files (all tier)
    echo "Downloading POI data..."
    for f in poi_records.bin poi_vertices.bin poi_cells.bin poi_entries.bin poi_meta.json; do
        echo "  $f"
        curl -sL -o "$INDEX_DIR/$f" "$BASE/planet/poi/all/$f" &
    done
    wait

    echo "Download complete."

    # Verify we got actual data (not XML error pages)
    GEO_SIZE=$(stat -c%s "$INDEX_DIR/geo_cells.bin" 2>/dev/null || echo 0)
    if [ "$GEO_SIZE" -lt 1000 ]; then
        echo "Error: downloaded files appear corrupt (geo_cells.bin is ${GEO_SIZE} bytes)"
        echo "Contents:"
        head -c 200 "$INDEX_DIR/geo_cells.bin"
        exit 1
    fi
    echo "Verified: geo_cells.bin is $((GEO_SIZE / 1024 / 1024)) MiB"
fi

# Create test auth config if it doesn't exist
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

echo "Starting server..."
exec query-server "$INDEX_DIR" 0.0.0.0:3000
