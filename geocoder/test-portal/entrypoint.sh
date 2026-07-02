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
# Minimal mode: admin-only at q2.5 + place nodes (city/town/village/
# hamlet/...).  Skips streets, addresses, interpolation, postcodes,
# postal polygons, POI data, and all string tiers except strings_core
# (which covers admin + place names).  Smallest possible deployable
# that resolves admin hierarchy + GeoNames cities500-equivalent detail.
MINIMAL="${MINIMAL:-0}"

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

# Sentinel file differs by mode — minimal doesn't download geo_cells.
SENTINEL="geo_cells.bin"
if [ "$MINIMAL" = "1" ]; then SENTINEL="admin_cells.bin"; fi

if [ -f "$INDEX_DIR/$SENTINEL" ]; then
    echo "Index data found at $INDEX_DIR (delete to force re-download)"
else
    echo "No index data found — downloading from Tigris (quality=$QUALITY, poi=$POI_TIER, minimal=$MINIMAL)..."
    mkdir -p "$INDEX_DIR"
    apt-get update -qq && apt-get install -y -qq zstd >/dev/null 2>&1 || true

    LATEST_JSON=$(curl -sL "$TIGRIS_BASE/latest.json")
    # latest.json is multi-line pretty-printed JSON; flatten with tr
    # before extracting "latest":"..." with sed.
    LATEST=$(printf '%s' "$LATEST_JSON" | tr -d '\n\r' | sed 's/.*"latest"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/')
    if [ -z "$LATEST" ] || [ "$LATEST" = "$LATEST_JSON" ]; then
        echo "Error: could not parse latest.json"; echo "$LATEST_JSON"; exit 1
    fi
    echo "Latest build: $LATEST"
    BASE="$TIGRIS_BASE/$LATEST"

    if [ "$MINIMAL" = "1" ]; then
        # Truly minimal — admin-only at q2.5 plus place nodes for
        # cities/towns/villages/hamlets.  No streets, no addresses,
        # no interpolation, no postcodes, no postal polygons, no POIs.
        # strings_core.bin covers admin + place names; no other tier
        # is needed.  Equivalent to GeoNames cities500-level detail
        # with admin boundary lookup.
        #
        # Cell index + place_nodes come from the admin-minimal/
        # subdir (filtered: admin polygon IDs in [L2..L8], place
        # types in {city,town,village,hamlet}). strings_core.bin
        # is shared, fetched from full/ — see the strings_core curl
        # below the main loop.
        # admin-minimal owns its own polygon + vertex files (re-simplified
        # at q2.5 over just the L2-L8 subset) plus its own cell index +
        # filtered place_nodes. strings_core.bin is shared from full/.
        admin_minimal_files="admin_cells.bin admin_entries.bin
                             admin_polygons.bin admin_vertices.bin
                             place_cells.bin place_entries.bin place_nodes.bin"
        echo "Downloading admin-minimal mode files..."
        for f in $admin_minimal_files; do
            echo "  $f"
            fetch "$BASE/planet/admin-minimal/$f" "$INDEX_DIR/$f" &
        done
        wait
        # Skip the per-mode loop below — admin-minimal has its own dir.
        full_files=""
    else
        # Full set: everything except postal_* (lives under each
        # quality variant in v15) and strings_poi (lives under each
        # POI tier in v15).  Both are fetched separately below.
        full_files="geo_cells.bin
                    street_entries.bin street_ways.bin street_nodes.bin
                    addr_entries.bin addr_points.bin addr_vertices.bin addr_postcodes.bin
                    interp_entries.bin interp_ways.bin interp_nodes.bin
                    admin_cells.bin admin_entries.bin admin_parents.bin
                    place_cells.bin place_entries.bin place_nodes.bin
                    postcode_centroids.bin postcode_centroid_cells.bin postcode_centroid_entries.bin
                    way_parents.bin way_postcodes.bin
                    strings_core.bin strings_street.bin strings_addr.bin
                    strings_postcode.bin"
    fi
    if [ -n "$full_files" ]; then
        echo "Downloading mode files..."
        for f in $full_files; do
            echo "  $f"
            fetch "$BASE/planet/full/$f" "$INDEX_DIR/$f" &
        done
        wait
    fi

    # Layout JSON (always needed — string tier offsets) and strings_core
    # (shared between full and admin-minimal — admin-minimal pulls it
    # from full/ since the new tier doesn't re-emit it).
    curl -sL -o "$INDEX_DIR/strings_layout.json" "$BASE/planet/full/strings_layout.json"
    if [ "$MINIMAL" = "1" ]; then
        fetch "$BASE/planet/full/strings_core.bin" "$INDEX_DIR/strings_core.bin"
    fi

    # Admin polygons + postal polygons from the selected quality variant.
    # Skipped entirely when MINIMAL=1 — admin-minimal has its own
    # admin_polygons.bin / admin_vertices.bin (re-simplified over the
    # L2-L8 subset only) and doesn't use postal polygons at all.
    if [ "$MINIMAL" != "1" ]; then
        echo "Downloading admin polygons (quality=$QUALITY)..."
        for f in admin_polygons.bin admin_vertices.bin postal_polygons.bin postal_vertices.bin; do
            echo "  $f"
            fetch "$BASE/planet/quality/$QUALITY/$f" "$INDEX_DIR/$f" &
        done
        wait
    fi

    if [ "$MINIMAL" != "1" ]; then
        # POI meta + tier data + tier-specific strings_poi.bin
        curl -sL -o "$INDEX_DIR/poi_meta.json" "$BASE/planet/poi/$POI_TIER/poi_meta.json"
        echo "Downloading POI data (tier=$POI_TIER)..."
        for f in poi_records.bin poi_vertices.bin poi_cells.bin poi_entries.bin strings_poi.bin; do
            echo "  $f"
            fetch "$BASE/planet/poi/$POI_TIER/$f" "$INDEX_DIR/$f" &
        done
        wait
    fi

    echo "Download complete."
    SENTINEL_SIZE=$(stat -c%s "$INDEX_DIR/$SENTINEL" 2>/dev/null || echo 0)
    if [ "$SENTINEL_SIZE" -lt 1000 ]; then
        echo "Error: $SENTINEL is only ${SENTINEL_SIZE} bytes — download likely failed"
        exit 1
    fi
    echo "Verified: $SENTINEL is $((SENTINEL_SIZE / 1024 / 1024)) MiB"
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

echo "Starting server (quality=$QUALITY, poi=$POI_TIER, minimal=$MINIMAL)..."
exec query-server "$INDEX_DIR" 0.0.0.0:3000
