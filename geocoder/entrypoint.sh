#!/bin/sh
set -e

DATA_DIR="${DATA_DIR:-/data}"

download_pbf() {
    mkdir -p "$DATA_DIR/pbf"
    for url in $PBF_URLS; do
        filename=$(basename "$url")
        if [ ! -f "$DATA_DIR/pbf/$filename" ]; then
            echo "Downloading $url..."
            curl -fSL -o "$DATA_DIR/pbf/$filename" "$url"
        else
            echo "Already downloaded: $filename"
        fi
    done
}

build_index() {
    files=""
    for f in "$DATA_DIR"/pbf/*.osm.pbf; do
        [ -f "$f" ] && files="$files $f"
    done

    build_args=""
    [ -n "$STREET_LEVEL" ] && build_args="$build_args --street-level $STREET_LEVEL"
    [ -n "$ADMIN_LEVEL" ] && build_args="$build_args --admin-level $ADMIN_LEVEL"
    [ -n "$MAX_ADMIN_LEVEL" ] && build_args="$build_args --max-admin-level $MAX_ADMIN_LEVEL"

    # Cache support
    if [ -n "$LOAD_CACHE" ] && [ -f "$LOAD_CACHE" ]; then
        echo "Loading from cache: $LOAD_CACHE"
        build_args="$build_args --load-cache $LOAD_CACHE"
    elif [ -z "$files" ]; then
        echo "Error: no PBF files found in $DATA_DIR/pbf/ and no cache file"
        exit 1
    fi

    [ -n "$SAVE_CACHE" ] && build_args="$build_args --save-cache $SAVE_CACHE"

    # Output mode
    if [ -n "$MULTI_OUTPUT" ]; then
        build_args="$build_args --multi-output"
    elif [ -n "$INDEX_MODE" ]; then
        build_args="$build_args --mode $INDEX_MODE"
    fi

    # Continent generation
    [ -n "$CONTINENTS" ] && build_args="$build_args --continents"

    mkdir -p "$DATA_DIR/index"
    echo "Building index..."
    if [ -n "$LOAD_CACHE" ] && [ -f "$LOAD_CACHE" ]; then
        build-index "$DATA_DIR/index" $build_args
    else
        build-index "$DATA_DIR/index" $files $build_args
    fi
    echo "Index built."
}

decompress_index() {
    local dir="$1"
    [ -d "$dir" ] || return 0
    local count
    count=$(find "$dir" -maxdepth 1 -name "*.bin.zst" 2>/dev/null | wc -l)
    [ "$count" -eq 0 ] && return 0
    echo "Decompressing $count .bin.zst files in $dir..."
    find "$dir" -maxdepth 1 -name "*.bin.zst" -print0 \
        | xargs -0 -n1 -P4 sh -c '
            raw="${1%.zst}"
            if [ -f "$raw" ]; then
                rm -f "$1"
            else
                zstd -d -T0 --rm --quiet "$1"
            fi
        ' _
}

serve() {
    # Determine which index dir to serve
    index_dir="$DATA_DIR/index"
    if [ -n "$MULTI_OUTPUT" ]; then
        # Default to full when multi-output was used
        index_dir="$DATA_DIR/index/${SERVE_MODE:-full}"
    fi

    # Decompress any .bin.zst that shipped in the data dir. Idempotent —
    # if a raw .bin already exists alongside, the compressed variant is
    # dropped. Server mmaps raw bytes so only .bin is needed at runtime.
    decompress_index "$index_dir"

    args="$index_dir"
    if [ -n "$DOMAIN" ]; then
        args="$args --domain $DOMAIN"
        if [ -n "$CACHE_DIR" ]; then
            args="$args --cache $CACHE_DIR"
        fi
    else
        args="$args ${BIND_ADDR:-0.0.0.0:3000}"
    fi
    [ -n "$STREET_LEVEL" ] && args="$args --street-level $STREET_LEVEL"
    [ -n "$ADMIN_LEVEL" ] && args="$args --admin-level $ADMIN_LEVEL"
    [ -n "$SEARCH_DISTANCE" ] && args="$args --search-distance $SEARCH_DISTANCE"
    echo "Starting server..."
    exec query-server $args
}

case "${1:-auto}" in
    build)
        download_pbf
        build_index
        ;;
    serve)
        serve
        ;;
    auto)
        download_pbf
        build_index
        serve
        ;;
    *)
        exec "$@"
        ;;
esac
