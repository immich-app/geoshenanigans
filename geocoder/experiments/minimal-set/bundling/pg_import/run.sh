#!/usr/bin/env bash
# Throwaway Postgres (Immich's postgres image, already on the box) to time Immich's geodata import
# pattern and measure the resulting on-disk size. Data dir under /ssd/shrink/tmp, removed afterwards.
set -euo pipefail
cd "$(dirname "$0")"
NAME=shrink-bundling-pg
IMG=ghcr.io/immich-app/postgres:14-vectorchord0.4.3-pgvectors0.2.0
PGDATA_HOST=/ssd/shrink/tmp/bundling-pgdata
docker rm -f "$NAME" >/dev/null 2>&1 || true
rm -rf "$PGDATA_HOST"; mkdir -p "$PGDATA_HOST"
docker run -d --name "$NAME" --cpus=8 --memory=8g -e POSTGRES_PASSWORD=measure -e POSTGRES_DB=immich \
  -v "$PGDATA_HOST":/var/lib/postgresql/data -v "$PWD":/work:ro "$IMG" >/dev/null
for _ in $(seq 1 90); do
  docker exec "$NAME" pg_isready -U postgres -d immich >/dev/null 2>&1 && break
  sleep 1
done
sleep 3
P() { docker exec -i "$NAME" psql -q -U postgres -d immich -v ON_ERROR_STOP=1 "$@"; }
P -f /work/schema.sql
t0=$(date +%s.%N)
# geodata (9 concurrent 5000-row INSERT streams) and naturalearth run concurrently, as Promise.all at map.repository.ts:69
( for i in $(seq 0 8); do P -f /work/places_$i.sql & done; wait ) &
GEO=$!
( P -f /work/ne.sql && P -c 'ALTER TABLE naturalearth_countries ADD PRIMARY KEY (id) WITH (FILLFACTOR = 100)' ) &
NE=$!
wait $GEO
t1=$(date +%s.%N)
# map.repository.ts:417-440: PK + 4 trigram GIN indexes, concurrently
P -c 'ALTER TABLE geodata_places ADD PRIMARY KEY (id) WITH (FILLFACTOR = 100)' &
P -c 'CREATE INDEX idx_geodata_places_alternate_names ON geodata_places USING gin (f_unaccent("alternateNames") gin_trgm_ops)' &
P -c 'CREATE INDEX idx_geodata_places_name ON geodata_places USING gin (f_unaccent(name) gin_trgm_ops)' &
P -c 'CREATE INDEX idx_geodata_places_admin1_name ON geodata_places USING gin (f_unaccent("admin1Name") gin_trgm_ops)' &
P -c 'CREATE INDEX idx_geodata_places_admin2_name ON geodata_places USING gin (f_unaccent("admin2Name") gin_trgm_ops)' &
wait
true
t2=$(date +%s.%N)
echo "insert phase $(echo "$t1 - $t0" | bc) s; index phase $(echo "$t2 - $t1" | bc) s; total $(echo "$t2 - $t0" | bc) s"
P -c "VACUUM ANALYZE geodata_places"; P -c "VACUUM ANALYZE naturalearth_countries"
P -c "SELECT relname, n_live_tup FROM pg_stat_user_tables ORDER BY relname;"
P -c "SELECT c.relname, c.relkind, pg_size_pretty(pg_relation_size(c.oid)) AS size, pg_relation_size(c.oid) AS bytes
      FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'public' AND (c.relname LIKE '%geodata%' OR c.relname LIKE '%naturalearth%')
      ORDER BY bytes DESC;"
P -c "SELECT 'geodata_places' t, pg_total_relation_size('geodata_places') total, pg_table_size('geodata_places') heap_toast, pg_indexes_size('geodata_places') idx
      UNION ALL SELECT 'naturalearth_countries', pg_total_relation_size('naturalearth_countries'), pg_table_size('naturalearth_countries'), pg_indexes_size('naturalearth_countries');"
# forward search as the Change location modal issues it (search.repository.ts:403-427)
P -c "\timing on" -c "SELECT name, \"admin1Name\", \"countryCode\" FROM geodata_places WHERE f_unaccent(name) %>> f_unaccent('Weehawken') or f_unaccent(\"admin2Name\") %>> f_unaccent('Weehawken') or f_unaccent(\"admin1Name\") %>> f_unaccent('Weehawken') or f_unaccent(\"alternateNames\") %>> f_unaccent('Weehawken') ORDER BY coalesce(f_unaccent(name) <->>> f_unaccent('Weehawken'), 0.1) + coalesce(f_unaccent(\"admin2Name\") <->>> f_unaccent('Weehawken'), 0.1) + coalesce(f_unaccent(\"admin1Name\") <->>> f_unaccent('Weehawken'), 0.1) + coalesce(f_unaccent(\"alternateNames\") <->>> f_unaccent('Weehawken'), 0.1) LIMIT 5;"
docker rm -f "$NAME" >/dev/null
rm -rf "$PGDATA_HOST"
