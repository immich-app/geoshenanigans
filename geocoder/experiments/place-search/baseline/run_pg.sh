#!/usr/bin/env bash
# Throwaway Postgres (Immich's image) with geodata_places as Immich builds it; left running on
# 127.0.0.1:55432 for the evaluation. Stop with: docker rm -f search-baseline-pg; rm -rf pgdata
set -euo pipefail
cd "$(dirname "$0")"
NAME=search-baseline-pg
IMG=ghcr.io/immich-app/postgres:14-vectorchord0.4.3-pgvectors0.2.0
docker rm -f "$NAME" >/dev/null 2>&1 || true
rm -rf pgdata; mkdir -p pgdata
/ssd/search/venv/bin/python make_sql.py
docker run -d --name "$NAME" --cpus=8 --memory=8g -p 127.0.0.1:55432:5432 -e POSTGRES_PASSWORD=search \
  -e POSTGRES_DB=immich -v "$PWD/pgdata":/var/lib/postgresql/data -v "$PWD":/work:ro "$IMG" >/dev/null
for _ in $(seq 1 90); do docker exec "$NAME" pg_isready -U postgres -d immich >/dev/null 2>&1 && break; sleep 1; done
sleep 3
P() { docker exec -i "$NAME" psql -q -U postgres -d immich -v ON_ERROR_STOP=1 "$@"; }
P -f /work/schema.sql
P -f /work/places.sql
P -c 'ALTER TABLE geodata_places ADD PRIMARY KEY (id) WITH (FILLFACTOR = 100)' &
P -c 'CREATE INDEX idx_geodata_places_alternate_names ON geodata_places USING gin (f_unaccent("alternateNames") gin_trgm_ops)' &
P -c 'CREATE INDEX idx_geodata_places_name ON geodata_places USING gin (f_unaccent(name) gin_trgm_ops)' &
P -c 'CREATE INDEX idx_geodata_places_admin1_name ON geodata_places USING gin (f_unaccent("admin1Name") gin_trgm_ops)' &
P -c 'CREATE INDEX idx_geodata_places_admin2_name ON geodata_places USING gin (f_unaccent("admin2Name") gin_trgm_ops)' &
wait
P -c "VACUUM ANALYZE geodata_places"
P -c "SELECT count(*) FROM geodata_places"
P -c "SELECT pg_size_pretty(pg_total_relation_size('geodata_places')) total, pg_size_pretty(pg_indexes_size('geodata_places')) idx"
