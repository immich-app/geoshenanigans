-- Immich schema subset for the geodata tables (InitialMigration.ts:28-31,66-75,161,171; geodata-places.table.ts)
CREATE EXTENSION IF NOT EXISTS unaccent;
CREATE EXTENSION IF NOT EXISTS cube;
CREATE EXTENSION IF NOT EXISTS earthdistance;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE OR REPLACE FUNCTION f_unaccent(text) RETURNS text
  PARALLEL SAFE STRICT IMMUTABLE LANGUAGE SQL RETURN unaccent('unaccent', $1);
CREATE OR REPLACE FUNCTION ll_to_earth_public(latitude double precision, longitude double precision)
  RETURNS public.earth PARALLEL SAFE STRICT IMMUTABLE LANGUAGE SQL AS $$
    SELECT public.cube(public.cube(public.cube(public.earth()*cos(radians(latitude))*cos(radians(longitude))),public.earth()*cos(radians(latitude))*sin(radians(longitude))),public.earth()*sin(radians(latitude)))::public.earth
  $$;
CREATE TABLE geodata_places ("id" integer NOT NULL, "name" character varying(200) NOT NULL, "longitude" double precision NOT NULL,
  "latitude" double precision NOT NULL, "countryCode" character(2) NOT NULL, "admin1Code" character varying(20),
  "admin2Code" character varying(80), "modificationDate" date NOT NULL, "admin1Name" character varying,
  "admin2Name" character varying, "alternateNames" character varying);
CREATE TABLE naturalearth_countries ("id" integer NOT NULL GENERATED ALWAYS AS IDENTITY, "admin" character varying(50) NOT NULL,
  "admin_a3" character varying(3) NOT NULL, "type" character varying(50) NOT NULL, "coordinates" polygon NOT NULL);
-- map.repository.ts:312-317: the GiST index exists before the rows go in
CREATE INDEX "IDX_geodata_gist_earthcoord" ON geodata_places (ll_to_earth_public(latitude, longitude));
