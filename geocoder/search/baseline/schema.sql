-- Immich schema subset for geodata_places (copied from /ssd/shrink/bundling/pg_import/schema.sql)
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
CREATE INDEX "IDX_geodata_gist_earthcoord" ON geodata_places (ll_to_earth_public(latitude, longitude));
