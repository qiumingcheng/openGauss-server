/* Bounded local inline history flush; same host transaction publishes files and removes inline rows. */
\echo Use "ALTER EXTENSION og_lake UPDATE TO '0.11'" to load this file. \quit
CREATE OR REPLACE FUNCTION og_lake.flush(regclass) RETURNS void
AS 'MODULE_PATHNAME', 'og_lake_flush' LANGUAGE C STRICT VOLATILE NOT FENCED;
REVOKE ALL ON FUNCTION og_lake.flush(regclass) FROM PUBLIC;
COMMENT ON FUNCTION og_lake.flush(regclass) IS
'Bounded local inline historical-data flush; explicit identities and position deletions, atomic host publication; bounded file-delete consolidation; physical GC deferred';
