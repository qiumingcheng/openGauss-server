/* Catalog creation does not migrate existing DuckLake metadata. */
\echo Use "ALTER EXTENSION og_lake UPDATE TO '0.6'" to load this file. \quit
CREATE OR REPLACE FUNCTION og_lake.create_catalog(name, name, text, text) RETURNS void
AS 'MODULE_PATHNAME', 'og_lake_create_catalog' LANGUAGE C VOLATILE NOT FENCED;
REVOKE ALL ON FUNCTION og_lake.create_catalog(name, name, text, text) FROM PUBLIC;
COMMENT ON FUNCTION og_lake.create_catalog(name, name, text, text) IS
'Administrator-only DuckLake 1.1 creation in the current transaction; requires configured private owner, unused absolute local root and NULL credential; creates no files';
