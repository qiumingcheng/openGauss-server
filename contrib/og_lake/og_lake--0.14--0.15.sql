/* Native inline B-tree storage binding; no DuckLake format change. */
\echo Use "ALTER EXTENSION og_lake UPDATE TO '0.15'" to load this file. \quit
ALTER TABLE og_lake_internal.index_definition ADD COLUMN inline_index_oid oid;
DO $og_lake_index_validate$
BEGIN
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_index_validate$;

CREATE FUNCTION og_lake.build_lake_index(uuid) RETURNS void
AS 'MODULE_PATHNAME', 'og_lake_build_lake_index' LANGUAGE C VOLATILE NOT FENCED;
COMMENT ON FUNCTION og_lake.build_lake_index(uuid) IS
'Build the host B-tree over the inline carrier transactionally; files remain scanned until a separate file backend is implemented';
