/* Persistent standard metadata options, no format migration. */
\echo Use "ALTER EXTENSION og_lake UPDATE TO '0.12'" to load this file. \quit

CREATE FUNCTION og_lake.set_option(regclass, text, text) RETURNS void
AS 'MODULE_PATHNAME', 'og_lake_set_table_option' LANGUAGE C VOLATILE NOT FENCED;
CREATE FUNCTION og_lake.set_option(name, text, text) RETURNS void
AS 'MODULE_PATHNAME', 'og_lake_set_catalog_option' LANGUAGE C VOLATILE NOT FENCED;
COMMENT ON FUNCTION og_lake.set_option(regclass, text, text) IS
'Table-owner positive data_inlining_row_limit; persistent standard metadata, caller transaction, no implicit snapshot or commit; no mixing with lake writes';
COMMENT ON FUNCTION og_lake.set_option(name, text, text) IS
'Administrator catalog-wide positive data_inlining_row_limit; existing table overrides win; caller transaction, no implicit snapshot or commit';
