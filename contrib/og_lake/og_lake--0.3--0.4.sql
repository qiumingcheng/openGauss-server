/* Native pure-inline UPDATE/DELETE; existing 0.3 INSERT bindings are retained. */

CREATE FUNCTION og_lake_internal.row_locator(anyelement) RETURNS bytea
AS 'MODULE_PATHNAME', 'og_lake_row_locator' LANGUAGE C STRICT STABLE NOT FENCED;
COMMENT ON FUNCTION og_lake_internal.row_locator(anyelement) IS
'Internal planner marker; execution requires a native lake scan slot and cannot reconstruct row identity from values';
REVOKE ALL ON FUNCTION og_lake_internal.row_locator(anyelement) FROM PUBLIC;

COMMENT ON FUNCTION og_lake.host_status() IS
'Native inline read and bounded INSERT/UPDATE/DELETE capabilities; no SDK or file I/O';
