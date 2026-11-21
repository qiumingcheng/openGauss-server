/* SQL58: serial read-only public CDC change-feed functions
 * table_insertions / table_deletions / table_changes over (regclass, bigint, bigint)
 * and (regclass, timestamptz, timestamptz), no catalog-format change. */
DO $og_lake_table_changes_runtime_0_58$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'table_changes')::boolean,false)
       OR COALESCE((og_lake.host_status()::jsonb ->> 'table_changes_sql_version') <> '0.58',true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.58 requires its reviewed table_changes runtime';
    END IF;
END
$og_lake_table_changes_runtime_0_58$;

CREATE FUNCTION og_lake.table_insertions(
    scope regclass, start_snapshot bigint, end_snapshot bigint)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'og_lake_table_insertions_version'
LANGUAGE C STABLE NOT FENCED SECURITY INVOKER
SET search_path = pg_catalog, pg_temp;

CREATE FUNCTION og_lake.table_insertions(
    scope regclass, start_snapshot timestamptz, end_snapshot timestamptz)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'og_lake_table_insertions_timestamp'
LANGUAGE C STABLE NOT FENCED SECURITY INVOKER
SET search_path = pg_catalog, pg_temp;

CREATE FUNCTION og_lake.table_deletions(
    scope regclass, start_snapshot bigint, end_snapshot bigint)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'og_lake_table_deletions_version'
LANGUAGE C STABLE NOT FENCED SECURITY INVOKER
SET search_path = pg_catalog, pg_temp;

CREATE FUNCTION og_lake.table_deletions(
    scope regclass, start_snapshot timestamptz, end_snapshot timestamptz)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'og_lake_table_deletions_timestamp'
LANGUAGE C STABLE NOT FENCED SECURITY INVOKER
SET search_path = pg_catalog, pg_temp;

CREATE FUNCTION og_lake.table_changes(
    scope regclass, start_snapshot bigint, end_snapshot bigint)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'og_lake_table_changes_version'
LANGUAGE C STABLE NOT FENCED SECURITY INVOKER
SET search_path = pg_catalog, pg_temp;

CREATE FUNCTION og_lake.table_changes(
    scope regclass, start_snapshot timestamptz, end_snapshot timestamptz)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'og_lake_table_changes_timestamp'
LANGUAGE C STABLE NOT FENCED SECURITY INVOKER
SET search_path = pg_catalog, pg_temp;

GRANT EXECUTE ON FUNCTION og_lake.table_insertions(regclass, bigint, bigint) TO PUBLIC;
GRANT EXECUTE ON FUNCTION og_lake.table_insertions(regclass, timestamptz, timestamptz) TO PUBLIC;
GRANT EXECUTE ON FUNCTION og_lake.table_deletions(regclass, bigint, bigint) TO PUBLIC;
GRANT EXECUTE ON FUNCTION og_lake.table_deletions(regclass, timestamptz, timestamptz) TO PUBLIC;
GRANT EXECUTE ON FUNCTION og_lake.table_changes(regclass, bigint, bigint) TO PUBLIC;
GRANT EXECUTE ON FUNCTION og_lake.table_changes(regclass, timestamptz, timestamptz) TO PUBLIC;
