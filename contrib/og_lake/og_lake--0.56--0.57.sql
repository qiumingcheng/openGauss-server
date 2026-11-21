/* SQL57: serial read-only native time_travel(regclass, bigint/timestamptz), no catalog-format change. */
DO $og_lake_time_travel_runtime_0_57$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'time_travel')::boolean,false)
       OR COALESCE((og_lake.host_status()::jsonb ->> 'time_travel_sql_version') <> '0.57',true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.57 requires its reviewed time_travel runtime';
    END IF;
END
$og_lake_time_travel_runtime_0_57$;

CREATE FUNCTION og_lake.time_travel(scope regclass, version bigint)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'og_lake_time_travel_version'
LANGUAGE C STABLE NOT FENCED SECURITY INVOKER
SET search_path = pg_catalog, pg_temp;

CREATE FUNCTION og_lake.time_travel(scope regclass, at_time timestamptz)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'og_lake_time_travel_timestamp'
LANGUAGE C STABLE NOT FENCED SECURITY INVOKER
SET search_path = pg_catalog, pg_temp;

GRANT EXECUTE ON FUNCTION og_lake.time_travel(regclass, bigint) TO PUBLIC;
GRANT EXECUTE ON FUNCTION og_lake.time_travel(regclass, timestamptz) TO PUBLIC;
