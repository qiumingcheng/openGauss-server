/* SQL56: logical-session last_committed_snapshot, no catalog-format change. */
DO $og_lake_last_committed_snapshot_runtime_0_56$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'last_committed_snapshot')::boolean,false)
       OR COALESCE((og_lake.host_status()::jsonb ->> 'last_committed_snapshot_sql_version') <> '0.56',true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.56 requires its reviewed last_committed_snapshot runtime';
    END IF;
END
$og_lake_last_committed_snapshot_runtime_0_56$;

CREATE FUNCTION og_lake.last_committed_snapshot(catalog_name name)
RETURNS TABLE(id bigint)
AS 'MODULE_PATHNAME', 'og_lake_last_committed_snapshot'
LANGUAGE C STABLE NOT FENCED SECURITY INVOKER
SET search_path = pg_catalog, pg_temp;
REVOKE ALL ON FUNCTION og_lake.last_committed_snapshot(name) FROM PUBLIC;
