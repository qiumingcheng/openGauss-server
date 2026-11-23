/* SQL55: read-only standard snapshot information, no catalog-format change. */
DO $og_lake_snapshot_metadata_runtime_0_55$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'snapshot_metadata')::boolean,false)
       OR COALESCE((og_lake.host_status()::jsonb ->> 'snapshot_metadata_sql_version') <> '0.55',true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.55 requires its reviewed public snapshot metadata runtime';
    END IF;
END
$og_lake_snapshot_metadata_runtime_0_55$;

CREATE FUNCTION og_lake.current_snapshot(catalog_name name)
RETURNS TABLE(id bigint)
AS 'MODULE_PATHNAME', 'og_lake_current_snapshot'
LANGUAGE C STABLE NOT FENCED SECURITY INVOKER
SET search_path = pg_catalog, pg_temp;
CREATE FUNCTION og_lake.snapshots(catalog_name name)
RETURNS TABLE(snapshot_id bigint, snapshot_time timestamptz,
              schema_version bigint, changes jsonb,
              author text, commit_message text, commit_extra_info text)
AS 'MODULE_PATHNAME', 'og_lake_snapshots'
LANGUAGE C STABLE NOT FENCED SECURITY INVOKER
SET search_path = pg_catalog, pg_temp;
REVOKE ALL ON FUNCTION og_lake.current_snapshot(name) FROM PUBLIC;
REVOKE ALL ON FUNCTION og_lake.snapshots(name) FROM PUBLIC;
