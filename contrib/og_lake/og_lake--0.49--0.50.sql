/* SQL50 only: lossless logical expiry. No cleanup/physical file deletion. */
CREATE FUNCTION og_lake.expire_snapshots(catalog_name name,
    versions bigint[] DEFAULT NULL, older_than timestamptz DEFAULT NULL,
    dry_run boolean DEFAULT true)
RETURNS TABLE(snapshot_id bigint, snapshot_time timestamptz, schema_version bigint,
    next_catalog_id bigint, next_file_id bigint)
AS 'MODULE_PATHNAME', 'og_lake_expire_snapshots' LANGUAGE C VOLATILE NOT FENCED;
REVOKE ALL ON FUNCTION og_lake.expire_snapshots(name,bigint[],timestamptz,boolean) FROM PUBLIC;
COMMENT ON FUNCTION og_lake.expire_snapshots(name,bigint[],timestamptz,boolean) IS
'Bounded logical snapshot expiry; defaults to preview; current head and native certificate anchors protected; all changes, inline data and files retained; locks last to caller commit';
DO $og_lake_snapshot_expiry_runtime_0_50$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'logical_snapshot_expiry')::boolean,false) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.50 requires its reviewed logical snapshot expiry native runtime';
    END IF;
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_snapshot_expiry_runtime_0_50$;
