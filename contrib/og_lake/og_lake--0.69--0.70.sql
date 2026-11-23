/* SQL70 (N6 / OPS1): widening ALTER COLUMN TYPE evolution + operational backup
 * manifest, catalog/index integrity verification/repair, and retention status. */
CREATE FUNCTION og_lake.backup_manifest(catalog_name name)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'og_lake_backup_manifest'
LANGUAGE C VOLATILE NOT FENCED;

CREATE FUNCTION og_lake.verify_catalog_integrity(catalog_name name, repair boolean DEFAULT false)
RETURNS TABLE(object_kind text, object_id text, severity text, status text, detail text)
AS 'MODULE_PATHNAME', 'og_lake_verify_catalog_integrity'
LANGUAGE C VOLATILE NOT FENCED;

CREATE FUNCTION og_lake.retention_status(catalog_name name)
RETURNS TABLE(
    catalog_name text,
    head_snapshot bigint,
    total_snapshots bigint,
    expired_scheduled_files bigint,
    scheduled_bytes bigint,
    active_pins bigint,
    min_pinned_snapshot bigint,
    delete_older_than text,
    oldest_xmin text,
    ready_index_segments bigint,
    retired_index_segments bigint,
    auto_compact text,
    target_file_size text
)
AS 'MODULE_PATHNAME', 'og_lake_retention_status'
LANGUAGE C VOLATILE NOT FENCED;

REVOKE ALL ON FUNCTION og_lake.backup_manifest(name) FROM PUBLIC;
REVOKE ALL ON FUNCTION og_lake.verify_catalog_integrity(name, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION og_lake.retention_status(name) FROM PUBLIC;

DO $og_lake_alter_type_ops_0_70$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.70','0.71','0.72','0.73','0.74'),true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'alter_column_type_evolution')::boolean,false)
       OR COALESCE((og_lake.host_status()::jsonb ->> 'alter_type_ops_sql_version') <> '0.70',true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'ops1_backup_integrity_retention')::boolean,false) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.70 requires its reviewed ALTER COLUMN TYPE and OPS1 runtime';
    END IF;
END
$og_lake_alter_type_ops_0_70$;
