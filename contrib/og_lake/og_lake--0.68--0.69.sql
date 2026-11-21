/* SQL69 (N9b): Local physical file and retired index-segment cleanup
 * protected by catalog ExclusiveLock guard, SQL0.68 active snapshot pins,
 * same-xact expiry refusal, conservative GetOldestXmin, and SDK openat2 unlink,
 * plus snapshot expiry scheduling of retired data/delete files and delete_older_than. */
CREATE FUNCTION og_lake.cleanup_old_files(
    catalog_name name,
    older_than timestamptz DEFAULT NULL,
    cleanup_all boolean DEFAULT false,
    dry_run boolean DEFAULT true
)
RETURNS TABLE(
    data_file_id bigint,
    path text,
    schedule_start timestamptz,
    deleted boolean,
    status text
)
AS 'MODULE_PATHNAME', 'og_lake_cleanup_old_files'
LANGUAGE C VOLATILE NOT FENCED;
REVOKE ALL ON FUNCTION og_lake.cleanup_old_files(name,timestamptz,boolean,boolean) FROM PUBLIC;

CREATE FUNCTION og_lake.delete_orphaned_files(
    catalog_name name,
    older_than timestamptz DEFAULT NULL,
    cleanup_all boolean DEFAULT false,
    dry_run boolean DEFAULT true
)
RETURNS TABLE(
    path text,
    file_size_bytes bigint,
    modified_at timestamptz,
    deleted boolean,
    status text
)
AS 'MODULE_PATHNAME', 'og_lake_delete_orphaned_files'
LANGUAGE C VOLATILE NOT FENCED;
REVOKE ALL ON FUNCTION og_lake.delete_orphaned_files(name,timestamptz,boolean,boolean) FROM PUBLIC;

CREATE FUNCTION og_lake.cleanup_index_segments(
    catalog_name name,
    dry_run boolean DEFAULT true
)
RETURNS TABLE(
    segment_uuid uuid,
    artifact_root_uri text,
    state text,
    deleted boolean,
    status text
)
AS 'MODULE_PATHNAME', 'og_lake_cleanup_index_segments'
LANGUAGE C VOLATILE NOT FENCED;
REVOKE ALL ON FUNCTION og_lake.cleanup_index_segments(name,boolean) FROM PUBLIC;

DO $og_lake_physical_cleanup_0_69$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.69','0.70','0.71','0.72','0.73','0.74'),true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'physical_cleanup')::boolean,false)
       OR COALESCE((og_lake.host_status()::jsonb ->> 'physical_cleanup_sql_version') <> '0.69',true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'n9b_local_cleanup')::boolean,false) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.69 requires its reviewed local physical cleanup runtime';
    END IF;
END
$og_lake_physical_cleanup_0_69$;
