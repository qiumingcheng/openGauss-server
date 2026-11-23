/* SQL60: DuckLake-compatible og_lake.merge_adjacent_files, persistent
 * target_file_size / auto_compact options, automatic background small-file merge,
 * and cross-schema-evolution (ADD/DROP COLUMN) multi-file & partitioned rewrite. */
DO $og_lake_merge_adjacent_runtime_0_60$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'merge_adjacent_files')::boolean,false)
       OR COALESCE((og_lake.host_status()::jsonb ->> 'merge_adjacent_sql_version') <> '0.60',true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'cross_evolution_rewrite')::boolean,false)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'target_file_size_option')::boolean,false) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.60 requires its reviewed merge_adjacent_files and cross-evolution rewrite runtime';
    END IF;
END
$og_lake_merge_adjacent_runtime_0_60$;

CREATE FUNCTION og_lake.merge_adjacent_files(
    relation regclass,
    max_files integer DEFAULT 16,
    target_file_size bigint DEFAULT NULL
)
RETURNS TABLE(
    schema_name text,
    table_name text,
    files_processed bigint,
    files_created bigint
)
AS 'MODULE_PATHNAME', 'og_lake_merge_adjacent_files'
LANGUAGE C VOLATILE NOT FENCED SECURITY INVOKER
SET search_path = pg_catalog, pg_temp;

COMMENT ON FUNCTION og_lake.merge_adjacent_files(regclass, integer, bigint) IS
'Greedy adjacent small-file compaction within each partition group toward target_file_size while preserving original row_id and birth_snapshot history.';

GRANT EXECUTE ON FUNCTION og_lake.merge_adjacent_files(regclass, integer, bigint) TO PUBLIC;
