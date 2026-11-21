/* SQL59: transaction-scoped read-only snapshot binding og_lake.bind_snapshot,
 * generational og_lake.rebuild_lake_index, DuckLake-compatible metadata functions
 * (table_info, list_files, options), and privilege-filtered public management status
 * views (catalog_status, table_status, index_status). No catalog-format change. */
DO $og_lake_bind_snapshot_runtime_0_59$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'bind_snapshot')::boolean,false)
       OR COALESCE((og_lake.host_status()::jsonb ->> 'bind_snapshot_sql_version') <> '0.59',true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'management_status_views')::boolean,false)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'rebuild_lake_index')::boolean,false) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.59 requires its reviewed bind_snapshot and management-views runtime';
    END IF;
END
$og_lake_bind_snapshot_runtime_0_59$;

CREATE FUNCTION og_lake.bind_snapshot(server_name name, snapshot_id bigint)
RETURNS void
AS 'MODULE_PATHNAME', 'og_lake_bind_snapshot'
LANGUAGE C VOLATILE NOT FENCED SECURITY INVOKER
SET search_path = pg_catalog, pg_temp;
COMMENT ON FUNCTION og_lake.bind_snapshot(name, bigint) IS
'Pin a read-only historical snapshot on server_name before the first lake access in an explicit READ COMMITTED transaction block; rejects writes/DDL/maintenance and unmapped columns.';

CREATE FUNCTION og_lake.rebuild_lake_index(index_id uuid)
RETURNS void
AS 'MODULE_PATHNAME', 'og_lake_rebuild_lake_index'
LANGUAGE C VOLATILE NOT FENCED;
COMMENT ON FUNCTION og_lake.rebuild_lake_index(uuid) IS
'Advance index definition_generation transactionally, retire prior segments/coverage without deleting reader-pinned artifacts, and build new-generation coverage.';

CREATE FUNCTION og_lake.table_info(catalog_name name)
RETURNS TABLE(
    table_name text,
    schema_id bigint,
    table_id bigint,
    table_uuid uuid,
    file_count bigint,
    file_size_bytes bigint,
    delete_file_count bigint,
    delete_file_size_bytes bigint
)
AS 'MODULE_PATHNAME', 'og_lake_table_info'
LANGUAGE C STABLE NOT FENCED SECURITY INVOKER
SET search_path = pg_catalog, pg_temp;

CREATE FUNCTION og_lake.list_files(
    scope regclass,
    snapshot_version bigint DEFAULT NULL,
    snapshot_time timestamptz DEFAULT NULL
)
RETURNS TABLE(
    data_file text,
    data_file_size_bytes bigint,
    data_file_footer_size bigint,
    data_file_encryption_key bytea,
    delete_file text,
    delete_file_size_bytes bigint,
    delete_file_footer_size bigint,
    delete_file_encryption_key bytea
)
AS 'MODULE_PATHNAME', 'og_lake_list_files'
LANGUAGE C STABLE NOT FENCED SECURITY INVOKER
SET search_path = pg_catalog, pg_temp;

CREATE FUNCTION og_lake.options(catalog_name name)
RETURNS TABLE(
    option_name text,
    description text,
    value text,
    scope text,
    scope_entry text
)
AS 'MODULE_PATHNAME', 'og_lake_options'
LANGUAGE C STABLE NOT FENCED SECURITY INVOKER
SET search_path = pg_catalog, pg_temp;

CREATE FUNCTION og_lake.get_catalog_status()
RETURNS TABLE(
    server_name name,
    server_oid oid,
    catalog_id uuid,
    format_version text,
    read_only boolean,
    credential_configured boolean,
    index_root_configured boolean,
    visible_snapshot bigint,
    bound_snapshot bigint,
    last_committed_snapshot bigint,
    bound_table_count bigint,
    auto_maintenance_enabled boolean,
    capabilities jsonb
)
AS 'MODULE_PATHNAME', 'og_lake_catalog_status_rows'
LANGUAGE C STABLE NOT FENCED SECURITY INVOKER
SET search_path = pg_catalog, pg_temp;

CREATE FUNCTION og_lake.get_table_status()
RETURNS TABLE(
    server_name name,
    relation regclass,
    schema_name name,
    table_name name,
    table_id bigint,
    table_uuid uuid,
    object_kind text,
    read_only boolean,
    effective_snapshot bigint,
    schema_version bigint,
    active_data_files bigint,
    active_data_bytes bigint,
    active_delete_files bigint,
    active_delete_bytes bigint,
    inline_carrier_count integer,
    inline_row_limit bigint,
    partition_keys integer,
    active_indexes bigint,
    flush_round_active boolean,
    reclaimed_row_heads bigint,
    analyze_committed_changes bigint,
    analyze_analyzed_changes bigint,
    analyze_snapshot bigint,
    analyze_last_success timestamptz,
    analyze_last_error integer
)
AS 'MODULE_PATHNAME', 'og_lake_table_status_rows'
LANGUAGE C STABLE NOT FENCED SECURITY INVOKER
SET search_path = pg_catalog, pg_temp;

CREATE FUNCTION og_lake.get_index_status()
RETURNS TABLE(
    index_uuid uuid,
    server_name name,
    relation regclass,
    index_name name,
    state text,
    kind text,
    method text,
    metric text,
    column_id bigint,
    column_name name,
    definition_generation bigint,
    registered_snapshot bigint,
    inline_index_ready boolean,
    ready_segments bigint,
    building_segments bigint,
    failed_segments bigint,
    retired_segments bigint,
    covered_files integer,
    uncovered_files integer,
    usable boolean,
    reason text
)
AS 'MODULE_PATHNAME', 'og_lake_index_status_rows'
LANGUAGE C STABLE NOT FENCED SECURITY INVOKER
SET search_path = pg_catalog, pg_temp;

CREATE VIEW og_lake.catalog_status AS SELECT * FROM og_lake.get_catalog_status();
CREATE VIEW og_lake.table_status AS SELECT * FROM og_lake.get_table_status();
CREATE VIEW og_lake.index_status AS SELECT * FROM og_lake.get_index_status();

GRANT EXECUTE ON FUNCTION og_lake.bind_snapshot(name, bigint) TO PUBLIC;
GRANT EXECUTE ON FUNCTION og_lake.rebuild_lake_index(uuid) TO PUBLIC;
GRANT EXECUTE ON FUNCTION og_lake.table_info(name) TO PUBLIC;
GRANT EXECUTE ON FUNCTION og_lake.list_files(regclass, bigint, timestamptz) TO PUBLIC;
GRANT EXECUTE ON FUNCTION og_lake.options(name) TO PUBLIC;
GRANT EXECUTE ON FUNCTION og_lake.get_catalog_status() TO PUBLIC;
GRANT EXECUTE ON FUNCTION og_lake.get_table_status() TO PUBLIC;
GRANT EXECUTE ON FUNCTION og_lake.get_index_status() TO PUBLIC;
GRANT SELECT ON og_lake.catalog_status, og_lake.table_status, og_lake.index_status TO PUBLIC;
