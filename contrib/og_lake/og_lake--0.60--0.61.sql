/* SQL61: Read-only external Parquet/CSV file foreign tables (og_lake_file_fdw)
 * confined by kernel openat2 anchor-fd roots and public schema inference. */
DO $og_lake_external_file_runtime_0_61$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'external_file_fdw')::boolean,false)
       OR COALESCE((og_lake.host_status()::jsonb ->> 'external_file_sql_version') <> '0.61',true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.61 requires its reviewed external_file_fdw runtime';
    END IF;
END
$og_lake_external_file_runtime_0_61$;

CREATE FUNCTION og_lake.file_fdw_handler() RETURNS fdw_handler
AS 'MODULE_PATHNAME', 'og_lake_file_fdw_handler' LANGUAGE C STRICT NOT FENCED;

CREATE FUNCTION og_lake.file_fdw_validator(text[], oid) RETURNS void
AS 'MODULE_PATHNAME', 'og_lake_file_fdw_validator' LANGUAGE C STRICT NOT FENCED;

CREATE FOREIGN DATA WRAPPER og_lake_file_fdw
HANDLER og_lake.file_fdw_handler VALIDATOR og_lake.file_fdw_validator;

REVOKE ALL ON FOREIGN DATA WRAPPER og_lake_file_fdw FROM PUBLIC;

CREATE FUNCTION og_lake.describe_external_file(
    server_name name,
    path text,
    format text DEFAULT NULL,
    header boolean DEFAULT true,
    delimiter text DEFAULT ','
)
RETURNS TABLE(
    column_name text,
    column_type text,
    nullable boolean,
    physical_ordinal integer
)
AS 'MODULE_PATHNAME', 'og_lake_describe_external_server_file'
LANGUAGE C VOLATILE NOT FENCED SECURITY INVOKER
SET search_path = pg_catalog, pg_temp;

COMMENT ON FUNCTION og_lake.describe_external_file(name, text, text, boolean, text) IS
'Inspect an external Parquet or CSV file beneath a trusted og_lake_file_fdw server external_root and return inferred column schema without catalog writes.';

CREATE FUNCTION og_lake.describe_external_file(
    path text
)
RETURNS TABLE(
    column_name text,
    column_type text,
    nullable boolean,
    physical_ordinal integer
)
AS 'MODULE_PATHNAME', 'og_lake_describe_external_path_file'
LANGUAGE C VOLATILE NOT FENCED SECURITY INVOKER
SET search_path = pg_catalog, pg_temp;

COMMENT ON FUNCTION og_lake.describe_external_file(text) IS
'Inspect an authorized standalone Parquet or CSV file path and return inferred column schema without catalog writes.';

CREATE FUNCTION og_lake.describe_external_path(
    path text,
    format text DEFAULT NULL,
    header boolean DEFAULT true,
    delimiter text DEFAULT ','
)
RETURNS TABLE(
    column_name text,
    column_type text,
    nullable boolean,
    physical_ordinal integer
)
AS 'MODULE_PATHNAME', 'og_lake_describe_external_path_file'
LANGUAGE C VOLATILE NOT FENCED SECURITY INVOKER
SET search_path = pg_catalog, pg_temp;

COMMENT ON FUNCTION og_lake.describe_external_path(text, text, boolean, text) IS
'Inspect an external Parquet or CSV file path authorized under server file privileges / safe_data_path with explicit format options.';

CREATE FUNCTION og_lake.external_file_info(
    server_name name,
    path text,
    format text DEFAULT NULL
)
RETURNS TABLE(
    format text,
    file_bytes bigint,
    row_count bigint,
    row_group_count integer,
    column_count integer
)
AS 'MODULE_PATHNAME', 'og_lake_external_file_info'
LANGUAGE C VOLATILE NOT FENCED SECURITY INVOKER
SET search_path = pg_catalog, pg_temp;

COMMENT ON FUNCTION og_lake.external_file_info(name, text, text) IS
'Return read-only size and Parquet footer population metadata for an external file beneath an og_lake_file_fdw server root.';

GRANT EXECUTE ON FUNCTION og_lake.describe_external_file(name, text, text, boolean, text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION og_lake.describe_external_file(text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION og_lake.describe_external_path(text, text, boolean, text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION og_lake.external_file_info(name, text, text) TO PUBLIC;
