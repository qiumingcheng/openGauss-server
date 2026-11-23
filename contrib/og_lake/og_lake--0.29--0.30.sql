/* Ordinary DuckLake bucket file partitions. Existing parent bits remain SQL26.
 * This is the direct29->30 path; uninstalled private27/28 are not inherited.
 * Standard metadata tables, SDK ABI and the existing two-argument API are unchanged. */
DO $og_lake_bucket_runtime_0_30$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.30','0.31','0.32','0.33','0.34','0.35', '0.36','0.37','0.38','0.39','0.40','0.41','0.42','0.43','0.44','0.45','0.46','0.47','0.48','0.49','0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.30 requires its reviewed native lake runtime';
    END IF;
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_bucket_runtime_0_30$;
CREATE FUNCTION og_lake.set_partition(scope regclass, partition_by text[], transforms text[])
RETURNS void AS 'MODULE_PATHNAME','og_lake_set_partition'
LANGUAGE C VOLATILE CALLED ON NULL INPUT NOT FENCED;
COMMENT ON FUNCTION og_lake.set_partition(regclass,text[],text[]) IS
'Owner-only ordered partition columns and corresponding identity/bucket(N) transforms. N is1..2147483647; bool/int2/int4/int8/UTF8 text only. SET/RESET require no registered inline carrier. Standard DuckLake file metadata; original two-argument identity API remains unchanged.';
