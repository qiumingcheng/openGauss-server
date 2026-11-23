/* SQL0.22 adds the first standard identity file-partition slice. */
DO $og_lake_partition_runtime$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.22','0.23','0.24','0.25','0.26','0.29','0.30','0.31','0.32','0.33','0.34','0.35', '0.36','0.37','0.38','0.39','0.40','0.41','0.42','0.43','0.44','0.45','0.46','0.47','0.48','0.49','0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.22 requires native lake runtime0.22,0.23,0.24 or0.25,0.26';
    END IF;
END
$og_lake_partition_runtime$;
/* Preserve existing canonical binding rows and their exact owner. No default,
 * new FK, trigger or side transaction. DROP already removes the entire row. */
ALTER TABLE og_lake_internal.table_binding
    ADD COLUMN flush_birth_cutoff bigint,
    ADD COLUMN flush_carrier_oid oid,
    ADD COLUMN flush_source_schema bigint,
    ADD COLUMN flush_source_origin bigint;
CREATE FUNCTION og_lake.set_partition(scope regclass, partition_by text[])
RETURNS void AS 'MODULE_PATHNAME','og_lake_set_partition'
LANGUAGE C VOLATILE NOT FENCED;
CREATE FUNCTION og_lake.reset_partition(scope regclass)
RETURNS void AS 'MODULE_PATHNAME','og_lake_reset_partition'
LANGUAGE C VOLATILE NOT FENCED;
CREATE FUNCTION og_lake.get_partition(scope regclass,
    OUT partition_key_index bigint, OUT column_name text, OUT transform text)
RETURNS SETOF record AS 'MODULE_PATHNAME','og_lake_get_partition'
LANGUAGE C STABLE NOT FENCED;
-- All endpoints enforce target ownership/current trusted catalog internally.
