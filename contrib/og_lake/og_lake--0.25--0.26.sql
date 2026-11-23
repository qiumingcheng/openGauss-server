/* SQL26 adds bounded typed native-parent synchronization-head maintenance.
 * This version changes no DuckLake format, binding shape or allocator state. */
DO $og_lake_native_parent_runtime_0_26$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.26','0.29','0.30','0.31','0.32','0.33','0.34','0.35', '0.36','0.37','0.38','0.39','0.40','0.41','0.42','0.43','0.44','0.45','0.46','0.47','0.48','0.49','0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.26 requires its reviewed native lake runtime';
    END IF;
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_native_parent_runtime_0_26$;
COMMENT ON FUNCTION og_lake.reclaim_row_heads(regclass,integer) IS
'Owner-only bounded synchronization-head reclamation in the caller transaction. SQL0.26 also supports one typed native-parent leaf per call; at most512 keys, busy heads skipped. No business-row deletion, implicit commit, VACUUM or lake-file GC.';
