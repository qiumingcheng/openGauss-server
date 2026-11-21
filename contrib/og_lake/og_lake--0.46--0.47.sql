/* SQL47 adds only plain full-scan INT2 native row Stream execution.
 * Original slots, snapshots, immutable whole-file tasks and join cleanup stay.
 * INT2 index/ANN SMP, Vec, PQ, temporal and partition expansion remain closed. */
DO $og_lake_int2_row_smp_runtime_0_47$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.47','0.48','0.49','0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'int2_row_smp')::boolean,false) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.47 requires its reviewed plain INT2 row-SMP native lake runtime';
    END IF;
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_int2_row_smp_runtime_0_47$;
