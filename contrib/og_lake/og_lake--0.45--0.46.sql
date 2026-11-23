/* SQL46 adds exact same-type INT2 scalar indexes only. Existing signed
 * INT64 page/dictionary layout, hashes, SDK ABI and source visibility stay fixed. */
DO $og_lake_int2_scalar_runtime_0_46$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.46','0.47','0.48','0.49','0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'int2_scalar_indexes')::boolean,false) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.46 requires its reviewed INT2 scalar-index native lake runtime';
    END IF;
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_int2_scalar_runtime_0_46$;
