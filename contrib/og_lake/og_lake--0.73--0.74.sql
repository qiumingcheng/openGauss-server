/* SQL74 (PGL-0 / IX5-N7): Expanded native pg_partition (RANGE/LIST/HASH, multi-leaf up to 64,
 * extended partition key & business column types) and high-dim Vamana+PQ build/recall/cache/cost. */
DO $og_lake_native_partition_expanded_0_74$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.74'),true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'native_partition_expanded')::boolean,false)
       OR COALESCE((og_lake.host_status()::jsonb ->> 'native_partition_expanded_sql_version') <> '0.74',true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.74 requires its reviewed expanded native partition and DiskANN/PQ runtime';
    END IF;
END
$og_lake_native_partition_expanded_0_74$;
