/* SQL72 (N7-VECTOR): SMP + Vectorized VecForeignScan parallel execution,
 * extended vectorized scalar/temporal/partition scans, and per-batch cleanup. */
DO $og_lake_vector_smp_0_72$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.72','0.73','0.74'),true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'vector_smp')::boolean,false)
       OR COALESCE((og_lake.host_status()::jsonb ->> 'vector_smp_sql_version') <> '0.72',true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.72 requires its reviewed SMP vectorized scan runtime';
    END IF;
END
$og_lake_vector_smp_0_72$;
