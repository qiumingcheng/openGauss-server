/* SQL73 (PGL-3): truncate(N) partition transform across INT2/INT4/INT8/TEXT/BYTEA. */
DO $og_lake_truncate_partition_0_73$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.73','0.74'),true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'truncate_partition_supported')::boolean,false)
       OR COALESCE((og_lake.host_status()::jsonb ->> 'truncate_partition_sql_version') <> '0.73',true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.73 requires its reviewed truncate(N) partition runtime';
    END IF;
END
$og_lake_truncate_partition_0_73$;
