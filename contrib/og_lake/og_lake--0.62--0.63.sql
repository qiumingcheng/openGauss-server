/* SQL63 (PGL-3): Expanded DuckLake file partition types (UUID/NUMERIC/DATE/temporal/BYTEA)
 * and compatibility combinations across extended payloads, indexes, COPY, and row SMP,
 * keeping standard truncate(...) explicitly rejected with 0A000. */
DO $og_lake_partition_combos_0_63$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'file_partition_combinations')::boolean,false)
       OR COALESCE((og_lake.host_status()::jsonb ->> 'file_partition_combinations_sql_version') <> '0.63',true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.63 requires its reviewed file partition combinations runtime';
    END IF;
END
$og_lake_partition_combos_0_63$;
