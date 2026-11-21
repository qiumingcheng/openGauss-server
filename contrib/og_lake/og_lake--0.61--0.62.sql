/* SQL62: Controlled local Parquet/CSV COPY FROM/TO into heap, foreign, and
 * native/file-partitioned DuckLake tables over openat2-confined paths. */
DO $og_lake_copy_io_runtime_0_62$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'copy_io')::boolean,false)
       OR COALESCE((og_lake.host_status()::jsonb ->> 'copy_io_sql_version') <> '0.62',true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.62 requires its reviewed controlled Parquet/CSV COPY I/O runtime';
    END IF;
END
$og_lake_copy_io_runtime_0_62$;
