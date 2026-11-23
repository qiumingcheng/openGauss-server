/* SQL65 (N7-STATS): Exact NUMERIC(p,s) canonical min/max bounds across
 * ducklake_table_column_stats and ducklake_file_column_stats, plus exact
 * file-level scalar/UUID/NUMERIC bounds across direct INSERT, flush, and rewrite/merge. */
DO $og_lake_stats_extended_0_65$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'stats_extended')::boolean,false)
       OR COALESCE((og_lake.host_status()::jsonb ->> 'stats_extended_sql_version') <> '0.65',true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'numeric_minmax_stats')::boolean,false)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'file_column_stats_bounds')::boolean,false) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.65 requires its reviewed extended stats runtime';
    END IF;
END
$og_lake_stats_extended_0_65$;
