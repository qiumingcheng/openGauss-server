/* SQL51 widens only explicit bounded same-group file rewrite. */
DO $og_lake_bounded_multi_rewrite_runtime_0_51$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'bounded_multi_file_rewrite')::boolean,false)
       OR COALESCE((og_lake.host_status()::jsonb ->> 'bounded_multi_file_rewrite_max_sources')::integer,0) <> 16 THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.51 requires its reviewed bounded same-group rewrite native runtime';
    END IF;
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_bounded_multi_rewrite_runtime_0_51$;
