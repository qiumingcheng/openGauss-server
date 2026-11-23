/* SQL49: protected native CREATE with authenticated typed literal defaults. */
DO $og_lake_create_default_runtime_0_49$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.49','0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'create_literal_defaults')::boolean,false) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.49 requires its reviewed native CREATE literal-default runtime';
    END IF;
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_create_default_runtime_0_49$;
