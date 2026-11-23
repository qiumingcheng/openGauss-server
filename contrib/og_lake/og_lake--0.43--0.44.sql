/* SQL44 admits logical BYTEA NOT NULL only; existing BLOB physical fields,
 * historical NULLs, ABI, column IDs and literal-default rules are unchanged. */
DO $og_lake_binary_not_null_runtime_0_44$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.44','0.45','0.46','0.47','0.48','0.49','0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'binary_not_null_constraints')::boolean,false) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.44 requires its reviewed BYTEA NOT NULL native lake runtime';
    END IF;
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_binary_not_null_runtime_0_44$;
