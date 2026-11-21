/* SQL54 adds finite numeric(p,s) with exact standard decimal128 parameters.
 * The host codec applies no rounding; no catalog schema or row-ID change. */
DO $og_lake_numeric_runtime_0_54$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'numeric_business_type')::boolean,false)
       OR COALESCE((og_lake.host_status()::jsonb ->> 'numeric_sdk_type')::integer,0) <> 15
       OR COALESCE((og_lake.host_status()::jsonb ->> 'numeric_optional_sdk_abi')::integer,0) <> 5 THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.54 requires its reviewed exact decimal128 native runtime';
    END IF;
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_numeric_runtime_0_54$;
