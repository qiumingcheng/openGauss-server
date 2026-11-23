/* SQL66 (N6 / PGL-1): Extended literal defaults across FLOAT4/FLOAT8/DATE/TIMESTAMP/
 * TIMESTAMPTZ/UUID/JSON/NUMERIC(p,s), ADD COLUMN ... NOT NULL DEFAULT <literal>,
 * and cross-session relcache/plan-cache invalidation across options/index/flush/rewrite. */
DO $og_lake_extended_defaults_0_66$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'extended_defaults')::boolean,false)
       OR COALESCE((og_lake.host_status()::jsonb ->> 'extended_defaults_sql_version') <> '0.66',true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'add_column_not_null_default')::boolean,false)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'relcache_plan_invalidation')::boolean,false) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.66 requires its reviewed extended defaults runtime';
    END IF;
END
$og_lake_extended_defaults_0_66$;
