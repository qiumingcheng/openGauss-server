/* SQL67 (IX5 / PGL-4): PQ background cancel/cleanup, ANN rescan cache
 * poison/invalidation, round-robin index cursor LRU/reset, autoanalyze outcome
 * telemetry, and EXPLAIN ANALYZE partition-prune/fallback reconciliation. */
DO $og_lake_ix_observability_0_67$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'ix_observability')::boolean,false)
       OR COALESCE((og_lake.host_status()::jsonb ->> 'ix_obs_sql_version') <> '0.67',true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'pgl4_local_observability')::boolean,false) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.67 requires its reviewed index and observability runtime';
    END IF;
END
$og_lake_ix_observability_0_67$;
