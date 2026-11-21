/* SQL64 (IX1~IX5 & N7-SMP): Extended payload columns (BYTEA/UUID/JSON/NUMERIC)
 * across scalar/ANN indexes and row SMP, plus PQ parallel ANN SMP (dop=2/4,
 * multi-file <= LAKE_SMP_MAX_FILES, unfiltered & general Boolean/IN scalar
 * filter trees) and qualified background index maintenance (IX5). */
DO $og_lake_ix_smp_extended_0_64$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'ix_smp_extended')::boolean,false)
       OR COALESCE((og_lake.host_status()::jsonb ->> 'ix_smp_extended_sql_version') <> '0.64',true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.64 requires its reviewed extended index and PQ SMP runtime';
    END IF;
END
$og_lake_ix_smp_extended_0_64$;
