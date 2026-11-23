/* SQL24 uses the existing SQL23 controls and typed row-head domains.
 * No table rewrite, new row identifiers or independent commit. */
/* Explicit known binary compatibility; installed SQL controls capabilities. */
DO $og_lake_native_parent_runtime_0_24$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.24','0.25','0.26','0.29','0.30','0.31','0.32','0.33','0.34','0.35', '0.36','0.37','0.38','0.39','0.40','0.41','0.42','0.43','0.44','0.45','0.46','0.47','0.48','0.49','0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.24 requires its reviewed native lake runtime';
    END IF;
END
$og_lake_native_parent_runtime_0_24$;
COMMENT ON FUNCTION og_lake.bind_native_partition_readonly(regclass,name,oid[],bigint[]) IS
'Administrator and parent owner: certify two real empty native RANGE leaves and their bounded lake sources in the caller transaction. This entry leaves the parent read-only; no autonomous commit.';
COMMENT ON FUNCTION og_lake.enable_native_partition_insert(regclass) IS
'Administrator and parent owner: enable the coordinated installed-version write capabilities after readonly certification. SQL0.23 permits inline INSERT; SQL0.24/0.25 also permit bounded RC UPDATE/DELETE and serial COPY FROM. Commit mode change before business DML; no direct-file INSERT, indexes, SMP or arbitrary partition DDL.';
DO $og_lake_native_parent_validate$
BEGIN
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_native_parent_validate$;
