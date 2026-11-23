/* SQL25 enables whole-parent manual ANALYZE. Its helper locks and
 * validates every existing native parent before transactionally
 * invalidating stale parent/leaf statistics. A busy parent raises
 * 55P03 and rolls back the entire upgrade; no skipped parents. */
/* Explicit known binary compatibility; installed SQL controls capabilities. */
DO $og_lake_native_parent_runtime_0_25$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.25','0.26','0.29','0.30','0.31','0.32','0.33','0.34','0.35', '0.36','0.37','0.38','0.39','0.40','0.41','0.42','0.43','0.44','0.45','0.46','0.47','0.48','0.49','0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.25 requires its reviewed native lake runtime';
    END IF;
END
$og_lake_native_parent_runtime_0_25$;
CREATE FUNCTION og_lake_internal._native_parent_statistics_upgrade()
RETURNS void AS 'MODULE_PATHNAME','og_lake_native_parent_statistics_upgrade'
LANGUAGE C VOLATILE NOT FENCED;
REVOKE ALL ON FUNCTION og_lake_internal._native_parent_statistics_upgrade() FROM PUBLIC;

/* Use DO/PERFORM, as existing native upgrade scripts do. This OG executor
 * does not run an ordinary top-level SELECT from an extension script.
 * No table/data/format changes; retain the existing private owner and shape.
 * The C function authenticates CurrentExtensionObject and actual targetA.
 */
DO $og_lake_native_statistics_upgrade$
BEGIN
    PERFORM og_lake_internal._index_catalog_validate();
    PERFORM og_lake_internal._native_parent_statistics_upgrade();
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_native_statistics_upgrade$;

