/* SQL0.20 adds capability-gated signed SMALLINT without changing standard
 * DuckLake or private control-table shapes. Older runtime binaries reject this
 * script atomically; retained SQL<=19 continue to reject SMALLINT. */
DO $og_lake_smallint_runtime$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.20','0.21','0.22','0.23','0.24','0.25','0.26','0.29','0.30','0.31','0.32','0.33','0.34','0.35', '0.36','0.37','0.38','0.39','0.40','0.41','0.42','0.43','0.44','0.45','0.46','0.47','0.48','0.49','0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.20 requires native lake runtime0.20,0.21,0.22,0.23,0.24 or0.25,0.26';
    END IF;
END
$og_lake_smallint_runtime$;
