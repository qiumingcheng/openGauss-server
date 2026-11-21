/* Enable bounded native inline INSERT only after the matching 0.3 runtime is installed. */
ALTER TABLE og_lake_internal.catalog_binding DROP CONSTRAINT catalog_binding_read_only_check;
ALTER TABLE og_lake_internal.table_binding DROP CONSTRAINT table_binding_read_only_check;
COMMENT ON FUNCTION og_lake.host_status() IS
'Native inline read and bounded INSERT capabilities; no SDK, file I/O, UPDATE or DELETE';

CREATE FUNCTION og_lake_internal.test_precommit_insert(regclass, integer) RETURNS void
AS 'MODULE_PATHNAME', 'og_lake_test_precommit_insert' LANGUAGE C STRICT VOLATILE NOT FENCED;
COMMENT ON FUNCTION og_lake_internal.test_precommit_insert(regclass, integer) IS
'Internal superuser-only probe: arm DEFAULT VALUES INSERT from a pre-commit callback; modes 0=first lake access,1=callback before lake,2=callback after lake';
REVOKE ALL ON FUNCTION og_lake_internal.test_precommit_insert(regclass, integer) FROM PUBLIC;
