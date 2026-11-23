/* Native host prototype; no data catalog is created by installation. */
\echo Use "CREATE EXTENSION og_lake" to load this file. \quit

CREATE SCHEMA og_lake;
CREATE SCHEMA og_lake_internal;
REVOKE ALL ON SCHEMA og_lake_internal FROM PUBLIC;
GRANT USAGE ON SCHEMA og_lake TO PUBLIC;

CREATE FUNCTION og_lake.fdw_handler() RETURNS fdw_handler
AS 'MODULE_PATHNAME', 'og_lake_fdw_handler' LANGUAGE C STRICT NOT FENCED;
CREATE FUNCTION og_lake.fdw_validator(text[], oid) RETURNS void
AS 'MODULE_PATHNAME', 'og_lake_fdw_validator' LANGUAGE C STRICT NOT FENCED;
CREATE FOREIGN DATA WRAPPER og_lake_fdw
HANDLER og_lake.fdw_handler VALIDATOR og_lake.fdw_validator;

CREATE FUNCTION og_lake.host_status() RETURNS text
AS 'MODULE_PATHNAME', 'og_lake_host_status' LANGUAGE C VOLATILE NOT FENCED;
COMMENT ON FUNCTION og_lake.host_status() IS
'Host prototype capabilities and session diagnostics; no lake data capability is enabled';

CREATE FUNCTION og_lake.create_catalog(name, name, text, text) RETURNS void
AS 'MODULE_PATHNAME', 'og_lake_unavailable' LANGUAGE C VOLATILE NOT FENCED;
CREATE FUNCTION og_lake.attach_catalog(name, name, text, boolean) RETURNS void
AS 'MODULE_PATHNAME', 'og_lake_unavailable' LANGUAGE C VOLATILE NOT FENCED;
CREATE FUNCTION og_lake.flush(regclass) RETURNS void
AS 'MODULE_PATHNAME', 'og_lake_unavailable' LANGUAGE C VOLATILE NOT FENCED;
REVOKE ALL ON FUNCTION og_lake.create_catalog(name, name, text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION og_lake.attach_catalog(name, name, text, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION og_lake.flush(regclass) FROM PUBLIC;

CREATE FUNCTION og_lake_internal.test_maintenance_claim() RETURNS boolean
AS 'MODULE_PATHNAME', 'og_lake_test_maintenance_claim' LANGUAGE C VOLATILE NOT FENCED;
CREATE FUNCTION og_lake_internal.test_maintenance_release() RETURNS boolean
AS 'MODULE_PATHNAME', 'og_lake_test_maintenance_release' LANGUAGE C VOLATILE NOT FENCED;
CREATE FUNCTION og_lake_internal.test_catalog_guard_acquire(oid, boolean) RETURNS boolean
AS 'MODULE_PATHNAME', 'og_lake_test_catalog_guard_acquire' LANGUAGE C STRICT VOLATILE NOT FENCED;
CREATE FUNCTION og_lake_internal.test_catalog_guard_release(oid) RETURNS boolean
AS 'MODULE_PATHNAME', 'og_lake_test_catalog_guard_release' LANGUAGE C STRICT VOLATILE NOT FENCED;
CREATE FUNCTION og_lake_internal.test_xact_state() RETURNS text
AS 'MODULE_PATHNAME', 'og_lake_test_xact_state' LANGUAGE C VOLATILE NOT FENCED;
REVOKE ALL ON FUNCTION og_lake_internal.test_maintenance_claim() FROM PUBLIC;
REVOKE ALL ON FUNCTION og_lake_internal.test_maintenance_release() FROM PUBLIC;
REVOKE ALL ON FUNCTION og_lake_internal.test_catalog_guard_acquire(oid, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION og_lake_internal.test_catalog_guard_release(oid) FROM PUBLIC;
REVOKE ALL ON FUNCTION og_lake_internal.test_xact_state() FROM PUBLIC;
