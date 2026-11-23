/* Native DuckLake 1.1 catalog, inline DML and bounded local Parquet reads. */
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
'Native inline DML and bounded local Parquet reads; file delete and version adapters remain gated';

CREATE FUNCTION og_lake.create_catalog(name, name, text, text) RETURNS void
AS 'MODULE_PATHNAME', 'og_lake_create_catalog' LANGUAGE C VOLATILE NOT FENCED;
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

CREATE TABLE og_lake_internal.catalog_binding (
    server_oid oid PRIMARY KEY,
    metadata_schema_oid oid NOT NULL UNIQUE,
    catalog_id uuid NOT NULL UNIQUE,
    data_root text NOT NULL,
    credential_ref text,
    format_version text NOT NULL CHECK (format_version = '1.1'),
    read_only boolean NOT NULL,
    binding_generation bigint NOT NULL CHECK (binding_generation > 0)
);
CREATE TABLE og_lake_internal.table_binding (
    binding_id uuid PRIMARY KEY,
    server_oid oid NOT NULL REFERENCES og_lake_internal.catalog_binding(server_oid),
    table_id bigint NOT NULL CHECK (table_id >= 0),
    table_uuid uuid NOT NULL,
    schema_version bigint NOT NULL CHECK (schema_version >= 0),
    object_kind text NOT NULL CHECK (object_kind = 'relation'),
    state text NOT NULL CHECK (state = 'active'),
    read_only boolean NOT NULL,
    UNIQUE (server_oid, table_id)
);
CREATE TABLE og_lake_internal.relation_binding (
    relation_oid oid PRIMARY KEY,
    binding_id uuid NOT NULL UNIQUE REFERENCES og_lake_internal.table_binding(binding_id) ON DELETE CASCADE
);
CREATE TABLE og_lake_internal.column_binding (
    binding_id uuid NOT NULL REFERENCES og_lake_internal.table_binding(binding_id) ON DELETE CASCADE,
    column_id bigint NOT NULL CHECK (column_id >= 0),
    attnum integer NOT NULL CHECK (attnum > 0),
    type_oid oid NOT NULL,
    typmod integer NOT NULL,
    collation_oid oid NOT NULL,
    codec_version integer NOT NULL CHECK (codec_version = 1),
    PRIMARY KEY (binding_id, column_id),
    UNIQUE (binding_id, attnum)
);
REVOKE ALL ON ALL TABLES IN SCHEMA og_lake_internal FROM PUBLIC;
CREATE OR REPLACE FUNCTION og_lake.attach_catalog(name, name, text, boolean) RETURNS void
AS 'MODULE_PATHNAME', 'og_lake_attach_catalog' LANGUAGE C VOLATILE NOT FENCED;
REVOKE ALL ON FUNCTION og_lake.attach_catalog(name, name, text, boolean) FROM PUBLIC;

CREATE FUNCTION og_lake_internal.test_precommit_insert(regclass, integer) RETURNS void
AS 'MODULE_PATHNAME', 'og_lake_test_precommit_insert' LANGUAGE C STRICT VOLATILE NOT FENCED;
COMMENT ON FUNCTION og_lake_internal.test_precommit_insert(regclass, integer) IS
'Internal superuser-only probe: arm DEFAULT VALUES INSERT from a pre-commit callback; modes 0=first lake access,1=callback before lake,2=callback after lake';
REVOKE ALL ON FUNCTION og_lake_internal.test_precommit_insert(regclass, integer) FROM PUBLIC;

CREATE FUNCTION og_lake_internal.row_locator(anyelement) RETURNS bytea
AS 'MODULE_PATHNAME', 'og_lake_row_locator' LANGUAGE C STRICT STABLE NOT FENCED;
COMMENT ON FUNCTION og_lake_internal.row_locator(anyelement) IS
'Internal planner marker; execution requires a native lake scan slot and cannot reconstruct row identity from values';
REVOKE ALL ON FUNCTION og_lake_internal.row_locator(anyelement) FROM PUBLIC;

/* 0.5 adds host-local access policy; no DuckLake 1.1 metadata changes. */
CREATE TABLE og_lake_internal.metadata_relation (
    metadata_schema_oid oid NOT NULL,
    relation_oid oid PRIMARY KEY,
    relation_name name NOT NULL,
    UNIQUE (metadata_schema_oid, relation_name)
);
REVOKE ALL ON og_lake_internal.metadata_relation FROM PUBLIC;

CREATE TABLE og_lake_internal.access_policy (
    singleton boolean PRIMARY KEY,
    internal_owner_oid oid NOT NULL
);
REVOKE ALL ON og_lake_internal.access_policy FROM PUBLIC;
CREATE FUNCTION og_lake.configure_internal_owner(name) RETURNS void
AS 'MODULE_PATHNAME', 'og_lake_configure_internal_owner' LANGUAGE C STRICT VOLATILE NOT FENCED;
REVOKE ALL ON FUNCTION og_lake.configure_internal_owner(name) FROM PUBLIC;
COMMENT ON FUNCTION og_lake.configure_internal_owner(name) IS
'Administrator-only, one-time private metadata owner registration after explicit metadata ownership preparation; atomic with the caller transaction';

COMMENT ON FUNCTION og_lake.create_catalog(name, name, text, text) IS
'Administrator-only DuckLake 1.1 creation in the current transaction; requires configured private owner, unused absolute local root and NULL credential; creates no files';

COMMENT ON FOREIGN DATA WRAPPER og_lake_fdw IS
'Native DuckLake 1.1: administrator attach or ordinary-owner CREATE with lake_schema; same-transaction inline DML';
