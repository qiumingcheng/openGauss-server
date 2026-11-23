/* Native DuckLake 1.1 catalog, inline DML and bounded local Parquet versions and position deletes. */
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
AS 'MODULE_PATHNAME', 'og_lake_flush' LANGUAGE C STRICT VOLATILE NOT FENCED;
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
    binding_generation bigint NOT NULL CHECK (binding_generation > 0),
    index_root text
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
'Native DuckLake 1.1: inline DML and local Parquet versions with inline/Parquet position deletes; file DML publishes inline; bounded local flush; Puffin and physical GC unavailable';

COMMENT ON FUNCTION og_lake.flush(regclass) IS
'Bounded local inline data/history and file-position deletion flush; original identities/snapshots preserved; physical GC deferred';

CREATE FUNCTION og_lake.set_option(regclass, text, text) RETURNS void
AS 'MODULE_PATHNAME', 'og_lake_set_table_option' LANGUAGE C VOLATILE NOT FENCED;
CREATE FUNCTION og_lake.set_option(name, text, text) RETURNS void
AS 'MODULE_PATHNAME', 'og_lake_set_catalog_option' LANGUAGE C VOLATILE NOT FENCED;
COMMENT ON FUNCTION og_lake.set_option(regclass, text, text) IS
'Table-owner positive data_inlining_row_limit; persistent standard metadata, caller transaction, no implicit snapshot or commit; no mixing with lake writes';
COMMENT ON FUNCTION og_lake.set_option(name, text, text) IS
'Administrator catalog-wide positive data_inlining_row_limit; existing table overrides win; caller transaction, no implicit snapshot or commit';

COMMENT ON FOREIGN DATA WRAPPER og_lake_fdw IS
'Native DuckLake 1.1: bounded inline/file DML and flush, positive inline configuration, transactional permanent-table RENAME; other ALTER and physical GC unavailable';

CREATE FUNCTION og_lake_internal._index_catalog_upgrade() RETURNS void
AS 'MODULE_PATHNAME', 'og_lake_index_catalog_upgrade' LANGUAGE C VOLATILE NOT FENCED;
CREATE FUNCTION og_lake_internal._index_catalog_validate() RETURNS void
AS 'MODULE_PATHNAME', 'og_lake_index_catalog_validate' LANGUAGE C VOLATILE NOT FENCED;
REVOKE ALL ON FUNCTION og_lake_internal._index_catalog_upgrade() FROM PUBLIC;
REVOKE ALL ON FUNCTION og_lake_internal._index_catalog_validate() FROM PUBLIC;
/* Included verbatim by the SQL0.15 installation and upgrade scripts.
 * Indexes are rebuildable host-owned derivatives, not ducklake_data_file rows.
 * No segment can become usable until an actual backend validates its objects.
 */
CREATE TABLE og_lake_internal.index_definition (
    index_uuid uuid PRIMARY KEY,
    binding_id uuid,
    index_name name NOT NULL,
    server_oid oid NOT NULL,
    table_id bigint NOT NULL CHECK (table_id >= 0),
    table_uuid uuid NOT NULL,
    column_id bigint NOT NULL CHECK (column_id >= 0),
    type_oid oid NOT NULL,
    collation_oid oid NOT NULL,
    definition_generation bigint NOT NULL CHECK (definition_generation > 0),
    definition_hash text NOT NULL,
    definition jsonb NOT NULL,
    registered_snapshot bigint NOT NULL CHECK (registered_snapshot >= 0),
    state text NOT NULL CHECK (state = 'ACTIVE'::text OR state = 'DROPPING'::text),
    inline_index_oid oid,
    CHECK (state = 'DROPPING'::text OR binding_id IS NOT NULL)
);
CREATE UNIQUE INDEX og_lake_index_definition_live_name
    ON og_lake_internal.index_definition(binding_id,index_name)
    WHERE state = 'ACTIVE'::text;

CREATE TABLE og_lake_internal.index_segment (
    segment_uuid uuid PRIMARY KEY,
    index_uuid uuid NOT NULL REFERENCES og_lake_internal.index_definition(index_uuid),
    definition_generation bigint NOT NULL CHECK (definition_generation > 0),
    build_token uuid NOT NULL,
    build_snapshot bigint NOT NULL CHECK (build_snapshot >= 0),
    source_manifest_hash text NOT NULL,
    algorithm_version integer NOT NULL CHECK (algorithm_version > 0),
    format_version integer NOT NULL CHECK (format_version > 0),
    artifact_root_uri text NOT NULL UNIQUE,
    manifest_hash text,
    state text NOT NULL CHECK (state = 'BUILDING'::text OR state = 'READY'::text OR
                              state = 'RETIRED'::text OR state = 'FAILED'::text),
    CHECK (state <> 'READY'::text OR manifest_hash IS NOT NULL),
    UNIQUE(segment_uuid,index_uuid,definition_generation)
);

CREATE TABLE og_lake_internal.index_coverage (
    segment_uuid uuid NOT NULL,
    source_file_id bigint NOT NULL CHECK (source_file_id >= 0),
    index_uuid uuid NOT NULL,
    definition_generation bigint NOT NULL CHECK (definition_generation > 0),
    source_checksum text NOT NULL,
    schema_mapping_hash text NOT NULL,
    physical_row_count bigint NOT NULL CHECK (physical_row_count >= 0),
    active boolean NOT NULL,
    source_file_bytes bigint NOT NULL CHECK (source_file_bytes >= 0),
    source_path text NOT NULL,
    PRIMARY KEY(segment_uuid,source_file_id),
    FOREIGN KEY(segment_uuid,index_uuid,definition_generation)
        REFERENCES og_lake_internal.index_segment(segment_uuid,index_uuid,definition_generation)
);
CREATE UNIQUE INDEX og_lake_index_coverage_one_live_source
    ON og_lake_internal.index_coverage(index_uuid,definition_generation,source_file_id)
    WHERE active;
REVOKE ALL ON og_lake_internal.index_definition,og_lake_internal.index_segment,
    og_lake_internal.index_coverage FROM PUBLIC;

/* 0.17: host-private stable heap row heads; not DuckLake data or snapshots.
 * No FK is added to table_binding: DROP explicitly removes this registry in
 * the same transaction, and an INTERNAL host dependency owns each head heap. */
CREATE TABLE og_lake_internal.row_lock_binding (
    binding_id uuid PRIMARY KEY,
    relation_oid oid NOT NULL UNIQUE,
    row_head_oid oid NOT NULL UNIQUE
) WITH (storage_type=astore,hasuids=false);
REVOKE ALL ON og_lake_internal.row_lock_binding FROM PUBLIC;
COMMENT ON TABLE og_lake_internal.row_lock_binding IS
'Exact canonical host binding to its stable ordinary heap row-head table. Heads survive flush and are never business delta rows.';

/* A configured0.13 private owner must own the new child tables before the
 * cross-domain FK is installed and the normal trust graph is checked. */
/* openGauss extension scripts execute utility statements, but skip SELECT.
 * DO is the standard utility entry for the protected C migration helper. */
DO $og_lake_index_upgrade$
BEGIN
    PERFORM og_lake_internal._index_catalog_upgrade();
END
$og_lake_index_upgrade$;
ALTER TABLE og_lake_internal.index_definition
    ADD CONSTRAINT og_lake_index_definition_binding_fk
    FOREIGN KEY(binding_id) REFERENCES og_lake_internal.table_binding(binding_id)
    ON DELETE SET NULL;
DO $og_lake_index_validate$
BEGIN
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_index_validate$;

CREATE FUNCTION og_lake.create_lake_index(regclass,name,jsonb) RETURNS uuid
AS 'MODULE_PATHNAME', 'og_lake_create_lake_index' LANGUAGE C VOLATILE NOT FENCED;
CREATE FUNCTION og_lake.drop_lake_index(uuid) RETURNS void
AS 'MODULE_PATHNAME', 'og_lake_drop_lake_index' LANGUAGE C VOLATILE NOT FENCED;
CREATE FUNCTION og_lake.lake_index_status(uuid) RETURNS text
AS 'MODULE_PATHNAME', 'og_lake_lake_index_status' LANGUAGE C VOLATILE NOT FENCED;
COMMENT ON FUNCTION og_lake.create_lake_index(regclass,name,jsonb) IS
'Register one scalar integer/bigint definition in the caller transaction; does not build objects or enable indexed queries';
COMMENT ON FUNCTION og_lake.drop_lake_index(uuid) IS
'Retire the definition and its coverage/segments transactionally; retains artifacts and metadata for protected later cleanup';
COMMENT ON FUNCTION og_lake.lake_index_status(uuid) IS
'Owner-only metadata status; usable=false until an actual index backend is implemented and verified';

CREATE FUNCTION og_lake.build_lake_index(uuid) RETURNS void
AS 'MODULE_PATHNAME', 'og_lake_build_lake_index' LANGUAGE C VOLATILE NOT FENCED;
COMMENT ON FUNCTION og_lake.build_lake_index(uuid) IS
'Build the host B-tree over the inline carrier transactionally; files remain scanned until a separate file backend is implemented';

COMMENT ON COLUMN og_lake_internal.catalog_binding.index_root IS
'Administrator-provisioned derived-index root; separate from all DuckLake data roots. NULL until explicitly configured.';
COMMENT ON FUNCTION og_lake.set_option(name, text, text) IS
'Catalog administrator: positive data_inlining_row_limit or independent canonical index_root. No implicit commit or business snapshot.';

CREATE FUNCTION og_lake_internal._row_lock_catalog_upgrade() RETURNS void
AS 'MODULE_PATHNAME', 'og_lake_row_lock_catalog_upgrade' LANGUAGE C VOLATILE NOT FENCED;
REVOKE ALL ON FUNCTION og_lake_internal._row_lock_catalog_upgrade() FROM PUBLIC;
DO $og_lake_row_lock_upgrade$
BEGIN
    PERFORM og_lake_internal._row_lock_catalog_upgrade();
END
$og_lake_row_lock_upgrade$;
