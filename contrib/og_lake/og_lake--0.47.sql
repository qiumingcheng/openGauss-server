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
    object_kind text NOT NULL CHECK (object_kind = 'relation' OR object_kind = 'partition'),
    state text NOT NULL CHECK (state = 'active'),
    read_only boolean NOT NULL,
    UNIQUE (server_oid, table_id),
    -- Host-private bounded flush round; all NULL means no active round.
    -- Actual payload births/partial_max remain standard DuckLake values.
    flush_birth_cutoff bigint,
    flush_carrier_oid oid,
    flush_source_schema bigint,
    flush_source_origin bigint
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

CREATE TABLE og_lake_internal.partition_binding (
    parent_relid oid NOT NULL,
    leaf_partition_oid oid NOT NULL,
    partition_no integer NOT NULL CHECK (partition_no>0),
    binding_id uuid NOT NULL UNIQUE REFERENCES og_lake_internal.table_binding(binding_id) ON DELETE CASCADE,
    certified_snapshot bigint NOT NULL CHECK (certified_snapshot>=0),
    source_hash text NOT NULL,
    PRIMARY KEY(parent_relid,leaf_partition_oid)
);
REVOKE ALL ON TABLE og_lake_internal.partition_binding FROM PUBLIC;
DO $og_lake_partition_owner$
DECLARE target oid;
BEGIN
    SELECT (SELECT internal_owner_oid FROM og_lake_internal.access_policy WHERE singleton) INTO target;
    IF target IS NOT NULL THEN
        EXECUTE pg_catalog.format('ALTER TABLE og_lake_internal.partition_binding OWNER TO %I',pg_catalog.pg_get_userbyid(target));
    END IF;
END
$og_lake_partition_owner$;

/* 0.18: host-private reclaimable heap row heads; not DuckLake data or snapshots.
 * No FK is added to table_binding: DROP explicitly removes this registry in
 * the same transaction, and an INTERNAL host dependency owns each head heap. */
CREATE TABLE og_lake_internal.row_lock_binding (
    binding_id uuid PRIMARY KEY,
    relation_oid oid NOT NULL,
    row_head_oid oid NOT NULL UNIQUE,
    reclaim_cursor bigint NOT NULL,
    reclaim_round_end bigint NOT NULL,
    reclaim_scanned bigint NOT NULL,
    reclaimed_rows bigint NOT NULL,
    reclaim_rounds bigint NOT NULL,
    last_reclaim_at timestamptz,
    last_vacuum_at timestamptz,
    live_rows_estimate bigint,
    dead_rows_estimate bigint,
    head_bytes bigint,
    soft_budget_exceeded boolean NOT NULL,
    last_delete_xid bigint,
    oldest_xmin_observed bigint,
    leaf_partition_oid oid NOT NULL,
    partition_no integer NOT NULL,
    CONSTRAINT row_lock_binding_parent_leaf_key UNIQUE(relation_oid,leaf_partition_oid),
    CONSTRAINT row_lock_binding_partition_no_nonnegative CHECK(partition_no>=0),
    CONSTRAINT row_lock_binding_leaf_domain CHECK((leaf_partition_oid='0'::oid)=(partition_no=0))
) WITH (storage_type=astore,hasuids=false);
REVOKE ALL ON og_lake_internal.row_lock_binding FROM PUBLIC;
COMMENT ON TABLE og_lake_internal.row_lock_binding IS
'Exact canonical host binding to its stable ordinary heap row-head table. Head heaps survive flush; idle head rows are reclaimed with native tuple locks. Estimates and bytes are observations, never a physical hard quota.';

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

/* One synchronous logical batch only; VACUUM remains a host maintenance job. */
CREATE FUNCTION og_lake.reclaim_row_heads(regclass, integer) RETURNS bigint
AS 'MODULE_PATHNAME', 'og_lake_reclaim_row_heads' LANGUAGE C STRICT VOLATILE NOT FENCED;
GRANT EXECUTE ON FUNCTION og_lake.reclaim_row_heads(regclass, integer) TO PUBLIC;
COMMENT ON FUNCTION og_lake.reclaim_row_heads(regclass, integer) IS
'Table owner/admin: delete at most 1..512 idle stable row heads in the caller transaction; no commit or vacuum; cannot mix with lake DML, CREATE or flush.';

/* SQL0.19: host-only scheduling counters; DuckLake standard tables unchanged.
 * No defaults, user expressions, triggers or FK callbacks. Counters are
 * explicitly initialized and updated in the successful caller transaction. */
CREATE TABLE og_lake_internal.analyze_state (
    binding_id uuid PRIMARY KEY,
    relation_oid oid NOT NULL,
    table_uuid uuid NOT NULL,
    committed_changes bigint NOT NULL,
    analyzed_changes bigint NOT NULL,
    analyzed_snapshot bigint,
    last_success timestamptz,
    last_attempt timestamptz,
    retry_after timestamptz,
    last_error integer
);
REVOKE ALL ON TABLE og_lake_internal.analyze_state FROM PUBLIC;
/* Upgrade may already have a registered private owner. A fresh installation
 * leaves ownership to the usual configure_internal_owner handover. */
DO $og_lake_analyze_owner$
DECLARE target oid;
BEGIN
    /* The scalar subquery yields NULL for a fresh, unconfigured installation.
     * A bare SELECT INTO can raise NO_DATA_FOUND in openGauss. */
    SELECT (SELECT internal_owner_oid FROM og_lake_internal.access_policy WHERE singleton) INTO target;
    IF target IS NOT NULL THEN
        EXECUTE pg_catalog.format('ALTER TABLE og_lake_internal.analyze_state OWNER TO %I',pg_catalog.pg_get_userbyid(target));
    END IF;
END
$og_lake_analyze_owner$;

/* SQL0.21 adds transactional file rewrite without catalog shape changes. */
DO $og_lake_rewrite_runtime$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.21','0.22','0.23','0.24','0.25','0.26','0.29','0.30','0.31','0.32','0.33','0.34','0.35', '0.36','0.37','0.38','0.39','0.40','0.41','0.42','0.43','0.44','0.45','0.46','0.47','0.48','0.49','0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.21 requires native lake runtime0.21,0.22,0.23,0.24 or0.25,0.26';
    END IF;
END
$og_lake_rewrite_runtime$;
CREATE FUNCTION og_lake.rewrite_data_files(regclass,bigint[]) RETURNS void
AS 'MODULE_PATHNAME','og_lake_rewrite_data_files' LANGUAGE C VOLATILE NOT FENCED;
COMMENT ON FUNCTION og_lake.rewrite_data_files(regclass,bigint[]) IS
'Table owner/admin: rewrite one or two complete current files in this transaction; preserve stable row/version identity, retain historical source intervals, abort on concurrent selected-file changes. No physical GC or transaction switch.';

/* SQL0.22 adds the first standard identity file-partition slice. */
DO $og_lake_partition_runtime$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.22','0.23','0.24','0.25','0.26','0.29','0.30','0.31','0.32','0.33','0.34','0.35', '0.36','0.37','0.38','0.39','0.40','0.41','0.42','0.43','0.44','0.45','0.46','0.47','0.48','0.49','0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.22 requires native lake runtime0.22,0.23,0.24 or0.25,0.26';
    END IF;
END
$og_lake_partition_runtime$;
CREATE FUNCTION og_lake.set_partition(scope regclass, partition_by text[])
RETURNS void AS 'MODULE_PATHNAME','og_lake_set_partition'
LANGUAGE C VOLATILE NOT FENCED;
CREATE FUNCTION og_lake.reset_partition(scope regclass)
RETURNS void AS 'MODULE_PATHNAME','og_lake_reset_partition'
LANGUAGE C VOLATILE NOT FENCED;
CREATE FUNCTION og_lake.get_partition(scope regclass,
    OUT partition_key_index bigint, OUT column_name text, OUT transform text)
RETURNS SETOF record AS 'MODULE_PATHNAME','og_lake_get_partition'
LANGUAGE C STABLE NOT FENCED;
-- All endpoints enforce target ownership/current trusted catalog internally.

/* SQL23 native RANGE parent read and bounded inline INSERT. */
DO $og_lake_native_parent_runtime$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.23','0.24','0.25','0.26','0.29','0.30','0.31','0.32','0.33','0.34','0.35', '0.36','0.37','0.38','0.39','0.40','0.41','0.42','0.43','0.44','0.45','0.46','0.47','0.48','0.49','0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.23 requires native lake runtime0.23,0.24 or0.25,0.26';
    END IF;
END
$og_lake_native_parent_runtime$;
CREATE FUNCTION og_lake.bind_native_partition_readonly(parent regclass, server name,
    native_leaves oid[], lake_tables bigint[])
RETURNS void AS 'MODULE_PATHNAME','og_lake_bind_native_partition_readonly'
LANGUAGE C VOLATILE NOT FENCED;
COMMENT ON FUNCTION og_lake.bind_native_partition_readonly(regclass,name,oid[],bigint[]) IS
'Isolated development slice: administrator+parent owner; two real empty native RANGE leaves; complete bounded readonly certification at one exact lake head; no DML/ALTER/recertification; no autonomous commit.';

CREATE FUNCTION og_lake.enable_native_partition_insert(parent regclass)
RETURNS void AS 'MODULE_PATHNAME','og_lake_enable_native_partition_insert'
LANGUAGE C VOLATILE NOT FENCED;
COMMENT ON FUNCTION og_lake.enable_native_partition_insert(regclass) IS
'Administrator+parent owner: atomically enable bounded inline parent INSERT after complete readonly certification; no snapshot or independent commit; commit mode change before business DML. No partition UPDATE/DELETE/COPY or direct-file support.';

DO $og_lake_native_parent_validate$
BEGIN
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_native_parent_validate$;

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


/* SQL26 adds bounded typed native-parent synchronization-head maintenance.
 * This version changes no DuckLake format, binding shape or allocator state. */
DO $og_lake_native_parent_runtime_0_26$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.26','0.29','0.30','0.31','0.32','0.33','0.34','0.35', '0.36','0.37','0.38','0.39','0.40','0.41','0.42','0.43','0.44','0.45','0.46','0.47','0.48','0.49','0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.26 requires its reviewed native lake runtime';
    END IF;
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_native_parent_runtime_0_26$;
COMMENT ON FUNCTION og_lake.reclaim_row_heads(regclass,integer) IS
'Owner-only bounded synchronization-head reclamation in the caller transaction. SQL0.26 also supports one typed native-parent leaf per call; at most512 keys, busy heads skipped. No business-row deletion, implicit commit, VACUUM or lake-file GC.';

/* Ordinary lake file partitions only. Native partition semantics retain SQL26.
 * 0.27/0.28 were private uninstalled candidates; this upgrade is directly26->29.
 * Standard DuckLake directory tables and the host binding shape are unchanged. */
DO $og_lake_file_partition_runtime_0_29$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.29','0.30','0.31','0.32','0.33','0.34','0.35', '0.36','0.37','0.38','0.39','0.40','0.41','0.42','0.43','0.44','0.45','0.46','0.47','0.48','0.49','0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.29 requires its reviewed native lake runtime';
    END IF;
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_file_partition_runtime_0_29$;
COMMENT ON FUNCTION og_lake.set_partition(regclass,text[]) IS
'Owner-only ordered top-level identity keys (bool, int2, int4, int8, text). SET/RESET require no registered inline carrier. Files record each ordered key in standard DuckLake metadata. No native database partition change.';

/* Ordinary DuckLake bucket file partitions. Existing parent bits remain SQL26.
 * This is the direct29->30 path; uninstalled private27/28 are not inherited.
 * Standard metadata tables, SDK ABI and the existing two-argument API are unchanged. */
DO $og_lake_bucket_runtime_0_30$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.30','0.31','0.32','0.33','0.34','0.35', '0.36','0.37','0.38','0.39','0.40','0.41','0.42','0.43','0.44','0.45','0.46','0.47','0.48','0.49','0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.30 requires its reviewed native lake runtime';
    END IF;
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_bucket_runtime_0_30$;
CREATE FUNCTION og_lake.set_partition(scope regclass, partition_by text[], transforms text[])
RETURNS void AS 'MODULE_PATHNAME','og_lake_set_partition'
LANGUAGE C VOLATILE CALLED ON NULL INPUT NOT FENCED;
COMMENT ON FUNCTION og_lake.set_partition(regclass,text[],text[]) IS
'Owner-only ordered partition columns and corresponding identity/bucket(N) transforms. N is1..2147483647; bool/int2/int4/int8/UTF8 text only. SET/RESET require no registered inline carrier. Standard DuckLake file metadata; original two-argument identity API remains unchanged.';

/* Finite temporal business fields use standard DuckLake format1.1.
 * Existing metadata tables, bucket APIs and exact SQL26 parent bits are unchanged.
 * This script does not certify arbitrary external VARCHAR producer sessions. */
DO $og_lake_temporal_runtime_0_31$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.31','0.32','0.33','0.34','0.35', '0.36','0.37','0.38','0.39','0.40','0.41','0.42','0.43','0.44','0.45','0.46','0.47','0.48','0.49','0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.31 requires its reviewed native lake runtime';
    END IF;
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_temporal_runtime_0_31$;

/* Standard DuckLake format1.1 calendar/epoch file partition rules.
 * SQL31 temporal fields and exact SQL26 native-parent bits are preserved.
 * No new SQL callable, private format, provenance marker or transaction commit. */
DO $og_lake_time_partition_runtime_0_32$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.32','0.33','0.34','0.35', '0.36','0.37','0.38','0.39','0.40','0.41','0.42','0.43','0.44','0.45','0.46','0.47','0.48','0.49','0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.32 requires its reviewed native lake runtime';
    END IF;
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_time_partition_runtime_0_32$;

/* SQL33 reserves time-file equality/NULL proof admission. SQL32 data
 * transformations and SQL26 native-parent rights are unchanged. Query proof
 * admission is independently controlled by its qualified installed-SQL gate. */
DO $og_lake_time_prune_runtime_0_33$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.33','0.34','0.35', '0.36','0.37','0.38','0.39','0.40','0.41','0.42','0.43','0.44','0.45','0.46','0.47','0.48','0.49','0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.33 requires its reviewed native lake runtime';
    END IF;
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_time_prune_runtime_0_33$;

/* SQL34 changes only ordinary native CREATE source evolution admission.
 * Existing descriptors/row identities are read from standard SQL metadata;
 * no new persistent table, locator ABI or public maintenance function exists. */
DO $og_lake_evolution_runtime_0_34$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.34','0.35', '0.36','0.37','0.38','0.39','0.40','0.41','0.42','0.43','0.44','0.45','0.46','0.47','0.48','0.49','0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.34 requires its reviewed native lake runtime';
    END IF;
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_evolution_runtime_0_34$;

/* SQL35 admits ordinary native CREATE first-modify refresh, partitioned
 * direct INSERT and monotone epoch range proofs. No persisted schema change,
 * new SDK ABI, native-parent right or public function is introduced. */
DO $og_lake_core_runtime_0_35$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.35', '0.36','0.37','0.38','0.39','0.40','0.41','0.42','0.43','0.44','0.45','0.46','0.47','0.48','0.49','0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.35 requires its reviewed native lake runtime';
    END IF;
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_core_runtime_0_35$;

/* SQL36 adds logical DROP for ordinary native CREATE bindings. ATTACH and
 * retained SQL keep host-only detach. Eight standard metadata lifetimes end in
 * the host commit; no carrier/file deletion or new persistent schema. */
DO $og_lake_drop_runtime_0_36$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.36','0.37','0.38','0.39','0.40','0.41','0.42','0.43','0.44','0.45','0.46','0.47','0.48','0.49','0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.36 requires its reviewed native lake runtime';
    END IF;
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_drop_runtime_0_36$;

/* SQL37 adds one nullable flat scalar column to an ordinary protected CREATE
 * table, preserving field IDs, historical physical schemas and initial NULLs.
 * No default expression, DROP COLUMN, SDK ABI or native-parent expansion. */
DO $og_lake_add_runtime_0_37$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.37','0.38','0.39','0.40','0.41','0.42','0.43','0.44','0.45','0.46','0.47','0.48','0.49','0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.37 requires its reviewed native lake runtime';
    END IF;
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_add_runtime_0_37$;

/* SQL38 permits bounded transaction-private tuple payload spill for ordinary
 * native lake DML. Stable IDs/publication, SDK and native-parent rights stay.
 * Old installed SQL retains its 64 MiB resident write-set refusal. */
DO $og_lake_spill_runtime_0_38$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.38','0.39','0.40','0.41','0.42','0.43','0.44','0.45','0.46','0.47','0.48','0.49','0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.38 requires its reviewed native lake runtime';
    END IF;
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_spill_runtime_0_38$;

/* SQL39 adds one nullable bool/int2/int4/int8/text column with a literal
 * default on ordinary protected CREATE. Initial and current values remain
 * distinct standard fields; old installed SQL keeps its NULL-only ADD gate. */
DO $og_lake_literal_default_runtime_0_39$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.39','0.40','0.41','0.42','0.43','0.44','0.45','0.46','0.47','0.48','0.49','0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.39 requires its reviewed native lake runtime';
    END IF;
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_literal_default_runtime_0_39$;
/* SQL40 adds ordinary protected CREATE and single-column SET/DROP NOT NULL.
 * Existing physical versions stay nullable. Host planning and actual execution
 * both verify current host constraints against their own statement snapshot. */
DO $og_lake_not_null_runtime_0_40$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.40','0.41','0.42','0.43','0.44','0.45','0.46','0.47','0.48','0.49','0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.40 requires its reviewed native lake runtime';
    END IF;
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_not_null_runtime_0_40$;
/* SQL41 changes only the current literal default of a committed ordinary
 * protected CREATE column. Stable ID and immutable initial value are retained. */
DO $og_lake_alter_default_runtime_0_41$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.41','0.42','0.43','0.44','0.45','0.46','0.47','0.48','0.49','0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.41 requires its reviewed native lake runtime';
    END IF;
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_alter_default_runtime_0_41$;
/* Restricted top-level scalar DROP preserves stable IDs and physical history. */
DO $og_lake_drop_column_runtime_0_42$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.42','0.43','0.44','0.45','0.46','0.47','0.48','0.49','0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.42 requires its reviewed native lake runtime';
    END IF;
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_drop_column_runtime_0_42$;

/* Raw BYTEA business values use the existing SDK binary ABI and standard BLOB. */
DO $og_lake_binary_runtime_0_43$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.43','0.44','0.45','0.46','0.47','0.48','0.49','0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.43 requires its reviewed native lake runtime';
    END IF;
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_binary_runtime_0_43$;

/* SQL44 admits logical BYTEA NOT NULL only; existing BLOB physical fields,
 * historical NULLs, ABI, column IDs and literal-default rules are unchanged. */
DO $og_lake_binary_not_null_runtime_0_44$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.44','0.45','0.46','0.47','0.48','0.49','0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'binary_not_null_constraints')::boolean,false) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.44 requires its reviewed BYTEA NOT NULL native lake runtime';
    END IF;
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_binary_not_null_runtime_0_44$;

/* SQL45 adds bounded standard BLOB literal defaults in Native only.
 * The 1 MiB limit is ENCODED metadata text (fully escaped payload <=256 KiB).
 * SDK ABI/capabilities, Parquet format, initial history and ADD NOT NULL stay unchanged. */
DO $og_lake_binary_literal_default_runtime_0_45$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.45','0.46','0.47','0.48','0.49','0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'binary_literal_defaults')::boolean,false) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.45 requires its reviewed BYTEA literal-default native lake runtime';
    END IF;
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_binary_literal_default_runtime_0_45$;

/* SQL46 adds exact same-type INT2 scalar indexes only. Existing signed
 * INT64 page/dictionary layout, hashes, SDK ABI and source visibility stay fixed. */
DO $og_lake_int2_scalar_runtime_0_46$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.46','0.47','0.48','0.49','0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'int2_scalar_indexes')::boolean,false) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.46 requires its reviewed INT2 scalar-index native lake runtime';
    END IF;
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_int2_scalar_runtime_0_46$;

/* SQL47 adds only plain full-scan INT2 native row Stream execution.
 * Original slots, snapshots, immutable whole-file tasks and join cleanup stay.
 * INT2 index/ANN SMP, Vec, PQ, temporal and partition expansion remain closed. */
DO $og_lake_int2_row_smp_runtime_0_47$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.47','0.48','0.49','0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'int2_row_smp')::boolean,false) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.47 requires its reviewed plain INT2 row-SMP native lake runtime';
    END IF;
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_int2_row_smp_runtime_0_47$;
