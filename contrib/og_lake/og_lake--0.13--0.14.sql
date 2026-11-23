/* Derived index definition registry; standard DuckLake1.1 metadata unchanged. */
\echo Use "ALTER EXTENSION og_lake UPDATE TO '0.14'" to load this file. \quit

CREATE FUNCTION og_lake_internal._index_catalog_upgrade() RETURNS void
AS 'MODULE_PATHNAME', 'og_lake_index_catalog_upgrade' LANGUAGE C VOLATILE NOT FENCED;
CREATE FUNCTION og_lake_internal._index_catalog_validate() RETURNS void
AS 'MODULE_PATHNAME', 'og_lake_index_catalog_validate' LANGUAGE C VOLATILE NOT FENCED;
REVOKE ALL ON FUNCTION og_lake_internal._index_catalog_upgrade() FROM PUBLIC;
REVOKE ALL ON FUNCTION og_lake_internal._index_catalog_validate() FROM PUBLIC;
/* Included verbatim by the SQL0.14 installation and upgrade scripts.
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
