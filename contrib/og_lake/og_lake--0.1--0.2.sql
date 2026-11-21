-- Native host 0.1 -> read-only inline 0.2; no lake data is modified.
\echo Use "ALTER EXTENSION og_lake UPDATE TO '0.2'" to load this file. \quit

CREATE TABLE og_lake_internal.catalog_binding (
    server_oid oid PRIMARY KEY,
    metadata_schema_oid oid NOT NULL UNIQUE,
    catalog_id uuid NOT NULL UNIQUE,
    data_root text NOT NULL,
    credential_ref text,
    format_version text NOT NULL CHECK (format_version = '1.1'),
    read_only boolean NOT NULL CHECK (read_only),
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
    read_only boolean NOT NULL CHECK (read_only),
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
