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
