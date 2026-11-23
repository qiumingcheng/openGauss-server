/* SQL23 native RANGE parent read and bounded inline INSERT. */
DO $og_lake_native_parent_runtime$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.23','0.24','0.25','0.26','0.29','0.30','0.31','0.32','0.33','0.34','0.35', '0.36','0.37','0.38','0.39','0.40','0.41','0.42','0.43','0.44','0.45','0.46','0.47','0.48','0.49','0.50','0.51','0.52','0.53','0.54','0.55','0.56','0.57','0.58','0.59','0.60','0.61','0.62','0.63','0.64','0.65','0.66','0.67','0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.23 requires native lake runtime0.23,0.24 or0.25,0.26';
    END IF;
END
$og_lake_native_parent_runtime$;
ALTER TABLE og_lake_internal.table_binding DROP CONSTRAINT table_binding_object_kind_check;
ALTER TABLE og_lake_internal.table_binding ADD CONSTRAINT table_binding_object_kind_check
    CHECK (object_kind='relation' OR object_kind='partition');
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
ALTER TABLE og_lake_internal.row_lock_binding
    ADD COLUMN leaf_partition_oid oid NOT NULL DEFAULT '0'::oid,
    ADD COLUMN partition_no integer NOT NULL DEFAULT 0;
ALTER TABLE og_lake_internal.row_lock_binding
    ALTER COLUMN leaf_partition_oid DROP DEFAULT,
    ALTER COLUMN partition_no DROP DEFAULT;
ALTER TABLE og_lake_internal.row_lock_binding DROP CONSTRAINT row_lock_binding_relation_oid_key;
ALTER TABLE og_lake_internal.row_lock_binding
    ADD CONSTRAINT row_lock_binding_parent_leaf_key UNIQUE(relation_oid,leaf_partition_oid),
    ADD CONSTRAINT row_lock_binding_partition_no_nonnegative CHECK(partition_no>=0),
    ADD CONSTRAINT row_lock_binding_leaf_domain CHECK((leaf_partition_oid='0'::oid)=(partition_no=0));
-- Existing ordinary bindings become0/0; runtime ordinary INSERT writes0/0 explicitly.
-- Native head dependencies remain INTERNAL on the real pg_class parent.
-- Parent DROP is still rejected by durable marker guard; not new DROP support.

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
