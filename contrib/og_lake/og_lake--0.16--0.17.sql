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
CREATE FUNCTION og_lake_internal._row_lock_catalog_upgrade() RETURNS void
AS 'MODULE_PATHNAME', 'og_lake_row_lock_catalog_upgrade' LANGUAGE C VOLATILE NOT FENCED;
REVOKE ALL ON FUNCTION og_lake_internal._row_lock_catalog_upgrade() FROM PUBLIC;
DO $og_lake_row_lock_upgrade$
BEGIN
    PERFORM og_lake_internal._row_lock_catalog_upgrade();
END
$og_lake_row_lock_upgrade$;
