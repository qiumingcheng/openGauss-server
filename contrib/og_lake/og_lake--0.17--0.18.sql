/* 0.18: preserve SQL0.17 ordinary RC and every existing head OID/key.
 * Constant defaults backfill old mapping rows, then are removed so runtime
 * private-table validation continues to forbid executable defaults. */
ALTER TABLE og_lake_internal.row_lock_binding ADD COLUMN reclaim_cursor bigint NOT NULL DEFAULT -1;
ALTER TABLE og_lake_internal.row_lock_binding ADD COLUMN reclaim_round_end bigint NOT NULL DEFAULT -1;
ALTER TABLE og_lake_internal.row_lock_binding ADD COLUMN reclaim_scanned bigint NOT NULL DEFAULT 0;
ALTER TABLE og_lake_internal.row_lock_binding ADD COLUMN reclaimed_rows bigint NOT NULL DEFAULT 0;
ALTER TABLE og_lake_internal.row_lock_binding ADD COLUMN reclaim_rounds bigint NOT NULL DEFAULT 0;
ALTER TABLE og_lake_internal.row_lock_binding ADD COLUMN last_reclaim_at timestamptz;
ALTER TABLE og_lake_internal.row_lock_binding ADD COLUMN last_vacuum_at timestamptz;
ALTER TABLE og_lake_internal.row_lock_binding ADD COLUMN live_rows_estimate bigint;
ALTER TABLE og_lake_internal.row_lock_binding ADD COLUMN dead_rows_estimate bigint;
ALTER TABLE og_lake_internal.row_lock_binding ADD COLUMN head_bytes bigint;
ALTER TABLE og_lake_internal.row_lock_binding ADD COLUMN soft_budget_exceeded boolean NOT NULL DEFAULT false;
ALTER TABLE og_lake_internal.row_lock_binding ADD COLUMN last_delete_xid bigint;
ALTER TABLE og_lake_internal.row_lock_binding ADD COLUMN oldest_xmin_observed bigint;
ALTER TABLE og_lake_internal.row_lock_binding ALTER COLUMN reclaim_cursor DROP DEFAULT;
ALTER TABLE og_lake_internal.row_lock_binding ALTER COLUMN reclaim_round_end DROP DEFAULT;
ALTER TABLE og_lake_internal.row_lock_binding ALTER COLUMN reclaim_scanned DROP DEFAULT;
ALTER TABLE og_lake_internal.row_lock_binding ALTER COLUMN reclaimed_rows DROP DEFAULT;
ALTER TABLE og_lake_internal.row_lock_binding ALTER COLUMN reclaim_rounds DROP DEFAULT;
ALTER TABLE og_lake_internal.row_lock_binding ALTER COLUMN soft_budget_exceeded DROP DEFAULT;
DO $og_lake_row_head_reclaim_validate$
BEGIN
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_row_head_reclaim_validate$;

/* One synchronous logical batch only; VACUUM remains a host maintenance job. */
CREATE FUNCTION og_lake.reclaim_row_heads(regclass, integer) RETURNS bigint
AS 'MODULE_PATHNAME', 'og_lake_reclaim_row_heads' LANGUAGE C STRICT VOLATILE NOT FENCED;
GRANT EXECUTE ON FUNCTION og_lake.reclaim_row_heads(regclass, integer) TO PUBLIC;
COMMENT ON FUNCTION og_lake.reclaim_row_heads(regclass, integer) IS
'Table owner/admin: delete at most 1..512 idle stable row heads in the caller transaction; no commit or vacuum; cannot mix with lake DML, CREATE or flush.';
