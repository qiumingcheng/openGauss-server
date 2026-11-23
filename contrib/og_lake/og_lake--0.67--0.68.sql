/* SQL68 (PGL-5): Single-node active reader snapshot pin registry
 * (BOUND_SNAPSHOT, STATEMENT_VIEW, TXN_READ) integrated with snapshot expiry,
 * thread-pool/backend death reap, and active_snapshot_pins() observability. */
CREATE FUNCTION og_lake.active_snapshot_pins()
RETURNS TABLE(
    database_oid oid,
    server_oid oid,
    backend_pid bigint,
    session_id bigint,
    snapshot_id bigint,
    pin_kind text,
    is_thread_pool boolean
)
AS 'MODULE_PATHNAME', 'og_lake_active_snapshot_pins'
LANGUAGE C VOLATILE NOT FENCED;
REVOKE ALL ON FUNCTION og_lake.active_snapshot_pins() FROM PUBLIC;

DO $og_lake_snapshot_pin_0_68$
BEGIN
    IF COALESCE((og_lake.host_status()::jsonb ->> 'module_version') NOT IN ('0.68','0.69','0.70','0.71','0.72','0.73','0.74'),true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'snapshot_pin')::boolean,false)
       OR COALESCE((og_lake.host_status()::jsonb ->> 'snapshot_pin_sql_version') <> '0.68',true)
       OR NOT COALESCE((og_lake.host_status()::jsonb ->> 'pgl5_snapshot_pin')::boolean,false) THEN
        RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='SQL0.68 requires its reviewed snapshot pin runtime';
    END IF;
END
$og_lake_snapshot_pin_0_68$;
