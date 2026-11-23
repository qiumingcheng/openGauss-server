/* SQL0.19 automatic logical ANALYZE; old snapshots/identities unchanged. */

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
