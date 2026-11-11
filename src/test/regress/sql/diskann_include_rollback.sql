-- [P1-4] Rollback for diskann_include_upgrade.sql.
-- PRECONDITION: no format-v2 (INCLUDE) diskann indexes may exist when
-- downgrading (old binaries cannot parse v2 metapages). Check with:
--   SELECT indexrelid::regclass FROM pg_index i
--   WHERE (SELECT relam FROM pg_class WHERE oid = i.indexrelid) = 8303
--     AND indnatts > indnkeyatts;

UPDATE pg_catalog.pg_am
SET amcanreturn = 0
WHERE oid = 8303;  /* DISKANN_AM_OID */

DELETE FROM pg_catalog.pg_proc WHERE proname = 'diskanncanreturn' AND pronamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'pg_catalog');
