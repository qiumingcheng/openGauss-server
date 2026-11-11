-- [P1-4] DiskANN INCLUDE: catalog upgrade script for existing clusters.
-- Registers pg_catalog.diskanncanreturn and points pg_am.amcanreturn at it
-- for the diskann AM (bootstrap row updated in pg_am.h + builtin_funcs.ini
-- for new installs; the .ini entry carries the reserved OID 8601 - repo-wide
-- conflict-checked, see DECISIONS). Rollback: diskann_include_rollback.sql
-- (downgrade requires dropping all v2/INCLUDE diskann indexes first,
-- design doc 8.3).
--
-- Notes (each failed approach was reproduced and archived):
-- * CREATE FUNCTION for INTERNAL language resolves the symbol via the
--   compiled-in builtin table (OID 8601) and expects that exact pg_proc
--   row to pre-exist -> "cache lookup failed".
-- * The hidden oid column is not insertable outside a real
--   inplace-upgrade session (isinplaceupgrade GUC is rejected).
-- * An INSERT..SELECT whose subquery READS pg_proc crashes the catalog
--   cache (assertion in CatalogCacheComputeTupleHashValueForProcAllArgs):
--   the insert's cache invalidation hits the still-running scan. The
--   guard is therefore a DELETE up front, and the INSERT subqueries only
--   touch pg_namespace/pg_language.
-- * fmgr resolves INTERNAL functions by prosrc NAME against the builtin
--   table, so the pg_proc row's own OID does not need to be 8601 on
--   upgraded clusters.

DELETE FROM pg_catalog.pg_proc
WHERE proname = 'diskanncanreturn'
  AND pronamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'pg_catalog');

INSERT INTO pg_catalog.pg_proc
    (proname, pronamespace, proowner, prolang, procost, prorows, provariadic,
     protransform, proisagg, proiswindow, prosecdef, proleakproof, proisstrict,
     proretset, provolatile, pronargs, pronargdefaults, prorettype, proargtypes,
     prosrc)
VALUES
    ('diskanncanreturn',
     (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'pg_catalog'),
     10,  /* BOOTSTRAP_SUPERUSERID */
     (SELECT oid FROM pg_catalog.pg_language WHERE lanname = 'internal'),
     1, 0, 0,
     '-', false, false, false, false, false,
     false, 'v', 1, 0, 'bool'::regtype, ARRAY['internal'::regtype]::oidvector,
     'diskanncanreturn');

UPDATE pg_catalog.pg_am
SET amcanreturn = 'pg_catalog.diskanncanreturn'::regproc
WHERE oid = 8303;  /* DISKANN_AM_OID */
