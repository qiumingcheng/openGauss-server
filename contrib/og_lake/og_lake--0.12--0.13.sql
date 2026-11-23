/* RENAME uses existing standard table versioning, without a format migration. */
\echo Use "ALTER EXTENSION og_lake UPDATE TO '0.13'" to load this file. \quit

COMMENT ON FOREIGN DATA WRAPPER og_lake_fdw IS
'Native DuckLake 1.1: bounded inline/file DML and flush, positive inline configuration, transactional permanent-table RENAME; other ALTER and physical GC unavailable';
