/* Enables file-origin DML using standard inline versions and position deletes. */
\echo Use "ALTER EXTENSION og_lake UPDATE TO '0.10'" to load this file. \quit
COMMENT ON FOREIGN DATA WRAPPER og_lake_fdw IS
'Native DuckLake 1.1: inline/file DML through standard inline carriers; Puffin and flush unavailable';
