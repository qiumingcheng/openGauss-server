/* No durable metadata migration: adds transactional business CREATE. */
\echo Use "ALTER EXTENSION og_lake UPDATE TO '0.7'" to load this file. \quit
COMMENT ON FOREIGN DATA WRAPPER og_lake_fdw IS
'Native DuckLake 1.1: administrator attach or ordinary-owner CREATE with lake_schema; same-transaction inline DML';
