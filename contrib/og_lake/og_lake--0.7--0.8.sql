/* No durable metadata migration: adds a bounded native local-file reader. */
\echo Use "ALTER EXTENSION og_lake UPDATE TO '0.8'" to load this file. \quit
COMMENT ON FOREIGN DATA WRAPPER og_lake_fdw IS
'Native DuckLake 1.1: inline DML and bounded local immutable Parquet reads; file deletes, mappings and partial versions rejected';
