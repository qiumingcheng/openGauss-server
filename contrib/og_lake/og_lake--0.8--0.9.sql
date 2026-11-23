/* No durable catalog migration; enables standard local file version reads. */
\echo Use "ALTER EXTENSION og_lake UPDATE TO '0.9'" to load this file. \quit
COMMENT ON FOREIGN DATA WRAPPER og_lake_fdw IS
'Native DuckLake 1.1: inline DML and local Parquet versions with inline/Parquet position deletes; Puffin and file DML unavailable';
