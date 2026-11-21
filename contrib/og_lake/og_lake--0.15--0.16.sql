/* Existing configured private owner remains the owner of the altered table. */
ALTER TABLE og_lake_internal.catalog_binding ADD COLUMN index_root text;
COMMENT ON COLUMN og_lake_internal.catalog_binding.index_root IS
'Administrator-provisioned derived-index root, separate from all DuckLake data roots.';
DO $og_lake_index_root_validate$
BEGIN
    PERFORM og_lake_internal._index_catalog_validate();
END
$og_lake_index_root_validate$;
COMMENT ON FUNCTION og_lake.set_option(name, text, text) IS
'Catalog administrator: positive data_inlining_row_limit or independent canonical index_root. No implicit commit or business snapshot.';
