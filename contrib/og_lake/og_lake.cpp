/*
 * Native lake inline prototype. SDK/file access remains disabled.
 * Static registration follows the in-tree file_fdw integration.
 */
#include "postgres.h"
#include "lake_native_partition.h"
#include "access/og_lake.h"
#include "access/reloptions.h"
#include "catalog/pg_foreign_table.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "lake_txn_private.h"
#include "lake_index_private.h"

extern "C" Datum og_lake_fdw_handler(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_fdw_validator(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_file_fdw_handler(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_file_fdw_validator(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_describe_external_server_file(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_describe_external_path_file(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_external_file_info(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_flush(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_rewrite_data_files(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_merge_adjacent_files(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_expire_snapshots(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_current_snapshot(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_snapshots(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_last_committed_snapshot(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_time_travel_version(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_time_travel_timestamp(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_table_insertions_version(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_table_insertions_timestamp(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_table_deletions_version(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_table_deletions_timestamp(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_table_changes_version(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_table_changes_timestamp(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_bind_snapshot(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_table_info(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_list_files(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_options(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_catalog_status_rows(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_table_status_rows(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_index_status_rows(PG_FUNCTION_ARGS);

extern "C" Datum og_lake_set_partition(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_reset_partition(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_get_partition(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_active_snapshot_pins(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_cleanup_old_files(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_delete_orphaned_files(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_cleanup_index_segments(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_backup_manifest(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_verify_catalog_integrity(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_retention_status(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_set_table_option(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_set_catalog_option(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_host_status(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_unavailable(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_test_maintenance_claim(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_test_maintenance_release(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_test_catalog_guard_acquire(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_test_catalog_guard_release(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_test_xact_state(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_test_precommit_insert(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_row_locator(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_test_locator(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_test_analyze_pause(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_test_pq_probe(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_test_smp_probe(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_test_rowhead_probe(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_test_index_maintenance_gate(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_test_autoanalyze_gate(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_row_lock_catalog_upgrade(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_reclaim_row_heads(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(og_lake_fdw_handler);
PG_FUNCTION_INFO_V1(og_lake_fdw_validator);
PG_FUNCTION_INFO_V1(og_lake_host_status);
PG_FUNCTION_INFO_V1(og_lake_unavailable);
PG_FUNCTION_INFO_V1(og_lake_test_maintenance_claim);
PG_FUNCTION_INFO_V1(og_lake_test_maintenance_release);
PG_FUNCTION_INFO_V1(og_lake_test_catalog_guard_acquire);
PG_FUNCTION_INFO_V1(og_lake_test_catalog_guard_release);
PG_FUNCTION_INFO_V1(og_lake_test_xact_state);

static void LakeUnavailable(void)
{
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("the requested native lake operation is not implemented"),
        errdetail("This prototype supports reads and bounded DML of existing inline tables; "
                  "The requested maintenance or storage capability is unavailable.")));
}

static void LakeDiagnosticPermission(void)
{
    if (!superuser())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("native lake host diagnostics require superuser privileges")));
}

Datum og_lake_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine* routine = makeNode(FdwRoutine);
    LakeInstallInlineCallbacks(routine);
    LakeInstallModifyCallbacks(routine);
    PG_RETURN_POINTER(routine);
}

Datum og_lake_fdw_validator(PG_FUNCTION_ARGS)
{
    List* options = untransformRelOptions(PG_GETARG_DATUM(0));
    Oid catalog = PG_GETARG_OID(1);
    LakeNativeValidateOptions(options, catalog);
    PG_RETURN_VOID();
}

Datum og_lake_host_status(PG_FUNCTION_ARGS)
{
    PG_RETURN_TEXT_P(cstring_to_text(LakeHostStateJson()));
}

Datum og_lake_unavailable(PG_FUNCTION_ARGS)
{
    LakeUnavailable();
    PG_RETURN_VOID();
}

Datum og_lake_test_maintenance_claim(PG_FUNCTION_ARGS)
{
    LakeDiagnosticPermission();
    PG_RETURN_BOOL(LakeTryMaintenanceClaim());
}

Datum og_lake_test_maintenance_release(PG_FUNCTION_ARGS)
{
    LakeDiagnosticPermission();
    PG_RETURN_BOOL(LakeReleaseMaintenanceClaim());
}

Datum og_lake_test_catalog_guard_acquire(PG_FUNCTION_ARGS)
{
    LakeDiagnosticPermission();
    PG_RETURN_BOOL(LakeTryCatalogGuard(PG_GETARG_OID(0), PG_GETARG_BOOL(1)));
}

Datum og_lake_test_catalog_guard_release(PG_FUNCTION_ARGS)
{
    LakeDiagnosticPermission();
    PG_RETURN_BOOL(LakeReleaseCatalogGuard(PG_GETARG_OID(0)));
}

Datum og_lake_test_xact_state(PG_FUNCTION_ARGS)
{
    LakeDiagnosticPermission();
    PG_RETURN_TEXT_P(cstring_to_text(LakeHostStateJson()));
}

struct LakeBuiltinEntry {
    const char* name;
    PGFunction function;
};

PGFunction LakeLookupBuiltinFunction(const char* name)
{
    static const LakeBuiltinEntry functions[] = {
        {"og_lake_fdw_handler", og_lake_fdw_handler},
        {"og_lake_file_fdw_handler", og_lake_file_fdw_handler},
        {"og_lake_file_fdw_validator", og_lake_file_fdw_validator},
        {"og_lake_describe_external_server_file", og_lake_describe_external_server_file},
        {"og_lake_describe_external_path_file", og_lake_describe_external_path_file},
        {"og_lake_external_file_info", og_lake_external_file_info},
        {"og_lake_bind_native_partition_readonly", og_lake_bind_native_partition_readonly},
        {"og_lake_enable_native_partition_insert", og_lake_enable_native_partition_insert},
        {"og_lake_native_parent_statistics_upgrade", og_lake_native_parent_statistics_upgrade},
        {"og_lake_fdw_validator", og_lake_fdw_validator},
        {"og_lake_host_status", og_lake_host_status},
        {"og_lake_attach_catalog", og_lake_attach_catalog},
        {"og_lake_create_catalog", og_lake_create_catalog},
        {"og_lake_configure_internal_owner", og_lake_configure_internal_owner},
        {"og_lake_unavailable", og_lake_unavailable},
        {"og_lake_flush", og_lake_flush},
        {"og_lake_rewrite_data_files", og_lake_rewrite_data_files},
        {"og_lake_merge_adjacent_files", og_lake_merge_adjacent_files},
        {"og_lake_expire_snapshots", og_lake_expire_snapshots},
        {"og_lake_current_snapshot", og_lake_current_snapshot},
        {"og_lake_snapshots", og_lake_snapshots},
        {"og_lake_last_committed_snapshot", og_lake_last_committed_snapshot},
        {"og_lake_time_travel_version", og_lake_time_travel_version},
        {"og_lake_time_travel_timestamp", og_lake_time_travel_timestamp},
        {"og_lake_table_insertions_version", og_lake_table_insertions_version},
        {"og_lake_table_insertions_timestamp", og_lake_table_insertions_timestamp},
        {"og_lake_table_deletions_version", og_lake_table_deletions_version},
        {"og_lake_table_deletions_timestamp", og_lake_table_deletions_timestamp},
        {"og_lake_table_changes_version", og_lake_table_changes_version},
        {"og_lake_table_changes_timestamp", og_lake_table_changes_timestamp},
        {"og_lake_bind_snapshot", og_lake_bind_snapshot},
        {"og_lake_table_info", og_lake_table_info},
        {"og_lake_list_files", og_lake_list_files},
        {"og_lake_options", og_lake_options},
        {"og_lake_catalog_status_rows", og_lake_catalog_status_rows},
        {"og_lake_table_status_rows", og_lake_table_status_rows},
        {"og_lake_index_status_rows", og_lake_index_status_rows},

        {"og_lake_set_partition", og_lake_set_partition},
        {"og_lake_reset_partition", og_lake_reset_partition},
        {"og_lake_get_partition", og_lake_get_partition},
        {"og_lake_active_snapshot_pins", og_lake_active_snapshot_pins},
        {"og_lake_cleanup_old_files", og_lake_cleanup_old_files},
        {"og_lake_delete_orphaned_files", og_lake_delete_orphaned_files},
        {"og_lake_cleanup_index_segments", og_lake_cleanup_index_segments},
        {"og_lake_backup_manifest", og_lake_backup_manifest},
        {"og_lake_verify_catalog_integrity", og_lake_verify_catalog_integrity},
        {"og_lake_retention_status", og_lake_retention_status},
        {"og_lake_set_table_option", og_lake_set_table_option},
        {"og_lake_set_catalog_option", og_lake_set_catalog_option},
        {"og_lake_create_lake_index", og_lake_create_lake_index},
        {"og_lake_build_lake_index", og_lake_build_lake_index},
        {"og_lake_rebuild_lake_index", og_lake_rebuild_lake_index},
        {"og_lake_drop_lake_index", og_lake_drop_lake_index},
        {"og_lake_lake_index_status", og_lake_lake_index_status},
        {"og_lake_index_catalog_upgrade", og_lake_index_catalog_upgrade},
        {"og_lake_index_catalog_validate", og_lake_index_catalog_validate},
        {"og_lake_row_lock_catalog_upgrade", og_lake_row_lock_catalog_upgrade},
        {"og_lake_reclaim_row_heads", og_lake_reclaim_row_heads},
        {"og_lake_test_maintenance_claim", og_lake_test_maintenance_claim},
        {"og_lake_test_maintenance_release", og_lake_test_maintenance_release},
        {"og_lake_test_catalog_guard_acquire", og_lake_test_catalog_guard_acquire},
        {"og_lake_test_catalog_guard_release", og_lake_test_catalog_guard_release},
        {"og_lake_test_xact_state", og_lake_test_xact_state},
        {"og_lake_test_precommit_insert", og_lake_test_precommit_insert},
        {"og_lake_row_locator", og_lake_row_locator},
        {"og_lake_test_locator", og_lake_test_locator},
        {"og_lake_test_analyze_pause", og_lake_test_analyze_pause},
        {"og_lake_test_pq_probe", og_lake_test_pq_probe},
        {"og_lake_test_smp_probe", og_lake_test_smp_probe},
        {"og_lake_test_rowhead_probe", og_lake_test_rowhead_probe},
        {"og_lake_test_index_maintenance_gate", og_lake_test_index_maintenance_gate},
        {"og_lake_test_autoanalyze_gate", og_lake_test_autoanalyze_gate}
    };
    for (Size i = 0; i < lengthof(functions); ++i) {
        if (strcmp(name, functions[i].name) == 0)
            return functions[i].function;
    }
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
        errmsg("unknown statically linked og_lake function \"%s\"", name)));
    return NULL;
}

