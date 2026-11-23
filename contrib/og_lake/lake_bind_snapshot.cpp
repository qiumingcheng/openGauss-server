/* Transaction-scoped read-only historical snapshot binding and privilege-filtered
 * public management/status views & metadata inspection functions (SQL0.59).
 *
 * Snapshot/metadata semantics adapted from DuckLake 0aaa8e10d4adfe2ca37da77134e991d20106d0e7
 * (MIT; notice in lake_catalog_ddl.inc):
 *   - functions/ducklake_table_info.cpp
 *   - functions/ducklake_list_files.cpp
 *   - functions/ducklake_options.cpp
 *   - storage/ducklake_metadata_manager.cpp GetSnapshot()
 * Host adaptation:
 *   - og_lake.bind_snapshot(server_name name, snapshot_id bigint) pins a read-only
 *     historical snapshot on the target catalog before any lake access in an explicit
 *     READ COMMITTED transaction block, enforces read-only isolation across all lake
 *     write/DDL/maintenance paths, maps surviving columns by stable column_id, and
 *     rejects queries referencing columns absent at the bound snapshot.
 *   - og_lake.table_info, og_lake.list_files, og_lake.options, and public status views
 *     (og_lake.catalog_status, og_lake.table_status, og_lake.index_status) filter rows
 *     strictly by caller business privileges and never expose credentials, encryption
 *     keys, or signed URLs.
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "lake_autoanalyze.h"
#include "lake_column_layout.h"
#include "lake_file_ann_catalog.h"
#include "lake_file_pq.h"
#include "lake_file_scalar.h"
#include "lake_index_private.h"
#include "lake_inline_ann.h"
#include "lake_inline_private.h"
#include "lake_inline_reader.h"
#include "lake_native_partition.h"
#include "lake_row_lock.h"
#include "lake_scalar_key.h"
#include "lake_txn_private.h"
#include "lake_version.h"
#include "access/heapam.h"
#include "access/og_lake.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "commands/sec_rls_cmds.h"
#include "distributelayer/streamCore.h"
#include "executor/spi.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "pgxc/pgxc.h"
#include "storage/lmgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/sec_rls_utils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/tuplestore.h"
#include <algorithm>
#include <vector>

extern "C" Datum og_lake_bind_snapshot(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_table_info(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_list_files(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_options(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_catalog_status_rows(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_table_status_rows(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_index_status_rows(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(og_lake_bind_snapshot);
PG_FUNCTION_INFO_V1(og_lake_table_info);
PG_FUNCTION_INFO_V1(og_lake_list_files);
PG_FUNCTION_INFO_V1(og_lake_options);
PG_FUNCTION_INFO_V1(og_lake_catalog_status_rows);
PG_FUNCTION_INFO_V1(og_lake_table_status_rows);
PG_FUNCTION_INFO_V1(og_lake_index_status_rows);

namespace {

struct LakeOptionMetadataEntry {
    const char* name;
    const char* description;
    bool safe_for_nonadmin_global;
};

static const LakeOptionMetadataEntry kLakeKnownOptions[] = {
    {"data_inlining_row_limit", "Maximum amount of rows to inline in a single insert", true},
    {"parquet_compression",
        "Compression algorithm for Parquet files (uncompressed, snappy, gzip, zstd, brotli, lz4, lz4_raw)", true},
    {"parquet_version", "Parquet format version (1 or 2)", true},
    {"parquet_compression_level", "Compression level for Parquet files written with zstd", true},
    {"parquet_row_group_size", "Number of rows per row group in Parquet files", true},
    {"parquet_row_group_size_bytes", "Number of bytes per row group in Parquet files", true},
    {"hive_file_pattern", "If partitioned data should be written in a hive-like folder structure", true},
    {"target_file_size", "The target data file size for insertion and compaction operations", true},
    {"version", "DuckLake format version", true},
    {"created_by", "Tool used to write the DuckLake", true},
    {"data_path", "Path to data files", false},
    {"require_commit_message", "If an explicit commit message is required for a snapshot commit.", true},
    {"rewrite_delete_threshold",
        "A threshold that determines the minimum amount of data that must be removed from a file before a rewrite is warranted. From 0 - 1.",
        true},
    {"delete_older_than",
        "How old unused files must be to be removed by cleanup functions.", true},
    {"expire_older_than",
        "How old snapshots must be, by default, to be expired by expire_snapshots", true},
    {"auto_compact", "Pre-defined schema used as a default value for the following compaction functions "
        "'ducklake_flush_inlined_data','ducklake_merge_adjacent_files', "
        "'ducklake_rewrite_data_files', 'ducklake_delete_orphaned_files'", true},
    {"encrypted", "Whether or not to encrypt Parquet files written to the data path", true},
    {"per_thread_output", "Whether to create separate output files per thread during parallel insertion", true},
    {"write_deletion_vectors", "Whether to write deletion vectors instead of positional delete files", true},
    {"sort_on_insert", "Whether to sort data on INSERT according to SET SORTED BY (default: true)", true},
    {"skip_stats_columns", "Columns for which min/max bounds are not recorded (counts are still recorded)", true},
};

static const char* LakeOptionDescription(const char* key, bool* safeGlobalOut)
{
    if (safeGlobalOut != NULL)
        *safeGlobalOut = false;
    if (key == NULL)
        return NULL;
    for (Size i = 0; i < lengthof(kLakeKnownOptions); ++i) {
        if (pg_strcasecmp(key, kLakeKnownOptions[i].name) == 0) {
            if (safeGlobalOut != NULL)
                *safeGlobalOut = kLakeKnownOptions[i].safe_for_nonadmin_global;
            return kLakeKnownOptions[i].description;
        }
    }
    return NULL;
}

static char* LakeSanitizePathOrUri(const char* raw, const char* dataRoot, bool isRelative)
{
    if (raw == NULL)
        return pstrdup("");
    const char* start = raw;
    if (!isRelative && dataRoot != NULL && *dataRoot != '\0') {
        Size rootLen = strlen(dataRoot);
        if (strncmp(start, dataRoot, rootLen) == 0) {
            start += rootLen;
            while (*start == '/')
                ++start;
        }
    }
    const char* queryPos = strchr(start, '?');
    const char* fragPos = strchr(start, '#');
    const char* end = NULL;
    if (queryPos != NULL && fragPos != NULL)
        end = (queryPos < fragPos) ? queryPos : fragPos;
    else if (queryPos != NULL)
        end = queryPos;
    else if (fragPos != NULL)
        end = fragPos;
    if (end != NULL)
        return pnstrdup(start, (Size)(end - start));
    return pstrdup(start);
}

static bool LakeMgmtOwnRow(const LakeMetaRows* rows, uint32 row, int column)
{
    return TransactionIdIsCurrentTransactionId(
        DatumGetTransactionId(LakeMetadataValue(rows, row, column, XIDOID)));
}

static int64 LakeMgmtVisibleHead(LakeInlineBinding* header, Snapshot snapshot, MemoryContext page,
    int64* schemaVersionOut = NULL)
{
    bool first = true;
    bool sawOwn = false;
    int64 cursor = 0;
    const char* snapRel = LakeMetadataRelation(header->metadata_schema_oid, "ducklake_snapshot");
    for (;;) {
        CHECK_FOR_INTERRUPTS();
        MemoryContextReset(page);
        MemoryContext parent = MemoryContextSwitchTo(page);
        Oid types[2] = {BOOLOID, INT8OID};
        Datum args[2] = {BoolGetDatum(first), Int64GetDatum(cursor)};
        LakeMetaRows* rows = LakeMetadataQuery(psprintf(
            "SELECT s.snapshot_id,s.schema_version,s.xmin FROM %s s WHERE ($1 OR s.snapshot_id<$2) "
            "ORDER BY s.snapshot_id DESC LIMIT 512", snapRel),
            2, types, args, NULL, snapshot, true, SPI_OK_SELECT, header->metadata_schema_oid);
        if (!rows->count) {
            if (sawOwn)
                ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("lake catalog has no committed base snapshot yet")));
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("lake catalog has no visible committed snapshot")));
        }
        for (uint32 i = 0; i < rows->count; ++i) {
            int64 id = DatumGetInt64(LakeMetadataValue(rows, i, 1, INT8OID));
            int64 schemaVer = DatumGetInt64(LakeMetadataValue(rows, i, 2, INT8OID));
            if (id < 0 || (!first && id >= cursor) || schemaVer < 0)
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("invalid descending lake snapshot identity")));
            first = false;
            cursor = id;
            if (!LakeMgmtOwnRow(rows, i, 3)) {
                MemoryContextSwitchTo(parent);
                MemoryContextReset(page);
                if (schemaVersionOut != NULL)
                    *schemaVersionOut = schemaVer;
                return id;
            }
            sawOwn = true;
        }
        MemoryContextSwitchTo(parent);
    }
}

static void LakeMgmtResolveVersion(LakeInlineBinding* header, Snapshot snapshot, int64 visibleHead,
    int64 requestedVersion, const char* callerName, MemoryContext page, int64* targetSnapshotOut, int64* targetSchemaOut)
{
    if (requestedVersion < 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("%s snapshot version must be nonnegative", callerName)));
    if (requestedVersion > visibleHead)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("%s snapshot " INT64_FORMAT " is newer than visible committed head " INT64_FORMAT,
                callerName, requestedVersion, visibleHead)));
    MemoryContextReset(page);
    MemoryContext parent = MemoryContextSwitchTo(page);
    Oid type = INT8OID;
    Datum arg = Int64GetDatum(requestedVersion);
    LakeMetaRows* rows = LakeMetadataQuery(psprintf(
        "SELECT s.snapshot_id,s.schema_version,s.xmin FROM %s s WHERE s.snapshot_id=$1",
        LakeMetadataRelation(header->metadata_schema_oid, "ducklake_snapshot")),
        1, &type, &arg, NULL, snapshot, true, SPI_OK_SELECT, header->metadata_schema_oid);
    if (rows->count > 1)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("duplicate lake snapshot identity")));
    if (rows->count == 0 || LakeMgmtOwnRow(rows, 0, 3))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("%s snapshot " INT64_FORMAT " does not exist or has expired", callerName, requestedVersion)));
    int64 snapId = DatumGetInt64(LakeMetadataValue(rows, 0, 1, INT8OID));
    int64 schemaVer = DatumGetInt64(LakeMetadataValue(rows, 0, 2, INT8OID));
    if (snapId != requestedVersion || schemaVer < 0)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid resolved lake snapshot metadata")));
    MemoryContextSwitchTo(parent);
    MemoryContextReset(page);
    *targetSnapshotOut = snapId;
    *targetSchemaOut = schemaVer;
}

static void LakeMgmtResolveTimestamp(LakeInlineBinding* header, Snapshot snapshot, int64 visibleHead,
    TimestampTz requestedTime, const char* callerName, MemoryContext page, int64* targetSnapshotOut, int64* targetSchemaOut)
{
    if (TIMESTAMP_NOT_FINITE(requestedTime) || !IS_VALID_TIMESTAMP(requestedTime) ||
        requestedTime > GetCurrentTimestamp())
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("%s cannot resolve a non-finite or future timestamp", callerName)));
    bool first = true;
    TimestampTz cursorTime = 0;
    int64 cursorId = 0;
    const char* snapRel = LakeMetadataRelation(header->metadata_schema_oid, "ducklake_snapshot");
    for (;;) {
        CHECK_FOR_INTERRUPTS();
        MemoryContextReset(page);
        MemoryContext parent = MemoryContextSwitchTo(page);
        Oid types[5] = {INT8OID, TIMESTAMPTZOID, BOOLOID, TIMESTAMPTZOID, INT8OID};
        Datum args[5] = {
            Int64GetDatum(visibleHead),
            TimestampTzGetDatum(requestedTime),
            BoolGetDatum(first),
            TimestampTzGetDatum(cursorTime),
            Int64GetDatum(cursorId)
        };
        LakeMetaRows* rows = LakeMetadataQuery(psprintf(
            "SELECT s.snapshot_id,s.snapshot_time,s.schema_version,s.xmin FROM %s s "
            "WHERE s.snapshot_id<=$1 AND s.snapshot_time<=$2 "
            "AND ($3 OR s.snapshot_time<$4 OR (s.snapshot_time=$4 AND s.snapshot_id<$5)) "
            "ORDER BY s.snapshot_time DESC, s.snapshot_id DESC LIMIT 512", snapRel),
            5, types, args, NULL, snapshot, true, SPI_OK_SELECT, header->metadata_schema_oid);
        if (!rows->count)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("%s found no committed lake snapshot at or before the requested timestamp", callerName)));
        for (uint32 i = 0; i < rows->count; ++i) {
            int64 id = DatumGetInt64(LakeMetadataValue(rows, i, 1, INT8OID));
            TimestampTz rowTime = DatumGetTimestampTz(LakeMetadataValue(rows, i, 2, TIMESTAMPTZOID));
            int64 schemaVer = DatumGetInt64(LakeMetadataValue(rows, i, 3, INT8OID));
            if (id < 0 || id > visibleHead || schemaVer < 0 || rowTime > requestedTime ||
                (!first && (rowTime > cursorTime || (rowTime == cursorTime && id >= cursorId))))
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("invalid descending lake snapshot timestamp order")));
            first = false;
            cursorTime = rowTime;
            cursorId = id;
            if (!LakeMgmtOwnRow(rows, i, 4)) {
                MemoryContextSwitchTo(parent);
                MemoryContextReset(page);
                *targetSnapshotOut = id;
                *targetSchemaOut = schemaVer;
                return;
            }
        }
        MemoryContextSwitchTo(parent);
    }
}

static bool LakeCallerCanSeeRelation(Oid relationOid)
{
    if (!OidIsValid(relationOid))
        return false;
    Relation rel = try_relation_open(relationOid, AccessShareLock);
    if (rel == NULL)
        return false;
    Oid userId = GetUserId();
    if (superuser_arg(userId)) {
        relation_close(rel, NoLock);
        return true;
    }
    Oid nsOid = RelationGetNamespace(rel);
    if (pg_namespace_aclcheck(nsOid, userId, ACL_USAGE) != ACLCHECK_OK) {
        relation_close(rel, NoLock);
        return false;
    }
    bool ok = pg_class_ownercheck(relationOid, userId) ||
        pg_class_aclcheck(relationOid, userId, ACL_SELECT) == ACLCHECK_OK ||
        pg_attribute_aclcheck_all(relationOid, userId, ACL_SELECT, ACLMASK_ANY) == ACLCHECK_OK;
    relation_close(rel, NoLock);
    return ok;
}

static ForeignServer* LakeTryGetForeignServer(Oid serverOid)
{
    if (!OidIsValid(serverOid) || !SearchSysCacheExists1(FOREIGNSERVEROID, ObjectIdGetDatum(serverOid)))
        return NULL;
    return GetForeignServer(serverOid);
}

static bool LakeCallerCanSeeCatalog(Oid serverOid, Snapshot snapshot)
{
    Oid userId = GetUserId();
    if (superuser_arg(userId) || pg_foreign_server_aclcheck(serverOid, userId, ACL_USAGE) == ACLCHECK_OK)
        return true;
    Oid type = OIDOID;
    Datum arg = ObjectIdGetDatum(serverOid);
    LakeMetaRows* rows = LakeMetadataQuery(
        "SELECT DISTINCT r.relation_oid FROM og_lake_internal.relation_binding r "
        "JOIN og_lake_internal.table_binding t USING(binding_id) "
        "WHERE t.server_oid=$1 AND t.state='active'",
        1, &type, &arg, NULL, snapshot, true, SPI_OK_SELECT);
    for (uint32 i = 0; i < rows->count; ++i) {
        Oid relOid = DatumGetObjectId(LakeMetadataValue(rows, i, 1, OIDOID));
        if (LakeCallerCanSeeRelation(relOid))
            return true;
    }
    if (LakeNativePartitionVersionEnabled()) {
        LakeMetaRows* pRows = LakeMetadataQuery(
            "SELECT DISTINCT p.parent_relid FROM og_lake_internal.partition_binding p "
            "JOIN og_lake_internal.table_binding t USING(binding_id) "
            "WHERE t.server_oid=$1 AND t.state='active'",
            1, &type, &arg, NULL, snapshot, true, SPI_OK_SELECT);
        for (uint32 i = 0; i < pRows->count; ++i) {
            Oid parentOid = DatumGetObjectId(LakeMetadataValue(pRows, i, 1, OIDOID));
            if (LakeCallerCanSeeRelation(parentOid))
                return true;
        }
    }
    return false;
}

static void LakeVerifyRequestedColumnBitmap(Oid relationOid, const LakeInlineBinding* binding,
    int64 boundSnapshotId, const Bitmapset* cols)
{
    if (bms_is_empty(cols))
        return;
    Relation host = heap_open(relationOid, AccessShareLock);
    TupleDesc desc = RelationGetDescr(host);
    Bitmapset* tmpset = bms_copy(cols);
    int member = -1;
    PG_TRY();
    {
        while ((member = bms_first_member(tmpset)) >= 0) {
            int attno = member + FirstLowInvalidHeapAttributeNumber;
            if (attno == InvalidAttrNumber) {
                for (int h = 0; h < desc->natts; ++h) {
                    if (!desc->attrs[h].attisdropped && LakeHostToLogical(binding, (AttrNumber)(h + 1)) < 0)
                        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
                            errmsg("column \"%s\" of relation \"%s\" does not exist at bound snapshot " INT64_FORMAT,
                                NameStr(desc->attrs[h].attname), RelationGetRelationName(host), boundSnapshotId)));
                }
            } else if (attno > 0) {
                if (attno > desc->natts || desc->attrs[attno - 1].attisdropped ||
                    LakeHostToLogical(binding, (AttrNumber)attno) < 0) {
                    const char* colName = (attno <= desc->natts) ? NameStr(desc->attrs[attno - 1].attname) : "?";
                    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
                        errmsg("column \"%s\" of relation \"%s\" does not exist at bound snapshot " INT64_FORMAT,
                            colName, RelationGetRelationName(host), boundSnapshotId)));
                }
            }
        }
        bms_free(tmpset);
        heap_close(host, NoLock);
    }
    PG_CATCH();
    {
        bms_free(tmpset);
        heap_close(host, NoLock);
        PG_RE_THROW();
    }
    PG_END_TRY();
}

static void LakeCheckBoundSnapshotOneRte(const RangeTblEntry* rte, bool isResultRel, const Bitmapset* extraVars)
{
    if (rte == NULL || rte->rtekind != RTE_RELATION || !OidIsValid(rte->relid))
        return;
    char relkind = get_rel_relkind(rte->relid);
    if (relkind == RELKIND_RELATION && LakeNativePartitionVersionEnabled()) {
        Relation rel = try_relation_open(rte->relid, AccessShareLock);
        if (rel != NULL) {
            bool marked = LakeHasNativePartitionMarker(rel);
            relation_close(rel, NoLock);
            if (marked && LakeTxnHasBoundSnapshot()) {
                Oid relType = OIDOID;
                Datum relArg = ObjectIdGetDatum(rte->relid);
                Snapshot snap = ActiveSnapshotSet() ? GetActiveSnapshot() : GetTransactionSnapshot();
                LakeMetaRows* pRows = LakeMetadataQuery(
                    "SELECT DISTINCT t.server_oid FROM og_lake_internal.partition_binding p "
                    "JOIN og_lake_internal.table_binding t USING(binding_id) "
                    "WHERE p.parent_relid=$1 AND t.state='active'",
                    1, &relType, &relArg, NULL, snap, true, SPI_OK_SELECT);
                for (uint32 i = 0; i < pRows->count; ++i) {
                    Oid srvOid = DatumGetObjectId(LakeMetadataValue(pRows, i, 1, OIDOID));
                    if (LakeTxnGetBoundCatalog(srvOid) != NULL)
                        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("bind_snapshot does not yet support native partitioned parent tables")));
                }
            }
        }
        return;
    }
    if (relkind != RELKIND_FOREIGN_TABLE)
        return;
    ForeignTable* foreign = GetForeignTable(rte->relid);
    if (!LakeTrustedForeignServer(foreign->serverid))
        return;
    LakeTxnCatalog* bound = LakeTxnGetBoundCatalog(foreign->serverid);
    if (bound == NULL)
        return;
    if (isResultRel || (rte->requiredPerms & (ACL_INSERT | ACL_UPDATE | ACL_DELETE)) != 0 ||
        !bms_is_empty(rte->insertedCols) || !bms_is_empty(rte->updatedCols))
        ereport(ERROR, (errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
            errmsg("cannot modify a native lake table in a transaction with a bound read-only snapshot")));

    MemoryContext caller = CurrentMemoryContext;
    MemoryContext scratch = AllocSetContextCreate(caller, "Lake bound snapshot RTE check", ALLOCSET_DEFAULT_SIZES);
    PG_TRY();
    {
        MemoryContextSwitchTo(scratch);
        LakeInlineBinding* binding = LakeLoadBoundSnapshotInlineBinding(rte->relid, bound->bound_snapshot_id,
            bound->bound_schema_version, bound->bound_visible_head, bound->business_snapshot, true);
        Bitmapset* combined = bms_union(rte->selectedCols, extraVars);
        LakeVerifyRequestedColumnBitmap(rte->relid, binding, bound->bound_snapshot_id, combined);
        bms_free(combined);
        MemoryContextSwitchTo(caller);
        MemoryContextDelete(scratch);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(caller);
        MemoryContextDelete(scratch);
        PG_RE_THROW();
    }
    PG_END_TRY();
}

struct LakeQueryVarCollectCtx {
    Index target_rtindex;
    int sublevels_up;
    Bitmapset* attnos;
};

static bool LakeCollectQueryVarsWalker(Node* node, LakeQueryVarCollectCtx* ctx)
{
    if (node == NULL)
        return false;
    if (IsA(node, Var)) {
        Var* var = (Var*)node;
        if (var->varno == ctx->target_rtindex && (int)var->varlevelsup == ctx->sublevels_up && var->varattno >= 0)
            ctx->attnos = bms_add_member(ctx->attnos, var->varattno - FirstLowInvalidHeapAttributeNumber);
        return false;
    }
    if (IsA(node, Query)) {
        ctx->sublevels_up++;
        bool res = query_tree_walker((Query*)node, (bool (*)())LakeCollectQueryVarsWalker, (void*)ctx, 0);
        ctx->sublevels_up--;
        return res;
    }
    return expression_tree_walker(node, (bool (*)())LakeCollectQueryVarsWalker, (void*)ctx);
}

static bool LakeResultRelationsContain(const List* list, int rti)
{
    if (list == NIL)
        return false;
    if (IsA(list, IntList))
        return list_member_int(list, rti);
    if (IsA(list, List)) {
        const ListCell* lc = NULL;
        foreach (lc, list) {
            if (LakeResultRelationsContain((const List*)lfirst(lc), rti))
                return true;
        }
    }
    return false;
}

static bool LakeBoundSnapshotQueryWalker(Node* node, void* opaque)
{
    if (node == NULL)
        return false;
    if (IsA(node, Query)) {
        Query* query = (Query*)node;
        ListCell* cell = NULL;
        Index rti = 0;
        foreach (cell, query->rtable) {
            ++rti;
            RangeTblEntry* rte = (RangeTblEntry*)lfirst(cell);
            bool isResultRel = (query->resultRelation > 0 && (Index)query->resultRelation == rti) ||
                LakeResultRelationsContain(query->resultRelations, (int)rti);
            LakeQueryVarCollectCtx vctx = {rti, 0, NULL};
            if (rte->rtekind == RTE_RELATION && get_rel_relkind(rte->relid) == RELKIND_FOREIGN_TABLE) {
                ForeignTable* ft = GetForeignTable(rte->relid);
                if (LakeTrustedForeignServer(ft->serverid) && LakeTxnGetBoundCatalog(ft->serverid) != NULL)
                    (void)query_tree_walker(query, (bool (*)())LakeCollectQueryVarsWalker, (void*)&vctx, 0);
            }
            PG_TRY();
            {
                LakeCheckBoundSnapshotOneRte(rte, isResultRel, vctx.attnos);
                bms_free(vctx.attnos);
            }
            PG_CATCH();
            {
                bms_free(vctx.attnos);
                PG_RE_THROW();
            }
            PG_END_TRY();
        }
        return query_tree_walker(query, (bool (*)())LakeBoundSnapshotQueryWalker, opaque, 0);
    }
    return expression_tree_walker(node, (bool (*)())LakeBoundSnapshotQueryWalker, opaque);
}

static void LakeRequirePublicManagementAdmission(const char* funcName)
{
    if (StreamThreadAmI() || IS_SPQ_RUNNING)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("%s requires the invoking serial backend", funcName)));
    if (!ActiveSnapshotSet())
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("%s requires an active statement snapshot", funcName)));
    LakeRequireAccessPolicy();
    LakeCheckMaskingAdmission();
    if (!LakeBindSnapshotVersionEnabled())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("%s requires installed SQL0.59", funcName)));
}

static void LakeResolveEffectiveCatalogSnapshot(Oid serverOid, Snapshot callerSnapshot,
    MemoryContext pageCtx, LakeInlineBinding** headerOut, Snapshot* effectiveHostSnapshotOut,
    int64* visibleHeadOut, int64* effectiveSnapOut, int64* effectiveSchemaOut, bool* isBoundOut)
{
    LakeTxnCatalog* bound = LakeTxnGetBoundCatalog(serverOid);
    Snapshot useSnap = (bound != NULL) ? bound->business_snapshot : callerSnapshot;
    LakeInlineBinding* header = LakeLoadCatalogHeader(serverOid, useSnap);
    if (bound != NULL) {
        *headerOut = header;
        *effectiveHostSnapshotOut = useSnap;
        *visibleHeadOut = bound->bound_visible_head;
        *effectiveSnapOut = bound->bound_snapshot_id;
        *effectiveSchemaOut = bound->bound_schema_version;
        if (isBoundOut != NULL)
            *isBoundOut = true;
        return;
    }
    int64 schemaVer = 0;
    int64 head = LakeMgmtVisibleHead(header, useSnap, pageCtx, &schemaVer);
    *headerOut = header;
    *effectiveHostSnapshotOut = useSnap;
    *visibleHeadOut = head;
    *effectiveSnapOut = head;
    *effectiveSchemaOut = schemaVer;
    if (isBoundOut != NULL)
        *isBoundOut = false;
}

} // namespace

void LakeCheckBoundSnapshotQuery(Query* query)
{
    if (!LakeTxnHasBoundSnapshot() || query == NULL || LakeGetMetadataScope() != NULL)
        return;
    (void)LakeBoundSnapshotQueryWalker((Node*)query, NULL);
}

void LakeCheckBoundSnapshotPlan(PlannedStmt* plan)
{
    if (!LakeTxnHasBoundSnapshot() || plan == NULL || plan->utilityStmt != NULL || LakeGetMetadataScope() != NULL)
        return;
    if (StreamThreadAmI())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("SMP execution is disabled for bound-snapshot lake queries")));
    ListCell* cell = NULL;
    int rti = 0;
    foreach (cell, plan->rtable) {
        ++rti;
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(cell);
        bool isResultRel = LakeResultRelationsContain(plan->resultRelations, rti);
        LakeCheckBoundSnapshotOneRte(rte, isResultRel, NULL);
    }
}

void LakeCheckBoundSnapshotScanVars(ForeignScanState* node, const LakeInlineBinding* binding)
{
    if (node == NULL || binding == NULL || !binding->bound_snapshot_read)
        return;
    ForeignScan* plan = (ForeignScan*)node->ss.ps.plan;
    Index scanRelid = plan->scan.scanrelid;
    Bitmapset* attnos = NULL;
    pull_varattnos((Node*)plan->scan.plan.targetlist, scanRelid, &attnos);
    pull_varattnos((Node*)plan->scan.plan.qual, scanRelid, &attnos);
    pull_varattnos((Node*)plan->fdw_exprs, scanRelid, &attnos);
    pull_varattnos((Node*)plan->fdw_scan_tlist, scanRelid, &attnos);
    if (plan->fdw_scan_tlist != NIL) {
        pull_varattnos((Node*)plan->scan.plan.targetlist, INDEX_VAR, &attnos);
        pull_varattnos((Node*)plan->scan.plan.qual, INDEX_VAR, &attnos);
    }
    PG_TRY();
    {
        LakeVerifyRequestedColumnBitmap(binding->relation_oid, binding, binding->snapshot_id, attnos);
        bms_free(attnos);
    }
    PG_CATCH();
    {
        bms_free(attnos);
        PG_RE_THROW();
    }
    PG_END_TRY();
}

Datum og_lake_bind_snapshot(PG_FUNCTION_ARGS)
{
    LakeRequirePublicManagementAdmission("bind_snapshot");
    if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
            errmsg("bind_snapshot server_name and snapshot_id cannot be NULL")));
    if (!IsTransactionBlock())
        ereport(ERROR, (errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION),
            errmsg("bind_snapshot can only be called inside an explicit transaction block")));

    const char* serverName = NameStr(*PG_GETARG_NAME(0));
    int64 requestedVersion = PG_GETARG_INT64(1);
    if (requestedVersion < 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("bind_snapshot snapshot_id must be nonnegative")));

    ForeignServer* server = GetForeignServerByName(serverName, false);
    if (!LakeTrustedForeignServer(server->serverid))
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg("foreign server \"%s\" is not an og_lake server", serverName)));

    MemoryContext callerCtx = CurrentMemoryContext;
    MemoryContext volatile workCtx = AllocSetContextCreate(callerCtx,
        "Lake bind_snapshot execution", ALLOCSET_DEFAULT_SIZES);
    Snapshot volatile snapshot = NULL;
    volatile int fastScope = 0;
    volatile bool fastActive = false;

    PG_TRY();
    {
        fastScope = LakeEnterFastValidationScope();
        fastActive = true;
        snapshot = RegisterSnapshot(GetActiveSnapshot());
        MemoryContextSwitchTo(workCtx);

        if (!LakeCallerCanSeeCatalog(server->serverid, snapshot))
            aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_FOREIGN_SERVER, serverName);

        LakeInlineBinding* header = LakeLoadCatalogHeader(server->serverid, snapshot);
        MemoryContext pageCtx = AllocSetContextCreate(workCtx,
            "Lake bind_snapshot page", ALLOCSET_DEFAULT_SIZES);
        int64 visibleHead = LakeMgmtVisibleHead(header, snapshot, pageCtx);
        int64 targetSnap = -1;
        int64 targetSchema = -1;
        LakeMgmtResolveVersion(header, snapshot, visibleHead, requestedVersion, "bind_snapshot",
            pageCtx, &targetSnap, &targetSchema);
        MemoryContextDelete(pageCtx);

        LakeLeaveFastValidationScope(fastScope);
        fastActive = false;

        MemoryContextSwitchTo(callerCtx);
        LakeTxnBindCatalogSnapshot(server->serverid, serverName, targetSnap, targetSchema, visibleHead, snapshot);

        UnregisterSnapshot(snapshot);
        snapshot = NULL;
        MemoryContextDelete(workCtx);
        workCtx = NULL;
    }
    PG_CATCH();
    {
        if (fastActive) {
            LakeLeaveFastValidationScope(fastScope);
            fastActive = false;
        }
        if (snapshot != NULL) {
            UnregisterSnapshot(snapshot);
            snapshot = NULL;
        }
        MemoryContextSwitchTo(callerCtx);
        if (workCtx != NULL) {
            MemoryContextDelete(workCtx);
            workCtx = NULL;
        }
        PG_RE_THROW();
    }
    PG_END_TRY();

    PG_RETURN_VOID();
}

/* DuckLake-compatible og_lake.table_info(catalog_name name) */
Datum og_lake_table_info(PG_FUNCTION_ARGS)
{
    LakeRequirePublicManagementAdmission("table_info");
    if (PG_ARGISNULL(0))
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("table_info catalog_name cannot be NULL")));

    ReturnSetInfo* result = (ReturnSetInfo*)fcinfo->resultinfo;
    if (!result || !IsA(result, ReturnSetInfo) || !(result->allowedModes & SFRM_Materialize) || !result->econtext)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("table_info requires materialize mode")));

    const char* serverName = NameStr(*PG_GETARG_NAME(0));
    ForeignServer* server = GetForeignServerByName(serverName, false);
    if (!LakeTrustedForeignServer(server->serverid))
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg("foreign server \"%s\" is not an og_lake server", serverName)));

    MemoryContext callerCtx = MemoryContextSwitchTo(result->econtext->ecxt_per_query_memory);
    TupleDesc desc = CreateTemplateTupleDesc(8, false);
    TupleDescInitEntry(desc, 1, "table_name", TEXTOID, -1, DEFAULT_COLLATION_OID);
    TupleDescInitEntry(desc, 2, "schema_id", INT8OID, -1, 0);
    TupleDescInitEntry(desc, 3, "table_id", INT8OID, -1, 0);
    TupleDescInitEntry(desc, 4, "table_uuid", UUIDOID, -1, 0);
    TupleDescInitEntry(desc, 5, "file_count", INT8OID, -1, 0);
    TupleDescInitEntry(desc, 6, "file_size_bytes", INT8OID, -1, 0);
    TupleDescInitEntry(desc, 7, "delete_file_count", INT8OID, -1, 0);
    TupleDescInitEntry(desc, 8, "delete_file_size_bytes", INT8OID, -1, 0);
    desc = BlessTupleDesc(desc);
    Tuplestorestate* store = tuplestore_begin_heap(true, false,
        Max(64, Min(u_sess->attr.attr_memory.work_mem, 1024)));
    MemoryContextSwitchTo(callerCtx);

    MemoryContext volatile workCtx = AllocSetContextCreate(callerCtx, "Lake table_info", ALLOCSET_DEFAULT_SIZES);
    Snapshot volatile snapshot = NULL;
    volatile int fastScope = 0;
    volatile bool fastActive = false;

    PG_TRY();
    {
        fastScope = LakeEnterFastValidationScope();
        fastActive = true;
        snapshot = RegisterSnapshot(GetActiveSnapshot());
        MemoryContextSwitchTo(workCtx);

        if (!LakeCallerCanSeeCatalog(server->serverid, snapshot))
            aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_FOREIGN_SERVER, serverName);

        MemoryContext pageCtx = AllocSetContextCreate(workCtx, "Lake table_info page", ALLOCSET_DEFAULT_SIZES);
        LakeInlineBinding* header = NULL;
        Snapshot useSnap = NULL;
        int64 visibleHead = 0, effSnap = 0, effSchema = 0;
        LakeResolveEffectiveCatalogSnapshot(server->serverid, snapshot, pageCtx,
            &header, &useSnap, &visibleHead, &effSnap, &effSchema, NULL);
        MemoryContextDelete(pageCtx);

        bool isAdmin = superuser();
        Oid srvType = OIDOID;
        Datum srvArg = ObjectIdGetDatum(server->serverid);
        LakeMetaRows* hostMap = isAdmin ? NULL : LakeMetadataQuery(
            "SELECT t.table_id, r.relation_oid FROM og_lake_internal.table_binding t "
            "JOIN og_lake_internal.relation_binding r USING(binding_id) "
            "WHERE t.server_oid=$1 AND t.state='active' AND t.object_kind='relation'",
            1, &srvType, &srvArg, NULL, useSnap, true, SPI_OK_SELECT);

        Oid snapType = INT8OID;
        Datum snapArg = Int64GetDatum(effSnap);
        const char* tblRel = LakeMetadataRelation(header->metadata_schema_oid, "ducklake_table");
        const char* dfRel = LakeMetadataRelation(header->metadata_schema_oid, "ducklake_data_file");
        const char* delRel = LakeMetadataRelation(header->metadata_schema_oid, "ducklake_delete_file");
        LakeMetaRows* rows = LakeMetadataQuery(psprintf(
            "SELECT t.table_name::text, t.schema_id, t.table_id, t.table_uuid, "
            "(SELECT count(*)::bigint FROM %s df WHERE df.table_id=t.table_id AND df.begin_snapshot<=$1 AND (df.end_snapshot IS NULL OR df.end_snapshot>$1)), "
            "(SELECT COALESCE(sum(df.file_size_bytes),0)::bigint FROM %s df WHERE df.table_id=t.table_id AND df.begin_snapshot<=$1 AND (df.end_snapshot IS NULL OR df.end_snapshot>$1)), "
            "(SELECT count(*)::bigint FROM %s dl WHERE dl.table_id=t.table_id AND dl.begin_snapshot<=$1 AND (dl.end_snapshot IS NULL OR dl.end_snapshot>$1)), "
            "(SELECT COALESCE(sum(dl.file_size_bytes),0)::bigint FROM %s dl WHERE dl.table_id=t.table_id AND dl.begin_snapshot<=$1 AND (dl.end_snapshot IS NULL OR dl.end_snapshot>$1)) "
            "FROM %s t WHERE t.begin_snapshot<=$1 AND (t.end_snapshot IS NULL OR t.end_snapshot>$1) ORDER BY t.table_id",
            dfRel, dfRel, delRel, delRel, tblRel),
            1, &snapType, &snapArg, NULL, useSnap, true, SPI_OK_SELECT, header->metadata_schema_oid);

        for (uint32 i = 0; i < rows->count; ++i) {
            int64 tableId = DatumGetInt64(LakeMetadataValue(rows, i, 3, INT8OID));
            if (!isAdmin) {
                bool authorized = false;
                for (uint32 m = 0; m < hostMap->count; ++m) {
                    if (DatumGetInt64(LakeMetadataValue(hostMap, m, 1, INT8OID)) == tableId) {
                        Oid relOid = DatumGetObjectId(LakeMetadataValue(hostMap, m, 2, OIDOID));
                        if (LakeCallerCanSeeRelation(relOid)) {
                            authorized = true;
                            break;
                        }
                    }
                }
                if (!authorized)
                    continue;
            }
            Datum vals[8];
            bool nulls[8] = {false};
            vals[0] = CStringGetTextDatum(LakeMetadataText(rows, i, 1));
            vals[1] = LakeMetadataValue(rows, i, 2, INT8OID);
            vals[2] = Int64GetDatum(tableId);
            vals[3] = LakeMetadataValue(rows, i, 4, UUIDOID);
            vals[4] = LakeMetadataValue(rows, i, 5, INT8OID);
            vals[5] = LakeMetadataValue(rows, i, 6, INT8OID);
            vals[6] = LakeMetadataValue(rows, i, 7, INT8OID);
            vals[7] = LakeMetadataValue(rows, i, 8, INT8OID);
            tuplestore_putvalues(store, desc, vals, nulls);
        }

        LakeLeaveFastValidationScope(fastScope);
        fastActive = false;
        UnregisterSnapshot(snapshot);
        snapshot = NULL;
        MemoryContextSwitchTo(callerCtx);
        MemoryContextDelete(workCtx);
        workCtx = NULL;

        tuplestore_donestoring(store);
        result->returnMode = SFRM_Materialize;
        result->setResult = store;
        result->setDesc = desc;
    }
    PG_CATCH();
    {
        if (fastActive) {
            LakeLeaveFastValidationScope(fastScope);
            fastActive = false;
        }
        if (snapshot != NULL) {
            UnregisterSnapshot(snapshot);
            snapshot = NULL;
        }
        MemoryContextSwitchTo(callerCtx);
        if (workCtx != NULL) {
            MemoryContextDelete(workCtx);
            workCtx = NULL;
        }
        PG_RE_THROW();
    }
    PG_END_TRY();

    fcinfo->isnull = true;
    return (Datum)0;
}

/* DuckLake-compatible og_lake.list_files(scope regclass, snapshot_version bigint, snapshot_time timestamptz) */
Datum og_lake_list_files(PG_FUNCTION_ARGS)
{
    LakeRequirePublicManagementAdmission("list_files");
    if (PG_ARGISNULL(0))
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("list_files target relation cannot be NULL")));
    bool hasVersion = !PG_ARGISNULL(1);
    bool hasTime = !PG_ARGISNULL(2);
    if (hasVersion && hasTime)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("list_files cannot specify both snapshot_version and snapshot_time")));

    ReturnSetInfo* result = (ReturnSetInfo*)fcinfo->resultinfo;
    if (!result || !IsA(result, ReturnSetInfo) || !(result->allowedModes & SFRM_Materialize) || !result->econtext)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("list_files requires materialize mode")));

    Oid relationOid = PG_GETARG_OID(0);
    Relation host = try_relation_open(relationOid, AccessShareLock);
    if (host == NULL)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("relation with OID %u does not exist", relationOid)));
    Oid nsOid = RelationGetNamespace(host);
    AclResult nsAcl = pg_namespace_aclcheck(nsOid, GetUserId(), ACL_USAGE);
    if (nsAcl != ACLCHECK_OK) {
        relation_close(host, NoLock);
        aclcheck_error(nsAcl, ACL_KIND_NAMESPACE, get_namespace_name(nsOid));
    }
    if (!superuser() && !pg_class_ownercheck(relationOid, GetUserId()) &&
        pg_class_aclcheck(relationOid, GetUserId(), ACL_SELECT) != ACLCHECK_OK &&
        pg_attribute_aclcheck_all(relationOid, GetUserId(), ACL_SELECT, ACLMASK_ALL) != ACLCHECK_OK) {
        char* relName = pstrdup(RelationGetRelationName(host));
        relation_close(host, NoLock);
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_CLASS, relName);
    }
    LakeRejectNativePartitionDDL(host, "list_files");
    if (!LakeIsNativeForeignRelation(host)) {
        char* relName = pstrdup(RelationGetRelationName(host));
        relation_close(host, NoLock);
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg("list_files target \"%s\" is not an og_lake foreign table", relName)));
    }
    ForeignTable* foreign = GetForeignTable(relationOid);
    if (!LakeTrustedForeignServer(foreign->serverid)) {
        relation_close(host, NoLock);
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("list_files target server is not a trusted og_lake server")));
    }
    if (RelationHasRlspolicy(relationOid) || RelationEnableRowSecurity(host) ||
        RelationForceRowSecurity(host) || host->rd_rules != NULL) {
        relation_close(host, NoLock);
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake row-security and rule combinations are not enabled")));
    }
    if (LakeTxnFindCreatedTable(relationOid, false) != NULL) {
        relation_close(host, NoLock);
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("list_files requires a committed native lake foreign table")));
    }
    Oid serverOid = foreign->serverid;
    relation_close(host, NoLock);

    MemoryContext callerCtx = MemoryContextSwitchTo(result->econtext->ecxt_per_query_memory);
    TupleDesc desc = CreateTemplateTupleDesc(8, false);
    TupleDescInitEntry(desc, 1, "data_file", TEXTOID, -1, DEFAULT_COLLATION_OID);
    TupleDescInitEntry(desc, 2, "data_file_size_bytes", INT8OID, -1, 0);
    TupleDescInitEntry(desc, 3, "data_file_footer_size", INT8OID, -1, 0);
    TupleDescInitEntry(desc, 4, "data_file_encryption_key", BYTEAOID, -1, 0);
    TupleDescInitEntry(desc, 5, "delete_file", TEXTOID, -1, DEFAULT_COLLATION_OID);
    TupleDescInitEntry(desc, 6, "delete_file_size_bytes", INT8OID, -1, 0);
    TupleDescInitEntry(desc, 7, "delete_file_footer_size", INT8OID, -1, 0);
    TupleDescInitEntry(desc, 8, "delete_file_encryption_key", BYTEAOID, -1, 0);
    desc = BlessTupleDesc(desc);
    Tuplestorestate* store = tuplestore_begin_heap(true, false,
        Max(64, Min(u_sess->attr.attr_memory.work_mem, 1024)));
    MemoryContextSwitchTo(callerCtx);

    MemoryContext volatile workCtx = AllocSetContextCreate(callerCtx, "Lake list_files", ALLOCSET_DEFAULT_SIZES);
    Snapshot volatile snapshot = NULL;
    volatile int fastScope = 0;
    volatile bool fastActive = false;

    PG_TRY();
    {
        fastScope = LakeEnterFastValidationScope();
        fastActive = true;
        snapshot = RegisterSnapshot(GetActiveSnapshot());
        MemoryContextSwitchTo(workCtx);

        MemoryContext pageCtx = AllocSetContextCreate(workCtx, "Lake list_files page", ALLOCSET_DEFAULT_SIZES);
        LakeInlineBinding* header = NULL;
        Snapshot useSnap = NULL;
        int64 visibleHead = 0, targetSnap = 0, targetSchema = 0;
        LakeResolveEffectiveCatalogSnapshot(serverOid, snapshot, pageCtx,
            &header, &useSnap, &visibleHead, &targetSnap, &targetSchema, NULL);
        if (hasVersion) {
            header = LakeLoadCatalogHeader(serverOid, snapshot);
            useSnap = snapshot;
            visibleHead = LakeMgmtVisibleHead(header, snapshot, pageCtx);
            LakeMgmtResolveVersion(header, snapshot, visibleHead, PG_GETARG_INT64(1), "list_files",
                pageCtx, &targetSnap, &targetSchema);
        } else if (hasTime) {
            header = LakeLoadCatalogHeader(serverOid, snapshot);
            useSnap = snapshot;
            visibleHead = LakeMgmtVisibleHead(header, snapshot, pageCtx);
            LakeMgmtResolveTimestamp(header, snapshot, visibleHead, PG_GETARG_TIMESTAMPTZ(2), "list_files",
                pageCtx, &targetSnap, &targetSchema);
        }
        MemoryContextDelete(pageCtx);

        Oid relType = OIDOID;
        Datum relArg = ObjectIdGetDatum(relationOid);
        LakeMetaRows* bound = LakeMetadataQuery(
            "SELECT t.table_id, t.table_uuid::text FROM og_lake_internal.relation_binding r "
            "JOIN og_lake_internal.table_binding t USING(binding_id) "
            "WHERE r.relation_oid=$1 AND t.state='active' AND t.object_kind='relation'",
            1, &relType, &relArg, NULL, useSnap, true, SPI_OK_SELECT);
        LakeMetadataRequireCount(bound, 1, "list_files relation binding");
        int64 tableId = DatumGetInt64(LakeMetadataValue(bound, 0, 1, INT8OID));

        Oid qTypes[2] = {INT8OID, INT8OID};
        Datum qArgs[2] = {Int64GetDatum(tableId), Int64GetDatum(targetSnap)};
        const char* tblRel = LakeMetadataRelation(header->metadata_schema_oid, "ducklake_table");
        const char* schRel = LakeMetadataRelation(header->metadata_schema_oid, "ducklake_schema");
        LakeMetaRows* tblRows = LakeMetadataQuery(psprintf(
            "SELECT t.path::text, t.path_is_relative, s.path::text, s.path_is_relative "
            "FROM %s t JOIN %s s ON s.schema_id=t.schema_id "
            "AND s.begin_snapshot<=$2 AND (s.end_snapshot IS NULL OR s.end_snapshot>$2) "
            "WHERE t.table_id=$1 AND t.begin_snapshot<=$2 AND (t.end_snapshot IS NULL OR t.end_snapshot>$2)",
            tblRel, schRel),
            2, qTypes, qArgs, NULL, useSnap, true, SPI_OK_SELECT, header->metadata_schema_oid);
        if (tblRows->count == 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("native lake table did not exist at snapshot " INT64_FORMAT, targetSnap)));
        LakeMetadataRequireCount(tblRows, 1, "list_files table path");
        const char* tblPath = LakeMetadataText(tblRows, 0, 1);
        bool tblRelFlag = DatumGetBool(LakeMetadataValue(tblRows, 0, 2, BOOLOID));
        const char* schPath = LakeMetadataText(tblRows, 0, 3);
        bool schRelFlag = DatumGetBool(LakeMetadataValue(tblRows, 0, 4, BOOLOID));
        char* tablePrefix = pstrdup("");
        if (tblRelFlag) {
            if (schRelFlag)
                tablePrefix = psprintf("%s%s", schPath, tblPath);
            else
                tablePrefix = LakeSanitizePathOrUri(psprintf("%s%s", schPath, tblPath), header->data_root, false);
        } else {
            tablePrefix = LakeSanitizePathOrUri(tblPath, header->data_root, false);
        }

        const char* dfRel = LakeMetadataRelation(header->metadata_schema_oid, "ducklake_data_file");
        const char* delRel = LakeMetadataRelation(header->metadata_schema_oid, "ducklake_delete_file");
        LakeMetaRows* files = LakeMetadataQuery(psprintf(
            "SELECT df.data_file_id, df.path::text, df.path_is_relative, df.file_size_bytes, df.footer_size, "
            "dl.path::text, dl.path_is_relative, dl.file_size_bytes, dl.footer_size "
            "FROM %s df LEFT JOIN %s dl ON dl.table_id=df.table_id AND dl.data_file_id=df.data_file_id "
            "AND dl.begin_snapshot<=$2 AND (dl.end_snapshot IS NULL OR dl.end_snapshot>$2) "
            "WHERE df.table_id=$1 AND df.begin_snapshot<=$2 AND (df.end_snapshot IS NULL OR df.end_snapshot>$2) "
            "ORDER BY COALESCE(df.file_order, df.data_file_id), df.data_file_id",
            dfRel, delRel),
            2, qTypes, qArgs, NULL, useSnap, true, SPI_OK_SELECT, header->metadata_schema_oid);

        for (uint32 i = 0; i < files->count; ++i) {
            Datum vals[8] = {0};
            bool nulls[8] = {false};
            const char* dfPathRaw = LakeMetadataText(files, i, 2);
            bool dfIsRel = DatumGetBool(LakeMetadataValue(files, i, 3, BOOLOID));
            char* dfClean = dfIsRel ?
                LakeSanitizePathOrUri(psprintf("%s%s", tablePrefix, dfPathRaw), header->data_root, true) :
                LakeSanitizePathOrUri(dfPathRaw, header->data_root, false);
            vals[0] = CStringGetTextDatum(dfClean);
            vals[1] = LakeMetadataValue(files, i, 4, INT8OID);
            vals[2] = LakeMetadataValue(files, i, 5, INT8OID, true, &nulls[2]);
            /* Encryption keys are never exposed through public status/metadata APIs. */
            nulls[3] = true;

            bool delMissing = false;
            Datum delPathDatum = LakeMetadataValue(files, i, 6, TEXTOID, true, &delMissing);
            if (delMissing) {
                nulls[4] = true;
                nulls[5] = true;
                nulls[6] = true;
                nulls[7] = true;
            } else {
                const char* delPathRaw = TextDatumGetCString(delPathDatum);
                bool delIsRel = DatumGetBool(LakeMetadataValue(files, i, 7, BOOLOID));
                char* delClean = delIsRel ?
                    LakeSanitizePathOrUri(psprintf("%s%s", tablePrefix, delPathRaw), header->data_root, true) :
                    LakeSanitizePathOrUri(delPathRaw, header->data_root, false);
                vals[4] = CStringGetTextDatum(delClean);
                vals[5] = LakeMetadataValue(files, i, 8, INT8OID);
                vals[6] = LakeMetadataValue(files, i, 9, INT8OID, true, &nulls[6]);
                nulls[7] = true;
            }
            tuplestore_putvalues(store, desc, vals, nulls);
        }

        LakeLeaveFastValidationScope(fastScope);
        fastActive = false;
        UnregisterSnapshot(snapshot);
        snapshot = NULL;
        MemoryContextSwitchTo(callerCtx);
        MemoryContextDelete(workCtx);
        workCtx = NULL;

        tuplestore_donestoring(store);
        result->returnMode = SFRM_Materialize;
        result->setResult = store;
        result->setDesc = desc;
    }
    PG_CATCH();
    {
        if (fastActive) {
            LakeLeaveFastValidationScope(fastScope);
            fastActive = false;
        }
        if (snapshot != NULL) {
            UnregisterSnapshot(snapshot);
            snapshot = NULL;
        }
        MemoryContextSwitchTo(callerCtx);
        if (workCtx != NULL) {
            MemoryContextDelete(workCtx);
            workCtx = NULL;
        }
        PG_RE_THROW();
    }
    PG_END_TRY();

    fcinfo->isnull = true;
    return (Datum)0;
}

/* DuckLake-compatible og_lake.options(catalog_name name) */
Datum og_lake_options(PG_FUNCTION_ARGS)
{
    LakeRequirePublicManagementAdmission("options");
    if (PG_ARGISNULL(0))
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("options catalog_name cannot be NULL")));

    ReturnSetInfo* result = (ReturnSetInfo*)fcinfo->resultinfo;
    if (!result || !IsA(result, ReturnSetInfo) || !(result->allowedModes & SFRM_Materialize) || !result->econtext)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("options requires materialize mode")));

    const char* serverName = NameStr(*PG_GETARG_NAME(0));
    ForeignServer* server = GetForeignServerByName(serverName, false);
    if (!LakeTrustedForeignServer(server->serverid))
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg("foreign server \"%s\" is not an og_lake server", serverName)));

    MemoryContext callerCtx = MemoryContextSwitchTo(result->econtext->ecxt_per_query_memory);
    TupleDesc desc = CreateTemplateTupleDesc(5, false);
    TupleDescInitEntry(desc, 1, "option_name", TEXTOID, -1, DEFAULT_COLLATION_OID);
    TupleDescInitEntry(desc, 2, "description", TEXTOID, -1, DEFAULT_COLLATION_OID);
    TupleDescInitEntry(desc, 3, "value", TEXTOID, -1, DEFAULT_COLLATION_OID);
    TupleDescInitEntry(desc, 4, "scope", TEXTOID, -1, DEFAULT_COLLATION_OID);
    TupleDescInitEntry(desc, 5, "scope_entry", TEXTOID, -1, DEFAULT_COLLATION_OID);
    desc = BlessTupleDesc(desc);
    Tuplestorestate* store = tuplestore_begin_heap(true, false,
        Max(64, Min(u_sess->attr.attr_memory.work_mem, 1024)));
    MemoryContextSwitchTo(callerCtx);

    MemoryContext volatile workCtx = AllocSetContextCreate(callerCtx, "Lake options", ALLOCSET_DEFAULT_SIZES);
    Snapshot volatile snapshot = NULL;
    volatile int fastScope = 0;
    volatile bool fastActive = false;

    PG_TRY();
    {
        fastScope = LakeEnterFastValidationScope();
        fastActive = true;
        snapshot = RegisterSnapshot(GetActiveSnapshot());
        MemoryContextSwitchTo(workCtx);

        if (!LakeCallerCanSeeCatalog(server->serverid, snapshot))
            aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_FOREIGN_SERVER, serverName);

        MemoryContext pageCtx = AllocSetContextCreate(workCtx, "Lake options page", ALLOCSET_DEFAULT_SIZES);
        LakeInlineBinding* header = NULL;
        Snapshot useSnap = NULL;
        int64 visibleHead = 0, effSnap = 0, effSchema = 0;
        LakeResolveEffectiveCatalogSnapshot(server->serverid, snapshot, pageCtx,
            &header, &useSnap, &visibleHead, &effSnap, &effSchema, NULL);
        MemoryContextDelete(pageCtx);

        bool isAdmin = superuser();
        Oid srvType = OIDOID;
        Datum srvArg = ObjectIdGetDatum(server->serverid);
        LakeMetaRows* hostMap = LakeMetadataQuery(
            "SELECT t.table_id, r.relation_oid FROM og_lake_internal.table_binding t "
            "JOIN og_lake_internal.relation_binding r USING(binding_id) "
            "WHERE t.server_oid=$1 AND t.state='active' AND t.object_kind='relation'",
            1, &srvType, &srvArg, NULL, useSnap, true, SPI_OK_SELECT);

        Oid snapType = INT8OID;
        Datum snapArg = Int64GetDatum(effSnap);
        const char* metaRel = LakeMetadataRelation(header->metadata_schema_oid, "ducklake_metadata");
        const char* schRel = LakeMetadataRelation(header->metadata_schema_oid, "ducklake_schema");
        const char* tblRel = LakeMetadataRelation(header->metadata_schema_oid, "ducklake_table");
        LakeMetaRows* rows = LakeMetadataQuery(psprintf(
            "SELECT m.key::text, m.value::text, m.scope::text, m.scope_id, "
            "(SELECT s.schema_name::text FROM %s s WHERE m.scope='schema' AND s.schema_id=m.scope_id "
            "AND s.begin_snapshot<=$1 AND (s.end_snapshot IS NULL OR s.end_snapshot>$1)), "
            "(SELECT ts.schema_name::text || '.' || t.table_name::text FROM %s t JOIN %s ts ON ts.schema_id=t.schema_id "
            "AND ts.begin_snapshot<=$1 AND (ts.end_snapshot IS NULL OR ts.end_snapshot>$1) "
            "WHERE m.scope='table' AND t.table_id=m.scope_id "
            "AND t.begin_snapshot<=$1 AND (t.end_snapshot IS NULL OR t.end_snapshot>$1)) "
            "FROM %s m ORDER BY m.key::text, COALESCE(m.scope::text,''), COALESCE(m.scope_id,-1)",
            schRel, tblRel, schRel, metaRel),
            1, &snapType, &snapArg, NULL, useSnap, true, SPI_OK_SELECT, header->metadata_schema_oid);

        for (uint32 i = 0; i < rows->count; ++i) {
            const char* key = LakeMetadataText(rows, i, 1);
            const char* val = LakeMetadataText(rows, i, 2);
            bool scopeNull = false;
            Datum scopeDatum = LakeMetadataValue(rows, i, 3, TEXTOID, true, &scopeNull);
            const char* rawScope = scopeNull ? NULL : TextDatumGetCString(scopeDatum);
            bool scopeIdNull = false;
            Datum scopeIdDatum = LakeMetadataValue(rows, i, 4, INT8OID, true, &scopeIdNull);
            int64 scopeId = scopeIdNull ? -1 : DatumGetInt64(scopeIdDatum);

            bool safeGlobal = false;
            const char* optDesc = LakeOptionDescription(key, &safeGlobal);
            const char* outScope = "GLOBAL";
            const char* scopeEntry = NULL;

            if (rawScope == NULL) {
                if (!isAdmin && !safeGlobal)
                    continue;
                if (pg_strcasecmp(key, "data_path") == 0)
                    val = LakeSanitizePathOrUri(val, NULL, false);
            } else if (pg_strcasecmp(rawScope, "schema") == 0) {
                if (!isAdmin)
                    continue;
                outScope = "SCHEMA";
                bool entryNull = false;
                Datum entryD = LakeMetadataValue(rows, i, 5, TEXTOID, true, &entryNull);
                if (!entryNull)
                    scopeEntry = TextDatumGetCString(entryD);
            } else if (pg_strcasecmp(rawScope, "table") == 0) {
                outScope = "TABLE";
                bool authorized = isAdmin;
                if (!authorized && !scopeIdNull) {
                    for (uint32 m = 0; m < hostMap->count; ++m) {
                        if (DatumGetInt64(LakeMetadataValue(hostMap, m, 1, INT8OID)) == scopeId) {
                            Oid relOid = DatumGetObjectId(LakeMetadataValue(hostMap, m, 2, OIDOID));
                            if (LakeCallerCanSeeRelation(relOid)) {
                                authorized = true;
                                break;
                            }
                        }
                    }
                }
                if (!authorized)
                    continue;
                bool entryNull = false;
                Datum entryD = LakeMetadataValue(rows, i, 6, TEXTOID, true, &entryNull);
                if (!entryNull)
                    scopeEntry = TextDatumGetCString(entryD);
            } else {
                if (!isAdmin)
                    continue;
                outScope = rawScope;
            }

            Datum vals[5] = {0};
            bool nulls[5] = {false};
            vals[0] = CStringGetTextDatum(key);
            if (optDesc != NULL)
                vals[1] = CStringGetTextDatum(optDesc);
            else
                nulls[1] = true;
            vals[2] = CStringGetTextDatum(val);
            vals[3] = CStringGetTextDatum(outScope);
            if (scopeEntry != NULL && *scopeEntry != '\0')
                vals[4] = CStringGetTextDatum(scopeEntry);
            else
                nulls[4] = true;
            tuplestore_putvalues(store, desc, vals, nulls);
        }

        LakeLeaveFastValidationScope(fastScope);
        fastActive = false;
        UnregisterSnapshot(snapshot);
        snapshot = NULL;
        MemoryContextSwitchTo(callerCtx);
        MemoryContextDelete(workCtx);
        workCtx = NULL;

        tuplestore_donestoring(store);
        result->returnMode = SFRM_Materialize;
        result->setResult = store;
        result->setDesc = desc;
    }
    PG_CATCH();
    {
        if (fastActive) {
            LakeLeaveFastValidationScope(fastScope);
            fastActive = false;
        }
        if (snapshot != NULL) {
            UnregisterSnapshot(snapshot);
            snapshot = NULL;
        }
        MemoryContextSwitchTo(callerCtx);
        if (workCtx != NULL) {
            MemoryContextDelete(workCtx);
            workCtx = NULL;
        }
        PG_RE_THROW();
    }
    PG_END_TRY();

    fcinfo->isnull = true;
    return (Datum)0;
}

/* Backing SRF for og_lake.catalog_status */
Datum og_lake_catalog_status_rows(PG_FUNCTION_ARGS)
{
    LakeRequirePublicManagementAdmission("catalog_status");
    ReturnSetInfo* result = (ReturnSetInfo*)fcinfo->resultinfo;
    if (!result || !IsA(result, ReturnSetInfo) || !(result->allowedModes & SFRM_Materialize) || !result->econtext)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("catalog_status requires materialize mode")));

    MemoryContext callerCtx = MemoryContextSwitchTo(result->econtext->ecxt_per_query_memory);
    TupleDesc desc = CreateTemplateTupleDesc(13, false);
    TupleDescInitEntry(desc, 1, "server_name", NAMEOID, -1, 0);
    TupleDescInitEntry(desc, 2, "server_oid", OIDOID, -1, 0);
    TupleDescInitEntry(desc, 3, "catalog_id", UUIDOID, -1, 0);
    TupleDescInitEntry(desc, 4, "format_version", TEXTOID, -1, DEFAULT_COLLATION_OID);
    TupleDescInitEntry(desc, 5, "read_only", BOOLOID, -1, 0);
    TupleDescInitEntry(desc, 6, "credential_configured", BOOLOID, -1, 0);
    TupleDescInitEntry(desc, 7, "index_root_configured", BOOLOID, -1, 0);
    TupleDescInitEntry(desc, 8, "visible_snapshot", INT8OID, -1, 0);
    TupleDescInitEntry(desc, 9, "bound_snapshot", INT8OID, -1, 0);
    TupleDescInitEntry(desc, 10, "last_committed_snapshot", INT8OID, -1, 0);
    TupleDescInitEntry(desc, 11, "bound_table_count", INT8OID, -1, 0);
    TupleDescInitEntry(desc, 12, "auto_maintenance_enabled", BOOLOID, -1, 0);
    TupleDescInitEntry(desc, 13, "capabilities", JSONBOID, -1, 0);
    desc = BlessTupleDesc(desc);
    Tuplestorestate* store = tuplestore_begin_heap(true, false,
        Max(64, Min(u_sess->attr.attr_memory.work_mem, 1024)));
    MemoryContextSwitchTo(callerCtx);

    MemoryContext volatile workCtx = AllocSetContextCreate(callerCtx, "Lake catalog_status", ALLOCSET_DEFAULT_SIZES);
    Snapshot volatile snapshot = NULL;
    volatile int fastScope = 0;
    volatile bool fastActive = false;

    PG_TRY();
    {
        fastScope = LakeEnterFastValidationScope();
        fastActive = true;
        snapshot = RegisterSnapshot(GetActiveSnapshot());
        MemoryContextSwitchTo(workCtx);

        LakeMetaRows* cats = LakeMetadataQuery(
            "SELECT c.server_oid, c.catalog_id, c.format_version::text, c.read_only, "
            "(c.credential_ref IS NOT NULL), (c.index_root IS NOT NULL) "
            "FROM og_lake_internal.catalog_binding c ORDER BY c.server_oid",
            0, NULL, NULL, NULL, snapshot, true, SPI_OK_SELECT);

        MemoryContext pageCtx = AllocSetContextCreate(workCtx, "Lake catalog_status page", ALLOCSET_DEFAULT_SIZES);
        bool bitmapCompiled = false;
#ifdef USE_LAKE_BITMAP
        bitmapCompiled = true;
#endif
        for (uint32 i = 0; i < cats->count; ++i) {
            Oid serverOid = DatumGetObjectId(LakeMetadataValue(cats, i, 1, OIDOID));
            ForeignServer* srv = LakeTryGetForeignServer(serverOid);
            if (srv == NULL || !LakeTrustedForeignServer(serverOid))
                continue;
            if (!LakeCallerCanSeeCatalog(serverOid, snapshot))
                continue;

            LakeInlineBinding* header = NULL;
            Snapshot useSnap = NULL;
            int64 visibleHead = 0, effSnap = 0, effSchema = 0;
            bool isBound = false;
            LakeResolveEffectiveCatalogSnapshot(serverOid, snapshot, pageCtx,
                &header, &useSnap, &visibleHead, &effSnap, &effSchema, &isBound);
            /* Always report true latest committed head in visible_snapshot, plus bound_snapshot separately. */
            int64 latestVisible = isBound ?
                LakeMgmtVisibleHead(LakeLoadCatalogHeader(serverOid, snapshot), snapshot, pageCtx) :
                visibleHead;

            LakeCommittedCatalogKey ckey;
            int64 lastComm = 0;
            bool hasLastComm = LakeCommittedKeyFromBinding(header, &ckey) &&
                LakeHostLookupCommittedSnapshot(&ckey, &lastComm);

            Oid srvType = OIDOID;
            Datum srvArg = ObjectIdGetDatum(serverOid);
            LakeMetaRows* rels = LakeMetadataQuery(
                "SELECT r.relation_oid FROM og_lake_internal.relation_binding r "
                "JOIN og_lake_internal.table_binding t USING(binding_id) "
                "WHERE t.server_oid=$1 AND t.state='active'",
                1, &srvType, &srvArg, NULL, useSnap, true, SPI_OK_SELECT);
            int64 visibleTables = 0;
            for (uint32 r = 0; r < rels->count; ++r) {
                Oid relOid = DatumGetObjectId(LakeMetadataValue(rels, r, 1, OIDOID));
                if (LakeCallerCanSeeRelation(relOid))
                    ++visibleTables;
            }

            bool catReadOnly = DatumGetBool(LakeMetadataValue(cats, i, 4, BOOLOID));
            bool idxRootSet = DatumGetBool(LakeMetadataValue(cats, i, 6, BOOLOID));
            char* capJson = psprintf(
                "{\"inline_dml\":%s,\"parquet_read\":true,\"flush\":%s,\"rewrite\":%s,\"merge_adjacent_files\":%s,"
                "\"time_travel\":true,\"table_changes\":true,\"bind_snapshot\":true,"
                "\"logical_expiry\":%s,\"scalar_index\":true,\"bitmap_index\":%s,"
                "\"diskann_index\":true,\"pq_compiled\":%s,\"file_index_ready\":%s,\"physical_gc\":%s,"
                "\"widening_alter_type\":%s,\"ops_tools\":true}",
                (catReadOnly || isBound) ? "false" : "true",
                (catReadOnly || isBound) ? "false" : "true",
                (catReadOnly || isBound) ? "false" : "true",
                (catReadOnly || isBound) ? "false" : "true",
                (catReadOnly || isBound) ? "false" : "true",
                bitmapCompiled ? "true" : "false",
                LakePqCompiled() ? "true" : "false",
                idxRootSet ? "true" : "false",
                (!catReadOnly && !isBound && LakePhysicalCleanupVersionEnabled()) ? "true" : "false",
                (!catReadOnly && !isBound && LakeAlterTypeOpsVersionEnabled()) ? "true" : "false");

            NameData srvName;
            namestrcpy(&srvName, srv->servername);
            Datum vals[13] = {0};
            bool nulls[13] = {false};
            vals[0] = NameGetDatum(&srvName);
            vals[1] = ObjectIdGetDatum(serverOid);
            vals[2] = LakeMetadataValue(cats, i, 2, UUIDOID);
            vals[3] = CStringGetTextDatum(LakeMetadataText(cats, i, 3));
            vals[4] = BoolGetDatum(catReadOnly || isBound);
            vals[5] = LakeMetadataValue(cats, i, 5, BOOLOID);
            vals[6] = BoolGetDatum(idxRootSet);
            vals[7] = Int64GetDatum(latestVisible);
            if (isBound)
                vals[8] = Int64GetDatum(effSnap);
            else
                nulls[8] = true;
            if (hasLastComm)
                vals[9] = Int64GetDatum(lastComm);
            else
                nulls[9] = true;
            vals[10] = Int64GetDatum(visibleTables);
            vals[11] = BoolGetDatum(g_instance.attr.attr_storage.enable_lake_maintenance);
            vals[12] = DirectFunctionCall1(jsonb_in, CStringGetDatum(capJson));
            tuplestore_putvalues(store, desc, vals, nulls);
        }
        MemoryContextDelete(pageCtx);

        LakeLeaveFastValidationScope(fastScope);
        fastActive = false;
        UnregisterSnapshot(snapshot);
        snapshot = NULL;
        MemoryContextSwitchTo(callerCtx);
        MemoryContextDelete(workCtx);
        workCtx = NULL;

        tuplestore_donestoring(store);
        result->returnMode = SFRM_Materialize;
        result->setResult = store;
        result->setDesc = desc;
    }
    PG_CATCH();
    {
        if (fastActive) {
            LakeLeaveFastValidationScope(fastScope);
            fastActive = false;
        }
        if (snapshot != NULL) {
            UnregisterSnapshot(snapshot);
            snapshot = NULL;
        }
        MemoryContextSwitchTo(callerCtx);
        if (workCtx != NULL) {
            MemoryContextDelete(workCtx);
            workCtx = NULL;
        }
        PG_RE_THROW();
    }
    PG_END_TRY();

    fcinfo->isnull = true;
    return (Datum)0;
}

/* Backing SRF for og_lake.table_status */
Datum og_lake_table_status_rows(PG_FUNCTION_ARGS)
{
    LakeRequirePublicManagementAdmission("table_status");
    ReturnSetInfo* result = (ReturnSetInfo*)fcinfo->resultinfo;
    if (!result || !IsA(result, ReturnSetInfo) || !(result->allowedModes & SFRM_Materialize) || !result->econtext)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("table_status requires materialize mode")));

    MemoryContext callerCtx = MemoryContextSwitchTo(result->econtext->ecxt_per_query_memory);
    TupleDesc desc = CreateTemplateTupleDesc(25, false);
    TupleDescInitEntry(desc, 1, "server_name", NAMEOID, -1, 0);
    TupleDescInitEntry(desc, 2, "relation", REGCLASSOID, -1, 0);
    TupleDescInitEntry(desc, 3, "schema_name", NAMEOID, -1, 0);
    TupleDescInitEntry(desc, 4, "table_name", NAMEOID, -1, 0);
    TupleDescInitEntry(desc, 5, "table_id", INT8OID, -1, 0);
    TupleDescInitEntry(desc, 6, "table_uuid", UUIDOID, -1, 0);
    TupleDescInitEntry(desc, 7, "object_kind", TEXTOID, -1, DEFAULT_COLLATION_OID);
    TupleDescInitEntry(desc, 8, "read_only", BOOLOID, -1, 0);
    TupleDescInitEntry(desc, 9, "effective_snapshot", INT8OID, -1, 0);
    TupleDescInitEntry(desc, 10, "schema_version", INT8OID, -1, 0);
    TupleDescInitEntry(desc, 11, "active_data_files", INT8OID, -1, 0);
    TupleDescInitEntry(desc, 12, "active_data_bytes", INT8OID, -1, 0);
    TupleDescInitEntry(desc, 13, "active_delete_files", INT8OID, -1, 0);
    TupleDescInitEntry(desc, 14, "active_delete_bytes", INT8OID, -1, 0);
    TupleDescInitEntry(desc, 15, "inline_carrier_count", INT4OID, -1, 0);
    TupleDescInitEntry(desc, 16, "inline_row_limit", INT8OID, -1, 0);
    TupleDescInitEntry(desc, 17, "partition_keys", INT4OID, -1, 0);
    TupleDescInitEntry(desc, 18, "active_indexes", INT8OID, -1, 0);
    TupleDescInitEntry(desc, 19, "flush_round_active", BOOLOID, -1, 0);
    TupleDescInitEntry(desc, 20, "reclaimed_row_heads", INT8OID, -1, 0);
    TupleDescInitEntry(desc, 21, "analyze_committed_changes", INT8OID, -1, 0);
    TupleDescInitEntry(desc, 22, "analyze_analyzed_changes", INT8OID, -1, 0);
    TupleDescInitEntry(desc, 23, "analyze_snapshot", INT8OID, -1, 0);
    TupleDescInitEntry(desc, 24, "analyze_last_success", TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(desc, 25, "analyze_last_error", INT4OID, -1, 0);
    desc = BlessTupleDesc(desc);
    Tuplestorestate* store = tuplestore_begin_heap(true, false,
        Max(64, Min(u_sess->attr.attr_memory.work_mem, 1024)));
    MemoryContextSwitchTo(callerCtx);

    MemoryContext volatile workCtx = AllocSetContextCreate(callerCtx, "Lake table_status", ALLOCSET_DEFAULT_SIZES);
    Snapshot volatile snapshot = NULL;
    volatile int fastScope = 0;
    volatile bool fastActive = false;

    PG_TRY();
    {
        fastScope = LakeEnterFastValidationScope();
        fastActive = true;
        snapshot = RegisterSnapshot(GetActiveSnapshot());
        MemoryContextSwitchTo(workCtx);

        LakeMetaRows* bindings = LakeMetadataQuery(
            "SELECT t.binding_id, t.server_oid, r.relation_oid, t.table_id, t.table_uuid, "
            "t.object_kind::text, (t.read_only OR c.read_only), (t.flush_birth_cutoff IS NOT NULL), "
            "(SELECT count(*)::bigint FROM og_lake_internal.index_definition d WHERE d.binding_id=t.binding_id AND d.state='ACTIVE'), "
            "(SELECT COALESCE(sum(h.reclaimed_rows),0)::bigint FROM og_lake_internal.row_lock_binding h WHERE h.binding_id=t.binding_id), "
            "a.committed_changes, a.analyzed_changes, a.analyzed_snapshot, a.last_success, a.last_error "
            "FROM og_lake_internal.table_binding t "
            "JOIN og_lake_internal.catalog_binding c USING(server_oid) "
            "JOIN og_lake_internal.relation_binding r USING(binding_id) "
            "LEFT JOIN og_lake_internal.analyze_state a USING(binding_id) "
            "WHERE t.state='active' ORDER BY t.server_oid, t.table_id",
            0, NULL, NULL, NULL, snapshot, true, SPI_OK_SELECT);

        MemoryContext pageCtx = AllocSetContextCreate(workCtx, "Lake table_status page", ALLOCSET_DEFAULT_SIZES);
        for (uint32 i = 0; i < bindings->count; ++i) {
            Oid serverOid = DatumGetObjectId(LakeMetadataValue(bindings, i, 2, OIDOID));
            Oid relOid = DatumGetObjectId(LakeMetadataValue(bindings, i, 3, OIDOID));
            ForeignServer* srv = LakeTryGetForeignServer(serverOid);
            if (srv == NULL || !LakeTrustedForeignServer(serverOid))
                continue;
            if (!LakeCallerCanSeeRelation(relOid))
                continue;
            Relation rel = try_relation_open(relOid, AccessShareLock);
            if (rel == NULL)
                continue;
            NameData schName, tblName, srvName;
            namestrcpy(&srvName, srv->servername);
            namestrcpy(&schName, get_namespace_name(RelationGetNamespace(rel)));
            namestrcpy(&tblName, RelationGetRelationName(rel));
            relation_close(rel, NoLock);

            LakeInlineBinding* header = NULL;
            Snapshot useSnap = NULL;
            int64 visibleHead = 0, effSnap = 0, effSchema = 0;
            bool isBound = false;
            LakeResolveEffectiveCatalogSnapshot(serverOid, snapshot, pageCtx,
                &header, &useSnap, &visibleHead, &effSnap, &effSchema, &isBound);

            int64 tableId = DatumGetInt64(LakeMetadataValue(bindings, i, 4, INT8OID));
            Oid qTypes[3] = {INT8OID, INT8OID, INT8OID};
            Datum qArgs[3] = {Int64GetDatum(tableId), Int64GetDatum(effSnap), Int64GetDatum(effSchema)};
            const char* dfRel = LakeMetadataRelation(header->metadata_schema_oid, "ducklake_data_file");
            const char* delRel = LakeMetadataRelation(header->metadata_schema_oid, "ducklake_delete_file");
            const char* inlRel = LakeMetadataRelation(header->metadata_schema_oid, "ducklake_inlined_data_tables");
            const char* metaRel = LakeMetadataRelation(header->metadata_schema_oid, "ducklake_metadata");
            const char* piRel = LakeMetadataRelation(header->metadata_schema_oid, "ducklake_partition_info");
            const char* pcRel = LakeMetadataRelation(header->metadata_schema_oid, "ducklake_partition_column");
            LakeMetaRows* stats = LakeMetadataQuery(psprintf(
                "SELECT "
                "(SELECT count(*)::bigint FROM %s df WHERE df.table_id=$1 AND df.begin_snapshot<=$2 AND (df.end_snapshot IS NULL OR df.end_snapshot>$2)), "
                "(SELECT COALESCE(sum(df.file_size_bytes),0)::bigint FROM %s df WHERE df.table_id=$1 AND df.begin_snapshot<=$2 AND (df.end_snapshot IS NULL OR df.end_snapshot>$2)), "
                "(SELECT count(*)::bigint FROM %s dl WHERE dl.table_id=$1 AND dl.begin_snapshot<=$2 AND (dl.end_snapshot IS NULL OR dl.end_snapshot>$2)), "
                "(SELECT COALESCE(sum(dl.file_size_bytes),0)::bigint FROM %s dl WHERE dl.table_id=$1 AND dl.begin_snapshot<=$2 AND (dl.end_snapshot IS NULL OR dl.end_snapshot>$2)), "
                "(SELECT count(*)::integer FROM %s it WHERE it.table_id=$1 AND it.schema_version<=$3), "
                "COALESCE((SELECT m.value::bigint FROM %s m WHERE m.key='data_inlining_row_limit' AND m.scope='table' AND m.scope_id=$1 LIMIT 1), "
                "         (SELECT m.value::bigint FROM %s m WHERE m.key='data_inlining_row_limit' AND m.scope IS NULL LIMIT 1)), "
                "(SELECT count(*)::integer FROM %s pi JOIN %s pc ON pc.partition_id=pi.partition_id AND pc.table_id=pi.table_id "
                " WHERE pi.table_id=$1 AND pi.begin_snapshot<=$2 AND (pi.end_snapshot IS NULL OR pi.end_snapshot>$2))",
                dfRel, dfRel, delRel, delRel, inlRel, metaRel, metaRel, piRel, pcRel),
                3, qTypes, qArgs, NULL, useSnap, true, SPI_OK_SELECT, header->metadata_schema_oid);
            LakeMetadataRequireCount(stats, 1, "table_status metadata metrics");

            Datum vals[25] = {0};
            bool nulls[25] = {false};
            vals[0] = NameGetDatum(&srvName);
            vals[1] = ObjectIdGetDatum(relOid);
            vals[2] = NameGetDatum(&schName);
            vals[3] = NameGetDatum(&tblName);
            vals[4] = Int64GetDatum(tableId);
            vals[5] = LakeMetadataValue(bindings, i, 5, UUIDOID);
            vals[6] = CStringGetTextDatum(LakeMetadataText(bindings, i, 6));
            vals[7] = BoolGetDatum(DatumGetBool(LakeMetadataValue(bindings, i, 7, BOOLOID)) || isBound);
            vals[8] = Int64GetDatum(effSnap);
            vals[9] = Int64GetDatum(effSchema);
            vals[10] = LakeMetadataValue(stats, 0, 1, INT8OID);
            vals[11] = LakeMetadataValue(stats, 0, 2, INT8OID);
            vals[12] = LakeMetadataValue(stats, 0, 3, INT8OID);
            vals[13] = LakeMetadataValue(stats, 0, 4, INT8OID);
            vals[14] = LakeMetadataValue(stats, 0, 5, INT4OID);
            vals[15] = LakeMetadataValue(stats, 0, 6, INT8OID, true, &nulls[15]);
            vals[16] = LakeMetadataValue(stats, 0, 7, INT4OID);
            vals[17] = LakeMetadataValue(bindings, i, 9, INT8OID);
            vals[18] = LakeMetadataValue(bindings, i, 8, BOOLOID);
            vals[19] = LakeMetadataValue(bindings, i, 10, INT8OID);
            vals[20] = LakeMetadataValue(bindings, i, 11, INT8OID, true, &nulls[20]);
            vals[21] = LakeMetadataValue(bindings, i, 12, INT8OID, true, &nulls[21]);
            vals[22] = LakeMetadataValue(bindings, i, 13, INT8OID, true, &nulls[22]);
            vals[23] = LakeMetadataValue(bindings, i, 14, TIMESTAMPTZOID, true, &nulls[23]);
            vals[24] = LakeMetadataValue(bindings, i, 15, INT4OID, true, &nulls[24]);
            tuplestore_putvalues(store, desc, vals, nulls);
        }
        MemoryContextDelete(pageCtx);

        LakeLeaveFastValidationScope(fastScope);
        fastActive = false;
        UnregisterSnapshot(snapshot);
        snapshot = NULL;
        MemoryContextSwitchTo(callerCtx);
        MemoryContextDelete(workCtx);
        workCtx = NULL;

        tuplestore_donestoring(store);
        result->returnMode = SFRM_Materialize;
        result->setResult = store;
        result->setDesc = desc;
    }
    PG_CATCH();
    {
        if (fastActive) {
            LakeLeaveFastValidationScope(fastScope);
            fastActive = false;
        }
        if (snapshot != NULL) {
            UnregisterSnapshot(snapshot);
            snapshot = NULL;
        }
        MemoryContextSwitchTo(callerCtx);
        if (workCtx != NULL) {
            MemoryContextDelete(workCtx);
            workCtx = NULL;
        }
        PG_RE_THROW();
    }
    PG_END_TRY();

    fcinfo->isnull = true;
    return (Datum)0;
}

/* Backing SRF for og_lake.index_status */
Datum og_lake_index_status_rows(PG_FUNCTION_ARGS)
{
    LakeRequirePublicManagementAdmission("index_status");
    ReturnSetInfo* result = (ReturnSetInfo*)fcinfo->resultinfo;
    if (!result || !IsA(result, ReturnSetInfo) || !(result->allowedModes & SFRM_Materialize) || !result->econtext)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("index_status requires materialize mode")));

    MemoryContext callerCtx = MemoryContextSwitchTo(result->econtext->ecxt_per_query_memory);
    TupleDesc desc = CreateTemplateTupleDesc(21, false);
    TupleDescInitEntry(desc, 1, "index_uuid", UUIDOID, -1, 0);
    TupleDescInitEntry(desc, 2, "server_name", NAMEOID, -1, 0);
    TupleDescInitEntry(desc, 3, "relation", REGCLASSOID, -1, 0);
    TupleDescInitEntry(desc, 4, "index_name", NAMEOID, -1, 0);
    TupleDescInitEntry(desc, 5, "state", TEXTOID, -1, DEFAULT_COLLATION_OID);
    TupleDescInitEntry(desc, 6, "kind", TEXTOID, -1, DEFAULT_COLLATION_OID);
    TupleDescInitEntry(desc, 7, "method", TEXTOID, -1, DEFAULT_COLLATION_OID);
    TupleDescInitEntry(desc, 8, "metric", TEXTOID, -1, DEFAULT_COLLATION_OID);
    TupleDescInitEntry(desc, 9, "column_id", INT8OID, -1, 0);
    TupleDescInitEntry(desc, 10, "column_name", NAMEOID, -1, 0);
    TupleDescInitEntry(desc, 11, "definition_generation", INT8OID, -1, 0);
    TupleDescInitEntry(desc, 12, "registered_snapshot", INT8OID, -1, 0);
    TupleDescInitEntry(desc, 13, "inline_index_ready", BOOLOID, -1, 0);
    TupleDescInitEntry(desc, 14, "ready_segments", INT8OID, -1, 0);
    TupleDescInitEntry(desc, 15, "building_segments", INT8OID, -1, 0);
    TupleDescInitEntry(desc, 16, "failed_segments", INT8OID, -1, 0);
    TupleDescInitEntry(desc, 17, "retired_segments", INT8OID, -1, 0);
    TupleDescInitEntry(desc, 18, "covered_files", INT4OID, -1, 0);
    TupleDescInitEntry(desc, 19, "uncovered_files", INT4OID, -1, 0);
    TupleDescInitEntry(desc, 20, "usable", BOOLOID, -1, 0);
    TupleDescInitEntry(desc, 21, "reason", TEXTOID, -1, DEFAULT_COLLATION_OID);
    desc = BlessTupleDesc(desc);
    Tuplestorestate* store = tuplestore_begin_heap(true, false,
        Max(64, Min(u_sess->attr.attr_memory.work_mem, 1024)));
    MemoryContextSwitchTo(callerCtx);

    MemoryContext volatile workCtx = AllocSetContextCreate(callerCtx, "Lake index_status", ALLOCSET_DEFAULT_SIZES);
    Snapshot volatile snapshot = NULL;
    volatile int fastScope = 0;
    volatile bool fastActive = false;

    PG_TRY();
    {
        fastScope = LakeEnterFastValidationScope();
        fastActive = true;
        snapshot = RegisterSnapshot(GetActiveSnapshot());
        MemoryContextSwitchTo(workCtx);

        (void)LakeIndexCatalog();
        bool isAdmin = superuser();
        LakeMetaRows* rows = LakeMetadataQuery(
            "SELECT d.index_uuid, d.index_uuid::text, d.server_oid, r.relation_oid, d.index_name::text, "
            "d.state::text, COALESCE(d.definition->>'kind',''), COALESCE(d.definition->>'method',''), "
            "d.definition->>'metric', d.column_id, d.type_oid, d.definition_generation, d.registered_snapshot, "
            "d.inline_index_oid, "
            "(SELECT count(*)::bigint FROM og_lake_internal.index_segment s WHERE s.index_uuid=d.index_uuid AND s.definition_generation=d.definition_generation AND s.state='READY'), "
            "(SELECT count(*)::bigint FROM og_lake_internal.index_segment s WHERE s.index_uuid=d.index_uuid AND s.definition_generation=d.definition_generation AND s.state='BUILDING'), "
            "(SELECT count(*)::bigint FROM og_lake_internal.index_segment s WHERE s.index_uuid=d.index_uuid AND s.definition_generation=d.definition_generation AND s.state='FAILED'), "
            "(SELECT count(*)::bigint FROM og_lake_internal.index_segment s WHERE s.index_uuid=d.index_uuid AND s.state='RETIRED'), "
            "(SELECT c.attnum::integer FROM og_lake_internal.column_binding c WHERE c.binding_id=d.binding_id AND c.column_id=d.column_id), "
            "d.table_id "
            "FROM og_lake_internal.index_definition d "
            "LEFT JOIN og_lake_internal.relation_binding r USING(binding_id) "
            "ORDER BY d.server_oid, d.index_name, d.index_uuid",
            0, NULL, NULL, NULL, snapshot, true, SPI_OK_SELECT);

        for (uint32 i = 0; i < rows->count; ++i) {
            Oid serverOid = DatumGetObjectId(LakeMetadataValue(rows, i, 3, OIDOID));
            bool relMissing = false;
            Datum relDatum = LakeMetadataValue(rows, i, 4, OIDOID, true, &relMissing);
            Oid relOid = relMissing ? InvalidOid : DatumGetObjectId(relDatum);
            if (!OidIsValid(relOid)) {
                if (!isAdmin)
                    continue;
            } else {
                if (!LakeCallerCanSeeRelation(relOid))
                    continue;
            }

            ForeignServer* srv = LakeTryGetForeignServer(serverOid);
            NameData srvName, idxName, colName;
            namestrcpy(&srvName, srv ? srv->servername : "");
            namestrcpy(&idxName, LakeMetadataText(rows, i, 5));
            bool colNameNull = true;
            bool attMissing = false;
            Datum attDatum = LakeMetadataValue(rows, i, 19, INT4OID, true, &attMissing);
            if (OidIsValid(relOid) && !attMissing) {
                Relation host = try_relation_open(relOid, AccessShareLock);
                if (host != NULL) {
                    int32 attno = DatumGetInt32(attDatum);
                    TupleDesc hdesc = RelationGetDescr(host);
                    if (attno >= 1 && attno <= hdesc->natts && !hdesc->attrs[attno - 1].attisdropped) {
                        namestrcpy(&colName, NameStr(hdesc->attrs[attno - 1].attname));
                        colNameNull = false;
                    }
                    relation_close(host, NoLock);
                }
            }

            const char* uuidText = LakeMetadataText(rows, i, 2);
            const char* state = LakeMetadataText(rows, i, 6);
            const char* kind = LakeMetadataText(rows, i, 7);
            const char* method = LakeMetadataText(rows, i, 8);
            bool metricNull = false;
            Datum metricDatum = LakeMetadataValue(rows, i, 9, TEXTOID, true, &metricNull);
            int64 colId = DatumGetInt64(LakeMetadataValue(rows, i, 10, INT8OID));
            Oid defType = DatumGetObjectId(LakeMetadataValue(rows, i, 11, OIDOID));
            int64 defGen = DatumGetInt64(LakeMetadataValue(rows, i, 12, INT8OID));
            int64 regSnap = DatumGetInt64(LakeMetadataValue(rows, i, 13, INT8OID));
            bool inlineMissing = false;
            Datum inlineDatum = LakeMetadataValue(rows, i, 14, OIDOID, true, &inlineMissing);
            int64 readySegs = DatumGetInt64(LakeMetadataValue(rows, i, 15, INT8OID));
            int64 buildingSegs = DatumGetInt64(LakeMetadataValue(rows, i, 16, INT8OID));
            int64 failedSegs = DatumGetInt64(LakeMetadataValue(rows, i, 17, INT8OID));
            int64 retiredSegs = DatumGetInt64(LakeMetadataValue(rows, i, 18, INT8OID));

            bool isBound = (LakeTxnGetBoundCatalog(serverOid) != NULL);
            bool ann = strcmp(kind, "vector") == 0 && strcmp(method, "diskann") == 0 && defType == VECTOROID;
            bool bitmap = strcmp(kind, "scalar") == 0 && strcmp(method, "bitmap") == 0;
            bool scalar = strcmp(kind, "scalar") == 0 && LakeScalarIndexedType(defType) && strcmp(method, "btree") == 0;
#ifdef USE_LAKE_BITMAP
            scalar = scalar || (bitmap && LakeScalarIndexedType(defType));
#endif
            bool protocol = ann || scalar;
            int fileCount = 0, coveredFiles = 0;
            bool inlineUsable = false;

            if (!isBound && protocol && strcmp(state, "ACTIVE") == 0 && OidIsValid(relOid)) {
                LakeInlineBinding* binding = LakeLoadCommittedInlineBinding(relOid, snapshot);
                fileCount = binding->file_count;
                for (int c = 0; c < binding->column_count; ++c) {
                    if (binding->columns[c].column_id == colId && binding->columns[c].type == defType) {
                        if (!inlineMissing) {
                            inlineUsable = ann ?
                                LakeInlineAnnAvailable(binding, uuidText, DatumGetObjectId(inlineDatum), c + 1,
                                    LakeAnnMetric(metricNull ? "" : TextDatumGetCString(metricDatum)), snapshot) :
                                LakeInlineIndexAvailable(binding, uuidText, DatumGetObjectId(inlineDatum), c + 1, snapshot);
                        }
                        if (LakeHasIndexRootVersion()) {
                            double physRows = 0, fileBytes = 0, rowGroups = 0;
                            if (ann)
                                LakeAnnFileCoverageEstimate(binding, uuidText, c + 1, snapshot,
                                    &physRows, &fileBytes, &rowGroups, &coveredFiles);
                            else
                                LakeScalarFileCoverageEstimate(binding, uuidText, c + 1, snapshot,
                                    &physRows, &fileBytes, &rowGroups, &coveredFiles);
                        }
                    }
                }
            } else if (isBound && OidIsValid(relOid) && srv != NULL && LakeTrustedForeignServer(serverOid)) {
                LakeTxnCatalog* bcat = LakeTxnGetBoundCatalog(serverOid);
                LakeInlineBinding* header = LakeLoadCatalogHeader(serverOid, bcat->business_snapshot);
                int64 tableId = DatumGetInt64(LakeMetadataValue(rows, i, 20, INT8OID));
                Oid qTypes[2] = {INT8OID, INT8OID};
                Datum qArgs[2] = {Int64GetDatum(tableId), Int64GetDatum(bcat->bound_snapshot_id)};
                LakeMetaRows* fcnt = LakeMetadataQuery(psprintf(
                    "SELECT count(*)::integer FROM %s df WHERE df.table_id=$1 "
                    "AND df.begin_snapshot<=$2 AND (df.end_snapshot IS NULL OR df.end_snapshot>$2)",
                    LakeMetadataRelation(header->metadata_schema_oid, "ducklake_data_file")),
                    2, qTypes, qArgs, NULL, bcat->business_snapshot, true, SPI_OK_SELECT, header->metadata_schema_oid);
                LakeMetadataRequireCount(fcnt, 1, "bound index_status file count");
                fileCount = DatumGetInt32(LakeMetadataValue(fcnt, 0, 1, INT4OID));
            }

            bool fileReady = !isBound && coveredFiles > 0;
            bool usable = !isBound && (inlineUsable || (fileReady && (!ann || u_sess->attr.attr_sql.enable_lake_ann)));
            const char* reason = NULL;
            if (strcmp(state, "ACTIVE") != 0)
                reason = "index_definition_retired";
            else if (isBound)
                reason = "bound_snapshot_historical_scan_fallback";
            else if (!protocol)
                reason = "index_method_or_type_unavailable";
            else if (failedSegs > 0 && !usable)
                reason = "failed_segments_present";
            else if (buildingSegs > 0 && !usable)
                reason = "building_segments_in_progress";
            else if (fileReady)
                reason = ann ? (u_sess->attr.attr_sql.enable_lake_ann ?
                    "ann_file_coverage_requires_runtime_validation" : "ann_disabled") :
                    (bitmap ? "bitmap_file_coverage_requires_runtime_validation" :
                        "scalar_file_coverage_requires_runtime_validation");
            else if (inlineMissing)
                reason = "inline_index_not_built";
            else if (inlineUsable)
                reason = ann ? "inline_diskann_candidates_with_complete_file_private_fallback" :
                    "inline_btree_with_complete_scan_fallback";
            else
                reason = ann ? "ann_disabled_or_native_index_snapshot_horizon" : "native_index_snapshot_horizon";

            Datum vals[21] = {0};
            bool nulls[21] = {false};
            vals[0] = LakeMetadataValue(rows, i, 1, UUIDOID);
            vals[1] = NameGetDatum(&srvName);
            if (OidIsValid(relOid))
                vals[2] = ObjectIdGetDatum(relOid);
            else
                nulls[2] = true;
            vals[3] = NameGetDatum(&idxName);
            vals[4] = CStringGetTextDatum(state);
            vals[5] = CStringGetTextDatum(kind);
            vals[6] = CStringGetTextDatum(method);
            if (metricNull)
                nulls[7] = true;
            else
                vals[7] = metricDatum;
            vals[8] = Int64GetDatum(colId);
            if (colNameNull)
                nulls[9] = true;
            else
                vals[9] = NameGetDatum(&colName);
            vals[10] = Int64GetDatum(defGen);
            vals[11] = Int64GetDatum(regSnap);
            vals[12] = BoolGetDatum(!inlineMissing);
            vals[13] = Int64GetDatum(readySegs);
            vals[14] = Int64GetDatum(buildingSegs);
            vals[15] = Int64GetDatum(failedSegs);
            vals[16] = Int64GetDatum(retiredSegs);
            vals[17] = Int32GetDatum(coveredFiles);
            vals[18] = Int32GetDatum(Max(0, fileCount - coveredFiles));
            vals[19] = BoolGetDatum(usable);
            vals[20] = CStringGetTextDatum(reason);
            tuplestore_putvalues(store, desc, vals, nulls);
        }

        LakeLeaveFastValidationScope(fastScope);
        fastActive = false;
        UnregisterSnapshot(snapshot);
        snapshot = NULL;
        MemoryContextSwitchTo(callerCtx);
        MemoryContextDelete(workCtx);
        workCtx = NULL;

        tuplestore_donestoring(store);
        result->returnMode = SFRM_Materialize;
        result->setResult = store;
        result->setDesc = desc;
    }
    PG_CATCH();
    {
        if (fastActive) {
            LakeLeaveFastValidationScope(fastScope);
            fastActive = false;
        }
        if (snapshot != NULL) {
            UnregisterSnapshot(snapshot);
            snapshot = NULL;
        }
        MemoryContextSwitchTo(callerCtx);
        if (workCtx != NULL) {
            MemoryContextDelete(workCtx);
            workCtx = NULL;
        }
        PG_RE_THROW();
    }
    PG_END_TRY();

    fcinfo->isnull = true;
    return (Datum)0;
}
