/* Native read-only table change-data-capture feeds (SQL0.58):
 *   og_lake.table_insertions(regclass, bigint, bigint / timestamptz, timestamptz)
 *   og_lake.table_deletions(regclass, bigint, bigint / timestamptz, timestamptz)
 *   og_lake.table_changes(regclass, bigint, bigint / timestamptz, timestamptz)
 *
 * Snapshot bound, schema projection, same-snapshot insert+delete suppression,
 * cumulative/embedded/inlined/full-file deletion attribution, rewrite_delete
 * exclusion, and preimage/postimage classification adapted from DuckLake
 * 0aaa8e10d4adfe2ca37da77134e991d20106d0e7 (MIT; notice in lake_catalog_ddl.inc):
 *   functions/ducklake_table_insertions.cpp
 *   functions/ducklake_table_changes.cpp
 *   storage/ducklake_metadata_manager.cpp (GetSnapshot, GetTableInsertions,
 *     GetTableDeletions, GetTableRewriteSnapshots)
 *   storage/ducklake_delete_filter.cpp
 * Host adaptation: materialized FROM-record SRF with strict expectedDesc check
 * against end_snapshot historical schema before scanning, single pinned RC
 * statement snapshot, 64-bit own-XID exclusion, extant authorized regclass ACL
 * (schema USAGE + whole-table or complete live-column SELECT), and journal-free
 * reads across inline carriers, Parquet files, delete files and inline deletes. */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "lake_uuid.h"
#include "lake_json.h"
#include "lake_binary.h"
#include "lake_column_layout.h"
#include "lake_column_default.h"
#include "lake_temporal_text.h"
#include "lake_types.h"
#include "lake_temporal.h"
#include "lake_vector_codec.h"
#include "lake_inline_private.h"
#include "lake_inline_reader.h"
#include "lake_inline_sources.h"
#include "lake_file_sdk.h"
#include "lake_file_reader.h"
#include "lake_native_partition.h"
#include "lake_txn_private.h"
#include "lake_version.h"
#include "access/heapam.h"
#include "access/og_lake.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "commands/sec_rls_cmds.h"
#include "distributelayer/streamCore.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "pgxc/pgxc.h"
#include "storage/lmgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/sec_rls_utils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/tuplestore.h"

extern "C" Datum og_lake_table_insertions_version(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_table_insertions_timestamp(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_table_deletions_version(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_table_deletions_timestamp(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_table_changes_version(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_table_changes_timestamp(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(og_lake_table_insertions_version);
PG_FUNCTION_INFO_V1(og_lake_table_insertions_timestamp);
PG_FUNCTION_INFO_V1(og_lake_table_deletions_version);
PG_FUNCTION_INFO_V1(og_lake_table_deletions_timestamp);
PG_FUNCTION_INFO_V1(og_lake_table_changes_version);
PG_FUNCTION_INFO_V1(og_lake_table_changes_timestamp);

enum LakeCdcMode {
    LAKE_CDC_INSERTIONS = 0,
    LAKE_CDC_DELETIONS = 1,
    LAKE_CDC_CHANGES = 2
};

struct LakeCdcKey {
    int64 snapshot_id;
    int64 row_id;
};

struct LakeCdcKeyEntry {
    LakeCdcKey key;
    bool matched;
};

struct LakeCdcPosSnap {
    int64 position;
    int64 snapshot_id;
};

static const long LAKE_CDC_KEY_LIMIT = 4194304L;

static const char* LakeCdcFuncName(LakeCdcMode mode)
{
    if (mode == LAKE_CDC_INSERTIONS)
        return "table_insertions";
    if (mode == LAKE_CDC_DELETIONS)
        return "table_deletions";
    return "table_changes";
}

static bool LakeCdcOwnRow(const LakeMetaRows* rows, uint32 row, int column)
{
    return TransactionIdIsCurrentTransactionId(
        DatumGetTransactionId(LakeMetadataValue(rows, row, column, XIDOID)));
}

static int LakeCdcCompareInt64(const void* lhs, const void* rhs)
{
    int64 a = *(const int64*)lhs;
    int64 b = *(const int64*)rhs;
    return a < b ? -1 : (a > b ? 1 : 0);
}

static bool LakeCdcInSnapSet(const int64* snaps, int count, int64 target)
{
    if (count <= 0 || snaps == NULL)
        return false;
    return bsearch(&target, snaps, (size_t)count, sizeof(int64), LakeCdcCompareInt64) != NULL;
}

static int LakeCdcComparePosSnap(const void* lhs, const void* rhs)
{
    const LakeCdcPosSnap* a = (const LakeCdcPosSnap*)lhs;
    const LakeCdcPosSnap* b = (const LakeCdcPosSnap*)rhs;
    if (a->position != b->position)
        return a->position < b->position ? -1 : 1;
    return a->snapshot_id < b->snapshot_id ? -1 : (a->snapshot_id > b->snapshot_id ? 1 : 0);
}

static bool LakeCdcHasPosSnap(const LakeCdcPosSnap* pairs, int count, int64 position, int64 snapshotId)
{
    if (count <= 0 || pairs == NULL)
        return false;
    LakeCdcPosSnap needle = {position, snapshotId};
    return bsearch(&needle, pairs, (size_t)count, sizeof(LakeCdcPosSnap), LakeCdcComparePosSnap) != NULL;
}

static int64 LakeCdcVisibleHead(LakeInlineBinding* header, Snapshot snapshot, MemoryContext page)
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
            if (!LakeCdcOwnRow(rows, i, 3)) {
                MemoryContextSwitchTo(parent);
                MemoryContextReset(page);
                return id;
            }
            sawOwn = true;
        }
        MemoryContextSwitchTo(parent);
    }
}

static void LakeCdcResolveVersion(LakeInlineBinding* header, Snapshot snapshot, int64 visibleHead,
    int64 requestedVersion, const char* funcName, MemoryContext page,
    int64* snapshotOut, int64* schemaOut)
{
    if (requestedVersion < 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("%s snapshot version must be nonnegative", funcName)));
    if (requestedVersion > visibleHead)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("requested snapshot " INT64_FORMAT " is newer than visible committed head " INT64_FORMAT,
                requestedVersion, visibleHead)));
    MemoryContextReset(page);
    MemoryContext parent = MemoryContextSwitchTo(page);
    Oid type = INT8OID;
    Datum arg = Int64GetDatum(requestedVersion);
    LakeMetaRows* rows = LakeMetadataQuery(psprintf(
        "SELECT s.snapshot_id,s.schema_version,s.xmin FROM %s s WHERE s.snapshot_id=$1",
        LakeMetadataRelation(header->metadata_schema_oid, "ducklake_snapshot")),
        1, &type, &arg, NULL, snapshot, true, SPI_OK_SELECT, header->metadata_schema_oid);
    if (rows->count > 1)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("duplicate lake snapshot identity")));
    if (rows->count == 0 || LakeCdcOwnRow(rows, 0, 3))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("requested snapshot " INT64_FORMAT " does not exist or has expired", requestedVersion)));
    int64 snapId = DatumGetInt64(LakeMetadataValue(rows, 0, 1, INT8OID));
    int64 schemaVer = DatumGetInt64(LakeMetadataValue(rows, 0, 2, INT8OID));
    if (snapId != requestedVersion || schemaVer < 0)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("invalid resolved lake snapshot metadata")));
    MemoryContextSwitchTo(parent);
    MemoryContextReset(page);
    *snapshotOut = snapId;
    if (schemaOut != NULL)
        *schemaOut = schemaVer;
}

static void LakeCdcResolveUpperTimestamp(LakeInlineBinding* header, Snapshot snapshot, int64 visibleHead,
    TimestampTz requestedTime, MemoryContext page, int64* snapshotOut, int64* schemaOut)
{
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
                errmsg("no committed lake snapshot exists at or before the requested end timestamp")));
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
            if (!LakeCdcOwnRow(rows, i, 4)) {
                MemoryContextSwitchTo(parent);
                MemoryContextReset(page);
                *snapshotOut = id;
                *schemaOut = schemaVer;
                return;
            }
        }
        MemoryContextSwitchTo(parent);
    }
}

/* Matches DuckLake GetSnapshot(..., SnapshotBound::LOWER_BOUND): earliest
 * committed snapshot at or after requestedTime, or visibleHead + 1 if none. */
static int64 LakeCdcResolveLowerTimestamp(LakeInlineBinding* header, Snapshot snapshot, int64 visibleHead,
    TimestampTz requestedTime, MemoryContext page)
{
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
            "WHERE s.snapshot_id<=$1 AND s.snapshot_time>=$2 "
            "AND ($3 OR s.snapshot_time>$4 OR (s.snapshot_time=$4 AND s.snapshot_id>$5)) "
            "ORDER BY s.snapshot_time ASC, s.snapshot_id ASC LIMIT 512", snapRel),
            5, types, args, NULL, snapshot, true, SPI_OK_SELECT, header->metadata_schema_oid);
        if (!rows->count) {
            MemoryContextSwitchTo(parent);
            MemoryContextReset(page);
            if (visibleHead == PG_INT64_MAX)
                ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg("lake snapshot frontier overflow")));
            return visibleHead + 1;
        }
        for (uint32 i = 0; i < rows->count; ++i) {
            int64 id = DatumGetInt64(LakeMetadataValue(rows, i, 1, INT8OID));
            TimestampTz rowTime = DatumGetTimestampTz(LakeMetadataValue(rows, i, 2, TIMESTAMPTZOID));
            int64 schemaVer = DatumGetInt64(LakeMetadataValue(rows, i, 3, INT8OID));
            if (id < 0 || id > visibleHead || schemaVer < 0 || rowTime < requestedTime ||
                (!first && (rowTime < cursorTime || (rowTime == cursorTime && id <= cursorId))))
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("invalid ascending lake snapshot timestamp order")));
            first = false;
            cursorTime = rowTime;
            cursorId = id;
            if (!LakeCdcOwnRow(rows, i, 4)) {
                MemoryContextSwitchTo(parent);
                MemoryContextReset(page);
                return id;
            }
        }
        MemoryContextSwitchTo(parent);
    }
}

static void LakeCdcValidatePrefixAttr(Form_pg_attribute attr, int ordinal, Oid expectedType,
    Oid expectedCollation, const char* funcName, const char* roleName)
{
    if (attr->attisdropped || attr->atttypid != expectedType || attr->atttypmod != -1 ||
        attr->attcollation != expectedCollation)
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
            errmsg("%s AS prefix column %d (\"%s\") must match %s metadata type and collation",
                funcName, ordinal, NameStr(attr->attname), roleName)));
}

static void LakeCdcValidateExpectedDesc(LakeCdcMode mode, TupleDesc expectedDesc, TupleDesc histDesc,
    int* prefixColsOut)
{
    const char* funcName = LakeCdcFuncName(mode);
    if (expectedDesc == NULL || histDesc == NULL)
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
            errmsg("%s requires valid expected and historical descriptors", funcName)));
    int prefix = 0;
    if (mode == LAKE_CDC_CHANGES) {
        if (expectedDesc->natts != 3 + histDesc->natts)
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("%s AS column count (%d) does not match 3 change metadata columns plus %d historical columns",
                    funcName, expectedDesc->natts, histDesc->natts)));
        LakeCdcValidatePrefixAttr(&expectedDesc->attrs[0], 1, INT8OID, InvalidOid, funcName, "snapshot_id bigint");
        LakeCdcValidatePrefixAttr(&expectedDesc->attrs[1], 2, INT8OID, InvalidOid, funcName, "rowid bigint");
        LakeCdcValidatePrefixAttr(&expectedDesc->attrs[2], 3, TEXTOID, DEFAULT_COLLATION_OID, funcName, "change_type text");
        prefix = 3;
    } else if (expectedDesc->natts == histDesc->natts) {
        prefix = 0;
    } else if (expectedDesc->natts == 2 + histDesc->natts) {
        LakeCdcValidatePrefixAttr(&expectedDesc->attrs[0], 1, INT8OID, InvalidOid, funcName, "snapshot_id bigint");
        LakeCdcValidatePrefixAttr(&expectedDesc->attrs[1], 2, INT8OID, InvalidOid, funcName, "rowid bigint");
        prefix = 2;
    } else {
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
            errmsg("%s AS column count (%d) does not match %d historical columns (or %d with snapshot_id, rowid prefix)",
                funcName, expectedDesc->natts, histDesc->natts, 2 + histDesc->natts)));
    }
    for (int i = 0; i < histDesc->natts; ++i) {
        Form_pg_attribute expAttr = &expectedDesc->attrs[prefix + i];
        Form_pg_attribute histAttr = &histDesc->attrs[i];
        if (expAttr->attisdropped ||
            expAttr->atttypid != histAttr->atttypid ||
            expAttr->atttypmod != histAttr->atttypmod ||
            expAttr->attcollation != histAttr->attcollation)
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("%s AS column %d (\"%s\") does not match historical column \"%s\" type, typmod or collation",
                    funcName, prefix + i + 1, NameStr(expAttr->attname), NameStr(histAttr->attname))));
    }
    *prefixColsOut = prefix;
}

static Oid LakeCdcAuthorizeTarget(Oid relationOid, const char* funcName)
{
    Relation host = try_relation_open(relationOid, AccessShareLock);
    if (host == NULL)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE),
            errmsg("relation with OID %u does not exist", relationOid)));
    Oid nsOid = RelationGetNamespace(host);
    AclResult nsAcl = pg_namespace_aclcheck(nsOid, GetUserId(), ACL_USAGE);
    if (nsAcl != ACLCHECK_OK) {
        relation_close(host, NoLock);
        aclcheck_error(nsAcl, ACL_KIND_NAMESPACE, get_namespace_name(nsOid));
    }
    if (pg_class_aclcheck(relationOid, GetUserId(), ACL_SELECT) != ACLCHECK_OK &&
        pg_attribute_aclcheck_all(relationOid, GetUserId(), ACL_SELECT, ACLMASK_ANY) != ACLCHECK_OK) {
        char* relName = pstrdup(RelationGetRelationName(host));
        relation_close(host, NoLock);
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_CLASS, relName);
    }
    LakeRejectNativePartitionDDL(host, funcName);
    if (!LakeIsNativeForeignRelation(host)) {
        char* relName = pstrdup(RelationGetRelationName(host));
        relation_close(host, NoLock);
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg("%s target \"%s\" is not an og_lake foreign table", funcName, relName)));
    }
    ForeignTable* foreign = GetForeignTable(relationOid);
    if (!LakeTrustedForeignServer(foreign->serverid)) {
        relation_close(host, NoLock);
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("%s target server is not a trusted og_lake server", funcName)));
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
            errmsg("%s requires a committed native lake foreign table", funcName)));
    }
    Oid serverOid = foreign->serverid;
    relation_close(host, NoLock);
    return serverOid;
}

static bool LakeCdcChangesHasRewriteForTable(const char* changes, int64 tableId)
{
    if (changes == NULL || *changes == '\0')
        return false;
    Size offset = 0;
    while (changes[offset] != '\0') {
        Size start = offset;
        while (changes[offset] != '\0' && changes[offset] != ':')
            ++offset;
        if (changes[offset] != ':')
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("malformed lake snapshot_changes entry without colon")));
        Size tokenLen = offset - start;
        ++offset;
        Size valStart = offset;
        bool quoted = false;
        while (changes[offset] != '\0') {
            if (changes[offset] == '"')
                quoted = !quoted;
            if (!quoted && changes[offset] == ',')
                break;
            ++offset;
        }
        if (quoted)
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("unbalanced quotes in lake snapshot_changes")));
        Size valLen = offset - valStart;
        if (changes[offset] == ',')
            ++offset;
        if (tokenLen == 14 && pg_strncasecmp(changes + start, "rewrite_delete", 14) == 0) {
            if (valLen == 0)
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("empty rewrite_delete table identity in lake snapshot_changes")));
            uint64 parsed = 0;
            for (Size k = 0; k < valLen; ++k) {
                unsigned char ch = (unsigned char)changes[valStart + k];
                if (ch < '0' || ch > '9' || parsed > ((uint64)PG_INT64_MAX - (ch - '0')) / 10)
                    ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("invalid rewrite_delete table identity in lake snapshot_changes")));
                parsed = parsed * 10 + (ch - '0');
            }
            if ((int64)parsed == tableId)
                return true;
        }
    }
    return false;
}

static int64* LakeCdcLoadRewriteSnapshots(LakeInlineBinding* binding, Snapshot snapshot,
    int64 startSnap, int64 endSnap, MemoryContext ownerCtx, int* countOut)
{
    int64 tableId = LakePermanentTableId(binding);
    MemoryContext page = AllocSetContextCreate(ownerCtx, "Lake CDC rewrite scan page",
        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    int capacity = 16;
    int count = 0;
    int64* snaps = (int64*)MemoryContextAlloc(ownerCtx, sizeof(int64) * capacity);
    bool first = true;
    int64 cursor = -1;
    const char* changesRel = LakeMetadataRelation(binding->metadata_schema_oid, "ducklake_snapshot_changes");
    for (;;) {
        CHECK_FOR_INTERRUPTS();
        MemoryContextReset(page);
        MemoryContext prev = MemoryContextSwitchTo(page);
        Oid types[4] = {INT8OID, BOOLOID, INT8OID, INT8OID};
        Datum args[4] = {Int64GetDatum(endSnap), BoolGetDatum(first), Int64GetDatum(cursor), Int64GetDatum(startSnap)};
        LakeMetaRows* rows = LakeMetadataQuery(psprintf(
            "SELECT c.snapshot_id,c.changes_made::text,c.xmin FROM %s c "
            "WHERE c.snapshot_id BETWEEN $4 AND $1 AND ($2 OR c.snapshot_id>$3) "
            "ORDER BY c.snapshot_id ASC LIMIT 512", changesRel),
            4, types, args, NULL, snapshot, true, SPI_OK_SELECT, binding->metadata_schema_oid);
        if (!rows->count) {
            MemoryContextSwitchTo(prev);
            break;
        }
        for (uint32 i = 0; i < rows->count; ++i) {
            int64 id = DatumGetInt64(LakeMetadataValue(rows, i, 1, INT8OID));
            if (id < startSnap || id > endSnap || (!first && id <= cursor))
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("invalid ascending lake snapshot_changes identity")));
            first = false;
            cursor = id;
            if (LakeCdcOwnRow(rows, i, 3))
                continue;
            bool isNull = false;
            (void)LakeMetadataValue(rows, i, 2, TEXTOID, true, &isNull);
            if (isNull)
                continue;
            const char* text = LakeMetadataText(rows, i, 2);
            if (LakeCdcChangesHasRewriteForTable(text, tableId)) {
                if (count == capacity) {
                    if (capacity > 1048576)
                        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                            errmsg("lake table rewrite snapshot history exceeds bound")));
                    capacity *= 2;
                    snaps = (int64*)repalloc(snaps, sizeof(int64) * capacity);
                }
                snaps[count++] = id;
            }
        }
        MemoryContextSwitchTo(prev);
    }
    MemoryContextDelete(page);
    *countOut = count;
    return snaps;
}

static void LakeCdcAppendRelativePath(StringInfo target, const char* part)
{
    if (part == NULL || *part == '\0' || *part == '/' || strchr(part, ':') != NULL)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake file reads currently require local relative paths")));
    if (strlen(part) >= MAXPGPATH)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            errmsg("native lake path component exceeds host limit")));
    const char* at = part;
    while (*at) {
        const char* end = strchr(at, '/');
        if (end == NULL)
            end = at + strlen(at);
        Size count = (Size)(end - at);
        if (count == 0 || (count == 1 && at[0] == '.') || (count == 2 && at[0] == '.' && at[1] == '.'))
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("invalid relative lake file path component")));
        if (target->len > 0 && target->data[target->len - 1] != '/')
            appendStringInfoChar(target, '/');
        appendBinaryStringInfo(target, at, (int)count);
        at = *end ? end + 1 : end;
    }
    if (target->len >= MAXPGPATH)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            errmsg("native lake file path exceeds host limit")));
}

static char* LakeCdcResolveDataFilePath(const char* root, const char* schemaPath,
    const char* tablePath, const char* relFile)
{
    if (root == NULL || root[0] != '/' || strlen(root) == 0 || root[strlen(root) - 1] != '/')
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("invalid native lake catalog data root")));
    StringInfoData path;
    initStringInfo(&path);
    appendStringInfoString(&path, root);
    LakeCdcAppendRelativePath(&path, schemaPath);
    LakeCdcAppendRelativePath(&path, tablePath);
    LakeCdcAppendRelativePath(&path, relFile);
    char* canonical = realpath(path.data, NULL);
    if (canonical == NULL)
        ereport(ERROR, (errcode_for_file_access(),
            errmsg("cannot resolve native lake data file: %m")));
    bool same = (strcmp(canonical, path.data) == 0);
    free(canonical);
    if (!same)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake file paths cannot use symbolic-link aliases")));
    return path.data;
}

static int64 LakeCdcOptionalInt64(const LakeMetaRows* rows, uint32 row, int column)
{
    bool absent = false;
    Datum val = LakeMetadataValue(rows, row, column, INT8OID, true, &absent);
    int64 res = absent ? -1 : DatumGetInt64(val);
    if (!absent && res < 0)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("negative native lake optional file metadata")));
    return res;
}

static void LakeCdcResolveTablePaths(LakeInlineBinding* binding, Snapshot snapshot,
    char** schemaPathOut, char** tablePathOut)
{
    Oid types[2] = {INT8OID, INT8OID};
    Datum args[2] = {Int64GetDatum(LakePermanentTableId(binding)), Int64GetDatum(binding->snapshot_id)};
    char* tables = LakeMetadataRelation(binding->metadata_schema_oid, "ducklake_table");
    char* schemas = LakeMetadataRelation(binding->metadata_schema_oid, "ducklake_schema");
    LakeMetaRows* paths = LakeMetadataQuery(psprintf(
        "SELECT t.path::text,t.path_is_relative,s.path::text,s.path_is_relative,s.parent_schema_id "
        "FROM %s t JOIN %s s ON s.schema_id=t.schema_id "
        "WHERE t.table_id=$1 AND t.begin_snapshot<=$2 AND (t.end_snapshot IS NULL OR t.end_snapshot>$2) "
        "AND s.begin_snapshot<=$2 AND (s.end_snapshot IS NULL OR s.end_snapshot>$2)", tables, schemas),
        2, types, args, NULL, snapshot, true, SPI_OK_SELECT, binding->metadata_schema_oid);
    LakeMetadataRequireCount(paths, 1, "CDC table/schema path");
    bool parentNull = false;
    (void)LakeMetadataValue(paths, 0, 5, INT8OID, true, &parentNull);
    if (!parentNull || !DatumGetBool(LakeMetadataValue(paths, 0, 2, BOOLOID)) ||
        !DatumGetBool(LakeMetadataValue(paths, 0, 4, BOOLOID)))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake nested or absolute schema/table paths are not enabled")));
    *tablePathOut = LakeMetadataText(paths, 0, 1);
    *schemaPathOut = LakeMetadataText(paths, 0, 3);
}

static void LakeCdcParseDataFileRow(LakeInlineBinding* binding, const LakeMetaRows* rows, uint32 i,
    const char* schemaPath, const char* tablePath, LakeDataFile* fileOut, int64* endSnapOut)
{
    errno_t rc = memset_s(fileOut, sizeof(*fileOut), 0, sizeof(*fileOut));
    securec_check(rc, "", "");
    fileOut->file_id = DatumGetInt64(LakeMetadataValue(rows, i, 1, INT8OID));
    fileOut->begin_snapshot = DatumGetInt64(LakeMetadataValue(rows, i, 2, INT8OID));
    *endSnapOut = LakeCdcOptionalInt64(rows, i, 3);
    Datum start = LakeMetadataValue(rows, i, 4, INT8OID, true, &fileOut->row_id_start_null);
    fileOut->row_id_start = fileOut->row_id_start_null ? 0 : DatumGetInt64(start);
    fileOut->record_count = DatumGetInt64(LakeMetadataValue(rows, i, 5, INT8OID));
    fileOut->file_bytes = DatumGetInt64(LakeMetadataValue(rows, i, 6, INT8OID));
    fileOut->row_group_count = LakeCdcOptionalInt64(rows, i, 7);
    fileOut->partial_max = LakeCdcOptionalInt64(rows, i, 13);
    if (fileOut->file_id < 0 || fileOut->begin_snapshot < 0 || fileOut->file_bytes < 12 ||
        fileOut->record_count < 0 ||
        (!fileOut->row_id_start_null && (fileOut->row_id_start < 0 ||
            fileOut->record_count > PG_INT64_MAX - fileOut->row_id_start)))
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("invalid native lake file metadata")));
    if (!LakeFileVersionedReadsEnabled() && (fileOut->row_id_start_null || fileOut->partial_max >= 0))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake explicit or partial file versions require extension 0.9")));
    if (!DatumGetBool(LakeMetadataValue(rows, i, 9, BOOLOID)) ||
        strcmp(LakeMetadataText(rows, i, 10), "parquet") != 0)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake file format or absolute path is not enabled")));
    for (int col = 11; col <= 12; ++col) {
        bool isnull = false;
        (void)LakeMetadataValue(rows, i, col, col == 12 ? TEXTOID : INT8OID, true, &isnull);
        if (!isnull)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("native lake file mapping or encryption is not enabled")));
    }
    fileOut->path = LakeCdcResolveDataFilePath(
        binding->data_root, schemaPath, tablePath, LakeMetadataText(rows, i, 8));
}

static void LakeCdcParseDeleteFileRow(LakeInlineBinding* binding, const LakeDataFile* dataFile,
    const LakeMetaRows* rows, uint32 i, const char* schemaPath, const char* tablePath,
    LakeDeleteFile* delOut)
{
    errno_t rc = memset_s(delOut, sizeof(*delOut), 0, sizeof(*delOut));
    securec_check(rc, "", "");
    delOut->file_id = DatumGetInt64(LakeMetadataValue(rows, i, 1, INT8OID));
    delOut->begin_snapshot = DatumGetInt64(LakeMetadataValue(rows, i, 2, INT8OID));
    delOut->record_count = DatumGetInt64(LakeMetadataValue(rows, i, 3, INT8OID));
    delOut->file_bytes = DatumGetInt64(LakeMetadataValue(rows, i, 4, INT8OID));
    if (!DatumGetBool(LakeMetadataValue(rows, i, 6, BOOLOID)) ||
        strcmp(LakeMetadataText(rows, i, 7), "parquet") != 0)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake Puffin or absolute delete files are not enabled")));
    bool keyNull = false;
    (void)LakeMetadataValue(rows, i, 8, TEXTOID, true, &keyNull);
    if (!keyNull)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake encrypted delete files are not enabled")));
    delOut->partial_max = LakeCdcOptionalInt64(rows, i, 9);
    if (delOut->file_id < 0 || delOut->begin_snapshot < 0 || delOut->record_count < 0 ||
        delOut->record_count > dataFile->record_count || delOut->file_bytes < 12)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("invalid native lake delete file metadata")));
    delOut->path = LakeCdcResolveDataFilePath(
        binding->data_root, schemaPath, tablePath, LakeMetadataText(rows, i, 5));
}

static AttrNumber LakeCdcCarrierAttribute(Relation relation, const char* name, Oid type)
{
    AttrNumber attnum = get_attnum(RelationGetRelid(relation), name);
    if (attnum <= 0 || attnum > RelationGetDescr(relation)->natts)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("native lake inline column \"%s\" is missing", name)));
    Form_pg_attribute attr = &RelationGetDescr(relation)->attrs[attnum - 1];
    bool compatible = attr->atttypid == type || (type == TEXTOID && attr->atttypid == VARCHAROID);
    if (attr->attisdropped || !compatible)
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
            errmsg("native lake inline column \"%s\" has an incompatible type", name)));
    return attnum;
}

typedef void (*LakeCdcRowSink)(int64 eventSnapshot, int64 rowId, const Datum* bizValues,
    const bool* bizNulls, void* sinkArg);

static void LakeCdcDecodeInlineColumns(const LakeInlineBinding* binding, const AttrNumber* colAttnums,
    const Oid* sourceTypes, const int32* sourceTypmods, const Datum* rawValues, const bool* rawNulls,
    Datum* outValues, bool* outNulls)
{
    Size binaryRowBytes = 0;
    for (int i = 0; i < binding->column_count; ++i) {
        if (colAttnums[i] == InvalidAttrNumber) {
            LakeMaterializeInitialDefault(&binding->columns[i], &outValues[i], &outNulls[i], &binaryRowBytes);
            continue;
        }
        int src = colAttnums[i] - 1;
        outNulls[i] = rawNulls[src];
        outValues[i] = (Datum)0;
        if (!outNulls[i]) {
            Oid srcType = sourceTypes[i];
            int32 srcTypmod = sourceTypmods[i];
            Oid dstType = LakeColumnTargetType(&binding->columns[i]);
            int32 dstTypmod = LakeColumnTargetTypmod(&binding->columns[i]);
            Datum decoded = (Datum)0;
            if (srcType == VECTOROID)
                decoded = LakeVectorReadText(rawValues[src], srcTypmod);
            else if (LakeNativeTypeIsTemporal(srcType))
                decoded = LakeTemporalReadText(rawValues[src], srcType);
            else if (srcType == BYTEAOID)
                decoded = LakeCopyBinaryDatum(rawValues[src], &binaryRowBytes);
            else if (srcType == UUIDOID)
                decoded = LakeCopyUuidDatum(rawValues[src], &binaryRowBytes);
            else if (srcType == JSONOID)
                decoded = LakeJsonReadInline(rawValues[src], &binaryRowBytes);
            else if (srcType == NUMERICOID)
                decoded = LakeCopyNumericDatum(rawValues[src], srcTypmod, &binaryRowBytes);
            else
                decoded = (srcType == TEXTOID) ? datumCopy(rawValues[src], false, -1) : rawValues[src];
            LakeTemporalValidateDatum(srcType, decoded);
            if (srcType != dstType || srcTypmod != dstTypmod)
                decoded = LakeWidenDatum(decoded, srcType, srcTypmod, dstType, dstTypmod);
            outValues[i] = decoded;
        }
    }
}

static void LakeCdcScanInlineCarriers(LakeInlineBinding* binding, Snapshot snapshot,
    int64 startSnap, int64 endSnap, bool scanDeletions, MemoryContext parentCtx,
    LakeCdcRowSink sink, void* sinkArg)
{
    int sourceCount = 0;
    if (binding->inline_sources_loaded)
        sourceCount = LakeInlineReadSourceCount(binding);
    else if (OidIsValid(binding->inline_oid))
        sourceCount = 1;
    if (sourceCount <= 0)
        return;

    MemoryContext rowCtx = AllocSetContextCreate(parentCtx, "Lake CDC inline row", ALLOCSET_DEFAULT_SIZES);
    Datum* bizValues = (Datum*)MemoryContextAllocZero(parentCtx, sizeof(Datum) * binding->column_count);
    bool* bizNulls = (bool*)MemoryContextAllocZero(parentCtx, sizeof(bool) * binding->column_count);

    for (int idx = 0; idx < sourceCount; ++idx) {
        CHECK_FOR_INTERRUPTS();
        const LakeInlineSource* source = binding->inline_sources_loaded ? &binding->inline_sources[idx] : NULL;
        Oid carrierOid = source ? source->relation_oid : binding->inline_oid;

        MemoryContext volatile carrierCtx = AllocSetContextCreate(parentCtx,
            "Lake CDC carrier scan", ALLOCSET_DEFAULT_SIZES);
        Relation volatile rel = NULL;
        TableScanDesc volatile scan = NULL;
        PG_TRY();
        {
            MemoryContextSwitchTo(carrierCtx);
            rel = heap_open(carrierOid, AccessShareLock);
            TupleDesc desc = RelationGetDescr((Relation)rel);
            if (desc->natts != (source ? source->column_count : binding->column_count) + 3)
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("native lake inline schema requires an unsupported projection adapter")));
            AttrNumber rowIdAtt = LakeCdcCarrierAttribute((Relation)rel, "_ducklake_row_id", INT8OID);
            AttrNumber beginAtt = LakeCdcCarrierAttribute((Relation)rel, "_ducklake_begin_snapshot", INT8OID);
            AttrNumber endAtt = LakeCdcCarrierAttribute((Relation)rel, "_ducklake_end_snapshot", INT8OID);
            if (source) {
                for (int p = 0; p < source->column_count; ++p) {
                    const LakeInlineColumn* field = &source->columns[p];
                    AttrNumber actual = LakeCdcCarrierAttribute(
                        (Relation)rel, source->column_names[p], LakeInlinePhysicalType(field->type));
                    if (field->type == NUMERICOID && desc->attrs[actual - 1].atttypmod != field->typmod)
                        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                            errmsg("native numeric carrier parameters changed")));
                    if (actual != p + 4)
                        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                            errmsg("native carrier complete physical field order changed")));
                }
            }
            AttrNumber* colAttnums = (AttrNumber*)palloc(sizeof(AttrNumber) * binding->column_count);
            Oid* sourceTypes = (Oid*)palloc(sizeof(Oid) * binding->column_count);
            int32* sourceTypmods = (int32*)palloc(sizeof(int32) * binding->column_count);
            for (int c = 0; c < binding->column_count; ++c) {
                int physical = source ? source->logical_to_source[c] : c;
                if (physical < 0) {
                    if (!source || physical != -1 || !LakeColumnCanBeMissing(binding, c, source->origin_snapshot))
                        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                            errmsg("inline field omission is not certified")));
                    colAttnums[c] = InvalidAttrNumber;
                    sourceTypes[c] = LakeColumnTargetType(&binding->columns[c]);
                    sourceTypmods[c] = LakeColumnTargetTypmod(&binding->columns[c]);
                    continue;
                }
                const LakeInlineColumn* srcCol = source ? &source->columns[physical] : &binding->columns[c];
                Oid dstType = LakeColumnTargetType(&binding->columns[c]);
                int32 dstTypmod = LakeColumnTargetTypmod(&binding->columns[c]);
                if (!LakeIsTypeCompatibleOrPromoted(srcCol->type, srcCol->typmod, dstType, dstTypmod))
                    ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("native lake inline column \"%s\" has an incompatible type",
                            source ? source->column_names[physical] : LakeInlinePhysicalName(&binding->columns[c]))));
                sourceTypes[c] = srcCol->type;
                sourceTypmods[c] = srcCol->typmod;
                colAttnums[c] = LakeCdcCarrierAttribute((Relation)rel,
                    source ? source->column_names[physical] : LakeInlinePhysicalName(&binding->columns[c]),
                    LakeInlinePhysicalType(srcCol->type));
                if (srcCol->type == NUMERICOID &&
                    desc->attrs[colAttnums[c] - 1].atttypmod != srcCol->typmod)
                    ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("native numeric inline parameters changed")));
            }
            Datum* rawValues = (Datum*)palloc0(sizeof(Datum) * desc->natts);
            bool* rawNulls = (bool*)palloc0(sizeof(bool) * desc->natts);
            scan = tableam_scan_begin((Relation)rel, snapshot, 0, NULL);
            for (;;) {
                CHECK_FOR_INTERRUPTS();
                MemoryContextSwitchTo(carrierCtx);
                Tuple tuple = tableam_scan_getnexttuple((TableScanDesc)scan, ForwardScanDirection);
                if (tuple == NULL)
                    break;
                tableam_tops_deform_tuple(tuple, desc, rawValues, rawNulls);
                int rIdx = rowIdAtt - 1;
                int bIdx = beginAtt - 1;
                int eIdx = endAtt - 1;
                if (rawNulls[rIdx] || rawNulls[bIdx])
                    ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("native lake inline row identity or begin snapshot is NULL")));
                int64 rowId = DatumGetInt64(rawValues[rIdx]);
                int64 begin = DatumGetInt64(rawValues[bIdx]);
                bool hasEnd = !rawNulls[eIdx];
                int64 end = hasEnd ? DatumGetInt64(rawValues[eIdx]) : 0;
                if (rowId < 0 || begin < 0 || (hasEnd && end < begin))
                    ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("native lake inline row has invalid version bounds")));
                if (begin > endSnap)
                    continue;
                if (binding->has_dropped_columns) {
                    const LakeInlineSource* physical = LakeFindInlineSource(binding, carrierOid);
                    if (!physical)
                        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                            errmsg("native CDC read lost its physical source identity")));
                    for (int p = 0; p < physical->column_count; ++p)
                        if (LakeFindRetiredColumn(binding, physical->columns[p].column_id))
                            LakeValidateRetiredBirth(binding, physical->columns[p].column_id, begin, begin);
                }
                for (int c = 0; c < binding->column_count; ++c)
                    if (colAttnums[c] == InvalidAttrNumber && !LakeColumnCanBeMissing(binding, c, begin))
                        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                            errmsg("inline row birth is newer than its missing field")));

                int64 eventSnap = -1;
                if (scanDeletions) {
                    if (hasEnd && end >= startSnap && end <= endSnap && end != begin)
                        eventSnap = end;
                } else {
                    if (begin >= startSnap && begin <= endSnap && !(hasEnd && end == begin))
                        eventSnap = begin;
                }
                if (eventSnap < 0)
                    continue;

                MemoryContextReset(rowCtx);
                MemoryContextSwitchTo(rowCtx);
                LakeCdcDecodeInlineColumns(binding, colAttnums, sourceTypes, sourceTypmods,
                    rawValues, rawNulls, bizValues, bizNulls);
                sink(eventSnap, rowId, bizValues, bizNulls, sinkArg);
            }
            MemoryContextSwitchTo(carrierCtx);
            tableam_scan_end((TableScanDesc)scan);
            scan = NULL;
            heap_close((Relation)rel, NoLock);
            rel = NULL;
            MemoryContextSwitchTo(parentCtx);
            MemoryContextDelete(carrierCtx);
            carrierCtx = NULL;
        }
        PG_CATCH();
        {
            if (scan != NULL) {
                MemoryContextSwitchTo(carrierCtx);
                tableam_scan_end((TableScanDesc)scan);
                scan = NULL;
            }
            if (rel != NULL) {
                heap_close((Relation)rel, NoLock);
                rel = NULL;
            }
            MemoryContextSwitchTo(parentCtx);
            if (carrierCtx != NULL) {
                MemoryContextDelete(carrierCtx);
                carrierCtx = NULL;
            }
            PG_RE_THROW();
        }
        PG_END_TRY();
    }
    MemoryContextDelete(rowCtx);
}

struct LakeCdcSameSnapCollect {
    LakeCdcPosSnap* items;
    int count;
    int capacity;
    int64 minSnap;
    int64 maxSnap;
};

static void LakeCdcAppendPosSnap(LakeCdcSameSnapCollect* state, int64 pos, int64 snap)
{
    if (snap < state->minSnap || snap > state->maxSnap)
        return;
    if (state->count >= LAKE_FILE_DELETE_POSITION_LIMIT)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            errmsg("native lake CDC deletion position budget exceeded")));
    if (state->count == state->capacity) {
        state->capacity = Max(16, state->capacity * 2);
        state->items = state->items ?
            (LakeCdcPosSnap*)repalloc(state->items, sizeof(LakeCdcPosSnap) * state->capacity) :
            (LakeCdcPosSnap*)palloc(sizeof(LakeCdcPosSnap) * state->capacity);
    }
    state->items[state->count].position = pos;
    state->items[state->count].snapshot_id = snap;
    ++state->count;
}

static void LakeCdcVisitSameSnapDelete(int64 position, int64 snapshotId, void* arg)
{
    LakeCdcAppendPosSnap((LakeCdcSameSnapCollect*)arg, position, snapshotId);
}

static LakeCdcPosSnap* LakeCdcLoadSameSnapDeletes(LakeInlineBinding* binding, Snapshot snapshot,
    const char* schemaPath, const char* tablePath, LakeDataFile* dataFile,
    int64 minBirth, int64 maxBirth, bool hasInlinedDel, const char* inlinedDelRel, int* countOut)
{
    LakeCdcSameSnapCollect collect = {NULL, 0, 0, minBirth, maxBirth};
    Oid types[3] = {INT8OID, INT8OID, INT8OID};
    Datum args[3] = {
        Int64GetDatum(LakePermanentTableId(binding)),
        Int64GetDatum(dataFile->file_id),
        Int64GetDatum(maxBirth)
    };
    char* deletes = LakeMetadataRelation(binding->metadata_schema_oid, "ducklake_delete_file");
    LakeMetaRows* delRows = LakeMetadataQuery(psprintf(
        "SELECT delete_file_id,begin_snapshot,delete_count,file_size_bytes,path::text,"
        "path_is_relative,format::text,encryption_key::text,partial_max "
        "FROM %s WHERE table_id=$1 AND data_file_id=$2 AND begin_snapshot<=$3 "
        "ORDER BY begin_snapshot ASC, delete_file_id ASC LIMIT 4097", deletes),
        3, types, args, NULL, snapshot, true, SPI_OK_SELECT, binding->metadata_schema_oid);
    if (delRows->count > 4096)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            errmsg("native lake delete file count exceeds limit")));
    for (uint32 i = 0; i < delRows->count; ++i) {
        LakeDeleteFile del = {};
        LakeCdcParseDeleteFileRow(binding, dataFile, delRows, i, schemaPath, tablePath, &del);
        int64 delUpper = del.partial_max >= 0 ? del.partial_max : del.begin_snapshot;
        if (delUpper < minBirth)
            continue;
        dataFile->deletes = &del;
        LakeSdkHandles* volatile handles = LakeFileHandles(CurrentMemoryContext);
        PG_TRY();
        {
            LakeFileVisitDeletes(dataFile, (LakeSdkHandles*)handles, LakeCdcVisitSameSnapDelete, &collect);
            handles = NULL;
            dataFile->deletes = NULL;
        }
        PG_CATCH();
        {
            dataFile->deletes = NULL;
            if (handles != NULL)
                LakeFileHandlesClose((LakeSdkHandles*)handles);
            PG_RE_THROW();
        }
        PG_END_TRY();
    }
    if (hasInlinedDel) {
        Oid inlTypes[3] = {INT8OID, INT8OID, INT8OID};
        Datum inlArgs[3] = {Int64GetDatum(dataFile->file_id), Int64GetDatum(minBirth), Int64GetDatum(maxBirth)};
        LakeMetaRows* inlRows = LakeMetadataQuery(psprintf(
            "SELECT row_id,begin_snapshot FROM %s WHERE file_id=$1 AND begin_snapshot>=$2 AND begin_snapshot<=$3 "
            "ORDER BY row_id,begin_snapshot LIMIT %d", inlinedDelRel, LAKE_FILE_DELETE_POSITION_LIMIT + 1),
            3, inlTypes, inlArgs, NULL, snapshot, true, SPI_OK_SELECT, binding->metadata_schema_oid);
        if (inlRows->count > (uint32)LAKE_FILE_DELETE_POSITION_LIMIT)
            ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                errmsg("native lake inline deletion input exceeds budget")));
        for (uint32 i = 0; i < inlRows->count; ++i) {
            int64 pos = DatumGetInt64(LakeMetadataValue(inlRows, i, 1, INT8OID));
            int64 snap = DatumGetInt64(LakeMetadataValue(inlRows, i, 2, INT8OID));
            if (pos < 0 || pos >= dataFile->record_count || snap < 0)
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("invalid native lake inline file deletion position")));
            LakeCdcAppendPosSnap(&collect, pos, snap);
        }
    }
    if (collect.count > 1)
        qsort(collect.items, (size_t)collect.count, sizeof(LakeCdcPosSnap), LakeCdcComparePosSnap);
    *countOut = collect.count;
    return collect.items;
}

static void LakeCdcValidateInlinedDeleteTable(LakeInlineBinding* binding, Snapshot snapshot,
    int64 endSnap, bool* existsOut, char** relOut)
{
    char* name = psprintf("ducklake_inlined_delete_%lld", (long long)LakePermanentTableId(binding));
    if (!OidIsValid(get_relname_relid(name, binding->metadata_schema_oid))) {
        *existsOut = false;
        *relOut = NULL;
        return;
    }
    if (!LakeFileVersionedReadsEnabled())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake inline file delete visibility requires extension 0.9")));
    char* carrier = LakeMetadataRelation(binding->metadata_schema_oid, name);
    Oid type = INT8OID;
    Datum arg = Int64GetDatum(endSnap);
    LakeMetaRows* invalid = LakeMetadataQuery(psprintf(
        "SELECT EXISTS(SELECT 1 FROM %s WHERE (begin_snapshot<=$1 OR begin_snapshot IS NULL) "
        "AND (file_id IS NULL OR file_id<0 OR row_id IS NULL OR row_id<0 OR begin_snapshot IS NULL OR begin_snapshot<0))",
        carrier), 1, &type, &arg, NULL, snapshot, true, SPI_OK_SELECT, binding->metadata_schema_oid);
    LakeMetadataRequireCount(invalid, 1, "CDC inline file deletion validity check");
    if (DatumGetBool(LakeMetadataValue(invalid, 0, 1, BOOLOID)))
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("invalid native lake inline file deletion")));
    *existsOut = true;
    *relOut = carrier;
}

static void LakeCdcScanFileInsertions(LakeInlineBinding* binding, Snapshot snapshot,
    int64 startSnap, int64 endSnap, const int64* rewriteSnaps, int rewriteCount,
    MemoryContext parentCtx, LakeCdcRowSink sink, void* sinkArg)
{
    Oid types[3] = {INT8OID, INT8OID, INT8OID};
    Datum args[3] = {
        Int64GetDatum(LakePermanentTableId(binding)),
        Int64GetDatum(endSnap),
        Int64GetDatum(startSnap)
    };
    char* filesRel = LakeMetadataRelation(binding->metadata_schema_oid, "ducklake_data_file");
    LakeMetaRows* rows = LakeMetadataQuery(psprintf(
        "SELECT data_file_id,begin_snapshot,end_snapshot,row_id_start,record_count,"
        "file_size_bytes,row_group_count,path::text,path_is_relative,file_format::text,"
        "mapping_id,encryption_key::text,partial_max "
        "FROM %s WHERE table_id=$1 AND begin_snapshot<=$2 "
        "AND (begin_snapshot>=$3 OR (partial_max IS NOT NULL AND partial_max>=$3)) "
        "ORDER BY data_file_id ASC LIMIT 4097", filesRel),
        3, types, args, NULL, snapshot, true, SPI_OK_SELECT, binding->metadata_schema_oid);
    if (rows->count == 0)
        return;
    if (rows->count > 4096)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            errmsg("native lake CDC file binding exceeds the 4096-file limit")));

    LakeRequireFileReadVersion();
    (void)LakeFileProvider();
    char* schemaPath = NULL;
    char* tablePath = NULL;
    LakeCdcResolveTablePaths(binding, snapshot, &schemaPath, &tablePath);
    bool hasInlinedDel = false;
    char* inlinedDelRel = NULL;
    LakeCdcValidateInlinedDeleteTable(binding, snapshot, endSnap, &hasInlinedDel, &inlinedDelRel);

    MemoryContext rowCtx = AllocSetContextCreate(parentCtx, "Lake CDC file insertion row", ALLOCSET_DEFAULT_SIZES);
    Datum* bizValues = (Datum*)MemoryContextAllocZero(parentCtx, sizeof(Datum) * binding->column_count);
    bool* bizNulls = (bool*)MemoryContextAllocZero(parentCtx, sizeof(bool) * binding->column_count);

    for (uint32 i = 0; i < rows->count; ++i) {
        CHECK_FOR_INTERRUPTS();
        MemoryContext volatile fileCtx = AllocSetContextCreate(parentCtx,
            "Lake CDC insertion file", ALLOCSET_DEFAULT_SIZES);
        LakeFileReader* volatile reader = NULL;
        PG_TRY();
        {
            MemoryContextSwitchTo(fileCtx);
            LakeDataFile dataFile = {};
            int64 fileEndSnap = -1;
            LakeCdcParseDataFileRow(binding, rows, i, schemaPath, tablePath, &dataFile, &fileEndSnap);
            if (!(dataFile.partial_max < 0 && LakeCdcInSnapSet(rewriteSnaps, rewriteCount, dataFile.begin_snapshot))) {
                int64 minBirth = Max(startSnap, dataFile.begin_snapshot);
                int64 maxBirth = dataFile.partial_max >= 0 ? Min(endSnap, dataFile.partial_max) : dataFile.begin_snapshot;
                int sameDelCount = 0;
                LakeCdcPosSnap* sameDels = (minBirth <= maxBirth) ?
                    LakeCdcLoadSameSnapDeletes(binding, snapshot, schemaPath, tablePath, &dataFile,
                        minBirth, maxBirth, hasInlinedDel, inlinedDelRel, &sameDelCount) : NULL;

                LakeInlineBinding one = *binding;
                one.file_count = 1;
                one.files = &dataFile;
                reader = LakeFileReaderOpen(&one);
                for (;;) {
                    CHECK_FOR_INTERRUPTS();
                    MemoryContextReset(rowCtx);
                    MemoryContextSwitchTo(rowCtx);
                    LakeInlineRowIdentity identity = {};
                    if (!LakeFileReaderNext((LakeFileReader*)reader, bizValues, bizNulls, &identity))
                        break;
                    int64 birth = identity.begin_snapshot;
                    if (birth < startSnap || birth > endSnap)
                        continue;
                    if (LakeCdcInSnapSet(rewriteSnaps, rewriteCount, birth))
                        continue;
                    if (LakeCdcHasPosSnap(sameDels, sameDelCount, identity.file_position, birth))
                        continue;
                    sink(birth, identity.row_id, bizValues, bizNulls, sinkArg);
                }
                MemoryContextSwitchTo(fileCtx);
                LakeFileReaderClose((LakeFileReader*)reader);
                reader = NULL;
            }
            MemoryContextSwitchTo(parentCtx);
            MemoryContextDelete(fileCtx);
            fileCtx = NULL;
        }
        PG_CATCH();
        {
            if (reader != NULL) {
                MemoryContextSwitchTo(fileCtx);
                LakeFileReaderClose((LakeFileReader*)reader);
                reader = NULL;
            }
            MemoryContextSwitchTo(parentCtx);
            if (fileCtx != NULL) {
                MemoryContextDelete(fileCtx);
                fileCtx = NULL;
            }
            PG_RE_THROW();
        }
        PG_END_TRY();
    }
    MemoryContextDelete(rowCtx);
}

struct LakeCdcFirstDeleteVisit {
    int64* firstDelSnap;
    int64 recordCount;
    int64 endSnap;
};

static void LakeCdcRecordFirstDelete(int64 position, int64 snapshotId, void* arg)
{
    LakeCdcFirstDeleteVisit* visit = (LakeCdcFirstDeleteVisit*)arg;
    if (position < 0 || position >= visit->recordCount)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("delete position exceeds native lake data file population")));
    if (snapshotId <= visit->endSnap && visit->firstDelSnap[position] < 0)
        visit->firstDelSnap[position] = snapshotId;
}

static void LakeCdcScanFileDeletions(LakeInlineBinding* binding, Snapshot snapshot,
    int64 startSnap, int64 endSnap, const int64* rewriteSnaps, int rewriteCount,
    MemoryContext parentCtx, LakeCdcRowSink sink, void* sinkArg)
{
    bool hasInlinedDel = false;
    char* inlinedDelRel = NULL;
    LakeCdcValidateInlinedDeleteTable(binding, snapshot, endSnap, &hasInlinedDel, &inlinedDelRel);

    Oid types[3] = {INT8OID, INT8OID, INT8OID};
    Datum args[3] = {
        Int64GetDatum(LakePermanentTableId(binding)),
        Int64GetDatum(endSnap),
        Int64GetDatum(startSnap)
    };
    char* filesRel = LakeMetadataRelation(binding->metadata_schema_oid, "ducklake_data_file");
    char* deletesRel = LakeMetadataRelation(binding->metadata_schema_oid, "ducklake_delete_file");
    char* inlClause = hasInlinedDel ?
        psprintf(" OR EXISTS(SELECT 1 FROM %s idel WHERE idel.file_id=f.data_file_id AND idel.begin_snapshot<=$2)",
            inlinedDelRel) : pstrdup("");
    LakeMetaRows* rows = LakeMetadataQuery(psprintf(
        "SELECT f.data_file_id,f.begin_snapshot,f.end_snapshot,f.row_id_start,f.record_count,"
        "f.file_size_bytes,f.row_group_count,f.path::text,f.path_is_relative,f.file_format::text,"
        "f.mapping_id,f.encryption_key::text,f.partial_max "
        "FROM %s f WHERE f.table_id=$1 AND f.begin_snapshot<=$2 "
        "AND (EXISTS(SELECT 1 FROM %s d WHERE d.table_id=$1 AND d.data_file_id=f.data_file_id AND d.begin_snapshot<=$2) "
        "OR (f.end_snapshot IS NOT NULL AND f.end_snapshot>=$3 AND f.end_snapshot<=$2)%s) "
        "ORDER BY f.data_file_id ASC LIMIT 4097", filesRel, deletesRel, inlClause),
        3, types, args, NULL, snapshot, true, SPI_OK_SELECT, binding->metadata_schema_oid);
    if (rows->count == 0)
        return;
    if (rows->count > 4096)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            errmsg("native lake CDC deletion file binding exceeds the 4096-file limit")));

    LakeRequireFileReadVersion();
    (void)LakeFileProvider();
    char* schemaPath = NULL;
    char* tablePath = NULL;
    LakeCdcResolveTablePaths(binding, snapshot, &schemaPath, &tablePath);

    MemoryContext rowCtx = AllocSetContextCreate(parentCtx, "Lake CDC file deletion row", ALLOCSET_DEFAULT_SIZES);
    Datum* bizValues = (Datum*)MemoryContextAllocZero(parentCtx, sizeof(Datum) * binding->column_count);
    bool* bizNulls = (bool*)MemoryContextAllocZero(parentCtx, sizeof(bool) * binding->column_count);

    for (uint32 i = 0; i < rows->count; ++i) {
        CHECK_FOR_INTERRUPTS();
        MemoryContext volatile fileCtx = AllocSetContextCreate(parentCtx,
            "Lake CDC deletion file", ALLOCSET_DEFAULT_SIZES);
        LakeFileReader* volatile reader = NULL;
        PG_TRY();
        {
            MemoryContextSwitchTo(fileCtx);
            LakeDataFile dataFile = {};
            int64 fileEndSnap = -1;
            LakeCdcParseDataFileRow(binding, rows, i, schemaPath, tablePath, &dataFile, &fileEndSnap);
            if (dataFile.record_count > LAKE_FILE_DELETE_POSITION_LIMIT)
                ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg("native lake CDC data file population exceeds deletion position budget")));

            int64* firstDelSnap = (int64*)palloc(sizeof(int64) * (Size)Max(dataFile.record_count, (int64)1));
            for (int64 pos = 0; pos < dataFile.record_count; ++pos)
                firstDelSnap[pos] = -1;

            Oid delTypes[3] = {INT8OID, INT8OID, INT8OID};
            Datum delArgs[3] = {
                Int64GetDatum(LakePermanentTableId(binding)),
                Int64GetDatum(dataFile.file_id),
                Int64GetDatum(endSnap)
            };
            LakeMetaRows* delRows = LakeMetadataQuery(psprintf(
                "SELECT delete_file_id,begin_snapshot,delete_count,file_size_bytes,path::text,"
                "path_is_relative,format::text,encryption_key::text,partial_max "
                "FROM %s WHERE table_id=$1 AND data_file_id=$2 AND begin_snapshot<=$3 "
                "ORDER BY begin_snapshot ASC, delete_file_id ASC LIMIT 4097", deletesRel),
                3, delTypes, delArgs, NULL, snapshot, true, SPI_OK_SELECT, binding->metadata_schema_oid);
            if (delRows->count > 4096)
                ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg("native lake delete file count exceeds limit")));
            for (uint32 d = 0; d < delRows->count; ++d) {
                LakeDeleteFile del = {};
                LakeCdcParseDeleteFileRow(binding, &dataFile, delRows, d, schemaPath, tablePath, &del);
                dataFile.deletes = &del;
                LakeCdcFirstDeleteVisit visit = {firstDelSnap, dataFile.record_count, endSnap};
                LakeSdkHandles* volatile handles = LakeFileHandles(fileCtx);
                PG_TRY();
                {
                    LakeFileVisitDeletes(&dataFile, (LakeSdkHandles*)handles, LakeCdcRecordFirstDelete, &visit);
                    handles = NULL;
                    dataFile.deletes = NULL;
                }
                PG_CATCH();
                {
                    dataFile.deletes = NULL;
                    if (handles != NULL)
                        LakeFileHandlesClose((LakeSdkHandles*)handles);
                    PG_RE_THROW();
                }
                PG_END_TRY();
            }

            if (hasInlinedDel) {
                Oid inlTypes[2] = {INT8OID, INT8OID};
                Datum inlArgs[2] = {Int64GetDatum(dataFile.file_id), Int64GetDatum(endSnap)};
                LakeMetaRows* inlRows = LakeMetadataQuery(psprintf(
                    "SELECT row_id,begin_snapshot FROM %s WHERE file_id=$1 AND begin_snapshot<=$2 "
                    "ORDER BY begin_snapshot ASC, row_id ASC LIMIT %d",
                    inlinedDelRel, LAKE_FILE_DELETE_POSITION_LIMIT + 1),
                    2, inlTypes, inlArgs, NULL, snapshot, true, SPI_OK_SELECT, binding->metadata_schema_oid);
                if (inlRows->count > (uint32)LAKE_FILE_DELETE_POSITION_LIMIT)
                    ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                        errmsg("native lake inline deletion input exceeds budget")));
                for (uint32 r = 0; r < inlRows->count; ++r) {
                    int64 pos = DatumGetInt64(LakeMetadataValue(inlRows, r, 1, INT8OID));
                    int64 snap = DatumGetInt64(LakeMetadataValue(inlRows, r, 2, INT8OID));
                    if (pos < 0 || pos >= dataFile.record_count || snap < 0)
                        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                            errmsg("inline deletion position exceeds its native lake file")));
                    if (firstDelSnap[pos] < 0)
                        firstDelSnap[pos] = snap;
                }
            }

            if (fileEndSnap >= startSnap && fileEndSnap <= endSnap &&
                !LakeCdcInSnapSet(rewriteSnaps, rewriteCount, fileEndSnap)) {
                for (int64 pos = 0; pos < dataFile.record_count; ++pos)
                    if (firstDelSnap[pos] < 0)
                        firstDelSnap[pos] = fileEndSnap;
            }

            bool anyInRange = false;
            for (int64 pos = 0; pos < dataFile.record_count; ++pos) {
                if (firstDelSnap[pos] >= startSnap && firstDelSnap[pos] <= endSnap) {
                    anyInRange = true;
                    break;
                }
            }
            if (anyInRange) {
                LakeInlineBinding one = *binding;
                one.file_count = 1;
                one.files = &dataFile;
                reader = LakeFileReaderOpen(&one);
                for (;;) {
                    CHECK_FOR_INTERRUPTS();
                    MemoryContextReset(rowCtx);
                    MemoryContextSwitchTo(rowCtx);
                    LakeInlineRowIdentity identity = {};
                    if (!LakeFileReaderNext((LakeFileReader*)reader, bizValues, bizNulls, &identity))
                        break;
                    int64 pos = identity.file_position;
                    int64 delSnap = firstDelSnap[pos];
                    if (delSnap < startSnap || delSnap > endSnap)
                        continue;
                    if (delSnap == identity.begin_snapshot)
                        continue;
                    sink(delSnap, identity.row_id, bizValues, bizNulls, sinkArg);
                }
                MemoryContextSwitchTo(fileCtx);
                LakeFileReaderClose((LakeFileReader*)reader);
                reader = NULL;
            }
            MemoryContextSwitchTo(parentCtx);
            MemoryContextDelete(fileCtx);
            fileCtx = NULL;
        }
        PG_CATCH();
        {
            if (reader != NULL) {
                MemoryContextSwitchTo(fileCtx);
                LakeFileReaderClose((LakeFileReader*)reader);
                reader = NULL;
            }
            MemoryContextSwitchTo(parentCtx);
            if (fileCtx != NULL) {
                MemoryContextDelete(fileCtx);
                fileCtx = NULL;
            }
            PG_RE_THROW();
        }
        PG_END_TRY();
    }
    MemoryContextDelete(rowCtx);
}

struct LakeCdcEmitContext {
    Tuplestorestate* store;
    TupleDesc outDesc;
    int prefixCols;
    int bizCols;
    Datum* outValues;
    bool* outNulls;
    HTAB* delKeys;
    Datum insertLabel;
    Datum postimageLabel;
    bool stageDeletions;
};

static void LakeCdcEmitFeedRow(int64 eventSnapshot, int64 rowId, const Datum* bizValues,
    const bool* bizNulls, void* arg)
{
    LakeCdcEmitContext* ctx = (LakeCdcEmitContext*)arg;
    if (ctx->prefixCols == 2) {
        ctx->outValues[0] = Int64GetDatum(eventSnapshot);
        ctx->outNulls[0] = false;
        ctx->outValues[1] = Int64GetDatum(rowId);
        ctx->outNulls[1] = false;
    } else if (ctx->prefixCols == 3) {
        ctx->outValues[0] = Int64GetDatum(eventSnapshot);
        ctx->outNulls[0] = false;
        ctx->outValues[1] = Int64GetDatum(rowId);
        ctx->outNulls[1] = false;
        LakeCdcKey key = {eventSnapshot, rowId};
        if (ctx->stageDeletions) {
            bool found = false;
            if (!found && hash_get_num_entries(ctx->delKeys) >= LAKE_CDC_KEY_LIMIT)
                ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg("table_changes deletion key budget exceeded")));
            LakeCdcKeyEntry* entry = (LakeCdcKeyEntry*)hash_search(ctx->delKeys, &key, HASH_ENTER, &found);
            if (!found)
                entry->matched = false;
            ctx->outValues[2] = (Datum)0;
            ctx->outNulls[2] = true;
        } else {
            bool found = false;
            LakeCdcKeyEntry* entry = (LakeCdcKeyEntry*)hash_search(ctx->delKeys, &key, HASH_FIND, &found);
            if (found && entry != NULL) {
                entry->matched = true;
                ctx->outValues[2] = ctx->postimageLabel;
            } else {
                ctx->outValues[2] = ctx->insertLabel;
            }
            ctx->outNulls[2] = false;
        }
    }
    for (int i = 0; i < ctx->bizCols; ++i) {
        ctx->outValues[ctx->prefixCols + i] = bizValues[i];
        ctx->outNulls[ctx->prefixCols + i] = bizNulls[i];
    }
    tuplestore_putvalues(ctx->store, ctx->outDesc, ctx->outValues, ctx->outNulls);
}

static Datum LakeCdcExecute(FunctionCallInfo fcinfo, LakeCdcMode mode, bool byTimestamp)
{
    const char* funcName = LakeCdcFuncName(mode);
    if (StreamThreadAmI() || IS_SPQ_RUNNING)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("%s requires the invoking serial backend", funcName)));
    if (!ActiveSnapshotSet())
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("%s requires an active statement snapshot", funcName)));
    LakeRequireAccessPolicy();
    LakeCheckMaskingAdmission();
    if (!LakeTableChangesVersionEnabled())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("%s requires installed SQL0.58", funcName)));
    if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
            errmsg("%s arguments cannot be NULL", funcName)));

    int64 startVersion = 0;
    int64 endVersion = 0;
    TimestampTz startTime = 0;
    TimestampTz endTime = 0;
    if (byTimestamp) {
        TimestampTz now = GetCurrentTimestamp();
        startTime = PG_GETARG_TIMESTAMPTZ(1);
        endTime = PG_GETARG_TIMESTAMPTZ(2);
        if (TIMESTAMP_NOT_FINITE(startTime) || !IS_VALID_TIMESTAMP(startTime) || startTime > now ||
            TIMESTAMP_NOT_FINITE(endTime) || !IS_VALID_TIMESTAMP(endTime) || endTime > now)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("%s timestamps must be finite and not in the future", funcName)));
    } else {
        startVersion = PG_GETARG_INT64(1);
        endVersion = PG_GETARG_INT64(2);
        if (startVersion < 0 || endVersion < 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("%s snapshot versions must be nonnegative", funcName)));
    }

    ReturnSetInfo* result = (ReturnSetInfo*)fcinfo->resultinfo;
    if (!result || !IsA(result, ReturnSetInfo) || !(result->allowedModes & SFRM_Materialize) || !result->econtext)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("%s requires materialize mode", funcName)));

    TupleDesc expectedDesc = NULL;
    if (get_call_result_type(fcinfo, NULL, &expectedDesc) != TYPEFUNC_COMPOSITE || expectedDesc == NULL)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("%s requires a FROM clause with an explicit AS column definition list", funcName)));

    Oid relationOid = PG_GETARG_OID(0);
    Oid serverOid = LakeCdcAuthorizeTarget(relationOid, funcName);

    int workMemKb = Max(64, Min(u_sess->attr.attr_memory.work_mem, 1024));
    MemoryContext callerCtx = MemoryContextSwitchTo(result->econtext->ecxt_per_query_memory);
    TupleDesc outDesc = BlessTupleDesc(CreateTupleDescCopy(expectedDesc));
    Tuplestorestate* outStore = tuplestore_begin_heap(true, false, workMemKb);
    MemoryContextSwitchTo(callerCtx);

    MemoryContext volatile scanCtx = AllocSetContextCreate(CurrentMemoryContext,
        "Lake CDC execution", ALLOCSET_DEFAULT_SIZES);
    Snapshot volatile snapshot = NULL;
    Tuplestorestate* volatile delStore = NULL;
    TupleTableSlot* volatile delSlot = NULL;
    volatile int fastScope = 0;
    volatile bool fastActive = false;

    PG_TRY();
    {
        fastScope = LakeEnterFastValidationScope();
        fastActive = true;
        snapshot = RegisterSnapshot(GetActiveSnapshot());
        MemoryContextSwitchTo(scanCtx);

        LakeInlineBinding* header = LakeLoadCatalogHeader(serverOid, snapshot);
        MemoryContext pageCtx = AllocSetContextCreate(scanCtx,
            "Lake CDC snapshot page", ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
        int64 visibleHead = LakeCdcVisibleHead(header, snapshot, pageCtx);
        int64 startSnap = -1;
        int64 endSnap = -1;
        int64 endSchema = -1;
        if (byTimestamp) {
            LakeCdcResolveUpperTimestamp(header, snapshot, visibleHead, endTime, pageCtx, &endSnap, &endSchema);
            startSnap = LakeCdcResolveLowerTimestamp(header, snapshot, visibleHead, startTime, pageCtx);
        } else {
            LakeCdcResolveVersion(header, snapshot, visibleHead, startVersion, funcName, pageCtx, &startSnap, NULL);
            LakeCdcResolveVersion(header, snapshot, visibleHead, endVersion, funcName, pageCtx, &endSnap, &endSchema);
        }
        MemoryContextDelete(pageCtx);

        TupleDesc endHistDesc = NULL;
        LakeInlineBinding* endBinding = LakeLoadHistoricalInlineBinding(
            relationOid, endSnap, endSchema, visibleHead, snapshot, &endHistDesc);
        int prefixCols = 0;
        LakeCdcValidateExpectedDesc(mode, expectedDesc, endHistDesc, &prefixCols);

        LakeLeaveFastValidationScope(fastScope);
        fastActive = false;

        if (startSnap <= endSnap) {
            int rewriteCount = 0;
            int64* rewriteSnaps = LakeCdcLoadRewriteSnapshots(
                endBinding, snapshot, startSnap, endSnap, scanCtx, &rewriteCount);

            Datum* outValues = (Datum*)palloc0(sizeof(Datum) * outDesc->natts);
            bool* outNulls = (bool*)palloc0(sizeof(bool) * outDesc->natts);
            LakeCdcEmitContext emitCtx = {};
            emitCtx.outDesc = outDesc;
            emitCtx.prefixCols = prefixCols;
            emitCtx.bizCols = endBinding->column_count;
            emitCtx.outValues = outValues;
            emitCtx.outNulls = outNulls;

            if (mode == LAKE_CDC_INSERTIONS) {
                emitCtx.store = outStore;
                LakeCdcScanInlineCarriers(endBinding, snapshot, startSnap, endSnap, false,
                    scanCtx, LakeCdcEmitFeedRow, &emitCtx);
                LakeCdcScanFileInsertions(endBinding, snapshot, startSnap, endSnap,
                    rewriteSnaps, rewriteCount, scanCtx, LakeCdcEmitFeedRow, &emitCtx);
            } else if (mode == LAKE_CDC_DELETIONS) {
                emitCtx.store = outStore;
                LakeCdcScanInlineCarriers(endBinding, snapshot, startSnap, endSnap, true,
                    scanCtx, LakeCdcEmitFeedRow, &emitCtx);
                LakeCdcScanFileDeletions(endBinding, snapshot, startSnap, endSnap,
                    rewriteSnaps, rewriteCount, scanCtx, LakeCdcEmitFeedRow, &emitCtx);
            } else {
                HASHCTL ctl;
                errno_t rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
                securec_check(rc, "", "");
                ctl.keysize = sizeof(LakeCdcKey);
                ctl.entrysize = sizeof(LakeCdcKeyEntry);
                ctl.hcxt = scanCtx;
                emitCtx.delKeys = hash_create("Lake CDC deletion keys", 256, &ctl,
                    HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
                emitCtx.insertLabel = CStringGetTextDatum("insert");
                emitCtx.postimageLabel = CStringGetTextDatum("update_postimage");
                Datum deleteLabel = CStringGetTextDatum("delete");
                Datum preimageLabel = CStringGetTextDatum("update_preimage");

                delStore = tuplestore_begin_heap(false, false, workMemKb);
                emitCtx.store = (Tuplestorestate*)delStore;
                emitCtx.stageDeletions = true;
                LakeCdcScanInlineCarriers(endBinding, snapshot, startSnap, endSnap, true,
                    scanCtx, LakeCdcEmitFeedRow, &emitCtx);
                LakeCdcScanFileDeletions(endBinding, snapshot, startSnap, endSnap,
                    rewriteSnaps, rewriteCount, scanCtx, LakeCdcEmitFeedRow, &emitCtx);

                emitCtx.store = outStore;
                emitCtx.stageDeletions = false;
                LakeCdcScanInlineCarriers(endBinding, snapshot, startSnap, endSnap, false,
                    scanCtx, LakeCdcEmitFeedRow, &emitCtx);
                LakeCdcScanFileInsertions(endBinding, snapshot, startSnap, endSnap,
                    rewriteSnaps, rewriteCount, scanCtx, LakeCdcEmitFeedRow, &emitCtx);

                MemoryContextSwitchTo(scanCtx);
                delSlot = MakeSingleTupleTableSlot(outDesc);
                while (tuplestore_gettupleslot((Tuplestorestate*)delStore, true, false, (TupleTableSlot*)delSlot)) {
                    CHECK_FOR_INTERRUPTS();
                    tableam_tslot_getallattrs((TupleTableSlot*)delSlot);
                    LakeCdcKey key = {
                        DatumGetInt64(delSlot->tts_values[0]),
                        DatumGetInt64(delSlot->tts_values[1])
                    };
                    bool found = false;
                    LakeCdcKeyEntry* entry = (LakeCdcKeyEntry*)hash_search(emitCtx.delKeys, &key, HASH_FIND, &found);
                    delSlot->tts_values[2] = (found && entry != NULL && entry->matched) ? preimageLabel : deleteLabel;
                    delSlot->tts_isnull[2] = false;
                    tuplestore_putvalues(outStore, outDesc, delSlot->tts_values, delSlot->tts_isnull);
                    ExecClearTuple((TupleTableSlot*)delSlot);
                }
                ExecDropSingleTupleTableSlot((TupleTableSlot*)delSlot);
                delSlot = NULL;
                tuplestore_end((Tuplestorestate*)delStore);
                delStore = NULL;
            }
        }

        UnregisterSnapshot(snapshot);
        snapshot = NULL;
        MemoryContextSwitchTo(callerCtx);
        MemoryContextDelete(scanCtx);
        scanCtx = NULL;

        tuplestore_donestoring(outStore);
        result->returnMode = SFRM_Materialize;
        result->setResult = outStore;
        result->setDesc = outDesc;
    }
    PG_CATCH();
    {
        if (fastActive) {
            LakeLeaveFastValidationScope(fastScope);
            fastActive = false;
        }
        if (delSlot != NULL) {
            MemoryContextSwitchTo(scanCtx);
            ExecDropSingleTupleTableSlot((TupleTableSlot*)delSlot);
            delSlot = NULL;
        }
        if (delStore != NULL) {
            MemoryContextSwitchTo(scanCtx);
            tuplestore_end((Tuplestorestate*)delStore);
            delStore = NULL;
        }
        if (snapshot != NULL) {
            UnregisterSnapshot(snapshot);
            snapshot = NULL;
        }
        MemoryContextSwitchTo(callerCtx);
        if (scanCtx != NULL) {
            MemoryContextDelete(scanCtx);
            scanCtx = NULL;
        }
        PG_RE_THROW();
    }
    PG_END_TRY();

    fcinfo->isnull = true;
    return (Datum)0;
}

Datum og_lake_table_insertions_version(PG_FUNCTION_ARGS)
{
    return LakeCdcExecute(fcinfo, LAKE_CDC_INSERTIONS, false);
}

Datum og_lake_table_insertions_timestamp(PG_FUNCTION_ARGS)
{
    return LakeCdcExecute(fcinfo, LAKE_CDC_INSERTIONS, true);
}

Datum og_lake_table_deletions_version(PG_FUNCTION_ARGS)
{
    return LakeCdcExecute(fcinfo, LAKE_CDC_DELETIONS, false);
}

Datum og_lake_table_deletions_timestamp(PG_FUNCTION_ARGS)
{
    return LakeCdcExecute(fcinfo, LAKE_CDC_DELETIONS, true);
}

Datum og_lake_table_changes_version(PG_FUNCTION_ARGS)
{
    return LakeCdcExecute(fcinfo, LAKE_CDC_CHANGES, false);
}

Datum og_lake_table_changes_timestamp(PG_FUNCTION_ARGS)
{
    return LakeCdcExecute(fcinfo, LAKE_CDC_CHANGES, true);
}
