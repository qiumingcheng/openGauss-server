/* Native read-only historical table scan (SQL0.57).
 * Snapshot/schema interval semantics adapted from DuckLake 0aaa8e10d4adfe2ca37da77134e991d20106d0e7
 * (MIT; notice in lake_catalog_ddl.inc): storage/ducklake_metadata_manager.cpp GetSnapshot()
 * and pg_ducklake src/time_travel.cpp.
 * Host adaptation: standard openGauss FROM-record materialized SRF with explicit AS column
 * definitions verified against historical DuckLake + column_binding metadata before scanning,
 * single pinned RC statement snapshot, 64-bit own-XID exclusion, extant authorized regclass ACL
 * (schema USAGE + whole-table or complete live-column SELECT), and journal-free read views. */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "lake_inline_private.h"
#include "lake_inline_reader.h"
#include "lake_native_partition.h"
#include "lake_txn_private.h"
#include "lake_version.h"
#include "access/heapam.h"
#include "access/og_lake.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "commands/sec_rls_cmds.h"
#include "distributelayer/streamCore.h"
#include "executor/spi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgxc/pgxc.h"
#include "storage/lmgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/sec_rls_utils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/tuplestore.h"

extern "C" Datum og_lake_time_travel_version(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_time_travel_timestamp(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(og_lake_time_travel_version);
PG_FUNCTION_INFO_V1(og_lake_time_travel_timestamp);

static bool LakeTimeTravelOwnRow(const LakeMetaRows* rows, uint32 row, int column)
{
    return TransactionIdIsCurrentTransactionId(
        DatumGetTransactionId(LakeMetadataValue(rows, row, column, XIDOID)));
}

static int64 LakeTimeTravelVisibleHead(LakeInlineBinding* header, Snapshot snapshot, MemoryContext page)
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
            if (!LakeTimeTravelOwnRow(rows, i, 3)) {
                MemoryContextSwitchTo(parent);
                MemoryContextReset(page);
                return id;
            }
            sawOwn = true;
        }
        MemoryContextSwitchTo(parent);
    }
}

static void LakeTimeTravelResolveVersion(LakeInlineBinding* header, Snapshot snapshot, int64 visibleHead,
    int64 requestedVersion, MemoryContext page, int64* targetSnapshotOut, int64* targetSchemaOut)
{
    if (requestedVersion < 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("time_travel snapshot version must be nonnegative")));
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
    if (rows->count == 0 || LakeTimeTravelOwnRow(rows, 0, 3))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("requested snapshot " INT64_FORMAT " does not exist or has expired", requestedVersion)));
    int64 snapId = DatumGetInt64(LakeMetadataValue(rows, 0, 1, INT8OID));
    int64 schemaVer = DatumGetInt64(LakeMetadataValue(rows, 0, 2, INT8OID));
    if (snapId != requestedVersion || schemaVer < 0)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("invalid resolved lake snapshot metadata")));
    MemoryContextSwitchTo(parent);
    MemoryContextReset(page);
    *targetSnapshotOut = snapId;
    *targetSchemaOut = schemaVer;
}

static void LakeTimeTravelResolveTimestamp(LakeInlineBinding* header, Snapshot snapshot, int64 visibleHead,
    TimestampTz requestedTime, MemoryContext page, int64* targetSnapshotOut, int64* targetSchemaOut)
{
    if (TIMESTAMP_NOT_FINITE(requestedTime) || !IS_VALID_TIMESTAMP(requestedTime) ||
        requestedTime > GetCurrentTimestamp())
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("cannot time travel to a non-finite or future timestamp")));
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
                errmsg("no committed lake snapshot exists at or before the requested timestamp")));
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
            if (!LakeTimeTravelOwnRow(rows, i, 4)) {
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

static void LakeTimeTravelValidateExpectedDesc(TupleDesc expectedDesc, TupleDesc histDesc)
{
    if (expectedDesc == NULL || histDesc == NULL || expectedDesc->natts != histDesc->natts)
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
            errmsg("time_travel AS column count (%d) does not match historical table schema (%d)",
                expectedDesc ? expectedDesc->natts : 0, histDesc ? histDesc->natts : 0)));
    for (int i = 0; i < histDesc->natts; ++i) {
        Form_pg_attribute expAttr = &expectedDesc->attrs[i];
        Form_pg_attribute histAttr = &histDesc->attrs[i];
        if (expAttr->attisdropped ||
            expAttr->atttypid != histAttr->atttypid ||
            expAttr->atttypmod != histAttr->atttypmod ||
            expAttr->attcollation != histAttr->attcollation)
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("time_travel AS column %d (\"%s\") does not match historical column \"%s\" type, typmod or collation",
                    i + 1, NameStr(expAttr->attname), NameStr(histAttr->attname))));
    }
}

static Oid LakeTimeTravelAuthorizeTarget(Oid relationOid)
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
    LakeRejectNativePartitionDDL(host, "time_travel");
    if (!LakeIsNativeForeignRelation(host)) {
        char* relName = pstrdup(RelationGetRelationName(host));
        relation_close(host, NoLock);
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg("time_travel target \"%s\" is not an og_lake foreign table", relName)));
    }
    ForeignTable* foreign = GetForeignTable(relationOid);
    if (!LakeTrustedForeignServer(foreign->serverid)) {
        relation_close(host, NoLock);
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("time_travel target server is not a trusted og_lake server")));
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
            errmsg("time_travel requires a committed native lake foreign table")));
    }
    Oid serverOid = foreign->serverid;
    relation_close(host, NoLock);
    return serverOid;
}

static Datum LakeTimeTravelExecute(FunctionCallInfo fcinfo, bool byTimestamp)
{
    if (StreamThreadAmI() || IS_SPQ_RUNNING)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("time_travel requires the invoking serial backend")));
    if (!ActiveSnapshotSet())
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("time_travel requires an active statement snapshot")));
    LakeRequireAccessPolicy();
    LakeCheckMaskingAdmission();
    if (!LakeTimeTravelVersionEnabled())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("time_travel requires installed SQL0.57")));
    if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
            errmsg("time_travel arguments cannot be NULL")));

    int64 requestedVersion = 0;
    TimestampTz requestedTime = 0;
    if (byTimestamp) {
        requestedTime = PG_GETARG_TIMESTAMPTZ(1);
        if (TIMESTAMP_NOT_FINITE(requestedTime) || !IS_VALID_TIMESTAMP(requestedTime) ||
            requestedTime > GetCurrentTimestamp())
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("cannot time travel to a non-finite or future timestamp")));
    } else {
        requestedVersion = PG_GETARG_INT64(1);
        if (requestedVersion < 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("time_travel snapshot version must be nonnegative")));
    }

    ReturnSetInfo* result = (ReturnSetInfo*)fcinfo->resultinfo;
    if (!result || !IsA(result, ReturnSetInfo) || !(result->allowedModes & SFRM_Materialize) || !result->econtext)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("time_travel requires materialize mode")));

    TupleDesc expectedDesc = NULL;
    if (get_call_result_type(fcinfo, NULL, &expectedDesc) != TYPEFUNC_COMPOSITE || expectedDesc == NULL)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("time_travel requires a FROM clause with an explicit AS column definition list")));

    Oid relationOid = PG_GETARG_OID(0);
    Oid serverOid = LakeTimeTravelAuthorizeTarget(relationOid);

    MemoryContext callerCtx = MemoryContextSwitchTo(result->econtext->ecxt_per_query_memory);
    TupleDesc outDesc = BlessTupleDesc(CreateTupleDescCopy(expectedDesc));
    Tuplestorestate* store = tuplestore_begin_heap(true, false,
        Max(64, Min(u_sess->attr.attr_memory.work_mem, 1024)));
    MemoryContextSwitchTo(callerCtx);

    MemoryContext volatile scanCtx = AllocSetContextCreate(CurrentMemoryContext,
        "Lake time_travel execution", ALLOCSET_DEFAULT_SIZES);
    Snapshot volatile snapshot = NULL;
    LakeInlineReader* volatile reader = NULL;
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
            "Lake time_travel snapshot page", ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
        int64 visibleHead = LakeTimeTravelVisibleHead(header, snapshot, pageCtx);
        int64 targetSnapshot = -1;
        int64 targetSchema = -1;
        if (byTimestamp)
            LakeTimeTravelResolveTimestamp(header, snapshot, visibleHead, requestedTime, pageCtx,
                &targetSnapshot, &targetSchema);
        else
            LakeTimeTravelResolveVersion(header, snapshot, visibleHead, requestedVersion, pageCtx,
                &targetSnapshot, &targetSchema);
        MemoryContextDelete(pageCtx);

        TupleDesc histDesc = NULL;
        LakeInlineBinding* binding = LakeLoadHistoricalInlineBinding(
            relationOid, targetSnapshot, targetSchema, visibleHead, snapshot, &histDesc);
        LakeTimeTravelValidateExpectedDesc(expectedDesc, histDesc);

        LakeLeaveFastValidationScope(fastScope);
        fastActive = false;

        LakeTxnTable* view = LakeTxnCreateQueryReadView(binding, snapshot, histDesc);
        reader = LakeInlineReaderOpen(view, InvalidCommandId);

        MemoryContext rowCtx = AllocSetContextCreate(scanCtx,
            "Lake time_travel row", ALLOCSET_DEFAULT_SIZES);
        Datum* values = (Datum*)palloc0(sizeof(Datum) * binding->column_count);
        bool* nulls = (bool*)palloc0(sizeof(bool) * binding->column_count);
        for (;;) {
            CHECK_FOR_INTERRUPTS();
            MemoryContextReset(rowCtx);
            MemoryContextSwitchTo(rowCtx);
            LakeInlineRowIdentity identity = {};
            if (!LakeInlineReaderNext((LakeInlineReader*)reader, values, nulls, &identity))
                break;
            tuplestore_putvalues(store, outDesc, values, nulls);
        }

        MemoryContextSwitchTo(scanCtx);
        LakeInlineReaderClose((LakeInlineReader*)reader);
        reader = NULL;
        UnregisterSnapshot(snapshot);
        snapshot = NULL;
        MemoryContextSwitchTo(callerCtx);
        MemoryContextDelete(scanCtx);
        scanCtx = NULL;

        tuplestore_donestoring(store);
        result->returnMode = SFRM_Materialize;
        result->setResult = store;
        result->setDesc = outDesc;
    }
    PG_CATCH();
    {
        if (fastActive) {
            LakeLeaveFastValidationScope(fastScope);
            fastActive = false;
        }
        if (reader != NULL) {
            MemoryContextSwitchTo(scanCtx);
            LakeInlineReaderClose((LakeInlineReader*)reader);
            reader = NULL;
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

Datum og_lake_time_travel_version(PG_FUNCTION_ARGS)
{
    return LakeTimeTravelExecute(fcinfo, false);
}

Datum og_lake_time_travel_timestamp(PG_FUNCTION_ARGS)
{
    return LakeTimeTravelExecute(fcinfo, true);
}
