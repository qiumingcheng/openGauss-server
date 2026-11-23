/* Native local physical file and retired index-segment garbage collection (SQL0.69 / N9b).
 * Protected by catalog ExclusiveLock try-guard, SQL0.68 active snapshot pins,
 * same-xact expiry refusal, conservative host GetOldestXmin, and SDK openat2 unlink. */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "lake_txn_private.h"
#include "lake_inline_private.h"
#include "lake_file_sdk.h"
#include "lake_version.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/tuplestore.h"
#include "utils/uuid.h"
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

extern "C" Datum og_lake_cleanup_old_files(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_delete_orphaned_files(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_cleanup_index_segments(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(og_lake_cleanup_old_files);
PG_FUNCTION_INFO_V1(og_lake_delete_orphaned_files);
PG_FUNCTION_INFO_V1(og_lake_cleanup_index_segments);

static const int LakeCleanupBatchLimit = 512;
static const int LakeOrphanScanMaxFiles = 16384;
static const int LakeOrphanScanMaxDepth = 8;

struct LakePathHashEntry {
    char key[MAXPGPATH];
};

struct LakeScheduledItem {
    int64 file_id;
    char* path;
    bool path_is_relative;
    TimestampTz schedule_start;
    bool schedule_null;
    TransactionId retired_xmin;
    bool xmin_frozen;
};

struct LakeOrphanItem {
    char* rel_path;
    int64 file_bytes;
    TimestampTz mtime;
};

struct LakeSegmentCleanupItem {
    Datum segment_uuid_datum;
    char* segment_uuid_text;
    char* artifact_uri;
    char* state;
    TransactionId retired_xmin;
    bool xmin_frozen;
};

static bool LakeEndsWithParquet(const char* path)
{
    if (path == NULL)
        return false;
    Size len = strlen(path);
    return len > 8 && strcmp(path + len - 8, ".parquet") == 0;
}

static void LakeNormalizeRelativePathChecked(const char* raw, StringInfo out)
{
    if (raw == NULL || *raw == '\0' || *raw == '/' || strchr(raw, ':') != NULL)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("cleanup encountered invalid relative path")));
    const char* at = raw;
    while (*at) {
        const char* end = strchr(at, '/');
        if (end == NULL)
            end = at + strlen(at);
        Size len = end - at;
        if ((len == 1 && at[0] == '.') || (len == 2 && at[0] == '.' && at[1] == '.'))
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("cleanup rejected directory traversal component")));
        if (len > 0) {
            if (out->len > 0 && out->data[out->len - 1] != '/')
                appendStringInfoChar(out, '/');
            appendBinaryStringInfo(out, at, len);
        }
        at = *end ? end + 1 : end;
    }
    if (out->len == 0 || out->len >= MAXPGPATH)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            errmsg("cleanup relative path exceeds host limit")));
}

void LakeCleanupAppendPart(StringInfo target, const char* part)
{
    if (part == NULL || *part == '\0')
        return;
    if (*part == '/' || strchr(part, ':') != NULL)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("cleanup path component must be relative")));
    const char* at = part;
    while (*at) {
        const char* end = strchr(at, '/');
        if (end == NULL)
            end = at + strlen(at);
        Size len = end - at;
        if ((len == 1 && at[0] == '.') || (len == 2 && at[0] == '.' && at[1] == '.'))
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("cleanup path contains invalid component")));
        if (len > 0) {
            if (target->len > 0 && target->data[target->len - 1] != '/')
                appendStringInfoChar(target, '/');
            appendBinaryStringInfo(target, at, len);
        }
        at = *end ? end + 1 : end;
    }
}

static TimestampTz LakeCleanupResolveCutoff(LakeInlineBinding* binding, Snapshot snapshot,
    bool hasExplicitOlder, TimestampTz explicitOlder, bool cleanupAll, bool* hasCutoff)
{
    if (hasExplicitOlder && cleanupAll)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("cleanup older_than and cleanup_all are mutually exclusive")));
    if (cleanupAll) {
        *hasCutoff = false;
        return 0;
    }
    *hasCutoff = true;
    if (hasExplicitOlder)
        return explicitOlder;

    const char* intervalText = "2 days";
    Oid types[1] = {TEXTOID};
    Datum args[1] = {CStringGetTextDatum("delete_older_than")};
    LakeMetaRows* optRows = LakeMetadataQuery(psprintf(
        "SELECT value::text FROM %s WHERE key=$1 AND scope IS NULL LIMIT 2",
        LakeMetadataRelation(binding->metadata_schema_oid, "ducklake_metadata")),
        1, types, args, NULL, snapshot, true, SPI_OK_SELECT, binding->metadata_schema_oid);
    if (optRows->count > 1)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("duplicate delete_older_than catalog option")));
    if (optRows->count == 1)
        intervalText = LakeMetadataText(optRows, 0, 1);

    Datum ivDatum = DirectFunctionCall3(interval_in, CStringGetDatum(intervalText),
        ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
    Interval* iv = DatumGetIntervalP(ivDatum);
    Interval zero = {0};
    if (interval_cmp_internal(iv, &zero) < 0)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("stored delete_older_than interval is negative")));
    return DatumGetTimestampTz(DirectFunctionCall2(timestamptz_mi_interval,
        TimestampTzGetDatum(GetCurrentTransactionStartTimestamp()), ivDatum));
}

static void LakeCheckWriteCleanupPreconditions(ForeignServer* server, bool checkNoExpiryInTxn)
{
    if (u_sess->utils_cxt.XactIsoLevel != XACT_READ_COMMITTED)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("physical cleanup requires READ COMMITTED")));
    if (u_sess->attr.attr_common.XactReadOnly)
        ereport(ERROR, (errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
            errmsg("cannot physically cleanup lake catalog in a read-only transaction")));
    LakeTxnRejectBindingDdl();
    if (checkNoExpiryInTxn && LakeTxnHasSnapshotExpiry())
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("physical cleanup cannot run in the same transaction as expire_snapshots; commit the expiry first")));
    if (LakeSnapshotPinMinForCatalog(u_sess->proc_cxt.MyDatabaseId, server->serverid) >= 0)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("cannot physically cleanup lake catalog \"%s\" while active snapshot pins exist",
                server->servername)));
    LakeLockCatalogRegistration();
    if (!LakeTryCatalogGuard(server->serverid, true))
        ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
            errmsg("cannot physically cleanup lake catalog \"%s\" while readers or writers hold catalog guard",
                server->servername)));
}

static void LakeValidateScheduledRelationShape(Relation relation)
{
    const char* names[] = {"data_file_id", "path", "path_is_relative", "schedule_start"};
    const Oid types[] = {INT8OID, VARCHAROID, BOOLOID, TIMESTAMPTZOID};
    TupleDesc descriptor = RelationGetDescr(relation);
    if (descriptor->natts != 4 || relation->trigdesc != NULL || relation->rd_rules != NULL)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("physical cleanup requires the unmodified ducklake_files_scheduled_for_deletion relation")));
    for (int i = 0; i < 4; ++i) {
        Form_pg_attribute column = &descriptor->attrs[i];
        if (column->attisdropped || strcmp(NameStr(column->attname), names[i]) != 0 ||
            column->atttypid != types[i])
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("ducklake_files_scheduled_for_deletion layout changed")));
    }
}

static bool LakeCleanupPinFree(LakeInlineBinding* binding)
{
    return LakeSnapshotPinMinForCatalog(u_sess->proc_cxt.MyDatabaseId, binding->server_oid) < 0;
}

static void LakeInsertPathKey(HTAB* tab, const char* path)
{
    if (path == NULL || *path == '\0' || strlen(path) >= MAXPGPATH)
        return;
    LakePathHashEntry key;
    errno_t rc = memset_s(&key, sizeof(key), 0, sizeof(key));
    securec_check(rc, "", "");
    rc = strncpy_s(key.key, sizeof(key.key), path, MAXPGPATH - 1);
    securec_check(rc, "", "");
    (void)hash_search(tab, &key, HASH_ENTER, NULL);
}

static bool LakeHasPathKey(HTAB* tab, const char* path)
{
    if (path == NULL || *path == '\0' || strlen(path) >= MAXPGPATH)
        return false;
    LakePathHashEntry key;
    errno_t rc = memset_s(&key, sizeof(key), 0, sizeof(key));
    securec_check(rc, "", "");
    rc = strncpy_s(key.key, sizeof(key.key), path, MAXPGPATH - 1);
    securec_check(rc, "", "");
    return hash_search(tab, &key, HASH_FIND, NULL) != NULL;
}

static HTAB* LakeCreatePathSet(MemoryContext ctx, const char* label)
{
    HASHCTL ctl;
    errno_t rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "", "");
    ctl.keysize = MAXPGPATH;
    ctl.entrysize = sizeof(LakePathHashEntry);
    ctl.hcxt = ctx;
    return hash_create(label, 1024, &ctl, HASH_ELEM | HASH_CONTEXT);
}

Datum og_lake_cleanup_old_files(PG_FUNCTION_ARGS)
{
    LakeInlineRequireAdministrator();
    LakeRequireAccessPolicy();
    if (!LakePhysicalCleanupVersionEnabled())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("physical cleanup requires installed SQL0.69")));
    if (PG_ARGISNULL(0) || PG_ARGISNULL(3))
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
            errmsg("cleanup_old_files catalog and dry_run cannot be NULL")));

    bool hasOlder = !PG_ARGISNULL(1);
    TimestampTz explicitOlder = hasOlder ? PG_GETARG_TIMESTAMPTZ(1) : 0;
    bool cleanupAll = PG_ARGISNULL(2) ? false : PG_GETARG_BOOL(2);
    bool dry = PG_GETARG_BOOL(3);

    ReturnSetInfo* result = (ReturnSetInfo*)fcinfo->resultinfo;
    if (result == NULL || !IsA(result, ReturnSetInfo) || !(result->allowedModes & SFRM_Materialize) || !result->econtext)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("cleanup_old_files requires materialize mode")));

    ForeignServer* server = GetForeignServerByName(NameStr(*PG_GETARG_NAME(0)), false);
    MemoryContext caller = MemoryContextSwitchTo(result->econtext->ecxt_per_query_memory);
    TupleDesc descriptor = CreateTemplateTupleDesc(5, false);
    TupleDescInitEntry(descriptor, 1, "data_file_id", INT8OID, -1, 0);
    TupleDescInitEntry(descriptor, 2, "path", TEXTOID, -1, DEFAULT_COLLATION_OID);
    TupleDescInitEntry(descriptor, 3, "schedule_start", TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(descriptor, 4, "deleted", BOOLOID, -1, 0);
    TupleDescInitEntry(descriptor, 5, "status", TEXTOID, -1, DEFAULT_COLLATION_OID);
    descriptor = BlessTupleDesc(descriptor);
    Tuplestorestate* store = tuplestore_begin_heap(true, false,
        Max(64, Min(u_sess->attr.attr_memory.work_mem, 1024)));
    MemoryContext workCtx = AllocSetContextCreate(result->econtext->ecxt_per_query_memory,
        "LakeCleanupOldFiles", ALLOCSET_DEFAULT_SIZES);
    MemoryContextSwitchTo(workCtx);

    if (!dry)
        LakeCheckWriteCleanupPreconditions(server, true);

    Snapshot volatile snapshot = NULL;
    PG_TRY();
    {
        Snapshot initial = ActiveSnapshotSet() ? GetActiveSnapshot() : GetTransactionSnapshot();
        LakeInlineBinding* binding = LakeLoadCatalogHeader(server->serverid, initial);
        Oid ns = binding->metadata_schema_oid;
        if (!dry && binding->read_only)
            ereport(ERROR, (errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
                errmsg("cannot physically cleanup a read-only lake catalog")));

        Oid schedOid = get_relname_relid("ducklake_files_scheduled_for_deletion", ns);
        if (!OidIsValid(schedOid))
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE),
                errmsg("missing ducklake_files_scheduled_for_deletion table")));
        Relation schedRel = heap_open(schedOid, dry ? AccessShareLock : ShareRowExclusiveLock);
        LakeValidateScheduledRelationShape(schedRel);

        if (!dry)
            CommandCounterIncrement();
        snapshot = RegisterSnapshot(dry ? initial : GetLatestSnapshot());
        LakeInlineBinding* fresh = LakeLoadCatalogHeader(server->serverid, snapshot);
        if (fresh->metadata_schema_oid != ns || fresh->server_oid != binding->server_oid ||
            strcmp(fresh->data_root, binding->data_root) != 0 || (!dry && fresh->read_only))
            ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                errmsg("physical cleanup catalog binding changed")));

        const bool pinFree = LakeCleanupPinFree(fresh);
        if (!dry && !pinFree)
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("cannot physically cleanup lake catalog \"%s\" while active snapshot pins exist",
                    server->servername)));

        bool hasCutoff = true;
        TimestampTz cutoff = LakeCleanupResolveCutoff(fresh, snapshot, hasOlder, explicitOlder, cleanupAll, &hasCutoff);
        TransactionId oldestXmin = GetOldestXmin(schedRel, false, true);

        /* Collect scheduled rows BEFORE closing the scan and BEFORE invoking SDK/SPI mutations. */
        List* candidates = NIL;
        TupleDesc schedDesc = RelationGetDescr(schedRel);
        TableScanDesc scan = tableam_scan_begin(schedRel, snapshot, 0, NULL);
        PG_TRY();
        {
            HeapTuple tup;
            while ((tup = (HeapTuple)tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
                CHECK_FOR_INTERRUPTS();
                bool isNull[4] = {false};
                Datum vals[4];
                for (int c = 0; c < 4; ++c)
                    vals[c] = tableam_tops_tuple_getattr(tup, c + 1, schedDesc, &isNull[c]);
                if (isNull[0] || isNull[1] || isNull[2])
                    continue;
                TimestampTz schedTs = isNull[3] ? 0 : DatumGetTimestampTz(vals[3]);
                if (hasCutoff && (isNull[3] || timestamptz_cmp_internal(schedTs, cutoff) >= 0))
                    continue;

                LakeScheduledItem* item = (LakeScheduledItem*)palloc0(sizeof(LakeScheduledItem));
                item->file_id = DatumGetInt64(vals[0]);
                item->path = TextDatumGetCString(vals[1]);
                item->path_is_relative = DatumGetBool(vals[2]);
                item->schedule_start = schedTs;
                item->schedule_null = isNull[3];
                item->xmin_frozen = HeapTupleHeaderXminFrozen(tup->t_data);
                item->retired_xmin = item->xmin_frozen ? FrozenTransactionId : HeapTupleGetRawXmin(tup);
                candidates = lappend(candidates, item);
                if (list_length(candidates) >= LakeCleanupBatchLimit)
                    break;
            }
            tableam_scan_end(scan);
        }
        PG_CATCH();
        {
            tableam_scan_end(scan);
            heap_close(schedRel, NoLock);
            PG_RE_THROW();
        }
        PG_END_TRY();
        heap_close(schedRel, NoLock);

        const char* dataRel = LakeMetadataRelation(ns, "ducklake_data_file");
        const char* delRel = LakeMetadataRelation(ns, "ducklake_delete_file");
        const char* schedName = LakeMetadataRelation(ns, "ducklake_files_scheduled_for_deletion");
        HTAB* liveRelPaths = LakeCreatePathSet(workCtx, "LakeLiveCleanupRefs");
        LakeMetaRows* activeData = LakeMetadataQuery(psprintf(
            "SELECT DISTINCT ON (f.data_file_id) f.data_file_id, f.path::text, f.path_is_relative, "
            "COALESCE(t.path::text,''), COALESCE(s.path::text,'') "
            "FROM %s f JOIN %s t ON t.table_id=f.table_id JOIN %s s ON s.schema_id=t.schema_id "
            "ORDER BY f.data_file_id, t.begin_snapshot DESC, s.begin_snapshot DESC",
            dataRel, LakeMetadataRelation(ns, "ducklake_table"),
            LakeMetadataRelation(ns, "ducklake_schema")),
            0, NULL, NULL, NULL, snapshot, true, SPI_OK_SELECT, ns);
        for (uint32 r = 0; r < activeData->count; ++r) {
            if (!DatumGetBool(LakeMetadataValue(activeData, r, 3, BOOLOID)))
                continue;
            StringInfoData rel;
            initStringInfo(&rel);
            LakeCleanupAppendPart(&rel, LakeMetadataText(activeData, r, 5));
            LakeCleanupAppendPart(&rel, LakeMetadataText(activeData, r, 4));
            LakeCleanupAppendPart(&rel, LakeMetadataText(activeData, r, 2));
            if (rel.len > 0 && rel.len < MAXPGPATH)
                LakeInsertPathKey(liveRelPaths, rel.data);
        }
        LakeMetaRows* activeDel = LakeMetadataQuery(psprintf(
            "SELECT DISTINCT ON (d.delete_file_id) d.delete_file_id, d.path::text, d.path_is_relative, "
            "COALESCE(t.path::text,''), COALESCE(s.path::text,'') "
            "FROM %s d JOIN %s t ON t.table_id=d.table_id JOIN %s s ON s.schema_id=t.schema_id "
            "ORDER BY d.delete_file_id, t.begin_snapshot DESC, s.begin_snapshot DESC",
            delRel, LakeMetadataRelation(ns, "ducklake_table"),
            LakeMetadataRelation(ns, "ducklake_schema")),
            0, NULL, NULL, NULL, snapshot, true, SPI_OK_SELECT, ns);
        for (uint32 r = 0; r < activeDel->count; ++r) {
            if (!DatumGetBool(LakeMetadataValue(activeDel, r, 3, BOOLOID)))
                continue;
            StringInfoData rel;
            initStringInfo(&rel);
            LakeCleanupAppendPart(&rel, LakeMetadataText(activeDel, r, 5));
            LakeCleanupAppendPart(&rel, LakeMetadataText(activeDel, r, 4));
            LakeCleanupAppendPart(&rel, LakeMetadataText(activeDel, r, 2));
            if (rel.len > 0 && rel.len < MAXPGPATH)
                LakeInsertPathKey(liveRelPaths, rel.data);
        }

        const OgLakeParquetMaintenanceApi* maint = dry ? NULL : LakeFileMaintenanceProvider();
        OgLakeSdkLimits limits = dry ? OgLakeSdkLimits{} : LakeFileLimits();

        ListCell* lc;
        foreach(lc, candidates) {
            CHECK_FOR_INTERRUPTS();
            LakeScheduledItem* item = (LakeScheduledItem*)lfirst(lc);
            bool deleted = false;
            const char* statusText = NULL;

            const bool xminSafe = item->xmin_frozen ||
                (TransactionIdIsValid(item->retired_xmin) && TransactionIdIsValid(oldestXmin) &&
                 TransactionIdPrecedes(item->retired_xmin, oldestXmin));

            char* normRel = NULL;
            bool pathOk = false;
            if (item->path_is_relative) {
                StringInfoData relBuf;
                initStringInfo(&relBuf);
                PG_TRY();
                {
                    LakeNormalizeRelativePathChecked(item->path, &relBuf);
                    if (LakeEndsWithParquet(relBuf.data)) {
                        normRel = relBuf.data;
                        pathOk = true;
                    }
                }
                PG_CATCH();
                {
                    FlushErrorState();
                    pathOk = false;
                }
                PG_END_TRY();
            }

            Oid idType = INT8OID;
            Datum idArg = Int64GetDatum(item->file_id);
            LakeMetaRows* idCheck = LakeMetadataQuery(psprintf(
                "SELECT EXISTS(SELECT 1 FROM %s WHERE data_file_id=$1) OR "
                "EXISTS(SELECT 1 FROM %s WHERE delete_file_id=$1)",
                dataRel, delRel),
                1, &idType, &idArg, NULL, snapshot, true, SPI_OK_SELECT, ns);
            const bool idRef = DatumGetBool(LakeMetadataValue(idCheck, 0, 1, BOOLOID));
            const bool pathRef = pathOk && LakeHasPathKey(liveRelPaths, normRel);

            if (idRef || pathRef) {
                statusText = "blocked_still_referenced";
            } else if (!item->path_is_relative || !pathOk) {
                statusText = "skipped_nonrelative_or_nonparquet";
            } else if (!pinFree) {
                statusText = "blocked_snapshot_pin";
            } else if (!xminSafe) {
                statusText = "deferred_oldest_xmin";
            } else if (dry) {
                statusText = "eligible";
            } else {
                uint32_t removed = 0;
                OgLakeSdkError err = {};
                int sdkRc = maint->delete_file(fresh->data_root, normRel, &limits, &removed, &err);
                CHECK_FOR_INTERRUPTS();
                LakeFileCheckStatus(sdkRc, &err);
                /* Only remove the scheduled record AFTER physical unlink / ENOENT confirmation. */
                Oid delTypes[2] = {INT8OID, TEXTOID};
                Datum delArgs[2] = {Int64GetDatum(item->file_id), CStringGetTextDatum(item->path)};
                (void)LakeMetadataQuery(psprintf(
                    "DELETE FROM %s WHERE data_file_id=$1 AND path::text=$2", schedName),
                    2, delTypes, delArgs, NULL, snapshot, false, SPI_OK_DELETE, ns);
                deleted = (removed != 0);
                statusText = deleted ? "deleted" : "already_absent";
            }

            Datum outVals[5];
            bool outNulls[5] = {false, false, item->schedule_null, false, false};
            outVals[0] = Int64GetDatum(item->file_id);
            outVals[1] = CStringGetTextDatum(item->path);
            outVals[2] = TimestampTzGetDatum(item->schedule_start);
            outVals[3] = BoolGetDatum(deleted);
            outVals[4] = CStringGetTextDatum(statusText);
            tuplestore_putvalues(store, descriptor, outVals, outNulls);
        }

        UnregisterSnapshot(snapshot);
        snapshot = NULL;
        tuplestore_donestoring(store);
        result->returnMode = SFRM_Materialize;
        result->setResult = store;
        result->setDesc = descriptor;
        MemoryContextSwitchTo(caller);
    }
    PG_CATCH();
    {
        if (snapshot)
            UnregisterSnapshot(snapshot);
        MemoryContextSwitchTo(caller);
        PG_RE_THROW();
    }
    PG_END_TRY();
    PG_RETURN_NULL();
}

int LakeOpenAnchoredDirectory(const char* absDir)
{
    if (absDir == NULL || absDir[0] != '/' || absDir[1] == '\0')
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("invalid local cleanup root path")));
    int curFd;
    do {
        curFd = open("/", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    } while (curFd < 0 && errno == EINTR);
    if (curFd < 0)
        ereport(ERROR, (errcode_for_file_access(),
            errmsg("cannot open root directory for lake cleanup: %m")));

    const char* p = absDir + 1;
    while (*p) {
        const char* slash = strchr(p, '/');
        Size len = slash ? (Size)(slash - p) : strlen(p);
        if (len > 0) {
            if ((len == 1 && p[0] == '.') || (len == 2 && p[0] == '.' && p[1] == '.') || len >= NAME_MAX) {
                close(curFd);
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("invalid path component in cleanup root")));
            }
            char comp[NAME_MAX + 1];
            memcpy(comp, p, len);
            comp[len] = '\0';
            int nextFd;
            do {
                nextFd = openat(curFd, comp, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
            } while (nextFd < 0 && errno == EINTR);
            close(curFd);
            if (nextFd < 0) {
                if (errno == ENOENT)
                    return -1;
                ereport(ERROR, (errcode_for_file_access(),
                    errmsg("cannot descend cleanup directory \"%s\": %m", comp)));
            }
            curFd = nextFd;
        }
        p = slash ? slash + 1 : p + len;
    }
    return curFd;
}

static void LakeWalkOrphanDir(int dirFd, const char* relPrefix, int depth,
    HTAB* knownPaths, bool hasCutoff, TimestampTz cutoff, List** outOrphans, int* seenCount)
{
    if (depth > LakeOrphanScanMaxDepth || *seenCount >= LakeOrphanScanMaxFiles)
        return;
    int dupFd = dup(dirFd);
    if (dupFd < 0)
        ereport(ERROR, (errcode_for_file_access(), errmsg("dup failed during orphan scan: %m")));
    DIR* dir = fdopendir(dupFd);
    if (dir == NULL) {
        close(dupFd);
        ereport(ERROR, (errcode_for_file_access(), errmsg("fdopendir failed during orphan scan: %m")));
    }
    PG_TRY();
    {
        struct dirent* ent;
        while ((ent = readdir(dir)) != NULL) {
            CHECK_FOR_INTERRUPTS();
            if (*seenCount >= LakeOrphanScanMaxFiles)
                break;
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
                continue;
            struct stat st;
            if (fstatat(dirFd, ent->d_name, &st, AT_SYMLINK_NOFOLLOW) != 0)
                continue;
            if (S_ISLNK(st.st_mode))
                continue;
            StringInfoData childRel;
            initStringInfo(&childRel);
            if (relPrefix && *relPrefix != '\0')
                appendStringInfo(&childRel, "%s/%s", relPrefix, ent->d_name);
            else
                appendStringInfoString(&childRel, ent->d_name);
            if (childRel.len >= MAXPGPATH)
                continue;
            if (S_ISDIR(st.st_mode)) {
                int subFd;
                do {
                    subFd = openat(dirFd, ent->d_name, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
                } while (subFd < 0 && errno == EINTR);
                if (subFd >= 0) {
                    PG_TRY();
                    {
                        LakeWalkOrphanDir(subFd, childRel.data, depth + 1, knownPaths,
                            hasCutoff, cutoff, outOrphans, seenCount);
                        close(subFd);
                    }
                    PG_CATCH();
                    {
                        close(subFd);
                        PG_RE_THROW();
                    }
                    PG_END_TRY();
                }
            } else if (S_ISREG(st.st_mode) && LakeEndsWithParquet(ent->d_name)) {
                if (LakeHasPathKey(knownPaths, childRel.data))
                    continue;
                TimestampTz mtimeTz = time_t_to_timestamptz((pg_time_t)st.st_mtime);
                if (hasCutoff && timestamptz_cmp_internal(mtimeTz, cutoff) >= 0)
                    continue;
                LakeOrphanItem* orphan = (LakeOrphanItem*)palloc0(sizeof(LakeOrphanItem));
                orphan->rel_path = childRel.data;
                orphan->file_bytes = (int64)st.st_size;
                orphan->mtime = mtimeTz;
                *outOrphans = lappend(*outOrphans, orphan);
                (*seenCount)++;
            }
        }
        closedir(dir);
    }
    PG_CATCH();
    {
        closedir(dir);
        PG_RE_THROW();
    }
    PG_END_TRY();
}

static void LakeCollectKnownRelativePaths(Oid ns, const char* dataRoot, Snapshot snapshot, HTAB* knownPaths)
{
    char* dataRel = LakeMetadataRelation(ns, "ducklake_data_file");
    char* delRel = LakeMetadataRelation(ns, "ducklake_delete_file");
    char* schedRel = LakeMetadataRelation(ns, "ducklake_files_scheduled_for_deletion");
    char* tblRel = LakeMetadataRelation(ns, "ducklake_table");
    char* schRel = LakeMetadataRelation(ns, "ducklake_schema");
    Size rootLen = strlen(dataRoot);

    const char* sqls[2];
    sqls[0] = psprintf(
        "SELECT DISTINCT ON (f.data_file_id) f.path::text, f.path_is_relative, "
        "COALESCE(t.path::text,''), COALESCE(t.path_is_relative,true), "
        "COALESCE(s.path::text,''), COALESCE(s.path_is_relative,true) "
        "FROM %s f JOIN %s t ON t.table_id=f.table_id JOIN %s s ON s.schema_id=t.schema_id "
        "ORDER BY f.data_file_id, t.begin_snapshot DESC, s.begin_snapshot DESC",
        dataRel, tblRel, schRel);
    sqls[1] = psprintf(
        "SELECT DISTINCT ON (d.delete_file_id) d.path::text, d.path_is_relative, "
        "COALESCE(t.path::text,''), COALESCE(t.path_is_relative,true), "
        "COALESCE(s.path::text,''), COALESCE(s.path_is_relative,true) "
        "FROM %s d JOIN %s t ON t.table_id=d.table_id JOIN %s s ON s.schema_id=t.schema_id "
        "ORDER BY d.delete_file_id, t.begin_snapshot DESC, s.begin_snapshot DESC",
        delRel, tblRel, schRel);

    for (int k = 0; k < 2; ++k) {
        LakeMetaRows* rows = LakeMetadataQuery(sqls[k], 0, NULL, NULL, NULL, snapshot, true, SPI_OK_SELECT, ns);
        for (uint32 i = 0; i < rows->count; ++i) {
            const char* fp = LakeMetadataText(rows, i, 1);
            bool fRel = DatumGetBool(LakeMetadataValue(rows, i, 2, BOOLOID));
            const char* tp = LakeMetadataText(rows, i, 3);
            bool tRel = DatumGetBool(LakeMetadataValue(rows, i, 4, BOOLOID));
            const char* sp = LakeMetadataText(rows, i, 5);
            bool sRel = DatumGetBool(LakeMetadataValue(rows, i, 6, BOOLOID));
            if (fRel && tRel && sRel) {
                StringInfoData rel;
                initStringInfo(&rel);
                LakeCleanupAppendPart(&rel, sp);
                LakeCleanupAppendPart(&rel, tp);
                LakeCleanupAppendPart(&rel, fp);
                LakeInsertPathKey(knownPaths, rel.data);
            } else if (!fRel && strncmp(fp, dataRoot, rootLen) == 0) {
                StringInfoData rel;
                initStringInfo(&rel);
                LakeCleanupAppendPart(&rel, fp + rootLen);
                LakeInsertPathKey(knownPaths, rel.data);
            }
        }
    }

    LakeMetaRows* sched = LakeMetadataQuery(psprintf(
        "SELECT path::text, path_is_relative FROM %s", schedRel),
        0, NULL, NULL, NULL, snapshot, true, SPI_OK_SELECT, ns);
    for (uint32 i = 0; i < sched->count; ++i) {
        const char* p = LakeMetadataText(sched, i, 1);
        bool isRel = DatumGetBool(LakeMetadataValue(sched, i, 2, BOOLOID));
        const char* relStr = NULL;
        if (isRel)
            relStr = p;
        else if (strncmp(p, dataRoot, rootLen) == 0)
            relStr = p + rootLen;
        if (relStr != NULL) {
            StringInfoData norm;
            initStringInfo(&norm);
            LakeCleanupAppendPart(&norm, relStr);
            LakeInsertPathKey(knownPaths, norm.data);
        }
    }

    for (LakeFileCandidate* cand = *LakeHostFileCandidatesAddress(); cand != NULL; cand = cand->next) {
        if (strncmp(cand->path, dataRoot, rootLen) == 0) {
            StringInfoData norm;
            initStringInfo(&norm);
            LakeCleanupAppendPart(&norm, cand->path + rootLen);
            LakeInsertPathKey(knownPaths, norm.data);
        }
    }
}

Datum og_lake_delete_orphaned_files(PG_FUNCTION_ARGS)
{
    LakeInlineRequireAdministrator();
    LakeRequireAccessPolicy();
    if (!LakePhysicalCleanupVersionEnabled())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("orphan file cleanup requires installed SQL0.69")));
    if (PG_ARGISNULL(0) || PG_ARGISNULL(3))
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
            errmsg("delete_orphaned_files catalog and dry_run cannot be NULL")));

    bool hasOlder = !PG_ARGISNULL(1);
    TimestampTz explicitOlder = hasOlder ? PG_GETARG_TIMESTAMPTZ(1) : 0;
    bool cleanupAll = PG_ARGISNULL(2) ? false : PG_GETARG_BOOL(2);
    bool dry = PG_GETARG_BOOL(3);

    ReturnSetInfo* result = (ReturnSetInfo*)fcinfo->resultinfo;
    if (result == NULL || !IsA(result, ReturnSetInfo) || !(result->allowedModes & SFRM_Materialize) || !result->econtext)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("delete_orphaned_files requires materialize mode")));

    ForeignServer* server = GetForeignServerByName(NameStr(*PG_GETARG_NAME(0)), false);
    MemoryContext caller = MemoryContextSwitchTo(result->econtext->ecxt_per_query_memory);
    TupleDesc descriptor = CreateTemplateTupleDesc(5, false);
    TupleDescInitEntry(descriptor, 1, "path", TEXTOID, -1, DEFAULT_COLLATION_OID);
    TupleDescInitEntry(descriptor, 2, "file_size_bytes", INT8OID, -1, 0);
    TupleDescInitEntry(descriptor, 3, "modified_at", TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(descriptor, 4, "deleted", BOOLOID, -1, 0);
    TupleDescInitEntry(descriptor, 5, "status", TEXTOID, -1, DEFAULT_COLLATION_OID);
    descriptor = BlessTupleDesc(descriptor);
    Tuplestorestate* store = tuplestore_begin_heap(true, false,
        Max(64, Min(u_sess->attr.attr_memory.work_mem, 1024)));
    MemoryContext workCtx = AllocSetContextCreate(result->econtext->ecxt_per_query_memory,
        "LakeDeleteOrphanedFiles", ALLOCSET_DEFAULT_SIZES);
    MemoryContextSwitchTo(workCtx);

    if (!dry)
        LakeCheckWriteCleanupPreconditions(server, false);

    Snapshot volatile snapshot = NULL;
    PG_TRY();
    {
        Snapshot initial = ActiveSnapshotSet() ? GetActiveSnapshot() : GetTransactionSnapshot();
        LakeInlineBinding* binding = LakeLoadCatalogHeader(server->serverid, initial);
        Oid ns = binding->metadata_schema_oid;
        if (!dry && binding->read_only)
            ereport(ERROR, (errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
                errmsg("cannot physically cleanup a read-only lake catalog")));
        if (!dry)
            CommandCounterIncrement();
        snapshot = RegisterSnapshot(dry ? initial : GetLatestSnapshot());
        LakeInlineBinding* fresh = LakeLoadCatalogHeader(server->serverid, snapshot);
        if (fresh->metadata_schema_oid != ns || fresh->server_oid != binding->server_oid ||
            strcmp(fresh->data_root, binding->data_root) != 0 || (!dry && fresh->read_only))
            ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                errmsg("orphan cleanup catalog binding changed")));
        const bool pinFree = LakeCleanupPinFree(fresh);
        if (!dry && !pinFree)
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("cannot delete orphaned files for catalog \"%s\" while active snapshot pins exist",
                    server->servername)));

        bool hasCutoff = true;
        TimestampTz cutoff = LakeCleanupResolveCutoff(fresh, snapshot, hasOlder, explicitOlder, cleanupAll, &hasCutoff);
        HTAB* knownPaths = LakeCreatePathSet(workCtx, "LakeKnownPaths");
        LakeCollectKnownRelativePaths(ns, fresh->data_root, snapshot, knownPaths);

        List* orphans = NIL;
        int rootFd = LakeOpenAnchoredDirectory(fresh->data_root);
        if (rootFd >= 0) {
            int seenCount = 0;
            PG_TRY();
            {
                LakeWalkOrphanDir(rootFd, "", 0, knownPaths, hasCutoff, cutoff, &orphans, &seenCount);
                close(rootFd);
            }
            PG_CATCH();
            {
                close(rootFd);
                PG_RE_THROW();
            }
            PG_END_TRY();
        }

        const OgLakeParquetMaintenanceApi* maint = dry ? NULL : LakeFileMaintenanceProvider();
        OgLakeSdkLimits limits = dry ? OgLakeSdkLimits{} : LakeFileLimits();

        ListCell* lc;
        foreach(lc, orphans) {
            CHECK_FOR_INTERRUPTS();
            LakeOrphanItem* item = (LakeOrphanItem*)lfirst(lc);
            bool deleted = false;
            const char* statusText = dry ? (pinFree ? "orphan_candidate" : "blocked_snapshot_pin") : NULL;
            if (!dry) {
                uint32_t removed = 0;
                OgLakeSdkError err = {};
                int sdkRc = maint->delete_file(fresh->data_root, item->rel_path, &limits, &removed, &err);
                CHECK_FOR_INTERRUPTS();
                LakeFileCheckStatus(sdkRc, &err);
                deleted = (removed != 0);
                statusText = deleted ? "deleted" : "already_absent";
            }
            Datum outVals[5];
            bool outNulls[5] = {false, false, false, false, false};
            outVals[0] = CStringGetTextDatum(item->rel_path);
            outVals[1] = Int64GetDatum(item->file_bytes);
            outVals[2] = TimestampTzGetDatum(item->mtime);
            outVals[3] = BoolGetDatum(deleted);
            outVals[4] = CStringGetTextDatum(statusText);
            tuplestore_putvalues(store, descriptor, outVals, outNulls);
        }

        UnregisterSnapshot(snapshot);
        snapshot = NULL;
        tuplestore_donestoring(store);
        result->returnMode = SFRM_Materialize;
        result->setResult = store;
        result->setDesc = descriptor;
        MemoryContextSwitchTo(caller);
    }
    PG_CATCH();
    {
        if (snapshot)
            UnregisterSnapshot(snapshot);
        MemoryContextSwitchTo(caller);
        PG_RE_THROW();
    }
    PG_END_TRY();
    PG_RETURN_NULL();
}

static bool LakeRemoveSegmentDirFd(int segFd, int depth)
{
    if (depth > LakeOrphanScanMaxDepth)
        return false;
    int dupFd = dup(segFd);
    DIR* dir = dupFd >= 0 ? fdopendir(dupFd) : NULL;
    if (dir == NULL) {
        if (dupFd >= 0)
            close(dupFd);
        return false;
    }
    bool removedAny = false;
    struct dirent* ent;
    while ((ent = readdir(dir)) != NULL) {
        if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
            continue;
        struct stat st;
        if (fstatat(segFd, ent->d_name, &st, AT_SYMLINK_NOFOLLOW) != 0)
            continue;
        if (S_ISDIR(st.st_mode)) {
            int subFd = openat(segFd, ent->d_name, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
            if (subFd >= 0) {
                if (LakeRemoveSegmentDirFd(subFd, depth + 1))
                    removedAny = true;
                close(subFd);
                if (unlinkat(segFd, ent->d_name, AT_REMOVEDIR) == 0)
                    removedAny = true;
            }
        } else if ((S_ISREG(st.st_mode) || S_ISLNK(st.st_mode)) &&
                   unlinkat(segFd, ent->d_name, 0) == 0) {
            removedAny = true;
        }
    }
    closedir(dir);
    return removedAny;
}

static bool LakeRemoveLocalSegmentDir(const char* indexRoot, const char* artifactUri)
{
    if (indexRoot == NULL || *indexRoot != '/' || artifactUri == NULL || *artifactUri != '/')
        return false;
    Size rootLen = strlen(indexRoot);
    while (rootLen > 1 && indexRoot[rootLen - 1] == '/')
        --rootLen;
    if (strncmp(artifactUri, indexRoot, rootLen) != 0)
        return false;
    const char* rel = artifactUri + rootLen;
    while (*rel == '/')
        ++rel;
    if (*rel == '\0')
        return false; /* Never unlink index_root itself. */

    /* Confirm segment path resides under anchored indexRoot without symlink traversal. */
    char cleanRoot[MAXPGPATH];
    if (rootLen >= MAXPGPATH)
        return false;
    memcpy(cleanRoot, indexRoot, rootLen);
    cleanRoot[rootLen] = '\0';
    int rootFd = LakeOpenAnchoredDirectory(cleanRoot);
    if (rootFd < 0)
        return false;

    StringInfoData relNorm;
    initStringInfo(&relNorm);
    LakeNormalizeRelativePathChecked(rel, &relNorm);

    /* Descend to parent of target segment dir, then recursively unlink regular files / subdirs. */
    int parentFd = rootFd;
    char* lastSlash = strrchr(relNorm.data, '/');
    char* leafName = relNorm.data;
    if (lastSlash != NULL) {
        *lastSlash = '\0';
        leafName = lastSlash + 1;
        const char* p = relNorm.data;
        while (*p) {
            const char* sep = strchr(p, '/');
            Size len = sep ? (Size)(sep - p) : strlen(p);
            if (len > 0) {
                char comp[NAME_MAX + 1];
                memcpy(comp, p, len);
                comp[len] = '\0';
                int nextFd = openat(parentFd, comp, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
                close(parentFd);
                if (nextFd < 0)
                    return false;
                parentFd = nextFd;
            }
            p = sep ? sep + 1 : p + len;
        }
    }

    int segFd = openat(parentFd, leafName, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    if (segFd < 0) {
        bool alreadyGone = (errno == ENOENT);
        close(parentFd);
        return !alreadyGone;
    }

    bool removedAny = LakeRemoveSegmentDirFd(segFd, 0);
    close(segFd);
    if (unlinkat(parentFd, leafName, AT_REMOVEDIR) == 0)
        removedAny = true;
    close(parentFd);
    return removedAny;
}

Datum og_lake_cleanup_index_segments(PG_FUNCTION_ARGS)
{
    LakeInlineRequireAdministrator();
    LakeRequireAccessPolicy();
    if (!LakePhysicalCleanupVersionEnabled())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("index segment cleanup requires installed SQL0.69")));
    if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
            errmsg("cleanup_index_segments catalog and dry_run cannot be NULL")));
    bool dry = PG_GETARG_BOOL(1);

    ReturnSetInfo* result = (ReturnSetInfo*)fcinfo->resultinfo;
    if (result == NULL || !IsA(result, ReturnSetInfo) || !(result->allowedModes & SFRM_Materialize) || !result->econtext)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("cleanup_index_segments requires materialize mode")));

    ForeignServer* server = GetForeignServerByName(NameStr(*PG_GETARG_NAME(0)), false);
    MemoryContext caller = MemoryContextSwitchTo(result->econtext->ecxt_per_query_memory);
    TupleDesc descriptor = CreateTemplateTupleDesc(5, false);
    TupleDescInitEntry(descriptor, 1, "segment_uuid", UUIDOID, -1, 0);
    TupleDescInitEntry(descriptor, 2, "artifact_root_uri", TEXTOID, -1, DEFAULT_COLLATION_OID);
    TupleDescInitEntry(descriptor, 3, "state", TEXTOID, -1, DEFAULT_COLLATION_OID);
    TupleDescInitEntry(descriptor, 4, "deleted", BOOLOID, -1, 0);
    TupleDescInitEntry(descriptor, 5, "status", TEXTOID, -1, DEFAULT_COLLATION_OID);
    descriptor = BlessTupleDesc(descriptor);
    Tuplestorestate* store = tuplestore_begin_heap(true, false,
        Max(64, Min(u_sess->attr.attr_memory.work_mem, 1024)));
    MemoryContext workCtx = AllocSetContextCreate(result->econtext->ecxt_per_query_memory,
        "LakeCleanupIndexSegments", ALLOCSET_DEFAULT_SIZES);
    MemoryContextSwitchTo(workCtx);

    if (!dry)
        LakeCheckWriteCleanupPreconditions(server, false);

    Snapshot volatile snapshot = NULL;
    PG_TRY();
    {
        Snapshot initial = ActiveSnapshotSet() ? GetActiveSnapshot() : GetTransactionSnapshot();
        LakeInlineBinding* binding = LakeLoadCatalogHeader(server->serverid, initial);
        if (!dry && binding->read_only)
            ereport(ERROR, (errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
                errmsg("cannot cleanup index segments for a read-only lake catalog")));
        if (!dry)
            CommandCounterIncrement();
        snapshot = RegisterSnapshot(dry ? initial : GetLatestSnapshot());
        LakeInlineBinding* fresh = LakeLoadCatalogHeader(server->serverid, snapshot);
        const bool pinFree = LakeCleanupPinFree(fresh);
        if (!dry && !pinFree)
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("cannot cleanup index segments for catalog \"%s\" while active snapshot pins exist",
                    server->servername)));

        Oid intNs = get_namespace_oid("og_lake_internal", false);
        Oid segRelOid = get_relname_relid("index_segment", intNs);
        Relation segRel = heap_open(segRelOid, dry ? AccessShareLock : ShareRowExclusiveLock);
        TransactionId oldestXmin = GetOldestXmin(segRel, false, true);

        /* Identify segment_uuids belonging to this catalog that are RETIRED or FAILED
         * and whose artifact_root_uri is not shared by any active/building segment. */
        Oid srvType = OIDOID;
        Datum srvArg = ObjectIdGetDatum(fresh->server_oid);
        LakeMetaRows* rootRows = LakeMetadataQuery(
            "SELECT index_root::text FROM og_lake_internal.catalog_binding WHERE server_oid=$1",
            1, &srvType, &srvArg, NULL, snapshot, true, SPI_OK_SELECT);
        char* indexRoot = NULL;
        if (rootRows->count == 1) {
            bool rootNull = false;
            Datum rDat = LakeMetadataValue(rootRows, 0, 1, TEXTOID, true, &rootNull);
            if (!rootNull)
                indexRoot = TextDatumGetCString(rDat);
        }

        LakeMetaRows* candMeta = LakeMetadataQuery(
            "SELECT s.segment_uuid::text, s.artifact_root_uri::text, s.state::text "
            "FROM og_lake_internal.index_segment s "
            "JOIN og_lake_internal.index_definition d USING(index_uuid) "
            "WHERE d.server_oid=$1 AND s.state IN ('RETIRED','FAILED') "
            "AND NOT EXISTS (SELECT 1 FROM og_lake_internal.index_segment a "
            "  WHERE a.artifact_root_uri=s.artifact_root_uri AND a.state IN ('READY','BUILDING')) "
            "ORDER BY s.build_snapshot, s.segment_uuid LIMIT 512",
            1, &srvType, &srvArg, NULL, snapshot, true, SPI_OK_SELECT);

        HTAB* targetSet = LakeCreatePathSet(workCtx, "LakeTargetSegments");
        for (uint32 i = 0; i < candMeta->count; ++i)
            LakeInsertPathKey(targetSet, LakeMetadataText(candMeta, i, 1));

        List* items = NIL;
        TupleDesc segDesc = RelationGetDescr(segRel);
        TableScanDesc scan = tableam_scan_begin(segRel, snapshot, 0, NULL);
        PG_TRY();
        {
            HeapTuple tup;
            while ((tup = (HeapTuple)tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
                CHECK_FOR_INTERRUPTS();
                bool isNull[11] = {false};
                Datum uuidD = tableam_tops_tuple_getattr(tup, 1, segDesc, &isNull[0]);
                Datum uriD = tableam_tops_tuple_getattr(tup, 9, segDesc, &isNull[8]);
                Datum stateD = tableam_tops_tuple_getattr(tup, 11, segDesc, &isNull[10]);
                if (isNull[0] || isNull[8] || isNull[10])
                    continue;
                char* uuidStr = DatumGetCString(DirectFunctionCall1(uuid_out, uuidD));
                if (!LakeHasPathKey(targetSet, uuidStr))
                    continue;
                LakeSegmentCleanupItem* item = (LakeSegmentCleanupItem*)palloc0(sizeof(LakeSegmentCleanupItem));
                item->segment_uuid_datum = DirectFunctionCall1(uuid_in, CStringGetDatum(uuidStr));
                item->segment_uuid_text = uuidStr;
                item->artifact_uri = TextDatumGetCString(uriD);
                item->state = TextDatumGetCString(stateD);
                item->xmin_frozen = HeapTupleHeaderXminFrozen(tup->t_data);
                item->retired_xmin = item->xmin_frozen ? FrozenTransactionId : HeapTupleGetRawXmin(tup);
                items = lappend(items, item);
            }
            tableam_scan_end(scan);
        }
        PG_CATCH();
        {
            tableam_scan_end(scan);
            heap_close(segRel, NoLock);
            PG_RE_THROW();
        }
        PG_END_TRY();
        heap_close(segRel, NoLock);

        ListCell* lc;
        foreach(lc, items) {
            CHECK_FOR_INTERRUPTS();
            LakeSegmentCleanupItem* item = (LakeSegmentCleanupItem*)lfirst(lc);
            bool deleted = false;
            const char* statusText = NULL;
            const bool xminSafe = item->xmin_frozen ||
                (TransactionIdIsValid(item->retired_xmin) && TransactionIdIsValid(oldestXmin) &&
                 TransactionIdPrecedes(item->retired_xmin, oldestXmin));

            if (indexRoot == NULL) {
                statusText = "missing_index_root";
            } else if (!pinFree) {
                statusText = "blocked_snapshot_pin";
            } else if (!xminSafe) {
                statusText = "deferred_oldest_xmin";
            } else if (dry) {
                statusText = "eligible";
            } else {
                bool unlinked = LakeRemoveLocalSegmentDir(indexRoot, item->artifact_uri);
                Oid st = UUIDOID;
                Datum sa = item->segment_uuid_datum;
                (void)LakeMetadataQuery(
                    "DELETE FROM og_lake_internal.index_coverage WHERE segment_uuid=$1",
                    1, &st, &sa, NULL, snapshot, false, SPI_OK_DELETE);
                (void)LakeMetadataQuery(
                    "DELETE FROM og_lake_internal.index_segment WHERE segment_uuid=$1",
                    1, &st, &sa, NULL, snapshot, false, SPI_OK_DELETE);
                deleted = true;
                statusText = unlinked ? "deleted" : "metadata_cleaned_dir_absent";
            }

            Datum outVals[5];
            bool outNulls[5] = {false, false, false, false, false};
            outVals[0] = item->segment_uuid_datum;
            outVals[1] = CStringGetTextDatum(item->artifact_uri);
            outVals[2] = CStringGetTextDatum(item->state);
            outVals[3] = BoolGetDatum(deleted);
            outVals[4] = CStringGetTextDatum(statusText);
            tuplestore_putvalues(store, descriptor, outVals, outNulls);
        }

        UnregisterSnapshot(snapshot);
        snapshot = NULL;
        tuplestore_donestoring(store);
        result->returnMode = SFRM_Materialize;
        result->setResult = store;
        result->setDesc = descriptor;
        MemoryContextSwitchTo(caller);
    }
    PG_CATCH();
    {
        if (snapshot)
            UnregisterSnapshot(snapshot);
        MemoryContextSwitchTo(caller);
        PG_RE_THROW();
    }
    PG_END_TRY();
    PG_RETURN_NULL();
}
