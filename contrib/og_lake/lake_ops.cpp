/* Operational backup manifest, catalog/index integrity verification & repair,
 * and retention observability functions (SQL0.70 / OPS1).
 * Reuses host snapshots/locks, anchored openat(O_NOFOLLOW) traversal, and
 * SDK footer validation without bypassing security boundaries or fabricating
 * lost business files.
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "lake_version.h"
#include "lake_inline_private.h"
#include "lake_file_sdk.h"
#include "lake_txn_private.h"
#include "access/heapam.h"
#include "access/og_lake.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "distributelayer/streamCore.h"
#include "executor/spi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "pgxc/pgxc.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/tuplestore.h"
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

extern "C" Datum og_lake_backup_manifest(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_verify_catalog_integrity(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_retention_status(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(og_lake_backup_manifest);
PG_FUNCTION_INFO_V1(og_lake_verify_catalog_integrity);
PG_FUNCTION_INFO_V1(og_lake_retention_status);

static void LakeOpsRequireEnv(const char* opName)
{
    LakeInlineRequireAdministrator();
    LakeRequireAccessPolicy();
    if (StreamThreadAmI() || IS_SPQ_RUNNING)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("%s requires the invoking serial backend", opName)));
    if (!ActiveSnapshotSet())
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("%s requires an active statement snapshot", opName)));
    if (!LakeAlterTypeOpsVersionEnabled())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("%s requires installed SQL0.70", opName)));
}

static bool LakeOpsAnchoredStatFile(const char* absRoot, const char* relPath, int64* sizeOut, const char** reasonOut)
{
    if (sizeOut) *sizeOut = -1;
    if (reasonOut) *reasonOut = NULL;
    if (absRoot == NULL || absRoot[0] != '/' || relPath == NULL || *relPath == '\0' ||
        *relPath == '/' || strlen(relPath) >= MAXPGPATH) {
        if (reasonOut) *reasonOut = "invalid relative path or root";
        return false;
    }
    int rootFd = LakeOpenAnchoredDirectory(absRoot);
    if (rootFd < 0) {
        if (reasonOut) *reasonOut = "catalog data_root directory does not exist";
        return false;
    }
    int curFd = rootFd;
    const char* p = relPath;
    while (*p) {
        const char* slash = strchr(p, '/');
        Size len = slash ? (Size)(slash - p) : strlen(p);
        if (len == 0) {
            p = slash + 1;
            continue;
        }
        if ((len == 1 && p[0] == '.') || (len == 2 && p[0] == '.' && p[1] == '.') || len >= NAME_MAX) {
            close(curFd);
            if (reasonOut) *reasonOut = "invalid relative component";
            return false;
        }
        char comp[NAME_MAX + 1];
        memcpy(comp, p, len);
        comp[len] = '\0';
        if (slash != NULL && *(slash + 1) != '\0') {
            int nextFd;
            do {
                nextFd = openat(curFd, comp, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
            } while (nextFd < 0 && errno == EINTR);
            close(curFd);
            if (nextFd < 0) {
                if (reasonOut) *reasonOut = (errno == ELOOP) ? "symlink directory rejected" :
                    (errno == ENOENT) ? "parent directory missing" : "cannot descend parent directory";
                return false;
            }
            curFd = nextFd;
            p = slash + 1;
        } else {
            struct stat st;
            int rc = fstatat(curFd, comp, &st, AT_SYMLINK_NOFOLLOW);
            close(curFd);
            if (rc != 0) {
                if (reasonOut) *reasonOut = (errno == ENOENT) ? "file missing on disk" : "stat failed";
                return false;
            }
            if (S_ISLNK(st.st_mode)) {
                if (reasonOut) *reasonOut = "symlink file rejected";
                return false;
            }
            if (!S_ISREG(st.st_mode)) {
                if (reasonOut) *reasonOut = "target is not a regular file";
                return false;
            }
            if (sizeOut) *sizeOut = (int64)st.st_size;
            return true;
        }
    }
    close(curFd);
    if (reasonOut) *reasonOut = "empty relative path";
    return false;
}

static bool LakeOpsCheckIndexDir(const char* indexRoot, const char* artifactUri, const char** reasonOut)
{
    if (reasonOut) *reasonOut = NULL;
    if (indexRoot == NULL || artifactUri == NULL || *indexRoot == '\0' || *artifactUri == '\0') {
        if (reasonOut) *reasonOut = "index_root or artifact_root_uri missing";
        return false;
    }
    Size rootLen = strlen(indexRoot);
    while (rootLen > 1 && indexRoot[rootLen - 1] == '/')
        --rootLen;
    if (strncmp(artifactUri, indexRoot, rootLen) != 0 ||
        (artifactUri[rootLen] != '/' && artifactUri[rootLen] != '\0')) {
        if (reasonOut) *reasonOut = "segment artifact directory escapes catalog index_root";
        return false;
    }
    int dirFd = LakeOpenAnchoredDirectory(artifactUri);
    if (dirFd < 0) {
        if (reasonOut) *reasonOut = "segment directory missing on disk";
        return false;
    }
    int dupFd = dup(dirFd);
    DIR* dir = dupFd >= 0 ? fdopendir(dupFd) : NULL;
    if (dir == NULL) {
        if (dupFd >= 0) close(dupFd);
        close(dirFd);
        if (reasonOut) *reasonOut = "cannot enumerate segment directory";
        return false;
    }
    bool hasRegular = false;
    struct dirent* ent;
    while ((ent = readdir(dir)) != NULL) {
        if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
            continue;
        struct stat st;
        if (fstatat(dirFd, ent->d_name, &st, AT_SYMLINK_NOFOLLOW) == 0 && S_ISREG(st.st_mode)) {
            hasRegular = true;
            break;
        }
    }
    closedir(dir);
    close(dirFd);
    if (!hasRegular) {
        if (reasonOut) *reasonOut = "segment directory contains no artifact files";
        return false;
    }
    return true;
}

static char* LakeOpsResolveRelativePath(const char* filePath, bool fileRel,
    const char* tablePath, bool tblRel, const char* schemaPath, bool schRel)
{
    StringInfoData rel;
    initStringInfo(&rel);
    if (!fileRel) {
        LakeCleanupAppendPart(&rel, filePath);
        return rel.data;
    }
    if (schRel && schemaPath)
        LakeCleanupAppendPart(&rel, schemaPath);
    if (tblRel && tablePath)
        LakeCleanupAppendPart(&rel, tablePath);
    LakeCleanupAppendPart(&rel, filePath);
    return rel.data;
}

Datum og_lake_backup_manifest(PG_FUNCTION_ARGS)
{
    LakeOpsRequireEnv("backup_manifest");
    if (PG_ARGISNULL(0))
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("backup_manifest catalog_name cannot be NULL")));

    ForeignServer* server = GetForeignServerByName(NameStr(*PG_GETARG_NAME(0)), false);
    MemoryContext caller = CurrentMemoryContext;
    MemoryContext workCtx = AllocSetContextCreate(caller, "LakeBackupManifest", ALLOCSET_DEFAULT_SIZES);
    Snapshot volatile snapshot = NULL;
    Datum jsonbOut = (Datum)0;

    PG_TRY();
    {
        MemoryContextSwitchTo(workCtx);
        snapshot = RegisterSnapshot(GetActiveSnapshot());
        LakeInlineBinding* binding = LakeLoadCatalogHeader(server->serverid, snapshot);
        Oid ns = binding->metadata_schema_oid;

        LakeCommittedCatalogKey ckey;
        int64 lastCommitted = -1;
        bool hasLastCommitted = LakeCommittedKeyFromBinding(binding, &ckey) &&
            LakeHostLookupCommittedSnapshot(&ckey, &lastCommitted);

        int activePins = 0;
        int64 minPin = -1;
        LakeSnapshotPinStatsForCatalog(u_sess->proc_cxt.MyDatabaseId, server->serverid, &activePins, &minPin);

        Oid srvType = OIDOID;
        Datum srvArg = ObjectIdGetDatum(server->serverid);
        LakeMetaRows* tables = LakeMetadataQuery(
            "SELECT t.table_id, t.table_uuid::text, t.schema_version, t.read_only, r.relation_oid "
            "FROM og_lake_internal.table_binding t LEFT JOIN og_lake_internal.relation_binding r USING(binding_id) "
            "WHERE t.server_oid=$1 AND t.state='active' ORDER BY t.table_id, r.relation_oid",
            1, &srvType, &srvArg, NULL, snapshot, true, SPI_OK_SELECT);

        LakeMetaRows* dataFiles = LakeMetadataQuery(psprintf(
            "SELECT DISTINCT ON (d.data_file_id) d.data_file_id, d.table_id, d.path::text, d.path_is_relative, "
            "d.record_count, d.file_size_bytes, COALESCE(t.path::text,''), COALESCE(t.path_is_relative,true), "
            "COALESCE(s.path::text,''), COALESCE(s.path_is_relative,true) "
            "FROM %s d LEFT JOIN %s t ON t.table_id=d.table_id LEFT JOIN %s s ON s.schema_id=t.schema_id "
            "WHERE d.end_snapshot IS NULL ORDER BY d.data_file_id, t.begin_snapshot DESC, s.begin_snapshot DESC",
            LakeMetadataRelation(ns, "ducklake_data_file"),
            LakeMetadataRelation(ns, "ducklake_table"),
            LakeMetadataRelation(ns, "ducklake_schema")),
            0, NULL, NULL, NULL, snapshot, true, SPI_OK_SELECT, ns);

        LakeMetaRows* delFiles = LakeMetadataQuery(psprintf(
            "SELECT DISTINCT ON (d.delete_file_id) d.delete_file_id, d.table_id, d.path::text, d.path_is_relative, "
            "d.delete_count, d.file_size_bytes, COALESCE(t.path::text,''), COALESCE(t.path_is_relative,true), "
            "COALESCE(s.path::text,''), COALESCE(s.path_is_relative,true) "
            "FROM %s d LEFT JOIN %s t ON t.table_id=d.table_id LEFT JOIN %s s ON s.schema_id=t.schema_id "
            "WHERE d.end_snapshot IS NULL ORDER BY d.delete_file_id, t.begin_snapshot DESC, s.begin_snapshot DESC",
            LakeMetadataRelation(ns, "ducklake_delete_file"),
            LakeMetadataRelation(ns, "ducklake_table"),
            LakeMetadataRelation(ns, "ducklake_schema")),
            0, NULL, NULL, NULL, snapshot, true, SPI_OK_SELECT, ns);

        LakeMetaRows* sched = LakeMetadataQuery(psprintf(
            "SELECT count(*) FROM %s",
            LakeMetadataRelation(ns, "ducklake_files_scheduled_for_deletion")),
            0, NULL, NULL, NULL, snapshot, true, SPI_OK_SELECT, ns);
        int64 schedCount = sched->count == 1 ? DatumGetInt64(LakeMetadataValue(sched, 0, 1, INT8OID)) : 0;

        LakeMetaRows* inlines = LakeMetadataQuery(psprintf(
            "SELECT table_id, schema_version, table_name::text FROM %s ORDER BY table_id, schema_version",
            LakeMetadataRelation(ns, "ducklake_inlined_data_tables")),
            0, NULL, NULL, NULL, snapshot, true, SPI_OK_SELECT, ns);

        LakeMetaRows* segs = LakeMetadataQuery(
            "SELECT s.segment_uuid::text, s.index_uuid::text, COALESCE(s.manifest_hash,''), s.artifact_root_uri::text "
            "FROM og_lake_internal.index_segment s JOIN og_lake_internal.index_definition d USING(index_uuid) "
            "WHERE d.server_oid=$1 AND s.state='READY' ORDER BY s.build_snapshot, s.segment_uuid",
            1, &srvType, &srvArg, NULL, snapshot, true, SPI_OK_SELECT);

        int64 verifiedData = 0, missingData = 0, totalDataBytes = 0;
        int64 verifiedDel = 0, missingDel = 0, totalDelBytes = 0;

        StringInfoData js;
        initStringInfo(&js);
        appendStringInfo(&js, "{\"module_version\":\"" OG_LAKE_MODULE_VERSION "\",");
        appendStringInfoString(&js, "\"server_name\":");
        escape_json(&js, server->servername);
        appendStringInfo(&js, ",\"server_oid\":%u,\"metadata_schema\":", (unsigned)server->serverid);
        escape_json(&js, binding->metadata_schema);
        appendStringInfoString(&js, ",\"data_root\":");
        escape_json(&js, binding->data_root);
        appendStringInfo(&js, ",\"read_only\":%s,\"head_snapshot_id\":" INT64_FORMAT
            ",\"schema_version\":" INT64_FORMAT ",\"binding_generation\":" INT64_FORMAT,
            binding->read_only ? "true" : "false", binding->snapshot_id, binding->schema_version,
            binding->binding_generation);
        if (hasLastCommitted)
            appendStringInfo(&js, ",\"last_committed_snapshot\":" INT64_FORMAT, lastCommitted);
        else
            appendStringInfoString(&js, ",\"last_committed_snapshot\":null");
        appendStringInfo(&js, ",\"active_snapshot_pins\":%d", activePins);
        if (minPin >= 0)
            appendStringInfo(&js, ",\"min_pinned_snapshot\":" INT64_FORMAT, minPin);
        else
            appendStringInfoString(&js, ",\"min_pinned_snapshot\":null");

        appendStringInfoString(&js, ",\"tables\":[");
        for (uint32 i = 0; i < tables->count; ++i) {
            int64 tid = DatumGetInt64(LakeMetadataValue(tables, i, 1, INT8OID));
            const char* tuuid = LakeMetadataText(tables, i, 2);
            int64 sver = DatumGetInt64(LakeMetadataValue(tables, i, 3, INT8OID));
            bool tro = DatumGetBool(LakeMetadataValue(tables, i, 4, BOOLOID));
            bool oidNull = false;
            Datum oidDat = LakeMetadataValue(tables, i, 5, OIDOID, true, &oidNull);
            if (i > 0) appendStringInfoChar(&js, ',');
            appendStringInfo(&js, "{\"table_id\":" INT64_FORMAT ",\"table_uuid\":", tid);
            escape_json(&js, tuuid);
            appendStringInfo(&js, ",\"schema_version\":" INT64_FORMAT ",\"read_only\":%s",
                sver, tro ? "true" : "false");
            if (!oidNull) {
                Oid relOid = DatumGetObjectId(oidDat);
                char* relName = get_rel_name(relOid);
                appendStringInfo(&js, ",\"relation_oid\":%u,\"relation_name\":", (unsigned)relOid);
                escape_json(&js, relName ? relName : "");
            }
            appendStringInfoChar(&js, '}');
        }

        appendStringInfoString(&js, "],\"data_files\":[");
        for (uint32 i = 0; i < dataFiles->count; ++i) {
            int64 fid = DatumGetInt64(LakeMetadataValue(dataFiles, i, 1, INT8OID));
            int64 tid = DatumGetInt64(LakeMetadataValue(dataFiles, i, 2, INT8OID));
            const char* fpath = LakeMetadataText(dataFiles, i, 3);
            bool frel = DatumGetBool(LakeMetadataValue(dataFiles, i, 4, BOOLOID));
            int64 rcnt = DatumGetInt64(LakeMetadataValue(dataFiles, i, 5, INT8OID));
            int64 fbytes = DatumGetInt64(LakeMetadataValue(dataFiles, i, 6, INT8OID));
            const char* tpath = LakeMetadataText(dataFiles, i, 7);
            bool trel = DatumGetBool(LakeMetadataValue(dataFiles, i, 8, BOOLOID));
            const char* spath = LakeMetadataText(dataFiles, i, 9);
            bool srel = DatumGetBool(LakeMetadataValue(dataFiles, i, 10, BOOLOID));
            char* rel = LakeOpsResolveRelativePath(fpath, frel, tpath, trel, spath, srel);
            int64 diskBytes = -1;
            const char* failReason = NULL;
            bool ok = LakeOpsAnchoredStatFile(binding->data_root, rel, &diskBytes, &failReason) && diskBytes == fbytes;
            if (ok) ++verifiedData; else ++missingData;
            totalDataBytes += fbytes;
            if (i > 0) appendStringInfoChar(&js, ',');
            appendStringInfo(&js, "{\"data_file_id\":" INT64_FORMAT ",\"table_id\":" INT64_FORMAT
                ",\"relative_path\":", fid, tid);
            escape_json(&js, rel);
            appendStringInfo(&js, ",\"record_count\":" INT64_FORMAT ",\"file_size_bytes\":" INT64_FORMAT
                ",\"disk_verified\":%s}", rcnt, fbytes, ok ? "true" : "false");
        }

        appendStringInfoString(&js, "],\"delete_files\":[");
        for (uint32 i = 0; i < delFiles->count; ++i) {
            int64 did = DatumGetInt64(LakeMetadataValue(delFiles, i, 1, INT8OID));
            int64 tid = DatumGetInt64(LakeMetadataValue(delFiles, i, 2, INT8OID));
            const char* fpath = LakeMetadataText(delFiles, i, 3);
            bool frel = DatumGetBool(LakeMetadataValue(delFiles, i, 4, BOOLOID));
            int64 dcnt = DatumGetInt64(LakeMetadataValue(delFiles, i, 5, INT8OID));
            int64 fbytes = DatumGetInt64(LakeMetadataValue(delFiles, i, 6, INT8OID));
            const char* tpath = LakeMetadataText(delFiles, i, 7);
            bool trel = DatumGetBool(LakeMetadataValue(delFiles, i, 8, BOOLOID));
            const char* spath = LakeMetadataText(delFiles, i, 9);
            bool srel = DatumGetBool(LakeMetadataValue(delFiles, i, 10, BOOLOID));
            char* rel = LakeOpsResolveRelativePath(fpath, frel, tpath, trel, spath, srel);
            int64 diskBytes = -1;
            const char* failReason = NULL;
            bool ok = LakeOpsAnchoredStatFile(binding->data_root, rel, &diskBytes, &failReason) && diskBytes == fbytes;
            if (ok) ++verifiedDel; else ++missingDel;
            totalDelBytes += fbytes;
            if (i > 0) appendStringInfoChar(&js, ',');
            appendStringInfo(&js, "{\"delete_file_id\":" INT64_FORMAT ",\"table_id\":" INT64_FORMAT
                ",\"relative_path\":", did, tid);
            escape_json(&js, rel);
            appendStringInfo(&js, ",\"delete_count\":" INT64_FORMAT ",\"file_size_bytes\":" INT64_FORMAT
                ",\"disk_verified\":%s}", dcnt, fbytes, ok ? "true" : "false");
        }

        appendStringInfo(&js, "],\"inline_table_count\":%u,\"scheduled_deletion_files\":" INT64_FORMAT
            ",\"index_segments\":[", inlines->count, schedCount);
        for (uint32 i = 0; i < segs->count; ++i) {
            if (i > 0) appendStringInfoChar(&js, ',');
            appendStringInfoString(&js, "{\"segment_uuid\":");
            escape_json(&js, LakeMetadataText(segs, i, 1));
            appendStringInfoString(&js, ",\"index_uuid\":");
            escape_json(&js, LakeMetadataText(segs, i, 2));
            appendStringInfoString(&js, ",\"manifest_hash\":");
            escape_json(&js, LakeMetadataText(segs, i, 3));
            appendStringInfoString(&js, ",\"artifact_root_uri\":");
            escape_json(&js, LakeMetadataText(segs, i, 4));
            appendStringInfoChar(&js, '}');
        }
        appendStringInfo(&js, "],\"summary\":{\"active_data_files\":%u,\"verified_data_files\":" INT64_FORMAT
            ",\"missing_or_mismatched_data_files\":" INT64_FORMAT ",\"total_active_data_bytes\":" INT64_FORMAT
            ",\"active_delete_files\":%u,\"verified_delete_files\":" INT64_FORMAT
            ",\"missing_or_mismatched_delete_files\":" INT64_FORMAT ",\"total_active_delete_bytes\":" INT64_FORMAT
            ",\"ready_index_segments\":%u}}",
            dataFiles->count, verifiedData, missingData, totalDataBytes,
            delFiles->count, verifiedDel, missingDel, totalDelBytes, segs->count);

        UnregisterSnapshot(snapshot);
        snapshot = NULL;
        MemoryContextSwitchTo(caller);
        Datum parsed = DirectFunctionCall1(jsonb_in, CStringGetDatum(js.data));
        jsonbOut = PointerGetDatum(PG_DETOAST_DATUM_COPY(parsed));
        MemoryContextDelete(workCtx);
    }
    PG_CATCH();
    {
        if (snapshot)
            UnregisterSnapshot(snapshot);
        MemoryContextSwitchTo(caller);
        MemoryContextDelete(workCtx);
        PG_RE_THROW();
    }
    PG_END_TRY();
    PG_RETURN_DATUM(jsonbOut);
}

static void LakeOpsPutIntegrityRow(Tuplestorestate* store, TupleDesc desc,
    const char* kind, const char* id, const char* severity, const char* status, const char* detail)
{
    Datum vals[5];
    bool nulls[5] = {false, false, false, false, false};
    vals[0] = CStringGetTextDatum(kind);
    vals[1] = CStringGetTextDatum(id);
    vals[2] = CStringGetTextDatum(severity);
    vals[3] = CStringGetTextDatum(status);
    vals[4] = CStringGetTextDatum(detail);
    tuplestore_putvalues(store, desc, vals, nulls);
}

static bool LakeOpsCheckParquetFooter(LakeSdkHandles* handles, const char* absPath,
    int64 expectedRows, int64 expectedBytes, StringInfo errBuf)
{
    OgLakeSdkLimits limits = LakeFileLimits();
    OgLakeSdkError err = {};
    int rc = 0;
    HOLD_INTERRUPTS();
    rc = handles->api->footer_open(absPath, &limits, &handles->footer, &err);
    RESUME_INTERRUPTS();
    CHECK_FOR_INTERRUPTS();
    if (rc != OG_LAKE_SDK_OK || handles->footer == NULL) {
        appendStringInfo(errBuf, "footer_open failed (%d): %s", rc, err.message);
        if (handles->footer) {
            HOLD_INTERRUPTS();
            handles->api->footer_close(&handles->footer);
            RESUME_INTERRUPTS();
        }
        return false;
    }
    const OgLakeSdkFooterInfo* info = NULL;
    HOLD_INTERRUPTS();
    info = handles->api->footer_info(handles->footer);
    RESUME_INTERRUPTS();
    bool ok = info != NULL && info->struct_size >= offsetof(OgLakeSdkFooterInfo, footer_bytes) &&
        info->num_rows == expectedRows && info->file_bytes == expectedBytes;
    if (!ok) {
        appendStringInfo(errBuf, "footer metadata mismatch (expected rows=" INT64_FORMAT " bytes=" INT64_FORMAT
            ", actual rows=%lld bytes=%lld)",
            expectedRows, expectedBytes,
            info ? (long long)info->num_rows : -1LL,
            info ? (long long)info->file_bytes : -1LL);
    }
    HOLD_INTERRUPTS();
    handles->api->footer_close(&handles->footer);
    RESUME_INTERRUPTS();
    return ok;
}

Datum og_lake_verify_catalog_integrity(PG_FUNCTION_ARGS)
{
    LakeOpsRequireEnv("verify_catalog_integrity");
    if (PG_ARGISNULL(0))
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
            errmsg("verify_catalog_integrity catalog_name cannot be NULL")));
    bool repair = PG_ARGISNULL(1) ? false : PG_GETARG_BOOL(1);

    ReturnSetInfo* result = (ReturnSetInfo*)fcinfo->resultinfo;
    if (result == NULL || !IsA(result, ReturnSetInfo) || !(result->allowedModes & SFRM_Materialize) || !result->econtext)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("verify_catalog_integrity requires materialize mode")));

    ForeignServer* server = GetForeignServerByName(NameStr(*PG_GETARG_NAME(0)), false);
    MemoryContext caller = MemoryContextSwitchTo(result->econtext->ecxt_per_query_memory);
    TupleDesc descriptor = CreateTemplateTupleDesc(5, false);
    TupleDescInitEntry(descriptor, 1, "object_kind", TEXTOID, -1, DEFAULT_COLLATION_OID);
    TupleDescInitEntry(descriptor, 2, "object_id", TEXTOID, -1, DEFAULT_COLLATION_OID);
    TupleDescInitEntry(descriptor, 3, "severity", TEXTOID, -1, DEFAULT_COLLATION_OID);
    TupleDescInitEntry(descriptor, 4, "status", TEXTOID, -1, DEFAULT_COLLATION_OID);
    TupleDescInitEntry(descriptor, 5, "detail", TEXTOID, -1, DEFAULT_COLLATION_OID);
    descriptor = BlessTupleDesc(descriptor);
    Tuplestorestate* store = tuplestore_begin_heap(true, false,
        Max(64, Min(u_sess->attr.attr_memory.work_mem, 1024)));
    MemoryContext workCtx = AllocSetContextCreate(result->econtext->ecxt_per_query_memory,
        "LakeVerifyIntegrity", ALLOCSET_DEFAULT_SIZES);
    MemoryContextSwitchTo(workCtx);

    if (repair) {
        if (u_sess->utils_cxt.XactIsoLevel != XACT_READ_COMMITTED)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("verify_catalog_integrity repair requires READ COMMITTED")));
        if (u_sess->attr.attr_common.XactReadOnly)
            ereport(ERROR, (errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
                errmsg("cannot repair lake catalog in a read-only transaction")));
        LakeTxnRejectBindingDdl();
        if (LakeTxnHasSnapshotExpiry())
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("verify_catalog_integrity repair cannot share a transaction with expire_snapshots")));
        LakeLockCatalogRegistration();
        if (!LakeTryCatalogGuard(server->serverid, true))
            ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
                errmsg("cannot repair lake catalog \"%s\" while readers or writers hold catalog guard",
                    server->servername)));
    }

    Snapshot volatile snapshot = NULL;
    LakeSdkHandles* volatile handles = NULL;
    PG_TRY();
    {
        Snapshot initial = repair ? GetLatestSnapshot() : GetActiveSnapshot();
        if (repair) CommandCounterIncrement();
        snapshot = RegisterSnapshot(initial);
        LakeInlineBinding* binding = LakeLoadCatalogHeader(server->serverid, snapshot);
        if (repair && binding->read_only)
            ereport(ERROR, (errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
                errmsg("cannot repair a read-only lake catalog")));

        Oid ns = binding->metadata_schema_oid;
        handles = LakeFileHandles(workCtx);
        int64 errors = 0, warnings = 0, repaired = 0, checkedFiles = 0, checkedSegs = 0;

        /* 1. Verify active Parquet data_files & delete_files cleanly (never forge business files on repair). */
        for (int pass = 0; pass < 2; ++pass) {
            const char* kind = (pass == 0) ? "data_file" : "delete_file";
            const char* relSql = LakeMetadataRelation(ns, (pass == 0) ? "ducklake_data_file" : "ducklake_delete_file");
            const char* idCol = (pass == 0) ? "data_file_id" : "delete_file_id";
            const char* cntCol = (pass == 0) ? "record_count" : "delete_count";
            LakeMetaRows* rows = LakeMetadataQuery(psprintf(
                "SELECT DISTINCT ON (d.%s) d.%s, d.path::text, d.path_is_relative, d.%s, d.file_size_bytes, "
                "COALESCE(t.path::text,''), COALESCE(t.path_is_relative,true), "
                "COALESCE(s.path::text,''), COALESCE(s.path_is_relative,true) "
                "FROM %s d LEFT JOIN %s t ON t.table_id=d.table_id LEFT JOIN %s s ON s.schema_id=t.schema_id "
                "WHERE d.end_snapshot IS NULL ORDER BY d.%s, t.begin_snapshot DESC, s.begin_snapshot DESC",
                idCol, idCol, cntCol, relSql,
                LakeMetadataRelation(ns, "ducklake_table"),
                LakeMetadataRelation(ns, "ducklake_schema"), idCol),
                0, NULL, NULL, NULL, snapshot, true, SPI_OK_SELECT, ns);
            for (uint32 i = 0; i < rows->count; ++i) {
                CHECK_FOR_INTERRUPTS();
                ++checkedFiles;
                int64 fid = DatumGetInt64(LakeMetadataValue(rows, i, 1, INT8OID));
                const char* fpath = LakeMetadataText(rows, i, 2);
                bool frel = DatumGetBool(LakeMetadataValue(rows, i, 3, BOOLOID));
                int64 expectCnt = DatumGetInt64(LakeMetadataValue(rows, i, 4, INT8OID));
                int64 expectBytes = DatumGetInt64(LakeMetadataValue(rows, i, 5, INT8OID));
                const char* tpath = LakeMetadataText(rows, i, 6);
                bool trel = DatumGetBool(LakeMetadataValue(rows, i, 7, BOOLOID));
                const char* spath = LakeMetadataText(rows, i, 8);
                bool srel = DatumGetBool(LakeMetadataValue(rows, i, 9, BOOLOID));
                char* rel = LakeOpsResolveRelativePath(fpath, frel, tpath, trel, spath, srel);
                int64 diskBytes = -1;
                const char* statReason = NULL;
                if (!LakeOpsAnchoredStatFile(binding->data_root, rel, &diskBytes, &statReason)) {
                    ++errors;
                    LakeOpsPutIntegrityRow(store, descriptor, kind, psprintf(INT64_FORMAT, fid), "error",
                        repair ? "unrepairable_business_data" : "corrupted_or_missing",
                        psprintf("%s: %s", rel, statReason ? statReason : "stat error"));
                } else if (diskBytes != expectBytes) {
                    ++errors;
                    LakeOpsPutIntegrityRow(store, descriptor, kind, psprintf(INT64_FORMAT, fid), "error",
                        repair ? "unrepairable_business_data" : "size_mismatch",
                        psprintf("%s: expected " INT64_FORMAT " bytes, found " INT64_FORMAT, rel, expectBytes, diskBytes));
                } else {
                    const char* absPath = psprintf("%s/%s", binding->data_root, rel);
                    StringInfoData fErr;
                    initStringInfo(&fErr);
                    if (!LakeOpsCheckParquetFooter(handles, absPath, expectCnt, expectBytes, &fErr)) {
                        ++errors;
                        LakeOpsPutIntegrityRow(store, descriptor, kind, psprintf(INT64_FORMAT, fid), "error",
                            repair ? "unrepairable_business_data" : "corrupt_parquet_footer", fErr.data);
                    }
                }
            }
        }

        /* 2. Check registered inline carriers exist. */
        LakeMetaRows* inlineTbls = LakeMetadataQuery(psprintf(
            "SELECT table_id, schema_version, table_name::text FROM %s",
            LakeMetadataRelation(ns, "ducklake_inlined_data_tables")),
            0, NULL, NULL, NULL, snapshot, true, SPI_OK_SELECT, ns);
        for (uint32 i = 0; i < inlineTbls->count; ++i) {
            const char* tblName = LakeMetadataText(inlineTbls, i, 3);
            if (!OidIsValid(get_relname_relid(tblName, ns))) {
                ++errors;
                LakeOpsPutIntegrityRow(store, descriptor, "inline_carrier", tblName, "error",
                    repair ? "unrepairable_business_data" : "missing_inline_relation",
                    psprintf("registered inline carrier %s.%s missing", binding->metadata_schema, tblName));
            }
        }

        /* 3. Verify READY/BUILDING index segments; repair=true safely degrades damaged segments to FAILED. */
        Oid srvType = OIDOID;
        Datum srvArg = ObjectIdGetDatum(server->serverid);
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
        LakeMetaRows* segs = LakeMetadataQuery(
            "SELECT s.segment_uuid::text, s.artifact_root_uri::text, s.state::text "
            "FROM og_lake_internal.index_segment s JOIN og_lake_internal.index_definition d USING(index_uuid) "
            "WHERE d.server_oid=$1 AND s.state IN ('READY','BUILDING') ORDER BY s.build_snapshot, s.segment_uuid",
            1, &srvType, &srvArg, NULL, snapshot, true, SPI_OK_SELECT);

        Oid intNs = get_namespace_oid("og_lake_internal", false);
        Relation segLockRel = NULL, covLockRel = NULL;
        if (repair && segs->count > 0) {
            segLockRel = heap_open(get_relname_relid("index_segment", intNs), ShareRowExclusiveLock);
            covLockRel = heap_open(get_relname_relid("index_coverage", intNs), ShareRowExclusiveLock);
        }

        PG_TRY();
        {
            for (uint32 i = 0; i < segs->count; ++i) {
                CHECK_FOR_INTERRUPTS();
                ++checkedSegs;
                const char* segUuid = LakeMetadataText(segs, i, 1);
                const char* artUri = LakeMetadataText(segs, i, 2);
                const char* dirReason = NULL;
                if (!LakeOpsCheckIndexDir(indexRoot, artUri, &dirReason)) {
                    ++warnings;
                    if (repair) {
                        Oid ut = UUIDOID;
                        Datum ua = DirectFunctionCall1(uuid_in, CStringGetDatum(segUuid));
                        (void)LakeMetadataQuery(
                            "UPDATE og_lake_internal.index_coverage SET active=false WHERE segment_uuid=$1 AND active",
                            1, &ut, &ua, NULL, snapshot, false, SPI_OK_UPDATE);
                        (void)LakeMetadataQuery(
                            "UPDATE og_lake_internal.index_segment SET state='FAILED' WHERE segment_uuid=$1 AND state IN ('READY','BUILDING')",
                            1, &ut, &ua, NULL, snapshot, false, SPI_OK_UPDATE);
                        ++repaired;
                        LakeOpsPutIntegrityRow(store, descriptor, "index_segment", segUuid, "warning",
                            "repaired_marked_failed",
                            psprintf("%s (%s): marked FAILED cleanly for rebuild", artUri, dirReason));
                    } else {
                        LakeOpsPutIntegrityRow(store, descriptor, "index_segment", segUuid, "warning",
                            "corrupt_or_missing_index_segment",
                            psprintf("%s (%s)", artUri, dirReason));
                    }
                }
            }
            if (covLockRel) heap_close(covLockRel, NoLock);
            if (segLockRel) heap_close(segLockRel, NoLock);
        }
        PG_CATCH();
        {
            if (covLockRel) heap_close(covLockRel, NoLock);
            if (segLockRel) heap_close(segLockRel, NoLock);
            PG_RE_THROW();
        }
        PG_END_TRY();

        LakeOpsPutIntegrityRow(store, descriptor, "summary", server->servername,
            errors > 0 ? "error" : (warnings > 0 ? "warning" : "ok"),
            (errors == 0 && warnings == 0) ? "healthy" : "issues_found",
            psprintf("checked_files=" INT64_FORMAT " checked_segments=" INT64_FORMAT
                " errors=" INT64_FORMAT " warnings=" INT64_FORMAT " repaired=" INT64_FORMAT,
                checkedFiles, checkedSegs, errors, warnings, repaired));

        LakeFileHandlesClose(handles);
        handles = NULL;
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
        if (handles) LakeFileHandlesClose(handles);
        if (snapshot) UnregisterSnapshot(snapshot);
        MemoryContextSwitchTo(caller);
        PG_RE_THROW();
    }
    PG_END_TRY();
    PG_RETURN_NULL();
}

Datum og_lake_retention_status(PG_FUNCTION_ARGS)
{
    LakeOpsRequireEnv("retention_status");
    if (PG_ARGISNULL(0))
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
            errmsg("retention_status catalog_name cannot be NULL")));

    ReturnSetInfo* result = (ReturnSetInfo*)fcinfo->resultinfo;
    if (result == NULL || !IsA(result, ReturnSetInfo) || !(result->allowedModes & SFRM_Materialize) || !result->econtext)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("retention_status requires materialize mode")));

    ForeignServer* server = GetForeignServerByName(NameStr(*PG_GETARG_NAME(0)), false);
    MemoryContext caller = MemoryContextSwitchTo(result->econtext->ecxt_per_query_memory);
    TupleDesc descriptor = CreateTemplateTupleDesc(13, false);
    TupleDescInitEntry(descriptor, 1, "catalog_name", TEXTOID, -1, DEFAULT_COLLATION_OID);
    TupleDescInitEntry(descriptor, 2, "head_snapshot", INT8OID, -1, 0);
    TupleDescInitEntry(descriptor, 3, "total_snapshots", INT8OID, -1, 0);
    TupleDescInitEntry(descriptor, 4, "expired_scheduled_files", INT8OID, -1, 0);
    TupleDescInitEntry(descriptor, 5, "scheduled_bytes", INT8OID, -1, 0);
    TupleDescInitEntry(descriptor, 6, "active_pins", INT8OID, -1, 0);
    TupleDescInitEntry(descriptor, 7, "min_pinned_snapshot", INT8OID, -1, 0);
    TupleDescInitEntry(descriptor, 8, "delete_older_than", TEXTOID, -1, DEFAULT_COLLATION_OID);
    TupleDescInitEntry(descriptor, 9, "oldest_xmin", TEXTOID, -1, DEFAULT_COLLATION_OID);
    TupleDescInitEntry(descriptor, 10, "ready_index_segments", INT8OID, -1, 0);
    TupleDescInitEntry(descriptor, 11, "retired_index_segments", INT8OID, -1, 0);
    TupleDescInitEntry(descriptor, 12, "auto_compact", TEXTOID, -1, DEFAULT_COLLATION_OID);
    TupleDescInitEntry(descriptor, 13, "target_file_size", TEXTOID, -1, DEFAULT_COLLATION_OID);
    descriptor = BlessTupleDesc(descriptor);
    Tuplestorestate* store = tuplestore_begin_heap(true, false,
        Max(64, Min(u_sess->attr.attr_memory.work_mem, 1024)));
    MemoryContext workCtx = AllocSetContextCreate(result->econtext->ecxt_per_query_memory,
        "LakeRetentionStatus", ALLOCSET_DEFAULT_SIZES);
    MemoryContextSwitchTo(workCtx);

    Snapshot volatile snapshot = NULL;
    PG_TRY();
    {
        snapshot = RegisterSnapshot(GetActiveSnapshot());
        LakeInlineBinding* binding = LakeLoadCatalogHeader(server->serverid, snapshot);
        Oid ns = binding->metadata_schema_oid;

        LakeMetaRows* snapCnt = LakeMetadataQuery(psprintf(
            "SELECT count(*) FROM %s", LakeMetadataRelation(ns, "ducklake_snapshot")),
            0, NULL, NULL, NULL, snapshot, true, SPI_OK_SELECT, ns);
        int64 totalSnaps = snapCnt->count == 1 ? DatumGetInt64(LakeMetadataValue(snapCnt, 0, 1, INT8OID)) : 0;

        LakeMetaRows* schedStats = LakeMetadataQuery(psprintf(
            "SELECT count(DISTINCT s.data_file_id), COALESCE(sum(COALESCE(df.file_size_bytes, dl.file_size_bytes, 0))::bigint, 0) "
            "FROM %s s LEFT JOIN %s df ON df.data_file_id=s.data_file_id "
            "LEFT JOIN %s dl ON dl.delete_file_id=s.data_file_id",
            LakeMetadataRelation(ns, "ducklake_files_scheduled_for_deletion"),
            LakeMetadataRelation(ns, "ducklake_data_file"),
            LakeMetadataRelation(ns, "ducklake_delete_file")),
            0, NULL, NULL, NULL, snapshot, true, SPI_OK_SELECT, ns);
        int64 schedFiles = schedStats->count == 1 ? DatumGetInt64(LakeMetadataValue(schedStats, 0, 1, INT8OID)) : 0;
        int64 schedBytes = schedStats->count == 1 ? DatumGetInt64(LakeMetadataValue(schedStats, 0, 2, INT8OID)) : 0;

        int pinCount = 0;
        int64 minPin = -1;
        LakeSnapshotPinStatsForCatalog(u_sess->proc_cxt.MyDatabaseId, server->serverid, &pinCount, &minPin);

        const char* deleteOlderThan = "2 days (default)";
        const char* autoCompact = "default";
        const char* targetFileSize = "default";
        LakeMetaRows* opts = LakeMetadataQuery(psprintf(
            "SELECT key::text, value::text FROM %s WHERE scope IS NULL "
            "AND key IN ('delete_older_than','auto_compact','target_file_size')",
            LakeMetadataRelation(ns, "ducklake_metadata")),
            0, NULL, NULL, NULL, snapshot, true, SPI_OK_SELECT, ns);
        for (uint32 i = 0; i < opts->count; ++i) {
            const char* k = LakeMetadataText(opts, i, 1);
            const char* v = LakeMetadataText(opts, i, 2);
            if (strcmp(k, "delete_older_than") == 0) deleteOlderThan = v;
            else if (strcmp(k, "auto_compact") == 0) autoCompact = v;
            else if (strcmp(k, "target_file_size") == 0) targetFileSize = v;
        }

        Oid srvType = OIDOID;
        Datum srvArg = ObjectIdGetDatum(server->serverid);
        LakeMetaRows* segStats = LakeMetadataQuery(
            "SELECT count(*) FILTER (WHERE s.state='READY')::bigint, "
            "count(*) FILTER (WHERE s.state IN ('RETIRED','FAILED'))::bigint "
            "FROM og_lake_internal.index_segment s JOIN og_lake_internal.index_definition d USING(index_uuid) "
            "WHERE d.server_oid=$1",
            1, &srvType, &srvArg, NULL, snapshot, true, SPI_OK_SELECT);
        int64 readySegs = segStats->count == 1 ? DatumGetInt64(LakeMetadataValue(segStats, 0, 1, INT8OID)) : 0;
        int64 retiredSegs = segStats->count == 1 ? DatumGetInt64(LakeMetadataValue(segStats, 0, 2, INT8OID)) : 0;

        TransactionId oldest = GetOldestXmin(NULL, false, true);
        char* xminStr = psprintf("%lu", (unsigned long)oldest);

        Datum outVals[13];
        bool outNulls[13] = {false};
        outVals[0] = CStringGetTextDatum(server->servername);
        outVals[1] = Int64GetDatum(binding->snapshot_id);
        outVals[2] = Int64GetDatum(totalSnaps);
        outVals[3] = Int64GetDatum(schedFiles);
        outVals[4] = Int64GetDatum(schedBytes);
        outVals[5] = Int64GetDatum(pinCount);
        if (minPin >= 0) {
            outVals[6] = Int64GetDatum(minPin);
        } else {
            outVals[6] = (Datum)0;
            outNulls[6] = true;
        }
        outVals[7] = CStringGetTextDatum(deleteOlderThan);
        outVals[8] = CStringGetTextDatum(xminStr);
        outVals[9] = Int64GetDatum(readySegs);
        outVals[10] = Int64GetDatum(retiredSegs);
        outVals[11] = CStringGetTextDatum(autoCompact);
        outVals[12] = CStringGetTextDatum(targetFileSize);
        tuplestore_putvalues(store, descriptor, outVals, outNulls);

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
        if (snapshot) UnregisterSnapshot(snapshot);
        MemoryContextSwitchTo(caller);
        PG_RE_THROW();
    }
    PG_END_TRY();
    PG_RETURN_NULL();
}
