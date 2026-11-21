/* Controlled local Parquet/CSV COPY FROM/TO over openat2-confined paths and
 * transactional candidate Parquet writers. */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "lake_copy.h"
#include "lake_arrow_builder.h"
#include "lake_external_fdw.h"
#include "lake_external_path.h"
#include "lake_file_reader.h"
#include "lake_file_sdk.h"
#include "lake_numeric.h"
#include "lake_types.h"
#include "lake_vector_codec.h"
#include "lake_version.h"
#include "access/htup.h"
#include "access/sysattr.h"
#include "access/tupmacs.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "executor/tuptable.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "optimizer/stream_check.h"
#include "storage/smgr/fd.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

struct LakeCopyParquetInState {
    const LakeExternalFdSdk* sdk;
    OgLakeSdkReader* reader;
    OgLakeSdkBatch* batch;
    int64 batch_row;
    int mapped_count;
    LakeExternalParquetColumnMap* col_map;
    MemoryContextCallback cleanup_cb;
    bool cleanup_registered;
};

struct LakeCopyParquetOutState {
    MemoryContext copy_context;
    char* path;
    int compression;
    int column_count;
    bool* numeric_as_float8;
    Datum* row_values;
    bool* row_nulls;
    LakeFileCandidate* candidate;
    LakeArrowBuilder* builder;
    bool started;
    bool finished;
    MemoryContextCallback cleanup_cb;
    bool cleanup_registered;
};

static void LakeCopyReleaseInState(LakeCopyParquetInState* state) noexcept
{
    if (state == NULL)
        return;
    if (state->sdk != NULL) {
        HOLD_INTERRUPTS();
        if (state->batch != NULL)
            state->sdk->base->batch_release(&state->batch);
        if (state->reader != NULL)
            state->sdk->base->reader_close(&state->reader);
        RESUME_INTERRUPTS();
    }
}

static void LakeCopyInCleanupCallback(std::shared_ptr<void> arg)
{
    LakeCopyReleaseInState((LakeCopyParquetInState*)arg.get());
}

static void LakeCopyReleaseOutState(LakeCopyParquetOutState* state) noexcept
{
    if (state == NULL)
        return;
    /* builder->context is parented to copy_context; MemoryContextDelete destroys it automatically. */
    state->builder = NULL;
}

static void LakeCopyOutCleanupCallback(std::shared_ptr<void> arg)
{
    LakeCopyReleaseOutState((LakeCopyParquetOutState*)arg.get());
}

void LakeCopyNormalizePath(CopyStmt* stmt)
{
    if (stmt == NULL || stmt->filename == NULL)
        return;
    if (LakeExternalIsRemoteUrl(stmt->filename)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("remote URL COPY requires a qualified remote transport"),
            errdetail("Only local files are enabled in SQL0.62; object-store URLs stay closed until remote SDK qualification.")));
    }
    if (pg_strncasecmp(stmt->filename, "file://", 7) == 0) {
        if (!LakeCopyIoVersionEnabled())
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("file:// URI COPY requires extension 0.62")));
        const char* local = stmt->filename + 7;
        if (local[0] != '/')
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("file:// COPY URI must specify an absolute local path")));
        stmt->filename = pstrdup(local);
    }
}

bool LakeCopyOptionsHaveParquet(List* options)
{
    ListCell* lc = NULL;
    foreach(lc, options) {
        DefElem* defel = (DefElem*)lfirst(lc);
        if (defel->defname != NULL && strcmp(defel->defname, "format") == 0) {
            const char* fmt = defGetString(defel);
            if (fmt != NULL && strcasecmp(fmt, "parquet") == 0)
                return true;
        }
    }
    return false;
}

int LakeCopyParseCompressionOption(const char* value)
{
    if (value == NULL || value[0] == '\0')
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("COPY COMPRESSION option requires a codec name")));
    if (strcasecmp(value, "zstd") == 0)
        return OG_LAKE_SDK_ZSTD;
    if (strcasecmp(value, "snappy") == 0)
        return OG_LAKE_SDK_SNAPPY;
    if (strcasecmp(value, "uncompressed") == 0 || strcasecmp(value, "none") == 0)
        return OG_LAKE_SDK_UNCOMPRESSED;
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("Parquet COPY compression \"%s\" is not supported (supported: zstd, snappy, uncompressed)", value)));
    return OG_LAKE_SDK_ZSTD;
}

bool LakeCopyAllowForeignTo(Relation rel)
{
    if (rel == NULL || RelationGetRelkind(rel) != RELKIND_FOREIGN_TABLE)
        return false;
    return LakeCopyIoVersionEnabled() &&
        (LakeIsNativeForeignRelation(rel) || LakeIsExternalFileRelation(rel));
}

bool LakeCopyAllowLakeFileFrom(void)
{
    return LakeCopyIoVersionEnabled();
}

Node* LakeCopyBuildRelationSelectQuery(Relation rel, List* attnums)
{
    if (rel == NULL || attnums == NIL)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("COPY TO requires at least one readable column")));
    TupleDesc tupDesc = RelationGetDescr(rel);
    char* schemaName = get_namespace_name(RelationGetNamespace(rel));
    char* relName = pstrdup(RelationGetRelationName(rel));
    RangeVar* rv = makeRangeVar(schemaName, relName, -1);

    List* tlist = NIL;
    ListCell* lc = NULL;
    foreach(lc, attnums) {
        int attnum = lfirst_int(lc);
        if (attnum < 1 || attnum > tupDesc->natts || tupDesc->attrs[attnum - 1].attisdropped)
            ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                errmsg("invalid COPY TO attribute number %d", attnum)));
        char* colName = pstrdup(NameStr(tupDesc->attrs[attnum - 1].attname));
        ColumnRef* cref = makeNode(ColumnRef);
        cref->fields = list_make1(makeString(colName));
        cref->location = -1;
        ResTarget* rt = makeNode(ResTarget);
        rt->name = NULL;
        rt->indirection = NIL;
        rt->val = (Node*)cref;
        rt->location = -1;
        tlist = lappend(tlist, rt);
    }

    SelectStmt* sel = makeNode(SelectStmt);
    sel->targetList = tlist;
    sel->fromClause = list_make1(rv);
    return (Node*)sel;
}

FILE* LakeCopyOpenAuthorizedCsvFile(const char* filename, bool isCsv)
{
    if (!LakeCopyIoVersionEnabled())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("controlled file COPY into native lake tables requires extension 0.62")));
    char* root = NULL;
    char* relPath = NULL;
    LakeExternalAuthorizePath(filename, &root, &relPath);
    LakeExternalOpenedFile opened = LakeExternalOpenReadonly(root, relPath, isCsv ? "csv" : "text");
    FILE* volatile fp = NULL;
    PG_TRY();
    {
        char fdPath[64];
        int n = snprintf(fdPath, sizeof(fdPath), "/proc/self/fd/%d", opened.fd);
        if (n < 0 || n >= (int)sizeof(fdPath))
            ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("failed to format descriptor path")));
        fp = AllocateFile(fdPath, PG_BINARY_R);
        int openErrno = errno;
        LakeExternalCloseOpenedFile(&opened);
        if (fp == NULL) {
            errno = openErrno;
            ereport(ERROR, (errcode_for_file_access(),
                errmsg("could not open file \"%s\" for reading: %m", filename)));
        }
    }
    PG_CATCH();
    {
        if (fp != NULL)
            (void)FreeFile(fp);
        LakeExternalCloseOpenedFile(&opened);
        PG_RE_THROW();
    }
    PG_END_TRY();
    return fp;
}

void* LakeCopyBeginParquetIn(Relation rel, const char* filename, List* attnumlist, MemoryContext copyContext)
{
    if (!LakeCopyIoVersionEnabled())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("COPY FROM FORMAT parquet requires extension 0.62")));
    if (rel == NULL || filename == NULL || !IS_SINGLE_NODE || StreamThreadAmI() || IS_SPQ_RUNNING)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Parquet COPY FROM requires single-node execution and a local file path")));
    if (RelationGetRelkind(rel) == RELKIND_FOREIGN_TABLE && !LakeIsNativeForeignRelation(rel))
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg("cannot COPY FROM into foreign table \"%s\"", RelationGetRelationName(rel))));
    if (GetDatabaseEncoding() != PG_UTF8)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Parquet COPY FROM requires a UTF8 database")));
    if (attnumlist == NIL)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Parquet COPY FROM requires at least one target column")));

    char* root = NULL;
    char* relPath = NULL;
    LakeExternalAuthorizePath(filename, &root, &relPath);

    MemoryContext caller = MemoryContextSwitchTo(copyContext);
    LakeCopyParquetInState* state = (LakeCopyParquetInState*)palloc0(sizeof(LakeCopyParquetInState));
    new(&state->cleanup_cb) MemoryContextCallback();
    state->cleanup_cb.func = LakeCopyInCleanupCallback;
    state->cleanup_cb.arg = std::shared_ptr<void>(std::shared_ptr<void>(), state);
    MemoryContextRegisterResetCallback(copyContext, &state->cleanup_cb);
    state->cleanup_registered = true;

    const LakeExternalFdSdk* sdk = LakeFileExternalFdProvider();
    state->sdk = sdk;

    LakeExternalOpenedFile opened = LakeExternalOpenReadonly(root, relPath, "parquet");
    OgLakeSdkFooter* volatile footer = NULL;
    PG_TRY();
    {
        footer = LakeExternalOpenFooter(sdk, opened.fd);
        const OgLakeSdkFooterInfo* info = NULL;
        HOLD_INTERRUPTS();
        info = sdk->base->footer_info(footer);
        RESUME_INTERRUPTS();
        if (info == NULL || info->object_version == NULL || info->object_version[0] == '\0')
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("external Parquet object version is unavailable")));

        state->col_map = LakeExternalBuildColumnMap(sdk, footer, info, RelationGetDescr(rel),
            attnumlist, &state->mapped_count);
        char* expectedVersion = pstrdup(info->object_version);
        LakeExternalCloseFooter(sdk, &footer);

        int64_t* ordinals = state->mapped_count > 0 ?
            (int64_t*)palloc(sizeof(int64_t) * state->mapped_count) : NULL;
        for (int i = 0; i < state->mapped_count; ++i)
            ordinals[i] = state->col_map[i].parquet_ordinal;

        OgLakeSdkReadOptions readOpts = {};
        readOpts.struct_size = sizeof(readOpts);
        readOpts.path = NULL;
        readOpts.limits = LakeFileLimits();
        readOpts.batch_rows = 4096;
        readOpts.num_field_ids = state->mapped_count;
        readOpts.field_ids = ordinals;
        readOpts.num_row_groups = -1;
        readOpts.projection_kind = OG_LAKE_SDK_BY_ORDINAL;

        OgLakeSdkError err;
        HOLD_INTERRUPTS();
        int rst = sdk->fd->reader_open_fd(opened.fd, expectedVersion, &readOpts, &state->reader, &err);
        RESUME_INTERRUPTS();
        LakeFileCheckStatus(rst, &err);
        if (ordinals != NULL)
            pfree(ordinals);
        pfree(expectedVersion);
    }
    PG_CATCH();
    {
        LakeExternalCloseFooter(sdk, &footer);
        LakeExternalCloseOpenedFile(&opened);
        MemoryContextSwitchTo(caller);
        PG_RE_THROW();
    }
    PG_END_TRY();
    LakeExternalCloseOpenedFile(&opened);
    MemoryContextSwitchTo(caller);
    return state;
}

bool LakeCopyNextParquetRow(void* opaque, Datum* values, bool* nulls)
{
    LakeCopyParquetInState* state = (LakeCopyParquetInState*)opaque;
    if (state == NULL || state->reader == NULL)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("Parquet COPY FROM reader state is not active")));

    while (state->batch == NULL || state->batch_row >= state->batch->num_rows) {
        if (state->batch != NULL) {
            HOLD_INTERRUPTS();
            state->sdk->base->batch_release(&state->batch);
            RESUME_INTERRUPTS();
            state->batch_row = 0;
        }
        OgLakeSdkError err;
        HOLD_INTERRUPTS();
        int st = state->sdk->base->reader_next(state->reader, &state->batch, &err);
        RESUME_INTERRUPTS();
        if (st == OG_LAKE_SDK_EOF) {
            state->batch = NULL;
            return false;
        }
        LakeFileCheckStatus(st, &err);
        CHECK_FOR_INTERRUPTS();
        if (state->mapped_count > 0)
            (void)LakeFileCheckArrowBatch(state->batch, state->mapped_count);
        state->batch_row = 0;
    }

    int64 row = state->batch_row++;
    for (int i = 0; i < state->mapped_count; ++i) {
        const LakeExternalParquetColumnMap* m = &state->col_map[i];
        const ArrowArray* arr = state->batch->array->children[i];
        const ArrowSchema* sch = state->batch->schema->children[i];
        int idx = m->target_attnum - 1;
        if (LakeFileDecodeNull(arr, row)) {
            nulls[idx] = true;
            values[idx] = (Datum)0;
        } else {
            Datum raw = (m->parquet_pg_type == VECTOROID) ?
                LakeVectorReadArrow(arr, sch, row, m->parquet_typmod) :
                LakeFileDecodeValue(arr, sch, row, m->parquet_pg_type, NULL, m->parquet_typmod);
            nulls[idx] = false;
            values[idx] = LakeExternalConvertDatum(raw, m);
        }
    }
    return true;
}

void* LakeCopyBeginParquetOut(const char* filename, int compression, MemoryContext copyContext)
{
    if (!LakeCopyIoVersionEnabled())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("COPY TO FORMAT parquet requires extension 0.62")));
    if (filename == NULL || !IS_SINGLE_NODE || StreamThreadAmI() || IS_SPQ_RUNNING)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Parquet COPY TO requires single-node execution and a local output file path")));
    if (GetDatabaseEncoding() != PG_UTF8)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Parquet COPY TO requires a UTF8 database")));

    char* cleanPath = LakeExternalAuthorizeWritePath(filename);
    MemoryContext caller = MemoryContextSwitchTo(copyContext);
    LakeCopyParquetOutState* state = (LakeCopyParquetOutState*)palloc0(sizeof(LakeCopyParquetOutState));
    state->copy_context = copyContext;
    state->path = pstrdup(cleanPath);
    state->compression = (compression >= 0) ? compression : OG_LAKE_SDK_ZSTD;
    new(&state->cleanup_cb) MemoryContextCallback();
    state->cleanup_cb.func = LakeCopyOutCleanupCallback;
    state->cleanup_cb.arg = std::shared_ptr<void>(std::shared_ptr<void>(), state);
    MemoryContextRegisterResetCallback(copyContext, &state->cleanup_cb);
    state->cleanup_registered = true;
    MemoryContextSwitchTo(caller);
    pfree(cleanPath);
    return state;
}

static bool LakeCopyMapExportColumn(Form_pg_attribute attr, OgLakeSdkField* fieldOut,
    int32* typmodOut, bool* numericAsFloat8Out)
{
    Oid type = attr->atttypid;
    *typmodOut = -1;
    *numericAsFloat8Out = false;

    if (type == VARCHAROID || type == BPCHAROID || type == TEXTOID) {
        fieldOut->type = OG_LAKE_SDK_UTF8;
        return true;
    }
    if (type == NUMERICOID && attr->atttypmod == -1) {
        fieldOut->type = OG_LAKE_SDK_FLOAT64;
        *numericAsFloat8Out = true;
        return true;
    }
    if (type == VECTOROID) {
        LakeVectorDimension(attr->atttypmod);
        fieldOut->type = OG_LAKE_SDK_FLOAT32_LIST;
        *typmodOut = attr->atttypmod;
        return true;
    }

    /* Reuse canonical DuckLake native type registry for all scalar/temporal/UUID/JSON/DECIMAL128 types. */
    const LakeNativeType* nt = LakeNativeTypeByOid(type);
    (void)LakeFileProvider();
    LakeNativeTypeRequireSdk(type, LakeFileProvider());
    fieldOut->type = nt->sdk_type;
    if (type == NUMERICOID) {
        (void)LakeNumericParameters(attr->atttypmod, NULL, NULL);
        *typmodOut = attr->atttypmod;
    }
    return true;
}

void LakeCopyStartParquetOut(void* opaque, TupleDesc tupDesc)
{
    LakeCopyParquetOutState* state = (LakeCopyParquetOutState*)opaque;
    if (state == NULL || state->started)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("Parquet COPY TO destination was already initialized or is invalid")));
    if (tupDesc == NULL || tupDesc->natts <= 0)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Parquet COPY TO requires at least one output column")));

    int liveCount = 0;
    for (int i = 0; i < tupDesc->natts; ++i) {
        if (!tupDesc->attrs[i].attisdropped)
            ++liveCount;
    }
    if (liveCount <= 0 || liveCount > 4096)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Parquet COPY TO column count %d exceeds supported range 1..4096", liveCount)));

    OgLakeSdkField* fields = (OgLakeSdkField*)palloc0(sizeof(OgLakeSdkField) * liveCount);
    int32* typmods = (int32*)palloc0(sizeof(int32) * liveCount);
    bool* numFloat = (bool*)palloc0(sizeof(bool) * liveCount);
    OgLakeSdkListField* lists = (OgLakeSdkListField*)palloc0(sizeof(OgLakeSdkListField) * liveCount);
    OgLakeSdkDecimalField* decimals = (OgLakeSdkDecimalField*)palloc0(sizeof(OgLakeSdkDecimalField) * liveCount);
    int listCount = 0;
    int decCount = 0;

    int col = 0;
    for (int i = 0; i < tupDesc->natts; ++i) {
        Form_pg_attribute attr = &tupDesc->attrs[i];
        if (attr->attisdropped)
            continue;
        const char* rawName = NameStr(attr->attname);
        const char* colName = (rawName == NULL || rawName[0] == '\0' || strcmp(rawName, "?column?") == 0) ?
            psprintf("column%d", col) : rawName;
        for (int prev = 0; prev < col; ++prev) {
            if (strcmp(fields[prev].name, colName) == 0)
                ereport(ERROR, (errcode(ERRCODE_DUPLICATE_COLUMN),
                    errmsg("Parquet COPY TO output contains duplicate column name \"%s\"", colName)));
        }
        fields[col].field_id = (int64_t)(col + 1);
        fields[col].name = pstrdup(colName);
        fields[col].nullable = 1;
        (void)LakeCopyMapExportColumn(attr, &fields[col], &typmods[col], &numFloat[col]);
        if (fields[col].type == OG_LAKE_SDK_FLOAT32_LIST) {
            lists[listCount].field_id = fields[col].field_id;
            lists[listCount].element_field_id = (int64_t)(liveCount + 1 + listCount);
            lists[listCount].element_nullable = 1;
            ++listCount;
        } else if (fields[col].type == OG_LAKE_SDK_DECIMAL128) {
            int p = 0, s = 0;
            LakeNumericParameters(typmods[col], &p, &s);
            decimals[decCount].struct_size = sizeof(OgLakeSdkDecimalField);
            decimals[decCount].field_ordinal = col;
            decimals[decCount].precision = p;
            decimals[decCount].scale = s;
            ++decCount;
        }
        ++col;
    }

    MemoryContext ownerCtx = state->copy_context;
    MemoryContext prev = MemoryContextSwitchTo(ownerCtx);
    state->column_count = liveCount;
    state->numeric_as_float8 = (bool*)palloc(sizeof(bool) * liveCount);
    for (int i = 0; i < liveCount; ++i)
        state->numeric_as_float8[i] = numFloat[i];
    state->row_values = (Datum*)palloc0(sizeof(Datum) * liveCount);
    state->row_nulls = (bool*)palloc0(sizeof(bool) * liveCount);
    MemoryContextSwitchTo(prev);

    OgLakeSdkWriteOptions options = {};
    options.struct_size = sizeof(options);
    options.path = state->path;
    options.limits = LakeFileLimits();
    options.num_fields = liveCount;
    options.fields = fields;
    options.compression = (uint32_t)state->compression;
    options.row_group_rows = 4096;

    state->candidate = LakeFileCandidateOpen(
        &options, listCount, listCount > 0 ? lists : NULL, decCount, decCount > 0 ? decimals : NULL);
    prev = MemoryContextSwitchTo(ownerCtx);
    state->builder = LakeArrowBuilderCreate(ownerCtx, fields, liveCount, 4096, 1024 * 1024, typmods);
    MemoryContextSwitchTo(prev);
    state->started = true;

    pfree(fields);
    pfree(typmods);
    pfree(numFloat);
    pfree(lists);
    pfree(decimals);
}

void LakeCopyReceiveParquetSlot(void* opaque, TupleTableSlot* slot)
{
    LakeCopyParquetOutState* state = (LakeCopyParquetOutState*)opaque;
    if (state == NULL || !state->started || state->finished || state->builder == NULL || state->candidate == NULL)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("Parquet COPY TO writer is not active")));

    tableam_tslot_getallattrs(slot);
    TupleDesc tupDesc = slot->tts_tupleDescriptor;
    int outIdx = 0;
    for (int i = 0; i < tupDesc->natts; ++i) {
        if (tupDesc->attrs[i].attisdropped)
            continue;
        if (outIdx >= state->column_count)
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("Parquet COPY TO slot width disagrees with output schema")));
        state->row_nulls[outIdx] = slot->tts_isnull[i];
        if (slot->tts_isnull[i]) {
            state->row_values[outIdx] = (Datum)0;
        } else if (state->numeric_as_float8[outIdx]) {
            state->row_values[outIdx] = DirectFunctionCall1(numeric_float8, slot->tts_values[i]);
        } else {
            state->row_values[outIdx] = slot->tts_values[i];
        }
        ++outIdx;
    }

    if (!LakeArrowBuilderAppend(state->builder, state->row_values, state->row_nulls)) {
        LakeArrowBuilderWrite(state->builder, state->candidate->api, state->candidate->writer);
        LakeArrowBuilderReset(state->builder);
        CHECK_FOR_INTERRUPTS();
        if (!LakeArrowBuilderAppend(state->builder, state->row_values, state->row_nulls))
            ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                errmsg("empty Parquet COPY batch rejected a row")));
    }
}

void LakeCopyFinishParquetOut(void* opaque)
{
    LakeCopyParquetOutState* state = (LakeCopyParquetOutState*)opaque;
    if (state == NULL || !state->started || state->finished)
        return;
    if (state->builder != NULL && LakeArrowBuilderCount(state->builder) > 0) {
        LakeArrowBuilderWrite(state->builder, state->candidate->api, state->candidate->writer);
        LakeArrowBuilderReset(state->builder);
    }
    if (state->builder != NULL) {
        LakeArrowBuilder* b = state->builder;
        state->builder = NULL;
        LakeArrowBuilderDestroy(b);
    }
    LakeFileCandidateFinish(state->candidate);
    LakeFileCandidatePublish(state->candidate);
    state->finished = true;
}

void LakeCopyEndParquetState(void* opaque)
{
    /* Wait for MemoryContextCallback for shared cleanup, or release active reader/builder promptly. */
    (void)opaque;
}

/* Controlled local JSON / JSONL COPY FROM/TO & external FDW helpers (SQL0.71). */
static const Size LAKE_JSON_MAX_LINE_BYTES = 16U * 1024U * 1024U;
static const Size LAKE_JSON_MAX_ARRAY_BYTES = 64U * 1024U * 1024U;
static const int LAKE_JSON_DESCRIBE_MAX_ROWS = 200;

struct LakeJsonSource {
    FILE* fp;
    bool initialized;
    bool is_array;
    bool eof;
    int64 row_no;
    Jsonb* array_jb;
    JsonbIterator* array_it;
    StringInfoData line_buf;
};

struct LakeCopyJsonColumn {
    int target_idx;
    AttrNumber attnum;
    Oid typid;
    int32 typmod;
    bool attnotnull;
    char* attname;
    FmgrInfo in_func;
    Oid typioparam;
};

struct LakeCopyJsonInState {
    LakeJsonSource src;
    int natts;
    int mapped_count;
    LakeCopyJsonColumn* cols;
    bool enforce_not_null;
    MemoryContext row_context;
    MemoryContextCallback cleanup_cb;
    bool cleanup_registered;
};

enum LakeCopyJsonOutKind {
    LAKE_JSON_OUT_BOOL = 0,
    LAKE_JSON_OUT_NUMBER,
    LAKE_JSON_OUT_RAW_JSON,
    LAKE_JSON_OUT_ARRAY,
    LAKE_JSON_OUT_COMPOSITE,
    LAKE_JSON_OUT_STRING
};

struct LakeCopyJsonOutCol {
    int src_idx;
    char* attname;
    LakeCopyJsonOutKind kind;
    FmgrInfo out_func;
};

struct LakeCopyJsonOutState {
    MemoryContext copy_context;
    MemoryContext row_context;
    char* path;
    FILE* fp;
    bool started;
    bool finished;
    int out_count;
    LakeCopyJsonOutCol* cols;
    StringInfoData line_buf;
    MemoryContextCallback cleanup_cb;
    bool cleanup_registered;
};

bool LakeCopyAllowJsonFormat(void)
{
    return LakeCopyIoVersionEnabled() && LakeJsonFormatVersionEnabled();
}

bool LakeCopyOptionsHaveJson(List* options)
{
    ListCell* lc = NULL;
    foreach(lc, options) {
        DefElem* defel = (DefElem*)lfirst(lc);
        if (defel->defname != NULL && strcmp(defel->defname, "format") == 0) {
            const char* fmt = defGetString(defel);
            if (fmt != NULL &&
                (strcasecmp(fmt, "json") == 0 || strcasecmp(fmt, "jsonl") == 0 ||
                 strcasecmp(fmt, "ndjson") == 0))
                return true;
        }
    }
    return false;
}

static void LakeJsonSourceClose(LakeJsonSource* src) noexcept
{
    if (src == NULL)
        return;
    if (src->fp != NULL) {
        (void)fclose(src->fp);
        src->fp = NULL;
    }
    src->array_it = NULL;
    src->array_jb = NULL;
    if (src->line_buf.data != NULL) {
        pfree(src->line_buf.data);
        src->line_buf.data = NULL;
        src->line_buf.len = 0;
        src->line_buf.maxlen = 0;
    }
    src->eof = true;
}

static void LakeJsonSourceInitFromFd(LakeJsonSource* src, int fd, MemoryContext ownerCtx)
{
    memset(src, 0, sizeof(LakeJsonSource));
    int dupFd = dup(fd);
    if (dupFd < 0)
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not duplicate external JSON file descriptor: %m")));
    if (lseek(dupFd, 0, SEEK_SET) < 0) {
        int saveErr = errno;
        (void)close(dupFd);
        errno = saveErr;
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not seek external JSON file descriptor: %m")));
    }
    FILE* fp = fdopen(dupFd, "rb");
    if (fp == NULL) {
        int saveErr = errno;
        (void)close(dupFd);
        errno = saveErr;
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not open JSON stream: %m")));
    }
    src->fp = fp;
    MemoryContext prev = MemoryContextSwitchTo(ownerCtx);
    initStringInfo(&src->line_buf);
    MemoryContextSwitchTo(prev);
}

static void LakeJsonSkipBomAndWs(FILE* fp, bool skipNewlines)
{
    int c = fgetc(fp);
    if (c == 0xEF) {
        int c2 = fgetc(fp);
        int c3 = fgetc(fp);
        if (c2 != 0xBB || c3 != 0xBF) {
            if (c3 != EOF)
                ungetc(c3, fp);
            if (c2 != EOF)
                ungetc(c2, fp);
            ungetc(c, fp);
        }
    } else if (c != EOF) {
        ungetc(c, fp);
    }

    while ((c = fgetc(fp)) != EOF) {
        if (c == ' ' || c == '\t' || (skipNewlines && (c == '\r' || c == '\n')))
            continue;
        ungetc(c, fp);
        break;
    }
}

static bool LakeJsonReadLine(FILE* fp, StringInfo buf, bool* hadData)
{
    *hadData = false;
    resetStringInfo(buf);
    for (;;) {
        int c = fgetc(fp);
        if (c == EOF) {
            if (ferror(fp))
                ereport(ERROR, (errcode_for_file_access(), errmsg("error reading JSON/JSONL file: %m")));
            break;
        }
        *hadData = true;
        if (c == '\n')
            break;
        if (buf->len >= (int)LAKE_JSON_MAX_LINE_BYTES)
            ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                errmsg("JSONL line exceeds maximum size of 16 MiB")));
        appendStringInfoCharMacro(buf, (char)c);
    }
    while (buf->len > 0 && (buf->data[buf->len - 1] == '\r' || buf->data[buf->len - 1] == ' ' ||
                            buf->data[buf->len - 1] == '\t'))
        buf->data[--buf->len] = '\0';
    return *hadData || buf->len > 0;
}

static JsonbSuperHeader LakeJsonSourceNextObject(LakeJsonSource* src, MemoryContext arrayOwnerCtx)
{
    if (src->eof || src->fp == NULL)
        return NULL;

    if (!src->initialized) {
        src->initialized = true;
        LakeJsonSkipBomAndWs(src->fp, true);
        int first = fgetc(src->fp);
        if (first == EOF) {
            if (ferror(src->fp))
                ereport(ERROR, (errcode_for_file_access(), errmsg("error reading JSON file: %m")));
            src->eof = true;
            return NULL;
        }
        ungetc(first, src->fp);
        if (first == '[') {
            src->is_array = true;
            StringInfoData fullBuf;
            initStringInfo(&fullBuf);
            char chunk[8192];
            for (;;) {
                size_t got = fread(chunk, 1, sizeof(chunk), src->fp);
                if (got > 0) {
                    if ((Size)fullBuf.len + got > LAKE_JSON_MAX_ARRAY_BYTES)
                        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                            errmsg("JSON array file exceeds 64 MiB limit; use JSONL/NDJSON for larger inputs")));
                    appendBinaryStringInfo(&fullBuf, chunk, (int)got);
                }
                if (got < sizeof(chunk)) {
                    if (ferror(src->fp))
                        ereport(ERROR, (errcode_for_file_access(), errmsg("error reading JSON array file: %m")));
                    break;
                }
            }
            (void)pg_verify_mbstr(PG_UTF8, fullBuf.data, fullBuf.len, false);
            MemoryContext prev = MemoryContextSwitchTo(arrayOwnerCtx);
            Jsonb* jb = DatumGetJsonb(DirectFunctionCall1(jsonb_in, CStringGetDatum(fullBuf.data)));
            pfree(fullBuf.data);
            if (!JB_ROOT_IS_ARRAY(jb) || JB_ROOT_IS_SCALAR(jb))
                ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                    errmsg("JSON input root must be an array of JSON objects or newline-delimited JSON objects")));
            src->array_jb = jb;
            src->array_it = JsonbIteratorInit((JsonbSuperHeader)VARDATA(jb));
            JsonbValue tok = {};
            int r = JsonbIteratorNext(&src->array_it, &tok, false);
            if (r != WJB_BEGIN_ARRAY)
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid JSON root array state")));
            MemoryContextSwitchTo(prev);
        }
    }

    if (src->is_array) {
        JsonbValue elem = {};
        int r = JsonbIteratorNext(&src->array_it, &elem, true);
        if (r == WJB_END_ARRAY || r == WJB_DONE) {
            src->eof = true;
            return NULL;
        }
        ++src->row_no;
        if (r != WJB_ELEM || elem.type != jbvBinary ||
            !JsonbSuperHeaderIsObject((JsonbSuperHeader)elem.binary.data))
            ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("JSON element %ld is not a JSON object", (long)src->row_no)));
        return (JsonbSuperHeader)elem.binary.data;
    }

    for (;;) {
        bool hadData = false;
        if (!LakeJsonReadLine(src->fp, &src->line_buf, &hadData)) {
            src->eof = true;
            return NULL;
        }
        const char* p = src->line_buf.data;
        while (*p == ' ' || *p == '\t')
            ++p;
        if (*p == '\0') {
            if (!hadData) {
                src->eof = true;
                return NULL;
            }
            continue;
        }
        ++src->row_no;
        (void)pg_verify_mbstr(PG_UTF8, p, (int)strlen(p), false);
        Jsonb* jb = DatumGetJsonb(DirectFunctionCall1(jsonb_in, CStringGetDatum(p)));
        if (!JB_ROOT_IS_OBJECT(jb) || JB_ROOT_IS_SCALAR(jb))
            ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("JSON row %ld is not a JSON object", (long)src->row_no)));
        return (JsonbSuperHeader)VARDATA(jb);
    }
}

static JsonbValue* LakeJsonFindField(JsonbSuperHeader objHeader, const char* colname)
{
    JsonbValue key = {};
    key.type = jbvString;
    key.string.val = (char*)colname;
    key.string.len = (int)strlen(colname);
    JsonbValue* exact = findJsonbValueFromSuperHeader(objHeader, JB_FOBJECT, NULL, &key);
    if (exact != NULL)
        return exact;

    /* Case-insensitive fallback if exact key name is absent, matching Parquet lookup semantics. */
    JsonbIterator* it = JsonbIteratorInit(objHeader);
    JsonbValue ktok = {};
    JsonbValue vtok = {};
    JsonbValue* matched = NULL;
    int matchCount = 0;
    int r = JsonbIteratorNext(&it, &ktok, true);
    if (r != WJB_BEGIN_OBJECT)
        return NULL;
    while ((r = JsonbIteratorNext(&it, &ktok, true)) != WJB_END_OBJECT && r != WJB_DONE) {
        if (r != WJB_KEY)
            continue;
        int vr = JsonbIteratorNext(&it, &vtok, true);
        if (vr != WJB_VALUE)
            break;
        if (ktok.type == jbvString && ktok.string.len == key.string.len) {
            char* kcopy = pnstrdup(ktok.string.val, (Size)ktok.string.len);
            bool eq = (pg_strcasecmp(kcopy, colname) == 0);
            pfree(kcopy);
            if (eq) {
                ++matchCount;
                if (matchCount > 1)
                    ereport(ERROR, (errcode(ERRCODE_AMBIGUOUS_COLUMN),
                        errmsg("JSON object contains multiple case-folded matches for column \"%s\"", colname)));
                matched = (JsonbValue*)palloc(sizeof(JsonbValue));
                *matched = vtok;
            }
        }
    }
    return matched;
}

static Datum LakeJsonDecodeField(JsonbValue* val, const LakeCopyJsonColumn* col)
{
    if (col->typid == JSONOID) {
        Jsonb* jb = JsonbValueToJsonb(val);
        char* jsonText = JsonbToCString(NULL, (JsonbSuperHeader)VARDATA(jb), VARSIZE(jb));
        return DirectFunctionCall1(json_in, CStringGetDatum(jsonText));
    }
    if (col->typid == JSONBOID) {
        return JsonbGetDatum(JsonbValueToJsonb(val));
    }
    if (col->typid == VECTOROID) {
        if (val->type == jbvString) {
            char* s = pnstrdup(val->string.val, (Size)val->string.len);
            return InputFunctionCall((FmgrInfo*)&col->in_func, s, col->typioparam, col->typmod);
        }
        if (val->type == jbvBinary && JsonbSuperHeaderIsArray((JsonbSuperHeader)val->binary.data)) {
            StringInfoData vecStr;
            initStringInfo(&vecStr);
            appendStringInfoChar(&vecStr, '[');
            JsonbIterator* it = JsonbIteratorInit((JsonbSuperHeader)val->binary.data);
            JsonbValue elem = {};
            int r = JsonbIteratorNext(&it, &elem, false);
            if (r != WJB_BEGIN_ARRAY)
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid JSON vector array")));
            bool first = true;
            while ((r = JsonbIteratorNext(&it, &elem, true)) == WJB_ELEM) {
                if (!first)
                    appendStringInfoChar(&vecStr, ',');
                first = false;
                if (elem.type != jbvNumeric)
                    ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("JSON array element for vector column \"%s\" must be numeric", col->attname)));
                char* numText = DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(elem.numeric)));
                appendStringInfoString(&vecStr, numText);
            }
            appendStringInfoChar(&vecStr, ']');
            return InputFunctionCall((FmgrInfo*)&col->in_func, vecStr.data, col->typioparam, col->typmod);
        }
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
            errmsg("JSON value for vector column \"%s\" must be an array or string", col->attname)));
    }

    if (val->type == jbvBool) {
        if (col->typid == BOOLOID)
            return BoolGetDatum(val->boolean);
        if (col->typid == TEXTOID || col->typid == VARCHAROID || col->typid == BPCHAROID)
            return InputFunctionCall((FmgrInfo*)&col->in_func, pstrdup(val->boolean ? "true" : "false"),
                col->typioparam, col->typmod);
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
            errmsg("JSON boolean cannot populate column \"%s\"", col->attname)));
    }
    if (val->type == jbvNumeric) {
        char* numText = DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(val->numeric)));
        return InputFunctionCall((FmgrInfo*)&col->in_func, numText, col->typioparam, col->typmod);
    }
    if (val->type == jbvString) {
        char* str = pnstrdup(val->string.val, (Size)val->string.len);
        return InputFunctionCall((FmgrInfo*)&col->in_func, str, col->typioparam, col->typmod);
    }
    if (val->type == jbvBinary) {
        if (col->typid == TEXTOID || col->typid == VARCHAROID || col->typid == BPCHAROID) {
            Jsonb* jb = JsonbValueToJsonb(val);
            char* text = JsonbToCString(NULL, (JsonbSuperHeader)VARDATA(jb), VARSIZE(jb));
            return InputFunctionCall((FmgrInfo*)&col->in_func, text, col->typioparam, col->typmod);
        }
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
            errmsg("nested JSON container cannot populate scalar column \"%s\"", col->attname)));
    }
    ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
        errmsg("unsupported JSON value kind for column \"%s\"", col->attname)));
    return (Datum)0;
}

static void LakeCopyJsonInCleanupCallback(std::shared_ptr<void> arg)
{
    LakeCopyJsonInState* state = (LakeCopyJsonInState*)arg.get();
    if (state != NULL)
        LakeJsonSourceClose(&state->src);
}

void* LakeCopyBeginJsonInFd(Relation rel, int fd, List* attnumlist, MemoryContext copyContext, bool enforceNotNull)
{
    if (!LakeCopyAllowJsonFormat())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("JSON/JSONL COPY and external scans require extension 0.71")));
    if (rel == NULL || fd < 0 || !IS_SINGLE_NODE || StreamThreadAmI() || IS_SPQ_RUNNING)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("JSON/JSONL scan requires single-node execution and a valid local file")));
    if (GetDatabaseEncoding() != PG_UTF8)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("JSON/JSONL input requires a UTF8 database")));
    if (attnumlist == NIL)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("JSON/JSONL input requires at least one column")));

    TupleDesc tupDesc = RelationGetDescr(rel);
    MemoryContext prev = MemoryContextSwitchTo(copyContext);
    LakeCopyJsonInState* state = (LakeCopyJsonInState*)palloc0(sizeof(LakeCopyJsonInState));
    state->natts = tupDesc->natts;
    state->mapped_count = list_length(attnumlist);
    state->enforce_not_null = enforceNotNull;
    state->cols = (LakeCopyJsonColumn*)palloc0(sizeof(LakeCopyJsonColumn) * state->mapped_count);
    state->row_context = AllocSetContextCreate(copyContext, "Lake JSON COPY IN row", ALLOCSET_DEFAULT_SIZES);

    int i = 0;
    ListCell* lc = NULL;
    foreach(lc, attnumlist) {
        int attnum = lfirst_int(lc);
        if (attnum < 1 || attnum > tupDesc->natts || tupDesc->attrs[attnum - 1].attisdropped)
            ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                errmsg("invalid target column number %d for JSON input", attnum)));
        Form_pg_attribute attr = &tupDesc->attrs[attnum - 1];
        Oid inFuncOid = InvalidOid;
        getTypeInputInfo(attr->atttypid, &inFuncOid, &state->cols[i].typioparam);
        fmgr_info_cxt(inFuncOid, &state->cols[i].in_func, copyContext);
        state->cols[i].target_idx = attnum - 1;
        state->cols[i].attnum = (AttrNumber)attnum;
        state->cols[i].typid = attr->atttypid;
        state->cols[i].typmod = attr->atttypmod;
        state->cols[i].attnotnull = attr->attnotnull;
        state->cols[i].attname = pstrdup(NameStr(attr->attname));
        ++i;
    }

    LakeJsonSourceInitFromFd(&state->src, fd, copyContext);
    new(&state->cleanup_cb) MemoryContextCallback();
    state->cleanup_cb.func = LakeCopyJsonInCleanupCallback;
    state->cleanup_cb.arg = std::shared_ptr<void>(std::shared_ptr<void>(), state);
    MemoryContextRegisterResetCallback(copyContext, &state->cleanup_cb);
    state->cleanup_registered = true;
    MemoryContextSwitchTo(prev);
    return state;
}

void* LakeCopyBeginJsonIn(Relation rel, const char* filename, List* attnumlist, MemoryContext copyContext)
{
    if (!LakeCopyAllowJsonFormat())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("COPY FROM FORMAT json requires extension 0.71")));
    if (rel == NULL || filename == NULL)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("JSON COPY FROM requires single-node execution and a local file path")));
    if (RelationGetRelkind(rel) == RELKIND_FOREIGN_TABLE && !LakeIsNativeForeignRelation(rel))
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg("cannot COPY FROM into foreign table \"%s\"", RelationGetRelationName(rel))));

    char* root = NULL;
    char* relPath = NULL;
    LakeExternalAuthorizePath(filename, &root, &relPath);
    LakeExternalOpenedFile opened = LakeExternalOpenReadonly(root, relPath, "json");
    void* volatile state = NULL;
    PG_TRY();
    {
        state = LakeCopyBeginJsonInFd(rel, opened.fd, attnumlist, copyContext, false);
    }
    PG_CATCH();
    {
        LakeExternalCloseOpenedFile(&opened);
        PG_RE_THROW();
    }
    PG_END_TRY();
    LakeExternalCloseOpenedFile(&opened);
    return state;
}

bool LakeCopyNextJsonRow(void* opaque, Datum* values, bool* nulls)
{
    LakeCopyJsonInState* state = (LakeCopyJsonInState*)opaque;
    if (state == NULL)
        return false;

    for (int i = 0; i < state->natts; ++i) {
        values[i] = (Datum)0;
        nulls[i] = true;
    }

    MemoryContextReset(state->row_context);
    MemoryContext caller = MemoryContextSwitchTo(state->row_context);
    JsonbSuperHeader volatile obj = NULL;
    PG_TRY();
    {
        obj = LakeJsonSourceNextObject(&state->src, state->src.line_buf.data ?
            state->row_context->parent : state->row_context);
        if (obj != NULL) {
            for (int i = 0; i < state->mapped_count; ++i) {
                const LakeCopyJsonColumn* col = &state->cols[i];
                JsonbValue* val = LakeJsonFindField(obj, col->attname);
                if (val == NULL || val->type == jbvNull) {
                    if (state->enforce_not_null && col->attnotnull)
                        ereport(ERROR, (errcode(ERRCODE_NOT_NULL_VIOLATION),
                            errmsg("null value in column \"%s\" violates not-null constraint", col->attname)));
                    nulls[col->target_idx] = true;
                    values[col->target_idx] = (Datum)0;
                } else {
                    values[col->target_idx] = LakeJsonDecodeField(val, col);
                    nulls[col->target_idx] = false;
                }
            }
        }
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(caller);
        PG_RE_THROW();
    }
    PG_END_TRY();
    MemoryContextSwitchTo(caller);
    return obj != NULL;
}

static void LakeCopyJsonOutCleanupCallback(std::shared_ptr<void> arg)
{
    LakeCopyJsonOutState* state = (LakeCopyJsonOutState*)arg.get();
    if (state == NULL)
        return;
    if (state->fp != NULL) {
        (void)fclose(state->fp);
        state->fp = NULL;
    }
    if (!state->finished && state->path != NULL)
        (void)unlink(state->path);
}

void* LakeCopyBeginJsonOut(const char* filename, MemoryContext copyContext)
{
    if (!LakeCopyAllowJsonFormat())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("COPY TO FORMAT json requires extension 0.71")));
    if (filename == NULL || !IS_SINGLE_NODE || StreamThreadAmI() || IS_SPQ_RUNNING)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("JSON COPY TO requires single-node execution and a local output file path")));
    if (GetDatabaseEncoding() != PG_UTF8)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("JSON COPY TO requires a UTF8 database")));

    char* cleanPath = LakeExternalAuthorizeWritePath(filename);
    MemoryContext caller = MemoryContextSwitchTo(copyContext);
    LakeCopyJsonOutState* state = (LakeCopyJsonOutState*)palloc0(sizeof(LakeCopyJsonOutState));
    state->copy_context = copyContext;
    state->row_context = AllocSetContextCreate(copyContext, "Lake JSON COPY OUT row", ALLOCSET_DEFAULT_SIZES);
    state->path = pstrdup(cleanPath);
    pfree(cleanPath);

    new(&state->cleanup_cb) MemoryContextCallback();
    state->cleanup_cb.func = LakeCopyJsonOutCleanupCallback;
    state->cleanup_cb.arg = std::shared_ptr<void>(std::shared_ptr<void>(), state);
    MemoryContextRegisterResetCallback(copyContext, &state->cleanup_cb);
    state->cleanup_registered = true;

    int fd = open(state->path, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        int err = errno;
        MemoryContextSwitchTo(caller);
        errno = err;
        ereport(ERROR, (errcode(err == EEXIST ? ERRCODE_DUPLICATE_FILE : ERRCODE_IO_ERROR),
            errmsg("could not open JSON COPY TO file \"%s\" for writing: %m", state->path)));
    }
    state->fp = fdopen(fd, "wb");
    if (state->fp == NULL) {
        int err = errno;
        (void)close(fd);
        (void)unlink(state->path);
        MemoryContextSwitchTo(caller);
        errno = err;
        ereport(ERROR, (errcode_for_file_access(),
            errmsg("could not create buffered stream for JSON COPY TO file \"%s\": %m", state->path)));
    }
    MemoryContextSwitchTo(caller);
    return state;
}

static void AppendJsonNormalizedNumber(StringInfo buf, const char* raw)
{
    if (raw == NULL || raw[0] == '\0') {
        appendStringInfoString(buf, "null");
        return;
    }
    /* openGauss numeric_out omits leading '0' for |x| < 1 (".05", "-.05"); normalize for RFC 8259. */
    if (raw[0] == '.' && isdigit((unsigned char)raw[1])) {
        appendStringInfoChar(buf, '0');
        appendStringInfoString(buf, raw);
        return;
    }
    if (raw[0] == '-' && raw[1] == '.' && isdigit((unsigned char)raw[2])) {
        appendStringInfoString(buf, "-0");
        appendStringInfoString(buf, raw + 1);
        return;
    }
    const char* p = (raw[0] == '-') ? raw + 1 : raw;
    if (!isdigit((unsigned char)p[0])) {
        escape_json(buf, raw);
        return;
    }
    appendStringInfoString(buf, raw);
}

void LakeCopyStartJsonOut(void* opaque, TupleDesc tupDesc)
{
    LakeCopyJsonOutState* state = (LakeCopyJsonOutState*)opaque;
    if (state == NULL || state->started)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("JSON COPY TO destination was already initialized or is invalid")));
    if (tupDesc == NULL || tupDesc->natts <= 0)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("JSON COPY TO requires at least one output column")));

    MemoryContext caller = MemoryContextSwitchTo(state->copy_context);
    state->cols = (LakeCopyJsonOutCol*)palloc0(sizeof(LakeCopyJsonOutCol) * (Size)tupDesc->natts);
    state->out_count = 0;

    for (int i = 0; i < tupDesc->natts; ++i) {
        if (tupDesc->attrs[i].attisdropped)
            continue;
        const char* rawName = NameStr(tupDesc->attrs[i].attname);
        char autoName[NAMEDATALEN];
        if (rawName == NULL || rawName[0] == '\0' || strcmp(rawName, "?column?") == 0) {
            snprintf(autoName, sizeof(autoName), "column%d", i);
            rawName = autoName;
        }

        LakeCopyJsonOutCol* col = &state->cols[state->out_count++];
        col->src_idx = i;
        col->attname = pstrdup(rawName);

        Oid typid = tupDesc->attrs[i].atttypid;
        Oid baseTypid = (get_typtype(typid) == TYPTYPE_DOMAIN) ? getBaseType(typid) : typid;
        Oid typOutput = InvalidOid;
        bool typIsVarlena = false;

        if (baseTypid == BOOLOID) {
            col->kind = LAKE_JSON_OUT_BOOL;
        } else if (baseTypid == INT2OID || baseTypid == INT4OID || baseTypid == INT8OID ||
                   baseTypid == FLOAT4OID || baseTypid == FLOAT8OID || baseTypid == NUMERICOID) {
            col->kind = LAKE_JSON_OUT_NUMBER;
            getTypeOutputInfo(typid, &typOutput, &typIsVarlena);
            fmgr_info_cxt(typOutput, &col->out_func, state->copy_context);
        } else if (baseTypid == JSONOID || baseTypid == JSONBOID) {
            col->kind = LAKE_JSON_OUT_RAW_JSON;
            getTypeOutputInfo(typid, &typOutput, &typIsVarlena);
            fmgr_info_cxt(typOutput, &col->out_func, state->copy_context);
        } else if (type_is_array(baseTypid) || baseTypid == RECORDARRAYOID) {
            col->kind = LAKE_JSON_OUT_ARRAY;
        } else if (get_typtype(baseTypid) == TYPTYPE_COMPOSITE || baseTypid == RECORDOID) {
            col->kind = LAKE_JSON_OUT_COMPOSITE;
        } else {
            col->kind = LAKE_JSON_OUT_STRING;
            getTypeOutputInfo(typid, &typOutput, &typIsVarlena);
            fmgr_info_cxt(typOutput, &col->out_func, state->copy_context);
        }
    }

    if (state->out_count <= 0) {
        MemoryContextSwitchTo(caller);
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("JSON COPY TO requires at least one non-dropped output column")));
    }

    initStringInfo(&state->line_buf);
    state->started = true;
    MemoryContextSwitchTo(caller);
}

void LakeCopyReceiveJsonSlot(void* opaque, TupleTableSlot* slot)
{
    LakeCopyJsonOutState* state = (LakeCopyJsonOutState*)opaque;
    if (state == NULL || !state->started || state->finished || state->fp == NULL)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("JSON COPY TO destination is not active")));

    tableam_tslot_getallattrs(slot);
    MemoryContextReset(state->row_context);
    MemoryContext caller = MemoryContextSwitchTo(state->row_context);
    PG_TRY();
    {
        StringInfo buf = &state->line_buf;
        resetStringInfo(buf);
        appendStringInfoCharMacro(buf, '{');

        for (int i = 0; i < state->out_count; ++i) {
            const LakeCopyJsonOutCol* col = &state->cols[i];
            if (i > 0)
                appendStringInfoCharMacro(buf, ',');
            escape_json(buf, col->attname);
            appendStringInfoCharMacro(buf, ':');

            if (slot->tts_isnull[col->src_idx]) {
                appendStringInfoString(buf, "null");
                continue;
            }

            Datum val = slot->tts_values[col->src_idx];
            switch (col->kind) {
                case LAKE_JSON_OUT_BOOL:
                    appendStringInfoString(buf, DatumGetBool(val) ? "true" : "false");
                    break;
                case LAKE_JSON_OUT_NUMBER:
                    AppendJsonNormalizedNumber(buf, OutputFunctionCall((FmgrInfo*)&col->out_func, val));
                    break;
                case LAKE_JSON_OUT_RAW_JSON:
                    appendStringInfoString(buf, OutputFunctionCall((FmgrInfo*)&col->out_func, val));
                    break;
                case LAKE_JSON_OUT_ARRAY: {
                    Datum jd = DirectFunctionCall1(array_to_json, val);
                    text* jt = DatumGetTextP(jd);
                    appendBinaryStringInfo(buf, VARDATA_ANY(jt), VARSIZE_ANY_EXHDR(jt));
                    break;
                }
                case LAKE_JSON_OUT_COMPOSITE: {
                    Datum jd = DirectFunctionCall1(row_to_json, val);
                    text* jt = DatumGetTextP(jd);
                    appendBinaryStringInfo(buf, VARDATA_ANY(jt), VARSIZE_ANY_EXHDR(jt));
                    break;
                }
                case LAKE_JSON_OUT_STRING:
                default:
                    escape_json(buf, OutputFunctionCall((FmgrInfo*)&col->out_func, val));
                    break;
            }
        }

        appendStringInfoString(buf, "}\n");
        if (fwrite(buf->data, 1, (size_t)buf->len, state->fp) != (size_t)buf->len)
            ereport(ERROR, (errcode_for_file_access(),
                errmsg("could not write JSON COPY TO file \"%s\": %m", state->path)));
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(caller);
        PG_RE_THROW();
    }
    PG_END_TRY();
    MemoryContextSwitchTo(caller);
}

void LakeCopyFinishJsonOut(void* opaque)
{
    LakeCopyJsonOutState* state = (LakeCopyJsonOutState*)opaque;
    if (state == NULL || !state->started || state->finished)
        return;
    if (state->fp != NULL) {
        FILE* fp = state->fp;
        state->fp = NULL;
        if (fflush(fp) != 0 || ferror(fp)) {
            (void)fclose(fp);
            ereport(ERROR, (errcode_for_file_access(),
                errmsg("could not flush JSON COPY TO file \"%s\": %m", state->path)));
        }
        if (fclose(fp) != 0)
            ereport(ERROR, (errcode_for_file_access(),
                errmsg("could not close JSON COPY TO file \"%s\": %m", state->path)));
    }
    state->finished = true;
}

void LakeCopyEndJsonState(void* opaque, bool isFrom)
{
    if (opaque == NULL)
        return;
    if (isFrom) {
        LakeCopyJsonInState* inState = (LakeCopyJsonInState*)opaque;
        LakeJsonSourceClose(&inState->src);
        return;
    }
    LakeCopyJsonOutState* outState = (LakeCopyJsonOutState*)opaque;
    if (outState->fp != NULL) {
        (void)fclose(outState->fp);
        outState->fp = NULL;
    }
    if (!outState->finished && outState->path != NULL)
        (void)unlink(outState->path);
}

/* Shared JSON Schema Inference & Info Inspection */
enum LakeJsonInferKind {
    LAKE_JSON_UNKNOWN = 0,
    LAKE_JSON_BOOL,
    LAKE_JSON_INT8,
    LAKE_JSON_FLOAT8,
    LAKE_JSON_DATE,
    LAKE_JSON_JSON,
    LAKE_JSON_TEXT
};

struct LakeJsonInferCol {
    char* name;
    LakeJsonInferKind kind;
};

static bool LakeJsonIsInt64Str(const char* tok)
{
    if (*tok == '\0')
        return false;
    const char* p = tok;
    if (*p == '+' || *p == '-')
        ++p;
    if (*p == '\0' || (*p == '0' && p[1] != '\0'))
        return false;
    while (*p != '\0') {
        if (!isdigit((unsigned char)*p))
            return false;
        ++p;
    }
    errno = 0;
    char* end = NULL;
    (void)strtoll(tok, &end, 10);
    return errno == 0 && end != NULL && *end == '\0';
}

static bool LakeJsonIsDateStr(const char* tok, Size len)
{
    if (len != 10 || tok[4] != '-' || tok[7] != '-')
        return false;
    for (int i = 0; i < 10; ++i) {
        if (i == 4 || i == 7)
            continue;
        if (!isdigit((unsigned char)tok[i]))
            return false;
    }
    int y = (tok[0] - '0') * 1000 + (tok[1] - '0') * 100 + (tok[2] - '0') * 10 + (tok[3] - '0');
    int m = (tok[5] - '0') * 10 + (tok[6] - '0');
    int d = (tok[8] - '0') * 10 + (tok[9] - '0');
    return y >= 1 && m >= 1 && m <= 12 && d >= 1 && d <= 31;
}

static LakeJsonInferKind LakeJsonClassifyVal(const JsonbValue* v)
{
    switch (v->type) {
        case jbvNull:
            return LAKE_JSON_UNKNOWN;
        case jbvBool:
            return LAKE_JSON_BOOL;
        case jbvNumeric: {
            char* numStr = DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(v->numeric)));
            return LakeJsonIsInt64Str(numStr) ? LAKE_JSON_INT8 : LAKE_JSON_FLOAT8;
        }
        case jbvString:
            return LakeJsonIsDateStr(v->string.val, (Size)v->string.len) ? LAKE_JSON_DATE : LAKE_JSON_TEXT;
        case jbvBinary:
        case jbvArray:
        case jbvObject:
            return LAKE_JSON_JSON;
        default:
            return LAKE_JSON_TEXT;
    }
}

static LakeJsonInferKind LakeJsonMergeKind(LakeJsonInferKind existing, LakeJsonInferKind sample)
{
    if (sample == LAKE_JSON_UNKNOWN)
        return existing;
    if (existing == LAKE_JSON_UNKNOWN)
        return sample;
    if (existing == sample)
        return existing;
    if ((existing == LAKE_JSON_INT8 && sample == LAKE_JSON_FLOAT8) ||
        (existing == LAKE_JSON_FLOAT8 && sample == LAKE_JSON_INT8))
        return LAKE_JSON_FLOAT8;
    if (existing == LAKE_JSON_JSON || sample == LAKE_JSON_JSON)
        return LAKE_JSON_JSON;
    return LAKE_JSON_TEXT;
}

static const char* LakeJsonKindName(LakeJsonInferKind kind)
{
    switch (kind) {
        case LAKE_JSON_BOOL:
            return "boolean";
        case LAKE_JSON_INT8:
            return "bigint";
        case LAKE_JSON_FLOAT8:
            return "double precision";
        case LAKE_JSON_DATE:
            return "date";
        case LAKE_JSON_JSON:
            return "json";
        case LAKE_JSON_UNKNOWN:
        case LAKE_JSON_TEXT:
        default:
            return "text";
    }
}

void LakeJsonInspectFd(int fd, Tuplestorestate* store, TupleDesc outDesc)
{
    if (!LakeJsonFormatVersionEnabled())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("external JSON schema inspection requires extension 0.71")));

    MemoryContext callerCtx = CurrentMemoryContext;
    MemoryContext scratchCtx = AllocSetContextCreate(callerCtx, "Lake JSON inspect", ALLOCSET_DEFAULT_SIZES);
    LakeJsonSource src = {};
    LakeJsonSourceInitFromFd(&src, fd, callerCtx);
    List* cols = NIL;

    PG_TRY();
    {
        MemoryContext rowCtx = AllocSetContextCreate(scratchCtx, "Lake JSON inspect row", ALLOCSET_DEFAULT_SIZES);
        for (int n = 0; n < LAKE_JSON_DESCRIBE_MAX_ROWS; ++n) {
            MemoryContextReset(rowCtx);
            MemoryContextSwitchTo(rowCtx);
            JsonbSuperHeader obj = LakeJsonSourceNextObject(&src, scratchCtx);
            MemoryContextSwitchTo(callerCtx);
            if (obj == NULL)
                break;

            JsonbIterator* it = JsonbIteratorInit(obj);
            JsonbValue k = {};
            JsonbValue v = {};
            if (JsonbIteratorNext(&it, &k, true) != WJB_BEGIN_OBJECT)
                continue;
            int r;
            while ((r = JsonbIteratorNext(&it, &k, true)) != WJB_END_OBJECT && r != WJB_DONE) {
                if (r != WJB_KEY)
                    continue;
                if (JsonbIteratorNext(&it, &v, true) != WJB_VALUE)
                    break;
                char* keyName = pnstrdup(k.string.val, (Size)k.string.len);
                MemoryContextSwitchTo(rowCtx);
                LakeJsonInferKind knd = LakeJsonClassifyVal(&v);
                MemoryContextSwitchTo(callerCtx);

                LakeJsonInferCol* found = NULL;
                ListCell* lc = NULL;
                foreach(lc, cols) {
                    LakeJsonInferCol* c = (LakeJsonInferCol*)lfirst(lc);
                    if (strcmp(c->name, keyName) == 0) {
                        found = c;
                        break;
                    }
                }
                if (found == NULL) {
                    MemoryContextSwitchTo(scratchCtx);
                    found = (LakeJsonInferCol*)palloc0(sizeof(LakeJsonInferCol));
                    found->name = pstrdup(keyName);
                    found->kind = knd;
                    cols = lappend(cols, found);
                    MemoryContextSwitchTo(callerCtx);
                } else {
                    found->kind = LakeJsonMergeKind(found->kind, knd);
                }
                pfree(keyName);
            }
        }
        LakeJsonSourceClose(&src);
    }
    PG_CATCH();
    {
        LakeJsonSourceClose(&src);
        MemoryContextSwitchTo(callerCtx);
        MemoryContextDelete(scratchCtx);
        PG_RE_THROW();
    }
    PG_END_TRY();

    if (cols == NIL) {
        MemoryContextDelete(scratchCtx);
        ereport(ERROR, (errcode(ERRCODE_NO_DATA),
            errmsg("cannot infer schema from empty external JSON file")));
    }

    int ordinal = 1;
    ListCell* lc = NULL;
    foreach(lc, cols) {
        LakeJsonInferCol* c = (LakeJsonInferCol*)lfirst(lc);
        Datum vals[4];
        bool nulls[4] = {false, false, false, false};
        vals[0] = CStringGetTextDatum(c->name);
        vals[1] = CStringGetTextDatum(LakeJsonKindName(c->kind));
        vals[2] = BoolGetDatum(true);
        vals[3] = Int32GetDatum(ordinal++);
        tuplestore_putvalues(store, outDesc, vals, nulls);
    }
    MemoryContextDelete(scratchCtx);
}

void LakeJsonCountFd(int fd, int64* rowCountOut, int32* colCountOut)
{
    if (!LakeJsonFormatVersionEnabled())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("external JSON inspection requires extension 0.71")));

    MemoryContext callerCtx = CurrentMemoryContext;
    MemoryContext scratchCtx = AllocSetContextCreate(callerCtx, "Lake JSON count", ALLOCSET_DEFAULT_SIZES);
    LakeJsonSource src = {};
    LakeJsonSourceInitFromFd(&src, fd, callerCtx);
    int64 rows = 0;
    int32 maxCols = 0;

    PG_TRY();
    {
        MemoryContext rowCtx = AllocSetContextCreate(scratchCtx, "Lake JSON count row", ALLOCSET_DEFAULT_SIZES);
        for (;;) {
            CHECK_FOR_INTERRUPTS();
            MemoryContextReset(rowCtx);
            MemoryContextSwitchTo(rowCtx);
            JsonbSuperHeader obj = LakeJsonSourceNextObject(&src, scratchCtx);
            if (obj == NULL) {
                MemoryContextSwitchTo(callerCtx);
                break;
            }
            ++rows;
            int32 colCount = (int32)(*(uint32*)obj & JB_CMASK);
            if (colCount > maxCols)
                maxCols = colCount;
            MemoryContextSwitchTo(callerCtx);
        }
        LakeJsonSourceClose(&src);
    }
    PG_CATCH();
    {
        LakeJsonSourceClose(&src);
        MemoryContextSwitchTo(callerCtx);
        MemoryContextDelete(scratchCtx);
        PG_RE_THROW();
    }
    PG_END_TRY();
    MemoryContextDelete(scratchCtx);
    *rowCountOut = rows;
    *colCountOut = maxCols;
}
