/* Read-only external Parquet/CSV FDW, openat2 confinement, and schema inference. */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "lake_external_fdw.h"
#include "lake_copy.h"
#include "lake_external_path.h"
#include "lake_file_reader.h"
#include "lake_file_sdk.h"
#include "lake_numeric.h"
#include "lake_types.h"
#include "lake_vector_codec.h"
#include "lake_version.h"
#include "lake_sample.h"
#include "access/datavec/vector.h"
#include "access/og_lake.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_language.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/extension.h"
#include "commands/vacuum.h"
#include "executor/tuptable.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/stream_check.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "storage/buf/block.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tuplestore.h"
#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <math.h>
#include <unistd.h>

extern "C" Datum og_lake_file_fdw_handler(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_file_fdw_validator(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_describe_external_server_file(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_describe_external_path_file(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_external_file_info(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(og_lake_file_fdw_handler);
PG_FUNCTION_INFO_V1(og_lake_file_fdw_validator);
PG_FUNCTION_INFO_V1(og_lake_describe_external_server_file);
PG_FUNCTION_INFO_V1(og_lake_describe_external_path_file);
PG_FUNCTION_INFO_V1(og_lake_external_file_info);

struct LakeExternalTableOptions {
    char* relative_path;
    char* format;
    bool is_parquet;
    bool is_json;
    List* copy_options;
};

struct LakeExternalPlanState {
    char* root;
    char* relative_path;
    char* format;
    bool is_parquet;
    bool is_json;
    List* copy_options;
    List* projected_attnums; /* 1-based live host AttrNumbers */
    BlockNumber pages;
    double ntuples;
    int64 file_bytes;
    int64 row_groups;
};

struct LakeExternalScanState {
    bool is_parquet;
    bool is_json;
    char* root;
    char* relative_path;
    char* format;
    List* copy_options;
    List* projected_attnums;
    /* Parquet state (SDK6) */
    const LakeExternalFdSdk* sdk;
    OgLakeSdkReader* reader;
    OgLakeSdkBatch* batch;
    int64 batch_row;
    char* expected_version;
    int64 file_bytes;
    int64 row_groups;
    int64 total_rows;
    int mapped_count;
    LakeExternalParquetColumnMap* col_map;
    MemoryContext row_context;
    /* CSV / TEXT state (host COPY parser via openat2 fd) */
    CopyState cstate;
    /* JSON / JSONL streaming state (SQL0.71) */
    void* json_state;
    MemoryContextCallback cleanup_cb;
};

static bool LakeExternalTrustedWrapper(Oid fdwOid)
{
    ForeignDataWrapper* fdw = GetForeignDataWrapper(fdwOid);
    if (strcmp(fdw->fdwname, "og_lake_file_fdw") != 0)
        return false;
    Oid extension = get_extension_oid("og_lake", true);
    if (!OidIsValid(extension) || getExtensionOfObject(ForeignDataWrapperRelationId, fdw->fdwid) != extension)
        return false;
    const Oid functions[] = {fdw->fdwhandler, fdw->fdwvalidator};
    const char* symbols[] = {"og_lake_file_fdw_handler", "og_lake_file_fdw_validator"};
    for (Size i = 0; i < lengthof(functions); ++i) {
        HeapTuple tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(functions[i]));
        if (!HeapTupleIsValid(tuple))
            return false;
        Form_pg_proc proc = (Form_pg_proc)GETSTRUCT(tuple);
        bool sourceNull = false, libNull = false;
        Datum source = SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_prosrc, &sourceNull);
        Datum library = SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_probin, &libNull);
        bool valid = proc->prolang == ClanguageId && superuser_arg(proc->proowner) &&
            !sourceNull && !libNull &&
            strcmp(TextDatumGetCString(source), symbols[i]) == 0 &&
            strcmp(TextDatumGetCString(library), "$libdir/og_lake") == 0 &&
            getExtensionOfObject(ProcedureRelationId, functions[i]) == extension;
        ReleaseSysCache(tuple);
        if (!valid)
            return false;
    }
    return true;
}

bool LakeIsExternalFileRelation(Relation relation)
{
    if (relation == NULL || RelationGetRelkind(relation) != RELKIND_FOREIGN_TABLE)
        return false;
    ForeignTable* table = GetForeignTable(RelationGetRelid(relation));
    ForeignServer* server = GetForeignServer(table->serverid);
    return LakeExternalTrustedWrapper(server->fdwid);
}

static char* LakeExternalServerRoot(Oid serverOid)
{
    ForeignServer* server = GetForeignServer(serverOid);
    if (!LakeExternalTrustedWrapper(server->fdwid))
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg("foreign server \"%s\" is not an og_lake_file_fdw external file server", server->servername)));
    const char* root = NULL;
    ListCell* cell = NULL;
    foreach(cell, server->options) {
        DefElem* opt = (DefElem*)lfirst(cell);
        if (strcmp(opt->defname, "external_root") == 0 && root == NULL)
            root = defGetString(opt);
        else
            ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                errmsg("invalid or duplicate option \"%s\" on external file server \"%s\"",
                    opt->defname, server->servername)));
    }
    if (root == NULL || root[0] == '\0')
        ereport(ERROR, (errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
            errmsg("external file server \"%s\" requires an external_root directory option", server->servername)));
    return LakeExternalValidateRoot(root);
}

static List* LakeExternalAttributeForceNotNull(Oid relid)
{
    Relation rel = heap_open(relid, AccessShareLock);
    TupleDesc tupDesc = RelationGetDescr(rel);
    List* fnnCols = NIL;
    for (int attnum = 1; attnum <= tupDesc->natts; ++attnum) {
        if (tupDesc->attrs[attnum - 1].attisdropped)
            continue;
        List* opts = GetForeignColumnOptions(relid, attnum);
        ListCell* lc = NULL;
        foreach(lc, opts) {
            DefElem* def = (DefElem*)lfirst(lc);
            if (strcmp(def->defname, "force_not_null") == 0 && defGetBoolean(def)) {
                char* attname = pstrdup(NameStr(tupDesc->attrs[attnum - 1].attname));
                fnnCols = lappend(fnnCols, makeString(attname));
            }
        }
    }
    heap_close(rel, AccessShareLock);
    if (fnnCols != NIL)
        return list_make1(makeDefElem("force_not_null", (Node*)fnnCols));
    return NIL;
}

static LakeExternalTableOptions LakeExternalParseTableOptions(List* options, Oid relid)
{
    const char* relPath = NULL;
    const char* formatOpt = NULL;
    List* copyOpts = NIL;
    ListCell* cell = NULL;
    bool hasCsvOpt = false;
    foreach(cell, options) {
        DefElem* def = (DefElem*)lfirst(cell);
        const char* name = def->defname;
        if (strcmp(name, "path") == 0 || strcmp(name, "filename") == 0) {
            if (relPath != NULL)
                ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                    errmsg("duplicate or conflicting external file path option \"%s\"", name)));
            relPath = defGetString(def);
        } else if (strcmp(name, "format") == 0) {
            if (formatOpt != NULL)
                ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                    errmsg("duplicate external file option \"format\"")));
            formatOpt = defGetString(def);
            copyOpts = lappend(copyOpts, def);
        } else if (strcmp(name, "header") == 0 || strcmp(name, "delimiter") == 0 ||
                   strcmp(name, "quote") == 0 || strcmp(name, "escape") == 0 ||
                   strcmp(name, "null") == 0 || strcmp(name, "encoding") == 0) {
            hasCsvOpt = true;
            copyOpts = lappend(copyOpts, def);
        } else {
            ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                errmsg("invalid option \"%s\" for og_lake_file_fdw foreign table", name)));
        }
    }
    if (relPath == NULL || relPath[0] == '\0')
        ereport(ERROR, (errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
            errmsg("external file foreign table requires option \"path\"")));
    char* normFormat = LakeExternalNormalizeFormat(formatOpt, relPath);
    if (!LakeExternalValidRelativePath(relPath, normFormat))
        ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_DATA),
            errmsg("invalid relative external file path \"%s\" for format \"%s\"", relPath, normFormat)));
    bool isParquet = (strcmp(normFormat, "parquet") == 0);
    bool isJson = (strcmp(normFormat, "json") == 0);
    if (isJson && !LakeJsonFormatVersionEnabled())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("external JSON/JSONL files require extension 0.71")));
    if (isParquet && hasCsvOpt)
        ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
            errmsg("CSV formatting options are not valid for Parquet external file tables")));
    if (isJson && hasCsvOpt)
        ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
            errmsg("CSV formatting options are not valid for JSON external file tables")));
    if (!isParquet && !isJson) {
        if (formatOpt == NULL)
            copyOpts = lappend(copyOpts, makeDefElem("format", (Node*)makeString(normFormat)));
        if (OidIsValid(relid))
            copyOpts = list_concat(copyOpts, LakeExternalAttributeForceNotNull(relid));
        ProcessCopyOptions(NULL, true, copyOpts);
    }

    LakeExternalTableOptions result;
    result.relative_path = pstrdup(relPath);
    result.format = normFormat;
    result.is_parquet = isParquet;
    result.is_json = isJson;
    result.copy_options = copyOpts;
    return result;
}

bool LakeExternalMapSdkType(uint32 sdkType, Oid* pgTypeOut, const char** sqlNameOut)
{
    switch (sdkType) {
        case OG_LAKE_SDK_BOOL:
            *pgTypeOut = BOOLOID;
            *sqlNameOut = "boolean";
            return true;
        case OG_LAKE_SDK_INT16:
            *pgTypeOut = INT2OID;
            *sqlNameOut = "smallint";
            return true;
        case OG_LAKE_SDK_INT32:
            *pgTypeOut = INT4OID;
            *sqlNameOut = "integer";
            return true;
        case OG_LAKE_SDK_INT64:
            *pgTypeOut = INT8OID;
            *sqlNameOut = "bigint";
            return true;
        case OG_LAKE_SDK_FLOAT32:
            *pgTypeOut = FLOAT4OID;
            *sqlNameOut = "real";
            return true;
        case OG_LAKE_SDK_FLOAT64:
            *pgTypeOut = FLOAT8OID;
            *sqlNameOut = "double precision";
            return true;
        case OG_LAKE_SDK_UTF8:
            *pgTypeOut = TEXTOID;
            *sqlNameOut = "text";
            return true;
        case OG_LAKE_SDK_BINARY:
            *pgTypeOut = BYTEAOID;
            *sqlNameOut = "bytea";
            return true;
        case OG_LAKE_SDK_UUID:
            *pgTypeOut = UUIDOID;
            *sqlNameOut = "uuid";
            return true;
        case OG_LAKE_SDK_JSON:
            *pgTypeOut = JSONOID;
            *sqlNameOut = "json";
            return true;
        case OG_LAKE_SDK_DECIMAL128:
            *pgTypeOut = NUMERICOID;
            *sqlNameOut = "numeric";
            return true;
        case OG_LAKE_SDK_DATE32:
            *pgTypeOut = DATEOID;
            *sqlNameOut = "date";
            return true;
        case OG_LAKE_SDK_TIMESTAMP_US:
            *pgTypeOut = TIMESTAMPOID;
            *sqlNameOut = "timestamp without time zone";
            return true;
        case OG_LAKE_SDK_TIMESTAMPTZ_US:
            *pgTypeOut = TIMESTAMPTZOID;
            *sqlNameOut = "timestamp with time zone";
            return true;
        case OG_LAKE_SDK_FLOAT32_LIST:
            *pgTypeOut = VECTOROID;
            *sqlNameOut = "vector";
            return true;
        default:
            return false;
    }
}

static bool LakeExternalTypeCompatible(Oid parquetPgType, Oid targetPgType)
{
    if (parquetPgType == targetPgType)
        return true;
    if (parquetPgType == INT2OID && (targetPgType == INT4OID || targetPgType == INT8OID))
        return true;
    if (parquetPgType == INT4OID && targetPgType == INT8OID)
        return true;
    if (parquetPgType == FLOAT4OID && targetPgType == FLOAT8OID)
        return true;
    if (parquetPgType == TEXTOID && targetPgType == VARCHAROID)
        return true;
    return false;
}

Datum og_lake_file_fdw_validator(PG_FUNCTION_ARGS)
{
    if (!LakeExternalFileVersionEnabled())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake external file wrapper requires installed extension 0.61")));
    List* options = untransformRelOptions(PG_GETARG_DATUM(0));
    Oid catalog = PG_GETARG_OID(1);
    if (catalog == ForeignServerRelationId) {
        if (!superuser())
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("only system administrators may configure external file servers")));
        const char* root = NULL;
        ListCell* cell = NULL;
        foreach(cell, options) {
            DefElem* def = (DefElem*)lfirst(cell);
            if (strcmp(def->defname, "external_root") == 0 && root == NULL)
                root = defGetString(def);
            else
                ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                    errmsg("invalid or duplicate external server option \"%s\"", def->defname)));
        }
        if (root == NULL || root[0] == '\0')
            ereport(ERROR, (errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
                errmsg("external file server requires external_root directory option")));
        (void)LakeExternalValidateRoot(root);
    } else if (catalog == ForeignTableRelationId) {
        (void)LakeExternalParseTableOptions(options, InvalidOid);
    } else if (catalog == AttributeRelationId) {
        ListCell* cell = NULL;
        foreach(cell, options) {
            DefElem* def = (DefElem*)lfirst(cell);
            if (strcmp(def->defname, "force_not_null") != 0)
                ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                    errmsg("invalid external column option \"%s\"", def->defname)));
            (void)defGetBoolean(def);
        }
    } else if (options != NIL) {
        ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
            errmsg("og_lake_file_fdw does not accept options on this object")));
    }
    PG_RETURN_VOID();
}

void LakeExternalBindForeignTable(Oid relationOid, CreateForeignTableStmt* statement)
{
    if (!LakeExternalFileVersionEnabled())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake external file wrapper requires installed extension 0.61")));
    ForeignTable* foreign = GetForeignTable(relationOid);
    ForeignServer* server = GetForeignServer(foreign->serverid);
    if (!LakeExternalTrustedWrapper(server->fdwid))
        return;
    (void)LakeExternalServerRoot(server->serverid);
    LakeExternalTableOptions opts = LakeExternalParseTableOptions(foreign->options, relationOid);

    Relation rel = heap_open(relationOid, AccessShareLock);
    PG_TRY();
    {
        if (statement != NULL && (statement->write_only || statement->part_state != NULL ||
            statement->error_relation != NULL || statement->base.inhRelations != NIL))
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("external file foreign table does not support partitioned, write-only, or error tables")));
        if (rel->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("external file foreign table must be permanent")));
        if (rel->trigdesc != NULL || rel->rd_rules != NULL)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("external file foreign table does not support triggers or rules")));
        if (GetDatabaseEncoding() != PG_UTF8)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("external file foreign tables require a UTF8 database")));
        TupleDesc desc = RelationGetDescr(rel);
        if (desc->natts < 1)
            ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                errmsg("external file foreign table requires at least one column")));
        for (int i = 0; i < desc->natts; ++i) {
            Form_pg_attribute attr = &desc->attrs[i];
            if (attr->attisdropped)
                continue;
            if (opts.is_parquet) {
                Oid mappedType = attr->atttypid;
                if (mappedType == VARCHAROID) {
                    if (attr->atttypmod != -1 && attr->atttypmod < VARHDRSZ)
                        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("unsupported varchar typmod on external Parquet column \"%s\"",
                                NameStr(attr->attname))));
                } else {
                    (void)LakeNativeTypeByOid(mappedType);
                    if (mappedType == NUMERICOID && attr->atttypmod != -1)
                        (void)LakeNumericParameters(attr->atttypmod, NULL, NULL);
                    if (mappedType == VECTOROID)
                        (void)LakeVectorDimension(attr->atttypmod);
                    else if (mappedType != NUMERICOID && attr->atttypmod != -1)
                        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("external Parquet column \"%s\" typmod is not supported",
                                NameStr(attr->attname))));
                }
            }
        }
    }
    PG_CATCH();
    {
        heap_close(rel, NoLock);
        PG_RE_THROW();
    }
    PG_END_TRY();
    heap_close(rel, NoLock);
}

static List* LakeExternalCollectUsedAttnums(RelOptInfo* baserel, Relation rel)
{
    TupleDesc desc = RelationGetDescr(rel);
    Bitmapset* used = NULL;
    pull_varattnos((Node*)baserel->reltarget->exprs, baserel->relid, &used);
    ListCell* lc = NULL;
    foreach(lc, baserel->baserestrictinfo) {
        RestrictInfo* rinfo = (RestrictInfo*)lfirst(lc);
        pull_varattnos((Node*)rinfo->clause, baserel->relid, &used);
    }
    bool needAll = bms_is_member(InvalidAttrNumber - FirstLowInvalidHeapAttributeNumber, used);
    if (!needAll && baserel->attr_needed != NULL &&
        !bms_is_empty(baserel->attr_needed[InvalidAttrNumber - baserel->min_attr]))
        needAll = true;
    List* attnums = NIL;
    for (int i = 0; i < desc->natts; ++i) {
        if (desc->attrs[i].attisdropped)
            continue;
        AttrNumber attnum = desc->attrs[i].attnum;
        bool needed = needAll || bms_is_member(attnum - FirstLowInvalidHeapAttributeNumber, used);
        if (!needed && baserel->attr_needed != NULL && attnum <= baserel->max_attr)
            needed = !bms_is_empty(baserel->attr_needed[attnum - baserel->min_attr]);
        if (needed)
            attnums = lappend_int(attnums, (int)attnum);
    }
    bms_free(used);
    return attnums;
}

OgLakeSdkFooter* LakeExternalOpenFooter(const LakeExternalFdSdk* sdk, int fd)
{
    OgLakeSdkLimits limits = LakeFileLimits();
    OgLakeSdkError err;
    OgLakeSdkFooter* raw = NULL;
    HOLD_INTERRUPTS();
    int st = sdk->fd->footer_open_fd(fd, &limits, &raw, &err);
    RESUME_INTERRUPTS();
    LakeFileCheckStatus(st, &err);
    return raw;
}

void LakeExternalCloseFooter(const LakeExternalFdSdk* sdk, OgLakeSdkFooter* volatile* slot) noexcept
{
    if (sdk == NULL || slot == NULL)
        return;
    OgLakeSdkFooter* raw = *slot;
    *slot = NULL;
    if (raw != NULL) {
        HOLD_INTERRUPTS();
        sdk->base->footer_close(&raw);
        RESUME_INTERRUPTS();
    }
}

static void LakeExternalGetForeignRelSize(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid)
{
    if ((Index)root->parse->resultRelation == baserel->relid)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("external file foreign tables are read-only sources")));
    ListCell* rmLc = NULL;
    foreach(rmLc, root->parse->rowMarks) {
        RowMarkClause* rc = (RowMarkClause*)lfirst(rmLc);
        if (rc != NULL && rc->rti == baserel->relid)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("external file foreign tables are read-only sources without row locks")));
    }
    for (AttrNumber attr = baserel->min_attr; attr < 0; ++attr) {
        if (baserel->attr_needed != NULL && !bms_is_empty(baserel->attr_needed[attr - baserel->min_attr]))
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("external file foreign tables do not expose heap system columns")));
    }

    ForeignTable* foreign = GetForeignTable(foreigntableid);
    char* extRoot = LakeExternalServerRoot(foreign->serverid);
    LakeExternalTableOptions opts = LakeExternalParseTableOptions(foreign->options, foreigntableid);
    Relation rel = heap_open(foreigntableid, AccessShareLock);
    List* projected = LakeExternalCollectUsedAttnums(baserel, rel);
    heap_close(rel, NoLock);

    LakeExternalPlanState* planState = (LakeExternalPlanState*)palloc0(sizeof(LakeExternalPlanState));
    planState->root = extRoot;
    planState->relative_path = opts.relative_path;
    planState->format = opts.format;
    planState->is_parquet = opts.is_parquet;
    planState->is_json = opts.is_json;
    planState->copy_options = opts.copy_options;
    planState->projected_attnums = projected;
    planState->row_groups = -1;

    LakeExternalOpenedFile opened = LakeExternalOpenReadonly(extRoot, opts.relative_path, opts.format);
    OgLakeSdkFooter* volatile footer = NULL;
    const LakeExternalFdSdk* sdk = NULL;
    double estRows = -1.0;
    PG_TRY();
    {
        planState->file_bytes = opened.file_bytes;
        if (opts.is_parquet) {
            sdk = LakeFileExternalFdProvider();
            footer = LakeExternalOpenFooter(sdk, opened.fd);
            const OgLakeSdkFooterInfo* info = NULL;
            HOLD_INTERRUPTS();
            info = sdk->base->footer_info(footer);
            RESUME_INTERRUPTS();
            if (info == NULL)
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("external Parquet footer metadata is unavailable")));
            planState->row_groups = info->num_row_groups;
            estRows = (double)info->num_rows;
            LakeExternalCloseFooter(sdk, &footer);
        }
    }
    PG_CATCH();
    {
        LakeExternalCloseFooter(sdk, &footer);
        LakeExternalCloseOpenedFile(&opened);
        PG_RE_THROW();
    }
    PG_END_TRY();
    LakeExternalCloseOpenedFile(&opened);

    BlockNumber pages = (BlockNumber)((planState->file_bytes + (BLCKSZ - 1)) / BLCKSZ);
    if (pages < 1)
        pages = 1;
    planState->pages = pages;

    double ntuples;
    if (estRows >= 0.0) {
        ntuples = clamp_row_est(estRows);
        baserel->tuples = ntuples;
    } else if (baserel->pages > 0) {
        double density = baserel->tuples / (double)baserel->pages;
        ntuples = clamp_row_est(density * (double)pages);
    } else {
        int tupleWidth = MAXALIGN(baserel->reltarget->width) + MAXALIGN(sizeof(HeapTupleHeaderData));
        if (tupleWidth < 1)
            tupleWidth = 64;
        ntuples = clamp_row_est((double)planState->file_bytes / (double)tupleWidth);
        baserel->tuples = ntuples;
    }
    planState->ntuples = ntuples;
    baserel->rows = clamp_row_est(ntuples * clauselist_selectivity(root, baserel->baserestrictinfo, 0, JOIN_INNER, NULL));
    baserel->fdw_private = (void*)planState;
}

static void LakeExternalGetForeignPaths(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid)
{
    (void)foreigntableid;
    LakeExternalPlanState* planState = (LakeExternalPlanState*)baserel->fdw_private;
    if (IS_STREAM_PLAN && u_sess->opt_cxt.query_dop > 1)
        mark_stream_unsupport();
    Cost startupCost = baserel->baserestrictcost.startup;
    Cost cpuPerTuple = u_sess->attr.attr_sql.cpu_tuple_cost * (planState->is_parquet ? 4.0 : 10.0) +
        baserel->baserestrictcost.per_tuple;
    Cost runCost = u_sess->attr.attr_sql.seq_page_cost * planState->pages + cpuPerTuple * planState->ntuples;
    if (root->parse->is_flt_frame) {
        startupCost += baserel->reltarget->cost.startup;
        runCost += baserel->reltarget->cost.per_tuple * baserel->rows;
    }
    Cost totalCost = startupCost + runCost;
    add_path(root, baserel, (Path*)create_foreignscan_path(root, baserel,
        startupCost, totalCost, NIL, NULL, NULL, NIL, 1));
}

static ForeignScan* LakeExternalGetForeignPlan(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid,
    ForeignPath* best_path, List* tlist, List* scan_clauses, Plan* outer_plan)
{
    (void)root;
    (void)foreigntableid;
    (void)best_path;
    (void)outer_plan;
    LakeExternalPlanState* planState = (LakeExternalPlanState*)baserel->fdw_private;
    scan_clauses = extract_actual_clauses(scan_clauses, false);
    List* priv = lappend(
        list_make4(
            makeString(pstrdup(planState->format)),
            makeString(pstrdup(planState->relative_path)),
            makeString(psprintf("%lld", (long long)planState->file_bytes)),
            makeString(psprintf("%lld", (long long)planState->row_groups))),
        planState->projected_attnums ? (Node*)copyObject(planState->projected_attnums) : (Node*)NIL);
    return make_foreignscan(tlist, scan_clauses, baserel->relid, NIL, priv, NIL, NIL, NULL, EXEC_ON_DATANODES);
}

static void LakeExternalReleaseState(LakeExternalScanState* state) noexcept
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
    if (state->cstate != NULL) {
        CopyState cs = state->cstate;
        state->cstate = NULL;
        EndCopyFrom(cs);
    }
    if (state->json_state != NULL) {
        void* js = state->json_state;
        state->json_state = NULL;
        LakeCopyEndJsonState(js, true);
    }
}

static void LakeExternalOpenJsonReader(LakeExternalScanState* state, Relation rel, MemoryContext ownerCtx,
    bool enforceNotNull)
{
    LakeExternalOpenedFile opened = LakeExternalOpenReadonly(state->root, state->relative_path, "json");
    PG_TRY();
    {
        state->file_bytes = opened.file_bytes;
        state->json_state = LakeCopyBeginJsonInFd(rel, opened.fd, state->projected_attnums,
            ownerCtx, enforceNotNull);
    }
    PG_CATCH();
    {
        LakeExternalCloseOpenedFile(&opened);
        PG_RE_THROW();
    }
    PG_END_TRY();
    LakeExternalCloseOpenedFile(&opened);
}

static void LakeExternalCleanupCallback(std::shared_ptr<void> arg)
{
    LakeExternalReleaseState((LakeExternalScanState*)arg.get());
}

static int32 LakeExternalFindParquetOrdinal(const OgLakeSdkFooterInfo* info, const char* attname)
{
    int32 exactMatch = -1;
    int32 ciMatch = -1;
    int ciCount = 0;
    for (int32 i = 0; i < info->num_fields; ++i) {
        const char* fname = info->fields[i].name;
        if (fname == NULL)
            continue;
        if (strcmp(fname, attname) == 0) {
            if (exactMatch >= 0)
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("external Parquet file has duplicate column name \"%s\"", attname)));
            exactMatch = i;
        } else if (pg_strcasecmp(fname, attname) == 0) {
            ciMatch = i;
            ++ciCount;
        }
    }
    if (exactMatch >= 0)
        return exactMatch;
    if (ciCount == 1)
        return ciMatch;
    if (ciCount > 1)
        ereport(ERROR, (errcode(ERRCODE_AMBIGUOUS_COLUMN),
            errmsg("external Parquet column name \"%s\" matches multiple case-folded columns", attname)));
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
        errmsg("column \"%s\" does not exist in external Parquet file", attname)));
    return -1;
}

LakeExternalParquetColumnMap* LakeExternalBuildColumnMap(const LakeExternalFdSdk* sdk,
    OgLakeSdkFooter* footer, const OgLakeSdkFooterInfo* info, TupleDesc desc, List* targetAttnums,
    int* mappedCountOut)
{
    int count = list_length(targetAttnums);
    LakeExternalParquetColumnMap* colMap = count > 0 ?
        (LakeExternalParquetColumnMap*)palloc0(sizeof(LakeExternalParquetColumnMap) * count) : NULL;
    int idx = 0;
    ListCell* lc = NULL;
    foreach(lc, targetAttnums) {
        AttrNumber attnum = (AttrNumber)lfirst_int(lc);
        if (attnum < 1 || attnum > desc->natts || desc->attrs[attnum - 1].attisdropped)
            ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                errmsg("invalid target attribute %d for Parquet column mapping", attnum)));
        Form_pg_attribute attr = &desc->attrs[attnum - 1];
        int32 ordinal = LakeExternalFindParquetOrdinal(info, NameStr(attr->attname));
        for (int j = 0; j < idx; ++j) {
            if (colMap[j].parquet_ordinal == ordinal)
                ereport(ERROR, (errcode(ERRCODE_DUPLICATE_COLUMN),
                    errmsg("duplicate Parquet column projection for \"%s\"", NameStr(attr->attname))));
        }
        Oid pqPgType = InvalidOid;
        const char* sqlName = NULL;
        if (!LakeExternalMapSdkType(info->fields[ordinal].type, &pqPgType, &sqlName))
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("external Parquet column \"%s\" uses an unsupported physical type",
                    NameStr(attr->attname))));
        if (!LakeExternalTypeCompatible(pqPgType, attr->atttypid))
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("external Parquet column \"%s\" has type %s, incompatible with declared SQL column type",
                    NameStr(attr->attname), sqlName)));

        int32 pqTypmod = -1;
        if (pqPgType == NUMERICOID) {
            if (sdk->decimal == NULL)
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("external Parquet provider lacks decimal parameter ABI")));
            OgLakeSdkDecimalField dec = {};
            dec.struct_size = sizeof(dec);
            OgLakeSdkError err;
            HOLD_INTERRUPTS();
            int dst = sdk->decimal->footer_decimal(footer, ordinal, &dec, &err);
            RESUME_INTERRUPTS();
            LakeFileCheckStatus(dst, &err);
            if (!LakeDecimal128Parameters(dec.precision, dec.scale))
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("external Parquet numeric column \"%s\" precision(%d,%d) exceeds supported decimal128",
                        NameStr(attr->attname), dec.precision, dec.scale)));
            pqTypmod = (int32)(((uint32)dec.precision << 16) | (uint16)dec.scale) + VARHDRSZ;
            if (attr->atttypmod != -1) {
                int p = 0, s = 0;
                LakeNumericParameters(attr->atttypmod, &p, &s);
                if (p != dec.precision || s != dec.scale)
                    ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("external Parquet numeric column \"%s\" decimal(%d,%d) differs from table column numeric(%d,%d)",
                            NameStr(attr->attname), dec.precision, dec.scale, p, s)));
            }
        } else if (pqPgType == VECTOROID) {
            LakeVectorDimension(attr->atttypmod);
            pqTypmod = attr->atttypmod;
        }

        colMap[idx].target_attnum = attnum;
        colMap[idx].parquet_ordinal = ordinal;
        colMap[idx].parquet_pg_type = pqPgType;
        colMap[idx].target_pg_type = attr->atttypid;
        colMap[idx].parquet_typmod = pqTypmod;
        colMap[idx].target_typmod = attr->atttypmod;
        ++idx;
    }
    *mappedCountOut = idx;
    return colMap;
}

static void LakeExternalOpenParquetReader(LakeExternalScanState* state, Relation rel, bool firstOpen)
{
    LakeExternalOpenedFile opened = LakeExternalOpenReadonly(state->root, state->relative_path, "parquet");
    OgLakeSdkFooter* volatile footer = NULL;
    const LakeExternalFdSdk* sdk = state->sdk ? state->sdk : LakeFileExternalFdProvider();
    state->sdk = sdk;

    PG_TRY();
    {
        OgLakeSdkError err;
        if (firstOpen) {
            footer = LakeExternalOpenFooter(sdk, opened.fd);
            const OgLakeSdkFooterInfo* info = NULL;
            HOLD_INTERRUPTS();
            info = sdk->base->footer_info(footer);
            RESUME_INTERRUPTS();
            if (info == NULL || info->object_version == NULL || info->object_version[0] == '\0')
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("external Parquet object version is unavailable")));

            state->file_bytes = opened.file_bytes;
            state->row_groups = info->num_row_groups;
            state->total_rows = info->num_rows;
            state->expected_version = pstrdup(info->object_version);

            TupleDesc desc = RelationGetDescr(rel);
            state->col_map = LakeExternalBuildColumnMap(sdk, footer, info, desc,
                state->projected_attnums, &state->mapped_count);
            LakeExternalCloseFooter(sdk, &footer);
        }

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

        HOLD_INTERRUPTS();
        int rst = sdk->fd->reader_open_fd(opened.fd, state->expected_version, &readOpts, &state->reader, &err);
        RESUME_INTERRUPTS();
        LakeFileCheckStatus(rst, &err);
        if (ordinals != NULL)
            pfree(ordinals);
        state->batch = NULL;
        state->batch_row = 0;
    }
    PG_CATCH();
    {
        LakeExternalCloseFooter(sdk, &footer);
        LakeExternalCloseOpenedFile(&opened);
        PG_RE_THROW();
    }
    PG_END_TRY();
    LakeExternalCloseOpenedFile(&opened);
}

static CopyState LakeExternalOpenCsvCopy(Relation rel, const char* root, const char* relPath,
    const char* format, List* copyOpts)
{
    LakeExternalOpenedFile opened = LakeExternalOpenReadonly(root, relPath, format);
    CopyState volatile cstate = NULL;
    PG_TRY();
    {
        char fdPath[64];
        int len = snprintf(fdPath, sizeof(fdPath), "/proc/self/fd/%d", opened.fd);
        if (len < 0 || len >= (int)sizeof(fdPath))
            ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("failed to format descriptor path")));
        cstate = BeginCopyFrom(rel, fdPath, NIL, copyOpts, NULL, NULL);
    }
    PG_CATCH();
    {
        LakeExternalCloseOpenedFile(&opened);
        PG_RE_THROW();
    }
    PG_END_TRY();
    LakeExternalCloseOpenedFile(&opened);
    return cstate;
}

Datum LakeExternalConvertDatum(Datum value, const LakeExternalParquetColumnMap* map)
{
    if (map->parquet_pg_type == map->target_pg_type) {
        if (map->target_pg_type == NUMERICOID && map->target_typmod == -1 && map->parquet_typmod != -1) {
            /* Value already formatted with exact file typmod; no extra cast needed. */
            return value;
        }
        return value;
    }
    if (map->parquet_pg_type == INT2OID && map->target_pg_type == INT4OID)
        return Int32GetDatum((int32)DatumGetInt16(value));
    if (map->parquet_pg_type == INT2OID && map->target_pg_type == INT8OID)
        return Int64GetDatum((int64)DatumGetInt16(value));
    if (map->parquet_pg_type == INT4OID && map->target_pg_type == INT8OID)
        return Int64GetDatum((int64)DatumGetInt32(value));
    if (map->parquet_pg_type == FLOAT4OID && map->target_pg_type == FLOAT8OID)
        return Float8GetDatum((float8)DatumGetFloat4(value));
    if (map->parquet_pg_type == TEXTOID && map->target_pg_type == VARCHAROID) {
        if (map->target_typmod >= VARHDRSZ)
            return DirectFunctionCall3(varchar, value, Int32GetDatum(map->target_typmod), BoolGetDatum(false));
        return value;
    }
    ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
        errmsg("unsupported external Parquet type widening")));
    return value;
}

static void LakeExternalBeginForeignScan(ForeignScanState* node, int eflags)
{
    node->fdw_state = NULL;
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return;
    if (StreamThreadAmI())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("external file scan requires serial execution")));

    Relation rel = node->ss.ss_currentRelation;
    Oid relid = RelationGetRelid(rel);
    ForeignTable* foreign = GetForeignTable(relid);
    char* extRoot = LakeExternalServerRoot(foreign->serverid);
    LakeExternalTableOptions opts = LakeExternalParseTableOptions(foreign->options, relid);

    ForeignScan* plan = (ForeignScan*)node->ss.ps.plan;
    List* projected = NIL;
    if (plan->fdw_private != NIL && list_length(plan->fdw_private) == 5)
        projected = (List*)list_nth(plan->fdw_private, 4);
    else {
        TupleDesc desc = RelationGetDescr(rel);
        for (int i = 0; i < desc->natts; ++i)
            if (!desc->attrs[i].attisdropped)
                projected = lappend_int(projected, desc->attrs[i].attnum);
    }

    EState* estate = node->ss.ps.state;
    MemoryContext prev = MemoryContextSwitchTo(estate->es_query_cxt);
    LakeExternalScanState* state = (LakeExternalScanState*)palloc0(sizeof(LakeExternalScanState));
    state->is_parquet = opts.is_parquet;
    state->is_json = opts.is_json;
    state->root = extRoot;
    state->relative_path = opts.relative_path;
    state->format = opts.format;
    state->copy_options = opts.copy_options;
    state->projected_attnums = projected;
    state->row_context = AllocSetContextCreate(estate->es_query_cxt,
        "Lake external scan row", ALLOCSET_DEFAULT_SIZES);

    /* COPY's transaction-owned file and query memory are released by the host
     * on abort. JSON owns its own reset callback. Only the Parquet SDK handles
     * need an FDW callback after the query's child contexts have been deleted. */
    if (state->is_parquet) {
        new (&state->cleanup_cb) MemoryContextCallback();
        state->cleanup_cb.func = LakeExternalCleanupCallback;
        state->cleanup_cb.arg = std::shared_ptr<void>(std::shared_ptr<void>(), state);
        MemoryContextRegisterResetCallback(estate->es_query_cxt, &state->cleanup_cb);
    }
    node->fdw_state = (void*)state;

    PG_TRY();
    {
        if (state->is_parquet)
            LakeExternalOpenParquetReader(state, rel, true);
        else if (state->is_json)
            LakeExternalOpenJsonReader(state, rel, estate->es_query_cxt, true);
        else
            state->cstate = LakeExternalOpenCsvCopy(rel, state->root, state->relative_path,
                state->format, state->copy_options);
    }
    PG_CATCH();
    {
        LakeExternalReleaseState(state);
        MemoryContextSwitchTo(prev);
        PG_RE_THROW();
    }
    PG_END_TRY();
    MemoryContextSwitchTo(prev);
}

static TupleTableSlot* LakeExternalIterateForeignScan(ForeignScanState* node)
{
    LakeExternalScanState* state = (LakeExternalScanState*)node->fdw_state;
    TupleTableSlot* slot = node->ss.ss_ScanTupleSlot;
    ExecClearTuple(slot);

    if (state->is_json) {
        if (LakeCopyNextJsonRow(state->json_state, slot->tts_values, slot->tts_isnull))
            ExecStoreVirtualTuple(slot);
        return slot;
    }

    if (!state->is_parquet) {
        ErrorContextCallback errctx;
        errctx.callback = CopyFromErrorCallback;
        errctx.arg = (void*)state->cstate;
        errctx.previous = t_thrd.log_cxt.error_context_stack;
        t_thrd.log_cxt.error_context_stack = &errctx;
        bool found = NextCopyFrom(state->cstate, NULL, slot->tts_values, slot->tts_isnull, NULL);
        t_thrd.log_cxt.error_context_stack = errctx.previous;
        if (found)
            ExecStoreVirtualTuple(slot);
        return slot;
    }

    CHECK_FOR_INTERRUPTS();
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
            return slot;
        }
        LakeFileCheckStatus(st, &err);
        CHECK_FOR_INTERRUPTS();
        OgLakeSdkBatch* batch = LakeFileCheckArrowBatch(state->batch, state->mapped_count);
        if (batch->object_version == NULL || strcmp(batch->object_version, state->expected_version) != 0)
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("external Parquet file changed concurrently during scan")));
        state->batch_row = 0;
    }

    TupleDesc desc = slot->tts_tupleDescriptor;
    for (int i = 0; i < desc->natts; ++i) {
        slot->tts_values[i] = (Datum)0;
        slot->tts_isnull[i] = true;
    }
    MemoryContextReset(state->row_context);
    MemoryContext oldCtx = MemoryContextSwitchTo(state->row_context);
    int64 row = state->batch_row++;
    PG_TRY();
    {
        for (int i = 0; i < state->mapped_count; ++i) {
            const LakeExternalParquetColumnMap* m = &state->col_map[i];
            int idx = m->target_attnum - 1;
            const ArrowArray* arr = state->batch->array->children[i];
            const ArrowSchema* sch = state->batch->schema->children[i];
            if (LakeFileDecodeNull(arr, row)) {
                if (desc->attrs[idx].attnotnull)
                    ereport(ERROR, (errcode(ERRCODE_NOT_NULL_VIOLATION),
                        errmsg("null value in column \"%s\" violates not-null constraint",
                            NameStr(desc->attrs[idx].attname))));
                slot->tts_isnull[idx] = true;
                slot->tts_values[idx] = (Datum)0;
            } else {
                Datum raw = (m->parquet_pg_type == VECTOROID) ?
                    LakeVectorReadArrow(arr, sch, row, m->parquet_typmod) :
                    LakeFileDecodeValue(arr, sch, row, m->parquet_pg_type, NULL, m->parquet_typmod);
                slot->tts_isnull[idx] = false;
                slot->tts_values[idx] = LakeExternalConvertDatum(raw, m);
            }
        }
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(oldCtx);
        PG_RE_THROW();
    }
    PG_END_TRY();
    MemoryContextSwitchTo(oldCtx);
    ExecStoreVirtualTuple(slot);
    return slot;
}

static void LakeExternalReScanForeignScan(ForeignScanState* node)
{
    LakeExternalScanState* state = (LakeExternalScanState*)node->fdw_state;
    ExecClearTuple(node->ss.ss_ScanTupleSlot);
    if (state == NULL)
        return;
    LakeExternalReleaseState(state);
    if (state->is_parquet)
        LakeExternalOpenParquetReader(state, node->ss.ss_currentRelation, false);
    else if (state->is_json)
        LakeExternalOpenJsonReader(state, node->ss.ss_currentRelation, node->ss.ps.state->es_query_cxt, true);
    else {
        MemoryContext previous = MemoryContextSwitchTo(node->ss.ps.state->es_query_cxt);
        state->cstate = LakeExternalOpenCsvCopy(node->ss.ss_currentRelation, state->root,
            state->relative_path, state->format, state->copy_options);
        MemoryContextSwitchTo(previous);
    }
}

static void LakeExternalEndForeignScan(ForeignScanState* node)
{
    LakeExternalScanState* state = (LakeExternalScanState*)node->fdw_state;
    if (state != NULL) {
        LakeExternalReleaseState(state);
        node->fdw_state = NULL;
    }
}

static void LakeExternalExplainForeignScan(ForeignScanState* node, ExplainState* es)
{
    ForeignTable* foreign = GetForeignTable(RelationGetRelid(node->ss.ss_currentRelation));
    LakeExternalTableOptions opts = LakeExternalParseTableOptions(foreign->options,
        RelationGetRelid(node->ss.ss_currentRelation));
    ExplainPropertyText("Foreign Format", opts.format, es);
    ExplainPropertyText("Foreign File", opts.relative_path, es);
    ForeignScan* plan = (ForeignScan*)node->ss.ps.plan;
    if (es->costs && plan->fdw_private != NIL && list_length(plan->fdw_private) == 5) {
        int64 bytes = strtoll(strVal(list_nth(plan->fdw_private, 2)), NULL, 10);
        int64 rowGroups = strtoll(strVal(list_nth(plan->fdw_private, 3)), NULL, 10);
        ExplainPropertyLong("Foreign File Size", (long)bytes, es);
        if (opts.is_parquet && rowGroups >= 0)
            ExplainPropertyLong("External Parquet Row Groups", (long)rowGroups, es);
    }
}

/* One reservoir for all formats. Parquet can skip decoding a row that was
 * not selected, while CSV and JSON have to parse it to reach the next row. */
struct LakeExternalReservoir {
    LakeAnalyzeSample* samples;
    int target;
    int count;
    uint64 seen;
    double skip;
    double random_state;
};

static int LakeExternalSampleSlot(LakeExternalReservoir* reservoir)
{
    uint64 ordinal = reservoir->seen++;
    if (reservoir->count < reservoir->target)
        return reservoir->count++;
    if (reservoir->skip < 0)
        reservoir->skip = anl_get_next_S((double)ordinal, reservoir->target, &reservoir->random_state);
    if (reservoir->skip-- > 0)
        return -1;
    return (int)(reservoir->target * anl_random_fract());
}

static void LakeExternalStoreSample(LakeExternalReservoir* reservoir, int slot,
    TupleDesc desc, Datum* values, bool* nulls)
{
    if (slot < 0)
        return;
    LakeAnalyzeSample* sample = &reservoir->samples[slot];
    HeapTuple tuple = heap_form_tuple(desc, values, nulls);
    if (sample->tuple != NULL)
        heap_freetuple(sample->tuple);
    sample->tuple = tuple;
    sample->ordinal = reservoir->seen - 1;
}

static int LakeExternalAcquireSampleRows(Relation onerel, int elevel, HeapTuple* rows, int targrows,
    double* totalrows, double* totaldeadrows, void* additionalData, bool estimate_table_rownum)
{
    (void)additionalData;
    (void)estimate_table_rownum;
    TupleDesc tupDesc = RelationGetDescr(onerel);
    Oid relid = RelationGetRelid(onerel);
    ForeignTable* foreign = GetForeignTable(relid);
    char* extRoot = LakeExternalServerRoot(foreign->serverid);
    LakeExternalTableOptions opts = LakeExternalParseTableOptions(foreign->options, relid);

    if (targrows <= 0 || (Size)targrows > MaxAllocSize / sizeof(LakeAnalyzeSample))
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("invalid external ANALYZE sample size")));
    LakeExternalReservoir reservoir = {(LakeAnalyzeSample*)palloc0(targrows * sizeof(LakeAnalyzeSample)),
        targrows, 0, 0, -1, anl_init_selection_state(targrows)};
    *totalrows = 0.0;
    *totaldeadrows = 0.0;

    Datum* values = (Datum*)palloc0(tupDesc->natts * sizeof(Datum));
    bool* nulls = (bool*)palloc0(tupDesc->natts * sizeof(bool));
    MemoryContext oldCtx = CurrentMemoryContext;
    MemoryContext tupCtx = AllocSetContextCreate(oldCtx,
        "Lake external ANALYZE tuple", ALLOCSET_DEFAULT_SIZES);

    if (opts.is_json) {
        LakeExternalScanState state = {};
        state.is_json = true;
        state.root = extRoot;
        state.relative_path = opts.relative_path;
        state.format = opts.format;
        for (int i = 0; i < tupDesc->natts; ++i)
            if (!tupDesc->attrs[i].attisdropped)
                state.projected_attnums = lappend_int(state.projected_attnums, tupDesc->attrs[i].attnum);

        PG_TRY();
        {
            LakeExternalOpenJsonReader(&state, onerel, oldCtx, false);
            for (;;) {
                vacuum_delay_point();
                MemoryContextReset(tupCtx);
                MemoryContextSwitchTo(tupCtx);
                bool found = LakeCopyNextJsonRow(state.json_state, values, nulls);
                MemoryContextSwitchTo(oldCtx);
                if (!found)
                    break;
                int slot = LakeExternalSampleSlot(&reservoir);
                LakeExternalStoreSample(&reservoir, slot, tupDesc, values, nulls);
            }
        }
        PG_CATCH();
        {
            MemoryContextSwitchTo(oldCtx);
            LakeExternalReleaseState(&state);
            MemoryContextDelete(tupCtx);
            PG_RE_THROW();
        }
        PG_END_TRY();
        LakeExternalReleaseState(&state);
    } else if (!opts.is_parquet) {
        CopyState volatile cstate = LakeExternalOpenCsvCopy(onerel, extRoot, opts.relative_path,
            opts.format, opts.copy_options);
        ErrorContextCallback errctx;
        errctx.callback = CopyFromErrorCallback;
        errctx.arg = (void*)cstate;
        errctx.previous = t_thrd.log_cxt.error_context_stack;
        t_thrd.log_cxt.error_context_stack = &errctx;
        PG_TRY();
        {
            for (;;) {
                vacuum_delay_point();
                MemoryContextReset(tupCtx);
                MemoryContextSwitchTo(tupCtx);
                bool found = NextCopyFrom(cstate, NULL, values, nulls, NULL);
                MemoryContextSwitchTo(oldCtx);
                if (!found)
                    break;
                int slot = LakeExternalSampleSlot(&reservoir);
                LakeExternalStoreSample(&reservoir, slot, tupDesc, values, nulls);
            }
        }
        PG_CATCH();
        {
            t_thrd.log_cxt.error_context_stack = errctx.previous;
            MemoryContextSwitchTo(oldCtx);
            EndCopyFrom(cstate);
            MemoryContextDelete(tupCtx);
            PG_RE_THROW();
        }
        PG_END_TRY();
        t_thrd.log_cxt.error_context_stack = errctx.previous;
        EndCopyFrom(cstate);
    } else {
        LakeExternalScanState state = {};
        state.is_parquet = true;
        state.root = extRoot;
        state.relative_path = opts.relative_path;
        state.format = opts.format;
        state.sdk = LakeFileExternalFdProvider();
        for (int i = 0; i < tupDesc->natts; ++i)
            if (!tupDesc->attrs[i].attisdropped)
                state.projected_attnums = lappend_int(state.projected_attnums, tupDesc->attrs[i].attnum);

        PG_TRY();
        {
            LakeExternalOpenParquetReader(&state, onerel, true);
            for (;;) {
                vacuum_delay_point();
                while (state.batch == NULL || state.batch_row >= state.batch->num_rows) {
                    if (state.batch != NULL) {
                        HOLD_INTERRUPTS();
                        state.sdk->base->batch_release(&state.batch);
                        RESUME_INTERRUPTS();
                        state.batch_row = 0;
                    }
                    OgLakeSdkError err;
                    HOLD_INTERRUPTS();
                    int st = state.sdk->base->reader_next(state.reader, &state.batch, &err);
                    RESUME_INTERRUPTS();
                    if (st == OG_LAKE_SDK_EOF) {
                        state.batch = NULL;
                        break;
                    }
                    LakeFileCheckStatus(st, &err);
                    (void)LakeFileCheckArrowBatch(state.batch, state.mapped_count);
                    state.batch_row = 0;
                }
                if (state.batch == NULL)
                    break;
                int64 row = state.batch_row++;
                int sampleSlot = LakeExternalSampleSlot(&reservoir);
                if (sampleSlot >= 0) {
                    MemoryContextReset(tupCtx);
                    MemoryContextSwitchTo(tupCtx);
                    for (int i = 0; i < tupDesc->natts; ++i) {
                        values[i] = (Datum)0;
                        nulls[i] = true;
                    }
                    for (int i = 0; i < state.mapped_count; ++i) {
                        const LakeExternalParquetColumnMap* m = &state.col_map[i];
                        int idx = m->target_attnum - 1;
                        const ArrowArray* arr = state.batch->array->children[i];
                        const ArrowSchema* sch = state.batch->schema->children[i];
                        if (!LakeFileDecodeNull(arr, row)) {
                            Datum raw = (m->parquet_pg_type == VECTOROID) ?
                                LakeVectorReadArrow(arr, sch, row, m->parquet_typmod) :
                                LakeFileDecodeValue(arr, sch, row, m->parquet_pg_type, NULL, m->parquet_typmod);
                            nulls[idx] = false;
                            values[idx] = LakeExternalConvertDatum(raw, m);
                        }
                    }
                    MemoryContextSwitchTo(oldCtx);
                    LakeExternalStoreSample(&reservoir, sampleSlot, tupDesc, values, nulls);
                }
            }
        }
        PG_CATCH();
        {
            MemoryContextSwitchTo(oldCtx);
            LakeExternalReleaseState(&state);
            MemoryContextDelete(tupCtx);
            PG_RE_THROW();
        }
        PG_END_TRY();
        LakeExternalReleaseState(&state);
    }

    MemoryContextDelete(tupCtx);
    int numrows = reservoir.count;
    qsort(reservoir.samples, numrows, sizeof(LakeAnalyzeSample), LakeCompareSampleOrdinal);
    for (int i = 0; i < numrows; ++i)
        rows[i] = reservoir.samples[i].tuple;
    *totalrows = (double)reservoir.seen;
    pfree(reservoir.samples);
    pfree(values);
    pfree(nulls);
    ereport(elevel, (errmsg("\"%s\": file contains %.0f rows; %d rows in sample",
        RelationGetRelationName(onerel), *totalrows, numrows)));
    return numrows;
}

static bool LakeExternalAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc* func,
    BlockNumber* totalpages, void* additionalData, bool estimate_table_rownum)
{
    (void)additionalData;
    (void)estimate_table_rownum;
    ForeignTable* foreign = GetForeignTable(RelationGetRelid(relation));
    char* extRoot = LakeExternalServerRoot(foreign->serverid);
    LakeExternalTableOptions opts = LakeExternalParseTableOptions(foreign->options, RelationGetRelid(relation));
    LakeExternalOpenedFile opened = LakeExternalOpenReadonly(extRoot, opts.relative_path, opts.format);
    BlockNumber pages = (BlockNumber)((opened.file_bytes + (BLCKSZ - 1)) / BLCKSZ);
    LakeExternalCloseOpenedFile(&opened);
    if (pages < 1)
        pages = 1;
    *totalpages = pages;
    *func = LakeExternalAcquireSampleRows;
    return true;
}

static int LakeExternalIsForeignRelUpdatable(Relation rel)
{
    (void)rel;
    return 0;
}

Datum og_lake_file_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine* routine = makeNode(FdwRoutine);
    routine->GetForeignRelSize = LakeExternalGetForeignRelSize;
    routine->GetForeignPaths = LakeExternalGetForeignPaths;
    routine->GetForeignPlan = LakeExternalGetForeignPlan;
    routine->BeginForeignScan = LakeExternalBeginForeignScan;
    routine->IterateForeignScan = LakeExternalIterateForeignScan;
    routine->ReScanForeignScan = LakeExternalReScanForeignScan;
    routine->EndForeignScan = LakeExternalEndForeignScan;
    routine->ExplainForeignScan = LakeExternalExplainForeignScan;
    routine->AnalyzeForeignTable = LakeExternalAnalyzeForeignTable;
    routine->AcquireSampleRows = LakeExternalAcquireSampleRows;
    routine->IsForeignRelUpdatable = LakeExternalIsForeignRelUpdatable;
    PG_RETURN_POINTER(routine);
}

/* CSV Schema Inference Helpers */
enum LakeCsvColKind {
    LAKE_CSV_UNKNOWN = 0,
    LAKE_CSV_BOOL,
    LAKE_CSV_INT8,
    LAKE_CSV_FLOAT8,
    LAKE_CSV_DATE,
    LAKE_CSV_TEXT
};

struct LakeCsvColInfer {
    char* name;
    LakeCsvColKind kind;
    bool nullable;
};

static bool LakeCsvIsBoolToken(const char* tok)
{
    return pg_strcasecmp(tok, "true") == 0 || pg_strcasecmp(tok, "false") == 0;
}

static bool LakeCsvIsInt64Token(const char* tok)
{
    if (*tok == '\0')
        return false;
    const char* p = tok;
    if (*p == '+' || *p == '-')
        ++p;
    if (*p == '\0')
        return false;
    if (*p == '0' && p[1] != '\0')
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

static bool LakeCsvIsFloat8Token(const char* tok)
{
    if (*tok == '\0')
        return false;
    errno = 0;
    char* end = NULL;
    double val = strtod(tok, &end);
    if (errno == ERANGE || end == NULL || end == tok || *end != '\0')
        return false;
    return isfinite(val) != 0;
}

static bool LakeCsvIsDateToken(const char* tok)
{
    if (strlen(tok) != 10 || tok[4] != '-' || tok[7] != '-')
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
    return y >= 1 && m >= 1 && m <= 12 && d >= 1 && d <= day_tab[isleap(y)][m - 1];
}

static LakeCsvColKind LakeCsvMergeKind(LakeCsvColKind existing, const char* tok)
{
    if (existing == LAKE_CSV_TEXT)
        return LAKE_CSV_TEXT;
    LakeCsvColKind sample = LAKE_CSV_TEXT;
    if (LakeCsvIsBoolToken(tok))
        sample = LAKE_CSV_BOOL;
    else if (LakeCsvIsInt64Token(tok))
        sample = LAKE_CSV_INT8;
    else if (LakeCsvIsFloat8Token(tok))
        sample = LAKE_CSV_FLOAT8;
    else if (LakeCsvIsDateToken(tok))
        sample = LAKE_CSV_DATE;
    if (existing == LAKE_CSV_UNKNOWN)
        return sample;
    if (existing == sample)
        return existing;
    if ((existing == LAKE_CSV_INT8 && sample == LAKE_CSV_FLOAT8) ||
        (existing == LAKE_CSV_FLOAT8 && sample == LAKE_CSV_INT8))
        return LAKE_CSV_FLOAT8;
    return LAKE_CSV_TEXT;
}

static const char* LakeCsvKindName(LakeCsvColKind kind)
{
    switch (kind) {
        case LAKE_CSV_BOOL:
            return "boolean";
        case LAKE_CSV_INT8:
            return "bigint";
        case LAKE_CSV_FLOAT8:
            return "double precision";
        case LAKE_CSV_DATE:
            return "date";
        case LAKE_CSV_UNKNOWN:
        case LAKE_CSV_TEXT:
        default:
            return "text";
    }
}

/* Return complete logical records only. The byte budget may end inside a
 * quoted field, a doubled quote, or a multibyte character. */
static List* LakeCsvSampleRecords(const char* buf, Size len, bool atEof, char delim)
{
    List* records = NIL;
    List* fields = NIL;
    StringInfoData field;
    initStringInfo(&field);
    bool quoted = false;
    Size start = 0;
    for (Size i = 0; i < len && list_length(records) < 101; ++i) {
        char c = buf[i];
        if (c == '"') {
            if (quoted && i + 1 < len && buf[i + 1] == '"') {
                appendStringInfoChar(&field, '"');
                ++i;
            } else {
                quoted = !quoted;
            }
        } else if (!quoted && c == delim) {
            fields = lappend(fields, pstrdup(field.data));
            resetStringInfo(&field);
        } else if (!quoted && (c == '\n' || c == '\r')) {
            pg_verify_mbstr(PG_UTF8, buf + start, i - start, false);
            fields = lappend(fields, pstrdup(field.data));
            records = lappend(records, fields);
            fields = NIL;
            resetStringInfo(&field);
            if (c == '\r' && i + 1 < len && buf[i + 1] == '\n')
                ++i;
            start = i + 1;
        } else {
            appendStringInfoChar(&field, c);
        }
    }
    if (atEof && start < len && list_length(records) < 101) {
        if (quoted)
            ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT), errmsg("unterminated CSV quoted field")));
        pg_verify_mbstr(PG_UTF8, buf + start, len - start, false);
        records = lappend(records, lappend(fields, pstrdup(field.data)));
    }
    pfree(field.data);
    if (records == NIL)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            errmsg("CSV record exceeds schema inspection buffer")));
    return records;
}

static void LakeDescribeParquetIntoStore(const LakeExternalFdSdk* sdk, int fd, Tuplestorestate* store, TupleDesc outDesc)
{
    OgLakeSdkFooter* volatile footer = NULL;
    PG_TRY();
    {
        OgLakeSdkError err;
        footer = LakeExternalOpenFooter(sdk, fd);
        const OgLakeSdkFooterInfo* info = NULL;
        HOLD_INTERRUPTS();
        info = sdk->base->footer_info(footer);
        RESUME_INTERRUPTS();
        if (info == NULL || info->num_fields <= 0)
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("external Parquet footer has no columns")));
        for (int32 i = 0; i < info->num_fields; ++i) {
            Oid pgType = InvalidOid;
            const char* sqlType = NULL;
            if (!LakeExternalMapSdkType(info->fields[i].type, &pgType, &sqlType))
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("external Parquet column \"%s\" uses an unsupported physical type",
                        info->fields[i].name ? info->fields[i].name : "?")));
            const char* formatted = sqlType;
            if (pgType == NUMERICOID) {
                if (sdk->decimal == NULL)
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("external Parquet decimal inspection requires decimal parameter ABI")));
                OgLakeSdkDecimalField dec = {};
                dec.struct_size = sizeof(dec);
                HOLD_INTERRUPTS();
                int dst = sdk->decimal->footer_decimal(footer, i, &dec, &err);
                RESUME_INTERRUPTS();
                LakeFileCheckStatus(dst, &err);
                formatted = psprintf("numeric(%d,%d)", dec.precision, dec.scale);
            }
            Datum vals[4];
            bool nulls[4] = {false, false, false, false};
            vals[0] = CStringGetTextDatum(info->fields[i].name ? info->fields[i].name : psprintf("column%d", i));
            vals[1] = CStringGetTextDatum(formatted);
            vals[2] = BoolGetDatum(info->fields[i].nullable != 0);
            vals[3] = Int32GetDatum(i + 1);
            tuplestore_putvalues(store, outDesc, vals, nulls);
        }
        LakeExternalCloseFooter(sdk, &footer);
    }
    PG_CATCH();
    {
        LakeExternalCloseFooter(sdk, &footer);
        PG_RE_THROW();
    }
    PG_END_TRY();
}

static void LakeDescribeCsvIntoStore(int fd, bool header, char delim, Tuplestorestate* store, TupleDesc outDesc)
{
    static const Size LAKE_CSV_SNIFF_BYTES = 65536;
    char* buf = (char*)palloc(LAKE_CSV_SNIFF_BYTES + 1);
    Size total = 0;
    while (total <= LAKE_CSV_SNIFF_BYTES) {
        ssize_t n = pread(fd, buf + total, LAKE_CSV_SNIFF_BYTES + 1 - total, (off_t)total);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            ereport(ERROR, (errcode_for_file_access(), errmsg("failed reading external CSV file for schema inference: %m")));
        }
        if (n == 0)
            break;
        total += (Size)n;
    }
    if (total == 0)
        ereport(ERROR, (errcode(ERRCODE_NO_DATA), errmsg("cannot infer schema from empty external CSV file")));

    List* records = LakeCsvSampleRecords(buf, Min(total, LAKE_CSV_SNIFF_BYTES),
        total <= LAKE_CSV_SNIFF_BYTES, delim);
    List* firstFields = (List*)linitial(records);
    int ncols = list_length(firstFields);
    if (ncols <= 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("external CSV file has zero columns")));

    LakeCsvColInfer* cols = (LakeCsvColInfer*)palloc0(sizeof(LakeCsvColInfer) * ncols);
    for (int i = 0; i < ncols; ++i) {
        cols[i].kind = LAKE_CSV_UNKNOWN;
        cols[i].nullable = false;
        if (header) {
            const char* raw = (const char*)list_nth(firstFields, i);
            while (*raw == ' ' || *raw == '\t')
                ++raw;
            char* trimmed = pstrdup(raw);
            Size tlen = strlen(trimmed);
            while (tlen > 0 && (trimmed[tlen - 1] == ' ' || trimmed[tlen - 1] == '\t'))
                trimmed[--tlen] = '\0';
            cols[i].name = trimmed[0] != '\0' ? trimmed : psprintf("column%d", i);
        } else {
            cols[i].name = psprintf("column%d", i);
        }
    }

    int dataIdx = 0;
    ListCell* lc = NULL;
    foreach(lc, records) {
        if (header && dataIdx == 0) {
            ++dataIdx;
            continue;
        }
        List* rowFields = (List*)lfirst(lc);
        if (list_length(rowFields) != ncols)
            ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
                errmsg("CSV row %d has %d columns, expected %d", dataIdx + 1, list_length(rowFields), ncols)));
        for (int c = 0; c < ncols; ++c) {
            const char* val = (const char*)list_nth(rowFields, c);
            if (val[0] == '\0')
                cols[c].nullable = true;
            else
                cols[c].kind = LakeCsvMergeKind(cols[c].kind, val);
        }
        ++dataIdx;
    }

    int dataRows = header ? (list_length(records) - 1) : list_length(records);
    for (int i = 0; i < ncols; ++i) {
        if (dataRows == 0 || cols[i].kind == LAKE_CSV_UNKNOWN)
            cols[i].nullable = true;
        Datum vals[4];
        bool nulls[4] = {false, false, false, false};
        vals[0] = CStringGetTextDatum(cols[i].name);
        vals[1] = CStringGetTextDatum(LakeCsvKindName(cols[i].kind));
        vals[2] = BoolGetDatum(true); /* CSV columns are inferred nullable unless explicit DDL specifies NOT NULL */
        vals[3] = Int32GetDatum(i + 1);
        tuplestore_putvalues(store, outDesc, vals, nulls);
    }
}

static Datum LakeDescribeExternalCommon(FunctionCallInfo fcinfo, const char* root,
    const char* relPath, const char* formatOpt, bool header, char delim)
{
    if (!LakeExternalFileVersionEnabled())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("external file schema inference requires installed extension 0.61")));
    ReturnSetInfo* result = (ReturnSetInfo*)fcinfo->resultinfo;
    if (!result || !IsA(result, ReturnSetInfo) || !(result->allowedModes & SFRM_Materialize) || !result->econtext)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("describe_external_file requires materialize mode")));

    char* format = LakeExternalNormalizeFormat(formatOpt, relPath);
    MemoryContext callerCtx = MemoryContextSwitchTo(result->econtext->ecxt_per_query_memory);
    TupleDesc outDesc = CreateTemplateTupleDesc(4, false);
    TupleDescInitEntry(outDesc, 1, "column_name", TEXTOID, -1, DEFAULT_COLLATION_OID);
    TupleDescInitEntry(outDesc, 2, "column_type", TEXTOID, -1, DEFAULT_COLLATION_OID);
    TupleDescInitEntry(outDesc, 3, "nullable", BOOLOID, -1, 0);
    TupleDescInitEntry(outDesc, 4, "physical_ordinal", INT4OID, -1, 0);
    outDesc = BlessTupleDesc(outDesc);
    Tuplestorestate* store = tuplestore_begin_heap(true, false,
        Max(64, Min(u_sess->attr.attr_memory.work_mem, 1024)));
    MemoryContextSwitchTo(callerCtx);

    LakeExternalOpenedFile opened = LakeExternalOpenReadonly(root, relPath, format);
    PG_TRY();
    {
        if (strcmp(format, "parquet") == 0) {
            const LakeExternalFdSdk* sdk = LakeFileExternalFdProvider();
            LakeDescribeParquetIntoStore(sdk, opened.fd, store, outDesc);
        } else if (strcmp(format, "json") == 0) {
            LakeJsonInspectFd(opened.fd, store, outDesc);
        } else {
            LakeDescribeCsvIntoStore(opened.fd, header, delim, store, outDesc);
        }
    }
    PG_CATCH();
    {
        LakeExternalCloseOpenedFile(&opened);
        PG_RE_THROW();
    }
    PG_END_TRY();
    LakeExternalCloseOpenedFile(&opened);

    result->returnMode = SFRM_Materialize;
    result->setResult = store;
    result->setDesc = outDesc;
    return (Datum)0;
}

static char LakeParseDelimArg(FunctionCallInfo fcinfo, int argno, const char* fallback)
{
    const char* s = (PG_NARGS() <= argno || PG_ARGISNULL(argno)) ? fallback : text_to_cstring(PG_GETARG_TEXT_PP(argno));
    if (s == NULL || strlen(s) != 1 || s[0] == '\n' || s[0] == '\r')
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("external CSV delimiter must be a single non-newline character")));
    return s[0];
}

Datum og_lake_describe_external_server_file(PG_FUNCTION_ARGS)
{
    if (!LakeExternalFileVersionEnabled())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("external file schema inference requires installed extension 0.61")));
    if (PG_NARGS() < 2 || PG_ARGISNULL(0) || PG_ARGISNULL(1))
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
            errmsg("describe_external_file server_name and path cannot be NULL")));
    const char* serverName = NameStr(*PG_GETARG_NAME(0));
    const char* relPath = text_to_cstring(PG_GETARG_TEXT_PP(1));
    const char* formatOpt = (PG_NARGS() > 2 && !PG_ARGISNULL(2)) ? text_to_cstring(PG_GETARG_TEXT_PP(2)) : NULL;
    bool header = (PG_NARGS() > 3 && !PG_ARGISNULL(3)) ? PG_GETARG_BOOL(3) : true;
    char delim = LakeParseDelimArg(fcinfo, 4, ",");

    ForeignServer* server = GetForeignServerByName(serverName, false);
    if (pg_foreign_server_aclcheck(server->serverid, GetUserId(), ACL_USAGE) != ACLCHECK_OK)
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_FOREIGN_SERVER, serverName);
    char* root = LakeExternalServerRoot(server->serverid);
    return LakeDescribeExternalCommon(fcinfo, root, relPath, formatOpt, header, delim);
}

Datum og_lake_describe_external_path_file(PG_FUNCTION_ARGS)
{
    if (!LakeExternalFileVersionEnabled())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("external file schema inference requires installed extension 0.61")));
    if (PG_NARGS() < 1 || PG_ARGISNULL(0))
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
            errmsg("describe_external_file path cannot be NULL")));
    const char* rawPath = text_to_cstring(PG_GETARG_TEXT_PP(0));
    const char* formatOpt = (PG_NARGS() > 1 && !PG_ARGISNULL(1)) ? text_to_cstring(PG_GETARG_TEXT_PP(1)) : NULL;
    bool header = (PG_NARGS() > 2 && !PG_ARGISNULL(2)) ? PG_GETARG_BOOL(2) : true;
    char delim = LakeParseDelimArg(fcinfo, 3, ",");

    char* root = NULL;
    char* relPath = NULL;
    LakeExternalAuthorizePath(rawPath, &root, &relPath);
    return LakeDescribeExternalCommon(fcinfo, root, relPath, formatOpt, header, delim);
}

Datum og_lake_external_file_info(PG_FUNCTION_ARGS)
{
    if (!LakeExternalFileVersionEnabled())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("external_file_info requires installed extension 0.61")));
    if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
            errmsg("external_file_info server_name and path cannot be NULL")));
    const char* serverName = NameStr(*PG_GETARG_NAME(0));
    const char* relPath = text_to_cstring(PG_GETARG_TEXT_PP(1));
    const char* formatOpt = PG_ARGISNULL(2) ? NULL : text_to_cstring(PG_GETARG_TEXT_PP(2));

    ReturnSetInfo* result = (ReturnSetInfo*)fcinfo->resultinfo;
    if (!result || !IsA(result, ReturnSetInfo) || !(result->allowedModes & SFRM_Materialize) || !result->econtext)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("external_file_info requires materialize mode")));

    ForeignServer* server = GetForeignServerByName(serverName, false);
    if (pg_foreign_server_aclcheck(server->serverid, GetUserId(), ACL_USAGE) != ACLCHECK_OK)
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_FOREIGN_SERVER, serverName);
    char* root = LakeExternalServerRoot(server->serverid);
    char* format = LakeExternalNormalizeFormat(formatOpt, relPath);

    MemoryContext callerCtx = MemoryContextSwitchTo(result->econtext->ecxt_per_query_memory);
    TupleDesc outDesc = CreateTemplateTupleDesc(5, false);
    TupleDescInitEntry(outDesc, 1, "format", TEXTOID, -1, DEFAULT_COLLATION_OID);
    TupleDescInitEntry(outDesc, 2, "file_bytes", INT8OID, -1, 0);
    TupleDescInitEntry(outDesc, 3, "row_count", INT8OID, -1, 0);
    TupleDescInitEntry(outDesc, 4, "row_group_count", INT4OID, -1, 0);
    TupleDescInitEntry(outDesc, 5, "column_count", INT4OID, -1, 0);
    outDesc = BlessTupleDesc(outDesc);
    Tuplestorestate* store = tuplestore_begin_heap(true, false,
        Max(64, Min(u_sess->attr.attr_memory.work_mem, 1024)));
    MemoryContextSwitchTo(callerCtx);

    LakeExternalOpenedFile opened = LakeExternalOpenReadonly(root, relPath, format);
    OgLakeSdkFooter* volatile footer = NULL;
    const LakeExternalFdSdk* sdk = NULL;
    Datum vals[5] = {};
    bool nulls[5] = {false, false, false, false, false};
    vals[0] = CStringGetTextDatum(format);
    vals[1] = Int64GetDatum(opened.file_bytes);
    PG_TRY();
    {
        if (strcmp(format, "parquet") == 0) {
            sdk = LakeFileExternalFdProvider();
            footer = LakeExternalOpenFooter(sdk, opened.fd);
            const OgLakeSdkFooterInfo* info = NULL;
            HOLD_INTERRUPTS();
            info = sdk->base->footer_info(footer);
            RESUME_INTERRUPTS();
            if (info == NULL)
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("external Parquet footer metadata is unavailable")));
            vals[2] = Int64GetDatum(info->num_rows);
            vals[3] = Int32GetDatum(info->num_row_groups);
            vals[4] = Int32GetDatum(info->num_fields);
            LakeExternalCloseFooter(sdk, &footer);
        } else if (strcmp(format, "json") == 0) {
            int64 rows = 0;
            int32 cols = 0;
            LakeJsonCountFd(opened.fd, &rows, &cols);
            vals[2] = Int64GetDatum(rows);
            nulls[3] = true;
            vals[4] = Int32GetDatum(cols);
        } else {
            nulls[2] = true;
            nulls[3] = true;
            nulls[4] = true;
        }
        tuplestore_putvalues(store, outDesc, vals, nulls);
    }
    PG_CATCH();
    {
        LakeExternalCloseFooter(sdk, &footer);
        LakeExternalCloseOpenedFile(&opened);
        PG_RE_THROW();
    }
    PG_END_TRY();
    LakeExternalCloseOpenedFile(&opened);

    result->returnMode = SFRM_Materialize;
    result->setResult = store;
    result->setDesc = outDesc;
    return (Datum)0;
}
