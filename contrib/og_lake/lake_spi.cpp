/* SQL is used for metadata only. Results outlive SPI_finish in caller memory. */
#include <memory>
#include <new>
#include <pthread.h>
#include "postgres.h"
#include "lake_types.h"
#include "knl/knl_variable.h"
#include "lake_inline_private.h"
#include "access/heapam.h"
#include "access/og_lake.h"
#include "catalog/pg_namespace.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "storage/lmgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/plancache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"

/* Nested catalog planning must not change the caller's Stream or Vector decision.
 * BuildCachedPlan restores these flags only on coordinators in this host;
 * single-node SPI can otherwise re-enable an outer serial planning retry, and
 * try_vector_engine_strategy='force' would vectorize heap metadata queries.
 * Keep catalog work serial/row and restore explicitly across PostgreSQL ERROR.
 */
struct LakeMetadataPlannerState {
    int dop;
    bool smp_enabled;
    bool stream;
    bool stream_support;
    bool dngather_support;
    bool vector_engine;
    int vector_strategy;
};

static void LakeRestoreMetadataPlanner(const LakeMetadataPlannerState& saved)
{
    u_sess->opt_cxt.query_dop=saved.dop;
    u_sess->opt_cxt.smp_enabled=saved.smp_enabled;
    u_sess->opt_cxt.is_stream=saved.stream;
    u_sess->opt_cxt.is_stream_support=saved.stream_support;
    u_sess->opt_cxt.is_dngather_support=saved.dngather_support;
    u_sess->attr.attr_sql.enable_vector_engine=saved.vector_engine;
    u_sess->attr.attr_sql.vectorEngineStrategy=saved.vector_strategy;
}

static const int LAKE_SPI_CACHE_CAPACITY = 64;
static const int LAKE_SPI_CACHE_MAX_ARGS = 8;
static const Size LAKE_SPI_CACHE_MAX_SQL = 2048;

struct LakeSpiCacheEntry {
    bool used;
    bool retired;
    int active_depth;
    Oid database_oid;
    Oid owner_oid;
    Oid namespace_oid;
    int nargs;
    Oid argtypes[LAKE_SPI_CACHE_MAX_ARGS];
    uint64 sql_hash;
    Size sql_len;
    char* sql;
    uint64 last_used;
    SPIPlanPtr plan;
};

struct LakeSpiSessionCache {
    MemoryContext session_cxt;
    MemoryContextCallback* reset_cb;
    uint64 tick;
    LakeSpiCacheEntry entries[LAKE_SPI_CACHE_CAPACITY];
    LakeSpiSessionCache* next;
};

static pthread_mutex_t g_lake_spi_cache_mutex = PTHREAD_MUTEX_INITIALIZER;
static LakeSpiSessionCache* g_lake_spi_caches = NULL;

static uint64 LakeSpiHashSql(const char* sql, Size len)
{
    uint64 h = 14695981039346656037ULL;
    for (Size i = 0; i < len; ++i) {
        h ^= (unsigned char)sql[i];
        h *= 1099511628211ULL;
    }
    return h;
}

static void LakeSpiSessionCacheReset(std::shared_ptr<void> ptr)
{
    LakeSpiSessionCache* cache = (LakeSpiSessionCache*)ptr.get();
    if (cache == NULL)
        return;
    pthread_mutex_lock(&g_lake_spi_cache_mutex);
    LakeSpiSessionCache** link = &g_lake_spi_caches;
    while (*link != NULL && *link != cache)
        link = &(*link)->next;
    if (*link == cache)
        *link = cache->next;
    pthread_mutex_unlock(&g_lake_spi_cache_mutex);
}

static LakeSpiSessionCache* LakeGetSpiSessionCache(void)
{
    if (u_sess == NULL || u_sess->cache_mem_cxt == NULL || IsBootstrapProcessingMode())
        return NULL;
    MemoryContext target = u_sess->cache_mem_cxt;
    pthread_mutex_lock(&g_lake_spi_cache_mutex);
    LakeSpiSessionCache* cache = g_lake_spi_caches;
    while (cache != NULL && cache->session_cxt != target)
        cache = cache->next;
    pthread_mutex_unlock(&g_lake_spi_cache_mutex);
    if (cache != NULL)
        return cache;
    MemoryContext old = MemoryContextSwitchTo(target);
    cache = (LakeSpiSessionCache*)palloc0(sizeof(LakeSpiSessionCache));
    cache->session_cxt = target;
    MemoryContextCallback* cb = (MemoryContextCallback*)palloc(sizeof(MemoryContextCallback));
    new(cb) MemoryContextCallback();
    cb->func = LakeSpiSessionCacheReset;
    cb->arg = std::shared_ptr<void>(std::shared_ptr<void>(), cache);
    cache->reset_cb = cb;
    MemoryContextSwitchTo(old);
    MemoryContextRegisterResetCallback(target, cb);
    pthread_mutex_lock(&g_lake_spi_cache_mutex);
    cache->next = g_lake_spi_caches;
    g_lake_spi_caches = cache;
    pthread_mutex_unlock(&g_lake_spi_cache_mutex);
    return cache;
}

static LakeSpiCacheEntry* LakeSpiLookupPlan(LakeSpiSessionCache* cache, Oid dbOid, Oid ownerOid,
    Oid nsOid, const char* sql, Size sqlLen, uint64 sqlHash, int nargs, const Oid* argtypes)
{
    for (int i = 0; i < LAKE_SPI_CACHE_CAPACITY; ++i) {
        LakeSpiCacheEntry* e = &cache->entries[i];
        if (!e->used || e->retired || e->plan == NULL)
            continue;
        if (e->sql_hash != sqlHash || e->sql_len != sqlLen || e->nargs != nargs ||
            e->database_oid != dbOid || e->owner_oid != ownerOid || e->namespace_oid != nsOid)
            continue;
        bool argsMatch = true;
        for (int a = 0; a < nargs; ++a) {
            if (e->argtypes[a] != argtypes[a]) {
                argsMatch = false;
                break;
            }
        }
        if (argsMatch && memcmp(e->sql, sql, sqlLen) == 0) {
            e->last_used = ++cache->tick;
            return e;
        }
    }
    return NULL;
}

static void LakeSpiInsertPlan(LakeSpiSessionCache* cache, Oid dbOid, Oid ownerOid,
    Oid nsOid, const char* sql, Size sqlLen, uint64 sqlHash, int nargs, const Oid* argtypes, SPIPlanPtr plan)
{
    if (SPI_keepplan(plan) != 0)
        return;
    LakeSpiCacheEntry* victim = NULL;
    for (int i = 0; i < LAKE_SPI_CACHE_CAPACITY; ++i) {
        LakeSpiCacheEntry* e = &cache->entries[i];
        if (!e->used) {
            victim = e;
            break;
        }
        if (e->active_depth == 0) {
            if (e->retired) {
                victim = e;
                break;
            }
            if (victim == NULL || e->last_used < victim->last_used)
                victim = e;
        }
    }
    if (victim == NULL) {
        (void)SPI_freeplan(plan);
        return;
    }
    if (victim->used) {
        if (victim->plan != NULL)
            (void)SPI_freeplan(victim->plan);
        if (victim->sql != NULL)
            pfree(victim->sql);
    }
    MemoryContext old = MemoryContextSwitchTo(cache->session_cxt);
    victim->sql = (char*)palloc(sqlLen + 1);
    memcpy(victim->sql, sql, sqlLen + 1);
    MemoryContextSwitchTo(old);
    victim->used = true;
    victim->retired = false;
    victim->active_depth = 0;
    victim->database_oid = dbOid;
    victim->owner_oid = ownerOid;
    victim->namespace_oid = nsOid;
    victim->nargs = nargs;
    for (int a = 0; a < nargs; ++a)
        victim->argtypes[a] = argtypes[a];
    victim->sql_hash = sqlHash;
    victim->sql_len = sqlLen;
    victim->last_used = ++cache->tick;
    victim->plan = plan;
}

void LakeInlineRequireAdministrator(void)
{
    if (!superuser())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("native lake inline prototype currently requires superuser privileges"),
            errdetail("The ordinary-user internal catalog permission adapter has not been enabled.")));
}

LakeMetaRows* LakeMetadataQuery(const char* sql, int nargs, Oid* argtypes,
    Datum* args, const char* nulls, Snapshot snapshot, bool readOnly, int expectedStatus, Oid expectedNamespace)
{
    MemoryContext caller=CurrentMemoryContext;
    LakeMetaRows* result=(LakeMetaRows*)palloc0(sizeof(LakeMetaRows));
    /* Native catalog reads prove policy trust before switching any identity. */
    Oid owner=LakeMetadataOwner();
    if(!OidIsValid(owner)) LakeInlineRequireAdministrator();
    if(!OidIsValid(expectedNamespace)) expectedNamespace=get_namespace_oid("og_lake_internal",false);
    Oid savedUser=InvalidOid; int savedSecurity=0;
    GetUserIdAndSecContext(&savedUser,&savedSecurity);
    LakeMetadataScope scope={expectedNamespace,OidIsValid(owner)?owner:savedUser,OidIsValid(owner)};
    if(OidIsValid(owner)) {
        LakeCheckMaskingAdmission();
        LakeValidatePrivateNamespace(expectedNamespace,scope.owner_oid);
    }
    /* Allocate/register session state before entering a restricted identity. */
    LakeMetadataScope* previousScope=LakeSetMetadataScope(&scope);
    volatile bool connected=false;
    volatile bool pathPushed=false;
    volatile int gucNest=0;
    LakeSpiCacheEntry* volatile activeEntry=NULL;
    const LakeMetadataPlannerState planner={u_sess->opt_cxt.query_dop,u_sess->opt_cxt.smp_enabled,
        u_sess->opt_cxt.is_stream,u_sess->opt_cxt.is_stream_support,u_sess->opt_cxt.is_dngather_support,
        u_sess->attr.attr_sql.enable_vector_engine,u_sess->attr.attr_sql.vectorEngineStrategy};
    PG_TRY();
    {
        u_sess->opt_cxt.query_dop=1;
        u_sess->opt_cxt.smp_enabled=false;
        u_sess->opt_cxt.is_stream=false;
        u_sess->opt_cxt.is_stream_support=false;
        u_sess->attr.attr_sql.enable_vector_engine=false;
        u_sess->attr.attr_sql.vectorEngineStrategy=OFF_VECTOR_ENGINE;
        gucNest=NewGUCNestLevel();
        if(OidIsValid(owner))
            SetUserIdAndSecContext(owner,savedSecurity|SECURITY_LOCAL_USERID_CHANGE|SECURITY_RESTRICTED_OPERATION);
        OverrideSearchPath path;
        errno_t rc=memset_s(&path,sizeof(path),0,sizeof(path)); securec_check(rc,"","");
        path.schemas=list_make1_oid(PG_CATALOG_NAMESPACE);
        PushOverrideSearchPath(&path); pathPushed=true;
        if(SPI_connect()!=SPI_OK_CONNECT)
            ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("native lake could not connect to SPI")));
        connected=true;
        Size sqlLen=strlen(sql);
        bool cacheEligible=readOnly&&expectedStatus==SPI_OK_SELECT&&nargs>=0&&
            nargs<=LAKE_SPI_CACHE_MAX_ARGS&&sqlLen<=LAKE_SPI_CACHE_MAX_SQL&&
            (nargs==0||argtypes!=NULL);
        LakeSpiSessionCache* sessionCache=cacheEligible?LakeGetSpiSessionCache():NULL;
        uint64 sqlHash=sessionCache?LakeSpiHashSql(sql,sqlLen):0;
        SPIPlanPtr plan=NULL;
        if(sessionCache!=NULL) {
            LakeSpiCacheEntry* hit=LakeSpiLookupPlan(sessionCache,u_sess->proc_cxt.MyDatabaseId,
                scope.owner_oid,expectedNamespace,sql,sqlLen,sqlHash,nargs,argtypes);
            if(hit!=NULL) {
                plan=hit->plan;
                ++hit->active_depth;
                activeEntry=hit;
            }
        }
        bool fromCache=(plan!=NULL);
        if(plan==NULL) {
            plan=SPI_prepare(sql,nargs,argtypes);
            if(plan==NULL)
                ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("native lake could not prepare metadata query")));
        }
        ListCell* sourceCell=NULL;
        foreach(sourceCell,SPI_plan_get_plan_sources(plan)) {
            CachedPlanSource* source=(CachedPlanSource*)lfirst(sourceCell);
            if(!fromCache||source->is_valid) {
                ListCell* queryCell=NULL;
                foreach(queryCell,source->query_list)
                    LakeCheckQueryPermissions((Query*)lfirst(queryCell));
            }
        }
        /* parserOpenTable, planner and ExecutorStart independently validate
         * actual objects during parse, cached-plan rebuild and execution.
         * Exact fixed FK/check constraints are the only indirect code allowed.
         */
        int status=SPI_execute_snapshot(plan,args,nulls,snapshot,InvalidSnapshot,readOnly,!readOnly,0);
        /* Identity restoration cannot throw; do it before any cleanup. */
        SetUserIdAndSecContext(savedUser,savedSecurity);
        LakeRestoreMetadataScope(previousScope);
        if(status!=expectedStatus)
            ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("native lake metadata query returned unexpected SPI status %d",status)));
        MemoryContextSwitchTo(caller);
        uint64 processed=(uint64)SPI_processed;
        if(processed>(uint64)PG_UINT32_MAX || processed>MaxAllocSize/sizeof(HeapTuple))
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake metadata result is too large")));
        result->count=(uint32)processed;
        if(SPI_tuptable!=NULL) {
            result->descriptor=CreateTupleDescCopy(SPI_tuptable->tupdesc);
            result->rows=(HeapTuple*)palloc0(sizeof(HeapTuple)*result->count);
            for(uint32 i=0;i<result->count;++i) result->rows[i]=heap_copytuple(SPI_tuptable->vals[i]);
        }
        if(fromCache) {
            if(activeEntry!=NULL) {
                if(activeEntry->active_depth>0) --activeEntry->active_depth;
                activeEntry=NULL;
            }
        } else if(sessionCache!=NULL && !plan->saved && !plan->oneshot) {
            LakeSpiInsertPlan(sessionCache,u_sess->proc_cxt.MyDatabaseId,
                scope.owner_oid,expectedNamespace,sql,sqlLen,sqlHash,nargs,argtypes,plan);
            if(!plan->saved)
                SPI_freeplan(plan);
        } else {
            SPI_freeplan(plan);
        }
        int finishStatus=SPI_finish(); connected=false;
        if(finishStatus!=SPI_OK_FINISH)
            ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("native lake could not finish SPI")));
        PopOverrideSearchPath(); pathPushed=false;
        AtEOXact_GUC(false,gucNest); gucNest=0;
        MemoryContextSwitchTo(caller);
        LakeRestoreMetadataPlanner(planner);
    }
    PG_CATCH();
    {
        /* Publisher catches errors and may retry a subtransaction. Do not rely
         * on eventual top-level abort to remove the restricted identity. */
        if(activeEntry!=NULL) {
            if(activeEntry->active_depth>0) --activeEntry->active_depth;
            activeEntry->retired=true;
            activeEntry=NULL;
        }
        SetUserIdAndSecContext(savedUser,savedSecurity);
        LakeRestoreMetadataScope(previousScope);
        LakeRestoreMetadataPlanner(planner);
        MemoryContextSwitchTo(caller);
        if(connected) (void)SPI_finish();
        if(pathPushed) PopOverrideSearchPath();
        if(gucNest!=0) AtEOXact_GUC(false,gucNest);
        LakeRestoreMetadataPlanner(planner);
        PG_RE_THROW();
    }
    PG_END_TRY();
    return result;
}

Datum LakeMetadataValue(const LakeMetaRows* rows, uint32 row, int column,
    Oid expectedType, bool nullable, bool* isnull)
{
    if (rows->descriptor == NULL || row >= rows->count || column <= 0 ||
        column > rows->descriptor->natts || rows->descriptor->attrs[column - 1].atttypid != expectedType)
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("native lake metadata has an unexpected result type")));
    bool nullValue = false;
    Datum value = heap_getattr(rows->rows[row], column, rows->descriptor, &nullValue);
    if (nullValue && !nullable)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("native lake metadata contains an unexpected NULL")));
    if (isnull != NULL)
        *isnull = nullValue;
    return value;
}

char* LakeMetadataText(const LakeMetaRows* rows, uint32 row, int column)
{
    return TextDatumGetCString(LakeMetadataValue(rows, row, column, TEXTOID));
}

void LakeMetadataRequireCount(const LakeMetaRows* rows, uint32 count, const char* object)
{
    if (rows->count != count)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("native lake expected %u %s records but found %u", count, object, rows->count)));
}

struct LakeRequiredMetadataColumn {
    const char* table;
    const char* column;
    Oid type;
};

static void LakeCheckMetadataColumns(Relation relation, const char* table)
{
    /* Check physical types before SPI parsing can invoke a user-defined cast.
     * Full 1.1 DDL/constraint equivalence remains a separate compatibility gate.
     */
    static const LakeRequiredMetadataColumn required[] = {
        {"ducklake_snapshot", "snapshot_time", TIMESTAMPTZOID},
        {"ducklake_snapshot", "next_catalog_id", INT8OID},
        {"ducklake_snapshot", "next_file_id", INT8OID},
        {"ducklake_snapshot_changes", "author", TEXTOID},
        {"ducklake_snapshot_changes", "commit_message", TEXTOID},
        {"ducklake_snapshot_changes", "commit_extra_info", TEXTOID},
        {"ducklake_partition_info", "partition_id", INT8OID},
        {"ducklake_partition_info", "table_id", INT8OID},
        {"ducklake_partition_info", "begin_snapshot", INT8OID},
        {"ducklake_partition_info", "end_snapshot", INT8OID},
        {"ducklake_partition_column", "partition_id", INT8OID},
        {"ducklake_partition_column", "table_id", INT8OID},
        {"ducklake_partition_column", "partition_key_index", INT8OID},
        {"ducklake_partition_column", "column_id", INT8OID},
        {"ducklake_partition_column", "transform", TEXTOID},
        {"ducklake_file_partition_value", "data_file_id", INT8OID},
        {"ducklake_file_partition_value", "table_id", INT8OID},
        {"ducklake_file_partition_value", "partition_key_index", INT8OID},
        {"ducklake_file_partition_value", "partition_value", TEXTOID},
        {"ducklake_data_file", "partition_id", INT8OID},
        {"ducklake_metadata", "key", TEXTOID}, {"ducklake_metadata", "value", TEXTOID},
        {"ducklake_metadata", "scope", TEXTOID}, {"ducklake_metadata", "scope_id", INT8OID},
        {"ducklake_schema", "schema_id", INT8OID}, {"ducklake_schema", "schema_uuid", UUIDOID},
        {"ducklake_schema", "schema_name", TEXTOID}, {"ducklake_schema", "parent_schema_id", INT8OID},
        {"ducklake_schema", "begin_snapshot", INT8OID}, {"ducklake_schema", "end_snapshot", INT8OID},
        {"ducklake_view", "view_name", TEXTOID}, {"ducklake_view", "schema_id", INT8OID},
        {"ducklake_view", "begin_snapshot", INT8OID}, {"ducklake_view", "end_snapshot", INT8OID},
        {"ducklake_table", "schema_id", INT8OID}, {"ducklake_table", "table_name", TEXTOID},
        {"ducklake_snapshot", "snapshot_id", INT8OID}, {"ducklake_snapshot", "schema_version", INT8OID},
        {"ducklake_table", "table_id", INT8OID}, {"ducklake_table", "table_uuid", UUIDOID},
        {"ducklake_table", "begin_snapshot", INT8OID}, {"ducklake_table", "end_snapshot", INT8OID},
        {"ducklake_column", "table_id", INT8OID}, {"ducklake_column", "column_id", INT8OID},
        {"ducklake_column", "column_order", INT8OID}, {"ducklake_column", "column_name", TEXTOID},
        {"ducklake_column", "column_type", TEXTOID}, {"ducklake_column", "parent_column", INT8OID},
        {"ducklake_column", "begin_snapshot", INT8OID}, {"ducklake_column", "end_snapshot", INT8OID},
        {"ducklake_inlined_data_tables", "table_id", INT8OID},
        {"ducklake_inlined_data_tables", "table_name", TEXTOID},
        {"ducklake_inlined_data_tables", "schema_version", INT8OID},
        {"ducklake_schema_versions", "table_id", INT8OID},
        {"ducklake_schema_versions", "schema_version", INT8OID},
        {"ducklake_schema_versions", "begin_snapshot", INT8OID},
        {"ducklake_data_file", "table_id", INT8OID}, {"ducklake_data_file", "begin_snapshot", INT8OID},
        {"ducklake_data_file", "end_snapshot", INT8OID},
        {"ducklake_delete_file", "table_id", INT8OID}, {"ducklake_delete_file", "begin_snapshot", INT8OID},
        {"ducklake_delete_file", "end_snapshot", INT8OID}
    };
    for (Size i = 0; i < lengthof(required); ++i) {
        if (strcmp(table, required[i].table) != 0) continue;
        AttrNumber attnum = get_attnum(RelationGetRelid(relation), required[i].column);
        if (attnum <= 0)
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("DuckLake 1.1 metadata column is missing")));
        Form_pg_attribute attr = &RelationGetDescr(relation)->attrs[attnum - 1];
        if (attr->attisdropped || (attr->atttypid != required[i].type &&
            !(required[i].type == TEXTOID && attr->atttypid == VARCHAROID)))
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("DuckLake 1.1 metadata column \"%s.%s\" has an incompatible physical type", table, required[i].column)));
    }
}

char* LakeMetadataRelation(Oid schemaOid, const char* table)
{
    /* Native relation locks protect the opened table's shape. Namespace rename
     * does not honor this object lock in every host path; the prepared-query
     * RTE check above additionally verifies the bound namespace identity.
     */
    LockDatabaseObject(NamespaceRelationId, schemaOid, 0, AccessShareLock);
    char* schema = get_namespace_name(schemaOid);
    if (schema == NULL)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_SCHEMA), errmsg("native lake metadata schema no longer exists")));
    Oid oid = get_relname_relid(table, schemaOid);
    if (!OidIsValid(oid))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("native lake metadata table \"%s\" is missing", table)));
    Relation relation = heap_open(oid, AccessShareLock);
    Oid internalOwner=LakeMetadataOwner();
    Oid expectedOwner=OidIsValid(internalOwner)?internalOwner:GetUserId();
    if(OidIsValid(internalOwner)) {
        LakeValidatePrivateNamespace(schemaOid,internalOwner);
        LakeValidatePrivateRelation(relation,internalOwner,schemaOid==get_namespace_oid("og_lake_internal",false));
        LakeCheckMetadataObjectIdentity(schemaOid,oid,internalOwner);
    }
    if (relation->rd_rel->relowner != expectedOwner)
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("native lake metadata and inline relations must be owned by the attaching administrator")));
    if (relation->rd_rel->relkind != RELKIND_RELATION || relation->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT ||
        RelationIsPartitioned(relation) || RowRelationIsCompressed(relation) ||
        relation->rd_rel->relhassubclass ||
        RelationIsColStore(relation))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake metadata must be ordinary local tables owned by the attaching administrator")));
    LakeCheckMetadataColumns(relation, table);
    heap_close(relation, NoLock);
    return quote_qualified_identifier(schema, table);
}

Oid LakeInlineType(const char* name)
{
    return LakeNativeTypeByDuck(name)->oid;
}
