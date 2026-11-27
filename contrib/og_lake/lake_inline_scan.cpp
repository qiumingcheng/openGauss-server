/* Native TableAM reader. There is no SPI business-data scan or DuckDB call. */
#include "postgres.h"
#include "lake_version.h"
#include "lake_column_layout.h"
#include "lake_inline_sources.h"
#include "lake_cost.h"
#include "knl/knl_variable.h"
#include "lake_scalar_key.h"
#include "lake_txn_private.h"
#include "lake_inline_reader.h"
#include "lake_partition_prune.h"
#include "lake_stream.h"
#include "optimizer/stream_check.h"
#include "lake_index_private.h"
#include "lake_inline_ann.h"
#include "lake_file_ann.h"
#include "lake_ann_rescan.h"
#include "catalog/pg_opfamily.h"
#include "access/heapam.h"
#include "access/og_lake.h"
#include "catalog/pg_type.h"
#include "catalog/pg_am.h"
#include "catalog/pg_class.h"
#include "access/skey.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "commands/explain.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/plancat.h"
#include "optimizer/restrictinfo.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/spccache.h"
#include "utils/guc.h"
#include "vecexecutor/vectorbatch.h"
#include <math.h>

static const int LAKE_SCAN_PRIVATE_VERSION = 8; /* Bounded boolean scalar tree and native RHS expressions. */

struct LakeAnnFilterLeafState {
    LakeAnnScalarExpr* expression;
    AttrNumber column;
    ExprState* rhs;
    Oid operator_oid;
    Oid subtype;
    int null_mode;
    bool array;
};

struct LakeInlineScanState {
    LakeInlineBinding* binding;
    LakeTxnTable* transaction_table;
    LakeInlineReader* reader;
    LakePartitionPrune* partition_prune;
    bool partition_pending;
    bool* last_partition_skipped;
    CommandId command_limit;
    AttrNumber whole_row_slot;
    AttrNumber locator_slot;
    uint64 returned;
    bool stream_scan;
    LakeStreamWorker* stream_worker;
    ExprState* inline_key;
    bool index_pending; /* Correlated PARAM_EXEC values are not ready during Begin. */
    bool inline_array;
    int ann_metric;
    uint32 ann_limit;
    bool ann_used;
    LakeAnnScalarExpr* ann_filter;
    List* ann_filter_metadata;
    MemoryContext ann_filter_context;
    int ann_filter_leaf_count;
    LakeAnnFilterLeafState ann_filter_leaves[LAKE_ANN_SCALAR_MAX_LEAVES];
    int inline_null_mode; /* 0=comparison, 1=IS NULL, 2=IS NOT NULL. */
    Oid inline_index_oid;
    Oid inline_operator;
    AttrNumber inline_column;
    char* scalar_uuid;
    /* These contexts belong to es_query_cxt, never the executor m_scanCxt
     * (ExecEndVecForeignScan deletes that context before the FDW End callback). */
    MemoryContext vector_row_context;
    MemoryContext vector_pending_context;
    HeapTuple vector_pending;
    LakeInlineRowIdentity vector_pending_identity;
    uint64 vector_batches;
    uint64 vector_peak_bytes;
    uint64 vector_rescans;
    bool vector_extended;
    bool vector_first_row;
    bool vector_has_varlena;
    int vector_col_count;
    bool* vector_col_varlena;
    bool* vector_col_wide;
    Size* vector_col_sizes;
    Size* vector_col_batch_bytes;
    bool* projected;
};

struct LakeProjectionCtx {
    const LakeInlineBinding* binding;
    Index scanrelid;
    bool has_fdw_scan_tlist;
    int business_columns;
    bool need_all;
    bool* projected;
};

static bool LakeCollectProjectedWalker(Node* node, LakeProjectionCtx* ctx)
{
    if (node == NULL || ctx->need_all)
        return ctx->need_all;
    if (IsA(node, Var)) {
        Var* var = (Var*)node;
        if (var->varlevelsup == 0 &&
            (var->varno == ctx->scanrelid ||
             (ctx->has_fdw_scan_tlist && var->varno == INDEX_VAR))) {
            if (var->varattno <= 0 || var->varattno > ctx->business_columns) {
                ctx->need_all = true;
                return true;
            }
            int logical = LakeHostToLogical(ctx->binding, var->varattno);
            if (logical < 0 || logical >= ctx->binding->column_count) {
                ctx->need_all = true;
                return true;
            }
            ctx->projected[logical] = true;
        }
        return false;
    }
    return expression_tree_walker(node, (bool (*)())LakeCollectProjectedWalker, (void*)ctx);
}

/* Payload bound in addition to the host BatchMaxSize row/column allocations.
 * A consumed row crossing the boundary is retained exactly once for next call. */
static const Size LAKE_VECTOR_PAYLOAD_BYTES=8*1024*1024;

bool LakeVectorType(Oid type, bool extended)
{
    /* INT2 is admitted only to the row path in SQL0.20; SQL0.72 qualifies extended types. */
    if(type==BOOLOID||type==INT4OID||type==INT8OID||
        type==FLOAT4OID||type==FLOAT8OID||type==TEXTOID)return true;
    return extended&&(type==INT2OID||type==DATEOID||type==TIMESTAMPOID||
                      type==TIMESTAMPTZOID||type==BYTEAOID||type==NUMERICOID);
}

bool LakeVectorBindingSupported(const LakeInlineBinding* binding, bool extended)
{
    if(!binding||binding->bound_snapshot_read||LakeBindingHasUuid(binding)||LakeBindingHasJson(binding))
        return false;
    if(!extended&&(LakeBindingHasBinary(binding)||LakeBindingHasNumeric(binding)||
                   LakeIdentityPartitioned(binding)||LakeInlineHasHistoricalSources(binding)))
        return false;
    if(extended&&(binding->has_added_columns||binding->has_dropped_columns||
                  binding->has_altered_types||binding->retired_count>0||
                  binding->future_count>0||
                  (LakeInlineHasHistoricalSources(binding)&&!LakePartitionComboVersionEnabled())))
        return false;
    if(binding->column_count<1)return false;
    for(int i=0;i<binding->column_count;++i)
        if(!LakeVectorType(binding->columns[i].type,extended))return false;
    return true;
}

static bool LakeVectorPlanAllowed(PlannerInfo* root,Oid relationOid,
    ForeignPath* path,List* indexData,List* scanList,const LakeInlineBinding* binding)
{
    const bool vectorSmp=LakeVectorSmpVersionEnabled();
    if(!LakeVectorBindingSupported(binding,vectorSmp))return false;
    if(root->parse->commandType!=CMD_SELECT||root->parse->rowMarks!=NIL||
       root->parse->hasModifyingCTE||indexData!=NIL||scanList!=NIL||
       !u_sess->attr.attr_sql.enable_vector_engine||
       u_sess->attr.attr_sql.vectorEngineStrategy==OFF_VECTOR_ENGINE||
       !LakeVectorBatchVersionEnabled())return false;
    if(path->path.dop>1){
        if(!vectorSmp||list_length(path->fdw_private)!=1||
           !IsA(linitial(path->fdw_private),String)||
           strcmp(strVal(linitial(path->fdw_private)),"lake_smp_v1")!=0)
            return false;
    }else if(u_sess->opt_cxt.query_dop>1&&!vectorSmp){
        return false;
    }
    Relation relation=heap_open(relationOid,NoLock);
    TupleDesc descriptor=RelationGetDescr(relation);
    bool allowed=descriptor->natts>0&&(!vectorSmp||descriptor->natts==binding->column_count);
    for(int i=0;allowed&&i<descriptor->natts;++i)
        if(descriptor->attrs[i].attisdropped||!LakeVectorType(descriptor->attrs[i].atttypid,vectorSmp))allowed=false;
    heap_close(relation,NoLock);
    /* Native CheckVectorEngineUnsupportedFeature remains responsible for
     * expression/operator support and can fall back the complete plan. */
    return allowed;
}

static const char* LakeInlineAccessLabel(Oid indexOid)
{
    if(!OidIsValid(indexOid))return "TableAM sequential";
    HeapTuple tuple=SearchSysCache1(RELOID,ObjectIdGetDatum(indexOid));
    if(!HeapTupleIsValid(tuple))return "unavailable native index";
    Oid method=((Form_pg_class)GETSTRUCT(tuple))->relam;ReleaseSysCache(tuple);
    if(method==DISKANN_AM_OID)return "native DiskANN approximate candidates";
    return method==UBTREE_AM_OID?"native UB-tree":method==BTREE_AM_OID?"native B-tree":"unavailable native index";
}

/* Planner estimates only: they never select or suppress business rows. */
struct LakeInlineEstimate {
    const char* row_source;
    LakeInlineBinding* smp_binding;
    double visible_rows;
    double physical_versions;
    double data_pages;
    Oid carrier_tablespace;
    double io_cost;
    double visibility_cost;
    double visible_row_cost;
    double target_cost;
    double startup_cost;
    int unknown_graphs;
    LakeCostLedger* ledger;
};

static void LakeEstimateInlineRows(PlannerInfo* root, RelOptInfo* baserel,
    Oid relationOid, LakeInlineBinding* binding, Snapshot snapshot)
{
    LakeInlineEstimate* estimate = (LakeInlineEstimate*)palloc0(sizeof(LakeInlineEstimate));
    baserel->fdw_private = estimate;estimate->smp_binding=binding;
    if(binding->identity.kind==LAKE_TABLE_LOCAL) {
        LakeTxnTable* local=LakeTxnFindCreatedTable(relationOid);
        if(local==NULL) ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("local lake plan has no active object")));
        estimate->visible_rows=(double)local->active_rows;
        estimate->row_source="transaction-local object / private surviving rows";
        estimate->carrier_tablespace=InvalidOid;
        baserel->tuples=estimate->visible_rows;baserel->pages=0;
        set_baserel_size_estimates(root,baserel);
        return;
    }
    estimate->data_pages = (double)LakeInlinePhysicalPages(binding);
    double fileRows=0;
    for(int i=0;i<binding->file_count;++i){
        estimate->data_pages+=ceil((double)binding->files[i].file_bytes/BLCKSZ);
        fileRows+=(double)binding->files[i].record_count;
    }
    Relation host = heap_open(relationOid, NoLock);
    bool analyzed = host->rd_rel->relpages > 0 && host->rd_rel->reltuples >= 0;
    double analyzedRows = host->rd_rel->reltuples;
    heap_close(host, NoLock);
    /* A physical version estimate is useful for scan work, never exact COUNT. */
    Oid types[1] = {INT8OID};
    Datum args[1] = {Int64GetDatum(LakePermanentTableId(binding))};
    LakeMetaRows* stats = LakeMetadataQuery(psprintf("SELECT record_count FROM %s WHERE table_id=$1",
        LakeMetadataRelation(binding->metadata_schema_oid, "ducklake_table_stats")),
        1, types, args, NULL, snapshot, true, SPI_OK_SELECT, binding->metadata_schema_oid);
    if (stats->count > 1)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("native lake has duplicate table statistics for inline cost estimate")));
    /* A standard table created before its first INSERT can have no stats row.
     * Missing/NULL estimates are not a corruption of its validated binding. */
    bool unknown = stats->count == 0;
    Datum value = (Datum)0;
    if (!unknown) value = LakeMetadataValue(stats, 0, 1, INT8OID, true, &unknown);
    double physical = 0;
    if (!unknown) {
        int64 count = DatumGetInt64(value);
        if (count < 0)
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("native lake has a negative storage-version estimate")));
        physical = (double)count;
    } else if (LakeInlineReadSourceCount(binding)>0) {
        int32 width = get_relation_data_width(relationOid, InvalidOid, NULL);
        double tupleBytes = Max(1, width) + sizeof(HeapTupleHeaderData) + 3 * sizeof(int64);
        /* Deliberately coarse: pages include TOAST while logical width may
         * differ from compressed/external storage. This is no exact count. */
        physical = estimate->data_pages * BLCKSZ / tupleBytes;
    }
    if (LakeInlineReadSourceCount(binding)==0 && binding->file_count==0) {
        estimate->visible_rows = 0;
        estimate->row_source = "committed inline layout empty; private changes unestimated";
    } else if (analyzed) {
        estimate->visible_rows = analyzedRows;
        estimate->row_source = "pg_class / ANALYZE (may be stale)";
    } else {
        estimate->visible_rows = physical;
        estimate->row_source = unknown ? "fallback from physical pages and width" :
            "fallback from stored-version count (not live COUNT)";
    }
    estimate->physical_versions = Max(physical, estimate->visible_rows);
    if(binding->file_count!=0){
        /* Physical Parquet versions still cost I/O after logical deletes.
         * An analyzed live-row estimate must not be raised to that population.
         * As with heap ANALYZE, later commits/files can make pg_class stale;
         * file metadata cannot reconstruct the missing logical distribution. */
        estimate->physical_versions=Max(estimate->physical_versions,fileRows);
        if(analyzed)estimate->row_source="pg_class / ANALYZE visible file+inline estimate (may be stale)";
        else {
            estimate->visible_rows=Max(estimate->visible_rows,fileRows);
            estimate->row_source="unanalyzed physical file populations + inline estimate; not live COUNT";
        }
    }
    estimate->carrier_tablespace = InvalidOid;
    if (OidIsValid(binding->inline_oid)) {
        Relation carrier = heap_open(binding->inline_oid, AccessShareLock);
        estimate->carrier_tablespace = carrier->rd_rel->reltablespace;
        heap_close(carrier, NoLock);
    }
    baserel->tuples = estimate->visible_rows;
    baserel->pages = estimate->data_pages;
    /* Apply native MCV/NDV/histogram selectivity exactly once; also sets width. */
    set_baserel_size_estimates(root, baserel);
}

static List* LakeInlineExplainValues(const LakeInlineEstimate* estimate)
{
    List* result = list_make1(makeString(pstrdup(estimate->row_source)));
    const double values[] = {estimate->visible_rows, estimate->physical_versions,
        estimate->data_pages, estimate->io_cost, estimate->visibility_cost,
        estimate->visible_row_cost, estimate->target_cost, estimate->startup_cost};
    for (Size i = 0; i < lengthof(values); ++i)
        result = lappend(result, makeFloat(psprintf("%.17g", values[i])));
    result = lappend(result, makeInteger(estimate->unknown_graphs));
    return result;
}

static List* LakeInlinePlanEstimates(ForeignScan* plan)
{
    List* data = plan->fdw_private;
    if (list_length(data) != 5 || !IsA(linitial(data), Integer) || intVal(linitial(data)) != LAKE_SCAN_PRIVATE_VERSION ||
        !IsA(lsecond(data), Integer) || !IsA(lthird(data), Integer) || !IsA(lfourth(data), List))
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid native lake scan private layout")));
    List* estimates = (List*)lfourth(data);
    int len = list_length(estimates);
    if ((len != 9 && len != 10) || !IsA(linitial(estimates), String))
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid native lake scan estimate layout")));
    for (int i = 1; i < 9; ++i)
        if (!IsA(list_nth(estimates, i), Float))
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid native lake scan estimate value")));
    if (len == 10 && !IsA(list_nth(estimates, 9), Integer))
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid native lake scan estimate unknown graph value")));
    return estimates;
}

static void LakeInlineGetForeignRelSizeImpl(PlannerInfo* root, RelOptInfo* baserel, Oid relationOid)
{
    LakeCheckQueryPermissions(root->parse);
    if ((root->parse->commandType != CMD_SELECT && root->parse->commandType != CMD_INSERT &&
         root->parse->commandType != CMD_UPDATE && root->parse->commandType != CMD_DELETE &&
         root->parse->commandType != CMD_MERGE) || root->parse->rowMarks != NIL)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("native lake inline access does not support row locks")));
    for (AttrNumber attr = baserel->min_attr; attr < 0; ++attr) {
        if (!bms_is_empty(baserel->attr_needed[attr - baserel->min_attr]))
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("native lake does not expose heap system columns")));
    }
    Snapshot snapshot = ActiveSnapshotSet() ? GetActiveSnapshot() : GetTransactionSnapshot();
    LakeTxnCatalog* boundCat = LakeTxnGetBoundCatalog(GetForeignTable(relationOid)->serverid);
    if (boundCat != NULL) {
        LakeInlineBinding* binding = LakeLoadBoundSnapshotInlineBinding(relationOid, boundCat->bound_snapshot_id,
            boundCat->bound_schema_version, boundCat->bound_visible_head, boundCat->business_snapshot, false);
        for (AttrNumber attr = 1; attr <= baserel->max_attr; ++attr) {
            if (baserel->attr_needed != NULL && !bms_is_empty(baserel->attr_needed[attr - baserel->min_attr])) {
                if (LakeHostToLogical(binding, attr) < 0) {
                    Relation host = heap_open(relationOid, AccessShareLock);
                    const char* colName = (attr <= RelationGetDescr(host)->natts) ?
                        pstrdup(NameStr(RelationGetDescr(host)->attrs[attr - 1].attname)) : "?";
                    const char* relName = pstrdup(RelationGetRelationName(host));
                    heap_close(host, NoLock);
                    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
                        errmsg("column \"%s\" of relation \"%s\" does not exist at bound snapshot " INT64_FORMAT,
                            colName, relName, boundCat->bound_snapshot_id)));
                }
            }
        }
        LakeEstimateInlineRows(root, baserel, relationOid, binding, boundCat->business_snapshot);
        return;
    }
    LakeInlineBinding* binding = LakeLoadInlineBinding(relationOid, snapshot);
    LakeEstimateInlineRows(root, baserel, relationOid, binding, snapshot);
}

static void LakeInlineGetForeignRelSize(PlannerInfo* root, RelOptInfo* baserel, Oid relationOid)
{
    int fastScope = LakeEnterFastValidationScope();
    PG_TRY();
    {
        LakeInlineGetForeignRelSizeImpl(root, baserel, relationOid);
    }
    PG_CATCH();
    {
        LakeLeaveFastValidationScope(fastScope);
        PG_RE_THROW();
    }
    PG_END_TRY();
    LakeLeaveFastValidationScope(fastScope);
}

static void LakeInlineGetForeignPaths(PlannerInfo* root, RelOptInfo* baserel, Oid relationOid)
{
    LakeInlineEstimate* estimate = (LakeInlineEstimate*)baserel->fdw_private;
    int smpDop=estimate->smp_binding->bound_snapshot_read?1:LakeStreamPlanDop(root,baserel,estimate->smp_binding,true);
    /* Use the host planner's ordinary Stream-to-serial retry. Otherwise a
     * serial lake path can be copied into a worker by an unrelated join or
     * outer pipeline even though this first slice cannot bind that shape. */
    if(IS_STREAM_PLAN&&u_sess->opt_cxt.query_dop>1&&smpDop<=1)mark_stream_unsupport();
    double pageCost = 0;
    get_tablespace_page_costs(estimate->carrier_tablespace, NULL, &pageCost);
    estimate->io_cost = pageCost * estimate->data_pages;
    /* Full physical stream; a selective WHERE does not reduce these inputs.
     * This is an uncalibrated native-unit baseline, not a remote/file model. */
    estimate->visibility_cost = estimate->physical_versions *
        (u_sess->attr.attr_sql.cpu_tuple_cost + 3 * u_sess->attr.attr_sql.cpu_operator_cost);
    estimate->visible_row_cost = estimate->visible_rows *
        (u_sess->attr.attr_sql.cpu_tuple_cost + baserel->baserestrictcost.per_tuple);
    estimate->startup_cost = baserel->baserestrictcost.startup;
    if (root->parse->is_flt_frame) {
        estimate->startup_cost += baserel->reltarget->cost.startup;
        estimate->target_cost = baserel->reltarget->cost.per_tuple * baserel->rows;
    }
    double total = estimate->startup_cost + estimate->io_cost + estimate->visibility_cost +
        estimate->visible_row_cost + estimate->target_cost;
    double randomCost=0;get_tablespace_page_costs(estimate->carrier_tablespace,&randomCost,NULL);
    estimate->ledger=LakeCostCreate(estimate->smp_binding,estimate->startup_cost,total,
        estimate->visible_rows,estimate->physical_versions,estimate->visibility_cost,
        estimate->visible_row_cost,estimate->target_cost,LakeInlinePhysicalPages(estimate->smp_binding),
        pageCost,randomCost,u_sess->attr.attr_sql.cpu_tuple_cost,u_sess->attr.attr_sql.cpu_operator_cost,
        LakeTxnStreamReadEligible());
    LakeCostResult fullCost=LakeCostFinish(estimate->ledger);
    total=fullCost.total;estimate->startup_cost=fullCost.startup;estimate->unknown_graphs=fullCost.unknown_graphs;
    add_path(root, baserel, (Path*)create_foreignscan_path(root, baserel,
        estimate->startup_cost, total+(u_sess->attr.attr_sql.enable_seqscan?0:g_instance.cost_cxt.disable_cost), NIL, NULL, NULL, NIL, 1));
    if(smpDop>1){
        LakeCostResult parallelCost=LakeCostFinish(estimate->ledger,-1,smpDop,u_sess->opt_cxt.smp_thread_cost);
        double parallel=parallelCost.total;
        add_path(root,baserel,(Path*)create_foreignscan_path(root,baserel,parallelCost.startup,
            parallel+(u_sess->attr.attr_sql.enable_seqscan?0:g_instance.cost_cxt.disable_cost),NIL,NULL,NULL,
            list_make1(makeString(pstrdup("lake_smp_v1"))),smpDop));
    }
    /* A path rooted in the current writer cannot omit historical carriers.
     * Keep the complete row stream until index roots describe every source. */
    if(!estimate->smp_binding->bound_snapshot_read&&
       (LakeIxSmpExtendedVersionEnabled()||(!LakeBindingHasBinary(estimate->smp_binding)&&!LakeBindingHasUuid(estimate->smp_binding)&&!LakeBindingHasJson(estimate->smp_binding)&&!LakeBindingHasNumeric(estimate->smp_binding)))&&
       (!LakeInlineHasHistoricalSources(estimate->smp_binding)||
        (LakePartitionComboVersionEnabled()&&!estimate->smp_binding->has_added_columns&&
         !estimate->smp_binding->has_dropped_columns&&estimate->smp_binding->retired_count==0&&
         estimate->smp_binding->future_count==0))){
        LakeAddInlineIndexPaths(root,baserel,relationOid,estimate->startup_cost,total,estimate->ledger);
        LakeAddInlineAnnPaths(root,baserel,relationOid,estimate->startup_cost,total,estimate->ledger);
    }
}

/* Serialized filter nodes use Lists/Integers only; native RHS expressions are
 * separate fdw_exprs entries. No pointer, Datum or ScanKey is serialized. */
static void LakeAnnCheckFilterShape(Node* encoded,bool expressions,int depth,int* nodes,int* leaves)
{
    if(!encoded||!IsA(encoded,List)||depth>LAKE_ANN_SCALAR_MAX_DEPTH||++*nodes>LAKE_ANN_SCALAR_MAX_NODES)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake ANN scalar filter tree bounds")));
    List* tree=(List*)encoded;
    if(tree==NIL||!linitial(tree)||!IsA(linitial(tree),Integer))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake ANN scalar filter node")));
    int kind=intVal(linitial(tree));
    if(kind==LAKE_ANN_SCALAR_LEAF){
        if(list_length(tree)!=7||++*leaves>LAKE_ANN_SCALAR_MAX_LEAVES)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake ANN scalar filter leaf")));
        for(int i=1;i<6;++i)if(!list_nth(tree,i)||!IsA(list_nth(tree,i),Integer))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake ANN scalar filter value")));
        Node* rhs=(Node*)list_nth(tree,6);
        if(!rhs||(expressions?(!IsA(rhs,Const)&&!IsA(rhs,Param)):!IsA(rhs,Integer)))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake ANN scalar filter RHS")));
        return;
    }
    if((kind!=LAKE_ANN_SCALAR_AND&&kind!=LAKE_ANN_SCALAR_OR&&kind!=LAKE_ANN_SCALAR_NOT)||
        list_length(tree)!=2||!lsecond(tree)||!IsA(lsecond(tree),List))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake ANN scalar boolean node")));
    List* children=(List*)lsecond(tree);int count=list_length(children);
    if((kind==LAKE_ANN_SCALAR_NOT?count!=1:count<2)||count>LAKE_ANN_SCALAR_MAX_LEAVES)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake ANN scalar boolean arity")));
    ListCell* cell=NULL;
    foreach(cell,children)LakeAnnCheckFilterShape((Node*)lfirst(cell),expressions,depth+1,nodes,leaves);
}

static List* LakeAnnSerializeFilter(List* tree,List** expressions)
{
    int kind=intVal(linitial(tree));
    if(kind==LAKE_ANN_SCALAR_LEAF){
        List* result=NIL;
        for(int i=0;i<6;++i)result=lappend(result,copyObject(list_nth(tree,i)));
        result=lappend(result,makeInteger(list_length(*expressions)));
        *expressions=lappend(*expressions,copyObject(list_nth(tree,6)));
        return result;
    }
    List* children=NIL;ListCell* cell=NULL;
    foreach(cell,(List*)lsecond(tree))children=lappend(children,LakeAnnSerializeFilter((List*)lfirst(cell),expressions));
    return list_make2(makeInteger(kind),children);
}

static ForeignScan* LakeInlineGetForeignPlan(PlannerInfo* root, RelOptInfo* baserel, Oid relationOid,
    ForeignPath* path, List* tlist, List* clauses, Plan*)
{
    Expr* locator = NULL;
    ListCell* cell = NULL;
    foreach(cell, tlist) {
        TargetEntry* entry = (TargetEntry*)lfirst(cell);
        if (LakeIsLocatorPlaceholder(entry->expr, baserel->relid)) {
            if (locator != NULL && !equal(locator, entry->expr))
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("multiple native lake locator placeholders")));
            locator = entry->expr;
        }
    }
    List* scanList = NIL;
    List* privateData = list_make3(makeInteger(LAKE_SCAN_PRIVATE_VERSION), makeInteger(0), makeInteger(0));
    if (locator != NULL) {
        Relation relation = heap_open(relationOid, NoLock);
        TupleDesc desc = RelationGetDescr(relation);
        for (int i = 0; i < desc->natts; ++i) {
            Form_pg_attribute attr = &desc->attrs[i];
            /* A host hole retains its resno, never an invalid-type Var. */
            Expr* column = attr->attisdropped?(Expr*)makeNullConst(INT4OID,-1,InvalidOid):
                (Expr*)makeVar(baserel->relid, i + 1, attr->atttypid, attr->atttypmod, attr->attcollation, 0);
            scanList = lappend(scanList, makeTargetEntry((Expr*)column, i + 1, pstrdup(NameStr(attr->attname)), false));
        }
        Var* whole = makeWholeRowVar(planner_rt_fetch(baserel->relid, root), baserel->relid, 0, false);
        scanList = lappend(scanList, makeTargetEntry((Expr*)whole, desc->natts + 1, pstrdup("og_lake_old_row"), false));
        scanList = lappend(scanList, makeTargetEntry((Expr*)copyObject(locator), desc->natts + 2, pstrdup("og_lake_row_locator"), false));
        privateData = list_make3(makeInteger(LAKE_SCAN_PRIVATE_VERSION), makeInteger(desc->natts + 1), makeInteger(desc->natts + 2));
        heap_close(relation, NoLock);
    }
    privateData = lappend(privateData, LakeInlineExplainValues((LakeInlineEstimate*)baserel->fdw_private));
    List* indexData=NIL;List* expressions=NIL;
    bool smpPath=list_length(path->fdw_private)==1&&IsA(linitial(path->fdw_private),String)&&
        strcmp(strVal(linitial(path->fdw_private)),"lake_smp_v1")==0;
    if(path->fdw_private!=NIL&&!smpPath) {
        int count=list_length(path->fdw_private);
        if(count!=7&&count!=8&&count!=9)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake inline index path")));
        for(int i=0;i<6;++i)indexData=lappend(indexData,copyObject(list_nth(path->fdw_private,i)));
        expressions=list_make1(copyObject(list_nth(path->fdw_private,6)));
        if(count>=8)indexData=lappend(indexData,copyObject(list_nth(path->fdw_private,7)));
        if(count==9){
            List* filter=(List*)list_nth(path->fdw_private,8);int nodes=0,leaves=0;
            LakeAnnCheckFilterShape((Node*)filter,true,1,&nodes,&leaves);
            indexData=lappend(indexData,LakeAnnSerializeFilter(filter,&expressions));
        }
    }
    privateData=lappend(privateData,indexData);
    ForeignScan* plan=make_foreignscan(tlist, extract_actual_clauses(clauses, false), baserel->relid,
        expressions, privateData, scanList, NIL, NULL, EXEC_ON_DATANODES);
    plan->scan.plan.vec_output=LakeVectorPlanAllowed(root,relationOid,path,indexData,scanList,
        ((LakeInlineEstimate*)baserel->fdw_private)->smp_binding);
    return plan;
}

static List* LakeInlinePlanIndex(ForeignScan* plan)
{
    (void)LakeInlinePlanEstimates(plan);
    List* data=(List*)list_nth(plan->fdw_private,4);
    if(data==NIL)return NIL;
    if(!IsA(data,List)||(list_length(data)!=6&&list_length(data)!=7&&list_length(data)!=8)||!IsA(linitial(data),String))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake inline index plan")));
    for(int i=1;i<Min(7,list_length(data));++i)if(!IsA(list_nth(data,i),Integer))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake inline index plan value")));
    int mode=intVal(list_nth(data,5));bool ann=mode>=11&&mode<=13;
    if((ann&&((list_length(data)!=7&&list_length(data)!=8)||intVal(list_nth(data,6))<1||intVal(list_nth(data,6))>1000))||
        (!ann&&list_length(data)!=6))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake ANN candidate limit")));
    if(list_length(data)==8){int nodes=0,leaves=0;
        LakeAnnCheckFilterShape((Node*)list_nth(data,7),false,1,&nodes,&leaves);
    }
    return data;
}

/* Build owned runtime nodes and retain only native RHS ExprState handles. */
static LakeAnnScalarExpr* LakeAnnInitFilter(ForeignScanState* node,LakeInlineScanState* state,List* tree)
{
    auto* result=(LakeAnnScalarExpr*)palloc0(sizeof(LakeAnnScalarExpr));result->kind=intVal(linitial(tree));
    if(result->kind!=LAKE_ANN_SCALAR_LEAF){
        List* children=(List*)lsecond(tree);result->child_count=list_length(children);
        result->children=(LakeAnnScalarExpr**)palloc0(sizeof(LakeAnnScalarExpr*)*result->child_count);
        ListCell* cell=NULL;int i=0;
        foreach(cell,children)result->children[i++]=LakeAnnInitFilter(node,state,(List*)lfirst(cell));
        return result;
    }
    int index=state->ann_filter_leaf_count++;
    if(index>=LAKE_ANN_SCALAR_MAX_LEAVES)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("lake ANN scalar leaf budget exceeded")));
    LakeAnnFilterLeafState* leaf=&state->ann_filter_leaves[index];leaf->expression=result;
    result->leaf.column=intVal(lsecond(tree));leaf->column=result->leaf.column;leaf->operator_oid=(Oid)intVal(lthird(tree));
    Oid type=(Oid)intVal(lfourth(tree));leaf->null_mode=intVal(list_nth(tree,4));int array=intVal(list_nth(tree,5));
    int rhsIndex=intVal(list_nth(tree,6));ForeignScan* plan=(ForeignScan*)node->ss.ps.plan;
    if(rhsIndex!=index+1||rhsIndex>=list_length(plan->fdw_exprs)||array<0||array>1||leaf->null_mode<0||leaf->null_mode>2||
        result->leaf.column<1||result->leaf.column>state->binding->column_count||type==INT2OID||!LakeScalarIndexedType(type)||
        state->binding->columns[result->leaf.column-1].type!=type)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake ANN scalar filter binding")));
    Expr* rhs=(Expr*)list_nth(plan->fdw_exprs,rhsIndex);leaf->array=array!=0;
    if(!rhs||(!IsA(rhs,Const)&&!IsA(rhs,Param)))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake ANN scalar expression")));
    Oid rightType=leaf->array?get_element_type(exprType((Node*)rhs)):exprType((Node*)rhs);
    if(leaf->null_mode){
        if(leaf->array||leaf->operator_oid!=InvalidOid||!IsA(rhs,Const)||!((Const*)rhs)->constisnull||rightType!=type)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake ANN scalar NULL filter")));
    }else{
        Oid op=leaf->operator_oid,function=get_opcode(op);int strategy=get_op_opfamily_strategy(op,LakeScalarFamily(type));Oid leftType,familyRight;
        if(!OidIsValid(op)||op>=FirstNormalObjectId||!OidIsValid(function)||function>=FirstNormalObjectId||!func_strict(function)||
            !LakeScalarArgumentType(type,rightType)||strategy<1||strategy>5||(leaf->array&&strategy!=BTEqualStrategyNumber))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake ANN scalar comparison")));
        get_op_opfamily_properties(op,LakeScalarFamily(type),false,&strategy,&leftType,&familyRight);
        if(leftType!=type||familyRight!=rightType)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("lake ANN scalar operand types changed")));
        leaf->subtype=rightType;
    }
    MemoryContext previous=MemoryContextSwitchTo(node->ss.ps.state->es_query_cxt);
    leaf->rhs=ExecInitExpr(rhs,&node->ss.ps);MemoryContextSwitchTo(previous);return result;
}

/* The metadata/ExprStates survive in query memory; ScanKeys and their fmgr
 * allocations are bounded to one rescan and never accumulate there. */
static LakeAnnScalarExpr* LakeAnnRebuildFilter(LakeInlineScanState* state,List* tree,int* leafIndex)
{
    auto* result=(LakeAnnScalarExpr*)palloc0(sizeof(LakeAnnScalarExpr));result->kind=intVal(linitial(tree));
    if(result->kind==LAKE_ANN_SCALAR_LEAF){
        LakeAnnFilterLeafState* leaf=&state->ann_filter_leaves[(*leafIndex)++];
        result->leaf.column=leaf->column;leaf->expression=result;
    }else{
        List* children=(List*)lsecond(tree);result->child_count=list_length(children);
        result->children=(LakeAnnScalarExpr**)palloc0(sizeof(LakeAnnScalarExpr*)*result->child_count);
        ListCell* cell=NULL;int i=0;
        foreach(cell,children)result->children[i++]=LakeAnnRebuildFilter(state,(List*)lfirst(cell),leafIndex);
    }
    return result;
}

static void LakeAnnRefreshFilter(ForeignScanState* node,LakeInlineScanState* state)
{
    for(int i=0;i<state->ann_filter_leaf_count;++i){
        LakeAnnFilterLeafState* leaf=&state->ann_filter_leaves[i];ScanKeyData* key=&leaf->expression->leaf.key;
        bool absent=false;Datum value=ExecEvalExprSwitchContext(leaf->rhs,node->ss.ps.ps_ExprContext,&absent,NULL);
        if(leaf->null_mode){
            ScanKeyEntryInitialize(key,SK_ISNULL|(leaf->null_mode==1?SK_SEARCHNULL:SK_SEARCHNOTNULL),
                1,InvalidStrategy,InvalidOid,InvalidOid,InvalidOid,(Datum)0);
        }else{
            int strategy=get_op_opfamily_strategy(leaf->operator_oid,LakeScalarFamily(
                state->binding->columns[leaf->expression->leaf.column-1].type));
            /* Keep NULL arrays and NULL elements intact for the reader's SQL
             * three-valued evaluation. A rescan reevaluates every parameter. */
            ScanKeyEntryInitialize(key,(absent?SK_ISNULL:0)|(leaf->array?SK_SEARCHARRAY:0),
                1,strategy,leaf->subtype,InvalidOid,get_opcode(leaf->operator_oid),value);
        }
    }
}

static void LakeInlineIndexRescan(ForeignScanState* node,LakeInlineScanState* state)
{
    ScanKeyData key;
    if(state->ann_metric){
        MemoryContext previous=CurrentMemoryContext;
        PG_TRY();{
            if(state->ann_filter_metadata!=NIL){
                MemoryContextReset(state->ann_filter_context);state->ann_filter=NULL;MemoryContextSwitchTo(state->ann_filter_context);
                int leafIndex=0;state->ann_filter=LakeAnnRebuildFilter(state,state->ann_filter_metadata,&leafIndex);
                Assert(leafIndex==state->ann_filter_leaf_count);LakeAnnRefreshFilter(node,state);
            }
            bool absent=false;Datum value=ExecEvalExprSwitchContext(state->inline_key,node->ss.ps.ps_ExprContext,&absent,NULL);
            bool available=!state->stream_scan&&!state->ann_filter&&u_sess->attr.attr_sql.enable_lake_ann&&OidIsValid(state->inline_index_oid)&&
                LakeInlineAnnAvailable(state->binding,strVal(linitial(LakeInlinePlanIndex((ForeignScan*)node->ss.ps.plan))),
                    state->inline_index_oid,state->inline_column,state->ann_metric,LakeTxnBusinessSnapshot(state->transaction_table));
            state->ann_used=LakeInlineReaderUseAnn(state->reader,available?state->inline_index_oid:InvalidOid,
                state->inline_column,state->ann_metric,value,absent);
            if(state->partition_prune!=NULL&&state->last_partition_skipped!=NULL)
                LakeInlineReaderApplyFilePruning(state->reader,state->last_partition_skipped,state->binding->file_count);
            bool hasResidualQual=((ForeignScan*)node->ss.ps.plan)->scan.plan.qual!=NIL;
            LakeInlineReaderUseFileAnn(state->reader,strVal(linitial(LakeInlinePlanIndex((ForeignScan*)node->ss.ps.plan))),
                state->inline_column,state->ann_metric,value,absent,u_sess->attr.attr_sql.enable_lake_ann,state->ann_limit,
                state->ann_filter,LakeStreamWorkerAnnSources(state->stream_worker),hasResidualQual);
        }PG_CATCH();{MemoryContextSwitchTo(previous);PG_RE_THROW();}PG_END_TRY();
        MemoryContextSwitchTo(previous);return;
    }
    if(state->inline_null_mode) {
        int flags=SK_ISNULL|(state->inline_null_mode==1?SK_SEARCHNULL:SK_SEARCHNOTNULL);
        ScanKeyEntryInitialize(&key,flags,1,InvalidStrategy,InvalidOid,InvalidOid,InvalidOid,(Datum)0);
    } else {
        bool absent=false;Datum value=ExecEvalExprSwitchContext(state->inline_key,
            node->ss.ps.ps_ExprContext,&absent,NULL);
        int strategy;Oid leftType,rightType;
        /* Match native ExecIndexBuildScanKeys: the subtype belongs to the RHS,
         * not the indexed column. Cross-integer comparisons never narrow a key. */
        get_op_opfamily_properties(state->inline_operator,LakeScalarFamily(state->binding->columns[state->inline_column-1].type),false,
            &strategy,&leftType,&rightType);
        int flags=(absent?SK_ISNULL:0)|(state->inline_array?SK_SEARCHARRAY:0);
        ScanKeyEntryInitialize(&key,flags,1,strategy,rightType,InvalidOid,
            get_opcode(state->inline_operator),value);
    }
    if(OidIsValid(state->inline_index_oid))
        LakeInlineReaderUseIndex(state->reader,state->inline_index_oid,state->inline_column,&key);
    else LakeInlineReaderReScan(state->reader);
    if(state->partition_prune!=NULL&&state->last_partition_skipped!=NULL)
        LakeInlineReaderApplyFilePruning(state->reader,state->last_partition_skipped,state->binding->file_count);
    if(state->scalar_uuid)
        LakeInlineReaderUseFileScalar(state->reader,state->scalar_uuid,state->inline_column,&key,
            LakeStreamWorkerScalarSources(state->stream_worker));
}

static void LakeInlineClose(LakeInlineScanState* state)
{
    if (state != NULL) {
        LakeInlineReaderClose(state->reader);
        state->reader = NULL;
        if(state->stream_scan)LakeTxnReleaseStreamReadView(state->transaction_table);
        else LakeTxnReleaseReadView(state->transaction_table);
        LakeStreamWorkerEnd(state->stream_worker);state->stream_worker=NULL;
        state->transaction_table = NULL;
        if(state->ann_filter_context){MemoryContextDelete(state->ann_filter_context);state->ann_filter_context=NULL;state->ann_filter=NULL;}
        if(state->vector_row_context){MemoryContextDelete(state->vector_row_context);state->vector_row_context=NULL;}
        if(state->vector_pending_context){MemoryContextDelete(state->vector_pending_context);state->vector_pending_context=NULL;}
        state->vector_pending=NULL;
    }
}

/* Correlation inputs are filled by the host only when the outer tuple is
 * available. Inspect the complete FDW expression list, including scalar filter
 * leaves, without evaluating any RHS during executor initialization. */
static bool LakeInlineHasExecParam(Node* node,void* context)
{
    if(node==NULL)return false;
    if(IsA(node,Param))return ((Param*)node)->paramkind==PARAM_EXEC;
    return expression_tree_walker(node,(bool(*)())LakeInlineHasExecParam,context);
}

static void LakeInlineBeginImpl(ForeignScanState* node, int eflags)
{
    LakeCheckExecutorPermissions(node->ss.ps.state);
    node->fdw_state = NULL;
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY) return;
    bool streamScan=StreamThreadAmI()&&node->ss.ps.plan->dop>1;
    if(StreamThreadAmI()&&!streamScan)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake Stream scan requires a prebound parallel path")));
    EState* estate = node->ss.ps.state;
    MemoryContext previous = MemoryContextSwitchTo(estate->es_query_cxt);
    LakeInlineScanState* state = (LakeInlineScanState*)palloc0(sizeof(LakeInlineScanState));
    node->fdw_state = state;
    PG_TRY();
    {
        char** expectedVersions=NULL;state->stream_scan=streamScan;
        if(streamScan)state->stream_worker=LakeStreamOpenWorker(node,eflags,&state->transaction_table,&expectedVersions);
        else state->transaction_table = LakeTxnReadStatementTable(RelationGetRelid(node->ss.ss_currentRelation), estate->es_snapshot);
        state->binding = state->transaction_table->binding;
        if (state->binding->bound_snapshot_read) {
            if (IsA(node->ss.ps.plan, VecForeignScan) || streamScan ||
                LakeInlinePlanIndex((ForeignScan*)node->ss.ps.plan) != NIL ||
                intVal(lsecond(((ForeignScan*)node->ss.ps.plan)->fdw_private)) != 0 ||
                intVal(lthird(((ForeignScan*)node->ss.ps.plan)->fdw_private)) != 0)
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("bound-snapshot lake scan requires a serial row read plan")));
            LakeCheckBoundSnapshotScanVars(node, state->binding);
        }
        if(!IsA(node->ss.ps.plan,VecForeignScan)&&(streamScan||LakeInlinePlanIndex((ForeignScan*)node->ss.ps.plan)!=NIL)){
            if(!streamScan&&!LakeIxSmpExtendedVersionEnabled()){
                LakeRejectBinaryAcceleration(state->binding,"cached batch, SMP or index execution");
                LakeRejectUuidAcceleration(state->binding,"cached batch, SMP or index execution");
                LakeRejectJsonAcceleration(state->binding,"cached batch, SMP or index execution");
                LakeRejectNumericAcceleration(state->binding,"cached batch, SMP or index execution");
            }
            if(!streamScan&&(!LakePartitionComboVersionEnabled()||
               state->binding->has_added_columns||state->binding->has_dropped_columns||
               state->binding->retired_count>0||state->binding->future_count>0))
                LakeRejectIdentityPartitionIntersection(state->binding,"cached batch, SMP or index execution; replan serially");
        }
        if(IsA(node->ss.ps.plan,VecForeignScan)){
            const bool vectorSmp=streamScan?LakeStreamWorkerVectorSmpQualified(state->stream_worker):LakeVectorSmpVersionEnabled();
            if(!LakeVectorBatchVersionEnabled()||((streamScan||node->ss.ps.plan->dop>1)&&!vectorSmp))
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake batch scan requires SQL0.18 and DOP1")));
            if(!vectorSmp){
                LakeRejectBinaryAcceleration(state->binding,"cached batch, SMP or index execution");
                LakeRejectUuidAcceleration(state->binding,"cached batch, SMP or index execution");
                LakeRejectJsonAcceleration(state->binding,"cached batch, SMP or index execution");
                LakeRejectNumericAcceleration(state->binding,"cached batch, SMP or index execution");
                LakeRejectIdentityPartitionIntersection(state->binding,"cached batch, SMP or index execution; replan serially");
            }else{
                LakeRejectUuidAcceleration(state->binding,"cached batch, SMP or index execution");
                LakeRejectJsonAcceleration(state->binding,"cached batch, SMP or index execution");
            }
            if(!LakeVectorBindingSupported(state->binding,vectorSmp))
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("cached native lake batch scan layout is unsupported; replan serially")));
            if(LakeInlinePlanIndex((ForeignScan*)node->ss.ps.plan)!=NIL||
               intVal(lsecond(((ForeignScan*)node->ss.ps.plan)->fdw_private))!=0||
               intVal(lthird(((ForeignScan*)node->ss.ps.plan)->fdw_private))!=0)
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("invalid native lake batch scan layout")));
            TupleDesc hostDesc=node->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
            if(hostDesc->natts<=0||hostDesc->natts!=state->binding->column_count)
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("invalid native lake batch scan layout")));
            state->vector_extended=vectorSmp;
            state->vector_col_count=hostDesc->natts;
            state->vector_col_varlena=(bool*)palloc0(sizeof(bool)*state->vector_col_count);
            state->vector_col_wide=(bool*)palloc0(sizeof(bool)*state->vector_col_count);
            state->vector_col_sizes=(Size*)palloc0(sizeof(Size)*state->vector_col_count);
            state->vector_col_batch_bytes=(Size*)palloc0(sizeof(Size)*state->vector_col_count);
            for(int i=0;i<state->vector_col_count;++i){
                Form_pg_attribute attr=&hostDesc->attrs[i];
                if(attr->attisdropped||!LakeVectorType(attr->atttypid,vectorSmp))
                    ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("unsupported native lake batch column type")));
                if(attr->atttypid==TEXTOID||attr->atttypid==BYTEAOID||attr->atttypid==NUMERICOID){
                    state->vector_col_varlena[i]=true;
                    state->vector_has_varlena=true;
                }
            }
            state->vector_row_context=AllocSetContextCreate(estate->es_query_cxt,"Lake vector source row",ALLOCSET_DEFAULT_SIZES);
            state->vector_pending_context=AllocSetContextCreate(estate->es_query_cxt,"Lake vector pending row",ALLOCSET_DEFAULT_SIZES);
        }
        ForeignScan* plan = (ForeignScan*)node->ss.ps.plan;
        int businessColumns = LakeHostColumnCount(state->binding);
        (void)LakeInlinePlanEstimates(plan);
        if (intVal(lsecond(plan->fdw_private)) != 0 || intVal(lthird(plan->fdw_private)) != 0) {
            if (intVal(lsecond(plan->fdw_private)) != businessColumns + 1 ||
                intVal(lthird(plan->fdw_private)) != businessColumns + 2 ||
                node->ss.ss_ScanTupleSlot->tts_tupleDescriptor->natts != businessColumns + 2)
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("invalid native lake locator scan layout")));
            state->whole_row_slot = businessColumns + 1;
            state->locator_slot = businessColumns + 2;
        } else if (node->ss.ss_ScanTupleSlot->tts_tupleDescriptor->natts != businessColumns) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("invalid native lake business scan layout")));
        }
        state->command_limit = state->binding->bound_snapshot_read ? InvalidCommandId :
            (streamScan?LakeTxnBusinessSnapshot(state->transaction_table)->curcid:estate->es_snapshot->curcid);
        state->partition_prune=state->binding->bound_snapshot_read?NULL:LakePartitionPruneCreate(node,state->binding);
        state->partition_pending=state->partition_prune!=NULL;
        state->reader = LakeInlineReaderOpen(state->transaction_table,state->command_limit,false,false,expectedVersions,state->partition_pending);
        LakeStreamWorkerReady(state->stream_worker);
        List* indexData=LakeInlinePlanIndex(plan);
        if(indexData!=NIL) {
            if(list_length(plan->fdw_exprs)<1||(list_length(indexData)!=8&&list_length(plan->fdw_exprs)!=1))
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake inline index argument")));
            state->inline_index_oid=(Oid)intVal(lsecond(indexData));
            state->inline_column=intVal(lthird(indexData));
            state->inline_operator=(Oid)intVal(lfourth(indexData));
            Oid keyType=(Oid)intVal(list_nth(indexData,4));
            Expr* argument=(Expr*)linitial(plan->fdw_exprs);
            state->inline_null_mode=intVal(list_nth(indexData,5));
            Oid argumentType=exprType((Node*)argument);
            if(state->inline_null_mode>=11&&state->inline_null_mode<=13){
                state->ann_metric=state->inline_null_mode-10;state->ann_limit=(uint32)intVal(list_nth(indexData,6));
                if(state->inline_column<=0||state->inline_column>businessColumns||keyType!=VECTOROID||argumentType!=VECTOROID||
                    state->binding->columns[state->inline_column-1].type!=VECTOROID||
                    state->inline_operator!=LakeAnnOperator(state->ann_metric)||(!IsA(argument,Const)&&!IsA(argument,Param)))
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake DiskANN plan argument")));
            }else if(state->inline_null_mode) {
                if(state->inline_null_mode<1 || state->inline_null_mode>2 ||
                   state->inline_operator!=InvalidOid || !IsA(argument,Const) || !((Const*)argument)->constisnull ||
                   state->inline_column<=0 || state->inline_column>businessColumns ||
                   !LakeScalarIndexedType(keyType) || argumentType!=keyType ||
                   state->binding->columns[state->inline_column-1].type!=keyType)
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake inline index null predicate")));
            } else {
                Oid elementType=get_element_type(argumentType);
                state->inline_array=OidIsValid(elementType);
                if(state->inline_array)argumentType=elementType;
                if(keyType==INT2OID&&state->inline_array)
                    ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("INT2 scalar arrays require a separate contract")));
                int strategy=get_op_opfamily_strategy(state->inline_operator,LakeScalarFamily(keyType));
                if(state->inline_column<=0 || state->inline_column>businessColumns ||
                   !LakeScalarIndexedType(keyType) ||
                   state->binding->columns[state->inline_column-1].type!=keyType ||
                   (!IsA(argument,Const) && !IsA(argument,Param)) ||
                   !LakeScalarArgumentType(keyType,argumentType) ||
                   strategy<1 || strategy>5 || (state->inline_array && strategy!=BTEqualStrategyNumber) ||
                   get_opcode(state->inline_operator)>=FirstNormalObjectId)
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake inline index comparison")));
                Oid leftType,rightType;
                get_op_opfamily_properties(state->inline_operator,LakeScalarFamily(state->binding->columns[state->inline_column-1].type),false,
                    &strategy,&leftType,&rightType);
                if(leftType!=keyType || rightType!=argumentType)
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("lake inline index operand types changed")));
            }
            if(streamScan){
                if(state->ann_metric){
                    if(!LakeStreamWorkerAnnSources(state->stream_worker))
                        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("lake SMP ANN has no prebound descriptor")));
                }else{
                    if(!LakeStreamWorkerScalarSources(state->stream_worker))
                        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("lake SMP scalar index has no prebound descriptor")));
                    state->scalar_uuid=pstrdup(strVal(linitial(indexData)));
                }
                state->inline_index_oid=InvalidOid; /* Entire inline input belongs to participant0. */
            }
            if(!streamScan&&!state->ann_metric&&(!OidIsValid(state->inline_index_oid)||
                !LakeInlineIndexAvailable(state->binding,strVal(linitial(indexData)),state->inline_index_oid,
                    state->inline_column,LakeTxnBusinessSnapshot(state->transaction_table))))
                state->inline_index_oid=InvalidOid;
            if(!streamScan&&!state->ann_metric&&LakeHasIndexRootVersion())state->scalar_uuid=pstrdup(strVal(linitial(indexData)));
            state->inline_key=ExecInitExpr(argument,&node->ss.ps);
            if(list_length(indexData)==8){
                state->ann_filter_metadata=(List*)list_nth(indexData,7);
                state->ann_filter_context=AllocSetContextCreate(estate->es_query_cxt,"Lake ANN scalar parameters",ALLOCSET_DEFAULT_SIZES);
                MemoryContext caller=MemoryContextSwitchTo(state->ann_filter_context);
                state->ann_filter=LakeAnnInitFilter(node,state,state->ann_filter_metadata);
                MemoryContextSwitchTo(caller);
                if(list_length(plan->fdw_exprs)!=state->ann_filter_leaf_count+1)
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("unused lake ANN scalar RHS expressions")));
            }
        }
        if (!state->binding->bound_snapshot_read &&
            state->locator_slot == InvalidAttrNumber &&
            state->whole_row_slot == InvalidAttrNumber &&
            plan->scan.scanrelid > 0) {
            LakeProjectionCtx projCtx = {};
            projCtx.binding = state->binding;
            projCtx.scanrelid = plan->scan.scanrelid;
            projCtx.has_fdw_scan_tlist = (plan->fdw_scan_tlist != NIL);
            projCtx.business_columns = businessColumns;
            projCtx.projected = (bool*)palloc0(sizeof(bool) * state->binding->column_count);
            (void)LakeCollectProjectedWalker((Node*)plan->scan.plan.targetlist, &projCtx);
            (void)LakeCollectProjectedWalker((Node*)plan->scan.plan.qual, &projCtx);
            (void)LakeCollectProjectedWalker((Node*)plan->fdw_scan_tlist, &projCtx);
            (void)LakeCollectProjectedWalker((Node*)plan->fdw_exprs, &projCtx);
            if (!projCtx.need_all) {
                if (state->inline_key != NULL && state->inline_column > 0) {
                    int logical = LakeHostToLogical(state->binding, state->inline_column);
                    if (logical < 0 || logical >= state->binding->column_count)
                        projCtx.need_all = true;
                    else
                        projCtx.projected[logical] = true;
                }
                for (int i = 0; !projCtx.need_all && i < state->ann_filter_leaf_count; ++i) {
                    int logical = LakeHostToLogical(state->binding, state->ann_filter_leaves[i].column);
                    if (logical < 0 || logical >= state->binding->column_count)
                        projCtx.need_all = true;
                    else
                        projCtx.projected[logical] = true;
                }
            }
            if (projCtx.need_all) {
                pfree(projCtx.projected);
            } else {
                state->projected = projCtx.projected;
                LakeInlineReaderSetProjected(state->reader, state->projected, state->binding->column_count);
                if (IsA(node->ss.ps.plan, VecForeignScan) && state->vector_col_varlena != NULL) {
                    state->vector_has_varlena = false;
                    for (int i = 0; i < state->vector_col_count; ++i) {
                        if (!state->projected[i])
                            state->vector_col_varlena[i] = false;
                        else if (state->vector_col_varlena[i])
                            state->vector_has_varlena = true;
                    }
                }
            }
        }
        if (indexData != NIL) {
            /* Stream workers retain their prebound Begin/Ready protocol.
             * Serial correlated keys are initialized by the first executed
             * Iterate, or by the host's post-parameter-assignment ReScan. */
            state->index_pending=!streamScan&&(state->partition_pending||LakeInlineHasExecParam((Node*)plan->fdw_exprs,NULL));
            if(state->index_pending&&state->ann_metric)LakeInlineReaderEnableAnnRescanCache(state->reader);
            if(!state->index_pending)LakeInlineIndexRescan(node,state);
        }
        if(streamScan&&state->ann_metric){
            int files=LakeInlineReaderAnnFiles(state->reader);
            LakeStreamWorkerAnnReady(state->stream_worker,files,LakeInlineReaderPqFiles(state->reader),state->binding->file_count-files,LakeInlineReaderAnnCandidateRows(state->reader));
        }else if(streamScan){
            int files=LakeInlineReaderScalarFiles(state->reader);
            LakeStreamWorkerIndexReady(state->stream_worker,files,LakeInlineReaderBitmapFiles(state->reader),
                state->binding->file_count-files);
        }
        MemoryContextSwitchTo(previous);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(previous);
        LakeInlineClose(state);
        PG_RE_THROW();
    }
    PG_END_TRY();
}

static void LakeInlineBegin(ForeignScanState* node, int eflags)
{
    int fastScope = LakeEnterFastValidationScope();
    PG_TRY();
    {
        LakeInlineBeginImpl(node, eflags);
    }
    PG_CATCH();
    {
        LakeLeaveFastValidationScope(fastScope);
        PG_RE_THROW();
    }
    PG_END_TRY();
    LakeLeaveFastValidationScope(fastScope);
}

/* Caller has placed the N business values in the scan slot and selected the
 * per-tuple context. Both hidden values describe exactly that same row. */
static void LakeInlineFinishSlot(LakeInlineScanState* state, TupleTableSlot* slot,
    const LakeInlineRowIdentity* identity)
{
    if (state->locator_slot != InvalidAttrNumber) {
        HeapTuple business = heap_form_tuple(state->transaction_table->descriptor, slot->tts_values, slot->tts_isnull);
        slot->tts_values[state->whole_row_slot - 1] = HeapTupleGetDatum(business);
        slot->tts_isnull[state->whole_row_slot - 1] = false;
        LakeRowLocator locator;
        errno_t rc = memset_s(&locator, sizeof(locator), 0, sizeof(locator));
        securec_check(rc, "", "");
        locator.source = identity->source;
        locator.database_oid = u_sess->proc_cxt.MyDatabaseId;
        locator.relation_oid = state->binding->relation_oid;
        locator.server_oid = state->binding->server_oid;
        locator.schema_version = state->binding->schema_version;
        locator.identity = state->binding->identity;
        locator.read_snapshot = state->binding->snapshot_id;
        locator.transaction_nonce = LakeTxnNonce();
        locator.producer_command_id = state->command_limit;
        if (identity->local_token == 0) {
            locator.row_id=identity->row_id;locator.begin_snapshot=identity->begin_snapshot;
            if(identity->source==LAKE_LOCATOR_INLINE)locator.inline_oid=identity->inline_oid;
            else{locator.file_id=identity->file_id;locator.file_row_position=identity->file_position;}
        }else locator.local_version_token=identity->local_token;
        slot->tts_values[state->locator_slot - 1] = PointerGetDatum(LakeEncodeRowLocator(&locator));
        slot->tts_isnull[state->locator_slot - 1] = false;
    }
    LakeStreamWorkerFirstRow(state->stream_worker);
    ++state->returned;LakeStreamWorkerRow(state->stream_worker);
    ExecStoreVirtualTuple(slot);
}

static TupleTableSlot* LakeInlineIterate(ForeignScanState* node)
{
    LakeInlineScanState* state = (LakeInlineScanState*)node->fdw_state;
    TupleTableSlot* slot = node->ss.ss_ScanTupleSlot;
    ExecClearTuple(slot);
    if (state == NULL) return slot;
    MemoryContext previous = MemoryContextSwitchTo(node->ss.ps.ps_ExprContext->ecxt_per_tuple_memory);
    PG_TRY();
    {
        if(state->partition_pending){
            MemoryContextSwitchTo(node->ss.ps.state->es_query_cxt);
            const bool* skipped=LakePartitionPruneRefresh(state->partition_prune,node->ss.ps.ps_ExprContext);
            if(state->binding->file_count>0){
                if(!state->last_partition_skipped)
                    state->last_partition_skipped=(bool*)palloc(sizeof(bool)*state->binding->file_count);
                memcpy(state->last_partition_skipped,skipped,sizeof(bool)*state->binding->file_count);
            }
            LakeInlineReaderApplyFilePruning(state->reader,skipped,state->binding->file_count);
            state->partition_pending=false;
            MemoryContextSwitchTo(node->ss.ps.ps_ExprContext->ecxt_per_tuple_memory);
        }
        if(state->index_pending){
            /* ForeignNext enters in per-tuple memory; initial ScanKey/fmgr
             * state must have the same query lifetime as Begin setup. */
            MemoryContextSwitchTo(node->ss.ps.state->es_query_cxt);
            LakeInlineIndexRescan(node,state);
            state->index_pending=false;
            MemoryContextSwitchTo(node->ss.ps.ps_ExprContext->ecxt_per_tuple_memory);
        }
        LakeInlineRowIdentity identity;
        Datum* values=(Datum*)palloc(sizeof(Datum)*state->binding->column_count);
        bool* nulls=(bool*)palloc(sizeof(bool)*state->binding->column_count);
        if (LakeInlineReaderNext(state->reader, values, nulls, &identity)){
            LakeScatterHostValues(state->binding,state->transaction_table->descriptor,values,nulls,slot->tts_values,slot->tts_isnull);
            LakeInlineFinishSlot(state, slot, &identity);
        }
        pfree(values);pfree(nulls);
        MemoryContextSwitchTo(previous);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(previous);
        PG_RE_THROW();
    }
    PG_END_TRY();
    return slot;
}

/* Normalize varlena datums while vector_row_context is alive. VarBuf->Append
 * copies flattened bytes before vector_row_context is reset after the batch. */
static inline Size LakeVectorRowBytes(const LakeInlineScanState* state,Datum* values,const bool* nulls)
{
    const int ncols=state->vector_col_count;
    Size bytes=0;
    if(!state->vector_has_varlena){
        for(int i=0;i<ncols;++i)
            if(!nulls[i])bytes+=sizeof(Datum);
        return bytes;
    }
    Size* colSizes=state->vector_col_sizes;
    for(int i=0;i<ncols;++i){
        if(nulls[i]){
            colSizes[i]=0;
            continue;
        }
        Size added;
        if(state->vector_col_varlena[i]){
            Pointer ptr=DatumGetPointer(values[i]);
            if(unlikely(VARATT_IS_EXTENDED(ptr))){
                values[i]=PointerGetDatum(PG_DETOAST_DATUM_PACKED(values[i]));
                ptr=DatumGetPointer(values[i]);
            }
            Size rawLen=VARSIZE_ANY(ptr);
            colSizes[i]=rawLen;
            added=MAXALIGN(rawLen);
        }else{
            colSizes[i]=0;
            added=sizeof(Datum);
        }
        if(unlikely(added>LAKE_VECTOR_PAYLOAD_BYTES||bytes>LAKE_VECTOR_PAYLOAD_BYTES-added))
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake batch source row exceeds 8 MiB payload limit")));
        bytes+=added;
    }
    return bytes;
}

static VectorBatch* LakeInlineIterateVector(VecForeignScanState* node)
{
    LakeInlineScanState* state=(LakeInlineScanState*)node->fdw_state;
    VectorBatch* batch=node->m_pScanBatch;
    if(state==NULL)return NULL;
    if(state->vector_row_context==NULL||state->vector_pending_context==NULL||
       state->locator_slot!=InvalidAttrNumber||state->inline_key!=NULL||
       batch->m_cols!=state->binding->column_count||
       batch->m_cols!=state->vector_col_count)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("invalid native lake batch scan layout")));
    batch->Reset(true);batch->ResetSelection(true);batch->m_checkSel=false;
    /* Reclaim oversized VarBuf slabs after wide batches so alternating wide
     * columns never retain an 8 MiB peak, while keeping normal 16 KiB buffers. */
    if(state->vector_has_varlena){
        for(int i=0;i<batch->m_cols;++i){
            state->vector_col_batch_bytes[i]=0;
            if(state->vector_col_wide[i]){
                if(batch->m_arr[i].m_buf!=NULL)batch->m_arr[i].m_buf->DeInit();
                state->vector_col_wide[i]=false;
            }
        }
    }
    TupleDesc descriptor=state->transaction_table->descriptor;
    Datum* values=node->m_values;bool* nulls=node->m_nulls;Size payload=0;
    Size contextPayload=0;
    MemoryContext caller=CurrentMemoryContext;
    PG_TRY();
    {
        if(state->partition_pending){
            MemoryContextSwitchTo(node->ss.ps.state->es_query_cxt);
            const bool* skipped=LakePartitionPruneRefresh(state->partition_prune,node->ss.ps.ps_ExprContext);
            if(state->binding->file_count>0){
                if(!state->last_partition_skipped)
                    state->last_partition_skipped=(bool*)palloc(sizeof(bool)*state->binding->file_count);
                memcpy(state->last_partition_skipped,skipped,sizeof(bool)*state->binding->file_count);
            }
            LakeInlineReaderApplyFilePruning(state->reader,skipped,state->binding->file_count);
            state->partition_pending=false;
        }
        MemoryContextReset(state->vector_row_context);
        MemoryContextSwitchTo(state->vector_row_context);
        while(batch->m_rows<BatchMaxSize){
            CHECK_FOR_INTERRUPTS();
            if(state->vector_pending==NULL){
                bool needFallback=false,eos=false;
                int filled=LakeInlineReaderFillVectorBatch(state->reader,batch,
                    &payload,LAKE_VECTOR_PAYLOAD_BYTES,
                    state->vector_has_varlena?state->vector_col_batch_bytes:NULL,
                    &needFallback,&eos);
                if(filled>0){
                    if(unlikely(state->stream_worker!=NULL&&!state->vector_first_row)){
                        LakeStreamWorkerFirstRow(state->stream_worker);
                        state->vector_first_row=true;
                    }
                    state->returned+=(uint64)filled;
                }
                if(!needFallback)
                    break;
            }
            if(unlikely(contextPayload>=512*1024)){
                MemoryContextReset(state->vector_row_context);
                contextPayload=0;
            }
            LakeInlineRowIdentity identity;
            bool pending=state->vector_pending!=NULL;
            if(pending){
                heap_deform_tuple(state->vector_pending,descriptor,values,nulls);
                identity=state->vector_pending_identity;
            }else if(!LakeInlineReaderNext(state->reader,values,nulls,&identity))break;
            Size rowBytes=LakeVectorRowBytes(state,values,nulls);
            if(batch->m_rows>0&&rowBytes>LAKE_VECTOR_PAYLOAD_BYTES-payload){
                if(pending)
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake batch pending row did not fit an empty batch")));
                MemoryContextSwitchTo(state->vector_pending_context);
                state->vector_pending=heap_form_tuple(descriptor,values,nulls);
                state->vector_pending_identity=identity;
                MemoryContextSwitchTo(state->vector_row_context);
                break;
            }
            if(unlikely(state->stream_worker!=NULL&&!state->vector_first_row)){
                LakeStreamWorkerFirstRow(state->stream_worker);
                state->vector_first_row=true;
            }
            const int row=batch->m_rows;
            const int ncols=batch->m_cols;
            if(!state->vector_has_varlena){
                for(int i=0;i<ncols;++i){
                    ScalarVector* vector=&batch->m_arr[i];
                    if(nulls[i])vector->SetNull(row);
                    else vector->m_vals[row]=values[i];
                    ++vector->m_rows;
                }
            }else{
                for(int i=0;i<ncols;++i){
                    ScalarVector* vector=&batch->m_arr[i];
                    if(nulls[i])vector->SetNull(row);
                    else if(state->vector_col_varlena[i]){
                        Size sz=state->vector_col_sizes[i];
                        vector->m_vals[row]=PointerGetDatum(vector->AddVars((const char*)DatumGetPointer(values[i]),(int)sz));
                        state->vector_col_batch_bytes[i]+=MAXALIGN(sz);
                    }else vector->m_vals[row]=values[i];
                    ++vector->m_rows;
                }
            }
            ++batch->m_rows;++state->returned;payload+=rowBytes;contextPayload+=rowBytes;
            if(pending){
                state->vector_pending=NULL;
                MemoryContextReset(state->vector_pending_context);
            }
        }
        if(state->vector_has_varlena){
            for(int i=0;i<batch->m_cols;++i){
                if(state->vector_col_batch_bytes[i]>VAR_BUF_SIZE)state->vector_col_wide[i]=true;
                state->vector_col_batch_bytes[i]=0;
            }
        }
        MemoryContextSwitchTo(caller);
        MemoryContextReset(state->vector_row_context);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(caller);
        /* Reader's SDK callbacks/resource owner and FDW End close cross-call
         * resources; query-owned converter contexts also unwind on ERROR. */
        PG_RE_THROW();
    }
    PG_END_TRY();
    if(batch->m_rows>0){
        if(state->stream_worker!=NULL)LakeStreamWorkerAddRows(state->stream_worker,(uint64)batch->m_rows);
        ++state->vector_batches;
        state->vector_peak_bytes=Max(state->vector_peak_bytes,(uint64)payload);
    }
    return batch->m_rows==0?NULL:batch;
}

static void LakeInlineReScan(ForeignScanState* node)
{
    if(StreamThreadAmI()&&node->ss.ps.plan->dop>1)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake SMP first slice does not support rescans")));
    LakeInlineScanState* state = (LakeInlineScanState*)node->fdw_state;
    ExecClearTuple(node->ss.ss_ScanTupleSlot);
    if(state!=NULL) {
        if(state->vector_row_context){MemoryContextReset(state->vector_row_context);++state->vector_rescans;}
        if(state->vector_pending_context)MemoryContextReset(state->vector_pending_context);
        state->vector_pending=NULL;
        if(state->partition_prune){
            state->partition_pending=true;
            if(state->inline_key!=NULL)state->index_pending=true;
            LakeInlineReaderReScan(state->reader);
        }else if(state->inline_key!=NULL){
            LakeInlineIndexRescan(node,state);
            state->index_pending=false;
        }else LakeInlineReaderReScan(state->reader);
    }
}

static void LakeInlineEnd(ForeignScanState* node)
{
    LakeInlineClose((LakeInlineScanState*)node->fdw_state);
    node->fdw_state = NULL;
}

static void LakeInlineExplain(ForeignScanState* node, ExplainState* explain)
{
    LakeStreamExplain(node->ss.ps.plan,explain);
    ExplainPropertyText("Native Lake", "native inline prototype", explain);
    bool vectorPlan=IsA(node->ss.ps.plan,VecForeignScan);
    ExplainPropertyText("Lake Batch Delivery",vectorPlan?"native VecForeignScan / owned scalar VectorBatch":"row ForeignScan",explain);
    List* indexPlan=LakeInlinePlanIndex((ForeignScan*)node->ss.ps.plan);
    ExplainPropertyText("Planned Inline Access",LakeInlineAccessLabel(indexPlan==NIL?InvalidOid:(Oid)intVal(lsecond(indexPlan))),explain);
    bool annPlan=indexPlan!=NIL&&intVal(list_nth(indexPlan,5))>=11;
    if(annPlan)ExplainPropertyText("ANN Semantics","approximate inline/file candidates; uncovered/private exact scan; native distance rerank",explain);
    if(annPlan&&list_length(indexPlan)==8){
        List* filter=(List*)list_nth(indexPlan,7);
        bool single=intVal(linitial(filter))==LAKE_ANN_SCALAR_LEAF;
        ExplainPropertyText("ANN Scalar Filter",single?
            "native scalar column scan to physical-position mask; rejected nodes remain navigable; inline complete scan":
            "native scalar boolean tree to physical-position mask; SQL three-valued logic; rejected nodes remain navigable; inline complete scan",explain);
    }
    ExplainPropertyText("Planned File Access",annPlan?"DiskANN Parquet candidates with coverage validation":indexPlan!=NIL&&LakeHasIndexRootVersion()?
        "scalar Parquet candidates with coverage validation":"complete SDK scan",explain);
    List* estimates = LakeInlinePlanEstimates((ForeignScan*)node->ss.ps.plan);
    ExplainPropertyText("Row Estimate", strVal(linitial(estimates)), explain);
    const char* labels[] = {"Visible Row Baseline", "Stored Version Estimate", "Inline Data Pages",
        "Inline Read Cost", "Version Filter Cost", "Visible Row And Qual Cost", "Target Cost", "Base Planner Startup Cost"};
    for (int i = 0; i < 8; ++i)
        ExplainPropertyFloat(labels[i], floatVal(list_nth(estimates, i + 1)), 2, explain);
    ExplainPropertyText("Cost Model",node->ss.ps.plan->dop>1?
        "source-exclusive whole-file task maximum; inline owner0; native units; not calibrated":
        "source-exclusive file/inline ledger; raw graph statistics may be unknown; native units; not calibrated",explain);
    LakeInlineScanState* state = (LakeInlineScanState*)node->fdw_state;
    if (state != NULL && state->binding != NULL) {
        if(state->partition_prune){
            ExplainPropertyLong("Lake Partition Files Skipped",LakePartitionPruneSkipped(state->partition_prune),explain);
            ExplainPropertyLong("Lake Partition Files Admitted",LakeInlineReaderAdmittedFiles(state->reader),explain);
            ExplainPropertyText("Lake Partition Proof","native predicate_refuted_by; original row quals retained",explain);
        }
        if(vectorPlan){
            ExplainPropertyLong("Lake Vector Batches",(long)state->vector_batches,explain);
            ExplainPropertyLong("Lake Vector Source Rows",(long)state->returned,explain);
            ExplainPropertyInteger("Lake Vector Peak Payload Bytes",state->vector_peak_bytes,explain);
            ExplainPropertyLong("Lake Vector Rescans",(long)state->vector_rescans,explain);
            ExplainPropertyInteger("Lake Vector Row Capacity",BatchMaxSize,explain);
            ExplainPropertyInteger("Lake Vector Payload Capacity",LAKE_VECTOR_PAYLOAD_BYTES,explain);
        }
        ExplainPropertyText("Actual Inline Access",LakeInlineAccessLabel(state->inline_key==NULL||(state->ann_metric&&!state->ann_used)?InvalidOid:state->inline_index_oid),explain);
        int totalFiles=state->binding->file_count;
        int admittedFiles=LakeInlineReaderAdmittedFiles(state->reader);
        int prunedFiles=Max(0,totalFiles-admittedFiles);
        int scalarFiles=LakeInlineReaderScalarFiles(state->reader),annFiles=LakeInlineReaderAnnFiles(state->reader);
        int bitmapFiles=LakeInlineReaderBitmapFiles(state->reader);
        const char* actualAccess=(admittedFiles==0&&totalFiles>0)?"partition pruned (no admitted files)":
            annFiles?"verified DiskANN Parquet candidates":bitmapFiles?"verified bitmap Parquet candidates":
            scalarFiles?"verified sorted Parquet candidates":"complete SDK scan";
        ExplainPropertyText("Actual File Access",actualAccess,explain);
        if(prunedFiles>0)ExplainPropertyInteger("Lake Partition Files Pruned",prunedFiles,explain);
        ExplainPropertyInteger("Verified Bitmap File Coverage",bitmapFiles,explain);
        ExplainPropertyInteger("Verified ANN File Coverage",annFiles,explain);
        ExplainPropertyInteger("Verified PQ File Coverage",LakeInlineReaderPqFiles(state->reader),explain);
        if(annFiles)ExplainPropertyLong("ANN Candidate Positions Admitted",(long)LakeInlineReaderAnnCandidateRows(state->reader),explain);
        LakeAnnRescanStats cache=LakeInlineReaderAnnRescanStats(state->reader);
        if(state->ann_used||annFiles||cache.hits||cache.misses||cache.loads||cache.evictions||cache.invalidations){
            ExplainPropertyLong("ANN Rescan Cache Hits",(long)cache.hits,explain);
            ExplainPropertyLong("ANN Rescan Cache Misses",(long)cache.misses,explain);
            ExplainPropertyLong("ANN Rescan Cache Loads",(long)cache.loads,explain);
            ExplainPropertyLong("ANN Rescan Cache Evictions",(long)cache.evictions,explain);
            ExplainPropertyLong("ANN Rescan Cache Invalidations",(long)cache.invalidations,explain);
            ExplainPropertyLong("ANN Rescan Fresh LUTs",(long)cache.queries,explain);
            ExplainPropertyLong("ANN Rescan Retained Bytes",(long)cache.bytes,explain);
            ExplainPropertyLong("ANN Rescan Peak Bytes",(long)cache.peak,explain);
        }
        ExplainPropertyInteger("Verified Scalar File Coverage",scalarFiles,explain);
        ExplainPropertyInteger("Complete File Fallback Inputs",Max(0,admittedFiles-bitmapFiles-scalarFiles-annFiles),explain);
        if(list_length(estimates)>=10&&intVal(list_nth(estimates,9))>0)
            ExplainPropertyInteger("Planner Unknown Graph Files",intVal(list_nth(estimates,9)),explain);
        ExplainPropertyText("Lake Snapshot", psprintf("%lld", (long long)state->binding->snapshot_id), explain);
        ExplainPropertyInteger("Inline Inputs", LakeInlineReadSourceCount(state->binding), explain);
        ExplainPropertyInteger("Local File Inputs", state->binding->file_count, explain);
    }
}

void LakeInstallInlineCallbacks(FdwRoutine* routine)
{
    routine->GetForeignRelSize = LakeInlineGetForeignRelSize;
    routine->GetForeignPaths = LakeInlineGetForeignPaths;
    routine->GetForeignPlan = LakeInlineGetForeignPlan;
    routine->BeginForeignScan = LakeInlineBegin;
    routine->IterateForeignScan = LakeInlineIterate;
    routine->VecIterateForeignScan = LakeInlineIterateVector;
    routine->ReScanForeignScan = LakeInlineReScan;
    routine->EndForeignScan = LakeInlineEnd;
    routine->ExplainForeignScan = LakeInlineExplain;
    LakeInstallAnalyzeCallbacks(routine);
}
