/* Restricted top-level DROP COLUMN, reusing the reviewed native
 * definition claim/child ownership protocol. No data rewrite or new SDK ABI. */
/*
Copyright 2018-2025 Stichting DuckDB Foundation

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
#include "postgres.h"
#include "lake_column_layout.h"
#include "lake_column_default.h"
#include "lake_types.h"
#include "access/genam.h"
#include "catalog/index.h"
#include "lake_inline_reader.h"
#include "lake_txn_private.h"
#include "lake_create_publish.h"
#include "access/heapam.h"
#include "access/og_lake.h"
#include "catalog/pg_inherits_fn.h"
#include "commands/extension.h"
#include "executor/spi.h"
#include "knl/knl_variable.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "storage/lmgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"

#include "nodes/nodeFuncs.h"

extern void LakeTxnForgetReadOnlyTableForRename(Oid relationOid);
struct LakeDropColumnPlan {
    MemoryContext context;LakeTxnState* state;LakeInlineBinding* original;
    int column;bool noop;
};
static void LakeCheckDropFieldDependencies(LakeInlineBinding* binding,int column,Relation host,Snapshot snapshot)
{
    int64 id=binding->columns[column].column_id;
    Oid types[3]={INT8OID,INT8OID,UUIDOID};
    Datum args[3]={Int64GetDatum(LakePermanentTableId(binding)),Int64GetDatum(id),
        DirectFunctionCall1(uuid_in,CStringGetDatum(binding->table_uuid))};
    LakeMetaRows* rows=LakeMetadataQuery(psprintf("SELECT EXISTS(SELECT 1 FROM %s p JOIN %s c USING(partition_id) "
        "WHERE p.table_id=$1 AND c.table_id=$1 AND c.column_id=$2) OR EXISTS(SELECT 1 FROM %s WHERE table_id=$1)",
        LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_partition_info"),
        LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_partition_column"),
        LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_sort_info")),
        2,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    LakeMetadataRequireCount(rows,1,"retained partition or sort dependency");
    if(DatumGetBool(LakeMetadataValue(rows,0,1,BOOLOID)))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake DROP COLUMN with retained partition key or sort history requires its origin-field adapter")));
    rows=LakeMetadataQuery("SELECT EXISTS(SELECT 1 FROM og_lake_internal.index_definition "
        "WHERE table_uuid=$3 AND column_id=$2 AND state='ACTIVE')",3,types,args,NULL,snapshot,true,SPI_OK_SELECT);
    LakeMetadataRequireCount(rows,1,"dropped native index field dependency");
    if(DatumGetBool(LakeMetadataValue(rows,0,1,BOOLOID)))
        ereport(ERROR,(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),errmsg("active lake index depends on dropped column")));
    List* indexes=RelationGetIndexList(host);ListCell* cell=NULL;
    foreach(cell,indexes){
        Relation index=index_open(lfirst_oid(cell),AccessShareLock);bool used=false;
        for(int n=0;n<index->rd_index->indnatts;++n)
            if(index->rd_index->indkey.values[n]==binding->columns[column].output_attnum)used=true;
        /* Expression/partial native host indexes are outside this slice; do
         * not accidentally let an AUTO dependency disappear under RESTRICT. */
        used|=RelationGetIndexExpressions(index)!=NIL||RelationGetIndexPredicate(index)!=NIL;
        index_close(index,NoLock);
        if(used)ereport(ERROR,(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),errmsg("host index depends on dropped lake column")));
    }
    list_free(indexes);
}
void* LakeNativePrepareDropColumn(Relation relation,List* commands)
{
    if(!LakeIsNativeForeignRelation(relation)||!LakeDropColumnVersionEnabled())return NULL;
    LakeRejectNativePartitionDDL(relation,"DROP COLUMN");
    if(list_length(commands)!=1)return NULL;
    AlterTableCmd* command=(AlterTableCmd*)linitial(commands);
    if(command->subtype!=AT_DropColumn)return NULL;
    if(!command->name||!*command->name||command->behavior!=DROP_RESTRICT||command->def||
       command->is_first||command->after_name||command->recursing)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake DROP requires one top-level scalar RESTRICT column")));
    if(!pg_class_ownercheck(RelationGetRelid(relation),GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER,ACL_KIND_CLASS,RelationGetRelationName(relation));
    if(u_sess->utils_cxt.XactIsoLevel!=XACT_READ_COMMITTED)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake DROP COLUMN requires READ COMMITTED")));
    if(u_sess->attr.attr_common.XactReadOnly)
        ereport(ERROR,(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),errmsg("cannot drop lake column in a read-only transaction")));
    if(creating_extension||RelationIsPartitioned(relation)||relation->rd_rel->relpersistence!=RELPERSISTENCE_PERMANENT||
       find_inheritance_children(RelationGetRelid(relation),NoLock)!=NIL||
       LakeTxnFindCreatedTable(RelationGetRelid(relation))!=NULL||!LakeNativeProtectedCreate(RelationGetRelid(relation)))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake DROP COLUMN requires a committed ordinary protected CREATE table")));
    AttrNumber attnum=get_attnum(RelationGetRelid(relation),command->name);
    if(attnum<=0){
        if(!command->missing_ok)ereport(ERROR,(errcode(ERRCODE_UNDEFINED_COLUMN),errmsg("native lake DROP column does not exist")));
        LakeDropColumnPlan* noop=(LakeDropColumnPlan*)palloc0(sizeof(LakeDropColumnPlan));noop->noop=true;return noop;
    }
    Form_pg_attribute attr=&RelationGetDescr(relation)->attrs[attnum-1];
    if(attr->attisdropped||attr->attinhcount||attr->atttypid==VECTOROID||!LakeNativeFlatTypmod(attr->atttypid,attr->atttypmod))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake DROP requires an ordinary flat scalar")));
    LakeTryLockCatalogRegistration();
    if(!OidIsValid(LakeMetadataOwner()))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake DROP requires a private metadata owner")));
    LakeTxnState* state=LakeTxnBeginColumnDefinition();
    LakeTxnForgetReadOnlyTableForRename(RelationGetRelid(relation));
    MemoryContext caller=CurrentMemoryContext;
    MemoryContext context=AllocSetContextCreate(caller,"Native lake drop column statement",ALLOCSET_DEFAULT_SIZES);
    Snapshot volatile snapshot=NULL;LakeDropColumnPlan* result=NULL;
    PG_TRY();{
        MemoryContextSwitchTo(context);snapshot=RegisterSnapshot(GetLatestSnapshot());
        result=(LakeDropColumnPlan*)palloc0(sizeof(LakeDropColumnPlan));result->context=context;result->state=state;
        result->original=LakeLoadCommittedInlineBinding(RelationGetRelid(relation),snapshot);
        result->column=LakeHostToLogical(result->original,attnum);
        if(result->column<0||result->original->read_only||!result->original->drop_columns||result->original->column_count<=1)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake DROP cannot remove the last field or an unwritable binding")));
        LakeCheckDropFieldDependencies(result->original,result->column,relation,snapshot);
        LakeTxnForgetReadOnlyTableForRename(RelationGetRelid(relation));
        UnregisterSnapshot(snapshot);snapshot=NULL;MemoryContextSwitchTo(caller);
    }PG_CATCH();{
        MemoryContextSwitchTo(caller);if(snapshot)UnregisterSnapshot(snapshot);MemoryContextDelete(context);PG_RE_THROW();
    }PG_END_TRY();return result;
}
static void LakeWriteDroppedColumn(LakeDropColumnPlan* plan,int64 head,int64 commit,int64 schema,Snapshot snapshot)
{
    const LakeInlineBinding* old=plan->original;Oid ns=old->metadata_schema_oid;
    Oid types[4]={INT8OID,INT8OID,INT8OID,INT8OID};
    Datum args[4]={Int64GetDatum(LakePermanentTableId(old)),Int64GetDatum(old->columns[plan->column].column_id),
        Int64GetDatum(head),Int64GetDatum(commit)};
    LakeMetaRows* wrote=LakeMetadataQuery(psprintf("UPDATE %s SET end_snapshot=$4 WHERE table_id=$1 AND column_id=$2 "
        "AND parent_column IS NULL AND begin_snapshot<=$3 AND(end_snapshot IS NULL OR end_snapshot>$3)",
        LakeDefinitionRelation(ns,"ducklake_column")),4,types,args,NULL,snapshot,false,SPI_OK_UPDATE,ns);
    LakeMetadataRequireCount(wrote,1,"ended native dropped field definition");
    /* Fixed DuckLake ExpireDroppedColumns: genuine field retirement ends its
     * tags too. Other versions, IDs, carrier fields and file stats survive. */
    (void)LakeMetadataQuery(psprintf("UPDATE %s SET end_snapshot=$4 WHERE table_id=$1 AND column_id=$2 "
        "AND begin_snapshot<=$3 AND(end_snapshot IS NULL OR end_snapshot>$3)",
        LakeDefinitionRelation(ns,"ducklake_column_tag")),4,types,args,NULL,snapshot,false,SPI_OK_UPDATE,ns);
    Oid vt[3]={INT8OID,INT8OID,INT8OID};
    Datum va[3]={args[3],Int64GetDatum(schema),args[0]};
    wrote=LakeMetadataQuery(psprintf("INSERT INTO %s(begin_snapshot,schema_version,table_id) VALUES($1,$2,$3)",
        LakeDefinitionRelation(ns,"ducklake_schema_versions")),3,vt,va,NULL,snapshot,false,SPI_OK_INSERT,ns);
    LakeMetadataRequireCount(wrote,1,"drop column table schema version");
    Oid ht[5]={INT8OID,UUIDOID,INT8OID,UUIDOID,OIDOID};
    Datum ha[5]={va[1],DirectFunctionCall1(uuid_in,CStringGetDatum(old->binding_id)),args[0],
        DirectFunctionCall1(uuid_in,CStringGetDatum(old->table_uuid)),ObjectIdGetDatum(old->server_oid)};
    wrote=LakeMetadataQuery("UPDATE og_lake_internal.table_binding SET schema_version=$1 WHERE binding_id=$2 "
        "AND table_id=$3 AND table_uuid=$4 AND server_oid=$5 AND state='active' AND NOT read_only",
        5,ht,ha,NULL,snapshot,false,SPI_OK_UPDATE);
    LakeMetadataRequireCount(wrote,1,"drop column private binding version");
    Oid ct[2]={INT8OID,TEXTOID};Datum ca[2]={args[3],CStringGetTextDatum(psprintf("altered_table:" INT64_FORMAT,LakePermanentTableId(old)))};
    wrote=LakeMetadataQuery(psprintf("INSERT INTO %s(snapshot_id,changes_made,author,commit_message,commit_extra_info) "
        "VALUES($1,$2,NULL,NULL,NULL)",LakeMetadataRelation(ns,"ducklake_snapshot_changes")),
        2,ct,ca,NULL,snapshot,false,SPI_OK_INSERT,ns);
    LakeMetadataRequireCount(wrote,1,"drop column changeset");
}

void LakeNativeFinishDropColumn(void* opaque)
{
    LakeDropColumnPlan* plan=(LakeDropColumnPlan*)opaque;
    MemoryContext caller=CurrentMemoryContext;ResourceOwner owner=t_thrd.utils_cxt.CurrentResourceOwner;
    if(plan->noop){pfree(plan);return;}
    void* hostSlot=LakeHostPrepareImmediatePublish(plan->original,GetCurrentSubTransactionId());
    int64 winningSnapshot=-1;
    bool complete=false;volatile bool inside=false;
    PG_TRY();{
        MemoryContextSwitchTo(plan->context);CommandCounterIncrement();
        Relation host=heap_open(plan->original->relation_oid,NoLock);TupleDesc desc=RelationGetDescr(host);
        if(desc->natts!=LakeHostColumnCount(plan->original))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native drop column host attribute count changed")));
        for(int c=0;c<plan->original->column_count;++c){
            Form_pg_attribute attr=&desc->attrs[LakeLogicalToHost(plan->original,c)];
            const LakeInlineColumn* field=&plan->original->columns[c];
            if(c==plan->column){
                if(!attr->attisdropped||attr->atttypid!=InvalidOid||attr->attnotnull||attr->atthasdef)
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("host DROP did not leave the expected empty attribute slot")));
            }else if(attr->attisdropped||attr->atttypid!=field->type||attr->atttypmod!=field->typmod||
                     strcmp(NameStr(attr->attname),field->name)||attr->attnotnull!=field->not_null)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("host DROP changed a surviving field")));
        }
        heap_close(host,NoLock);
        for(int attempt=0;attempt<16&&!complete;++attempt){
            CHECK_FOR_INTERRUPTS();
            MemoryContext scratch=AllocSetContextCreate(plan->context,"Native drop column claim",ALLOCSET_DEFAULT_SIZES);
            plan->state->publication_subtransaction=true;BeginInternalSubTransaction(NULL);inside=true;
            MemoryContextSwitchTo(scratch);Oid ns=plan->original->metadata_schema_oid;
            (void)LakeMetadataRelation(ns,"ducklake_metadata");
            Relation options=heap_open(get_relname_relid("ducklake_metadata",ns),ShareLock);heap_close(options,NoLock);
            Snapshot fresh=GetLatestSnapshot();
            /* Host has changed; reuse the existing metadata-only ADD fence,
             * including current logical flags, without opening changed host. */
            (void)LakeReloadAddColumnDefinition(plan->original,fresh);
            const char* snapshots=LakeMetadataRelation(ns,"ducklake_snapshot");
            LakeMetaRows* rows=LakeMetadataQuery(psprintf("SELECT snapshot_id,schema_version,next_catalog_id,next_file_id "
                "FROM %s ORDER BY snapshot_id DESC LIMIT 1",snapshots),0,NULL,NULL,NULL,fresh,true,SPI_OK_SELECT,ns);
            LakeMetadataRequireCount(rows,1,"drop column snapshot head");
            int64 head=DatumGetInt64(LakeMetadataValue(rows,0,1,INT8OID));
            int64 schema=DatumGetInt64(LakeMetadataValue(rows,0,2,INT8OID));
            int64 catalog=DatumGetInt64(LakeMetadataValue(rows,0,3,INT8OID));
            int64 file=DatumGetInt64(LakeMetadataValue(rows,0,4,INT8OID));
            if(head<0||head==PG_INT64_MAX||schema<0||schema==PG_INT64_MAX||catalog<0||
               catalog>=LAKE_REFERENCE_LOCAL_ID_START||file<0)
                ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native drop column snapshot counters exceed their bounds")));
            Oid types[4]={INT8OID,INT8OID,INT8OID,INT8OID};
            Datum args[4]={Int64GetDatum(head+1),Int64GetDatum(schema+1),Int64GetDatum(catalog),Int64GetDatum(file)};
            LakeMetaRows* claim=LakeMetadataQuery(psprintf("INSERT INTO %s(snapshot_id,snapshot_time,schema_version,next_catalog_id,next_file_id) "
                "VALUES($1,pg_catalog.now(),$2,$3,$4) ON CONFLICT(snapshot_id) DO NOTHING RETURNING snapshot_id",snapshots),
                4,types,args,NULL,fresh,false,SPI_OK_INSERT_RETURNING,ns);
            if(claim->count>1)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("duplicate native drop column snapshot claim")));
            if(claim->count==1){
                Snapshot own=LakeCreatePushOwnWriteSnapshot(GetLatestSnapshot());
                LakeWriteDroppedColumn(plan,head,head+1,schema+1,own);
                PopActiveSnapshot();ReleaseCurrentSubTransaction();inside=false;plan->state->publication_subtransaction=false;complete=true;
                winningSnapshot=head+1;
            }else{RollbackAndReleaseCurrentSubTransaction();inside=false;plan->state->publication_subtransaction=false;}
            MemoryContextSwitchTo(plan->context);t_thrd.utils_cxt.CurrentResourceOwner=owner;MemoryContextDelete(scratch);
            if(!complete)pg_usleep(1000L*(attempt+1));
        }
        if(!complete)ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native drop column snapshot claim exceeded retry limit")));
        CacheInvalidateRelcacheByRelid(plan->original->relation_oid);CommandCounterIncrement();
        (void)LakeLoadCommittedInlineBinding(plan->original->relation_oid,GetLatestSnapshot());
        if(winningSnapshot>=0)LakeHostActivateImmediatePublish(hostSlot,winningSnapshot);
        MemoryContextSwitchTo(caller);MemoryContextDelete(plan->context);
    }PG_CATCH();{
        MemoryContextSwitchTo(caller);ErrorData* error=CopyErrorData();FlushErrorState();
        if(inside)RollbackAndReleaseCurrentSubTransaction();
        plan->state->publication_subtransaction=false;t_thrd.utils_cxt.CurrentResourceOwner=owner;
        MemoryContextDelete(plan->context);ReThrowError(error);
    }PG_END_TRY();
}
