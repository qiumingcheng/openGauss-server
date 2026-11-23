/* Restricted logical nullability; adapted from the native ADD claim protocol.
 * Host AX covers validation and publication. Old physical versions are retained. */
/*
Copyright 2018-2025 Stichting DuckDB Foundation

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
#include "postgres.h"
#include "lake_column_layout.h"
#include "lake_types.h"
#include "lake_nullability.h"
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

extern void LakeTxnForgetReadOnlyTableForRename(Oid relationOid);
struct LakeNullabilityPlan {
    MemoryContext context;
    LakeTxnState* state;
    LakeInlineBinding* original;
    int column;
    bool target;
    bool changed;
};

static void LakeValidateVisibleNotNull(LakeNullabilityPlan* plan,Snapshot snapshot,CommandId limit)
{
    MemoryContext owner=CurrentMemoryContext;
    MemoryContext scratch=AllocSetContextCreate(owner,"Native NOT NULL visible row",ALLOCSET_DEFAULT_SIZES);
    Datum* values=(Datum*)palloc0(sizeof(Datum)*plan->original->column_count);
    bool* nulls=(bool*)palloc0(sizeof(bool)*plan->original->column_count);
    LakeInlineReader* volatile reader=NULL;LakeTxnTable* volatile table=NULL;
    PG_TRY();{
        table=LakeTxnReadStatementTable(plan->original->relation_oid,snapshot);
        /* Full logical stream, with normal visibility, file deletes and initial
         * values. No pruning, sampling, index path or physical enumeration. */
        reader=LakeInlineReaderOpen((LakeTxnTable*)table,limit);
        for(;;){
            CHECK_FOR_INTERRUPTS();MemoryContextReset(scratch);MemoryContextSwitchTo(scratch);
            LakeInlineRowIdentity identity;
            bool found=LakeInlineReaderNext((LakeInlineReader*)reader,values,nulls,&identity);
            MemoryContextSwitchTo(owner);
            if(!found)break;
            if(nulls[plan->column])
                ereport(ERROR,(errcode(ERRCODE_NOT_NULL_VIOLATION),
                    errmsg("column \"%s\" contains null values",plan->original->columns[plan->column].name)));
        }
        LakeInlineReaderClose((LakeInlineReader*)reader);reader=NULL;
        LakeTxnReleaseReadView((LakeTxnTable*)table);table=NULL;
        MemoryContextDelete(scratch);pfree(values);pfree(nulls);
    }PG_CATCH();{
        MemoryContextSwitchTo(owner);
        if(reader)LakeInlineReaderClose((LakeInlineReader*)reader);
        if(table)LakeTxnReleaseReadView((LakeTxnTable*)table);
        MemoryContextDelete(scratch);pfree(values);pfree(nulls);PG_RE_THROW();
    }PG_END_TRY();
}

void* LakeNativePrepareNullability(Relation relation,List* commands)
{
    if(!LakeIsNativeForeignRelation(relation)||!LakeNotNullVersionEnabled())return NULL;
    LakeRejectNativePartitionDDL(relation,"NOT NULL");
    if(list_length(commands)!=1)return NULL;
    AlterTableCmd* command=(AlterTableCmd*)linitial(commands);
    if(command->subtype!=AT_SetNotNull&&command->subtype!=AT_DropNotNull)return NULL;
    if(!command->name||!*command->name||command->def||command->missing_ok||
       command->is_first||command->after_name||command->recursing)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake NOT NULL requires one plain top-level column")));
    if(!pg_class_ownercheck(RelationGetRelid(relation),GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER,ACL_KIND_CLASS,RelationGetRelationName(relation));
    if(u_sess->utils_cxt.XactIsoLevel!=XACT_READ_COMMITTED)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake NOT NULL requires READ COMMITTED")));
    if(u_sess->attr.attr_common.XactReadOnly)
        ereport(ERROR,(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),errmsg("cannot alter lake nullability in a read-only transaction")));
    if(creating_extension||RelationIsPartitioned(relation)||relation->rd_rel->relpersistence!=RELPERSISTENCE_PERMANENT||
       find_inheritance_children(RelationGetRelid(relation),NoLock)!=NIL||
       LakeTxnFindCreatedTable(RelationGetRelid(relation))!=NULL||!LakeNativeProtectedCreate(RelationGetRelid(relation)))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake NOT NULL requires a committed ordinary protected CREATE table")));
    AttrNumber attnum=get_attnum(RelationGetRelid(relation),command->name);
    if(attnum<=0||attnum>RelationGetDescr(relation)->natts)
        ereport(ERROR,(errcode(ERRCODE_UNDEFINED_COLUMN),errmsg("native lake NOT NULL column does not exist")));
    Form_pg_attribute attr=&RelationGetDescr(relation)->attrs[attnum-1];
    if(attr->attisdropped||!LakeNotNullType(attr->atttypid)||!LakeNativeFlatTypmod(attr->atttypid,attr->atttypmod))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake NOT NULL requires an admitted flat scalar")));
    /* SET partition already takes AS -> registry -> SRE. Under our host AX,
     * never wait for the inverse registry edge: fail 40001 instead. */
    LakeTryLockCatalogRegistration();
    if(!OidIsValid(LakeMetadataOwner()))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake NOT NULL requires a private metadata owner")));
    LakeTxnState* state=LakeTxnBeginColumnDefinition(); /* USER subid, not claim. */
    LakeTxnForgetReadOnlyTableForRename(RelationGetRelid(relation));
    MemoryContext caller=CurrentMemoryContext;
    MemoryContext context=AllocSetContextCreate(caller,"Native lake nullability statement",ALLOCSET_DEFAULT_SIZES);
    Snapshot volatile snapshot=NULL;LakeNullabilityPlan* result=NULL;
    PG_TRY();{
        MemoryContextSwitchTo(context);
        /* Capture the command boundary before any metadata SPI increments it. */
        CommandId limit=GetCurrentCommandId(false);
        snapshot=RegisterSnapshot(GetLatestSnapshot());
        result=(LakeNullabilityPlan*)palloc0(sizeof(LakeNullabilityPlan));
        result->context=context;result->state=state;result->column=-1;
        result->target=command->subtype==AT_SetNotNull;
        result->original=LakeLoadCommittedInlineBinding(RelationGetRelid(relation),snapshot);
        result->column=LakeHostToLogical(result->original,attnum);
        if(result->column<0)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native definition target has no stable host mapping")));
        if(result->original->read_only||!result->original->not_null_constraints)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake NOT NULL target is not writable")));
        result->changed=result->original->columns[result->column].not_null!=result->target;
        if(result->changed&&result->target)LakeValidateVisibleNotNull(result,snapshot,limit);
        /* Retain no old descriptor root across the normal pg_attribute edit. */
        LakeTxnForgetReadOnlyTableForRename(RelationGetRelid(relation));
        UnregisterSnapshot(snapshot);snapshot=NULL;MemoryContextSwitchTo(caller);
    }PG_CATCH();{
        MemoryContextSwitchTo(caller);if(snapshot)UnregisterSnapshot(snapshot);
        MemoryContextDelete(context);PG_RE_THROW();
    }PG_END_TRY();
    return result;
}

static void LakeWriteNullability(LakeNullabilityPlan* plan,int64 head,int64 commit,int64 schema,Snapshot snapshot)
{
    const LakeInlineBinding* old=plan->original;Oid ns=old->metadata_schema_oid;
    Oid types[5]={INT8OID,INT8OID,INT8OID,INT8OID,BOOLOID};
    Datum args[5]={Int64GetDatum(LakePermanentTableId(old)),Int64GetDatum(old->columns[plan->column].column_id),
        Int64GetDatum(head),Int64GetDatum(commit),BoolGetDatum(!plan->target)};
    const char* columns=LakeDefinitionRelation(ns,"ducklake_column");
    LakeMetaRows* wrote=LakeMetadataQuery(psprintf("UPDATE %s SET end_snapshot=$4 WHERE table_id=$1 AND column_id=$2 "
        "AND parent_column IS NULL AND begin_snapshot<=$3 AND(end_snapshot IS NULL OR end_snapshot>$3)",columns),
        5,types,args,NULL,snapshot,false,SPI_OK_UPDATE,ns);
    LakeMetadataRequireCount(wrote,1,"ended native nullability definition");
    /* Copy the complete old definition, including separate initial/current
     * defaults. Nullability does not allocate a field ID or change introduction. */
    wrote=LakeMetadataQuery(psprintf("INSERT INTO %s(column_id,begin_snapshot,end_snapshot,table_id,column_order,"
        "column_name,column_type,initial_default,default_value,nulls_allowed,parent_column,default_value_type,default_value_dialect) "
        "SELECT column_id,$4,NULL,table_id,column_order,column_name,column_type,initial_default,default_value,$5,"
        "parent_column,default_value_type,default_value_dialect FROM %s WHERE table_id=$1 AND column_id=$2 "
        "AND parent_column IS NULL AND begin_snapshot<=$3 AND end_snapshot=$4",columns,columns),
        5,types,args,NULL,snapshot,false,SPI_OK_INSERT,ns);
    LakeMetadataRequireCount(wrote,1,"new native nullability definition");
    Oid vt[3]={INT8OID,INT8OID,INT8OID};
    Datum va[3]={args[3],Int64GetDatum(schema),args[0]};
    wrote=LakeMetadataQuery(psprintf("INSERT INTO %s(begin_snapshot,schema_version,table_id) VALUES($1,$2,$3)",
        LakeDefinitionRelation(ns,"ducklake_schema_versions")),3,vt,va,NULL,snapshot,false,SPI_OK_INSERT,ns);
    LakeMetadataRequireCount(wrote,1,"nullability table schema version");
    Oid ht[5]={INT8OID,UUIDOID,INT8OID,UUIDOID,OIDOID};
    Datum ha[5]={va[1],DirectFunctionCall1(uuid_in,CStringGetDatum(old->binding_id)),args[0],
        DirectFunctionCall1(uuid_in,CStringGetDatum(old->table_uuid)),ObjectIdGetDatum(old->server_oid)};
    wrote=LakeMetadataQuery("UPDATE og_lake_internal.table_binding SET schema_version=$1 WHERE binding_id=$2 "
        "AND table_id=$3 AND table_uuid=$4 AND server_oid=$5 AND state='active' AND NOT read_only",
        5,ht,ha,NULL,snapshot,false,SPI_OK_UPDATE);
    LakeMetadataRequireCount(wrote,1,"nullability private binding version");
    Oid ct[2]={INT8OID,TEXTOID};Datum ca[2]={args[3],CStringGetTextDatum(psprintf("altered_table:" INT64_FORMAT,LakePermanentTableId(old)))};
    wrote=LakeMetadataQuery(psprintf("INSERT INTO %s(snapshot_id,changes_made,author,commit_message,commit_extra_info) "
        "VALUES($1,$2,NULL,NULL,NULL)",LakeMetadataRelation(ns,"ducklake_snapshot_changes")),
        2,ct,ca,NULL,snapshot,false,SPI_OK_INSERT,ns);
    LakeMetadataRequireCount(wrote,1,"nullability changeset");
}

void LakeNativeFinishNullability(void* opaque)
{
    LakeNullabilityPlan* plan=(LakeNullabilityPlan*)opaque;
    MemoryContext caller=CurrentMemoryContext;ResourceOwner owner=t_thrd.utils_cxt.CurrentResourceOwner;
    bool complete=!plan->changed;volatile bool inside=false;
    void* hostSlot=plan->changed?LakeHostPrepareImmediatePublish(plan->original,GetCurrentSubTransactionId()):NULL;
    int64 winningSnapshot=-1;
    PG_TRY();{
        MemoryContextSwitchTo(plan->context);CommandCounterIncrement();
        Relation host=heap_open(plan->original->relation_oid,NoLock);TupleDesc desc=RelationGetDescr(host);
        if(desc->natts!=LakeHostColumnCount(plan->original))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native nullability host attribute count changed")));
        for(int c=0;c<plan->original->column_count;++c){
            Form_pg_attribute attr=&desc->attrs[LakeLogicalToHost(plan->original,c)];const LakeInlineColumn* field=&plan->original->columns[c];
            if(attr->attisdropped||attr->atttypid!=field->type||attr->atttypmod!=field->typmod||
               strcmp(NameStr(attr->attname),field->name)||attr->attnotnull!=(c==plan->column?plan->target:field->not_null))
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native nullability host definition changed unexpectedly")));
        }
        heap_close(host,NoLock);
        for(int attempt=0;attempt<16&&!complete;++attempt){
            CHECK_FOR_INTERRUPTS();
            MemoryContext scratch=AllocSetContextCreate(plan->context,"Native nullability claim",ALLOCSET_DEFAULT_SIZES);
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
            LakeMetadataRequireCount(rows,1,"nullability snapshot head");
            int64 head=DatumGetInt64(LakeMetadataValue(rows,0,1,INT8OID));
            int64 schema=DatumGetInt64(LakeMetadataValue(rows,0,2,INT8OID));
            int64 catalog=DatumGetInt64(LakeMetadataValue(rows,0,3,INT8OID));
            int64 file=DatumGetInt64(LakeMetadataValue(rows,0,4,INT8OID));
            if(head<0||head==PG_INT64_MAX||schema<0||schema==PG_INT64_MAX||catalog<0||
               catalog>=LAKE_REFERENCE_LOCAL_ID_START||file<0)
                ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native nullability snapshot counters exceed their bounds")));
            Oid types[4]={INT8OID,INT8OID,INT8OID,INT8OID};
            Datum args[4]={Int64GetDatum(head+1),Int64GetDatum(schema+1),Int64GetDatum(catalog),Int64GetDatum(file)};
            LakeMetaRows* claim=LakeMetadataQuery(psprintf("INSERT INTO %s(snapshot_id,snapshot_time,schema_version,next_catalog_id,next_file_id) "
                "VALUES($1,pg_catalog.now(),$2,$3,$4) ON CONFLICT(snapshot_id) DO NOTHING RETURNING snapshot_id",snapshots),
                4,types,args,NULL,fresh,false,SPI_OK_INSERT_RETURNING,ns);
            if(claim->count>1)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("duplicate native nullability snapshot claim")));
            if(claim->count==1){
                Snapshot own=LakeCreatePushOwnWriteSnapshot(GetLatestSnapshot());
                LakeWriteNullability(plan,head,head+1,schema+1,own);
                PopActiveSnapshot();ReleaseCurrentSubTransaction();inside=false;plan->state->publication_subtransaction=false;complete=true;
                winningSnapshot=head+1;
            }else{RollbackAndReleaseCurrentSubTransaction();inside=false;plan->state->publication_subtransaction=false;}
            MemoryContextSwitchTo(plan->context);t_thrd.utils_cxt.CurrentResourceOwner=owner;MemoryContextDelete(scratch);
            if(!complete)pg_usleep(1000L*(attempt+1));
        }
        if(!complete)ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native nullability snapshot claim exceeded retry limit")));
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
