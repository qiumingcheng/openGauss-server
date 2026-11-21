/* Snapshot-specific logical NOT NULL; physical retained versions stay nullable.
 * No SQL expression evaluation, newest-snapshot substitution or SDK I/O here. */
#include "postgres.h"
#include "lake_column_layout.h"
#include "lake_nullability.h"
#include "lake_txn_private.h"
#include "lake_stream.h"
#include "distributelayer/streamCore.h"
#include "lake_types.h"
#include "access/heapam.h"
#include "access/og_lake.h"
#include "commands/sec_rls_cmds.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "foreign/foreign.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/sec_rls_utils.h"
#include "nodes/plannodes.h"
#include "utils/snapmgr.h"

bool LakeNotNullType(Oid type)
{
    return type==BOOLOID||type==INT2OID||type==INT4OID||type==INT8OID||
        type==FLOAT4OID||type==FLOAT8OID||type==TEXTOID||type==DATEOID||
        type==TIMESTAMPOID||type==TIMESTAMPTZOID||(type==UUIDOID&&LakeUuidBusinessVersionEnabled())||
        (type==JSONOID&&LakeJsonBusinessVersionEnabled())||
        (type==NUMERICOID&&LakeNumericBusinessVersionEnabled())||
        /* SQL43 BYTEA stays nullable-only. Logical NULL is the slot/reader
         * validity bit, independent of BLOB length, payload and row identity.
         * Existing visible-row validation and final after-image checks apply;
         * retained physical versions remain nullable, including ended NULLs. */
        (type==BYTEAOID&&LakeBinaryNotNullVersionEnabled());
}

void LakeReadFieldNullability(LakeInlineBinding* binding,LakeInlineColumn* fields,int count,
    int64 schemaSnapshot,Snapshot hostSnapshot)
{
    MemoryContext owner=CurrentMemoryContext;
    MemoryContext scratch=AllocSetContextCreate(owner,"Native field nullability metadata",ALLOCSET_DEFAULT_SIZES);
    PG_TRY();{
        MemoryContextSwitchTo(scratch);
        Oid types[2]={INT8OID,INT8OID};
        Datum args[2]={Int64GetDatum(LakePermanentTableId(binding)),Int64GetDatum(schemaSnapshot)};
        LakeMetaRows* rows=LakeMetadataQuery(psprintf("SELECT column_id,nulls_allowed FROM %s "
            "WHERE table_id=$1 AND parent_column IS NULL AND begin_snapshot<=$2 "
            "AND(end_snapshot IS NULL OR end_snapshot>$2) ORDER BY column_order",
            LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_column")),
            2,types,args,NULL,hostSnapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
        LakeMetadataRequireCount(rows,count,"native field nullability");
        for(int c=0;c<count;++c){
            if(DatumGetInt64(LakeMetadataValue(rows,c,1,INT8OID))!=fields[c].column_id)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake nullability field identity changed")));
            fields[c].not_null=!DatumGetBool(LakeMetadataValue(rows,c,2,BOOLOID));
            if(fields[c].not_null&&!LakeNotNullType(fields[c].type))
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake NOT NULL requires an admitted flat scalar")));
        }
        /* Only bools escape into the caller's already budgeted field array;
         * transient SPI tuples/arguments never survive a source-map load. */
        MemoryContextSwitchTo(owner);MemoryContextDelete(scratch);
    }PG_CATCH();{
        MemoryContextSwitchTo(owner);MemoryContextDelete(scratch);PG_RE_THROW();
    }PG_END_TRY();
}

void LakeCheckNotNullSlot(const LakeInlineBinding* binding,TupleTableSlot* slot)
{
    if(!binding->not_null_constraints||slot==NULL)return;
    if(slot->tts_tupleDescriptor->natts!=LakeHostColumnCount(binding))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake NOT NULL input shape changed")));
    /* Caller has fetched all attributes. This is the final after-image after
     * host defaults and any EPQ re-evaluation; never a raw flush version. */
    for(int c=0;c<binding->column_count;++c)
        if(binding->columns[c].not_null&&slot->tts_isnull[LakeLogicalToHost(binding,c)])
            ereport(ERROR,(errcode(ERRCODE_NOT_NULL_VIOLATION),
                errmsg("null value in column \"%s\" violates native lake not-null constraint",binding->columns[c].name)));
}

static bool LakeConstraintTarget(const RangeTblEntry* rte)
{
    if(!rte||rte->rtekind!=RTE_RELATION||get_rel_relkind(rte->relid)!=RELKIND_FOREIGN_TABLE)return false;
    Relation relation=heap_open(rte->relid,AccessShareLock);
    bool native=LakeIsNativeForeignRelation(relation);
    heap_close(relation,NoLock);
    return native&&LakeNotNullVersionEnabled()&&LakeNativeProtectedCreate(rte->relid);
}

static void LakeCheckOneConstraintSnapshot(Oid oid,Snapshot snapshot,
    LakeConstraintProof* proof=NULL,LakeConstraintRelationProof* capture=NULL,MemoryContext shared=NULL)
{
    if (!StreamThreadAmI() && capture == NULL &&
        LakeTxnGetBoundCatalog(GetForeignTable(oid)->serverid) != NULL)
        return;
    if(snapshot==NULL)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("native lake constraint check requires the statement snapshot")));
    MemoryContext owner=CurrentMemoryContext;
    MemoryContext scratch=AllocSetContextCreate(owner,"Native lake constraint snapshot",ALLOCSET_DEFAULT_SIZES);
    Relation volatile relation=NULL;
    PG_TRY();{
        MemoryContextSwitchTo(scratch);relation=heap_open(oid,AccessShareLock);
        LakeInlineBinding* binding=LakeLoadConstraintDefinition(relation,snapshot);
        TupleDesc desc=RelationGetDescr(relation);
        if(!binding||!binding->not_null_constraints||desc->natts!=LakeHostColumnCount(binding))
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                errmsg("native lake statement schema is incompatible with current host constraints")));
        for(int c=0;c<binding->column_count;++c){
            Form_pg_attribute attr=&desc->attrs[LakeLogicalToHost(binding,c)];const LakeInlineColumn* field=&binding->columns[c];
            if(attr->attisdropped||attr->attnum!=field->output_attnum||attr->atttypid!=field->type||
               attr->atttypmod!=field->typmod||attr->attnotnull!=field->not_null)
                ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                    errmsg("native lake statement schema is incompatible with current host constraints")));
        }
        if(capture){
            Size bytes=(Size)desc->natts*sizeof(LakeConstraintFieldProof);
            if(bytes>1024*1024||proof->bytes>1024*1024-bytes)
                ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake Stream constraint proof exceeds1MiB")));
            proof->bytes+=bytes;capture->columns=desc->natts;capture->server_oid=binding->server_oid;
            capture->fields=(LakeConstraintFieldProof*)MemoryContextAllocZero(shared,bytes);
            for(int c=0;c<desc->natts;++c){
                Form_pg_attribute attr=&desc->attrs[c];LakeConstraintFieldProof* field=&capture->fields[c];
                int logical=LakeHostToLogical(binding,attr->attnum);
                field->dropped=attr->attisdropped;
                field->column_id=logical<0?-1:binding->columns[logical].column_id;field->attnum=attr->attnum;
                if(logical<0)for(int r=0;r<binding->retired_count;++r)
                    if(binding->retired_columns[r].column.output_attnum==attr->attnum)
                        field->column_id=binding->retired_columns[r].column.column_id;
                if(field->column_id<0||(logical<0)!=attr->attisdropped)
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("incomplete native host hole constraint proof")));
                field->type=attr->atttypid;field->typmod=attr->atttypmod;field->collation=attr->attcollation;
                namestrcpy(&field->name,NameStr(attr->attname));field->not_null=attr->attnotnull;
            }
        }
        heap_close(relation,NoLock);relation=NULL;
        MemoryContextSwitchTo(owner);MemoryContextDelete(scratch);
    }PG_CATCH();{
        MemoryContextSwitchTo(owner);if(relation)heap_close(relation,NoLock);
        MemoryContextDelete(scratch);PG_RE_THROW();
    }PG_END_TRY();
}

struct LakeConstraintWalk { Snapshot snapshot; };
static bool LakeConstraintQueryWalker(Node* node,void* opaque)
{
    if(node==NULL)return false;
    LakeConstraintWalk* state=(LakeConstraintWalk*)opaque;
    if(IsA(node,Query)){
        Query* query=(Query*)node;ListCell* cell=NULL;
        foreach(cell,query->rtable){
            RangeTblEntry* rte=(RangeTblEntry*)lfirst(cell);
            if(LakeConstraintTarget(rte)){
                if(StreamThreadAmI())
                    ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake constraint planning is not enabled inside a Stream worker")));
                LakeCheckOneConstraintSnapshot(rte->relid,state->snapshot);
            }
        }
        return query_tree_walker(query,(bool(*)())LakeConstraintQueryWalker,opaque,0);
    }
    return expression_tree_walker(node,(bool(*)())LakeConstraintQueryWalker,opaque);
}

void LakeCheckNotNullQuerySnapshot(Query* query,Snapshot snapshot)
{
    if(LakeGetMetadataScope()!=NULL)return;
    /* Original recursive ACL/security/RC checks have already run in
     * LakeCheckQueryPermissions. They precede every elevated metadata read. */
    LakeConstraintWalk state={snapshot};
    (void)LakeConstraintQueryWalker((Node*)query,&state);
}

static void LakeCheckConstraintPlanAccess(PlannedStmt* plan)
{
    ListCell* cell=NULL;
    LakeCheckMaskingAdmission();(void)ExecCheckRTPerms(plan->rtable,true);LakeRequireAccessPolicy();
    foreach(cell,plan->rtable){
        RangeTblEntry* rte=(RangeTblEntry*)lfirst(cell);
        if(rte->security_barrier||rte->securityQuals!=NIL||
           (rte->rtekind==RTE_RELATION&&RelationHasRlspolicy(rte->relid)))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("native lake row-security and security-barrier combinations are not enabled")));
    }
}

void LakeCheckNotNullPlanSnapshot(PlannedStmt* plan,Snapshot snapshot)
{
    if(LakeGetMetadataScope()!=NULL||!plan||plan->utilityStmt)return;
    bool needed=false;ListCell* cell=NULL;
    foreach(cell,plan->rtable)
        if(LakeConstraintTarget((RangeTblEntry*)lfirst(cell))){needed=true;break;}
    if(!needed)return;
    /* Cached dummy plans still retain RTE permissions and relation locks.
     * This path is also called by Fusion after it pushes its actual snapshot. */
    LakeCheckConstraintPlanAccess(plan);
    if(StreamThreadAmI()){LakeCheckStreamConstraintSnapshot(plan,snapshot);return;}

    foreach(cell,plan->rtable){
        RangeTblEntry* rte=(RangeTblEntry*)lfirst(cell);
        if(LakeConstraintTarget(rte))LakeCheckOneConstraintSnapshot(rte->relid,snapshot);
    }
}

LakeConstraintProof* LakeCaptureNotNullPlanProof(PlannedStmt* plan,Snapshot snapshot,MemoryContext shared)
{
    if(StreamThreadAmI()||LakeGetMetadataScope()!=NULL)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake constraint proof requires an ordinary Stream leader")));
    LakeConstraintProof* proof=NULL;LakeConstraintRelationProof** tail=NULL;
    int rti=0;ListCell* cell=NULL;
    foreach(cell,plan->rtable){
        ++rti;RangeTblEntry* rte=(RangeTblEntry*)lfirst(cell);if(!LakeConstraintTarget(rte))continue;
        if(!proof){
            LakeCheckConstraintPlanAccess(plan);
            proof=(LakeConstraintProof*)MemoryContextAllocZero(shared,sizeof(LakeConstraintProof));
            proof->bytes=sizeof(LakeConstraintProof);proof->rtable_length=list_length(plan->rtable);tail=&proof->head;
        }
        if(proof->relations>=64||proof->bytes>1024*1024-sizeof(LakeConstraintRelationProof))
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake Stream constraint proof exceeds its relation bound")));
        proof->bytes+=sizeof(LakeConstraintRelationProof);++proof->relations;
        auto* entry=(LakeConstraintRelationProof*)MemoryContextAllocZero(shared,sizeof(LakeConstraintRelationProof));
        entry->rti=rti;entry->relation_oid=rte->relid;*tail=entry;tail=&entry->next;
        LakeCheckOneConstraintSnapshot(rte->relid,snapshot,proof,entry,shared);
    }
    return proof;
}

void LakeVerifyNotNullPlanProof(const LakeConstraintProof* proof,PlannedStmt* plan)
{
    if(!StreamThreadAmI()||!proof||proof->rtable_length!=list_length(plan->rtable)||
       proof->relations<1||proof->relations>64||proof->bytes>1024*1024)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake Stream constraint proof is missing or incomplete")));
    const LakeConstraintRelationProof* expected=proof->head;int rti=0,seen=0;ListCell* cell=NULL;
    foreach(cell,plan->rtable){
        ++rti;RangeTblEntry* rte=(RangeTblEntry*)lfirst(cell);
        if(!LakeConstraintTarget(rte))continue;
        if(!expected||expected->rti!=rti||expected->relation_oid!=rte->relid||
           GetForeignTable(rte->relid)->serverid!=expected->server_oid)
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake Stream constraint RTE coverage changed")));
        Relation host=heap_open(rte->relid,AccessShareLock);TupleDesc desc=RelationGetDescr(host);
        if(desc->natts!=expected->columns||expected->columns<1||expected->columns>MaxHeapAttributeNumber||!expected->fields)
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake Stream host constraint shape changed")));
        for(int c=0;c<desc->natts;++c){
            Form_pg_attribute attr=&desc->attrs[c];const LakeConstraintFieldProof* field=&expected->fields[c];
            if(field->column_id<0||attr->attisdropped!=field->dropped||attr->attnum!=field->attnum||attr->atttypid!=field->type||
               attr->atttypmod!=field->typmod||attr->attcollation!=field->collation||
               strcmp(NameStr(attr->attname),NameStr(field->name))||attr->attnotnull!=field->not_null)
                ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake Stream host constraints changed")));
        }
        heap_close(host,NoLock);expected=expected->next;++seen;
    }
    if(expected||seen!=proof->relations)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake Stream constraint RTE coverage is incomplete")));
}
