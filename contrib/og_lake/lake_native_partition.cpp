/* Native parent marker, typed native range primitives and readonly admission.
 * Persistent marker presence can never authorize an ordinary heap fallback.
 * Activation remains closed until the root-coordinated SQL capability exists. */
#include "postgres.h"
#include "access/og_lake.h"
#include "lake_native_partition.h"
#include "lake_types.h"
#include "lake_version.h"
#include "access/heapam.h"
#include "access/reloptions.h"
#include "access/tableam.h"
#include "catalog/pg_class.h"
#include "catalog/pg_partition.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_type.h"
#include "knl/knl_variable.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/nodeFuncs.h"
#include "storage/lmgr.h"
#include "storage/lock/lock.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/partitionmap.h"
#include "utils/partitionmap_gs.h"
#include "utils/partcache.h"
#include "utils/snapmgr.h"
#include "optimizer/planmem_walker.h"
#include "optimizer/clauses.h"

bool LakeHasNativePartitionMarker(Relation relation)
{
    /* Foreign-table options have a different layout; never cast them to heap
     * StdRdOptions. A marked ordinary r with a damaged parttype is still marked. */
    return relation && relation->rd_rel->relkind==RELKIND_RELATION && relation->rd_options &&
        ((StdRdOptions*)relation->rd_options)->lake_partition_root;
}

void LakeRejectNativePartitionDDL(Relation relation,const char* operation)
{
    if(LakeHasNativePartitionMarker(relation))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake partition root does not support %s before binding DDL qualification",operation)));
}

void LakeRejectNativePartitionRelOptions(Relation relation,List* options)
{
    LakeRejectNativePartitionDDL(relation,"SET/RESET/REPLACE reloptions");
    ListCell* cell=NULL;
    foreach(cell,options) {
        DefElem* option=(DefElem*)lfirst(cell);
        if(option && pg_strcasecmp(option->defname,"lake_partition_root")==0)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("lake_partition_root cannot be set or reset by user DDL")));
    }
}

struct LakeNativePlanCheck { MethodPlanWalkerContext walker; Bitmapset* parents; Bitmapset* seen; bool insert; bool mutation; };
static bool LakeNativeTargetRti(PlannedStmt* plan,int index)
{
    ListCell* group=NULL;foreach(group,plan->resultRelations)
        if(list_member_int((List*)lfirst(group),index))return true;
    return false;
}
static bool LakeNativeReturningSystemVar(Node* node,void*)
{
    if(!node)return false;
    if(IsA(node,Var)&&((Var*)node)->varattno<0)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake parent RETURNING system columns are not enabled")));
    return expression_tree_walker(node,(bool(*)())LakeNativeReturningSystemVar,NULL);
}
static bool LakeNativeCheckReadPlan(Node* node,void* argument)
{
    if(!node)return false;LakeNativePlanCheck* check=(LakeNativePlanCheck*)argument;
    if(IsA(node,Stream)||IsA(node,VecStream)||IsA(node,RemoteQuery)||IsA(node,VecRemoteQuery)||
       IsA(node,VecForeignScan)||IsA(node,RowToVec)||IsA(node,VecToRow)||IsA(node,VecModifyTable))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake parent requires a serial readonly row plan")));
    if(IsA(node,ModifyTable)) {
        ModifyTable* modify=(ModifyTable*)node;
        bool allowed=(check->insert&&modify->operation==CMD_INSERT)||
            (check->mutation&&(modify->operation==CMD_UPDATE||modify->operation==CMD_DELETE));
        if(!allowed||modify->upsertAction!=UPSERT_NONE)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native parent UPDATE/DELETE requires its typed row-head and EPQ adapter")));
        (void)LakeNativeReturningSystemVar((Node*)modify->returningLists,NULL);
    }
    if(IsA(node,ForeignScan)) {
        ForeignScan* scan=(ForeignScan*)node;
        if(bms_is_member(scan->scan.scanrelid,check->parents)) {
            bool business=list_length(scan->fdw_private)==1&&IsA(linitial(scan->fdw_private),Integer)&&intVal(linitial(scan->fdw_private))==1;
            bool locator=check->mutation&&list_length(scan->fdw_private)==3&&IsA(linitial(scan->fdw_private),Integer)&&
                intVal(linitial(scan->fdw_private))==2&&IsA(lsecond(scan->fdw_private),Integer)&&IsA(lthird(scan->fdw_private),Integer);
            if(!scan->scan.isPartTbl||scan->scan.plan.dop>1||(!business&&!locator)||scan->fdw_exprs!=NIL)
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("cached native parent plan requires replan")));
            check->seen=bms_add_member(check->seen,scan->scan.scanrelid);
        }
    }
    return plan_tree_walker(node,(MethodWalker)LakeNativeCheckReadPlan,argument);
}

/* A no-op UPDATE/DELETE keeps its ModifyTable root. Only its native
 * constant-false, childless input proves that no target scan can execute.
 * Keep subplans/initplans outside this exception: they need their own scan
 * validation and cannot borrow the target's empty-input proof. */
static bool LakeNativeEmptyMutationPlan(PlannedStmt* plan,const Bitmapset* parents)
{
    if(!plan||!IsA(plan->planTree,ModifyTable)||plan->subplans!=NIL)return false;
    ModifyTable* modify=(ModifyTable*)plan->planTree;
    if((modify->operation!=CMD_UPDATE&&modify->operation!=CMD_DELETE)||
       modify->operation!=plan->commandType||list_length(modify->plans)!=1||
       list_length(modify->resultRelations)!=1||modify->plan.dop>1||
       modify->plan.initPlan!=NIL||modify->plan.lefttree||modify->plan.righttree)return false;
    List* targets=modify->resultRelations;
    if(!modify->plan.isinherit) {
        if(!IsA(targets,List))return false;
        targets=(List*)linitial(targets);
    }
    if(!targets||!IsA(targets,IntList)||list_length(targets)!=1||
       !bms_is_member(linitial_int(targets),parents))return false;
    Plan* input=(Plan*)linitial(modify->plans);
    return input&&input->dop<=1&&is_dummy_plan(input)&&input->lefttree==NULL&&input->righttree==NULL&&input->initPlan==NIL;
}

void LakeCheckNativePartitionExecutor(PlannedStmt* plan)
{
    if(!plan)return;
    MemoryContext caller=CurrentMemoryContext;
    MemoryContext context=AllocSetContextCreate(caller,"Native parent startup validation",ALLOCSET_DEFAULT_SIZES);
    PG_TRY(); {
        MemoryContextSwitchTo(context);LakeNativePlanCheck check={};ListCell* cell=NULL;int rti=0;bool marked=false;
        check.insert=plan->commandType==CMD_INSERT&&LakeNativePartitionWriteVersionEnabled();
        check.mutation=(plan->commandType==CMD_UPDATE||plan->commandType==CMD_DELETE)&&LakeNativePartitionMutationVersionEnabled();
        foreach(cell,plan->rtable) {
            ++rti;RangeTblEntry* entry=(RangeTblEntry*)lfirst(cell);
            if(entry->rtekind!=RTE_RELATION||entry->relkind!=RELKIND_RELATION)continue;
            Relation relation=heap_open(entry->relid,AccessShareLock);
            if(LakeHasNativePartitionMarker(relation)) {
                LakeRequireNativePartitionVersion();
                marked=true;
                if((plan->commandType!=CMD_SELECT&&!check.insert&&!check.mutation)||plan->hasModifyingCTE||plan->rowMarks!=NIL)
                    ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake parent writes and row locking are not enabled")));
                Snapshot snapshot=ActiveSnapshotSet()?GetActiveSnapshot():GetTransactionSnapshot();
                (void)LakeNativePartitionServer(relation,snapshot);
                if((check.insert||check.mutation)&&LakeNativeTargetRti(plan,rti))LakeNativePartitionCheckInsertTarget(relation,snapshot);
                if(!check.insert||!LakeNativeTargetRti(plan,rti))check.parents=bms_add_member(check.parents,rti);
            }
            heap_close(relation,NoLock);
        }
        if(marked) {
            exec_init_plan_tree_base(&check.walker.base,plan);
            (void)LakeNativeCheckReadPlan((Node*)plan->planTree,&check);
            foreach(cell,plan->subplans)(void)LakeNativeCheckReadPlan((Node*)lfirst(cell),&check);
            if(!bms_is_subset(check.parents,check.seen)&&!is_dummy_plan(plan->planTree)&&
               !(check.mutation&&LakeNativeEmptyMutationPlan(plan,check.parents)))
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake parent cannot execute a cached heap scan"),
                    errhint("Replan using the native lake ForeignScan/PartIterator adapter; no heap fallback is permitted.")));
        }
        MemoryContextSwitchTo(caller);MemoryContextDelete(context);
    }PG_CATCH();{MemoryContextSwitchTo(caller);MemoryContextDelete(context);PG_RE_THROW();}PG_END_TRY();
}

/* A bypass object can predate binding/relcache invalidation. Do not assume
 * fusion qualification ran again: all actual executions recheck the durable
 * marker before touching native heap storage, including SELECT/UPDATE/DELETE.
 * Replanning can then use the qualified normal executor path. */
void LakeRejectNativePartitionFusion(PlannedStmt* plan)
{
    if(!plan)return;ListCell* cell=NULL;
    foreach(cell,plan->rtable) {
        RangeTblEntry* entry=(RangeTblEntry*)lfirst(cell);
        if(entry->rtekind!=RTE_RELATION||entry->relkind!=RELKIND_RELATION)continue;
        Relation relation=heap_open(entry->relid,AccessShareLock);bool marked=LakeHasNativePartitionMarker(relation);
        heap_close(relation,NoLock);
        if(marked)ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake parent cannot execute an OpFusion heap fast path"),
            errhint("Replan through the native lake executor adapter; heap bypass never owns this parent's data.")));
    }
}

void LakeCheckNativePartitionExtensionDrop(void)
{
    /* Read the durable core marker, independently of extension table presence.
     * A missing/dropped private binding can therefore never authorize DROP or
     * silently turn the parent back into an empty local partitioned table. */
    Relation classes=heap_open(RelationRelationId,AccessShareLock);
    SysScanDesc scan=systable_beginscan(classes,InvalidOid,false,SnapshotNow,0,NULL);
    HeapTuple tuple=NULL;bool marked=false;
    while(HeapTupleIsValid(tuple=systable_getnext(scan))) {
        CHECK_FOR_INTERRUPTS();Form_pg_class form=(Form_pg_class)GETSTRUCT(tuple);
        if(form->relkind!=RELKIND_RELATION)continue;
        bool absent=false;Datum raw=heap_getattr(tuple,Anum_pg_class_reloptions,RelationGetDescr(classes),&absent);
        if(absent)continue;
        bytea* options=heap_reloptions(RELKIND_RELATION,raw,false);
        if(options) {marked=((StdRdOptions*)options)->lake_partition_root;pfree(options);}
        if(marked)break;
    }
    systable_endscan(scan);heap_close(classes,NoLock);
    if(marked)
        ereport(ERROR,(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
            errmsg("cannot drop og_lake while a native lake partition root is marked"),
            errhint("The closed bridge cannot remove a marker or downgrade a root to heap storage.")));
}

int LakeNativePartitionLeafLimit(void)
{
    return LakeNativePartitionExpandedVersionEnabled() ? LAKE_NATIVE_RANGE_LEAF_LIMIT : 2;
}

static void LakeNativeRangeCheckParent(Relation parent)
{
    bool expanded = LakeNativePartitionExpandedVersionEnabled();
    bool strategyOk = parent && parent->partMap && (PartitionMapIsRange(parent->partMap) ||
        (expanded && (PartitionMapIsList(parent->partMap) || PartitionMapIsHash(parent->partMap))));
    if(!parent || parent->rd_rel->relkind!=RELKIND_RELATION ||
       parent->rd_rel->relpersistence!=RELPERSISTENCE_PERMANENT ||
       !RelationIsPartitioned(parent) || RelationIsSubPartitioned(parent) ||
       !RelationIsRowFormat(parent) || RelationIsUstoreFormat(parent) || !strategyOk ||
       !PartExprKeyIsNull(parent))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake bridge requires a permanent one-level heap RANGE/LIST/HASH parent")));
}

LakeNativeRangeLayout LakeNativeRangeDescribe(Relation parent)
{
    LakeNativeRangeCheckParent(parent);
    bool expanded=LakeNativePartitionExpandedVersionEnabled();
    int limit=LakeNativePartitionLeafLimit();
    int2vector* keys=GetPartitionKey(parent->partMap);
    TupleDesc desc=RelationGetDescr(parent);
    if(!expanded){
        RangePartitionMap* map=(RangePartitionMap*)parent->partMap;
        if(!keys || keys->dim1!=1 || keys->values[0]<=0 || keys->values[0]>desc->natts ||
           desc->attrs[keys->values[0]-1].attisdropped ||
           desc->attrs[keys->values[0]-1].atttypid!=INT4OID ||
           map->rangeElementsNum!=2)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("native lake bridge foundation requires one int4 key and two native RANGE leaves")));
    }else{
        if(!keys || keys->dim1<1 || keys->dim1>MAX_PARTKEY_NUMS)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("native lake parent partition key layout is unsupported")));
        for(int k=0;k<keys->dim1;++k){
            AttrNumber att=keys->values[k];
            if(att<=0||att>desc->natts||desc->attrs[att-1].attisdropped||
               desc->attrs[att-1].atttypid==VECTOROID)
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("native lake parent partition key column type is unsupported")));
            (void)LakeNativeTypeByOid(desc->attrs[att-1].atttypid);
        }
    }

    LakeNativeRangeLayout layout={};layout.parent_relid=RelationGetRelid(parent);
    layout.strategy=parent->partMap->type;
    layout.key_attribute=keys->values[0];
    if(PartitionMapIsRange(parent->partMap)){
        RangePartitionMap* map=(RangePartitionMap*)parent->partMap;
        if(map->rangeElementsNum<1||map->rangeElementsNum>limit)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("native lake partition leaf count %d exceeds limit %d",map->rangeElementsNum,limit)));
        layout.leaf_count=map->rangeElementsNum;
        for(int i=0;i<layout.leaf_count;++i){
            RangeElement* element=&map->rangeElements[i];
            if(!OidIsValid(element->partitionOid) || !PARTITIONNO_IS_VALID(element->partitionno) || element->isInterval)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native range leaf identity")));
            layout.leaves[i]={layout.parent_relid,element->partitionOid,element->partitionno};
        }
    }else if(PartitionMapIsList(parent->partMap)){
        ListPartitionMap* map=(ListPartitionMap*)parent->partMap;
        if(map->listElementsNum<1||map->listElementsNum>limit)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("native lake LIST partition leaf count %d exceeds limit %d",map->listElementsNum,limit)));
        layout.leaf_count=map->listElementsNum;
        for(int i=0;i<layout.leaf_count;++i){
            ListPartElement* element=&map->listElements[i];
            if(!OidIsValid(element->partitionOid) || !PARTITIONNO_IS_VALID(element->partitionno))
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native list leaf identity")));
            layout.leaves[i]={layout.parent_relid,element->partitionOid,element->partitionno};
        }
    }else{
        HashPartitionMap* map=(HashPartitionMap*)parent->partMap;
        if(map->hashElementsNum<1||map->hashElementsNum>limit)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("native lake HASH partition leaf count %d exceeds limit %d",map->hashElementsNum,limit)));
        layout.leaf_count=map->hashElementsNum;
        for(int i=0;i<layout.leaf_count;++i){
            HashPartElement* element=&map->hashElements[i];
            if(!OidIsValid(element->partitionOid) || !PARTITIONNO_IS_VALID(element->partitionno))
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native hash leaf identity")));
            layout.leaves[i]={layout.parent_relid,element->partitionOid,element->partitionno};
        }
    }
    for(int i=0;i<layout.leaf_count;++i){
        for(int p=0;p<i;++p)if(layout.leaves[p].leaf_partition_oid==layout.leaves[i].leaf_partition_oid ||
            layout.leaves[p].partition_no==layout.leaves[i].partition_no)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("duplicate native partition leaf identity")));
    }
    return layout;
}

bool LakeNativePartitionIdentityEqual(const LakeNativePartitionIdentity* a,const LakeNativePartitionIdentity* b)
{
    return a && b && a->parent_relid==b->parent_relid && a->leaf_partition_oid==b->leaf_partition_oid &&
        a->partition_no==b->partition_no;
}

static void LakeNativeRangeCheckLayout(Relation parent,const LakeNativeRangeLayout* layout)
{
    LakeNativeRangeLayout now=LakeNativeRangeDescribe(parent);
    if(!layout || layout->parent_relid!=now.parent_relid || layout->strategy!=now.strategy ||
       layout->key_attribute!=now.key_attribute || layout->leaf_count!=now.leaf_count)
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native partition layout changed")));
    for(int i=0;i<now.leaf_count;++i)if(!LakeNativePartitionIdentityEqual(&layout->leaves[i],&now.leaves[i]))
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native partition leaf identity changed")));
}

static void LakeNativeRangeCheckOpened(Relation parent,Partition part,const LakeNativePartitionIdentity* wanted)
{
    if(!part || part->pd_id!=wanted->leaf_partition_oid || part->pd_part->parentid!=RelationGetRelid(parent) ||
       part->pd_part->parttype!=PART_OBJ_TYPE_TABLE_PARTITION || GetCurrentPartitionNo(part->pd_id)!=wanted->partition_no)
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native partition leaf was replaced or detached")));
}

struct LakeNativeEmptyResources { Partition part; Relation dummy; TableScanDesc scan; };

void LakeNativeRangeRequirePhysicallyEmpty(Relation parent,const LakeNativeRangeLayout* layout)
{
    LakeNativeRangeCheckLayout(parent,layout);
    LOCKTAG tag;SET_LOCKTAG_RELATION(tag,u_sess->proc_cxt.MyDatabaseId,RelationGetRelid(parent));
    if(!CheckLock(&tag,AccessExclusiveLock,false))
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("native lake empty-leaf validation requires the parent AccessExclusiveLock")));
    for(int i=0;i<layout->leaf_count;++i) {
        CHECK_FOR_INTERRUPTS();
        /* Fields survive PG longjmp; no changed automatic C++ local resource
         * is consulted by PG_CATCH and no ordinary C++ heap is allocated. */
        LakeNativeEmptyResources* resource=(LakeNativeEmptyResources*)palloc0(sizeof(LakeNativeEmptyResources));
        PG_TRY(); {
            const LakeNativePartitionIdentity* key=&layout->leaves[i];
            resource->part=PartitionOpenWithPartitionno(parent,key->leaf_partition_oid,key->partition_no,AccessExclusiveLock);
            LakeNativeRangeCheckOpened(parent,resource->part,key);resource->dummy=partitionGetRelation(parent,resource->part);
            resource->scan=tableam_scan_begin(resource->dummy,SnapshotAny,0,NULL);
            if(tableam_scan_getnexttuple(resource->scan,ForwardScanDirection)!=NULL)
                ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("native lake initial binding requires physically empty native leaves")));
            tableam_scan_end(resource->scan);resource->scan=NULL;releaseDummyRelation(&resource->dummy);
            partitionClose(parent,resource->part,NoLock);resource->part=NULL;
        } PG_CATCH(); {
            if(resource->scan)tableam_scan_end(resource->scan);
            if(resource->dummy)releaseDummyRelation(&resource->dummy);
            if(resource->part)partitionClose(parent,resource->part,NoLock);
            pfree(resource);PG_RE_THROW();
        } PG_END_TRY();
        pfree(resource);
    }
}

LakeNativePartitionIdentity LakeNativeRangeRoute(Relation parent,const LakeNativeRangeLayout* layout,TupleTableSlot* slot)
{
    LakeNativeRangeCheckLayout(parent,layout);
    if(!slot || !slot->tts_tupleDescriptor || slot->tts_tupleDescriptor->natts!=RelationGetDescr(parent)->natts)
        ereport(ERROR,(errcode(ERRCODE_DATATYPE_MISMATCH),errmsg("native partition route tuple descriptor mismatch")));
    for(int c=0;c<slot->tts_tupleDescriptor->natts;++c) {
        Form_pg_attribute actual=&slot->tts_tupleDescriptor->attrs[c];
        Form_pg_attribute expected=&RelationGetDescr(parent)->attrs[c];
        if(actual->attisdropped!=expected->attisdropped || actual->atttypid!=expected->atttypid ||
           actual->atttypmod!=expected->atttypmod || actual->attcollation!=expected->attcollation)
            ereport(ERROR,(errcode(ERRCODE_DATATYPE_MISMATCH),errmsg("native partition route column descriptor mismatch")));
    }
    HeapTuple tuple=(HeapTuple)tableam_tslot_get_tuple_from_slot(parent,slot);
    int partitionno=INVALID_PARTITION_NO;
    Oid leaf=heapTupleGetPartitionOid(parent,tuple,&partitionno,false,false,true);
    LakeNativePartitionIdentity identity={RelationGetRelid(parent),leaf,partitionno};
    int found=-1;
    for(int i=0;i<layout->leaf_count;++i)if(LakeNativePartitionIdentityEqual(&identity,&layout->leaves[i]))found=i;
    if(found<0)
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native router returned a changed or unbound leaf")));
    Partition part=PartitionOpenWithPartitionno(parent,leaf,partitionno,RowExclusiveLock);
    LakeNativeRangeCheckOpened(parent,part,&identity);partitionClose(parent,part,NoLock);
    return identity;
}
