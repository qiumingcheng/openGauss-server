/* Native heap row heads serialize writers of stable lake row IDs. No business
 * value or birth snapshot is stored here: after locking, the caller MUST look
 * up the latest lake version and perform native EPQ before staging an update.
 * Native MVCC owns insertion/tuple locks and subtransaction rollback; there is
 * deliberately no session cache of "already locked" logical rows. */
#include "postgres.h"
#include "lake_row_lock.h"
#include "lake_native_partition.h"
#include "lake_txn_private.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/xact.h"
#include "access/og_lake_maintenance.h"
#include "access/og_lake.h"
#include "commands/vacuum.h"
#include "nodes/makefuncs.h"
#include "pgstat.h"
#include "storage/buf/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "utils/timestamp.h"
#include "knl/knl_variable.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_class.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_event_trigger.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_index.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "commands/tablecmds.h"
#include "executor/spi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/uuid.h"

PG_FUNCTION_INFO_V1(og_lake_row_lock_catalog_upgrade);
PG_FUNCTION_INFO_V1(og_lake_reclaim_row_heads);

/* Private, disabled-by-default acceptance diagnostics; no installed SQL ABI. */
extern void LakeTestRowHeadBeforeEnsure(Oid relationOid,int64 rowId,const char* binding,int attempt);
extern void LakeTestRowHeadAfterEnsure(Oid relationOid,int64 rowId,const char* binding,int attempt,uint32 inserted);
extern void LakeTestRowHeadLockResult(Oid relationOid,int64 rowId,const char* binding,int attempt,uint32 count);
extern void LakeTestRowHeadExhausted(Oid relationOid,int64 rowId,const char* binding);

static char* LakeRowLockVersion(Oid* owner = NULL)
{
    Relation relation=heap_open(ExtensionRelationId,AccessShareLock);
    ScanKeyData key;
    ScanKeyInit(&key,Anum_pg_extension_extname,BTEqualStrategyNumber,F_NAMEEQ,CStringGetDatum("og_lake"));
    SysScanDesc scan=systable_beginscan(relation,ExtensionNameIndexId,true,NULL,1,&key);
    HeapTuple tuple=systable_getnext(scan);
    char* version=NULL;
    if(HeapTupleIsValid(tuple)) {
        bool isnull=false;
        Datum value=tableam_tops_tuple_getattr(tuple,Anum_pg_extension_extversion,RelationGetDescr(relation),&isnull);
        if(!isnull) version=TextDatumGetCString(value);
        if(owner!=NULL) *owner=((Form_pg_extension)GETSTRUCT(tuple))->extowner;
    }
    systable_endscan(scan);heap_close(relation,AccessShareLock);return version;
}

bool LakeRowLocksEnabled(void)
{
    char* version=LakeRowLockVersion();
    bool enabled=version!=NULL && (strcmp(version,"0.17")==0 || (strcmp(version,"0.18")==0||(strcmp(version,"0.19")==0||(strcmp(version,"0.20")==0||(strcmp(version,"0.21")==0||strcmp(version,"0.22")==0)))));
    if(version!=NULL) pfree(version);
    return enabled || LakeNativePartitionVersionEnabled();
}

bool LakeRowHeadReclaimEnabled(void)
{
    char* version=LakeRowLockVersion();
    bool enabled=version!=NULL && (strcmp(version,"0.18")==0||(strcmp(version,"0.19")==0||(strcmp(version,"0.20")==0||(strcmp(version,"0.21")==0||strcmp(version,"0.22")==0))));
    if(version!=NULL) pfree(version);
    return enabled || LakeNativePartitionVersionEnabled();
}

bool LakeNativeRowHeadMaintenanceEnabled(void)
{
    char* version=LakeRowLockVersion();
    const uint32 caps=LakeNativePartitionContractCapabilities(version);
    if(version!=NULL)pfree(version);
    return (caps&LAKE_NATIVE_PARENT_ROW_HEAD_MAINTENANCE)!=0;
}

static void LakeBadRowHead(void)
{
    ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
        errmsg("native lake stable row-head identity or shape changed")));
}

static Relation LakeOpenRowLockMap(Oid owner,LOCKMODE mode)
{
    Oid ns=get_namespace_oid("og_lake_internal",false);
    Oid oid=get_relname_relid("row_lock_binding",ns);
    if(!OidIsValid(oid) || getExtensionOfObject(RelationRelationId,oid)!=get_extension_oid("og_lake",false))
        LakeBadRowHead();
    LakeValidatePrivateNamespace(ns,owner);
    Relation map=heap_open(oid,mode);
    LakeValidatePrivateRelation(map,owner,true);return map;
}

static Datum LakeRowHeadAttribute(Relation relation,HeapTuple tuple,int attribute)
{
    bool isnull=false;
    Datum value=tableam_tops_tuple_getattr(tuple,attribute,RelationGetDescr(relation),&isnull);
    if(isnull) LakeBadRowHead();
    return value;
}

/* Registry identity is coupled to both existing canonical binding maps. These
 * are raw native reads, so validating a parsed private query cannot recurse
 * into SPI or execute administrator-supplied expressions. */
static bool LakeRowHeadCanonical(Oid headOid,Oid owner)
{
    Relation map=LakeOpenRowLockMap(owner,AccessShareLock);
    ScanKeyData key;
    ScanKeyInit(&key,3,BTEqualStrategyNumber,F_OIDEQ,ObjectIdGetDatum(headOid));
    SysScanDesc scan=systable_beginscan(map,InvalidOid,false,SnapshotNow,1,&key);
    HeapTuple tuple=systable_getnext(scan);
    if(!HeapTupleIsValid(tuple)) {
        systable_endscan(scan);heap_close(map,NoLock);return false;
    }
    Datum binding=datumCopy(LakeRowHeadAttribute(map,tuple,1),false,UUID_LEN);
    Oid host=DatumGetObjectId(LakeRowHeadAttribute(map,tuple,2));
    const bool nativeSchema=LakeNativePartitionVersionEnabled();
    Oid leaf=nativeSchema?DatumGetObjectId(LakeRowHeadAttribute(map,tuple,17)):InvalidOid;
    int partitionNo=nativeSchema?DatumGetInt32(LakeRowHeadAttribute(map,tuple,18)):0;
    if((OidIsValid(leaf)&&partitionNo<=0)||(!OidIsValid(leaf)&&partitionNo!=0)) LakeBadRowHead();
    if(HeapTupleIsValid(systable_getnext(scan))) LakeBadRowHead();
    systable_endscan(scan);heap_close(map,NoLock);
    Oid ns=get_namespace_oid("og_lake_internal",false);
    if(OidIsValid(leaf)) {
        if(!LakeNativePartitionWriteVersionEnabled()) LakeBadRowHead();
        Relation parent=heap_open(host,AccessShareLock);
        if(!LakeHasNativePartitionMarker(parent)) LakeBadRowHead();
        LakeNativeRangeLayout layout=LakeNativeRangeDescribe(parent);bool exact=false;
        for(int i=0;i<layout.leaf_count;++i)
            exact|=layout.leaves[i].leaf_partition_oid==leaf&&layout.leaves[i].partition_no==partitionNo;
        if(!exact) LakeBadRowHead();
        Partition part=PartitionOpenWithPartitionno(parent,leaf,partitionNo,AccessShareLock);
        if(!part||part->pd_id!=leaf) LakeBadRowHead();
        partitionClose(parent,part,NoLock);heap_close(parent,NoLock);
        Relation partitions=heap_open(get_relname_relid("partition_binding",ns),AccessShareLock);
        LakeValidatePrivateRelation(partitions,owner,true);
        ScanKeyData keys[3];
        ScanKeyInit(&keys[0],1,BTEqualStrategyNumber,F_OIDEQ,ObjectIdGetDatum(host));
        ScanKeyInit(&keys[1],2,BTEqualStrategyNumber,F_OIDEQ,ObjectIdGetDatum(leaf));
        ScanKeyInit(&keys[2],4,BTEqualStrategyNumber,F_UUID_EQ,binding);
        scan=systable_beginscan(partitions,InvalidOid,false,SnapshotNow,3,keys);tuple=systable_getnext(scan);
        if(!HeapTupleIsValid(tuple)||DatumGetInt32(LakeRowHeadAttribute(partitions,tuple,3))!=partitionNo)
            LakeBadRowHead();
        if(HeapTupleIsValid(systable_getnext(scan))) LakeBadRowHead();
        systable_endscan(scan);heap_close(partitions,NoLock);
    } else {
        Relation relations=heap_open(get_relname_relid("relation_binding",ns),AccessShareLock);
        LakeValidatePrivateRelation(relations,owner,true);
        ScanKeyInit(&key,1,BTEqualStrategyNumber,F_OIDEQ,ObjectIdGetDatum(host));
        scan=systable_beginscan(relations,InvalidOid,false,SnapshotNow,1,&key);
        tuple=systable_getnext(scan);
        if(!HeapTupleIsValid(tuple) || !DatumGetBool(DirectFunctionCall2(uuid_eq,binding,
            LakeRowHeadAttribute(relations,tuple,2)))) LakeBadRowHead();
        if(HeapTupleIsValid(systable_getnext(scan))) LakeBadRowHead();
        systable_endscan(scan);heap_close(relations,NoLock);
    }
    Relation tables=heap_open(get_relname_relid("table_binding",ns),AccessShareLock);
    LakeValidatePrivateRelation(tables,owner,true);
    ScanKeyInit(&key,1,BTEqualStrategyNumber,F_UUID_EQ,binding);
    scan=systable_beginscan(tables,InvalidOid,false,SnapshotNow,1,&key);
    tuple=systable_getnext(scan);
    if(!HeapTupleIsValid(tuple)) LakeBadRowHead();
    Oid server=DatumGetObjectId(LakeRowHeadAttribute(tables,tuple,2));
    char* kind=TextDatumGetCString(LakeRowHeadAttribute(tables,tuple,6));
    if(strcmp(kind,OidIsValid(leaf)?"partition":"relation")) LakeBadRowHead();pfree(kind);
    if(HeapTupleIsValid(systable_getnext(scan))) LakeBadRowHead();
    systable_endscan(scan);heap_close(tables,NoLock);
    if(!OidIsValid(leaf)&&(get_rel_relkind(host)!=RELKIND_FOREIGN_TABLE || GetForeignTable(host)->serverid!=server)) LakeBadRowHead();
    Relation dependencies=heap_open(DependRelationId,AccessShareLock);
    ScanKeyData depKeys[2];
    ScanKeyInit(&depKeys[0],Anum_pg_depend_classid,BTEqualStrategyNumber,F_OIDEQ,ObjectIdGetDatum(RelationRelationId));
    ScanKeyInit(&depKeys[1],Anum_pg_depend_objid,BTEqualStrategyNumber,F_OIDEQ,ObjectIdGetDatum(headOid));
    scan=systable_beginscan(dependencies,DependDependerIndexId,true,SnapshotNow,2,depKeys);
    int internal=0;
    while(HeapTupleIsValid(tuple=systable_getnext(scan))) {
        Form_pg_depend dep=(Form_pg_depend)GETSTRUCT(tuple);
        if(dep->deptype!=DEPENDENCY_INTERNAL) continue;
        if(dep->objsubid!=0 || dep->refclassid!=RelationRelationId || dep->refobjid!=host || dep->refobjsubid!=0)
            LakeBadRowHead();
        ++internal;
    }
    systable_endscan(scan);heap_close(dependencies,NoLock);
    if(internal!=1) LakeBadRowHead();
    pfree(DatumGetPointer(binding));return true;
}

static void LakeCheckRowHeadShape(Relation relation,Oid owner)
{
    Oid ns=get_namespace_oid("og_lake_internal",false);
    if(RelationGetNamespace(relation)!=ns || RelationIsUstoreFormat(relation) || RELATION_HAS_UIDS(relation) ||
       OidIsValid(getExtensionOfObject(RelationRelationId,RelationGetRelid(relation)))) LakeBadRowHead();
    LakeValidatePrivateRelation(relation,owner,false);
    TupleDesc desc=RelationGetDescr(relation);
    if(desc->natts!=1 || desc->attrs[0].attisdropped || !desc->attrs[0].attnotnull ||
       desc->attrs[0].atttypid!=INT8OID || strcmp(NameStr(desc->attrs[0].attname),"row_id")!=0 ||
       OidIsValid(desc->attrs[0].attcollation)) LakeBadRowHead();
    List* indexes=RelationGetIndexList(relation);
    if(list_length(indexes)!=1) LakeBadRowHead();
    Relation index=index_open(linitial_oid(indexes),AccessShareLock);
    if(index->rd_rel->relam!=BTREE_AM_OID || index->rd_index->indrelid!=RelationGetRelid(relation) ||
       !index->rd_index->indisprimary || !index->rd_index->indisunique || index->rd_index->indnatts!=1 ||
       index->rd_index->indkey.values[0]!=1 || index->rd_opfamily[0]!=INTEGER_BTREE_FAM_OID)
        LakeBadRowHead();
    index_close(index,NoLock);list_free(indexes);
}

bool LakeIsRegisteredRowLockHead(Relation relation,Oid owner)
{
    if(RelationGetNamespace(relation)!=get_namespace_oid("og_lake_internal",false) ||
       !LakeRowHeadCanonical(RelationGetRelid(relation),owner)) return false;
    LakeCheckRowHeadShape(relation,owner);return true;
}

List* LakeRowLockHeadRelations(Oid owner)
{
    Relation map=LakeOpenRowLockMap(owner,AccessShareLock);
    SysScanDesc scan=systable_beginscan(map,InvalidOid,false,SnapshotNow,0,NULL);
    List* heads=NIL;HeapTuple tuple;
    while(HeapTupleIsValid(tuple=systable_getnext(scan))) {
        Oid oid=DatumGetObjectId(LakeRowHeadAttribute(map,tuple,3));
        Relation head=heap_open(oid,AccessShareLock);
        if(!LakeIsRegisteredRowLockHead(head,owner)) LakeBadRowHead();
        heap_close(head,NoLock);heads=lappend_oid(heads,oid);
    }
    systable_endscan(scan);heap_close(map,NoLock);return heads;
}

/* Each metadata command takes its own latest snapshot. In particular the
 * SELECT after a unique-key wait must not reuse the pre-INSERT snapshot. */
static LakeMetaRows* LakeRowHeadQuery(const char* sql,int nargs,Oid* types,Datum* args,bool readOnly,int status)
{
    Snapshot fresh=RegisterSnapshot(GetLatestSnapshot());
    LakeMetaRows* result=NULL;
    PG_TRY();
    {
        result=LakeMetadataQuery(sql,nargs,types,args,NULL,fresh,readOnly,status);
        UnregisterSnapshot(fresh);
    }
    PG_CATCH();{UnregisterSnapshot(fresh);PG_RE_THROW();}PG_END_TRY();
    return result;
}

/* Closed DDL only. The configured role cannot execute user event triggers.
 * Dynamic row-head tables follow their business relation, not extension
 * membership, including when they are backfilled by ALTER EXTENSION. */
static void LakeRowHeadUtility(const char* sql,Oid owner)
{
    if(LakeGetMetadataScope()!=NULL)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("row-head DDL cannot nest in a metadata scope")));
    Relation events=heap_open(EventTriggerRelationId,ShareLock);
    SysScanDesc scan=systable_beginscan(events,InvalidOid,false,SnapshotNow,0,NULL);
    HeapTuple tuple;
    while(HeapTupleIsValid(tuple=systable_getnext(scan)))
        if(((Form_pg_event_trigger)GETSTRUCT(tuple))->evtenabled!='D')
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("lake row-head DDL requires no enabled event triggers")));
    systable_endscan(scan);heap_close(events,NoLock);
    Oid savedUser;int savedSecurity;GetUserIdAndSecContext(&savedUser,&savedSecurity);
    MemoryContext caller=CurrentMemoryContext;
    bool savedCreating=creating_extension;
    volatile bool connected=false,pathPushed=false;
    volatile int gucNest=0;
    PG_TRY();
    {
        gucNest=NewGUCNestLevel();
        SetUserIdAndSecContext(owner,savedSecurity|SECURITY_LOCAL_USERID_CHANGE|SECURITY_RESTRICTED_OPERATION);
        OverrideSearchPath path;errno_t rc=memset_s(&path,sizeof(path),0,sizeof(path));securec_check(rc,"","");
        path.schemas=list_make1_oid(PG_CATALOG_NAMESPACE);PushOverrideSearchPath(&path);pathPushed=true;
        if(SPI_connect()!=SPI_OK_CONNECT) elog(ERROR,"could not connect row-head DDL SPI");
        connected=true;creating_extension=false;
        int status=SPI_execute(sql,false,0);
        creating_extension=savedCreating;SetUserIdAndSecContext(savedUser,savedSecurity);
        if(status!=SPI_OK_UTILITY) elog(ERROR,"unexpected row-head DDL SPI status %d",status);
        int finish=SPI_finish();connected=false;
        if(finish!=SPI_OK_FINISH) elog(ERROR,"could not finish row-head DDL SPI");
        PopOverrideSearchPath();pathPushed=false;AtEOXact_GUC(false,gucNest);gucNest=0;
        MemoryContextSwitchTo(caller);
    }
    PG_CATCH();
    {
        creating_extension=savedCreating;SetUserIdAndSecContext(savedUser,savedSecurity);
        MemoryContextSwitchTo(caller);
        if(connected) (void)SPI_finish();
        if(pathPushed) PopOverrideSearchPath();
        if(gucNest!=0) AtEOXact_GUC(false,gucNest);
        PG_RE_THROW();
    }
    PG_END_TRY();
    CommandCounterIncrement();
}

static void LakePrepareRowLockBindingInternal(const LakeInlineBinding* binding,Oid relationOid,
    const LakeNativePartitionIdentity* native=NULL)
{
    LakeLockCatalogRegistration();
    CommandCounterIncrement();
    Oid owner=LakeMetadataOwner();
    if(!OidIsValid(owner)) {LakeInlineRequireAdministrator();owner=GetUserId();}
    Oid type=OIDOID;Datum argument=ObjectIdGetDatum(relationOid);
    Oid canonicalTypes[3]={OIDOID,OIDOID,INT4OID};
    Datum canonicalArgs[3]={argument,ObjectIdGetDatum(native?native->leaf_partition_oid:InvalidOid),
        Int32GetDatum(native?native->partition_no:0)};
    LakeMetaRows* canonical=native?LakeRowHeadQuery(
        "SELECT p.binding_id::text,t.server_oid,t.table_id,t.table_uuid::text FROM og_lake_internal.partition_binding p "
        "JOIN og_lake_internal.table_binding t USING(binding_id) WHERE p.parent_relid=$1 AND p.leaf_partition_oid=$2 AND p.partition_no=$3 "
        "AND t.object_kind='partition' AND t.state='active'",3,canonicalTypes,canonicalArgs,true,SPI_OK_SELECT):LakeRowHeadQuery(
        "SELECT r.binding_id::text,t.server_oid,t.table_id,t.table_uuid::text FROM og_lake_internal.relation_binding r "
        "JOIN og_lake_internal.table_binding t USING(binding_id) WHERE r.relation_oid=$1",
        1,&type,&argument,true,SPI_OK_SELECT);
    LakeMetadataRequireCount(canonical,1,"canonical row-head binding");
    char* uuid=LakeMetadataText(canonical,0,1);
    if(binding!=NULL && (binding->server_oid!=DatumGetObjectId(LakeMetadataValue(canonical,0,2,OIDOID)) ||
        (binding->identity.kind==LAKE_TABLE_PERMANENT && LakePermanentTableId(binding)!=DatumGetInt64(LakeMetadataValue(canonical,0,3,INT8OID))) ||
        strcmp(binding->table_uuid,LakeMetadataText(canonical,0,4))!=0)) LakeBadRowHead();
    const bool nativeSchema=LakeNativePartitionVersionEnabled();
    LakeMetaRows* existing=nativeSchema?LakeRowHeadQuery(
        "SELECT row_head_oid FROM og_lake_internal.row_lock_binding WHERE relation_oid=$1 AND leaf_partition_oid=$2 AND partition_no=$3",
        3,canonicalTypes,canonicalArgs,true,SPI_OK_SELECT):LakeRowHeadQuery(
        "SELECT row_head_oid FROM og_lake_internal.row_lock_binding WHERE relation_oid=$1",1,&type,&argument,true,SPI_OK_SELECT);
    if(existing->count!=0) {
        LakeMetadataRequireCount(existing,1,"existing row-head binding");
        Relation head=heap_open(DatumGetObjectId(LakeMetadataValue(existing,0,1,OIDOID)),AccessShareLock);
        if(!LakeIsRegisteredRowLockHead(head,owner)) LakeBadRowHead();
        heap_close(head,NoLock);return;
    }
    char compact[33];int length=0;
    for(const char* p=uuid;*p;++p) if(*p!='-') {
        if(length>=32) LakeBadRowHead();compact[length++]=*p;
    }
    if(length!=32) LakeBadRowHead();compact[length]='\0';
    char* name=psprintf("_og_lake_row_head_%s",compact);
    Oid ns=get_namespace_oid("og_lake_internal",false);
    if(OidIsValid(get_relname_relid(name,ns))) LakeBadRowHead();
    LakeRowHeadUtility(psprintf("CREATE TABLE %s (row_id pg_catalog.int8 PRIMARY KEY) "
        "WITH (orientation=row,storage_type=astore,hasuids=false)",quote_qualified_identifier("og_lake_internal",name)),owner);
    Oid headOid=get_relname_relid(name,ns);
    if(!OidIsValid(headOid)) LakeBadRowHead();
    ObjectAddress head,host;
    ObjectAddressSet(head,RelationRelationId,headOid);ObjectAddressSet(host,RelationRelationId,relationOid);
    recordDependencyOn(&head,&host,DEPENDENCY_INTERNAL);
    Oid types[5]={UUIDOID,OIDOID,OIDOID,OIDOID,INT4OID};
    Datum args[5]={DirectFunctionCall1(uuid_in,CStringGetDatum(uuid)),argument,ObjectIdGetDatum(headOid),
        canonicalArgs[1],canonicalArgs[2]};
    if(nativeSchema) {
        LakeMetadataRequireCount(LakeRowHeadQuery("INSERT INTO og_lake_internal.row_lock_binding(binding_id,relation_oid,row_head_oid,reclaim_cursor,reclaim_round_end,"
            "reclaim_scanned,reclaimed_rows,reclaim_rounds,last_reclaim_at,last_vacuum_at,live_rows_estimate,dead_rows_estimate,head_bytes,soft_budget_exceeded,last_delete_xid,oldest_xmin_observed,leaf_partition_oid,partition_no) "
            "VALUES($1,$2,$3,-1,-1,0,0,0,NULL,NULL,NULL,NULL,NULL,false,NULL,NULL,$4,$5)",5,types,args,false,SPI_OK_INSERT),1,"new typed row-head binding");
    } else if(LakeRowHeadReclaimEnabled()) {
    LakeMetadataRequireCount(LakeRowHeadQuery("INSERT INTO og_lake_internal.row_lock_binding(binding_id,relation_oid,row_head_oid,reclaim_cursor,reclaim_round_end,"
        "reclaim_scanned,reclaimed_rows,reclaim_rounds,last_reclaim_at,last_vacuum_at,live_rows_estimate,dead_rows_estimate,head_bytes,soft_budget_exceeded,last_delete_xid,oldest_xmin_observed) "
        "VALUES($1,$2,$3,-1,-1,0,0,0,NULL,NULL,NULL,NULL,NULL,false,NULL,NULL)",3,types,args,false,SPI_OK_INSERT),1,"new row-head binding");
    } else {
        LakeMetadataRequireCount(LakeRowHeadQuery("INSERT INTO og_lake_internal.row_lock_binding(binding_id,relation_oid,row_head_oid) "
            "VALUES($1,$2,$3)",3,types,args,false,SPI_OK_INSERT),1,"new row-head binding");
    }
    CommandCounterIncrement();
    Relation checked=heap_open(headOid,AccessShareLock);
    if(!LakeIsRegisteredRowLockHead(checked,owner)) LakeBadRowHead();
    heap_close(checked,NoLock);
}

void LakePrepareRowLockBinding(const LakeInlineBinding* binding,Oid relationOid,Snapshot snapshot)
{
    (void)snapshot; /* Binding DDL needs own-CID/current metadata, never an old business snapshot. */
    if(LakeRowLocksEnabled()) LakePrepareRowLockBindingInternal(binding,relationOid);
}

void LakePrepareNativePartitionRowLockBinding(const LakeInlineBinding* binding,
    const LakeNativePartitionIdentity* identity,Snapshot snapshot)
{
    if(!LakeNativePartitionWriteVersionEnabled()||!binding||!identity||!snapshot||
       binding->relation_oid!=identity->parent_relid||!OidIsValid(identity->leaf_partition_oid)||
       identity->partition_no<=0||binding->identity.kind!=LAKE_TABLE_PERMANENT)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native leaf row-head preparation requires the coordinated typed write binding")));
    LOCKTAG tag;SET_LOCKTAG_RELATION(tag,u_sess->proc_cxt.MyDatabaseId,identity->parent_relid);
    if(!CheckLock(&tag,AccessExclusiveLock,false))
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native row-head promotion requires the parent AccessExclusiveLock")));
    Relation parent=heap_open(identity->parent_relid,NoLock);
    if(!LakeHasNativePartitionMarker(parent)) LakeBadRowHead();
    LakeNativeRangeLayout layout=LakeNativeRangeDescribe(parent);bool exact=false;
    for(int i=0;i<layout.leaf_count;++i)exact|=LakeNativePartitionIdentityEqual(identity,&layout.leaves[i]);
    if(!exact) LakeBadRowHead();
    heap_close(parent,NoLock);
    LakePrepareRowLockBindingInternal(binding,identity->parent_relid,identity);
}

void LakeDropRowLockBinding(Oid relationOid,Snapshot snapshot)
{
    (void)snapshot;
    if(!LakeRowLocksEnabled()) return;
    CommandCounterIncrement();
    Oid type=OIDOID;Datum arg=ObjectIdGetDatum(relationOid);
    /* Dependency traversal may already have removed the INTERNAL head heap.
     * Do not try to reopen it here; the host DROP and this map deletion share
     * one transaction/savepoint and restore together on rollback. */
    const char* removal=LakeNativePartitionVersionEnabled()?
        "DELETE FROM og_lake_internal.row_lock_binding WHERE relation_oid=$1 AND leaf_partition_oid='0'::pg_catalog.oid AND partition_no=0":
        "DELETE FROM og_lake_internal.row_lock_binding WHERE relation_oid=$1";
    LakeMetadataRequireCount(LakeRowHeadQuery(removal,1,&type,&arg,false,SPI_OK_DELETE),1,"deleted row-head binding");
    CommandCounterIncrement();
}

void LakeLockLogicalRow(LakeTxnTable* table,int64 rowId)
{
    if(!LakeRowLocksEnabled()) return;
    LakeTxnRequireActiveTable(table);
    if(table->binding->identity.kind!=LAKE_TABLE_PERMANENT || table->binding->binding_id==NULL ||
       table->binding->table_uuid==NULL || rowId<0 || rowId>=LAKE_REFERENCE_LOCAL_ID_START)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("row-head lock requires a permanent lake row ID")));
    MemoryContext caller=CurrentMemoryContext;
    MemoryContext work=AllocSetContextCreate(caller,"Lake row-head lock",ALLOCSET_SMALL_MINSIZE,ALLOCSET_SMALL_INITSIZE,ALLOCSET_SMALL_MAXSIZE);
    PG_TRY();
    {
        MemoryContextSwitchTo(work);
        CommandCounterIncrement();
        Oid types[5]={OIDOID,UUIDOID,OIDOID,INT8OID,UUIDOID};
        Datum args[5]={ObjectIdGetDatum(table->binding->relation_oid),
            DirectFunctionCall1(uuid_in,CStringGetDatum(table->binding->binding_id)),
            ObjectIdGetDatum(table->binding->server_oid),Int64GetDatum(LakePermanentTableId(table->binding)),
            DirectFunctionCall1(uuid_in,CStringGetDatum(table->binding->table_uuid))};
        LakeMetaRows* found=NULL;
        if(OidIsValid(table->native_leaf_partition_oid)) {
            if(!LakeNativePartitionWriteVersionEnabled()||table->native_partition_no<=0||table->binding->read_only)
                LakeBadRowHead();
            Oid typedTypes[7]={OIDOID,UUIDOID,OIDOID,INT8OID,UUIDOID,OIDOID,INT4OID};
            Datum typedArgs[7]={args[0],args[1],args[2],args[3],args[4],ObjectIdGetDatum(table->native_leaf_partition_oid),Int32GetDatum(table->native_partition_no)};
            found=LakeRowHeadQuery("SELECT h.row_head_oid FROM og_lake_internal.row_lock_binding h "
                "JOIN og_lake_internal.partition_binding p ON p.parent_relid=h.relation_oid AND p.leaf_partition_oid=h.leaf_partition_oid "
                "AND p.partition_no=h.partition_no AND p.binding_id=h.binding_id "
                "JOIN og_lake_internal.table_binding t ON t.binding_id=h.binding_id "
                "WHERE h.relation_oid=$1 AND h.binding_id=$2 AND t.server_oid=$3 AND t.table_id=$4 AND t.table_uuid=$5 "
                "AND h.leaf_partition_oid=$6 AND h.partition_no=$7 AND t.object_kind='partition' AND t.state='active' AND NOT t.read_only",
                7,typedTypes,typedArgs,true,SPI_OK_SELECT);
        } else {
            const char* ordinary="SELECT h.row_head_oid FROM og_lake_internal.row_lock_binding h "
                "JOIN og_lake_internal.relation_binding r ON r.relation_oid=h.relation_oid AND r.binding_id=h.binding_id "
                "JOIN og_lake_internal.table_binding t ON t.binding_id=h.binding_id "
                "WHERE h.relation_oid=$1 AND h.binding_id=$2 AND t.server_oid=$3 AND t.table_id=$4 AND t.table_uuid=$5";
            found=LakeRowHeadQuery(LakeNativePartitionVersionEnabled()?
                psprintf("%s AND h.leaf_partition_oid='0'::pg_catalog.oid AND h.partition_no=0",ordinary):ordinary,
                5,types,args,true,SPI_OK_SELECT);
        }
        LakeMetadataRequireCount(found,1,"logical row-head owner");
        Oid headOid=DatumGetObjectId(LakeMetadataValue(found,0,1,OIDOID));
        Relation head=heap_open(headOid,RowShareLock);
        Oid owner=LakeMetadataOwner();if(!OidIsValid(owner)) owner=GetUserId();
        if(!LakeIsRegisteredRowLockHead(head,owner)) LakeBadRowHead();
        char* relation=quote_qualified_identifier("og_lake_internal",RelationGetRelationName(head));
        Oid rowType=INT8OID;Datum row=Int64GetDatum(rowId);
        MemoryContext attempts=AllocSetContextCreate(work,"Lake row-head ensure attempts",ALLOCSET_SMALL_SIZES);
        bool locked=false;
        for(int attempt=0;attempt<(LakeRowHeadReclaimEnabled()?32:1) && !locked;++attempt) {
            CHECK_FOR_INTERRUPTS();MemoryContextSwitchTo(work);MemoryContextReset(attempts);MemoryContextSwitchTo(attempts);
            LakeTestRowHeadBeforeEnsure(table->binding->relation_oid,rowId,table->binding->binding_id,attempt+1);
            LakeMetaRows* inserted=LakeRowHeadQuery(psprintf("INSERT INTO %s(row_id) VALUES($1) ON CONFLICT DO NOTHING",relation),
                1,&rowType,&row,false,SPI_OK_INSERT);
            if(inserted->count>1) LakeBadRowHead();
            CommandCounterIncrement();
            LakeTestRowHeadAfterEnsure(table->binding->relation_oid,rowId,table->binding->binding_id,attempt+1,inserted->count);
            LakeMetaRows* foundHead=LakeRowHeadQuery(psprintf("SELECT row_id FROM %s WHERE row_id=$1 FOR UPDATE",relation),
                1,&rowType,&row,false,SPI_OK_SELECT);
            if(foundHead->count>1) LakeBadRowHead();
            locked=foundHead->count==1;
            LakeTestRowHeadLockResult(table->binding->relation_oid,rowId,table->binding->binding_id,attempt+1,foundHead->count);
            if(!locked) {
                if(!LakeRowHeadReclaimEnabled()) LakeMetadataRequireCount(foundHead,1,"locked stable row head");
                /* A concurrent DELETE can retire the old ctid between ensure
                 * and locking. Retry the stable key, never an old address. */
                CHECK_FOR_INTERRUPTS();pg_usleep(1000L);
            }
        }
        MemoryContextSwitchTo(work);MemoryContextDelete(attempts);
        if(!locked) {
            LakeTestRowHeadExhausted(table->binding->relation_oid,rowId,table->binding->binding_id);
            ereport(ERROR,(errcode(ERRCODE_LOCK_NOT_AVAILABLE),errmsg("native lake row-head was repeatedly reclaimed while locking"),
                errhint("Retry the statement; metadata maintenance is contending for this logical row.")));
        }
        heap_close(head,NoLock);
        MemoryContextSwitchTo(caller);MemoryContextDelete(work);
    }
    PG_CATCH();{MemoryContextSwitchTo(caller);MemoryContextDelete(work);PG_RE_THROW();}PG_END_TRY();
}

/* Called only by the existing, claimed launcher/worker maintenance loop.
 * The persistent cursor advances over inspected KEYS, including busy tuples.
 * Each round has a fixed high key so continuous high-key inserts cannot keep
 * older low-key heads outside the next round forever. */
static void LakeRequireHeadWorker(void)
{
    if(!IsNativeLakeMaintenanceProcess() || IsNativeLakeLauncherProcess())
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("row-head reclamation is restricted to the native maintenance worker")));
}

static void LakeRequireNativeHeadTarget(const LakeRowHeadMaintenanceTarget* target)
{
    if(target==NULL || !OidIsValid(target->relation_oid) ||
       !OidIsValid(target->leaf_partition_oid) || !OidIsValid(target->head_oid) ||
       target->partition_no<=0 || strnlen(target->binding_id,sizeof(target->binding_id))!=36 ||
       !OidIsValid(target->server_oid) || target->table_id<0 || target->table_id>=LAKE_REFERENCE_LOCAL_ID_START ||
       strnlen(target->table_uuid,sizeof(target->table_uuid))!=36)
        LakeBadRowHead();
}

/* Metadata-only complete owner lookup. It never reads a lake file, acquires a
 * row_id lock, or treats relation_oid as a unique key in the typed registry. */
static LakeMetaRows* LakeNativeHeadMapping(const LakeRowHeadMaintenanceTarget* target,bool lockMap)
{
    LakeRequireNativeHeadTarget(target);
    Oid types[8]={OIDOID,OIDOID,INT4OID,UUIDOID,OIDOID,OIDOID,INT8OID,UUIDOID};
    Datum args[8]={ObjectIdGetDatum(target->relation_oid),ObjectIdGetDatum(target->leaf_partition_oid),
        Int32GetDatum(target->partition_no),DirectFunctionCall1(uuid_in,CStringGetDatum(target->binding_id)),
        ObjectIdGetDatum(target->head_oid),ObjectIdGetDatum(target->server_oid),Int64GetDatum(target->table_id),
        DirectFunctionCall1(uuid_in,CStringGetDatum(target->table_uuid))};
    return LakeRowHeadQuery(psprintf("SELECT h.row_head_oid,h.reclaim_cursor,h.reclaim_round_end,h.soft_budget_exceeded "
        "FROM og_lake_internal.row_lock_binding h "
        "JOIN og_lake_internal.partition_binding p ON p.parent_relid=h.relation_oid "
        "AND p.leaf_partition_oid=h.leaf_partition_oid AND p.partition_no=h.partition_no AND p.binding_id=h.binding_id "
        "JOIN og_lake_internal.table_binding t ON t.binding_id=p.binding_id "
        "JOIN og_lake_internal.catalog_binding c ON c.server_oid=t.server_oid "
        "WHERE h.relation_oid=$1 AND h.leaf_partition_oid=$2 AND h.partition_no=$3 AND h.binding_id=$4 "
        "AND h.row_head_oid=$5 AND t.server_oid=$6 AND t.table_id=$7 AND t.table_uuid=$8 "
        "AND t.object_kind='partition' AND t.state='active' AND NOT t.read_only AND NOT c.read_only%s",
        lockMap?" FOR UPDATE OF h SKIP LOCKED":""),8,types,args,!lockMap,SPI_OK_SELECT);
}

/* Parent admission is independent of file/inline cardinality. Check the whole
 * fixed two-leaf canonical binding before choosing a single maintenance head.
 * A read-only parent has no row heads; report read-only, not a missing-head or
 * empty-success result. Parent lock is retained by the caller transaction. */
static void LakeRequireNativeHeadParent(Relation parent)
{
    if(!LakeHasNativePartitionMarker(parent)) LakeBadRowHead();
    LakeNativeRangeLayout layout=LakeNativeRangeDescribe(parent);
    Oid type=OIDOID;Datum argument=ObjectIdGetDatum(RelationGetRelid(parent));
    LakeMetaRows* rows=LakeRowHeadQuery("SELECT p.leaf_partition_oid,p.partition_no,t.read_only,c.read_only,"
        "t.object_kind,t.state,h.row_head_oid FROM og_lake_internal.partition_binding p "
        "JOIN og_lake_internal.table_binding t ON t.binding_id=p.binding_id "
        "JOIN og_lake_internal.catalog_binding c ON c.server_oid=t.server_oid "
        "LEFT JOIN og_lake_internal.row_lock_binding h ON h.relation_oid=p.parent_relid "
        "AND h.leaf_partition_oid=p.leaf_partition_oid AND h.partition_no=p.partition_no AND h.binding_id=p.binding_id "
        "WHERE p.parent_relid=$1 "
        "ORDER BY p.leaf_partition_oid",1,&type,&argument,true,SPI_OK_SELECT);
    LakeMetadataRequireCount(rows,layout.leaf_count,"native parent row-head canonical leaves");
    for(uint32 i=0;i<rows->count;++i) {
        Oid leaf=DatumGetObjectId(LakeMetadataValue(rows,i,1,OIDOID));
        int number=DatumGetInt32(LakeMetadataValue(rows,i,2,INT4OID));bool found=false;
        for(int j=0;j<layout.leaf_count;++j)
            found|=layout.leaves[j].leaf_partition_oid==leaf&&layout.leaves[j].partition_no==number;
        if(!found || strcmp(LakeMetadataText(rows,i,5),"partition") || strcmp(LakeMetadataText(rows,i,6),"active"))
            LakeBadRowHead();
        if(DatumGetBool(LakeMetadataValue(rows,i,3,BOOLOID)) || DatumGetBool(LakeMetadataValue(rows,i,4,BOOLOID)))
            ereport(ERROR,(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),errmsg("cannot reclaim row heads of a read-only lake binding")));
        if(!OidIsValid(DatumGetObjectId(LakeMetadataValue(rows,i,7,OIDOID)))) LakeBadRowHead();
    }
}

/* One map row per call, oldest attempted leaf first. SKIP applies only to the
 * private map tuple; the chosen leaf's existing bounded key scan also skips
 * busy head tuples. Advancing even a busy/empty leaf gives its sibling a turn. */
static bool LakeSelectNativeHead(Relation parent,LakeRowHeadMaintenanceTarget* target)
{
    LakeRequireNativeHeadParent(parent);
    const bool saved=u_sess->attr.attr_common.allow_concurrent_tuple_update;
    LakeMetaRows* rows=NULL;Oid type=OIDOID;Datum argument=ObjectIdGetDatum(RelationGetRelid(parent));
    PG_TRY();
    {
        u_sess->attr.attr_common.allow_concurrent_tuple_update=true;
        rows=LakeRowHeadQuery("SELECT h.relation_oid,h.leaf_partition_oid,h.partition_no,h.binding_id::text,h.row_head_oid,"
            "t.server_oid,t.table_id,t.table_uuid::text "
            "FROM og_lake_internal.row_lock_binding h "
            "JOIN og_lake_internal.partition_binding p ON p.parent_relid=h.relation_oid "
            "AND p.leaf_partition_oid=h.leaf_partition_oid AND p.partition_no=h.partition_no AND p.binding_id=h.binding_id "
            "JOIN og_lake_internal.table_binding t ON t.binding_id=p.binding_id "
            "JOIN og_lake_internal.catalog_binding c ON c.server_oid=t.server_oid "
            "WHERE h.relation_oid=$1 AND h.leaf_partition_oid<>'0'::pg_catalog.oid AND h.partition_no>0 "
            "AND t.object_kind='partition' AND t.state='active' AND NOT t.read_only AND NOT c.read_only "
            "ORDER BY h.last_reclaim_at ASC NULLS FIRST,h.leaf_partition_oid LIMIT 1 FOR UPDATE OF h SKIP LOCKED",
            1,&type,&argument,false,SPI_OK_SELECT);
        u_sess->attr.attr_common.allow_concurrent_tuple_update=saved;
    }
    PG_CATCH();{u_sess->attr.attr_common.allow_concurrent_tuple_update=saved;PG_RE_THROW();}PG_END_TRY();
    if(rows->count==0) return false;
    LakeMetadataRequireCount(rows,1,"one native leaf row-head maintenance target");
    target->relation_oid=DatumGetObjectId(LakeMetadataValue(rows,0,1,OIDOID));
    target->leaf_partition_oid=DatumGetObjectId(LakeMetadataValue(rows,0,2,OIDOID));
    target->partition_no=DatumGetInt32(LakeMetadataValue(rows,0,3,INT4OID));
    char* binding=LakeMetadataText(rows,0,4);
    if(strlen(binding)!=36) LakeBadRowHead();
    errno_t rc=strcpy_s(target->binding_id,sizeof(target->binding_id),binding);securec_check(rc,"","");
    target->head_oid=DatumGetObjectId(LakeMetadataValue(rows,0,5,OIDOID));
    target->server_oid=DatumGetObjectId(LakeMetadataValue(rows,0,6,OIDOID));
    target->table_id=DatumGetInt64(LakeMetadataValue(rows,0,7,INT8OID));
    char* tableUuid=LakeMetadataText(rows,0,8);
    if(strlen(tableUuid)!=36) LakeBadRowHead();
    rc=strcpy_s(target->table_uuid,sizeof(target->table_uuid),tableUuid);securec_check(rc,"","");
    LakeRequireNativeHeadTarget(target);return true;
}

/* A private head relation may itself be busy (for example under a host DDL
 * lock). Do not wait one full statement budget on the same leaf every round.
 * Only record the attempted leaf selection: no cursor advance, deleted count,
 * pgstat estimate or last_vacuum_at is fabricated. */
static void LakeRecordNativeHeadAttempt(const LakeRowHeadMaintenanceTarget* target)
{
    LakeRequireNativeHeadTarget(target);
    Oid types[5]={OIDOID,OIDOID,INT4OID,UUIDOID,OIDOID};
    Datum args[5]={ObjectIdGetDatum(target->relation_oid),ObjectIdGetDatum(target->leaf_partition_oid),
        Int32GetDatum(target->partition_no),DirectFunctionCall1(uuid_in,CStringGetDatum(target->binding_id)),
        ObjectIdGetDatum(target->head_oid)};
    const bool saved=u_sess->attr.attr_common.allow_concurrent_tuple_update;
    PG_TRY();
    {
        u_sess->attr.attr_common.allow_concurrent_tuple_update=true;
        LakeMetadataRequireCount(LakeRowHeadQuery("UPDATE og_lake_internal.row_lock_binding "
            "SET last_reclaim_at=pg_catalog.clock_timestamp() WHERE relation_oid=$1 AND leaf_partition_oid=$2 "
            "AND partition_no=$3 AND binding_id=$4 AND row_head_oid=$5",5,types,args,false,SPI_OK_UPDATE),
            1,"busy native leaf row-head attempt");
        u_sess->attr.attr_common.allow_concurrent_tuple_update=saved;
    }
    PG_CATCH();{u_sess->attr.attr_common.allow_concurrent_tuple_update=saved;PG_RE_THROW();}PG_END_TRY();
}

static void LakeObserveRowHeads(Oid relationOid,Oid headOid,bool vacuumed,
    const LakeRowHeadMaintenanceTarget* native=NULL)
{
    Relation head=heap_open(headOid,AccessShareLock);
    TransactionId oldest=GetOldestXmin(head);
    heap_close(head,NoLock);
    /* PG collector values are expressly estimates. The byte high-water mark
     * is observability only; reusable pages must not cause foreground rejection.
     * Pressure means more estimated work than one worker's deletion capacity. */
    Oid types[7]={OIDOID,OIDOID,INT8OID,INT8OID,OIDOID,INT4OID,UUIDOID};
    Datum args[7]={ObjectIdGetDatum(relationOid),ObjectIdGetDatum(headOid),
        Int64GetDatum((int64)512*g_instance.attr.attr_storage.lake_flush_max_batches),Int64GetDatum((int64)oldest)};
    if(native!=NULL) {
        LakeRequireNativeHeadTarget(native);
        if(native->relation_oid!=relationOid || native->head_oid!=headOid) LakeBadRowHead();
        args[4]=ObjectIdGetDatum(native->leaf_partition_oid);args[5]=Int32GetDatum(native->partition_no);
        args[6]=DirectFunctionCall1(uuid_in,CStringGetDatum(native->binding_id));
    }
    LakeMetadataRequireCount(LakeRowHeadQuery(psprintf("UPDATE og_lake_internal.row_lock_binding SET "
        "live_rows_estimate=pg_catalog.pg_stat_get_live_tuples($2),dead_rows_estimate=pg_catalog.pg_stat_get_dead_tuples($2),"
        "head_bytes=pg_catalog.pg_total_relation_size($2::pg_catalog.regclass),oldest_xmin_observed=$4,"
        "soft_budget_exceeded=(pg_catalog.pg_stat_get_live_tuples($2)+pg_catalog.pg_stat_get_dead_tuples($2)>$3)%s "
        "WHERE relation_oid=$1 AND row_head_oid=$2%s",vacuumed?",last_vacuum_at=pg_catalog.statement_timestamp()":"",
        native?" AND leaf_partition_oid=$5 AND partition_no=$6 AND binding_id=$7":""),
        native?7:4,types,args,false,SPI_OK_UPDATE),1,"row-head maintenance observation");
}

/* heap_lock_tuple changes SKIP to ERROR when the public concurrent-update
 * policy is off. Only these closed private synchronization primitives need
 * unconditional SKIP semantics. Restore the caller's policy on every exit;
 * no business DML, ensure INSERT, version lookup or VACUUM runs in this scope. */
static LakeMetaRows* LakeTryLockHeadReclaimMap(Oid relationOid,const LakeRowHeadMaintenanceTarget* native=NULL)
{
    const bool saved=u_sess->attr.attr_common.allow_concurrent_tuple_update;
    LakeMetaRows* result=NULL;
    Oid type=OIDOID;Datum argument=ObjectIdGetDatum(relationOid);
    PG_TRY();
    {
        u_sess->attr.attr_common.allow_concurrent_tuple_update=true;
        result=native?LakeNativeHeadMapping(native,true):LakeRowHeadQuery("SELECT row_head_oid,reclaim_cursor,reclaim_round_end,soft_budget_exceeded FROM og_lake_internal.row_lock_binding "
            "WHERE relation_oid=$1 FOR UPDATE SKIP LOCKED",1,&type,&argument,false,SPI_OK_SELECT);
        u_sess->attr.attr_common.allow_concurrent_tuple_update=saved;
    }
    PG_CATCH();
    {
        u_sess->attr.attr_common.allow_concurrent_tuple_update=saved;
        PG_RE_THROW();
    }
    PG_END_TRY();return result;
}

static TM_Result LakeTryLockIdleRowHead(Relation head,HeapTuple tuple,Buffer* buffer,TM_FailureData* failure)
{
    const bool saved=u_sess->attr.attr_common.allow_concurrent_tuple_update;
    TM_Result result=TM_Invisible;
    PG_TRY();
    {
        u_sess->attr.attr_common.allow_concurrent_tuple_update=true;
        result=heap_lock_tuple(head,tuple,buffer,GetCurrentCommandId(true),
            LockTupleExclusive,LockWaitSkip,false,failure);
        u_sess->attr.attr_common.allow_concurrent_tuple_update=saved;
    }
    PG_CATCH();
    {
        u_sess->attr.attr_common.allow_concurrent_tuple_update=saved;
        PG_RE_THROW();
    }
    PG_END_TRY();return result;
}

static LakeRowHeadReclaimResult LakeReclaimLogicalRowHeadsInternal(Oid relationOid,int budgetMs,int maximumRows,
    const LakeRowHeadMaintenanceTarget* native=NULL)
{
    LakeRowHeadReclaimResult result={InvalidOid,0,0,false,false,{}};
    if(!LakeRowHeadReclaimEnabled() || budgetMs<=0) return result;
    if(maximumRows<1 || maximumRows>512) elog(ERROR,"invalid internal row-head batch bound");
    const TimestampTz started=GetCurrentTimestamp();
    /* Never wait for a concurrent DROP. This native host lock protects the
     * canonical mapping throughout this short caller-owned transaction. */
    if(!ConditionalLockRelationOid(relationOid,AccessShareLock)) return result;
    if(native!=NULL) {
        LakeRequireNativeHeadTarget(native);
        if(native->relation_oid!=relationOid) LakeBadRowHead();
    } else if(get_rel_relkind(relationOid)!=RELKIND_FOREIGN_TABLE)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native parent row-head reclamation requires its typed maintenance adapter")));
    Oid type=OIDOID;Datum argument=ObjectIdGetDatum(relationOid);
    LakeMetaRows* identity=native?LakeNativeHeadMapping(native,false):LakeRowHeadQuery("SELECT row_head_oid FROM og_lake_internal.row_lock_binding WHERE relation_oid=$1",
        1,&type,&argument,true,SPI_OK_SELECT);
    LakeMetadataRequireCount(identity,1,"row-head maintenance owner");
    LakeMetaRows* mapping=LakeTryLockHeadReclaimMap(relationOid,native);
    if(mapping->count==0) return result;
    LakeMetadataRequireCount(mapping,1,"locked row-head maintenance owner");
    Oid headOid=DatumGetObjectId(LakeMetadataValue(mapping,0,1,OIDOID));
    if(headOid!=DatumGetObjectId(LakeMetadataValue(identity,0,1,OIDOID))) LakeBadRowHead();
    int64 cursor=DatumGetInt64(LakeMetadataValue(mapping,0,2,INT8OID));
    int64 roundEnd=DatumGetInt64(LakeMetadataValue(mapping,0,3,INT8OID));
    bool wasPressure=DatumGetBool(LakeMetadataValue(mapping,0,4,BOOLOID));
    if(cursor < -1 || roundEnd < -1 || cursor>=LAKE_REFERENCE_LOCAL_ID_START || roundEnd>=LAKE_REFERENCE_LOCAL_ID_START ||
       (roundEnd>=0 && cursor>roundEnd) || (roundEnd<0 && cursor!=-1)) LakeBadRowHead();
    if(native!=NULL && !ConditionalLockRelationOid(headOid,RowExclusiveLock)) {
        LakeRecordNativeHeadAttempt(native);return result;
    }
    Relation volatile head=NULL,index=NULL;IndexScanDesc volatile scan=NULL;
    Snapshot volatile snapshot=InvalidSnapshot;Buffer volatile buffer=InvalidBuffer;
    const bool savedConcurrentUpdate=u_sess->attr.attr_common.allow_concurrent_tuple_update;
    PG_TRY();
    {
        /* The map row is already ours; each head is deleted only after its
         * exclusive SKIP lock succeeds. Keep the private policy through DELETE
         * and cursor UPDATE: heapam checks the policy before recognizing our
         * own tuple locker. No business DML runs in this protected section. */
        u_sess->attr.attr_common.allow_concurrent_tuple_update=true;
        head=heap_open(headOid,RowExclusiveLock);
        if(!LakeIsRegisteredRowLockHead((Relation)head,LakeMetadataOwner())) LakeBadRowHead();
        result.head_oid=headOid;
        if(native!=NULL) result.native_target=*native;
        List* indexes=RelationGetIndexList((Relation)head);
        index=index_open(linitial_oid(indexes),AccessShareLock);list_free(indexes);
        snapshot=RegisterSnapshot(GetLatestSnapshot());
        ScanKeyData key;
        ScanKeyInit(&key,1,BTGreaterEqualStrategyNumber,F_INT8GE,Int64GetDatum(0));
        scan=index_beginscan((Relation)head,(Relation)index,(Snapshot)snapshot,1,0);
        if(roundEnd<0) {
            index_rescan((IndexScanDesc)scan,&key,1,NULL,0);
            HeapTuple maximum=(HeapTuple)index_getnext((IndexScanDesc)scan,BackwardScanDirection);
            if(HeapTupleIsValid(maximum)) {
                roundEnd=DatumGetInt64(LakeRowHeadAttribute((Relation)head,maximum,1));
                if(roundEnd<0 || roundEnd>=LAKE_REFERENCE_LOCAL_ID_START) LakeBadRowHead();
            }
        }
        int64 keys[512];int count=0;bool reachedEnd=roundEnd<0;
        ScanKeyInit(&key,1,BTGreaterStrategyNumber,F_INT8GT,Int64GetDatum(cursor));
        index_rescan((IndexScanDesc)scan,&key,1,NULL,0);
        while(count<maximumRows && !reachedEnd) {
            CHECK_FOR_INTERRUPTS();
            HeapTuple tuple=(HeapTuple)index_getnext((IndexScanDesc)scan,ForwardScanDirection);
            if(!HeapTupleIsValid(tuple)) {reachedEnd=true;break;}
            int64 id=DatumGetInt64(LakeRowHeadAttribute((Relation)head,tuple,1));
            if(id>roundEnd) {reachedEnd=true;break;}
            if(id<=cursor || id<0 || (count>0 && id<=keys[count-1])) LakeBadRowHead();
            keys[count++]=id;
        }
        int64 advanced=cursor;
        for(int i=0;i<count;++i) {
            CHECK_FOR_INTERRUPTS();
            if(TimestampDifferenceExceeds(started,GetCurrentTimestamp(),budgetMs)) break;
            /* Exact PK point probes bound inspected keys without a planner
             * choosing a full scan for an ANY array. No ctid survives a commit. */
            ScanKeyInit(&key,1,BTEqualStrategyNumber,F_INT8EQ,Int64GetDatum(keys[i]));
            index_rescan((IndexScanDesc)scan,&key,1,NULL,0);
            HeapTuple tuple=(HeapTuple)index_getnext((IndexScanDesc)scan,ForwardScanDirection);
            if(HeapTupleIsValid(tuple)) {
                if(DatumGetInt64(LakeRowHeadAttribute((Relation)head,tuple,1))!=keys[i]) LakeBadRowHead();
                HeapTuple copy=heap_copytuple(tuple);TM_FailureData failure;
                /* On ERROR heap_lock_tuple may already have unpinned its
                 * output buffer. Only a normal return transfers that pin to
                 * us; transaction abort owns all primitive-error resources. */
                TM_Result locked=LakeTryLockIdleRowHead((Relation)head,copy,(Buffer*)&buffer,&failure);
                if(BufferIsValid((Buffer)buffer)) {
                    Buffer pinned=(Buffer)buffer;buffer=InvalidBuffer;ReleaseBuffer(pinned);
                }
                if(locked==TM_Ok) {
                    simple_heap_delete((Relation)head,&copy->t_self);++result.deleted;
                } else if(locked!=TM_WouldBlock && locked!=TM_Deleted && locked!=TM_Updated && locked!=TM_SelfModified)
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("unexpected row-head skip-lock result %d",(int)locked)));
                heap_freetuple(copy);
            }
            advanced=keys[i];++result.inspected;
        }
        bool wrapped=reachedEnd && result.inspected==(uint32)count;
        if(wrapped) {advanced=-1;roundEnd=-1;}
        index_endscan((IndexScanDesc)scan);scan=NULL;UnregisterSnapshot((Snapshot)snapshot);snapshot=InvalidSnapshot;
        index_close((Relation)index,NoLock);index=NULL;heap_close((Relation)head,NoLock);head=NULL;
        Oid types[10]={OIDOID,OIDOID,INT8OID,INT8OID,INT8OID,INT8OID,INT8OID,OIDOID,INT4OID,UUIDOID};
        Datum args[10]={argument,ObjectIdGetDatum(headOid),Int64GetDatum(advanced),Int64GetDatum(roundEnd),
            Int64GetDatum(result.inspected),Int64GetDatum(result.deleted),Int64GetDatum(wrapped?1:0)};
        if(native!=NULL) {
            args[7]=ObjectIdGetDatum(native->leaf_partition_oid);args[8]=Int32GetDatum(native->partition_no);
            args[9]=DirectFunctionCall1(uuid_in,CStringGetDatum(native->binding_id));
        }
        LakeMetadataRequireCount(LakeRowHeadQuery(psprintf("UPDATE og_lake_internal.row_lock_binding SET reclaim_cursor=$3,reclaim_round_end=$4,"
            "reclaim_scanned=reclaim_scanned+$5,reclaimed_rows=reclaimed_rows+$6,reclaim_rounds=reclaim_rounds+$7,"
            "last_reclaim_at=%s,last_delete_xid=CASE WHEN $6>0 THEN pg_catalog.txid_current() ELSE last_delete_xid END "
            "WHERE relation_oid=$1 AND row_head_oid=$2%s",native?"pg_catalog.clock_timestamp()":"pg_catalog.statement_timestamp()",
            native?" AND leaf_partition_oid=$8 AND partition_no=$9 AND binding_id=$10":""),
            native?10:7,types,args,false,SPI_OK_UPDATE),1,"row-head maintenance cursor");
        LakeObserveRowHeads(relationOid,headOid,false,native);
        LakeMetaRows* pressure=native?LakeNativeHeadMapping(native,false):LakeRowHeadQuery("SELECT soft_budget_exceeded FROM og_lake_internal.row_lock_binding WHERE relation_oid=$1",
            1,&type,&argument,true,SPI_OK_SELECT);
        LakeMetadataRequireCount(pressure,1,"row-head pressure observation");
        result.soft_budget_exceeded=DatumGetBool(LakeMetadataValue(pressure,0,native?4:1,BOOLOID));
        result.pressure_raised=result.soft_budget_exceeded && !wasPressure;
        u_sess->attr.attr_common.allow_concurrent_tuple_update=savedConcurrentUpdate;
    }
    PG_CATCH();
    {
        u_sess->attr.attr_common.allow_concurrent_tuple_update=savedConcurrentUpdate;
        /* The output Buffer ID does not prove ownership after primitive
         * ERROR: some heap_lock_tuple paths already release their pin. Native
         * transaction abort reclaims any remaining primitive pin; ending the
         * index scan below releases only that scan's independent pin. */
        if(scan!=NULL) index_endscan((IndexScanDesc)scan);
        if(snapshot!=InvalidSnapshot) UnregisterSnapshot((Snapshot)snapshot);
        if(index!=NULL) index_close((Relation)index,NoLock);
        if(head!=NULL) heap_close((Relation)head,NoLock);
        PG_RE_THROW();
    }
    PG_END_TRY();return result;
}

LakeRowHeadReclaimResult LakeReclaimLogicalRowHeads(Oid relationOid,int budgetMs)
{
    LakeRequireHeadWorker();
    return LakeReclaimLogicalRowHeadsInternal(relationOid,budgetMs,512);
}

static LakeRowHeadReclaimResult LakeReclaimNativePartitionRowHeadsInternal(Oid parentOid,int budgetMs,int maximumRows)
{
    LakeRowHeadReclaimResult result={InvalidOid,0,0,false,false,{}};
    if(!LakeNativeRowHeadMaintenanceEnabled() || budgetMs<=0) return result;
    if(maximumRows<1 || maximumRows>512) elog(ERROR,"invalid native parent row-head batch bound");
    if(!ConditionalLockRelationOid(parentOid,AccessShareLock)) return result;
    Relation parent=heap_open(parentOid,NoLock);
    LakeRowHeadMaintenanceTarget target={};
    if(LakeSelectNativeHead(parent,&target))
        result=LakeReclaimLogicalRowHeadsInternal(parentOid,budgetMs,maximumRows,&target);
    heap_close(parent,NoLock);return result;
}

LakeRowHeadReclaimResult LakeReclaimNativePartitionRowHeads(Oid parentOid,int budgetMs)
{
    LakeRequireHeadWorker();
    return LakeReclaimNativePartitionRowHeadsInternal(parentOid,budgetMs,512);
}

/* Synchronous logical metadata reclamation in the CALLER's transaction.
 * Never commit, restart, truncate or run VACUUM from a SQL C function. */
Datum og_lake_reclaim_row_heads(PG_FUNCTION_ARGS)
{
    LakeRequireAccessPolicy();
    if(!LakeRowHeadReclaimEnabled())
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("manual row-head reclamation requires extension 0.18")));
    if(u_sess->attr.attr_common.XactReadOnly)
        ereport(ERROR,(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),errmsg("cannot reclaim lake row heads in a read-only transaction")));
    Oid relationOid=PG_GETARG_OID(0);int maximumRows=PG_GETARG_INT32(1);
    if(maximumRows<1 || maximumRows>512)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("row-head batch limit must be between 1 and 512")));
    Relation host=heap_open(relationOid,AccessShareLock);
    /* Keep the legacy wrong-object rejection until a future contract opens
     * this precise path; the existing SQL0.25 error behavior is unchanged. */
    const bool native=LakeNativeRowHeadMaintenanceEnabled() && LakeHasNativePartitionMarker(host);
    if(!native && !LakeIsNativeForeignRelation(host))
        ereport(ERROR,(errcode(ERRCODE_WRONG_OBJECT_TYPE),errmsg("row-head reclamation requires a native lake business table")));
    if(!superuser() && !pg_class_ownercheck(relationOid,GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER,ACL_KIND_CLASS,RelationGetRelationName(host));
    if(native) {
        LakeRequireNativeHeadParent(host);
        LakeTxnRejectBindingDdl();
        LakeRowHeadReclaimResult result=LakeReclaimNativePartitionRowHeadsInternal(relationOid,1000,maximumRows);
        heap_close(host,NoLock);PG_RETURN_INT64((int64)result.deleted);
    }
    /* Head maintenance needs canonical synchronization metadata, not a scan
     * binding with every lake file. Keep admission independent of lake size. */
    Oid type=OIDOID;Datum argument=ObjectIdGetDatum(relationOid);
    LakeMetaRows* binding=LakeRowHeadQuery("SELECT t.read_only,c.read_only FROM og_lake_internal.row_lock_binding h "
        "JOIN og_lake_internal.relation_binding r ON r.relation_oid=h.relation_oid AND r.binding_id=h.binding_id "
        "JOIN og_lake_internal.table_binding t ON t.binding_id=h.binding_id "
        "JOIN og_lake_internal.catalog_binding c ON c.server_oid=t.server_oid WHERE h.relation_oid=$1",
        1,&type,&argument,true,SPI_OK_SELECT);
    LakeMetadataRequireCount(binding,1,"manual row-head canonical binding");
    if(DatumGetBool(LakeMetadataValue(binding,0,1,BOOLOID)) || DatumGetBool(LakeMetadataValue(binding,0,2,BOOLOID)))
        ereport(ERROR,(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),errmsg("cannot reclaim row heads of a read-only lake binding")));
    /* Existing mutation markers are subtransaction-owned: they reject both
     * write->maintenance and maintenance->DML/CREATE/flush ordering, and a
     * savepoint rollback removes only its own marker and physical changes. */
    LakeTxnRejectBindingDdl();
    LakeRowHeadReclaimResult result=LakeReclaimLogicalRowHeadsInternal(relationOid,1000,maximumRows);
    heap_close(host,NoLock);
    PG_RETURN_INT64((int64)result.deleted);
}

/* Top abort can already remove DEFAULT_LOCKMETHOD session relation locks.
 * CheckLock alone ignores owner kind; ReleaseLockIfHeld then checks the exact
 * session owner (NULL). Avoid an unconditional Unlock on an already-aborted
 * lock, while still releasing locks after an ERROR between transactions. */
static void LakeReleaseHeadSessionLockIfHeld(const LockRelId* id,LOCKMODE mode)
{
    LOCKTAG tag;SET_LOCKTAG_RELATION(tag,id->dbId,id->relId);
    if(CheckLock(&tag,mode,true)) ReleaseLockIfHeld(&tag,mode,true);
}

static bool LakeVacuumLogicalRowHeadsInternal(Oid relationOid,Oid expectedHeadOid,
    const LakeRowHeadMaintenanceTarget* native=NULL)
{
    LakeRequireHeadWorker();
    volatile LockRelId hostLock,headLock;volatile bool hostHeld=false,headHeld=false;
    MemoryContext originalPortal=t_thrd.mem_cxt.portal_mem_cxt;
    MemoryContext volatile portal=NULL;
    StartTransactionCommand();PushActiveSnapshot(GetTransactionSnapshot());
    if(!ConditionalLockRelationOid(relationOid,ShareUpdateExclusiveLock) ||
       !ConditionalLockRelationOid(expectedHeadOid,ShareUpdateExclusiveLock)) {
        PopActiveSnapshot();CommitTransactionCommand();return false;
    }
    /* The future capability reads installed extension metadata. This worker
     * entry begins outside a transaction, so never test it in the wrapper. */
    if(native!=NULL && !LakeNativeRowHeadMaintenanceEnabled()) {
        PopActiveSnapshot();CommitTransactionCommand();return false;
    }
    /* This helper starts outside a transaction. Read pg_class only after
     * its transaction/snapshot and the existing conditional host/head locks
     * are established. Never add a syscache lookup to the lock-failure path.
     * The legacy relation-only API still skips native parents. A typed caller
     * supplies a copied complete target, validated only inside the transaction. */
    if(native==NULL && get_rel_relkind(relationOid)!=RELKIND_FOREIGN_TABLE) {
        PopActiveSnapshot();CommitTransactionCommand();return false;
    }
    PG_TRY();
    {
        Oid type=OIDOID;Datum argument=ObjectIdGetDatum(relationOid);
        if(native!=NULL) {
            LakeRequireNativeHeadTarget(native);
            if(native->relation_oid!=relationOid || native->head_oid!=expectedHeadOid) LakeBadRowHead();
        }
        LakeMetaRows* mapping=native?LakeNativeHeadMapping(native,false):LakeRowHeadQuery("SELECT row_head_oid FROM og_lake_internal.row_lock_binding WHERE relation_oid=$1",
            1,&type,&argument,true,SPI_OK_SELECT);
        LakeMetadataRequireCount(mapping,1,"vacuum row-head owner");
        if(DatumGetObjectId(LakeMetadataValue(mapping,0,1,OIDOID))!=expectedHeadOid) LakeBadRowHead();
        Relation host=heap_open(relationOid,NoLock);Relation head=heap_open(expectedHeadOid,NoLock);
        if(native!=NULL) LakeRequireNativeHeadParent(host);
        if(!LakeIsRegisteredRowLockHead(head,LakeMetadataOwner())) LakeBadRowHead();
        hostLock.relId=host->rd_lockInfo.lockRelId.relId;
        hostLock.dbId=host->rd_lockInfo.lockRelId.dbId;
        hostLock.bktId=host->rd_lockInfo.lockRelId.bktId;
        headLock.relId=head->rd_lockInfo.lockRelId.relId;
        headLock.dbId=head->rd_lockInfo.lockRelId.dbId;
        headLock.bktId=head->rd_lockInfo.lockRelId.bktId;
        /* Vacuum commits internally. Retain exact host/head OID identity using
         * session locks and release them explicitly on every error path. SUE
         * is compatible with normal readers/writers, but excludes another vacuum.
         * The host pin must also use a non-fast-path mode: this host requires
         * every fast-path bit to clear at transaction end, even when a weak
         * session relation lock would otherwise remain held across COMMIT. */
        LockRelationIdForSession((LockRelId*)&hostLock,ShareUpdateExclusiveLock);hostHeld=true;
        LockRelationIdForSession((LockRelId*)&headLock,ShareUpdateExclusiveLock);headHeld=true;
        heap_close(head,NoLock);heap_close(host,NoLock);PopActiveSnapshot();CommitTransactionCommand();
        /* Publish committed deletion counters before VACUUM subtracts removed
         * tuples. This worker has no frontend idle loop; leaving the counters
         * until process exit would add already-vacuumed rows back to pgstat.
         * The native reporting API must be called outside a transaction. */
        pgstat_report_stat(true);
        portal=AllocSetContextCreate(t_thrd.mem_cxt.msg_mem_cxt,"Lake row-head vacuum portal",ALLOCSET_DEFAULT_SIZES);
        t_thrd.mem_cxt.portal_mem_cxt=(MemoryContext)portal;
        StartTransactionCommand();
        /* Stack storage survives vacuum's internal commits, as in autovacuum. */
        VacuumStmt statement;errno_t rc=memset_s(&statement,sizeof(statement),0,sizeof(statement));securec_check(rc,"","");
        statement.type=T_VacuumStmt;statement.options=VACOPT_VACUUM|VACOPT_ANALYZE|VACOPT_NOWAIT;
        statement.flags=VACFLG_SIMPLE_HEAP;statement.rely_oid=InvalidOid;
        statement.freeze_min_age=-1;statement.freeze_table_age=-1;
        vacuum(&statement,expectedHeadOid,false,NULL,true);
        /* The native routine returns in an implicit transaction. Record an
         * attempted/completed vacuum plus approximate live/dead/cutoff state;
         * a retained old snapshot can legitimately leave every dead row. */
        PushActiveSnapshot(GetTransactionSnapshot());pgstat_clear_snapshot();
        if(native!=NULL) {
            LakeMetadataRequireCount(LakeNativeHeadMapping(native,false),1,"native row-head owner after vacuum commits");
            Relation checked=heap_open(expectedHeadOid,NoLock);
            if(!LakeIsRegisteredRowLockHead(checked,LakeMetadataOwner())) LakeBadRowHead();
            heap_close(checked,NoLock);
        }
        LakeObserveRowHeads(relationOid,expectedHeadOid,true,native);
        PopActiveSnapshot();CommitTransactionCommand();
        t_thrd.mem_cxt.portal_mem_cxt=originalPortal;
        MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);
        /* vacuum can return early with a context still under our portal. */
        t_thrd.vacuum_cxt.vac_context=NULL;
        MemoryContextDelete((MemoryContext)portal);portal=NULL;
        UnlockRelationIdForSession((LockRelId*)&headLock,ShareUpdateExclusiveLock);headHeld=false;
        UnlockRelationIdForSession((LockRelId*)&hostLock,ShareUpdateExclusiveLock);hostHeld=false;
    }
    PG_CATCH();
    {
        t_thrd.mem_cxt.portal_mem_cxt=originalPortal;
        AbortOutOfAnyTransaction();MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);
        if(headHeld) {LakeReleaseHeadSessionLockIfHeld((const LockRelId*)&headLock,ShareUpdateExclusiveLock);headHeld=false;}
        if(hostHeld) {LakeReleaseHeadSessionLockIfHeld((const LockRelId*)&hostLock,ShareUpdateExclusiveLock);hostHeld=false;}
        /* Match autovacuum: deleting portal children invalidates vac_context.
         * Do not leave its global pointer dangling after a cancelled vacuum. */
        t_thrd.vacuum_cxt.vac_context=NULL;
        if(portal!=NULL) MemoryContextDelete((MemoryContext)portal);
        PG_RE_THROW();
    }
    PG_END_TRY();return true;
}

bool LakeVacuumLogicalRowHeads(Oid relationOid,Oid expectedHeadOid)
{
    return LakeVacuumLogicalRowHeadsInternal(relationOid,expectedHeadOid);
}

bool LakeVacuumNativePartitionRowHeads(const LakeRowHeadMaintenanceTarget* target)
{
    LakeRequireHeadWorker();
    LakeRequireNativeHeadTarget(target);
    return LakeVacuumLogicalRowHeadsInternal(target->relation_oid,target->head_oid,target);
}

Datum og_lake_row_lock_catalog_upgrade(PG_FUNCTION_ARGS)
{
    LakeInlineRequireAdministrator();LakeTxnRejectBindingDdl();
    char* version=LakeRowLockVersion();
    bool coordinated=LakeNativePartitionVersionEnabled();
    if(!creating_extension || version==NULL || (!coordinated && (strcmp(version,"0.16")!=0 && strcmp(version,"0.17")!=0 && (strcmp(version,"0.18")!=0&&(strcmp(version,"0.19")!=0&&(strcmp(version,"0.20")!=0&&(strcmp(version,"0.21")!=0&&strcmp(version,"0.22")!=0)))))))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("row-head catalog initialization is restricted to extension 0.17/0.18 scripts")));
    pfree(version);
    /* openGauss sets pg_extension.extversion to the target BEFORE running the
     * update script. The new map initially retains the extension owner while
     * its namespace may already have a configured private owner. Establish
     * that exact bootstrap state natively before normal owner validation. */
    LakeAdoptRowLockCatalogForUpgrade();
    LakeLockCatalogRegistration();CommandCounterIncrement();
    LakeMetaRows* relations=LakeRowHeadQuery("SELECT relation_oid FROM og_lake_internal.relation_binding ORDER BY relation_oid",
        0,NULL,NULL,true,SPI_OK_SELECT);
    for(uint32 i=0;i<relations->count;++i)
        LakePrepareRowLockBindingInternal(NULL,DatumGetObjectId(LakeMetadataValue(relations,i,1,OIDOID)));
    PG_RETURN_VOID();
}
