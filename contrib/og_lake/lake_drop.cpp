/* Native logical DROP adapter. Standard eight-relation end_snapshot semantics
 * follow pinned DuckLake 0aaa8e10, ducklake_metadata_manager.cpp:3148-3176
 * (MIT; full fixed-reference notice follows). Host journal/ACL/snapshot publication are native.
 * No physical carrier, Parquet or historical statistics are deleted here. */

/*
Copyright 2018-2025 Stichting DuckDB Foundation

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
#include "postgres.h"
#include "lake_drop.h"
#include "lake_create_publish.h"
#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "knl/knl_variable.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

static LakeMetaRows* LakeDropCatalog(LakeInlineBinding* binding, Snapshot snapshot)
{
    Oid type=OIDOID;Datum arg=ObjectIdGetDatum(binding->server_oid);
    LakeMetaRows* row=LakeMetadataQuery(
        "SELECT catalog_id::text,binding_generation FROM og_lake_internal.catalog_binding "
        "WHERE server_oid=$1",1,&type,&arg,NULL,snapshot,true,SPI_OK_SELECT);
    LakeMetadataRequireCount(row,1,"logical DROP catalog identity");
    return row;
}

static LakeMetaRows* LakeDropSource(LakeInlineBinding* binding,Snapshot snapshot,int64 head)
{
    Oid types[2]={INT8OID,INT8OID};
    Datum args[2]={Int64GetDatum(LakePermanentTableId(binding)),Int64GetDatum(head)};
    LakeMetaRows* row=LakeMetadataQuery(psprintf(
        "SELECT table_uuid::text,begin_snapshot,schema_id FROM %s WHERE table_id=$1 "
        "AND begin_snapshot<=$2 AND (end_snapshot IS NULL OR end_snapshot>$2)",
        LakeDefinitionRelation(binding->metadata_schema_oid,"ducklake_table")),
        2,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    if(row->count==0)
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake DROP target is no longer visible")));
    LakeMetadataRequireCount(row,1,"logical DROP visible table");
    if(strcmp(LakeMetadataText(row,0,1),binding->table_uuid)!=0)
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake DROP target identity changed")));
    return row;
}

void LakeValidateAttachedSourceAfterBinding(LakeInlineBinding* before)
{
    /* INSERT of table_binding's unconditional UNIQUE(server_oid,table_id)
     * has completed. A concurrent native CREATE owner's DROP may have been
     * the deleting transaction we waited for. Re-read AFTER that wait. The
     * unique key remains ours to transaction end; a native CREATE owner of
     * the same permanent ID cannot coexist or be fabricated via ALTER.
     * This is not a general external-writer DDL synchronization protocol. */
    Snapshot snapshot=RegisterSnapshot(GetLatestSnapshot());
    PG_TRY(); {
        LakeInlineBinding* now=LakeLoadCatalogHeader(before->server_oid,snapshot);
        if(now->metadata_schema_oid!=before->metadata_schema_oid||strcmp(now->data_root,before->data_root))
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake ATTACH catalog changed while binding")));
        Oid types[2]={INT8OID,INT8OID};
        Datum args[2]={Int64GetDatum(LakePermanentTableId(before)),Int64GetDatum(now->snapshot_id)};
        LakeMetaRows* row=LakeMetadataQuery(psprintf(
            "SELECT table_uuid::text FROM %s WHERE table_id=$1 AND begin_snapshot<=$2 "
            "AND (end_snapshot IS NULL OR end_snapshot>$2)",
            LakeDefinitionRelation(now->metadata_schema_oid,"ducklake_table")),
            2,types,args,NULL,snapshot,true,SPI_OK_SELECT,now->metadata_schema_oid);
        if(row->count==0)
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake ATTACH source was dropped while binding")));
        LakeMetadataRequireCount(row,1,"ATTACH source after binding");
        if(strcmp(LakeMetadataText(row,0,1),before->table_uuid))
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake ATTACH source identity changed while binding")));
        UnregisterSnapshot(snapshot);
    } PG_CATCH(); {
        UnregisterSnapshot(snapshot);PG_RE_THROW();
    } PG_END_TRY();
}

void LakeStageCommittedDrop(Oid relationOid,Oid serverOid)
{
    if(!LakeLogicalDropVersionEnabled())
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake logical DROP requires its installed SQL contract")));
    if(u_sess->utils_cxt.XactIsoLevel!=XACT_READ_COMMITTED)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake logical DROP requires READ COMMITTED")));
    if(u_sess->attr.attr_common.XactReadOnly)
        ereport(ERROR,(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),errmsg("cannot drop a lake table in a read-only transaction")));
    if(!OidIsValid(LakeMetadataOwner()))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake logical DROP requires a private metadata owner")));
    LakeTxnState* state=LakeTxnBeginDrop(serverOid);
    MemoryContext caller=CurrentMemoryContext;
    MemoryContext context=AllocSetContextCreate(state->context,"Native lake DROP event",
        ALLOCSET_DEFAULT_MINSIZE,ALLOCSET_DEFAULT_INITSIZE,ALLOCSET_DEFAULT_MAXSIZE);
    Snapshot volatile snapshot=NULL;
    PG_TRY();
    {
        MemoryContextSwitchTo(context);
        snapshot=RegisterSnapshot(GetLatestSnapshot());
        LakeDropEvent* event=(LakeDropEvent*)palloc0(sizeof(LakeDropEvent));
        event->context=context;event->owner_subid=GetCurrentSubTransactionId();
        /* Unlike business loading, the catalog header never resolves a data
         * file path, carrier projection or footer. DROP can retire an object
         * whose payload is damaged without first reading that payload. */
        LakeInlineBinding* binding=LakeLoadCatalogHeader(serverOid,snapshot);
        Oid type=OIDOID;Datum arg=ObjectIdGetDatum(relationOid);
        LakeMetaRows* row=LakeMetadataQuery(
            "SELECT t.binding_id::text,t.server_oid,t.table_id,t.table_uuid::text,t.read_only "
            "FROM og_lake_internal.relation_binding r JOIN og_lake_internal.table_binding t USING(binding_id) "
            "WHERE r.relation_oid=$1 AND t.state='active' AND t.object_kind='relation'",
            1,&type,&arg,NULL,snapshot,true,SPI_OK_SELECT);
        LakeMetadataRequireCount(row,1,"logical DROP host binding");
        int64 tableId=DatumGetInt64(LakeMetadataValue(row,0,3,INT8OID));
        if(DatumGetObjectId(LakeMetadataValue(row,0,2,OIDOID))!=serverOid||tableId<0||tableId>=LAKE_REFERENCE_LOCAL_ID_START)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native lake DROP binding identity")));
        binding->relation_oid=relationOid;binding->identity=LakePermanentIdentity(tableId);
        binding->binding_id=LakeMetadataText(row,0,1);binding->table_uuid=LakeMetadataText(row,0,4);
        binding->read_only|=DatumGetBool(LakeMetadataValue(row,0,5,BOOLOID));
        if(binding->read_only)
            ereport(ERROR,(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),errmsg("cannot drop a read-only lake table")));
        event->binding=binding;
        LakeMetaRows* catalog=LakeDropCatalog(binding,snapshot);
        event->catalog_uuid=LakeMetadataText(catalog,0,1);
        event->catalog_generation=DatumGetInt64(LakeMetadataValue(catalog,0,2,INT8OID));
        LakeMetaRows* source=LakeDropSource(binding,snapshot,binding->snapshot_id);
        event->source_begin=DatumGetInt64(LakeMetadataValue(source,0,2,INT8OID));
        event->schema_id=DatumGetInt64(LakeMetadataValue(source,0,3,INT8OID));
        if(event->catalog_generation<=0||event->source_begin<0||event->source_begin>binding->snapshot_id||
           event->schema_id<0||event->schema_id>=LAKE_REFERENCE_LOCAL_ID_START)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native lake DROP source counters")));
        /* Header/UUID/path strings are bounded by existing catalog admission.
         * Conservative charge follows the already qualified RENAME journal. */
        event->charged_bytes=256*1024;
        UnregisterSnapshot(snapshot);snapshot=NULL;MemoryContextSwitchTo(caller);
        LakeTxnAdoptDrop(event);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(caller);
        if(snapshot!=NULL)UnregisterSnapshot(snapshot);
        MemoryContextDelete(context);PG_RE_THROW();
    }
    PG_END_TRY();
}

static int LakeDropCompare(const void* left,const void* right)
{
    int64 a=LakePermanentTableId((*(LakeDropEvent* const*)left)->binding);
    int64 b=LakePermanentTableId((*(LakeDropEvent* const*)right)->binding);
    return a<b?-1:a>b?1:0;
}

LakeDropEvent** LakeCollectDrops(LakeTxnState* state,int* count)
{
    LakeDropEvent** events=(LakeDropEvent**)palloc(sizeof(LakeDropEvent*)*64);*count=0;
    for(LakeDropEvent* event=state->drops;event;event=event->next) {
        if(*count>=64)ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("native lake DROP journal overflow")));
        for(int i=0;i<*count;++i)
            if(events[i]->binding->server_oid!=event->binding->server_oid||
               events[i]->binding->metadata_schema_oid!=event->binding->metadata_schema_oid||
               LakePermanentTableId(events[i]->binding)==LakePermanentTableId(event->binding))
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("inconsistent native lake DROP identities")));
        events[(*count)++]=event;
    }
    qsort(events,*count,sizeof(LakeDropEvent*),LakeDropCompare);return events;
}

void LakeValidateDrops(LakeDropEvent** events,int count,Snapshot snapshot,int64 head)
{
    for(int i=0;i<count;++i) {
        LakeDropEvent* event=events[i];LakeInlineBinding* before=event->binding;
        /* The host pg_class/pg_foreign_table and host binding are already
         * deleted in our transaction. Never use RENAME's host-reopen check. */
        LakeInlineBinding* now=LakeLoadCatalogHeader(before->server_oid,snapshot);
        LakeMetaRows* catalog=LakeDropCatalog(now,snapshot);
        if(now->read_only||now->metadata_schema_oid!=before->metadata_schema_oid||
           strcmp(now->data_root,before->data_root)||strcmp(LakeMetadataText(catalog,0,1),event->catalog_uuid)||
           DatumGetInt64(LakeMetadataValue(catalog,0,2,INT8OID))!=event->catalog_generation)
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake DROP catalog changed before commit")));
        LakeMetaRows* source=LakeDropSource(before,snapshot,head);
        if(DatumGetInt64(LakeMetadataValue(source,0,2,INT8OID))!=event->source_begin||
           DatumGetInt64(LakeMetadataValue(source,0,3,INT8OID))!=event->schema_id)
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake DROP definition changed before commit")));
    }
}

/* End only trusted standard metadata rows. Extra untouched fields never enter
 * predicates or expressions; all three accessed columns are checked natively.
 * LakeMetadataRelation also rejects untrusted OIDs, policies and user actions. */
static char* LakeDropEndRelation(Oid ns,const char* name,const char* id)
{
    char* qualified=LakeMetadataRelation(ns,name);
    Relation relation=heap_open(get_relname_relid(name,ns),AccessShareLock);
    const char* columns[]={id,"begin_snapshot","end_snapshot"};
    for(int i=0;i<3;++i) {
        AttrNumber att=get_attnum(RelationGetRelid(relation),columns[i]);
        if(att<=0||RelationGetDescr(relation)->attrs[att-1].attisdropped||
           RelationGetDescr(relation)->attrs[att-1].atttypid!=INT8OID)
            ereport(ERROR,(errcode(ERRCODE_DATATYPE_MISMATCH),errmsg("native lake DROP metadata identity or snapshot column has an unsupported type")));
    }
    heap_close(relation,NoLock);return qualified;
}

void LakeWriteDrops(LakeDropEvent** events,int count,Snapshot snapshot,int64 commit)
{
    /* Same standard set as pinned DuckLake DropTables(ids,false). Registrations,
     * payloads, stats and unversioned auxiliary rows remain for later GC. */
    static const char* tables[]={"ducklake_partition_info","ducklake_column","ducklake_column_tag",
        "ducklake_data_file","ducklake_delete_file","ducklake_tag","ducklake_sort_info"};
    for(int i=0;i<count;++i) {
        LakeDropEvent* event=events[i];LakeInlineBinding* binding=event->binding;Oid ns=binding->metadata_schema_oid;
        Oid types[4]={INT8OID,INT8OID,INT8OID,UUIDOID};
        Datum args[4]={Int64GetDatum(LakePermanentTableId(binding)),Int64GetDatum(commit),
            Int64GetDatum(event->source_begin),DirectFunctionCall1(uuid_in,CStringGetDatum(binding->table_uuid))};
        LakeMetaRows* ended=LakeMetadataQuery(psprintf("UPDATE %s SET end_snapshot=$2 WHERE table_id=$1 "
            "AND begin_snapshot=$3 AND table_uuid=$4 AND end_snapshot IS NULL",
            LakeDefinitionRelation(ns,"ducklake_table")),4,types,args,NULL,snapshot,false,SPI_OK_UPDATE,ns);
        if(ended->count==0)
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake DROP target changed during publication")));
        LakeMetadataRequireCount(ended,1,"logical DROP table retirement");
        for(int t=0;t<lengthof(tables);++t) {
            const char* id=strcmp(tables[t],"ducklake_tag")==0?"object_id":"table_id";
            (void)LakeMetadataQuery(psprintf("UPDATE %s SET end_snapshot=$2 WHERE %s=$1 AND end_snapshot IS NULL",
                LakeDropEndRelation(ns,tables[t],id),id),2,types,args,NULL,snapshot,false,SPI_OK_UPDATE,ns);
        }
    }
}
