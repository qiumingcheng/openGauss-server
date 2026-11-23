/* Adapt pinned DuckLake table-name versioning to host DDL and transaction locks. */
#include "postgres.h"
#include "lake_column_layout.h"
#include "knl/knl_variable.h"
#include "lake_rename.h"
#include "lake_create_publish.h"
#include "access/og_lake.h"
#include "access/heapam.h"
#include "catalog/pg_type.h"
#include "catalog/pg_inherits_fn.h"
#include "commands/extension.h"
#include "executor/spi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

static LakeMetaRows* LakeRenameSource(LakeInlineBinding* binding, Snapshot snapshot, int64 head)
{
    Oid types[2]={INT8OID,INT8OID};
    Datum args[2]={Int64GetDatum(LakePermanentTableId(binding)),Int64GetDatum(head)};
    LakeMetaRows* rows=LakeMetadataQuery(psprintf(
        "SELECT table_uuid::text,begin_snapshot,schema_id,table_name::text,path::text,path_is_relative "
        "FROM %s WHERE table_id=$1 AND begin_snapshot<=$2 AND (end_snapshot IS NULL OR end_snapshot>$2)",
        LakeDefinitionRelation(binding->metadata_schema_oid,"ducklake_table")),
        2,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    if(rows->count==0)
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake RENAME source is no longer visible")));
    LakeMetadataRequireCount(rows,1,"RENAME sources");
    if(strcmp(LakeMetadataText(rows,0,1),binding->table_uuid)!=0)
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake RENAME source identity changed")));
    return rows;
}

static LakeMetaRows* LakeRenameSchema(LakeRenameEvent* event, Snapshot snapshot, int64 head)
{
    Oid types[2]={INT8OID,INT8OID};Datum args[2]={Int64GetDatum(event->schema_id),Int64GetDatum(head)};
    LakeMetaRows* rows=LakeMetadataQuery(psprintf(
        "SELECT schema_uuid::text,schema_name::text,parent_schema_id FROM %s "
        "WHERE schema_id=$1 AND begin_snapshot<=$2 AND (end_snapshot IS NULL OR end_snapshot>$2)",
        LakeDefinitionRelation(event->binding->metadata_schema_oid,"ducklake_schema")),
        2,types,args,NULL,snapshot,true,SPI_OK_SELECT,event->binding->metadata_schema_oid);
    if(rows->count==0)
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake RENAME schema no longer exists")));
    LakeMetadataRequireCount(rows,1,"RENAME schemas");
    bool top=false;(void)LakeMetadataValue(rows,0,3,INT8OID,true,&top);
    if(!top)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake RENAME does not yet support nested lake schemas")));
    return rows;
}

static void LakeNativeRename(Relation relation, const char* newName, const char* newSchema,
    const char* oldColumn, const char* newColumn, bool recursing, int expectedParents)
{
    if(!LakeIsNativeForeignRelation(relation)) return;
    const bool columnRename=oldColumn!=NULL;
    LakeRequireAccessPolicy();
    if(columnRename)LakeRequireColumnRenameVersion();else LakeRequireTableRenameVersion();
    if(columnRename) {
        if(recursing || expectedParents!=0 || RelationIsPartitioned(relation) ||
           find_inheritance_children(RelationGetRelid(relation),NoLock)!=NIL)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake column RENAME does not support inheritance or partitions")));
        if(newColumn==NULL || *newColumn=='\0' || strlen(newColumn)>=NAMEDATALEN ||
           pg_strncasecmp(newColumn,"_ducklake_",9)==0)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("unsupported or reserved native lake column name")));
        AttrNumber attribute=get_attnum(RelationGetRelid(relation),oldColumn);
        if(attribute<=0 || attribute>RelationGetDescr(relation)->natts)
            ereport(ERROR,(errcode(ERRCODE_UNDEFINED_COLUMN),errmsg("lake column \"%s\" does not exist",oldColumn)));
        Form_pg_attribute old=&RelationGetDescr(relation)->attrs[attribute-1];
        if(old->attinhcount!=0 || old->attisdropped)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("cannot rename an inherited or dropped lake column")));
        for(int c=0;c<RelationGetDescr(relation)->natts;++c)
            if(c!=attribute-1 && LakeIdentifierEquals(newColumn,NameStr(RelationGetDescr(relation)->attrs[c].attname)))
                ereport(ERROR,(errcode(ERRCODE_DUPLICATE_COLUMN),errmsg("lake column names must be unique under ASCII case folding")));
    }
    if(!OidIsValid(LakeMetadataOwner()) || creating_extension || newSchema!=NULL)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake RENAME requires a private owner and an unchanged host schema outside extension scripts")));
    if(!pg_class_ownercheck(RelationGetRelid(relation),GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER,ACL_KIND_CLASS,RelationGetRelationName(relation));
    if(u_sess->attr.attr_common.XactReadOnly)
        ereport(ERROR,(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),errmsg("cannot rename a lake table in a read-only transaction")));
    if(newName==NULL || *newName=='\0' || strlen(newName)>=NAMEDATALEN)
        ereport(ERROR,(errcode(ERRCODE_INVALID_NAME),errmsg("invalid native lake table name")));
    if(LakeTxnFindCreatedTable(RelationGetRelid(relation))!=NULL)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("renaming transaction-local lake tables is not yet supported")));
    LakeTxnState* state=LakeTxnBeginRename(GetForeignTable(RelationGetRelid(relation))->serverid);
    for(LakeRenameEvent* prior=state->renames;prior!=NULL;prior=prior->next)
        if(columnRename || prior->column_rename)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake column RENAME accepts one column and no other lake rename in a transaction")));
    if(columnRename)LakeTxnForgetReadOnlyTableForRename(RelationGetRelid(relation));
    MemoryContext caller=CurrentMemoryContext;
    MemoryContext context=AllocSetContextCreate(state->context,"Native lake RENAME event",
        ALLOCSET_DEFAULT_MINSIZE,ALLOCSET_DEFAULT_INITSIZE,ALLOCSET_DEFAULT_MAXSIZE);
    Snapshot volatile snapshot=NULL;
    PG_TRY();
    {
        MemoryContextSwitchTo(context);
        snapshot=RegisterSnapshot(GetLatestSnapshot());
        LakeRenameEvent* event=(LakeRenameEvent*)palloc0(sizeof(LakeRenameEvent));
        event->context=context;event->owner_subid=GetCurrentSubTransactionId();
        event->host_namespace=RelationGetNamespace(relation);
        event->binding=LakeLoadCommittedInlineBinding(RelationGetRelid(relation),snapshot);
        if(event->binding->read_only)
            ereport(ERROR,(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),errmsg("cannot rename a read-only lake table")));
        LakeMetaRows* source=LakeRenameSource(event->binding,snapshot,event->binding->snapshot_id);
        event->source_begin=DatumGetInt64(LakeMetadataValue(source,0,2,INT8OID));
        event->schema_id=DatumGetInt64(LakeMetadataValue(source,0,3,INT8OID));
        if(event->source_begin<0 || event->source_begin>event->binding->snapshot_id || event->schema_id<0 ||
           event->schema_id>=LAKE_REFERENCE_LOCAL_ID_START)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native lake RENAME source counters")));
        event->old_name=LakeMetadataText(source,0,4);event->new_name=pstrdup(newName);
        event->column_rename=columnRename;
        if(columnRename) {
            event->column_attnum=get_attnum(RelationGetRelid(relation),oldColumn);
            if(event->column_attnum<=0 || LakeHostToLogical(event->binding,event->column_attnum)<0)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake column RENAME mapping")));
            event->column_id=event->binding->columns[LakeHostToLogical(event->binding,event->column_attnum)].column_id;
            event->old_column_name=pstrdup(oldColumn);event->new_column_name=pstrdup(newColumn);
            Oid ct[3]={INT8OID,INT8OID,INT8OID};Datum ca[3]={Int64GetDatum(LakePermanentTableId(event->binding)),
                Int64GetDatum(event->column_id),Int64GetDatum(event->binding->snapshot_id)};
            LakeMetaRows* column=LakeMetadataQuery(psprintf("SELECT begin_snapshot,column_name::text FROM %s "
                "WHERE table_id=$1 AND column_id=$2 AND parent_column IS NULL AND begin_snapshot<=$3 "
                "AND (end_snapshot IS NULL OR end_snapshot>$3)",
                LakeDefinitionRelation(event->binding->metadata_schema_oid,"ducklake_column")),
                3,ct,ca,NULL,snapshot,true,SPI_OK_SELECT,event->binding->metadata_schema_oid);
            LakeMetadataRequireCount(column,1,"RENAME column sources");
            event->column_source_begin=DatumGetInt64(LakeMetadataValue(column,0,1,INT8OID));
            if(event->column_source_begin<0 || event->column_source_begin>event->binding->snapshot_id ||
               strcmp(LakeMetadataText(column,0,2),oldColumn)!=0)
                ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake column RENAME source changed")));
        }
        bool pathNull=false;Datum path=LakeMetadataValue(source,0,5,TEXTOID,true,&pathNull);
        event->path=pathNull?NULL:TextDatumGetCString(path);
        Datum relative=LakeMetadataValue(source,0,6,BOOLOID,true,&event->path_relative_null);
        event->path_relative=!event->path_relative_null && DatumGetBool(relative);
        LakeMetaRows* schema=LakeRenameSchema(event,snapshot,event->binding->snapshot_id);
        event->schema_uuid=LakeMetadataText(schema,0,1);event->schema_name=LakeMetadataText(schema,0,2);
        event->charged_bytes=256*1024+512*event->binding->column_count;
        UnregisterSnapshot(snapshot);snapshot=NULL;
        MemoryContextSwitchTo(caller);
        LakeTxnAdoptRename(event); /* All checks precede the non-throwing link. */
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(caller);
        if(snapshot!=NULL) UnregisterSnapshot(snapshot);
        MemoryContextDelete(context);
        PG_RE_THROW();
    }
    PG_END_TRY();
}

void LakeNativeRenameForeignTable(Relation relation,const char* newName,const char* newSchema)
{
    LakeNativeRename(relation,newName,newSchema,NULL,NULL,false,0);
}

void LakeNativeRenameForeignColumn(Relation relation,const char* oldName,const char* newName,
    bool recursing,int expectedParents)
{
    LakeNativeRename(relation,RelationGetRelationName(relation),NULL,oldName,newName,recursing,expectedParents);
}

static int LakeRenameCompare(const void* a,const void* b)
{
    int64 left=LakePermanentTableId((*(LakeRenameEvent* const*)a)->binding);
    int64 right=LakePermanentTableId((*(LakeRenameEvent* const*)b)->binding);
    return left<right?-1:left>right?1:0;
}

LakeRenameEvent** LakeCollectRenames(LakeTxnState* state,int* count)
{
    LakeRenameEvent** events=(LakeRenameEvent**)palloc(sizeof(LakeRenameEvent*)*64);*count=0;
    for(LakeRenameEvent* event=state->renames;event!=NULL;event=event->next) {
        bool newer=false;
        for(int i=0;i<*count;++i) newer|=events[i]->binding->relation_oid==event->binding->relation_oid;
        if(newer) continue;
        if(*count>=64) ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("native lake RENAME journal overflow")));
        for(int i=0;i<*count;++i)
            if(events[i]->binding->server_oid!=event->binding->server_oid ||
               events[i]->binding->metadata_schema_oid!=event->binding->metadata_schema_oid ||
               LakePermanentTableId(events[i]->binding)==LakePermanentTableId(event->binding))
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("inconsistent native lake RENAME identities")));
        events[(*count)++]=event;
    }
    qsort(events,*count,sizeof(LakeRenameEvent*),LakeRenameCompare);
    return events;
}

void LakeValidateRenames(LakeRenameEvent** events,int count,Snapshot snapshot,int64 head)
{
    for(int i=0;i<count;++i) {
        LakeRenameEvent* e=events[i];LakeInlineBinding* b=e->binding;
        LakeInlineBinding* now=LakeLoadCommittedInlineBinding(b->relation_oid,snapshot);
        if(!LakeColumnLayoutsEqual(b,now) || now->read_only || now->server_oid!=b->server_oid ||
           LakePermanentTableId(now)!=LakePermanentTableId(b) || strcmp(now->table_uuid,b->table_uuid)!=0 ||
           strcmp(now->binding_id,b->binding_id)!=0 || now->column_count!=b->column_count ||
           now->inline_oid!=b->inline_oid || get_rel_namespace(b->relation_oid)!=e->host_namespace ||
           strcmp(get_rel_name(b->relation_oid),e->new_name)!=0)
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake RENAME binding changed before commit")));
        for(int c=0;c<b->column_count;++c) {
            const char* expected=e->column_rename && b->columns[c].output_attnum==e->column_attnum?e->new_column_name:b->columns[c].name;
            if(now->columns[c].column_id!=b->columns[c].column_id || now->columns[c].type!=b->columns[c].type ||
               now->columns[c].typmod!=b->columns[c].typmod || now->columns[c].element_column_id!=b->columns[c].element_column_id ||
               strcmp(now->columns[c].name,expected)!=0 ||
               (OidIsValid(b->inline_oid) && strcmp(LakeInlinePhysicalName(&now->columns[c]),LakeInlinePhysicalName(&b->columns[c]))!=0))
                ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake RENAME column mapping changed")));
        }
        if(e->column_rename) {
            Oid ct[3]={INT8OID,INT8OID,INT8OID};Datum ca[3]={Int64GetDatum(LakePermanentTableId(b)),
                Int64GetDatum(e->column_id),Int64GetDatum(head)};
            LakeMetaRows* column=LakeMetadataQuery(psprintf("SELECT begin_snapshot,column_name::text FROM %s "
                "WHERE table_id=$1 AND column_id=$2 AND parent_column IS NULL AND begin_snapshot<=$3 "
                "AND (end_snapshot IS NULL OR end_snapshot>$3)",LakeDefinitionRelation(b->metadata_schema_oid,"ducklake_column")),
                3,ct,ca,NULL,snapshot,true,SPI_OK_SELECT,b->metadata_schema_oid);
            LakeMetadataRequireCount(column,1,"current RENAME column sources");
            if(DatumGetInt64(LakeMetadataValue(column,0,1,INT8OID))!=e->column_source_begin ||
               strcmp(LakeMetadataText(column,0,2),e->old_column_name)!=0)
                ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake column RENAME source changed before commit")));
            LakeMetaRows* names=LakeMetadataQuery(psprintf("SELECT column_id,column_name::text FROM %s "
                "WHERE table_id=$1 AND parent_column IS NULL AND begin_snapshot<=$3 "
                "AND (end_snapshot IS NULL OR end_snapshot>$3)",LakeDefinitionRelation(b->metadata_schema_oid,"ducklake_column")),
                3,ct,ca,NULL,snapshot,true,SPI_OK_SELECT,b->metadata_schema_oid);
            for(uint32 n=0;n<names->count;++n)
                if(DatumGetInt64(LakeMetadataValue(names,n,1,INT8OID))!=e->column_id &&
                   LakeIdentifierEquals(LakeMetadataText(names,n,2),e->new_column_name))
                    ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake column RENAME target conflicts with a live column")));
        }
        LakeMetaRows* source=LakeRenameSource(b,snapshot,head);
        bool pathNull=false,relativeNull=false;Datum path=LakeMetadataValue(source,0,5,TEXTOID,true,&pathNull);
        Datum relative=LakeMetadataValue(source,0,6,BOOLOID,true,&relativeNull);
        if(DatumGetInt64(LakeMetadataValue(source,0,2,INT8OID))!=e->source_begin ||
           DatumGetInt64(LakeMetadataValue(source,0,3,INT8OID))!=e->schema_id ||
           strcmp(LakeMetadataText(source,0,4),e->old_name)!=0 || pathNull!=(e->path==NULL) ||
           (!pathNull && strcmp(TextDatumGetCString(path),e->path)!=0) || relativeNull!=e->path_relative_null ||
           (!relativeNull && DatumGetBool(relative)!=e->path_relative))
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake RENAME definition changed before commit")));
        LakeMetaRows* schema=LakeRenameSchema(e,snapshot,head);
        if(strcmp(LakeMetadataText(schema,0,1),e->schema_uuid)!=0 || strcmp(LakeMetadataText(schema,0,2),e->schema_name)!=0)
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake RENAME schema changed before commit")));
        for(int j=0;j<i;++j)
            if(events[j]->schema_id==e->schema_id && LakeIdentifierEquals(events[j]->new_name,e->new_name))
                ereport(ERROR,(errcode(ERRCODE_DUPLICATE_TABLE),errmsg("native lake RENAME batch has duplicate final names")));
        Oid types[2]={INT8OID,INT8OID};Datum args[2]={Int64GetDatum(e->schema_id),Int64GetDatum(head)};
        Oid ns=b->metadata_schema_oid;
        LakeMetaRows* names=LakeMetadataQuery(psprintf(
            "SELECT table_name::text AS name,table_id FROM %s WHERE schema_id=$1 AND begin_snapshot<=$2 "
            "AND (end_snapshot IS NULL OR end_snapshot>$2) UNION ALL "
            "SELECT view_name::text AS name,NULL::bigint FROM %s WHERE schema_id=$1 AND begin_snapshot<=$2 "
            "AND (end_snapshot IS NULL OR end_snapshot>$2)",LakeDefinitionRelation(ns,"ducklake_table"),
            LakeDefinitionRelation(ns,"ducklake_view")),2,types,args,NULL,snapshot,true,SPI_OK_SELECT,ns);
        for(uint32 n=0;n<names->count;++n) {
            bool view=false;Datum id=LakeMetadataValue(names,n,2,INT8OID,true,&view);bool replaced=false;
            for(int k=0;!view && k<count;++k) replaced|=DatumGetInt64(id)==LakePermanentTableId(events[k]->binding);
            if(!replaced && LakeIdentifierEquals(LakeMetadataText(names,n,1),e->new_name))
                ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake RENAME target name conflicts with a live table or view")));
        }
    }
}

void LakeWriteRenames(LakeRenameEvent** events,int count,Snapshot snapshot,int64 commit,int64 schema)
{
    for(int i=0;i<count;++i) {
        LakeRenameEvent* e=events[i];LakeInlineBinding* b=e->binding;Oid ns=b->metadata_schema_oid;
        const char* tables=LakeDefinitionRelation(ns,"ducklake_table");
        Oid types[8]={INT8OID,UUIDOID,INT8OID,INT8OID,TEXTOID,TEXTOID,BOOLOID,INT8OID};
        Datum args[8]={Int64GetDatum(LakePermanentTableId(b)),DirectFunctionCall1(uuid_in,CStringGetDatum(b->table_uuid)),
            Int64GetDatum(commit),Int64GetDatum(e->schema_id),CStringGetTextDatum(e->new_name),
            e->path==NULL?(Datum)0:CStringGetTextDatum(e->path),BoolGetDatum(e->path_relative),Int64GetDatum(e->source_begin)};
        char nulls[9]="        ";if(e->path==NULL)nulls[5]='n';if(e->path_relative_null)nulls[6]='n';
        LakeMetaRows* changed=NULL;
        if(e->column_rename) {
            const char* columns=LakeDefinitionRelation(ns,"ducklake_column");
            Oid ct[5]={INT8OID,INT8OID,INT8OID,INT8OID,TEXTOID};
            Datum ca[5]={args[0],Int64GetDatum(e->column_id),Int64GetDatum(e->column_source_begin),
                Int64GetDatum(commit),CStringGetTextDatum(e->new_column_name)};
            /* Copy the complete standard definition, preserving LIST child ID,
             * defaults/nullability/order/type. End the source only after the
             * INSERT SELECT has read it under the attempt snapshot. */
            changed=LakeMetadataQuery(psprintf("INSERT INTO %s "
                "(column_id,begin_snapshot,end_snapshot,table_id,column_order,column_name,column_type,initial_default,default_value,"
                "nulls_allowed,parent_column,default_value_type,default_value_dialect) "
                "SELECT column_id,$4,NULL,table_id,column_order,$5,column_type,initial_default,default_value,"
                "nulls_allowed,parent_column,default_value_type,default_value_dialect FROM %s "
                "WHERE table_id=$1 AND column_id=$2 AND begin_snapshot=$3 AND end_snapshot IS NULL AND parent_column IS NULL",
                columns,columns),5,ct,ca,NULL,snapshot,false,SPI_OK_INSERT,ns);
            LakeMetadataRequireCount(changed,1,"new RENAME column definitions");
            changed=LakeMetadataQuery(psprintf("UPDATE %s SET end_snapshot=$4 WHERE table_id=$1 "
                "AND column_id=$2 AND begin_snapshot=$3 AND end_snapshot IS NULL AND parent_column IS NULL",columns),
                5,ct,ca,NULL,snapshot,false,SPI_OK_UPDATE,ns);
            LakeMetadataRequireCount(changed,1,"ended RENAME column definitions");
        } else {
        changed=LakeMetadataQuery(psprintf("UPDATE %s SET end_snapshot=$3 WHERE table_id=$1 "
            "AND begin_snapshot=$8 AND end_snapshot IS NULL",tables),8,types,args,nulls,snapshot,false,SPI_OK_UPDATE,ns);
        LakeMetadataRequireCount(changed,1,"ended RENAME definitions");
        changed=LakeMetadataQuery(psprintf("INSERT INTO %s "
            "(table_id,table_uuid,begin_snapshot,end_snapshot,schema_id,table_name,path,path_is_relative) "
            "VALUES($1,$2,$3,NULL,$4,$5,$6,$7)",tables),8,types,args,nulls,snapshot,false,SPI_OK_INSERT,ns);
        LakeMetadataRequireCount(changed,1,"new RENAME definitions");
        }
        Oid vt[3]={INT8OID,INT8OID,INT8OID};Datum va[3]={Int64GetDatum(commit),Int64GetDatum(schema),args[0]};
        changed=LakeMetadataQuery(psprintf("INSERT INTO %s(begin_snapshot,schema_version,table_id) VALUES($1,$2,$3)",
            LakeDefinitionRelation(ns,"ducklake_schema_versions")),3,vt,va,NULL,snapshot,false,SPI_OK_INSERT,ns);
        LakeMetadataRequireCount(changed,1,"RENAME schema versions");
        Oid ht[5]={INT8OID,UUIDOID,INT8OID,UUIDOID,OIDOID};
        Datum ha[5]={Int64GetDatum(schema),DirectFunctionCall1(uuid_in,CStringGetDatum(b->binding_id)),args[0],args[1],ObjectIdGetDatum(b->server_oid)};
        changed=LakeMetadataQuery("UPDATE og_lake_internal.table_binding SET schema_version=$1 WHERE binding_id=$2 "
            "AND table_id=$3 AND table_uuid=$4 AND server_oid=$5 AND state='active' AND NOT read_only",
            5,ht,ha,NULL,snapshot,false,SPI_OK_UPDATE);
        LakeMetadataRequireCount(changed,1,"RENAME host binding versions");
    }
}
