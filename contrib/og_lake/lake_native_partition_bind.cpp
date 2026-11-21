/* Controlled native-parent readonly certification. No standard DuckLake table
 * format change and no synthetic pg_class leaf. New SQL capability is closed. */
#include "postgres.h"
#include "lake_native_partition.h"
#include "lake_txn_private.h"
#include "lake_row_lock.h"
#include "lake_inline_reader.h"
#include "lake_create_publish.h"
#include "catalog/namespace.h"
#include "catalog/indexing.h"
#include "catalog/pg_extension.h"
#include "utils/fmgroids.h"
#include "utils/syscache.h"
#include "access/og_lake.h"
#include "access/heapam.h"
#include "access/reloptions.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/dependency.h"
#include "commands/extension.h"
#include "executor/spi.h"
#include "foreign/foreign.h"
#include "knl/knl_variable.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "commands/sec_rls_cmds.h"
#include "utils/sec_rls_utils.h"
#include "utils/snapmgr.h"
#include <openssl/sha.h>

PG_FUNCTION_INFO_V1(og_lake_bind_native_partition_readonly);
static const int64 LakeNativeCertificationRows=4096;
static const int LakeNativeCertificationFiles=16;

void LakeRequireNativePartitionVersion(void)
{
    (void)LakeHostTransactionStateAddress(); /* Preserve public precommit/preprepare admission. */
    if(!LakeNativePartitionVersionEnabled())
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native parent lake reads require the coordinated readonly partition SQL capability")));
}

static void LakeNativePartitionControl(bool publication=false)
{
    if(publication) {
        if(!LakeNativePartitionWriteVersionEnabled())
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native partition publication is not activated")));
    } else LakeRequireNativePartitionVersion();
    LakeRequireAccessPolicy();
    Oid owner=LakeMetadataOwner(),ns=get_namespace_oid("og_lake_internal",false);
    Oid oid=get_relname_relid("partition_binding",ns),extension=get_extension_oid("og_lake",false);
    if(!OidIsValid(owner)||!OidIsValid(oid)||getExtensionOfObject(RelationRelationId,oid)!=extension)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native partition control binding is unavailable or untrusted")));
    Relation relation=heap_open(oid,AccessShareLock);LakeValidatePrivateRelation(relation,owner,true);
    const char* names[]={"parent_relid","leaf_partition_oid","partition_no","binding_id","certified_snapshot","source_hash"};
    Oid types[]={OIDOID,OIDOID,INT4OID,UUIDOID,INT8OID,TEXTOID};
    for(int c=0;c<6;++c){int a=get_attnum(oid,names[c]);
        if(a<=0||a>RelationGetDescr(relation)->natts||RelationGetDescr(relation)->attrs[a-1].attisdropped||
           RelationGetDescr(relation)->attrs[a-1].atttypid!=types[c])
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native partition control column shape changed")));
    }
    heap_close(relation,NoLock);
}

static void LakeNativeContextString(StringInfo out,const char* text)
{ appendStringInfo(out,"%zu:",text?strlen(text):0);if(text)appendStringInfoString(out,text);appendStringInfoChar(out,';'); }

static char* LakeNativePartitionSourceHash(const LakeInlineBinding* b,int64 anchor=-1,int64 schema=-1)
{
    StringInfoData s;initStringInfo(&s);
    appendStringInfo(&s,"native-root-source-v1;%u;%u;" INT64_FORMAT ";" INT64_FORMAT ";" INT64_FORMAT ";%u;%d;",
        b->server_oid,b->metadata_schema_oid,LakePermanentTableId(b),anchor<0?b->snapshot_id:anchor,schema<0?b->schema_version:schema,b->inline_oid,b->column_count);
    LakeNativeContextString(&s,b->table_uuid);LakeNativeContextString(&s,b->data_root);
    for(int c=0;c<b->column_count;++c){const LakeInlineColumn* col=&b->columns[c];
        appendStringInfo(&s,INT64_FORMAT ";%u;%d;" INT64_FORMAT ";",col->column_id,col->type,col->typmod,col->element_column_id);
        LakeNativeContextString(&s,col->name);LakeNativeContextString(&s,LakeInlinePhysicalName(col));
    }
    appendStringInfo(&s,"%d;",b->file_count);
    for(int f=0;f<b->file_count;++f){const LakeDataFile* file=&b->files[f];
        appendStringInfo(&s,INT64_FORMAT ";" INT64_FORMAT ";" INT64_FORMAT ";%d;" INT64_FORMAT ";" INT64_FORMAT ";" INT64_FORMAT ";" INT64_FORMAT ";",
            file->file_id,file->begin_snapshot,file->row_id_start,file->row_id_start_null,file->record_count,file->file_bytes,file->row_group_count,file->partial_max);
        LakeNativeContextString(&s,file->path);
        appendStringInfo(&s,"%d;",file->deletes!=NULL);
        if(file->deletes){const LakeDeleteFile* d=file->deletes;
            appendStringInfo(&s,INT64_FORMAT ";" INT64_FORMAT ";" INT64_FORMAT ";" INT64_FORMAT ";" INT64_FORMAT ";",
                d->file_id,d->begin_snapshot,d->record_count,d->file_bytes,d->partial_max);
            LakeNativeContextString(&s,d->path);
        }
        appendStringInfo(&s,"%d;",file->inline_delete_count);
        for(int d=0;d<file->inline_delete_count;++d)appendStringInfo(&s,INT64_FORMAT ";",file->inline_delete_positions[d]);
    }
    unsigned char bytes[SHA256_DIGEST_LENGTH];if(!SHA256((const unsigned char*)s.data,s.len,bytes))
        ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("could not hash native partition source identity")));
    char* result=(char*)palloc(65);static const char hex[]="0123456789abcdef";
    for(int i=0;i<32;++i){result[2*i]=hex[bytes[i]>>4];result[2*i+1]=hex[bytes[i]&15];}result[64]='\0';pfree(s.data);return result;
}

/* Closed until the independent future mutation contract is genuinely admitted.
 * Legacy SQL23 always keeps its original whole-head equality contract. */
static bool LakeNativeTableCertificatesEnabled(void)
{
    return OG_LAKE_NATIVE_PARTITION_MUTATION_SQL_VERSION[0]&&OG_LAKE_NATIVE_PARTITION_SQL_VERSION[0]&&
        strcmp(OG_LAKE_NATIVE_PARTITION_MUTATION_SQL_VERSION,OG_LAKE_NATIVE_PARTITION_SQL_VERSION)!=0&&
        LakeNativePartitionMutationVersionEnabled();
}

struct LakeNativeCertificateToken {
    LakeNativePartitionIdentity host;
    Oid server;
    int64 table_id,anchor,schema;
    bool read_only;
    char* binding_uuid;
    char* table_uuid;
    char* hash;
};
struct LakeNativePublicationCertificates {
    int count;
    LakeNativeCertificateToken* tokens;
};
static const int64 LakeNativeCertificateHistoryRows=4096;
static const int64 LakeNativeCertificateHistoryBytes=4*1024*1024;

static LakeNativeCertificateToken* LakeNativeReadCertificateTokens(Relation parent,Snapshot snapshot,int* leafCountOut=NULL)
{
    LakeNativeRangeLayout layout=LakeNativeRangeDescribe(parent);
    Oid type=OIDOID;Datum argument=ObjectIdGetDatum(RelationGetRelid(parent));
    const char* query="SELECT p.leaf_partition_oid,p.partition_no,p.binding_id::text,p.certified_snapshot,p.source_hash::text,"
        "t.server_oid,t.table_id,t.table_uuid::text,t.schema_version,t.read_only,"
        "EXISTS(SELECT 1 FROM og_lake_internal.relation_binding r WHERE r.binding_id=t.binding_id) "
        "FROM og_lake_internal.partition_binding p JOIN og_lake_internal.table_binding t USING(binding_id) "
        "WHERE p.parent_relid=$1 AND t.state='active' AND t.object_kind='partition' ORDER BY p.leaf_partition_oid";
    LakeMetaRows* rows=LakeMetadataQuery(query,
        1,&type,&argument,NULL,snapshot,true,SPI_OK_SELECT);
    LakeMetadataRequireCount(rows,layout.leaf_count,"complete native certificate map");
    auto* tokens=(LakeNativeCertificateToken*)palloc0(sizeof(LakeNativeCertificateToken)*Max(1,layout.leaf_count));
    for(int i=0;i<layout.leaf_count;++i){
        LakeNativeCertificateToken* token=&tokens[i];
        token->host={RelationGetRelid(parent),DatumGetObjectId(LakeMetadataValue(rows,i,1,OIDOID)),
            DatumGetInt32(LakeMetadataValue(rows,i,2,INT4OID))};
        bool found=false;for(int j=0;j<layout.leaf_count;++j)
            if(LakeNativePartitionIdentityEqual(&token->host,&layout.leaves[j]))found=true;
        token->binding_uuid=LakeMetadataText(rows,i,3);token->anchor=DatumGetInt64(LakeMetadataValue(rows,i,4,INT8OID));
        token->hash=LakeMetadataText(rows,i,5);token->server=DatumGetObjectId(LakeMetadataValue(rows,i,6,OIDOID));
        token->table_id=DatumGetInt64(LakeMetadataValue(rows,i,7,INT8OID));token->table_uuid=LakeMetadataText(rows,i,8);
        token->schema=DatumGetInt64(LakeMetadataValue(rows,i,9,INT8OID));token->read_only=DatumGetBool(LakeMetadataValue(rows,i,10,BOOLOID));
        if(!found||!OidIsValid(token->server)||token->table_id<0||token->table_id>=LAKE_REFERENCE_LOCAL_ID_START||
           token->anchor<0||token->schema<0||strlen(token->binding_uuid)!=36||strlen(token->table_uuid)!=36||strlen(token->hash)!=64||
           strspn(token->hash,"0123456789abcdef")!=64||DatumGetBool(LakeMetadataValue(rows,i,11,BOOLOID)))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native certificate identity")));
        if(i&&(tokens[i-1].host.leaf_partition_oid>=token->host.leaf_partition_oid||
           token->server!=tokens[0].server||token->anchor!=tokens[0].anchor||token->schema!=tokens[0].schema||
           token->read_only!=tokens[0].read_only))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native sibling certificates do not share one catalog snapshot")));
        for(int p=0;p<i;++p)
            if(token->table_id==tokens[p].table_id||!strcmp(token->binding_uuid,tokens[p].binding_uuid)||
               !strcmp(token->table_uuid,tokens[p].table_uuid))
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native sibling certificates do not form one distinct catalog set")));
    }
    if(leafCountOut)*leafCountOut=layout.leaf_count;
    return tokens;
}

static int LakeNativeCertificateOrder(const void* left,const void* right)
{
    const auto* a=(const LakeNativeCertificateToken*)left;const auto* b=(const LakeNativeCertificateToken*)right;
    if(a->host.parent_relid!=b->host.parent_relid)return a->host.parent_relid<b->host.parent_relid?-1:1;
    return a->host.leaf_partition_oid<b->host.leaf_partition_oid?-1:a->host.leaf_partition_oid>b->host.leaf_partition_oid?1:0;
}

static bool LakeNativeSameCertificate(const LakeNativeCertificateToken* a,const LakeNativeCertificateToken* b)
{
    return LakeNativePartitionIdentityEqual(&a->host,&b->host)&&a->server==b->server&&a->table_id==b->table_id&&
        a->anchor==b->anchor&&a->schema==b->schema&&a->read_only==b->read_only&&
        !strcmp(a->binding_uuid,b->binding_uuid)&&!strcmp(a->table_uuid,b->table_uuid)&&!strcmp(a->hash,b->hash);
}

/* Exact raw multiset comparison precedes host logical-name adaptation. No
 * schema-version0 guess and no user-defined equality/casts under SPI owner. */
static void LakeNativeRequireSameRawSchema(const LakeInlineBinding* binding,int64 anchor,Snapshot snapshot)
{
    Oid types[3]={INT8OID,INT8OID,INT8OID};
    Datum args[3]={Int64GetDatum(LakePermanentTableId(binding)),Int64GetDatum(anchor),Int64GetDatum(binding->snapshot_id)};
    const char* names[]={"ducklake_table","ducklake_column"};
    const char* fields[]={"table_id,table_uuid,schema_id,table_name::text,path::text,path_is_relative",
        "column_id,column_order,column_name::text,column_type::text,parent_column,initial_default::text,default_value::text,"
        "nulls_allowed,default_value_type::text,default_value_dialect::text"};
    for(int i=0;i<2;++i){
        const char* table=LakeDefinitionRelation(binding->metadata_schema_oid,names[i]);
        char* before=psprintf("SELECT %s FROM %s WHERE table_id=$1 AND begin_snapshot<=$2 AND(end_snapshot IS NULL OR end_snapshot>$2)",fields[i],table);
        char* after=psprintf("SELECT %s FROM %s WHERE table_id=$1 AND begin_snapshot<=$3 AND(end_snapshot IS NULL OR end_snapshot>$3)",fields[i],table);
        LakeMetaRows* rows=LakeMetadataQuery(psprintf("SELECT NOT EXISTS ((%s EXCEPT ALL %s) UNION ALL (%s EXCEPT ALL %s))",
            before,after,after,before),3,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
        LakeMetadataRequireCount(rows,1,"native raw table schema proof");
        if(!DatumGetBool(LakeMetadataValue(rows,0,1,BOOLOID)))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native certified table schema changed")));
    }
    const char* versions=LakeDefinitionRelation(binding->metadata_schema_oid,"ducklake_schema_versions");
    LakeMetaRows* versionsChanged=LakeMetadataQuery(psprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE table_id=$1 AND begin_snapshot>$2 AND begin_snapshot<=$3)",versions),
        3,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    LakeMetadataRequireCount(versionsChanged,1,"native table schema epoch proof");
    if(DatumGetBool(LakeMetadataValue(versionsChanged,0,1,BOOLOID)))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native certified table schema epoch changed")));
}

static void LakeNativeRequireDisjointHistory(const LakeInlineBinding* binding,
    const LakeNativeCertificateToken* tokens,int leafCount,Snapshot snapshot)
{
    const int64 anchor=tokens[0].anchor,head=binding->snapshot_id;
    if(anchor<0||head<anchor||head-anchor>LakeNativeCertificateHistoryRows)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native certificate history requires bounded maintenance catch-up")));
    Oid types[2]={INT8OID,INT8OID};Datum args[2]={Int64GetDatum(anchor),Int64GetDatum(head)};
    const char* snapshots=LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_snapshot");
    LakeMetaRows* epoch=LakeMetadataQuery(psprintf("SELECT schema_version FROM %s WHERE snapshot_id=$1",snapshots),
        1,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    LakeMetadataRequireCount(epoch,1,"native certificate anchor snapshot");
    if(DatumGetInt64(LakeMetadataValue(epoch,0,1,INT8OID))!=tokens[0].schema||tokens[0].schema>binding->schema_version)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native certificate schema anchor is incoherent")));
    if(head==anchor)return;
    const char* changes=LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_snapshot_changes");
    LakeMetaRows* budget=LakeMetadataQuery(psprintf("SELECT count(*)::bigint,COALESCE(sum(octet_length(changes_made)),0)::bigint "
        "FROM %s WHERE snapshot_id>$1 AND snapshot_id<=$2",changes),2,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    LakeMetadataRequireCount(budget,1,"native certificate history budget");
    int64 bytes=DatumGetInt64(LakeMetadataValue(budget,0,2,INT8OID));
    if(DatumGetInt64(LakeMetadataValue(budget,0,1,INT8OID))!=head-anchor||bytes<0||bytes>LakeNativeCertificateHistoryBytes)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native certificate history is missing or exceeds its byte budget")));
    LakeMetaRows* history=LakeMetadataQuery(psprintf("SELECT snapshot_id,changes_made::text FROM %s WHERE snapshot_id>$1 AND snapshot_id<=$2 ORDER BY snapshot_id",changes),
        2,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    if((int64)history->count!=head-anchor)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native certificate history is incomplete")));
    for(uint32 row=0;row<history->count;++row){
        CHECK_FOR_INTERRUPTS();
        if(DatumGetInt64(LakeMetadataValue(history,row,1,INT8OID))!=anchor+(int64)row+1)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native certificate history has a gap")));
        const char* cursor=LakeMetadataText(history,row,2);
        if(!*cursor)ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native certificate history has an unclassified empty change")));
        LakePublicationChange change;
        while(LakeNextCertificateChange(&cursor,&change)){
            CHECK_FOR_INTERRUPTS();
            if(change.kind==LAKE_CHANGE_CREATED_TABLE)continue;
            for(int i=0;i<leafCount;++i)if(change.table_id==tokens[i].table_id)
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native certified leaf changed outside its atomic publisher")));
        }
    }
}

static LakeNativeLeafReadBinding LakeLoadNativePartitionLeafUnpinned(Relation parent,const LakeNativePartitionIdentity* leaf,Snapshot snapshot,bool publication=false,int64 claimedSnapshot=-1)
{
    LakeNativePartitionControl(publication);
    if(!LakeHasNativePartitionMarker(parent))
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native partition marker is absent")));
    LakeNativeRangeLayout layout=LakeNativeRangeDescribe(parent);bool found=false;
    for(int i=0;i<layout.leaf_count;++i)if(LakeNativePartitionIdentityEqual(leaf,&layout.leaves[i]))found=true;
    if(!found)ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native leaf is no longer in the certified parent")));
    Oid types[2]={OIDOID,OIDOID};Datum args[2]={ObjectIdGetDatum(leaf->parent_relid),ObjectIdGetDatum(leaf->leaf_partition_oid)};
    LakeMetaRows* rows=LakeMetadataQuery("SELECT t.binding_id::text,t.server_oid,t.table_id,t.table_uuid::text,t.schema_version,"
        "t.read_only,p.partition_no,p.certified_snapshot,p.source_hash::text,"
        "EXISTS(SELECT 1 FROM og_lake_internal.relation_binding r WHERE r.binding_id=t.binding_id) "
        "FROM og_lake_internal.partition_binding p JOIN og_lake_internal.table_binding t USING(binding_id) "
        "WHERE p.parent_relid=$1 AND p.leaf_partition_oid=$2 AND t.state='active' AND t.object_kind='partition'",
        2,types,args,NULL,snapshot,true,SPI_OK_SELECT);
    LakeMetadataRequireCount(rows,1,"certified native partition binding");
    Oid server=DatumGetObjectId(LakeMetadataValue(rows,0,2,OIDOID));
    int64 table=DatumGetInt64(LakeMetadataValue(rows,0,3,INT8OID));
    bool readOnly=DatumGetBool(LakeMetadataValue(rows,0,6,BOOLOID));
    if(table<0||table>=LAKE_REFERENCE_LOCAL_ID_START||(!readOnly&&!LakeNativePartitionWriteVersionEnabled())||
       DatumGetInt32(LakeMetadataValue(rows,0,7,INT4OID))!=leaf->partition_no||
       DatumGetBool(LakeMetadataValue(rows,0,10,BOOLOID)))
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native leaf binding identity or readonly scope changed")));
    int64 certified=DatumGetInt64(LakeMetadataValue(rows,0,8,INT8OID));
    LakeInlineBinding* head=LakeLoadCatalogHeader(server,snapshot);
    if(!readOnly&&head->read_only)
        ereport(ERROR,(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),errmsg("native leaf catalog binding is read-only")));
    bool relaxed=LakeNativeTableCertificatesEnabled();
    bool ownClaim=publication&&claimedSnapshot>0&&head->snapshot_id==claimedSnapshot&&
        (relaxed?certified<=claimedSnapshot-1:certified==claimedSnapshot-1);
    if(certified<0||(relaxed&&publication&&claimedSnapshot>0&&!ownClaim)||(!relaxed&&head->snapshot_id!=certified&&!ownClaim))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native partition catalog has changed since certification")));
    LakeInlineBinding* b=LakeReadNativePartitionSource(server,table,snapshot,parent,LakeMetadataText(rows,0,1),
        ownClaim?(relaxed?claimedSnapshot-1:certified):-1);
    b->read_only=readOnly;
    if(certified<0||(!relaxed&&b->snapshot_id!=certified))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native partition catalog has changed since readonly certification"),
            errhint("This slice permits only the certified lake snapshot, including no unrelated-table head advance; recertification or routed writers is required.")));
    if(relaxed){
        MemoryContext caller=CurrentMemoryContext;
        MemoryContext proof=AllocSetContextCreate(caller,"Native table certificate proof",ALLOCSET_DEFAULT_SIZES);
        PG_TRY();{
            MemoryContextSwitchTo(proof);
        int tokenCount=0;
        LakeNativeCertificateToken* tokens=LakeNativeReadCertificateTokens(parent,snapshot,&tokenCount);
        LakeNativeRequireDisjointHistory(b,tokens,tokenCount,snapshot);
        for(int i=0;i<tokenCount;++i){
            LakeInlineBinding* raw=tokens[i].table_id==table?b:
                LakeReadNativePartitionSource(server,tokens[i].table_id,snapshot,parent,tokens[i].binding_uuid,
                    ownClaim?claimedSnapshot-1:-1);
            LakeNativeRequireSameRawSchema(raw,tokens[i].anchor,snapshot);
            if(strcmp(raw->table_uuid,tokens[i].table_uuid)||
               strcmp(LakeNativePartitionSourceHash(raw,tokens[i].anchor,tokens[i].schema),tokens[i].hash))
                ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native sibling certificate source identity changed")));
        }
            MemoryContextSwitchTo(caller);MemoryContextDelete(proof);
        }PG_CATCH();{
            MemoryContextSwitchTo(caller);MemoryContextDelete(proof);PG_RE_THROW();
        }PG_END_TRY();
    }
    if(strcmp(b->table_uuid,LakeMetadataText(rows,0,4))||(!relaxed&&b->schema_version!=DatumGetInt64(LakeMetadataValue(rows,0,5,INT8OID)))||
       (!relaxed&&strcmp(LakeNativePartitionSourceHash(b),LakeMetadataText(rows,0,9))))
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native certified table, schema or physical source changed")));
    LakeNativeLeafReadBinding result={*leaf,b,certified};return result;
}

/* A fresh latest pointer may be the mutable SecondarySnapshot. Reserve before
 * copying and retain it through all nested metadata SPI in the future path. */
static LakeNativeLeafReadBinding LakeLoadNativePartitionLeafInternal(Relation parent,
    const LakeNativePartitionIdentity* leaf,Snapshot snapshot,bool publication=false,int64 claimedSnapshot=-1)
{
    if(!LakeNativeTableCertificatesEnabled())
        return LakeLoadNativePartitionLeafUnpinned(parent,leaf,snapshot,publication,claimedSnapshot);
    if(!snapshot)ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("native certificate requires an explicit snapshot")));
    ResourceOwner owner=t_thrd.utils_cxt.CurrentResourceOwner;ResourceOwnerEnlargeSnapshots(owner);
    Snapshot pinned=RegisterSnapshotOnOwner(PgFdwCopySnapshot(snapshot),owner);
    LakeNativeLeafReadBinding result={};
    PG_TRY();{result=LakeLoadNativePartitionLeafUnpinned(parent,leaf,pinned,publication,claimedSnapshot);
        UnregisterSnapshotFromOwner(pinned,owner);
    }PG_CATCH();{UnregisterSnapshotFromOwner(pinned,owner);PG_RE_THROW();}PG_END_TRY();
    return result;
}

LakeNativeLeafReadBinding LakeLoadNativePartitionLeaf(Relation parent,const LakeNativePartitionIdentity* leaf,Snapshot snapshot)
{ return LakeLoadNativePartitionLeafInternal(parent,leaf,snapshot); }

static void LakeNativeCheckPublisher(LakeTxnState* state,const LakeTxnTable* table)
{
    if(!state||state->phase!=LAKE_TXN_PUBLISHING||!state->publication_subtransaction||!table||table->pending||
       !table->active||!OidIsValid(table->native_leaf_partition_oid)||!LakeNativePartitionWriteVersionEnabled()||
       table->binding->read_only||state->writable_server!=table->binding->server_oid||
       state->binding_ddl||state->flush||state->rewrite||state->renames)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native partition publisher has no typed publication authority")));
    bool canonical=false;
    for(LakeTxnCatalog* catalog=state->catalogs;catalog;catalog=catalog->next) {
        if(!catalog->active||catalog!=table->catalog||catalog->server_oid!=state->writable_server)continue;
        for(LakeTxnTable* root=catalog->tables;root;root=root->next)
            if(root->active&&root->binding->binding_id&&table->binding->binding_id&&
               root->binding->relation_oid==table->binding->relation_oid&&
               root->native_leaf_partition_oid==table->native_leaf_partition_oid&&root->native_partition_no==table->native_partition_no&&
               LakeSameIdentity(root->binding->identity,table->binding->identity)&&
               !strcmp(root->binding->binding_id,table->binding->binding_id)&&!strcmp(root->binding->table_uuid,table->binding->table_uuid))canonical=true;
    }
    if(!canonical)ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native publisher leaf journal is not canonical")));
}

LakeInlineBinding* LakeNativePartitionPublicationBinding(LakeTxnState* state,const LakeTxnTable* table,
    Snapshot snapshot,int64 claimedSnapshot)
{
    LakeNativeCheckPublisher(state,table);
    Relation parent=heap_open(table->binding->relation_oid,AccessShareLock);
    LakeNativePartitionIdentity identity={RelationGetRelid(parent),table->native_leaf_partition_oid,table->native_partition_no};
    LakeNativeLeafReadBinding leaf=LakeLoadNativePartitionLeafInternal(parent,&identity,snapshot,true,claimedSnapshot);
    if(leaf.binding->read_only||!LakeSameIdentity(leaf.binding->identity,table->binding->identity)||
       strcmp(leaf.binding->binding_id,table->binding->binding_id)||strcmp(leaf.binding->table_uuid,table->binding->table_uuid))
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native publication leaf binding changed")));
    heap_close(parent,NoLock);return leaf.binding;
}

LakeNativePublicationCertificates* LakeNativePreparePublicationCertificates(LakeTxnState* state,
    LakeTxnTable** tables,int count,Snapshot snapshot)
{
    if(!LakeNativeTableCertificatesEnabled())return NULL;
    if(count<0||count>MaxAllocSize/(LAKE_NATIVE_RANGE_LEAF_LIMIT*sizeof(LakeNativeCertificateToken)))
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native publication certificate set is too large")));
    auto* result=(LakeNativePublicationCertificates*)palloc0(sizeof(LakeNativePublicationCertificates));
    int capacity=Max(1,count)*2;
    result->tokens=(LakeNativeCertificateToken*)palloc0((Size)capacity*sizeof(LakeNativeCertificateToken));
    if(!snapshot)ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("native certificate requires an explicit snapshot")));
    ResourceOwner owner=t_thrd.utils_cxt.CurrentResourceOwner;ResourceOwnerEnlargeSnapshots(owner);
    Snapshot pinned=RegisterSnapshotOnOwner(PgFdwCopySnapshot(snapshot),owner);
    PG_TRY();{
        for(int at=0;at<count;++at){
            LakeTxnTable* table=tables[at];if(!OidIsValid(table->native_leaf_partition_oid))continue;
            LakeNativeCheckPublisher(state,table);
            bool prior=false;for(int i=0;i<result->count;++i)
                if(result->tokens[i].host.parent_relid==table->binding->relation_oid)prior=true;
            if(prior)continue;
            Relation parent=heap_open(table->binding->relation_oid,AccessShareLock);
            int tokenCount=0;
            LakeNativeCertificateToken* tokens=LakeNativeReadCertificateTokens(parent,pinned,&tokenCount);
            if(result->count+tokenCount>capacity){
                while(result->count+tokenCount>capacity)capacity*=2;
                result->tokens=(LakeNativeCertificateToken*)repalloc(result->tokens,(Size)capacity*sizeof(LakeNativeCertificateToken));
            }
            for(int i=0;i<tokenCount;++i){
                if(tokens[i].read_only)ereport(ERROR,(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),errmsg("native publication sibling is read-only")));
                /* This is the already-authorized publisher checked above,
                 * reading all sibling certificates before its snapshot claim.
                 * Keep public reads on their separate precommit admission. */
                (void)LakeLoadNativePartitionLeafInternal(parent,&tokens[i].host,pinned,true);
                result->tokens[result->count++]=tokens[i];
            }
            heap_close(parent,NoLock);
        }
        qsort(result->tokens,result->count,sizeof(LakeNativeCertificateToken),LakeNativeCertificateOrder);
        UnregisterSnapshotFromOwner(pinned,owner);
    }PG_CATCH();{UnregisterSnapshotFromOwner(pinned,owner);PG_RE_THROW();}PG_END_TRY();
    return result;
}

/* The catalog PK has been won. This read-only check detects stale attempt
 * tokens; final exact CAS closes a later metadata-only anchor race. No extra
 * tuple locks or temporary change to business DML GUC policy is introduced. */
bool LakeNativePublicationCertificatesStillCurrent(LakeTxnState* state,
    const LakeNativePublicationCertificates* saved,Snapshot snapshot)
{
    if(!saved)return true;
    if(!LakeNativeTableCertificatesEnabled()||!state||state->phase!=LAKE_TXN_PUBLISHING||!state->publication_subtransaction)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native certificate recheck requires actual publication")));
    if(!snapshot)ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("native certificate requires an explicit snapshot")));
    ResourceOwner owner=t_thrd.utils_cxt.CurrentResourceOwner;ResourceOwnerEnlargeSnapshots(owner);
    Snapshot pinned=RegisterSnapshotOnOwner(PgFdwCopySnapshot(snapshot),owner);bool valid=true;
    PG_TRY();{
        for(int i=0;i<saved->count;){
            Oid parentId=saved->tokens[i].host.parent_relid;
            int groupCount=0;
            while(i+groupCount<saved->count&&saved->tokens[i+groupCount].host.parent_relid==parentId)++groupCount;
            Relation parent=heap_open(parentId,AccessShareLock);
            int currentCount=0;
            LakeNativeCertificateToken* current=LakeNativeReadCertificateTokens(parent,pinned,&currentCount);
            if(currentCount!=groupCount)valid=false;
            for(int j=0;valid&&j<groupCount;++j)
                if(!LakeNativeSameCertificate(&saved->tokens[i+j],&current[j]))valid=false;
            heap_close(parent,NoLock);
            if(!valid)break;
            i+=groupCount;
        }
        UnregisterSnapshotFromOwner(pinned,owner);
    }PG_CATCH();{UnregisterSnapshotFromOwner(pinned,owner);PG_RE_THROW();}PG_END_TRY();
    return valid;
}

static const LakeNativeCertificateToken* LakeNativeFindCertificate(
    const LakeNativePublicationCertificates* saved,const LakeNativePartitionIdentity* identity)
{
    if(saved)for(int i=0;i<saved->count;++i)
        if(LakeNativePartitionIdentityEqual(&saved->tokens[i].host,identity))return &saved->tokens[i];
    ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native publication has no complete prevalidated certificate")));
    return NULL;
}

static const LakeNativePartitionIdentity* LakeNativeOrderedCertificateIdentity(
    const LakeNativePublicationCertificates* saved,Oid parent,int ordinal)
{
    for(int i=0;i<saved->count;++i)if(saved->tokens[i].host.parent_relid==parent)
        if(ordinal--==0)return &saved->tokens[i].host;
    ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native sibling certificate set is incomplete")));
    return NULL;
}

/* Metadata only, called once after all final per-table business writes in a
 * successful catalog claim. No SDK payload read or row-ID allocation here. */
void LakeNativePartitionRefreshPublished(LakeTxnState* state,LakeTxnTable** tables,int count,
    Snapshot snapshot,int64 published,const LakeNativePublicationCertificates* certificates)
{
    if(published<=0||count<0||(count&&!tables)||!snapshot)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid native partition head refresh")));
    for(int at=0;at<count;++at) {
        LakeTxnTable* table=tables[at];if(!OidIsValid(table->native_leaf_partition_oid))continue;
        bool prior=false;for(int p=0;p<at;++p)if(tables[p]->binding->relation_oid==table->binding->relation_oid&&
            OidIsValid(tables[p]->native_leaf_partition_oid))prior=true;
        if(prior)continue;LakeNativeCheckPublisher(state,table);LakeNativePartitionControl(true);
        Relation parent=heap_open(table->binding->relation_oid,AccessShareLock);LakeNativeRangeLayout layout=LakeNativeRangeDescribe(parent);
        for(int f=0;f<layout.leaf_count;++f) {
            const LakeNativePartitionIdentity* leaf=certificates?
                LakeNativeOrderedCertificateIdentity(certificates,RelationGetRelid(parent),f):&layout.leaves[f];
            Oid types[2]={OIDOID,OIDOID};
            Datum args[2]={ObjectIdGetDatum(RelationGetRelid(parent)),ObjectIdGetDatum(leaf->leaf_partition_oid)};
            LakeMetaRows* map=LakeMetadataQuery("SELECT p.binding_id::text,p.partition_no,p.certified_snapshot,p.source_hash::text,"
                "t.server_oid,t.table_id,t.table_uuid::text,t.read_only FROM og_lake_internal.partition_binding p "
                "JOIN og_lake_internal.table_binding t USING(binding_id) WHERE p.parent_relid=$1 AND p.leaf_partition_oid=$2 "
                "AND t.state='active' AND t.object_kind='partition'",2,types,args,NULL,snapshot,true,SPI_OK_SELECT);
            LakeMetadataRequireCount(map,1,"native publication complete leaf map");
            if(DatumGetInt32(LakeMetadataValue(map,0,2,INT4OID))!=leaf->partition_no||
               (!certificates&&DatumGetInt64(LakeMetadataValue(map,0,3,INT8OID))!=published-1)||
               DatumGetObjectId(LakeMetadataValue(map,0,5,OIDOID))!=state->writable_server||
               DatumGetBool(LakeMetadataValue(map,0,8,BOOLOID)))
                ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native partition certification changed before atomic refresh")));
            int64 tableId=DatumGetInt64(LakeMetadataValue(map,0,6,INT8OID));
            LakeInlineBinding* now=LakeReadNativePartitionSource(state->writable_server,tableId,snapshot,parent,LakeMetadataText(map,0,1));
            if(now->snapshot_id!=published||strcmp(now->table_uuid,LakeMetadataText(map,0,7)))
                ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native refreshed source does not belong to the winning catalog snapshot")));
            if(certificates){
                const LakeNativeCertificateToken* old=LakeNativeFindCertificate(certificates,leaf);
                if(old->server!=state->writable_server||old->table_id!=tableId||old->read_only||
                   old->anchor!=DatumGetInt64(LakeMetadataValue(map,0,3,INT8OID))||
                   strcmp(old->hash,LakeMetadataText(map,0,4))||strcmp(old->binding_uuid,LakeMetadataText(map,0,1))||
                   strcmp(old->table_uuid,LakeMetadataText(map,0,7)))
                    ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native prevalidated sibling certificate changed")));
                Oid st[6]={UUIDOID,OIDOID,INT8OID,UUIDOID,INT8OID,INT8OID};
                Datum sa[6]={DirectFunctionCall1(uuid_in,CStringGetDatum(old->binding_uuid)),ObjectIdGetDatum(old->server),
                    Int64GetDatum(old->table_id),DirectFunctionCall1(uuid_in,CStringGetDatum(old->table_uuid)),
                    Int64GetDatum(old->schema),Int64GetDatum(now->schema_version)};
                LakeMetadataRequireCount(LakeMetadataQuery("UPDATE og_lake_internal.table_binding SET schema_version=$6 "
                    "WHERE binding_id=$1 AND server_oid=$2 AND table_id=$3 AND table_uuid=$4 AND schema_version=$5 "
                    "AND state='active' AND object_kind='partition' AND NOT read_only",6,st,sa,NULL,snapshot,false,SPI_OK_UPDATE),
                    1,"atomic native certificate schema anchor");
                Oid pt[8]={OIDOID,OIDOID,UUIDOID,INT4OID,INT8OID,TEXTOID,INT8OID,TEXTOID};
                Datum pa[8]={ObjectIdGetDatum(leaf->parent_relid),ObjectIdGetDatum(leaf->leaf_partition_oid),sa[0],
                    Int32GetDatum(leaf->partition_no),Int64GetDatum(old->anchor),CStringGetTextDatum(old->hash),
                    Int64GetDatum(published),CStringGetTextDatum(LakeNativePartitionSourceHash(now))};
                LakeMetadataRequireCount(LakeMetadataQuery("UPDATE og_lake_internal.partition_binding SET certified_snapshot=$7,source_hash=$8 "
                    "WHERE parent_relid=$1 AND leaf_partition_oid=$2 AND binding_id=$3 AND partition_no=$4 "
                    "AND certified_snapshot=$5 AND source_hash=$6",8,pt,pa,NULL,snapshot,false,SPI_OK_UPDATE),
                    1,"atomic exact native sibling certificate refresh");
                CommandCounterIncrement();snapshot->curcid=GetCurrentCommandId(false);continue;
            }
            Oid ut[5]={OIDOID,OIDOID,INT8OID,TEXTOID,TEXTOID};
            Datum ua[5]={args[0],args[1],Int64GetDatum(published),CStringGetTextDatum(LakeNativePartitionSourceHash(now)),
                CStringGetTextDatum(LakeMetadataText(map,0,4))};
            LakeMetadataRequireCount(LakeMetadataQuery("UPDATE og_lake_internal.partition_binding SET certified_snapshot=$3,source_hash=$4 "
                "WHERE parent_relid=$1 AND leaf_partition_oid=$2 AND certified_snapshot=$3-1 AND source_hash=$5",5,ut,ua,NULL,
                snapshot,false,SPI_OK_UPDATE),1,"atomic native leaf certification refresh");
            CommandCounterIncrement();snapshot->curcid=GetCurrentCommandId(false);
        }
        heap_close(parent,NoLock);
    }
}

Oid LakeNativePartitionServer(Relation parent,Snapshot snapshot)
{
    LakeNativeRangeLayout layout=LakeNativeRangeDescribe(parent);Oid server=InvalidOid;int64 certified=-1;bool readOnly=true;
    LakeNativePartitionControl();Oid type=OIDOID;Datum argument=ObjectIdGetDatum(RelationGetRelid(parent));
    LakeMetaRows* count=LakeMetadataQuery("SELECT count(*)::bigint FROM og_lake_internal.partition_binding WHERE parent_relid=$1",
        1,&type,&argument,NULL,snapshot,true,SPI_OK_SELECT);
    LakeMetadataRequireCount(count,1,"complete native parent mapping");
    if(DatumGetInt64(LakeMetadataValue(count,0,1,INT8OID))!=layout.leaf_count)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native parent contains missing or extra leaf mappings")));
    for(int i=0;i<layout.leaf_count;++i){LakeNativeLeafReadBinding leaf=LakeLoadNativePartitionLeaf(parent,&layout.leaves[i],snapshot);
        if(i&&(server!=leaf.binding->server_oid||certified!=leaf.certified_snapshot||readOnly!=leaf.binding->read_only))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native parent leaves do not share one certified catalog snapshot")));
        server=leaf.binding->server_oid;certified=leaf.certified_snapshot;readOnly=leaf.binding->read_only;
    }
    return server;
}

struct LakeNativeCertificationScan {
    MemoryContext context;
    MemoryContext row_context;
    LakeInlineReader* reader;
    TupleTableSlot* slot;
};
static void LakeNativeCertificationClose(LakeNativeCertificationScan* scan)
{
    if(scan->reader){LakeInlineReaderClose(scan->reader);scan->reader=NULL;}
    if(scan->slot){ExecDropSingleTupleTableSlot(scan->slot);scan->slot=NULL;}
    if(scan->context){MemoryContextDelete(scan->context);scan->context=NULL;}
}

static void LakeNativeCertifyPayload(Relation parent,const LakeNativeRangeLayout* layout,
    const LakeNativePartitionIdentity* leaf,LakeInlineBinding* source,Snapshot snapshot)
{
    if(source->file_count>LakeNativeCertificationFiles)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native partition certification exceeds the bounded file set")));
    int64 physical=0;
    for(int f=0;f<source->file_count;++f){int64 n=source->files[f].record_count;
        if(n<0||n>LakeNativeCertificationRows-physical)
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native partition certification exceeds4096 physical file versions")));
        physical+=n;
    }
    if(OidIsValid(source->inline_oid)){
        const char* name=get_rel_name(source->inline_oid);if(!name)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native source carrier missing")));
        LakeMetaRows* n=LakeMetadataQuery(psprintf("SELECT count(*)::bigint FROM %s",LakeMetadataRelation(source->metadata_schema_oid,name)),
            0,NULL,NULL,NULL,snapshot,true,SPI_OK_SELECT,source->metadata_schema_oid);
        LakeMetadataRequireCount(n,1,"native certification inline population");
        int64 count=DatumGetInt64(LakeMetadataValue(n,0,1,INT8OID));
        if(count<0||count>LakeNativeCertificationRows-physical)
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native partition certification exceeds4096 physical versions")));
    }
    MemoryContext caller=CurrentMemoryContext;
    LakeNativeCertificationScan* scan=(LakeNativeCertificationScan*)palloc0(sizeof(LakeNativeCertificationScan));
    scan->context=AllocSetContextCreate(caller,"Native partition complete source certification",ALLOCSET_DEFAULT_SIZES);
    PG_TRY();{
        MemoryContextSwitchTo(scan->context);scan->row_context=AllocSetContextCreate(scan->context,"Native certification source row",ALLOCSET_DEFAULT_SIZES);
        LakeTxnTable* view=LakeTxnCreateQueryReadView(source,snapshot,RelationGetDescr(parent));
        scan->reader=LakeInlineReaderOpen(view,snapshot->curcid);scan->slot=MakeSingleTupleTableSlot(view->descriptor);
        Datum* values=(Datum*)palloc(sizeof(Datum)*source->column_count);bool* nulls=(bool*)palloc(sizeof(bool)*source->column_count);
        HASHCTL control={};control.keysize=sizeof(int64);control.entrysize=sizeof(int64);control.hcxt=scan->context;
        HTAB* identities=hash_create("Native certified visible row IDs",128,&control,HASH_ELEM|HASH_BLOBS|HASH_CONTEXT);
        int64 visible=0;
        for(;;){CHECK_FOR_INTERRUPTS();ExecClearTuple(scan->slot);MemoryContextReset(scan->row_context);MemoryContextSwitchTo(scan->row_context);
            LakeInlineRowIdentity identity={};if(!LakeInlineReaderNext(scan->reader,values,nulls,&identity))break;
            if(++visible>LakeNativeCertificationRows)ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native source certification coverage exceeded")));
            if(identity.row_id<0||identity.row_id>=LAKE_REFERENCE_LOCAL_ID_START||identity.begin_snapshot<0||identity.begin_snapshot>source->snapshot_id)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native source visible logical identity")));
            bool duplicate=false;(void)hash_search(identities,&identity.row_id,HASH_ENTER,&duplicate);
            if(duplicate)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("multiple visible native source births share one row ID")));
            for(int c=0;c<source->column_count;++c){scan->slot->tts_values[c]=values[c];scan->slot->tts_isnull[c]=nulls[c];}
            ExecStoreVirtualTuple(scan->slot);LakeNativePartitionIdentity actual=LakeNativeRangeRoute(parent,layout,scan->slot);
            if(!LakeNativePartitionIdentityEqual(&actual,leaf))
                ereport(ERROR,(errcode(ERRCODE_CHECK_VIOLATION),errmsg("lake payload lies outside its native leaf boundary")));
            MemoryContextSwitchTo(scan->context);
        }
        MemoryContextSwitchTo(caller);LakeNativeCertificationClose(scan);pfree(scan);
    }PG_CATCH();{MemoryContextSwitchTo(caller);LakeNativeCertificationClose(scan);pfree(scan);PG_RE_THROW();}PG_END_TRY();
}

/* Binder body and exact SQL registration follow in this same isolated slice. */
struct LakeNativeBindResources { Snapshot captured; Snapshot verify; ResourceOwner owner; bool own_pushed; };

static void LakeNativeWriteRootMarker(Relation parent)
{
    /* Only the controlled binder calls this after every map and source check.
     * User reloptions validation is deliberately not disabled session-wide. */
    Relation classes=heap_open(RelationRelationId,RowExclusiveLock);
    HeapTuple old=SearchSysCache1(RELOID,ObjectIdGetDatum(RelationGetRelid(parent)));
    if(!HeapTupleIsValid(old))ereport(ERROR,(errcode(ERRCODE_UNDEFINED_TABLE),errmsg("native parent disappeared")));
    bool absent=false;Datum raw=SysCacheGetAttr(RELOID,old,Anum_pg_class_reloptions,&absent);
    List* marker=list_make1(makeDefElem(pstrdup("lake_partition_root"),(Node*)makeString(pstrdup("true"))));
    Datum options=transformRelOptions(absent?(Datum)0:raw,marker,NULL,NULL,false,false);
    Datum values[Natts_pg_class]={0};bool nulls[Natts_pg_class]={false},replace[Natts_pg_class]={false};
    values[Anum_pg_class_reloptions-1]=options;replace[Anum_pg_class_reloptions-1]=true;
    HeapTuple updated=heap_modify_tuple(old,RelationGetDescr(classes),values,nulls,replace);
    simple_heap_update(classes,&updated->t_self,updated);CatalogUpdateIndexes(classes,updated);
    heap_freetuple(updated);ReleaseSysCache(old);heap_close(classes,NoLock);
    CacheInvalidateRelcacheByRelid(RelationGetRelid(parent));CommandCounterIncrement();
}

Datum og_lake_bind_native_partition_readonly(PG_FUNCTION_ARGS)
{
    LakeRequireNativePartitionVersion();LakeNativePartitionControl();LakeInlineRequireAdministrator();
    if(PG_ARGISNULL(0)||PG_ARGISNULL(1)||PG_ARGISNULL(2)||PG_ARGISNULL(3))
        ereport(ERROR,(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),errmsg("native readonly bind arguments cannot be NULL")));
    if(u_sess->attr.attr_common.XactReadOnly||u_sess->utils_cxt.XactIsoLevel!=XACT_READ_COMMITTED)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native readonly binding requires a writable READ COMMITTED transaction")));
    LakeTxnRejectBindingDdl();LakeLockCatalogRegistration();
    Oid parentId=PG_GETARG_OID(0);Relation parent=heap_open(parentId,AccessExclusiveLock);
    if(!pg_class_ownercheck(parentId,GetUserId()))aclcheck_error(ACLCHECK_NOT_OWNER,ACL_KIND_CLASS,RelationGetRelationName(parent));
    if(parent->rd_rules||parent->trigdesc||RelationHasRlspolicy(parentId)||RelationEnableRowSecurity(parent))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native readonly binding does not admit host rules, triggers or RLS")));
    if(LakeHasNativePartitionMarker(parent))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native partition recertification requires its separate adapter")));
    LakeNativeRangeLayout layout=LakeNativeRangeDescribe(parent);LakeNativeRangeRequirePhysicallyEmpty(parent,&layout);
    ForeignServer* server=GetForeignServerByName(NameStr(*PG_GETARG_NAME(1)),false);
    AclResult permission=pg_foreign_server_aclcheck(server->serverid,GetUserId(),ACL_USAGE);
    if(permission!=ACLCHECK_OK)aclcheck_error(permission,ACL_KIND_FOREIGN_SERVER,server->servername);
    if(!LakeTrustedForeignServer(server->serverid))
        ereport(ERROR,(errcode(ERRCODE_WRONG_OBJECT_TYPE),errmsg("native readonly bind requires a trusted og_lake server")));
    ArrayType* leafArray=PG_GETARG_ARRAYTYPE_P(2);ArrayType* tableArray=PG_GETARG_ARRAYTYPE_P(3);
    if(ARR_NDIM(leafArray)!=1||ARR_ELEMTYPE(leafArray)!=OIDOID||ARR_NDIM(tableArray)!=1||ARR_ELEMTYPE(tableArray)!=INT8OID)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("native bind requires one-dimensional oid[] and bigint[]")));
    Datum* leaves=NULL;Datum* ids=NULL;bool* leafNull=NULL;bool* idNull=NULL;int nl=0,nt=0;
    deconstruct_array(leafArray,OIDOID,4,true,'i',&leaves,&leafNull,&nl);
    deconstruct_array(tableArray,INT8OID,8,true,'d',&ids,&idNull,&nt);
    if(nl!=layout.leaf_count||nt!=layout.leaf_count)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("native readonly binding requires exact entries for each real leaf")));
    int64 tableIds[LAKE_NATIVE_RANGE_LEAF_LIMIT]={};
    for(int i=0;i<nl;++i){
        if(leafNull[i]||idNull[i]||DatumGetInt64(ids[i])<0||DatumGetInt64(ids[i])>=LAKE_REFERENCE_LOCAL_ID_START)
            ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid native leaf or permanent table identity")));
        int match=-1;for(int j=0;j<layout.leaf_count;++j)if(DatumGetObjectId(leaves[i])==layout.leaves[j].leaf_partition_oid)match=j;
        if(match<0)ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("supplied native leaf is not a current child of the parent")));
        for(int p=0;p<i;++p)if(leaves[p]==leaves[i]||ids[p]==ids[i])
            ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("native leaves and backing lake tables must be distinct")));
        tableIds[match]=DatumGetInt64(ids[i]);
    }
    pfree(leaves);pfree(ids);pfree(leafNull);pfree(idNull);
    if((Pointer)leafArray!=PG_GETARG_POINTER(2))pfree(leafArray);
    if((Pointer)tableArray!=PG_GETARG_POINTER(3))pfree(tableArray);
    MemoryContext caller=CurrentMemoryContext;
    MemoryContext context=AllocSetContextCreate(caller,"Native readonly partition binding",ALLOCSET_DEFAULT_SIZES);
    LakeNativeBindResources* resource=(LakeNativeBindResources*)palloc0(sizeof(LakeNativeBindResources));
    resource->owner=t_thrd.utils_cxt.CurrentResourceOwner;
    PG_TRY();{
        MemoryContextSwitchTo(context);
        LakeInlineBinding* header=LakeLoadCatalogHeader(server->serverid,GetLatestSnapshot());
        (void)LakeMetadataRelation(header->metadata_schema_oid,"ducklake_snapshot");
        Relation snapshots=heap_open(get_relname_relid("ducklake_snapshot",header->metadata_schema_oid),ShareLock);heap_close(snapshots,NoLock);
        /* Every standard writer must INSERT its snapshot before publishing.
         * SHARE therefore fences commits throughout bounded certification,
         * including external writers using the same authoritative SQL catalog. */
        resource->captured=RegisterSnapshotOnOwner(GetLatestSnapshot(),resource->owner);
        header=LakeLoadCatalogHeader(server->serverid,resource->captured);int64 certified=header->snapshot_id;
        Oid ot=OIDOID;Datum oa=ObjectIdGetDatum(parentId);
        LakeMetaRows* noParent=LakeMetadataQuery("SELECT EXISTS(SELECT 1 FROM og_lake_internal.relation_binding WHERE relation_oid=$1) OR "
            "EXISTS(SELECT 1 FROM og_lake_internal.partition_binding WHERE parent_relid=$1)",1,&ot,&oa,NULL,resource->captured,true,SPI_OK_SELECT);
        LakeMetadataRequireCount(noParent,1,"native parent binding absence");
        if(DatumGetBool(LakeMetadataValue(noParent,0,1,BOOLOID)))
            ereport(ERROR,(errcode(ERRCODE_DUPLICATE_OBJECT),errmsg("native parent already has an adapter binding")));
        LakeInlineBinding* sources[LAKE_NATIVE_RANGE_LEAF_LIMIT]={};char* hashes[LAKE_NATIVE_RANGE_LEAF_LIMIT]={};
        for(int i=0;i<layout.leaf_count;++i){
            Oid types[2]={OIDOID,INT8OID};Datum args[2]={ObjectIdGetDatum(server->serverid),Int64GetDatum(tableIds[i])};
            LakeMetaRows* absent=LakeMetadataQuery("SELECT EXISTS(SELECT 1 FROM og_lake_internal.table_binding WHERE server_oid=$1 AND table_id=$2)",
                2,types,args,NULL,resource->captured,true,SPI_OK_SELECT);
            LakeMetadataRequireCount(absent,1,"native source alias absence");
            if(DatumGetBool(LakeMetadataValue(absent,0,1,BOOLOID)))
                ereport(ERROR,(errcode(ERRCODE_DUPLICATE_OBJECT),errmsg("native lake source already has a host binding or alias")));
            sources[i]=LakeReadNativePartitionSource(server->serverid,tableIds[i],resource->captured,parent);
            if(sources[i]->snapshot_id!=certified)
                ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native sources do not share the certification snapshot")));
            hashes[i]=LakeNativePartitionSourceHash(sources[i]);
            LakeNativeCertifyPayload(parent,&layout,&layout.leaves[i],sources[i],resource->captured);
        }
        resource->verify=RegisterSnapshotOnOwner(GetLatestSnapshot(),resource->owner);
        LakeNativeRangeLayout finalLayout=LakeNativeRangeDescribe(parent);
        for(int i=0;i<layout.leaf_count;++i){
            if(!LakeNativePartitionIdentityEqual(&layout.leaves[i],&finalLayout.leaves[i]))
                ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native leaf changed before binding publication")));
            LakeInlineBinding* now=LakeReadNativePartitionSource(server->serverid,tableIds[i],resource->verify,parent);
            if(now->snapshot_id!=certified||strcmp(hashes[i],LakeNativePartitionSourceHash(now)))
                ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native source changed after complete payload certification")));
        }
        Snapshot own=LakeCreatePushOwnWriteSnapshot(resource->verify);resource->own_pushed=true;
        for(int i=0;i<layout.leaf_count;++i){
            Datum uuid=OidFunctionCall0(F_GEN_RANDOM_UUID);sources[i]->binding_id=DatumGetCString(DirectFunctionCall1(uuid_out,uuid));
            Oid types[8]={UUIDOID,OIDOID,INT8OID,UUIDOID,INT8OID,OIDOID,INT4OID,TEXTOID};
            Datum args[8]={uuid,ObjectIdGetDatum(server->serverid),Int64GetDatum(tableIds[i]),
                DirectFunctionCall1(uuid_in,CStringGetDatum(sources[i]->table_uuid)),Int64GetDatum(sources[i]->schema_version),
                ObjectIdGetDatum(layout.leaves[i].leaf_partition_oid),Int32GetDatum(layout.leaves[i].partition_no),CStringGetTextDatum(hashes[i])};
            LakeMetadataRequireCount(LakeMetadataQuery("INSERT INTO og_lake_internal.table_binding(binding_id,server_oid,table_id,table_uuid,schema_version,object_kind,state,read_only) "
                "VALUES($1,$2,$3,$4,$5,'partition','active',true)",5,types,args,NULL,own,false,SPI_OK_INSERT),1,"native leaf table binding");
            Oid pt[6]={OIDOID,OIDOID,INT4OID,UUIDOID,INT8OID,TEXTOID};
            Datum pa[6]={ObjectIdGetDatum(parentId),args[5],args[6],uuid,Int64GetDatum(certified),args[7]};
            LakeMetadataRequireCount(LakeMetadataQuery("INSERT INTO og_lake_internal.partition_binding(parent_relid,leaf_partition_oid,partition_no,binding_id,certified_snapshot,source_hash) "
                "VALUES($1,$2,$3,$4,$5,$6)",6,pt,pa,NULL,own,false,SPI_OK_INSERT),1,"certified native leaf mapping");
            for(int c=0;c<sources[i]->column_count;++c){Form_pg_attribute attr=&RelationGetDescr(parent)->attrs[c];
                Oid ct[6]={UUIDOID,INT8OID,INT4OID,OIDOID,INT4OID,OIDOID};
                Datum ca[6]={uuid,Int64GetDatum(sources[i]->columns[c].column_id),Int32GetDatum(c+1),ObjectIdGetDatum(attr->atttypid),Int32GetDatum(attr->atttypmod),ObjectIdGetDatum(attr->attcollation)};
                LakeMetadataRequireCount(LakeMetadataQuery("INSERT INTO og_lake_internal.column_binding(binding_id,column_id,attnum,type_oid,typmod,collation_oid,codec_version) "
                    "VALUES($1,$2,$3,$4,$5,$6,1)",6,ct,ca,NULL,own,false,SPI_OK_INSERT),1,"native leaf stable column mapping");
            }
        }
        PopActiveSnapshot();resource->own_pushed=false;
        ObjectAddress object={RelationRelationId,parentId,0};ObjectAddress depends={ForeignServerRelationId,server->serverid,0};
        recordDependencyOn(&object,&depends,DEPENDENCY_NORMAL);
        depends.classId=ExtensionRelationId;depends.objectId=get_extension_oid("og_lake",false);recordDependencyOn(&object,&depends,DEPENDENCY_NORMAL);
        /* Binding changes the meaning of this real parent from local heap to
         * the lake source. Old scaffold statistics must not authorize CBO use.
         * This is the same AX-protected host transaction as the new binding. */
        if(LakeNativePartitionAnalyzeVersionEnabled())LakeNativeParentInvalidateStatistics(parent);
        LakeNativeWriteRootMarker(parent);
        UnregisterSnapshotFromOwner(resource->verify,resource->owner);resource->verify=NULL;
        UnregisterSnapshotFromOwner(resource->captured,resource->owner);resource->captured=NULL;
        MemoryContextSwitchTo(caller);MemoryContextDelete(context);pfree(resource);heap_close(parent,NoLock);
    }PG_CATCH();{
        MemoryContextSwitchTo(caller);ErrorData* error=CopyErrorData();FlushErrorState();
        if(resource->own_pushed)PopActiveSnapshot();
        if(resource->verify)UnregisterSnapshotFromOwner(resource->verify,resource->owner);
        if(resource->captured)UnregisterSnapshotFromOwner(resource->captured,resource->owner);
        MemoryContextDelete(context);pfree(resource);heap_close(parent,NoLock);ReThrowError(error);
    }PG_END_TRY();
    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(og_lake_enable_native_partition_insert);
Datum og_lake_enable_native_partition_insert(PG_FUNCTION_ARGS)
{
    LakeRequireNativePartitionVersion();
    if(!LakeNativePartitionWriteVersionEnabled())
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native partition INSERT is not activated")));
    LakeNativePartitionControl();LakeInlineRequireAdministrator();
    if(PG_ARGISNULL(0))ereport(ERROR,(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),errmsg("native parent cannot be NULL")));
    if(u_sess->attr.attr_common.XactReadOnly||u_sess->utils_cxt.XactIsoLevel!=XACT_READ_COMMITTED)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native INSERT promotion requires writable READ COMMITTED")));
    LakeTxnRejectBindingDdl();LakeLockCatalogRegistration();
    Relation parent=heap_open(PG_GETARG_OID(0),AccessExclusiveLock);
    if(!LakeHasNativePartitionMarker(parent))
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native parent must first be completely certified")));
    if(!pg_class_ownercheck(RelationGetRelid(parent),GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER,ACL_KIND_CLASS,RelationGetRelationName(parent));
    MemoryContext caller=CurrentMemoryContext;
    MemoryContext context=AllocSetContextCreate(caller,"Native parent INSERT promotion",ALLOCSET_DEFAULT_SIZES);
    LakeNativeBindResources* resource=(LakeNativeBindResources*)palloc0(sizeof(LakeNativeBindResources));
    resource->owner=t_thrd.utils_cxt.CurrentResourceOwner;
    PG_TRY(); {
        MemoryContextSwitchTo(context);
        Snapshot initial=GetLatestSnapshot();Oid server=LakeNativePartitionServer(parent,initial);
        LakeInlineBinding* header=LakeLoadCatalogHeader(server,initial);
        Relation snapshots=heap_open(get_relname_relid("ducklake_snapshot",header->metadata_schema_oid),ShareLock);heap_close(snapshots,NoLock);
        resource->captured=RegisterSnapshotOnOwner(GetLatestSnapshot(),resource->owner);
        /* The parent AEL and catalog snapshot SHARE lock fence every legitimate
         * routed/external standard publication while mode is changed. No SDK
         * certification is repeated: exact certified source/head is required. */
        LakeNativePartitionCheckInsertTarget(parent,resource->captured,false);
        LakeNativeRangeLayout layout=LakeNativeRangeDescribe(parent);bool readonly=false;
        for(int i=0;i<layout.leaf_count;++i) {
            LakeNativeLeafReadBinding leaf=LakeLoadNativePartitionLeaf(parent,&layout.leaves[i],resource->captured);
            if(i&&readonly!=leaf.binding->read_only)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native parent write modes differ")));
            readonly=leaf.binding->read_only;
            LakePrepareNativePartitionRowLockBinding(leaf.binding,&layout.leaves[i],resource->captured);
        }
        if(readonly) {
            Snapshot own=LakeCreatePushOwnWriteSnapshot(resource->captured);resource->own_pushed=true;
            Oid type=OIDOID;Datum argument=ObjectIdGetDatum(RelationGetRelid(parent));
            LakeMetadataRequireCount(LakeMetadataQuery("UPDATE og_lake_internal.table_binding SET read_only=false WHERE binding_id IN "
                "(SELECT binding_id FROM og_lake_internal.partition_binding WHERE parent_relid=$1) "
                "AND state='active' AND object_kind='partition' AND read_only",1,&type,&argument,NULL,own,false,SPI_OK_UPDATE),
                layout.leaf_count,"atomic native parent INSERT promotion");
            CommandCounterIncrement();PopActiveSnapshot();resource->own_pushed=false;
            CacheInvalidateRelcacheByRelid(RelationGetRelid(parent));
        }
        UnregisterSnapshotFromOwner(resource->captured,resource->owner);resource->captured=NULL;
        MemoryContextSwitchTo(caller);MemoryContextDelete(context);pfree(resource);heap_close(parent,NoLock);
    } PG_CATCH(); {
        MemoryContextSwitchTo(caller);ErrorData* error=CopyErrorData();FlushErrorState();
        if(resource->own_pushed)PopActiveSnapshot();
        if(resource->captured)UnregisterSnapshotFromOwner(resource->captured,resource->owner);
        MemoryContextDelete(context);pfree(resource);heap_close(parent,NoLock);ReThrowError(error);
    } PG_END_TRY();PG_RETURN_VOID();
}
