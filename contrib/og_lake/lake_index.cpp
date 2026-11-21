/* Derived-index registration. A definition is not a built/usable index. */
#include "postgres.h"
#include "lake_version.h"
#include "lake_index_maintenance.h"
#include "lake_inline_sources.h"
#include "knl/knl_variable.h"
#include "lake_scalar_key.h"
#include "lake_index_private.h"
#include "lake_file_scalar.h"
#include "lake_file_ann_catalog.h"
#include "lake_file_pq.h"
#include "lake_inline_ann.h"
#include "lake_txn_private.h"
#include "access/og_lake.h"
#include "access/heapam.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include <openssl/sha.h>

PG_FUNCTION_INFO_V1(og_lake_create_lake_index);
PG_FUNCTION_INFO_V1(og_lake_drop_lake_index);
PG_FUNCTION_INFO_V1(og_lake_lake_index_status);

Oid LakeIndexCatalog(void)
{
    LakeRequireAccessPolicy();
    if(!LakeHasIndexCatalogVersion() || !OidIsValid(LakeMetadataOwner()))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake index definitions require extension0.14 and a private metadata owner")));
    Oid ns=get_namespace_oid("og_lake_internal",false);
    const char* names[]={"index_definition","index_segment","index_coverage"};
    for(Size i=0;i<lengthof(names);++i) (void)LakeMetadataRelation(ns,names[i]);
    return ns;
}

void LakeIndexDdl(void)
{
    if(u_sess->attr.attr_common.XactReadOnly)
        ereport(ERROR,(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),errmsg("cannot change lake indexes in a read-only transaction")));
    LakeTxnRejectBindingDdl();LakeLockCatalogRegistration();
}

struct LakeIndexInput {char* column;bool ann;int metric;bool bitmap;bool pq;bool has_chunks;uint32 chunks;};
static uint32 LakeDefaultPqChunks(int dim)
{
    static const int targetSubdims[]={4,2,3,5,6,8,1};
    for(int sub:targetSubdims)if(dim>0&&dim%sub==0)return (uint32)(dim/sub);
    return 1;
}
static LakeIndexInput LakeIndexColumn(Jsonb* definition)
{
    if(!JB_ROOT_IS_OBJECT(definition))
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("lake index definition must be a JSON object")));
    JsonbIterator* iterator=JsonbIteratorInit(VARDATA(definition));JsonbValue value;
    int token;uint32 keys=0;LakeIndexInput input={NULL,false,0,false,false,false,1};char* kind=NULL;char* method=NULL;
    while((token=JsonbIteratorNext(&iterator,&value,true))!=WJB_DONE){
        if(token!=WJB_KEY)continue;
        if(value.string.len<=0||value.string.len>16||memchr(value.string.val,'\0',value.string.len)!=NULL)
            ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("unknown lake index option")));
        char* key=pnstrdup(value.string.val,value.string.len);token=JsonbIteratorNext(&iterator,&value,true);uint32 bit=0;
        if(token!=WJB_VALUE)ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid lake index option")));
        if(strcmp(key,"version")==0){bit=1;
            if(value.type!=jbvNumeric||strcmp(DatumGetCString(DirectFunctionCall1(numeric_out,NumericGetDatum(value.numeric))),"1")!=0)
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("lake index definition version must be integer1")));
        }else if(strcmp(key,"enable_pq")==0){bit=32;
            if(value.type!=jbvBool||value.boolean)
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("legacy enable_pq only accepts false; use pq=true and pq_chunks")));
        }else if(strcmp(key,"pq")==0){bit=64;
            if(value.type!=jbvBool)ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("pq must be boolean")));
            input.pq=value.boolean;
        }else if(strcmp(key,"pq_chunks")==0){bit=128;
            if(value.type!=jbvNumeric)ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("pq_chunks must be a positive integer")));
            const char* digits=DatumGetCString(DirectFunctionCall1(numeric_out,NumericGetDatum(value.numeric)));
            uint32 n=0;const char* at=digits;
            for(;*at>='0'&&*at<='9';++at){n=n*10+(*at-'0');if(n>1536)break;}
            if(!*digits||*at||n<1||n>1536)ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("pq_chunks must be an integer in1..1536")));
            input.has_chunks=true;
            input.chunks=n;
        }else{
            if(value.type!=jbvString||value.string.len<=0||value.string.len>=NAMEDATALEN||memchr(value.string.val,'\0',value.string.len)!=NULL)
                ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("lake index options require ordinary strings")));
            char* text=pnstrdup(value.string.val,value.string.len);
            if(strcmp(key,"kind")==0){bit=2;kind=text;}
            else if(strcmp(key,"method")==0){bit=4;method=text;}
            else if(strcmp(key,"column")==0){bit=8;input.column=text;}
            else if(strcmp(key,"metric")==0){bit=16;input.metric=LakeAnnMetric(text);}
            else ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("unknown lake index option \"%s\"",key)));
        }
        if(keys&bit)ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("repeated lake index option")));
        keys|=bit;
    }
    if((keys&15)!=15)ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("lake index requires version, kind, method and column")));
    input.ann=strcmp(kind,"vector")==0;
    if(input.ann){
        if((keys&31)!=31||strcmp(method,"diskann")!=0||((keys&32)&&input.pq)||((keys&128)&&!input.pq))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("vector index requires diskann and metric; pq_chunks requires pq=true without legacy enable_pq")));
        if(input.pq&&!LakePqCompiled())ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("lake CPU PQ support was not compiled from a qualified --pq-build")));
    }else{
        if(keys!=15||strcmp(kind,"scalar")!=0||(strcmp(method,"btree")!=0&&strcmp(method,"bitmap")!=0))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("unsupported lake index kind or method")));
        input.bitmap=strcmp(method,"bitmap")==0;
#ifndef USE_LAKE_BITMAP
        if(input.bitmap)ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("lake bitmap index support was not compiled")));
#endif
    }
    return input;
}

static char* LakeIndexHash(const char* tableUuid,const char* definition)
{
    char* input=psprintf("og_lake_index_definition_v1|%s|%s",tableUuid,definition);
    unsigned char digest[SHA256_DIGEST_LENGTH];
    if(SHA256((const unsigned char*)input,strlen(input),digest)==NULL)
        ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("could not hash native lake index definition")));
    static const char hex[]="0123456789abcdef";char* result=(char*)palloc(SHA256_DIGEST_LENGTH*2+1);
    for(int i=0;i<SHA256_DIGEST_LENGTH;++i) {result[i*2]=hex[digest[i]>>4];result[i*2+1]=hex[digest[i]&15];}
    result[SHA256_DIGEST_LENGTH*2]='\0';return result;
}

void LakeIndexPermission(const char* uuid,Snapshot snapshot)
{
    if(superuser()) return;
    Oid type=UUIDOID;Datum arg=DirectFunctionCall1(uuid_in,CStringGetDatum(uuid));
    LakeMetaRows* rows=LakeMetadataQuery("SELECT r.relation_oid FROM og_lake_internal.index_definition i "
        "JOIN og_lake_internal.relation_binding r USING(binding_id) WHERE i.index_uuid=$1",
        1,&type,&arg,NULL,snapshot,true,SPI_OK_SELECT);
    if(rows->count!=1 || !pg_class_ownercheck(DatumGetObjectId(LakeMetadataValue(rows,0,1,OIDOID)),GetUserId()))
        ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),errmsg("lake index administration requires its host table owner")));
}

static void LakeIndexRetire(const char* uuid,Snapshot snapshot)
{
    Oid type=UUIDOID;Datum arg=DirectFunctionCall1(uuid_in,CStringGetDatum(uuid));
    (void)LakeMetadataQuery("UPDATE og_lake_internal.index_coverage SET active=false WHERE index_uuid=$1 AND active",
        1,&type,&arg,NULL,snapshot,false,SPI_OK_UPDATE);
    (void)LakeMetadataQuery("UPDATE og_lake_internal.index_segment SET state=CASE WHEN state='BUILDING' THEN 'FAILED' "
        "ELSE 'RETIRED' END WHERE index_uuid=$1 AND state IN ('BUILDING','READY')",
        1,&type,&arg,NULL,snapshot,false,SPI_OK_UPDATE);
    (void)LakeMetadataQuery("UPDATE og_lake_internal.index_definition SET state='DROPPING' WHERE index_uuid=$1 AND state='ACTIVE'",
        1,&type,&arg,NULL,snapshot,false,SPI_OK_UPDATE);
}

Datum og_lake_create_lake_index(PG_FUNCTION_ARGS)
{
    if(PG_ARGISNULL(0)||PG_ARGISNULL(1)||PG_ARGISNULL(2))
        ereport(ERROR,(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),errmsg("lake index target, name and definition are required")));
    LakeIndexInput input=LakeIndexColumn(PG_GETARG_JSONB(2));char* column=input.column;const char* name=NameStr(*PG_GETARG_NAME(1));
    if(*name=='\0') ereport(ERROR,(errcode(ERRCODE_INVALID_NAME),errmsg("lake index name cannot be empty")));
    Relation host=heap_open(PG_GETARG_OID(0),AccessShareLock);
    if(!LakeIsNativeForeignRelation(host)) ereport(ERROR,(errcode(ERRCODE_WRONG_OBJECT_TYPE),errmsg("lake index requires a native lake table")));
    if(!pg_class_ownercheck(RelationGetRelid(host),GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER,ACL_KIND_CLASS,RelationGetRelationName(host));
    Oid ns=LakeIndexCatalog();LakeIndexDdl();
    if(LakeTxnFindCreatedTable(RelationGetRelid(host))!=NULL)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("lake index requires a committed permanent table")));
    Relation definitions=heap_open(get_relname_relid("index_definition",ns),ShareRowExclusiveLock);heap_close(definitions,NoLock);
    Snapshot snapshot=RegisterSnapshot(GetLatestSnapshot());
    Datum result=(Datum)0;
    PG_TRY();
    {
        LakeInlineBinding* binding=LakeLoadCommittedInlineBinding(RelationGetRelid(host),snapshot);
        if(!LakeIxSmpExtendedVersionEnabled()){
            LakeRejectBinaryAcceleration(binding,"lake index definitions");
            LakeRejectUuidAcceleration(binding,"lake index definitions");
            LakeRejectJsonAcceleration(binding,"lake index definitions");
            LakeRejectNumericAcceleration(binding,"lake index definitions");
        }
        if(!LakePartitionComboVersionEnabled()||binding->has_added_columns||binding->has_dropped_columns||
           binding->retired_count>0||binding->future_count>0)
            LakeRejectIdentityPartitionIntersection(binding,"lake index definitions");
        if(binding->read_only) ereport(ERROR,(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),errmsg("cannot index a read-only lake table")));
        int position=-1;
        for(int i=0;i<binding->column_count;++i) if(strcmp(binding->columns[i].name,column)==0) position=i;
        if(position<0) ereport(ERROR,(errcode(ERRCODE_UNDEFINED_COLUMN),errmsg("lake index column does not exist")));
        LakeInlineColumn* key=&binding->columns[position];
        if(input.ann?(key->type!=VECTOROID||key->typmod<1||key->typmod>1536):
            !LakeScalarIndexedType(key->type))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("lake index key type or vector dimension is unsupported")));
        Oid collation=RelationGetDescr(host)->attrs[key->output_attnum-1].attcollation;
        char* normalized=psprintf("{\"version\":1,\"kind\":\"scalar\",\"method\":\"%s\",\"column_id\":%lld,\"type_oid\":%u,\"collation_oid\":%u}",
            input.bitmap?"bitmap":"btree",(long long)key->column_id,key->type,collation);
        if(input.ann){
            if(!LakeHasIndexRootVersion())ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("lake DiskANN requires extension0.16")));
            const char* metrics[]={"l2","ip","cosine"};
            normalized=psprintf("{\"version\":1,\"kind\":\"vector\",\"method\":\"diskann\",\"column_id\":%lld,\"type_oid\":%u,\"collation_oid\":%u,\"metric\":\"%s\",\"dimensions\":%d,\"enable_pq\":false}",
                (long long)key->column_id,key->type,collation,metrics[input.metric-1],key->typmod);
            if(input.pq){
                if(!input.has_chunks)input.chunks=LakeDefaultPqChunks(key->typmod);
                if(input.chunks>(uint32)key->typmod||key->typmod%input.chunks)
                    ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("pq_chunks must divide the vector dimension")));
                normalized=psprintf("{\"version\":1,\"kind\":\"vector\",\"method\":\"diskann\",\"column_id\":%lld,\"type_oid\":%u,\"collation_oid\":%u,\"metric\":\"%s\",\"dimensions\":%d,\"pq\":true,\"pq_chunks\":%u}",
                    (long long)key->column_id,key->type,collation,metrics[input.metric-1],key->typmod,input.chunks);
            }
        }
        Oid nt[2]={UUIDOID,NAMEOID};Datum na[2]={DirectFunctionCall1(uuid_in,CStringGetDatum(binding->binding_id)),NameGetDatum(PG_GETARG_NAME(1))};
        LakeMetaRows* existing=LakeMetadataQuery("SELECT index_uuid::text FROM og_lake_internal.index_definition "
            "WHERE binding_id=$1 AND index_name=$2 AND state='ACTIVE'",2,nt,na,NULL,snapshot,true,SPI_OK_SELECT);
        if(existing->count!=0) ereport(ERROR,(errcode(ERRCODE_DUPLICATE_OBJECT),errmsg("lake index name already exists for this table")));
        Datum uuid=OidFunctionCall0(F_GEN_RANDOM_UUID);
        Oid types[13]={UUIDOID,UUIDOID,NAMEOID,OIDOID,INT8OID,UUIDOID,INT8OID,OIDOID,OIDOID,INT8OID,TEXTOID,JSONBOID,INT8OID};
        Datum args[13]={uuid,na[0],na[1],ObjectIdGetDatum(binding->server_oid),Int64GetDatum(LakePermanentTableId(binding)),
            DirectFunctionCall1(uuid_in,CStringGetDatum(binding->table_uuid)),Int64GetDatum(key->column_id),ObjectIdGetDatum(key->type),
            ObjectIdGetDatum(collation),Int64GetDatum(1),CStringGetTextDatum(LakeIndexHash(binding->table_uuid,normalized)),
            DirectFunctionCall1(jsonb_in,CStringGetDatum(normalized)),Int64GetDatum(binding->snapshot_id)};
        LakeMetaRows* written=LakeMetadataQuery("INSERT INTO og_lake_internal.index_definition "
            "(index_uuid,binding_id,index_name,server_oid,table_id,table_uuid,column_id,type_oid,collation_oid,definition_generation,"
            "definition_hash,definition,registered_snapshot,state) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'ACTIVE')",
            13,types,args,NULL,snapshot,false,SPI_OK_INSERT);
        LakeMetadataRequireCount(written,1,"index definitions");result=datumCopy(uuid,false,16);
        LakeInvalidateRelationSafe(RelationGetRelid(host));
        UnregisterSnapshot(snapshot);
    }
    PG_CATCH();{UnregisterSnapshot(snapshot);PG_RE_THROW();}PG_END_TRY();
    heap_close(host,NoLock);PG_RETURN_DATUM(result);
}

Datum og_lake_drop_lake_index(PG_FUNCTION_ARGS)
{
    if(PG_ARGISNULL(0)) ereport(ERROR,(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),errmsg("lake index UUID is required")));
    (void)LakeIndexCatalog();LakeIndexDdl();Snapshot snapshot=RegisterSnapshot(GetLatestSnapshot());
    PG_TRY();
    {
        char* uuid=DatumGetCString(DirectFunctionCall1(uuid_out,PG_GETARG_DATUM(0)));LakeIndexPermission(uuid,snapshot);
        Oid type=UUIDOID;Datum arg=PG_GETARG_DATUM(0);
        LakeMetaRows* rows=LakeMetadataQuery("SELECT d.index_uuid::text, r.relation_oid FROM og_lake_internal.index_definition d "
            "LEFT JOIN og_lake_internal.relation_binding r USING(binding_id) WHERE d.index_uuid=$1 FOR UPDATE OF d",
            1,&type,&arg,NULL,snapshot,false,SPI_OK_SELECT);
        if(rows->count==0) ereport(ERROR,(errcode(ERRCODE_UNDEFINED_OBJECT),errmsg("lake index does not exist")));
        LakeMetadataRequireCount(rows,1,"locked index definitions");
        bool relMissing=false;
        Datum relDatum=LakeMetadataValue(rows,0,2,OIDOID,true,&relMissing);
        Oid hostOid=relMissing?InvalidOid:DatumGetObjectId(relDatum);
        LakeIndexRetire(uuid,snapshot);
        LakeIndexCursorReset(uuid);
        LakeInvalidateRelationSafe(hostOid);
        UnregisterSnapshot(snapshot);
    }
    PG_CATCH();{UnregisterSnapshot(snapshot);PG_RE_THROW();}PG_END_TRY();
    PG_RETURN_VOID();
}

Datum og_lake_lake_index_status(PG_FUNCTION_ARGS)
{
    if(PG_ARGISNULL(0)) ereport(ERROR,(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),errmsg("lake index UUID is required")));
    (void)LakeIndexCatalog();Snapshot snapshot=RegisterSnapshot(GetLatestSnapshot());char* result=NULL;
    PG_TRY();
    {
        char* uuid=DatumGetCString(DirectFunctionCall1(uuid_out,PG_GETARG_DATUM(0)));LakeIndexPermission(uuid,snapshot);
        Oid type=UUIDOID;Datum arg=PG_GETARG_DATUM(0);
        bool inlineVersion=LakeHasInlineIndexVersion();
        /* READY counts obey the same method/type/algorithm/format contract as
         * readers. A sorted artifact cannot qualify a bitmap definition, or
         * vice versa; obsolete definition generations do not count either. */
        LakeMetaRows* rows=LakeMetadataQuery(psprintf("SELECT state::text,(SELECT count(*)::bigint FROM og_lake_internal.index_segment s "
            "WHERE s.index_uuid=d.index_uuid AND s.definition_generation=d.definition_generation AND s.state='READY' AND ("
            "(d.definition->>'kind'='scalar' AND (d.type_oid IN (%u,%u,%u,%u) OR (d.type_oid=%u AND %s)) "
            "AND ((d.definition->>'method'='btree' AND s.format_version=1) OR "
            "(d.definition->>'method'='bitmap' AND s.format_version=2)) "
            "AND s.algorithm_version=CASE WHEN d.type_oid IN (%u,%u) THEN 2 ELSE 1 END) OR "
            "(d.definition->>'kind'='vector' AND d.definition->>'method'='diskann' AND d.type_oid=%u "
            "AND (s.format_version=1 OR (s.format_version=2 AND d.definition->>'pq'='true')) AND s.algorithm_version=2))),%s,"
            "COALESCE(d.definition->>'kind',''),COALESCE(d.definition->>'method',''),d.type_oid "
            "FROM og_lake_internal.index_definition d WHERE index_uuid=$1",
            INT4OID,INT8OID,FLOAT4OID,FLOAT8OID,INT2OID,
            LakeInt2ScalarIndexVersionEnabled()?"true":"false",FLOAT4OID,FLOAT8OID,VECTOROID,
            inlineVersion?"inline_index_oid":"NULL::oid"),1,&type,&arg,NULL,snapshot,true,SPI_OK_SELECT);
        if(rows->count==0) ereport(ERROR,(errcode(ERRCODE_UNDEFINED_OBJECT),errmsg("lake index does not exist")));
        LakeMetadataRequireCount(rows,1,"index status definitions");const char* state=LakeMetadataText(rows,0,1);
        if(strcmp(state,"ACTIVE")!=0 && strcmp(state,"DROPPING")!=0)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake index state")));
        if(inlineVersion) {
            bool missing=false;Datum stored=LakeMetadataValue(rows,0,3,OIDOID,true,&missing);
            const char* kind=LakeMetadataText(rows,0,4);const char* method=LakeMetadataText(rows,0,5);
            Oid definitionType=DatumGetObjectId(LakeMetadataValue(rows,0,6,OIDOID));
            bool usable=false,ann=strcmp(kind,"vector")==0&&strcmp(method,"diskann")==0&&definitionType==VECTOROID;
            bool bitmap=strcmp(kind,"scalar")==0&&strcmp(method,"bitmap")==0;
            bool scalar=strcmp(kind,"scalar")==0&&LakeScalarIndexedType(definitionType)&&strcmp(method,"btree")==0;
#ifdef USE_LAKE_BITMAP
            scalar=scalar||(bitmap&&LakeScalarIndexedType(definitionType));
#endif
            bool protocol=ann||scalar;int fileCount=0,coveredFiles=0;bool fileVersion=LakeHasIndexRootVersion();
            if(protocol&&(!missing||fileVersion) && strcmp(state,"ACTIVE")==0) {
                LakeMetaRows* target=LakeMetadataQuery("SELECT r.relation_oid,d.column_id,d.definition->>'metric' FROM og_lake_internal.index_definition d "
                    "JOIN og_lake_internal.relation_binding r USING(binding_id) WHERE d.index_uuid=$1",
                    1,&type,&arg,NULL,snapshot,true,SPI_OK_SELECT);
                LakeMetadataRequireCount(target,1,"inline index target");
                LakeInlineBinding* binding=LakeLoadCommittedInlineBinding(DatumGetObjectId(LakeMetadataValue(target,0,1,OIDOID)),snapshot);
                fileCount=binding->file_count;
                int64 columnId=DatumGetInt64(LakeMetadataValue(target,0,2,INT8OID));
                for(int i=0;i<binding->column_count;++i)if(binding->columns[i].column_id==columnId&&binding->columns[i].type==definitionType){
                    if(!missing)usable=ann?
                        LakeInlineAnnAvailable(binding,uuid,DatumGetObjectId(stored),i+1,LakeAnnMetric(LakeMetadataText(target,0,3)),snapshot):
                        LakeInlineIndexAvailable(binding,uuid,DatumGetObjectId(stored),i+1,snapshot);
                    if(fileVersion){
                        double physicalRows,fileBytes,rowGroups;
                        if(ann)LakeAnnFileCoverageEstimate(binding,uuid,i+1,snapshot,&physicalRows,&fileBytes,&rowGroups,&coveredFiles);
                        else LakeScalarFileCoverageEstimate(binding,uuid,i+1,snapshot,&physicalRows,&fileBytes,&rowGroups,&coveredFiles);
                    }
                }
            }
            const char* reason=strcmp(state,"ACTIVE")!=0?"index_definition_retired":!protocol?"index_method_or_type_unavailable":missing?"inline_index_not_built":
                usable?(ann?"inline_diskann_candidates_with_complete_file_private_fallback":"inline_btree_with_complete_scan_fallback"):
                ann?"ann_disabled_or_native_index_snapshot_horizon":"native_index_snapshot_horizon";
            bool fileReady=coveredFiles>0;
            if(fileReady)reason=ann?(u_sess->attr.attr_sql.enable_lake_ann?
                "ann_file_coverage_requires_runtime_validation":"ann_disabled"):
                bitmap?"bitmap_file_coverage_requires_runtime_validation":"scalar_file_coverage_requires_runtime_validation";
            char* fileInfo=fileVersion?psprintf(",\"file_covered_inputs\":%d,\"file_uncovered_inputs\":%d,\"file_runtime_validation\":true",
                coveredFiles,Max(0,fileCount-coveredFiles)):pstrdup("");
            if(ann){
                LakeMetaRows* pqStatus=LakeMetadataQuery("SELECT COALESCE((d.definition->>'pq')::boolean,false),"
                    "(SELECT count(*)::bigint FROM og_lake_internal.index_segment s WHERE s.index_uuid=d.index_uuid "
                    "AND s.definition_generation=d.definition_generation AND s.state='READY' AND s.algorithm_version=2 "
                    "AND s.format_version=2 AND d.definition->>'pq'='true') FROM og_lake_internal.index_definition d WHERE d.index_uuid=$1",
                    1,&type,&arg,NULL,snapshot,true,SPI_OK_SELECT);
                LakeMetadataRequireCount(pqStatus,1,"PQ index status");
                fileInfo=psprintf("%s,\"pq_requested\":%s,\"pq_compiled\":%s,\"pq_ready_segments\":%lld,\"pq_runtime_validation\":true",fileInfo,
                    DatumGetBool(LakeMetadataValue(pqStatus,0,1,BOOLOID))?"true":"false",LakePqCompiled()?"true":"false",
                    (long long)DatumGetInt64(LakeMetadataValue(pqStatus,0,2,INT8OID)));
            }
            result=psprintf("{\"index_uuid\":\"%s\",\"state\":\"%s\",\"ready_segments\":%lld,\"inline_index_ready\":%s,"
                "\"file_index_ready\":%s,\"usable\":%s,\"reason\":\"%s\"%s}",uuid,state,
                (long long)DatumGetInt64(LakeMetadataValue(rows,0,2,INT8OID)),missing?"false":"true",fileReady?"true":"false",
                (usable||(fileReady&&(!ann||u_sess->attr.attr_sql.enable_lake_ann)))?"true":"false",reason,fileInfo);
        } else result=psprintf("{\"index_uuid\":\"%s\",\"state\":\"%s\",\"ready_segments\":%lld,\"usable\":false,\"reason\":\"index_backend_not_implemented\"}",
            uuid,state,(long long)DatumGetInt64(LakeMetadataValue(rows,0,2,INT8OID)));
        UnregisterSnapshot(snapshot);
    }
    PG_CATCH();{UnregisterSnapshot(snapshot);PG_RE_THROW();}PG_END_TRY();
    PG_RETURN_TEXT_P(cstring_to_text(result));
}

void LakeIndexRetireRelation(Oid relationOid,Snapshot snapshot)
{
    if(!LakeHasIndexCatalogVersion()) return;
    (void)LakeIndexCatalog();Oid type=OIDOID;Datum arg=ObjectIdGetDatum(relationOid);
    LakeMetaRows* rows=LakeMetadataQuery("SELECT i.index_uuid::text FROM og_lake_internal.index_definition i "
        "JOIN og_lake_internal.relation_binding r USING(binding_id) WHERE r.relation_oid=$1 ORDER BY i.index_uuid FOR UPDATE OF i",
        1,&type,&arg,NULL,snapshot,false,SPI_OK_SELECT);
    for(uint32 i=0;i<rows->count;++i) LakeIndexRetire(LakeMetadataText(rows,i,1),snapshot);
}

/* Exact physical source retirement; definitions/inline indexes and other file
 * coverage survive. Caller acquired registration SRE before its snapshot PK.
 * Inactive BUILDING pins stay intact until their owner revalidates/aborts. */
void LakeIndexRetireFile(const LakeInlineBinding* b,int64 fileId,Snapshot snapshot)
{
    if(!LakeHasIndexCatalogVersion())return;
    (void)LakeIndexCatalog();
    Oid types[5]={UUIDOID,OIDOID,INT8OID,UUIDOID,INT8OID};
    Datum args[5]={DirectFunctionCall1(uuid_in,CStringGetDatum(b->binding_id)),ObjectIdGetDatum(b->server_oid),
        Int64GetDatum(LakePermanentTableId(b)),DirectFunctionCall1(uuid_in,CStringGetDatum(b->table_uuid)),Int64GetDatum(fileId)};
    LakeMetaRows* definitions=LakeMetadataQuery("SELECT index_uuid FROM og_lake_internal.index_definition "
        "WHERE binding_id=$1 AND server_oid=$2 AND table_id=$3 AND table_uuid=$4 ORDER BY index_uuid LIMIT 65 FOR UPDATE",
        4,types,args,NULL,snapshot,false,SPI_OK_SELECT);
    if(definitions->count>64)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("rewrite exceeds the64-definition retirement budget")));
    for(uint32 i=0;i<definitions->count;++i){
        Oid ct[2]={UUIDOID,INT8OID};Datum ca[2]={LakeMetadataValue(definitions,i,1,UUIDOID),args[4]};
        (void)LakeMetadataQuery("UPDATE og_lake_internal.index_coverage SET active=false "
            "WHERE index_uuid=$1 AND source_file_id=$2 AND active",2,ct,ca,NULL,snapshot,false,SPI_OK_UPDATE);
        CommandCounterIncrement();
        (void)LakeMetadataQuery("UPDATE og_lake_internal.index_segment s SET state='RETIRED' "
            "WHERE s.index_uuid=$1 AND s.state='READY' AND NOT EXISTS(SELECT 1 FROM og_lake_internal.index_coverage c "
            "WHERE c.segment_uuid=s.segment_uuid AND c.active)",1,ct,ca,NULL,GetLatestSnapshot(),false,SPI_OK_UPDATE);
    }
}
