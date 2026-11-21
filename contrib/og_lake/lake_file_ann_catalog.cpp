/* Adapter algorithm_version2 uses native double-norm eligibility. It does not
 * change the native DiskANN algorithm or portable graph format1.
 * Native DiskANN artifacts use the same token/coverage publication protocol
 * as scalar artifacts. They never replace authoritative data or row identity. */
#include "postgres.h"
#include "lake_cost.h"
#include "knl/knl_variable.h"
#include "lake_file_ann_catalog.h"
#include "lake_ann_build.h"
#include "lake_file_ann.h"
#include "lake_file_pq.h"
#include "lake_ann_rescan.h"
#include "lake_file_reader.h"
#include "lake_file_sdk.h"
#include "lake_index_private.h"
#include "lake_index_maintenance.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/array.h"
#include "utils/snapmgr.h"
#include "utils/fmgroids.h"
#include <openssl/sha.h>

static char* LakeAnnHash(const char* input)
{
    unsigned char digest[SHA256_DIGEST_LENGTH];
    if(SHA256((const unsigned char*)input,strlen(input),digest)==NULL)
        ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("could not hash lake ANN identity")));
    char* result=(char*)palloc(65);static const char hex[]="0123456789abcdef";
    for(int i=0;i<32;++i){result[i*2]=hex[digest[i]>>4];result[i*2+1]=hex[digest[i]&15];}
    result[64]='\0';return result;
}

static char* LakeAnnSchemaHash(const LakeInlineBinding* binding)
{
    StringInfoData input;initStringInfo(&input);
    appendStringInfo(&input,"og_lake_ann_schema_v1|%d",binding->column_count);
    for(int c=0;c<binding->column_count;++c)
        appendStringInfo(&input,"|%lld:%u:%d:%lld",(long long)binding->columns[c].column_id,binding->columns[c].type,binding->columns[c].typmod,(long long)binding->columns[c].element_column_id);
    /* Stable field IDs and types survive table/column renames. */
    return LakeAnnHash(input.data);
}

static char* LakeAnnSourceHash(const LakeDataFile* file,const char* version,const char* schema)
{
    return LakeAnnHash(psprintf("og_lake_ann_source_v1|%lld|%lld|%lld|%lld|%d|%lld|%lld|%lld|%zu:%s|%zu:%s|%s",
        (long long)file->file_id,(long long)file->record_count,(long long)file->file_bytes,(long long)file->begin_snapshot,
        file->row_id_start_null?1:0,(long long)file->row_id_start,(long long)file->partial_max,(long long)file->row_group_count,
        strlen(file->path),file->path,strlen(version),version,schema));
}

static char* LakeAnnContext(const char* uuid,int64 generation,const char* definition,
    const char* segment,const char* token,const char* source,const char* schema)
{
    return LakeAnnHash(psprintf("og_lake_ann_context_v2|%s|%lld|%s|%s|%s|%s|%s",
        uuid,(long long)generation,definition,segment,token,source,schema));
}

struct LakeAnnVerify {OgLakeAnnReader* reader;};
static bool LakeAnnReadyVerified(const OgLakeParquetSdkApi* api,int dimension,int metric,
    const char* context,const char* manifest,const char* directory,int format,int64 fileId,const char* source,const char* schema,
    int* diagnosticStatus=NULL)
{
    if(diagnosticStatus)*diagnosticStatus=OG_LAKE_SDK_INVALID;
    LakePqDescriptor descriptor={};if(!LakePqParseDescriptor(manifest,format,&descriptor))return false;
    LakeAnnVerify* state=(LakeAnnVerify*)palloc0(sizeof(LakeAnnVerify));
    OgLakeSdkLimits limits=LakeFileLimits();OgLakeSdkError error;OgLakeAnnSpec spec={};spec.struct_size=sizeof(spec);
    const char* data=psprintf("%s/%s",directory,OG_LAKE_ANN_NODES_FILE);
    const char* lookup=psprintf("%s/%s",directory,OG_LAKE_ANN_LOOKUP_FILE);
    int volatile code=OG_LAKE_SDK_OK;
    PG_TRY();{
        HOLD_INTERRUPTS();code=og_lake_ann_inspect(api,&limits,dimension,metric,context,data,lookup,&spec,&error);
        RESUME_INTERRUPTS();CHECK_FOR_INTERRUPTS();
        if(code==OG_LAKE_SDK_OK){
            HOLD_INTERRUPTS();code=og_lake_ann_reader_open(api,&limits,&spec,descriptor.graph_root,data,lookup,&state->reader,&error);
            RESUME_INTERRUPTS();CHECK_FOR_INTERRUPTS();}
        if(code==OG_LAKE_SDK_OK){HOLD_INTERRUPTS();code=og_lake_ann_verify(state->reader,&error);RESUME_INTERRUPTS();}
        if(code==OG_LAKE_SDK_OK&&format==2){int pqStatus=OG_LAKE_SDK_INVALID;
            if(!LakePqVerify(api,&spec,&descriptor,fileId,source,schema,directory,&pqStatus))
                code=diagnosticStatus?pqStatus:OG_LAKE_SDK_INVALID;}
        HOLD_INTERRUPTS();og_lake_ann_reader_close(&state->reader);RESUME_INTERRUPTS();CHECK_FOR_INTERRUPTS();
        if(diagnosticStatus)*diagnosticStatus=code;
        if(code!=OG_LAKE_SDK_OK&&code!=OG_LAKE_SDK_IO&&code!=OG_LAKE_SDK_INVALID&&
            !(diagnosticStatus&&(code==OG_LAKE_SDK_MEMORY||code==OG_LAKE_SDK_UNSUPPORTED||code==OG_LAKE_SDK_STATE)))
            LakeFileCheckStatus(code,&error);
    }PG_CATCH();{HOLD_INTERRUPTS();og_lake_ann_reader_close(&state->reader);RESUME_INTERRUPTS();PG_RE_THROW();}PG_END_TRY();
    pfree(state);return code==OG_LAKE_SDK_OK;
}

/* Reopen the finished same-root graph. Every training/encoding node is read
 * through its authenticated page reader, so codes use exact dense node order. */
static LakePqBuilt LakeAnnBuildPq(const OgLakeParquetSdkApi* api,int dimension,int metric,const char* context,
    const char* graphRoot,const char* directory,int64 fileId,const char* source,const char* schema,uint32 chunks)
{
    auto* state=(LakeAnnVerify*)palloc0(sizeof(LakeAnnVerify));LakePqBuilt result={};
    OgLakeSdkLimits limits=LakeFileLimits();OgLakeSdkError error;OgLakeAnnSpec spec={};spec.struct_size=sizeof(spec);
    const char* nodes=psprintf("%s/%s",directory,OG_LAKE_ANN_NODES_FILE);
    const char* lookup=psprintf("%s/%s",directory,OG_LAKE_ANN_LOOKUP_FILE);
    PG_TRY();{
        HOLD_INTERRUPTS();int code=og_lake_ann_inspect(api,&limits,dimension,metric,context,nodes,lookup,&spec,&error);RESUME_INTERRUPTS();LakeFileCheckStatus(code,&error);CHECK_FOR_INTERRUPTS();
        HOLD_INTERRUPTS();code=og_lake_ann_reader_open(api,&limits,&spec,graphRoot,nodes,lookup,&state->reader,&error);RESUME_INTERRUPTS();LakeFileCheckStatus(code,&error);CHECK_FOR_INTERRUPTS();
        result=LakePqBuild(state->reader,fileId,source,schema,graphRoot,directory,chunks);
        HOLD_INTERRUPTS();og_lake_ann_reader_close(&state->reader);RESUME_INTERRUPTS();CHECK_FOR_INTERRUPTS();
    }PG_CATCH();{HOLD_INTERRUPTS();og_lake_ann_reader_close(&state->reader);RESUME_INTERRUPTS();PG_RE_THROW();}PG_END_TRY();
    pfree(state);return result;
}

int LakeBuildAnnFileIndexes(LakeInlineBinding* binding,const char* uuid,int column,int metric,
    int64 generation,const char* definitionHash,Snapshot snapshot,bool pq,uint32 pqChunks)
{
    if(pq&&!LakePqCompiled())ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("lake CPU PQ build support was not compiled")));
    if(pq&&(!pqChunks||pqChunks>(uint32)binding->columns[column].typmod||binding->columns[column].typmod%pqChunks))
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("pq_chunks must divide the vector dimension")));
    if(!LakeHasIndexRootVersion()||binding->file_count==0)return 0;
    Oid rootType=OIDOID;Datum rootArg=ObjectIdGetDatum(binding->server_oid);
    LakeMetaRows* roots=LakeMetadataQuery("SELECT index_root::text,xmin::text FROM og_lake_internal.catalog_binding WHERE server_oid=$1",
        1,&rootType,&rootArg,NULL,snapshot,true,SPI_OK_SELECT);
    LakeMetadataRequireCount(roots,1,"ANN index store");bool absent=false;
    Datum rootValue=LakeMetadataValue(roots,0,1,TEXTOID,true,&absent);if(absent)return -1;
    const char* root=TextDatumGetCString(rootValue);
    TransactionId configured=DatumGetTransactionId(DirectFunctionCall1(xidin,CStringGetDatum(LakeMetadataText(roots,0,2))));
    if(TransactionIdIsCurrentTransactionId(configured))
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("commit index_root configuration before writing index files")));
    /* Registration precedes all catalog reads and is held through this first
     * synchronous implementation. It conflicts with administrative DDL, not
     * ordinary SELECT/INSERT/UPDATE/DELETE on existing tables. */
    LakeAcquireCatalogGuard(binding->server_oid);
    const OgLakeParquetSdkApi* api=LakeFileProvider();
    if(!(api->capabilities&OG_LAKE_SDK_OBJECT_VERSION))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("file index builds require immutable source object identities")));
    int candidates=0;for(LakeFileCandidate* c=*LakeHostFileCandidatesAddress();c;c=c->next)++candidates;
    int capacity=Max(0,(16-candidates)/(pq?5:2)),built=0;
    if(capacity==0)ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("commit existing file candidates before building more index segments")));
    char* schema=LakeAnnSchemaHash(binding);
    for(int f=0;f<binding->file_count&&built<capacity;++f){
        CHECK_FOR_INTERRUPTS();LakeDataFile* file=&binding->files[f];
        char* version=LakeFileObjectVersion(binding,f);
        if(version==NULL)ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("ANN source has no immutable-object identity")));
        char* source=LakeAnnSourceHash(file,version,schema);
        Oid knownTypes[5]={UUIDOID,INT8OID,INT8OID,TEXTOID,BOOLOID};Datum knownArgs[5]={DirectFunctionCall1(uuid_in,CStringGetDatum(uuid)),
            Int64GetDatum(generation),Int64GetDatum(file->file_id),CStringGetTextDatum(source),BoolGetDatum(pq)};
        LakeMetaRows* known=LakeMetadataQuery("SELECT s.segment_uuid::text,s.build_token::text,s.artifact_root_uri::text,"
            "s.manifest_hash::text,c.schema_mapping_hash::text,c.source_checksum::text,c.physical_row_count,"
            "c.source_file_bytes,c.source_path::text,s.format_version FROM og_lake_internal.index_coverage c JOIN "
            "og_lake_internal.index_segment s USING(segment_uuid,index_uuid,definition_generation) WHERE c.index_uuid=$1 "
            "AND c.definition_generation=$2 AND c.source_file_id=$3 AND c.active AND s.state='READY' "
            "AND s.source_manifest_hash=$4 AND s.algorithm_version=2 AND (s.format_version=1 OR (s.format_version=2 AND $5))",
            5,knownTypes,knownArgs,NULL,snapshot,true,SPI_OK_SELECT);
        if(known->count>1)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("multiple live ANN coverages for one physical source")));
        if(known->count==1){
            const char* segment=LakeMetadataText(known,0,1);
            char* directory=psprintf("%s%s",root,segment);
            if(strcmp(LakeMetadataText(known,0,3),directory)!=0||
                strcmp(LakeMetadataText(known,0,5),schema)!=0||
                strcmp(LakeMetadataText(known,0,6),psprintf("object-version-v1:%s",version))!=0||
                DatumGetInt64(LakeMetadataValue(known,0,7,INT8OID))!=file->record_count||
                DatumGetInt64(LakeMetadataValue(known,0,8,INT8OID))!=file->file_bytes||
                strcmp(LakeMetadataText(known,0,9),file->path)!=0)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("existing ANN coverage identity changed")));
            char* context=LakeAnnContext(uuid,generation,definitionHash,segment,
                LakeMetadataText(known,0,2),source,schema);
            if(LakeAnnReadyVerified(api,binding->columns[column].typmod,metric,context,
                LakeMetadataText(known,0,4),directory,DatumGetInt32(LakeMetadataValue(known,0,10,INT4OID)),file->file_id,source,schema))continue;
        }
        Datum segment=OidFunctionCall0(F_GEN_RANDOM_UUID),token=OidFunctionCall0(F_GEN_RANDOM_UUID);
        const char* segmentText=DatumGetCString(DirectFunctionCall1(uuid_out,segment));
        const char* tokenText=DatumGetCString(DirectFunctionCall1(uuid_out,token));
        char* directory=psprintf("%s%s",root,segmentText);
        char* context=LakeAnnContext(uuid,generation,definitionHash,segmentText,tokenText,source,schema);
        Oid types[8]={UUIDOID,UUIDOID,INT8OID,UUIDOID,INT8OID,TEXTOID,TEXTOID,TEXTOID};
        Datum args[8]={segment,knownArgs[0],Int64GetDatum(generation),token,Int64GetDatum(binding->snapshot_id),
            CStringGetTextDatum(source),CStringGetTextDatum(directory),(Datum)0};
        LakeMetaRows* added=LakeMetadataQuery("INSERT INTO og_lake_internal.index_segment(segment_uuid,index_uuid,definition_generation,"
            "build_token,build_snapshot,source_manifest_hash,algorithm_version,format_version,artifact_root_uri,state) "
            "VALUES($1,$2,$3,$4,$5,$6,2,1,$7,'BUILDING')",7,types,args,NULL,snapshot,false,SPI_OK_INSERT);
        LakeMetadataRequireCount(added,1,"ANN BUILDING segment");
        LakeEnsureLocalDirectory(directory);
        LakeFileCandidate* data=NULL;LakeFileCandidate* lookup=NULL;OgLakeAnnWritten written={};
        LakeBuildAnnFileSegment(binding,f,column,metric,version,context,directory,&data,&lookup,&written);
        LakePqBuilt quantized={};if(pq)quantized=LakeAnnBuildPq(api,binding->columns[column].typmod,metric,context,
            written.manifest_hash,directory,file->file_id,source,schema,pqChunks);
        char* finalVersion=LakeFileObjectVersion(binding,f);
        if(!finalVersion||strcmp(finalVersion,version)!=0)ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("lake ANN source changed during PQ sidecar build")));
        args[7]=CStringGetTextDatum(quantized.enabled?quantized.descriptor:written.manifest_hash);
        LakeMetaRows* ready=LakeMetadataQuery(psprintf("UPDATE og_lake_internal.index_segment SET state='READY',manifest_hash=$8,format_version=%d "
            "WHERE segment_uuid=$1 AND index_uuid=$2 AND definition_generation=$3 AND build_token=$4 AND build_snapshot=$5 "
            "AND source_manifest_hash=$6 AND artifact_root_uri=$7 AND state='BUILDING'",quantized.enabled?2:1),8,types,args,NULL,snapshot,false,SPI_OK_UPDATE);
        LakeMetadataRequireCount(ready,1,"ANN READY token publication");
        (void)LakeMetadataQuery("UPDATE og_lake_internal.index_coverage SET active=false WHERE index_uuid=$1 "
            "AND definition_generation=$2 AND source_file_id=$3 AND active",3,knownTypes,knownArgs,NULL,snapshot,false,SPI_OK_UPDATE);
        Oid coverageTypes[10]={UUIDOID,INT8OID,UUIDOID,INT8OID,TEXTOID,TEXTOID,INT8OID,BOOLOID,INT8OID,TEXTOID};
        Datum coverageArgs[10]={segment,Int64GetDatum(file->file_id),knownArgs[0],Int64GetDatum(generation),
            CStringGetTextDatum(psprintf("object-version-v1:%s",version)),CStringGetTextDatum(schema),Int64GetDatum(file->record_count),
            BoolGetDatum(true),Int64GetDatum(file->file_bytes),CStringGetTextDatum(file->path)};
        LakeMetaRows* covered=LakeMetadataQuery("INSERT INTO og_lake_internal.index_coverage(segment_uuid,source_file_id,index_uuid,"
            "definition_generation,source_checksum,schema_mapping_hash,physical_row_count,active,source_file_bytes,source_path) "
            "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",10,coverageTypes,coverageArgs,NULL,snapshot,false,SPI_OK_INSERT);
        LakeMetadataRequireCount(covered,1,"ANN physical coverage publication");
        (void)LakeMetadataQuery("UPDATE og_lake_internal.index_segment s SET state='RETIRED' WHERE s.index_uuid=$1 AND s.state='READY' "
            "AND NOT EXISTS(SELECT 1 FROM og_lake_internal.index_coverage c WHERE c.segment_uuid=s.segment_uuid AND c.active)",
            1,knownTypes,knownArgs,NULL,snapshot,false,SPI_OK_UPDATE);
        LakeFileCandidatePublish(data);LakeFileCandidatePublish(lookup);
        if(quantized.enabled)for(int i=0;i<3;++i)LakeFileCandidatePublish(quantized.files[i]);++built;
    }
    return built;
}


/* Query results live in the caller's native candidate context. No SDK object
 * or C++ graph cache survives this preparation phase. */
struct LakeAnnFilePositions {uint32 count;OgLakeAnnAddress* rows;bool covered;bool pq_used;};
struct LakeFileAnnSelection {int files;LakeAnnFilePositions* positions;OgLakeAnnReader* reader;};

bool LakeAnnFileCovered(const LakeFileAnnSelection* selection,int file)
{return selection&&file>=0&&file<selection->files&&selection->positions[file].covered;}
bool LakeAnnFilePqUsed(const LakeFileAnnSelection* selection,int file)
{return LakeAnnFileCovered(selection,file)&&selection->positions[file].pq_used;}
uint32 LakeAnnFilePositionCount(const LakeFileAnnSelection* selection,int file)
{return LakeAnnFileCovered(selection,file)?selection->positions[file].count:0;}
const OgLakeAnnAddress* LakeAnnFilePosition(const LakeFileAnnSelection* selection,int file,uint32 index)
{
    if(!LakeAnnFileCovered(selection,file)||index>=selection->positions[file].count)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid lake ANN candidate position")));
    return &selection->positions[file].rows[index];
}
static int LakeAnnCompareAddress(const void* left,const void* right)
{
    const auto* a=(const OgLakeAnnAddress*)left;const auto* b=(const OgLakeAnnAddress*)right;
    return a->row_position<b->row_position?-1:a->row_position>b->row_position?1:0;
}

/* Consumer-only metadata preparation. Shared images contain immutable POD and
 * copied strings, never SPI tuples, SDK handles, ExprState or a transaction. */
LakeAnnFileSources* LakePrepareAnnFileSources(LakeInlineBinding* binding,const char* uuid,
    AttrNumber column,int metric,char* const* versions,MemoryContext owner,Size maxBytes,bool rawOnly)
{
    if(!binding||column<1||column>binding->column_count||metric<1||metric>3)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid lake ANN metadata preparation")));
    const char* metrics[]={"l2","ip","cosine"};
    MemoryContext previous=CurrentMemoryContext;
    MemoryContextSwitchTo(owner);
    auto* sources=(LakeAnnFileSources*)palloc0(sizeof(LakeAnnFileSources));
    sources->index_uuid=pstrdup(uuid);sources->column_id=binding->columns[column-1].column_id;
    sources->metric=metric;sources->raw_only=rawOnly;
    MemoryContextSwitchTo(previous);
    if(!LakeHasIndexRootVersion()||binding->file_count==0||!versions||column<1||column>binding->column_count||metric<1||metric>3||
       !(LakeFileProvider()->capabilities&OG_LAKE_SDK_OBJECT_VERSION)){
        if(rawOnly)ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("lake SMP ANN metadata/provider is no longer qualified")));
        return sources;
    }
    Datum* ids=(Datum*)palloc(sizeof(Datum)*binding->file_count);
    for(int f=0;f<binding->file_count;++f)ids[f]=Int64GetDatum(binding->files[f].file_id);
    Oid types[3]={UUIDOID,UUIDOID,INT8ARRAYOID};Datum args[3]={DirectFunctionCall1(uuid_in,CStringGetDatum(uuid)),
        DirectFunctionCall1(uuid_in,CStringGetDatum(binding->binding_id)),
        PointerGetDatum(construct_array(ids,binding->file_count,INT8OID,8,true,'d'))};
    Snapshot snapshot=RegisterSnapshot(GetLatestSnapshot());LakeMetaRows* rows=NULL;LakeMetaRows* roots=NULL;
    PG_TRY();{
        if(rawOnly){
            LakeMetaRows* definition=LakeMetadataQuery("SELECT COALESCE(definition->>'pq','false')::boolean,column_id,type_oid,table_uuid::text,server_oid,definition->>'metric' FROM og_lake_internal.index_definition "
                "WHERE index_uuid=$1 AND binding_id=$2 AND state='ACTIVE' AND definition->>'kind'='vector'",2,types,args,NULL,snapshot,true,SPI_OK_SELECT);
            LakeMetadataRequireCount(definition,1,"lake SMP ANN definition");
            if(DatumGetBool(LakeMetadataValue(definition,0,1,BOOLOID))||
               DatumGetInt64(LakeMetadataValue(definition,0,2,INT8OID))!=binding->columns[column-1].column_id||
               DatumGetObjectId(LakeMetadataValue(definition,0,3,OIDOID))!=VECTOROID||
               strcmp(LakeMetadataText(definition,0,4),binding->table_uuid)||
               DatumGetObjectId(LakeMetadataValue(definition,0,5,OIDOID))!=binding->server_oid||
               strcmp(LakeMetadataText(definition,0,6),metrics[metric-1]))
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("cached lake SMP ANN definition is no longer qualified for this raw metric; replan serially")));
        }
        rows=LakeMetadataQuery(psprintf("SELECT d.definition_generation,d.definition_hash::text,d.column_id,d.type_oid,d.table_uuid::text,"
            "d.server_oid,s.segment_uuid::text,s.build_token::text,s.source_manifest_hash::text,s.manifest_hash::text,"
            "s.artifact_root_uri::text,c.source_file_id,c.source_checksum::text,c.schema_mapping_hash::text,"
            "c.physical_row_count,c.source_file_bytes,c.source_path::text,d.definition->>'metric',s.format_version FROM og_lake_internal.index_definition d "
            "JOIN og_lake_internal.index_segment s USING(index_uuid,definition_generation) "
            "JOIN og_lake_internal.index_coverage c USING(segment_uuid,index_uuid,definition_generation) "
            "WHERE d.index_uuid=$1 AND d.binding_id=$2 AND d.state='ACTIVE' AND d.definition->>'kind'='vector' "
            "AND s.state='READY' AND s.algorithm_version=2 AND (s.format_version=1 OR (s.format_version=2 AND d.definition->>'pq'='true')) AND c.active AND c.source_file_id=ANY($3)%s",
            maxBytes?" LIMIT 4097":""),3,types,args,NULL,snapshot,true,SPI_OK_SELECT);
        Oid rootType=OIDOID;Datum rootArg=ObjectIdGetDatum(binding->server_oid);
        roots=LakeMetadataQuery("SELECT index_root::text FROM og_lake_internal.catalog_binding WHERE server_oid=$1",
            1,&rootType,&rootArg,NULL,snapshot,true,SPI_OK_SELECT);UnregisterSnapshot(snapshot);
    }PG_CATCH();{UnregisterSnapshot(snapshot);PG_RE_THROW();}PG_END_TRY();
    if(maxBytes&&rows->count>4096)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("lake ANN READY alternatives exceed4096")));
    ereport(DEBUG1,(errmsg("lake file ANN metadata selection"),errdetail("coverage_rows=%u bound_files=%d",rows->count,binding->file_count)));
    LakeMetadataRequireCount(roots,1,"ANN read store");bool absent=false;
    Datum rootValue=LakeMetadataValue(roots,0,1,TEXTOID,true,&absent);if(absent)return sources;
    const char* root=TextDatumGetCString(rootValue);char* schema=LakeAnnSchemaHash(binding);
    Size used=sizeof(*sources)+strlen(uuid)+1;
    if(maxBytes&&(used>maxBytes||rows->count>4096||sizeof(LakeAnnFileSource)*(Size)Max(1,rows->count)>maxBytes-used))
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("lake SMP ANN metadata exceeds the descriptor budget")));
    sources->entries=(LakeAnnFileSource*)MemoryContextAllocZero(owner,sizeof(LakeAnnFileSource)*Max(1,rows->count));
    used+=sizeof(LakeAnnFileSource)*Max(1,rows->count);
    for(uint32 r=0;r<rows->count;++r){CHECK_FOR_INTERRUPTS();
        int f=-1;int64 fileId=DatumGetInt64(LakeMetadataValue(rows,r,12,INT8OID));
        for(int i=0;i<binding->file_count;++i)if(binding->files[i].file_id==fileId)f=i;
        if(f<0||!versions[f])continue;
        LakeDataFile* file=&binding->files[f];const char* version=versions[f];
        const char* segment=LakeMetadataText(rows,r,7);char* directory=psprintf("%s%s",root,segment);
        char* source=LakeAnnSourceHash(file,version,schema);
        if(DatumGetInt64(LakeMetadataValue(rows,r,3,INT8OID))!=binding->columns[column-1].column_id||
           DatumGetObjectId(LakeMetadataValue(rows,r,4,OIDOID))!=VECTOROID||
           strcmp(LakeMetadataText(rows,r,5),binding->table_uuid)!=0||
           DatumGetObjectId(LakeMetadataValue(rows,r,6,OIDOID))!=binding->server_oid||
           strcmp(LakeMetadataText(rows,r,9),source)!=0||strcmp(LakeMetadataText(rows,r,11),directory)!=0||
           strcmp(LakeMetadataText(rows,r,13),psprintf("object-version-v1:%s",version))!=0||
           strcmp(LakeMetadataText(rows,r,14),schema)!=0||strcmp(LakeMetadataText(rows,r,18),metrics[metric-1])!=0||
           DatumGetInt64(LakeMetadataValue(rows,r,15,INT8OID))!=file->record_count||
           DatumGetInt64(LakeMetadataValue(rows,r,16,INT8OID))!=file->file_bytes||strcmp(LakeMetadataText(rows,r,17),file->path)!=0)continue;
        int format=DatumGetInt32(LakeMetadataValue(rows,r,19,INT4OID));
        /* A cached parallel plan cannot silently become PQ or mix domains. */
        if(rawOnly&&format!=1)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("lake SMP ANN source requires raw graph format1; replan serially")));
        const char* context=LakeAnnContext(uuid,DatumGetInt64(LakeMetadataValue(rows,r,1,INT8OID)),
            LakeMetadataText(rows,r,2),segment,LakeMetadataText(rows,r,8),source,schema);
        const char* texts[]={version,directory,source,schema,context,LakeMetadataText(rows,r,10)};
        for(int n=0;n<6;++n){Size bytes=strlen(texts[n])+1;
            if(maxBytes&&(bytes>maxBytes||used>maxBytes-bytes))
                ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("lake SMP ANN descriptor strings exceed1MiB")));
            used+=bytes;
        }
        auto* entry=&sources->entries[sources->count++];entry->file_id=fileId;entry->format=format;
        entry->object_version=MemoryContextStrdup(owner,texts[0]);entry->directory=MemoryContextStrdup(owner,texts[1]);
        entry->source_hash=MemoryContextStrdup(owner,texts[2]);entry->schema_hash=MemoryContextStrdup(owner,texts[3]);
        entry->context_hash=MemoryContextStrdup(owner,texts[4]);entry->manifest_hash=MemoryContextStrdup(owner,texts[5]);
    }
    return sources;
}

LakeFileAnnSelection* LakeSelectAnnFiles(LakeInlineBinding* binding,const char* uuid,
    AttrNumber column,int metric,Datum query,uint32 beam,uint32 minimum,char* const* versions,const LakeAnnScalarExpr* filter,
    const LakeAnnFileSources* prebound,LakeAnnRescanCache* rescan,uint32 emitLimit)
{
    auto* selection=(LakeFileAnnSelection*)palloc0(sizeof(LakeFileAnnSelection));selection->files=binding->file_count;
    if(beam<1||beam>1000||minimum<1||minimum>beam)ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid lake file ANN beam")));
    if(emitLimit==0||emitLimit>beam)emitLimit=beam;
    if(emitLimit<minimum)emitLimit=minimum;
    selection->positions=(LakeAnnFilePositions*)palloc0(sizeof(LakeAnnFilePositions)*Max(1,binding->file_count));
    if(binding->file_count==0||!versions||column<1||column>binding->column_count||metric<1||metric>3)return selection;
    const LakeAnnFileSources* sources=prebound?prebound:
        LakePrepareAnnFileSources(binding,uuid,column,metric,versions,CurrentMemoryContext);
    if(!sources||sources->column_id!=binding->columns[column-1].column_id||sources->metric!=metric||strcmp(sources->index_uuid,uuid))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("lake ANN prebound definition changed")));
    OgLakeSdkLimits limits=LakeFileLimits();int64 retained=0;
    PG_TRY();{
        for(uint32 r=0;r<sources->count;++r){CHECK_FOR_INTERRUPTS();const auto* entry=&sources->entries[r];
            int f=-1;int64 fileId=entry->file_id;
            for(int i=0;i<binding->file_count;++i)if(binding->files[i].file_id==fileId)f=i;
            if(f<0||!versions[f]||selection->positions[f].covered)continue;
            LakeDataFile* file=&binding->files[f];char* version=LakeFileObjectVersion(binding,f);
            if(filter&&(file->record_count<0||file->record_count>(int64)OG_LAKE_ANN_MAX_PAGES*OG_LAKE_ANN_PAGE_ROWS))continue;
            if(!version||strcmp(version,versions[f])||strcmp(version,entry->object_version))continue;
            const char* directory=entry->directory;const char* source=entry->source_hash;
            const char* schema=entry->schema_hash;
            /* Candidate allocations are bounded across files separately from
             * the temporary SDK/component reader budgets. */
            int64 budget=(int64)beam*OG_LAKE_ANN_MAX_POSTINGS*sizeof(OgLakeAnnAddress);
            if(retained>limits.memory_bytes-budget)continue;
            const char* context=entry->context_hash;
            const char* nodes=psprintf("%s/%s",directory,OG_LAKE_ANN_NODES_FILE);
            const char* lookup=psprintf("%s/%s",directory,OG_LAKE_ANN_LOOKUP_FILE);
            LakePqInput pqInput={};pqInput.source_file=fileId;pqInput.source_hash=source;pqInput.schema_hash=schema;pqInput.directory=directory;
            int artifactFormat=entry->format;
            if(!LakePqParseDescriptor(entry->manifest_hash,artifactFormat,&pqInput.descriptor))continue;
            LakeAnnRescanKey cacheKey={fileId,binding->columns[column-1].column_id,binding->columns[column-1].typmod,metric,
                uuid,version,directory,source,schema,context,entry->manifest_hash};
            if(LakeAnnRescanIsPoisoned(rescan,&cacheKey))continue;
            bool sharedPool=LakeAnnSharedPoolEnabled();
            LakeAnnRescanEntry* cached=LakeAnnRescanFind(rescan,&cacheKey,!sharedPool);
            if(!cached&&sharedPool)
                cached=LakeAnnSharedAcquire(rescan,&cacheKey,artifactFormat==2,CurrentMemoryContext);
            OgLakeSdkError error;OgLakeAnnSpec spec={};spec.struct_size=sizeof(spec);int code=OG_LAKE_SDK_OK;
            if(cached)spec=*LakeAnnRescanSpec(cached);
            if(!cached){
                HOLD_INTERRUPTS();code=og_lake_ann_reader_open_inspect(LakeFileProvider(),&limits,binding->columns[column-1].typmod,
                    metric,context,pqInput.descriptor.graph_root,nodes,lookup,&spec,&selection->reader,&error);
                RESUME_INTERRUPTS();CHECK_FOR_INTERRUPTS();
                if(code!=OG_LAKE_SDK_OK&&code!=OG_LAKE_SDK_CANCELLED&&code!=OG_LAKE_SDK_MEMORY&&(artifactFormat==2||sharedPool))
                    LakeAnnRescanInvalidate(rescan,cached,&cacheKey);
            }
            LakeFileAnnCandidates* found=NULL;MemoryContext owner=CurrentMemoryContext;
            MemoryContext filterContext=NULL;LakeAnnFileMask mask={};
            if(code==OG_LAKE_SDK_OK&&filter){
                filterContext=AllocSetContextCreate(owner,"Lake ANN scalar file positions",ALLOCSET_DEFAULT_SIZES);
                /* Source errors propagate; only errors in derived graph
                 * artifacts below are eligible for complete file fallback. */
                if(!LakeFileReadAnnScalarMask(binding,f,version,filter,&mask,filterContext,prebound!=NULL))code=OG_LAKE_SDK_UNSUPPORTED;
            }
            if(code==OG_LAKE_SDK_OK){
                PG_TRY();{
                    if(filter&&mask.count==0)found=(LakeFileAnnCandidates*)palloc0(sizeof(LakeFileAnnCandidates));
                    else {
                        if(!cached){
                            if(sharedPool)cached=LakeAnnSharedLoad(rescan,&cacheKey,selection->reader,artifactFormat==2?&pqInput:NULL,owner);
                            if(!cached&&rescan&&artifactFormat==2)cached=LakeAnnRescanLoad(rescan,&cacheKey,selection->reader,&pqInput);
                        }
                        found=LakeSearchAnnFile(selection->reader,query,beam,filter?&mask:NULL,artifactFormat==2?&pqInput:NULL,cached);
                    }
                }
                PG_CATCH();{MemoryContextSwitchTo(owner);ErrorData* failure=CopyErrorData();int sqlstate=failure->sqlerrcode;
                    if(sqlstate!=ERRCODE_DATA_CORRUPTED&&sqlstate!=ERRCODE_INDEX_CORRUPTED&&sqlstate!=ERRCODE_IO_ERROR&&sqlstate!=ERRCODE_FDW_ERROR){FreeErrorData(failure);PG_RE_THROW();}
                    FreeErrorData(failure);FlushErrorState();LakeAnnRescanInvalidate(rescan,cached,&cacheKey);cached=NULL;code=OG_LAKE_SDK_INVALID;}
                PG_END_TRY();
            }
            uint32 matched=mask.count;
            if(filterContext)MemoryContextDelete(filterContext);
            HOLD_INTERRUPTS();og_lake_ann_reader_close(&selection->reader);RESUME_INTERRUPTS();CHECK_FOR_INTERRUPTS();
            if(code==OG_LAKE_SDK_CANCELLED)LakeFileCheckStatus(code,&error);
            if(code!=OG_LAKE_SDK_OK||!found){ereport(DEBUG1,(errmsg("lake file ANN artifact unavailable"),errdetail("file=%lld status=%d",(long long)fileId,code)));continue;}
            struct LakeAnnRankedPos { OgLakeAnnAddress address; uint32 rank; };
            auto* ranked=(LakeAnnRankedPos*)palloc(sizeof(LakeAnnRankedPos)*Max(found->count,(uint32)1));
            bool valid=!found->budget_exhausted;
            for(uint32 i=0;i<found->count;++i){
                ranked[i].address=found->rows[i].address;
                ranked[i].rank=i;
                const auto& a=ranked[i].address;
                if(a.file_id!=fileId||a.row_position<0||a.row_position>=file->record_count||
                    a.row_id<0||a.birth_snapshot<0)valid=false;
            }
            if(found->count>1){
                qsort(ranked,found->count,sizeof(LakeAnnRankedPos),[](const void* pa,const void* pb)->int{
                    const auto* a=(const LakeAnnRankedPos*)pa;const auto* b=(const LakeAnnRankedPos*)pb;
                    if(a->address.file_id!=b->address.file_id)return a->address.file_id<b->address.file_id?-1:1;
                    if(a->address.row_position!=b->address.row_position)return a->address.row_position<b->address.row_position?-1:1;
                    return a->rank<b->rank?-1:(a->rank>b->rank?1:0);
                });
            }
            uint32 count=0;
            for(uint32 i=0;i<found->count;++i){
                if(count&&ranked[i].address.row_position==ranked[count-1].address.row_position){
                    if(ranked[i].address.row_id!=ranked[count-1].address.row_id||
                        ranked[i].address.birth_snapshot!=ranked[count-1].address.birth_snapshot)valid=false;
                }else ranked[count++]=ranked[i];
            }
            uint32 visited=found->visited_candidates;bool pqUsed=found->pq_used;
            if(found->rows)pfree(found->rows);pfree(found);
            /* Native frozen/slave vertices can repeat an existing posting.
             * Only unique physical rows count toward the requested SQL K.
             * An empty, v2-qualified graph has no indexable source row. */
            if(count<minimum&&spec.posting_count!=0&&(!filter||matched!=0))valid=false;
            ereport(DEBUG1,(errmsg("lake file ANN candidates"),errdetail("file=%lld nodes=%u visited=%u unique_rows=%u requested=%u usable=%d pq_used=%d",
                (long long)fileId,spec.node_count,visited,count,minimum,valid?1:0,pqUsed?1:0)));
            if(filter)ereport(DEBUG1,(errmsg("lake filtered ANN candidates"),errdetail("file=%lld scalar_matches=%u admitted_rows=%u usable=%d",
                (long long)fileId,matched,count,valid?1:0)));
            if(valid){
                uint32 keep=Min(count,emitLimit);
                if(keep<count){
                    qsort(ranked,count,sizeof(LakeAnnRankedPos),[](const void* pa,const void* pb)->int{
                        const auto* a=(const LakeAnnRankedPos*)pa;const auto* b=(const LakeAnnRankedPos*)pb;
                        return a->rank<b->rank?-1:(a->rank>b->rank?1:0);
                    });
                }
                auto* positions=(OgLakeAnnAddress*)palloc(sizeof(OgLakeAnnAddress)*Max(keep,(uint32)1));
                for(uint32 i=0;i<keep;++i)positions[i]=ranked[i].address;
                if(keep>1)qsort(positions,keep,sizeof(*positions),LakeAnnCompareAddress);
                pfree(ranked);
                selection->positions[f]={keep,positions,true,pqUsed};
                retained+=budget;
            }else pfree(ranked);
        }
    }PG_CATCH();{HOLD_INTERRUPTS();og_lake_ann_reader_close(&selection->reader);RESUME_INTERRUPTS();PG_RE_THROW();}PG_END_TRY();
    return selection;
}

void LakeAnnFileCoverageEstimate(const LakeInlineBinding* binding,const char* uuid,AttrNumber column,
    Snapshot snapshot,double* rowCount,double* byteCount,double* groupCount,int* fileCount,LakeCostLedger* ledger)
{
    *rowCount=*byteCount=*groupCount=0;*fileCount=0;
    if(!LakeHasIndexRootVersion()||binding->file_count==0)return;
    Datum* ids=(Datum*)palloc(sizeof(Datum)*binding->file_count);
    for(int f=0;f<binding->file_count;++f)ids[f]=Int64GetDatum(binding->files[f].file_id);
    Oid types[3]={UUIDOID,UUIDOID,INT8ARRAYOID};Datum args[3]={DirectFunctionCall1(uuid_in,CStringGetDatum(uuid)),
        DirectFunctionCall1(uuid_in,CStringGetDatum(binding->binding_id)),
        PointerGetDatum(construct_array(ids,binding->file_count,INT8OID,8,true,'d'))};
    LakeMetaRows* rows=LakeMetadataQuery("SELECT c.source_file_id,c.physical_row_count,c.source_file_bytes,c.source_path::text,"
        "c.schema_mapping_hash::text,d.column_id,d.type_oid,d.table_uuid::text,s.format_version,s.manifest_hash::text FROM og_lake_internal.index_definition d "
        "JOIN og_lake_internal.index_segment s USING(index_uuid,definition_generation) "
        "JOIN og_lake_internal.index_coverage c USING(segment_uuid,index_uuid,definition_generation) "
        "WHERE d.index_uuid=$1 AND d.binding_id=$2 AND d.state='ACTIVE' AND s.state='READY' "
        "AND (s.format_version=1 OR (s.format_version=2 AND d.definition->>'pq'='true')) AND s.algorithm_version=2 AND c.active AND c.source_file_id=ANY($3)",
        3,types,args,NULL,snapshot,true,SPI_OK_SELECT);
    char* schema=LakeAnnSchemaHash(binding);
    for(uint32 r=0;r<rows->count;++r){
        int64 fileId=DatumGetInt64(LakeMetadataValue(rows,r,1,INT8OID));
        for(int f=0;f<binding->file_count;++f){
            const LakeDataFile* file=&binding->files[f];
            if(file->file_id!=fileId)continue;
            if(DatumGetInt64(LakeMetadataValue(rows,r,2,INT8OID))!=file->record_count||
                DatumGetInt64(LakeMetadataValue(rows,r,3,INT8OID))!=file->file_bytes||
                strcmp(LakeMetadataText(rows,r,4),file->path)!=0||strcmp(LakeMetadataText(rows,r,5),schema)!=0||
                DatumGetInt64(LakeMetadataValue(rows,r,6,INT8OID))!=binding->columns[column-1].column_id||
                DatumGetObjectId(LakeMetadataValue(rows,r,7,OIDOID))!=binding->columns[column-1].type||
                strcmp(LakeMetadataText(rows,r,8),binding->table_uuid)!=0)continue;
            if(ledger&&(f>=ledger->files||ledger->source[f].file_id!=fileId))continue;
            if(ledger){
                int format=DatumGetInt32(LakeMetadataValue(rows,r,9,INT4OID));LakePqDescriptor descriptor={};
                if(!LakePqParseDescriptor(LakeMetadataText(rows,r,10),format,&descriptor))continue;
                if(!LakeCostCover(ledger,f,format,descriptor.node_count,LakePqCompiled()?descriptor.chunks:0,descriptor.training_rows))continue;
            }
            *rowCount+=(double)file->record_count;*byteCount+=(double)file->file_bytes;
            *groupCount+=(double)Max((int64)1,file->row_group_count);++*fileCount;
        }
    }
}

void LakeAnnDescribeMaintenanceTask(LakeIndexArtifactTask* task)
{
    task->version=LakeFileObjectVersion(task->binding,0);
    if(task->version==NULL)ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("background ANN source lacks an immutable-object token")));
    task->schema=LakeAnnSchemaHash(task->binding);
    task->source=LakeAnnSourceHash(&task->binding->files[0],task->version,task->schema);
    task->context=LakeAnnContext(task->uuid,task->generation,task->definition_hash,task->segment,task->token,task->source,task->schema);
    task->directory=psprintf("%s%s",task->root,task->segment);task->algorithm=2;task->format=1;
}
LakeIndexArtifactHealth LakeAnnCheckMaintenanceTask(LakeIndexArtifactTask* task)
{
    if(!LakeIndexArtifactMetadata(task)||task->known_algorithm!=2||
       (task->known_format!=1&&!(task->pq&&task->known_format==2)))return LAKE_ARTIFACT_REBUILD;
    const char* names[]={OG_LAKE_ANN_NODES_FILE,OG_LAKE_ANN_LOOKUP_FILE,LAKE_PQ_MODEL_FILE,LAKE_PQ_CODES_FILE,LAKE_PQ_MANIFEST_FILE};
    LakeIndexArtifactHealth files=LakeIndexArtifactFiles(task->known_directory,names,task->known_format==2?5:2);
    if(files!=LAKE_ARTIFACT_VALID)return files;
    const char* context=LakeAnnContext(task->uuid,task->generation,task->definition_hash,
        task->known_segment,task->known_token,task->source,task->schema);
    int status=OG_LAKE_SDK_OK;
    bool valid=LakeAnnReadyVerified(LakeFileProvider(),task->binding->columns[task->column].typmod,task->metric,
        context,task->known_manifest,task->known_directory,task->known_format,task->binding->files[0].file_id,task->source,task->schema,&status);
    return valid?LAKE_ARTIFACT_VALID:status==OG_LAKE_SDK_INVALID?LAKE_ARTIFACT_REBUILD:LAKE_ARTIFACT_RETRY;
}
void LakeAnnBuildMaintenanceTask(LakeIndexArtifactTask* task)
{
    LakeFileCandidate* nodes=NULL;LakeFileCandidate* lookup=NULL;OgLakeAnnWritten written={};
    MemoryContext owner=CurrentMemoryContext;
    LakeBuildAnnFileSegment(task->binding,0,task->column,task->metric,task->version,task->context,task->directory,&nodes,&lookup,&written);
    MemoryContextSwitchTo(owner);
    LakePqBuilt pq={};
    if(task->pq)pq=LakeAnnBuildPq(LakeFileProvider(),task->binding->columns[task->column].typmod,task->metric,
        task->context,written.manifest_hash,task->directory,task->binding->files[0].file_id,task->source,task->schema,task->pq_chunks);
    MemoryContextSwitchTo(owner);
    char* version=LakeFileObjectVersion(task->binding,0);
    if(version==NULL||strcmp(version,task->version)!=0)
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("background ANN source changed during graph/PQ build")));
    task->manifest=pstrdup(pq.enabled?pq.descriptor:written.manifest_hash);task->format=pq.enabled?2:1;
    task->candidates[0]=nodes;task->candidates[1]=lookup;task->candidate_count=2;
    if(pq.enabled)for(int i=0;i<3;++i)task->candidates[task->candidate_count++]=pq.files[i];
}
