/* Host publication around portable Parquet sorted pages. Source addresses are
 * physical (file,position); query visibility never belongs to the index. */
#include "postgres.h"
#include "lake_cost.h"
#include "lake_index_maintenance.h"
#include "knl/knl_variable.h"
#include "lake_scalar_key.h"
#include "lake_file_scalar.h"
#include "lake_file_reader.h"
#include "lake_file_sdk.h"
#include "lake_index_private.h"
#include "lake_scalar_format.h"
#ifdef USE_LAKE_BITMAP
#include "lake_bitmap_format.h"
#endif
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "parser/parse_oper.h"
#include "utils/builtins.h"
#include "utils/array.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/tuplesort.h"
#include <memory>
#include <new>
#include <openssl/sha.h>

static char* LakeScalarHash(const char* input)
{
    unsigned char digest[SHA256_DIGEST_LENGTH];
    if(SHA256((const unsigned char*)input,strlen(input),digest)==NULL)
        ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("could not hash lake scalar identity")));
    char* result=(char*)palloc(65);static const char hex[]="0123456789abcdef";
    for(int i=0;i<32;++i){result[i*2]=hex[digest[i]>>4];result[i*2+1]=hex[digest[i]&15];}
    result[64]='\0';return result;
}

static char* LakeScalarSchemaHash(const LakeInlineBinding* binding)
{
    StringInfoData input;initStringInfo(&input);
    appendStringInfo(&input,"og_lake_scalar_schema_v1|%d",binding->column_count);
    for(int c=0;c<binding->column_count;++c)
        appendStringInfo(&input,"|%lld:%u",(long long)binding->columns[c].column_id,binding->columns[c].type);
    /* Stable field IDs and types survive table/column renames. */
    return LakeScalarHash(input.data);
}

static char* LakeScalarSourceHash(const LakeDataFile* file,const char* version,const char* schema)
{
    return LakeScalarHash(psprintf("og_lake_scalar_source_v1|%lld|%lld|%lld|%lld|%d|%lld|%lld|%lld|%zu:%s|%zu:%s|%s",
        (long long)file->file_id,(long long)file->record_count,(long long)file->file_bytes,(long long)file->begin_snapshot,
        file->row_id_start_null?1:0,(long long)file->row_id_start,(long long)file->partial_max,(long long)file->row_group_count,
        strlen(file->path),file->path,strlen(version),version,schema));
}

static char* LakeScalarContext(const char* uuid,int64 generation,const char* definition,
    const char* segment,const char* token,const char* source,const char* schema)
{
    return LakeScalarHash(psprintf("og_lake_scalar_context_v1|%s|%lld|%s|%s|%s|%s|%s",
        uuid,(long long)generation,definition,segment,token,source,schema));
}

struct LakeScalarBuild {
    Tuplesortstate* sort;
    TupleTableSlot* slot;
    OgLakeScalarWriter* writer;
    OgLakeScalarReader* reader;
#ifdef USE_LAKE_BITMAP
    OgLakeBitmapWriter* bitmap_writer;
    OgLakeBitmapReader* bitmap_reader;
#endif
    Oid key_type;
    int64 input_rows;
};

static void LakeScalarBuildClose(LakeScalarBuild* build)
{
    HOLD_INTERRUPTS();og_lake_scalar_reader_close(&build->reader);
    og_lake_scalar_writer_close(&build->writer);
#ifdef USE_LAKE_BITMAP
    og_lake_bitmap_reader_close(&build->bitmap_reader);
    og_lake_bitmap_writer_close(&build->bitmap_writer);
#endif
    RESUME_INTERRUPTS();
    if(build->slot){ExecDropSingleTupleTableSlot(build->slot);build->slot=NULL;}
    if(build->sort){tuplesort_end(build->sort);build->sort=NULL;}
}

static uint32 LakeScalarStorageType(Oid type)
{
    if(!LakeScalarIndexedType(type))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("unadmitted lake scalar storage key type")));
    return type==INT2OID?OG_LAKE_SDK_INT16:type==INT4OID?OG_LAKE_SDK_INT32:OG_LAKE_SDK_INT64;
}

static void LakeScalarPutKey(int64 file,int64 position,Datum key,bool absent,void* argument)
{
    LakeScalarBuild* build=(LakeScalarBuild*)argument;ExecClearTuple(build->slot);
    build->slot->tts_values[0]=Int64GetDatum(absent?0:
        LakeScalarOrderKey(key,build->key_type));
    build->slot->tts_isnull[0]=absent;
    build->slot->tts_values[1]=Int64GetDatum(file);build->slot->tts_isnull[1]=false;
    build->slot->tts_values[2]=Int64GetDatum(position);build->slot->tts_isnull[2]=false;
    ExecStoreVirtualTuple(build->slot);tuplesort_puttupleslot(build->sort,build->slot);
    if(build->input_rows==PG_INT64_MAX)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("lake scalar physical row count overflow")));
    ++build->input_rows;
}

static LakeFileCandidate* LakeScalarCandidate(const char* directory,bool lookup,bool bitmap)
{
    int32 count=0;const OgLakeSdkField* fields=NULL;const char* name=NULL;
#ifdef USE_LAKE_BITMAP
    if(bitmap){fields=og_lake_bitmap_fields(&count);name=OG_LAKE_BITMAP_DATA_FILE;}
    else
#endif
    {fields=og_lake_scalar_fields(lookup?1:0,&count);name=lookup?OG_LAKE_SCALAR_LOOKUP_FILE:OG_LAKE_SCALAR_DATA_FILE;}
    OgLakeSdkWriteOptions options={};options.struct_size=sizeof(options);
    options.path=psprintf("%s/%s",directory,name);
    options.limits=LakeFileLimits();options.num_fields=count;options.fields=fields;
    options.compression=OG_LAKE_SDK_ZSTD;options.row_group_rows=bitmap?256:OG_LAKE_SCALAR_PAGE_ROWS;
    return LakeFileCandidateOpen(&options);
}

/* The catalog coverage counts source rows; a bitmap Parquet file counts
 * dictionary keys. Keeping these populations separate prevents false READY. */
struct LakeScalarBuildOutput {
    int64 physical_rows;
    int64 artifact_rows;
    char manifest_hash[65];
};

#ifdef USE_LAKE_BITMAP
static OgLakeBitmapFileSpec LakeScalarBitmapSpec(Oid keyType,const LakeDataFile* file,const char* context)
{
    OgLakeBitmapFileSpec spec={};spec.struct_size=sizeof(spec);
    spec.format_version=OG_LAKE_BITMAP_FORMAT_VERSION;
    spec.key_type=LakeScalarStorageType(keyType);
    spec.file_id=file->file_id;spec.physical_rows=file->record_count;spec.context_hash=context;
    return spec;
}
#endif

static void LakeScalarBuildSegment(LakeInlineBinding* binding,int fileIndex,int column,
    const char* version,const char* context,const char* directory,LakeFileCandidate** data,
    LakeFileCandidate** lookup,LakeScalarBuildOutput* written,bool bitmap)
{
    LakeScalarBuild* build=(LakeScalarBuild*)palloc0(sizeof(LakeScalarBuild));
    build->key_type=binding->columns[column].type;
    PG_TRY();
    {
        TupleDesc descriptor=CreateTemplateTupleDesc(3,false);descriptor->td_tam_ops=TableAmHeap;
        TupleDescInitEntry(descriptor,1,"key",INT8OID,-1,0);
        TupleDescInitEntry(descriptor,2,"file",INT8OID,-1,0);
        TupleDescInitEntry(descriptor,3,"position",INT8OID,-1,0);
        build->slot=MakeSingleTupleTableSlot(descriptor);
        Oid less=InvalidOid;get_sort_group_operators(INT8OID,true,false,false,&less,NULL,NULL,NULL);
        AttrNumber columns[3]={1,2,3};Oid operators[3]={less,less,less};
        Oid collations[3]={InvalidOid,InvalidOid,InvalidOid};bool nulls[3]={false,false,false};
        int memory=Max(64,Min(u_sess->attr.attr_memory.work_mem,16384));
        build->sort=tuplesort_begin_heap(descriptor,3,columns,operators,collations,nulls,memory,false);
        LakeFileVisitScalarKeys(binding,fileIndex,column+1,version,LakeScalarPutKey,build);
        if(build->input_rows!=binding->files[fileIndex].record_count)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("scalar index lacks complete physical source coverage")));
        tuplesort_performsort(build->sort);
        *data=LakeScalarCandidate(directory,false,bitmap);
        if(!bitmap)*lookup=LakeScalarCandidate(directory,true,false);
        OgLakeSdkLimits limits=LakeFileLimits();OgLakeSdkError error;
        uint32 keyType=LakeScalarStorageType(build->key_type);
        int status;
#ifdef USE_LAKE_BITMAP
        OgLakeBitmapFileSpec bitmapSpec=LakeScalarBitmapSpec(build->key_type,&binding->files[fileIndex],context);
        if(bitmap){
            HOLD_INTERRUPTS();status=og_lake_bitmap_writer_open((*data)->api,&limits,&bitmapSpec,
                (*data)->writer,&build->bitmap_writer,&error);RESUME_INTERRUPTS();
        }else
#endif
        {
            HOLD_INTERRUPTS();status=og_lake_scalar_writer_open((*data)->api,&limits,keyType,context,
                (*data)->writer,(*lookup)->writer,&build->writer,&error);RESUME_INTERRUPTS();
        }
        LakeFileCheckStatus(status,&error);CHECK_FOR_INTERRUPTS();
        OgLakeScalarEntry* entries=(OgLakeScalarEntry*)palloc(sizeof(OgLakeScalarEntry)*OG_LAKE_SCALAR_PAGE_ROWS);
        int count=0;
        bool more=true;
        while(more){
            more=tuplesort_gettupleslot(build->sort,true,build->slot,NULL);
            if(more){
                CHECK_FOR_INTERRUPTS();tableam_tslot_getallattrs(build->slot);
                entries[count++]={build->slot->tts_isnull[0]?0:DatumGetInt64(build->slot->tts_values[0]),
                    DatumGetInt64(build->slot->tts_values[1]),DatumGetInt64(build->slot->tts_values[2]),build->slot->tts_isnull[0]?1:0};
                ExecClearTuple(build->slot);
            }
            if(count&&(count==(int)OG_LAKE_SCALAR_PAGE_ROWS||!more)){
#ifdef USE_LAKE_BITMAP
                if(bitmap){
                    HOLD_INTERRUPTS();status=og_lake_bitmap_writer_append(build->bitmap_writer,entries,count,&error);RESUME_INTERRUPTS();
                }else
#endif
                {
                    HOLD_INTERRUPTS();status=og_lake_scalar_writer_append(build->writer,entries,count,&error);RESUME_INTERRUPTS();
                }
                LakeFileCheckStatus(status,&error);CHECK_FOR_INTERRUPTS();count=0;
            }
        }
        pfree(entries);
#ifdef USE_LAKE_BITMAP
        if(bitmap){
            OgLakeBitmapWritten result={};result.struct_size=sizeof(result);
            HOLD_INTERRUPTS();status=og_lake_bitmap_writer_finish(build->bitmap_writer,&result,&error);RESUME_INTERRUPTS();
            LakeFileCheckStatus(status,&error);CHECK_FOR_INTERRUPTS();
            written->physical_rows=result.physical_source_rows;written->artifact_rows=result.dictionary_rows;
            memcpy(written->manifest_hash,result.manifest_hash,sizeof(written->manifest_hash));
        }else
#endif
        {
            OgLakeScalarWritten result={};result.struct_size=sizeof(result);
            HOLD_INTERRUPTS();status=og_lake_scalar_writer_finish(build->writer,&result,&error);RESUME_INTERRUPTS();
            LakeFileCheckStatus(status,&error);CHECK_FOR_INTERRUPTS();
            written->physical_rows=result.num_rows;written->artifact_rows=result.num_rows;
            memcpy(written->manifest_hash,result.manifest_hash,sizeof(written->manifest_hash));
        }
        LakeFileCandidateFinish(*data);if(*lookup)LakeFileCandidateFinish(*lookup);
        if(written->physical_rows!=build->input_rows||(*data)->written.num_rows!=written->artifact_rows)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("scalar artifact row population changed")));
        /* Reopen finished bytes before READY. Bitmap open verifies every
         * payload, mutual exclusion and the full physical population. */
#ifdef USE_LAKE_BITMAP
        if(bitmap){
            HOLD_INTERRUPTS();status=og_lake_bitmap_reader_open((*data)->api,&limits,&bitmapSpec,written->manifest_hash,
                (*data)->path,&build->bitmap_reader,&error);RESUME_INTERRUPTS();
            LakeFileCheckStatus(status,&error);CHECK_FOR_INTERRUPTS();
        }else
#endif
        {
            HOLD_INTERRUPTS();status=og_lake_scalar_reader_open((*data)->api,&limits,keyType,context,written->manifest_hash,
                (*data)->path,(*lookup)->path,&build->reader,&error);RESUME_INTERRUPTS();
            LakeFileCheckStatus(status,&error);CHECK_FOR_INTERRUPTS();
            HOLD_INTERRUPTS();status=og_lake_scalar_verify(build->reader,&error);RESUME_INTERRUPTS();
            LakeFileCheckStatus(status,&error);CHECK_FOR_INTERRUPTS();
        }
        char* after=LakeFileObjectVersion(binding,fileIndex);
        if(after==NULL||strcmp(after,version)!=0)
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("scalar source changed before artifact publication")));
        LakeScalarBuildClose(build);
    }
    PG_CATCH();{LakeScalarBuildClose(build);PG_RE_THROW();}PG_END_TRY();
}

static bool LakeScalarReadyVerified(const OgLakeParquetSdkApi* api,Oid keyType,
    const char* context,const char* manifest,const char* directory,const LakeDataFile* file,bool bitmap,int* diagnosticStatus=NULL)
{
    LakeScalarBuild* state=(LakeScalarBuild*)palloc0(sizeof(LakeScalarBuild));
    OgLakeSdkLimits limits=LakeFileLimits();OgLakeSdkError error;
    int volatile code=OG_LAKE_SDK_OK;
    PG_TRY();
    {
#ifdef USE_LAKE_BITMAP
        if(bitmap){
            OgLakeBitmapFileSpec spec=LakeScalarBitmapSpec(keyType,file,context);
            const char* path=psprintf("%s/%s",directory,OG_LAKE_BITMAP_DATA_FILE);
            HOLD_INTERRUPTS();code=og_lake_bitmap_reader_open(api,&limits,&spec,manifest,path,&state->bitmap_reader,&error);
            RESUME_INTERRUPTS();CHECK_FOR_INTERRUPTS();
        }else
#endif
        {
            const char* data=psprintf("%s/%s",directory,OG_LAKE_SCALAR_DATA_FILE);
            const char* lookup=psprintf("%s/%s",directory,OG_LAKE_SCALAR_LOOKUP_FILE);
            HOLD_INTERRUPTS();code=og_lake_scalar_reader_open(api,&limits,
                LakeScalarStorageType(keyType),context,manifest,data,lookup,&state->reader,&error);
            RESUME_INTERRUPTS();CHECK_FOR_INTERRUPTS();
            if(code==OG_LAKE_SDK_OK){
                HOLD_INTERRUPTS();code=og_lake_scalar_verify(state->reader,&error);RESUME_INTERRUPTS();
            }
        }
        LakeScalarBuildClose(state);CHECK_FOR_INTERRUPTS();
        /* Only corrupt/unavailable derived files trigger repair. Cancellation,
         * unsupported provider and budgets remain explicit build errors. */
        if(diagnosticStatus)*diagnosticStatus=code;
        if(code!=OG_LAKE_SDK_OK&&code!=OG_LAKE_SDK_IO&&code!=OG_LAKE_SDK_INVALID&&
            !(diagnosticStatus&&(code==OG_LAKE_SDK_MEMORY||code==OG_LAKE_SDK_UNSUPPORTED||code==OG_LAKE_SDK_STATE)))
            LakeFileCheckStatus(code,&error);
    }
    PG_CATCH();{LakeScalarBuildClose(state);PG_RE_THROW();}PG_END_TRY();
    pfree(state);return code==OG_LAKE_SDK_OK;
}

int LakeBuildScalarFileIndexes(LakeInlineBinding* binding,const char* uuid,int column,
    int64 generation,const char* definitionHash,Snapshot snapshot,bool bitmap)
{
#ifndef USE_LAKE_BITMAP
    if(bitmap)ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("bitmap indexes require a qualified CRoaring build")));
#endif
    if(!LakeHasIndexRootVersion()||binding->file_count==0)return 0;
    Oid rootType=OIDOID;Datum rootArg=ObjectIdGetDatum(binding->server_oid);
    LakeMetaRows* roots=LakeMetadataQuery("SELECT index_root::text,xmin::text FROM og_lake_internal.catalog_binding WHERE server_oid=$1",
        1,&rootType,&rootArg,NULL,snapshot,true,SPI_OK_SELECT);
    LakeMetadataRequireCount(roots,1,"scalar index store");bool absent=false;
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
    int capacity=Max(0,(16-candidates)/(bitmap?1:2)),built=0;
    if(capacity==0)ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("commit existing file candidates before building more index segments")));
    char* schema=LakeScalarSchemaHash(binding);
    for(int f=0;f<binding->file_count&&built<capacity;++f){
        CHECK_FOR_INTERRUPTS();LakeDataFile* file=&binding->files[f];
        char* version=LakeFileObjectVersion(binding,f);
        if(version==NULL)ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("scalar source has no immutable-object identity")));
        char* source=LakeScalarSourceHash(file,version,schema);
        Oid knownTypes[6]={UUIDOID,INT8OID,INT8OID,TEXTOID,INT4OID,INT4OID};Datum knownArgs[6]={DirectFunctionCall1(uuid_in,CStringGetDatum(uuid)),
            Int64GetDatum(generation),Int64GetDatum(file->file_id),CStringGetTextDatum(source),
            Int32GetDatum(LakeScalarAlgorithm(binding->columns[column].type)),Int32GetDatum(bitmap?2:1)};
        LakeMetaRows* known=LakeMetadataQuery("SELECT s.segment_uuid::text,s.build_token::text,s.artifact_root_uri::text,"
            "s.manifest_hash::text,c.schema_mapping_hash::text,c.source_checksum::text,c.physical_row_count,"
            "c.source_file_bytes,c.source_path::text FROM og_lake_internal.index_coverage c JOIN "
            "og_lake_internal.index_segment s USING(segment_uuid,index_uuid,definition_generation) WHERE c.index_uuid=$1 "
            "AND c.definition_generation=$2 AND c.source_file_id=$3 AND c.active AND s.state='READY' "
            "AND s.source_manifest_hash=$4 AND s.algorithm_version=$5 AND s.format_version=$6",
            6,knownTypes,knownArgs,NULL,snapshot,true,SPI_OK_SELECT);
        if(known->count>1)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("multiple live scalar coverages for one physical source")));
        if(known->count==1){
            const char* segment=LakeMetadataText(known,0,1);
            char* directory=psprintf("%s%s",root,segment);
            if(strcmp(LakeMetadataText(known,0,3),directory)!=0||
                strcmp(LakeMetadataText(known,0,5),schema)!=0||
                strcmp(LakeMetadataText(known,0,6),psprintf("object-version-v1:%s",version))!=0||
                DatumGetInt64(LakeMetadataValue(known,0,7,INT8OID))!=file->record_count||
                DatumGetInt64(LakeMetadataValue(known,0,8,INT8OID))!=file->file_bytes||
                strcmp(LakeMetadataText(known,0,9),file->path)!=0)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("existing scalar coverage identity changed")));
            char* context=LakeScalarContext(uuid,generation,definitionHash,segment,
                LakeMetadataText(known,0,2),source,schema);
            if(LakeScalarReadyVerified(api,binding->columns[column].type,context,
                LakeMetadataText(known,0,4),directory,file,bitmap))continue;
        }
        Datum segment=OidFunctionCall0(F_GEN_RANDOM_UUID),token=OidFunctionCall0(F_GEN_RANDOM_UUID);
        const char* segmentText=DatumGetCString(DirectFunctionCall1(uuid_out,segment));
        const char* tokenText=DatumGetCString(DirectFunctionCall1(uuid_out,token));
        char* directory=psprintf("%s%s",root,segmentText);
        char* context=LakeScalarContext(uuid,generation,definitionHash,segmentText,tokenText,source,schema);
        Oid types[8]={UUIDOID,UUIDOID,INT8OID,UUIDOID,INT8OID,TEXTOID,TEXTOID,TEXTOID};
        Datum args[8]={segment,knownArgs[0],Int64GetDatum(generation),token,Int64GetDatum(binding->snapshot_id),
            CStringGetTextDatum(source),CStringGetTextDatum(directory),(Datum)0};
        LakeMetaRows* added=LakeMetadataQuery(psprintf("INSERT INTO og_lake_internal.index_segment(segment_uuid,index_uuid,definition_generation,"
            "build_token,build_snapshot,source_manifest_hash,algorithm_version,format_version,artifact_root_uri,state) "
            "VALUES($1,$2,$3,$4,$5,$6,%d,%d,$7,'BUILDING')",LakeScalarAlgorithm(binding->columns[column].type),bitmap?2:1),7,types,args,NULL,snapshot,false,SPI_OK_INSERT);
        LakeMetadataRequireCount(added,1,"scalar BUILDING segment");
        LakeEnsureLocalDirectory(directory);
        LakeFileCandidate* data=NULL;LakeFileCandidate* lookup=NULL;LakeScalarBuildOutput written={};
        LakeScalarBuildSegment(binding,f,column,version,context,directory,&data,&lookup,&written,bitmap);
        args[7]=CStringGetTextDatum(written.manifest_hash);
        LakeMetaRows* ready=LakeMetadataQuery("UPDATE og_lake_internal.index_segment SET state='READY',manifest_hash=$8 "
            "WHERE segment_uuid=$1 AND index_uuid=$2 AND definition_generation=$3 AND build_token=$4 AND build_snapshot=$5 "
            "AND source_manifest_hash=$6 AND artifact_root_uri=$7 AND state='BUILDING'",8,types,args,NULL,snapshot,false,SPI_OK_UPDATE);
        LakeMetadataRequireCount(ready,1,"scalar READY token publication");
        (void)LakeMetadataQuery("UPDATE og_lake_internal.index_coverage SET active=false WHERE index_uuid=$1 "
            "AND definition_generation=$2 AND source_file_id=$3 AND active",3,knownTypes,knownArgs,NULL,snapshot,false,SPI_OK_UPDATE);
        Oid coverageTypes[10]={UUIDOID,INT8OID,UUIDOID,INT8OID,TEXTOID,TEXTOID,INT8OID,BOOLOID,INT8OID,TEXTOID};
        Datum coverageArgs[10]={segment,Int64GetDatum(file->file_id),knownArgs[0],Int64GetDatum(generation),
            CStringGetTextDatum(psprintf("object-version-v1:%s",version)),CStringGetTextDatum(schema),Int64GetDatum(file->record_count),
            BoolGetDatum(true),Int64GetDatum(file->file_bytes),CStringGetTextDatum(file->path)};
        LakeMetaRows* covered=LakeMetadataQuery("INSERT INTO og_lake_internal.index_coverage(segment_uuid,source_file_id,index_uuid,"
            "definition_generation,source_checksum,schema_mapping_hash,physical_row_count,active,source_file_bytes,source_path) "
            "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",10,coverageTypes,coverageArgs,NULL,snapshot,false,SPI_OK_INSERT);
        LakeMetadataRequireCount(covered,1,"scalar physical coverage publication");
        (void)LakeMetadataQuery("UPDATE og_lake_internal.index_segment s SET state='RETIRED' WHERE s.index_uuid=$1 AND s.state='READY' "
            "AND NOT EXISTS(SELECT 1 FROM og_lake_internal.index_coverage c WHERE c.segment_uuid=s.segment_uuid AND c.active)",
            1,knownTypes,knownArgs,NULL,snapshot,false,SPI_OK_UPDATE);
        LakeFileCandidatePublish(data);if(lookup)LakeFileCandidatePublish(lookup);++built;
    }
    return built;
}

/* Typed handles share an address interface, never an opaque representation.
 * A zero-position result is still verified coverage, unlike no result. */
struct LakeScalarPositions {
    OgLakeScalarResult* sorted;
#ifdef USE_LAKE_BITMAP
    OgLakeBitmapResult* bitmap;
#endif
};
struct LakeFileScalarSelection {
    int files;
    LakeScalarPositions* positions;
    OgLakeScalarReader* pending_reader;
#ifdef USE_LAKE_BITMAP
    OgLakeBitmapReader* pending_bitmap_reader;
#endif
};

static void LakeScalarPositionsClose(LakeScalarPositions* positions)
{
    og_lake_scalar_result_close(&positions->sorted);
#ifdef USE_LAKE_BITMAP
    og_lake_bitmap_result_close(&positions->bitmap);
#endif
}

void LakeScalarFileSelectionClose(LakeFileScalarSelection* selection)
{
    if(selection==NULL)return;
    HOLD_INTERRUPTS();og_lake_scalar_reader_close(&selection->pending_reader);
#ifdef USE_LAKE_BITMAP
    og_lake_bitmap_reader_close(&selection->pending_bitmap_reader);
#endif
    for(int i=0;i<selection->files;++i)LakeScalarPositionsClose(&selection->positions[i]);
    RESUME_INTERRUPTS();
}

static void LakeScalarSelectionCleanup(std::shared_ptr<void> pointer)
{
    LakeScalarFileSelectionClose((LakeFileScalarSelection*)pointer.get());
}

bool LakeScalarFileCovered(const LakeFileScalarSelection* selection,int fileIndex)
{
    if(!selection||fileIndex<0||fileIndex>=selection->files)return false;
    const LakeScalarPositions* positions=&selection->positions[fileIndex];
#ifdef USE_LAKE_BITMAP
    if(positions->bitmap)return true;
#endif
    return positions->sorted!=NULL;
}

bool LakeScalarFileBitmap(const LakeFileScalarSelection* selection,int fileIndex)
{
#ifdef USE_LAKE_BITMAP
    return LakeScalarFileCovered(selection,fileIndex)&&selection->positions[fileIndex].bitmap!=NULL;
#else
    return false;
#endif
}

int64 LakeScalarFilePositionCount(const LakeFileScalarSelection* selection,int fileIndex)
{
    if(!LakeScalarFileCovered(selection,fileIndex))return 0;
    const LakeScalarPositions* positions=&selection->positions[fileIndex];
#ifdef USE_LAKE_BITMAP
    if(positions->bitmap)return og_lake_bitmap_result_count(positions->bitmap);
#endif
    return og_lake_scalar_result_count(positions->sorted);
}

static const OgLakeScalarPosition* LakeScalarPositionValues(const LakeFileScalarSelection* selection,int fileIndex)
{
    const LakeScalarPositions* positions=&selection->positions[fileIndex];
#ifdef USE_LAKE_BITMAP
    if(positions->bitmap)return og_lake_bitmap_result_positions(positions->bitmap);
#endif
    return og_lake_scalar_result_positions(positions->sorted);
}

int64 LakeScalarFilePosition(const LakeFileScalarSelection* selection,int fileIndex,int64 index)
{
    if(!LakeScalarFileCovered(selection,fileIndex)||index<0||index>=LakeScalarFilePositionCount(selection,fileIndex))
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid lake scalar candidate position")));
    return LakeScalarPositionValues(selection,fileIndex)[index].row_position;
}

bool LakeScalarFileContains(const LakeFileScalarSelection* selection,int fileIndex,int64 position)
{
    if(!LakeScalarFileCovered(selection,fileIndex))return true;
    int64 low=0,high=LakeScalarFilePositionCount(selection,fileIndex);
    const OgLakeScalarPosition* values=LakeScalarPositionValues(selection,fileIndex);
    while(low<high){int64 mid=low+(high-low)/2;if(values[mid].row_position<position)low=mid+1;else high=mid;}
    return low<LakeScalarFilePositionCount(selection,fileIndex)&&values[low].row_position==position;
}


/* Consumer/serial-backend metadata phase. No SDK reader is retained here.
 * All strings are copied out of the SPI result into the supplied owner. */
LakeScalarFileSources* LakePrepareScalarFileSources(LakeInlineBinding* binding,const char* uuid,
    AttrNumber column,char* const* objectVersions,MemoryContext owner,Size maxBytes)
{
    if(!binding||!uuid||column<1||column>binding->column_count)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid lake scalar source binding")));
    Size bytes=sizeof(LakeScalarFileSources)+sizeof(LakeScalarFileSource)*(Size)Max(1,binding->file_count)+strlen(uuid)+1;
    if(maxBytes&&bytes>maxBytes)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("lake SMP scalar descriptors exceed bounded memory")));
    auto* result=(LakeScalarFileSources*)MemoryContextAllocZero(owner,sizeof(LakeScalarFileSources));
    result->index_uuid=MemoryContextStrdup(owner,uuid);result->column_id=binding->columns[column-1].column_id;
    result->type=binding->columns[column-1].type;result->files=binding->file_count;
    result->entries=(LakeScalarFileSource*)MemoryContextAllocZero(owner,sizeof(LakeScalarFileSource)*Max(1,result->files));
    if(!LakeHasIndexRootVersion()||!objectVersions||!binding->file_count||
       !(LakeFileProvider()->capabilities&OG_LAKE_SDK_OBJECT_VERSION))return result;
            Datum* ids=(Datum*)palloc(sizeof(Datum)*binding->file_count);
            for(int i=0;i<binding->file_count;++i)ids[i]=Int64GetDatum(binding->files[i].file_id);
            Oid types[3]={UUIDOID,UUIDOID,INT8ARRAYOID};Datum args[3]={DirectFunctionCall1(uuid_in,CStringGetDatum(uuid)),
                DirectFunctionCall1(uuid_in,CStringGetDatum(binding->binding_id)),
                PointerGetDatum(construct_array(ids,binding->file_count,INT8OID,8,true,'d'))};
            Snapshot snapshot=RegisterSnapshot(GetLatestSnapshot());LakeMetaRows* rows;LakeMetaRows* roots;
            PG_TRY();
            {
                rows=LakeMetadataQuery("SELECT d.definition_generation,d.definition_hash::text,d.column_id,d.type_oid,d.table_uuid::text,"
                    "d.server_oid,s.segment_uuid::text,s.build_token::text,s.source_manifest_hash::text,s.manifest_hash::text,"
                    "s.artifact_root_uri::text,c.source_file_id,c.source_checksum::text,c.schema_mapping_hash::text,"
                    "c.physical_row_count,c.source_file_bytes,c.source_path::text,d.definition->>'method' FROM og_lake_internal.index_definition d "
                    "JOIN og_lake_internal.index_segment s USING(index_uuid,definition_generation) "
                    "JOIN og_lake_internal.index_coverage c USING(segment_uuid,index_uuid,definition_generation) "
                    "WHERE d.index_uuid=$1 AND d.binding_id=$2 AND d.state='ACTIVE' AND s.state='READY' "
                    "AND d.definition->>'kind'='scalar' AND d.definition->>'method' IN ('btree','bitmap') "
                    "AND s.algorithm_version=CASE WHEN d.type_oid IN (700,701) THEN 2 ELSE 1 END "
                    "AND s.format_version=CASE WHEN d.definition->>'method'='bitmap' THEN 2 ELSE 1 END "
                    "AND c.active AND c.source_file_id=ANY($3)",
                    3,types,args,NULL,snapshot,true,SPI_OK_SELECT);
                Oid rootType=OIDOID;Datum rootArg=ObjectIdGetDatum(binding->server_oid);
                roots=LakeMetadataQuery("SELECT index_root::text FROM og_lake_internal.catalog_binding WHERE server_oid=$1",
                    1,&rootType,&rootArg,NULL,snapshot,true,SPI_OK_SELECT);UnregisterSnapshot(snapshot);
            }
            PG_CATCH();{UnregisterSnapshot(snapshot);PG_RE_THROW();}PG_END_TRY();
            LakeMetadataRequireCount(roots,1,"scalar read store");bool noRoot=false;
            Datum rootValue=LakeMetadataValue(roots,0,1,TEXTOID,true,&noRoot);
            const char* root=noRoot?NULL:TextDatumGetCString(rootValue);char* schema=LakeScalarSchemaHash(binding);
    for(uint32 r=0;r<rows->count&&root!=NULL;++r){
        CHECK_FOR_INTERRUPTS();int f=-1;int64 fileId=DatumGetInt64(LakeMetadataValue(rows,r,12,INT8OID));
        for(int i=0;i<binding->file_count;++i)if(binding->files[i].file_id==fileId)f=i;
        if(f<0||objectVersions[f]==NULL||result->entries[f].object_version)continue;
        bool bitmap=strcmp(LakeMetadataText(rows,r,18),"bitmap")==0;
#ifndef USE_LAKE_BITMAP
        if(bitmap)continue;
#endif
        LakeDataFile* file=&binding->files[f];
        const char* version=objectVersions[f];
        const char* segment=LakeMetadataText(rows,r,7);char* directory=psprintf("%s%s",root,segment);
        char* source=LakeScalarSourceHash(file,version,schema);
        if(DatumGetInt64(LakeMetadataValue(rows,r,3,INT8OID))!=binding->columns[column-1].column_id||
            DatumGetObjectId(LakeMetadataValue(rows,r,4,OIDOID))!=binding->columns[column-1].type||
            strcmp(LakeMetadataText(rows,r,5),binding->table_uuid)!=0||
            DatumGetObjectId(LakeMetadataValue(rows,r,6,OIDOID))!=binding->server_oid||
            strcmp(LakeMetadataText(rows,r,9),source)!=0||strcmp(LakeMetadataText(rows,r,11),directory)!=0||
            strcmp(LakeMetadataText(rows,r,13),psprintf("object-version-v1:%s",version))!=0||
            strcmp(LakeMetadataText(rows,r,14),schema)!=0||
            DatumGetInt64(LakeMetadataValue(rows,r,15,INT8OID))!=file->record_count||
            DatumGetInt64(LakeMetadataValue(rows,r,16,INT8OID))!=file->file_bytes||
            strcmp(LakeMetadataText(rows,r,17),file->path)!=0)continue;
        const char* context=LakeScalarContext(uuid,DatumGetInt64(LakeMetadataValue(rows,r,1,INT8OID)),
            LakeMetadataText(rows,r,2),segment,LakeMetadataText(rows,r,8),source,schema);
        const char* manifest=LakeMetadataText(rows,r,10);
        Size item=strlen(version)+strlen(directory)+strlen(context)+strlen(manifest)+4;
        if(maxBytes&&(item>maxBytes||bytes>maxBytes-item))
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("lake SMP scalar descriptors exceed bounded memory")));
        bytes+=item;auto* entry=&result->entries[f];entry->file_id=fileId;entry->bitmap=bitmap;
        entry->object_version=MemoryContextStrdup(owner,version);entry->directory=MemoryContextStrdup(owner,directory);
        entry->context_hash=MemoryContextStrdup(owner,context);entry->manifest_hash=MemoryContextStrdup(owner,manifest);
    }
    return result;
}

LakeFileScalarSelection* LakeSelectScalarFiles(LakeInlineBinding* binding,const char* uuid,AttrNumber column,
    const ScanKeyData* key,char* const* objectVersions,MemoryContext context,const LakeScalarFileSources* prebound)
{
    MemoryContext caller=MemoryContextSwitchTo(context);
    LakeFileScalarSelection* selection=(LakeFileScalarSelection*)palloc0(sizeof(LakeFileScalarSelection));
    selection->files=binding->file_count;
    selection->positions=(LakeScalarPositions*)palloc0(sizeof(LakeScalarPositions)*Max(1,binding->file_count));
    MemoryContextCallback* cleanup=(MemoryContextCallback*)palloc(sizeof(MemoryContextCallback));
    new(cleanup) MemoryContextCallback();cleanup->func=LakeScalarSelectionCleanup;
    cleanup->arg=std::shared_ptr<void>(std::shared_ptr<void>(),selection);
    MemoryContextRegisterResetCallback(context,cleanup);
    PG_TRY();
    {
        if((prebound||LakeHasIndexRootVersion())&&binding->file_count>0&&objectVersions&&key&&column>0&&column<=binding->column_count&&
            (LakeFileProvider()->capabilities&OG_LAKE_SDK_OBJECT_VERSION)){
            OgLakeSdkLimits limits=LakeFileLimits();int predicate=key->sk_strategy;
            int absent=(key->sk_flags&SK_ISNULL)?1:0;int64 count=0,argumentBytes=0;
            int64* values=NULL;bool argumentsFit=true;
            if(key->sk_flags&SK_SEARCHNULL){predicate=OG_LAKE_SCALAR_IS_NULL;absent=0;}
            else if(key->sk_flags&SK_SEARCHNOTNULL){predicate=OG_LAKE_SCALAR_NOT_NULL;absent=0;}
            else if(key->sk_flags&SK_SEARCHARRAY){
                predicate=OG_LAKE_SCALAR_IN;
                if(!absent){
                    ArrayType* array=DatumGetArrayTypeP(key->sk_argument);
                    Oid type=ARR_ELEMTYPE(array);int16 length;bool byValue;char alignment;
                    get_typlenbyvalalign(type,&length,&byValue,&alignment);
                    int entries=ArrayGetNItems(ARR_NDIM(array),ARR_DIMS(array));
                    argumentsFit=(uint64)entries<=(uint64)limits.memory_bytes/(2*sizeof(Datum)+sizeof(bool));
                    if(argumentsFit){
                        Datum* elements;bool* nulls;deconstruct_array(array,type,length,byValue,alignment,&elements,&nulls,&entries);
                        values=(int64*)palloc(sizeof(int64)*Max(entries,1));
                        argumentBytes=sizeof(int64)*(int64)Max(entries,1);
                        for(int i=0;i<entries;++i)if(!nulls[i])values[count++]=LakeScalarOrderKey(elements[i],type);
                        if(elements)pfree(elements);if(nulls)pfree(nulls);
                    }
                    if((Pointer)array!=DatumGetPointer(key->sk_argument))pfree(array);
                }
            }else if(!absent){values=(int64*)palloc(sizeof(int64));argumentBytes=sizeof(int64);
                values[count++]=LakeScalarOrderKey(key->sk_argument,key->sk_subtype);}
            if(argumentsFit){
            const LakeScalarFileSources* sources=prebound?prebound:
                LakePrepareScalarFileSources(binding,uuid,column,objectVersions,context);
            if(sources->files!=binding->file_count||sources->column_id!=binding->columns[column-1].column_id||
               sources->type!=binding->columns[column-1].type||strcmp(sources->index_uuid,uuid)!=0)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("lake scalar descriptor binding changed")));
            int64 retained=0;
            for(int f=0;f<binding->file_count;++f){
                CHECK_FOR_INTERRUPTS();const auto* descriptor=&sources->entries[f];
                if(!descriptor->object_version||!objectVersions[f]||LakeScalarFileCovered(selection,f))continue;
                LakeDataFile* file=&binding->files[f];int64 fileId=file->file_id;bool bitmap=descriptor->bitmap;
                if(descriptor->file_id!=fileId||strcmp(descriptor->object_version,objectVersions[f])!=0)continue;
#ifndef USE_LAKE_BITMAP
                if(bitmap)continue;
#else
                if(bitmap&&count>OG_LAKE_BITMAP_MAX_KEYS)continue;
#endif
                char* version=LakeFileObjectVersion(binding,f);
                if(version==NULL||strcmp(version,objectVersions[f])!=0)continue;
                const char* directory=descriptor->directory;
                OgLakeSdkLimits available=limits;available.memory_bytes-=retained+argumentBytes;
                if(available.memory_bytes<1024*1024)continue;
                available.footer_bytes=Min(available.footer_bytes,available.memory_bytes);
                const char* contextHash=descriptor->context_hash;
                uint32 type=LakeScalarStorageType(binding->columns[column-1].type);
                OgLakeSdkError error;int code;
#ifdef USE_LAKE_BITMAP
                if(bitmap){
                    OgLakeBitmapFileSpec spec=LakeScalarBitmapSpec(binding->columns[column-1].type,file,contextHash);
                    const char* data=psprintf("%s/%s",directory,OG_LAKE_BITMAP_DATA_FILE);
                    HOLD_INTERRUPTS();code=og_lake_bitmap_reader_open(LakeFileProvider(),&available,&spec,
                        descriptor->manifest_hash,data,&selection->pending_bitmap_reader,&error);RESUME_INTERRUPTS();
                    CHECK_FOR_INTERRUPTS();
                    if(code==OG_LAKE_SDK_OK){
                        int64 nullValue=0;const int64* bitmapValues=values;int64 bitmapCount=count;
                        /* A NULL comparison still has one (ignored) argument. */
                        if(absent&&predicate<=5&&count==0){bitmapValues=&nullValue;bitmapCount=1;}
                        HOLD_INTERRUPTS();code=og_lake_bitmap_select(selection->pending_bitmap_reader,predicate,absent,
                            bitmapValues,bitmapCount,&selection->positions[f].bitmap,&error);RESUME_INTERRUPTS();CHECK_FOR_INTERRUPTS();
                    }
                    HOLD_INTERRUPTS();og_lake_bitmap_reader_close(&selection->pending_bitmap_reader);RESUME_INTERRUPTS();
                }else
#endif
                {
                    const char* data=psprintf("%s/%s",directory,OG_LAKE_SCALAR_DATA_FILE);
                    const char* lookup=psprintf("%s/%s",directory,OG_LAKE_SCALAR_LOOKUP_FILE);
                    HOLD_INTERRUPTS();code=og_lake_scalar_reader_open(LakeFileProvider(),&available,type,contextHash,
                        descriptor->manifest_hash,data,lookup,&selection->pending_reader,&error);RESUME_INTERRUPTS();
                    CHECK_FOR_INTERRUPTS();
                    if(code==OG_LAKE_SDK_OK){
                        int64 nullValue=0;const int64* scalarValues=values;int64 scalarCount=count;
                        /* A strict NULL comparison still has one ignored RHS. */
                        if(absent&&predicate<=5&&count==0){scalarValues=&nullValue;scalarCount=1;}
                        HOLD_INTERRUPTS();code=og_lake_scalar_select(selection->pending_reader,predicate,absent,scalarValues,scalarCount,
                            &selection->positions[f].sorted,&error);RESUME_INTERRUPTS();CHECK_FOR_INTERRUPTS();
                    }
                    HOLD_INTERRUPTS();og_lake_scalar_reader_close(&selection->pending_reader);RESUME_INTERRUPTS();
                }
                if(code==OG_LAKE_SDK_CANCELLED)LakeFileCheckStatus(code,&error);
                bool valid=code==OG_LAKE_SDK_OK&&LakeScalarFileCovered(selection,f);
                int64 total=valid?LakeScalarFilePositionCount(selection,f):0;
                const OgLakeScalarPosition* entries=valid?LakeScalarPositionValues(selection,f):NULL;
                if(total<0||total>file->record_count||(total>0&&entries==NULL)||total>available.memory_bytes/(2*(int64)sizeof(*entries)))valid=false;
                for(int64 i=0;valid&&i<total;++i)
                    if(entries[i].file_id!=fileId||entries[i].row_position<0||entries[i].row_position>=file->record_count||
                        (i>0&&entries[i].row_position<=entries[i-1].row_position))valid=false;
                if(valid)retained+=Max((int64)1024,total*2)*(int64)sizeof(*entries);
                else{HOLD_INTERRUPTS();LakeScalarPositionsClose(&selection->positions[f]);RESUME_INTERRUPTS();}
            }
            }
            if(values)pfree(values);
        }
        MemoryContextSwitchTo(caller);
    }
    PG_CATCH();{MemoryContextSwitchTo(caller);LakeScalarFileSelectionClose(selection);PG_RE_THROW();}PG_END_TRY();
    return selection;
}

void LakeScalarFileCoverageEstimate(const LakeInlineBinding* binding,const char* uuid,AttrNumber column,
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
        "c.schema_mapping_hash::text,d.column_id,d.type_oid,d.table_uuid::text,d.definition->>'method' FROM og_lake_internal.index_definition d "
        "JOIN og_lake_internal.index_segment s USING(index_uuid,definition_generation) "
        "JOIN og_lake_internal.index_coverage c USING(segment_uuid,index_uuid,definition_generation) "
        "WHERE d.index_uuid=$1 AND d.binding_id=$2 AND d.state='ACTIVE' AND s.state='READY' "
        "AND d.definition->>'kind'='scalar' AND d.definition->>'method' IN ('btree','bitmap') "
        "AND s.format_version=CASE WHEN d.definition->>'method'='bitmap' THEN 2 ELSE 1 END "
        "AND s.algorithm_version=CASE WHEN d.type_oid IN (700,701) THEN 2 ELSE 1 END AND c.active AND c.source_file_id=ANY($3)",
        3,types,args,NULL,snapshot,true,SPI_OK_SELECT);
    char* schema=LakeScalarSchemaHash(binding);
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
#ifndef USE_LAKE_BITMAP
            if(ledger&&strcmp(LakeMetadataText(rows,r,9),"bitmap")==0)continue;
#endif
            if(ledger&&!LakeCostCover(ledger,f))continue;
            *rowCount+=(double)file->record_count;*byteCount+=(double)file->file_bytes;
            *groupCount+=(double)Max((int64)1,file->row_group_count);++*fileCount;
        }
    }
}

void LakeScalarDescribeMaintenanceTask(LakeIndexArtifactTask* task)
{
    task->version=LakeFileObjectVersion(task->binding,0);
    if(task->version==NULL)ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("background scalar source lacks an immutable-object token")));
    task->schema=LakeScalarSchemaHash(task->binding);
    task->source=LakeScalarSourceHash(&task->binding->files[0],task->version,task->schema);
    task->context=LakeScalarContext(task->uuid,task->generation,task->definition_hash,task->segment,task->token,task->source,task->schema);
    task->directory=psprintf("%s%s",task->root,task->segment);
    task->algorithm=LakeScalarAlgorithm(task->binding->columns[task->column].type);task->format=task->bitmap?2:1;
}
LakeIndexArtifactHealth LakeScalarCheckMaintenanceTask(LakeIndexArtifactTask* task)
{
    if(!LakeIndexArtifactMetadata(task)||task->known_algorithm!=task->algorithm||task->known_format!=task->format)return LAKE_ARTIFACT_REBUILD;
    const char* names[2]={OG_LAKE_SCALAR_DATA_FILE,OG_LAKE_SCALAR_LOOKUP_FILE};
#ifdef USE_LAKE_BITMAP
    if(task->bitmap)names[0]=OG_LAKE_BITMAP_DATA_FILE;
#else
    if(task->bitmap)return LAKE_ARTIFACT_RETRY;
#endif
    LakeIndexArtifactHealth files=LakeIndexArtifactFiles(task->known_directory,names,task->bitmap?1:2);
    if(files!=LAKE_ARTIFACT_VALID)return files;
    const char* context=LakeScalarContext(task->uuid,task->generation,task->definition_hash,
        task->known_segment,task->known_token,task->source,task->schema);
    int status=OG_LAKE_SDK_OK;
    bool valid=LakeScalarReadyVerified(LakeFileProvider(),task->binding->columns[task->column].type,
        context,task->known_manifest,task->known_directory,&task->binding->files[0],task->bitmap,&status);
    return valid?LAKE_ARTIFACT_VALID:status==OG_LAKE_SDK_INVALID?LAKE_ARTIFACT_REBUILD:LAKE_ARTIFACT_RETRY;
}
void LakeScalarBuildMaintenanceTask(LakeIndexArtifactTask* task)
{
    LakeFileCandidate* data=NULL;LakeFileCandidate* lookup=NULL;LakeScalarBuildOutput written={};
    MemoryContext owner=CurrentMemoryContext;
    LakeScalarBuildSegment(task->binding,0,task->column,task->version,task->context,task->directory,&data,&lookup,&written,task->bitmap);
    MemoryContextSwitchTo(owner);
    task->manifest=pstrdup(written.manifest_hash);task->candidates[0]=data;task->candidate_count=1;
    if(lookup)task->candidates[task->candidate_count++]=lookup;
}
