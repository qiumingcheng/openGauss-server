/* Native tuple materialization from the provider C Data batches. */
#include "postgres.h"
#include "lake_uuid.h"
#include "lake_json.h"
#include "lake_binary.h"
#include "lake_version.h"
#include "lake_column_layout.h"
#include "lake_column_default.h"
#include "lake_temporal.h"
#include "lake_scalar_key.h"
#include "lake_types.h"
#include "lake_vector_codec.h"
#include "lake_file_reader.h"
#include "lake_file_sdk.h"
#include "lake_file_scalar.h"
#include "lake_file_ann_catalog.h"
#include "lake_ann_format.h"
#include "lake_file_ann.h"
#include "lake_ann_rescan.h"
#include "lake_inline_reader.h"
#include "lake_inline_sources.h"
#include "lake_locator.h"
#include "lake_txn_private.h"
#include "access/transam.h"
#include "access/tuptoaster.h"
#include "catalog/pg_type.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"
#include "vecexecutor/vectorbatch.h"

struct LakeFileState {
    bool admitted;
    bool partition_skipped;
    bool explicit_row_id;
    bool explicit_snapshot;
    bool rewritten_birth_upper;
    int present_columns;
    bool retired_fields;
    int64 retired_birth_min;
    int64 retired_birth_end;
    int* logical_to_batch; /* Current compact field index -> present Arrow child; -1 proven NULL. */
    Oid* field_types; /* Physical Arrow decoded Oid per compact field; InvalidOid if absent. */
    int32* field_typmods; /* Physical Arrow decoded typmod per compact field. */
    int64* present_field_ids; /* Complete present fields plus the two existing hidden slots. */
    char* object_version;
    bool indexed;
    int group_count;
    int32* groups;
    int64* group_first;
    int64* group_rows;
    int64 selected_rows;
    int delete_count;
    int64* deleted_positions;
    bool projection_built;
    int open_columns;
    int* open_to_batch;
    int64* open_field_ids;
};
struct LakeFileReader {
    LakeInlineBinding* binding;
    LakeTxnTable* table;
    CommandId command_limit;
    LakeSdkHandles* handles;
    LakeFileState* states;
    int64* field_ids;
    int file_index;
    int64 batch_offset;
    int64 position;
    int group_cursor;
    int64 group_offset;
    LakeAnnRescanCache* ann_rescan_cache;
    MemoryContext candidate_context;
    LakeFileScalarSelection* selection;
    LakeFileAnnSelection* ann_selection;
    char* const* expected_versions;
    MemoryContext context;
    const int64* sample_positions;
    int sample_position_count;
    LakeAnalyzeIoBudget* analyze_budget;
    LakeSdkHandles* admission_handles; /* reused across parameter rescans */
    int admitted_files; /* distinct sources validated during this reader lifetime */
    bool deferred_admission;
    bool admission_pending;
    bool verify_rewrite_partition; /* default false, never enabled by scans/workers */
    MemoryContext rewrite_partition_scratch;
    bool metadata_prebound; /* consumer already checked the installed SQL capability */
    OgLakeSdkIoStats sample_last_io;
    bool* projected;
};

template<class Function>static auto LakeSdkCall(Function function)->decltype(function())
{
    /* Even immediate signal handlers must not longjmp across provider C++.
     * A broken provider exception is converted only after its stack unwinds. */
    HOLD_INTERRUPTS();
    try{
        auto result=function();RESUME_INTERRUPTS();return result;
    }catch(...){RESUME_INTERRUPTS();}
    ereport(ERROR,(errcode(ERRCODE_FDW_ERROR),errmsg("native lake provider violated its C exception boundary")));
    return decltype(function())();
}

static void LakeAnalyzeCheckIo(LakeAnalyzeIoBudget* budget)
{
    if(budget==NULL)return;
    CHECK_FOR_INTERRUPTS();
    if(budget->files>1024||budget->groups>8192||budget->planned_decoded_bytes>budget->max_decoded_bytes||
       budget->planned_bytes>budget->max_plan_bytes||budget->reserved_footer_bytes>budget->max_plan_bytes-budget->planned_bytes||
       budget->observed_reader_bytes>budget->max_read_bytes||
       TimestampDifferenceExceeds(budget->started,GetCurrentTimestamp(),budget->max_milliseconds))
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake ANALYZE sampling I/O budget exhausted"),
            errdetail("files=%u groups=%u reader_bytes=%llu planned_bytes=%llu footer_reserve=%llu; independent footer I/O is unobserved",
                budget->files,budget->groups,(unsigned long long)budget->observed_reader_bytes,
                (unsigned long long)budget->planned_bytes,(unsigned long long)budget->reserved_footer_bytes)));
}

static void LakeAnalyzeAddIo(LakeAnalyzeIoBudget* budget,const OgLakeSdkIoStats* value,const OgLakeSdkIoStats* prior=NULL)
{
    if(budget==NULL)return;
    OgLakeSdkIoStats zero={};if(prior==NULL)prior=&zero;
    if(value->bytes_read<prior->bytes_read||value->read_calls<prior->read_calls||value->decoded_rows<prior->decoded_rows)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake SDK reader counters moved backwards")));
    uint64 bytes=value->bytes_read-prior->bytes_read,calls=value->read_calls-prior->read_calls,rows=value->decoded_rows-prior->decoded_rows;
    if(bytes>PG_UINT64_MAX-budget->observed_reader_bytes||calls>PG_UINT64_MAX-budget->observed_reader_calls||rows>PG_UINT64_MAX-budget->observed_decoded_rows)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake ANALYZE I/O counters overflow")));
    budget->observed_reader_bytes+=bytes;budget->observed_reader_calls+=calls;budget->observed_decoded_rows+=rows;
    LakeAnalyzeCheckIo(budget);
}

static void LakeAnalyzeObserveReader(LakeFileReader* reader)
{
    if(reader->analyze_budget==NULL||reader->handles->reader==NULL)return;
    OgLakeSdkIoStats now={};
    LakeSdkCall([&]{reader->handles->api->reader_stats(reader->handles->reader,&now);return 0;});
    LakeAnalyzeAddIo(reader->analyze_budget,&now,&reader->sample_last_io);reader->sample_last_io=now;
}

static uint32 LakeSdkColumnType(Oid type)
{
    return LakeNativeTypeByOid(type)->sdk_type;
}

static const OgLakeSdkFooterInfo* LakeCheckFooter(LakeSdkHandles* handles,const char* path,
    int64 rows,int64 bytes,int64 groups)
{
    OgLakeSdkError error;OgLakeSdkLimits limits=LakeFileLimits();
    LakeFileCheckStatus(LakeSdkCall([&]{return handles->api->footer_open(path,&limits,&handles->footer,&error);}),&error);
    const OgLakeSdkFooterInfo* footer=LakeSdkCall([&]{return handles->api->footer_info(handles->footer);});
    /* ABI1 readers need only the original prefix; the optional footer-length
     * suffix is required separately by writers which publish file metadata. */
    if(footer==NULL||footer->struct_size<offsetof(OgLakeSdkFooterInfo,footer_bytes)||footer->num_rows!=rows||footer->file_bytes!=bytes||
        (groups>=0&&footer->num_row_groups!=groups))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake file footer disagrees with its catalog")));
    return footer;
}

static int LakeFindFieldOrdinal(const OgLakeSdkFooterInfo* footer,int64 id)
{
    int found=-1;
    for(int i=0;i<footer->num_fields;++i){
        if(footer->fields[i].field_id!=id)continue;
        if(found>=0)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("duplicate or incompatible native lake file field")));
        found=i;
    }
    return found;
}

static int LakeFindField(const OgLakeSdkFooterInfo* footer,int64 id,uint32 type)
{
    int found=LakeFindFieldOrdinal(footer,id);
    if(found>=0&&footer->fields[found].type!=type)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("duplicate or incompatible native lake file field")));
    return found;
}

static void LakeCheckFooterFieldMatch(const LakeInlineBinding* binding,const OgLakeSdkFooter* footerHandle,
    const OgLakeSdkFooterInfo* footer,const OgLakeParquetSdkApi* api,int ordinal,
    Oid expectedType,int32 expectedTypmod,const char* errMsg,Oid* actualTypeOut,int32* actualTypmodOut)
{
    uint32 actualSdk=footer->fields[ordinal].type;
    Oid actualType=expectedType;
    if(actualSdk!=LakeSdkColumnType(expectedType)){
        if(!binding->alter_type_evolution)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("%s",errMsg)));
        actualType=LakeNativeTypeBySdk(actualSdk)->oid;
        if(actualType==expectedType||!LakeIsWideningPromotion(actualType,-1,expectedType,expectedTypmod))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("%s",errMsg)));
    }
    LakeNativeTypeRequireSdk(actualType,api);
    int32 actualTypmod=(expectedType==VECTOROID&&actualType==VECTOROID)?expectedTypmod:-1;
    if(actualType==NUMERICOID){
        if(!binding->alter_type_evolution){
            LakeFileCheckDecimal(footerHandle,ordinal,expectedTypmod);
            actualTypmod=expectedTypmod;
        }else{
            int p=0,s=0;
            LakeFileReadDecimalParameters(footerHandle,ordinal,&p,&s);
            actualTypmod=(int32)(((uint32)p<<16)|(uint16)s)+VARHDRSZ;
            if(!LakeIsTypeCompatibleOrPromoted(NUMERICOID,actualTypmod,expectedType,expectedTypmod))
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake decimal footer parameters differ from its column history")));
        }
    }
    if(actualTypeOut)*actualTypeOut=actualType;
    if(actualTypmodOut)*actualTypmodOut=actualTypmod;
}

static void LakeValidateFileFooter(LakeFileReader* reader,int index)
{
    LakeDataFile* file=&reader->binding->files[index];LakeFileState* state=&reader->states[index];
    LakeSdkHandles* handles=reader->handles;
    for(int i=0;i<reader->binding->column_count;++i)
        LakeNativeTypeRequireSdk(reader->binding->columns[i].type,handles->api);
    for(int i=0;i<reader->binding->retired_count;++i)
        if(reader->binding->retired_columns[i].column.type==UUIDOID||
           reader->binding->retired_columns[i].column.type==JSONOID||
           reader->binding->retired_columns[i].column.type==NUMERICOID)
            LakeNativeTypeRequireSdk(reader->binding->retired_columns[i].column.type,handles->api);
    const OgLakeSdkFooterInfo* footer=LakeCheckFooter(handles,file->path,file->record_count,file->file_bytes,file->row_group_count);
    state->explicit_row_id=LakeFindField(footer,LAKE_FILE_ROW_ID,OG_LAKE_SDK_INT64)>=0;
    state->explicit_snapshot=LakeFindField(footer,LAKE_FILE_SNAPSHOT_ID,OG_LAKE_SDK_INT64)>=0;
    /* Historical and bound queries also use read-only views. The catalog
     * owner captured the capability before any reader or Stream worker opened
     * a file; no worker may infer it by querying pg_extension. The footer
     * and each actual row still have to satisfy the recorded birth bounds. */
    state->rewritten_birth_upper=reader->binding->rewrite_birth_bound&&state->explicit_row_id&&
        state->explicit_snapshot&&file->partial_max<0&&file->row_id_start_null;
    int logicalCount=reader->binding->column_count;
    /* This is a reader-owned projection, never a mutation of a cached binding. */
    if(!state->logical_to_batch){
        Size bytes=(Size)logicalCount*(sizeof(int)+sizeof(Oid)+sizeof(int32))+(Size)(logicalCount+2)*sizeof(int64);
        if(reader->binding->column_evolution&&
           (Size)reader->binding->file_count>64UL*1024UL*1024UL/bytes)
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake file projection budget exceeded")));
        state->logical_to_batch=(int*)palloc(sizeof(int)*logicalCount);
        state->field_types=(Oid*)palloc0(sizeof(Oid)*logicalCount);
        state->field_typmods=(int32*)palloc(sizeof(int32)*logicalCount);
        state->present_field_ids=(int64*)palloc(sizeof(int64)*(logicalCount+2));
    }
    state->present_columns=0;
    for(int i=0;i<reader->binding->column_count;++i){
        const LakeInlineColumn* column=&reader->binding->columns[i];
        int ord=LakeFindFieldOrdinal(footer,column->column_id);
        if(ord<0){
            int64 upper=state->rewritten_birth_upper?file->begin_snapshot:(state->explicit_snapshot?file->partial_max:file->begin_snapshot);
            if(!LakeColumnCanBeMissing(reader->binding,i,upper))
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake file omission lacks a complete initial NULL proof")));
            state->logical_to_batch[i]=-1;
            state->field_types[i]=InvalidOid;
            state->field_typmods[i]=-1;
            continue;
        }
        Oid fileType=InvalidOid;int32 fileTypmod=-1;
        LakeCheckFooterFieldMatch(reader->binding,handles->footer,footer,handles->api,ord,
            column->type,column->typmod,"duplicate or incompatible native lake file field",
            &fileType,&fileTypmod);
        state->logical_to_batch[i]=state->present_columns;
        state->field_types[i]=fileType;
        state->field_typmods[i]=fileTypmod;
        state->present_field_ids[state->present_columns++]=column->column_id;
        if(column->type==VECTOROID){
            if(footer->struct_size<OG_LAKE_SDK_FIELD_END(OgLakeSdkFooterInfo,list_fields)||
                footer->num_list_fields<1||footer->num_list_fields>footer->num_fields||footer->list_fields==NULL)
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake vector footer has no element identity metadata")));
            int matched=0;
            for(int n=0;n<footer->num_list_fields;++n)if(footer->list_fields[n].field_id==column->column_id){
                if(footer->list_fields[n].element_field_id!=column->element_column_id||footer->list_fields[n].element_nullable>1)
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake vector element identity differs from its catalog")));
                ++matched;
            }
            if(matched!=1)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake vector footer has missing or duplicate parent identities")));
        }
    }
    state->retired_fields=false;state->retired_birth_min=0;state->retired_birth_end=PG_INT64_MAX;
    int expected=state->present_columns+(state->explicit_row_id?1:0)+(state->explicit_snapshot?1:0);
    for(int f=0;f<footer->num_fields;++f){
        int64 id=footer->fields[f].field_id;bool live=false;
        for(int earlier=0;earlier<f;++earlier)if(footer->fields[earlier].field_id==id)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("duplicate native lake footer field identity")));
        if(id==LAKE_FILE_ROW_ID||id==LAKE_FILE_SNAPSHOT_ID)continue;
        for(int c=0;c<logicalCount;++c)if(reader->binding->columns[c].column_id==id)live=true;
        if(live)continue;
        const LakeRetiredColumn* r=LakeFindRetiredColumn(reader->binding,id);
        if(!r){
            const LakeInlineColumn* future=LakeFindFutureColumn(reader->binding,id);
            if(future&&future->element_column_id<0&&
               future->introduced_snapshot>reader->binding->snapshot_id&&
               file->begin_snapshot<=reader->binding->snapshot_id){
                LakeCheckFooterFieldMatch(reader->binding,handles->footer,footer,handles->api,f,
                    future->type,future->typmod,"incompatible future native lake footer field",NULL,NULL);
                ++expected;continue;
            }
        }
        if(!r||r->column.element_column_id>=0)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("unknown or incompatible retired native lake footer field")));
        LakeCheckFooterFieldMatch(reader->binding,handles->footer,footer,handles->api,f,
            r->column.type,r->column.typmod,"unknown or incompatible retired native lake footer field",NULL,NULL);
        int64 upper=state->rewritten_birth_upper?file->begin_snapshot:(state->explicit_snapshot?file->partial_max:file->begin_snapshot);
        LakeValidateRetiredBirth(reader->binding,id,file->begin_snapshot,upper);
        state->retired_fields=true;state->retired_birth_min=Max(state->retired_birth_min,r->column.introduced_snapshot);
        state->retired_birth_end=Min(state->retired_birth_end,r->retired_snapshot);++expected;
    }
    if(footer->struct_size>=OG_LAKE_SDK_FIELD_END(OgLakeSdkFooterInfo,list_fields)){
        if(footer->num_list_fields<0||footer->num_list_fields>footer->num_fields||
           (footer->num_list_fields>0&&!footer->list_fields))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native list footer metadata")));
        for(int n=0;n<footer->num_list_fields;++n){
            bool recognized=false;
            for(int c=0;c<logicalCount;++c)if(reader->binding->columns[c].type==VECTOROID&&
                reader->binding->columns[c].column_id==footer->list_fields[n].field_id)recognized=true;
            if(!recognized)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("unknown native footer list parent")));
        }
    }
    if(footer->num_fields!=expected)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake evolved or unknown hidden file columns are not enabled")));
    if(!reader->metadata_prebound&&!(reader->table&&reader->table->stream_readonly)&&!LakeFileVersionedReadsEnabled()&&(state->explicit_row_id||state->explicit_snapshot))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake hidden file versions require extension 0.9")));
    if((file->row_id_start_null&&!state->explicit_row_id)||(file->partial_max>=0&&!state->explicit_snapshot))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake file is missing its required hidden identity or snapshot column")));
    if((handles->api->capabilities&OG_LAKE_SDK_OBJECT_VERSION)&&footer->struct_size>=OG_LAKE_SDK_FIELD_END(OgLakeSdkFooterInfo,object_version)&&
        footer->object_version!=NULL&&footer->object_version[0]!='\0'&&strlen(footer->object_version)<=512)
        state->object_version=pstrdup(footer->object_version);
    LakeSdkCall([&]{handles->api->footer_close(&handles->footer);return 0;});
}

char* LakeFileObjectVersion(LakeInlineBinding* binding,int fileIndex)
{
    if(fileIndex<0||fileIndex>=binding->file_count)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid lake file identity lookup")));
    LakeSdkHandles* handles=LakeFileHandles(CurrentMemoryContext);
    LakeDataFile* file=&binding->files[fileIndex];
    const OgLakeSdkFooterInfo* footer=LakeCheckFooter(handles,file->path,file->record_count,file->file_bytes,file->row_group_count);
    char* version=NULL;
    if((handles->api->capabilities&OG_LAKE_SDK_OBJECT_VERSION)&&footer->struct_size>=OG_LAKE_SDK_FIELD_END(OgLakeSdkFooterInfo,object_version)&&
        footer->object_version!=NULL&&footer->object_version[0]!='\0'&&strlen(footer->object_version)<=512)
        version=pstrdup(footer->object_version);
    LakeFileHandlesClose(handles);return version;
}

static bool LakeSdkNull(const ArrowArray* array,int64 row)
{
    if(array->buffers[0]==NULL){
        if(array->null_count>0)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake Arrow validity buffer is missing")));
        return false;
    }
    uint64 index=(uint64)(array->offset+row);
    return (((const uint8*)array->buffers[0])[index/8]>>(index%8)&1)==0;
}

/* Validate binary structure and the bounded logical index before either the
 * validity bitmap or offsets are dereferenced, including all-NULL columns.
 * Arrow C Data exposes no actual buffer extents; these remain SDK-owned. */
static int64 LakeCheckBinaryLayout(const ArrowArray* array,const ArrowSchema* schema,int64 row)
{
    if(array==NULL||schema==NULL||array->n_buffers!=3||array->buffers==NULL||schema->format==NULL||
       (strcmp(schema->format,"z")!=0&&strcmp(schema->format,"Z")!=0)||
       array->buffers[1]==NULL||row<0||array->offset<0||row>=array->length||
       array->offset>PG_INT64_MAX-row)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native lake Arrow binary layout")));
    int64 index=array->offset+row;
    Size width=strcmp(schema->format,"z")==0?sizeof(int32):sizeof(int64);
    if((uint64)index>MaxAllocSize/width-2)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake Arrow binary index exceeds its bound")));
    return index;
}

static Datum LakeSdkBinaryValue(const ArrowArray* array,const ArrowSchema* schema,int64 row,Size* rowBytes)
{
    int64 index=LakeCheckBinaryLayout(array,schema,row);
    Size width=strcmp(schema->format,"z")==0?sizeof(int32):sizeof(int64);
    int64 start,end;
    if(width==sizeof(int32)){start=((const int32*)array->buffers[1])[index];end=((const int32*)array->buffers[1])[index+1];}
    else{start=((const int64*)array->buffers[1])[index];end=((const int64*)array->buffers[1])[index+1];}
    if(start<0||end<start||end>(int64)MaxAllocSize||end-start>(int64)MaxAllocSize-VARHDRSZ||
       (end>start&&array->buffers[2]==NULL))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native lake Arrow binary offsets")));
    Size length=(Size)(end-start);
    LakeReserveBinaryBytes(length,rowBytes);
    bytea* value=(bytea*)palloc(VARHDRSZ+length);
    SET_VARSIZE(value,VARHDRSZ+length);
    if(length){errno_t rc=memcpy_s(VARDATA(value),length,(const char*)array->buffers[2]+start,length);securec_check(rc,"","");}
    return PointerGetDatum(value);
}

/* Validate even NULL UUID fields before touching their validity bitmap. SDK
 * owns actual C Data buffer extents; native code checks bounded index/shape. */
static int64 LakeCheckUuidLayout(const ArrowArray* array,const ArrowSchema* schema,int64 row)
{
    if(!array||!schema||!schema->format||strcmp(schema->format,"w:16")||
       array->n_buffers!=2||!array->buffers||array->offset<0||row<0||row>=array->length||
       array->offset>PG_INT64_MAX-row||
       (uint64)(array->offset+row)>((uint64)MaxAllocSize-UUID_LEN)/UUID_LEN||!array->buffers[1]||
       array->n_children!=0||schema->n_children!=0||array->dictionary||schema->dictionary)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake UUID Arrow value requires bounded fixed16 storage")));
    return array->offset+row;
}

/* Validate JSON offsets even for NULL rows before accessing validity bits.
 * As for existing C Data fields, the SDK owns the actual buffer capacities. */
static int64 LakeCheckJsonLayout(const ArrowArray* array,const ArrowSchema* schema,int64 row)
{
    if(!array||!schema||!schema->format||
       (strcmp(schema->format,"u")&&strcmp(schema->format,"U"))||
       array->n_buffers!=3||!array->buffers||!array->buffers[1]||
       array->offset<0||row<0||row>=array->length||
       array->offset>PG_INT64_MAX-row-1||
       (uint64)(array->offset+row)>((uint64)MaxAllocSize/
           (strcmp(schema->format,"u")==0?sizeof(int32):sizeof(int64)))-2||
       array->n_children!=0||schema->n_children!=0||array->dictionary||schema->dictionary)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake JSON Arrow field requires bounded UTF8 storage")));
    return array->offset+row;
}

/* Schema proof precedes validity even for an all-NULL decimal batch. */
static int64 LakeCheckNumericLayout(const ArrowArray* array,const ArrowSchema* schema,int64 row,int32 typmod)
{
    int p,s;LakeNumericParameters(typmod,&p,&s);char format[32],explicit128[36];
    int a=snprintf(format,sizeof(format),"d:%d,%d",p,s);
    int b=snprintf(explicit128,sizeof(explicit128),"d:%d,%d,128",p,s);
    if(a<0||b<0||a>=(int)sizeof(format)||b>=(int)sizeof(explicit128)||
       !array||!schema||!schema->format||(strcmp(schema->format,format)&&strcmp(schema->format,explicit128))||
       array->n_buffers!=2||!array->buffers||!array->buffers[1]||
       array->offset<0||row<0||row>=array->length||array->offset>PG_INT64_MAX-row||
       (uint64)(array->offset+row)>((uint64)MaxAllocSize-16)/16||
       array->n_children!=0||schema->n_children!=0||array->dictionary||schema->dictionary)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake numeric Arrow field requires exact bounded decimal128 parameters")));
    return array->offset+row;
}

static Datum LakeSdkValue(const ArrowArray* array,const ArrowSchema* schema,int64 row,Oid type,Size* binaryRowBytes=NULL,int32 typmod=-1)
{
    Size singleBinary=0;
    if(type==NUMERICOID){
        Size offset=(Size)LakeCheckNumericLayout(array,schema,row,typmod)*16;
        return LakeNumericFromCoefficient((const unsigned char*)array->buffers[1]+offset,
            typmod,binaryRowBytes?binaryRowBytes:&singleBinary);
    }
    if(type==JSONOID){
        int64 index=LakeCheckJsonLayout(array,schema,row),start,end;
        if(strcmp(schema->format,"u")==0){
            start=((const int32*)array->buffers[1])[index];
            end=((const int32*)array->buffers[1])[index+1];
        }else{
            start=((const int64*)array->buffers[1])[index];
            end=((const int64*)array->buffers[1])[index+1];
        }
        if(start<0||end<start||end>(int64)MaxAllocSize||
           end-start>(int64)MaxAllocSize-VARHDRSZ||
           (end>start&&!array->buffers[2]))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake JSON Arrow offsets are invalid")));
        const char* text=end==start?"":(const char*)array->buffers[2]+start;
        return LakeJsonFromUtf8Bytes(text,(Size)(end-start),
            binaryRowBytes?binaryRowBytes:&singleBinary);
    }
    if(type==BYTEAOID)return LakeSdkBinaryValue(array,schema,row,binaryRowBytes?binaryRowBytes:&singleBinary);
    if(type==UUIDOID){
        Size offset=(Size)LakeCheckUuidLayout(array,schema,row)*UUID_LEN;
        return LakeUuidFromBytes((const char*)array->buffers[1]+offset,
            binaryRowBytes?binaryRowBytes:&singleBinary);
    }
    int64 index=array->offset+row;
    const void* data=array->buffers[1];
    if(data==NULL)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake Arrow value buffer is missing")));
    if(type==BOOLOID&&strcmp(schema->format,"b")==0)return BoolGetDatum((((const uint8*)data)[index/8]>>(index%8)&1)!=0);
    if(type==INT2OID&&strcmp(schema->format,"s")==0)return Int16GetDatum(((const int16*)data)[index]);
    if(type==INT4OID&&strcmp(schema->format,"i")==0)return Int32GetDatum(((const int32*)data)[index]);
    if(type==INT8OID&&strcmp(schema->format,"l")==0)return Int64GetDatum(((const int64*)data)[index]);
    if(type==DATEOID&&strcmp(schema->format,"tdD")==0)
        return DateADTGetDatum(LakeTemporalDateFromUnix(((const int32*)data)[index]));
    if(type==TIMESTAMPOID&&strcmp(schema->format,"tsu:")==0)
        return TimestampGetDatum(LakeTemporalTimestampFromUnix(((const int64*)data)[index]));
    if(type==TIMESTAMPTZOID&&strcmp(schema->format,"tsu:UTC")==0)
        return TimestampTzGetDatum(LakeTemporalTimestampFromUnix(((const int64*)data)[index]));
    if(type==FLOAT4OID&&strcmp(schema->format,"f")==0)return Float4GetDatum(((const float4*)data)[index]);
    if(type==FLOAT8OID&&strcmp(schema->format,"g")==0)return Float8GetDatum(((const float8*)data)[index]);
    if(type!=TEXTOID||array->n_buffers!=3||(strcmp(schema->format,"u")!=0&&strcmp(schema->format,"U")!=0))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake Arrow type differs from its declared file type")));
    int64 start,end;
    if(strcmp(schema->format,"u")==0){start=((const int32*)data)[index];end=((const int32*)data)[index+1];}
    else{start=((const int64*)data)[index];end=((const int64*)data)[index+1];}
    if(start<0||end<start||end-start>(int64)MaxAllocSize-VARHDRSZ||(end>start&&array->buffers[2]==NULL))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native lake Arrow text offsets")));
    int length=(int)(end-start);const char* text=length?(const char*)array->buffers[2]+start:"";
    if(memchr(text,0,length)!=NULL)
        ereport(ERROR,(errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),errmsg("native lake text contains a zero byte")));
    (void)pg_verify_mbstr(PG_UTF8,text,length,false);
    char* converted=pg_any_to_server(text,length,PG_UTF8);
    return PointerGetDatum(cstring_to_text_with_len(converted,converted==text?length:strlen(converted)));
}

static OgLakeSdkBatch* LakeCheckBatch(OgLakeSdkBatch* batch,int columns)
{
    if(batch==NULL||batch->struct_size<offsetof(OgLakeSdkBatch,object_version)||batch->num_rows<=0||batch->num_rows>4096||
        batch->array==NULL||batch->schema==NULL||batch->original_positions==NULL||
        batch->array->length!=batch->num_rows||batch->array->offset!=0||
        batch->array->n_children!=columns||batch->schema->n_children!=columns||
        (columns>0&&(batch->array->children==NULL||batch->schema->children==NULL)))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native lake Arrow batch layout")));
    for(int i=0;i<columns;++i){
        ArrowArray* array=batch->array->children[i];ArrowSchema* schema=batch->schema->children[i];
        if(array==NULL||schema==NULL||schema->format==NULL||array->offset<0||array->length<batch->num_rows||
            array->offset>PG_INT64_MAX-array->length||array->n_buffers<2||array->buffers==NULL||
            array->dictionary!=NULL||schema->dictionary!=NULL||
            ((strcmp(schema->format,"+l")==0)?(array->n_children!=1||schema->n_children!=1):
                (array->n_children!=0||schema->n_children!=0)))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native lake Arrow field layout")));
        if(strcmp(schema->format,"+l")==0){
            ArrowArray* element=array->children?array->children[0]:NULL;
            ArrowSchema* elementSchema=schema->children?schema->children[0]:NULL;
            if(element==NULL||elementSchema==NULL||elementSchema->format==NULL||strcmp(elementSchema->format,"f")!=0||
                element->offset<0||element->length<0||element->offset>PG_INT64_MAX-element->length||
                element->n_buffers!=2||element->buffers==NULL||element->n_children!=0||elementSchema->n_children!=0||
                element->dictionary!=NULL||elementSchema->dictionary!=NULL)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native lake Arrow LIST element layout")));
        }
    }
    return batch;
}

static int64 LakeRequiredInteger(OgLakeSdkBatch* batch,int column,int64 row)
{
    ArrowArray* array=batch->array->children[column];ArrowSchema* schema=batch->schema->children[column];
    if(LakeSdkNull(array,row)||strcmp(schema->format,"l")!=0)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native lake hidden integer value")));
    int64 value=DatumGetInt64(LakeSdkValue(array,schema,row,INT8OID));
    if(value<0)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("negative native lake hidden integer value")));
    return value;
}

static Datum LakeDecodeFileField(const ArrowArray* array,const ArrowSchema* schema,int64 row,
    Oid fileType,int32 fileTypmod,Oid targetType,int32 targetTypmod,Size* binaryRowBytes,bool* isnull)
{
    if(fileType==BYTEAOID)(void)LakeCheckBinaryLayout(array,schema,row);
    if(fileType==UUIDOID)(void)LakeCheckUuidLayout(array,schema,row);
    if(fileType==JSONOID)(void)LakeCheckJsonLayout(array,schema,row);
    if(fileType==NUMERICOID)(void)LakeCheckNumericLayout(array,schema,row,fileTypmod);
    bool absent=LakeSdkNull(array,row);
    if(isnull)*isnull=absent;
    if(absent)return (Datum)0;
    Datum val=fileType==VECTOROID?LakeVectorReadArrow(array,schema,row,fileTypmod):
        LakeSdkValue(array,schema,row,fileType,binaryRowBytes,fileTypmod);
    if(fileType!=targetType||fileTypmod!=targetTypmod){
        val=LakeWidenDatum(val,fileType,fileTypmod,targetType,targetTypmod);
        if(targetType==NUMERICOID&&binaryRowBytes)
            val=LakeCopyNumericDatum(val,targetTypmod,binaryRowBytes);
    }
    return val;
}

void LakeFileVisitScalarKeys(LakeInlineBinding* binding,int fileIndex,AttrNumber column,
    const char* objectVersion,LakeScalarKeyVisitor visitor,void* argument,bool metadataPrebound)
{
    if(fileIndex<0||fileIndex>=binding->file_count||column<=0||column>binding->column_count||visitor==NULL)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid lake scalar build projection")));
    if(binding->has_added_columns||binding->has_dropped_columns)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("scalar index build after ADD COLUMN requires its schema adapter")));
    Oid type=binding->columns[column-1].type;
    if(!LakeScalarIndexedType(type))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("lake file scalar key type is unsupported")));
    LakeFileReader validation={};validation.binding=binding;validation.metadata_prebound=metadataPrebound;
    validation.handles=LakeFileHandles(CurrentMemoryContext);
    validation.states=(LakeFileState*)palloc0(sizeof(LakeFileState)*binding->file_count);
    validation.field_ids=(int64*)palloc(sizeof(int64)*binding->column_count);
    for(int i=0;i<binding->column_count;++i)validation.field_ids[i]=binding->columns[i].column_id;
    LakeValidateFileFooter(&validation,fileIndex);
    if(objectVersion==NULL||validation.states[fileIndex].object_version==NULL||
        strcmp(objectVersion,validation.states[fileIndex].object_version)!=0)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("scalar source immutable-object identity changed")));
    LakeDataFile* file=&binding->files[fileIndex];LakeSdkHandles* handles=validation.handles;
    int64 field=binding->columns[column-1].column_id;
    OgLakeSdkReadOptions options={};options.struct_size=sizeof(options);options.path=file->path;
    options.limits=LakeFileLimits();options.batch_rows=4096;options.num_field_ids=1;options.field_ids=&field;options.num_row_groups=-1;
    OgLakeSdkError error;
    LakeFileCheckStatus(LakeSdkCall([&]{return handles->api->reader_open(&options,&handles->reader,&error);}),&error);
    int64 seen=0;
    for(;;){
        CHECK_FOR_INTERRUPTS();
        int status=LakeSdkCall([&]{return handles->api->reader_next(handles->reader,&handles->batch,&error);});
        if(status==OG_LAKE_SDK_EOF){
            if(handles->batch!=NULL||seen!=file->record_count)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("scalar build did not cover its complete physical source")));
            break;
        }
        LakeFileCheckStatus(status,&error);OgLakeSdkBatch* batch=LakeCheckBatch(handles->batch,1);
        if(batch->struct_size<sizeof(*batch)||batch->object_version==NULL||strcmp(batch->object_version,objectVersion)!=0)
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("scalar key batch belongs to another source object")));
        for(int64 row=0;row<batch->num_rows;++row){
            if(seen>=file->record_count||batch->original_positions[row]!=seen)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("scalar source lost its original physical positions")));
            bool absent=false;
            Datum value=LakeDecodeFileField(batch->array->children[0],batch->schema->children[0],row,
                validation.states[fileIndex].field_types[column-1],validation.states[fileIndex].field_typmods[column-1],
                LakeColumnTargetType(&binding->columns[column-1]),LakeColumnTargetTypmod(&binding->columns[column-1]),
                NULL,&absent);
            /* Host callback executes after the provider's C++ stack unwinds. */
            visitor(file->file_id,seen++,value,absent,argument);
        }
        LakeSdkCall([&]{handles->api->batch_release(&handles->batch);return 0;});
    }
    LakeFileHandlesClose(handles);
    LakeFileState* checked=&validation.states[fileIndex];
    if(checked->logical_to_batch)pfree(checked->logical_to_batch);
    if(checked->field_types)pfree(checked->field_types);
    if(checked->field_typmods)pfree(checked->field_typmods);
    if(checked->present_field_ids)pfree(checked->present_field_ids);
    if(checked->object_version)pfree(checked->object_version);
    pfree(validation.states);pfree(validation.field_ids);
}

struct LakeAnnTruthPositions {Bitmapset* yes;Bitmapset* no;};
struct LakeAnnMaskBuild {
    const ScanKeyData* key;
    LakeAnnTruthPositions* truth;
    Oid source_type;
    int64* values;
    int value_count;
    bool array;
    bool has_null;
};

static Bitmapset* LakeAnnEmptyDomain(int64 rows)
{
    Bitmapset* result=rows?bms_make_singleton((int)rows-1):NULL;
    return rows?bms_del_member(result,(int)rows-1):result;
}

static void LakeAnnTruthClose(LakeAnnTruthPositions* truth)
{
    if(truth->yes)bms_free(truth->yes);if(truth->no)bms_free(truth->no);
    truth->yes=truth->no=NULL;
}

static int LakeAnnCompareKey(const void* left,const void* right)
{
    int64 a=*(const int64*)left,b=*(const int64*)right;
    return a==b?0:(a<b?-1:1);
}

static bool LakeAnnArrayContains(const LakeAnnMaskBuild* build,int64 value)
{
    int low=0,high=build->value_count;
    while(low<high){int mid=low+(high-low)/2;if(build->values[mid]<value)low=mid+1;else high=mid;}
    return low<build->value_count&&build->values[low]==value;
}

static void LakeAnnMaskVisit(int64 fileId,int64 position,Datum value,bool isnull,void* argument)
{
    (void)fileId;
    LakeAnnMaskBuild* build=(LakeAnnMaskBuild*)argument;const ScanKeyData* key=build->key;
    bool known=true,match=false;
    if(key->sk_flags&SK_SEARCHNULL)match=isnull;
    else if(key->sk_flags&SK_SEARCHNOTNULL)match=!isnull;
    else if(build->array){
        if(key->sk_flags&SK_ISNULL)known=false; /* NULL array, not empty array. */
        else if(build->value_count==0&&!build->has_null)match=false;
        else if(isnull)known=false;
        else{
            match=LakeAnnArrayContains(build,LakeScalarOrderKey(value,build->source_type));
            if(!match&&build->has_null)known=false;
        }
    }else if(isnull||(key->sk_flags&SK_ISNULL))known=false;
    else match=DatumGetBool(FunctionCall2Coll(const_cast<FmgrInfo*>(&key->sk_func),key->sk_collation,value,key->sk_argument));
    /* Both sets were allocated in the parent context for the full domain.
     * bms_del_member preserves nwords, so these inserts cannot move ownership
     * into the temporary provider/argument context. */
    if(known){Bitmapset** target=match?&build->truth->yes:&build->truth->no;
        *target=bms_add_member(*target,(int)position);}
}

static bool LakeAnnScalarLeaf(LakeInlineBinding* binding,int fileIndex,const char* version,
    const LakeAnnScalarQual* qual,LakeAnnTruthPositions* truth,bool metadataPrebound)
{
    if(qual->column<1||qual->column>binding->column_count||(binding->columns[qual->column-1].type==INT2OID||
        !LakeScalarIndexedType(binding->columns[qual->column-1].type)))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake ANN scalar leaf column")));
    const ScanKeyData* key=&qual->key;int flags=key->sk_flags;
    bool nullTest=flags&(SK_SEARCHNULL|SK_SEARCHNOTNULL),array=flags&SK_SEARCHARRAY;
    if((flags&~(SK_ISNULL|SK_SEARCHNULL|SK_SEARCHNOTNULL|SK_SEARCHARRAY))||
        ((flags&SK_SEARCHNULL)&&(flags&SK_SEARCHNOTNULL))||(nullTest&&array)||
        (!nullTest&&(key->sk_strategy<1||key->sk_strategy>5||
            key->sk_func.fn_oid==InvalidOid||key->sk_func.fn_oid>=FirstNormalObjectId||
            !LakeScalarArgumentType(binding->columns[qual->column-1].type,key->sk_subtype)))||
        (array&&key->sk_strategy!=BTEqualStrategyNumber))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake ANN scalar leaf key")));
    int64 rows=binding->files[fileIndex].record_count;
    truth->yes=LakeAnnEmptyDomain(rows);truth->no=LakeAnnEmptyDomain(rows);
    MemoryContext owner=CurrentMemoryContext;
    MemoryContext work=AllocSetContextCreate(owner,"Lake ANN scalar leaf",ALLOCSET_DEFAULT_SIZES);
    bool usable=true;
    PG_TRY();{
        MemoryContextSwitchTo(work);
        LakeAnnMaskBuild build={key,truth,binding->columns[qual->column-1].type,NULL,0,array,false};
        if(array&&!(flags&SK_ISNULL)&&toast_raw_datum_size(key->sk_argument)>1024*1024)usable=false;
        if(usable&&array&&!(flags&SK_ISNULL)){
            ArrayType* input=DatumGetArrayTypeP(key->sk_argument);Oid type=ARR_ELEMTYPE(input);
            if(type!=key->sk_subtype)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("lake ANN array parameter type changed")));
            int entries=ArrayGetNItems(ARR_NDIM(input),ARR_DIMS(input));
            /* An oversized parameter falls back before source output. Never
             * turn unsupported/over-budget input into an empty true set. */
            usable=entries<=LAKE_ANN_SCALAR_MAX_ARRAY;
            if(usable){
                int16 length;bool byValue;char alignment;Datum* elements=NULL;bool* nulls=NULL;
                get_typlenbyvalalign(type,&length,&byValue,&alignment);
                deconstruct_array(input,type,length,byValue,alignment,&elements,&nulls,&entries);
                build.values=(int64*)palloc(sizeof(int64)*Max(entries,1));
                for(int i=0;i<entries;++i){
                    if(nulls[i])build.has_null=true;
                    else build.values[build.value_count++]=LakeScalarOrderKey(elements[i],type);
                }
                if(build.value_count>1)qsort(build.values,build.value_count,sizeof(int64),LakeAnnCompareKey);
                if(elements)pfree(elements);if(nulls)pfree(nulls);
            }
        }
        if(usable)LakeFileVisitScalarKeys(binding,fileIndex,qual->column,version,LakeAnnMaskVisit,&build,metadataPrebound);
        MemoryContextSwitchTo(owner);
    }PG_CATCH();{MemoryContextSwitchTo(owner);MemoryContextDelete(work);PG_RE_THROW();}PG_END_TRY();
    MemoryContextDelete(work);
    return usable;
}

static void LakeAnnValidateScalarTree(const LakeAnnScalarExpr* expression,int depth,int* nodes,int* leaves)
{
    if(!expression||depth>LAKE_ANN_SCALAR_MAX_DEPTH||++*nodes>LAKE_ANN_SCALAR_MAX_NODES)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake ANN scalar expression bounds")));
    if(expression->kind==LAKE_ANN_SCALAR_LEAF){
        if(expression->child_count!=0||++*leaves>LAKE_ANN_SCALAR_MAX_LEAVES)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake ANN scalar leaf bounds")));
        return;
    }
    if((expression->kind!=LAKE_ANN_SCALAR_AND&&expression->kind!=LAKE_ANN_SCALAR_OR&&expression->kind!=LAKE_ANN_SCALAR_NOT)||
        expression->child_count<1||expression->child_count>LAKE_ANN_SCALAR_MAX_NODES||!expression->children||
        (expression->kind==LAKE_ANN_SCALAR_NOT&&expression->child_count!=1))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake ANN scalar expression kind")));
    for(int i=0;i<expression->child_count;++i)LakeAnnValidateScalarTree(expression->children[i],depth+1,nodes,leaves);
}

static bool LakeAnnReadScalarTree(LakeInlineBinding* binding,int fileIndex,const char* version,
    const LakeAnnScalarExpr* expression,LakeAnnTruthPositions* result,bool metadataPrebound)
{
    CHECK_FOR_INTERRUPTS();
    if(expression->kind==LAKE_ANN_SCALAR_LEAF)return LakeAnnScalarLeaf(binding,fileIndex,version,&expression->leaf,result,metadataPrebound);
    if(expression->kind==LAKE_ANN_SCALAR_NOT){
        if(!LakeAnnReadScalarTree(binding,fileIndex,version,expression->children[0],result,metadataPrebound))return false;
        Bitmapset* previous=result->yes;result->yes=result->no;result->no=previous;return true;
    }
    for(int i=0;i<expression->child_count;++i){
        LakeAnnTruthPositions next={};
        if(!LakeAnnReadScalarTree(binding,fileIndex,version,expression->children[i],&next,metadataPrebound)){
            LakeAnnTruthClose(&next);LakeAnnTruthClose(result);return false;}
        if(i==0){*result=next;continue;}
        LakeAnnTruthPositions combined={};
        if(expression->kind==LAKE_ANN_SCALAR_AND){combined.yes=bms_intersect(result->yes,next.yes);combined.no=bms_union(result->no,next.no);}
        else{combined.yes=bms_union(result->yes,next.yes);combined.no=bms_intersect(result->no,next.no);}
        LakeAnnTruthClose(result);LakeAnnTruthClose(&next);*result=combined;
    }
    return true;
}

bool LakeFileReadAnnScalarMask(LakeInlineBinding* binding,int fileIndex,const char* version,
    const LakeAnnScalarExpr* expression,LakeAnnFileMask* mask,MemoryContext context,bool metadataPrebound)
{
    if(!binding||fileIndex<0||fileIndex>=binding->file_count||!expression||!mask||!version)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid lake ANN scalar mask binding")));
    int64 rows=binding->files[fileIndex].record_count;
    if(rows<0||rows>(int64)OG_LAKE_ANN_MAX_PAGES*OG_LAKE_ANN_PAGE_ROWS)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid lake ANN scalar mask source")));
    int nodes=0,leaves=0;LakeAnnValidateScalarTree(expression,1,&nodes,&leaves);
    MemoryContext owner=MemoryContextSwitchTo(context);bool usable=false;
    PG_TRY();{
        LakeAnnTruthPositions truth={};usable=LakeAnnReadScalarTree(binding,fileIndex,version,expression,&truth,metadataPrebound);
        if(usable){
            if(bms_overlap(truth.yes,truth.no)||(int64)bms_num_members(truth.yes)+bms_num_members(truth.no)>rows)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("lake ANN scalar truth domains overlap")));
            mask->positions=truth.yes;mask->count=(uint32)bms_num_members(truth.yes);truth.yes=NULL;
        }
        LakeAnnTruthClose(&truth);MemoryContextSwitchTo(owner);
    }PG_CATCH();{MemoryContextSwitchTo(owner);PG_RE_THROW();}PG_END_TRY();
    return usable;
}

void LakeFileVisitAnnRows(LakeInlineBinding* binding,int fileIndex,AttrNumber column,
    const char* version,LakeAnnRowVisitor visitor,void* argument)
{
    if(binding&&(binding->has_added_columns||binding->has_dropped_columns))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("ANN index build after ADD COLUMN requires its schema adapter")));

    if(!binding||fileIndex<0||fileIndex>=binding->file_count||column<=0||column>binding->column_count||!version||!visitor||
        binding->columns[column-1].type!=VECTOROID||binding->columns[column-1].typmod<1||binding->columns[column-1].typmod>1536)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid lake ANN physical source projection")));
    MemoryContext owner=CurrentMemoryContext;
    MemoryContext work=AllocSetContextCreate(owner,"Lake ANN physical source",ALLOCSET_DEFAULT_SIZES);
    MemoryContext rowContext=NULL;LakeSdkHandles* volatile handles=NULL;
    PG_TRY();{
        MemoryContextSwitchTo(work);LakeFileReader validation={};validation.binding=binding;
        handles=LakeFileHandles(work);validation.handles=handles;
        validation.states=(LakeFileState*)palloc0(sizeof(LakeFileState)*binding->file_count);
        validation.field_ids=(int64*)palloc(sizeof(int64)*binding->column_count);
        for(int i=0;i<binding->column_count;++i)validation.field_ids[i]=binding->columns[i].column_id;
        LakeValidateFileFooter(&validation,fileIndex);const auto& state=validation.states[fileIndex];
        if(!state.object_version||strcmp(state.object_version,version)!=0)
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("ANN physical source object changed")));
        LakeDataFile* file=&binding->files[fileIndex];int64 fields[3]={binding->columns[column-1].column_id,0,0};
        int count=1,rowId=-1,birth=-1;
        if(state.explicit_row_id){rowId=count;fields[count++]=LAKE_FILE_ROW_ID;}
        if(state.explicit_snapshot){birth=count;fields[count++]=LAKE_FILE_SNAPSHOT_ID;}
        OgLakeSdkReadOptions options={};options.struct_size=sizeof(options);options.path=file->path;options.limits=LakeFileLimits();
        options.batch_rows=512;options.num_field_ids=count;options.field_ids=fields;options.num_row_groups=-1;OgLakeSdkError error;
        LakeFileCheckStatus(LakeSdkCall([&]{return handles->api->reader_open(&options,&handles->reader,&error);}),&error);
        rowContext=AllocSetContextCreate(work,"Lake ANN physical row",ALLOCSET_SMALL_SIZES);int64 seen=0;
        for(;;){CHECK_FOR_INTERRUPTS();int status=LakeSdkCall([&]{return handles->api->reader_next(handles->reader,&handles->batch,&error);});
            if(status==OG_LAKE_SDK_EOF){if(handles->batch||seen!=file->record_count)
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("ANN build lacks complete physical source input")));break;}
            LakeFileCheckStatus(status,&error);OgLakeSdkBatch* batch=LakeCheckBatch(handles->batch,count);
            if(batch->struct_size<sizeof(*batch)||!batch->object_version||strcmp(batch->object_version,version)!=0)
                ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("ANN vector batch belongs to a different source object")));
            for(int64 i=0;i<batch->num_rows;++i){CHECK_FOR_INTERRUPTS();
                if(seen>=file->record_count||batch->original_positions[i]!=seen||
                    (rowId<0&&(file->row_id_start<0||file->row_id_start>PG_INT64_MAX-seen)))
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("ANN physical source address is invalid")));
                LakeInlineRowIdentity identity={};identity.source=LAKE_LOCATOR_FILE;identity.file_id=file->file_id;identity.file_position=seen;
                identity.row_id=rowId>=0?LakeRequiredInteger(batch,rowId,i):file->row_id_start+seen;
                identity.begin_snapshot=birth>=0?LakeRequiredInteger(batch,birth,i):file->begin_snapshot;
                if(file->partial_max>=0&&identity.begin_snapshot>file->partial_max)
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("ANN source birth exceeds its partial maximum")));
                MemoryContextSwitchTo(rowContext);bool absent=LakeSdkNull(batch->array->children[0],i);
                Datum value=absent?(Datum)0:LakeVectorReadArrow(batch->array->children[0],batch->schema->children[0],i,binding->columns[column-1].typmod);
                visitor(&identity,value,absent,argument);MemoryContextSwitchTo(work);MemoryContextReset(rowContext);++seen;}
            LakeSdkCall([&]{handles->api->batch_release(&handles->batch);return 0;});}
        LakeFileHandlesClose(handles);MemoryContextSwitchTo(owner);MemoryContextDelete(work);
    }PG_CATCH();{MemoryContextSwitchTo(owner);LakeFileHandlesClose(handles);MemoryContextDelete(work);PG_RE_THROW();}PG_END_TRY();
}

static bool LakeDeletePathMatches(OgLakeSdkBatch* batch,int64 row,const char* path)
{
    ArrowArray* array=batch->array->children[0];ArrowSchema* schema=batch->schema->children[0];
    if(LakeSdkNull(array,row)||array->n_buffers!=3||array->buffers[1]==NULL)return false;
    int64 index=array->offset+row,start,end;
    if(strcmp(schema->format,"u")==0){start=((const int32*)array->buffers[1])[index];end=((const int32*)array->buffers[1])[index+1];}
    else if(strcmp(schema->format,"U")==0){start=((const int64*)array->buffers[1])[index];end=((const int64*)array->buffers[1])[index+1];}
    else return false;
    if(start<0||end<start||end-start!=(int64)strlen(path)||array->buffers[2]==NULL)return false;
    return memcmp((const char*)array->buffers[2]+start,path,(Size)(end-start))==0;
}

static int LakeComparePosition(const void* lhs,const void* rhs)
{
    int64 a=*(const int64*)lhs,b=*(const int64*)rhs;return a<b?-1:a>b?1:0;
}

/* Reuses the exact reader validation for flush; no provider callback may
 * invoke database ERROR. This visitor runs only after reader_next returns. */
void LakeFileVisitDeletes(LakeDataFile* file,LakeSdkHandles* handles,
    LakeDeleteVersionVisitor visitor,void* argument,OgLakeSdkIoStats* stats,LakeAnalyzeIoBudget* budget)
{
    if(file->deletes==NULL)return;
    LakeDeleteFile* del=file->deletes;
    const OgLakeSdkFooterInfo* footer=LakeCheckFooter(handles,del->path,del->record_count,del->file_bytes,-1);
    bool snapshots=LakeFindField(footer,LAKE_FILE_SNAPSHOT_ID,OG_LAKE_SDK_INT64)>=0;
    if(LakeFindField(footer,LAKE_DELETE_PATH_ID,OG_LAKE_SDK_UTF8)<0||
        LakeFindField(footer,LAKE_DELETE_POSITION_ID,OG_LAKE_SDK_INT64)<0||footer->num_fields!=(snapshots?3:2)||
        (del->partial_max>=0&&!snapshots))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("unsupported native lake Parquet delete schema")));
    if(budget){
        if(footer->num_row_groups<0||(footer->num_row_groups>0&&footer->row_groups==NULL))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("ANALYZE delete footer lacks row groups")));
        for(int g=0;g<footer->num_row_groups;++g){
            const OgLakeSdkRowGroup* group=&footer->row_groups[g];
            if(group->columns==NULL)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("ANALYZE delete group lacks column byte estimates")));
            for(int c=0;c<footer->num_fields;++c){int64 bytes=group->columns[c].uncompressed_bytes;
                if(bytes<0||(uint64)bytes>PG_UINT64_MAX-budget->planned_decoded_bytes)
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid ANALYZE delete decode estimate")));
                budget->planned_decoded_bytes+=(uint64)bytes;
            }
        }
        LakeAnalyzeCheckIo(budget);
    }
    LakeSdkCall([&]{handles->api->footer_close(&handles->footer);return 0;});
    int64 fields[3]={LAKE_DELETE_PATH_ID,LAKE_DELETE_POSITION_ID,LAKE_FILE_SNAPSHOT_ID};
    OgLakeSdkReadOptions options={};options.struct_size=sizeof(options);options.path=del->path;options.limits=LakeFileLimits();
    options.batch_rows=4096;options.num_field_ids=snapshots?3:2;options.field_ids=fields;options.num_row_groups=-1;
    OgLakeSdkError error;
    LakeFileCheckStatus(LakeSdkCall([&]{return handles->api->reader_open(&options,&handles->reader,&error);}),&error);
    int64 seen=0,last=-1;OgLakeSdkIoStats previous={};
    for(;;){
        CHECK_FOR_INTERRUPTS();
        int status=LakeSdkCall([&]{return handles->api->reader_next(handles->reader,&handles->batch,&error);});
        if(budget){OgLakeSdkIoStats observed={};
            LakeSdkCall([&]{handles->api->reader_stats(handles->reader,&observed);return 0;});
            LakeAnalyzeAddIo(budget,&observed,&previous);previous=observed;}
        if(status==OG_LAKE_SDK_EOF){
            if(handles->batch!=NULL||seen!=del->record_count)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake delete stream population disagrees")));
            break;
        }
        LakeFileCheckStatus(status,&error);OgLakeSdkBatch* batch=LakeCheckBatch(handles->batch,options.num_field_ids);
        for(int64 row=0;row<batch->num_rows;++row){
            if(seen>=del->record_count||batch->original_positions[row]!=seen++)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake delete stream lost original positions")));
            int64 position=LakeRequiredInteger(batch,1,row);
            if(position<=last||position>=file->record_count||!LakeDeletePathMatches(batch,row,file->path))
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native lake delete position or data file identity")));
            last=position;
            int64 snapshot=snapshots?LakeRequiredInteger(batch,2,row):del->begin_snapshot;
            if(del->partial_max>=0&&snapshot>del->partial_max)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake delete snapshot exceeds its partial maximum")));
            visitor(position,snapshot,argument);
        }
        LakeSdkCall([&]{handles->api->batch_release(&handles->batch);return 0;});
    }
    if(stats)LakeSdkCall([&]{handles->api->reader_stats(handles->reader,stats);return 0;});
    LakeFileHandlesClose(handles);

}
struct LakeVisibleDeleteVisit { LakeFileState* state; int64 snapshot; LakeAnalyzeIoBudget* budget; };
static void LakeKeepVisibleDelete(int64 position,int64 snapshot,void* argument)
{
    LakeVisibleDeleteVisit* visit=(LakeVisibleDeleteVisit*)argument;
    LakeAnalyzeCheckIo(visit->budget);
    if(snapshot<=visit->snapshot)visit->state->deleted_positions[visit->state->delete_count++]=position;
}

static void LakeLoadDeletePositions(LakeFileReader* reader,int index,LakeSdkHandles* handles)
{
    LakeDataFile* file=&reader->binding->files[index];LakeFileState* state=&reader->states[index];
    int64 maximum=file->inline_delete_count+(file->deletes?file->deletes->record_count:0);
    /* Even a zero catalog count must be checked against the actual footer. */
    if(maximum==0&&file->deletes==NULL)return;
    if(maximum>LAKE_FILE_DELETE_POSITION_LIMIT)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake deletion allocation exceeds budget")));
    state->deleted_positions=(int64*)palloc(sizeof(int64)*(Size)Max(maximum,(int64)1));
    for(int i=0;i<file->inline_delete_count;++i)state->deleted_positions[state->delete_count++]=file->inline_delete_positions[i];
    LakeVisibleDeleteVisit visit={state,reader->binding->snapshot_id,reader->analyze_budget};
    OgLakeSdkIoStats observed={};
    LakeFileVisitDeletes(file,handles,LakeKeepVisibleDelete,&visit,reader->analyze_budget?&observed:NULL,reader->analyze_budget);
    qsort(state->deleted_positions,state->delete_count,sizeof(int64),LakeComparePosition);
    int distinct=0;
    for(int i=0;i<state->delete_count;++i){
        if(distinct==0||state->deleted_positions[i]!=state->deleted_positions[distinct-1])
            state->deleted_positions[distinct++]=state->deleted_positions[i];
    }
    state->delete_count=distinct;
}

static void LakeFileAdmitSelected(LakeFileReader* reader)
{
    LakeInlineBinding* binding=reader->binding;
    if(reader->admission_handles==NULL)
        reader->admission_handles=LakeFileHandles(reader->context);
    LakeSdkHandles* deletes=reader->admission_handles;
    for(int i=0;i<binding->file_count;++i){
        CHECK_FOR_INTERRUPTS();LakeFileState* state=&reader->states[i];
        if(state->partition_skipped)continue;
        if(!state->admitted){
            LakeValidateFileFooter(reader,i);
            if(reader->expected_versions&&(!reader->expected_versions[i]||!state->object_version||
               strcmp(reader->expected_versions[i],state->object_version)))
                ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake Stream source object changed after prebinding")));
            LakeLoadDeletePositions(reader,i,deletes);
            state->admitted=true;++reader->admitted_files;
        }
        if(state->explicit_row_id)continue;
        LakeDataFile* current=&binding->files[i];
        for(int j=0;j<i;++j){
            const LakeFileState* priorState=&reader->states[j];
            /* A pruned file has no footer evidence. Its zero-initialized
             * explicit_row_id flag is not evidence of implicit identities. */
            if(priorState->partition_skipped||!priorState->admitted||priorState->explicit_row_id)continue;
            LakeDataFile* prior=&binding->files[j];
            if(current->row_id_start<prior->row_id_start+prior->record_count&&
                prior->row_id_start<current->row_id_start+current->record_count)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("overlapping implicit native lake file identity ranges")));
        }
    }
    LakeFileHandlesClose(deletes);reader->admission_pending=false;
}

LakeFileReader* LakeFileReaderOpen(LakeInlineBinding* binding,LakeTxnTable* table,CommandId commandLimit,
    LakeAnalyzeIoBudget* analyzeBudget,char* const* expectedVersions,bool deferredAdmission)
{
    if(binding->file_count==0)return NULL;
    if(deferredAdmission&&(analyzeBudget||expectedVersions))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("lake partition admission requires an ordinary row scan")));
    LakeFileReader* reader=(LakeFileReader*)palloc0(sizeof(LakeFileReader));reader->binding=binding;reader->context=CurrentMemoryContext;
    reader->table=table;reader->command_limit=commandLimit;reader->expected_versions=expectedVersions;
    reader->analyze_budget=analyzeBudget;
    reader->handles=LakeFileHandles(CurrentMemoryContext);
    reader->states=(LakeFileState*)palloc0(sizeof(LakeFileState)*binding->file_count);
    reader->field_ids=(int64*)palloc(sizeof(int64)*(binding->column_count+2));
    for(int i=0;i<binding->column_count;++i)reader->field_ids[i]=binding->columns[i].column_id;
    reader->deferred_admission=deferredAdmission;reader->admission_pending=true;
    /* Legacy callers retain eager admission of every bound file. The new
     * ordinary partition path supplies its mask before any row is emitted. */
    if(!deferredAdmission)LakeFileAdmitSelected(reader);
    return reader;
}

int LakeFileReaderAdmittedFiles(const LakeFileReader* reader)
{return reader?reader->admitted_files:0;}

void LakeFileReaderRequireAdmitted(const LakeFileReader* reader)
{
    if(reader&&reader->admission_pending)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("lake file source admission is pending")));
}

void LakeFileReaderApplyPruning(LakeFileReader* reader,const bool* skipped,int fileCount)
{
    if(!reader||!reader->deferred_admission||!reader->admission_pending||!skipped||
       fileCount!=reader->binding->file_count||reader->file_index!=0||reader->position!=0||reader->handles->reader)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("lake file pruning must precede source iteration")));
    MemoryContext previous=MemoryContextSwitchTo(reader->context);
    PG_TRY();
    {
        for(int f=0;f<fileCount;++f)reader->states[f].partition_skipped=skipped[f];
        LakeFileAdmitSelected(reader);
        MemoryContextSwitchTo(previous);
    }
    PG_CATCH();{MemoryContextSwitchTo(previous);PG_RE_THROW();}PG_END_TRY();
}

static bool LakeFileCandidateCovered(const LakeFileReader* reader,int file)
{return reader->sample_positions?file==0:reader->ann_selection?LakeAnnFileCovered(reader->ann_selection,file):LakeScalarFileCovered(reader->selection,file);}
static int64 LakeFileCandidateCount(const LakeFileReader* reader,int file)
{return reader->sample_positions?reader->sample_position_count:reader->ann_selection?LakeAnnFilePositionCount(reader->ann_selection,file):LakeScalarFilePositionCount(reader->selection,file);}
static int64 LakeFileCandidatePosition(const LakeFileReader* reader,int file,int64 index)
{return reader->sample_positions?reader->sample_positions[index]:reader->ann_selection?LakeAnnFilePosition(reader->ann_selection,file,(uint32)index)->row_position:LakeScalarFilePosition(reader->selection,file,index);}
static int64 LakeFileCandidateFind(const LakeFileReader* reader,int file,int64 position)
{
    int64 low=0,high=LakeFileCandidateCount(reader,file);
    while(low<high){int64 middle=low+(high-low)/2;if(LakeFileCandidatePosition(reader,file,middle)<position)low=middle+1;else high=middle;}
    return low<LakeFileCandidateCount(reader,file)&&LakeFileCandidatePosition(reader,file,low)==position?low:-1;
}
static void LakeFilePrepareCandidateGroups(LakeFileReader* reader,int f)
{
    if(!LakeFileCandidateCovered(reader,f))return;
    CHECK_FOR_INTERRUPTS();LakeDataFile* file=&reader->binding->files[f];LakeFileState* state=&reader->states[f];
    const OgLakeSdkFooterInfo* footer=LakeCheckFooter(reader->handles,file->path,file->record_count,file->file_bytes,file->row_group_count);
    bool same=footer->struct_size>=OG_LAKE_SDK_FIELD_END(OgLakeSdkFooterInfo,object_version)&&footer->object_version&&state->object_version&&
        strcmp(footer->object_version,state->object_version)==0;
    if(same&&footer->num_row_groups>=0&&footer->num_row_groups<=(int)(MaxAllocSize/(2*sizeof(int64)))){
        int groups=footer->num_row_groups;
        state->groups=(int32*)palloc(sizeof(int32)*Max(groups,1));
        state->group_first=(int64*)palloc(sizeof(int64)*Max(groups,1));
        state->group_rows=(int64*)palloc(sizeof(int64)*Max(groups,1));
        int64 position=0,count=LakeFileCandidateCount(reader,f);
        for(int group=0;group<groups;++group){
            const OgLakeSdkRowGroup* rows=&footer->row_groups[group];
            if(rows->first_row<0||rows->num_rows<0||rows->first_row>file->record_count-rows->num_rows)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid index source row-group population")));
            if(position==count||LakeFileCandidatePosition(reader,f,position)>=rows->first_row+rows->num_rows)continue;
            if(LakeFileCandidatePosition(reader,f,position)<rows->first_row)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("index candidate falls outside source row groups")));
            int selected=state->group_count++;state->groups[selected]=group;
            state->group_first[selected]=rows->first_row;state->group_rows[selected]=rows->num_rows;
            state->selected_rows+=rows->num_rows;
            if(reader->analyze_budget){
                LakeAnalyzeIoBudget* budget=reader->analyze_budget;++budget->groups;
                if(rows->columns==NULL)
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("ANALYZE source group has no column byte inventory")));
                for(int c=0;c<footer->num_fields;++c){
                    const OgLakeSdkColumnChunk* chunk=&rows->columns[c];
                    if(chunk->compressed_bytes<0||chunk->uncompressed_bytes<0||
                       (uint64)chunk->compressed_bytes>PG_UINT64_MAX-budget->planned_bytes||
                       (uint64)chunk->uncompressed_bytes>PG_UINT64_MAX-budget->planned_decoded_bytes)
                        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid ANALYZE source group byte estimate")));
                    budget->planned_bytes+=(uint64)chunk->compressed_bytes;
                    budget->planned_decoded_bytes+=(uint64)chunk->uncompressed_bytes;
                }
                LakeAnalyzeCheckIo(budget);
            }
            while(position<count&&LakeFileCandidatePosition(reader,f,position)<rows->first_row+rows->num_rows)++position;
        }
        if(position!=count||state->selected_rows>file->record_count)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("index candidates exceed their physical file")));
        state->indexed=true;
    }
    LakeSdkCall([&]{reader->handles->api->footer_close(&reader->handles->footer);return 0;});
    if((reader->ann_selection||reader->sample_positions)&&!state->indexed)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake source object changed during candidate preparation")));
}

void LakeFileReaderUseScalar(LakeFileReader* reader,const char* uuid,AttrNumber column,const ScanKeyData* key,const LakeScalarFileSources* sources)
{
    if(reader==NULL)return;
    if(reader->file_index!=0||reader->position!=0||reader->handles->reader!=NULL)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("file scalar selection must precede its first row")));
    if(reader->candidate_context==NULL)
        reader->candidate_context=AllocSetContextCreate(reader->context,"Lake scalar candidates",
            ALLOCSET_DEFAULT_MINSIZE,ALLOCSET_DEFAULT_INITSIZE,ALLOCSET_DEFAULT_MAXSIZE);
    else MemoryContextReset(reader->candidate_context);
    reader->selection=NULL;reader->ann_selection=NULL;
    for(int f=0;f<reader->binding->file_count;++f){
        LakeFileState* state=&reader->states[f];state->indexed=false;state->group_count=0;
        state->groups=NULL;state->group_first=state->group_rows=NULL;state->selected_rows=0;
    }
    MemoryContext caller=MemoryContextSwitchTo(reader->candidate_context);
    PG_TRY();
    {
        char** versions=(char**)palloc(sizeof(char*)*reader->binding->file_count);
        for(int f=0;f<reader->binding->file_count;++f)
            versions[f]=reader->states[f].partition_skipped?NULL:reader->states[f].object_version;
        reader->selection=LakeSelectScalarFiles(reader->binding,uuid,column,key,versions,reader->candidate_context,sources);
        for(int f=0;f<reader->binding->file_count;++f)LakeFilePrepareCandidateGroups(reader,f);
        MemoryContextSwitchTo(caller);
    }
    PG_CATCH();{MemoryContextSwitchTo(caller);PG_RE_THROW();}PG_END_TRY();
}

/* Check selected graph addresses against authoritative hidden source columns
 * before emitting any business row. The immutable object version is checked
 * on every batch. A bad derived address makes this whole file a full scan. */
static bool LakeFileCheckAnnAddresses(LakeFileReader* reader,int f)
{
    LakeFileState* state=&reader->states[f];LakeDataFile* file=&reader->binding->files[f];
    int64 count=LakeFileCandidateCount(reader,f);if(count==0)return true;
    if(!state->explicit_row_id&&!state->explicit_snapshot){
        for(int64 i=0;i<count;++i){const auto* address=LakeAnnFilePosition(reader->ann_selection,f,(uint32)i);
            if(address->row_id!=file->row_id_start+address->row_position||address->birth_snapshot!=file->begin_snapshot)return false;}
        return true;}
    int64 fields[2];int columns=0,rowId=-1,birth=-1;
    if(state->explicit_row_id){rowId=columns;fields[columns++]=LAKE_FILE_ROW_ID;}
    if(state->explicit_snapshot){birth=columns;fields[columns++]=LAKE_FILE_SNAPSHOT_ID;}
    LakeSdkHandles* handles=LakeFileHandles(CurrentMemoryContext);bool valid=true;int64 checked=0,seen=0;int group=0;int64 groupOffset=0;
    PG_TRY();{
        OgLakeSdkReadOptions options={};options.struct_size=sizeof(options);options.path=file->path;options.limits=LakeFileLimits();
        options.batch_rows=512;options.num_field_ids=columns;options.field_ids=fields;
        options.num_row_groups=state->group_count;options.row_groups=state->groups;OgLakeSdkError error;
        LakeFileCheckStatus(LakeSdkCall([&]{return handles->api->reader_open(&options,&handles->reader,&error);}),&error);
        for(;;){CHECK_FOR_INTERRUPTS();int code=LakeSdkCall([&]{return handles->api->reader_next(handles->reader,&handles->batch,&error);});
            if(code==OG_LAKE_SDK_EOF){if(handles->batch)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("ANN address stream returned a batch at EOF")));break;}
            LakeFileCheckStatus(code,&error);OgLakeSdkBatch* batch=LakeCheckBatch(handles->batch,columns);
            if(batch->struct_size<sizeof(*batch)||!batch->object_version||strcmp(batch->object_version,state->object_version)!=0)
                ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("ANN source changed during address verification")));
            for(int64 i=0;i<batch->num_rows;++i){int64 position=batch->original_positions[i];
                while(group<state->group_count&&groupOffset==state->group_rows[group]){++group;groupOffset=0;}
                if(group>=state->group_count||position!=state->group_first[group]+groupOffset++||position<0||position>=file->record_count)
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("ANN address source lost its original positions")));
                ++seen;int64 selected=LakeFileCandidateFind(reader,f,position);
                if(selected<0)continue;const auto* address=LakeAnnFilePosition(reader->ann_selection,f,(uint32)selected);
                int64 actualRow=rowId>=0?LakeRequiredInteger(batch,rowId,i):file->row_id_start+position;
                int64 actualBirth=birth>=0?LakeRequiredInteger(batch,birth,i):file->begin_snapshot;
                if(actualRow!=address->row_id||actualBirth!=address->birth_snapshot)valid=false;
                ++checked;}
            LakeSdkCall([&]{handles->api->batch_release(&handles->batch);return 0;});}
        LakeFileHandlesClose(handles);
    }PG_CATCH();{LakeFileHandlesClose(handles);PG_RE_THROW();}PG_END_TRY();
    return valid&&checked==count&&seen==state->selected_rows;
}

void LakeFileReaderEnableAnnRescanCache(LakeFileReader* reader)
{if(reader&&!reader->ann_rescan_cache)reader->ann_rescan_cache=LakeAnnRescanCreate(reader->context);}
LakeAnnRescanStats LakeFileReaderAnnRescanStats(const LakeFileReader* reader)
{return LakeAnnRescanStatsRead(reader?reader->ann_rescan_cache:NULL);}
void LakeFileReaderUseAnn(LakeFileReader* reader,const char* uuid,AttrNumber column,int metric,Datum query,bool enabled,uint32 minimum,const LakeAnnScalarExpr* filter,const LakeAnnFileSources* prebound,bool hasResidualQual)
{
    if(!reader)return;
    if(reader->file_index!=0||reader->position!=0||reader->handles->reader)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("ANN file selection must precede source reading")));
    if(!reader->candidate_context)reader->candidate_context=AllocSetContextCreate(reader->context,"Lake file index candidates",ALLOCSET_DEFAULT_SIZES);
    else MemoryContextReset(reader->candidate_context);
    reader->selection=NULL;reader->ann_selection=NULL;
    for(int f=0;f<reader->binding->file_count;++f){auto* state=&reader->states[f];state->indexed=false;state->group_count=0;
        state->groups=NULL;state->group_first=state->group_rows=NULL;state->selected_rows=0;}
    if(!enabled)return;
    MemoryContext caller=MemoryContextSwitchTo(reader->candidate_context);
    PG_TRY();{
        char** versions=(char**)palloc0(sizeof(char*)*reader->binding->file_count);
        bool privateChanges=reader->table&&LakeTxnTableHasChanges(reader->table);
        for(int f=0;f<reader->binding->file_count;++f){const auto& state=reader->states[f];const auto& file=reader->binding->files[f];
            /* First query slice scans affected files completely, so ended,
             * future or privately hidden near neighbors cannot exhaust ANN's
             * beam and silently reduce LIMIT population. Later fusion may
             * replace this conservative admission with visible expansion. */
            if(!state.partition_skipped&&!privateChanges&&state.delete_count==0&&(!state.explicit_snapshot||
                (file.partial_max>=0&&file.partial_max<=reader->binding->snapshot_id)))versions[f]=state.object_version;
            if(!versions[f])ereport(DEBUG1,(errmsg("lake file ANN complete visibility scan"),errdetail("file=%lld private=%d deletes=%d partial_max=%lld snapshot=%lld",
                (long long)file.file_id,privateChanges?1:0,state.delete_count,(long long)file.partial_max,(long long)reader->binding->snapshot_id)));}
        uint32 searchBeam=filter?1000U:(hasResidualQual?Min(1000U,Max(256U,minimum*8U)):Min(1000U,Max(128U,minimum*6U)));
        if(searchBeam<minimum)searchBeam=Min(1000U,minimum);
        uint32 emitLimit=(filter||hasResidualQual)?searchBeam:Min(searchBeam,Max(minimum+20U,minimum*3U));
        reader->ann_selection=LakeSelectAnnFiles(reader->binding,uuid,column,metric,query,searchBeam,minimum,versions,filter,prebound,reader->ann_rescan_cache,emitLimit);
        for(int f=0;f<reader->binding->file_count;++f){LakeFilePrepareCandidateGroups(reader,f);
            if(reader->states[f].indexed&&!LakeFileCheckAnnAddresses(reader,f)){
                ereport(DEBUG1,(errmsg("lake file ANN address verification fallback"),errdetail("file=%lld",(long long)reader->binding->files[f].file_id)));
                reader->states[f].indexed=false;}}
        MemoryContextSwitchTo(caller);
    }PG_CATCH();{MemoryContextSwitchTo(caller);PG_RE_THROW();}PG_END_TRY();
}
int LakeFileReaderPqFiles(const LakeFileReader* reader)
{
    int count=0;if(reader&&reader->ann_selection)for(int f=0;f<reader->binding->file_count;++f)
        if(reader->states[f].indexed&&LakeAnnFilePqUsed(reader->ann_selection,f))++count;
    return count;
}
int64 LakeFileReaderAnnCandidateRows(const LakeFileReader* reader)
{
    int64 count=0;if(reader&&reader->ann_selection)for(int f=0;f<reader->binding->file_count;++f)
        if(reader->states[f].indexed)count+=LakeAnnFilePositionCount(reader->ann_selection,f);
    return count;
}
int LakeFileReaderAnnFiles(const LakeFileReader* reader)
{
    int count=0;if(reader&&reader->ann_selection)for(int f=0;f<reader->binding->file_count;++f)if(reader->states[f].indexed)++count;
    return count;
}

int LakeFileReaderScalarFiles(const LakeFileReader* reader)
{
    int files=0;if(reader&&reader->selection)for(int f=0;f<reader->binding->file_count;++f)if(reader->states[f].indexed)++files;
    return files;
}

int LakeFileReaderBitmapFiles(const LakeFileReader* reader)
{
    int files=0;if(reader&&reader->selection)for(int f=0;f<reader->binding->file_count;++f)
        if(reader->states[f].indexed&&LakeScalarFileBitmap(reader->selection,f))++files;
    return files;
}


void LakeFileReaderVerifyRewritePartition(LakeFileReader* reader)
{
    if(!reader||reader->verify_rewrite_partition||!LakePartitionRewriteVersionEnabled()||reader->table||reader->analyze_budget||
       reader->deferred_admission||reader->metadata_prebound||!reader->expected_versions||
       reader->file_index!=0||reader->position!=0||reader->handles->reader||
       reader->selection||reader->ann_selection||reader->binding->file_count<1||reader->binding->file_count>LakeRewriteSourceLimit()||
       !reader->binding->column_evolution||
       (!LakeMergeAdjacentVersionEnabled()&&(reader->binding->has_added_columns||reader->binding->has_dropped_columns)))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("invalid protected partition rewrite reader")));
    for(int f=0;f<reader->binding->file_count;++f){
        const auto* spec=&reader->binding->files[f].partition.spec;
        if(!spec->enabled||spec->key_count<1||!spec->keys||!reader->states[f].admitted||reader->states[f].partition_skipped)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("rewrite source lacks a complete admitted partition spec")));
        for(int k=0;k<spec->key_count;++k){
            const auto* key=&spec->keys[k];int c=key->column_index;
            if(!key->host_bound||c<0||c>=reader->binding->column_count||
               reader->binding->columns[c].column_id!=key->column_id||reader->states[f].logical_to_batch[c]<0)
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("rewrite historical partition key requires a present physical field")));
        }
    }
    reader->rewrite_partition_scratch=AllocSetContextCreate(reader->context,"Rewrite physical partition key",ALLOCSET_DEFAULT_SIZES);
    reader->verify_rewrite_partition=true;
}

/* The source batch is already decoded once. Read only key columns here, in a
 * separate scratch context reset for EVERY physical row, including discarded
 * versions. No callback enters the SDK, no metadata/PK work enters a reader. */
static void LakeCheckRewritePhysicalPartition(LakeFileReader* reader,const OgLakeSdkBatch* batch,int64 row)
{
    MemoryContext caller=CurrentMemoryContext;
    MemoryContextReset(reader->rewrite_partition_scratch);
    PG_TRY();{
        MemoryContextSwitchTo(reader->rewrite_partition_scratch);
        LakeInlineBinding* b=reader->binding;LakeFileState* state=&reader->states[reader->file_index];
        const LakePartitionFile* part=&b->files[reader->file_index].partition;
        Datum* values=(Datum*)palloc0(sizeof(Datum)*b->column_count);
        bool* nulls=(bool*)palloc0(sizeof(bool)*b->column_count);
        bool* decoded=(bool*)palloc0(sizeof(bool)*b->column_count);
        int* attrs=(int*)palloc(sizeof(int)*b->column_count);
        for(int c=0;c<b->column_count;++c)attrs[c]=c;
        for(int k=0;k<part->spec.key_count;++k){
            int c=part->spec.keys[k].column_index;if(decoded[c])continue;decoded[c]=true;
            int child=state->logical_to_batch[c];
            if(child<0)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("rewrite historical partition field disappeared")));
            ArrowArray* array=batch->array->children[child];ArrowSchema* schema=batch->schema->children[child];
            values[c]=LakeDecodeFileField(array,schema,row,state->field_types[c],state->field_typmods[c],
                b->columns[c].type,b->columns[c].typmod,NULL,&nulls[c]);
        }
        if(!LakePartitionTupleMatches(&part->spec,&part->tuple,values,nulls,attrs))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("rewrite physical row contradicts its complete historical partition tuple")));
        MemoryContextSwitchTo(caller);MemoryContextReset(reader->rewrite_partition_scratch);
    }PG_CATCH();{
        MemoryContextSwitchTo(caller);MemoryContextReset(reader->rewrite_partition_scratch);PG_RE_THROW();
    }PG_END_TRY();
}

static void LakeFileEnsureProjection(LakeFileReader* reader, LakeFileState* state)
{
    if (state->projection_built)
        return;
    if (reader->projected == NULL) {
        state->open_columns = state->present_columns;
        state->open_field_ids = state->present_field_ids;
        state->open_to_batch = state->logical_to_batch;
        state->projection_built = true;
        return;
    }
    MemoryContext caller = MemoryContextSwitchTo(reader->context);
    if (state->open_field_ids == NULL || state->open_field_ids == state->present_field_ids)
        state->open_field_ids = (int64*)palloc(sizeof(int64) * (state->present_columns + 2));
    if (state->open_to_batch == NULL || state->open_to_batch == state->logical_to_batch)
        state->open_to_batch = (int*)palloc(sizeof(int) * reader->binding->column_count);
    state->open_columns = 0;
    for (int c = 0; c < reader->binding->column_count; ++c) {
        if (!reader->projected[c] || state->logical_to_batch[c] < 0) {
            state->open_to_batch[c] = -1;
            continue;
        }
        int slot = state->open_columns++;
        state->open_field_ids[slot] = reader->binding->columns[c].column_id;
        state->open_to_batch[c] = slot;
    }
    state->projection_built = true;
    MemoryContextSwitchTo(caller);
}

void LakeFileReaderSetProjected(LakeFileReader* reader, const bool* projected, int count)
{
    if (reader == NULL)
        return;
    if (reader->file_index != 0 || reader->position != 0 ||
        reader->handles->reader != NULL || reader->handles->batch != NULL)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("file projection must precede its first row")));
    if (projected != NULL && count != reader->binding->column_count)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("lake file projection width disagrees with table schema")));
    MemoryContext caller = MemoryContextSwitchTo(reader->context);
    if (projected == NULL) {
        if (reader->projected != NULL) {
            pfree(reader->projected);
            reader->projected = NULL;
        }
    } else {
        if (reader->projected == NULL)
            reader->projected = (bool*)palloc(sizeof(bool) * count);
        memcpy(reader->projected, projected, sizeof(bool) * count);
    }
    MemoryContextSwitchTo(caller);
    for (int f = 0; f < reader->binding->file_count; ++f)
        reader->states[f].projection_built = false;
}

/* Open the next Arrow batch for the current file, or close the file at EOF and advance file_index. */
static bool LakeFileEnsureBatch(LakeFileReader* reader, LakeDataFile* file, LakeFileState* state)
{
    LakeSdkHandles* handles = reader->handles;
    if (handles->reader == NULL) {
        LakeFileEnsureProjection(reader, state);
        int totalFields = state->open_columns;
        if (state->explicit_row_id)
            state->open_field_ids[totalFields++] = LAKE_FILE_ROW_ID;
        if (state->explicit_snapshot)
            state->open_field_ids[totalFields++] = LAKE_FILE_SNAPSHOT_ID;
        OgLakeSdkReadOptions options = {};
        options.struct_size = sizeof(options);
        options.path = file->path;
        options.limits = LakeFileLimits();
        options.batch_rows = 4096;
        options.num_field_ids = totalFields;
        options.field_ids = state->open_field_ids;
        options.num_row_groups = state->indexed ? state->group_count : -1;
        options.row_groups = state->groups;
        OgLakeSdkError error;
        LakeFileCheckStatus(LakeSdkCall([&]{return handles->api->reader_open(&options, &handles->reader, &error);}), &error);
    }
    int columns = state->open_columns + (state->explicit_row_id ? 1 : 0) + (state->explicit_snapshot ? 1 : 0);
    if (handles->batch == NULL || reader->batch_offset >= handles->batch->num_rows) {
        LakeSdkCall([&]{handles->api->batch_release(&handles->batch); return 0;});
        reader->batch_offset = 0;
        OgLakeSdkError error;
        int status = LakeSdkCall([&]{return handles->api->reader_next(handles->reader, &handles->batch, &error);});
        LakeAnalyzeObserveReader(reader);
        if (status == OG_LAKE_SDK_EOF) {
            if (handles->batch != NULL || reader->position != (state->indexed ? state->selected_rows : file->record_count))
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("native lake file stream population disagrees")));
            LakeSdkCall([&]{handles->api->reader_close(&handles->reader); return 0;});
            ++reader->file_index;
            reader->position = 0;
            reader->group_cursor = 0;
            reader->group_offset = 0;
            return false;
        }
        LakeFileCheckStatus(status, &error);
        (void)LakeCheckBatch(handles->batch, columns);
        if (reader->expected_versions && (handles->batch->struct_size < sizeof(*handles->batch) ||
            !handles->batch->object_version ||
            strcmp(handles->batch->object_version, reader->expected_versions[reader->file_index])))
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("native lake Stream batch belongs to another source object")));
        if (state->indexed && (handles->batch->struct_size < sizeof(*handles->batch) ||
            handles->batch->object_version == NULL ||
            strcmp(handles->batch->object_version, state->object_version) != 0))
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("indexed source object changed during file fetch")));
    }
    return true;
}

static inline bool LakeArrowNotNull(const uint8* valid, int64 index)
{
    return valid == NULL || (((valid[index / 8] >> (index % 8)) & 1) != 0);
}

static bool LakeFileCanVectorFill(const LakeFileReader* reader, const LakeFileState* state,
    const LakeDataFile* file, const VectorBatch* batch, const bool* projected)
{
    if (state->indexed || state->delete_count > 0 || state->explicit_row_id || state->explicit_snapshot ||
        state->rewritten_birth_upper || reader->verify_rewrite_partition || reader->analyze_budget != NULL ||
        reader->binding->retired_count > 0 || file->begin_snapshot > reader->binding->snapshot_id ||
        (file->partial_max >= 0 && file->begin_snapshot > file->partial_max) ||
        (reader->table != NULL && LakeTxnTableHasChanges(reader->table)) ||
        GetDatabaseEncoding() != PG_UTF8 ||
        batch->m_cols != reader->binding->column_count)
        return false;
    for (int c = 0; c < reader->binding->column_count; ++c) {
        if (projected != NULL && !projected[c])
            continue;
        if (state->logical_to_batch[c] < 0)
            return false;
        Oid colType = reader->binding->columns[c].type;
        if (state->field_types[c] != colType || LakeColumnTargetType(&reader->binding->columns[c]) != colType)
            return false;
        switch (colType) {
            case BOOLOID:
            case INT2OID:
            case INT4OID:
            case INT8OID:
            case FLOAT4OID:
            case FLOAT8OID:
                break;
            case TEXTOID:
                if (batch->m_arr[c].m_buf == NULL)
                    return false;
                break;
            default:
                return false;
        }
    }
    return true;
}

static const char* LakeExpectedArrowFormat(Oid oid)
{
    switch (oid) {
        case BOOLOID: return "b";
        case INT2OID: return "s";
        case INT4OID: return "i";
        case INT8OID: return "l";
        case FLOAT4OID: return "f";
        case FLOAT8OID: return "g";
        default: return NULL;
    }
}

int LakeFileReaderFillVectorBatch(LakeFileReader* reader, VectorBatch* batch,
    const bool* projected, Size* payloadBytes, Size payloadLimit, Size* colBatchBytes,
    bool* needFallback, bool* eos)
{
    if (needFallback) *needFallback = false;
    if (eos) *eos = false;
    if (reader == NULL) {
        if (eos) *eos = true;
        return 0;
    }
    LakeFileReaderRequireAdmitted(reader);
    LakeInlineBinding* binding = reader->binding;
    int filled = 0;
    while (batch->m_rows < BatchMaxSize && reader->file_index < binding->file_count) {
        CHECK_FOR_INTERRUPTS();
        LakeDataFile* file = &binding->files[reader->file_index];
        LakeFileState* state = &reader->states[reader->file_index];
        if (state->partition_skipped || (state->indexed && state->selected_rows == 0)) {
            ++reader->file_index;
            continue;
        }
        if (!LakeFileCanVectorFill(reader, state, file, batch, projected)) {
            if (needFallback) *needFallback = true;
            break;
        }
        if (!LakeFileEnsureBatch(reader, file, state))
            continue;
        OgLakeSdkBatch* srcBatch = reader->handles->batch;
        int64 avail = srcBatch->num_rows - reader->batch_offset;
        int take = (int)Min(avail, (int64)(BatchMaxSize - batch->m_rows));
        const int64* origPos = srcBatch->original_positions + reader->batch_offset;
        const int64 basePos = reader->position;
        const int64 recCount = file->record_count;
        for (int r = 0; r < take; ++r) {
            int64 pos = origPos[r];
            if (unlikely(pos != basePos + r || pos < 0 || pos >= recCount))
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("native lake file positions lost their original order")));
        }

        int ncols = binding->column_count;
        int projectedCount = 0;
        int textCount = 0;
        for (int c = 0; c < ncols; ++c) {
            if (projected != NULL && !projected[c])
                continue;
            ++projectedCount;
            int child = state->open_to_batch[c];
            ArrowArray* arr = srcBatch->array->children[child];
            ArrowSchema* sch = srcBatch->schema->children[child];
            if (arr->buffers[0] == NULL && arr->null_count > 0)
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("native lake Arrow validity buffer is missing")));
            if (arr->buffers[1] == NULL)
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("native lake Arrow value buffer is missing")));
            Oid colType = binding->columns[c].type;
            if (colType == TEXTOID) {
                if (arr->n_buffers != 3 || (strcmp(sch->format, "u") != 0 && strcmp(sch->format, "U") != 0))
                    ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("native lake Arrow type differs from its declared file type")));
                ++textCount;
            } else {
                const char* expectedFmt = LakeExpectedArrowFormat(colType);
                if (expectedFmt == NULL || strcmp(sch->format, expectedFmt) != 0)
                    ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("native lake Arrow type differs from its declared file type")));
            }
        }

        Size remainingBudget = *payloadBytes <= payloadLimit ? payloadLimit - *payloadBytes : 0;
        Size slicePayload = 0;
        bool hitLimit = false;
        if (textCount > 0 || (Size)projectedCount * sizeof(Datum) * (Size)take > remainingBudget) {
            for (int r = 0; r < take; ++r) {
                Size rowBytes = 0;
                int64 batchRow = reader->batch_offset + r;
                for (int c = 0; c < ncols; ++c) {
                    if (projected != NULL && !projected[c])
                        continue;
                    int child = state->open_to_batch[c];
                    ArrowArray* arr = srcBatch->array->children[child];
                    const uint8* valid = (const uint8*)arr->buffers[0];
                    int64 idx = arr->offset + batchRow;
                    if (!LakeArrowNotNull(valid, idx))
                        continue;
                    Size added;
                    if (binding->columns[c].type == TEXTOID) {
                        ArrowSchema* sch = srcBatch->schema->children[child];
                        int64 start, end;
                        if (sch->format[0] == 'u') {
                            const int32* offs = (const int32*)arr->buffers[1];
                            start = offs[idx];
                            end = offs[idx + 1];
                        } else {
                            const int64* offs = (const int64*)arr->buffers[1];
                            start = offs[idx];
                            end = offs[idx + 1];
                        }
                        if (unlikely(start < 0 || end < start || end - start > (int64)MaxAllocSize - VARHDRSZ ||
                            (end > start && arr->buffers[2] == NULL)))
                            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                                errmsg("invalid native lake Arrow text offsets")));
                        added = MAXALIGN(VARHDRSZ + (Size)(end - start));
                    } else {
                        added = sizeof(Datum);
                    }
                    if (unlikely(added > payloadLimit || rowBytes > payloadLimit - added))
                        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                            errmsg("native lake batch source row exceeds 8 MiB payload limit")));
                    rowBytes += added;
                }
                if (batch->m_rows + r > 0 && rowBytes > payloadLimit - (*payloadBytes + slicePayload)) {
                    take = r;
                    hitLimit = true;
                    break;
                }
                slicePayload += rowBytes;
            }
        }

        if (take > 0) {
            const int dstBase = batch->m_rows;
            const bool trackFixedPayload = (textCount == 0 && !hitLimit &&
                (Size)projectedCount * sizeof(Datum) * (Size)take <= remainingBudget);
            for (int c = 0; c < ncols; ++c) {
                ScalarVector* vec = &batch->m_arr[c];
                if (projected != NULL && !projected[c]) {
                    memset(vec->m_flag + dstBase, V_NULL_MASK, (size_t)take);
                    vec->m_rows += take;
                    continue;
                }
                int child = state->open_to_batch[c];
                ArrowArray* arr = srcBatch->array->children[child];
                ArrowSchema* sch = srcBatch->schema->children[child];
                const uint8* valid = (const uint8*)arr->buffers[0];
                const int64 idx0 = arr->offset + reader->batch_offset;
                ScalarValue* dstVals = vec->m_vals + dstBase;
                Size nonNullRows = 0;
                Oid colType = binding->columns[c].type;
                switch (colType) {
                    case BOOLOID: {
                        const uint8* data = (const uint8*)arr->buffers[1];
                        if (valid == NULL) {
                            for (int r = 0; r < take; ++r) {
                                int64 idx = idx0 + r;
                                dstVals[r] = BoolGetDatum((((data[idx / 8] >> (idx % 8)) & 1) != 0));
                            }
                            nonNullRows = (Size)take;
                        } else {
                            for (int r = 0; r < take; ++r) {
                                int64 idx = idx0 + r;
                                if (((valid[idx / 8] >> (idx % 8)) & 1) == 0)
                                    vec->SetNull(dstBase + r);
                                else {
                                    dstVals[r] = BoolGetDatum((((data[idx / 8] >> (idx % 8)) & 1) != 0));
                                    ++nonNullRows;
                                }
                            }
                        }
                        break;
                    }
                    case INT2OID: {
                        const int16* src = (const int16*)arr->buffers[1] + idx0;
                        if (valid == NULL) {
                            for (int r = 0; r < take; ++r)
                                dstVals[r] = Int16GetDatum(src[r]);
                            nonNullRows = (Size)take;
                        } else {
                            for (int r = 0; r < take; ++r) {
                                int64 idx = idx0 + r;
                                if (((valid[idx / 8] >> (idx % 8)) & 1) == 0)
                                    vec->SetNull(dstBase + r);
                                else {
                                    dstVals[r] = Int16GetDatum(src[r]);
                                    ++nonNullRows;
                                }
                            }
                        }
                        break;
                    }
                    case INT4OID: {
                        const int32* src = (const int32*)arr->buffers[1] + idx0;
                        if (valid == NULL) {
                            for (int r = 0; r < take; ++r)
                                dstVals[r] = Int32GetDatum(src[r]);
                            nonNullRows = (Size)take;
                        } else {
                            for (int r = 0; r < take; ++r) {
                                int64 idx = idx0 + r;
                                if (((valid[idx / 8] >> (idx % 8)) & 1) == 0)
                                    vec->SetNull(dstBase + r);
                                else {
                                    dstVals[r] = Int32GetDatum(src[r]);
                                    ++nonNullRows;
                                }
                            }
                        }
                        break;
                    }
                    case INT8OID: {
                        const int64* src = (const int64*)arr->buffers[1] + idx0;
                        if (valid == NULL) {
                            if (sizeof(ScalarValue) == sizeof(int64))
                                memcpy(dstVals, src, sizeof(int64) * (Size)take);
                            else
                                for (int r = 0; r < take; ++r)
                                    dstVals[r] = Int64GetDatum(src[r]);
                            nonNullRows = (Size)take;
                        } else {
                            for (int r = 0; r < take; ++r) {
                                int64 idx = idx0 + r;
                                if (((valid[idx / 8] >> (idx % 8)) & 1) == 0)
                                    vec->SetNull(dstBase + r);
                                else {
                                    dstVals[r] = Int64GetDatum(src[r]);
                                    ++nonNullRows;
                                }
                            }
                        }
                        break;
                    }
                    case FLOAT4OID: {
                        const float4* src = (const float4*)arr->buffers[1] + idx0;
                        if (valid == NULL) {
                            for (int r = 0; r < take; ++r)
                                dstVals[r] = Float4GetDatum(src[r]);
                            nonNullRows = (Size)take;
                        } else {
                            for (int r = 0; r < take; ++r) {
                                int64 idx = idx0 + r;
                                if (((valid[idx / 8] >> (idx % 8)) & 1) == 0)
                                    vec->SetNull(dstBase + r);
                                else {
                                    dstVals[r] = Float4GetDatum(src[r]);
                                    ++nonNullRows;
                                }
                            }
                        }
                        break;
                    }
                    case FLOAT8OID: {
                        const float8* src = (const float8*)arr->buffers[1] + idx0;
                        if (valid == NULL) {
                            for (int r = 0; r < take; ++r)
                                dstVals[r] = Float8GetDatum(src[r]);
                            nonNullRows = (Size)take;
                        } else {
                            for (int r = 0; r < take; ++r) {
                                int64 idx = idx0 + r;
                                if (((valid[idx / 8] >> (idx % 8)) & 1) == 0)
                                    vec->SetNull(dstBase + r);
                                else {
                                    dstVals[r] = Float8GetDatum(src[r]);
                                    ++nonNullRows;
                                }
                            }
                        }
                        break;
                    }
                    case TEXTOID: {
                        const bool smallUtf8 = (sch->format[0] == 'u');
                        const int32* offs32 = smallUtf8 ? (const int32*)arr->buffers[1] : NULL;
                        const int64* offs64 = smallUtf8 ? NULL : (const int64*)arr->buffers[1];
                        const char* bytes = (const char*)arr->buffers[2];
                        Size colBytes = 0;
                        for (int r = 0; r < take; ++r) {
                            int64 idx = idx0 + r;
                            if (valid != NULL && ((valid[idx / 8] >> (idx % 8)) & 1) == 0) {
                                vec->SetNull(dstBase + r);
                                continue;
                            }
                            int64 start = smallUtf8 ? offs32[idx] : offs64[idx];
                            int64 end = smallUtf8 ? offs32[idx + 1] : offs64[idx + 1];
                            int len = (int)(end - start);
                            const char* txt = len ? bytes + start : "";
                            if (unlikely(memchr(txt, 0, len) != NULL))
                                ereport(ERROR, (errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
                                    errmsg("native lake text contains a zero byte")));
                            (void)pg_verify_mbstr(PG_UTF8, txt, len, false);
                            text* dst = (text*)vec->m_buf->Allocate(len + VARHDRSZ);
                            SET_VARSIZE(dst, len + VARHDRSZ);
                            if (len > 0) {
                                errno_t rc = memcpy_s(VARDATA(dst), len, txt, len);
                                securec_check(rc, "", "");
                            }
                            dstVals[r] = PointerGetDatum(dst);
                            colBytes += MAXALIGN((Size)(len + VARHDRSZ));
                        }
                        if (colBatchBytes != NULL)
                            colBatchBytes[c] += colBytes;
                        break;
                    }
                    default:
                        Assert(false);
                        break;
                }
                if (trackFixedPayload && colType != TEXTOID)
                    slicePayload += nonNullRows * sizeof(Datum);
                vec->m_rows += take;
            }
            batch->m_rows += take;
            reader->batch_offset += take;
            reader->position += take;
            *payloadBytes += slicePayload;
            filled += take;
        }
        if (hitLimit)
            break;
    }
    if (eos && reader->file_index >= binding->file_count)
        *eos = true;
    return filled;
}

bool LakeFileReaderNext(LakeFileReader* reader,Datum* values,bool* nulls,LakeInlineRowIdentity* identity)
{
    if(reader==NULL)return false;
    LakeFileReaderRequireAdmitted(reader);
    LakeInlineBinding* binding=reader->binding;
    while(reader->file_index<binding->file_count){
        CHECK_FOR_INTERRUPTS();LakeDataFile* file=&binding->files[reader->file_index];
        LakeFileState* state=&reader->states[reader->file_index];
        if(state->partition_skipped||(state->indexed&&state->selected_rows==0)){++reader->file_index;continue;}
        if(!LakeFileEnsureBatch(reader,file,state))continue;
        int row_id_column=state->explicit_row_id?state->open_columns:-1;
        int snapshot_column=state->explicit_snapshot?(state->open_columns+(state->explicit_row_id?1:0)):-1;
        OgLakeSdkBatch* batch=reader->handles->batch;int64 row=reader->batch_offset;
        int64 position=batch->original_positions[row],expected=reader->position;
        if(state->indexed){
            while(reader->group_cursor<state->group_count&&reader->group_offset==state->group_rows[reader->group_cursor]){
                ++reader->group_cursor;reader->group_offset=0;
            }
            if(reader->group_cursor>=state->group_count)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("indexed source exceeded its selected row groups")));
            expected=state->group_first[reader->group_cursor]+reader->group_offset++;
        }
        if(position!=expected||position<0||position>=file->record_count)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake file positions lost their original order")));
        int64 row_id=row_id_column>=0?LakeRequiredInteger(batch,row_id_column,row):file->row_id_start+position;
        int64 begin=snapshot_column>=0?LakeRequiredInteger(batch,snapshot_column,row):file->begin_snapshot;
        if(state->retired_fields&&((!state->rewritten_birth_upper&&begin<state->retired_birth_min)||begin<0||begin>=state->retired_birth_end))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native file row birth contradicts retired fields")));
        if((file->partial_max>=0&&begin>file->partial_max)||(state->rewritten_birth_upper&&begin>file->begin_snapshot))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake row snapshot exceeds its partial maximum")));
        for(int c=0;c<binding->column_count;++c)
            if(state->logical_to_batch[c]<0&&!LakeColumnCanBeMissing(binding,c,begin))
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("file row birth is newer than its missing field")));
        ++reader->batch_offset;++reader->position;
        if(reader->verify_rewrite_partition)LakeCheckRewritePhysicalPartition(reader,batch,row);
        if(state->indexed&&LakeFileCandidateFind(reader,reader->file_index,position)<0)continue;
        if(begin>binding->snapshot_id||(state->delete_count>0&&
            bsearch(&position,state->deleted_positions,state->delete_count,sizeof(int64),LakeComparePosition)!=NULL))continue;
        /* Hide before copying Datums. SQL0.17 resolves this through the view's
         * shared stable-row journal, including origins with an older birth;
         * legacy tables keep their exact (row_id,birth) overlay contract. */
        if(reader->table!=NULL&&LakeTxnHidesInlineRow(reader->table,row_id,begin,reader->command_limit))continue;
        Size binaryRowBytes=0;
        for(int i=0;i<binding->column_count;++i){
            if(reader->projected&&!reader->projected[i]){values[i]=(Datum)0;nulls[i]=true;continue;}
            int child=state->open_to_batch[i];
            if(child<0){LakeMaterializeInitialDefault(&binding->columns[i],&values[i],&nulls[i],&binaryRowBytes);continue;}
            ArrowArray* array=batch->array->children[child];ArrowSchema* schema=batch->schema->children[child];
            values[i]=LakeDecodeFileField(array,schema,row,state->field_types[i],state->field_typmods[i],
                LakeColumnTargetType(&binding->columns[i]),LakeColumnTargetTypmod(&binding->columns[i]),
                &binaryRowBytes,&nulls[i]);
        }
        identity->source=LAKE_LOCATOR_FILE;identity->inline_oid=InvalidOid;identity->row_id=row_id;identity->begin_snapshot=begin;identity->local_token=0;
        identity->file_id=file->file_id;identity->file_position=position;identity->ended=false;identity->end_snapshot=0;return true;
    }
    return false;
}

void LakeFileReaderClose(LakeFileReader* reader)
{
    if(reader==NULL)return;
    LakeAnnRescanClose(&reader->ann_rescan_cache);
    if(reader->rewrite_partition_scratch){MemoryContextDelete(reader->rewrite_partition_scratch);reader->rewrite_partition_scratch=NULL;}
    reader->verify_rewrite_partition=false;
    LakeFileHandlesClose(reader->handles);
    if(reader->candidate_context){MemoryContextDelete(reader->candidate_context);reader->candidate_context=NULL;reader->selection=NULL;reader->ann_selection=NULL;}
}
void LakeFileReaderReScan(LakeFileReader* reader)
{
    if(reader==NULL)return;
    LakeFileHandlesClose(reader->handles);reader->file_index=0;reader->position=0;reader->batch_offset=0;
    reader->group_cursor=0;reader->group_offset=0;
    if(reader->deferred_admission)reader->admission_pending=true;
    if(reader->candidate_context)MemoryContextReset(reader->candidate_context);
    reader->selection=NULL;reader->ann_selection=NULL;
    for(int f=0;f<reader->binding->file_count;++f){reader->states[f].indexed=false;reader->states[f].groups=NULL;}
}

const char* LakeFileReaderObjectVersion(const LakeFileReader* reader,int file)
{
    if(!reader||file<0||file>=reader->binding->file_count)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid lake file source identity request")));
    return reader->states[file].object_version;
}

const int64* LakeFileReaderVisibleDeletes(const LakeFileReader* reader,int file,int* count)
{
    if(!reader||file<0||file>=reader->binding->file_count||!count)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid lake Stream delete projection")));
    *count=reader->states[file].delete_count;return reader->states[file].deleted_positions;
}

/* Positions are selected before reading business values. Reuse authoritative
 * query visibility, but never replace failed group admission by a full scan. */
void LakeFileReadSamplePositions(LakeTxnTable* table,CommandId commandLimit,int fileIndex,
    const int64* positions,int count,LakeAnalyzeRowVisitor visitor,void* argument,LakeAnalyzeIoBudget* budget)
{
    LakeInlineBinding* binding=table->binding;
    if(fileIndex<0||fileIndex>=binding->file_count||count<=0||positions==NULL||visitor==NULL||budget==NULL)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid native lake ANALYZE file sample")));
    LakeDataFile* file=&binding->files[fileIndex];
    if(file->file_bytes<0||file->record_count<0)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid ANALYZE source population or byte size")));
    for(int i=0;i<count;++i)if(positions[i]<0||positions[i]>=file->record_count||(i&&positions[i]<=positions[i-1]))
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("ANALYZE positions must be sorted distinct physical addresses")));
    ++budget->files;
    /* Independent footer_open has no SDK byte counter. Reserve conservatively;
     * this is neither measured total I/O nor a transport upper-bound proof.
     * reader_open's footer traffic is counted separately by reader_stats. */
    OgLakeSdkLimits limits=LakeFileLimits();
    uint64 reserve=2*Min((uint64)file->file_bytes,(uint64)limits.footer_bytes+65536);
    budget->unobserved_footer_opens+=2;
    if(file->deletes){
        if(file->deletes->file_bytes<0||(uint64)file->deletes->file_bytes>PG_UINT64_MAX-budget->planned_bytes)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid ANALYZE delete file byte size")));
        budget->planned_bytes+=(uint64)file->deletes->file_bytes;
        reserve+=Min((uint64)file->deletes->file_bytes,(uint64)limits.footer_bytes+65536);
        ++budget->unobserved_footer_opens;
    }
    if(reserve>PG_UINT64_MAX-budget->reserved_footer_bytes)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("ANALYZE footer reservation overflow")));
    budget->reserved_footer_bytes+=reserve;LakeAnalyzeCheckIo(budget);
    MemoryContext caller=CurrentMemoryContext;
    MemoryContext scratch=AllocSetContextCreate(caller,"Lake ANALYZE sampled file",ALLOCSET_DEFAULT_SIZES);
    LakeFileReader* volatile reader=NULL;
    PG_TRY();{
        MemoryContextSwitchTo(scratch);
        LakeInlineBinding* one=(LakeInlineBinding*)palloc(sizeof(*one));*one=*binding;
        one->file_count=1;one->files=file;
        reader=LakeFileReaderOpen(one,table,commandLimit,budget);
        ((LakeFileReader*)reader)->sample_positions=positions;
        ((LakeFileReader*)reader)->sample_position_count=count;
        LakeFilePrepareCandidateGroups((LakeFileReader*)reader,0);
        MemoryContext rowContext=AllocSetContextCreate(scratch,"Lake ANALYZE file row",ALLOCSET_DEFAULT_SIZES);
        Datum* values=(Datum*)palloc0(sizeof(Datum)*binding->column_count);
        bool* nulls=(bool*)palloc0(sizeof(bool)*binding->column_count);
        for(;;){
            MemoryContextReset(rowContext);MemoryContextSwitchTo(rowContext);
            LakeInlineRowIdentity identity;
            bool found=LakeFileReaderNext((LakeFileReader*)reader,values,nulls,&identity);
            MemoryContextSwitchTo(caller);if(!found)break;
            visitor(values,nulls,&identity,argument);LakeAnalyzeCheckIo(budget);
        }
        LakeFileReaderClose((LakeFileReader*)reader);reader=NULL;
        MemoryContextSwitchTo(caller);MemoryContextDelete(scratch);
    }PG_CATCH();{
        MemoryContextSwitchTo(caller);if(reader)LakeFileReaderClose((LakeFileReader*)reader);
        MemoryContextDelete(scratch);PG_RE_THROW();
    }PG_END_TRY();
}

bool LakeFileDecodeNull(const ArrowArray* array,int64 row)
{
    return LakeSdkNull(array,row);
}

Datum LakeFileDecodeValue(const ArrowArray* array,const ArrowSchema* schema,
    int64 row,Oid type,Size* binaryRowBytes,int32 typmod)
{
    return LakeSdkValue(array,schema,row,type,binaryRowBytes,typmod);
}

OgLakeSdkBatch* LakeFileCheckArrowBatch(OgLakeSdkBatch* batch,int columns)
{
    return LakeCheckBatch(batch,columns);
}
