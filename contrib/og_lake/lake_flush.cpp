/* Standard DuckLake inline history -> immutable files before snapshot claim. */
#include "postgres.h"
#include "lake_column_layout.h"
#include "lake_inline_sources.h"
#include "lake_temporal_text.h"
#include "lake_vector_codec.h"
#include "lake_types.h"
#include "lake_flush.h"
#include "lake_version.h"
#include "lake_arrow_builder.h"
#include "knl/knl_variable.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "parser/parse_oper.h"
#include "utils/tuplesort.h"
#include "utils/tuplestore.h"
#include "access/heapam.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/snapmgr.h"
#include "utils/rel.h"

/* Move an ordered prefix of complete logical versions per call. The native
 * sort can spill; publication deletes only this exact identity interval. */
static const int LAKE_FLUSH_VERSION_LIMIT=262144;
static_assert(LAKE_FLUSH_VERSION_LIMIT==LAKE_PARTITION_FLUSH_MAP_LIMIT,"partition map must cover the original complete source prefix");
struct LakeFlushScan {
    Relation relation;
    TableScanDesc scan;
    Tuplesortstate* sort;
    Tuplestorestate* spool;
    TupleTableSlot* slot;
};

static void LakeFlushCloseInput(LakeFlushScan* input)
{
    if(input->slot){ExecDropSingleTupleTableSlot(input->slot);input->slot=NULL;}
    if(input->sort){tuplesort_end(input->sort);input->sort=NULL;}
    if(input->spool){tuplestore_end(input->spool);input->spool=NULL;}
    if(input->scan){tableam_scan_end(input->scan);input->scan=NULL;}
    if(input->relation){heap_close(input->relation,NoLock);input->relation=NULL;}
}

static void LakeFlushOrderInput(LakeFlushOperation* op,LakeFlushScan* input,TupleDesc desc,int memoryKB=0)
{
    TupleDesc ordered=CreateTupleDescCopy(desc);ordered->td_tam_ops=TableAmHeap;
    input->slot=MakeSingleTupleTableSlot(ordered);
    AttrNumber keys[2]={1,2};Oid less=InvalidOid;
    get_sort_group_operators(INT8OID,true,false,false,&less,NULL,NULL,NULL);
    Oid operators[2]={less,less},collations[2]={InvalidOid,InvalidOid};
    bool nullsFirst[2]={false,false};
    /* Respect smaller session budgets; bound maintenance sort memory above.
     * Large carriers use the host temporary-file and cancellation machinery. */
    int memory=Max(64,Min(u_sess->attr.attr_memory.work_mem,16384));
    if(memoryKB>0)memory=Min(memory,memoryKB);
    input->sort=tuplesort_begin_heap(ordered,2,keys,operators,collations,nullsFirst,memory,false);
    tuplesort_set_bound(input->sort,(int64)LAKE_FLUSH_VERSION_LIMIT+1);
    const LakeInlineBinding* logical=op->table->binding;
    const LakeInlineSource* physical=logical->inline_sources_loaded?
        LakeFindInlineSource(logical,LakeFlushBinding(op)->inline_oid):NULL;
    input->scan=tableam_scan_begin(input->relation,LakeTxnBusinessSnapshot(op->table),0,NULL);
    for(;;){
        CHECK_FOR_INTERRUPTS();Tuple tuple=tableam_scan_getnexttuple(input->scan,ForwardScanDirection);
        if(tuple==NULL)break;
        ExecClearTuple(input->slot);
        tableam_tops_deform_tuple(tuple,desc,input->slot->tts_values,input->slot->tts_isnull);
        Datum* values=input->slot->tts_values;bool* absent=input->slot->tts_isnull;
        if(absent[0]||absent[1])
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("NULL native flush version identity")));
        int64 id=DatumGetInt64(values[0]),birth=DatumGetInt64(values[1]);
        if(id<0||id>=LAKE_REFERENCE_LOCAL_ID_START||birth<0)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native flush version identity")));
        /* The source writer uses its original complete schema. A missing
         * later field is NULL only for versions born before its introduction,
         * including rows outside this round's H; never export corrupt rows. */
        if(physical)for(int c=0;c<logical->column_count;++c)
            if(physical->logical_to_source[c]<0&&!LakeColumnCanBeMissing(logical,c,birth))
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native flush row lacks a required added field")));
        if(physical)for(int p=0;p<physical->column_count;++p)
            if(LakeFindRetiredColumn(logical,physical->columns[p].column_id))
                LakeValidateRetiredBirth(logical,physical->columns[p].column_id,birth,birth);
        int64 birthLimit=op->round.enabled?op->round.birth_cutoff:op->cutoff;
        if(birth>birthLimit)continue;
        if(op->round.enabled){
            if(op->round.eligible_rows==PG_INT64_MAX)
                ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("identity flush eligible version count overflows")));
            ++op->round.eligible_rows;
        }
        if(!absent[2]&&(DatumGetInt64(values[2])<=birth||DatumGetInt64(values[2])>op->cutoff))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native flush version end snapshot")));
        ExecStoreVirtualTuple(input->slot);tuplesort_puttupleslot(input->sort,input->slot);
    }
    ExecClearTuple(input->slot);
    tableam_scan_end(input->scan);input->scan=NULL;
    tuplesort_performsort(input->sort);
}

static uint32 LakeFlushType(Oid type)
{
    LakeNativeTypeRequireSdk(type,LakeFileProvider());
    return LakeNativeTypeByOid(type)->sdk_type;
}

void LakeFlushAppend(LakeArrowBuilder* batch,LakeFileCandidate* candidate,Datum* values,bool* nulls)
{
    if(!LakeArrowBuilderAppend(batch,values,nulls)){
        LakeArrowBuilderWrite(batch,candidate->api,candidate->writer);LakeArrowBuilderReset(batch);
        if(!LakeArrowBuilderAppend(batch,values,nulls))
            ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("empty native flush batch rejected a row")));
    }
}

int32* LakeFlushTypmods(const LakeInlineBinding* binding,int count)
{
    if(!binding||count<binding->column_count||count>binding->column_count+2)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("native decimal writer schema differs from its source binding")));
    bool numeric=false;
    for(int c=0;c<binding->column_count;++c)if(binding->columns[c].type==NUMERICOID)numeric=true;
    if(!numeric)return NULL; /* old scalar/UUID/JSON/LIST builder unchanged */
    int32* result=(int32*)palloc(sizeof(int32)*count);
    for(int c=0;c<count;++c)result[c]=c<binding->column_count?binding->columns[c].typmod:-1;
    return result;
}

LakeFileCandidate* LakeFlushOpen(const char* path,const OgLakeSdkField* fields,int count,
    int listCount,const OgLakeSdkListField* lists,const LakeInlineBinding* binding)
{
    OgLakeSdkWriteOptions options={};options.struct_size=sizeof(options);options.path=path;
    options.limits=LakeFileLimits();options.num_fields=count;options.fields=fields;
    options.compression=OG_LAKE_SDK_ZSTD;options.row_group_rows=4096;
    int decimalsCount=0;OgLakeSdkDecimalField* decimals=NULL;
    for(int c=0;c<count;++c)if(fields[c].type==OG_LAKE_SDK_DECIMAL128)++decimalsCount;
    if(decimalsCount){
        if(!binding||count<binding->column_count)
            ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("native decimal writer requires its original column parameters")));
        decimals=(OgLakeSdkDecimalField*)palloc0(sizeof(*decimals)*decimalsCount);int at=0;
        for(int c=0;c<count;++c)if(fields[c].type==OG_LAKE_SDK_DECIMAL128){
            if(c>=binding->column_count||binding->columns[c].type!=NUMERICOID||fields[c].field_id!=binding->columns[c].column_id)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native decimal ordinal identity changed")));
            int p,s;LakeNumericParameters(binding->columns[c].typmod,&p,&s);
            decimals[at++]={sizeof(OgLakeSdkDecimalField),c,p,s};
        }
    }
    LakeFileCandidate* result=LakeFileCandidateOpen(&options,listCount,lists,decimalsCount,decimals);
    if(decimals)pfree(decimals);
    return result;
}

void LakeFinishFileOutput(MemoryContext context,const LakeInlineBinding* binding,LakeFlushOutput* output,
    const OgLakeSdkField* fields,int count)
{
    LakeFileCandidateFinish(output->candidate);
    LakeSdkHandles* handles=LakeFileHandles(context);
    OgLakeSdkLimits limits=LakeFileLimits();OgLakeSdkError error;
    HOLD_INTERRUPTS();int status=handles->api->footer_open(output->candidate->path,&limits,&handles->footer,&error);RESUME_INTERRUPTS();
    LakeFileCheckStatus(status,&error);CHECK_FOR_INTERRUPTS();
    const OgLakeSdkFooterInfo* info=handles->api->footer_info(handles->footer);
    if(info==NULL||info->struct_size<offsetof(OgLakeSdkFooterInfo,object_version)||!(handles->api->capabilities&OG_LAKE_SDK_FOOTER_BYTES)||
        info->num_fields!=count||info->num_row_groups<=0||info->fields==NULL||info->row_groups==NULL||
        info->num_rows!=output->candidate->written.num_rows||info->file_bytes!=output->candidate->written.file_bytes||
        info->footer_bytes<=0||info->footer_bytes>info->file_bytes-8)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native flush output metadata")));
    output->footer_bytes=info->footer_bytes;output->row_groups=info->num_row_groups;
    output->column_bytes=(int64*)palloc0(sizeof(int64)*count);
    for(int c=0;c<count;++c){
        if(info->fields[c].field_id!=fields[c].field_id||info->fields[c].type!=fields[c].type)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native flush output field identity changed")));
        if(fields[c].type==OG_LAKE_SDK_DECIMAL128){
            if(!binding||c>=binding->column_count||binding->columns[c].type!=NUMERICOID||
               fields[c].field_id!=binding->columns[c].column_id||info->fields[c].nullable!=fields[c].nullable)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native decimal finished file lost its column identity")));
            LakeFileCheckDecimal(handles->footer,c,binding->columns[c].typmod);
        }
        if(fields[c].type==OG_LAKE_SDK_FLOAT32_LIST){
            const LakeInlineColumn* expected=&binding->columns[c];int matches=0;
            if(info->struct_size<OG_LAKE_SDK_FIELD_END(OgLakeSdkFooterInfo,list_fields)||
                info->num_list_fields<1||info->num_list_fields>count||info->list_fields==NULL)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native vector flush output lacks element metadata")));
            for(int i=0;i<info->num_list_fields;++i)if(info->list_fields[i].field_id==expected->column_id){
                if(info->list_fields[i].element_field_id!=expected->element_column_id)
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native vector flush output element identity changed")));
                ++matches;
            }
            if(matches!=1)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native vector flush output has ambiguous element metadata")));
        }
        for(int g=0;g<info->num_row_groups;++g){
            if(info->row_groups[g].columns==NULL)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native flush output has no column chunks")));
            int64 bytes=info->row_groups[g].columns[c].compressed_bytes;
            if(bytes<0||output->column_bytes[c]>PG_INT64_MAX-bytes)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native flush column byte count")));
            output->column_bytes[c]+=bytes;
        }
    }
    LakeFileHandlesClose(handles);
}

void LakeFlushFinish(LakeFlushOperation* operation,LakeFlushOutput* output,
    const OgLakeSdkField* fields,int count)
{ LakeFinishFileOutput(operation->context,LakeFlushBinding(operation),output,fields,count); }

int LakeFlushDataCandidateCount(const LakeFlushOperation* op)
{
    int count=0;
    if(op->group_count){
        for(int g=0;g<op->group_count;++g){
            if(op->groups[g].data.candidate)++count;
            if(op->groups[g].deletes.candidate)++count;
        }
    }else{if(op->data.candidate)++count;if(op->deletes.candidate)++count;}
    return count;
}

static bool LakeFlushIdentityKey(const LakePartitionSpec* spec,const LakeIdentityFlushGroup* group,
    const Datum* values,const bool* nulls,const int* attrs)
{ return LakePartitionTupleMatches(spec,&group->key,values,nulls,attrs); }

/* Carrier temporal columns are VARCHAR while partition transforms consume
 * native time Datums. The raw slot must remain intact for spill/replay and
 * for the standard payload decoder. Only the separate key view is changed. */
static const Datum* LakeFlushNativePartitionValues(const LakePartitionSpec* spec,
    const Datum* raw,const bool* nulls,const int* attrs,Datum* decoded,int natts)
{
    if(!decoded)return raw;
    memcpy(decoded,raw,sizeof(Datum)*(Size)natts);
    for(int k=0;k<spec->key_count;++k){
        const LakePartitionField* field=&spec->keys[k];
        if(!LakeNativeTypeIsTemporal(field->type))continue;
        int attr=attrs[field->column_index];
        if(attr<0||attr>=natts)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("time partition carrier attribute is outside its slot")));
        if(!nulls[attr])decoded[attr]=LakeTemporalReadText(raw[attr],field->type);
    }
    return decoded;
}


static char* LakeFlushGroupPath(const char* original,const char* relative)
{
    const char* slash=strrchr(original,'/');
    if(!slash||slash==original)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid identity flush directory")));
    return psprintf("%.*s/%s",(int)(slash-original),original,relative);
}

/* Keep the existing global logical-version prefix. A spillable host spool
 * permits one SDK writer at a time without changing each group's R/birth order.
 * Physical ordinals are local to each file, never the global source ordinal. */
/* Read-only preparation: never hold a control-row lock while SDK work runs.
 * The existing canonical map supplies ownership, alias uniqueness and DROP
 * lifetime. Its progress columns are nullable and have no expression defaults. */
static void LakeReadIdentityFlushRound(LakeFlushOperation* op)
{
    LakeInlineBinding* b=op->table->binding;
    if(!LakeIdentityPartitionVersionEnabled()||b->binding_id==NULL)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("identity flush round needs a canonical SQL22 carrier")));
    Oid types[3]={UUIDOID,UUIDOID,OIDOID};
    Datum args[3]={DirectFunctionCall1(uuid_in,CStringGetDatum(b->binding_id)),
        DirectFunctionCall1(uuid_in,CStringGetDatum(b->table_uuid)),ObjectIdGetDatum(b->relation_oid)};
    LakeMetaRows* state=LakeMetadataQuery("SELECT t.flush_birth_cutoff,t.flush_carrier_oid,t.flush_source_schema,t.flush_source_origin "
        "FROM og_lake_internal.table_binding t JOIN og_lake_internal.relation_binding r ON r.binding_id=t.binding_id "
        "WHERE t.binding_id=$1 AND t.table_uuid=$2 AND r.relation_oid=$3 AND t.state='active' AND NOT t.read_only",
        3,types,args,NULL,LakeTxnBusinessSnapshot(op->table),true,SPI_OK_SELECT);
    LakeMetadataRequireCount(state,1,"canonical identity flush round");
    Datum values[4];bool missing[4];Oid expected[4]={INT8OID,OIDOID,INT8OID,INT8OID};int absent=0;
    for(int i=0;i<4;++i){values[i]=LakeMetadataValue(state,0,i+1,expected[i],true,&missing[i]);if(missing[i])++absent;}
    if(absent!=0&&absent!=4)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("partially initialized identity flush round")));
    if(b->inline_sources_loaded){
        LakeValidateInlineSources(b);
        const LakeInlineSource* selected=NULL;
        if(absent==0){
            selected=LakeFindInlineSource(b,DatumGetObjectId(values[1]));
            if(selected==NULL||selected->schema_version!=DatumGetInt64(values[2])||
               selected->origin_snapshot!=DatumGetInt64(values[3]))
                ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("active native flush round lost its historical carrier")));
        }else{
            /* Older schemas receive no new writes. Drain them first; current
             * schema rounds retain H so continuous DML cannot move their
             * finite goal. Empty registrations need no file or lake snapshot. */
            Oid type=INT8OID;Datum cutoff=Int64GetDatum(op->cutoff);
            for(int i=0;i<b->inline_source_count;++i){
                const LakeInlineSource* candidate=&b->inline_sources[i];
                const char* carrier=LakeMetadataRelation(b->metadata_schema_oid,candidate->table_name);
                LakeMetaRows* eligible=LakeMetadataQuery(psprintf("SELECT EXISTS(SELECT 1 FROM %s "
                    "WHERE _ducklake_begin_snapshot IS NULL OR _ducklake_begin_snapshot<=$1)",carrier),1,&type,&cutoff,NULL,
                    LakeTxnBusinessSnapshot(op->table),true,SPI_OK_SELECT,b->metadata_schema_oid);
                LakeMetadataRequireCount(eligible,1,"historical carrier flush eligibility");
                if(DatumGetBool(LakeMetadataValue(eligible,0,1,BOOLOID))){selected=candidate;break;}
            }
        }
        /* The complete statement view remains retained by op->table. Only
         * physical names/spec are copied into this operation-owned source
         * view; shared catalog/file metadata stays in that retained view. */
        LakeInlineBinding* source=(LakeInlineBinding*)palloc(sizeof(LakeInlineBinding));
        *source=*b;
        source->column_count=selected?selected->column_count:b->column_count;
        source->columns=LakeCopyInlineColumns(selected?selected->columns:b->columns,source->column_count);
        for(int c=0;c<source->column_count;++c){
            source->columns[c].inline_name=selected?pstrdup(selected->column_names[c]):NULL;
            /* This compact source view feeds Arrow, never host slot attnums. */
            source->columns[c].output_attnum=(AttrNumber)(c+1);
        }
        source->inline_oid=selected?selected->relation_oid:InvalidOid;
        source->inline_schema_version=selected?selected->schema_version:b->inline_write_schema_version;
        source->inline_origin_snapshot=selected?selected->origin_snapshot:b->inline_write_origin_snapshot;
        source->host_natts=source->column_count;
        source->inline_sources_loaded=false;source->inline_sources=NULL;source->inline_source_count=0;
        source->inline_write_index=-1;
        MemSet(&source->inline_partition,0,sizeof(source->inline_partition));
        source->inline_partition.end_snapshot=-1;
        if(selected)source->inline_partition=LakePartitionCopySpec(&selected->partition,op->context);
        op->source_binding=source;
        op->source_partition=LakePartitionCopySpec(&source->inline_partition,op->context);
        if(selected){
            op->round.enabled=true;op->round.existing=absent==0;
            op->round.birth_cutoff=absent==0?DatumGetInt64(values[0]):op->cutoff;
            if(op->round.birth_cutoff<0||op->round.birth_cutoff>op->cutoff)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid historical carrier flush cutoff")));
        }
        return;
    }
    if(!b->inline_partition.enabled||!OidIsValid(b->inline_oid)){
        if(absent!=4)
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("active identity flush round lost its partitioned carrier")));
        return; /* Valid ordinary/file-only SQL22 flush has no round to mutate. */
    }
    op->round.enabled=true;op->round.existing=absent==0;
    op->round.birth_cutoff=absent==4?op->cutoff:DatumGetInt64(values[0]);
    if(op->round.birth_cutoff<0||op->round.birth_cutoff>op->cutoff||
       (absent==0&&(DatumGetObjectId(values[1])!=b->inline_oid||DatumGetInt64(values[2])!=b->inline_schema_version||
                    DatumGetInt64(values[3])!=b->inline_origin_snapshot)))
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("identity flush round source changed")));
}

static void LakeFlushRememberVersion(LakeFlushOperation* op,int64 rowid,int64 birth,
    int64 ended,int group,int64 position)
{
    if(op->version_map_count>=LAKE_PARTITION_FLUSH_MAP_LIMIT||group<0||group>=op->group_count||
       position<0||position>=LAKE_PARTITION_FLUSH_MAP_LIMIT)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake bucket source mapping exceeds its bound")));
    if(op->version_map_count==op->version_map_capacity){
        int capacity=op->version_map_capacity?Min(op->version_map_capacity*2,LAKE_PARTITION_FLUSH_MAP_LIMIT):128;
        Size bytes=sizeof(LakeFlushVersionPosition)*(Size)capacity;
        op->version_map=op->version_map?(LakeFlushVersionPosition*)repalloc(op->version_map,bytes):
            (LakeFlushVersionPosition*)MemoryContextAlloc(op->context,bytes);
        op->version_map_capacity=capacity;
    }
    op->version_map[op->version_map_count++]={rowid,birth,ended,group,(int32)position};
}

static LakeFlushOperation* LakePrepareIdentityFlush(LakeFlushOperation* op,
    const char* dataPath,const char* dataName,const char* deletePath,const char* deleteName)
{
    LakeInlineBinding* b=LakeFlushBinding(op);int count=b->column_count+2;
    op->source_partition=LakePartitionCopySpec(&b->inline_partition,op->context);
    if(!op->source_partition.enabled||op->source_partition.key_count<1)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("identity flush lacks its inline source partition")));
    auto* fields=(OgLakeSdkField*)palloc0(sizeof(OgLakeSdkField)*count);
    auto* lists=(OgLakeSdkListField*)palloc0(sizeof(OgLakeSdkListField)*b->column_count);int listCount=0;
    for(int c=0;c<b->column_count;++c){
        fields[c]={b->columns[c].column_id,LakeFlushType(b->columns[c].type),1,b->columns[c].name};
        if(b->columns[c].type==VECTOROID)lists[listCount++]={b->columns[c].column_id,b->columns[c].element_column_id,1};
    }
    fields[count-2]={LAKE_FILE_ROW_ID,OG_LAKE_SDK_INT64,0,"_ducklake_internal_row_id"};
    fields[count-1]={LAKE_FILE_SNAPSHOT_ID,OG_LAKE_SDK_INT64,0,"_ducklake_internal_snapshot_id"};
    op->groups=(LakeIdentityFlushGroup*)palloc0(sizeof(LakeIdentityFlushGroup)*LAKE_IDENTITY_FLUSH_GROUP_LIMIT);
    auto* input=(LakeFlushScan*)palloc0(sizeof(LakeFlushScan));
    PG_TRY();{
        input->relation=heap_open(b->inline_oid,AccessShareLock);TupleDesc desc=RelationGetDescr(input->relation);
        if(desc->natts!=b->column_count+3||get_attnum(b->inline_oid,"_ducklake_row_id")!=1||
           get_attnum(b->inline_oid,"_ducklake_begin_snapshot")!=2||get_attnum(b->inline_oid,"_ducklake_end_snapshot")!=3)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("unsupported identity flush inline layout")));
        for(int i=0;i<3;++i)if(desc->attrs[i].atttypid!=INT8OID)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid identity flush hidden column type")));
        int* attrs=(int*)palloc(sizeof(int)*b->column_count);
        for(int c=0;c<b->column_count;++c){
            attrs[c]=get_attnum(b->inline_oid,LakeInlinePhysicalName(&b->columns[c]))-1;
            if(attrs[c]<3||attrs[c]>=desc->natts||desc->attrs[attrs[c]].atttypid!=LakeInlinePhysicalType(b->columns[c].type)||
               (b->columns[c].type==NUMERICOID&&desc->attrs[attrs[c]].atttypmod!=b->columns[c].typmod))
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("identity flush source column changed")));
        }
        for(int k=0;k<op->source_partition.key_count;++k)
            if(op->source_partition.keys[k].column_index<0||op->source_partition.keys[k].column_index>=b->column_count)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("identity flush source key is outside its columns")));
        bool timeKeys=false;
        for(int k=0;k<op->source_partition.key_count;++k)
            if(LakeNativeTypeIsTemporal(op->source_partition.keys[k].type))timeKeys=true;
        Datum* nativeKeys=timeKeys?(Datum*)palloc(sizeof(Datum)*(Size)desc->natts):NULL;
        /* During copying both sort and spool can retain bounded host memory;
         * work_mem/spill still applies. These are not SDK/RSS budget claims. */
        int memory=Max(64,Min(u_sess->attr.attr_memory.work_mem/2,8192));
        input->spool=tuplestore_begin_heap(true,false,memory);
        LakeFlushOrderInput(op,input,desc,memory);
        while(tuplesort_gettupleslot(input->sort,true,input->slot,NULL)){
            CHECK_FOR_INTERRUPTS();tableam_tslot_getallattrs(input->slot);
            Datum* raw=input->slot->tts_values;bool* absent=input->slot->tts_isnull;
            int64 id=DatumGetInt64(raw[0]),birth=DatumGetInt64(raw[1]);
            if(op->rows&&(id<op->last_row_id||(id==op->last_row_id&&birth<=op->last_birth)))
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("duplicate or unordered identity flush version")));
            if(op->rows>=LAKE_FLUSH_VERSION_LIMIT)break;
            const Datum* keyValues=LakeFlushNativePartitionValues(&op->source_partition,raw,absent,attrs,nativeKeys,desc->natts);
            int group=0;
            for(;group<op->group_count;++group)if(LakeFlushIdentityKey(&op->source_partition,&op->groups[group],keyValues,absent,attrs))break;
            /* Stop before this row; no unselected identity enters the source
             * clear predicate. The next maintenance call can make progress. */
            if(group==op->group_count&&group==LAKE_IDENTITY_FLUSH_GROUP_LIMIT)break;
            if(group==op->group_count){
                auto* g=&op->groups[op->group_count++];
                g->key=LakePartitionFormTuple(&op->source_partition,keyValues,absent,attrs,op->context);
                g->min_birth=g->min_row_id=g->min_end=PG_INT64_MAX;g->max_birth=g->max_end=-1;
                g->null_counts=(int64*)palloc0(sizeof(int64)*b->column_count);
                g->minima=(char**)palloc0(sizeof(char*)*b->column_count);
                g->maxima=(char**)palloc0(sizeof(char*)*b->column_count);
            }
            auto* g=&op->groups[group];
            if(!absent[2]){
                int64 end=DatumGetInt64(raw[2]);
                if(end<=birth||end>op->cutoff)
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid identity flush end snapshot")));
                ++g->ended_count;g->min_end=Min(g->min_end,end);g->max_end=Max(g->max_end,end);
            }
            if(LakePartitionNeedsSourceVersionMap(&op->source_partition))
                LakeFlushRememberVersion(op,id,birth,absent[2]?-1:DatumGetInt64(raw[2]),group,g->rows);
            tuplestore_puttupleslot(input->spool,input->slot);++g->rows;++op->rows;
            g->min_birth=Min(g->min_birth,birth);g->max_birth=Max(g->max_birth,birth);g->min_row_id=Min(g->min_row_id,id);
            op->last_row_id=id;op->last_birth=birth;op->min_birth=Min(op->min_birth,birth);
            op->max_birth=Max(op->max_birth,birth);op->min_row_id=Min(op->min_row_id,id);
        }
        tuplesort_end(input->sort);input->sort=NULL;
        int artifacts=op->group_count;
        for(int g=0;g<op->group_count;++g)if(op->groups[g].ended_count)++artifacts;
        if(artifacts>LakeFileCandidateSlotsAvailable())
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("identity flush exceeds the transaction candidate budget")));
        Datum* values=(Datum*)palloc(sizeof(Datum)*count);bool* nulls=(bool*)palloc0(sizeof(bool)*count);
        bool collectBounds=LakeStatsExtendedVersionEnabled();
        for(int group=0;group<op->group_count;++group){
            auto* g=&op->groups[group];
            g->data.relative_path=psprintf("%s.part-%d.parquet",dataName,group);
            g->data_path=LakeFlushGroupPath(dataPath,g->data.relative_path);
            g->data.candidate=LakeFlushOpen(g->data_path,fields,count,listCount,listCount?lists:NULL,b);
            LakeArrowBuilder* batch=LakeArrowBuilderCreate(op->context,fields,count,4096,1024*1024,LakeFlushTypmods(b,count));
            if(g->ended_count)g->ended=(LakeFlushEnded*)palloc(sizeof(LakeFlushEnded)*(Size)g->ended_count);
            int64 position=0;int ended=0;int sourcePosition=0;tuplestore_rescan(input->spool);
            while(tuplestore_gettupleslot(input->spool,true,false,input->slot)){
                CHECK_FOR_INTERRUPTS();tableam_tslot_getallattrs(input->slot);
                Datum* raw=input->slot->tts_values;bool* absent=input->slot->tts_isnull;
                const Datum* keyValues=LakeFlushNativePartitionValues(&op->source_partition,raw,absent,attrs,nativeKeys,desc->natts);
                bool included=LakeFlushIdentityKey(&op->source_partition,g,keyValues,absent,attrs);
                if(op->version_map){
                    if(sourcePosition>=op->version_map_count)
                        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("bucket spool exceeds original source map")));
                    const auto& mapped=op->version_map[sourcePosition++];
                    if(mapped.row_id!=DatumGetInt64(raw[0])||mapped.birth!=DatumGetInt64(raw[1])||
                       included!=(mapped.group==group)||(included&&mapped.position!=position))
                        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("bucket spool/group order differs from original source map")));
                }
                if(!included)continue;
                for(int c=0;c<b->column_count;++c){
                    values[c]=raw[attrs[c]];nulls[c]=absent[attrs[c]];
                    if(nulls[c])++g->null_counts[c];
                    else{
                        if(collectBounds)
                            LakeObserveColumnBounds(op->context,b->columns[c].type,b->columns[c].typmod,
                                values[c],&g->minima[c],&g->maxima[c]);
                        if(b->columns[c].type==VECTOROID)values[c]=LakeVectorReadText(values[c],b->columns[c].typmod);
                        else if(LakeNativeTypeIsTemporal(b->columns[c].type))values[c]=LakeTemporalReadText(values[c],b->columns[c].type);
                    }
                }
                values[count-2]=raw[0];values[count-1]=raw[1];
                LakeFlushAppend(batch,g->data.candidate,values,nulls);
                for(int c=0;c<b->column_count;++c)
                    if(!nulls[c]&&b->columns[c].type==VECTOROID)pfree(DatumGetPointer(values[c]));
                if(!absent[2]){
                    if(ended>=g->ended_count)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("identity flush end count changed")));
                    g->ended[ended++]={position,DatumGetInt64(raw[2])};
                }
                ++position;
            }
            if(position!=g->rows||ended!=g->ended_count||(op->version_map&&sourcePosition!=op->version_map_count))
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("identity flush group coverage changed")));
            LakeArrowBuilderWrite(batch,g->data.candidate->api,g->data.candidate->writer);LakeArrowBuilderDestroy(batch);
            LakeFlushFinish(op,&g->data,fields,count);
            if(g->data.candidate->written.num_rows!=g->rows)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("identity flush data footer population differs from source")));
            if(g->ended_count){
                OgLakeSdkField schema[3]={{LAKE_DELETE_PATH_ID,OG_LAKE_SDK_UTF8,0,"file_path"},
                    {LAKE_DELETE_POSITION_ID,OG_LAKE_SDK_INT64,0,"pos"},{LAKE_FILE_SNAPSHOT_ID,OG_LAKE_SDK_INT64,0,"_ducklake_internal_snapshot_id"}};
                g->deletes.relative_path=psprintf("%s.part-%d.parquet",deleteName,group);
                g->deletes.candidate=LakeFlushOpen(LakeFlushGroupPath(deletePath,g->deletes.relative_path),schema,3);
                batch=LakeArrowBuilderCreate(op->context,schema,3,4096,1024*1024);
                Datum mark[3]={CStringGetTextDatum(g->data_path),0,0};bool missing[3]={false,false,false};
                for(int e=0;e<g->ended_count;++e){
                    CHECK_FOR_INTERRUPTS();mark[1]=Int64GetDatum(g->ended[e].position);mark[2]=Int64GetDatum(g->ended[e].end_snapshot);
                    LakeFlushAppend(batch,g->deletes.candidate,mark,missing);
                }
                LakeArrowBuilderWrite(batch,g->deletes.candidate->api,g->deletes.candidate->writer);LakeArrowBuilderDestroy(batch);
                LakeFlushFinish(op,&g->deletes,schema,3);
                if(g->deletes.candidate->written.num_rows!=g->ended_count)
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("identity flush delete footer population differs from source")));
            }
        }
        LakeFlushCloseInput(input);
    }PG_CATCH();{MemoryContextSwitchTo(op->context);LakeFlushCloseInput(input);PG_RE_THROW();}PG_END_TRY();
    return op;
}

static LakeFlushOperation* LakePrepareInlineFlushInternal(LakeTxnTable* table,MemoryContext context,
    const char* dataPath,const char* dataName,const char* deletePath,const char* deleteName)
{
    LakeTxnRequireActiveTable(table);
    LakeInlineBinding* binding=table->binding;
    for(int i=0;i<binding->column_count;++i)LakeNativeTypeRequireWrite(binding->columns[i].type);
    MemoryContext caller=MemoryContextSwitchTo(context);
    LakeFlushOperation* op=(LakeFlushOperation*)palloc0(sizeof(LakeFlushOperation));
    op->context=context;op->table=table;op->owner_subid=GetCurrentSubTransactionId();op->cutoff=binding->snapshot_id;
    op->min_birth=op->min_row_id=op->min_end=PG_INT64_MAX;op->max_birth=op->max_end=-1;
    op->last_row_id=op->last_birth=-1;
    op->data.relative_path=pstrdup(dataName);op->deletes.relative_path=pstrdup(deleteName);
    if(LakeIdentityPartitionVersionEnabled())LakeReadIdentityFlushRound(op);
    binding=LakeFlushBinding(op);
    if(!OidIsValid(binding->inline_oid)){MemoryContextSwitchTo(caller);return op;}
    if(binding->inline_partition.enabled){
        LakePrepareIdentityFlush(op,dataPath,dataName,deletePath,deleteName);
        MemoryContextSwitchTo(caller);return op;
    }
    int count=binding->column_count+2;
    OgLakeSdkField* fields=(OgLakeSdkField*)palloc0(sizeof(OgLakeSdkField)*count);
    OgLakeSdkListField* lists=(OgLakeSdkListField*)palloc0(sizeof(OgLakeSdkListField)*binding->column_count);
    int listCount=0;
    op->null_counts=(int64*)palloc0(sizeof(int64)*binding->column_count);
    op->minima=(char**)palloc0(sizeof(char*)*binding->column_count);
    op->maxima=(char**)palloc0(sizeof(char*)*binding->column_count);
    for(int i=0;i<binding->column_count;++i){
        fields[i]={binding->columns[i].column_id,LakeFlushType(binding->columns[i].type),1,binding->columns[i].name};
        if(binding->columns[i].type==VECTOROID)
            lists[listCount++]={binding->columns[i].column_id,binding->columns[i].element_column_id,1};
    }
    fields[count-2]={LAKE_FILE_ROW_ID,OG_LAKE_SDK_INT64,0,"_ducklake_internal_row_id"};
    fields[count-1]={LAKE_FILE_SNAPSHOT_ID,OG_LAKE_SDK_INT64,0,"_ducklake_internal_snapshot_id"};
    LakeFlushScan* resources=(LakeFlushScan*)palloc0(sizeof(LakeFlushScan));
    PG_TRY();
    {
        resources->relation=heap_open(binding->inline_oid,AccessShareLock);
        TupleDesc desc=RelationGetDescr(resources->relation);
        if(desc->natts!=binding->column_count+3||get_attnum(binding->inline_oid,"_ducklake_row_id")!=1||
            get_attnum(binding->inline_oid,"_ducklake_begin_snapshot")!=2||get_attnum(binding->inline_oid,"_ducklake_end_snapshot")!=3)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("unsupported native flush inline layout")));
        for(int i=0;i<3;++i)if(desc->attrs[i].atttypid!=INT8OID)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native flush hidden column type")));
        int* attrs=(int*)palloc(sizeof(int)*binding->column_count);
        for(int i=0;i<binding->column_count;++i){
            attrs[i]=get_attnum(binding->inline_oid,LakeInlinePhysicalName(&binding->columns[i]))-1;
            if(attrs[i]<3||attrs[i]>=desc->natts||desc->attrs[attrs[i]].atttypid!=LakeInlinePhysicalType(binding->columns[i].type)||
               (binding->columns[i].type==NUMERICOID&&desc->attrs[attrs[i]].atttypmod!=binding->columns[i].typmod))
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native flush input column changed")));
        }
        Datum* values=(Datum*)palloc(sizeof(Datum)*count);bool* nulls=(bool*)palloc0(sizeof(bool)*count);
        LakeArrowBuilder* batch=LakeArrowBuilderCreate(context,fields,count,4096,1024*1024,LakeFlushTypmods(binding,count));
        bool collectBounds=LakeStatsExtendedVersionEnabled();
        LakeFlushOrderInput(op,resources,desc);
        while(tuplesort_gettupleslot(resources->sort,true,resources->slot,NULL)){
            CHECK_FOR_INTERRUPTS();tableam_tslot_getallattrs(resources->slot);
            Datum* raw=resources->slot->tts_values;bool* rawNull=resources->slot->tts_isnull;
            int64 id=DatumGetInt64(raw[0]),birth=DatumGetInt64(raw[1]);
            if(op->rows>0&&(id<op->last_row_id||(id==op->last_row_id&&birth<=op->last_birth)))
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("duplicate or unordered native flush logical version")));
            /* Examine one extra key so a duplicate at the boundary cannot be
             * deleted without having been written. The rest stays inline. */
            if(op->rows>=LAKE_FLUSH_VERSION_LIMIT)break;
            if(!rawNull[2]){
                int64 end=DatumGetInt64(raw[2]);
                if(end<=birth||end>op->cutoff)
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native flush version end snapshot")));
                if(op->ended_count==op->ended_capacity){
                    op->ended_capacity=op->ended_capacity?op->ended_capacity*2:128;
                    Size bytes=sizeof(LakeFlushEnded)*op->ended_capacity;
                    op->ended=op->ended?(LakeFlushEnded*)repalloc(op->ended,bytes):(LakeFlushEnded*)palloc(bytes);
                }
                op->ended[op->ended_count++]={op->rows,end};op->min_end=Min(op->min_end,end);op->max_end=Max(op->max_end,end);
            }
            for(int i=0;i<binding->column_count;++i){
                values[i]=raw[attrs[i]];nulls[i]=rawNull[attrs[i]];
                if(nulls[i])++op->null_counts[i];
                else{
                    if(collectBounds)
                        LakeObserveColumnBounds(context,binding->columns[i].type,binding->columns[i].typmod,
                            values[i],&op->minima[i],&op->maxima[i]);
                    if(binding->columns[i].type==VECTOROID)
                        values[i]=LakeVectorReadText(values[i],binding->columns[i].typmod);
                    else if(LakeNativeTypeIsTemporal(binding->columns[i].type))
                        values[i]=LakeTemporalReadText(values[i],binding->columns[i].type);
                }
            }
            values[count-2]=raw[0];values[count-1]=raw[1];
            if(op->data.candidate==NULL)op->data.candidate=LakeFlushOpen(dataPath,fields,count,listCount,listCount?lists:NULL,binding);
            LakeFlushAppend(batch,op->data.candidate,values,nulls);
            for(int i=0;i<binding->column_count;++i)
                if(!nulls[i]&&binding->columns[i].type==VECTOROID)pfree(DatumGetPointer(values[i]));
            ++op->rows;op->last_row_id=id;op->last_birth=birth;
            op->min_row_id=Min(op->min_row_id,id);op->min_birth=Min(op->min_birth,birth);op->max_birth=Max(op->max_birth,birth);
        }
        LakeFlushCloseInput(resources);
        if(op->rows){LakeArrowBuilderWrite(batch,op->data.candidate->api,op->data.candidate->writer);LakeFlushFinish(op,&op->data,fields,count);}
        LakeArrowBuilderDestroy(batch);
        if(op->ended_count){
            OgLakeSdkField deleted[3]={{LAKE_DELETE_PATH_ID,OG_LAKE_SDK_UTF8,0,"file_path"},
                {LAKE_DELETE_POSITION_ID,OG_LAKE_SDK_INT64,0,"pos"},{LAKE_FILE_SNAPSHOT_ID,OG_LAKE_SDK_INT64,0,"_ducklake_internal_snapshot_id"}};
            op->deletes.candidate=LakeFlushOpen(deletePath,deleted,3);
            batch=LakeArrowBuilderCreate(context,deleted,3,4096,1024*1024);
            Datum row[3]={CStringGetTextDatum(dataPath),0,0};bool absent[3]={false,false,false};
            for(int i=0;i<op->ended_count;++i){CHECK_FOR_INTERRUPTS();row[1]=Int64GetDatum(op->ended[i].position);row[2]=Int64GetDatum(op->ended[i].end_snapshot);LakeFlushAppend(batch,op->deletes.candidate,row,absent);}
            LakeArrowBuilderWrite(batch,op->deletes.candidate->api,op->deletes.candidate->writer);LakeArrowBuilderDestroy(batch);
            LakeFlushFinish(op,&op->deletes,deleted,3);
        }
        MemoryContextSwitchTo(caller);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(caller);
        LakeFlushCloseInput(resources);
        /* Session-owned candidates survive until post-lock abort cleanup. */
        PG_RE_THROW();
    }
    PG_END_TRY();return op;
}

LakeFlushOperation* LakePrepareInlineFlush(LakeTxnTable* table,MemoryContext parent,
    const char* dataPath,const char* dataName,const char* deletePath,const char* deleteName)
{
    MemoryContext caller=CurrentMemoryContext;
    MemoryContext context=AllocSetContextCreate(parent,"Native lake flush preparation",
        ALLOCSET_DEFAULT_MINSIZE,ALLOCSET_DEFAULT_INITSIZE,ALLOCSET_DEFAULT_MAXSIZE);
    LakeFlushOperation* result=NULL;
    PG_TRY();
    {
        result=LakePrepareInlineFlushInternal(table,context,dataPath,dataName,deletePath,deleteName);
    }
    PG_CATCH();
    {
        /* Includes errors before scan construction, plus OOM/oversized rows.
         * Candidate writers own their schemas and live in session memory;
         * their physical close/abort still runs after native lock release. */
        MemoryContextSwitchTo(caller);
        MemoryContextDelete(context);
        PG_RE_THROW();
    }
    PG_END_TRY();
    return result;
}

PG_FUNCTION_INFO_V1(og_lake_flush);
extern "C" Datum og_lake_flush(PG_FUNCTION_ARGS)
{
    LakeInlineRequireAdministrator();LakeRequireAccessPolicy();LakeRequireFlushVersion();
    Oid relation=PG_GETARG_OID(0);
    /* The prepared operation keeps this explicit view through publication.
     * Unlike an idle readonly journal, uncommitted flush input legitimately
     * needs its immutable binding/snapshot until transaction/subtransaction end. */
    LakeTxnTable* table=LakeTxnReadStatementTable(relation,GetActiveSnapshot());
    MemoryContext parent=LakeTxnBeginFlush(table);
    /* Provider admission precedes directory creation; no persistent file or
     * catalog mutation may precede a rejected SDK/transaction mode. */
    (void)LakeFileLifecycleProvider();
    char* directory=LakePrepareFlushDirectory(table->binding,LakeTxnBusinessSnapshot(table));
    char* identity=DatumGetCString(DirectFunctionCall1(uuid_out,OidFunctionCall0(F_GEN_RANDOM_UUID)));
    char* data=pstrdup(psprintf("og-%s.parquet",identity));
    char* deleted=pstrdup(psprintf("og-%s-delete.parquet",identity));
    MemoryContext caller=CurrentMemoryContext;
    LakeFlushOperation* volatile prepared=NULL;
    PG_TRY();
    {
        prepared=LakePrepareInlineFlush(table,parent,psprintf("%s/%s",directory,data),data,
            psprintf("%s/%s",directory,deleted),deleted);
        LakePrepareFileDeleteFlush((LakeFlushOperation*)prepared,directory,identity);
        LakeTxnAdoptFlush((LakeFlushOperation*)prepared);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(caller);
        /* Data preparation owns failures before it returns. The sibling
         * deletion preparation shares that context and has not been adopted. */
        if(prepared!=NULL)MemoryContextDelete(((LakeFlushOperation*)prepared)->context);
        PG_RE_THROW();
    }
    PG_END_TRY();
    PG_RETURN_VOID();
}
