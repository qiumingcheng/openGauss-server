/* Native input owns all buffers. writer_append only borrows them for one call. */
#include "postgres.h"
#include "lake_uuid.h"
#include "lake_temporal.h"
#include "lake_types.h"
#include "knl/knl_variable.h"
#include "lake_arrow_builder.h"
#include "access/tuptoaster.h"
#include "access/datavec/vector.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include <cmath>

struct LakeArrowColumn {
    ArrowArray array;
    ArrowSchema schema;
    const void* buffers[3];
    unsigned char* valid;
    void* fixed;
    StringInfoData text;
    ArrowArray element_array;
    ArrowSchema element_schema;
    ArrowArray* element_array_ptr;
    ArrowSchema* element_schema_ptr;
    const void* element_buffers[2];
};
struct LakeArrowBuilder {
    MemoryContext context;
    MemoryContext data_context;
    OgLakeSdkField* fields;
    int32* typmods;
    int count;
    int capacity;
    int rows;
    Size limit;
    Size fixed_bytes;
    Size text_bytes;
    LakeArrowColumn* columns;
    struct varlena** packed;
    ArrowArray array;
    ArrowSchema schema;
    const void* root_buffers[1];
};

static void LakeArrowArrayRelease(ArrowArray* array) { array->release=NULL; }
static void LakeArrowSchemaRelease(ArrowSchema* schema) { schema->release=NULL; }

static const char* LakeArrowFormat(uint32 type)
{
    const LakeNativeType* mapped=LakeNativeTypeBySdk(type);
    if(type!=OG_LAKE_SDK_FLOAT32_LIST)LakeNativeTypeRequireWrite(mapped->oid);
    return mapped->arrow_format;
}

static void LakeArrowBuilderInitialize(LakeArrowBuilder* b)
{
    MemoryContext caller=CurrentMemoryContext;
    PG_TRY();
    {
    MemoryContextSwitchTo(b->data_context);
    b->rows=0;b->text_bytes=0;
    b->columns=(LakeArrowColumn*)palloc0(sizeof(LakeArrowColumn)*b->count);
    b->packed=(struct varlena**)palloc0(sizeof(struct varlena*)*b->count);
    errno_t rc=memset_s(&b->array,sizeof(b->array),0,sizeof(b->array));securec_check(rc,"","");
    rc=memset_s(&b->schema,sizeof(b->schema),0,sizeof(b->schema));securec_check(rc,"","");
    b->array.n_buffers=1;b->array.buffers=b->root_buffers;
    b->array.n_children=b->schema.n_children=b->count;
    b->array.children=(ArrowArray**)palloc0(sizeof(ArrowArray*)*b->count);
    b->schema.children=(ArrowSchema**)palloc0(sizeof(ArrowSchema*)*b->count);
    b->array.release=LakeArrowArrayRelease;b->schema.release=LakeArrowSchemaRelease;
    b->schema.format="+s";
    Size bits=((Size)b->capacity+7)/8;
    for(int i=0;i<b->count;++i) {
        LakeArrowColumn* c=&b->columns[i];uint32 type=b->fields[i].type;
        c->valid=(unsigned char*)palloc0(bits);c->buffers[0]=c->valid;
        int width=LakeNativeTypeBySdk(type)->fixed_width;
        Size bytes=width==0?bits:width>0?(Size)width*b->capacity:sizeof(int32)*(b->capacity+1);
        c->fixed=palloc0(bytes);c->buffers[1]=c->fixed;
        c->array.n_buffers=(type==OG_LAKE_SDK_UTF8||type==OG_LAKE_SDK_BINARY||type==OG_LAKE_SDK_JSON)?3:2;c->array.buffers=c->buffers;
        c->array.release=LakeArrowArrayRelease;
        c->schema.format=type==OG_LAKE_SDK_DECIMAL128?LakeNumericArrowFormat(b->typmods[i]):LakeArrowFormat(type);c->schema.name=b->fields[i].name;
        c->schema.flags=b->fields[i].nullable?ARROW_FLAG_NULLABLE:0;
        c->schema.release=LakeArrowSchemaRelease;
        if((type==OG_LAKE_SDK_UTF8||type==OG_LAKE_SDK_BINARY||type==OG_LAKE_SDK_JSON)||type==OG_LAKE_SDK_FLOAT32_LIST)initStringInfo(&c->text);
        if(type==OG_LAKE_SDK_FLOAT32_LIST){
            c->element_array_ptr=&c->element_array;c->element_schema_ptr=&c->element_schema;
            c->array.n_children=c->schema.n_children=1;
            c->array.children=&c->element_array_ptr;c->schema.children=&c->element_schema_ptr;
            c->element_array.n_buffers=2;c->element_array.buffers=c->element_buffers;
            c->element_array.release=LakeArrowArrayRelease;
            c->element_schema.format="f";c->element_schema.name="element";
            c->element_schema.flags=ARROW_FLAG_NULLABLE;
            c->element_schema.release=LakeArrowSchemaRelease;
        }
        b->array.children[i]=&c->array;b->schema.children[i]=&c->schema;
    }
    MemoryContextSwitchTo(caller);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(caller);PG_RE_THROW();
    }
    PG_END_TRY();
}

LakeArrowBuilder* LakeArrowBuilderCreate(MemoryContext parent,const OgLakeSdkField* fields,
    int count,int rows,Size inputBytes,const int32* typmods)
{
    if(fields==NULL||count<1||count>1603||rows<1||rows>4096||inputBytes<1024||inputBytes>64*1024*1024)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid native Arrow batch limits")));
    Size bits=((Size)rows+7)/8,fixed=sizeof(LakeArrowBuilder)+sizeof(LakeArrowColumn)*count+
        (sizeof(struct varlena*)+sizeof(ArrowArray*)+sizeof(ArrowSchema*)+(typmods?sizeof(int32):0))*count;
    for(int i=0;i<count;++i) {
        (void)LakeArrowFormat(fields[i].type);
        if(fields[i].type==OG_LAKE_SDK_DECIMAL128&&(!typmods||!LakeNumericTypmod(typmods[i])))
            ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("native Arrow decimal field has no exact parameters")));
        if(fields[i].name==NULL||fields[i].name[0]=='\0')
            ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("native Arrow field name is empty")));
        int width=LakeNativeTypeBySdk(fields[i].type)->fixed_width;
        fixed+=bits+(width==0?bits:width>0?(Size)width*rows:sizeof(int32)*(rows+1)+1024);
    }
    bool uuid=false;
    for(int i=0;i<count;++i)if(fields[i].type==OG_LAKE_SDK_UUID||fields[i].type==OG_LAKE_SDK_DECIMAL128)uuid=true;
    /* UUID is wider than every existing flat fixed field. Keep the original
     * budget rather than reject a legal wide row solely because the caller
     * requested4096 transport rows. Old schemas take their unchanged path. */
    while(uuid&&fixed>=inputBytes&&rows>1){
        rows=(rows+1)/2;bits=((Size)rows+7)/8;
        fixed=sizeof(LakeArrowBuilder)+sizeof(LakeArrowColumn)*count+
            (sizeof(struct varlena*)+sizeof(ArrowArray*)+sizeof(ArrowSchema*)+(typmods?sizeof(int32):0))*count;
        for(int i=0;i<count;++i){
            int width=LakeNativeTypeBySdk(fields[i].type)->fixed_width;
            fixed+=bits+(width==0?bits:width>0?(Size)width*rows:sizeof(int32)*(rows+1)+1024);
        }
    }
    if(fixed>=inputBytes)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native Arrow fixed buffers exceed input budget")));
    MemoryContext caller=CurrentMemoryContext;
    MemoryContext context=AllocSetContextCreate(parent,"Native lake Arrow batch",ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,ALLOCSET_DEFAULT_MAXSIZE);
    LakeArrowBuilder* b=NULL;
    PG_TRY();
    {
        MemoryContextSwitchTo(context);b=(LakeArrowBuilder*)palloc0(sizeof(LakeArrowBuilder));
        b->context=context;b->count=count;b->capacity=rows;b->limit=inputBytes;b->fixed_bytes=fixed;
        b->typmods=typmods?(int32*)palloc(sizeof(int32)*count):NULL;
        if(typmods)for(int i=0;i<count;++i)b->typmods[i]=typmods[i];
        b->fields=(OgLakeSdkField*)palloc(sizeof(OgLakeSdkField)*count);
        for(int i=0;i<count;++i){b->fields[i]=fields[i];b->fields[i].name=pstrdup(fields[i].name);}
        b->data_context=AllocSetContextCreate(context,"Native lake Arrow buffers",ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,ALLOCSET_DEFAULT_MAXSIZE);
        LakeArrowBuilderInitialize(b);MemoryContextSwitchTo(caller);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(caller);MemoryContextDelete(context);PG_RE_THROW();
    }
    PG_END_TRY();
    return b;
}

static void LakeArrowFreePacked(LakeArrowBuilder* b,const Datum* values)
{
    for(int i=0;i<b->count;++i)if(b->packed[i]!=NULL) {
        if((void*)b->packed[i]!=DatumGetPointer(values[i]))pfree(b->packed[i]);
        b->packed[i]=NULL;
    }
}

bool LakeArrowBuilderAppend(LakeArrowBuilder* b,const Datum* values,const bool* nulls)
{
    if(b->rows>=b->capacity)return false;
    CHECK_FOR_INTERRUPTS();Size extra=0,remaining=b->limit-b->fixed_bytes-b->text_bytes;bool fits=true;
    for(int i=0;i<b->count;++i) {
        if(nulls[i]&&!b->fields[i].nullable)
            ereport(ERROR,(errcode(ERRCODE_NOT_NULL_VIOLATION),errmsg("NULL in required native Arrow field")));
        if(!nulls[i]&&((b->fields[i].type==OG_LAKE_SDK_UTF8||b->fields[i].type==OG_LAKE_SDK_BINARY||b->fields[i].type==OG_LAKE_SDK_JSON)||b->fields[i].type==OG_LAKE_SDK_FLOAT32_LIST)) {
            /* Check raw payload lengths before detoasting. A byte-full batch
             * must not fetch/copy a large value merely to reject and retry it. */
            Size bytes=toast_raw_datum_size(values[i]);
            if(bytes<VARHDRSZ)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native Arrow text length")));
            if(b->fields[i].type==OG_LAKE_SDK_FLOAT32_LIST){
                if(bytes<VECTOR_SIZE(1)||bytes>VECTOR_SIZE(VECTOR_MAX_DIM)||
                    (bytes-VECTOR_SIZE(0))%sizeof(float4)!=0)
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native Arrow vector length")));
                bytes-=VECTOR_SIZE(0);
            }else bytes-=VARHDRSZ;
            if(!fits||bytes>remaining-extra)fits=false;
            else extra+=bytes;
        }
    }
    if(!fits) {
        if(b->rows==0)ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("one native Arrow row exceeds input budget")));
        return false;
    }
    for(int i=0;i<b->count;++i)
        if(!nulls[i]){
            if((b->fields[i].type==OG_LAKE_SDK_UTF8||b->fields[i].type==OG_LAKE_SDK_BINARY||b->fields[i].type==OG_LAKE_SDK_JSON))b->packed[i]=PG_DETOAST_DATUM_PACKED(values[i]);
            else if(b->fields[i].type==OG_LAKE_SDK_FLOAT32_LIST)b->packed[i]=PG_DETOAST_DATUM(values[i]);
        }
    MemoryContext caller=CurrentMemoryContext;int row=b->rows;
    PG_TRY();
    {
    MemoryContextSwitchTo(b->data_context);
    for(int i=0;i<b->count;++i) {
        LakeArrowColumn* c=&b->columns[i];uint32 type=b->fields[i].type;
        if(nulls[i])++c->array.null_count;
        else {
            c->valid[row>>3]|=(1u<<(row&7));
            if(type==OG_LAKE_SDK_BOOL) {
                if(DatumGetBool(values[i]))((unsigned char*)c->fixed)[row>>3]|=(1u<<(row&7));
            }else if(type==OG_LAKE_SDK_INT16)((int16*)c->fixed)[row]=DatumGetInt16(values[i]);
            else if(type==OG_LAKE_SDK_INT32)((int32*)c->fixed)[row]=DatumGetInt32(values[i]);
            else if(type==OG_LAKE_SDK_INT64)((int64*)c->fixed)[row]=DatumGetInt64(values[i]);
            else if(type==OG_LAKE_SDK_DATE32)((int32*)c->fixed)[row]=LakeTemporalDateToUnix(DatumGetDateADT(values[i]));
            else if(type==OG_LAKE_SDK_TIMESTAMP_US)((int64*)c->fixed)[row]=LakeTemporalTimestampToUnix(DatumGetTimestamp(values[i]));
            else if(type==OG_LAKE_SDK_TIMESTAMPTZ_US)((int64*)c->fixed)[row]=LakeTemporalTimestampToUnix(DatumGetTimestampTz(values[i]));
            else if(type==OG_LAKE_SDK_FLOAT32)((float4*)c->fixed)[row]=DatumGetFloat4(values[i]);
            else if(type==OG_LAKE_SDK_FLOAT64)((float8*)c->fixed)[row]=DatumGetFloat8(values[i]);
            else if(type==OG_LAKE_SDK_UUID){
                pg_uuid_t* value=DatumGetUUIDP(values[i]);
                if(!value)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native Arrow UUID datum is missing")));
                errno_t copy=memcpy_s((char*)c->fixed+(Size)row*UUID_LEN,UUID_LEN,value->data,UUID_LEN);
                securec_check(copy,"","");
            }
            else if(type==OG_LAKE_SDK_DECIMAL128){
                LakeNumericCoefficient(values[i],b->typmods[i],
                    (unsigned char*)c->fixed+(Size)row*16);
            }
            else if(type==OG_LAKE_SDK_FLOAT32_LIST){
                Vector* value=(Vector*)b->packed[i];
                if(value->dim<1||value->dim>VECTOR_MAX_DIM||VARSIZE(value)!=VECTOR_SIZE(value->dim))
                    ereport(ERROR,(errcode(ERRCODE_DATA_EXCEPTION),errmsg("native Arrow vector header disagrees with its value size")));
                for(int n=0;n<value->dim;++n)if(!std::isfinite(value->x[n]))
                    ereport(ERROR,(errcode(ERRCODE_DATA_EXCEPTION),errmsg("native Arrow vector must have finite elements")));
                appendBinaryStringInfo(&c->text,(char*)value->x,sizeof(float4)*value->dim);
            }else appendBinaryStringInfo(&c->text,VARDATA_ANY(b->packed[i]),VARSIZE_ANY_EXHDR(b->packed[i]));
        }
        if((type==OG_LAKE_SDK_UTF8||type==OG_LAKE_SDK_BINARY||type==OG_LAKE_SDK_JSON))((int32*)c->fixed)[row+1]=c->text.len;
        else if(type==OG_LAKE_SDK_FLOAT32_LIST)((int32*)c->fixed)[row+1]=c->text.len/sizeof(float4);
    }
    ++b->rows;b->text_bytes+=extra;MemoryContextSwitchTo(caller);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(caller);PG_RE_THROW();
    }
    PG_END_TRY();
    LakeArrowFreePacked(b,values);return true;
}

int LakeArrowBuilderCount(const LakeArrowBuilder* b){return b->rows;}

void LakeArrowBuilderWrite(LakeArrowBuilder* b,const OgLakeParquetSdkApi* api,OgLakeSdkWriter* writer)
{
    if(api==NULL||writer==NULL||api->writer_append==NULL||!(api->capabilities&OG_LAKE_SDK_WRITER))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake provider has no writer")));
    for(int i=0;i<b->count;++i){
        if(b->fields[i].type==OG_LAKE_SDK_UUID)LakeNativeTypeRequireSdk(UUIDOID,api);
        if(b->fields[i].type==OG_LAKE_SDK_JSON)LakeNativeTypeRequireSdk(JSONOID,api);
        if(b->fields[i].type==OG_LAKE_SDK_DECIMAL128)LakeNativeTypeRequireSdk(NUMERICOID,api);
    }
    if(b->rows==0)return;
    b->array.length=b->rows;
    for(int i=0;i<b->count;++i) {
        b->columns[i].array.length=b->rows;
        if((b->fields[i].type==OG_LAKE_SDK_UTF8||b->fields[i].type==OG_LAKE_SDK_BINARY||b->fields[i].type==OG_LAKE_SDK_JSON))b->columns[i].buffers[2]=b->columns[i].text.data;
        else if(b->fields[i].type==OG_LAKE_SDK_FLOAT32_LIST){
            b->columns[i].element_array.length=b->columns[i].text.len/sizeof(float4);
            b->columns[i].element_buffers[1]=b->columns[i].text.data;
        }
    }
    OgLakeSdkError error;
    HOLD_INTERRUPTS();int status=api->writer_append(writer,&b->array,&b->schema,&error);RESUME_INTERRUPTS();
    LakeFileCheckStatus(status,&error);CHECK_FOR_INTERRUPTS();
    if(b->array.release!=LakeArrowArrayRelease||b->schema.release!=LakeArrowSchemaRelease)
        ereport(ERROR,(errcode(ERRCODE_FDW_ERROR),errmsg("native lake provider consumed borrowed Arrow input")));
    for(int i=0;i<b->count;++i)
        if(b->columns[i].array.release!=LakeArrowArrayRelease||b->columns[i].schema.release!=LakeArrowSchemaRelease)
            ereport(ERROR,(errcode(ERRCODE_FDW_ERROR),errmsg("native lake provider consumed borrowed Arrow column")));
        else if(b->fields[i].type==OG_LAKE_SDK_FLOAT32_LIST&&
            (b->columns[i].element_array.release!=LakeArrowArrayRelease||b->columns[i].element_schema.release!=LakeArrowSchemaRelease))
            ereport(ERROR,(errcode(ERRCODE_FDW_ERROR),errmsg("native lake provider consumed borrowed Arrow list elements")));
}

void LakeArrowBuilderReset(LakeArrowBuilder* b)
{
    MemoryContextReset(b->data_context);LakeArrowBuilderInitialize(b);
}
void LakeArrowBuilderDestroy(LakeArrowBuilder* b){MemoryContextDelete(b->context);}
