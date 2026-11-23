/* Standard nested inline VARCHAR / Parquet LIST<FLOAT> -> native vector(n).
 * Schema admission and SQL NULL handling belong to the bound readers. */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "lake_vector_codec.h"
#include "access/datavec/vector.h"
#include "access/tuptoaster.h"
#include "catalog/pg_type.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "vendor/arrow20/abi.h"
#include <cmath>

void LakeVectorDimension(int32 dimension)
{
    if(dimension<1||dimension>VECTOR_MAX_DIM)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake vectors require an explicit supported dimension")));
}

Datum LakeVectorReadText(Datum value,int32 dimension)
{
    LakeVectorDimension(dimension);
    /* Check TOAST's raw length before fetching the value. This is a resource
     * limit on canonical nested text, never a truncating conversion. */
    const Size maximum=(Size)dimension*64+16;
    if(toast_raw_datum_size(value)>maximum+VARHDRSZ)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake vector text exceeds its input budget")));
    text* packed=DatumGetTextPP(value);Size bytes=VARSIZE_ANY_EXHDR(packed);
    if(memchr(VARDATA_ANY(packed),'\0',bytes)!=NULL)
        ereport(ERROR,(errcode(ERRCODE_DATA_EXCEPTION),errmsg("native lake vector text contains an embedded zero byte")));
    char* input=text_to_cstring(packed);
    /* Public native input validates syntax, exact dimension and finite values.
     * It accepts DuckLake's bracketed list syntax without PostgreSQL array text. */
    Datum result=DirectFunctionCall3(vector_in,CStringGetDatum(input),ObjectIdGetDatum(InvalidOid),Int32GetDatum(dimension));
    pfree(input);if((Pointer)packed!=DatumGetPointer(value))pfree(packed);
    return result;
}

Datum LakeVectorWriteText(Datum value,int32 dimension)
{
    LakeVectorDimension(dimension);
    Size raw=toast_raw_datum_size(value);
    if(raw!=VECTOR_SIZE(dimension))
        ereport(ERROR,(errcode(ERRCODE_DATA_EXCEPTION),errmsg("native lake vector size differs from its declared dimension")));
    Vector* vector=DatumGetVector(value);
    if(vector->dim!=dimension)
        ereport(ERROR,(errcode(ERRCODE_DATA_EXCEPTION),errmsg("native lake vector dimension differs from its binding")));
    for(int i=0;i<dimension;++i)
        if(!std::isfinite(vector->x[i]))
            ereport(ERROR,(errcode(ERRCODE_DATA_EXCEPTION),errmsg("native lake vector elements must be finite")));
    char* output=DatumGetCString(DirectFunctionCall1(vector_out,PointerGetDatum(vector)));
    Datum result=CStringGetTextDatum(output);pfree(output);
    if((Pointer)vector!=DatumGetPointer(value))pfree(vector);
    return result;
}

static bool LakeVectorArrowNull(const ArrowArray* array,int64 row)
{
    if(array->null_count < -1||array->null_count>array->length)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native lake vector validity population")));
    if(array->buffers[0]==NULL){
        if(array->null_count>0)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake vector validity bitmap is missing")));
        return false;
    }
    int64 position=array->offset+row;
    return ((((const unsigned char*)array->buffers[0])[position/8]>>(position%8))&1)==0;
}

Datum LakeVectorReadArrow(const ArrowArray* array,const ArrowSchema* schema,int64 row,int32 dimension)
{
    LakeVectorDimension(dimension);
    if(array==NULL||schema==NULL||schema->format==NULL||strcmp(schema->format,"+l")!=0||
        array->length<0||array->offset<0||array->offset>PG_INT64_MAX-array->length||row<0||row>=array->length||
        array->n_buffers!=2||array->buffers==NULL||array->buffers[1]==NULL||
        array->n_children!=1||array->children==NULL||schema->n_children!=1||schema->children==NULL||
        array->dictionary!=NULL||schema->dictionary!=NULL)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native lake vector LIST batch")));
    if((uint64)(array->offset+array->length)>((uint64)MaxAllocSize/sizeof(int32))-1)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake vector offsets exceed their buffer budget")));
    if(LakeVectorArrowNull(array,row))
        ereport(ERROR,(errcode(ERRCODE_DATA_EXCEPTION),errmsg("NULL native lake vector must be handled by its tuple validity")));
    const ArrowArray* child=array->children[0];const ArrowSchema* element=schema->children[0];
    if(child==NULL||element==NULL||element->format==NULL||strcmp(element->format,"f")!=0||
        child->length<0||child->offset<0||child->offset>PG_INT64_MAX-child->length||
        child->n_buffers!=2||child->buffers==NULL||child->buffers[1]==NULL||
        child->n_children!=0||element->n_children!=0||child->dictionary!=NULL||element->dictionary!=NULL)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native lake vector FLOAT element batch")));
    const int32* offsets=(const int32*)array->buffers[1];int64 index=array->offset+row;
    int64 first=offsets[index],last=offsets[index+1];
    if(first<0||last<first||last>child->length||last-first!=dimension)
        ereport(ERROR,(errcode(ERRCODE_DATA_EXCEPTION),errmsg("native lake vector LIST length differs from its binding")));
    if((uint64)(child->offset+last)>(uint64)MaxAllocSize/sizeof(float4))
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake vector elements exceed their buffer budget")));
    Datum* values=(Datum*)palloc(sizeof(Datum)*dimension);
    for(int i=0;i<dimension;++i){
        if(LakeVectorArrowNull(child,first+i))
            ereport(ERROR,(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),errmsg("native lake vector elements must not be NULL")));
        float4 item;errno_t rc=memcpy_s(&item,sizeof(item),
            (const char*)child->buffers[1]+(child->offset+first+i)*sizeof(item),sizeof(item));
        securec_check(rc,"","");values[i]=Float4GetDatum(item);
    }
    int16 length;bool byValue;char align;get_typlenbyvalalign(FLOAT4OID,&length,&byValue,&align);
    ArrayType* input=construct_array(values,dimension,FLOAT4OID,length,byValue,align);
    /* Reuse the public native conversion's finite/dimension checks and vector
     * initialization. No private varlena layout is persisted or invented here. */
    Datum result=DirectFunctionCall2(array_to_vector,PointerGetDatum(input),Int32GetDatum(dimension));
    pfree(input);pfree(values);return result;
}
