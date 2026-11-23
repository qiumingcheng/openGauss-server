/* Host tuple coordinates never become compact Arrow coordinates after DROP. */
#include "postgres.h"
#include "lake_column_layout.h"
#include "lake_inline_private.h"
#include "lake_inline_sources.h"
#include "lake_column_default.h"
#include "utils/memutils.h"

int LakeHostColumnCount(const LakeInlineBinding* binding)
{return binding->host_natts?binding->host_natts:binding->column_count;}
int LakeHostToLogical(const LakeInlineBinding* binding,AttrNumber attnum)
{
    if(attnum<=0||attnum>LakeHostColumnCount(binding))return -1;
    for(int c=0;c<binding->column_count;++c)
        if(binding->columns[c].output_attnum==attnum)return c;
    return -1; /* a certified hole; caller must explicitly handle absence */
}
int LakeLogicalToHost(const LakeInlineBinding* binding,int c)
{
    if(c<0||c>=binding->column_count||binding->columns[c].output_attnum<=0||
       binding->columns[c].output_attnum>LakeHostColumnCount(binding))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake host field map is invalid")));
    return binding->columns[c].output_attnum-1;
}
const LakeRetiredColumn* LakeFindRetiredColumn(const LakeInlineBinding* binding,int64 id)
{
    for(int c=0;c<binding->retired_count;++c)
        if(binding->retired_columns[c].column.column_id==id)return &binding->retired_columns[c];
    return NULL;
}
static void LakeCheckTupleShape(const LakeInlineBinding* binding,TupleDesc desc)
{
    if(desc==NULL||desc->natts<1||desc->natts>MaxHeapAttributeNumber||desc->natts!=LakeHostColumnCount(binding))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake host tuple shape changed")));
    int live=0;bool seen[MaxHeapAttributeNumber]={false};
    for(int h=0;h<desc->natts;++h)if(!desc->attrs[h].attisdropped)++live;
    if(live!=binding->column_count)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake host tuple live field count changed")));
    for(int c=0;c<binding->column_count;++c){
        int h=LakeLogicalToHost(binding,c);
        if(seen[h])ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("duplicate native lake host field mapping")));
        seen[h]=true;Form_pg_attribute a=&desc->attrs[h];
        if(a->attisdropped||a->atttypid!=binding->columns[c].type||a->atttypmod!=binding->columns[c].typmod)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake host tuple field identity changed")));
    }
}
void LakeGatherHostTuple(const LakeInlineBinding* binding,TupleDesc desc,HeapTuple tuple,Datum* values,bool* nulls)
{
    LakeCheckTupleShape(binding,desc);
    Datum* host=(Datum*)palloc(sizeof(Datum)*desc->natts);bool* absent=(bool*)palloc(sizeof(bool)*desc->natts);
    heap_deform_tuple(tuple,desc,host,absent);
    for(int c=0;c<binding->column_count;++c){int h=LakeLogicalToHost(binding,c);values[c]=host[h];nulls[c]=absent[h];}
    /* Returned byref values remain owned by tuple, not these two temporary arrays. */
    pfree(host);pfree(absent);
}
void LakeScatterHostValues(const LakeInlineBinding* binding,TupleDesc desc,const Datum* values,const bool* nulls,
    Datum* hostValues,bool* hostNulls)
{
    if (binding->bound_snapshot_read) {
        if (desc == NULL || desc->natts < 1 || desc->natts > MaxHeapAttributeNumber || desc->natts != LakeHostColumnCount(binding))
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("native lake bound-snapshot host tuple shape changed")));
        for (int h = 0; h < desc->natts; ++h) {
            hostValues[h] = (Datum)0;
            hostNulls[h] = true;
        }
        for (int c = 0; c < binding->column_count; ++c) {
            AttrNumber outAtt = binding->columns[c].output_attnum;
            if (outAtt == InvalidAttrNumber)
                continue;
            if (outAtt < 1 || outAtt > desc->natts)
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("native lake bound-snapshot host field map is invalid")));
            Form_pg_attribute a = &desc->attrs[outAtt - 1];
            if (a->attisdropped || a->atttypid != LakeColumnTargetType(&binding->columns[c]) ||
                a->atttypmod != LakeColumnTargetTypmod(&binding->columns[c]))
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("native lake bound-snapshot host field identity changed")));
            hostValues[outAtt - 1] = values[c];
            hostNulls[outAtt - 1] = nulls[c];
        }
        return;
    }
    LakeCheckTupleShape(binding,desc);
    for(int h=0;h<desc->natts;++h){hostValues[h]=(Datum)0;hostNulls[h]=true;}
    for(int c=0;c<binding->column_count;++c){int h=LakeLogicalToHost(binding,c);hostValues[h]=values[c];hostNulls[h]=nulls[c];}
}
HeapTuple LakeFormHostTuple(const LakeInlineBinding* binding,TupleDesc desc,const Datum* values,const bool* nulls)
{
    Datum* h=(Datum*)palloc0(sizeof(Datum)*desc->natts);bool* n=(bool*)palloc(sizeof(bool)*desc->natts);
    LakeScatterHostValues(binding,desc,values,nulls,h,n);HeapTuple tuple=heap_form_tuple(desc,h,n);pfree(h);pfree(n);return tuple;
}
void LakeCopyRetiredColumns(LakeInlineBinding* target,const LakeInlineBinding* source)
{
    if(source->retired_count<0||source->retired_count>MaxHeapAttributeNumber)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake retired field bounds changed")));
    target->retired_columns=source->retired_count?(LakeRetiredColumn*)palloc(sizeof(LakeRetiredColumn)*source->retired_count):NULL;
    for(int i=0;i<source->retired_count;++i){
        target->retired_columns[i]=source->retired_columns[i];
        LakeInlineColumn* to=&target->retired_columns[i].column;const LakeInlineColumn* from=&source->retired_columns[i].column;
        to->name=pstrdup(from->name);to->inline_name=from->inline_name?pstrdup(from->inline_name):NULL;
        /* Clone inherits the fully charged source budget, as current columns do. */
        LakeCopyColumnDefault(to,from,NULL);
    }
}
void LakeValidateRetiredBirth(const LakeInlineBinding* binding,int64 id,int64 lower,int64 upper)
{
    const LakeRetiredColumn* r=LakeFindRetiredColumn(binding,id);
    if(!binding->drop_columns||!r||lower<r->column.introduced_snapshot||upper<lower||upper>=r->retired_snapshot)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("retired native lake physical field has no complete lifetime proof")));
}

bool LakeColumnLayoutsEqual(const LakeInlineBinding* a,const LakeInlineBinding* b)
{
    if(LakeHostColumnCount(a)!=LakeHostColumnCount(b)||a->column_count!=b->column_count||
       a->has_dropped_columns!=b->has_dropped_columns||a->retired_count!=b->retired_count)return false;
    for(int c=0;c<a->column_count;++c)
        if(a->columns[c].output_attnum!=b->columns[c].output_attnum)return false;
    for(int c=0;c<a->retired_count;++c){
        const LakeRetiredColumn* x=&a->retired_columns[c];
        const LakeRetiredColumn* y=LakeFindRetiredColumn(b,x->column.column_id);
        if(!y||x->retired_snapshot!=y->retired_snapshot||x->collation!=y->collation||
           x->column.output_attnum!=y->column.output_attnum||x->column.type!=y->column.type||
           x->column.typmod!=y->column.typmod||x->column.element_column_id!=y->column.element_column_id||
           strcmp(x->column.name,y->column.name)||!LakeColumnDefaultsEqual(&x->column,&y->column))return false;
    }
    return true;
}
