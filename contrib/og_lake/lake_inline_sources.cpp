#include "postgres.h"
#include "lake_column_layout.h"
#include "lake_inline_sources.h"
#include "lake_column_default.h"
#include "lake_types.h"
#include "utils/memutils.h"
#include "catalog/pg_type.h"

/* Installed SQL34/35 source evolution admission is owned by
 * lake_catalog.cpp. Retained SQL versions keep their single-source contract. */

bool LakeBindingHasBinary(const LakeInlineBinding* binding)
{
    if(!binding)return false;
    for(int i=0;i<binding->column_count;++i)if(binding->columns[i].type==BYTEAOID)return true;
    for(int i=0;i<binding->retired_count;++i)if(binding->retired_columns[i].column.type==BYTEAOID)return true;
    return false;
}
void LakeRejectBinaryAcceleration(const LakeInlineBinding* binding,const char* operation)
{
    if(LakeBindingHasBinary(binding))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake BYTEA does not support %s; replan serially",operation)));
}
bool LakeBindingHasUuid(const LakeInlineBinding* binding)
{
    if(!binding)return false;
    for(int i=0;i<binding->column_count;++i)if(binding->columns[i].type==UUIDOID)return true;
    for(int i=0;i<binding->retired_count;++i)if(binding->retired_columns[i].column.type==UUIDOID)return true;
    return false;
}
void LakeRejectUuidAcceleration(const LakeInlineBinding* binding,const char* operation)
{
    if(LakeBindingHasUuid(binding))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake UUID cannot use %s in SQL0.52",operation)));
}

bool LakeBindingHasJson(const LakeInlineBinding* binding)
{
    if(!binding)return false;
    for(int i=0;i<binding->column_count;++i)if(binding->columns[i].type==JSONOID)return true;
    for(int i=0;i<binding->retired_count;++i)if(binding->retired_columns[i].column.type==JSONOID)return true;
    return false;
}
void LakeRejectJsonAcceleration(const LakeInlineBinding* binding,const char* operation)
{
    if(LakeBindingHasJson(binding))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake JSON cannot use %s in SQL0.53",operation)));
}

bool LakeBindingHasNumeric(const LakeInlineBinding* binding)
{
    if(!binding)return false;
    for(int i=0;i<binding->column_count;++i)if(binding->columns[i].type==NUMERICOID)return true;
    for(int i=0;i<binding->retired_count;++i)if(binding->retired_columns[i].column.type==NUMERICOID)return true;
    return false;
}
void LakeRejectNumericAcceleration(const LakeInlineBinding* binding,const char* operation)
{
    if(LakeBindingHasNumeric(binding))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake numeric cannot use %s in SQL0.54",operation)));
}

bool LakeBindingHasTemporal(const LakeInlineBinding* binding)
{
    if(!binding)return false;
    for(int i=0;i<binding->column_count;++i)
        if(LakeNativeTypeIsTemporal(binding->columns[i].type))return true;
    for(int i=0;i<binding->retired_count;++i)
        if(LakeNativeTypeIsTemporal(binding->retired_columns[i].column.type))return true;
    return false;
}

static void LakeInlineSourceCorrupt(const char* message)
{
    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("%s",message)));
}

/* Native descriptor allocations are bounded independently of provider batches.
 * Call before allocating; never clip the visible field/source domain. */
void LakeReserveColumnMapBytes(LakeInlineBinding* binding,Size bytes)
{
    if(!binding->column_evolution)return;
    const Size limit=64UL*1024UL*1024UL;
    if(binding->column_map_bytes>limit||bytes>limit-binding->column_map_bytes)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake column mapping budget exceeded")));
    binding->column_map_bytes+=bytes;
}

bool LakeColumnCanBeMissing(const LakeInlineBinding* binding,int column,int64 birthUpper)
{
    if(!binding->column_evolution||column<0||column>=binding->column_count||birthUpper<0)return false;
    const LakeInlineColumn* field=&binding->columns[column];
    return (field->initial_null_proven||(binding->literal_defaults&&field->initial_value_proven))&&field->element_column_id<0&&
        field->introduced_snapshot>birthUpper&&field->introduced_snapshot<=binding->snapshot_id;
}

LakeInlineColumn* LakeCopyInlineColumns(const LakeInlineColumn* columns,int count,LakeInlineBinding* allocationBudget)
{
    if(count<1||count>MaxHeapAttributeNumber||columns==NULL)
        LakeInlineSourceCorrupt("invalid physical column copy bounds");
    LakeInlineColumn* result=(LakeInlineColumn*)palloc(sizeof(LakeInlineColumn)*count);
    for(int i=0;i<count;++i){
        result[i]=columns[i];LakeCopyColumnDefault(&result[i],&columns[i],allocationBudget);
        result[i].name=pstrdup(columns[i].name);
        result[i].inline_name=columns[i].inline_name?pstrdup(columns[i].inline_name):NULL;
    }
    return result;
}

static void LakeInlineSourceBounds(const LakeInlineBinding* binding)
{
    if(!binding||!binding->inline_sources_loaded)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("native lake multi-source descriptor is not loaded")));
    if(binding->inline_source_count<0||binding->inline_source_count>LAKE_INLINE_SOURCE_LIMIT||
       (binding->inline_source_count!=0&&binding->inline_sources==NULL)||
       binding->column_count<1||binding->column_count>MaxHeapAttributeNumber||
       binding->inline_write_index< -1||binding->inline_write_index>=binding->inline_source_count||
       binding->inline_write_schema_version<0||binding->inline_write_schema_version>binding->schema_version||
       binding->inline_write_origin_snapshot<0||binding->inline_write_origin_snapshot>binding->snapshot_id)
        LakeInlineSourceCorrupt("invalid native lake multi-source descriptor bounds");
}

void LakeValidateInlineSources(const LakeInlineBinding* binding)
{
    LakeInlineSourceBounds(binding);
    int64 previous=-1;int current=-1;
    for(int i=0;i<binding->inline_source_count;++i){
        const LakeInlineSource* item=&binding->inline_sources[i];
        if(!OidIsValid(item->relation_oid)||!item->table_name||!*item->table_name||
           strlen(item->table_name)>=NAMEDATALEN||item->schema_version<=previous||
           item->schema_version>binding->inline_write_schema_version||item->origin_snapshot<0||
           item->origin_snapshot>binding->inline_write_origin_snapshot||
           item->column_count<1||item->column_count>MaxHeapAttributeNumber||item->column_names==NULL||
           item->columns==NULL||item->logical_to_source==NULL)
            LakeInlineSourceCorrupt("invalid native lake inline source identity or historical schema");
        previous=item->schema_version;
        if(item->schema_version==binding->inline_write_schema_version){
            if(item->origin_snapshot!=binding->inline_write_origin_snapshot)
                LakeInlineSourceCorrupt("current inline carrier has a different schema origin");
            if(item->column_count!=binding->column_count)
                LakeInlineSourceCorrupt("current inline carrier omits a current business field");
            current=i;
        }
        for(int c=0;c<item->column_count;++c){
            if(!item->column_names[c]||!*item->column_names[c]||strlen(item->column_names[c])>=NAMEDATALEN)
                LakeInlineSourceCorrupt("invalid native lake historical inline column name");
            for(int old=0;old<c;++old)
                if(strcmp(item->column_names[old],item->column_names[c])==0)
                    LakeInlineSourceCorrupt("duplicate native lake historical inline column name");
        }
        int previousPhysical=-1;
        bool used[MaxHeapAttributeNumber]={false};
        for(int c=0;c<binding->column_count;++c){
            int physical=item->logical_to_source[c];
            if(physical<0){
                if(physical!=-1||!LakeColumnCanBeMissing(binding,c,item->origin_snapshot))
                    LakeInlineSourceCorrupt("historical inline omission has no initial NULL proof");
                continue;
            }
            if(physical>=item->column_count||physical<=previousPhysical||used[physical])
                LakeInlineSourceCorrupt("historical inline field mapping is not a complete stable-ID order");
            used[physical]=true;previousPhysical=physical;
            const LakeInlineColumn* old=&item->columns[physical];const LakeInlineColumn* now=&binding->columns[c];
            bool typeOk = item->schema_version==binding->inline_write_schema_version ?
                (old->type==now->type&&(old->type!=NUMERICOID||old->typmod==now->typmod)) :
                (binding->alter_type_evolution ?
                    LakeIsTypeCompatibleOrPromoted(old->type,old->typmod,now->type,now->typmod) :
                    (old->type==now->type&&(old->type!=NUMERICOID||old->typmod==now->typmod)));
            if(old->column_id!=now->column_id||!typeOk||old->element_column_id!=now->element_column_id||
               strcmp(old->name,item->column_names[physical]))
                LakeInlineSourceCorrupt("historical inline field mapping changed identity or type");
        }
        for(int p=0;p<item->column_count;++p)if(!used[p]){
            const LakeRetiredColumn* retired=LakeFindRetiredColumn(binding,item->columns[p].column_id);
            LakeValidateRetiredBirth(binding,item->columns[p].column_id,item->origin_snapshot,item->origin_snapshot);
            bool retiredTypeOk = retired && (binding->alter_type_evolution ?
                LakeIsTypeCompatibleOrPromoted(item->columns[p].type,item->columns[p].typmod,
                    retired->column.type,retired->column.typmod) :
                (item->columns[p].type==retired->column.type&&
                    (item->columns[p].type!=NUMERICOID||item->columns[p].typmod==retired->column.typmod)));
            if(!retired||!retiredTypeOk||item->columns[p].element_column_id!=-1||
               item->schema_version==binding->inline_write_schema_version)
                LakeInlineSourceCorrupt("unmapped historical inline field is not a retired scalar");
        }
        for(int old=0;old<i;++old)
            if(binding->inline_sources[old].relation_oid==item->relation_oid||
               strcmp(binding->inline_sources[old].table_name,item->table_name)==0)
                LakeInlineSourceCorrupt("duplicate native lake inline source registration");
    }
    if(current!=binding->inline_write_index)
        LakeInlineSourceCorrupt("native lake current write carrier selection differs from table schema");
}

bool LakeInlineHasHistoricalSources(const LakeInlineBinding* binding)
{
    if(!binding)return false;
    if(binding->has_added_columns||binding->has_dropped_columns||binding->has_altered_types)return true; /* Also guards one current carrier plus old files. */
    if(!binding->inline_sources_loaded)return false;
    LakeInlineSourceBounds(binding);
    return binding->inline_source_count>1||
        (binding->inline_source_count==1&&binding->inline_write_index!=0);
}

int LakeInlineReadSourceCount(const LakeInlineBinding* binding)
{
    if(binding->inline_sources_loaded){LakeInlineSourceBounds(binding);return binding->inline_source_count;}
    return OidIsValid(binding->inline_oid)?1:0;
}
Oid LakeInlineReadSourceOid(const LakeInlineBinding* binding,int index)
{
    int count=LakeInlineReadSourceCount(binding);
    if(index<0||index>=count)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("native lake inline source index is outside its descriptor")));
    return binding->inline_sources_loaded?binding->inline_sources[index].relation_oid:binding->inline_oid;
}

const LakeInlineSource* LakeFindInlineSource(const LakeInlineBinding* binding,Oid sourceOid)
{
    LakeInlineSourceBounds(binding);
    if(!OidIsValid(sourceOid))return NULL;
    for(int i=0;i<binding->inline_source_count;++i)
        if(binding->inline_sources[i].relation_oid==sourceOid)return &binding->inline_sources[i];
    return NULL;
}

const LakeInlineSource* LakeInlineWriteSource(const LakeInlineBinding* binding)
{
    LakeInlineSourceBounds(binding);
    return binding->inline_write_index<0?NULL:&binding->inline_sources[binding->inline_write_index];
}

void LakeSetInlineWriteSource(LakeInlineBinding* binding,int index)
{
    binding->inline_write_index=index;
    LakeValidateInlineSources(binding);
    const LakeInlineSource* item=LakeInlineWriteSource(binding);
    binding->inline_oid=item?item->relation_oid:InvalidOid;
    binding->inline_schema_version=item?item->schema_version:binding->inline_write_schema_version;
    binding->inline_origin_snapshot=item?item->origin_snapshot:binding->inline_write_origin_snapshot;
    if(item)binding->inline_partition=item->partition;
    else {MemSet(&binding->inline_partition,0,sizeof(binding->inline_partition));binding->inline_partition.end_snapshot=-1;}
    for(int c=0;c<binding->column_count;++c)
        binding->columns[c].inline_name=item?item->column_names[item->logical_to_source[c]]:NULL;
}

void LakeCopyInlineSources(LakeInlineBinding* target,const LakeInlineBinding* source,MemoryContext owner)
{
    if(!source->inline_sources_loaded){
        MemoryContext previous=MemoryContextSwitchTo(owner);
        PG_TRY();{LakeCopyRetiredColumns(target,source);MemoryContextSwitchTo(previous);}
        PG_CATCH();{MemoryContextSwitchTo(previous);PG_RE_THROW();}PG_END_TRY();return;
    }
    LakeValidateInlineSources(source);
    if(source->column_evolution&&source->column_map_bytes>64UL*1024UL*1024UL)
        LakeInlineSourceCorrupt("source descriptor clone exceeds its inherited byte budget");
    if(target==source||target->columns==source->columns)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("native lake source copy requires separately owned business columns")));
    MemoryContext caller=MemoryContextSwitchTo(owner);
    PG_TRY();
    {
        LakeCopyRetiredColumns(target,source);
        target->inline_sources_loaded=true;target->inline_source_count=source->inline_source_count;
        target->inline_write_index=source->inline_write_index;
        target->inline_write_schema_version=source->inline_write_schema_version;
        target->inline_write_origin_snapshot=source->inline_write_origin_snapshot;
        target->inline_sources=source->inline_source_count?(LakeInlineSource*)palloc0(sizeof(LakeInlineSource)*source->inline_source_count):NULL;
        for(int i=0;i<source->inline_source_count;++i){
            const LakeInlineSource* from=&source->inline_sources[i];LakeInlineSource* to=&target->inline_sources[i];
            *to=*from;to->table_name=pstrdup(from->table_name);
            to->columns=LakeCopyInlineColumns(from->columns,from->column_count);
            to->logical_to_source=(int*)palloc(sizeof(int)*source->column_count);
            for(int c=0;c<source->column_count;++c)to->logical_to_source[c]=from->logical_to_source[c];
            to->column_names=(char**)palloc0(sizeof(char*)*from->column_count);
            for(int c=0;c<from->column_count;++c)to->column_names[c]=pstrdup(from->column_names[c]);
            to->partition=LakePartitionCopySpec(&from->partition,owner);
        }
        LakeSetInlineWriteSource(target,target->inline_write_index);
        MemoryContextSwitchTo(caller);
    }
    PG_CATCH();{MemoryContextSwitchTo(caller);PG_RE_THROW();}PG_END_TRY();
}
