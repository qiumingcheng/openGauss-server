/* Equal-size virtual-block sampling over physical file and inline positions.
 * Selection precedes business-value/visibility tests. Rejected positions are
 * never refilled from easier files, which would change inclusion probabilities. */
#include "postgres.h"
#include "lake_column_layout.h"
#include "knl/knl_variable.h"
#include "lake_sample.h"
#include "lake_native_partition.h"
#include "lake_inline_reader.h"
#include "lake_file_reader.h"
#include "lake_txn_private.h"
#include "access/heapam.h"
#include "catalog/pg_type.h"
#include "commands/vacuum.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "port.h"
#include "storage/buf/block.h"
#include "utils/memutils.h"
#include "utils/tuplestore.h"
#include "utils/timestamp.h"
#include "utils/builtins.h"
#include "utils/uuid.h"
#include "utils/fmgroids.h"

extern void LakeTestAnalyzeAfterRow(Oid relationOid,uint64 visibleRows);
static const uint64 LAKE_ANALYZE_INLINE_BYTES=UINT64CONST(512)*1024*1024;
static const uint64 LAKE_ANALYZE_INLINE_ROWS=1000000;

struct LakeSampleFile { int table_index,index; int64 file_id; uint64 first; uint64 rows; };
struct LakeFileSampleState {
    Relation relation;
    LakeTxnTable* table; /* Active inline source only, not a merged row-ID namespace. */
    LakeTxnTable** tables;
    int table_count;
    CommandId command_limit;
    MemoryContext output_context;
    MemoryContext scratch;
    LakeInlineReader* inline_reader;
    Tuplestorestate* inline_rows;
    TupleTableSlot* slot;
    TupleDesc store_desc;
    HeapTuple* tuples;
    int target,count;
    uint64 inline_count,inline_bytes,sample_bytes;
    LakeAnalyzeIoBudget budget;
};

static void LakeSampleCheckTime(LakeFileSampleState* state)
{
    CHECK_FOR_INTERRUPTS();vacuum_delay_point();
    if(TimestampDifferenceExceeds(state->budget.started,GetCurrentTimestamp(),state->budget.max_milliseconds))
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake ANALYZE sampling time budget exhausted")));
}

/* Same Algorithm S and BlockSamplerData as native ANALYZE. Follow the
 * existing NEON private-rand-state helper pattern; ordinary BlockSampler_Next
 * uses glibc random() on this build, whose state has no seed-readback API.
 * Six unmodified random UUID bytes initialize a private host rand48 stream. */
static BlockNumber LakeSampleNext(BlockSampler bs,unsigned short state[3],LakeFileSampleState* sample)
{
    BlockNumber remaining=bs->N-bs->t;int wanted=bs->n-bs->m;
    Assert(BlockSampler_HasMore(bs));
    if((BlockNumber)wanted>=remaining){++bs->m;return bs->t++;}
    double draw=((double)pg_lrand48(state)+1)/2147483649.0;
    double cutoff=1.0-(double)wanted/(double)remaining;
    uint32 skipped=0;
    while(draw<cutoff){
        /* Algorithm S may skip nearly MaxBlockNumber virtual blocks. Poll
         * without consuming RNG state or changing any numeric recurrence. */
        if((skipped++&4095U)==0)LakeSampleCheckTime(sample);
        ++bs->t;--remaining;cutoff*=1.0-(double)wanted/(double)remaining;
    }
    ++bs->m;return bs->t++;
}

static int LakeSampleCompareFile(const void* a,const void* b)
{
    const LakeSampleFile* x=(const LakeSampleFile*)a;const LakeSampleFile* y=(const LakeSampleFile*)b;
    if(x->table_index!=y->table_index)return x->table_index<y->table_index?-1:1;
    return x->file_id<y->file_id?-1:x->file_id>y->file_id?1:0;
}

/* Every visible selected row is kept, in stable physical-domain order. */
static void LakeSampleKeep(Datum* values,bool* nulls,const LakeInlineRowIdentity* identity,void* argument)
{
    LakeFileSampleState* state=(LakeFileSampleState*)argument;
    if(state->count>=state->target)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake ANALYZE sample exceeds selected positions")));
    MemoryContext caller=MemoryContextSwitchTo(state->output_context);
    HeapTuple tuple=LakeFormHostTuple(state->table->binding,RelationGetDescr(state->relation),values,nulls);
    if(tuple->t_len>UINT64CONST(512)*1024*1024-state->sample_bytes){
        heap_freetuple(tuple);MemoryContextSwitchTo(caller);
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake ANALYZE returned sample memory budget exhausted")));
    }
    state->sample_bytes+=tuple->t_len;state->tuples[state->count++]=tuple;
    MemoryContextSwitchTo(caller);
    LakeTestAnalyzeAfterRow(RelationGetRelid(state->relation),(uint64)state->count);
    LakeSampleCheckTime(state);
}

/* Host-MVCC-visible physical inline versions remain candidates even when lake
 * ended/future or hidden by the private journal. Only selected positions are
 * tested for logical visibility. This one ordered pass may spill to temp. */
static void LakeSampleStoreInline(LakeFileSampleState* state)
{
    TupleDesc business=RelationGetDescr(state->relation);int n=state->table->binding->column_count;
    int extra=state->table_count>1?5:4;
    if(business->natts!=LakeHostColumnCount(state->table->binding))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("ANALYZE host field layout changed")));
    TupleDesc desc=CreateTemplateTupleDesc(n+extra,false);desc->td_tam_ops=TableAmHeap;
    for(int i=0;i<n;++i){
        Form_pg_attribute attr=&business->attrs[LakeLogicalToHost(state->table->binding,i)];
        TupleDescInitEntry(desc,i+1,NameStr(attr->attname),attr->atttypid,attr->atttypmod,attr->attndims);
        desc->attrs[i].attcollation=attr->attcollation;
    }
    TupleDescInitEntry(desc,n+1,"sample_row_id",INT8OID,-1,0);
    TupleDescInitEntry(desc,n+2,"sample_birth",INT8OID,-1,0);
    TupleDescInitEntry(desc,n+3,"sample_end",INT8OID,-1,0);
    TupleDescInitEntry(desc,n+4,"sample_source",INT2OID,-1,0);
    if(state->table_count>1)TupleDescInitEntry(desc,n+5,"sample_table",INT4OID,-1,0);
    state->store_desc=desc;
    /* This bound limits resident temp buffering, not total inline scan I/O. */
    state->inline_rows=tuplestore_begin_heap(true,false,Max(64,Min(u_sess->attr.attr_memory.work_mem,16*1024)));
    Datum* values=(Datum*)palloc0(sizeof(Datum)*(n+extra));
    bool* nulls=(bool*)palloc0(sizeof(bool)*(n+extra));
    MemoryContext rowContext=AllocSetContextCreate(state->scratch,"Lake ANALYZE inline row",ALLOCSET_DEFAULT_SIZES);
    for(int tableIndex=0;tableIndex<state->table_count;++tableIndex){
    state->table=state->tables[tableIndex];
    if(n!=state->table->binding->column_count)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("ANALYZE table domains require identical business projection")));
    state->inline_reader=LakeInlineReaderOpen(state->table,state->command_limit,true,true);
    for(;;){
        LakeSampleCheckTime(state);MemoryContextReset(rowContext);MemoryContextSwitchTo(rowContext);
        LakeInlineRowIdentity identity;
        bool found=LakeInlineReaderNext(state->inline_reader,values,nulls,&identity);
        MemoryContextSwitchTo(state->scratch);if(!found)break;
        values[n]=Int64GetDatum(identity.row_id);nulls[n]=false;
        values[n+1]=Int64GetDatum(identity.begin_snapshot);nulls[n+1]=false;
        values[n+2]=Int64GetDatum(identity.end_snapshot);nulls[n+2]=!identity.ended;
        values[n+3]=Int16GetDatum(identity.source);nulls[n+3]=false;
        if(state->table_count>1){values[n+4]=Int32GetDatum(tableIndex);nulls[n+4]=false;}
        HeapTuple tuple=heap_form_tuple(desc,values,nulls);
        if(state->inline_count>=LAKE_ANALYZE_INLINE_ROWS||tuple->t_len>LAKE_ANALYZE_INLINE_BYTES-state->inline_bytes)
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake ANALYZE inline candidate spool budget exhausted")));
        state->inline_bytes+=tuple->t_len;++state->inline_count;
        tuplestore_puttuple(state->inline_rows,tuple);heap_freetuple(tuple);
    }
    LakeInlineReaderClose(state->inline_reader);state->inline_reader=NULL;
    }
    state->table=state->tables[0];
    MemoryContextDelete(rowContext);
    state->slot=MakeSingleTupleTableSlot(desc);tuplestore_rescan(state->inline_rows);
}

static void LakeSampleCleanup(LakeFileSampleState* state,bool failed)
{
    if(state->inline_reader){LakeInlineReaderClose(state->inline_reader);state->inline_reader=NULL;}
    if(state->slot){ExecDropSingleTupleTableSlot(state->slot);state->slot=NULL;}
    if(state->inline_rows){tuplestore_end(state->inline_rows);state->inline_rows=NULL;}
    if(failed)for(int i=0;i<state->count;++i)if(state->tuples[i])heap_freetuple(state->tuples[i]);
    if(state->scratch){MemoryContextDelete(state->scratch);state->scratch=NULL;}
}

BlockNumber LakeAnalyzePhysicalPages(const LakeInlineBinding* binding)
{
    uint64 pages=LakeInlinePhysicalPages(binding);
    for(int i=0;i<binding->file_count;++i){
        if(binding->files[i].file_bytes<0)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("negative lake file byte size")));
        uint64 bytes=(uint64)binding->files[i].file_bytes;
        pages+=bytes/BLCKSZ+(bytes%BLCKSZ!=0);
        if(pages>(uint64)MaxBlockNumber)
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("lake ANALYZE equivalent file pages exceed the host interface")));
    }
    /* Equivalent byte pages for pg_class only, never Parquet addresses. */
    return (BlockNumber)pages;
}

/* One global physical domain, common inclusion probability and whole-call
 * budgets. Callers build both views from one pinned host snapshot/CID; the
 * binding checks below additionally require one catalog and lake snapshot.
 * Views and Relation are borrowed and remain caller-owned on every path. */
int LakeAcquireTableSampleRows(Relation relation,LakeTxnTable* const* tables,int tableCount,CommandId commandLimit,
    HeapTuple* rows,int target,double* totalrows,double* totaldeadrows,int elevel)
{
    if(!relation||!tables||tableCount<1||tableCount>LakeNativePartitionLeafLimit()||!rows||!totalrows||!totaldeadrows||commandLimit==InvalidCommandId)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid bounded lake ANALYZE table domains")));
    for(int t=0;t<tableCount;++t){
        if(!tables[t]||!tables[t]->binding||!tables[t]->descriptor)
            ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("lake ANALYZE table view is missing")));
        const LakeInlineBinding* b=tables[t]->binding;
        if(t&&(b->server_oid!=tables[0]->binding->server_oid||b->snapshot_id!=tables[0]->binding->snapshot_id))
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("lake ANALYZE domains do not share one catalog snapshot")));
        if(LakeHostColumnCount(b)!=RelationGetDescr(relation)->natts||tables[t]->descriptor->natts!=RelationGetDescr(relation)->natts)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("lake ANALYZE domains require one complete business descriptor")));
        for(int a=0;a<tables[t]->descriptor->natts;++a){
            const auto* x=&tables[t]->descriptor->attrs[a];const auto* y=&RelationGetDescr(relation)->attrs[a];
            if(x->attisdropped!=y->attisdropped||x->atttypid!=y->atttypid||x->atttypmod!=y->atttypmod||x->attcollation!=y->attcollation)
                ereport(ERROR,(errcode(ERRCODE_DATATYPE_MISMATCH),errmsg("lake ANALYZE source descriptor differs from its relation")));
        }
        for(int previous=0;previous<t;++previous)
            if(b->table_uuid&&tables[previous]->binding->table_uuid&&!strcmp(b->table_uuid,tables[previous]->binding->table_uuid))
                ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("lake ANALYZE repeats the same table domain")));
    }
    if(target<=0||(Size)target>MaxAllocSize/sizeof(uint64))
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("invalid lake file ANALYZE sample size")));
    MemoryContext caller=CurrentMemoryContext;
    LakeFileSampleState* state=(LakeFileSampleState*)palloc0(sizeof(*state));
    state->relation=relation;state->table=tables[0];state->table_count=tableCount;
    state->tables=(LakeTxnTable**)palloc(sizeof(LakeTxnTable*)*tableCount);
    for(int t=0;t<tableCount;++t)state->tables[t]=tables[t];
    state->command_limit=commandLimit;state->target=target;
    state->output_context=caller;state->tuples=(HeapTuple*)palloc0(sizeof(HeapTuple)*target);
    state->scratch=AllocSetContextCreate(caller,"Lake ANALYZE sampling",ALLOCSET_DEFAULT_SIZES);
    state->budget.max_read_bytes=UINT64CONST(1024)*1024*1024;
    state->budget.max_plan_bytes=UINT64CONST(2048)*1024*1024;
    state->budget.max_decoded_bytes=UINT64CONST(2048)*1024*1024;
    state->budget.started=GetCurrentTimestamp();state->budget.max_milliseconds=120000;
    *totalrows=*totaldeadrows=0;
    PG_TRY();{
        MemoryContextSwitchTo(state->scratch);
        int fileCount=0;
        for(int t=0;t<tableCount;++t){
            int count=tables[t]->binding->file_count;
            if(count<0||(Size)count>MaxAllocSize/sizeof(LakeSampleFile)-(Size)fileCount)
                ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("lake ANALYZE file inventory is too large")));
            fileCount+=count;
        }
        LakeSampleFile* files=(LakeSampleFile*)palloc0(sizeof(*files)*Max(1,fileCount));
        int at=0;for(int t=0;t<tableCount;++t)for(int i=0;i<tables[t]->binding->file_count;++i){
            files[at].table_index=t;files[at].index=i;files[at].file_id=tables[t]->binding->files[i].file_id;++at;
        }
        qsort(files,fileCount,sizeof(*files),LakeSampleCompareFile);
        uint64 filePopulation=0;
        for(int i=0;i<fileCount;++i){
            LakeInlineBinding* binding=tables[files[i].table_index]->binding;
            if((i&&files[i].table_index==files[i-1].table_index&&files[i].file_id==files[i-1].file_id)||binding->files[files[i].index].record_count<0)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake ANALYZE physical file inventory")));
            files[i].first=filePopulation;files[i].rows=(uint64)binding->files[files[i].index].record_count;
            if(files[i].rows>UINT64CONST(9007199254740991)-filePopulation)
                ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("lake ANALYZE physical population exceeds exact host representation")));
            filePopulation+=files[i].rows;
        }
        LakeSampleStoreInline(state);
        if(state->inline_count>UINT64CONST(9007199254740991)-filePopulation)
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("lake ANALYZE combined physical population is too large")));
        uint64 population=filePopulation+state->inline_count;
        uint64* positions=(uint64*)palloc(sizeof(uint64)*target);int selected=0;
        unsigned short seed[3],randomState[3];
        pg_uuid_t* random=DatumGetUUIDP(OidFunctionCall0(F_GEN_RANDOM_UUID));
        for(int i=0;i<3;++i){seed[i]=random->data[2*i]|((unsigned short)random->data[2*i+1]<<8);randomState[i]=seed[i];}
        pfree(random);
        uint64 length=1,blocks=population;int sampledBlocks=0,perBlock=1;double probability=1;
        LakeSampleCheckTime(state);
        if(population<=(uint64)target){
            for(uint64 p=0;p<population;++p){
                if((p&4095U)==0)LakeSampleCheckTime(state);
                positions[selected++]=p;
            }
            sampledBlocks=(int)population;
        }
        else {
            length=4096;blocks=population/length+(population%length!=0);
            if(blocks>(uint64)MaxBlockNumber)
                ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("lake ANALYZE virtual block domain exceeds BlockSampler")));
            sampledBlocks=(int)Min((uint64)Min(8,Max(1,target/32)),blocks);
            perBlock=Min((int)length,target/sampledBlocks);
            if(sampledBlocks*perBlock<Min(target,Max(100,target/4)))
                ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("requested lake statistics precision exceeds the bounded sampling plan")));
            probability=((double)sampledBlocks/(double)blocks)*((double)perBlock/(double)length);
            BlockSamplerData outer;BlockSampler_Init(&outer,(BlockNumber)blocks,sampledBlocks);
            while(BlockSampler_HasMore(&outer)){
                LakeSampleCheckTime(state);
                uint64 first=(uint64)LakeSampleNext(&outer,randomState,state)*length;
                BlockSamplerData inner;BlockSampler_Init(&inner,(BlockNumber)length,perBlock);
                int draws=0;
                while(BlockSampler_HasMore(&inner)){
                    if((draws++&1023U)==0)LakeSampleCheckTime(state);
                    uint64 position=first+LakeSampleNext(&inner,randomState,state);
                    /* Padded positions have no tuple. Do not refill. */
                    if(position<population)positions[selected++]=position;
                }
            }
        }
        int next=0;
        for(int i=0;i<fileCount;++i){
            uint64 end=files[i].first+files[i].rows;int start=next;
            while(next<selected&&positions[next]<end)++next;
            if(next==start)continue;
            int count=next-start;int64* local=(int64*)palloc(sizeof(int64)*count);
            for(int j=0;j<count;++j)local[j]=(int64)(positions[start+j]-files[i].first);
            state->table=tables[files[i].table_index];
            LakeFileReadSamplePositions(tables[files[i].table_index],commandLimit,files[i].index,local,count,LakeSampleKeep,state,&state->budget);
            pfree(local);
        }
        uint64 inlineCursor=0;int n=tables[0]->binding->column_count;
        for(;next<selected;++next){
            uint64 wanted=positions[next]-filePopulation;
            while(inlineCursor<wanted){
                LakeSampleCheckTime(state);
                if(!tuplestore_advance(state->inline_rows,true))
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("lake ANALYZE inline spool ended before its candidate")));
                ++inlineCursor;
            }
            if(!tuplestore_gettupleslot(state->inline_rows,true,false,state->slot))
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("lake ANALYZE inline candidate is missing")));
            ++inlineCursor;tableam_tslot_getallattrs(state->slot);
            Datum* values=state->slot->tts_values;bool* nulls=state->slot->tts_isnull;
            LakeInlineRowIdentity identity={};identity.source=(uint8)DatumGetInt16(values[n+3]);
            identity.row_id=DatumGetInt64(values[n]);identity.begin_snapshot=DatumGetInt64(values[n+1]);
            identity.ended=!nulls[n+2];identity.end_snapshot=identity.ended?DatumGetInt64(values[n+2]):0;
            int owner=tableCount>1?DatumGetInt32(values[n+4]):0;
            if((tableCount>1&&nulls[n+4])||owner<0||owner>=tableCount)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("lake ANALYZE inline candidate has an invalid table domain")));
            LakeTxnTable* table=tables[owner];const LakeInlineBinding* binding=table->binding;
            bool visible=identity.source==LAKE_LOCATOR_TXN||
                (identity.begin_snapshot<=binding->snapshot_id&&(!identity.ended||identity.end_snapshot>binding->snapshot_id)&&
                 !LakeTxnHidesInlineRow(table,identity.row_id,identity.begin_snapshot,commandLimit));
            if(visible){state->table=table;LakeSampleKeep(values,nulls,&identity,state);}
            ExecClearTuple(state->slot);
        }
        /* Thin survivors cannot reliably build MCV/NDV/histograms. ERROR before
         * returning tuples leaves the old pg_statistic entries intact. */
        if(population>(uint64)target&&state->count<Min(target,Max(100,target/4)))
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("lake ANALYZE sample has insufficient visible survivors"),
                errdetail("selected=%d retained=%d physical=%llu; no value-dependent refill was performed",selected,state->count,(unsigned long long)population)));
        *totalrows=state->count/probability;
        ereport(elevel,(errmsg("\"%s\": native lake physical-domain sample retained %d of %d positions; estimated %.0f visible rows",
            RelationGetRelationName(relation),state->count,selected,*totalrows)));
        ereport(DEBUG1,(errmsg("native lake ANALYZE file sampling evidence"),
            errdetail("seed=%u,%u,%u physical=%llu L=%llu M=%llu B=%d r=%d p=%.17g kept=%d sample_bytes=%llu inline_candidates=%llu inline_spool=%llu files=%u groups=%u planned_bytes=%llu planned_decoded=%llu reader_bytes=%llu reader_calls=%llu reader_decoded=%llu unobserved_footer_opens=%llu footer_reserve=%llu",
                seed[0],seed[1],seed[2],(unsigned long long)population,(unsigned long long)length,(unsigned long long)blocks,sampledBlocks,perBlock,probability,state->count,(unsigned long long)state->sample_bytes,
                (unsigned long long)state->inline_count,(unsigned long long)state->inline_bytes,state->budget.files,state->budget.groups,
                (unsigned long long)state->budget.planned_bytes,(unsigned long long)state->budget.planned_decoded_bytes,
                (unsigned long long)state->budget.observed_reader_bytes,(unsigned long long)state->budget.observed_reader_calls,
                (unsigned long long)state->budget.observed_decoded_rows,(unsigned long long)state->budget.unobserved_footer_opens,
                (unsigned long long)state->budget.reserved_footer_bytes)));
        MemoryContextSwitchTo(caller);LakeSampleCleanup(state,false);
        for(int i=0;i<state->count;++i)rows[i]=state->tuples[i];
    }PG_CATCH();{
        MemoryContextSwitchTo(caller);LakeSampleCleanup(state,true);PG_RE_THROW();
    }PG_END_TRY();
    int result=state->count;pfree(state->tables);pfree(state->tuples);pfree(state);return result;
}

/* Retained single-table entry uses the same algorithm, RNG calls, physical
 * domain order, output order and budgets. Its logical table index is always0. */
int LakeAcquireFileSampleRows(Relation relation,LakeTxnTable* table,CommandId commandLimit,
    HeapTuple* rows,int target,double* totalrows,double* totaldeadrows,int elevel)
{
    LakeTxnTable* tables[1]={table};
    return LakeAcquireTableSampleRows(relation,tables,1,commandLimit,rows,target,totalrows,totaldeadrows,elevel);
}
