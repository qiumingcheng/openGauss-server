/* Transaction-private detached tuple payload, not a DuckLake persisted format. */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "lake_txn_private.h"
#include "access/htup.h"
#include "storage/buf/buffile.h"
#include "utils/errcodes.h"
#include "miscadmin.h"
#include <limits.h>

static const Size LAKE_PAYLOAD_RESIDENT_LIMIT=8*1024*1024;
/* Frozen host buffile.cpp MAX_PHYSICAL_FILESIZE is exactly 0x40000000.
 * Tell may represent a segment endpoint or a dirty buffer crossing it.
 * Keep that pair intact; Seek flushes before testing a newly created segment. */
static const uint64 LAKE_BUFFILE_SEGMENT_BYTES=UINT64CONST(0x40000000);
static const uint64 LAKE_PAYLOAD_DISK_LIMIT=UINT64CONST(64)*1024*1024*1024;
static const Size LAKE_PAYLOAD_BASE_CHARGE=64*1024;
static const Size LAKE_PAYLOAD_SEGMENT_CHARGE=64*1024;
static const uint64 LAKE_PAYLOAD_MAGIC=UINT64CONST(0x4c414b4554555038);

struct LakePayloadRecord {
    uint64 magic;
    uint64 token;
    uint32 length;
    uint32 header_size;
};
struct LakeTxnPayloadStore {
    BufFile* file;
    MemoryContext context;
    bool poisoned;
    bool closed;
    int append_file;
    off_t append_offset;
    uint64 append_bytes;
    Size reserved_charge;
};

bool LakeTxnRowHasPayload(const LakeTxnRow* row)
{
    return row!=NULL&&row->payload_kind!=LAKE_PAYLOAD_TOMBSTONE;
}

void LakeTxnPayloadCheck(const LakeTxnState* state)
{
    if(state!=NULL&&state->payload_store!=NULL&&state->payload_store->poisoned)
        ereport(ERROR,(errcode(ERRCODE_IO_ERROR),
            errmsg("native lake tuple spill failed; the transaction must be rolled back")));
}

static void LakePayloadExact(bool condition,const char* operation)
{
    if(!condition)ereport(ERROR,(errcode(ERRCODE_IO_ERROR),
        errmsg("native lake tuple spill %s did not complete",operation)));
}

static void LakePayloadPosition(int file,off_t offset,uint64 expected)
{
    LakePayloadExact(file>=0&&file<=64&&offset>=0&&
        (uint64)offset<=LAKE_BUFFILE_SEGMENT_BYTES+BLCKSZ,"position validation");
    uint64 actual=(uint64)file*LAKE_BUFFILE_SEGMENT_BYTES+(uint64)offset;
    LakePayloadExact(actual==expected&&actual<=LAKE_PAYLOAD_DISK_LIMIT,"host segment geometry validation");
}

Size LakeTxnStoreRowPayload(LakeTxnState* state,LakeTxnRow* row,HeapTuple tuple,
    bool spillEnabled,Size journalCharge)
{
    LakeTxnPayloadCheck(state);
    if(tuple==NULL){row->payload_kind=LAKE_PAYLOAD_TOMBSTONE;return 0;}
    Size bytes=sizeof(HeapTupleData)+(Size)tuple->t_len;
    if(bytes>LAKE_INLINE_TXN_MEMORY_LIMIT)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake inline row exceeds the write-set limit")));
    row->payload_length=tuple->t_len;
    bool resident=!spillEnabled || (bytes<=LAKE_PAYLOAD_RESIDENT_LIMIT &&
        state->resident_payload_bytes<=LAKE_PAYLOAD_RESIDENT_LIMIT-bytes &&
        journalCharge<=LAKE_INLINE_TXN_MEMORY_LIMIT-bytes &&
        state->staged_bytes<=LAKE_INLINE_TXN_MEMORY_LIMIT-journalCharge-bytes);
    if(resident){
        row->tuple=heap_copytuple(tuple);row->payload_kind=LAKE_PAYLOAD_RESIDENT;
        return bytes;
    }
    if(sizeof(off_t)<sizeof(int64))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake tuple spill requires 64-bit file offsets")));
    LakeTxnPayloadStore* store=state->payload_store;
    if(store==NULL){
        store=(LakeTxnPayloadStore*)MemoryContextAllocZero(state->context,sizeof(LakeTxnPayloadStore));
        state->payload_store=store;store->context=state->context;
    }
    if(store->closed)ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake tuple spill is already closed")));
    uint64 recordBytes=(uint64)sizeof(LakePayloadRecord)+sizeof(HeapTupleData)+tuple->t_len;
    if(recordBytes>LAKE_PAYLOAD_DISK_LIMIT||store->append_bytes>LAKE_PAYLOAD_DISK_LIMIT-recordBytes)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake tuple spill exceeds the 64 GiB transaction limit")));
    uint64 end=store->append_bytes+recordBytes;
    uint64 segments=end/LAKE_BUFFILE_SEGMENT_BYTES+1; /* conservative endpoint reservation */
    if(segments>65||segments>(uint64)INT_MAX||
       segments>(LAKE_INLINE_TXN_MEMORY_LIMIT-LAKE_PAYLOAD_BASE_CHARGE)/LAKE_PAYLOAD_SEGMENT_CHARGE)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake tuple spill segment budget is exhausted")));
    Size charge=LAKE_PAYLOAD_BASE_CHARGE+(Size)segments*LAKE_PAYLOAD_SEGMENT_CHARGE;
    Assert(charge>=store->reserved_charge);
    Size growth=charge-store->reserved_charge;
    if(journalCharge>LAKE_INLINE_TXN_MEMORY_LIMIT||growth>LAKE_INLINE_TXN_MEMORY_LIMIT-journalCharge||
       state->staged_bytes>LAKE_INLINE_TXN_MEMORY_LIMIT-journalCharge-growth)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake tuple spill metadata exceeds the 64 MiB resident limit")));
    /* File/segment allocation lives to top transaction end, so this charge is
     * never refunded by a user subabort or an unlinked completed record. */
    state->staged_bytes+=growth;store->reserved_charge=charge;
    MemoryContext caller=CurrentMemoryContext;
    ResourceOwner owner=t_thrd.utils_cxt.CurrentResourceOwner;
    PG_TRY();
    {
        MemoryContextSwitchTo(state->context);
        t_thrd.utils_cxt.CurrentResourceOwner=t_thrd.utils_cxt.TopTransactionResourceOwner;
        LakePayloadExact(t_thrd.utils_cxt.CurrentResourceOwner!=NULL,"top resource owner selection");
        if(store->file==NULL)store->file=BufFileCreateTemp(false);
        LakePayloadPosition(store->append_file,store->append_offset,store->append_bytes);
        LakePayloadExact(BufFileSeek(store->file,store->append_file,store->append_offset,SEEK_SET)==0,"append seek");
        row->payload_file=store->append_file;row->payload_offset=store->append_offset;
        LakePayloadRecord record={LAKE_PAYLOAD_MAGIC,row->local_token,tuple->t_len,sizeof(HeapTupleData)};
        HeapTupleData header=*tuple;header.t_data=NULL;
        LakePayloadExact(BufFileWrite(store->file,&record,sizeof(record))==sizeof(record),"record write");
        LakePayloadExact(BufFileWrite(store->file,&header,sizeof(header))==sizeof(header),"header write");
        LakePayloadExact(BufFileWrite(store->file,tuple->t_data,tuple->t_len)==tuple->t_len,"payload write");
        int file=0;off_t offset=0;BufFileTell(store->file,&file,&offset);
        LakePayloadPosition(file,offset,end);
        store->append_file=file;store->append_offset=offset;store->append_bytes=end;
        row->payload_store=store;row->payload_kind=LAKE_PAYLOAD_SPILLED;
        MemoryContextSwitchTo(caller);t_thrd.utils_cxt.CurrentResourceOwner=owner;
    }
    PG_CATCH();
    {
        store->poisoned=true;
        MemoryContextSwitchTo(caller);t_thrd.utils_cxt.CurrentResourceOwner=owner;
        PG_RE_THROW();
    }
    PG_END_TRY();
    return 0;
}

HeapTuple LakeTxnMaterializeRow(const LakeTxnRow* row)
{
    LakeTxnPayloadCheck(LakeHostExistingTransactionState());
    if(!LakeTxnRowHasPayload(row))
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake private row is a tombstone")));
    if(row->payload_kind==LAKE_PAYLOAD_RESIDENT){
        if(row->tuple==NULL||row->tuple->t_len!=row->payload_length)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake resident tuple identity differs")));
        return heap_copytuple(row->tuple);
    }
    LakeTxnPayloadStore* store=row->payload_store;
    if(row->payload_kind!=LAKE_PAYLOAD_SPILLED||store==NULL||store->file==NULL||store->closed||store->poisoned)
        ereport(ERROR,(errcode(ERRCODE_IO_ERROR),errmsg("native lake tuple spill is unavailable; the transaction must be rolled back")));
    if(row->payload_length<SizeofHeapTupleHeader||row->payload_length>LAKE_INLINE_TXN_MEMORY_LIMIT-sizeof(HeapTupleData))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake spilled tuple length is invalid")));
    HeapTuple result=(HeapTuple)palloc0(HEAPTUPLESIZE+(Size)row->payload_length);
    MemoryContext caller=CurrentMemoryContext;ResourceOwner owner=t_thrd.utils_cxt.CurrentResourceOwner;
    PG_TRY();
    {
        MemoryContextSwitchTo(store->context);
        t_thrd.utils_cxt.CurrentResourceOwner=t_thrd.utils_cxt.TopTransactionResourceOwner;
        LakePayloadExact(t_thrd.utils_cxt.CurrentResourceOwner!=NULL,"top resource owner selection");
        LakePayloadExact(BufFileSeek(store->file,row->payload_file,row->payload_offset,SEEK_SET)==0,"read seek");
        LakePayloadRecord record={};HeapTupleData header;
        LakePayloadExact(BufFileRead(store->file,&record,sizeof(record))==sizeof(record),"record read");
        LakePayloadExact(record.magic==LAKE_PAYLOAD_MAGIC&&record.token==row->local_token&&
            record.length==row->payload_length&&record.header_size==sizeof(HeapTupleData),"record identity validation");
        LakePayloadExact(BufFileRead(store->file,&header,sizeof(header))==sizeof(header),"header read");
        LakePayloadExact(header.t_data==NULL&&header.t_len==row->payload_length&&header.tupTableType==HEAP_TUPLE,"header identity validation");
        *result=header;result->t_data=(HeapTupleHeader)((char*)result+HEAPTUPLESIZE);
        LakePayloadExact(BufFileRead(store->file,result->t_data,row->payload_length)==row->payload_length,"payload read");
        LakePayloadExact(result->t_data->t_hoff>=SizeofHeapTupleHeader&&result->t_data->t_hoff<=result->t_len,"tuple bounds validation");
        MemoryContextSwitchTo(caller);t_thrd.utils_cxt.CurrentResourceOwner=owner;
    }
    PG_CATCH();
    {
        store->poisoned=true;
        MemoryContextSwitchTo(caller);t_thrd.utils_cxt.CurrentResourceOwner=owner;
        heap_freetuple(result);PG_RE_THROW();
    }
    PG_END_TRY();
    return result;
}

void LakeTxnPayloadVerify(LakeTxnState* state)
{
    LakeTxnPayloadCheck(state);
    if(state==NULL||state->payload_store==NULL)return;
    for(LakeTxnCatalog* catalog=state->catalogs;catalog;catalog=catalog->next)
        for(LakeTxnTable* table=catalog->tables;table;table=table->next)
            for(LakeTxnRow* row=table->rows;row;row=row->next)
                if(row->payload_kind==LAKE_PAYLOAD_SPILLED){
                    CHECK_FOR_INTERRUPTS();HeapTuple tuple=LakeTxnMaterializeRow(row);heap_freetuple(tuple);
                }
}

void LakeTxnPayloadClose(LakeTxnState* state)
{
    LakeTxnPayloadCheck(state);
    LakeTxnPayloadStore* store=state?state->payload_store:NULL;
    if(store==NULL||store->closed)return;
    MemoryContext caller=CurrentMemoryContext;ResourceOwner owner=t_thrd.utils_cxt.CurrentResourceOwner;
    PG_TRY();
    {
        MemoryContextSwitchTo(store->context);
        t_thrd.utils_cxt.CurrentResourceOwner=t_thrd.utils_cxt.TopTransactionResourceOwner;
        if(store->file){
            char dummy=0;
            /* Host Read first checks dirty-buffer Flush even for a zero-byte
             * request. Close ignores that return, so never rely on Close to
             * discover a failed final write. This is still PRE_COMMIT. */
            LakePayloadExact(BufFileRead(store->file,&dummy,0)==0,"final flush");
            BufFile* file=store->file;store->file=NULL;store->closed=true;
            BufFileClose(file);
        }else store->closed=true;
        MemoryContextSwitchTo(caller);t_thrd.utils_cxt.CurrentResourceOwner=owner;
    }
    PG_CATCH();
    {
        store->poisoned=true;
        MemoryContextSwitchTo(caller);t_thrd.utils_cxt.CurrentResourceOwner=owner;PG_RE_THROW();
    }
    PG_END_TRY();
}
