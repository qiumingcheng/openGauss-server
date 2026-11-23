/* TableAM physical visibility, lake version visibility and private after-images. */
#include "postgres.h"
#include "lake_uuid.h"
#include "lake_json.h"
#include "lake_binary.h"
#include "lake_column_layout.h"
#include "lake_column_default.h"
#include "lake_temporal_text.h"
#include "lake_types.h"
#include "lake_version.h"
#include "lake_temporal.h"
#include "lake_txn_private.h"
#include "lake_inline_reader.h"
#include "lake_inline_sources.h"
#include "lake_vector_codec.h"
#include "lake_file_reader.h"
#include "lake_ann_rescan.h"
#include "lake_index_private.h"
#include "lake_inline_ann.h"
#include "access/hbindex_am.h"
#include "catalog/index.h"
#include "access/heapam.h"
#include "access/genam.h"
#include "catalog/pg_type.h"
#include "commands/vacuum.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "access/datavec/diskann.h"

struct LakeInlineReader {
    LakeTxnTable* table;
    LakeTxnRow* cursor;
    CommandId command_limit;
    uint64 last_token;
    uint64 journal_generation;
    bool base_done;
    int source_index; /* Current immutable read_sources entry, independent of writer. */
    bool for_analyze;
    bool physical_inline; /* ANALYZE candidate domain; never an ordinary query. */
    LakeFileReader* files;
    Relation relation;
    TableScanDesc scan;
    Relation index_relation;
    IndexScanDesc index_scan;
    TupleTableSlot* index_slot;
    ScanKeyData index_key;
    bool ann;
    HTAB* ann_seen;
    MemoryContext owner_context;
    MemoryContext scan_context;
    Datum* values;
    bool* nulls;
    AttrNumber row_id_attnum;
    AttrNumber begin_attnum;
    AttrNumber end_attnum;
    AttrNumber* column_attnums;
    Oid* source_types;
    int32* source_typmods;
    bool* projected;
};

static AttrNumber LakeReaderAttribute(Relation relation, const char* name, Oid type)
{
    AttrNumber attnum = get_attnum(RelationGetRelid(relation), name);
    if (attnum <= 0 || attnum > RelationGetDescr(relation)->natts)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("native lake inline column \"%s\" is missing", name)));
    Form_pg_attribute attr = &RelationGetDescr(relation)->attrs[attnum - 1];
    bool compatible = attr->atttypid == type || (type == TEXTOID && attr->atttypid == VARCHAROID);
    if (attr->attisdropped || !compatible)
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
            errmsg("native lake inline column \"%s\" has an incompatible type", name)));
    return attnum;
}

/* TableAM can retain page-at-a-time Ustore tuple copies between calls.
 * Its descriptor and allocations must outlive the caller's per-row context. */
static void LakeReaderBeginScan(LakeInlineReader* reader)
{
    if(reader->index_relation!=NULL &&
       RelationGetRelid(reader->relation)==reader->index_relation->rd_index->indrelid) {
        if(RelationIsUstoreFormat(reader->relation)){
            TupleDesc descriptor=CreateTupleDescCopy(RelationGetDescr(reader->relation));descriptor->td_tam_ops=TableAmUstore;
            reader->index_slot=MakeSingleTupleTableSlot(descriptor);
        }
        reader->index_scan=scan_handler_idx_beginscan(reader->relation,reader->index_relation,
            LakeTxnBusinessSnapshot(reader->table),reader->ann?0:1,reader->ann?1:0);
        scan_handler_idx_rescan(reader->index_scan,reader->ann?NULL:&reader->index_key,reader->ann?0:1,
            reader->ann?&reader->index_key:NULL,reader->ann?1:0);
        if(reader->ann){
            /* Reuse the executor's native batch protocol. It normalizes the
             * query once per expansion and releases it after searching. */
            ((DiskAnnScanOpaque)reader->index_scan->opaque)->orderedBatch=true;
            (void)diskann_advance_ordered_batch(reader->index_scan);
        }
        return;
    }
    if (reader->for_analyze && !RelationIsUstoreFormat(reader->relation)) {
        reader->scan = tableam_scan_begin_sampling(reader->relation,
            LakeTxnBusinessSnapshot(reader->table), 0, NULL, true, false);
    } else {
        reader->scan = tableam_scan_begin(reader->relation,
            LakeTxnBusinessSnapshot(reader->table), 0, NULL);
        if (reader->for_analyze && (reader->scan->rs_syncscan || reader->scan->rs_startblock != 0))
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("native lake ANALYZE requires an ordered physical scan")));
    }
}

/* Closing/reopening one carrier never refreshes the statement snapshot and
 * never drops its transaction-level relation lock. TableAM-owned page tuples
 * are released before the per-source context is destroyed. */
static void LakeReaderEndCarrier(LakeInlineReader* reader)
{
    MemoryContext caller=CurrentMemoryContext;
    PG_TRY();
    {
        if(reader->index_slot!=NULL){
            MemoryContextSwitchTo(reader->scan_context);
            ExecDropSingleTupleTableSlot(reader->index_slot);reader->index_slot=NULL;
            MemoryContextSwitchTo(caller);
        }
        if(reader->index_scan!=NULL){
            MemoryContextSwitchTo(reader->scan_context);
            scan_handler_idx_endscan(reader->index_scan);reader->index_scan=NULL;
            MemoryContextSwitchTo(caller);
        }
        if(reader->scan){
            MemoryContextSwitchTo(reader->scan_context);tableam_scan_end(reader->scan);reader->scan=NULL;
            MemoryContextSwitchTo(caller);
        }
        if(reader->scan_context){MemoryContextDelete(reader->scan_context);reader->scan_context=NULL;}
        if(reader->relation){heap_close(reader->relation,NoLock);reader->relation=NULL;}
        if(reader->column_attnums){pfree(reader->column_attnums);reader->column_attnums=NULL;}
        if(reader->source_types){pfree(reader->source_types);reader->source_types=NULL;}
        if(reader->source_typmods){pfree(reader->source_typmods);reader->source_typmods=NULL;}
        if(reader->values){pfree(reader->values);reader->values=NULL;}
        if(reader->nulls){pfree(reader->nulls);reader->nulls=NULL;}
    }
    PG_CATCH();{MemoryContextSwitchTo(caller);PG_RE_THROW();}PG_END_TRY();
}

static bool LakeReaderOpenCarrier(LakeInlineReader* reader,int index)
{
    LakeInlineBinding* binding=reader->table->binding;
    const LakeInlineSource* source=NULL;
    if(binding->inline_sources_loaded){
        int count=LakeInlineReadSourceCount(binding);
        if(index<0||index>=count)return false;
        source=&binding->inline_sources[index];
    }else if(index!=0||!OidIsValid(binding->inline_oid))return false;
    Oid oid=source?source->relation_oid:binding->inline_oid;
    MemoryContext caller=MemoryContextSwitchTo(reader->owner_context);
    PG_TRY();
    {
        reader->relation=heap_open(oid,AccessShareLock);
        TupleDesc desc=RelationGetDescr(reader->relation);
        if(desc->natts!=(source?source->column_count:binding->column_count)+3)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("native lake inline schema requires an unsupported projection adapter")));
        reader->row_id_attnum=LakeReaderAttribute(reader->relation,"_ducklake_row_id",INT8OID);
        reader->begin_attnum=LakeReaderAttribute(reader->relation,"_ducklake_begin_snapshot",INT8OID);
        reader->end_attnum=LakeReaderAttribute(reader->relation,"_ducklake_end_snapshot",INT8OID);
        if(source)for(int p=0;p<source->column_count;++p){
            const LakeInlineColumn* field=&source->columns[p];
            AttrNumber actual=LakeReaderAttribute(reader->relation,source->column_names[p],LakeInlinePhysicalType(field->type));
            if(field->type==NUMERICOID&&desc->attrs[actual-1].atttypmod!=field->typmod)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native numeric carrier parameters changed")));
            if(actual!=p+4)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native carrier complete physical field order changed")));
        }
        reader->column_attnums=(AttrNumber*)palloc(sizeof(AttrNumber)*binding->column_count);
        reader->source_types=(Oid*)palloc0(sizeof(Oid)*binding->column_count);
        reader->source_typmods=(int32*)palloc(sizeof(int32)*binding->column_count);
        for(int i=0;i<binding->column_count;++i){
            int physical=source?source->logical_to_source[i]:i;
            if(physical<0){
                if(!source||physical!=-1||!LakeColumnCanBeMissing(binding,i,source->origin_snapshot))
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("inline field omission is not certified")));
                reader->column_attnums[i]=InvalidAttrNumber;
                reader->source_types[i]=InvalidOid;
                reader->source_typmods[i]=-1;
                continue;
            }
            Oid srcType=source?source->columns[physical].type:binding->columns[i].type;
            int32 srcTypmod=source?source->columns[physical].typmod:binding->columns[i].typmod;
            reader->column_attnums[i]=LakeReaderAttribute(reader->relation,
                source?source->column_names[physical]:LakeInlinePhysicalName(&binding->columns[i]),
                LakeInlinePhysicalType(srcType));
            if(srcType==NUMERICOID&&
               desc->attrs[reader->column_attnums[i]-1].atttypmod!=srcTypmod)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native numeric inline parameters changed")));
            reader->source_types[i]=srcType;
            reader->source_typmods[i]=srcTypmod;
        }
        reader->values=(Datum*)palloc0(sizeof(Datum)*desc->natts);
        reader->nulls=(bool*)palloc0(sizeof(bool)*desc->natts);
        reader->scan_context=AllocSetContextCreate(reader->owner_context,"Native lake TableAM scan",
            ALLOCSET_DEFAULT_MINSIZE,ALLOCSET_DEFAULT_INITSIZE,ALLOCSET_DEFAULT_MAXSIZE);
        MemoryContextSwitchTo(reader->scan_context);LakeReaderBeginScan(reader);
        reader->source_index=index;MemoryContextSwitchTo(caller);
    }
    PG_CATCH();{MemoryContextSwitchTo(caller);PG_RE_THROW();}PG_END_TRY();
    return true;
}

void LakeInlineReaderClose(LakeInlineReader* reader)
{
    if (reader == NULL) return;
    MemoryContext caller = CurrentMemoryContext;
    PG_TRY();
    {
        LakeFileReaderClose(reader->files);
        if(reader->index_slot!=NULL){
            MemoryContextSwitchTo(reader->scan_context);
            ExecDropSingleTupleTableSlot(reader->index_slot);reader->index_slot=NULL;
            MemoryContextSwitchTo(caller);
        }
        if(reader->index_scan!=NULL) {
            MemoryContextSwitchTo(reader->scan_context);
            scan_handler_idx_endscan(reader->index_scan);reader->index_scan=NULL;
            MemoryContextSwitchTo(caller);
        }
        if (reader->scan != NULL) {
            MemoryContextSwitchTo(reader->scan_context);
            tableam_scan_end(reader->scan);
            reader->scan = NULL;
            MemoryContextSwitchTo(caller);
        }
        /* End scan first: release buffer/reference ownership before cached
         * Ustore tuples and the TableAM allocation context are reclaimed. */
        if(reader->ann&&!reader->index_key.sk_flags&&DatumGetPointer(reader->index_key.sk_argument)!=NULL){
            pfree(DatumGetPointer(reader->index_key.sk_argument));reader->index_key.sk_argument=(Datum)0;
        }
        if(reader->ann_seen){hash_destroy(reader->ann_seen);reader->ann_seen=NULL;}
        if (reader->scan_context != NULL) {
            MemoryContextDelete(reader->scan_context);
            reader->scan_context = NULL;
        }
        if(reader->index_relation!=NULL) {
            index_close(reader->index_relation,NoLock);reader->index_relation=NULL;
        }
        if (reader->relation != NULL) {
            heap_close(reader->relation, NoLock);
            reader->relation = NULL;
        }
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(caller);
        PG_RE_THROW();
    }
    PG_END_TRY();
}

LakeInlineReader* LakeInlineReaderOpen(LakeTxnTable* table,
    CommandId commandLimit, bool forAnalyze, bool physicalInline, char* const* expectedVersions,
    bool deferredFileAdmission)
{
    LakeTxnRequireActiveTable(table);
    LakeInlineReader* reader = (LakeInlineReader*)palloc0(sizeof(LakeInlineReader));
    reader->table = table;
    reader->owner_context=CurrentMemoryContext;
    reader->cursor = LakeTxnJournalTable(table)->rows;
    reader->command_limit = commandLimit;
    reader->for_analyze = forAnalyze;
    reader->physical_inline = physicalInline;
    if(physicalInline&&!forAnalyze)
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("physical inline enumeration is reserved for ANALYZE")));
    LakeInlineBinding* binding = table->binding;
    MemoryContext caller = CurrentMemoryContext;
    PG_TRY();
    {
        if(forAnalyze&&!physicalInline&&binding->file_count!=0)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake file ANALYZE is not enabled")));
        if(deferredFileAdmission&&(forAnalyze||physicalInline))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("ANALYZE cannot defer lake file admission")));
        reader->files=physicalInline?NULL:LakeFileReaderOpen(binding,table,commandLimit,NULL,expectedVersions,deferredFileAdmission);
        reader->source_index=-1;
        reader->base_done=binding->identity.kind!=LAKE_TABLE_PERMANENT||!LakeReaderOpenCarrier(reader,0);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(caller);
        LakeInlineReaderClose(reader);
        PG_RE_THROW();
    }
    PG_END_TRY();
    return reader;
}

bool LakeInlineReaderNext(LakeInlineReader* reader, Datum* values,
    bool* nulls, LakeInlineRowIdentity* identity)
{
    LakeTxnRequireActiveTable(reader->table);
    LakeFileReaderRequireAdmitted(reader->files);
    LakeInlineBinding* binding = reader->table->binding;
    for (;;) {
        CHECK_FOR_INTERRUPTS();
        if (reader->for_analyze) vacuum_delay_point();
        if (reader->base_done) {
            if(LakeFileReaderNext(reader->files,values,nulls,identity))return true;
            LakeTxnRow* row = LakeTxnNextRow(reader->table, &reader->cursor,
                &reader->last_token, &reader->journal_generation, reader->command_limit);
            if (row == NULL) return false;
            HeapTuple payload=LakeTxnMaterializeRow(row);
            LakeGatherHostTuple(binding, reader->table->descriptor, payload, values, nulls);
            Size binaryRowBytes=0;
            for (int i = 0; i < binding->column_count; ++i) {
                if (reader->projected && !reader->projected[i]) {
                    values[i] = (Datum)0;
                    nulls[i] = true;
                    continue;
                }
                if(!nulls[i])LakeTemporalValidateDatum(binding->columns[i].type,values[i]);
                if(!nulls[i]&&binding->columns[i].type==BYTEAOID)
                    values[i]=LakeCopyBinaryDatum(values[i],&binaryRowBytes);
                else if(!nulls[i]&&binding->columns[i].type==UUIDOID)
                    values[i]=LakeCopyUuidDatum(values[i],&binaryRowBytes);
                else if(!nulls[i]&&binding->columns[i].type==JSONOID)
                    values[i]=LakeCopyJsonDatum(values[i],&binaryRowBytes);
                else if(!nulls[i]&&binding->columns[i].type==NUMERICOID)
                    values[i]=LakeCopyNumericDatum(values[i],binding->columns[i].typmod,&binaryRowBytes);
                else if (!nulls[i] && (binding->columns[i].type == TEXTOID||binding->columns[i].type==VECTOROID))
                    values[i] = datumCopy(values[i], false, -1);
            }
            heap_freetuple(payload); /* By-reference output now belongs to caller's row context. */
            identity->row_id = 0;
            identity->begin_snapshot = 0;
            identity->local_token = row->local_token;
            identity->source=LAKE_LOCATOR_TXN;identity->inline_oid=InvalidOid;identity->file_id=0;identity->file_position=0;
            identity->ended=false;identity->end_snapshot=0;
            return true;
        }
        MemoryContext caller = CurrentMemoryContext;
        Tuple volatile tuple = NULL;
        bool volatile found=false;
        PG_TRY();
        {
            MemoryContextSwitchTo(reader->scan_context);
            if(reader->index_scan!=NULL&&reader->index_slot!=NULL){
                /* Match native IndexNext: Ustore slot ownership frees each
                 * fetched undo/physical tuple before the next one is stored. */
                ExecClearTuple(reader->index_slot);
                found=IndexGetnextSlot(reader->index_scan,ForwardScanDirection,reader->index_slot);
                if(found){
                    tableam_tslot_getallattrs(reader->index_slot);
                    int attributes=RelationGetDescr(reader->relation)->natts;
                    for(int i=0;i<attributes;++i){reader->values[i]=reader->index_slot->tts_values[i];reader->nulls[i]=reader->index_slot->tts_isnull[i];}
                }
            }else{
                tuple = reader->index_scan!=NULL ?
                    (Tuple)scan_handler_idx_getnext(reader->index_scan,ForwardScanDirection) :
                    tableam_scan_getnexttuple(reader->scan, ForwardScanDirection);
                found=tuple!=NULL;
                if(found)tableam_tops_deform_tuple((Tuple)tuple, RelationGetDescr(reader->relation),reader->values,reader->nulls);
            }
            /* AM tuple flags are defined only when a tuple was returned.
             * Empty/exhausted DiskANN scans may leave xs_recheck untouched. */
            if(found && reader->index_scan!=NULL && reader->index_scan->xs_recheck)
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("lake inline B-tree returned a lossy candidate")));
            MemoryContextSwitchTo(caller);
        }
        PG_CATCH();
        {
            MemoryContextSwitchTo(caller);
            PG_RE_THROW();
        }
        PG_END_TRY();
        if (!found) {
            if(reader->index_scan!=NULL&&reader->ann&&diskann_advance_ordered_batch(reader->index_scan))continue;
            if(binding->inline_sources_loaded&&reader->source_index+1<binding->inline_source_count){
                int next=reader->source_index+1;
                LakeReaderEndCarrier(reader);
                if(!LakeReaderOpenCarrier(reader,next))
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake inline read source disappeared")));
                continue;
            }
            reader->base_done=true;continue;
        }
        /* Copy output Datums below in the caller's own per-row context. */
        int rowIndex = reader->row_id_attnum - 1;
        int beginIndex = reader->begin_attnum - 1;
        int endIndex = reader->end_attnum - 1;
        if (reader->nulls[rowIndex] || reader->nulls[beginIndex])
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("native lake inline row identity or begin snapshot is NULL")));
        int64 rowid = DatumGetInt64(reader->values[rowIndex]);
        int64 begin = DatumGetInt64(reader->values[beginIndex]);
        int64 end = reader->nulls[endIndex] ? 0 : DatumGetInt64(reader->values[endIndex]);
        if (rowid < 0 || begin < 0 || (!reader->nulls[endIndex] && end < begin))
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("native lake inline row has invalid version bounds")));
        if(binding->has_dropped_columns){
            const LakeInlineSource* physical=LakeFindInlineSource(binding,RelationGetRelid(reader->relation));
            if(!physical)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native read lost its physical source identity")));
            for(int p=0;p<physical->column_count;++p)
                if(LakeFindRetiredColumn(binding,physical->columns[p].column_id))
                    LakeValidateRetiredBirth(binding,physical->columns[p].column_id,begin,begin);
        }
        for(int c=0;c<binding->column_count;++c)
            if(reader->column_attnums[c]==InvalidAttrNumber&&!LakeColumnCanBeMissing(binding,c,begin))
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("inline row birth is newer than its missing field")));
        if (!reader->physical_inline && (begin > binding->snapshot_id || (!reader->nulls[endIndex] && end <= binding->snapshot_id) ||
            LakeTxnHidesInlineRow(reader->table, rowid, begin, reader->command_limit))) continue;
        if(reader->ann_seen){
            LakeOriginKey key={(uint64)rowid,(uint64)begin};bool duplicate=false;
            hash_search(reader->ann_seen,&key,HASH_FIND,&duplicate);
            if(duplicate)continue;
            if(hash_get_num_entries(reader->ann_seen)>=262144)
                ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("lake DiskANN candidate identity budget exceeded")));
            hash_search(reader->ann_seen,&key,HASH_ENTER,NULL);
        }
        Size binaryRowBytes=0;
        for (int i = 0; i < binding->column_count; ++i) {
            if (reader->projected && !reader->projected[i]) {
                values[i] = (Datum)0;
                nulls[i] = true;
                continue;
            }
            if(reader->column_attnums[i]==InvalidAttrNumber){
                LakeMaterializeInitialDefault(&binding->columns[i],&values[i],&nulls[i],&binaryRowBytes);continue;
            }
            int source = reader->column_attnums[i] - 1;
            nulls[i] = reader->nulls[source];
            values[i] = (Datum)0;
            if(!nulls[i]){
                Oid srcType=reader->source_types[i];
                int32 srcTypmod=reader->source_typmods[i];
                Oid tgtType=LakeColumnTargetType(&binding->columns[i]);
                int32 tgtTypmod=LakeColumnTargetTypmod(&binding->columns[i]);
                Datum val=(Datum)0;
                if(srcType==VECTOROID)
                    val=LakeVectorReadText(reader->values[source],srcTypmod);
                else if(LakeNativeTypeIsTemporal(srcType))
                    val=LakeTemporalReadText(reader->values[source],srcType);
                else if(srcType==BYTEAOID)
                    val=LakeCopyBinaryDatum(reader->values[source],&binaryRowBytes);
                else if(srcType==UUIDOID)
                    val=LakeCopyUuidDatum(reader->values[source],&binaryRowBytes);
                else if(srcType==JSONOID)
                    val=LakeJsonReadInline(reader->values[source],&binaryRowBytes);
                else if(srcType==NUMERICOID)
                    val=LakeCopyNumericDatum(reader->values[source],srcTypmod,&binaryRowBytes);
                else val=srcType==TEXTOID?
                    datumCopy(reader->values[source],false,-1):reader->values[source];
                LakeTemporalValidateDatum(srcType,val);
                if(srcType!=tgtType||srcTypmod!=tgtTypmod){
                    val=LakeWidenDatum(val,srcType,srcTypmod,tgtType,tgtTypmod);
                    if(tgtType==NUMERICOID)
                        val=LakeCopyNumericDatum(val,tgtTypmod,&binaryRowBytes);
                }
                values[i]=val;
            }
        }
        identity->row_id = rowid;
        identity->begin_snapshot = begin;
        identity->local_token = 0;
        identity->source=LAKE_LOCATOR_INLINE;identity->inline_oid=RelationGetRelid(reader->relation);identity->file_id=0;identity->file_position=0;
        identity->ended=!reader->nulls[endIndex];identity->end_snapshot=end;
        return true;
    }
}

void LakeInlineReaderUseIndex(LakeInlineReader* reader,Oid indexOid,AttrNumber column,const ScanKeyData* key)
{
    const LakeInlineBinding* binding=reader->table->binding;
    bool multiSourceAllowed=LakePartitionComboVersionEnabled()&&!binding->has_added_columns&&
        !binding->has_dropped_columns&&!binding->has_altered_types&&binding->retired_count==0&&binding->future_count==0;
    if(binding->inline_sources_loaded&&!multiSourceAllowed&&(binding->inline_source_count!=1||binding->inline_write_index!=0))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("a single inline index cannot cover historical carriers")));
    if(reader->for_analyze || reader->relation==NULL || key==NULL)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("invalid lake inline indexed read")));
    if(reader->index_relation==NULL)reader->index_relation=LakeOpenInlineIndex(indexOid,reader->table->binding,column);
    else if(RelationGetRelid(reader->index_relation)!=indexOid)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("lake inline index changed during rescan")));
    reader->index_key=*key;
    LakeInlineReaderReScan(reader);
}

bool LakeInlineReaderUseAnn(LakeInlineReader* reader,Oid indexOid,AttrNumber column,int metric,Datum query,bool isnull)
{
    const LakeInlineBinding* binding=reader->table->binding;
    bool multiSourceAllowed=LakePartitionComboVersionEnabled()&&!binding->has_added_columns&&
        !binding->has_dropped_columns&&!binding->has_altered_types&&binding->retired_count==0&&binding->future_count==0;
    if(binding->inline_sources_loaded&&!multiSourceAllowed&&(binding->inline_source_count!=1||binding->inline_write_index!=0)){
        LakeInlineReaderReScan(reader);return false;
    }
    if(reader->for_analyze||(OidIsValid(indexOid)&&reader->relation!=NULL&&RelationIsUstoreFormat(reader->relation)))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("invalid lake inline DiskANN reader")));
    if(reader->relation==NULL){LakeInlineReaderReScan(reader);return false;}
    MemoryContext caller=MemoryContextSwitchTo(reader->owner_context);
    Datum previous=reader->ann?reader->index_key.sk_argument:(Datum)0;
    Datum owned=(Datum)0;
    if(!isnull){
        Vector* vector=(Vector*)PG_DETOAST_DATUM_COPY(query);
        if(vector->dim!=reader->table->binding->columns[column-1].typmod)
            ereport(ERROR,(errcode(ERRCODE_DATA_EXCEPTION),errmsg("lake DiskANN query vector dimension differs from the column")));
        double norm=0;for(int i=0;i<vector->dim;++i)norm+=(double)vector->x[i]*vector->x[i];
        if(metric==3&&norm==0){pfree(vector);isnull=true;}else owned=PointerGetDatum(vector);
    }
    /* NULL/zero-cosine queries use the exact stream. The retained plan is safe
     * even when parameters or the session ANN opt-in change between executions. */
    bool use=!isnull&&OidIsValid(indexOid);
    if(reader->index_slot){
        if(reader->scan_context)MemoryContextSwitchTo(reader->scan_context);
        ExecDropSingleTupleTableSlot(reader->index_slot);reader->index_slot=NULL;
    }
    if(reader->index_scan){
        if(reader->scan_context)MemoryContextSwitchTo(reader->scan_context);
        scan_handler_idx_endscan(reader->index_scan);reader->index_scan=NULL;
    }
    MemoryContextSwitchTo(reader->owner_context);
    if(reader->index_relation){index_close(reader->index_relation,NoLock);reader->index_relation=NULL;}
    if(reader->ann_seen){hash_destroy(reader->ann_seen);reader->ann_seen=NULL;}
    if(previous)pfree(DatumGetPointer(previous));
    reader->index_key.sk_argument=(Datum)0;reader->ann=false;
    if(use){
        reader->index_relation=LakeOpenInlineAnnIndex(indexOid,reader->table->binding,column,metric);
        ScanKeyEntryInitialize(&reader->index_key,0,1,InvalidStrategy,VECTOROID,InvalidOid,get_opcode(LakeAnnOperator(metric)),owned);
        reader->ann=true;
        HASHCTL control;MemSet(&control,0,sizeof(control));control.keysize=sizeof(LakeOriginKey);control.entrysize=sizeof(LakeOriginKey);
        control.hcxt=reader->owner_context;reader->ann_seen=hash_create("Lake DiskANN returned versions",128,&control,HASH_ELEM|HASH_BLOBS|HASH_CONTEXT);
    }else if(owned)pfree(DatumGetPointer(owned));
    LakeInlineReaderReScan(reader);MemoryContextSwitchTo(caller);return use;
}

void LakeInlineReaderUseFileScalar(LakeInlineReader* reader,const char* uuid,AttrNumber column,const ScanKeyData* key,const LakeScalarFileSources* sources)
{
    if(reader->for_analyze||key==NULL)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("invalid lake file scalar read")));
    LakeFileReaderUseScalar(reader->files,uuid,column,key,sources);
}

void LakeInlineReaderUseFileAnn(LakeInlineReader* reader,const char* uuid,AttrNumber column,int metric,Datum query,bool isnull,bool enabled,uint32 minimum,const LakeAnnScalarExpr* filter,const LakeAnnFileSources* prebound,bool hasResidualQual)
{
    if(reader->for_analyze)ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("ANN is not an ANALYZE stream")));
    bool use=enabled&&!isnull;
    if(use){Vector* vector=DatumGetVector(PG_DETOAST_DATUM(query));
        if(vector->dim!=reader->table->binding->columns[column-1].typmod)
            ereport(ERROR,(errcode(ERRCODE_DATA_EXCEPTION),errmsg("lake file ANN query dimension differs from the column")));
        if(metric==3&&DatumGetFloat8(DirectFunctionCall1(vector_norm,PointerGetDatum(vector)))<=0)use=false;}
    LakeFileReaderUseAnn(reader->files,uuid,column,metric,query,use,minimum,filter,prebound,hasResidualQual);
}
void LakeInlineReaderEnableAnnRescanCache(LakeInlineReader* reader)
{if(reader)LakeFileReaderEnableAnnRescanCache(reader->files);}
LakeAnnRescanStats LakeInlineReaderAnnRescanStats(const LakeInlineReader* reader)
{return LakeFileReaderAnnRescanStats(reader?reader->files:NULL);}
int LakeInlineReaderPqFiles(const LakeInlineReader* reader)
{return reader?LakeFileReaderPqFiles(reader->files):0;}
int64 LakeInlineReaderAnnCandidateRows(const LakeInlineReader* reader)
{return reader?LakeFileReaderAnnCandidateRows(reader->files):0;}
int LakeInlineReaderAnnFiles(const LakeInlineReader* reader)
{return reader?LakeFileReaderAnnFiles(reader->files):0;}

int LakeInlineReaderScalarFiles(const LakeInlineReader* reader)
{
    return reader?LakeFileReaderScalarFiles(reader->files):0;
}

int LakeInlineReaderBitmapFiles(const LakeInlineReader* reader)
{
    return reader?LakeFileReaderBitmapFiles(reader->files):0;
}

int LakeInlineReaderAdmittedFiles(const LakeInlineReader* reader)
{return reader?LakeFileReaderAdmittedFiles(reader->files):0;}

void LakeInlineReaderApplyFilePruning(LakeInlineReader* reader,const bool* skipped,int fileCount)
{
    if(!reader)ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("lake inline reader is not initialized")));
    LakeFileReaderApplyPruning(reader->files,skipped,fileCount);
}

void LakeInlineReaderSetProjected(LakeInlineReader* reader, const bool* projected, int count)
{
    if (reader == NULL)
        return;
    if (projected != NULL && count != reader->table->binding->column_count)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("lake inline reader projection width disagrees with table schema")));
    MemoryContext caller = MemoryContextSwitchTo(reader->owner_context);
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
    LakeFileReaderSetProjected(reader->files, projected, count);
}

int LakeInlineReaderFillVectorBatch(LakeInlineReader* reader, VectorBatch* batch,
    Size* payloadBytes, Size payloadLimit, Size* colBatchBytes,
    bool* needFallback, bool* eos)
{
    LakeTxnRequireActiveTable(reader->table);
    if (!reader->base_done || reader->files == NULL) {
        if (needFallback) *needFallback = true;
        if (eos) *eos = false;
        return 0;
    }
    return LakeFileReaderFillVectorBatch(reader->files, batch, reader->projected,
        payloadBytes, payloadLimit, colBatchBytes, needFallback, eos);
}

void LakeInlineReaderReScan(LakeInlineReader* reader)
{
    LakeTxnRequireActiveTable(reader->table);
    LakeFileReaderReScan(reader->files);
    bool restartSources=reader->table->binding->inline_sources_loaded;
    if(restartSources){
        LakeReaderEndCarrier(reader);reader->source_index=-1;
        (void)LakeReaderOpenCarrier(reader,0);
    }
    if (!restartSources && reader->relation != NULL) {
        MemoryContext caller = CurrentMemoryContext;
        PG_TRY();
        {
            MemoryContextSwitchTo(reader->scan_context);
            if(reader->index_slot!=NULL){ExecDropSingleTupleTableSlot(reader->index_slot);reader->index_slot=NULL;}
            if(reader->index_scan!=NULL) {
                scan_handler_idx_endscan(reader->index_scan);reader->index_scan=NULL;
            } else if(reader->scan!=NULL) {
                tableam_scan_end(reader->scan);reader->scan=NULL;
            }
            MemoryContextSwitchTo(caller);
            /* Native UHeapRescan does not free every page-at-a-time tuple.
             * Reopen the same snapshot after ending the old scan, so repeated
             * partial rescans do not accumulate those abandoned copies. */
            MemoryContextReset(reader->scan_context);
            MemoryContextSwitchTo(reader->scan_context);
            LakeReaderBeginScan(reader);
            MemoryContextSwitchTo(caller);
        }
        PG_CATCH();
        {
            MemoryContextSwitchTo(caller);
            PG_RE_THROW();
        }
        PG_END_TRY();
    }
    reader->base_done = reader->scan == NULL && reader->index_scan==NULL;
    reader->last_token = 0;
    reader->journal_generation = 0;
    reader->cursor = LakeTxnJournalTable(reader->table)->rows;
    /* Never refresh the business snapshot or create a new command boundary. */
}
