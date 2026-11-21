/* Read-only public snapshot metadata. Grammar/key mapping adapted from
 * DuckLake 0aaa8e10d4adfe2ca37da77134e991d20106d0e7 (MIT; notice in
 * lake_catalog_ddl.inc): ducklake_snapshots.cpp, ducklake_transaction_changes.cpp,
 * ducklake_util.cpp and the bundled DuckDB SQLIdentifier printer.
 * Host adaptation: one RC statement Snapshot, 64-bit own-XID exclusion,
 * trusted metadata SPI, bounded pages and a spillable query-owned tuplestore.
 * No snapshot allocation, transaction boundary, SDK call or catalog mutation. */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "lake_txn_private.h"
#include "lake_version.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "distributelayer/streamCore.h"
#include "executor/spi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgxc/pgxc.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/json.h"
#include "utils/jsonb.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/tuplestore.h"

extern "C" Datum og_lake_current_snapshot(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_snapshots(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_last_committed_snapshot(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(og_lake_current_snapshot);
PG_FUNCTION_INFO_V1(og_lake_snapshots);
PG_FUNCTION_INFO_V1(og_lake_last_committed_snapshot);

static const int LakeSnapshotInfoPage = 512;
/* Mechanical fixed-source keyword union, not PostgreSQL's keyword list. */
#include "lake_snapshot_keywords.inc"

static void LakeSnapshotInfoMalformed(const char* detail)
{
    ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
        errmsg("invalid DuckLake snapshot metadata"), errdetail("%s", detail)));
}

static char LakeInfoLower(char c)
{ return c >= 'A' && c <= 'Z' ? (char)(c + ('a' - 'A')) : c; }

static char* LakeInfoFold(const char* input)
{
    char* result = pstrdup(input);
    for (Size i = 0; result[i]; ++i) result[i] = LakeInfoLower(result[i]);
    return result;
}

static char* LakeInfoIdentifier(const char* input)
{
    bool quote = !input[0] || (input[0] == '_' && input[1] >= '0' && input[1] <= '9');
    for (Size i = 0; input[i]; ++i) {
        unsigned char c = (unsigned char)input[i];
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' ||
              (i > 0 && c >= '0' && c <= '9'))) quote = true;
    }
    char* folded = LakeInfoFold(input);
    int low = 0, high = (int)lengthof(LakeSnapshotKeywords);
    while (low < high) {
        int mid = low + (high - low) / 2;
        int cmp = strcmp(folded, LakeSnapshotKeywords[mid]);
        if (cmp == 0) { quote = true; break; }
        if (cmp < 0) high = mid; else low = mid + 1;
    }
    pfree(folded);
    if (!quote) return pstrdup(input);
    StringInfoData out; initStringInfo(&out); appendStringInfoChar(&out, '"');
    for (Size i = 0; input[i]; ++i) {
        if (input[i] == '"') appendStringInfoChar(&out, '"');
        appendStringInfoChar(&out, input[i]);
    }
    appendStringInfoChar(&out, '"'); return out.data;
}

static char* LakeInfoQuoted(const char* input, Size* offset)
{
    if (input[*offset] != '"') LakeSnapshotInfoMalformed("expected a quoted change value");
    ++*offset;
    StringInfoData out; initStringInfo(&out);
    while (input[*offset]) {
        char c = input[(*offset)++];
        if (c != '"') { appendStringInfoChar(&out, c); continue; }
        if (input[*offset] == '"') {
            ++*offset; appendStringInfoChar(&out, '"'); continue;
        }
        return out.data;
    }
    LakeSnapshotInfoMalformed("unterminated quoted change value"); return NULL;
}

/* Keep the quoted component chain as the identity, so dots inside components
 * cannot collide with qualification separators. Input is the persisted grammar. */
static char* LakeInfoPath(const char* input, bool entry, char** schema)
{
    Size offset = 0; List* parts = NIL;
    do {
        CHECK_FOR_INTERRUPTS();
        char* part = LakeInfoQuoted(input, &offset);
        if (!part[0]) LakeSnapshotInfoMalformed("empty qualified-name component");
        parts = lappend(parts, part);
        if (!input[offset]) break;
        if (input[offset++] != '.' || !input[offset])
            LakeSnapshotInfoMalformed("invalid qualified-name separator");
    } while (true);
    int count = list_length(parts);
    if (entry && count < 2) LakeSnapshotInfoMalformed("created entry has no schema");
    StringInfoData out; initStringInfo(&out);
    ListCell* cell = NULL; int index = 0;
    foreach(cell, parts) {
        if (index) appendStringInfoChar(&out, '.');
        appendStringInfoString(&out, LakeInfoIdentifier((char*)lfirst(cell)));
        if (schema && index == count - 2) *schema = pstrdup(out.data);
        ++index;
    }
    return out.data;
}

static uint64 LakeInfoUnsigned(const char* value)
{
    if (!value[0]) LakeSnapshotInfoMalformed("empty numeric change identity");
    uint64 result = 0;
    for (Size i = 0; value[i]; ++i) {
        unsigned char c = (unsigned char)value[i];
        if (c < '0' || c > '9' || result > (PG_UINT64_MAX - (c - '0')) / 10)
            LakeSnapshotInfoMalformed("invalid unsigned change identity");
        result = result * 10 + (c - '0');
    }
    return result;
}

static const char* LakeInfoKeys[] = {
    "schemas_created", "schemas_dropped", "tables_created", "views_created",
    "scalar_macros_created", "table_macros_created", "tables_dropped", "tables_altered",
    "tables_inserted_into", "tables_deleted_from", "views_dropped", "scalar_macros_dropped",
    "table_macros_dropped", "views_altered", "inlined_insert", "inlined_delete", "flushed_inlined",
    "merge_adjacent", "rewrite_delete", "compacted_table", "tags_created", "tags_dropped", "snapshots_expired"
};
struct LakeInfoChangeType { const char* token; int key; bool numeric; };
static const LakeInfoChangeType LakeInfoTypes[] = {
    {"created_schema",0,false}, {"dropped_schema",1,true}, {"created_table",2,false},
    {"created_view",3,false}, {"created_scalar_macro",4,false}, {"created_table_macro",5,false},
    {"dropped_table",6,true}, {"altered_table",7,true}, {"inserted_into_table",8,true},
    {"deleted_from_table",9,true}, {"dropped_view",10,true}, {"dropped_scalar_macro",11,true},
    {"dropped_table_macro",12,true}, {"altered_view",13,true}, {"inlined_insert",14,true},
    {"inlined_delete",15,true}, {"flushed_inlined",16,true}, {"inline_flush",16,true},
    {"merge_adjacent",17,true}, {"rewrite_delete",18,true}, {"compacted_table",19,true},
    {"created_tag",20,false}, {"dropped_tag",21,false}, {"expired_snapshot",22,true}
};
struct LakeInfoChange { char* identity; char* value; int key; bool numeric; uint64 number; };
struct LakeInfoSchema { char* identity; char* value; };
static uint32 LakeInfoHash(const void* key, Size size)
{
    (void)size;
    const unsigned char* value = (const unsigned char*)*(const char* const*)key;
    uint32 hash = 2166136261U;
    while (*value) hash = (hash ^ *value++) * 16777619U;
    return hash;
}
static int LakeInfoCompare(const void* left, const void* right, Size size)
{ (void)size; return strcmp(*(const char* const*)left, *(const char* const*)right); }

static HTAB* LakeInfoHashTable(const char* label, Size entrysize)
{
    HASHCTL ctl; errno_t rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl)); securec_check(rc,"","");
    ctl.keysize = sizeof(char*); ctl.entrysize = entrysize; ctl.hash = LakeInfoHash;
    ctl.match = LakeInfoCompare; ctl.hcxt = CurrentMemoryContext;
    return hash_create(label, 32, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);
}
static int LakeInfoChangeOrder(const void* left, const void* right)
{
    const LakeInfoChange* a = *(const LakeInfoChange* const*)left;
    const LakeInfoChange* b = *(const LakeInfoChange* const*)right;
    if (a->key != b->key) return a->key < b->key ? -1 : 1;
    if (a->numeric) return a->number < b->number ? -1 : (a->number > b->number ? 1 : 0);
    return strcmp(a->value, b->value);
}

static Datum LakeInfoChanges(const char* input)
{
    HTAB* entries = LakeInfoHashTable("Snapshot change entries", sizeof(LakeInfoChange));
    HTAB* schemas = LakeInfoHashTable("Snapshot created schemas", sizeof(LakeInfoSchema));
    Size offset = 0;
    while (input[offset]) {
        CHECK_FOR_INTERRUPTS();
        Size start = offset;
        while (input[offset] && input[offset] != ':') ++offset;
        if (input[offset] != ':') LakeSnapshotInfoMalformed("change type has no colon");
        char* token = LakeInfoFold(pnstrdup(input + start, offset - start)); ++offset;
        const LakeInfoChangeType* type = NULL;
        for (Size i = 0; i < lengthof(LakeInfoTypes); ++i)
            if (!strcmp(token, LakeInfoTypes[i].token)) { type = &LakeInfoTypes[i]; break; }
        if (!type) LakeSnapshotInfoMalformed("unknown change type");
        start = offset; bool quoted = false;
        while (input[offset]) {
            if (input[offset] == '"') quoted = !quoted;
            if (!quoted && input[offset] == ',') break;
            ++offset;
        }
        if (quoted) LakeSnapshotInfoMalformed("unbalanced change quotes");
        char* raw = pnstrdup(input + start, offset - start);
        if (input[offset]) ++offset;
        char* value = NULL; uint64 number = 0;
        int group = (type->key == 3 ? 2 : type->key); /* created table/view share first-wins map */
        if (type->numeric) {
            number = LakeInfoUnsigned(raw); value = psprintf(UINT64_FORMAT, number);
        } else if (type->key == 20 || type->key == 21) {
            Size pos = 0; value = LakeInfoQuoted(raw, &pos);
            if (raw[pos]) LakeSnapshotInfoMalformed("trailing tag-name bytes");
        } else {
            char* schema = NULL;
            value = LakeInfoPath(raw, type->key != 0, type->key == 0 ? NULL : &schema);
            if (schema) {
                char* identity = psprintf("%d:%s", group, LakeInfoFold(schema)); bool found = false;
                LakeInfoSchema* original = (LakeInfoSchema*)hash_search(schemas, &identity, HASH_ENTER, &found);
                if (!found) original->value = schema;
                /* Preserve the spelling of the first schema-map key, as upstream. */
                value = psprintf("%s%s", original->value, value + strlen(schema));
            }
        }
        char* identity = psprintf("%d:%s", group, type->numeric ? value : LakeInfoFold(value));
        bool found = false;
        LakeInfoChange* change = (LakeInfoChange*)hash_search(entries, &identity, HASH_ENTER, &found);
        if (!found) { change->key = type->key; change->value = value; change->number = number; change->numeric = type->numeric; }
    }
    long count = hash_get_num_entries(entries);
    if (count < 0 || (Size)count > MaxAllocSize / sizeof(LakeInfoChange*))
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("snapshot changes exceed host allocation limit")));
    LakeInfoChange** ordered = (LakeInfoChange**)palloc(Max((Size)1, (Size)count * sizeof(LakeInfoChange*)));
    HASH_SEQ_STATUS scan; hash_seq_init(&scan, entries); long n = 0; LakeInfoChange* item = NULL;
    while ((item = (LakeInfoChange*)hash_seq_search(&scan)) != NULL) ordered[n++] = item;
    qsort(ordered, count, sizeof(LakeInfoChange*), LakeInfoChangeOrder);
    StringInfoData json; initStringInfo(&json); appendStringInfoChar(&json, '{');
    int previous = -1;
    for (long i = 0; i < count; ++i) {
        CHECK_FOR_INTERRUPTS();
        if (ordered[i]->key != previous) {
            if (previous >= 0) appendStringInfoString(&json, "],");
            escape_json(&json, LakeInfoKeys[ordered[i]->key]); appendStringInfoString(&json, ":[");
            previous = ordered[i]->key;
        } else appendStringInfoChar(&json, ',');
        escape_json(&json, ordered[i]->value);
    }
    if (previous >= 0) appendStringInfoChar(&json, ']');
    appendStringInfoChar(&json, '}');
    return DirectFunctionCall1(jsonb_in, CStringGetDatum(json.data));
}

static bool LakeInfoOwn(const LakeMetaRows* rows, uint32 row, int column)
{ return TransactionIdIsCurrentTransactionId(DatumGetTransactionId(LakeMetadataValue(rows,row,column,XIDOID))); }

static int64 LakeInfoHead(LakeInlineBinding* binding, Snapshot snapshot, MemoryContext page)
{
    bool first = true, sawOwn = false; int64 cursor = 0;
    for (;;) {
        CHECK_FOR_INTERRUPTS(); MemoryContextReset(page); MemoryContext parent = MemoryContextSwitchTo(page);
        Oid types[2] = {BOOLOID,INT8OID}; Datum args[2] = {BoolGetDatum(first),Int64GetDatum(cursor)};
        LakeMetaRows* rows = LakeMetadataQuery(psprintf(
            "SELECT s.snapshot_id,s.schema_version,s.xmin FROM %s s WHERE ($1 OR s.snapshot_id<$2) "
            "ORDER BY s.snapshot_id DESC LIMIT 512",LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_snapshot")),
            2,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
        if (!rows->count) {
            if (sawOwn) ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("lake catalog has no committed base snapshot yet")));
            LakeSnapshotInfoMalformed("catalog has no visible committed snapshot");
        }
        for (uint32 i = 0; i < rows->count; ++i) {
            int64 id = DatumGetInt64(LakeMetadataValue(rows,i,1,INT8OID));
            if (id < 0 || (!first && id >= cursor) || DatumGetInt64(LakeMetadataValue(rows,i,2,INT8OID)) < 0)
                LakeSnapshotInfoMalformed("invalid descending snapshot identity");
            first = false; cursor = id;
            if (!LakeInfoOwn(rows,i,3)) { MemoryContextSwitchTo(parent); MemoryContextReset(page); return id; }
            sawOwn = true;
        }
        MemoryContextSwitchTo(parent);
    }
}

static void LakeInfoList(LakeInlineBinding* binding, Snapshot snapshot, int64 head,
    MemoryContext page, Tuplestorestate* store, TupleDesc descriptor)
{
    int64 cursor = -1;
    while (cursor < head) {
        CHECK_FOR_INTERRUPTS(); MemoryContextReset(page); MemoryContext parent = MemoryContextSwitchTo(page);
        Oid types[2] = {INT8OID,INT8OID}; Datum args[2] = {Int64GetDatum(cursor),Int64GetDatum(head)};
        LakeMetaRows* rows = LakeMetadataQuery(psprintf(
            "SELECT s.snapshot_id,s.snapshot_time,s.schema_version,c.changes_made::text,c.author::text,c.commit_message::text,c.commit_extra_info::text,s.xmin "
            "FROM %s s LEFT JOIN %s c USING(snapshot_id) WHERE s.snapshot_id>$1 AND s.snapshot_id<=$2 "
            "ORDER BY s.snapshot_id LIMIT 512",LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_snapshot"),
            LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_snapshot_changes")),
            2,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
        if (!rows->count) LakeSnapshotInfoMalformed("committed snapshot head disappeared from pinned snapshot");
        for (uint32 i = 0; i < rows->count; ++i) {
            CHECK_FOR_INTERRUPTS();
            Datum values[7]; bool nulls[7] = {false};
            values[0] = LakeMetadataValue(rows,i,1,INT8OID); int64 id = DatumGetInt64(values[0]);
            values[1] = LakeMetadataValue(rows,i,2,TIMESTAMPTZOID);
            values[2] = LakeMetadataValue(rows,i,3,INT8OID);
            if (id <= cursor || id > head || DatumGetInt64(values[2]) < 0)
                LakeSnapshotInfoMalformed("invalid ascending snapshot identity or duplicate changes row");
            cursor = id; /* Also advance for filtered own rows; never starve pagination. */
            if (LakeInfoOwn(rows,i,8)) continue;
            /* OG MemoryContextReset recursively resets, but retains child
             * contexts. Delete each row context (including both hash tables)
             * instead of accumulating child-context headers across pages. */
            MemoryContext rowContext = AllocSetContextCreate(page,"Lake snapshot changes row",ALLOCSET_DEFAULT_MINSIZE,
                ALLOCSET_DEFAULT_INITSIZE,ALLOCSET_DEFAULT_MAXSIZE);
            MemoryContextSwitchTo(rowContext);
            bool missing = false; Datum changes = LakeMetadataValue(rows,i,4,TEXTOID,true,&missing);
            values[3] = LakeInfoChanges(missing ? "" : TextDatumGetCString(changes));
            for (int j = 4; j < 7; ++j) values[j] = LakeMetadataValue(rows,i,j+1,TEXTOID,true,&nulls[j]);
            tuplestore_putvalues(store,descriptor,values,nulls);
            MemoryContextSwitchTo(page);
            MemoryContextDelete(rowContext);
        }
        MemoryContextSwitchTo(parent);
    }
}

static Datum LakeSnapshotInfo(FunctionCallInfo fcinfo, bool list)
{
    LakeInlineRequireAdministrator();
    if (StreamThreadAmI() || IS_SPQ_RUNNING)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("public snapshot metadata requires the invoking serial backend")));
    if (!ActiveSnapshotSet())
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("public snapshot metadata requires an active statement snapshot")));
    LakeRequireAccessPolicy();
    if (!LakeSnapshotMetadataVersionEnabled())
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("public snapshot metadata requires installed SQL0.55")));
    if (PG_ARGISNULL(0))
        ereport(ERROR,(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),errmsg("lake catalog name cannot be NULL")));
    ReturnSetInfo* result = (ReturnSetInfo*)fcinfo->resultinfo;
    if (!result || !IsA(result,ReturnSetInfo) || !(result->allowedModes & SFRM_Materialize) || !result->econtext)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("public snapshot metadata requires materialize mode")));
    ForeignServer* server = GetForeignServerByName(NameStr(*PG_GETARG_NAME(0)),false);
    MemoryContext caller = MemoryContextSwitchTo(result->econtext->ecxt_per_query_memory);
    TupleDesc descriptor = CreateTemplateTupleDesc(list ? 7 : 1,false);
    TupleDescInitEntry(descriptor,1,list ? "snapshot_id" : "id",INT8OID,-1,0);
    if (list) {
        TupleDescInitEntry(descriptor,2,"snapshot_time",TIMESTAMPTZOID,-1,0);
        TupleDescInitEntry(descriptor,3,"schema_version",INT8OID,-1,0);
        TupleDescInitEntry(descriptor,4,"changes",JSONBOID,-1,0);
        TupleDescInitEntry(descriptor,5,"author",TEXTOID,-1,0);
        TupleDescInitEntry(descriptor,6,"commit_message",TEXTOID,-1,0);
        TupleDescInitEntry(descriptor,7,"commit_extra_info",TEXTOID,-1,0);
    }
    descriptor = BlessTupleDesc(descriptor);
    Tuplestorestate* store = tuplestore_begin_heap(true,false,Max(64,Min(u_sess->attr.attr_memory.work_mem,1024)));
    Snapshot volatile snapshot = NULL; MemoryContext volatile page = NULL;
    PG_TRY(); {
        snapshot = RegisterSnapshot(GetActiveSnapshot());
        LakeInlineBinding* binding = LakeLoadCatalogHeader(server->serverid,snapshot);
        page = AllocSetContextCreate(CurrentMemoryContext,"Lake public snapshot page",ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,ALLOCSET_DEFAULT_MAXSIZE);
        int64 head = LakeInfoHead(binding,snapshot,page);
        if (list) LakeInfoList(binding,snapshot,head,page,store,descriptor);
        else { Datum value = Int64GetDatum(head); bool isnull = false; tuplestore_putvalues(store,descriptor,&value,&isnull); }
        MemoryContextDelete(page); page = NULL;
        UnregisterSnapshot(snapshot); snapshot = NULL;
        tuplestore_donestoring(store); result->returnMode = SFRM_Materialize;
        result->setResult = store; result->setDesc = descriptor;
        MemoryContextSwitchTo(caller);
    } PG_CATCH(); {
        MemoryContextSwitchTo(result->econtext->ecxt_per_query_memory);
        if (page) MemoryContextDelete(page);
        if (snapshot) UnregisterSnapshot(snapshot);
        MemoryContextSwitchTo(caller); PG_RE_THROW();
    } PG_END_TRY();
    fcinfo->isnull = true; return (Datum)0;
}

Datum og_lake_current_snapshot(PG_FUNCTION_ARGS) { return LakeSnapshotInfo(fcinfo,false); }
Datum og_lake_snapshots(PG_FUNCTION_ARGS) { return LakeSnapshotInfo(fcinfo,true); }

Datum og_lake_last_committed_snapshot(PG_FUNCTION_ARGS)
{
    LakeInlineRequireAdministrator();
    if (StreamThreadAmI() || IS_SPQ_RUNNING)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("public snapshot metadata requires the invoking serial backend")));
    if (!ActiveSnapshotSet())
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("public snapshot metadata requires an active statement snapshot")));
    LakeRequireAccessPolicy();
    if (!LakeLastCommittedVersionEnabled())
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("last_committed_snapshot requires installed SQL0.56")));
    if (PG_ARGISNULL(0))
        ereport(ERROR,(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),errmsg("lake catalog name cannot be NULL")));
    ReturnSetInfo* result = (ReturnSetInfo*)fcinfo->resultinfo;
    if (!result || !IsA(result,ReturnSetInfo) || !(result->allowedModes & SFRM_Materialize) || !result->econtext)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("public snapshot metadata requires materialize mode")));
    ForeignServer* server = GetForeignServerByName(NameStr(*PG_GETARG_NAME(0)),false);
    MemoryContext caller = MemoryContextSwitchTo(result->econtext->ecxt_per_query_memory);
    TupleDesc descriptor = CreateTemplateTupleDesc(1,false);
    TupleDescInitEntry(descriptor,1,"id",INT8OID,-1,0);
    descriptor = BlessTupleDesc(descriptor);
    Tuplestorestate* store = tuplestore_begin_heap(true,false,Max(64,Min(u_sess->attr.attr_memory.work_mem,1024)));
    Snapshot volatile snapshot = NULL;
    PG_TRY(); {
        snapshot = RegisterSnapshot(GetActiveSnapshot());
        LakeInlineBinding* binding = LakeLoadCatalogHeader(server->serverid,snapshot);
        LakeCommittedCatalogKey key;
        if (!LakeCommittedKeyFromBinding(binding, &key))
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("invalid native lake catalog binding identity")));
        int64 committedId = 0;
        bool hasCommitted = LakeHostLookupCommittedSnapshot(&key, &committedId);
        Datum value = hasCommitted ? Int64GetDatum(committedId) : (Datum)0;
        bool isnull = !hasCommitted;
        tuplestore_putvalues(store,descriptor,&value,&isnull);
        UnregisterSnapshot(snapshot); snapshot = NULL;
        tuplestore_donestoring(store); result->returnMode = SFRM_Materialize;
        result->setResult = store; result->setDesc = descriptor;
        MemoryContextSwitchTo(caller);
    } PG_CATCH(); {
        MemoryContextSwitchTo(result->econtext->ecxt_per_query_memory);
        if (snapshot) UnregisterSnapshot(snapshot);
        MemoryContextSwitchTo(caller); PG_RE_THROW();
    } PG_END_TRY();
    fcinfo->isnull = true; return (Datum)0;
}
