/* Format 1.1 native inline publication. All SQL stays in the user's transaction. */
#include "postgres.h"
#include "lake_uuid.h"
#include "lake_column_layout.h"
#include "lake_nullability.h"
#include "lake_column_default.h"
#include "lake_version.h"
#include "lake_inline_sources.h"
#include "lake_temporal_text.h"
#include <limits.h>
#include "access/og_lake.h"
#include "lake_native_partition.h"
#include "lake_autoanalyze.h"
#include "lake_vector_codec.h"
#include "lake_types.h"
#include "lake_decimal128_codec.h"
#include "knl/knl_variable.h"
#include "lake_txn_private.h"
#include "lake_create_publish.h"
#include "lake_flush.h"
#include "lake_rewrite.h"
#include "lake_index_private.h"
#include "lake_arrow_builder.h"
#include "lake_rename.h"
#include "lake_drop.h"
#include "lake_row_lock.h"
#include "lake_inline_reader.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/pg_am.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/reltrigger.h"
#include "utils/snapmgr.h"

struct LakeWriteColumn { const char* name; Oid type; };
static const LakeWriteColumn snapshotColumns[] = {
    {"snapshot_id",INT8OID},{"snapshot_time",TIMESTAMPTZOID},{"schema_version",INT8OID},
    {"next_catalog_id",INT8OID},{"next_file_id",INT8OID}
};
static const LakeWriteColumn statsColumns[] = {
    {"table_id",INT8OID},{"record_count",INT8OID},{"next_row_id",INT8OID},{"file_size_bytes",INT8OID}
};
static const LakeWriteColumn columnStatsColumns[] = {
    {"table_id",INT8OID},{"column_id",INT8OID},{"contains_null",BOOLOID},{"contains_nan",BOOLOID},
    {"min_value",TEXTOID},{"max_value",TEXTOID},{"extra_stats",TEXTOID},
    {"min_is_exact",BOOLOID},{"max_is_exact",BOOLOID}
};
static const LakeWriteColumn changesColumns[] = {
    {"snapshot_id",INT8OID},{"changes_made",TEXTOID},{"author",TEXTOID},
    {"commit_message",TEXTOID},{"commit_extra_info",TEXTOID}
};

/* Exact SQL35 admission; retained SQL34 keeps its partitioned direct refusal. */
static bool LakePartitionDirectInsertEnabled(void) { return LakeCoreFollowupsVersionEnabled(); }
/* Candidate ownership currently admits sixteen files across the whole host
 * transaction. This is independent of flush's two-group maintenance prefix. */
static const int LAKE_DIRECT_INSERT_GROUP_LIMIT=16;

/* Only newly inserted logical rows may omit permanent identities. */
static bool LakeDirectInsertEligible(const LakeTxnTable* input)
{
    const LakeTxnTable* table=input->journal?input->journal:input;
    bool partitionAdapter=LakePartitionDirectInsertEnabled()&&table->binding->inline_sources_loaded;
    if(OidIsValid(table->native_leaf_partition_oid)||!LakeDirectInsertVersionEnabled()||
       (LakeIdentityPartitioned(table->binding)&&!partitionAdapter)||table->pending||table->binding->identity.kind!=LAKE_TABLE_PERMANENT||
       !OidIsValid(LakeMetadataOwner()))return false;
    for(const LakeTxnIdentity* identity=table->identities;identity;identity=identity->next)
        if(identity->committed_origin)return false;
    for(int c=0;c<table->binding->column_count;++c){Oid t=table->binding->columns[c].type;
        if(t!=BOOLOID&&t!=INT2OID&&t!=INT4OID&&t!=INT8OID&&t!=FLOAT4OID&&t!=FLOAT8OID&&t!=TEXTOID&&t!=BYTEAOID&&t!=UUIDOID&&t!=JSONOID&&t!=NUMERICOID&&
           !(partitionAdapter&&LakeNativeTypeIsTemporal(t)))return false;}
    return true;
}

static void LakeRequireColumn(Relation relation, const char* name, Oid type)
{
    AttrNumber number = get_attnum(RelationGetRelid(relation),name);
    if (number <= 0)
        ereport(ERROR,(errcode(ERRCODE_DATATYPE_MISMATCH),errmsg("native lake write column \"%s\" is missing",name)));
    Form_pg_attribute attr = &RelationGetDescr(relation)->attrs[number-1];
    if (attr->attisdropped || (attr->atttypid != type && !(type==TEXTOID && attr->atttypid==VARCHAROID)))
        ereport(ERROR,(errcode(ERRCODE_DATATYPE_MISMATCH),errmsg("native lake write column \"%s\" has the wrong type",name)));
}

static void LakeRequireNoUserActions(Relation relation,const LakeInlineBinding* business=NULL)
{
    TupleConstr* constraints = RelationGetDescr(relation)->constr;
    if (relation->trigdesc != NULL || relation->rd_rules != NULL ||
        (constraints != NULL && (constraints->num_check != 0 ||
            (constraints->num_defval != 0 && (!business||!business->literal_defaults)))))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake publication requires tables without triggers, rules, defaults or CHECK constraints")));
    if(business&&business->literal_defaults)LakeValidateHostDefaults(business,relation);
    if(business&&business->not_null_constraints){
        TupleDesc desc=RelationGetDescr(relation);
        if(desc->natts!=LakeHostColumnCount(business))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake publication constraint shape changed")));
        for(int c=0;c<business->column_count;++c)
            if(desc->attrs[LakeLogicalToHost(business,c)].attnotnull!=business->columns[c].not_null)
                ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake publication constraints changed")));
    }
}

static void LakeRequireSnapshotPrimaryKey(Relation relation)
{
    bool found = false;
    List* indexes = RelationGetIndexList(relation);
    ListCell* cell = NULL;
    AttrNumber key = get_attnum(RelationGetRelid(relation),"snapshot_id");
    foreach(cell,indexes) {
        Relation index = index_open(lfirst_oid(cell),AccessShareLock);
        if (index->rd_index->indisprimary && index->rd_index->indisvalid &&
            index->rd_index->indimmediate && index->rd_index->indnatts == 1 &&
            index->rd_index->indkey.values[0] == key && index->rd_rel->relam == BTREE_AM_OID)
            found = true;
        index_close(index,NoLock);
    }
    list_free(indexes);
    if (!found)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake snapshot publication requires an immediate primary key on snapshot_id")));
}

static char* LakeWriteRelation(LakeInlineBinding* binding, const char* name,
    const LakeWriteColumn* columns, int count, bool primaryKey = false)
{
    char* qualified = LakeMetadataRelation(binding->metadata_schema_oid,name);
    Oid oid = get_relname_relid(name,binding->metadata_schema_oid);
    Relation relation = heap_open(oid,AccessShareLock);
    LakeRequireNoUserActions(relation);
    if (RelationGetDescr(relation)->natts != count)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake write catalog is not the supported format 1.1 layout")));
    for (int i=0;i<count;++i) LakeRequireColumn(relation,columns[i].name,columns[i].type);
    if (primaryKey) LakeRequireSnapshotPrimaryKey(relation);
    heap_close(relation,NoLock);
    return qualified;
}

static void LakeCheckWriteOptions(LakeInlineBinding* binding, Snapshot snapshot)
{
    /* Options-only commits need not create a snapshot/changeset. Read their
     * standard metadata rows directly at admission and again at publication. */
    char* name = LakeMetadataRelation(binding->metadata_schema_oid, "ducklake_metadata");
    Relation relation = heap_open(get_relname_relid("ducklake_metadata", binding->metadata_schema_oid), AccessShareLock);
    LakeRequireColumn(relation, "key", TEXTOID);
    LakeRequireColumn(relation, "value", TEXTOID);
    LakeRequireColumn(relation, "scope", TEXTOID);
    LakeRequireColumn(relation, "scope_id", INT8OID);
    heap_close(relation, NoLock);
    Oid type = INT8OID;
    Datum argument = binding->identity.kind==LAKE_TABLE_PERMANENT ? Int64GetDatum(LakePermanentTableId(binding)) : (Datum)0;
    LakeMetaRows* options = LakeMetadataQuery(psprintf("SELECT key::text,value::text FROM %s WHERE "
        "(key='require_commit_message' AND scope IS NULL) OR "
        "(key='skip_stats_columns' AND scope='table' AND scope_id=$1)", name),
        1, &type, &argument, binding->identity.kind==LAKE_TABLE_LOCAL ? "n" : NULL, snapshot, true, SPI_OK_SELECT, binding->metadata_schema_oid);
    bool sawCommitMessage = false, sawSkippedStats = false;
    for (uint32 i = 0; i < options->count; ++i) {
        char* key = LakeMetadataText(options, i, 1);
        bool isnull = false;
        Datum value = LakeMetadataValue(options, i, 2, TEXTOID, true, &isnull);
        const char* setting = isnull ? NULL : TextDatumGetCString(value);
        if (strcmp(key, "require_commit_message") == 0) {
            if (sawCommitMessage)
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("duplicate native lake require_commit_message option")));
            sawCommitMessage = true;
            if (setting == NULL || pg_strcasecmp(setting, "false") != 0)
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("native lake inline INSERT does not yet support required commit messages")));
        } else {
            if (sawSkippedStats)
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("duplicate native lake skip_stats_columns option")));
            sawSkippedStats = true;
            if (setting != NULL && setting[0] != '\0')
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("native lake inline INSERT does not yet support skip_stats_columns")));
        }
    }
}

static int64 LakeFileInlineLimit(LakeInlineBinding* binding,Snapshot snapshot)
{
    char* options=LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_metadata");
    char* tables=LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_table");
    Oid types[2]={INT8OID,INT8OID};
    Datum args[2]={Int64GetDatum(LakePermanentTableId(binding)),Int64GetDatum(binding->snapshot_id)};
    LakeMetaRows* rows=LakeMetadataQuery(psprintf("SELECT scope::text,value::text FROM %s WHERE key='data_inlining_row_limit' "
        "AND (scope IS NULL OR (scope='table' AND scope_id=$1) OR (scope='schema' AND scope_id=(SELECT schema_id "
        "FROM %s WHERE table_id=$1 AND begin_snapshot<=$2 AND (end_snapshot IS NULL OR end_snapshot>$2))))",options,tables),
        2,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    bool seen[3]={false,false,false};int64 limits[3]={10,10,10};
    for(uint32 i=0;i<rows->count;++i){
        bool global=false;Datum scope=LakeMetadataValue(rows,i,1,TEXTOID,true,&global);
        int rank=global?0:strcmp(TextDatumGetCString(scope),"schema")==0?1:2;
        if(seen[rank])ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("duplicate native lake inline limit at one scope")));
        const char* value=LakeMetadataText(rows,i,2);
        if(*value=='\0')ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("empty native lake inline limit")));
        for(const char* c=value;*c;++c)if(*c<'0'||*c>'9')
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake inline limit must be a nonnegative integer")));
        limits[rank]=DatumGetInt64(DirectFunctionCall1(int8in,CStringGetDatum(value)));seen[rank]=true;
    }
    return seen[2]?limits[2]:seen[1]?limits[1]:limits[0];
}

void LakeValidateInlineWriteTarget(LakeTxnTable* table, Relation hostRelation, Snapshot snapshot)
{
    LakeInlineBinding* binding = table->binding;
    LakeCheckWriteOptions(binding, snapshot);
    if(LakeFileModificationsEnabled()&&binding->identity.kind==LAKE_TABLE_PERMANENT&&
        (binding->file_count>0||!OidIsValid(binding->inline_oid)||LakeIdentityPartitioned(binding))) {
        int64 limit=LakeFileInlineLimit(binding,snapshot),fileOrigins=0;
        for(LakeTxnIdentity* identity=table->identities;identity!=NULL;identity=identity->next)
            if(identity->committed_origin&&identity->head!=NULL&&identity->origin.source==LAKE_LOCATOR_FILE)++fileOrigins;
        if(!LakeDirectInsertEligible(table)&&(limit<=0||table->active_rows>limit||fileOrigins>limit))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("native lake file DML exceeds its configured inline path"),
                errdetail("Direct data/delete-file writes are not implemented; increase the standard inline limit or use a smaller transaction.")));
    }
    if (GetDatabaseEncoding() != PG_UTF8)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake inline writes currently require a UTF8 database")));
    if (!OidIsValid(binding->inline_oid) && !(LakeFileModificationsEnabled() &&
        binding->identity.kind==LAKE_TABLE_PERMANENT && OidIsValid(LakeMetadataOwner())))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake inline writes require an existing current inline table")));
    LakeRequireNoUserActions(hostRelation,OidIsValid(table->native_leaf_partition_oid)?NULL:binding);
    if(OidIsValid(table->native_leaf_partition_oid)) {
        if(!LakeNativePartitionWriteVersionEnabled()||!LakeHasNativePartitionMarker(hostRelation))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native leaf writes require the coordinated routing capability")));
        List* indexes=RelationGetIndexList(hostRelation);bool any=indexes!=NIL;list_free(indexes);
        if(any)ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native parent index constraints require their partition write adapter")));
        const LakeTxnTable* journal=table->journal?table->journal:table;
        int64 limit=LakeFileInlineLimit(binding,snapshot);
        if(limit<=0||journal->active_rows>limit)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native parent INSERT requires its positive bounded inline path; direct-file routing is not enabled")));
    }
    for(int source=0;source<LakeInlineReadSourceCount(binding);++source) {
        Oid sourceOid=LakeInlineReadSourceOid(binding,source);
        const LakeInlineSource* descriptor=binding->inline_sources_loaded?&binding->inline_sources[source]:NULL;
        Relation inlined = heap_open(sourceOid,AccessShareLock);
        LakeRequireNoUserActions(inlined);
        if (RelationGetDescr(inlined)->natts != (descriptor?descriptor->column_count:binding->column_count)+3)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake inline write layout is not supported")));
        LakeRequireColumn(inlined,"_ducklake_row_id",INT8OID);
        LakeRequireColumn(inlined,"_ducklake_begin_snapshot",INT8OID);
        LakeRequireColumn(inlined,"_ducklake_end_snapshot",INT8OID);
        for(int i=0;i<(descriptor?descriptor->column_count:binding->column_count);++i){
            const LakeInlineColumn* field=descriptor?&descriptor->columns[i]:&binding->columns[i];
            const char* name=descriptor?descriptor->column_names[i]:LakeInlinePhysicalName(field);
            LakeRequireColumn(inlined,name,LakeInlinePhysicalType(field->type));
            if(field->type==NUMERICOID&&RelationGetDescr(inlined)->attrs[get_attnum(sourceOid,name)-1].atttypmod!=field->typmod)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native numeric publication carrier parameters changed")));
        }
        heap_close(inlined,NoLock);
    }
    (void)LakeWriteRelation(binding,"ducklake_snapshot",snapshotColumns,lengthof(snapshotColumns),true);
    (void)LakeWriteRelation(binding,"ducklake_snapshot_changes",changesColumns,lengthof(changesColumns),true);
    (void)LakeWriteRelation(binding,"ducklake_table_stats",statsColumns,lengthof(statsColumns));
    (void)LakeWriteRelation(binding,"ducklake_table_column_stats",columnStatsColumns,lengthof(columnStatsColumns));
    if(binding->identity.kind==LAKE_TABLE_LOCAL) return; /* Checked raw CREATE shape; no permanent column definitions yet. */
    char* columns = LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_column");
    Relation columnRelation=heap_open(get_relname_relid("ducklake_column",binding->metadata_schema_oid),AccessShareLock);
    LakeRequireColumn(columnRelation,"nulls_allowed",BOOLOID);
    LakeRequireColumn(columnRelation,"default_value",TEXTOID);
    heap_close(columnRelation,NoLock);
    Oid types[2]={INT8OID,INT8OID}; Datum args[2]={Int64GetDatum(LakePermanentTableId(binding)),Int64GetDatum(binding->snapshot_id)};
    LakeMetaRows* defaults=LakeMetadataQuery(psprintf("SELECT nulls_allowed,default_value::text FROM %s "
        "WHERE table_id=$1 AND parent_column IS NULL AND begin_snapshot <= $2 AND (end_snapshot IS NULL OR end_snapshot>$2) ORDER BY column_order",columns),
        2,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    LakeMetadataRequireCount(defaults,binding->column_count,"write column definitions");
    for(uint32 i=0;i<defaults->count;++i) {
        bool nullAllowedIsNull=false, defaultIsNull=false;
        Datum nullable=LakeMetadataValue(defaults,i,1,BOOLOID,true,&nullAllowedIsNull);
        Datum value=LakeMetadataValue(defaults,i,2,TEXTOID,true,&defaultIsNull);
        const LakeInlineColumn* field=&binding->columns[i];
        const char* expected="NULL";
        if(binding->literal_defaults&&field->current_literal){
            if(!field->defaults_validated)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake write default is unvalidated")));
            expected=LakeCurrentDefaultMetadata(field->type,field->current_literal,NULL);
        }
        if(nullAllowedIsNull ||
           (binding->not_null_constraints?DatumGetBool(nullable)==field->not_null:!DatumGetBool(nullable)) ||
           (defaultIsNull?strcmp(expected,"NULL")!=0:strcmp(TextDatumGetCString(value),expected)!=0))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("native lake inline INSERT requires exactly admitted column constraints and defaults")));
    }
}

/* The existing shared changeset parser intentionally keeps its old closed
 * contract. Only the SQL0.17 RC row publisher admits this native layout-only
 * operation, after exact committed-source preparation and post-claim checks. */
static bool LakeSkipPublicationInlineFlush(const char** cursor)
{
    static const char prefix[]="inline_flush:";
    const char* p=*cursor;
    if(strncmp(p,prefix,sizeof(prefix)-1)!=0)return false;
    p+=sizeof(prefix)-1;
    if(*p<'0'||*p>'9')
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native inline-flush changeset identity")));
    uint64 id=0;
    do {
        unsigned digit=*p-'0';
        if(id>((uint64)PG_INT64_MAX-digit)/10)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native inline-flush changeset identity overflows")));
        id=id*10+digit;++p;
    } while(*p>='0'&&*p<='9');
    if(id>=(uint64)LAKE_REFERENCE_LOCAL_ID_START)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("reference-local inline-flush identity is unsupported")));
    if(*p!='\0') {
        if(*p!=','||p[1]=='\0')
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native inline-flush changeset separator")));
        ++p;
    }
    *cursor=p;return true;
}

static void LakeCheckConcurrentChanges(const LakeMetaRows* rows, const LakeTxnCatalog* catalog,
    bool allowNativeInlineChanges=false, bool allowNativeInlineFlush=false, bool allowFileInserts=false, bool allowFileLayout=false, bool allowFileDeletes=false,
    bool firstModifyBaselines=false)
{
    for(uint32 row=0;row<rows->count;++row) {
        int64 changeSnapshot=-1;
        if(firstModifyBaselines) {
            changeSnapshot=DatumGetInt64(LakeMetadataValue(rows,row,2,INT8OID));
            /* Caller has checked complete row count against current frontier;
             * this also verifies exact order/IDs, not a count-only shortcut. */
            if(catalog->read_snapshot<0||catalog->read_snapshot>PG_INT64_MAX-1-(int64)row||
               changeSnapshot!=catalog->read_snapshot+1+(int64)row)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake first modification history is not contiguous")));
        }
        bool isnull=false;Datum value=LakeMetadataValue(rows,row,1,TEXTOID,true,&isnull);
        if(isnull) continue;
        const char* cursor=TextDatumGetCString(value);
        LakePublicationChange change;
        while(*cursor!='\0') {
            if(allowNativeInlineFlush&&LakeSkipPublicationInlineFlush(&cursor))continue;
            if(!LakeNextPublicationChange(&cursor,&change))break;
            if(change.kind==LAKE_CHANGE_CREATED_TABLE) {
                for(const LakeTxnTable* table=catalog->tables;table!=NULL;table=table->next)
                    if(table->active && table->pending!=NULL &&
                       LakeIdentifierEquals(change.schema_name,table->pending->definition->schema_name) &&
                       LakeIdentifierEquals(change.table_name,table->pending->definition->table_name))
                        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake CREATE conflicts with a concurrent name")));
                continue; /* A different table CREATE cannot change an existing table's rows. */
            }
            if(change.kind==LAKE_CHANGE_ALTERED_TABLE) {
                for(const LakeTxnTable* table=catalog->tables;table!=NULL;table=table->next)
                    if(table->active && table->pending==NULL && LakePermanentTableId(table->binding)==change.table_id) {
                        if(firstModifyBaselines&&!OidIsValid(table->native_leaf_partition_oid)&&
                           table->binding->inline_sources_loaded) {
                            if(table->first_modify!=NULL) {
                                if(table->first_modify->snapshot_id!=table->binding->snapshot_id||
                                   table->first_modify->snapshot_id<catalog->read_snapshot)
                                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake first modification frontier changed")));
                                if(changeSnapshot<=table->first_modify->snapshot_id)continue;
                            } else if(LakeTxnPristineModifyRoot(table))continue;
                        }
                        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake write conflicts with a concurrent table schema change")));
                    }
                continue;
            }
            if(change.kind==LAKE_CHANGE_REWRITE_FILE||change.kind==LAKE_CHANGE_MERGE_FILE||change.kind==LAKE_CHANGE_FILE_DELETE){
                if(!allowFileLayout||(change.kind==LAKE_CHANGE_FILE_DELETE&&!allowFileDeletes))
                    ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake write conflicts with an unqualified file layout change")));
                /* Caller must validate selected sources or freshly resolve
                 * every exact committed R/birth origin before claiming. */
                continue;
            }
            if(change.kind==LAKE_CHANGE_FILE_INSERT&&allowFileInserts){
                /* A known append cannot end an existing logical version. RC
                 * origin checks still run; old SQL and flush stay closed. */
                if(!allowNativeInlineChanges)
                    for(const LakeTxnTable* table=catalog->tables;table;table=table->next)
                        if(table->active&&table->delete_attempts&&table->binding->identity.kind==LAKE_TABLE_PERMANENT&&
                           LakePermanentTableId(table->binding)==change.table_id)
                            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake write conflicts with concurrent file insertion")));
                continue;
            }
            if(change.kind!=LAKE_CHANGE_INLINED_INSERT &&
               !(allowNativeInlineChanges && change.kind==LAKE_CHANGE_INLINED_DELETE))
                ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake write conflicts with a concurrent catalog or data change")));
            /* The native RC publisher validates exact inline sources/file
             * positions after winning the snapshot PK. Flush additionally
             * carries post-cutoff ends to their prepared file positions. */
            if(allowNativeInlineChanges)continue;
            for(const LakeTxnTable* table=catalog->tables;table!=NULL;table=table->next)
                if(table->active && table->binding->identity.kind==LAKE_TABLE_PERMANENT &&
                   table->delete_attempts!=NULL && LakePermanentTableId(table->binding)==change.table_id)
                    ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake UPDATE or DELETE conflicts with a concurrent insert into the same table")));
        }
    }
}

static bool LakeTryStatsInteger(const char* text, Oid type, int64* value)
{
    bool negative=false;
    if(*text=='-' || *text=='+') { negative=*text=='-'; ++text; }
    if(*text<'0' || *text>'9') return false;
    uint64 limit=(uint64)PG_INT64_MAX+(negative?1:0), number=0;
    while(*text>='0' && *text<='9') {
        unsigned digit=*text-'0';
        if(number>(limit-digit)/10) return false;
        number=number*10+digit; ++text;
    }
    if(*text!='\0') return false;
    *value=negative?(number==(uint64)PG_INT64_MAX+1?PG_INT64_MIN:-(int64)number):(int64)number;
    if(type==INT2OID)return *value>=PG_INT16_MIN&&*value<=PG_INT16_MAX;
    return type!=INT4OID || (*value>=PG_INT32_MIN && *value<=PG_INT32_MAX);
}

bool LakeStatsMaintainBounds(Oid type)
{
    if(!LakeNativeTypeByOid(type)->maintain_bounds)return false;
    if(type==NUMERICOID)return LakeStatsExtendedVersionEnabled();
    return true;
}

static bool LakeTryNumericStatsCompare(int32 typmod, const char* left, const char* right, int* comparison)
{
    if(!LakeNumericTypmod(typmod) || left==NULL || right==NULL)return false;
    int precision=((typmod-VARHDRSZ)>>16)&0xFFFF;
    int scale=(typmod-VARHDRSZ)&0xFFFF;
    unsigned char a[16],b[16];
    if(!LakeDecimal128Encode(left,strlen(left),precision,scale,a) ||
       !LakeDecimal128Encode(right,strlen(right),precision,scale,b))return false;
    int signA=(a[15]&0x80)?1:0;
    int signB=(b[15]&0x80)?1:0;
    if(signA!=signB){*comparison=signA?-1:1;return true;}
    for(int i=15;i>=0;--i){
        if(a[i]!=b[i]){*comparison=a[i]<b[i]?-1:1;return true;}
    }
    *comparison=0;return true;
}

static bool LakeTryCompareStats(Oid type, const char* left, const char* right, int* comparison, int32 typmod=-1)
{
    if(type==UUIDOID){
        pg_uuid_t a,b;
        if(!LakeUuidTryStatsBytes(left,&a)||!LakeUuidTryStatsBytes(right,&b))return false;
        *comparison=memcmp(a.data,b.data,UUID_LEN);return true;
    }
    if(type==NUMERICOID)return LakeTryNumericStatsCompare(typmod,left,right,comparison);
    if(type==TEXTOID) { *comparison=strcmp(left,right); return true; }
    int64 a,b;
    if(type==BOOLOID) {
        if(strcmp(left,"true")==0) a=1;
        else if(strcmp(left,"false")==0) a=0;
        else return false;
        if(strcmp(right,"true")==0) b=1;
        else if(strcmp(right,"false")==0) b=0;
        else return false;
    } else if(!LakeTryStatsInteger(left,type,&a) || !LakeTryStatsInteger(right,type,&b)) return false;
    *comparison=a==b?0:(a<b?-1:1);
    return true;
}

static int LakeCompareStats(Oid type, const char* left, const char* right, int32 typmod=-1)
{
    int comparison=0;
    if(!LakeTryCompareStats(type,left,right,&comparison,typmod))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native lake statistics generated from input values")));
    return comparison;
}

/* Port of DuckLakeStats::MergeBound for admitted bounded scalar types.
 * A missing old bound remains unknown; a present non-exact VARCHAR/DECIMAL bound
 * is still useful and is preserved unless a more extreme exact batch wins. */
static char* LakeMergeStatsBound(Oid type, char* oldValue, char* batchValue,
    bool knownEmpty, bool isMin, bool oldExact, bool* exact, int32 typmod=-1)
{
    /* A write invalidates even externally supplied float bounds. Retaining
     * an old narrower range would permit unsafe pruning of new values. */
    if(!LakeStatsMaintainBounds(type)){*exact=false;return NULL;}
    if(knownEmpty) { *exact=true; return batchValue; }
    if(oldValue==NULL) return NULL;
    int comparison=0;
    if(!LakeTryCompareStats(type,oldValue,oldValue,&comparison,typmod)) return NULL;
    *exact=(type!=TEXTOID&&type!=NUMERICOID)||oldExact;
    if(batchValue==NULL) return oldValue; /* The added batch is entirely NULL. */
    if(!LakeTryCompareStats(type,batchValue,oldValue,&comparison,typmod)) return NULL;
    if(comparison!=0 && ((comparison<0)==isMin)) {
        *exact=true;
        return batchValue;
    }
    return oldValue; /* Equality ANDs with true, hence preserves old exactness. */
}

static char* LakeStatsValue(Oid type, Datum value, int32 typmod=-1)
{
    /* Bounds for floats remain unknown until SQL/NaN comparison equivalence
     * is qualified. Native ANALYZE still samples their typed values. */
    if(!LakeStatsMaintainBounds(type))return NULL;
    if(type==TEXTOID) return TextDatumGetCString(value);
    if(type==BOOLOID) return pstrdup(DatumGetBool(value)?"true":"false");
    if(type==UUIDOID)return DatumGetCString(DirectFunctionCall1(uuid_out,value));
    if(type==NUMERICOID){
        if(!LakeNumericTypmod(typmod))return NULL;
        char formatted[64];
        LakeNumericFormatCanonical(value,typmod,formatted,sizeof(formatted));
        return pstrdup(formatted);
    }
    if(type==INT2OID) return psprintf("%d",(int)DatumGetInt16(value));
    if(type==INT4OID) return psprintf("%d",DatumGetInt32(value));
    return psprintf("%lld",(long long)DatumGetInt64(value));
}

void LakeObserveColumnBounds(MemoryContext targetContext, Oid type, int32 typmod,
    Datum value, char** minimum, char** maximum)
{
    char* formatted=LakeStatsValue(type,value,typmod);
    if(formatted==NULL)return;
    if(*minimum==NULL || LakeCompareStats(type,formatted,*minimum,typmod)<0){
        char* nextMin=MemoryContextStrdup(targetContext,formatted);
        if(*minimum!=NULL)pfree(*minimum);
        *minimum=nextMin;
    }
    if(*maximum==NULL || LakeCompareStats(type,formatted,*maximum,typmod)>0){
        char* nextMax=MemoryContextStrdup(targetContext,formatted);
        if(*maximum!=NULL)pfree(*maximum);
        *maximum=nextMax;
    }
    pfree(formatted);
}

static void LakeInsertFileColumnStats(Oid ns, int64 fileId, int64 tableId,
    const LakeInlineColumn* column, int64 columnBytes, int64 rowCount, int64 nullCount,
    const char* minValue, const char* maxValue, Snapshot snapshot, const char* errContext)
{
    const char* nan=(column->type==FLOAT4OID||column->type==FLOAT8OID)?"NULL":"false";
    Oid ct[8]={INT8OID,INT8OID,INT8OID,INT8OID,INT8OID,INT8OID,TEXTOID,TEXTOID};
    Datum ca[8]={Int64GetDatum(fileId),Int64GetDatum(tableId),Int64GetDatum(column->column_id),
        Int64GetDatum(columnBytes),Int64GetDatum(rowCount-nullCount),Int64GetDatum(nullCount),
        minValue?CStringGetTextDatum(minValue):(Datum)0,maxValue?CStringGetTextDatum(maxValue):(Datum)0};
    char cn[9]={' ',' ',' ',' ',' ',' ',minValue?' ':'n',maxValue?' ':'n','\0'};
    LakeMetaRows* result=LakeMetadataQuery(
        psprintf("INSERT INTO %s(data_file_id,table_id,column_id,column_size_bytes,value_count,"
            "null_count,min_value,max_value,contains_nan,extra_stats,min_is_exact,max_is_exact) "
            "VALUES($1,$2,$3,$4,$5,$6,$7,$8,%s,NULL,%s,%s)",
            LakeMetadataRelation(ns,"ducklake_file_column_stats"),
            nan,minValue?"true":"NULL",maxValue?"true":"NULL"),
        8,ct,ca,cn,snapshot,false,SPI_OK_INSERT,ns);
    LakeMetadataRequireCount(result,1,errContext);
}

/* ResolveNullOnlyBounds in the fixed format-1.1 reference inspects the
 * predecessor's live data before writing this commit's new/deleted versions.
 * We publish end markers and new rows earlier, so reconstruct that logical
 * visibility explicitly: include end=commitSnapshot, exclude begin=commitSnapshot.
 * With file inputs we cannot prove all-NULL from the inline relation alone.
 * Preserve unknown bounds rather than scan/decode files while holding the
 * successful snapshot claim. This can defer bound recovery for all-NULL files,
 * but cannot invent a narrower range which prunes surviving file values.
 * No result is cached across publication attempts or transactions. */
static bool LakePredecessorMayHaveValues(LakeInlineBinding* binding, const char* carrier,
    const char* columnName, Snapshot snapshot, int64 commitSnapshot)
{
    Assert(commitSnapshot > 0);
    if(binding->file_count>0||LakeInlineReadSourceCount(binding)>1) return true;
    Oid type=INT8OID;
    Datum cutoff=Int64GetDatum(commitSnapshot-1);
    LakeMetaRows* values=LakeMetadataQuery(psprintf("SELECT EXISTS(SELECT 1 FROM %s "
        "WHERE _ducklake_begin_snapshot<=$1 AND "
        "(_ducklake_end_snapshot IS NULL OR _ducklake_end_snapshot>$1) AND %s IS NOT NULL)",
        carrier,quote_identifier(columnName)),1,&type,&cutoff,NULL,snapshot,true,SPI_OK_SELECT,
        binding->metadata_schema_oid);
    LakeMetadataRequireCount(values,1,"predecessor inline column value check");
    return DatumGetBool(LakeMetadataValue(values,0,1,BOOLOID));
}

static int64 LakeNewIdentityCount(const LakeTxnTable* table)
{
    int64 count = 0;
    for (LakeTxnIdentity* identity = table->identities; identity != NULL; identity = identity->next)
        if (!identity->committed_origin && LakeTxnRowHasPayload(identity->head)) ++count;
    return count;
}

static int64 LakeEndedOriginCount(const LakeTxnTable* table)
{
    int64 count = 0;
    for (LakeTxnIdentity* identity = table->identities; identity != NULL; identity = identity->next)
        if (identity->committed_origin && identity->head != NULL) ++count;
    return count;
}

/* A format-1.1 CREATE may leave an empty inline carrier without any global
 * statistics. Only a successful snapshot claim may initialize its allocator.
 * Check all MVCC-visible stored versions and file records, not just rows/files
 * visible at S_read. Missing statistics in a used table are not a zero frontier.
 * This proof is attempt-local and is repeated after a failed claim's rollback. */
static void LakeValidateUninitializedInlineTable(LakeTxnTable* table, Snapshot snapshot,
    const char* carrier, const char* columnStats, int64 newIdentities)
{
    LakeInlineBinding* binding = table->binding;
    Oid ns = binding->metadata_schema_oid;
    if (table->active_rows <= 0 || newIdentities != table->active_rows || LakeEndedOriginCount(table) != 0)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("native lake cannot initialize missing statistics for existing row identities")));

    Oid type = INT8OID;
    Datum argument = Int64GetDatum(LakePermanentTableId(binding));
    char* registry = LakeMetadataRelation(ns, "ducklake_inlined_data_tables");
    LakeMetaRows* carriers = LakeMetadataQuery(psprintf(
        "SELECT table_name::text,schema_version FROM %s WHERE table_id=$1", registry),
        1, &type, &argument, NULL, snapshot, true, SPI_OK_SELECT, ns);
    if(binding->inline_sources_loaded){
        LakeValidateInlineSources(binding);
        if(carriers->count!=(uint32)binding->inline_source_count||carriers->count<1)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake carrier registry changed during statistics initialization")));
        bool* seen=(bool*)palloc0(sizeof(bool)*binding->inline_source_count);
        for(uint32 r=0;r<carriers->count;++r){
            const char* name=LakeMetadataText(carriers,r,1);
            int64 version=DatumGetInt64(LakeMetadataValue(carriers,r,2,INT8OID));
            Oid oid=get_relname_relid(name,ns);int found=-1;
            for(int i=0;i<binding->inline_source_count;++i)
                if(binding->inline_sources[i].relation_oid==oid)found=i;
            if(found<0||seen[found]||binding->inline_sources[found].schema_version!=version||
               strcmp(binding->inline_sources[found].table_name,name)!=0)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("ambiguous historical carrier during statistics initialization")));
            seen[found]=true;
            LakeMetaRows* rows=LakeMetadataQuery(psprintf("SELECT EXISTS(SELECT 1 FROM %s)",
                LakeMetadataRelation(ns,name)),0,NULL,NULL,NULL,snapshot,true,SPI_OK_SELECT,ns);
            LakeMetadataRequireCount(rows,1,"uninitialized historical carrier checks");
            if(DatumGetBool(LakeMetadataValue(rows,0,1,BOOLOID)))
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake cannot initialize missing statistics for a nonempty inline carrier")));
        }
        pfree(seen);
    }else{
    if (carriers->count != 1)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake statistics initialization requires exactly one empty inline carrier")));
    const char* name = LakeMetadataText(carriers, 0, 1);
    int64 schema = DatumGetInt64(LakeMetadataValue(carriers, 0, 2, INT8OID));
    if (get_relname_relid(name, ns) != binding->inline_oid || schema < 0 || schema > binding->schema_version)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("native lake inline carrier changed during statistics initialization")));

    LakeMetaRows* rows = LakeMetadataQuery(psprintf("SELECT EXISTS(SELECT 1 FROM %s)", carrier),
        0, NULL, NULL, NULL, snapshot, true, SPI_OK_SELECT, ns);
    LakeMetadataRequireCount(rows, 1, "uninitialized inline carrier checks");
    if (DatumGetBool(LakeMetadataValue(rows, 0, 1, BOOLOID)))
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("native lake cannot initialize missing statistics for a nonempty inline carrier")));

    }

    const char* relations[] = {LakeMetadataRelation(ns, "ducklake_data_file"),
        LakeMetadataRelation(ns, "ducklake_delete_file"), columnStats};
    for (Size i = 0; i < lengthof(relations); ++i) {
        LakeMetaRows* records = LakeMetadataQuery(psprintf(
            "SELECT EXISTS(SELECT 1 FROM %s WHERE table_id=$1)", relations[i]),
            1, &type, &argument, NULL, snapshot, true, SPI_OK_SELECT, ns);
        LakeMetadataRequireCount(records, 1, "uninitialized table history checks");
        if (DatumGetBool(LakeMetadataValue(records, 0, 1, BOOLOID)))
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("native lake cannot initialize missing statistics with file history or existing column statistics")));
    }
}

/* Fixed reference UpdateGlobalTableStatsSql inserts column rows only for a
 * previously uninitialized table. All bounds here cover the complete first
 * surviving batch; an all-NULL column has NULL bounds and NULL exactness. */
static void LakeInsertInitialColumnStats(LakeInlineBinding* binding, const char* columnStats,
    Snapshot snapshot, char* const* batchMin, char* const* batchMax, const bool* batchNull)
{
    Oid types[7] = {INT8OID, INT8OID, BOOLOID, TEXTOID, TEXTOID, BOOLOID, BOOLOID};
    char* sql = psprintf("INSERT INTO %s (table_id,column_id,contains_null,contains_nan,"
        "min_value,max_value,extra_stats,min_is_exact,max_is_exact) "
        "VALUES($1,$2,$3,NULL,$4,$5,NULL,$6,$7)", columnStats);
    for (int i = 0; i < binding->column_count; ++i) {
        CHECK_FOR_INTERRUPTS();
        Datum args[7] = {Int64GetDatum(LakePermanentTableId(binding)), Int64GetDatum(binding->columns[i].column_id),
            BoolGetDatum(batchNull[i]), batchMin[i] ? CStringGetTextDatum(batchMin[i]) : (Datum)0,
            batchMax[i] ? CStringGetTextDatum(batchMax[i]) : (Datum)0, BoolGetDatum(true), BoolGetDatum(true)};
        char nulls[8] = {' ', ' ', ' ', batchMin[i] ? ' ' : 'n', batchMax[i] ? ' ' : 'n',
            batchMin[i] ? ' ' : 'n', batchMax[i] ? ' ' : 'n', '\0'};
        LakeMetaRows* inserted = LakeMetadataQuery(sql, lengthof(types), types, args, nulls,
            snapshot, false, SPI_OK_INSERT, binding->metadata_schema_oid);
        LakeMetadataRequireCount(inserted, 1, "initialized column statistics");
    }
}

static void LakeAdvancePublicationCid(Snapshot snapshot)
{
    /* Never mutate a borrowed business snapshot. SQL0.10 publication pushes
     * its own copy before creating or looking up any new carrier. */
    if(!ActiveSnapshotSet()||GetActiveSnapshot()!=snapshot)
        ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("native lake carrier publication needs an owned active snapshot")));
    CommandCounterIncrement();UpdateActiveSnapshotCommandId();
}

/* A registration names the TABLE schema, not the catalog-wide frontier.
 * Pinned DuckLake GetTableSchemaVersions uses visible per-table versions;
 * GetInlinedTableColumnsSql/GetBeginSnapshotForSchemaVersion fall back to
 * the table's first snapshot only for a table without an alteration version.
 * Prove the entire flattened physical schema before using that fallback. */
static int64 LakeFirstInlineSchemaVersion(const LakeInlineBinding* binding,Snapshot snapshot)
{
    Oid ns=binding->metadata_schema_oid;
    Oid types[2]={INT8OID,INT8OID};
    Datum args[2]={Int64GetDatum(LakePermanentTableId(binding)),Int64GetDatum(binding->snapshot_id)};
    const char* versionTable=LakeDefinitionRelation(ns,"ducklake_schema_versions");
    LakeMetaRows* versions=LakeMetadataQuery(psprintf("SELECT schema_version,begin_snapshot FROM %s WHERE table_id=$1 "
        "AND schema_version=(SELECT max(schema_version) FROM %s WHERE table_id=$1 AND begin_snapshot<=$2)",
        versionTable,versionTable),2,types,args,NULL,snapshot,true,SPI_OK_SELECT,ns);
    if(versions->count>1)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("ambiguous first inline schema version")));
    int64 version=0,origin=0;
    if(versions->count==1){
        version=DatumGetInt64(LakeMetadataValue(versions,0,1,INT8OID));
        origin=DatumGetInt64(LakeMetadataValue(versions,0,2,INT8OID));
    }else{
        LakeMetaRows* created=LakeMetadataQuery(psprintf("SELECT min(begin_snapshot) FROM %s WHERE table_id=$1",
            LakeDefinitionRelation(ns,"ducklake_table")),1,types,args,NULL,snapshot,true,SPI_OK_SELECT,ns);
        LakeMetadataRequireCount(created,1,"first inline table origin");
        origin=DatumGetInt64(LakeMetadataValue(created,0,1,INT8OID));
    }
    if(version<0||version>binding->schema_version||origin<0||origin>binding->snapshot_id)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid first inline table schema origin")));
    const char* columns=LakeDefinitionRelation(ns,"ducklake_column");
    LakeMetaRows* current=LakeMetadataQuery(psprintf("SELECT max(begin_snapshot),count(*) FROM %s "
        "WHERE table_id=$1 AND begin_snapshot<=$2 AND(end_snapshot IS NULL OR end_snapshot>$2)",columns),
        2,types,args,NULL,snapshot,true,SPI_OK_SELECT,ns);
    LakeMetadataRequireCount(current,1,"first inline current columns");
    int64 latest=DatumGetInt64(LakeMetadataValue(current,0,1,INT8OID));
    int64 count=DatumGetInt64(LakeMetadataValue(current,0,2,INT8OID));
    int64 expected=binding->column_count;
    for(int c=0;c<binding->column_count;++c)if(binding->columns[c].element_column_id>=0)++expected;
    Datum historical[2]={args[0],Int64GetDatum(origin)};
    LakeMetaRows* source=LakeMetadataQuery(psprintf("SELECT count(*) FROM %s WHERE table_id=$1 "
        "AND begin_snapshot<=$2 AND(end_snapshot IS NULL OR end_snapshot>$2)",columns),
        2,types,historical,NULL,snapshot,true,SPI_OK_SELECT,ns);
    LakeMetadataRequireCount(source,1,"first inline historical columns");
    /* Every current row is visible at origin iff its begin<=origin; its end
     * is already >S (or NULL). Equal counts then exclude extra old columns.
     * This proves identical field IDs/names/types/parents/order without an
     * arbitrary missing-version guess or a second type interpretation. */
    if(count!=expected||latest<0||latest>origin||
       DatumGetInt64(LakeMetadataValue(source,0,1,INT8OID))!=count)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("first inline carrier has no matching visible table schema version")));
    LakeMetaRows* roots=LakeMetadataQuery(psprintf("SELECT column_id,column_name::text FROM %s "
        "WHERE table_id=$1 AND parent_column IS NULL AND begin_snapshot<=$2 "
        "AND(end_snapshot IS NULL OR end_snapshot>$2) ORDER BY column_order",columns),
        2,types,historical,NULL,snapshot,true,SPI_OK_SELECT,ns);
    LakeMetadataRequireCount(roots,binding->column_count,"first inline root mapping");
    for(int c=0;c<binding->column_count;++c)
        if(DatumGetInt64(LakeMetadataValue(roots,c,1,INT8OID))!=binding->columns[c].column_id ||
           strcmp(LakeMetadataText(roots,c,2),binding->columns[c].name)!=0)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("first inline physical columns do not match their table schema")));
    return version;
}

static void LakePublishEnsureInlineCarrier(LakeInlineBinding* binding,Snapshot snapshot)
{
    if(OidIsValid(binding->inline_oid))return;
    if(!LakeFileModificationsEnabled()||!OidIsValid(LakeMetadataOwner()))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake first inline carrier requires extension 0.10 and private owner")));
    const int64 carrierSchema=LakeFirstInlineSchemaVersion(binding,snapshot);
    if(binding->inline_sources_loaded){
        LakeValidateInlineSources(binding);
        if(binding->inline_write_index!=-1||carrierSchema!=binding->inline_write_schema_version)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake current inline schema differs before carrier creation")));
        if(binding->inline_source_count>=LAKE_INLINE_SOURCE_LIMIT)
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake inline source descriptor budget exceeded")));
    }
    char* compact=pstrdup(binding->table_uuid);char* write=compact;
    for(char* read=compact;*read;++read)if(*read!='-')*write++=*read;
    *write='\0';
    if(strlen(compact)!=32)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake inline carrier has no valid table UUID")));
    char* name=binding->inline_sources_loaded?
        psprintf("ducklake_inlined_data_" INT64_FORMAT "_" INT64_FORMAT,LakePermanentTableId(binding),carrierSchema):
        psprintf("ducklake_inlined_%s",compact);
    if(strlen(name)>=NAMEDATALEN)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake current inline carrier name exceeds identifier limit")));
    Oid oid=LakeCreatePrivateCarrier(binding,name,LakeMetadataOwner(),false);
    LakeAdvancePublicationCid(snapshot);
    static const LakeWriteColumn shape[]={{"table_id",INT8OID},{"table_name",TEXTOID},{"schema_version",INT8OID}};
    char* registrations=LakeWriteRelation(binding,"ducklake_inlined_data_tables",shape,lengthof(shape));
    Oid types[3]={INT8OID,TEXTOID,INT8OID};
    Datum args[3]={Int64GetDatum(LakePermanentTableId(binding)),CStringGetTextDatum(name),Int64GetDatum(carrierSchema)};
    LakeMetaRows* written=LakeMetadataQuery(psprintf("INSERT INTO %s(table_id,table_name,schema_version) VALUES($1,$2,$3)",registrations),
        3,types,args,NULL,snapshot,false,SPI_OK_INSERT,binding->metadata_schema_oid);
    LakeMetadataRequireCount(written,1,"first inline carrier registration");
    /* This binding is an attempt-local copy. A failed attempt must not leave
     * a rolled-back relation OID in the transaction's durable journal. */
    binding->inline_oid=oid;
    if(binding->inline_sources_loaded){
        int index=binding->inline_source_count;
        LakeReserveColumnMapBytes(binding,sizeof(LakeInlineSource)+NAMEDATALEN+
            (Size)binding->column_count*(sizeof(LakeInlineColumn)+sizeof(char*)+sizeof(int)+2*NAMEDATALEN));
        Size bytes=sizeof(LakeInlineSource)*(index+1);
        binding->inline_sources=binding->inline_sources?(LakeInlineSource*)repalloc(binding->inline_sources,bytes):
            (LakeInlineSource*)palloc0(bytes);
        LakeInlineSource* item=&binding->inline_sources[index];MemSet(item,0,sizeof(*item));
        item->relation_oid=oid;item->table_name=pstrdup(name);item->schema_version=carrierSchema;
        item->origin_snapshot=binding->inline_write_origin_snapshot;item->column_count=binding->column_count;
        item->columns=LakeCopyInlineColumns(binding->columns,item->column_count,binding);
        item->logical_to_source=(int*)palloc(sizeof(int)*binding->column_count);
        item->column_names=(char**)palloc0(sizeof(char*)*item->column_count);
        for(int c=0;c<item->column_count;++c){
            item->logical_to_source[c]=c;item->column_names[c]=pstrdup(binding->columns[c].name);
            item->columns[c].inline_name=NULL;
        }
        item->partition=LakePartitionCopySpec(&binding->partition,CurrentMemoryContext);
        ++binding->inline_source_count;LakeSetInlineWriteSource(binding,index);
    }
    LakeAdvancePublicationCid(snapshot);
}

static void LakePublishFileDeletion(LakeInlineBinding* binding,const LakeRowLocator* origin,
    Snapshot snapshot,int64 commitSnapshot)
{
    static const LakeWriteColumn shape[]={{"file_id",INT8OID},{"row_id",INT8OID},{"begin_snapshot",INT8OID}};
    Oid types[3]={INT8OID,INT8OID,INT8OID};
    Datum fileArgs[3]={Int64GetDatum(LakePermanentTableId(binding)),Int64GetDatum((int64)origin->file_id),Int64GetDatum(commitSnapshot-1)};
    char* data=LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_data_file");
    LakeMetaRows* live=LakeMetadataQuery(psprintf("SELECT record_count FROM %s WHERE table_id=$1 AND data_file_id=$2 "
        "AND begin_snapshot<=$3 AND (end_snapshot IS NULL OR end_snapshot>$3)",data),
        3,types,fileArgs,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    if(live->count!=1)
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake source file was retired before publication")));
    int64 count=DatumGetInt64(LakeMetadataValue(live,0,1,INT8OID));
    if(count<0||origin->file_row_position>=(uint64)count||origin->begin_snapshot>=(uint64)commitSnapshot)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake source file version is outside its physical range")));
    char* name=psprintf("ducklake_inlined_delete_%lld",(long long)LakePermanentTableId(binding));
    if(!OidIsValid(get_relname_relid(name,binding->metadata_schema_oid))) {
        (void)LakeCreatePrivateCarrier(binding,name,LakeMetadataOwner(),true);
        LakeAdvancePublicationCid(snapshot);
    }
    char* deletes=LakeWriteRelation(binding,name,shape,lengthof(shape));
    Datum args[3]={Int64GetDatum((int64)origin->file_id),Int64GetDatum((int64)origin->file_row_position),Int64GetDatum(commitSnapshot)};
    LakeMetaRows* existing=LakeMetadataQuery(psprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE file_id=$1 AND row_id=$2 AND begin_snapshot<$3)",deletes),
        3,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    LakeMetadataRequireCount(existing,1,"existing file position deletion");
    if(DatumGetBool(LakeMetadataValue(existing,0,1,BOOLOID)))
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake file position was concurrently deleted")));
    LakeMetaRows* written=LakeMetadataQuery(psprintf("INSERT INTO %s(file_id,row_id,begin_snapshot) VALUES($1,$2,$3)",deletes),
        3,types,args,NULL,snapshot,false,SPI_OK_INSERT,binding->metadata_schema_oid);
    LakeMetadataRequireCount(written,1,"file position deletion");LakeAdvancePublicationCid(snapshot);
}

/* Immutable identity-free INSERT payloads for an eligible permanent table.
 * Created outside publication subtransactions; retries only change metadata. */
struct LakePreparedInsert {
    LakeFlushOutput output;
    int64 rows;
    int64 inline_limit;
    char* path_key;
    char** minima;
    char** maxima;
    char** file_minima;
    char** file_maxima;
    bool* has_null;
    int64* null_counts;
    /* One immutable payload per complete current-spec tuple. Only the head
     * owns aggregate table minima/maxima/has_null; null_counts are per file. */
    bool grouped;
    LakePartitionTuple key;
    LakePreparedInsert* next;
};

static int LakeInsertFileCount(const LakePreparedInsert* files)
{
    int count=0;
    for(const LakePreparedInsert* f=files;f;f=f->next){
        if(++count>LAKE_DIRECT_INSERT_GROUP_LIMIT)
            ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("direct INSERT exceeds its file group budget")));
    }
    return count;
}

static int64 LakeInsertFileRows(const LakePreparedInsert* files)
{
    int64 rows=0;
    for(const LakePreparedInsert* f=files;f;f=f->next){
        if(f->rows<=0||f->rows>PG_INT64_MAX-rows)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid direct INSERT group row count")));
        rows+=f->rows;
    }
    return rows;
}

static int64 LakeInsertFileBytes(const LakePreparedInsert* files)
{
    int64 bytes=0;
    for(const LakePreparedInsert* f=files;f;f=f->next){
        if(!f->output.candidate||f->output.candidate->written.file_bytes<0||
           f->output.candidate->written.file_bytes>PG_INT64_MAX-bytes)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid direct INSERT group byte count")));
        bytes+=f->output.candidate->written.file_bytes;
    }
    return bytes;
}

static char* LakeInsertPathKey(LakeInlineBinding* binding,Snapshot snapshot)
{
    Oid types[2]={INT8OID,INT8OID};
    Datum args[2]={Int64GetDatum(LakePermanentTableId(binding)),Int64GetDatum(binding->snapshot_id)};
    LakeMetaRows* paths=LakeMetadataQuery(psprintf("SELECT s.path::text,s.path_is_relative,t.path::text,t.path_is_relative,"
        "s.parent_schema_id FROM %s t JOIN %s s ON s.schema_id=t.schema_id WHERE t.table_id=$1 AND "
        "t.begin_snapshot<=$2 AND(t.end_snapshot IS NULL OR t.end_snapshot>$2) AND s.begin_snapshot<=$2 AND "
        "(s.end_snapshot IS NULL OR s.end_snapshot>$2)",LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_table"),
        LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_schema")),2,types,args,NULL,snapshot,true,SPI_OK_SELECT,
        binding->metadata_schema_oid);
    LakeMetadataRequireCount(paths,1,"direct INSERT path binding");bool parentNull=false;
    LakeMetadataValue(paths,0,5,INT8OID,true,&parentNull);
    if(!parentNull||!DatumGetBool(LakeMetadataValue(paths,0,2,BOOLOID))||!DatumGetBool(LakeMetadataValue(paths,0,4,BOOLOID)))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("direct INSERT requires local unnested relative paths")));
    const char* schema=LakeMetadataText(paths,0,1);const char* table=LakeMetadataText(paths,0,3);
    return psprintf("%zu:%s%zu:%s",strlen(schema),schema,strlen(table),table);
}

static void LakeCheckInsertBinding(const LakeInlineBinding* old,const LakeInlineBinding* now)
{
    if(!LakeColumnLayoutsEqual(old,now)||!LakeIdentityPartitionSpecEqual(&old->partition,&now->partition))
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("direct INSERT partition binding changed")));
    if(now->read_only||LakePermanentTableId(old)!=LakePermanentTableId(now)||
       old->metadata_schema_oid!=now->metadata_schema_oid||strcmp(old->table_uuid,now->table_uuid)||
       old->column_count!=now->column_count||!old->data_root||!now->data_root||strcmp(old->data_root,now->data_root))
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("direct INSERT binding changed before publication")));
    for(int c=0;c<old->column_count;++c)
        if(old->columns[c].column_id!=now->columns[c].column_id||old->columns[c].type!=now->columns[c].type||
           old->columns[c].typmod!=now->columns[c].typmod||
           !LakeColumnDefaultsEqual(&old->columns[c],&now->columns[c])||
           old->columns[c].element_column_id!=now->columns[c].element_column_id||strcmp(old->columns[c].name,now->columns[c].name))
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("direct INSERT column binding changed before publication")));
}

static LakePreparedInsert* LakePreparePartitionInsertFiles(LakeTxnTable* table,LakeInlineBinding* now,
    Snapshot snapshot,MemoryContext parent,int64 inlineLimit)
{
    if(!LakePartitionDirectInsertEnabled()||!now->inline_sources_loaded||!now->partition.enabled||
       now->partition.key_count<=0||!LakeDirectInsertEligible(table))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("partitioned direct INSERT is not admitted")));
    MemoryContext caller=MemoryContextSwitchTo(parent);
    int columns=now->column_count;
    bool collectFileBounds=LakeStatsExtendedVersionEnabled();
    int slots=Min(LAKE_DIRECT_INSERT_GROUP_LIMIT,LakeFileCandidateSlotsAvailable());
    LakePreparedInsert* head=NULL;LakePreparedInsert* tail=NULL;
    int groups=0;
    Datum* values=(Datum*)palloc(sizeof(Datum)*columns);
    bool* nulls=(bool*)palloc(sizeof(bool)*columns);
    int* attrs=(int*)palloc(sizeof(int)*columns);
    for(int c=0;c<columns;++c)attrs[c]=c;
    /* Discover the complete final write set before creating any SDK output.
     * Unlike maintenance this operation may never stop after a prefix. */
    for(LakeTxnRow* row=table->rows;row;row=row->next){
        if(!LakeTxnRowHasPayload(row)||row->identity->head!=row)continue;
        CHECK_FOR_INTERRUPTS();
        if(row->identity->committed_origin)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("partitioned direct INSERT contains an old logical row")));
        HeapTuple payload=LakeTxnMaterializeRow(row);
        LakeGatherHostTuple(table->binding,table->descriptor,payload,values,nulls);
        LakePreparedInsert* group=head;
        while(group&&!LakePartitionTupleMatches(&now->partition,&group->key,values,nulls,attrs))group=group->next;
        if(!group){
            if(groups>=slots)
                ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("partitioned direct INSERT exceeds available candidate files")));
            group=(LakePreparedInsert*)palloc0(sizeof(LakePreparedInsert));
            group->grouped=true;
            group->key=LakePartitionFormTuple(&now->partition,values,nulls,attrs,parent);
            group->null_counts=(int64*)palloc0(sizeof(int64)*columns);
            group->file_minima=(char**)palloc0(sizeof(char*)*columns);
            group->file_maxima=(char**)palloc0(sizeof(char*)*columns);
            if(tail)tail->next=group;else{
                head=group;head->inline_limit=inlineLimit;head->path_key=LakeInsertPathKey(now,snapshot);
                head->minima=(char**)palloc0(sizeof(char*)*columns);
                head->maxima=(char**)palloc0(sizeof(char*)*columns);
                head->has_null=(bool*)palloc0(sizeof(bool)*columns);
            }
            tail=group;++groups;
        }
        if(group->rows==PG_INT64_MAX)
            ereport(ERROR,(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),errmsg("direct INSERT group count overflow")));
        ++group->rows;
        for(int c=0;c<columns;++c){
            if(nulls[c]){head->has_null[c]=true;++group->null_counts[c];continue;}
            LakeObserveColumnBounds(parent,now->columns[c].type,now->columns[c].typmod,values[c],&head->minima[c],&head->maxima[c]);
            if(collectFileBounds)
                LakeObserveColumnBounds(parent,now->columns[c].type,now->columns[c].typmod,values[c],&group->file_minima[c],&group->file_maxima[c]);
        }
        heap_freetuple(payload);
    }
    if(!head||LakeInsertFileRows(head)!=table->active_rows||table->active_rows!=LakeNewIdentityCount(table))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("partitioned direct INSERT survivor set differs")));
    char* directory=LakePrepareFlushDirectory(now,snapshot);
    OgLakeSdkField* fields=(OgLakeSdkField*)palloc0(sizeof(OgLakeSdkField)*columns);
    for(int c=0;c<columns;++c){
        LakeNativeTypeRequireSdk(now->columns[c].type,LakeFileProvider());
        fields[c]={now->columns[c].column_id,LakeNativeTypeByOid(now->columns[c].type)->sdk_type,1,now->columns[c].name};
    }
    /* At most one builder and SDK writer is live. Each group's physical order
     * follows the surviving private journal; IDs are assigned only at commit. */
    for(LakePreparedInsert* group=head;group;group=group->next){
        CHECK_FOR_INTERRUPTS();
        char* uuid=DatumGetCString(DirectFunctionCall1(uuid_out,OidFunctionCall0(F_GEN_RANDOM_UUID)));
        group->output.relative_path=psprintf("og_lake_insert_%s.parquet",uuid);
        char* absolute=psprintf("%s/%s",directory,group->output.relative_path);
        group->output.candidate=LakeFlushOpen(absolute,fields,columns,0,NULL,now);
        LakeArrowBuilder* batch=LakeArrowBuilderCreate(parent,fields,columns,512,16*1024*1024,LakeFlushTypmods(now,columns));
        int64 written=0;
        for(LakeTxnRow* row=table->rows;row;row=row->next){
            if(!LakeTxnRowHasPayload(row)||row->identity->head!=row)continue;
            CHECK_FOR_INTERRUPTS();HeapTuple payload=LakeTxnMaterializeRow(row);
            LakeGatherHostTuple(table->binding,table->descriptor,payload,values,nulls);
            if(!LakePartitionTupleMatches(&now->partition,&group->key,values,nulls,attrs)){
                heap_freetuple(payload);continue;
            }
            LakeFlushAppend(batch,group->output.candidate,values,nulls);++written;
            heap_freetuple(payload);
        }
        LakeArrowBuilderWrite(batch,group->output.candidate->api,group->output.candidate->writer);
        LakeArrowBuilderDestroy(batch);
        LakeFlushOperation validation={};validation.context=parent;validation.table=table;
        LakeFlushFinish(&validation,&group->output,fields,columns);
        if(written!=group->rows||group->output.candidate->written.num_rows!=group->rows)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("partitioned direct INSERT physical count differs")));
    }
    MemoryContextSwitchTo(caller);return head;
}

static LakePreparedInsert* LakePrepareInsertFile(LakeTxnTable* table,Snapshot snapshot,MemoryContext parent)
{
    if(!LakeDirectInsertEligible(table)||table->active_rows==0)return NULL;
    LakeInlineBinding* now=LakeLoadCommittedInlineBinding(table->binding->relation_oid,snapshot);
    LakeCheckInsertBinding(table->binding,now);LakeCheckWriteOptions(now,snapshot);
    int64 inlineLimit=LakeFileInlineLimit(now,snapshot);
    if(table->active_rows<=inlineLimit)return NULL;
    if(now->partition.enabled&&now->partition.key_count>0)
        return LakePreparePartitionInsertFiles(table,now,snapshot,parent,inlineLimit);
    MemoryContext caller=MemoryContextSwitchTo(parent);
    LakePreparedInsert* file=(LakePreparedInsert*)palloc0(sizeof(LakePreparedInsert));
    file->inline_limit=inlineLimit;file->path_key=LakeInsertPathKey(now,snapshot);
    char* directory=LakePrepareFlushDirectory(now,snapshot);
    char* uuid=DatumGetCString(DirectFunctionCall1(uuid_out,OidFunctionCall0(F_GEN_RANDOM_UUID)));
    file->output.relative_path=psprintf("og_lake_insert_%s.parquet",uuid);
    char* absolute=psprintf("%s/%s",directory,file->output.relative_path);
    int columns=now->column_count;
    OgLakeSdkField* fields=(OgLakeSdkField*)palloc0(sizeof(OgLakeSdkField)*columns);
    file->minima=(char**)palloc0(sizeof(char*)*columns);file->maxima=(char**)palloc0(sizeof(char*)*columns);
    file->file_minima=file->minima;file->file_maxima=file->maxima;
    file->has_null=(bool*)palloc0(sizeof(bool)*columns);file->null_counts=(int64*)palloc0(sizeof(int64)*columns);
    for(int c=0;c<columns;++c){LakeNativeTypeRequireSdk(now->columns[c].type,LakeFileProvider());
        fields[c]={now->columns[c].column_id,LakeNativeTypeByOid(now->columns[c].type)->sdk_type,1,now->columns[c].name};}
    /* Neither reserved row_id nor snapshot columns are written. Permanent IDs
     * and birth remain unknown until a successful catalog claim. */
    file->output.candidate=LakeFlushOpen(absolute,fields,columns,0,NULL,now);
    LakeArrowBuilder* batch=LakeArrowBuilderCreate(parent,fields,columns,512,16*1024*1024,LakeFlushTypmods(now,columns));
    Datum* values=(Datum*)palloc(sizeof(Datum)*columns);bool* nulls=(bool*)palloc(sizeof(bool)*columns);
    for(LakeTxnRow* row=table->rows;row;row=row->next){
        if(!LakeTxnRowHasPayload(row)||row->identity->head!=row)continue;
        CHECK_FOR_INTERRUPTS();
        if(row->identity->committed_origin)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("direct INSERT encountered a committed origin")));
        HeapTuple payload=LakeTxnMaterializeRow(row);
        LakeGatherHostTuple(table->binding,table->descriptor,payload,values,nulls);
        LakeFlushAppend(batch,file->output.candidate,values,nulls);++file->rows;
        for(int c=0;c<columns;++c){
            if(nulls[c]){file->has_null[c]=true;++file->null_counts[c];continue;}
            LakeObserveColumnBounds(parent,now->columns[c].type,now->columns[c].typmod,values[c],&file->minima[c],&file->maxima[c]);
        }
        heap_freetuple(payload);
    }
    if(file->rows!=table->active_rows||file->rows!=LakeNewIdentityCount(table))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("direct INSERT survivor count changed during preparation")));
    LakeArrowBuilderWrite(batch,file->output.candidate->api,file->output.candidate->writer);LakeArrowBuilderDestroy(batch);
    /* Existing finish validator checks immutable footer/field IDs/bytes and
     * durability. Its table is used solely for the field-schema checks. */
    LakeFlushOperation validation={};validation.context=parent;validation.table=table;
    LakeFlushFinish(&validation,&file->output,fields,columns);
    if(file->output.candidate->written.num_rows!=file->rows)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("direct INSERT immutable file count differs from survivors")));
    MemoryContextSwitchTo(caller);return file;
}

static void LakeRegisterInsertFile(LakeTxnTable* table,LakePreparedInsert* file,Snapshot snapshot,
    int64 commitSnapshot,int64 fileId,int64 rowStart)
{
    LakeInlineBinding* b=table->binding;Oid ns=b->metadata_schema_oid;
    if(fileId<0||rowStart<0||!file||file->rows<=0||!LakeDirectInsertEligible(table)||
       (file->grouped?(!LakePartitionDirectInsertEnabled()||!b->inline_sources_loaded||
         !b->partition.enabled||file->key.count!=b->partition.key_count):file->rows!=table->active_rows))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid prepared direct INSERT publication")));
    Oid types[8]={INT8OID,INT8OID,INT8OID,TEXTOID,INT8OID,INT8OID,INT8OID,INT8OID};
    Datum args[8]={Int64GetDatum(fileId),Int64GetDatum(LakePermanentTableId(b)),Int64GetDatum(commitSnapshot),
        CStringGetTextDatum(file->output.relative_path),Int64GetDatum(file->rows),
        Int64GetDatum(file->output.candidate->written.file_bytes),Int64GetDatum(file->output.footer_bytes),Int64GetDatum(rowStart)};
    char* files=LakeMetadataRelation(ns,"ducklake_data_file");
    LakeMetaRows* result=LakeMetadataQuery(psprintf("INSERT INTO %s(data_file_id,table_id,begin_snapshot,end_snapshot,file_order,path,"
        "path_is_relative,file_format,record_count,file_size_bytes,footer_size,row_id_start,partition_id,encryption_key,"
        "mapping_id,partial_max,row_group_count) SELECT $1,$2,$3,NULL,COALESCE(MAX(file_order),-1)+1,$4,true,'parquet',"
        "$5,$6,$7,$8,%s,NULL,NULL,NULL,%lld FROM %s WHERE table_id=$2",files,
        file->grouped?psprintf("%lld",(long long)b->partition.partition_id):"NULL",(long long)file->output.row_groups,files),
        8,types,args,NULL,snapshot,false,SPI_OK_INSERT,ns);
    LakeMetadataRequireCount(result,1,"direct INSERT file registration");
    if(file->grouped)for(int k=0;k<b->partition.key_count;++k){
        const LakePartitionValue* value=&file->key.values[k];
        Oid pt[4]={INT8OID,INT8OID,INT8OID,TEXTOID};
        Datum pa[4]={args[0],args[1],Int64GetDatum(k),value->is_null?(Datum)0:CStringGetTextDatum(value->canonical_text)};
        char pn[5]={' ',' ',' ',value->is_null?'n':' ','\0'};
        result=LakeMetadataQuery(psprintf("INSERT INTO %s(data_file_id,table_id,partition_key_index,partition_value) "
            "VALUES($1,$2,$3,$4)",LakeMetadataRelation(ns,"ducklake_file_partition_value")),
            4,pt,pa,pn,snapshot,false,SPI_OK_INSERT,ns);
        LakeMetadataRequireCount(result,1,"direct INSERT partition value");
    }
    bool collectBounds=LakeStatsExtendedVersionEnabled();
    for(int c=0;c<b->column_count;++c){
        LakeInsertFileColumnStats(ns,fileId,LakePermanentTableId(b),&b->columns[c],
            file->output.column_bytes[c],file->rows,file->null_counts[c],
            collectBounds&&file->file_minima?file->file_minima[c]:NULL,
            collectBounds&&file->file_maxima?file->file_maxima[c]:NULL,
            snapshot,"direct INSERT file column statistics");
    }
}

static void LakePublishInlineBatch(StringInfo batch,int nargs,int rows,Oid* types,Datum* args,
    char* nulls,Snapshot snapshot,Oid ns,MemoryContext payloads)
{
    if(rows==0)return;
    nulls[nargs*rows]='\0';
    LakeMetaRows* written=LakeMetadataQuery(batch->data,nargs*rows,types,args,nulls,
        snapshot,false,SPI_OK_INSERT,ns);
    LakeMetadataRequireCount(written,(uint32)rows,"inserted inline row batch");
    MemoryContextReset(payloads);
}

static void LakePublishTable(LakeTxnTable* table, Snapshot snapshot, int64 commitSnapshot,
    LakePreparedInsert* insertFile=NULL,int64 fileId=-1)
{
    LakeInlineBinding* binding=table->binding;
    Oid ns=binding->metadata_schema_oid;
    if(table->active_rows>0&&!OidIsValid(binding->inline_oid)&&insertFile==NULL)LakePublishEnsureInlineCarrier(binding,snapshot);
    char* carrier=OidIsValid(binding->inline_oid)?LakeMetadataRelation(ns,get_rel_name(binding->inline_oid)):NULL;
    /* End the exact source version once. Shared inline never contains private
     * intermediate versions; a zero match is an optimistic write conflict. */
    for(LakeTxnIdentity* identity=table->identities;identity!=NULL;identity=identity->next) {
        if(!identity->committed_origin || identity->head==NULL) continue;
        const LakeRowLocator* origin=&identity->origin;
        if(origin->source==LAKE_LOCATOR_FILE) {
            LakePublishFileDeletion(binding,origin,snapshot,commitSnapshot);continue;
        }
        const LakeInlineSource* source=binding->inline_sources_loaded?LakeFindInlineSource(binding,origin->inline_oid):NULL;
        bool sourceKnown=binding->inline_sources_loaded?source!=NULL:OidIsValid(binding->inline_oid)&&origin->inline_oid==binding->inline_oid;
        if(!sourceKnown || origin->source!=LAKE_LOCATOR_INLINE || origin->begin_snapshot>=(uint64)commitSnapshot)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native lake source version for publication")));
        const char* sourceCarrier=source?LakeMetadataRelation(ns,source->table_name):carrier;
        Oid types[3]={INT8OID,INT8OID,INT8OID};
        Datum args[3]={Int64GetDatum(commitSnapshot),Int64GetDatum(origin->row_id),Int64GetDatum((int64)origin->begin_snapshot)};
        LakeMetaRows* ended=LakeMetadataQuery(psprintf("UPDATE %s SET _ducklake_end_snapshot=$1 "
            "WHERE _ducklake_row_id=$2 AND _ducklake_begin_snapshot=$3 AND _ducklake_end_snapshot IS NULL "
            "RETURNING _ducklake_row_id",sourceCarrier),3,types,args,NULL,snapshot,false,SPI_OK_UPDATE_RETURNING,ns);
        if(ended->count==0)
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake source version was concurrently modified")));
        LakeMetadataRequireCount(ended,1,"ended inline source versions");
    }
    if(table->active_rows==0) return; /* Standard pure inline DELETE retains stored statistics. */
    int64 newIdentities=LakeNewIdentityCount(table);
    char* stats=LakeWriteRelation(binding,"ducklake_table_stats",statsColumns,lengthof(statsColumns));
    char* colStats=LakeWriteRelation(binding,"ducklake_table_column_stats",columnStatsColumns,lengthof(columnStatsColumns));
    Oid tableType=INT8OID; Datum tableArg=Int64GetDatum(LakePermanentTableId(binding));
    LakeMetaRows* old=LakeMetadataQuery(psprintf("SELECT record_count,next_row_id,file_size_bytes FROM %s WHERE table_id=$1",stats),
        1,&tableType,&tableArg,NULL,snapshot,true,SPI_OK_SELECT,ns);
    bool initializing=old->count==0;
    bool unknownCount=false;
    int64 count=0, next=0, bytes=0;
    if(initializing) {
        if(insertFile!=NULL&&carrier==NULL){
            LakePublishEnsureInlineCarrier(binding,snapshot);
            carrier=LakeMetadataRelation(ns,get_rel_name(binding->inline_oid));
        }
        LakeValidateUninitializedInlineTable(table,snapshot,carrier,colStats,newIdentities);
        /* The standard snapshot PK serializes cooperating publishers; the
         * standard table_stats itself has no table_id UNIQUE constraint.
         * Seed and final values stay in this attempt and the user's outer TX.
         * The following write SPI advances CID before allocating the range. */
        LakeMetaRows* seeded=LakeMetadataQuery(psprintf("INSERT INTO %s "
            "(table_id,record_count,next_row_id,file_size_bytes) VALUES($1,0,0,0)",stats),
            1,&tableType,&tableArg,NULL,snapshot,false,SPI_OK_INSERT,ns);
        LakeMetadataRequireCount(seeded,1,"initialized table statistics");
    } else {
        LakeMetadataRequireCount(old,1,"table statistics");
        Datum countValue=LakeMetadataValue(old,0,1,INT8OID,true,&unknownCount);
        count=unknownCount?0:DatumGetInt64(countValue);
        next=DatumGetInt64(LakeMetadataValue(old,0,2,INT8OID));
        bool unknownBytes=false;
        Datum byteValue=LakeMetadataValue(old,0,3,INT8OID,true,&unknownBytes);
        if(unknownBytes)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake new inline versions require known file_size_bytes")));
        bytes=DatumGetInt64(byteValue);
    }
    if(count<0 || bytes<0 || next<0 || next>LAKE_REFERENCE_LOCAL_ID_START ||
       newIdentities>LAKE_REFERENCE_LOCAL_ID_START-next ||
       (!unknownCount && table->active_rows>PG_INT64_MAX-count))
        ereport(ERROR,(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),errmsg("native lake inline ID or statistics range is exhausted")));
    int64 start=next, end=next;
    if(newIdentities>0) {
        Oid rangeTypes[3]={INT8OID,INT8OID,INT8OID};
        Datum rangeArgs[3]={tableArg,Int64GetDatum(newIdentities),Int64GetDatum(LAKE_REFERENCE_LOCAL_ID_START)};
        LakeMetaRows* range=LakeMetadataQuery(psprintf("UPDATE %s SET next_row_id=next_row_id+$2 "
            "WHERE table_id=$1 AND next_row_id>=0 AND next_row_id<=$3-$2 RETURNING next_row_id-$2,next_row_id",stats),
            3,rangeTypes,rangeArgs,NULL,snapshot,false,SPI_OK_UPDATE_RETURNING,ns);
        LakeMetadataRequireCount(range,1,"allocated row-ID ranges");
        start=DatumGetInt64(LakeMetadataValue(range,0,1,INT8OID));
        end=DatumGetInt64(LakeMetadataValue(range,0,2,INT8OID));
        if(start!=next || end<start || end-start!=newIdentities || end>LAKE_REFERENCE_LOCAL_ID_START)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake row-ID allocation did not match publication state")));
    }

    char** batchMin=NULL;char** batchMax=NULL;bool* batchNull=NULL;
    if(insertFile!=NULL){
        int64 insertedRows=LakeInsertFileRows(insertFile);
        if(insertedRows!=newIdentities||insertedRows!=table->active_rows||end-start!=insertedRows||
           LakeInsertFileBytes(insertFile)>PG_INT64_MAX-bytes)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("direct INSERT allocation or file statistics mismatch")));
        int64 groupStart=start;int groupIndex=0;
        for(LakePreparedInsert* file=insertFile;file;file=file->next){
            LakeRegisterInsertFile(table,file,snapshot,commitSnapshot,fileId+groupIndex,groupStart);
            groupStart+=file->rows;++groupIndex;
            /* Subsequent MAX(file_order) and column rows see prior files from
             * this same atomic publication, with the original MVCC set. */
            LakeAdvancePublicationCid(snapshot);
        }
        if(groupStart!=end)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("direct INSERT grouped row-ID intervals differ")));
        batchMin=insertFile->minima;batchMax=insertFile->maxima;batchNull=insertFile->has_null;
    }else{
    StringInfoData insert; initStringInfo(&insert);
    char* inlineName=get_rel_name(binding->inline_oid);
    appendStringInfo(&insert,"INSERT INTO %s (_ducklake_row_id,_ducklake_begin_snapshot,_ducklake_end_snapshot",
        LakeMetadataRelation(ns,inlineName));
    for(int i=0;i<binding->column_count;++i) appendStringInfo(&insert,",%s",quote_identifier(LakeInlinePhysicalName(&binding->columns[i])));
    appendStringInfoString(&insert,") VALUES");
    int nargs=binding->column_count+2;
    /* Bound both SQL expression population and parameter memory. Fewer SPI
     * executions shorten the catalog claim without changing row-ID order or
     * the enclosing host transaction/savepoint boundary. */
    int capacity=Min(512,8192/nargs),batchRows=0;
    Size nestedEstimate=0;
    for(int i=0;i<binding->column_count;++i)if(binding->columns[i].type==VECTOROID)
        nestedEstimate+=(Size)binding->columns[i].typmod*64+16;
    if(nestedEstimate>0)capacity=Min(capacity,Max(1,(int)((8*1024*1024)/nestedEstimate)));
    Assert(capacity>0);
    Oid* types=(Oid*)palloc(sizeof(Oid)*nargs*capacity);
    Datum* args=(Datum*)palloc(sizeof(Datum)*nargs*capacity);
    char* nulls=(char*)palloc(nargs*capacity+1);
    Datum* values=(Datum*)palloc(sizeof(Datum)*binding->column_count);
    StringInfoData batch;initStringInfo(&batch);
    bool* rowNulls=(bool*)palloc(sizeof(bool)*binding->column_count);
    batchMin=(char**)palloc0(sizeof(char*)*binding->column_count);
    batchMax=(char**)palloc0(sizeof(char*)*binding->column_count);
    batchNull=(bool*)palloc0(sizeof(bool)*binding->column_count);
    for(int row=0;row<capacity;++row){
        types[row*nargs]=types[row*nargs+1]=INT8OID;
        for(int i=0;i<binding->column_count;++i)types[row*nargs+i+2]=LakeInlinePhysicalType(binding->columns[i].type);
    }
    /* A materialized row never lends pointers to a later SPI call. Build one
     * encoded row first, then copy all by-reference parameters into this batch.
     * A batch holds at most 8 MiB, or one already-admitted larger row. */
    MemoryContext owner=CurrentMemoryContext;
    MemoryContext rowPayload=AllocSetContextCreate(owner,"Native lake SPI input row",
        ALLOCSET_DEFAULT_MINSIZE,ALLOCSET_DEFAULT_INITSIZE,ALLOCSET_DEFAULT_MAXSIZE);
    MemoryContext batchPayload=AllocSetContextCreate(owner,"Native lake SPI batch payload",
        ALLOCSET_DEFAULT_MINSIZE,ALLOCSET_DEFAULT_INITSIZE,ALLOCSET_DEFAULT_MAXSIZE);
    Datum* physical=(Datum*)palloc(sizeof(Datum)*binding->column_count);
    bool* physicalByVal=(bool*)palloc(sizeof(bool)*binding->column_count);
    int16* physicalLen=(int16*)palloc(sizeof(int16)*binding->column_count);
    for(int i=0;i<binding->column_count;++i)
        get_typlenbyval(LakeInlinePhysicalType(binding->columns[i].type),&physicalLen[i],&physicalByVal[i]);
    const Size batchLimit=8*1024*1024;
    Size batchBytes=0;
    int64 rowid=start;
    int64 writtenVersions=0;
    for(LakeTxnRow* row=table->rows;row!=NULL;row=row->next) {
        if(!LakeTxnRowHasPayload(row) || row->identity->head!=row) continue;
        CHECK_FOR_INTERRUPTS();
        MemoryContextSwitchTo(rowPayload);
        HeapTuple payload=LakeTxnMaterializeRow(row);
        LakeGatherHostTuple(binding,table->descriptor,payload,values,rowNulls);
        Size rowBytes=0;
        for(int i=0;i<binding->column_count;++i){
            physical[i]=values[i];
            if(rowNulls[i])continue;
            Oid businessType=binding->columns[i].type;
            if(businessType==VECTOROID)physical[i]=LakeVectorWriteText(values[i],binding->columns[i].typmod);
            else if(LakeNativeTypeIsTemporal(businessType))physical[i]=LakeTemporalWriteText(values[i],businessType);
            if(!physicalByVal[i]){
                Size size=datumGetSize(physical[i],false,physicalLen[i]);
                if(size>LAKE_INLINE_TXN_MEMORY_LIMIT||rowBytes>LAKE_INLINE_TXN_MEMORY_LIMIT-size)
                    ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake encoded inline row is too large")));
                rowBytes+=size;
            }
        }
        MemoryContextSwitchTo(owner);
        if(batchRows&&(rowBytes>batchLimit||batchBytes>batchLimit-rowBytes)){
            LakePublishInlineBatch(&batch,nargs,batchRows,types,args,nulls,snapshot,ns,batchPayload);
            batchRows=0;batchBytes=0;
        }
        int64 finalRowId=row->identity->committed_origin?row->identity->origin.row_id:rowid++;
        if(batchRows==0){resetStringInfo(&batch);appendStringInfoString(&batch,insert.data);}
        else appendStringInfoChar(&batch,',');
        int offset=batchRows*nargs;
        appendStringInfo(&batch,"($%d,$%d,NULL",offset+1,offset+2);
        for(int i=0;i<binding->column_count;++i)appendStringInfo(&batch,",$%d",offset+i+3);
        appendStringInfoChar(&batch,')');
        args[offset]=Int64GetDatum(finalRowId);args[offset+1]=Int64GetDatum(commitSnapshot);
        nulls[offset]=nulls[offset+1]=' ';
        for(int i=0;i<binding->column_count;++i) {
            args[offset+i+2]=(Datum)0;nulls[offset+i+2]=rowNulls[i]?'n':' ';
            if(rowNulls[i]) { batchNull[i]=true; continue; }
            MemoryContextSwitchTo(batchPayload);
            args[offset+i+2]=datumCopy(physical[i],physicalByVal[i],physicalLen[i]);
            MemoryContextSwitchTo(owner);
            LakeObserveColumnBounds(owner,binding->columns[i].type,binding->columns[i].typmod,
                values[i],&batchMin[i],&batchMax[i]);
        }
        heap_freetuple(payload);MemoryContextReset(rowPayload);
        ++batchRows;++writtenVersions;batchBytes+=rowBytes;
        if(batchRows==capacity||batchBytes>=batchLimit){
            LakePublishInlineBatch(&batch,nargs,batchRows,types,args,nulls,snapshot,ns,batchPayload);
            batchRows=0;batchBytes=0;
        }
    }
    if(batchRows)LakePublishInlineBatch(&batch,nargs,batchRows,types,args,nulls,snapshot,ns,batchPayload);
    MemoryContextDelete(rowPayload);MemoryContextDelete(batchPayload);
    if(rowid!=end || writtenVersions!=table->active_rows) ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake staged row count changed during publication")));
    }
    Oid updateTypes[4]={INT8OID,INT8OID,INT8OID,INT8OID};
    Datum updateArgs[4]={tableArg,Int64GetDatum(unknownCount?0:count+table->active_rows),Int64GetDatum(end),
        Int64GetDatum(insertFile?bytes+LakeInsertFileBytes(insertFile):bytes)};
    char updateNulls[5]={' ',unknownCount?'n':' ',' ',' ','\0'};
    LakeMetaRows* updated=LakeMetadataQuery(psprintf("UPDATE %s SET record_count=$2,next_row_id=$3,file_size_bytes=$4 WHERE table_id=$1",stats),
        4,updateTypes,updateArgs,updateNulls,snapshot,false,SPI_OK_UPDATE,ns);
    LakeMetadataRequireCount(updated,1,"updated table statistics");

    if(initializing) {
        LakeInsertInitialColumnStats(binding,colStats,snapshot,batchMin,batchMax,batchNull);
        return;
    }
    LakeMetaRows* columns=LakeMetadataQuery(psprintf("SELECT column_id,contains_null,min_value::text,max_value::text,"
        "min_is_exact,max_is_exact,extra_stats IS NOT NULL FROM %s WHERE table_id=$1",colStats),
        1,&tableType,&tableArg,NULL,snapshot,true,SPI_OK_SELECT,ns);
    bool* seen=(bool*)palloc0(sizeof(bool)*binding->column_count);
    for(uint32 r=0;r<columns->count;++r) {
        int64 columnId=DatumGetInt64(LakeMetadataValue(columns,r,1,INT8OID)); int index=-1;
        for(int i=0;i<binding->column_count;++i) if(binding->columns[i].column_id==columnId) index=i;
        if(index<0) continue; /* Historical/non-root entries are not rewritten as current columns. */
        if(seen[index]) ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("duplicate native lake column statistics")));
        seen[index]=true;
        bool nullUnknown=false, minUnknown=false, maxUnknown=false, minExactUnknown=false, maxExactUnknown=false;
        Datum nullable=LakeMetadataValue(columns,r,2,BOOLOID,true,&nullUnknown);
        Datum oldMin=LakeMetadataValue(columns,r,3,TEXTOID,true,&minUnknown);
        Datum oldMax=LakeMetadataValue(columns,r,4,TEXTOID,true,&maxUnknown);
        Datum minExact=LakeMetadataValue(columns,r,5,BOOLOID,true,&minExactUnknown);
        Datum maxExact=LakeMetadataValue(columns,r,6,BOOLOID,true,&maxExactUnknown);
        bool knownEmpty=!unknownCount && count==0;
        bool hasNull=(!nullUnknown && DatumGetBool(nullable))||batchNull[index];
        if(knownEmpty) nullUnknown=false;
        bool adoptBounds=knownEmpty;
        bool hasExtraStats=DatumGetBool(LakeMetadataValue(columns,r,7,BOOLOID));
        /* Both missing bounds can mean all NULL or invalidated statistics.
         * contains_null=true alone cannot distinguish them. As in the reference,
         * only a complete non-NULL new batch bound plus a predecessor value check
         * may restore them; false, zero and empty text are non-NULL values too. */
        if(!adoptBounds && minUnknown && maxUnknown && !hasExtraStats &&
           (nullUnknown || DatumGetBool(nullable)) && batchMin[index]!=NULL && batchMax[index]!=NULL)
            adoptBounds=!LakePredecessorMayHaveValues(binding,carrier,LakeInlinePhysicalName(&binding->columns[index]),
                snapshot,commitSnapshot);
        bool minIsExact=false,maxIsExact=false;
        char* previousMin=minUnknown?NULL:TextDatumGetCString(oldMin);
        char* previousMax=maxUnknown?NULL:TextDatumGetCString(oldMax);
        Oid colType=binding->columns[index].type;
        int32 colTypmod=binding->columns[index].typmod;
        if((colType==UUIDOID||(colType==NUMERICOID&&LakeStatsExtendedVersionEnabled()))&&!adoptBounds){
            int comparison=0;
            if((previousMin&&!LakeTryCompareStats(colType,previousMin,previousMin,&comparison,colTypmod))||
               (previousMax&&!LakeTryCompareStats(colType,previousMax,previousMax,&comparison,colTypmod))||
               (previousMin&&previousMax&&
                (!LakeTryCompareStats(colType,previousMin,previousMax,&comparison,colTypmod)||comparison>0)))
                previousMin=previousMax=NULL; /* malformed/inverted bounds remain unknown */
        }
        char* minValue=LakeMergeStatsBound(colType,
            previousMin,batchMin[index],adoptBounds,true,
            !minExactUnknown && DatumGetBool(minExact),&minIsExact,colTypmod);
        char* maxValue=LakeMergeStatsBound(colType,
            previousMax,batchMax[index],adoptBounds,false,
            !maxExactUnknown && DatumGetBool(maxExact),&maxIsExact,colTypmod);
        minExact=BoolGetDatum(minIsExact); maxExact=BoolGetDatum(maxIsExact);
        minExactUnknown=maxExactUnknown=false;
        /* Unknown bounds remain unknown unless proved NULL-only above.
         * Missing stats rows stay missing; unimplemented extra stats are unknown. */
        Oid ct[7]={INT8OID,INT8OID,BOOLOID,TEXTOID,TEXTOID,BOOLOID,BOOLOID};
        Datum ca[7]={tableArg,Int64GetDatum(columnId),BoolGetDatum(hasNull),
            minValue?CStringGetTextDatum(minValue):(Datum)0,maxValue?CStringGetTextDatum(maxValue):(Datum)0,minExact,maxExact};
        char cn[8]={' ',' ',nullUnknown?'n':' ',minValue?' ':'n',maxValue?' ':'n',
            (!minValue||minExactUnknown)?'n':' ',(!maxValue||maxExactUnknown)?'n':' ','\0'};
        LakeMetaRows* cs=LakeMetadataQuery(psprintf("UPDATE %s SET contains_null=$3,contains_nan=NULL,min_value=$4,max_value=$5,"
            "extra_stats=NULL,min_is_exact=$6,max_is_exact=$7 WHERE table_id=$1 AND column_id=$2",colStats),
            7,ct,ca,cn,snapshot,false,SPI_OK_UPDATE,ns);
        LakeMetadataRequireCount(cs,1,"updated column statistics");
    }
    /* External writers can have left exact element bounds. Parent LIST stats
     * cannot update those leaf populations, so invalidate them conservatively. */
    for(int i=0;i<binding->column_count;++i)if(binding->columns[i].type==VECTOROID){
        Oid ct[2]={INT8OID,INT8OID};Datum ca[2]={tableArg,Int64GetDatum(binding->columns[i].element_column_id)};
        (void)LakeMetadataQuery(psprintf("UPDATE %s SET contains_null=NULL,contains_nan=NULL,min_value=NULL,max_value=NULL,"
            "extra_stats=NULL,min_is_exact=NULL,max_is_exact=NULL WHERE table_id=$1 AND column_id=$2",colStats),
            2,ct,ca,NULL,snapshot,false,SPI_OK_UPDATE,ns);
    }
}

static int LakeTableCompare(const void* left, const void* right)
{
    const LakeTableIdentity& a=(*(LakeTxnTable* const*)left)->binding->identity;
    const LakeTableIdentity& b=(*(LakeTxnTable* const*)right)->binding->identity;
    if(a.kind!=b.kind) return a.kind<b.kind?-1:1;
    uint64 ak=LakeIdentityKey(a),bk=LakeIdentityKey(b);
    return ak==bk?0:(ak<bk?-1:1);
}

/* Physical layouts can change while a writer holds the stable row head:
 * flush copies the committed source without taking that business-row lock.
 * Never rewrite the durable journal's origin or after-image while publishing. */
struct LakePublicationOrigin {
    uint64 row_id;
    LakeTxnIdentity* identity; /* attempt-local copy, never the live journal */
    bool found;
};

static HTAB* LakePublicationEmptyHash(const char* name,Size key,Size entry,MemoryContext context)
{
    HASHCTL control;
    errno_t rc=memset_s(&control,sizeof(control),0,sizeof(control));securec_check(rc,"","");
    control.hcxt=context;control.keysize=key;control.entrysize=entry;
    return hash_create(name,16,&control,HASH_ELEM|HASH_BLOBS|HASH_CONTEXT);
}

static const LakeDataFile* LakePublicationFile(const LakeInlineBinding* binding,uint64 fileId)
{
    for(int i=0;i<binding->file_count;++i)
        if((uint64)binding->files[i].file_id==fileId)return &binding->files[i];
    return NULL;
}

/* Metadata-only proof that an already checked immutable (file,position) still
 * addresses the same bytes and has the same deletion inputs. No SDK under PK. */
static bool LakePublicationSameFile(const LakeDataFile* a,const LakeDataFile* b)
{
    if(a==NULL||b==NULL||a->file_id!=b->file_id||a->begin_snapshot!=b->begin_snapshot||
       a->record_count!=b->record_count||a->file_bytes!=b->file_bytes||a->row_group_count!=b->row_group_count||
       a->row_id_start_null!=b->row_id_start_null||(!a->row_id_start_null&&a->row_id_start!=b->row_id_start)||
       !LakeIdentityPartitionFileEqual(&a->partition,&b->partition)||
       a->partial_max!=b->partial_max||strcmp(a->path,b->path)!=0||
       a->inline_delete_count!=b->inline_delete_count||(a->deletes==NULL)!=(b->deletes==NULL))return false;
    if(a->deletes!=NULL) {
        const LakeDeleteFile* x=a->deletes;const LakeDeleteFile* y=b->deletes;
        if(x->file_id!=y->file_id||x->begin_snapshot!=y->begin_snapshot||x->record_count!=y->record_count||
           x->file_bytes!=y->file_bytes||x->partial_max!=y->partial_max||strcmp(x->path,y->path)!=0)return false;
    }
    for(int i=0;i<a->inline_delete_count;++i)
        if(a->inline_delete_positions[i]!=b->inline_delete_positions[i])return false;
    return true;
}

static bool LakePublicationInlineLive(const LakeInlineBinding* binding,const LakeRowLocator* origin,Snapshot snapshot)
{
    const LakeInlineSource* source=binding->inline_sources_loaded?LakeFindInlineSource(binding,origin->inline_oid):NULL;
    if(binding->inline_sources_loaded?source==NULL:(!OidIsValid(binding->inline_oid)||origin->inline_oid!=binding->inline_oid))return false;
    Oid types[2]={INT8OID,INT8OID};
    Datum args[2]={Int64GetDatum(origin->row_id),Int64GetDatum((int64)origin->begin_snapshot)};
    LakeMetaRows* rows=LakeMetadataQuery(psprintf("SELECT _ducklake_row_id FROM %s "
        "WHERE _ducklake_row_id=$1 AND _ducklake_begin_snapshot=$2 AND _ducklake_end_snapshot IS NULL LIMIT 2",
        LakeMetadataRelation(binding->metadata_schema_oid,source?source->table_name:get_rel_name(binding->inline_oid))),
        2,types,args,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    if(rows->count>1)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("duplicate native lake committed source identity")));
    return rows->count==1;
}

static void LakePublicationSetOrigin(LakeRowLocator* origin,const LakeInlineBinding* binding,
    const LakeInlineRowIdentity* row,Oid leafOid)
{
    /* R and birth have already matched. Preserve nonce/producer command and
     * change only the attempt's physical address and fresh metadata version. */
    origin->source=row->source;origin->schema_version=binding->schema_version;
    origin->identity=binding->identity;origin->read_snapshot=binding->snapshot_id;
    origin->inline_oid=row->source==LAKE_LOCATOR_INLINE?row->inline_oid:InvalidOid;
    origin->file_id=row->source==LAKE_LOCATOR_FILE?row->file_id:0;
    origin->file_row_position=row->source==LAKE_LOCATOR_FILE?row->file_position:0;
    origin->leaf_oid=leafOid;
}

static LakeInlineBinding* LakeCopyPublicationBinding(const LakeInlineBinding* binding)
{
    LakeInlineBinding* copy=(LakeInlineBinding*)palloc(sizeof(LakeInlineBinding));*copy=*binding;
    if(binding->inline_sources_loaded){
        copy->columns=(LakeInlineColumn*)palloc(sizeof(LakeInlineColumn)*binding->column_count);
        for(int c=0;c<binding->column_count;++c){
            copy->columns[c]=binding->columns[c];
            LakeCopyColumnDefault(&copy->columns[c],&binding->columns[c],NULL);
            copy->columns[c].name=pstrdup(binding->columns[c].name);
            copy->columns[c].inline_name=NULL;
        }
        LakeCopyInlineSources(copy,binding,CurrentMemoryContext);
    }
    return copy;
}

static LakeTxnTable* LakePreparePublicationSources(LakeTxnTable* journal,LakeInlineBinding* binding,Snapshot snapshot)
{
    LakeTxnTable* copy=(LakeTxnTable*)palloc(sizeof(LakeTxnTable));*copy=*journal;
    copy->binding=LakeCopyPublicationBinding(binding);
    copy->identities=NULL;
    HTAB* wanted=LakePublicationEmptyHash("Native lake publication targets",sizeof(uint64),
        sizeof(LakePublicationOrigin),CurrentMemoryContext);
    int missing=0;LakeTxnIdentity** link=&copy->identities;
    for(LakeTxnIdentity* identity=journal->identities;identity!=NULL;identity=identity->next) {
        LakeTxnIdentity* local=(LakeTxnIdentity*)palloc(sizeof(LakeTxnIdentity));*local=*identity;
        local->next=NULL;*link=local;link=&local->next;
        if(!local->committed_origin||local->head==NULL)continue;
        const LakeRowLocator* origin=&identity->origin;
        bool reusable=false;
        if(origin->source==LAKE_LOCATOR_INLINE)
            reusable=LakePublicationInlineLive(binding,origin,snapshot);
        else if(origin->source==LAKE_LOCATOR_FILE) {
            const LakeDataFile* oldFile=LakePublicationFile(journal->binding,origin->file_id);
            const LakeDataFile* nowFile=LakePublicationFile(binding,origin->file_id);
            reusable=LakePublicationSameFile(oldFile,nowFile);
            if(reusable) {
                if(origin->file_row_position>=(uint64)nowFile->record_count)reusable=false;
                for(int d=0;d<nowFile->inline_delete_count;++d)
                    if((uint64)nowFile->inline_delete_positions[d]==origin->file_row_position)reusable=false;
            }
        }
        if(reusable) {
            local->origin.schema_version=binding->schema_version;local->origin.read_snapshot=binding->snapshot_id;
            continue;
        }
        uint64 rowId=(uint64)origin->row_id;bool duplicate=false;
        LakePublicationOrigin* target=(LakePublicationOrigin*)hash_search(wanted,&rowId,HASH_ENTER,&duplicate);
        if(duplicate)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("duplicate native lake staged logical source")));
        target->identity=local;target->found=false;++missing;
    }
    if(missing==0)return copy;

    MemoryContext caller=CurrentMemoryContext;
    MemoryContext scratch=AllocSetContextCreate(caller,"Native lake committed publication source scan",
        ALLOCSET_DEFAULT_MINSIZE,ALLOCSET_DEFAULT_INITSIZE,ALLOCSET_DEFAULT_MAXSIZE);
    LakeInlineReader* volatile reader=NULL;
    PG_TRY();
    {
        MemoryContextSwitchTo(scratch);
        /* A disconnected empty journal is essential: the real journal hides
         * precisely the committed versions whose physical addresses we need.
         * Do not call LakeTxnReadTable/ResolveLatestRow after PUBLISHING begins. */
        LakeTxnCatalog* catalog=(LakeTxnCatalog*)palloc(sizeof(LakeTxnCatalog));*catalog=*journal->catalog;
        catalog->business_snapshot=snapshot;catalog->active=true;
        LakeTxnTable* clean=(LakeTxnTable*)palloc0(sizeof(LakeTxnTable));
        clean->context=scratch;clean->binding=binding;clean->descriptor=journal->descriptor;clean->catalog=catalog;
        clean->active=true;clean->statement_snapshot=snapshot;clean->statement_rc=true;
        clean->origin_index=LakePublicationEmptyHash("Native lake empty origins",sizeof(LakeOriginKey),sizeof(LakeOriginEntry),scratch);
        clean->logical_origin_index=LakePublicationEmptyHash("Native lake empty logical origins",sizeof(uint64),sizeof(LakeLogicalOriginEntry),scratch);
        clean->token_index=LakePublicationEmptyHash("Native lake empty tokens",sizeof(uint64),sizeof(LakeTokenEntry),scratch);
        reader=LakeInlineReaderOpen(clean,GetCurrentCommandId(false));
        MemoryContext rows=AllocSetContextCreate(scratch,"Native lake publication source row",
            ALLOCSET_DEFAULT_MINSIZE,ALLOCSET_DEFAULT_INITSIZE,ALLOCSET_DEFAULT_MAXSIZE);
        Datum* values=(Datum*)palloc0(sizeof(Datum)*binding->column_count);
        bool* nulls=(bool*)palloc0(sizeof(bool)*binding->column_count);
        for(;;) {
            CHECK_FOR_INTERRUPTS();MemoryContextReset(rows);MemoryContextSwitchTo(rows);
            LakeInlineRowIdentity row;
            if(!LakeInlineReaderNext(reader,values,nulls,&row))break;
            uint64 rowId=(uint64)row.row_id;
            LakePublicationOrigin* target=(LakePublicationOrigin*)hash_search(wanted,&rowId,HASH_FIND,NULL);
            if(target==NULL)continue;
            if(row.source==LAKE_LOCATOR_TXN||row.begin_snapshot!=(int64)target->identity->origin.begin_snapshot)
                ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake business source version changed before publication")));
            if(target->found)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("multiple visible native lake committed source versions")));
            LakePublicationSetOrigin(&target->identity->origin,binding,&row,journal->native_leaf_partition_oid);target->found=true;--missing;
        }
        MemoryContextSwitchTo(scratch);LakeInlineReaderClose(reader);reader=NULL;
        MemoryContextSwitchTo(caller);MemoryContextDelete(scratch);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(caller);
        if(reader!=NULL)LakeInlineReaderClose(reader);
        MemoryContextDelete(scratch);PG_RE_THROW();
    }
    PG_END_TRY();
    if(missing!=0)
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake exact committed source version disappeared before publication")));
    return copy;
}

/* A successful snapshot PK protects subsequent cooperating publishers. This
 * fresh host snapshot also covers any layout committed while preparation ran.
 * A changed layout requests a new attempt, never SDK I/O inside the claim. */
static bool LakePublicationSourcesStillLive(LakeTxnTable* prepared,Snapshot snapshot,
    LakeTxnState* state,int64 claimedSnapshot)
{
    LakeInlineBinding* before=prepared->binding;
    LakeInlineBinding* now=OidIsValid(prepared->native_leaf_partition_oid)?
        LakeNativePartitionPublicationBinding(state,prepared,snapshot,claimedSnapshot):LakeLoadCommittedInlineBinding(before->relation_oid,snapshot);
    /* The claim can advance the global schema version for this same attempt's
     * pending CREATEs. Compare this existing table's actual columns instead. */
    if(!LakeColumnLayoutsEqual(before,now)||now->read_only||LakePermanentTableId(now)!=LakePermanentTableId(before)||
       strcmp(now->table_uuid,before->table_uuid)!=0||now->inline_oid!=before->inline_oid||
       now->column_count!=before->column_count||now->data_root==NULL||before->data_root==NULL||
       strcmp(now->data_root,before->data_root)!=0)return false;
    for(int c=0;c<before->column_count;++c)
        if(now->columns[c].column_id!=before->columns[c].column_id||now->columns[c].type!=before->columns[c].type||
           now->columns[c].element_column_id!=before->columns[c].element_column_id||
           now->columns[c].typmod!=before->columns[c].typmod||
           !LakeColumnDefaultsEqual(&now->columns[c],&before->columns[c])||
           strcmp(now->columns[c].name,before->columns[c].name)!=0)return false;
    for(LakeTxnIdentity* identity=prepared->identities;identity!=NULL;identity=identity->next) {
        if(!identity->committed_origin||identity->head==NULL)continue;
        const LakeRowLocator* origin=&identity->origin;
        if(origin->source==LAKE_LOCATOR_INLINE) {
            if(!LakePublicationInlineLive(now,origin,snapshot))return false;
        } else if(origin->source==LAKE_LOCATOR_FILE) {
            const LakeDataFile* file=LakePublicationFile(now,origin->file_id);
            if(!LakePublicationSameFile(LakePublicationFile(before,origin->file_id),file)||
               origin->file_row_position>=(uint64)file->record_count)return false;
            for(int d=0;d<file->inline_delete_count;++d)
                if((uint64)file->inline_delete_positions[d]==origin->file_row_position)return false;
        } else
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native lake prepared source kind")));
    }
    return true;
}

static void LakePublishInlineChangesImpl(LakeTxnState* state)
{
    LakeTxnCatalog* catalog=state->catalogs;
    while(catalog!=NULL && (!catalog->active||catalog->server_oid!=state->writable_server)) catalog=catalog->next;
    if(catalog==NULL) ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake write catalog is missing")));
    int tableCount=0, newInlineTableCount=0;
    for(LakeTxnTable* table=catalog->tables;table!=NULL;table=table->next)
        if(table->active && (table->pending!=NULL || LakeTxnTableHasChanges(table))) {
            ++tableCount;
            if(table->active_rows>0) ++newInlineTableCount;
        }
    if(tableCount==0) return;
    LakeTxnTable** tables=(LakeTxnTable**)MemoryContextAlloc(state->context,sizeof(LakeTxnTable*)*tableCount);
    int n=0;
    for(LakeTxnTable* table=catalog->tables;table!=NULL;table=table->next)
        if(table->active && (table->pending!=NULL || LakeTxnTableHasChanges(table))) tables[n++]=table;
    qsort(tables,tableCount,sizeof(LakeTxnTable*),LakeTableCompare);
    MemoryContext caller=CurrentMemoryContext;
    ResourceOwner owner=t_thrd.utils_cxt.CurrentResourceOwner;
    LakePreparedInsert** insertFiles=(LakePreparedInsert**)MemoryContextAllocZero(state->context,sizeof(LakePreparedInsert*)*tableCount);
    if(LakeDirectInsertVersionEnabled()){
        Snapshot prepareSnapshot=GetTransactionSnapshot();
        for(int i=0;i<tableCount;++i)insertFiles[i]=LakePrepareInsertFile(tables[i],prepareSnapshot,state->context);
    }
    /* Existing inline publications still reserve their original single slot.
     * A grouped direct payload replaces that slot with its complete file set. */
    int64 newFileSlots=newInlineTableCount;
    for(int i=0;i<tableCount;++i)if(insertFiles[i]){
        int extra=LakeInsertFileCount(insertFiles[i])-1;
        if(extra<0||newFileSlots>PG_INT64_MAX-extra)
            ereport(ERROR,(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),errmsg("direct INSERT file-slot count overflow")));
        newFileSlots+=extra;
    }
    for(int attempt=0;attempt<16;++attempt) {
        CHECK_FOR_INTERRUPTS();
        MemoryContext attemptContext=AllocSetContextCreate(state->context,"Native lake publication attempt",
            ALLOCSET_DEFAULT_MINSIZE,ALLOCSET_DEFAULT_INITSIZE,ALLOCSET_DEFAULT_MAXSIZE);
        volatile bool inSubtransaction=false;
        bool claimed=false;
        PG_TRY();
        {
            state->publication_subtransaction=true;
            BeginInternalSubTransaction(NULL); inSubtransaction=true;
            MemoryContextSwitchTo(attemptContext);
            LakeInlineBinding* first=tables[0]->binding;
            Oid ns=first->metadata_schema_oid;
            /* Freeze options before choosing the fresh publication snapshot.
             * Standard set_option INSERT/UPDATE/DELETE takes RowExclusiveLock.
             * Other publishers share this lock; business inline readers do not
             * wait. Failed claim attempts release it with their subtransaction. */
            (void)LakeMetadataRelation(ns,"ducklake_metadata");
            Relation options=heap_open(get_relname_relid("ducklake_metadata",ns),ShareLock);
            heap_close(options,NoLock);
            Snapshot snapshot=GetTransactionSnapshot();
            char* snapshots=LakeWriteRelation(first,"ducklake_snapshot",snapshotColumns,lengthof(snapshotColumns),true);
            char* changes=LakeWriteRelation(first,"ducklake_snapshot_changes",changesColumns,lengthof(changesColumns),true);
            LakeMetaRows* latest=LakeMetadataQuery(psprintf("SELECT snapshot_id,schema_version,next_catalog_id,next_file_id FROM %s "
                "ORDER BY snapshot_id DESC LIMIT 1",snapshots),0,NULL,NULL,NULL,snapshot,true,SPI_OK_SELECT,ns);
            LakeMetadataRequireCount(latest,1,"publication snapshots");
            int64 current=DatumGetInt64(LakeMetadataValue(latest,0,1,INT8OID));
            int64 schema=DatumGetInt64(LakeMetadataValue(latest,0,2,INT8OID));
            int64 nextCatalog=DatumGetInt64(LakeMetadataValue(latest,0,3,INT8OID));
            int64 nextFile=DatumGetInt64(LakeMetadataValue(latest,0,4,INT8OID));
            if(current<catalog->read_snapshot || current==PG_INT64_MAX || schema<0 || nextCatalog<0 ||
               nextFile<0 || nextFile>PG_INT64_MAX-newFileSlots)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native lake publication counters")));
            Oid changeTypes[2]={INT8OID,INT8OID};
            Datum changeArgs[2]={Int64GetDatum(catalog->read_snapshot),Int64GetDatum(current)};
            bool firstModifyBaselines=LakeTxnFirstModifyHistoryEnabled(state);
            LakeMetaRows* later=LakeMetadataQuery(psprintf("SELECT changes_made::text%s FROM %s WHERE snapshot_id>$1 AND snapshot_id<=$2 "
                "ORDER BY snapshot_id",firstModifyBaselines?",snapshot_id":"",changes),2,changeTypes,changeArgs,NULL,snapshot,true,SPI_OK_SELECT,ns);
            if ((uint64)later->count != (uint64)(current-catalog->read_snapshot))
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake commit history has a missing changeset")));
            bool remapSources=LakeRowLocksEnabled()&&u_sess->utils_cxt.XactIsoLevel==XACT_READ_COMMITTED;
            LakeCheckConcurrentChanges(later,catalog,LakeHasFlushVersion() &&
                OidIsValid(LakeMetadataOwner()) && u_sess->utils_cxt.XactIsoLevel==XACT_READ_COMMITTED,remapSources,LakeDirectInsertVersionEnabled(),
                remapSources&&OidIsValid(LakeMetadataOwner())&&LakeRewriteVersionEnabled(),false,firstModifyBaselines);
            int createCount=0;
            const LakeCreateDefinition** definitions=(const LakeCreateDefinition**)palloc(sizeof(LakeCreateDefinition*)*tableCount);
            for(int i=0;i<tableCount;++i) if(tables[i]->pending!=NULL)
                definitions[createCount++]=tables[i]->pending->definition;
            if(createCount)for(int i=0;i<tableCount;++i)
                if(OidIsValid(tables[i]->native_leaf_partition_oid))
                    ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native parent writes cannot mix with pending lake CREATE in this slice")));
            int64 finalCatalog=nextCatalog,finalSchema=schema;
            LakeCreateAttempt* createMap=createCount>0 ?
                LakePlanCreateAttempt(definitions,createCount,current,schema,nextCatalog,&finalCatalog,&finalSchema) : NULL;
            for(int i=0;i<createCount;++i) {
                LakeValidateCreateDefinition(definitions[i],snapshot,current);
                LakeValidateCreateOptions(definitions[i],snapshot);
                LakeCheckWriteOptions((LakeInlineBinding*)definitions[i]->binding,snapshot);
            }
            LakeNativePublicationCertificates* certificates=
                LakeNativePreparePublicationCertificates(state,tables,tableCount,snapshot);
            LakeInlineBinding** currentBindings=(LakeInlineBinding**)palloc0(sizeof(LakeInlineBinding*)*tableCount);
            for(int i=0;i<tableCount;++i) {
                if(tables[i]->pending!=NULL) {
                    LakeTxnRequireActiveTable(tables[i]);
                    if(tables[i]->active_rows>0) {
                        Relation host=heap_open(tables[i]->binding->relation_oid,AccessShareLock);
                        LakeValidateInlineWriteTarget(tables[i],host,snapshot);heap_close(host,NoLock);
                    }
                    continue;
                }
                LakeInlineBinding* now=OidIsValid(tables[i]->native_leaf_partition_oid)?
                    LakeNativePartitionPublicationBinding(state,tables[i],snapshot):
                    LakeLoadCommittedInlineBinding(tables[i]->binding->relation_oid,snapshot);
                LakeInlineBinding* old=tables[i]->binding;
                currentBindings[i]=now;
                if(LakeDirectInsertEligible(tables[i])){
                    int64 nowLimit=LakeFileInlineLimit(now,snapshot);
                    if(insertFiles[i]?insertFiles[i]->inline_limit!=nowLimit:tables[i]->active_rows>nowLimit)
                        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("direct INSERT policy changed after preparation")));
                }
                if(insertFiles[i]){
                    LakeCheckInsertBinding(old,now);
                    if(strcmp(insertFiles[i]->path_key,LakeInsertPathKey(now,snapshot)))
                        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("direct INSERT output path changed before publication")));
                }
                bool newCarrier=LakeRowLocksEnabled() && !OidIsValid(old->inline_oid) && OidIsValid(now->inline_oid);
                if(!LakeColumnLayoutsEqual(old,now) || now->read_only || LakePermanentTableId(now)!=LakePermanentTableId(old) || strcmp(now->table_uuid,old->table_uuid)!=0 ||
                   (now->inline_oid!=old->inline_oid && !newCarrier) || now->column_count!=old->column_count)
                    ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake write binding changed before publication")));
                for(int c=0;c<old->column_count;++c)
                    if(now->columns[c].column_id!=old->columns[c].column_id || now->columns[c].type!=old->columns[c].type ||
                       now->columns[c].element_column_id!=old->columns[c].element_column_id ||
                       !LakeColumnDefaultsEqual(&now->columns[c],&old->columns[c]) ||
                       strcmp(now->columns[c].name,old->columns[c].name)!=0)
                        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake write schema changed before publication")));
                Relation host=heap_open(old->relation_oid,AccessShareLock);
                LakeValidateInlineWriteTarget(tables[i],host,snapshot);
                heap_close(host,NoLock);
            }
            /* Resolve missing physical origins before entering the snapshot
             * PK gate. The scanner is pure committed input, with no own overlay.
             * All after-images, SQL expressions and durable journal links stay
             * exactly as evaluated by the user's original command/EPQ. */
            LakeTxnTable** prepared=(LakeTxnTable**)palloc0(sizeof(LakeTxnTable*)*tableCount);
            if(remapSources)for(int i=0;i<tableCount;++i)
                if(tables[i]->pending==NULL) {
                    prepared[i]=LakePreparePublicationSources(tables[i],currentBindings[i],snapshot);
                    /* An origin that moved into a file must also satisfy the
                     * existing file-DML admission/inline budget at publication. */
                    Relation host=heap_open(prepared[i]->binding->relation_oid,AccessShareLock);
                    LakeValidateInlineWriteTarget(prepared[i],host,snapshot);heap_close(host,NoLock);
                }
            Oid types[4]={INT8OID,INT8OID,INT8OID,INT8OID};
            Datum args[4]={Int64GetDatum(current+1),Int64GetDatum(finalSchema),Int64GetDatum(finalCatalog),Int64GetDatum(nextFile+newFileSlots)};
            LakeMetaRows* claim=LakeMetadataQuery(psprintf("INSERT INTO %s (snapshot_id,snapshot_time,schema_version,next_catalog_id,next_file_id) "
                "VALUES($1,pg_catalog.now(),$2,$3,$4) ON CONFLICT(snapshot_id) DO NOTHING RETURNING snapshot_id",snapshots),
                4,types,args,NULL,snapshot,false,SPI_OK_INSERT_RETURNING,ns);
            if(claim->count>1) ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake snapshot claim returned multiple rows")));
            claimed=claim->count==1;
            if(claimed&&remapSources) {
                Snapshot verify=GetLatestSnapshot();
                if(!LakeNativePublicationCertificatesStillCurrent(state,certificates,verify))claimed=false;
                for(int i=0;claimed&&i<tableCount;++i)
                    if(prepared[i]!=NULL&&!LakePublicationSourcesStillLive(prepared[i],verify,state,current+1)) {
                        /* The else branch removes this PK claim and releases
                         * all attempt locks before fresh committed preparation.
                         * No broad SQLSTATE catch may retry a business conflict. */
                        claimed=false;break;
                    }
            }
            if(claimed) {
                StringInfoData changeText; initStringInfo(&changeText);
                LakeTxnTable** published=(LakeTxnTable**)palloc(sizeof(LakeTxnTable*)*tableCount);
                int mapped=0;
                for(int i=0;i<tableCount;++i) {
                    published[i]=tables[i];
                    if(tables[i]->pending==NULL) {
                        if(prepared[i]!=NULL)published[i]=prepared[i];
                        else if(LakeFileModificationsEnabled()) {
                            LakeTxnTable* copy=(LakeTxnTable*)palloc(sizeof(LakeTxnTable));*copy=*tables[i];
                            copy->binding=LakeCopyPublicationBinding(LakeRowLocksEnabled()?currentBindings[i]:tables[i]->binding);
                            published[i]=copy;
                        }
                        continue;
                    }
                    LakeCreateAttempt* mapping=&createMap[mapped++];
                    Assert(mapping->definition==tables[i]->pending->definition);
                    LakeWriteCreateDefinition(mapping,snapshot);
                    LakeTxnTable* copy=(LakeTxnTable*)palloc(sizeof(LakeTxnTable));*copy=*tables[i];
                    copy->binding=LakeCopyPublicationBinding(tables[i]->binding);
                    copy->binding->identity=LakePermanentIdentity(mapping->permanent_table_id);
                    copy->binding->schema_version=mapping->commit_schema_version;
                    LakePublishHostBinding(copy->binding,copy->binding->relation_oid,snapshot);
                    LakeDetachPublishedCarrier(mapping->definition);
                    published[i]=copy;
                    LakeAppendCreatedTableChange(&changeText,mapping->definition->schema_name,mapping->definition->table_name);
                }
                /* Retain one active own-write snapshot while multiple SPI calls
                 * observe definitions just written by this successful attempt.
                 * Only CID advances; business S and the MVCC visible set do not. */
                bool ownedSnapshot=createCount>0||LakeFileModificationsEnabled();
                Snapshot ownWrites=ownedSnapshot ? LakeCreatePushOwnWriteSnapshot(snapshot) : snapshot;
                int64 insertSlot=nextFile;
                for(int i=0;i<tableCount;++i){
                    if(LakeTxnTableHasChanges(published[i]))LakePublishTable(published[i],ownWrites,current+1,insertFiles[i],insertSlot);
                    if(published[i]->active_rows>0)insertSlot+=insertFiles[i]?LakeInsertFileCount(insertFiles[i]):1;
                }
                if(insertSlot!=nextFile+newFileSlots)
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("direct INSERT file-ID slot accounting changed")));
                /* One host change-counter UPSERT per final changed table, after
                 * all business writes. It shares this claim and outer TX; no
                 * increment occurs in failed/retried publication attempts. */
                for(int i=0;i<tableCount;++i) if(LakeTxnTableHasChanges(published[i])) {
                    int64 fresh=LakeNewIdentityCount(published[i]),ended=LakeEndedOriginCount(published[i]);
                    if(fresh>PG_INT64_MAX-ended) ereport(ERROR,(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),errmsg("lake analysis change count overflow")));
                    LakeAutoAnalyzeNotePublished(published[i]->binding,fresh+ended,ownWrites);
                }
                /* The global claim and both leaf journals are one host TX.
                 * Refresh head/source authority only after every table write,
                 * never once per row, and with no SDK IO in this window. */
                LakeNativePartitionRefreshPublished(state,published,tableCount,ownWrites,current+1,certificates);
                for(int i=0;i<tableCount;++i) if(published[i]->active_rows>0) {
                    if(changeText.len!=0) appendStringInfoChar(&changeText,',');
                    appendStringInfo(&changeText,insertFiles[i]?"inserted_into_table:%lld":"inlined_insert:%lld",
                        (long long)LakePermanentTableId(published[i]->binding));
                }
                for(int i=0;i<tableCount;++i) if(LakeEndedOriginCount(published[i])>0) {
                    if(changeText.len!=0) appendStringInfoChar(&changeText,',');
                    appendStringInfo(&changeText,"inlined_delete:%lld",(long long)LakePermanentTableId(published[i]->binding));
                }
                Oid ct[2]={INT8OID,TEXTOID}; Datum ca[2]={args[0],CStringGetTextDatum(changeText.data)};
                LakeMetaRows* saved=LakeMetadataQuery(psprintf("INSERT INTO %s(snapshot_id,changes_made,author,commit_message,commit_extra_info) "
                    "VALUES($1,$2,NULL,NULL,NULL)",changes),2,ct,ca,NULL,snapshot,false,SPI_OK_INSERT,ns);
                LakeMetadataRequireCount(saved,1,"published changesets");
                /* Publication can add a carrier and copy full source defaults
                 * after its first binding check. Check the COMPLETE map under
                 * the SAME owned write snapshot, once per changed ordinary
                 * table, after definitions/rows/stats/certificates/changes.
                 * Reload map-only bytes: an attempt may inherit a journal
                 * that already includes the independent host-copy charge. */
                if(ownedSnapshot){
                    LakeAdvancePublicationCid(ownWrites);
                    for(int i=0;i<tableCount;++i)
                        if((LakeTxnTableHasChanges(published[i])||published[i]->pending!=NULL)&&
                           (published[i]->binding->column_evolution||(published[i]->pending!=NULL&&
                            (published[i]->binding->literal_defaults||published[i]->binding->not_null_constraints)))&&
                           !OidIsValid(published[i]->native_leaf_partition_oid))
                            (void)LakeLoadCommittedInlineBinding(published[i]->binding->relation_oid,ownWrites);
                }
                if(ownedSnapshot) PopActiveSnapshot();
                for(int i=0;i<tableCount;++i)
                    for(LakePreparedInsert* file=insertFiles[i];file;file=file->next)
                        LakeFileCandidatePublish(file->output.candidate);
                ReleaseCurrentSubTransaction();
                inSubtransaction=false;
                state->publication_subtransaction=false;
                LakeHostActivateDeferredPublish(first, current+1);
            } else {
                RollbackAndReleaseCurrentSubTransaction();
                inSubtransaction=false;
                state->publication_subtransaction=false;
            }
            MemoryContextSwitchTo(caller);
            t_thrd.utils_cxt.CurrentResourceOwner=owner;
            MemoryContextDelete(attemptContext);
        }
        PG_CATCH();
        {
            MemoryContextSwitchTo(caller);
            ErrorData* error=CopyErrorData(); FlushErrorState();
            if(inSubtransaction) RollbackAndReleaseCurrentSubTransaction();
            state->publication_subtransaction=false;
            t_thrd.utils_cxt.CurrentResourceOwner=owner;
            MemoryContextDelete(attemptContext);
            ReThrowError(error);
        }
        PG_END_TRY();
        if(claimed) return;
        /* Failed attempts own no publication locks while backing off. */
        pg_usleep(1000L*(attempt+1));
    }
    ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake snapshot publication exceeded its retry limit")));
}

void LakePublishInlineChanges(LakeTxnState* state)
{
    int fastScope = LakeEnterFastValidationScope();
    PG_TRY();
    {
        LakePublishInlineChangesImpl(state);
    }
    PG_CATCH();
    {
        LakeLeaveFastValidationScope(fastScope);
        PG_RE_THROW();
    }
    PG_END_TRY();
    LakeLeaveFastValidationScope(fastScope);
}

void LakePublishDrops(LakeTxnState* state)
{
    if(!LakeLogicalDropVersionEnabled())
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake logical DROP contract changed before publication")));
    int count=0;LakeDropEvent** events=LakeCollectDrops(state,&count);
    if(count==0) return;
    /* Host object removal is already complete but still uncommitted. Only
     * trusted catalog identity is checked below; deleted pg_class/binding
     * entries are never reopened. One host commit publishes all its drops. */
    CommandCounterIncrement();
    int64 read=events[0]->binding->snapshot_id;
    for(int i=1;i<count;++i) read=Min(read,events[i]->binding->snapshot_id);
    MemoryContext caller=CurrentMemoryContext;ResourceOwner owner=t_thrd.utils_cxt.CurrentResourceOwner;
    for(int attempt=0;attempt<16;++attempt) {
        CHECK_FOR_INTERRUPTS();
        MemoryContext context=AllocSetContextCreate(state->context,"Native lake logical DROP publication",
            ALLOCSET_DEFAULT_MINSIZE,ALLOCSET_DEFAULT_INITSIZE,ALLOCSET_DEFAULT_MAXSIZE);
        volatile bool inside=false;bool claimed=false;
        PG_TRY();
        {
            state->publication_subtransaction=true;BeginInternalSubTransaction(NULL);inside=true;
            MemoryContextSwitchTo(context);
            LakeInlineBinding* first=events[0]->binding;Oid ns=first->metadata_schema_oid;
            (void)LakeMetadataRelation(ns,"ducklake_metadata");
            Relation options=heap_open(get_relname_relid("ducklake_metadata",ns),ShareLock);heap_close(options,NoLock);
            Snapshot snapshot=GetTransactionSnapshot();LakeCheckWriteOptions(first,snapshot);
            char* snapshots=LakeWriteRelation(first,"ducklake_snapshot",snapshotColumns,lengthof(snapshotColumns),true);
            char* changes=LakeWriteRelation(first,"ducklake_snapshot_changes",changesColumns,lengthof(changesColumns),true);
            LakeMetaRows* latest=LakeMetadataQuery(psprintf("SELECT snapshot_id,schema_version,next_catalog_id,next_file_id "
                "FROM %s ORDER BY snapshot_id DESC LIMIT 1",snapshots),0,NULL,NULL,NULL,snapshot,true,SPI_OK_SELECT,ns);
            LakeMetadataRequireCount(latest,1,"logical DROP publication snapshots");
            int64 current=DatumGetInt64(LakeMetadataValue(latest,0,1,INT8OID));
            int64 schema=DatumGetInt64(LakeMetadataValue(latest,0,2,INT8OID));
            int64 nextCatalog=DatumGetInt64(LakeMetadataValue(latest,0,3,INT8OID));
            int64 nextFile=DatumGetInt64(LakeMetadataValue(latest,0,4,INT8OID));
            if(current<read || current==PG_INT64_MAX || schema<0 || schema==PG_INT64_MAX ||
               nextCatalog<0 || nextCatalog>LAKE_REFERENCE_LOCAL_ID_START || nextFile<0)
                ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake logical DROP publication counters are out of range")));
            Oid ct[2]={INT8OID,INT8OID};Datum ca[2]={Int64GetDatum(read),Int64GetDatum(current)};
            LakeMetaRows* later=LakeMetadataQuery(psprintf("SELECT changes_made::text FROM %s WHERE snapshot_id>$1 "
                "AND snapshot_id<=$2 ORDER BY snapshot_id",changes),2,ct,ca,NULL,snapshot,true,SPI_OK_SELECT,ns);
            if((uint64)later->count!=(uint64)(current-read))
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake logical DROP history has a missing changeset")));
            LakeTxnCatalog checkCatalog;errno_t rc=memset_s(&checkCatalog,sizeof(checkCatalog),0,sizeof(checkCatalog));
            securec_check(rc,"","");LakeCheckConcurrentChanges(later,&checkCatalog,true);
            LakeValidateDrops(events,count,snapshot,current);
            Oid types[4]={INT8OID,INT8OID,INT8OID,INT8OID};
            Datum args[4]={Int64GetDatum(current+1),Int64GetDatum(schema+1),Int64GetDatum(nextCatalog),Int64GetDatum(nextFile)};
            LakeMetaRows* claim=LakeMetadataQuery(psprintf("INSERT INTO %s "
                "(snapshot_id,snapshot_time,schema_version,next_catalog_id,next_file_id) "
                "VALUES($1,pg_catalog.now(),$2,$3,$4) ON CONFLICT(snapshot_id) DO NOTHING RETURNING snapshot_id",snapshots),
                4,types,args,NULL,snapshot,false,SPI_OK_INSERT_RETURNING,ns);
            if(claim->count>1) ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("multiple native lake logical DROP claims")));
            claimed=claim->count==1;
            if(claimed) {
                LakeWriteDrops(events,count,snapshot,current+1);
                StringInfoData text;initStringInfo(&text);
                for(int i=0;i<count;++i) {
                    if(text.len)appendStringInfoChar(&text,',');
                    appendStringInfo(&text,"dropped_table:" INT64_FORMAT,LakePermanentTableId(events[i]->binding));
                }
                Oid cts[2]={INT8OID,TEXTOID};Datum cas[2]={args[0],CStringGetTextDatum(text.data)};
                LakeMetaRows* saved=LakeMetadataQuery(psprintf("INSERT INTO %s "
                    "(snapshot_id,changes_made,author,commit_message,commit_extra_info) VALUES($1,$2,NULL,NULL,NULL)",changes),
                    2,cts,cas,NULL,snapshot,false,SPI_OK_INSERT,ns);
                LakeMetadataRequireCount(saved,1,"logical DROP changesets");
                ReleaseCurrentSubTransaction();
                inside=false;state->publication_subtransaction=false;
                LakeInvalidateRelationSafe(first->relation_oid);
                LakeHostActivateDeferredPublish(first, current+1);
            } else {
                RollbackAndReleaseCurrentSubTransaction();
                inside=false;state->publication_subtransaction=false;
            }
            MemoryContextSwitchTo(caller);
            t_thrd.utils_cxt.CurrentResourceOwner=owner;MemoryContextDelete(context);
        }
        PG_CATCH();
        {
            MemoryContextSwitchTo(caller);ErrorData* error=CopyErrorData();FlushErrorState();
            if(inside) RollbackAndReleaseCurrentSubTransaction();
            state->publication_subtransaction=false;t_thrd.utils_cxt.CurrentResourceOwner=owner;
            MemoryContextDelete(context);ReThrowError(error);
        }
        PG_END_TRY();
        if(claimed) return;
        pg_usleep(1000L*(attempt+1));
    }
    ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake logical DROP snapshot claim exceeded its retry limit")));
}


void LakePublishRenames(LakeTxnState* state)
{
    int count=0;LakeRenameEvent** events=LakeCollectRenames(state,&count);
    if(count==0) return;
    /* A final autocommit utility reaches PRE_COMMIT before the ordinary next
     * command's CCI. Make the completed host pg_class/type rename visible to
     * syscache and our own catalog checks. This advances only command
     * visibility, never the journal's fixed lake read_snapshot. */
    CommandCounterIncrement();
    int64 read=events[0]->binding->snapshot_id;
    for(int i=1;i<count;++i) read=Min(read,events[i]->binding->snapshot_id);
    MemoryContext caller=CurrentMemoryContext;ResourceOwner owner=t_thrd.utils_cxt.CurrentResourceOwner;
    for(int attempt=0;attempt<16;++attempt) {
        CHECK_FOR_INTERRUPTS();
        MemoryContext context=AllocSetContextCreate(state->context,"Native lake RENAME publication",
            ALLOCSET_DEFAULT_MINSIZE,ALLOCSET_DEFAULT_INITSIZE,ALLOCSET_DEFAULT_MAXSIZE);
        volatile bool inside=false;bool claimed=false;
        PG_TRY();
        {
            state->publication_subtransaction=true;BeginInternalSubTransaction(NULL);inside=true;
            MemoryContextSwitchTo(context);
            LakeInlineBinding* first=events[0]->binding;Oid ns=first->metadata_schema_oid;
            (void)LakeMetadataRelation(ns,"ducklake_metadata");
            Relation options=heap_open(get_relname_relid("ducklake_metadata",ns),ShareLock);heap_close(options,NoLock);
            Snapshot snapshot=GetTransactionSnapshot();LakeCheckWriteOptions(first,snapshot);
            char* snapshots=LakeWriteRelation(first,"ducklake_snapshot",snapshotColumns,lengthof(snapshotColumns),true);
            char* changes=LakeWriteRelation(first,"ducklake_snapshot_changes",changesColumns,lengthof(changesColumns),true);
            LakeMetaRows* latest=LakeMetadataQuery(psprintf("SELECT snapshot_id,schema_version,next_catalog_id,next_file_id "
                "FROM %s ORDER BY snapshot_id DESC LIMIT 1",snapshots),0,NULL,NULL,NULL,snapshot,true,SPI_OK_SELECT,ns);
            LakeMetadataRequireCount(latest,1,"RENAME publication snapshots");
            int64 current=DatumGetInt64(LakeMetadataValue(latest,0,1,INT8OID));
            int64 schema=DatumGetInt64(LakeMetadataValue(latest,0,2,INT8OID));
            int64 nextCatalog=DatumGetInt64(LakeMetadataValue(latest,0,3,INT8OID));
            int64 nextFile=DatumGetInt64(LakeMetadataValue(latest,0,4,INT8OID));
            if(current<read || current==PG_INT64_MAX || schema<0 || schema==PG_INT64_MAX ||
               nextCatalog<0 || nextCatalog>LAKE_REFERENCE_LOCAL_ID_START || nextFile<0)
                ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake RENAME publication counters are out of range")));
            Oid ct[2]={INT8OID,INT8OID};Datum ca[2]={Int64GetDatum(read),Int64GetDatum(current)};
            LakeMetaRows* later=LakeMetadataQuery(psprintf("SELECT changes_made::text FROM %s WHERE snapshot_id>$1 "
                "AND snapshot_id<=$2 ORDER BY snapshot_id",changes),2,ct,ca,NULL,snapshot,true,SPI_OK_SELECT,ns);
            if((uint64)later->count!=(uint64)(current-read))
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake RENAME history has a missing changeset")));
            LakeTxnCatalog checkCatalog;errno_t rc=memset_s(&checkCatalog,sizeof(checkCatalog),0,sizeof(checkCatalog));
            securec_check(rc,"","");LakeCheckConcurrentChanges(later,&checkCatalog,true);
            LakeValidateRenames(events,count,snapshot,current);
            Oid types[4]={INT8OID,INT8OID,INT8OID,INT8OID};
            Datum args[4]={Int64GetDatum(current+1),Int64GetDatum(schema+1),Int64GetDatum(nextCatalog),Int64GetDatum(nextFile)};
            LakeMetaRows* claim=LakeMetadataQuery(psprintf("INSERT INTO %s "
                "(snapshot_id,snapshot_time,schema_version,next_catalog_id,next_file_id) "
                "VALUES($1,pg_catalog.now(),$2,$3,$4) ON CONFLICT(snapshot_id) DO NOTHING RETURNING snapshot_id",snapshots),
                4,types,args,NULL,snapshot,false,SPI_OK_INSERT_RETURNING,ns);
            if(claim->count>1) ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("multiple native lake RENAME claims")));
            claimed=claim->count==1;
            if(claimed) {
                LakeWriteRenames(events,count,snapshot,current+1,schema+1);
                StringInfoData text;initStringInfo(&text);
                for(int i=0;i<count;++i) {
                    if(events[i]->column_rename) {
                        if(text.len)appendStringInfoChar(&text,',');
                        appendStringInfo(&text,"altered_table:" INT64_FORMAT,LakePermanentTableId(events[i]->binding));
                    } else LakeAppendCreatedTableChange(&text,events[i]->schema_name,events[i]->new_name);
                }
                Oid cts[2]={INT8OID,TEXTOID};Datum cas[2]={args[0],CStringGetTextDatum(text.data)};
                LakeMetaRows* saved=LakeMetadataQuery(psprintf("INSERT INTO %s "
                    "(snapshot_id,changes_made,author,commit_message,commit_extra_info) VALUES($1,$2,NULL,NULL,NULL)",changes),
                    2,cts,cas,NULL,snapshot,false,SPI_OK_INSERT,ns);
                LakeMetadataRequireCount(saved,1,"RENAME changesets");
                ReleaseCurrentSubTransaction();
                inside=false;state->publication_subtransaction=false;
                LakeInvalidateRelationSafe(first->relation_oid);
                LakeHostActivateDeferredPublish(first, current+1);
            } else {
                RollbackAndReleaseCurrentSubTransaction();
                inside=false;state->publication_subtransaction=false;
            }
            MemoryContextSwitchTo(caller);
            t_thrd.utils_cxt.CurrentResourceOwner=owner;MemoryContextDelete(context);
        }
        PG_CATCH();
        {
            MemoryContextSwitchTo(caller);ErrorData* error=CopyErrorData();FlushErrorState();
            if(inside) RollbackAndReleaseCurrentSubTransaction();
            state->publication_subtransaction=false;t_thrd.utils_cxt.CurrentResourceOwner=owner;
            MemoryContextDelete(context);ReThrowError(error);
        }
        PG_END_TRY();
        if(claimed) return;
        pg_usleep(1000L*(attempt+1));
    }
    ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake RENAME snapshot claim exceeded its retry limit")));
}

/* Append to lake_publish.cpp after transaction mode/version integration.
 * All candidate SDK calls have completed before entering this function. */
static void LakeWriteLateFlushEnds(LakeFlushOperation* op,Snapshot snapshot,
    const char* carrier,int64 file,int64 current)
{
    LakeInlineBinding* binding=LakeFlushBinding(op);Oid ns=binding->metadata_schema_oid;
    Oid types[5]={INT8OID,INT8OID,INT8OID,INT8OID,INT8OID};
    Datum args[5]={Int64GetDatum(op->round.enabled?op->round.birth_cutoff:op->cutoff),Int64GetDatum(op->last_row_id),
        Int64GetDatum(op->last_birth),Int64GetDatum(file),Int64GetDatum(op->cutoff)};
    char* prefix=psprintf("_ducklake_begin_snapshot<=$1 AND "
        "(_ducklake_row_id<$2 OR (_ducklake_row_id=$2 AND _ducklake_begin_snapshot<=$3))");
    /* Business payload, row ID and birth are immutable in a trusted carrier.
     * Other flushes/structural changes are still rejected. Only an end may
     * have advanced since preparation; all such commits precede our PK claim. */
    LakeMetaRows* source=LakeMetadataQuery(psprintf("SELECT count(*),max(_ducklake_end_snapshot),"
        "count(CASE WHEN _ducklake_end_snapshot>$5 THEN 1 END) FROM %s WHERE %s",carrier,prefix),
        5,types,args,NULL,snapshot,true,SPI_OK_SELECT,ns);
    LakeMetadataRequireCount(source,1,"flush source interval checks");
    int64 count=DatumGetInt64(LakeMetadataValue(source,0,1,INT8OID));
    bool noEnd=false;Datum maximum=LakeMetadataValue(source,0,2,INT8OID,true,&noEnd);
    int64 late=DatumGetInt64(LakeMetadataValue(source,0,3,INT8OID));
    if(count!=op->rows)
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native flush source interval changed before publication")));
    if(late<0||late>count||(!noEnd&&DatumGetInt64(maximum)>current))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native flush source end exceeds committed publication state")));
    if(late==0)return;
    char* name=psprintf("ducklake_inlined_delete_%lld",(long long)LakePermanentTableId(binding));
    if(!OidIsValid(get_relname_relid(name,ns))) {
        (void)LakeCreatePrivateCarrier(binding,name,LakeMetadataOwner(),true);
        LakeAdvancePublicationCid(snapshot);
    }
    static const LakeWriteColumn shape[]={{"file_id",INT8OID},{"row_id",INT8OID},{"begin_snapshot",INT8OID}};
    char* deletes=LakeWriteRelation(binding,name,shape,lengthof(shape));
    /* Number the complete prepared interval before filtering late ends.
     * Both keys match the native sort which wrote the immutable Parquet.
     * Preserve the actual deletion snapshot, including intermediate history. */
    LakeMetaRows* written=LakeMetadataQuery(psprintf("INSERT INTO %s(file_id,row_id,begin_snapshot) "
        "SELECT $4,physical_position,_ducklake_end_snapshot FROM "
        "(SELECT _ducklake_end_snapshot,pg_catalog.row_number() OVER "
        "(ORDER BY _ducklake_row_id,_ducklake_begin_snapshot)-1 AS physical_position "
        "FROM %s WHERE %s) AS prepared WHERE _ducklake_end_snapshot>$5",deletes,carrier,prefix),
        5,types,args,NULL,snapshot,false,SPI_OK_INSERT,ns);
    LakeMetadataRequireCount(written,(uint32)late,"late flush position deletions");
}

static void LakeWriteFlushOutputs(LakeFlushOperation* op,Snapshot snapshot,int64 firstFile,int64 current)
{
    LakeInlineBinding* b=LakeFlushBinding(op);Oid ns=b->metadata_schema_oid;
    int64 table=LakePermanentTableId(b);
    char* files=LakeMetadataRelation(ns,"ducklake_data_file");
    char* stats=LakeMetadataRelation(ns,"ducklake_table_stats");
    Oid types[9]={INT8OID,INT8OID,INT8OID,INT8OID,TEXTOID,INT8OID,INT8OID,INT8OID,INT8OID};
    Datum args[9]={Int64GetDatum(firstFile),Int64GetDatum(table),Int64GetDatum(op->min_birth),Int64GetDatum(op->max_birth),
        CStringGetTextDatum(op->data.relative_path),Int64GetDatum(op->rows),Int64GetDatum(op->data.candidate->written.file_bytes),
        Int64GetDatum(op->data.footer_bytes),Int64GetDatum(op->min_row_id)};
    LakeMetaRows* wrote=LakeMetadataQuery(psprintf("INSERT INTO %s(data_file_id,table_id,begin_snapshot,end_snapshot,file_order,path,"
        "path_is_relative,file_format,record_count,file_size_bytes,footer_size,row_id_start,partition_id,encryption_key,"
        "mapping_id,partial_max,row_group_count) SELECT $1,$2,$3,NULL,COALESCE(MAX(file_order),-1)+1,$5,true,'parquet',"
        "$6,$7,$8,$9,NULL,NULL,NULL,$4,%lld FROM %s WHERE table_id=$2",files,(long long)op->data.row_groups,files),
        9,types,args,NULL,snapshot,false,SPI_OK_INSERT,ns);
    LakeMetadataRequireCount(wrote,1,"flush data file registration");
    bool collectBounds=LakeStatsExtendedVersionEnabled();
    for(int c=0;c<b->column_count;++c){
        /* Nested leaf populations are not parent row populations. Omit this
         * optional statistic until the SDK returns a verified leaf summary. */
        if(b->columns[c].type==VECTOROID)continue;
        LakeInsertFileColumnStats(ns,firstFile,table,&b->columns[c],
            op->data.column_bytes[c],op->rows,op->null_counts[c],
            collectBounds&&op->minima?op->minima[c]:NULL,
            collectBounds&&op->maxima?op->maxima[c]:NULL,
            snapshot,"flush file column statistics");
    }
    if(op->deletes.candidate!=NULL){
        char* deleted=LakeMetadataRelation(ns,"ducklake_delete_file");
        Oid dt[8]={INT8OID,INT8OID,INT8OID,INT8OID,INT8OID,TEXTOID,INT8OID,INT8OID};
        Datum da[8]={Int64GetDatum(firstFile+1),args[1],Int64GetDatum(op->min_end),Int64GetDatum(op->max_end),args[0],
            CStringGetTextDatum(op->deletes.relative_path),Int64GetDatum(op->ended_count),Int64GetDatum(op->deletes.candidate->written.file_bytes)};
        wrote=LakeMetadataQuery(psprintf("INSERT INTO %s(delete_file_id,table_id,begin_snapshot,end_snapshot,data_file_id,path,"
            "path_is_relative,format,delete_count,file_size_bytes,footer_size,encryption_key,partial_max,row_group_count) "
            "VALUES($1,$2,$3,NULL,$5,$6,true,'parquet',$7,$8,%lld,NULL,$4,%lld)",deleted,
            (long long)op->deletes.footer_bytes,(long long)op->deletes.row_groups),8,dt,da,NULL,snapshot,false,SPI_OK_INSERT,ns);
        LakeMetadataRequireCount(wrote,1,"flush delete file registration");
    }
    Oid st[2]={INT8OID,INT8OID};Datum sa[2]={args[1],args[6]};
    wrote=LakeMetadataQuery(psprintf("UPDATE %s SET file_size_bytes=file_size_bytes+$2 WHERE table_id=$1 "
        "AND file_size_bytes>=0 AND file_size_bytes<=%lld-$2",stats,(long long)PG_INT64_MAX),
        2,st,sa,NULL,snapshot,false,SPI_OK_UPDATE,ns);
    LakeMetadataRequireCount(wrote,1,"flush table file bytes");
    /* Ordinary MVCC deletion preserves old SQL snapshots. Original begin/end
     * in output files preserve lake time travel for new SQL transactions. */
    const char* inlineName=get_rel_name(b->inline_oid);
    if(inlineName==NULL)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("flush inline carrier disappeared")));
    char* carrier=LakeMetadataRelation(ns,inlineName);
    LakeWriteLateFlushEnds(op,snapshot,carrier,firstFile,current);
    Oid clearTypes[3]={INT8OID,INT8OID,INT8OID};
    Datum clearArgs[3]={Int64GetDatum(op->round.enabled?op->round.birth_cutoff:op->cutoff),
        Int64GetDatum(op->last_row_id),Int64GetDatum(op->last_birth)};
    wrote=LakeMetadataQuery(psprintf("DELETE FROM %s WHERE _ducklake_begin_snapshot<=$1 "
        "AND (_ducklake_row_id<$2 OR (_ducklake_row_id=$2 AND _ducklake_begin_snapshot<=$3))",carrier),
        3,clearTypes,clearArgs,NULL,snapshot,false,SPI_OK_DELETE,ns);
    LakeMetadataRequireCount(wrote,(uint32)op->rows,"exact flushed inline versions");
}

/* Register new merged position files and retire only their old metadata.
 * Physical old objects remain for active SQL snapshots and later safe GC. */
static void LakeWriteFlushedFileDeletes(LakeFlushOperation* op,Snapshot snapshot,int64 firstFile)
{
    LakeInlineBinding* binding=op->table->binding;Oid ns=binding->metadata_schema_oid;
    char* deleted=LakeMetadataRelation(ns,"ducklake_delete_file");
    char* scheduled=LakeMetadataRelation(ns,"ducklake_files_scheduled_for_deletion");
    char* carrier=LakeMetadataRelation(ns,psprintf("ducklake_inlined_delete_%lld",(long long)LakePermanentTableId(binding)));
    for(int i=0;i<op->file_deletion_count;++i){
        LakeFlushFileDeletion* file=&op->file_deletions[i];
        Oid types[8]={INT8OID,INT8OID,INT8OID,INT8OID,INT8OID,TEXTOID,INT8OID,INT8OID};
        Datum args[8]={Int64GetDatum(firstFile+i),Int64GetDatum(LakePermanentTableId(binding)),
            Int64GetDatum(file->min_snapshot),Int64GetDatum(file->max_snapshot),Int64GetDatum(file->file->file_id),
            CStringGetTextDatum(file->output.relative_path),Int64GetDatum(file->rows),
            Int64GetDatum(file->output.candidate->written.file_bytes)};
        LakeMetaRows* wrote=LakeMetadataQuery(psprintf("INSERT INTO %s(delete_file_id,table_id,begin_snapshot,end_snapshot,"
            "data_file_id,path,path_is_relative,format,delete_count,file_size_bytes,footer_size,encryption_key,partial_max,row_group_count) "
            "VALUES($1,$2,$3,NULL,$5,$6,true,'parquet',$7,$8,%lld,NULL,$4,%lld)",deleted,
            (long long)file->output.footer_bytes,(long long)file->output.row_groups),8,types,args,NULL,snapshot,false,SPI_OK_INSERT,ns);
        LakeMetadataRequireCount(wrote,1,"merged position-delete file registration");
        if(file->file->deletes!=NULL){
            LakeDeleteFile* old=file->file->deletes;
            Oid dropTypes[3]={INT8OID,INT8OID,INT8OID};
            Datum dropArgs[3]={Int64GetDatum(old->file_id),args[1],args[4]};
            wrote=LakeMetadataQuery(psprintf("DELETE FROM %s WHERE delete_file_id=$1 AND table_id=$2 AND data_file_id=$3",deleted),
                3,dropTypes,dropArgs,NULL,snapshot,false,SPI_OK_DELETE,ns);
            LakeMetadataRequireCount(wrote,1,"retired position-delete metadata");
            /* Scheduled paths are relative to the catalog root, not table path:
             * this standard table has no table_id once old metadata is gone. */
            Size prefix=strlen(binding->data_root);
            if(strncmp(old->path,binding->data_root,prefix)!=0||old->path[prefix]=='\0')
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("retired deletion file lies outside its catalog root")));
            Oid st[2]={INT8OID,TEXTOID};Datum sa[2]={dropArgs[0],CStringGetTextDatum(old->path+prefix)};
            wrote=LakeMetadataQuery(psprintf("INSERT INTO %s(data_file_id,path,path_is_relative,schedule_start) "
                "VALUES($1,$2,true,pg_catalog.now())",scheduled),2,st,sa,NULL,snapshot,false,SPI_OK_INSERT,ns);
            LakeMetadataRequireCount(wrote,1,"scheduled old deletion object");
        }
        Oid clearTypes[2]={INT8OID,INT8OID};Datum clearArgs[2]={args[4],Int64GetDatum(op->cutoff)};
        wrote=LakeMetadataQuery(psprintf("DELETE FROM %s WHERE file_id=$1 AND begin_snapshot<=$2",carrier),
            2,clearTypes,clearArgs,NULL,snapshot,false,SPI_OK_DELETE,ns);
        LakeMetadataRequireCount(wrote,(uint32)file->inline_rows,"exact flushed inline file-deletion rows");
    }
}

struct LakeBucketLateEnds {
    LakeFlushEnded* rows[LAKE_IDENTITY_FLUSH_GROUP_LIMIT];
    int counts[LAKE_IDENTITY_FLUSH_GROUP_LIMIT];
};
/* Existing publication snapshot/claim already freezes the committed source.
 * Validate every prepared R/birth in one bounded result; never compare original
 * values to a bucket ID and never rehash payload under the commit claim. */
static LakeBucketLateEnds LakeReadBucketLateEnds(LakeFlushOperation* op,Snapshot snapshot,
    int64 current,const char* carrier,const char* prefix,const Datum* sourceArgs)
{
    LakeBucketLateEnds result={};
    if(!op->version_map||op->rows<1||op->rows>LAKE_PARTITION_FLUSH_MAP_LIMIT||
       op->version_map_count!=op->rows||op->version_map_capacity<op->version_map_count||
       op->version_map_capacity>LAKE_PARTITION_FLUSH_MAP_LIMIT)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("bucket publication lacks its bounded original source map")));
    int seenByGroup[LAKE_IDENTITY_FLUSH_GROUP_LIMIT]={};int64 groupedRows=0;
    for(int g=0;g<op->group_count;++g){
        if(op->groups[g].rows<1||op->groups[g].rows>op->rows-groupedRows)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("bucket group population is outside its original source")));
        groupedRows+=op->groups[g].rows;
        result.rows[g]=(LakeFlushEnded*)palloc(sizeof(LakeFlushEnded)*(Size)op->groups[g].rows);
    }
    if(groupedRows!=op->rows)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("bucket groups do not cover their original source")));
    MemoryContext parent=CurrentMemoryContext;
    MemoryContext page=AllocSetContextCreate(parent,"Lake bucket publication original source",ALLOCSET_DEFAULT_SIZES);
    MemoryContextSwitchTo(page);
    /* One source pass/sort, not repeated keyset scans of an unindexed carrier.
     * Only three fixed int8 columns and at most262145rows are materialized.
     * The +1 boundary rejects any extra cohort version before metadata writes. */
    Oid types[3]={INT8OID,INT8OID,INT8OID};
    Datum args[3]={sourceArgs[0],sourceArgs[1],sourceArgs[2]};
    LakeMetaRows* rows=LakeMetadataQuery(psprintf("SELECT _ducklake_row_id,_ducklake_begin_snapshot,_ducklake_end_snapshot "
        "FROM %s WHERE %s ORDER BY _ducklake_row_id,_ducklake_begin_snapshot LIMIT %d",
        carrier,prefix,op->version_map_count+1),3,types,args,NULL,snapshot,true,SPI_OK_SELECT,op->table->binding->metadata_schema_oid);
    if(rows->count!=(uint32)op->version_map_count)
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("bucket source cohort changed before publication")));
    int offset=0;
    for(uint32 r=0;r<rows->count;++r){
        CHECK_FOR_INTERRUPTS();const auto& mapped=op->version_map[offset++];
        int64 rowid=DatumGetInt64(LakeMetadataValue(rows,r,1,INT8OID));
        int64 birth=DatumGetInt64(LakeMetadataValue(rows,r,2,INT8OID));
        bool absent=false;Datum raw=LakeMetadataValue(rows,r,3,INT8OID,true,&absent);
        int64 ended=absent?-1:DatumGetInt64(raw);
        if(rowid!=mapped.row_id||birth!=mapped.birth||mapped.group<0||mapped.group>=op->group_count||
           mapped.position!=seenByGroup[mapped.group]++||mapped.position>=op->groups[mapped.group].rows||
           (!absent&&(ended<0||ended<=birth||ended>current))||mapped.captured_end<-1||
           (mapped.captured_end>=0?ended!=mapped.captured_end:(ended>=0&&ended<=op->cutoff)))
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("bucket original source identity or end changed before publication")));
        if(ended>op->cutoff){
            int g=mapped.group;int n=result.counts[g]++;
            if(n>=op->groups[g].rows)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("bucket late end exceeds its source group")));
            result.rows[g][n]={mapped.position,ended};
        }
    }
    MemoryContextSwitchTo(parent);MemoryContextDelete(page);
    for(int g=0;g<op->group_count;++g)if(seenByGroup[g]!=op->groups[g].rows)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("bucket original group has incomplete physical position coverage")));
    return result;
}
static void LakeWriteBucketLateEnds(LakeFlushOperation* op,Snapshot snapshot,int64 dataId,
    const LakeFlushEnded* rows,int count)
{
    if(count==0)return;
    LakeInlineBinding* b=op->table->binding;Oid ns=b->metadata_schema_oid;
    char* name=psprintf("ducklake_inlined_delete_%lld",(long long)LakePermanentTableId(b));
    if(!OidIsValid(get_relname_relid(name,ns))){
        (void)LakeCreatePrivateCarrier(b,name,LakeMetadataOwner(),true);LakeAdvancePublicationCid(snapshot);
    }
    static const LakeWriteColumn shape[]={{"file_id",INT8OID},{"row_id",INT8OID},{"begin_snapshot",INT8OID}};
    const char* deletes=LakeWriteRelation(b,name,shape,lengthof(shape));
    MemoryContext parent=CurrentMemoryContext;
    MemoryContext batch=AllocSetContextCreate(parent,"Lake bucket late end publication batch",ALLOCSET_DEFAULT_SIZES);
    for(int first=0;first<count;first+=256){
        CHECK_FOR_INTERRUPTS();MemoryContextReset(batch);MemoryContextSwitchTo(batch);
        int n=Min(256,count-first);Oid* types=(Oid*)palloc(sizeof(Oid)*3*n);Datum* args=(Datum*)palloc(sizeof(Datum)*3*n);
        StringInfoData sql;initStringInfo(&sql);appendStringInfo(&sql,"INSERT INTO %s(file_id,row_id,begin_snapshot) VALUES",deletes);
        for(int i=0;i<n;++i){
            if(i)appendStringInfoChar(&sql,',');appendStringInfo(&sql,"($%d,$%d,$%d)",3*i+1,3*i+2,3*i+3);
            types[3*i]=types[3*i+1]=types[3*i+2]=INT8OID;
            args[3*i]=Int64GetDatum(dataId);args[3*i+1]=Int64GetDatum(rows[first+i].position);args[3*i+2]=Int64GetDatum(rows[first+i].end_snapshot);
        }
        LakeMetadataRequireCount(LakeMetadataQuery(sql.data,3*n,types,args,NULL,snapshot,false,SPI_OK_INSERT,ns),
            (uint32)n,"bucket original group-local late-end positions");
        MemoryContextSwitchTo(parent);
    }
    MemoryContextDelete(batch);
}

/* Register all identity groups, transfer late ends by each file's own ordinal,
 * and only then remove the one original global identity interval. */
static void LakeWriteIdentityFlushOutputs(LakeFlushOperation* op,Snapshot snapshot,int64 firstFile,int64 current)
{
    LakeInlineBinding* b=LakeFlushBinding(op);Oid ns=b->metadata_schema_oid;
    if(op->group_count<1||op->group_count>LAKE_IDENTITY_FLUSH_GROUP_LIMIT||!op->source_partition.enabled)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid prepared identity flush groups")));
    const char* inlineName=get_rel_name(b->inline_oid);
    if(!inlineName)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("identity flush carrier disappeared")));
    const char* carrier=LakeMetadataRelation(ns,inlineName);
    const char* files=LakeMetadataRelation(ns,"ducklake_data_file");
    const char* prefix="_ducklake_begin_snapshot<=$1 AND (_ducklake_row_id<$2 OR "
        "(_ducklake_row_id=$2 AND _ducklake_begin_snapshot<=$3))";
    Oid sourceTypes[3]={INT8OID,INT8OID,INT8OID};
    Datum sourceArgs[3]={Int64GetDatum(op->round.birth_cutoff),Int64GetDatum(op->last_row_id),Int64GetDatum(op->last_birth)};
    LakeMetaRows* source=LakeMetadataQuery(psprintf("SELECT count(*)::bigint FROM %s WHERE %s",carrier,prefix),
        3,sourceTypes,sourceArgs,NULL,snapshot,true,SPI_OK_SELECT,ns);
    LakeMetadataRequireCount(source,1,"identity flush source interval");
    if(DatumGetInt64(LakeMetadataValue(source,0,1,INT8OID))!=op->rows)
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("identity flush source interval changed")));
    int keyCount=op->source_partition.key_count;
    if(keyCount<1||keyCount>(LakeBucketPartitionVersionEnabled()?MaxHeapAttributeNumber:b->column_count))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("identity flush partition key count changed")));
    for(int k=0;k<keyCount;++k){
        const auto& key=op->source_partition.keys[k];int c=key.column_index;
        if(c<0||c>=b->column_count||b->columns[c].column_id!=key.column_id||b->columns[c].type!=key.type||
           !LakePartitionFieldSupported(&key))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("identity flush partition column changed")));
    }
    /* Every many-to-one transform needs original source R/B -> group/P.
     * A SQL equality predicate on calendar output cannot recover the mapping. */
    bool mappedSource=LakePartitionNeedsSourceVersionMap(&op->source_partition);
    LakeBucketLateEnds lateEnds={};
    if(mappedSource)lateEnds=LakeReadBucketLateEnds(op,snapshot,current,carrier,prefix,sourceArgs);
    int64 next=firstFile,totalRows=0,totalBytes=0;
    for(int group=0;group<op->group_count;++group){
        LakeIdentityFlushGroup* g=&op->groups[group];int64 dataId=next++;
        if(!g->data.candidate||g->rows<1||g->rows>op->rows-totalRows||g->data.candidate->written.num_rows!=g->rows||
           g->data.candidate->written.file_bytes<0||totalBytes>PG_INT64_MAX-g->data.candidate->written.file_bytes)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid identity flush output population")));
        totalRows+=g->rows;totalBytes+=g->data.candidate->written.file_bytes;
        Oid types[11]={INT8OID,INT8OID,INT8OID,INT8OID,TEXTOID,INT8OID,INT8OID,INT8OID,INT8OID,INT8OID,INT8OID};
        Datum args[11]={Int64GetDatum(dataId),Int64GetDatum(LakePermanentTableId(b)),Int64GetDatum(g->min_birth),
            Int64GetDatum(g->max_birth),CStringGetTextDatum(g->data.relative_path),Int64GetDatum(g->rows),
            Int64GetDatum(g->data.candidate->written.file_bytes),Int64GetDatum(g->data.footer_bytes),
            Int64GetDatum(g->min_row_id),Int64GetDatum(op->source_partition.partition_id),Int64GetDatum(g->data.row_groups)};
        LakeMetaRows* wrote=LakeMetadataQuery(psprintf("INSERT INTO %s(data_file_id,table_id,begin_snapshot,end_snapshot,file_order,path,"
            "path_is_relative,file_format,record_count,file_size_bytes,footer_size,row_id_start,partition_id,encryption_key,"
            "mapping_id,partial_max,row_group_count) SELECT $1,$2,$3,NULL,COALESCE(MAX(file_order),-1)+1,$5,true,'parquet',"
            "$6,$7,$8,$9,$10,NULL,NULL,$4,$11 FROM %s WHERE table_id=$2",files,files),
            11,types,args,NULL,snapshot,false,SPI_OK_INSERT,ns);
        LakeMetadataRequireCount(wrote,1,"identity flush data file");
        if(g->key.count!=keyCount||!g->key.values)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("identity flush output lacks its complete tuple")));
        for(int k=0;k<keyCount;++k){
            const auto& value=g->key.values[k];
            if(!value.is_null&&!value.canonical_text)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("identity flush partition value lost its encoding")));
            Oid pt[4]={INT8OID,INT8OID,INT8OID,TEXTOID};
            Datum pa[4]={args[0],args[1],Int64GetDatum(k),value.is_null?(Datum)0:CStringGetTextDatum(value.canonical_text)};
            char pn[5]={' ',' ',' ',value.is_null?'n':' ','\0'};
            wrote=LakeMetadataQuery(psprintf("INSERT INTO %s(data_file_id,table_id,partition_key_index,partition_value) "
                "VALUES($1,$2,$3,$4)",LakeMetadataRelation(ns,"ducklake_file_partition_value")),
                4,pt,pa,pn,snapshot,false,SPI_OK_INSERT,ns);
            LakeMetadataRequireCount(wrote,1,"identity flush file partition value");
        }
        bool collectBounds=LakeStatsExtendedVersionEnabled();
        for(int c=0;c<b->column_count;++c){
            if(b->columns[c].type==VECTOROID)continue;
            if(g->null_counts[c]<0||g->null_counts[c]>g->rows)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid identity flush null count")));
            int keyIndex=LakePartitionFindIdentityKey(&op->source_partition,b->columns[c].column_id);
            const char* minVal=(collectBounds&&g->minima)?g->minima[c]:NULL;
            const char* maxVal=(collectBounds&&g->maxima)?g->maxima[c]:NULL;
            if(minVal==NULL&&maxVal==NULL&&keyIndex>=0&&!g->key.values[keyIndex].is_null)
                minVal=maxVal=g->key.values[keyIndex].canonical_text;
            LakeInsertFileColumnStats(ns,DatumGetInt64(args[0]),DatumGetInt64(args[1]),&b->columns[c],
                g->data.column_bytes[c],g->rows,g->null_counts[c],
                minVal,maxVal,snapshot,"identity flush file column statistics");
        }
        if(g->ended_count){
            if(!g->deletes.candidate||g->deletes.candidate->written.num_rows!=g->ended_count)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("identity flush ended output differs from source")));
            Oid dt[8]={INT8OID,INT8OID,INT8OID,INT8OID,INT8OID,TEXTOID,INT8OID,INT8OID};
            Datum da[8]={Int64GetDatum(next++),args[1],Int64GetDatum(g->min_end),Int64GetDatum(g->max_end),args[0],
                CStringGetTextDatum(g->deletes.relative_path),Int64GetDatum(g->ended_count),Int64GetDatum(g->deletes.candidate->written.file_bytes)};
            wrote=LakeMetadataQuery(psprintf("INSERT INTO %s(delete_file_id,table_id,begin_snapshot,end_snapshot,data_file_id,path,"
                "path_is_relative,format,delete_count,file_size_bytes,footer_size,encryption_key,partial_max,row_group_count) "
                "VALUES($1,$2,$3,NULL,$5,$6,true,'parquet',$7,$8,%lld,NULL,$4,%lld)",
                LakeMetadataRelation(ns,"ducklake_delete_file"),(long long)g->deletes.footer_bytes,(long long)g->deletes.row_groups),
                8,dt,da,NULL,snapshot,false,SPI_OK_INSERT,ns);
            LakeMetadataRequireCount(wrote,1,"identity flush ended-version file");
        }
        if(mappedSource){
            LakeWriteBucketLateEnds(op,snapshot,dataId,lateEnds.rows[group],lateEnds.counts[group]);
            continue;
        }
        /* Re-number exactly this prepared group's original interval before
         * filtering late ends. Native payload/R/birth/key are immutable. */
        int lateParams=keyCount+5,cutoffParam=lateParams;
        Oid* lt=(Oid*)palloc(sizeof(Oid)*lateParams);Datum* la=(Datum*)palloc0(sizeof(Datum)*lateParams);
        char* ln=(char*)palloc(lateParams+1);memset(ln,' ',lateParams);ln[lateParams]='\0';
        for(int i=0;i<4;++i)lt[i]=INT8OID;
        la[0]=sourceArgs[0];la[1]=sourceArgs[1];la[2]=sourceArgs[2];la[3]=args[0];
        StringInfoData groupPrefix;initStringInfo(&groupPrefix);appendStringInfoString(&groupPrefix,prefix);
        for(int k=0;k<keyCount;++k){
            const auto& key=op->source_partition.keys[k];const auto& value=g->key.values[k];
            lt[4+k]=key.type;la[4+k]=value.is_null?(Datum)0:value.value;if(value.is_null)ln[4+k]='n';
            const char* physicalKey=quote_identifier(LakeInlinePhysicalName(&b->columns[key.column_index]));
            /* Full byte-identity grouping for text. Host query collation is a
             * separate residual-filter concern and cannot merge file groups. */
            const char* collate=key.type==TEXTOID?" COLLATE pg_catalog.\"C\"":"";
            appendStringInfo(&groupPrefix," AND %s%s IS NOT DISTINCT FROM $%d%s",physicalKey,collate,5+k,collate);
        }
        lt[lateParams-1]=INT8OID;la[lateParams-1]=Int64GetDatum(op->cutoff);
        LakeMetaRows* late=LakeMetadataQuery(psprintf("SELECT count(*)::bigint,max(_ducklake_end_snapshot),"
            "count(CASE WHEN _ducklake_end_snapshot>$%d THEN 1 END)::bigint FROM %s WHERE %s",cutoffParam,carrier,groupPrefix.data),
            lateParams,lt,la,ln,snapshot,true,SPI_OK_SELECT,ns);
        LakeMetadataRequireCount(late,1,"identity flush group late-end check");
        bool noEnd=false;Datum maximum=LakeMetadataValue(late,0,2,INT8OID,true,&noEnd);
        int64 lateCount=DatumGetInt64(LakeMetadataValue(late,0,3,INT8OID));
        if(DatumGetInt64(LakeMetadataValue(late,0,1,INT8OID))!=g->rows||lateCount<0||lateCount>g->rows||
           (!noEnd&&DatumGetInt64(maximum)>current))
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("identity flush group changed before publication")));
        if(lateCount){
            char* name=psprintf("ducklake_inlined_delete_%lld",(long long)LakePermanentTableId(b));
            if(!OidIsValid(get_relname_relid(name,ns))){
                (void)LakeCreatePrivateCarrier(b,name,LakeMetadataOwner(),true);LakeAdvancePublicationCid(snapshot);
            }
            static const LakeWriteColumn shape[]={{"file_id",INT8OID},{"row_id",INT8OID},{"begin_snapshot",INT8OID}};
            const char* deletes=LakeWriteRelation(b,name,shape,lengthof(shape));
            wrote=LakeMetadataQuery(psprintf("INSERT INTO %s(file_id,row_id,begin_snapshot) "
                "SELECT $4,physical_position,_ducklake_end_snapshot FROM "
                "(SELECT _ducklake_end_snapshot,pg_catalog.row_number() OVER "
                "(ORDER BY _ducklake_row_id,_ducklake_begin_snapshot)-1 AS physical_position FROM %s WHERE %s) "
                "AS prepared WHERE _ducklake_end_snapshot>$%d",deletes,carrier,groupPrefix.data,cutoffParam),
                lateParams,lt,la,ln,snapshot,false,SPI_OK_INSERT,ns);
            LakeMetadataRequireCount(wrote,(uint32)lateCount,"identity flush group-local late-end positions");
        }
    }
    if(totalRows!=op->rows||next-firstFile!=LakeFlushDataCandidateCount(op))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("identity flush total rows or output slots differ")));
    Oid st[2]={INT8OID,INT8OID};Datum sa[2]={Int64GetDatum(LakePermanentTableId(b)),Int64GetDatum(totalBytes)};
    LakeMetaRows* wrote=LakeMetadataQuery(psprintf("UPDATE %s SET file_size_bytes=file_size_bytes+$2 WHERE table_id=$1 "
        "AND file_size_bytes>=0 AND file_size_bytes<=%lld-$2",LakeMetadataRelation(ns,"ducklake_table_stats"),(long long)PG_INT64_MAX),
        2,st,sa,NULL,snapshot,false,SPI_OK_UPDATE,ns);
    LakeMetadataRequireCount(wrote,1,"identity flush total file bytes");
    wrote=LakeMetadataQuery(psprintf("DELETE FROM %s WHERE %s",carrier,prefix),
        3,sourceTypes,sourceArgs,NULL,snapshot,false,SPI_OK_DELETE,ns);
    LakeMetadataRequireCount(wrote,(uint32)op->rows,"identity flush single source-interval removal");
}

/* H is scheduling metadata on the already canonical host binding, never a
 * DuckLake row birth or file partial_max. CAS closes/advances it atomically with
 * exactly this operation's source removal; no new source scan/SDK is required. */
static void LakeWriteIdentityFlushRound(LakeFlushOperation* op,Snapshot snapshot)
{
    if(!op->round.enabled||(!op->round.existing&&op->round.eligible_rows==0))return;
    LakeInlineBinding* b=LakeFlushBinding(op);
    if(op->round.eligible_rows<op->rows||op->round.birth_cutoff<0||op->round.birth_cutoff>op->cutoff)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid identity flush round population")));
    Oid types[11]={UUIDOID,UUIDOID,OIDOID,OIDOID,INT8OID,OIDOID,INT8OID,INT8OID,BOOLOID,BOOLOID,INT8OID};
    Datum args[11]={DirectFunctionCall1(uuid_in,CStringGetDatum(b->binding_id)),
        DirectFunctionCall1(uuid_in,CStringGetDatum(b->table_uuid)),ObjectIdGetDatum(b->server_oid),
        ObjectIdGetDatum(b->relation_oid),Int64GetDatum(op->round.birth_cutoff),ObjectIdGetDatum(b->inline_oid),
        Int64GetDatum(b->inline_schema_version),Int64GetDatum(b->inline_origin_snapshot),
        BoolGetDatum(op->round.eligible_rows==op->rows),BoolGetDatum(op->round.existing),Int64GetDatum(LakePermanentTableId(b))};
    LakeMetaRows* wrote=LakeMetadataQuery("UPDATE og_lake_internal.table_binding t SET "
        "flush_birth_cutoff=CASE WHEN $9 THEN NULL ELSE $5 END,flush_carrier_oid=CASE WHEN $9 THEN NULL ELSE $6 END,"
        "flush_source_schema=CASE WHEN $9 THEN NULL ELSE $7 END,flush_source_origin=CASE WHEN $9 THEN NULL ELSE $8 END "
        "WHERE t.binding_id=$1 AND t.table_uuid=$2 AND t.server_oid=$3 AND t.table_id=$11 "
        "AND t.state='active' AND NOT t.read_only AND EXISTS(SELECT 1 FROM og_lake_internal.relation_binding r "
        "WHERE r.binding_id=t.binding_id AND r.relation_oid=$4) AND "
        "((NOT $10 AND t.flush_birth_cutoff IS NULL AND t.flush_carrier_oid IS NULL AND t.flush_source_schema IS NULL AND t.flush_source_origin IS NULL) "
        "OR ($10 AND t.flush_birth_cutoff=$5 AND t.flush_carrier_oid=$6 AND t.flush_source_schema=$7 AND t.flush_source_origin=$8)) "
        "RETURNING t.binding_id",11,types,args,NULL,snapshot,false,SPI_OK_UPDATE_RETURNING);
    if(wrote->count==0)
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("identity flush round changed before publication")));
    LakeMetadataRequireCount(wrote,1,"identity flush round compare-and-set");
}
static void LakeCheckIdentityFlushRoundSource(const LakeFlushOperation* op,const LakeInlineBinding* check)
{
    const LakeInlineBinding* first=LakeFlushBinding(op);
    const LakeInlineBinding* logical=op->table->binding;
    const LakeInlineSource* source=check->inline_sources_loaded?LakeFindInlineSource(check,first->inline_oid):NULL;
    const LakeInlineSource* captured=logical->inline_sources_loaded?LakeFindInlineSource(logical,first->inline_oid):NULL;
    Oid sourceOid=source?source->relation_oid:check->inline_sources_loaded?InvalidOid:check->inline_oid;
    int64 sourceSchema=source?source->schema_version:check->inline_schema_version;
    int64 sourceOrigin=source?source->origin_snapshot:check->inline_origin_snapshot;
    const LakePartitionSpec* sourcePartition=source?&source->partition:&check->inline_partition;
    if(check->read_only||check->server_oid!=first->server_oid||check->metadata_schema_oid!=first->metadata_schema_oid||
       sourceOid!=first->inline_oid||strcmp(check->table_uuid,first->table_uuid)||
       sourceSchema!=first->inline_schema_version||sourceOrigin!=first->inline_origin_snapshot||
       !LakeIdentityPartitionSpecEqual(&op->source_partition,sourcePartition)||
       !LakeIdentityPartitionSpecEqual(&logical->partition,&check->partition))
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("identity flush source changed after claim")));
    /* The current logical schema and the selected historical physical schema
     * are independent fences. ADD may make their column counts different. */
    if(!LakeColumnLayoutsEqual(logical,check)||check->column_count!=logical->column_count||
       check->inline_write_schema_version!=logical->inline_write_schema_version||
       check->inline_write_origin_snapshot!=logical->inline_write_origin_snapshot||
       strcmp(check->binding_id,logical->binding_id))
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("identity flush logical schema changed after claim")));
    for(int c=0;c<logical->column_count;++c){
        const LakeInlineColumn* old=&logical->columns[c];const LakeInlineColumn* now=&check->columns[c];
        if(old->column_id!=now->column_id||old->type!=now->type||old->typmod!=now->typmod||
           old->element_column_id!=now->element_column_id||old->output_attnum!=now->output_attnum||
           !LakeColumnDefaultsEqual(old,now)||
           strcmp(old->name,now->name))
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("identity flush logical field map changed after claim")));
    }
    if(source){
        if(!captured||source->column_count!=first->column_count||source->column_count!=captured->column_count||
           strcmp(source->table_name,captured->table_name))
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("identity flush physical schema changed after claim")));
        for(int c=0;c<logical->column_count;++c)
            if(source->logical_to_source[c]!=captured->logical_to_source[c])
                ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("identity flush physical field map changed after claim")));
        for(int c=0;c<first->column_count;++c){
            const LakeInlineColumn* old=&first->columns[c];const LakeInlineColumn* now=&source->columns[c];
            if(old->column_id!=now->column_id||old->type!=now->type||old->typmod!=now->typmod||
               old->element_column_id!=now->element_column_id||strcmp(old->name,now->name)||
               !LakeColumnDefaultsEqual(old,now)||strcmp(LakeInlinePhysicalName(old),source->column_names[c]))
                ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("identity flush physical fields changed after claim")));
        }
    }else if(captured)
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("identity flush historical source disappeared after claim")));
}

void LakePublishInlineFlush(LakeTxnState* state)
{
    LakeFlushOperation* op=state->flush;
    if(op==NULL)return;
    if(op->rows==0&&op->file_deletion_count==0){
        /* A stale completed host round has no lake change to publish. Close it
         * using fresh exact-source validation, carrier emptiness verification,
         * and the same old-state CAS; no empty lake snapshot and no SDK I/O. */
        if(op->round.enabled&&op->round.existing){
            /* The ordinary committed-binding loader retains the catalog guard
             * and business-relation AS lock. Match normal publication option
             * protection before taking the fresh source/CAS snapshot. */
            LakeInlineBinding* first=LakeFlushBinding(op);
            Oid ns=first->metadata_schema_oid;
            Relation options=heap_open(get_relname_relid("ducklake_metadata",ns),ShareLock);heap_close(options,NoLock);
            Snapshot fresh=GetLatestSnapshot();
            LakeCheckIdentityFlushRoundSource(op,LakeLoadCommittedInlineBinding(op->table->binding->relation_oid,fresh));
            if(OidIsValid(first->inline_oid)){
                const char* inlineName=get_rel_name(first->inline_oid);
                if(!inlineName)
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("identity flush carrier disappeared")));
                const char* carrier=LakeMetadataRelation(ns,inlineName);
                Oid ct[1]={INT8OID};Datum ca[1]={Int64GetDatum(op->round.birth_cutoff)};
                LakeMetaRows* remain=LakeMetadataQuery(psprintf("SELECT 1 FROM %s WHERE "
                    "_ducklake_begin_snapshot IS NULL OR _ducklake_begin_snapshot<=$1 LIMIT 1",carrier),
                    1,ct,ca,NULL,fresh,true,SPI_OK_SELECT,ns);
                if(remain->count!=0)
                    ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                        errmsg("identity flush round carrier has eligible rows")));
            }
            Snapshot own=LakeCreatePushOwnWriteSnapshot(fresh);LakeWriteIdentityFlushRound(op,own);PopActiveSnapshot();
        }
        return;
    }
    LakeInlineBinding* first=LakeFlushBinding(op);Oid ns=first->metadata_schema_oid;
    const LakeInlineBinding* logical=op->table->binding;
    LakeTxnCatalog* catalog=op->table->catalog;
    int dataFiles=LakeFlushDataCandidateCount(op);
    int files=dataFiles+op->file_deletion_count;
    ResourceOwner owner=t_thrd.utils_cxt.CurrentResourceOwner;MemoryContext caller=CurrentMemoryContext;
    for(int attempt=0;attempt<16;++attempt){
        CHECK_FOR_INTERRUPTS();
        MemoryContext context=AllocSetContextCreate(state->context,"Native lake flush publication attempt",
            ALLOCSET_DEFAULT_MINSIZE,ALLOCSET_DEFAULT_INITSIZE,ALLOCSET_DEFAULT_MAXSIZE);
        volatile bool inside=false;bool claimed=false;
        PG_TRY();
        {
            state->publication_subtransaction=true;BeginInternalSubTransaction(NULL);inside=true;MemoryContextSwitchTo(context);
            Relation options=heap_open(get_relname_relid("ducklake_metadata",ns),ShareLock);heap_close(options,NoLock);
            Snapshot snapshot=GetTransactionSnapshot();
            char* snapshots=LakeWriteRelation(first,"ducklake_snapshot",snapshotColumns,lengthof(snapshotColumns),true);
            char* changes=LakeWriteRelation(first,"ducklake_snapshot_changes",changesColumns,lengthof(changesColumns),true);
            LakeMetaRows* head=LakeMetadataQuery(psprintf("SELECT snapshot_id,schema_version,next_catalog_id,next_file_id FROM %s "
                "ORDER BY snapshot_id DESC LIMIT 1",snapshots),0,NULL,NULL,NULL,snapshot,true,SPI_OK_SELECT,ns);
            LakeMetadataRequireCount(head,1,"flush publication head");
            int64 current=DatumGetInt64(LakeMetadataValue(head,0,1,INT8OID));
            int64 schema=DatumGetInt64(LakeMetadataValue(head,0,2,INT8OID));
            int64 nextCatalog=DatumGetInt64(LakeMetadataValue(head,0,3,INT8OID));
            int64 nextFile=DatumGetInt64(LakeMetadataValue(head,0,4,INT8OID));
            if(current<op->cutoff||current==PG_INT64_MAX||schema<0||nextCatalog<0||nextFile<0||nextFile>PG_INT64_MAX-files)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid native flush publication counters")));
            Oid ct[2]={INT8OID,INT8OID};Datum ca[2]={Int64GetDatum(op->cutoff),Int64GetDatum(current)};
            LakeMetaRows* later=LakeMetadataQuery(psprintf("SELECT changes_made::text FROM %s WHERE snapshot_id>$1 "
                "AND snapshot_id<=$2 ORDER BY snapshot_id",changes),2,ct,ca,NULL,snapshot,true,SPI_OK_SELECT,ns);
            if((uint64)later->count!=(uint64)(current-op->cutoff))
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native flush commit history is incomplete")));
            /* Known native row changes can merge: prepared payloads never
             * change, late inline ends get exact position marks, and later
             * file-origin marks remain outside the cutoff-bounded transfer.
             * Structural changes and other flushes still abort the attempt. */
            LakeCheckConcurrentChanges(later,catalog,LakeHasFlushVersion() &&
                OidIsValid(LakeMetadataOwner()) &&
                u_sess->utils_cxt.XactIsoLevel==XACT_READ_COMMITTED);
            LakeInlineBinding* now=LakeLoadCommittedInlineBinding(first->relation_oid,snapshot);
            if(op->source_binding&&OidIsValid(first->inline_oid))LakeCheckIdentityFlushRoundSource(op,now);
            if(!LakeColumnLayoutsEqual(logical,now)||now->read_only||(op->rows>0&&!op->source_binding&&now->inline_oid!=first->inline_oid)||LakePermanentTableId(now)!=LakePermanentTableId(first)||
                strcmp(now->table_uuid,first->table_uuid)!=0||now->column_count!=logical->column_count||
                now->data_root==NULL||first->data_root==NULL||strcmp(now->data_root,first->data_root)!=0)
                ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native flush binding changed before publication")));
            if(!LakeIdentityPartitionSpecEqual(&logical->partition,&now->partition)||
               (!op->source_binding&&!LakeIdentityPartitionSpecEqual(&first->inline_partition,&now->inline_partition)))
                ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native flush partition setting or source schema changed")));
            for(int c=0;c<logical->column_count;++c)
                if(now->columns[c].column_id!=logical->columns[c].column_id||now->columns[c].type!=logical->columns[c].type||
                    now->columns[c].element_column_id!=logical->columns[c].element_column_id||
                    !LakeColumnDefaultsEqual(&now->columns[c],&logical->columns[c])||
                    strcmp(now->columns[c].name,logical->columns[c].name)!=0)
                    ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native flush schema changed before publication")));
            LakeCheckWriteOptions(first,snapshot);
            Oid types[4]={INT8OID,INT8OID,INT8OID,INT8OID};
            Datum args[4]={Int64GetDatum(current+1),Int64GetDatum(schema),Int64GetDatum(nextCatalog),Int64GetDatum(nextFile+files)};
            LakeMetaRows* claim=LakeMetadataQuery(psprintf("INSERT INTO %s(snapshot_id,snapshot_time,schema_version,next_catalog_id,"
                "next_file_id) VALUES($1,pg_catalog.now(),$2,$3,$4) ON CONFLICT(snapshot_id) DO NOTHING RETURNING snapshot_id",snapshots),
                4,types,args,NULL,snapshot,false,SPI_OK_INSERT_RETURNING,ns);
            if(claim->count>1)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("multiple native flush snapshot claims")));
            claimed=claim->count==1;
            if(claimed){
                Snapshot verified=snapshot;
                if(op->round.enabled){
                    verified=GetLatestSnapshot();
                    LakeCheckIdentityFlushRoundSource(op,LakeLoadCommittedInlineBinding(first->relation_oid,verified));
                }
                Snapshot own=LakeCreatePushOwnWriteSnapshot(verified);
                if(op->rows>0){
                    if(op->group_count)LakeWriteIdentityFlushOutputs(op,own,nextFile,current);
                    else LakeWriteFlushOutputs(op,own,nextFile,current);
                }
                if(op->file_deletion_count>0)LakeWriteFlushedFileDeletes(op,own,nextFile+dataFiles);
                LakeWriteIdentityFlushRound(op,own);
                Oid changeTypes[2]={INT8OID,TEXTOID};
                StringInfoData description;initStringInfo(&description);
                if(op->rows>0)appendStringInfo(&description,"inline_flush:%lld",(long long)LakePermanentTableId(first));
                if(op->file_deletion_count>0){
                    if(description.len)appendStringInfoChar(&description,',');
                    appendStringInfo(&description,"deleted_from_table:%lld",(long long)LakePermanentTableId(first));
                }
                Datum changeArgs[2]={args[0],CStringGetTextDatum(description.data)};
                LakeMetaRows* saved=LakeMetadataQuery(psprintf("INSERT INTO %s(snapshot_id,changes_made,author,commit_message,commit_extra_info) "
                    "VALUES($1,$2,NULL,NULL,NULL)",changes),2,changeTypes,changeArgs,NULL,own,false,SPI_OK_INSERT,ns);
                LakeMetadataRequireCount(saved,1,"flush published changeset");PopActiveSnapshot();
                if(op->data.candidate)LakeFileCandidatePublish(op->data.candidate);
                for(int g=0;g<op->group_count;++g){
                    LakeFileCandidatePublish(op->groups[g].data.candidate);
                    if(op->groups[g].deletes.candidate)LakeFileCandidatePublish(op->groups[g].deletes.candidate);
                }
                for(int i=0;i<op->file_deletion_count;++i)LakeFileCandidatePublish(op->file_deletions[i].output.candidate);
                if(op->deletes.candidate)LakeFileCandidatePublish(op->deletes.candidate);
                ReleaseCurrentSubTransaction();
                inside=false;state->publication_subtransaction=false;
                LakeInvalidateRelationSafe(first->relation_oid);
                LakeHostActivateDeferredPublish(first, current+1);
            }else {
                RollbackAndReleaseCurrentSubTransaction();
                inside=false;state->publication_subtransaction=false;
            }
            MemoryContextSwitchTo(caller);
            t_thrd.utils_cxt.CurrentResourceOwner=owner;MemoryContextDelete(context);
        }
        PG_CATCH();
        {
            MemoryContextSwitchTo(caller);ErrorData* error=CopyErrorData();FlushErrorState();
            if(inside)RollbackAndReleaseCurrentSubTransaction();
            state->publication_subtransaction=false;
            t_thrd.utils_cxt.CurrentResourceOwner=owner;MemoryContextDelete(context);ReThrowError(error);
        }
        PG_END_TRY();
        if(claimed)return;
        pg_usleep(1000L*(attempt+1));
    }
    ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native flush snapshot publication exceeded retry limit")));
}

static void LakeWriteRewriteOutput(LakeRewriteOperation* op,Snapshot own,int64 nextFile,int64 publish)
{
    LakeInlineBinding* b=op->table->binding;Oid ns=b->metadata_schema_oid;
    char* data=LakeMetadataRelation(ns,"ducklake_data_file");char* deleted=LakeMetadataRelation(ns,"ducklake_delete_file");
    int64 oldBytes=0;
    for(int f=0;f<op->source_count;++f){
        int64 size=op->input->files[f].file_bytes;
        if(size<0||oldBytes>PG_INT64_MAX-size)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("rewrite source size overflow")));
        oldBytes+=size;
        Oid types[3]={INT8OID,INT8OID,INT8OID};
        Datum args[3]={Int64GetDatum(LakePermanentTableId(b)),Int64GetDatum(op->source_ids[f]),Int64GetDatum(publish)};
        LakeMetaRows* ended=LakeMetadataQuery(psprintf("UPDATE %s SET end_snapshot=$3 WHERE table_id=$1 "
            "AND data_file_id=$2 AND end_snapshot IS NULL",data),3,types,args,NULL,own,false,SPI_OK_UPDATE,ns);
        LakeMetadataRequireCount(ended,1,"retire exact rewrite source");
        (void)LakeMetadataQuery(psprintf("UPDATE %s SET end_snapshot=$3 WHERE table_id=$1 "
            "AND data_file_id=$2 AND end_snapshot IS NULL",deleted),3,types,args,NULL,own,false,SPI_OK_UPDATE,ns);
        /* Old inline file markers and all old metadata remain historical.
         * No physical deletion or posting-list address transfer is performed. */
        LakeIndexRetireFile(b,op->source_ids[f],own);
    }
    int64 newBytes=op->output.candidate?op->output.candidate->written.file_bytes:0;
    if(op->output.candidate){
        Oid types[9]={INT8OID,INT8OID,INT8OID,TEXTOID,INT8OID,INT8OID,INT8OID,INT8OID,INT8OID};
        Datum args[9]={Int64GetDatum(nextFile),Int64GetDatum(LakePermanentTableId(b)),Int64GetDatum(publish),
            CStringGetTextDatum(op->output.relative_path),Int64GetDatum(op->rows),Int64GetDatum(newBytes),
            Int64GetDatum(op->output.footer_bytes),Int64GetDatum(op->output.row_groups),
            op->partitioned?Int64GetDatum(op->partition.spec.partition_id):(Datum)0};
        char nulls[10]={' ',' ',' ',' ',' ',' ',' ',' ',op->partitioned?' ':'n','\0'};
        LakeMetaRows* wrote=LakeMetadataQuery(psprintf("INSERT INTO %s(data_file_id,table_id,begin_snapshot,end_snapshot,"
            "file_order,path,path_is_relative,file_format,record_count,file_size_bytes,footer_size,row_id_start,partition_id,"
            "encryption_key,mapping_id,partial_max,row_group_count) SELECT $1,$2,$3,NULL,COALESCE(MAX(file_order),-1)+1,$4,true,"
            "'parquet',$5,$6,$7,NULL,$9,NULL,NULL,NULL,$8 FROM %s WHERE table_id=$2",data,data),
            9,types,args,nulls,own,false,SPI_OK_INSERT,ns);
        LakeMetadataRequireCount(wrote,1,"register standard rewrite interval");
        if(op->partitioned){
            for(int k=0;k<op->partition.spec.key_count;++k){
                const auto* value=&op->partition.tuple.values[k];
                if(!value->is_null&&!value->canonical_text)
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("rewrite partition value lost its canonical encoding")));
                Oid pt[4]={INT8OID,INT8OID,INT8OID,TEXTOID};
                Datum pa[4]={args[0],args[1],Int64GetDatum(k),value->is_null?(Datum)0:CStringGetTextDatum(value->canonical_text)};
                char pn[5]={' ',' ',' ',value->is_null?'n':' ','\0'};
                wrote=LakeMetadataQuery(psprintf("INSERT INTO %s(data_file_id,table_id,partition_key_index,partition_value) "
                    "VALUES($1,$2,$3,$4)",LakeMetadataRelation(ns,"ducklake_file_partition_value")),
                    4,pt,pa,pn,own,false,SPI_OK_INSERT,ns);
                LakeMetadataRequireCount(wrote,1,"rewrite complete ordered file partition value");
            }
        }
        bool collectBounds=LakeStatsExtendedVersionEnabled();
        for(int c=0;c<b->column_count;++c){
            if(b->columns[c].type==VECTOROID)continue; /* Leaf populations are not parent populations. */
            LakeInsertFileColumnStats(ns,DatumGetInt64(args[0]),DatumGetInt64(args[1]),&b->columns[c],
                op->output.column_bytes[c],op->rows,op->null_counts[c],
                collectBounds&&op->minima?op->minima[c]:NULL,
                collectBounds&&op->maxima?op->maxima[c]:NULL,
                own,"rewrite physical file statistics");
        }
    }
    char* stats=LakeWriteRelation(b,"ducklake_table_stats",statsColumns,lengthof(statsColumns));
    Oid st[3]={INT8OID,INT8OID,INT8OID};Datum sa[3]={Int64GetDatum(LakePermanentTableId(b)),Int64GetDatum(oldBytes),Int64GetDatum(newBytes)};
    LakeMetaRows* changed=LakeMetadataQuery(psprintf("UPDATE %s SET file_size_bytes=CASE WHEN file_size_bytes IS NULL "
        "THEN NULL ELSE file_size_bytes-$2+$3 END WHERE table_id=$1 AND (file_size_bytes IS NULL OR "
        "(file_size_bytes>=$2 AND file_size_bytes-$2<=%lld-$3))",stats,(long long)PG_INT64_MAX),
        3,st,sa,NULL,own,false,SPI_OK_UPDATE,ns);
    LakeMetadataRequireCount(changed,1,"rewrite active data-file bytes");
    /* Logical record_count/next_row_id, pg_stats and conservative global
     * bounds are unchanged; layout work is not an autoanalyze DML increment. */
}

void LakePublishFileRewrite(LakeTxnState* state)
{
    LakeRewriteOperation* op=state->rewrite;
    if(!op||op->source_count<1||op->source_count>LakeRewriteSourceLimit())
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("invalid native rewrite publication")));
    LakeInlineBinding* first=op->table->binding;Oid ns=first->metadata_schema_oid;
    /* Every SDK source/output operation is already finished. Registration
     * is acquired inside each attempt, before options/PK; a failed claim's
     * rollback releases it before bounded backoff. */
    ResourceOwner owner=t_thrd.utils_cxt.CurrentResourceOwner;MemoryContext caller=CurrentMemoryContext;
    for(int attempt=0;attempt<16;++attempt){
        CHECK_FOR_INTERRUPTS();LakeRewriteCheckSourceObjects(op);
        MemoryContext context=AllocSetContextCreate(state->context,"Native rewrite publication",ALLOCSET_DEFAULT_SIZES);
        volatile bool inside=false;bool claimed=false;
        PG_TRY();{
            state->publication_subtransaction=true;BeginInternalSubTransaction(NULL);inside=true;MemoryContextSwitchTo(context);
            LakeLockCatalogRegistration();
            (void)LakeMetadataRelation(ns,"ducklake_metadata");
            Relation options=heap_open(get_relname_relid("ducklake_metadata",ns),ShareLock);heap_close(options,NoLock);
            Snapshot snapshot=GetTransactionSnapshot();LakeCheckWriteOptions(first,snapshot);
            char* snapshots=LakeWriteRelation(first,"ducklake_snapshot",snapshotColumns,lengthof(snapshotColumns),true);
            char* changes=LakeWriteRelation(first,"ducklake_snapshot_changes",changesColumns,lengthof(changesColumns),true);
            LakeMetaRows* head=LakeMetadataQuery(psprintf("SELECT snapshot_id,schema_version,next_catalog_id,next_file_id "
                "FROM %s ORDER BY snapshot_id DESC LIMIT 1",snapshots),0,NULL,NULL,NULL,snapshot,true,SPI_OK_SELECT,ns);
            LakeMetadataRequireCount(head,1,"rewrite publication header");
            int64 current=DatumGetInt64(LakeMetadataValue(head,0,1,INT8OID)),schema=DatumGetInt64(LakeMetadataValue(head,0,2,INT8OID));
            int64 nextCatalog=DatumGetInt64(LakeMetadataValue(head,0,3,INT8OID)),nextFile=DatumGetInt64(LakeMetadataValue(head,0,4,INT8OID));
            if(current<op->cutoff||current==PG_INT64_MAX||schema<0||nextCatalog<0||nextFile<op->next_file_at_cutoff||nextFile>=LAKE_REFERENCE_LOCAL_ID_START||nextCatalog>LAKE_REFERENCE_LOCAL_ID_START)
                ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("invalid rewrite publication frontier")));
            Oid ct[2]={INT8OID,INT8OID};Datum ca[2]={Int64GetDatum(op->cutoff),Int64GetDatum(current)};
            LakeMetaRows* later=LakeMetadataQuery(psprintf("SELECT changes_made::text FROM %s WHERE snapshot_id>$1 "
                "AND snapshot_id<=$2 ORDER BY snapshot_id",changes),2,ct,ca,NULL,snapshot,true,SPI_OK_SELECT,ns);
            if((uint64)later->count!=(uint64)(current-op->cutoff))
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("rewrite conflict history is incomplete")));
            LakeCheckConcurrentChanges(later,op->table->catalog,true,true,true,true,true);
            LakeInlineBinding* now=LakeLoadCommittedInlineBinding(first->relation_oid,snapshot);
            LakeRewriteCheckBinding(op,now);LakeRewriteCheckSources(op,snapshot);
            /* Reserve one standard file-ID generation even for empty output:
             * fixed DuckLake stats caches use next_file_id as their key. This
             * is a permitted counter gap, never a row-ID allocation. */
            Oid types[4]={INT8OID,INT8OID,INT8OID,INT8OID};
            Datum args[4]={Int64GetDatum(current+1),Int64GetDatum(schema),Int64GetDatum(nextCatalog),Int64GetDatum(nextFile+1)};
            LakeMetaRows* claim=LakeMetadataQuery(psprintf("INSERT INTO %s(snapshot_id,snapshot_time,schema_version,next_catalog_id,next_file_id) "
                "VALUES($1,pg_catalog.now(),$2,$3,$4) ON CONFLICT(snapshot_id) DO NOTHING RETURNING snapshot_id",snapshots),
                4,types,args,NULL,snapshot,false,SPI_OK_INSERT_RETURNING,ns);
            if(claim->count>1)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("multiple rewrite publication claims")));
            claimed=claim->count==1;
            if(claimed){
                Snapshot verify=GetLatestSnapshot();
                LakeRewriteCheckSources(op,verify);
                Snapshot own=LakeCreatePushOwnWriteSnapshot(verify);
                LakeWriteRewriteOutput(op,own,nextFile,current+1);
                Oid cTypes[2]={INT8OID,TEXTOID};Datum cArgs[2]={args[0],CStringGetTextDatum(psprintf("rewrite_delete:%lld",(long long)LakePermanentTableId(first)))};
                LakeMetaRows* saved=LakeMetadataQuery(psprintf("INSERT INTO %s(snapshot_id,changes_made,author,commit_message,commit_extra_info) "
                    "VALUES($1,$2,NULL,NULL,NULL)",changes),2,cTypes,cArgs,NULL,own,false,SPI_OK_INSERT,ns);
                LakeMetadataRequireCount(saved,1,"standard rewrite changeset");PopActiveSnapshot();
                if(op->output.candidate)LakeFileCandidatePublish(op->output.candidate);
                ReleaseCurrentSubTransaction();
                inside=false;state->publication_subtransaction=false;
                LakeInvalidateRelationSafe(first->relation_oid);
                LakeHostActivateDeferredPublish(first, current+1);
            }else {
                RollbackAndReleaseCurrentSubTransaction();
                inside=false;state->publication_subtransaction=false;
            }
            MemoryContextSwitchTo(caller);
            t_thrd.utils_cxt.CurrentResourceOwner=owner;MemoryContextDelete(context);
        }PG_CATCH();{
            MemoryContextSwitchTo(caller);ErrorData* error=CopyErrorData();FlushErrorState();
            if(inside)RollbackAndReleaseCurrentSubTransaction();state->publication_subtransaction=false;
            t_thrd.utils_cxt.CurrentResourceOwner=owner;MemoryContextDelete(context);ReThrowError(error);
        }PG_END_TRY();
        if(claimed)return;
        pg_usleep(1000L*(attempt+1));
    }
    ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native rewrite publication retry limit exceeded")));
}
