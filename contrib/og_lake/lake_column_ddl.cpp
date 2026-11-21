/* Restricted ADD, adapting fixed DuckLake field IDs/defaults/schema versions.
 * Host ALTER owns AX. No autonomous commit, SDK I/O or persisted private schema. */
/*
Copyright 2018-2025 Stichting DuckDB Foundation

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
#include "postgres.h"
#include "lake_column_layout.h"
#include "lake_inline_sources.h"
#include "lake_column_default.h"
#include "lake_nullability.h"
#include "catalog/heap.h"
#include "lake_txn_private.h"
#include "lake_create_publish.h"
#include "lake_file_sdk.h"
#include "lake_types.h"
#include "lake_version.h"
#include "access/og_lake.h"
#include "access/heapam.h"
#include "catalog/pg_inherits_fn.h"
#include "commands/extension.h"
#include "executor/spi.h"
#include "foreign/foreign.h"
#include "knl/knl_variable.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "utils/rel_gs.h"
#include "parser/parse_type.h"
#include "storage/lmgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

extern void LakeTxnForgetReadOnlyTableForRename(Oid relationOid);
struct LakeAddColumnPlan {
    MemoryContext context;
    LakeTxnState* state;
    LakeInlineBinding* original;
    char* name;
    Oid type;
    int32 typmod;
    bool not_null;
    AttrNumber attnum;
    Const* literal; /* Owned typed host default, never an executable expression. */
    char* canonical; /* Standard raw initial data; NULL means initial SQL NULL. */
};

void* LakeNativePrepareAddColumn(Relation relation,List* commands)
{
    if(!LakeIsNativeForeignRelation(relation)||!LakeColumnEvolutionVersionEnabled())return NULL;
    LakeRejectNativePartitionDDL(relation,"ADD COLUMN");
    if(list_length(commands)!=1)return NULL;
    AlterTableCmd* command=(AlterTableCmd*)linitial(commands);
    if(command->subtype!=AT_AddColumn||!command->def||!IsA(command->def,ColumnDef))return NULL;
    ColumnDef* column=(ColumnDef*)command->def;
    bool extended=LakeExtendedDefaultsVersionEnabled();
    if(command->missing_ok||command->is_first||command->after_name||command->recursing||
       (column->is_not_null&&!extended)||column->is_from_type||column->is_serial||column->is_identity||column->inhcount||
       column->generatedCol||column->update_default||column->cooked_default||column->fdwoptions||
       column->columnOptions||column->clientLogicColumnRef||column->constraints||column->position||
       column->storage||column->cmprs_mode!=ATT_CMPR_UNDEFINED||column->kvtype||column->dropped_attr||column->typname->arrayBounds)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake ADD requires one plain scalar column")));
    bool nonnullDefault=column->raw_default&&(!IsA(column->raw_default,A_Const)||
        nodeTag(&((A_Const*)column->raw_default)->val)!=T_Null);
    if(nonnullDefault&&!LakeLiteralDefaultVersionEnabled())
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake ADD accepts only an absent default or literal NULL")));
    if(!column->colname||!*column->colname||strlen(column->colname)>=NAMEDATALEN||
       pg_strncasecmp(column->colname,"_ducklake_",10)==0)
        ereport(ERROR,(errcode(ERRCODE_INVALID_NAME),errmsg("invalid native lake added column name")));
    if(!pg_class_ownercheck(RelationGetRelid(relation),GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER,ACL_KIND_CLASS,RelationGetRelationName(relation));
    if(u_sess->utils_cxt.XactIsoLevel!=XACT_READ_COMMITTED)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake ADD COLUMN requires READ COMMITTED")));
    if(u_sess->attr.attr_common.XactReadOnly)
        ereport(ERROR,(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),errmsg("cannot add a lake column in a read-only transaction")));
    if(creating_extension||RelationIsPartitioned(relation)||relation->rd_rel->relpersistence!=RELPERSISTENCE_PERMANENT||
       find_inheritance_children(RelationGetRelid(relation),NoLock)!=NIL||
       LakeTxnFindCreatedTable(RelationGetRelid(relation))!=NULL)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake ADD requires a committed ordinary CREATE table")));
    if(!LakeNativeProtectedCreate(RelationGetRelid(relation)))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake ADD requires a protected CREATE binding")));
    Oid type;int32 typmod;typenameTypeIdAndMod(NULL,column->typname,&type,&typmod);
    const LakeNativeType* native=LakeNativeTypeByOid(type);
    if(native->fixed_width==-2||!native->writable||!LakeNativeFlatTypmod(type,typmod))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake ADD requires an admitted flat scalar with its exact supported typmod")));
    if(column->is_not_null&&(!LakeNotNullVersionEnabled()||!LakeNotNullType(type)))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake ADD NOT NULL requires an admitted flat scalar column")));
    LakeNativeTypeRequireBusinessVersion(type);LakeNativeTypeRequireSdk(type,LakeFileProvider());
    /* Do not wait on the registry while owning target AX: SET obtains AS,
     * registry SRE, then upgrades to target SRE. Conditional failure breaks
     * the only reversed edge without changing that established protocol. */
    LakeTryLockCatalogRegistration();
    if(!OidIsValid(LakeMetadataOwner()))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake ADD requires a private metadata owner")));
    LakeTxnState* state=LakeTxnBeginColumnDefinition();
    LakeTxnForgetReadOnlyTableForRename(RelationGetRelid(relation));
    MemoryContext caller=CurrentMemoryContext;
    MemoryContext context=AllocSetContextCreate(caller,"Native lake ADD statement",ALLOCSET_DEFAULT_SIZES);
    Snapshot volatile snapshot=NULL;LakeAddColumnPlan* result=NULL;
    PG_TRY();{
        MemoryContextSwitchTo(context);snapshot=RegisterSnapshot(GetLatestSnapshot());
        result=(LakeAddColumnPlan*)palloc0(sizeof(LakeAddColumnPlan));result->context=context;result->state=state;
        result->original=LakeLoadCommittedInlineBinding(RelationGetRelid(relation),snapshot);
        if(result->original->read_only||!result->original->column_evolution||
           result->original->column_count>=MaxHeapAttributeNumber-3||
           RelationGetDescr(relation)->natts>=MaxHeapAttributeNumber)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake ADD target is not writable or exceeds its column bound")));
        for(int c=0;c<result->original->column_count;++c)
            if(LakeIdentifierEquals(column->colname,result->original->columns[c].name))
                ereport(ERROR,(errcode(ERRCODE_DUPLICATE_COLUMN),errmsg("lake column names must be unique under ASCII case folding")));
        result->name=pstrdup(column->colname);result->type=type;result->typmod=typmod;
        result->not_null=column->is_not_null;
        result->attnum=(AttrNumber)(RelationGetDescr(relation)->natts+1);
        if(result->not_null&&(!result->original->not_null_constraints||!result->original->literal_defaults))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake ADD NOT NULL target does not support defaults/constraints")));
        if(!extended&&((type==UUIDOID&&LakeUuidBusinessVersionEnabled())||
            (type==JSONOID&&LakeJsonBusinessVersionEnabled())||
            (type==NUMERICOID&&LakeNumericBusinessVersionEnabled()))&&column->raw_default){
            Const* literal=type==NUMERICOID?LakeParseNumericNullDefault(column->raw_default,typmod,get_typcollation(type)):
                LakeParseCurrentLiteralDefault(column->raw_default,type,typmod,get_typcollation(type));
            if(!literal->constisnull)
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake UUID or JSON ADD accepts only a NULL default")));
        }else if(result->not_null){
            if(!column->raw_default)
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake ADD NOT NULL requires a non-NULL literal default")));
            result->literal=LakeParseLiteralDefault(column->raw_default,type,typmod,get_typcollation(type));
            result->canonical=LakeEncodeLiteralDefault(type,typmod,result->literal->constvalue);
        }else if(nonnullDefault){
            Const* literal=extended?
                LakeParseCurrentLiteralDefault(column->raw_default,type,typmod,get_typcollation(type)):
                LakeParseLiteralDefault(column->raw_default,type,typmod,get_typcollation(type));
            if(!literal->constisnull){
                result->literal=literal;
                result->canonical=LakeEncodeLiteralDefault(type,typmod,literal->constvalue);
            }
        }
        UnregisterSnapshot(snapshot);snapshot=NULL;MemoryContextSwitchTo(caller);
    }PG_CATCH();{
        MemoryContextSwitchTo(caller);if(snapshot)UnregisterSnapshot(snapshot);MemoryContextDelete(context);PG_RE_THROW();
    }PG_END_TRY();
    /* NULL needs no pg_attrdef. SQL39 has captured a validated typed Const;
     * Finish installs it only after normal foreign ADD, outside claim retry.
     * The global foreign-table expression-default gate remains untouched. */
    column->raw_default=NULL;
    return result;
}

static int64 LakeAllocateAddedField(const LakeInlineBinding* binding,Snapshot snapshot)
{
    Oid type=INT8OID;Datum table=Int64GetDatum(LakePermanentTableId(binding));
    LakeMetaRows* rows=LakeMetadataQuery(psprintf("SELECT max(id) FROM ("
        "SELECT column_id AS id FROM %s WHERE table_id=$1 UNION ALL "
        "SELECT column_id AS id FROM %s WHERE table_id=$1) all_fields",
        LakeDefinitionRelation(binding->metadata_schema_oid,"ducklake_column"),
        LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_file_column_stats")),
        1,&type,&table,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    LakeMetadataRequireCount(rows,1,"complete native field allocator history");
    int64 maximum=DatumGetInt64(LakeMetadataValue(rows,0,1,INT8OID));
    /* Do not allocate into the existing standard hidden-field namespace.
     * The lowest Native-used reserved ID is the snapshot field, below int32
     * max; retain MAX across old children/stats before enforcing this bound. */
    int64 reserved=Min(Min(LAKE_FILE_SNAPSHOT_ID,LAKE_FILE_ROW_ID),
        Min(LAKE_DELETE_PATH_ID,LAKE_DELETE_POSITION_ID));
    if(maximum<0||maximum>=reserved-1||maximum>=PG_INT32_MAX)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake field IDs reach the reserved SDK field range")));
    return maximum+1;
}

static void LakeWriteAddedField(LakeAddColumnPlan* plan,int64 id,int64 commit,int64 schema,
    Oid collation,Snapshot snapshot)
{
    const LakeInlineBinding* old=plan->original;Oid ns=old->metadata_schema_oid;
    /* Existing write admission rejects a nonempty table-level skip policy.
     * Preserve that conservative boundary rather than restoring bounds which
     * a configured policy requested to omit. The options ShareLock is already
     * held by this claim, and failure rolls host/default/catalog changes back. */
    if(plan->literal){
    Oid optionType=INT8OID;Datum optionArg=Int64GetDatum(LakePermanentTableId(old));
    LakeMetaRows* policy=LakeMetadataQuery(psprintf("SELECT value::text FROM %s WHERE key='skip_stats_columns' "
        "AND scope='table' AND scope_id=$1",LakeMetadataRelation(ns,"ducklake_metadata")),
        1,&optionType,&optionArg,NULL,snapshot,true,SPI_OK_SELECT,ns);
    if(policy->count>1)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("duplicate native ADD statistics policy")));
    if(policy->count==1){
        bool isnull=false;Datum value=LakeMetadataValue(policy,0,1,TEXTOID,true,&isnull);
        if(!isnull&&*TextDatumGetCString(value))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake ADD does not support skip_stats_columns")));
    }
    }
    bool escaped=false;
    const char* current=LakeCurrentDefaultMetadata(plan->type,plan->canonical,&escaped);
    Oid types[10]={INT8OID,INT8OID,INT8OID,TEXTOID,TEXTOID,INT8OID,TEXTOID,TEXTOID,TEXTOID,BOOLOID};
    Datum args[10]={Int64GetDatum(id),Int64GetDatum(commit),Int64GetDatum(LakePermanentTableId(old)),
        CStringGetTextDatum(plan->name),CStringGetTextDatum(LakeNativeDuckType(plan->type,plan->typmod)),Int64GetDatum(schema),
        plan->canonical?CStringGetTextDatum(plan->canonical):(Datum)0,
        CStringGetTextDatum(current),
        CStringGetTextDatum(escaped?"expression":"literal"),
        BoolGetDatum(!plan->not_null)};
    char defaultNulls[11]={' ',' ',' ',' ',' ',' ',plan->canonical?' ':'n',' ',' ',' ','\0'};
    LakeMetaRows* wrote=LakeMetadataQuery(psprintf("INSERT INTO %s "
        "(column_id,begin_snapshot,end_snapshot,table_id,column_order,column_name,column_type,initial_default,"
        "default_value,nulls_allowed,parent_column,default_value_type,default_value_dialect) "
        "VALUES($1,$2,NULL,$3,$1,$4,$5,$7,$8,$10,NULL,$9,'duckdb')",
        LakeDefinitionRelation(ns,"ducklake_column")),10,types,args,defaultNulls,snapshot,false,SPI_OK_INSERT,ns);
    LakeMetadataRequireCount(wrote,1,"added native field definition");
    Oid vt[3]={INT8OID,INT8OID,INT8OID};Datum va[3]={args[1],args[5],args[2]};
    wrote=LakeMetadataQuery(psprintf("INSERT INTO %s(begin_snapshot,schema_version,table_id) VALUES($1,$2,$3)",
        LakeDefinitionRelation(ns,"ducklake_schema_versions")),3,vt,va,NULL,snapshot,false,SPI_OK_INSERT,ns);
    LakeMetadataRequireCount(wrote,1,"added field table schema version");
    /* Same standard policy as DuckLake WriteAddedFieldStats: no table stats
     * leaves initialization to the first writer; an unknown existing row count
     * leaves this field's statistics absent/unknown, never guessed empty. */
    Oid st[5]={INT8OID,INT8OID,BOOLOID,TEXTOID,BOOLOID};
    bool statsExtended=LakeStatsExtendedVersionEnabled();
    const LakeNativeType* native=LakeNativeTypeByOid(plan->type);
    bool maintainBounds=LakeExtendedDefaultsVersionEnabled()?
        (native->maintain_bounds&&(plan->type!=NUMERICOID||statsExtended)):
        ((plan->type!=BYTEAOID&&plan->type!=NUMERICOID)||(plan->type==NUMERICOID&&statsExtended));
    Datum sa[5]={args[2],args[0],BoolGetDatum(plan->canonical==NULL),args[6],BoolGetDatum(maintainBounds)};
    const char* sn=plan->canonical?NULL:"   n ";
    const char* exactSql=statsExtended?"CASE WHEN record_count>0 AND NOT $3 AND $5 THEN true END":
        "CASE WHEN $5 THEN true END";
    wrote=LakeMetadataQuery(psprintf("INSERT INTO %s(table_id,column_id,contains_null,contains_nan,min_value,max_value,"
        "extra_stats,min_is_exact,max_is_exact) SELECT table_id,$2,($3 AND record_count>0),false,"
        "CASE WHEN record_count>0 AND NOT $3 AND $5 THEN $4 END,CASE WHEN record_count>0 AND NOT $3 AND $5 THEN $4 END,NULL,"
        "%s,%s "
        "FROM %s WHERE table_id=$1 AND record_count IS NOT NULL",
        LakeMetadataRelation(ns,"ducklake_table_column_stats"),exactSql,exactSql,
        LakeMetadataRelation(ns,"ducklake_table_stats")),
        5,st,sa,sn,snapshot,false,SPI_OK_INSERT,ns);
    if(wrote->count>1)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("duplicate table statistics during ADD")));
    Oid mt[6]={UUIDOID,INT8OID,INT4OID,OIDOID,INT4OID,OIDOID};
    Datum ma[6]={DirectFunctionCall1(uuid_in,CStringGetDatum(old->binding_id)),args[0],Int32GetDatum(plan->attnum),
        ObjectIdGetDatum(plan->type),Int32GetDatum(plan->typmod),ObjectIdGetDatum(collation)};
    wrote=LakeMetadataQuery("INSERT INTO og_lake_internal.column_binding(binding_id,column_id,attnum,type_oid,typmod,collation_oid,codec_version) "
        "VALUES($1,$2,$3,$4,$5,$6,1)",6,mt,ma,NULL,snapshot,false,SPI_OK_INSERT);
    LakeMetadataRequireCount(wrote,1,"added host stable field map");
    Oid ht[5]={INT8OID,UUIDOID,INT8OID,UUIDOID,OIDOID};
    Datum ha[5]={args[5],ma[0],args[2],DirectFunctionCall1(uuid_in,CStringGetDatum(old->table_uuid)),ObjectIdGetDatum(old->server_oid)};
    wrote=LakeMetadataQuery("UPDATE og_lake_internal.table_binding SET schema_version=$1 WHERE binding_id=$2 "
        "AND table_id=$3 AND table_uuid=$4 AND server_oid=$5 AND state='active' AND NOT read_only",
        5,ht,ha,NULL,snapshot,false,SPI_OK_UPDATE);
    LakeMetadataRequireCount(wrote,1,"added field private binding version");
    Oid ct[2]={INT8OID,TEXTOID};Datum ca[2]={args[1],CStringGetTextDatum(psprintf("altered_table:" INT64_FORMAT,LakePermanentTableId(old)))};
    wrote=LakeMetadataQuery(psprintf("INSERT INTO %s(snapshot_id,changes_made,author,commit_message,commit_extra_info) "
        "VALUES($1,$2,NULL,NULL,NULL)",LakeMetadataRelation(ns,"ducklake_snapshot_changes")),
        2,ct,ca,NULL,snapshot,false,SPI_OK_INSERT,ns);
    LakeMetadataRequireCount(wrote,1,"added field changeset");
}

void LakeNativeFinishAddColumn(void* opaque)
{
    LakeAddColumnPlan* plan=(LakeAddColumnPlan*)opaque;
    MemoryContext caller=CurrentMemoryContext;ResourceOwner owner=t_thrd.utils_cxt.CurrentResourceOwner;
    void* hostSlot = LakeHostPrepareImmediatePublish(plan->original, GetCurrentSubTransactionId());
    int64 winningSnapshot = -1;
    bool complete=false;volatile bool inside=false;
    PG_TRY();{
        MemoryContextSwitchTo(plan->context);CommandCounterIncrement();
        Relation host=heap_open(plan->original->relation_oid,NoLock);
        TupleDesc desc=RelationGetDescr(host);
        if(desc->natts!=plan->attnum)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native ADD host attribute count changed")));
        Form_pg_attribute attr=&desc->attrs[plan->attnum-1];Oid collation=attr->attcollation;
        if(attr->attisdropped||attr->attnotnull!=plan->not_null||attr->atthasdef||attr->atttypid!=plan->type||
           attr->atttypmod!=plan->typmod||strcmp(NameStr(attr->attname),plan->name))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native ADD host attribute differs from its admitted definition")));
        if(plan->literal){
            /* This one catalog operation belongs to the user subtransaction,
             * not the retrying internal lake claim. It rolls back together with
             * normal host ADD, even if all claims fail or the caller rolls back. */
            plan->literal->constcollid=collation;
            (void)StoreAttrDefault(host,plan->attnum,(Node*)plan->literal,0,NULL);
        }
        heap_close(host,NoLock);
        if(plan->literal){CacheInvalidateRelcacheByRelid(plan->original->relation_oid);CommandCounterIncrement();}
        for(int attempt=0;attempt<16&&!complete;++attempt){
            CHECK_FOR_INTERRUPTS();
            MemoryContext scratch=AllocSetContextCreate(plan->context,"Native ADD claim",ALLOCSET_DEFAULT_SIZES);
            plan->state->publication_subtransaction=true;BeginInternalSubTransaction(NULL);inside=true;
            MemoryContextSwitchTo(scratch);Oid ns=plan->original->metadata_schema_oid;
            (void)LakeMetadataRelation(ns,"ducklake_metadata");
            Relation options=heap_open(get_relname_relid("ducklake_metadata",ns),ShareLock);heap_close(options,NoLock);
            Snapshot fresh=GetLatestSnapshot();
            LakeInlineBinding* current=LakeReloadAddColumnDefinition(plan->original,fresh);
            const char* snapshots=LakeMetadataRelation(ns,"ducklake_snapshot");
            LakeMetaRows* head=LakeMetadataQuery(psprintf("SELECT snapshot_id,schema_version,next_catalog_id,next_file_id "
                "FROM %s ORDER BY snapshot_id DESC LIMIT 1",snapshots),0,NULL,NULL,NULL,fresh,true,SPI_OK_SELECT,ns);
            LakeMetadataRequireCount(head,1,"ADD snapshot head");
            int64 h=DatumGetInt64(LakeMetadataValue(head,0,1,INT8OID));
            int64 schema=DatumGetInt64(LakeMetadataValue(head,0,2,INT8OID));
            int64 catalog=DatumGetInt64(LakeMetadataValue(head,0,3,INT8OID));
            int64 file=DatumGetInt64(LakeMetadataValue(head,0,4,INT8OID));
            if(h<0||h==PG_INT64_MAX||schema<0||schema==PG_INT64_MAX||catalog<0||catalog>=LAKE_REFERENCE_LOCAL_ID_START||file<0)
                ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native ADD snapshot counters exceed their bounds")));
            int64 field=LakeAllocateAddedField(current,fresh); /* Full history, each fresh attempt. */
            Oid types[4]={INT8OID,INT8OID,INT8OID,INT8OID};
            Datum args[4]={Int64GetDatum(h+1),Int64GetDatum(schema+1),Int64GetDatum(catalog),Int64GetDatum(file)};
            LakeMetaRows* claim=LakeMetadataQuery(psprintf("INSERT INTO %s(snapshot_id,snapshot_time,schema_version,next_catalog_id,next_file_id) "
                "VALUES($1,pg_catalog.now(),$2,$3,$4) ON CONFLICT(snapshot_id) DO NOTHING RETURNING snapshot_id",snapshots),
                4,types,args,NULL,fresh,false,SPI_OK_INSERT_RETURNING,ns);
            if(claim->count>1)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("duplicate native ADD snapshot claim")));
            if(claim->count==1){
                Snapshot own=LakeCreatePushOwnWriteSnapshot(GetLatestSnapshot());
                LakeWriteAddedField(plan,field,h+1,schema+1,collation,own);
                PopActiveSnapshot();ReleaseCurrentSubTransaction();inside=false;plan->state->publication_subtransaction=false;complete=true;
                winningSnapshot=h+1;
            }else{RollbackAndReleaseCurrentSubTransaction();inside=false;plan->state->publication_subtransaction=false;}
            MemoryContextSwitchTo(plan->context);t_thrd.utils_cxt.CurrentResourceOwner=owner;MemoryContextDelete(scratch);
            if(!complete)pg_usleep(1000L*(attempt+1));
        }
        if(!complete)ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native ADD snapshot claim exceeded retry limit")));
        CacheInvalidateRelcacheByRelid(plan->original->relation_oid);CommandCounterIncrement();
        /* Both host and standard/private schema are now complete. This creates
         * no transaction root/view, and runs outside the internal claim. */
        (void)LakeLoadCommittedInlineBinding(plan->original->relation_oid,GetLatestSnapshot());
        if(winningSnapshot>=0)LakeHostActivateImmediatePublish(hostSlot,winningSnapshot);
        MemoryContextSwitchTo(caller);MemoryContextDelete(plan->context);
    }PG_CATCH();{
        MemoryContextSwitchTo(caller);ErrorData* error=CopyErrorData();FlushErrorState();
        if(inside)RollbackAndReleaseCurrentSubTransaction();
        plan->state->publication_subtransaction=false;t_thrd.utils_cxt.CurrentResourceOwner=owner;
        MemoryContextDelete(plan->context);ReThrowError(error);
    }PG_END_TRY();
}
