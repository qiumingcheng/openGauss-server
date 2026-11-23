/* Fixed private carriers for ordinary CREATE and claimed DML publication. */
#include "postgres.h"
#include "lake_nullability.h"
#include "lake_column_default.h"
#include "lake_version.h"
#include "lake_types.h"
#include "knl/knl_variable.h"
#include "lake_txn_private.h"
#include "lake_row_lock.h"
#include "lake_create_publish.h"
#include "access/heapam.h"
#include "access/genam.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/pg_depend.h"
#include "catalog/indexing.h"
#include "commands/extension.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_event_trigger.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "executor/spi.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "access/datavec/vector.h"

static const char* LakeCreateTypeName(Oid type,int32 typmod)
{
    LakeNativeTypeRequireWrite(type);
    LakeNativeTypeRequireBusinessVersion(type);
    return LakeNativeSqlType(type,typmod);
}

/* transformCreateStmt retains the declaration nodes. Admit plain nullability
 * and, only in the authenticated49 route, its exact one DEFAULT/raw pair.
 * Named/deferred/informational and all other constraints stay closed. */
bool LakePlainCreateColumnConstraints(const ColumnDef* column,bool allowDefaults)
{
    if(column->constraints==NIL) return column->raw_default==NULL;
    int defaults=0;
    if(!LakeNotNullVersionEnabled()) return false;
    ListCell* cell=NULL;
    foreach(cell,column->constraints) {
        Node* node=(Node*)lfirst(cell);
        if(node==NULL||!IsA(node,Constraint)) return false;
        Constraint* c=(Constraint*)node;
        bool literal=c->contype==CONSTR_DEFAULT;
        if(literal&&(!allowDefaults||column->raw_default==NULL||c->raw_expr!=column->raw_default||++defaults!=1))return false;
        if((!literal&&((c->contype!=CONSTR_NULL&&c->contype!=CONSTR_NOTNULL)||
           (c->contype==CONSTR_NOTNULL)!=column->is_not_null))||
           c->conname!=NULL||c->deferrable||c->initdeferred||c->is_no_inherit||
           (!literal&&c->raw_expr!=NULL)||c->cooked_expr!=NULL||c->keys!=NIL||c->including!=NIL||
           c->exclusions!=NIL||c->options!=NIL||c->indexname!=NULL||c->indexspace!=NULL||
           c->access_method!=NULL||c->where_clause!=NULL||c->pktable!=NULL||
           c->fk_attrs!=NIL||c->pk_attrs!=NIL||c->fk_matchtype!='\0'||
           c->fk_upd_action!='\0'||c->fk_del_action!='\0'||c->old_conpfeqop!=NIL||
           OidIsValid(c->old_pktable_oid)||c->skip_validation||c->initially_valid||
           c->constraintOptions!=NIL||c->inforConstraint!=NULL||c->generated_when!='\0'||
           c->generated_kind!='\0'||c->update_expr!=NULL||c->isdisable)
            return false;
    }
    return defaults==(column->raw_default!=NULL?1:0);
}

static void LakeCheckCreateShape(Relation relation, CreateForeignTableStmt* statement)
{
#ifdef PGXC
    if(statement!=NULL && (statement->base.distributeby!=NULL || statement->base.subcluster!=NULL || statement->distributeby!=NULL))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake business CREATE does not support explicit distribution or node routing")));
#endif
    if(statement==NULL || statement->base.relation==NULL ||
       relation->rd_rel->relkind!=RELKIND_FOREIGN_TABLE || relation->rd_rel->relpersistence!=RELPERSISTENCE_PERMANENT ||
       statement->base.if_not_exists || statement->base.inhRelations!=NIL || statement->base.ofTypename!=NULL ||
       statement->base.constraints!=NIL || statement->base.options!=NIL || statement->base.clusterKeys!=NIL ||
       statement->base.tablespacename!=NULL || statement->base.partTableState!=NULL ||
       statement->base.autoIncStart!=NULL || statement->base.collate!=NULL ||
       statement->part_state!=NULL || statement->error_relation!=NULL || statement->write_only || statement->extOptions!=NIL)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake business CREATE requires an ordinary persistent foreign table without extra DDL features")));
    if(GetDatabaseEncoding()!=PG_UTF8)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake business CREATE requires UTF8")));
    if(RelationGetDescr(relation)->natts<1 || RelationGetDescr(relation)->natts>MaxHeapAttributeNumber-3)
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake CREATE has an unsupported column count")));
    bool defaults=LakeCreateLiteralDefaultVersionEnabled();
    ListCell* cell=NULL;
    foreach(cell,statement->base.tableElts) {
        Node* node=(Node*)lfirst(cell);
        if(!IsA(node,ColumnDef))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake CREATE accepts only explicit scalar columns")));
        ColumnDef* col=(ColumnDef*)node;
        if((col->is_not_null&&!LakeNotNullVersionEnabled()) || col->is_from_type || col->is_serial || col->is_identity || (col->raw_default!=NULL&&(!defaults||!IsA(col->raw_default,Const)||((Const*)col->raw_default)->constisnull)) ||
           col->cooked_default!=NULL || col->collClause!=NULL || !LakePlainCreateColumnConstraints(col,defaults) || col->fdwoptions!=NIL ||
           col->columnOptions!=NIL || col->clientLogicColumnRef!=NULL || col->generatedCol!='\0' || col->update_default!=NULL)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake CREATE requires admitted columns and only authenticated SQL49 typed defaults without collations or extra constraints")));
    }
    TupleDesc desc=RelationGetDescr(relation);
    if(list_length(statement->base.tableElts)!=desc->natts)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake CREATE requires one explicit definition per host column")));
    if(relation->trigdesc!=NULL || relation->rd_rules!=NULL ||
       (desc->constr!=NULL && (desc->constr->num_check!=0 || (!defaults&&desc->constr->num_defval!=0))))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake CREATE does not support table actions")));
    for(int i=0;i<desc->natts;++i) {
        Form_pg_attribute attr=&desc->attrs[i];
        (void)LakeCreateTypeName(attr->atttypid,attr->atttypmod);
        bool validTypmod=attr->atttypid==VECTOROID?
            (attr->atttypmod>=1&&attr->atttypmod<=VECTOR_MAX_DIM):LakeNativeFlatTypmod(attr->atttypid,attr->atttypmod);
        if(attr->attisdropped || (attr->attnotnull&&(!LakeNotNullVersionEnabled()||!LakeNotNullType(attr->atttypid))) || !validTypmod ||
           pg_strncasecmp(NameStr(attr->attname),"_ducklake_",9)==0)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake CREATE has an unsupported or reserved column")));
        for(int j=0;j<i;++j)
            if(LakeIdentifierEquals(NameStr(attr->attname),NameStr(desc->attrs[j].attname)))
                ereport(ERROR,(errcode(ERRCODE_DUPLICATE_COLUMN),errmsg("lake column names must be unique under ASCII case folding")));
    }
}

static void LakeCheckCarrierEventTriggers(void)
{
    /* Freeze the enabled set through our outer commit. Standard event-trigger
     * CREATE/ALTER/DROP takes RX; an old business MVCC snapshot is unsuitable. */
    CommandCounterIncrement();
    Relation triggers=heap_open(EventTriggerRelationId,ShareLock);
    SysScanDesc scan=systable_beginscan(triggers,InvalidOid,false,SnapshotNow,0,NULL);
    HeapTuple tuple;
    while(HeapTupleIsValid(tuple=systable_getnext(scan)))
        if(((Form_pg_event_trigger)GETSTRUCT(tuple))->evtenabled!='D')
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("native lake private carrier DDL requires no enabled event triggers")));
    systable_endscan(scan);heap_close(triggers,NoLock);
}

Oid LakeCreatePrivateCarrier(LakeInlineBinding* binding, const char* name, Oid owner, bool fileDeletes)
{
    LakeCheckCarrierEventTriggers();
    LakeValidatePrivateNamespace(binding->metadata_schema_oid,owner);
    if(OidIsValid(get_relname_relid(name,binding->metadata_schema_oid)))
        ereport(ERROR,(errcode(ERRCODE_DUPLICATE_TABLE),errmsg("native lake generated carrier name already exists")));
    StringInfoData sql;initStringInfo(&sql);
    appendStringInfo(&sql,"CREATE TABLE %s.%s (",quote_identifier(binding->metadata_schema),quote_identifier(name));
    if(fileDeletes) {
        appendStringInfoString(&sql,"file_id pg_catalog.int8,row_id pg_catalog.int8,begin_snapshot pg_catalog.int8");
    } else {
        appendStringInfoString(&sql,"_ducklake_row_id pg_catalog.int8,_ducklake_begin_snapshot pg_catalog.int8,_ducklake_end_snapshot pg_catalog.int8");
        for(int i=0;i<binding->column_count;++i)
            appendStringInfo(&sql,",%s %s",quote_identifier(binding->columns[i].name),LakeCreateTypeName(binding->columns[i].type,binding->columns[i].typmod));
    }
    appendStringInfoChar(&sql,')');
    Oid savedUser;int savedSecurity;GetUserIdAndSecContext(&savedUser,&savedSecurity);
    MemoryContext caller=CurrentMemoryContext;
    volatile bool connected=false,pathPushed=false;
    volatile int gucNest=0;
    if(LakeGetMetadataScope()!=NULL)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake carrier DDL cannot nest in a metadata scope")));
    PG_TRY();
    {
        gucNest=NewGUCNestLevel();
        SetUserIdAndSecContext(owner,savedSecurity|SECURITY_LOCAL_USERID_CHANGE|SECURITY_RESTRICTED_OPERATION);
        OverrideSearchPath path;errno_t rc=memset_s(&path,sizeof(path),0,sizeof(path));securec_check(rc,"","");
        path.schemas=list_make1_oid(PG_CATALOG_NAMESPACE);PushOverrideSearchPath(&path);pathPushed=true;
        if(SPI_connect()!=SPI_OK_CONNECT) ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("native lake carrier DDL SPI connect failed")));
        connected=true;
        int status=SPI_execute(sql.data,false,0);
        SetUserIdAndSecContext(savedUser,savedSecurity);
        if(status!=SPI_OK_UTILITY) ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("native lake carrier DDL returned %d",status)));
        int finish=SPI_finish();connected=false;
        if(finish!=SPI_OK_FINISH) ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("native lake carrier DDL SPI finish failed")));
        PopOverrideSearchPath();pathPushed=false;AtEOXact_GUC(false,gucNest);gucNest=0;
        MemoryContextSwitchTo(caller);
    }
    PG_CATCH();
    {
        SetUserIdAndSecContext(savedUser,savedSecurity);
        MemoryContextSwitchTo(caller);
        if(connected) (void)SPI_finish();
        if(pathPushed) PopOverrideSearchPath();
        if(gucNest!=0) AtEOXact_GUC(false,gucNest);
        PG_RE_THROW();
    }
    PG_END_TRY();
    CommandCounterIncrement();
    Oid oid=get_relname_relid(name,binding->metadata_schema_oid);
    if(!OidIsValid(oid)) ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake carrier DDL created no relation")));
    LakeRegisterCreatedCarrier(binding->metadata_schema_oid,oid);
    return oid;
}

static Oid LakeCreateCarrier(LakeInlineBinding* binding, const char* name, Oid owner)
{
    Oid oid=LakeCreatePrivateCarrier(binding,name,owner,false);
    ObjectAddress carrier,host;
    ObjectAddressSet(carrier,RelationRelationId,oid);
    ObjectAddressSet(host,RelationRelationId,binding->relation_oid);
    recordDependencyOn(&carrier,&host,DEPENDENCY_INTERNAL);
    return oid;
}

static bool LakeCreateInliningEnabled(const LakeCreateDefinition* definition, Snapshot snapshot)
{
    const LakeInlineBinding* binding=definition->binding;
    Oid type=INT8OID;Datum argument=Int64GetDatum(definition->schema_id);
    LakeMetaRows* options=LakeMetadataQuery(psprintf("SELECT scope::text,value::text FROM %s WHERE "
        "key='data_inlining_row_limit' AND (scope IS NULL OR (scope='schema' AND scope_id=$1))",
        LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_metadata")),1,&type,&argument,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    bool haveGlobal=false,haveSchema=false;int64 global=10,schema=10;
    for(uint32 i=0;i<options->count;++i) {
        bool isGlobal=false;(void)LakeMetadataValue(options,i,1,TEXTOID,true,&isGlobal);
        char* text=LakeMetadataText(options,i,2);
        for(const char* c=text;*c!='\0';++c) if(*c<'0'||*c>'9')
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake CREATE requires a nonnegative integer inline row limit")));
        if(text[0]=='\0') ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("empty lake inline row limit")));
        int64 value=DatumGetInt64(DirectFunctionCall1(int8in,CStringGetDatum(text)));
        if(isGlobal) { if(haveGlobal) ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("duplicate global inline limit")));haveGlobal=true;global=value; }
        else { if(haveSchema) ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("duplicate schema inline limit")));haveSchema=true;schema=value; }
    }
    return (haveSchema?schema:global)>0;
}

void LakeValidateCreateOptions(const LakeCreateDefinition* definition, Snapshot snapshot)
{
    if(LakeCreateInliningEnabled(definition,snapshot)!=OidIsValid(definition->binding->inline_oid))
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake inline configuration changed after business CREATE")));
}

void LakeNativeCreateForeignTable(Oid relationOid, CreateForeignTableStmt* statement, const char* lakeSchema)
{
    LakeRequireBusinessCreateVersion();
    if(creating_extension)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake business CREATE cannot run inside an extension script")));
    ForeignTable* foreign=GetForeignTable(relationOid);
    if(pg_foreign_server_aclcheck(foreign->serverid,GetUserId(),ACL_USAGE)!=ACLCHECK_OK)
        aclcheck_error(ACLCHECK_NO_PRIV,ACL_KIND_FOREIGN_SERVER,GetForeignServer(foreign->serverid)->servername);
    Relation host=heap_open(relationOid,AccessShareLock);
    if(host->rd_rel->relowner!=GetUserId() && !superuser())
        ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),errmsg("native lake CREATE requires the new host relation owner")));
    LakeCheckCreateShape(host,statement);
    /* Same mutex as admin registration/drop, before reading private registry. */
    LakeLockCatalogRegistration();
    Oid owner=LakeMetadataOwner();
    if(!OidIsValid(owner)) ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake CREATE requires a configured private owner")));
    bool statementRc=LakeRowLocksEnabled();
    Snapshot commandSnapshot=ActiveSnapshotSet()?GetActiveSnapshot():GetTransactionSnapshot();
    if(statementRc) commandSnapshot=RegisterSnapshot(commandSnapshot);
    PG_TRY();
    {
    LakeTxnCatalog* catalog=LakeTxnBeginCreate(foreign->serverid,commandSnapshot);
    /* Keep a stable command-local copy across nested writable DDL SPI, never
     * attach it to an ancestor catalog or a transaction-long readonly root. */
    Snapshot snapshot=statementRc?commandSnapshot:catalog->business_snapshot;
    LakeInlineBinding* binding=LakeLoadCatalogHeader(foreign->serverid,snapshot);
    binding->not_null_constraints=LakeNotNullVersionEnabled();
    binding->literal_defaults=LakeCreateLiteralDefaultVersionEnabled();
    if(binding->read_only) ereport(ERROR,(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),errmsg("native lake catalog is read-only")));
    binding->relation_oid=relationOid;
    binding->table_uuid=DatumGetCString(DirectFunctionCall1(uuid_out,OidFunctionCall0(F_GEN_RANDOM_UUID)));
    binding->binding_id=DatumGetCString(DirectFunctionCall1(uuid_out,OidFunctionCall0(F_GEN_RANDOM_UUID)));
    binding->column_count=RelationGetDescr(host)->natts;
    binding->columns=(LakeInlineColumn*)palloc0(sizeof(LakeInlineColumn)*binding->column_count);
    Size createDefaultBytes=0;
    int64 columnId=1;
    for(int i=0;i<binding->column_count;++i) {
        binding->columns[i].column_id=columnId++;binding->columns[i].output_attnum=i+1;
        binding->columns[i].typmod=RelationGetDescr(host)->attrs[i].atttypmod;
        binding->columns[i].element_column_id=-1;
        binding->columns[i].name=pstrdup(NameStr(RelationGetDescr(host)->attrs[i].attname));
        binding->columns[i].type=RelationGetDescr(host)->attrs[i].atttypid;
        binding->columns[i].not_null=RelationGetDescr(host)->attrs[i].attnotnull;
        if(binding->literal_defaults){
            LakeInlineColumn* field=&binding->columns[i];
            field->defaults_validated=true; /* Complete host map checked below. */
            ColumnDef* source=(ColumnDef*)list_nth(statement->base.tableElts,i);
            if(source->raw_default){
                Const* value=(Const*)source->raw_default;
                if(value->consttype!=field->type||value->consttypmod!=field->typmod||value->constisnull||
                   value->constcollid!=RelationGetDescr(host)->attrs[i].attcollation)
                    ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native CREATE typed default identity changed")));
                field->initial_literal=LakeEncodeLiteralDefault(field->type,field->typmod,value->constvalue);
                field->initial_value=value->constvalue;
                /* Bound statement capture as well as the later full journal
                 * preflight. Count two owned texts and the by-reference Datum
                 * before allocating the independent current string. */
                LakeInlineColumn sized=*field;sized.current_literal=field->initial_literal;
                Size bytes=LakeColumnDefaultBytes(&sized);
                if(bytes>LAKE_INLINE_TXN_MEMORY_LIMIT-createDefaultBytes)
                    ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native CREATE default capture exceeds its byte limit")));
                createDefaultBytes+=bytes;
                field->current_literal=pstrdup(field->initial_literal);
                /* Created fields are physically mandatory, never later-ADD. */
                field->initial_null_proven=false;field->initial_value_proven=false;
            }
        }
        if(binding->columns[i].type==VECTOROID)binding->columns[i].element_column_id=columnId++;
    }
    LakeValidateHostDefaults(binding,host);
    LakeCreateDefinition definition;errno_t rc=memset_s(&definition,sizeof(definition),0,sizeof(definition));securec_check(rc,"","");
    definition.binding=binding;definition.table_name=pstrdup(RelationGetRelationName(host));
    definition.relative_path=LakeCreateRelativePath(binding->table_uuid,definition.table_name);
    Oid type=INT8OID;Datum arg=Int64GetDatum(binding->snapshot_id);
    LakeMetaRows* schemas=LakeMetadataQuery(psprintf("SELECT schema_id,schema_uuid::text,schema_name::text,parent_schema_id FROM %s "
        "WHERE begin_snapshot<=$1 AND (end_snapshot IS NULL OR end_snapshot>$1)",
        LakeMetadataRelation(binding->metadata_schema_oid,"ducklake_schema")),1,&type,&arg,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
    int matched=0;
    for(uint32 i=0;i<schemas->count;++i) {
        char* name=LakeMetadataText(schemas,i,3);
        if(!LakeIdentifierEquals(name,lakeSchema)) continue;
        bool topLevel=false;(void)LakeMetadataValue(schemas,i,4,INT8OID,true,&topLevel);
        if(!topLevel) ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake CREATE requires an existing top-level lake schema")));
        definition.schema_id=DatumGetInt64(LakeMetadataValue(schemas,i,1,INT8OID));
        definition.schema_uuid=LakeMetadataText(schemas,i,2);definition.schema_name=name;++matched;
    }
    if(matched!=1) ereport(ERROR,(errcode(matched==0?ERRCODE_UNDEFINED_SCHEMA:ERRCODE_DATA_CORRUPTED),errmsg("lake schema does not have one visible identity")));
    for(LakeTxnTable* local=catalog->tables;local!=NULL;local=local->next)
        if(local->active && local->pending!=NULL && local->pending->definition->schema_id==definition.schema_id &&
           LakeIdentifierEquals(local->pending->definition->table_name,definition.table_name))
            ereport(ERROR,(errcode(ERRCODE_DUPLICATE_TABLE),errmsg("lake table already exists in this transaction")));
    /* This checks tables AND views in the pinned business snapshot. Translate
     * only a proven name collision here; publication uses serialization errors. */
    const char* objectRelations[]={"ducklake_table","ducklake_view"};
    const char* objectNames[]={"table_name","view_name"};
    Oid nameTypes[2]={INT8OID,INT8OID};Datum nameArgs[2]={Int64GetDatum(definition.schema_id),arg};
    for(int k=0;k<2;++k) {
        LakeMetaRows* names=LakeMetadataQuery(psprintf("SELECT %s::text FROM %s WHERE schema_id=$1 AND begin_snapshot<=$2 "
            "AND (end_snapshot IS NULL OR end_snapshot>$2)",objectNames[k],LakeMetadataRelation(binding->metadata_schema_oid,objectRelations[k])),
            2,nameTypes,nameArgs,NULL,snapshot,true,SPI_OK_SELECT,binding->metadata_schema_oid);
        for(uint32 i=0;i<names->count;++i)
            if(LakeIdentifierEquals(LakeMetadataText(names,i,1),definition.table_name))
                ereport(ERROR,(errcode(ERRCODE_DUPLICATE_TABLE),errmsg("lake table or view already exists")));
    }
    if(LakeCreateInliningEnabled(&definition,snapshot)) {
        char* compact=pstrdup(binding->table_uuid);char* write=compact;
        for(char* read=compact;*read!='\0';++read) if(*read!='-') *write++=*read;
        *write='\0';
        definition.inline_name=psprintf("ducklake_inlined_%s",compact);
        binding->inline_oid=LakeCreateCarrier(binding,definition.inline_name,owner);
    }
    (void)LakeTxnAdoptCreatedTable(catalog,binding,&definition);
    if(statementRc) UnregisterSnapshot(commandSnapshot);
    }
    PG_CATCH();
    {
        if(statementRc) UnregisterSnapshot(commandSnapshot);
        PG_RE_THROW();
    }
    PG_END_TRY();
    heap_close(host,NoLock);
}

void LakeNativeDropCreatedCarrier(LakeTxnTable* table)
{
    LakeTxnRequireActiveTable(table);
    if(table->pending==NULL) ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("expected local lake object")));
    LakeLockCatalogRegistration();
    if(OidIsValid(table->binding->inline_oid)) {
        Oid types[2]={OIDOID,OIDOID};Datum args[2]={ObjectIdGetDatum(table->binding->metadata_schema_oid),ObjectIdGetDatum(table->binding->inline_oid)};
        LakeMetaRows* removed=LakeMetadataQuery("DELETE FROM og_lake_internal.metadata_relation WHERE metadata_schema_oid=$1 AND relation_oid=$2",
            2,types,args,NULL,GetTransactionSnapshot(),false,SPI_OK_DELETE);
        LakeMetadataRequireCount(removed,1,"local carrier registry deletion");
    }
    /* The INTERNAL dependency removes the carrier with the owning foreign
     * relation, and host subtransaction rollback restores both physical DDL
     * and the registry row. No PRE_COMMIT DDL or standalone transaction. */
}

/* Called only after winning the snapshot claim. The carrier is now standard
 * durable lake storage, independent of any particular host foreign binding.
 * pg_depend deletion belongs to this retry subtransaction; failure restores
 * the temporary edge, preserving CREATE->DROP semantics before publication. */
void LakeDetachPublishedCarrier(const LakeCreateDefinition* definition)
{
    const LakeInlineBinding* binding=definition->binding;
    if(!OidIsValid(binding->inline_oid)) return;
    /* The publisher already validated the active journal before winning its
     * claim, and supplies its original pending definition. Do not call the
     * ordinary transaction-state lookup from PRE_COMMIT: that API deliberately
     * rejects starting new lake operations during commit callbacks. */
    if(binding->identity.kind!=LAKE_TABLE_LOCAL || binding->identity.key.local_object_token==0 ||
       definition->inline_name==NULL || get_relname_relid(definition->inline_name,binding->metadata_schema_oid)!=binding->inline_oid)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake publication expected its active local carrier owner")));
    Relation carrier=heap_open(binding->inline_oid,AccessShareLock);
    /* rd_createSubid is a relcache optimization, not durable ownership proof:
     * DROP in a child savepoint followed by rollback can rebuild the parent's
     * descriptor without that flag. The active CREATE journal, trusted OID
     * registration and exact temporary dependency jointly prove our object. */
    if(RelationGetNamespace(carrier)!=binding->metadata_schema_oid)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake publication expected its new private carrier")));
    Oid owner=LakeMetadataOwner();
    LakeValidatePrivateRelation(carrier,owner,false);
    LakeCheckMetadataObjectIdentity(binding->metadata_schema_oid,binding->inline_oid,owner);
    heap_close(carrier,NoLock);
    Relation depend=heap_open(DependRelationId,RowExclusiveLock);
    ScanKeyData keys[2];
    ScanKeyInit(&keys[0],Anum_pg_depend_classid,BTEqualStrategyNumber,F_OIDEQ,ObjectIdGetDatum(RelationRelationId));
    ScanKeyInit(&keys[1],Anum_pg_depend_objid,BTEqualStrategyNumber,F_OIDEQ,ObjectIdGetDatum(binding->inline_oid));
    SysScanDesc scan=systable_beginscan(depend,DependDependerIndexId,true,SnapshotNow,2,keys);
    HeapTuple tuple;int count=0;
    while(HeapTupleIsValid(tuple=systable_getnext(scan))) {
        Form_pg_depend row=(Form_pg_depend)GETSTRUCT(tuple);
        if(row->objsubid==0 && row->refclassid==RelationRelationId && row->refobjid==binding->relation_oid &&
           row->refobjsubid==0 && row->deptype==DEPENDENCY_INTERNAL) {
            simple_heap_delete(depend,&tuple->t_self);++count;
        }
    }
    systable_endscan(scan);heap_close(depend,NoLock);
    if(count!=1) ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake carrier temporary ownership edge is missing or duplicated")));
    CommandCounterIncrement();
}
