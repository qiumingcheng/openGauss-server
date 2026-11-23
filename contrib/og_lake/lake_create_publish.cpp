/* Native parameterized SQL adaptation of DuckLake 0aaa8e10, format 1.1.
 * See SOURCE_MAP.md and LICENSE.ducklake. No DuckDB execution dependency.
 * This file owns only standard definition publication and changeset syntax;
 * transaction journals, host DDL, snapshot claims and retries remain with caller. */
#include "postgres.h"
#include "lake_nullability.h"
#include "lake_column_default.h"
#include "lake_version.h"
#include "lake_types.h"
#include "lake_create_publish.h"
#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "access/datavec/vector.h"
#include "utils/rel.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

static const int64 LakeCreateLocalIdBoundary = INT64CONST(1000000000000000000);
struct LakeCreateColumnShape { const char* name; Oid type; };
static const LakeCreateColumnShape tableShape[] = {
    {"table_id",INT8OID},{"table_uuid",UUIDOID},{"begin_snapshot",INT8OID},
    {"end_snapshot",INT8OID},{"schema_id",INT8OID},{"table_name",TEXTOID},
    {"path",TEXTOID},{"path_is_relative",BOOLOID}
};
static const LakeCreateColumnShape columnShape[] = {
    {"column_id",INT8OID},{"begin_snapshot",INT8OID},{"end_snapshot",INT8OID},
    {"table_id",INT8OID},{"column_order",INT8OID},{"column_name",TEXTOID},
    {"column_type",TEXTOID},{"initial_default",TEXTOID},{"default_value",TEXTOID},
    {"nulls_allowed",BOOLOID},{"parent_column",INT8OID},{"default_value_type",TEXTOID},
    {"default_value_dialect",TEXTOID}
};
static const LakeCreateColumnShape columnTagShape[] = {
    {"table_id",INT8OID},{"column_id",INT8OID},{"begin_snapshot",INT8OID},
    {"end_snapshot",INT8OID},{"key",TEXTOID},{"value",TEXTOID}
};
static const LakeCreateColumnShape schemaShape[] = {
    {"schema_id",INT8OID},{"schema_uuid",UUIDOID},{"begin_snapshot",INT8OID},
    {"end_snapshot",INT8OID},{"schema_name",TEXTOID},{"path",TEXTOID},
    {"path_is_relative",BOOLOID},{"parent_schema_id",INT8OID}
};
static const LakeCreateColumnShape viewShape[] = {
    {"view_id",INT8OID},{"view_uuid",UUIDOID},{"begin_snapshot",INT8OID},
    {"end_snapshot",INT8OID},{"schema_id",INT8OID},{"view_name",TEXTOID},
    {"dialect",TEXTOID},{"sql",TEXTOID},{"column_aliases",TEXTOID}
};
static const LakeCreateColumnShape inlineShape[] = {
    {"table_id",INT8OID},{"table_name",TEXTOID},{"schema_version",INT8OID}
};
static const LakeCreateColumnShape versionShape[] = {
    {"begin_snapshot",INT8OID},{"schema_version",INT8OID},{"table_id",INT8OID}
};

static char* LakeCreateRelation(Oid ns, const char* name,
    const LakeCreateColumnShape* columns, int count)
{
    /* The existing helper checks private ownership, ACL, OID registration,
     * physical type safety, triggers/defaults/policies before any owner SPI. */
    if(!OidIsValid(LakeMetadataOwner()))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake CREATE publication requires a configured private metadata owner")));
    char* qualified=LakeMetadataRelation(ns,name);
    Relation relation=heap_open(get_relname_relid(name,ns),AccessShareLock);
    TupleDesc desc=RelationGetDescr(relation);
    if(desc->natts!=count)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake CREATE requires the standard format 1.1 catalog layout")));
    for(int i=0;i<count;++i) {
        AttrNumber a=get_attnum(RelationGetRelid(relation),columns[i].name);
        if(a<=0 || desc->attrs[a-1].attisdropped ||
           (desc->attrs[a-1].atttypid!=columns[i].type &&
            !(columns[i].type==TEXTOID && desc->attrs[a-1].atttypid==VARCHAROID)))
            ereport(ERROR,(errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("native lake CREATE catalog column \"%s.%s\" has an unsupported type",name,columns[i].name)));
    }
    heap_close(relation,NoLock);
    return qualified;
}

char* LakeDefinitionRelation(Oid ns, const char* name)
{
    if(strcmp(name,"ducklake_table")==0) return LakeCreateRelation(ns,name,tableShape,lengthof(tableShape));
    if(strcmp(name,"ducklake_column")==0) return LakeCreateRelation(ns,name,columnShape,lengthof(columnShape));
    if(strcmp(name,"ducklake_column_tag")==0) return LakeCreateRelation(ns,name,columnTagShape,lengthof(columnTagShape));
    if(strcmp(name,"ducklake_schema")==0) return LakeCreateRelation(ns,name,schemaShape,lengthof(schemaShape));
    if(strcmp(name,"ducklake_view")==0) return LakeCreateRelation(ns,name,viewShape,lengthof(viewShape));
    if(strcmp(name,"ducklake_schema_versions")==0) return LakeCreateRelation(ns,name,versionShape,lengthof(versionShape));
    ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("unknown native lake definition relation")));
    return NULL;
}

static unsigned char LakeAsciiLower(unsigned char c)
{
    return c>='A' && c<='Z' ? c+('a'-'A') : c;
}

bool LakeIdentifierEquals(const char* left, const char* right)
{
    if(left==NULL || right==NULL) return false;
    while(*left!='\0' && *right!='\0') {
        if(LakeAsciiLower((unsigned char)*left)!=LakeAsciiLower((unsigned char)*right)) return false;
        ++left; ++right;
    }
    return *left==*right;
}

char* LakeCreateRelativePath(const char* tableUuid, const char* tableName)
{
    if(tableUuid==NULL || *tableUuid=='\0' || tableName==NULL || *tableName=='\0')
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("native lake CREATE identity/name is empty")));
    bool useName=strlen(tableName)<=255; /* pinned DuckLake MAX_FILE_NAME_LENGTH */
    for(const unsigned char* p=(const unsigned char*)tableName;useName && *p!='\0';++p)
        if(!((*p>='a'&&*p<='z') || (*p>='A'&&*p<='Z') || (*p>='0'&&*p<='9') || *p=='_' || *p=='-'))
            useName=false;
    /* The initial local-storage slice has slash separator. UUID is produced and
     * normalized by trusted CREATE; caller must never pass a user path as UUID. */
    return psprintf("%s/",useName?tableName:tableUuid);
}

static void LakeAppendQuotedChangeName(StringInfo result, const char* name)
{
    if(name==NULL || *name=='\0')
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake changeset identifier is empty")));
    appendStringInfoChar(result,'"');
    for(const char* p=name;*p!='\0';++p) {
        if(*p=='"') appendStringInfoChar(result,'"');
        appendStringInfoChar(result,*p);
    }
    appendStringInfoChar(result,'"');
}

void LakeAppendCreatedTableChange(StringInfo result, const char* schema, const char* table)
{
    if(result->len!=0) appendStringInfoChar(result,',');
    appendStringInfoString(result,"created_table:");
    LakeAppendQuotedChangeName(result,schema);
    appendStringInfoChar(result,'.');
    LakeAppendQuotedChangeName(result,table);
}

static char* LakeReadQuotedChangeName(const char** cursor)
{
    const char* p=*cursor;
    if(*p!='"')
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake changeset expected quoted identifier")));
    ++p;
    StringInfoData value; initStringInfo(&value);
    for(;;) {
        if(*p=='\0')
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake changeset has an unterminated identifier")));
        if(*p=='"') {
            ++p;
            if(*p!='"') break;
        }
        appendStringInfoChar(&value,*p++);
    }
    if(value.len==0)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake changeset identifier is empty")));
    *cursor=p;
    return value.data;
}

bool LakeNextPublicationChange(const char** cursor, LakePublicationChange* change)
{
    const char* p=*cursor;
    if(*p=='\0') return false;
    change->table_id=0; change->schema_name=NULL; change->table_name=NULL;
    static const char created[]="created_table:", inserted[]="inlined_insert:", deleted[]="inlined_delete:",
        altered[]="altered_table:", fileInserted[]="inserted_into_table:",
        rewritten[]="rewrite_delete:",merged[]="merge_adjacent:",fileDeleted[]="deleted_from_table:";
    if(strncmp(p,created,sizeof(created)-1)==0) {
        change->kind=LAKE_CHANGE_CREATED_TABLE; p+=sizeof(created)-1;
        change->schema_name=LakeReadQuotedChangeName(&p);
        if(*p!='.')
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake created_table changeset lacks a schema separator")));
        ++p; change->table_name=LakeReadQuotedChangeName(&p);
        if(*p=='.')
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                errmsg("native lake commit cannot merge a nested-schema catalog change")));
    } else {
        if(strncmp(p,inserted,sizeof(inserted)-1)==0) {
            change->kind=LAKE_CHANGE_INLINED_INSERT; p+=sizeof(inserted)-1;
        } else if(strncmp(p,fileInserted,sizeof(fileInserted)-1)==0) {
            change->kind=LAKE_CHANGE_FILE_INSERT; p+=sizeof(fileInserted)-1;
        } else if(strncmp(p,rewritten,sizeof(rewritten)-1)==0) {
            change->kind=LAKE_CHANGE_REWRITE_FILE;p+=sizeof(rewritten)-1;
        } else if(strncmp(p,merged,sizeof(merged)-1)==0) {
            change->kind=LAKE_CHANGE_MERGE_FILE;p+=sizeof(merged)-1;
        } else if(strncmp(p,fileDeleted,sizeof(fileDeleted)-1)==0) {
            change->kind=LAKE_CHANGE_FILE_DELETE;p+=sizeof(fileDeleted)-1;
        } else if(strncmp(p,deleted,sizeof(deleted)-1)==0) {
            change->kind=LAKE_CHANGE_INLINED_DELETE; p+=sizeof(deleted)-1;
        } else if(strncmp(p,altered,sizeof(altered)-1)==0) {
            change->kind=LAKE_CHANGE_ALTERED_TABLE; p+=sizeof(altered)-1;
        } else
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                errmsg("native lake write conflicts with an unsupported concurrent catalog or data change")));
        if(*p<'0'||*p>'9')
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake changeset has an invalid table identity")));
        uint64 id=0;
        do {
            unsigned digit=*p-'0';
            if(id>((uint64)PG_INT64_MAX-digit)/10)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake changeset identity overflows")));
            id=id*10+digit; ++p;
        } while(*p>='0'&&*p<='9');
        if(id>=(uint64)LakeCreateLocalIdBoundary)
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake changeset contains a reference-local identity")));
        change->table_id=(int64)id;
    }
    if(*p!='\0') {
        if(*p!=',')
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake changeset has an invalid separator")));
        ++p;
        if(*p=='\0')
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake changeset has a trailing separator")));
    }
    *cursor=p;
    return true;
}

/* Only the future table-certificate reader uses this additive grammar.
 * Do not loosen LakeNextPublicationChange's historical conflict contract. */
bool LakeNextCertificateChange(const char** cursor,LakePublicationChange* change)
{
    const char* p=*cursor;
    if(strncmp(p,"inline_flush:",13)==0)p+=13;
    else if(strncmp(p,"flushed_inlined:",16)==0)p+=16;
    else return LakeNextPublicationChange(cursor,change);
    if(*p<'0'||*p>'9')ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid certificate flush table identity")));
    uint64 id=0;
    do {unsigned digit=*p-'0';
        if(id>((uint64)PG_INT64_MAX-digit)/10)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("certificate table identity overflows")));
        id=id*10+digit;++p;
    }while(*p>='0'&&*p<='9');
    if(id>=(uint64)LakeCreateLocalIdBoundary)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("certificate cannot use a reference-local table identity")));
    if(*p!='\0'){
        if(*p!=','||p[1]=='\0')ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid certificate changeset separator")));
        ++p;
    }
    change->kind=LAKE_CHANGE_INLINE_FLUSH;change->table_id=(int64)id;
    change->schema_name=NULL;change->table_name=NULL;*cursor=p;return true;
}

static const char* LakeCreateTypeName(Oid type,int32 typmod)
{
    LakeNativeTypeRequireWrite(type);
    return LakeNativeDuckType(type,typmod);
}

static void LakeCheckCreateShape(const LakeCreateDefinition* definition)
{
    if(definition==NULL || definition->binding==NULL || definition->schema_id<0 ||
       definition->schema_id>=LakeCreateLocalIdBoundary || definition->schema_uuid==NULL ||
       definition->schema_name==NULL || *definition->schema_name=='\0' ||
       definition->table_name==NULL || *definition->table_name=='\0')
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake CREATE descriptor is incomplete")));
    const LakeInlineBinding* b=definition->binding;
    if(!OidIsValid(b->relation_oid) || !OidIsValid(b->server_oid) || !OidIsValid(b->metadata_schema_oid) ||
       b->table_uuid==NULL || b->read_only || b->column_count<=0 ||
       b->column_count>MaxHeapAttributeNumber-3 || b->columns==NULL)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake CREATE binding is incomplete or read-only")));
    if(OidIsValid(b->inline_oid)!=(definition->inline_name!=NULL))
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake CREATE carrier name and identity disagree")));
    char* expected=LakeCreateRelativePath(b->table_uuid,definition->table_name);
    if(definition->relative_path==NULL || strcmp(expected,definition->relative_path)!=0)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake CREATE path differs from its stable definition")));
    pfree(expected);
    if(b->literal_defaults&&!LakeCreateLiteralDefaultVersionEnabled())
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native CREATE defaults require exact installed SQL49")));
    int64 nextColumnId=1;
    for(int i=0;i<b->column_count;++i) {
        const LakeInlineColumn* c=&b->columns[i];
        if(c->column_id!=nextColumnId++ || c->output_attnum!=i+1 || c->name==NULL || *c->name=='\0')
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake CREATE column identity/order is invalid")));
        if(c->type==VECTOROID){
            if(c->typmod<1||c->typmod>VECTOR_MAX_DIM||c->element_column_id!=nextColumnId++)
                ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake CREATE vector definition is invalid")));
        }else if(!LakeNativeFlatTypmod(c->type,c->typmod)||c->element_column_id!=-1)
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake CREATE scalar definition is invalid")));
        (void)LakeCreateTypeName(c->type,c->typmod);
        if(c->initial_value_proven||c->initial_null_proven||
           (b->literal_defaults&&!c->defaults_validated)||
           (c->initial_literal!=NULL)!=(c->current_literal!=NULL)||
           (c->initial_literal&&(!b->literal_defaults||!LakeLiteralDefaultType(c->type,c->typmod)||
             strcmp(c->initial_literal,c->current_literal))))
            ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native CREATE initial/current defaults or birth proof changed")));
        if(c->not_null&&(!b->not_null_constraints||!LakeNotNullType(c->type)))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake CREATE NOT NULL requires an admitted flat scalar")));
        if(strlen(c->name)>=9) {
            static const char reserved[]="_ducklake_";
            bool match=true;
            for(Size k=0;k<sizeof(reserved)-1;++k) match &= LakeAsciiLower((unsigned char)c->name[k])==reserved[k];
            if(match)
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake CREATE column uses a reserved prefix")));
        }
        for(int j=0;j<i;++j)
            if(LakeIdentifierEquals(c->name,b->columns[j].name))
                ereport(ERROR,(errcode(ERRCODE_DUPLICATE_COLUMN),errmsg("native lake CREATE column names conflict under lake identifier rules")));
    }
}

LakeCreateAttempt* LakePlanCreateAttempt(const LakeCreateDefinition* const* definitions,
    int count, int64 headSnapshot, int64 headSchemaVersion, int64 nextCatalogId,
    int64* finalNextCatalogId, int64* finalSchemaVersion)
{
    if(count<0 || headSnapshot<0 || headSnapshot==PG_INT64_MAX || headSchemaVersion<0 ||
       nextCatalogId<0 || nextCatalogId>LakeCreateLocalIdBoundary ||
       (count>0 && (definitions==NULL || headSchemaVersion==PG_INT64_MAX ||
                    (int64)count>LakeCreateLocalIdBoundary-nextCatalogId)))
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake CREATE publication counters are out of range")));
    *finalNextCatalogId=nextCatalogId+count;
    *finalSchemaVersion=headSchemaVersion+(count>0?1:0);
    if(count==0) return NULL;
    if((Size)count>MaxAllocSize/sizeof(LakeCreateAttempt))
        ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("native lake CREATE attempt contains too many tables")));
    LakeCreateAttempt* result=(LakeCreateAttempt*)palloc0(sizeof(LakeCreateAttempt)*count);
    for(int i=0;i<count;++i) {
        LakeCheckCreateShape(definitions[i]);
        for(int j=0;j<i;++j) {
            if(definitions[i]->binding->server_oid!=definitions[j]->binding->server_oid ||
               definitions[i]->binding->metadata_schema_oid!=definitions[j]->binding->metadata_schema_oid)
                ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native lake CREATE publication requires a single catalog")));
            if(definitions[i]->binding->relation_oid==definitions[j]->binding->relation_oid ||
               strcmp(definitions[i]->binding->table_uuid,definitions[j]->binding->table_uuid)==0 ||
               (OidIsValid(definitions[i]->binding->inline_oid) &&
                definitions[i]->binding->inline_oid==definitions[j]->binding->inline_oid) ||
               (definitions[i]->schema_id==definitions[j]->schema_id &&
                LakeIdentifierEquals(definitions[i]->table_name,definitions[j]->table_name)))
                ereport(ERROR,(errcode(ERRCODE_DUPLICATE_TABLE),errmsg("native lake CREATE batch contains duplicate relations or names")));
        }
        result[i].definition=definitions[i]; result[i].permanent_table_id=nextCatalogId+i;
        result[i].commit_snapshot=headSnapshot+1; result[i].commit_schema_version=*finalSchemaVersion;
    }
    return result;
}

void LakeValidateCreateDefinition(const LakeCreateDefinition* d, Snapshot snapshot, int64 headSnapshot)
{
    LakeCheckCreateShape(d);
    if(d->binding->literal_defaults){
        Relation host=heap_open(d->binding->relation_oid,AccessShareLock);
        LakeValidateHostDefaults(d->binding,host);
        heap_close(host,NoLock);
    }
    Oid ns=d->binding->metadata_schema_oid;
    char* schemas=LakeCreateRelation(ns,"ducklake_schema",schemaShape,lengthof(schemaShape));
    Oid types[2]={INT8OID,INT8OID}; Datum args[2]={Int64GetDatum(d->schema_id),Int64GetDatum(headSnapshot)};
    LakeMetaRows* parent=LakeMetadataQuery(psprintf("SELECT schema_uuid::text,schema_name::text,parent_schema_id FROM %s "
        "WHERE schema_id=$1 AND begin_snapshot<=$2 AND (end_snapshot IS NULL OR end_snapshot>$2)",schemas),
        2,types,args,NULL,snapshot,true,SPI_OK_SELECT,ns);
    if(parent->count==0)
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake CREATE parent schema no longer exists")));
    LakeMetadataRequireCount(parent,1,"CREATE parent schemas");
    bool parentIsNull=false;
    (void)LakeMetadataValue(parent,0,3,INT8OID,true,&parentIsNull);
    if(!parentIsNull || strcmp(LakeMetadataText(parent,0,1),d->schema_uuid)!=0 ||
       strcmp(LakeMetadataText(parent,0,2),d->schema_name)!=0)
        ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake CREATE parent schema changed before publication")));
    char* tables=LakeCreateRelation(ns,"ducklake_table",tableShape,lengthof(tableShape));
    char* views=LakeCreateRelation(ns,"ducklake_view",viewShape,lengthof(viewShape));
    /* Use an exact ASCII fold in C rather than locale-sensitive SQL lower().
     * Both relations share the standard name namespace. */
    LakeMetaRows* names=LakeMetadataQuery(psprintf("SELECT table_name::text AS name FROM %s WHERE schema_id=$1 "
        "AND begin_snapshot<=$2 AND (end_snapshot IS NULL OR end_snapshot>$2) UNION ALL "
        "SELECT view_name::text AS name FROM %s WHERE schema_id=$1 AND begin_snapshot<=$2 "
        "AND (end_snapshot IS NULL OR end_snapshot>$2)",tables,views),
        2,types,args,NULL,snapshot,true,SPI_OK_SELECT,ns);
    for(uint32 i=0;i<names->count;++i)
        if(LakeIdentifierEquals(LakeMetadataText(names,i,1),d->table_name))
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("native lake CREATE name was committed by another transaction")));
    if(OidIsValid(d->binding->inline_oid)) {
        (void)LakeMetadataRelation(ns,d->inline_name);
        if(get_relname_relid(d->inline_name,ns)!=d->binding->inline_oid)
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("native lake CREATE carrier identity changed")));
    }
}

void LakeWriteCreateDefinition(const LakeCreateAttempt* a, Snapshot snapshot)
{
    if(a==NULL || a->permanent_table_id<0 || a->permanent_table_id>=LakeCreateLocalIdBoundary ||
       a->commit_snapshot<=0 || a->commit_schema_version<=0)
        ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("native lake CREATE attempt has invalid permanent identifiers")));
    const LakeCreateDefinition* d=a->definition;
    LakeCheckCreateShape(d);
    const LakeInlineBinding* b=d->binding;
    Oid ns=b->metadata_schema_oid;
    char* tables=LakeCreateRelation(ns,"ducklake_table",tableShape,lengthof(tableShape));
    char* columns=LakeCreateRelation(ns,"ducklake_column",columnShape,lengthof(columnShape));
    char* versions=LakeCreateRelation(ns,"ducklake_schema_versions",versionShape,lengthof(versionShape));
    Oid tt[6]={INT8OID,TEXTOID,INT8OID,INT8OID,TEXTOID,TEXTOID};
    Datum ta[6]={Int64GetDatum(a->permanent_table_id),CStringGetTextDatum(b->table_uuid),
        Int64GetDatum(a->commit_snapshot),Int64GetDatum(d->schema_id),
        CStringGetTextDatum(d->table_name),CStringGetTextDatum(d->relative_path)};
    LakeMetaRows* written=LakeMetadataQuery(psprintf("INSERT INTO %s "
        "(table_id,table_uuid,begin_snapshot,end_snapshot,schema_id,table_name,path,path_is_relative) "
        "VALUES($1,$2::pg_catalog.uuid,$3,NULL,$4,$5,$6,true)",tables),
        6,tt,ta,NULL,snapshot,false,SPI_OK_INSERT,ns);
    LakeMetadataRequireCount(written,1,"created table definitions");
    for(int i=0;i<b->column_count;++i) {
        const LakeInlineColumn* c=&b->columns[i];
        bool expression=false;
        const char* current=LakeCurrentDefaultMetadata(c->type,c->current_literal,&expression);
        Oid ct[9]={INT8OID,INT8OID,INT8OID,TEXTOID,TEXTOID,BOOLOID,TEXTOID,TEXTOID,TEXTOID};
        Datum ca[9]={Int64GetDatum(c->column_id),Int64GetDatum(a->commit_snapshot),
            Int64GetDatum(a->permanent_table_id),CStringGetTextDatum(c->name),CStringGetTextDatum(LakeCreateTypeName(c->type,c->typmod)),BoolGetDatum(!c->not_null),
            c->initial_literal?CStringGetTextDatum(c->initial_literal):(Datum)0,
            CStringGetTextDatum(current),CStringGetTextDatum(c->type==VECTOROID?"":expression?"expression":"literal")};
        char nulls[10]={' ',' ',' ',' ',' ',' ',c->initial_literal?' ':'n',' ',' ','\0'};
        written=LakeMetadataQuery(psprintf("INSERT INTO %s "
            "(column_id,begin_snapshot,end_snapshot,table_id,column_order,column_name,column_type,"
            "initial_default,default_value,nulls_allowed,parent_column,default_value_type,default_value_dialect) "
            "VALUES($1,$2,NULL,$3,$1,$4,$5,$7,$8,$6,NULL,$9,'duckdb')",columns),
            9,ct,ca,nulls,snapshot,false,SPI_OK_INSERT,ns);
        LakeMetadataRequireCount(written,1,"created column definitions");
        if(c->type==VECTOROID){
            Oid et[4]={INT8OID,INT8OID,INT8OID,INT8OID};
            Datum ea[4]={Int64GetDatum(c->element_column_id),Int64GetDatum(a->commit_snapshot),
                Int64GetDatum(a->permanent_table_id),Int64GetDatum(c->column_id)};
            written=LakeMetadataQuery(psprintf("INSERT INTO %s "
                "(column_id,begin_snapshot,end_snapshot,table_id,column_order,column_name,column_type,"
                "initial_default,default_value,nulls_allowed,parent_column,default_value_type,default_value_dialect) "
                "VALUES($1,$2,NULL,$3,$1,'element','float32',NULL,'NULL',true,$4,'literal','duckdb')",columns),
                4,et,ea,NULL,snapshot,false,SPI_OK_INSERT,ns);
            LakeMetadataRequireCount(written,1,"created vector element definitions");
        }
    }
    if(OidIsValid(b->inline_oid)) {
        char* registrations=LakeCreateRelation(ns,"ducklake_inlined_data_tables",inlineShape,lengthof(inlineShape));
        Oid it[3]={INT8OID,TEXTOID,INT8OID};
        Datum ia[3]={Int64GetDatum(a->permanent_table_id),CStringGetTextDatum(d->inline_name),Int64GetDatum(a->commit_schema_version)};
        written=LakeMetadataQuery(psprintf("INSERT INTO %s(table_id,table_name,schema_version) VALUES($1,$2,$3)",registrations),
            3,it,ia,NULL,snapshot,false,SPI_OK_INSERT,ns);
        LakeMetadataRequireCount(written,1,"created inline registrations");
    }
    Oid vt[3]={INT8OID,INT8OID,INT8OID};
    Datum va[3]={Int64GetDatum(a->commit_snapshot),Int64GetDatum(a->commit_schema_version),Int64GetDatum(a->permanent_table_id)};
    written=LakeMetadataQuery(psprintf("INSERT INTO %s(begin_snapshot,schema_version,table_id) VALUES($1,$2,$3)",versions),
        3,vt,va,NULL,snapshot,false,SPI_OK_INSERT,ns);
    LakeMetadataRequireCount(written,1,"created schema version records");
}

Snapshot LakeCreatePushOwnWriteSnapshot(Snapshot publicationSnapshot)
{
    if(publicationSnapshot==InvalidSnapshot)
        ereport(ERROR,(errcode(ERRCODE_INTERNAL_ERROR),errmsg("native lake CREATE publication snapshot is missing")));
    CommandCounterIncrement();
    PushCopiedSnapshot(publicationSnapshot);
    UpdateActiveSnapshotCommandId();
    return GetActiveSnapshot();
}
