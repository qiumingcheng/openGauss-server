/* Reuse the host B-tree builder and IndexAM for private inline carriers.
 * This is an index over physical versions, not a second lake row authority. */
#include "postgres.h"
#include "lake_cost.h"
#include "knl/knl_variable.h"
#include "lake_scalar_key.h"
#include "lake_index_private.h"
#include "lake_file_scalar.h"
#include "lake_stream.h"
#include "lake_file_ann_catalog.h"
#include "lake_inline_ann.h"
#include "lake_txn_private.h"
#include "lake_version.h"
#include "lake_index_maintenance.h"
#include "access/heapam.h"
#include "access/genam.h"
#include "access/transam.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_index.h"
#include "catalog/pg_am.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_type.h"
#include "access/skey.h"
#include "commands/defrem.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/plancat.h"
#include "optimizer/paths.h"
#include "utils/spccache.h"
#include "lake_inline_reader.h"
#include <math.h>
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/selfuncs.h"

PG_FUNCTION_INFO_V1(og_lake_build_lake_index);
PG_FUNCTION_INFO_V1(og_lake_rebuild_lake_index);

/* The logical file method does not change the trusted inline IndexAM: both
 * scalar formats use an ordinary native B-tree/UB-tree over the carrier. */
static bool LakeInlineScalarMethod(const char* kind,const char* method)
{
    if(strcmp(kind,"scalar")!=0)return false;
    if(strcmp(method,"btree")==0)return true;
#ifdef USE_LAKE_BITMAP
    if(strcmp(method,"bitmap")==0)return true;
#endif
    return false;
}

static Oid LakeInlineOperatorClass(Oid type,bool ustore)
{
    const char* name=LakeScalarClassName(type);
    Oid method=ustore?UBTREE_AM_OID:BTREE_AM_OID;
    HeapTuple tuple=SearchSysCache3(CLAAMNAMENSP,ObjectIdGetDatum(method),
        CStringGetDatum(name),ObjectIdGetDatum(PG_CATALOG_NAMESPACE));
    if(!HeapTupleIsValid(tuple))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("native scalar B-tree operator class is unavailable")));
    Oid oid=HeapTupleGetOid(tuple);Form_pg_opclass op=(Form_pg_opclass)GETSTRUCT(tuple);
    if(oid>=FirstNormalObjectId||op->opcmethod!=method||op->opcnamespace!=PG_CATALOG_NAMESPACE||
        op->opcowner!=BOOTSTRAP_SUPERUSERID||op->opcfamily!=LakeScalarFamily(type,ustore)||op->opcintype!=type||OidIsValid(op->opckeytype))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("unreviewed native scalar operator class")));
    ReleaseSysCache(tuple);return oid;
}

Relation LakeOpenInlineIndex(Oid indexOid,const LakeInlineBinding* binding,AttrNumber column)
{
    if(column<=0 || column>binding->column_count || !OidIsValid(binding->inline_oid))
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("invalid lake inline index binding")));
    Relation carrier=heap_open(binding->inline_oid,AccessShareLock);
    LakeValidatePrivateRelation(carrier,LakeMetadataOwner(),false);
    Oid type=binding->columns[column-1].type;
    if(!LakeScalarIndexedType(type))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("unsupported native lake B-tree scalar key type")));
    AttrNumber attribute=get_attnum(binding->inline_oid,LakeInlinePhysicalName(&binding->columns[column-1]));
    Relation index=LakeOpenTrustedInlineIndex(indexOid,LakeMetadataOwner());
    bool ustore=RelationIsUstoreFormat(carrier);
    Oid opclass=LakeInlineOperatorClass(type,ustore);
    HeapTuple tuple=SearchSysCache1(INDEXRELID,ObjectIdGetDatum(indexOid));
    if(!HeapTupleIsValid(tuple))
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("lake inline index disappeared")));
    bool isnull=false;
    oidvector* classes=(oidvector*)DatumGetPointer(SysCacheGetAttr(INDEXRELID,tuple,Anum_pg_index_indclass,&isnull));
    if(index->rd_rel->relam!=(ustore?UBTREE_AM_OID:BTREE_AM_OID)||
       index->rd_index->indrelid!=binding->inline_oid || index->rd_index->indisunique ||
       index->rd_index->indisprimary || index->rd_index->indnatts!=1 || attribute<=0 ||
       index->rd_opfamily[0]!=LakeScalarFamily(type,ustore)||
       index->rd_index->indkey.values[0]!=attribute || isnull || classes->dim1!=1 || classes->values[0]!=opclass)
        ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("lake inline index identity or key changed")));
    ReleaseSysCache(tuple);heap_close(carrier,NoLock);return index;
}

static Oid LakeBuildNativeInlineIndex(LakeInlineBinding* binding,int column,const char* uuid)
{
    if(!OidIsValid(binding->inline_oid))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("lake index has no inline carrier to build")));
    Relation carrier=heap_open(binding->inline_oid,ShareLock);
    Oid owner=LakeMetadataOwner();
    LakeValidatePrivateRelation(carrier,owner,false);
    LakeCheckMetadataObjectIdentity(binding->metadata_schema_oid,binding->inline_oid,owner);
    bool ustore=RelationIsUstoreFormat(carrier);
    IndexStmt* statement=makeNode(IndexStmt);
    statement->relation=makeRangeVar(binding->metadata_schema,pstrdup(RelationGetRelationName(carrier)),-1);
    statement->schemaname=pstrdup(binding->metadata_schema);
    char compact[33];int length=0;
    for(const char* p=uuid;*p;++p)if(*p!='-') {
        if(length>=32)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake index UUID")));
        compact[length++]=*p;
    }
    if(length!=32)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("invalid lake index UUID")));
    compact[length]='\0';statement->idxname=psprintf("_og_lake_ix_%s",compact);
    statement->accessMethod=pstrdup(ustore?"ubtree":"btree");statement->isvalidated=true;
    IndexElem* key=makeNode(IndexElem);
    key->name=pstrdup(LakeInlinePhysicalName(&binding->columns[column]));
    key->opclass=list_make2(makeString(pstrdup("pg_catalog")),
        makeString(pstrdup(LakeScalarClassName(binding->columns[column].type))));
    key->ordering=SORTBY_DEFAULT;key->nulls_ordering=SORTBY_NULLS_DEFAULT;
    statement->indexParams=list_make1(key);
    Oid savedUser;int savedSecurity;GetUserIdAndSecContext(&savedUser,&savedSecurity);
    Oid result=InvalidOid;
    PG_TRY();
    {
        /* No caller expression or SQL is evaluated under the private role.
         * The complete carrier was checked and the key/operator class fixed. */
        SetUserIdAndSecContext(owner,savedSecurity|SECURITY_LOCAL_USERID_CHANGE|SECURITY_RESTRICTED_OPERATION);
        ObjectAddress address=DefineIndex(binding->inline_oid,statement,InvalidOid,false,true,false,true);
        result=address.objectId;
        SetUserIdAndSecContext(savedUser,savedSecurity);
    }
    PG_CATCH();{SetUserIdAndSecContext(savedUser,savedSecurity);PG_RE_THROW();}PG_END_TRY();
    CommandCounterIncrement();
    Relation checked=LakeOpenInlineIndex(result,binding,column+1);index_close(checked,NoLock);
    heap_close(carrier,NoLock);return result;
}

Datum og_lake_build_lake_index(PG_FUNCTION_ARGS)
{
    if(PG_ARGISNULL(0))
        ereport(ERROR,(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),errmsg("lake index UUID is required")));
    (void)LakeIndexCatalog();
    if(!LakeHasInlineIndexVersion())
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("lake index build requires extension0.15")));
    char* uuid=DatumGetCString(DirectFunctionCall1(uuid_out,PG_GETARG_DATUM(0)));
    Oid uuidType=UUIDOID;Datum uuidArg=PG_GETARG_DATUM(0);
    Snapshot first=RegisterSnapshot(GetLatestSnapshot());
    Oid hostOid=InvalidOid;
    PG_TRY();
    {
        LakeIndexPermission(uuid,first);
        LakeMetaRows* rows=LakeMetadataQuery("SELECT r.relation_oid FROM og_lake_internal.index_definition d "
            "JOIN og_lake_internal.relation_binding r USING(binding_id) WHERE d.index_uuid=$1 AND d.state='ACTIVE'",
            1,&uuidType,&uuidArg,NULL,first,true,SPI_OK_SELECT);
        LakeMetadataRequireCount(rows,1,"active lake index host");
        hostOid=DatumGetObjectId(LakeMetadataValue(rows,0,1,OIDOID));UnregisterSnapshot(first);
    }
    PG_CATCH();{UnregisterSnapshot(first);PG_RE_THROW();}PG_END_TRY();
    /* Host relation precedes definition row lock, matching DROP/RENAME. */
    Relation host=heap_open(hostOid,AccessShareLock);LakeIndexDdl();
    Snapshot snapshot=RegisterSnapshot(GetLatestSnapshot());
    PG_TRY();
    {
        LakeIndexPermission(uuid,snapshot);
        LakeMetaRows* rows=LakeMetadataQuery("SELECT d.column_id,d.type_oid,d.inline_index_oid,r.relation_oid,d.table_id,d.table_uuid::text,d.server_oid,"
            "d.definition_generation,d.definition_hash::text,d.definition->>'metric',"
            "COALESCE(d.definition->>'kind',''),COALESCE(d.definition->>'method',''),"
            "COALESCE((d.definition->>'pq')::boolean,false),COALESCE((d.definition->>'pq_chunks')::integer,1) "
            "FROM og_lake_internal.index_definition d JOIN og_lake_internal.relation_binding r USING(binding_id) "
            "WHERE d.index_uuid=$1 AND d.state='ACTIVE' FOR UPDATE OF d",
            1,&uuidType,&uuidArg,NULL,snapshot,false,SPI_OK_SELECT);
        LakeMetadataRequireCount(rows,1,"locked active lake index definition");
        if(DatumGetObjectId(LakeMetadataValue(rows,0,4,OIDOID))!=hostOid)
            ereport(ERROR,(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),errmsg("lake index host changed during build admission")));
        LakeInlineBinding* binding=LakeLoadCommittedInlineBinding(hostOid,snapshot);
        if(!LakeIxSmpExtendedVersionEnabled()){
            LakeRejectBinaryAcceleration(binding,"lake index build");
            LakeRejectUuidAcceleration(binding,"lake index build");
            LakeRejectJsonAcceleration(binding,"lake index build");
            LakeRejectNumericAcceleration(binding,"lake index build");
        }
        if(DatumGetInt64(LakeMetadataValue(rows,0,5,INT8OID))!=LakePermanentTableId(binding) ||
           strcmp(LakeMetadataText(rows,0,6),binding->table_uuid)!=0 ||
           DatumGetObjectId(LakeMetadataValue(rows,0,7,OIDOID))!=binding->server_oid)
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("lake index table identity changed")));
        if(binding->read_only)
            ereport(ERROR,(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),errmsg("cannot build an index for a read-only lake table")));
        int64 columnId=DatumGetInt64(LakeMetadataValue(rows,0,1,INT8OID));
        Oid type=DatumGetObjectId(LakeMetadataValue(rows,0,2,OIDOID));int column=-1;
        for(int i=0;i<binding->column_count;++i)
            if(binding->columns[i].column_id==columnId && binding->columns[i].type==type)column=i;
        if(column<0 || (!LakeScalarIndexedType(type) && type!=VECTOROID))
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("lake index key mapping changed")));
        const char* kind=LakeMetadataText(rows,0,11);const char* method=LakeMetadataText(rows,0,12);
        bool bitmap=strcmp(method,"bitmap")==0;
#ifndef USE_LAKE_BITMAP
        if(bitmap)ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("lake bitmap index support was not compiled")));
#endif
        if(type==VECTOROID?(strcmp(kind,"vector")!=0||strcmp(method,"diskann")!=0):!LakeInlineScalarMethod(kind,method))
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("lake index kind, method or key mapping changed")));
        int metric=type==VECTOROID?LakeAnnMetric(LakeMetadataText(rows,0,10)):0;
        int files=metric?LakeBuildAnnFileIndexes(binding,uuid,column,metric,
            DatumGetInt64(LakeMetadataValue(rows,0,8,INT8OID)),LakeMetadataText(rows,0,9),snapshot,
            DatumGetBool(LakeMetadataValue(rows,0,13,BOOLOID)),DatumGetInt32(LakeMetadataValue(rows,0,14,INT4OID))):LakeBuildScalarFileIndexes(binding,uuid,column,
            DatumGetInt64(LakeMetadataValue(rows,0,8,INT8OID)),LakeMetadataText(rows,0,9),snapshot,bitmap);
        if(!OidIsValid(binding->inline_oid)&&files<0)
            ereport(ERROR,(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),errmsg("configure a committed index_root before building a file-only lake index")));
        bool absent=false;Datum existing=LakeMetadataValue(rows,0,3,OIDOID,true,&absent);
        if(!absent) {
            Relation index=metric?LakeOpenInlineAnnIndex(DatumGetObjectId(existing),binding,column+1,metric):
                LakeOpenInlineIndex(DatumGetObjectId(existing),binding,column+1);index_close(index,NoLock);
        } else if(OidIsValid(binding->inline_oid)||!LakeHasIndexRootVersion()||(files==0&&binding->file_count==0)) {
            bool fileOnlyAnn=false;
            if(metric&&OidIsValid(binding->inline_oid)){
                Relation carrier=heap_open(binding->inline_oid,AccessShareLock);
                fileOnlyAnn=RelationIsUstoreFormat(carrier)&&binding->file_count>0&&files>=0;
                heap_close(carrier,NoLock);
            }
            /* File graphs use their own owned astore staging relation. A
             * Ustore carrier remains a complete TableAM input; do not undo
             * valid file publication by attempting an unsupported native
             * Ustore DiskANN. With no usable file backend, keep the explicit
             * native adapter rejection below. No inline OID is registered. */
            if(!fileOnlyAnn){
                Oid indexOid=metric?LakeBuildInlineAnnIndex(binding,column,uuid,metric):LakeBuildNativeInlineIndex(binding,column,uuid);
                Oid types[3]={UUIDOID,OIDOID,INT8OID};Datum args[3]={uuidArg,ObjectIdGetDatum(indexOid),Int64GetDatum(binding->snapshot_id)};
                /* For ANN, registration of the usable backend also records its
                 * earliest supported lake snapshot. An older reader scans fully. */
                LakeMetaRows* changed=LakeMetadataQuery(metric?
                    "UPDATE og_lake_internal.index_definition SET inline_index_oid=$2,registered_snapshot=$3 "
                    "WHERE index_uuid=$1 AND state='ACTIVE' AND inline_index_oid IS NULL":
                    "UPDATE og_lake_internal.index_definition SET inline_index_oid=$2 "
                    "WHERE index_uuid=$1 AND state='ACTIVE' AND inline_index_oid IS NULL",metric?3:2,types,args,NULL,
                    snapshot,false,SPI_OK_UPDATE);
                LakeMetadataRequireCount(changed,1,"built inline index binding");
            }
        }
        LakeIndexCursorReset(uuid);
        LakeInvalidateRelationSafe(hostOid);
        UnregisterSnapshot(snapshot);
    }
    PG_CATCH();{UnregisterSnapshot(snapshot);PG_RE_THROW();}PG_END_TRY();
    heap_close(host,NoLock);PG_RETURN_VOID();
}

Datum og_lake_rebuild_lake_index(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0))
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("lake index UUID is required")));
    (void)LakeIndexCatalog();
    if (!LakeBindSnapshotVersionEnabled())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("rebuild_lake_index requires installed SQL0.59")));
    char* uuid = DatumGetCString(DirectFunctionCall1(uuid_out, PG_GETARG_DATUM(0)));
    Oid uuidType = UUIDOID;
    Datum uuidArg = PG_GETARG_DATUM(0);
    Snapshot first = RegisterSnapshot(GetLatestSnapshot());
    Oid hostOid = InvalidOid;
    PG_TRY();
    {
        LakeMetaRows* exists = LakeMetadataQuery(
            "SELECT d.state::text FROM og_lake_internal.index_definition d WHERE d.index_uuid=$1",
            1, &uuidType, &uuidArg, NULL, first, true, SPI_OK_SELECT);
        if (exists->count == 0)
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("lake index does not exist")));
        LakeIndexPermission(uuid, first);
        if (strcmp(LakeMetadataText(exists, 0, 1), "ACTIVE") != 0)
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("cannot rebuild a retired lake index")));
        LakeMetaRows* rows = LakeMetadataQuery(
            "SELECT r.relation_oid FROM og_lake_internal.index_definition d "
            "JOIN og_lake_internal.relation_binding r USING(binding_id) WHERE d.index_uuid=$1 AND d.state='ACTIVE'",
            1, &uuidType, &uuidArg, NULL, first, true, SPI_OK_SELECT);
        LakeMetadataRequireCount(rows, 1, "active lake index host");
        hostOid = DatumGetObjectId(LakeMetadataValue(rows, 0, 1, OIDOID));
        UnregisterSnapshot(first);
    }
    PG_CATCH(); { UnregisterSnapshot(first); PG_RE_THROW(); } PG_END_TRY();

    Relation host = heap_open(hostOid, AccessShareLock);
    LakeIndexDdl();
    Snapshot volatile snapshot = RegisterSnapshot(GetLatestSnapshot());
    PG_TRY();
    {
        LakeIndexPermission(uuid, snapshot);
        LakeMetaRows* rows = LakeMetadataQuery(
            "SELECT d.column_id,d.type_oid,d.inline_index_oid,r.relation_oid,d.table_id,d.table_uuid::text,d.server_oid,"
            "d.definition_generation,d.definition_hash::text,d.definition->>'metric',"
            "COALESCE(d.definition->>'kind',''),COALESCE(d.definition->>'method',''),"
            "COALESCE((d.definition->>'pq')::boolean,false),COALESCE((d.definition->>'pq_chunks')::integer,1) "
            "FROM og_lake_internal.index_definition d JOIN og_lake_internal.relation_binding r USING(binding_id) "
            "WHERE d.index_uuid=$1 AND d.state='ACTIVE' FOR UPDATE OF d",
            1, &uuidType, &uuidArg, NULL, snapshot, false, SPI_OK_SELECT);
        LakeMetadataRequireCount(rows, 1, "locked active lake index definition");
        if (DatumGetObjectId(LakeMetadataValue(rows, 0, 4, OIDOID)) != hostOid)
            ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE), errmsg("lake index host changed during rebuild admission")));
        LakeInlineBinding* binding = LakeLoadCommittedInlineBinding(hostOid, snapshot);
        if(!LakeIxSmpExtendedVersionEnabled()){
            LakeRejectBinaryAcceleration(binding, "lake index rebuild");
            LakeRejectUuidAcceleration(binding, "lake index rebuild");
            LakeRejectJsonAcceleration(binding, "lake index rebuild");
            LakeRejectNumericAcceleration(binding, "lake index rebuild");
        }
        if (DatumGetInt64(LakeMetadataValue(rows, 0, 5, INT8OID)) != LakePermanentTableId(binding) ||
            strcmp(LakeMetadataText(rows, 0, 6), binding->table_uuid) != 0 ||
            DatumGetObjectId(LakeMetadataValue(rows, 0, 7, OIDOID)) != binding->server_oid)
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("lake index table identity changed")));
        if (binding->read_only)
            ereport(ERROR, (errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION), errmsg("cannot rebuild an index for a read-only lake table")));
        int64 columnId = DatumGetInt64(LakeMetadataValue(rows, 0, 1, INT8OID));
        Oid type = DatumGetObjectId(LakeMetadataValue(rows, 0, 2, OIDOID));
        int column = -1;
        for (int i = 0; i < binding->column_count; ++i)
            if (binding->columns[i].column_id == columnId && binding->columns[i].type == type)
                column = i;
        if (column < 0 || (!LakeScalarIndexedType(type) && type != VECTOROID))
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("lake index key mapping changed")));
        const char* kind = LakeMetadataText(rows, 0, 11);
        const char* method = LakeMetadataText(rows, 0, 12);
        bool bitmap = strcmp(method, "bitmap") == 0;
#ifndef USE_LAKE_BITMAP
        if (bitmap)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("lake bitmap index support was not compiled")));
#endif
        if (type == VECTOROID ? (strcmp(kind, "vector") != 0 || strcmp(method, "diskann") != 0) : !LakeInlineScalarMethod(kind, method))
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("lake index kind, method or key mapping changed")));
        int metric = type == VECTOROID ? LakeAnnMetric(LakeMetadataText(rows, 0, 10)) : 0;
        int64 oldGen = DatumGetInt64(LakeMetadataValue(rows, 0, 8, INT8OID));
        if (oldGen <= 0 || oldGen >= PG_INT64_MAX)
            ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("lake index definition generation overflow")));
        int64 newGen = oldGen + 1;
        const char* defHash = pstrdup(LakeMetadataText(rows, 0, 9));
        bool pqReq = DatumGetBool(LakeMetadataValue(rows, 0, 13, BOOLOID));
        int32 pqChunks = DatumGetInt32(LakeMetadataValue(rows, 0, 14, INT4OID));
        bool absent = false;
        Datum existing = LakeMetadataValue(rows, 0, 3, OIDOID, true, &absent);

        /* Retire prior generation segments/coverage without deleting artifacts
         * that concurrent readers may still hold open. Late background builders
         * revalidating against oldGen will detect definition_generation != oldGen. */
        (void)LakeMetadataQuery(
            "UPDATE og_lake_internal.index_coverage SET active=false WHERE index_uuid=$1 AND active",
            1, &uuidType, &uuidArg, NULL, snapshot, false, SPI_OK_UPDATE);
        (void)LakeMetadataQuery(
            "UPDATE og_lake_internal.index_segment SET state=CASE WHEN state='BUILDING' THEN 'FAILED' "
            "ELSE 'RETIRED' END WHERE index_uuid=$1 AND state IN ('BUILDING','READY')",
            1, &uuidType, &uuidArg, NULL, snapshot, false, SPI_OK_UPDATE);
        Oid genTypes[4] = {UUIDOID, INT8OID, INT8OID, INT8OID};
        Datum genArgs[4] = {uuidArg, Int64GetDatum(newGen), Int64GetDatum(binding->snapshot_id), Int64GetDatum(oldGen)};
        LakeMetaRows* advanced = LakeMetadataQuery(
            "UPDATE og_lake_internal.index_definition SET definition_generation=$2, registered_snapshot=$3 "
            "WHERE index_uuid=$1 AND state='ACTIVE' AND definition_generation=$4",
            4, genTypes, genArgs, NULL, snapshot, false, SPI_OK_UPDATE);
        LakeMetadataRequireCount(advanced, 1, "advanced lake index generation");

        CommandCounterIncrement();
        UnregisterSnapshot(snapshot);
        snapshot = RegisterSnapshot(GetLatestSnapshot());
        binding = LakeLoadCommittedInlineBinding(hostOid, snapshot);

        int files = metric ?
            LakeBuildAnnFileIndexes(binding, uuid, column, metric, newGen, defHash, snapshot, pqReq, pqChunks) :
            LakeBuildScalarFileIndexes(binding, uuid, column, newGen, defHash, snapshot, bitmap);
        if (!absent) {
            Relation index = metric ?
                LakeOpenInlineAnnIndex(DatumGetObjectId(existing), binding, column + 1, metric) :
                LakeOpenInlineIndex(DatumGetObjectId(existing), binding, column + 1);
            index_close(index, NoLock);
        } else if (OidIsValid(binding->inline_oid)) {
            bool fileOnlyAnn = false;
            if (metric) {
                Relation carrier = heap_open(binding->inline_oid, AccessShareLock);
                fileOnlyAnn = RelationIsUstoreFormat(carrier) && binding->file_count > 0 && files >= 0;
                heap_close(carrier, NoLock);
            }
            if (!fileOnlyAnn) {
                Oid indexOid = metric ?
                    LakeBuildInlineAnnIndex(binding, column, uuid, metric) :
                    LakeBuildNativeInlineIndex(binding, column, uuid);
                Oid types[3] = {UUIDOID, OIDOID, INT8OID};
                Datum args[3] = {uuidArg, ObjectIdGetDatum(indexOid), Int64GetDatum(binding->snapshot_id)};
                LakeMetaRows* changed = LakeMetadataQuery(metric ?
                    "UPDATE og_lake_internal.index_definition SET inline_index_oid=$2,registered_snapshot=$3 "
                    "WHERE index_uuid=$1 AND state='ACTIVE' AND inline_index_oid IS NULL" :
                    "UPDATE og_lake_internal.index_definition SET inline_index_oid=$2 "
                    "WHERE index_uuid=$1 AND state='ACTIVE' AND inline_index_oid IS NULL",
                    metric ? 3 : 2, types, args, NULL, snapshot, false, SPI_OK_UPDATE);
                LakeMetadataRequireCount(changed, 1, "rebuilt inline index binding");
            }
        }
        LakeIndexCursorReset(uuid);
        LakeInvalidateRelationSafe(hostOid);
        UnregisterSnapshot(snapshot);
    }
    PG_CATCH(); { UnregisterSnapshot(snapshot); PG_RE_THROW(); } PG_END_TRY();
    heap_close(host, NoLock);
    PG_RETURN_VOID();
}

/* Native build horizon checked against the retained business snapshot. RC
 * GetSnapshotData can advance the session TransactionXmin while this older
 * registered lake snapshot still needs pre-build physical/undo versions. */
bool LakeInlineIndexSnapshotSafe(Relation index,Snapshot snapshot)
{
    /* Retain the generic CSN rule for UB-tree too: the lake's retained host
     * snapshot can predate this RC statement. Native plancat's own-current-XID
     * RC shortcut therefore does not establish our older business horizon. */
    if(!index->rd_index->indcheckxmin)return true;
    TransactionId xmin=HeapTupleGetRawXmin(index->rd_indextuple);
    if(TransactionIdPrecedes(xmin,snapshot->xmin))return true;
    /* A NULL snapshot permits RecentXmin/CLOG FROZEN shortcuts and a cached
     * FROZEN CSN, which cannot authorize this older snapshot. Reuse the native
     * visibility entry, including its shared-storage transaction routing. */
    TransactionIdStatus status;
    return XidVisibleInSnapshot(xmin,snapshot,&status,InvalidBuffer,NULL);
}

bool LakeInlineIndexAvailable(const LakeInlineBinding* binding,const char* uuid,Oid indexOid,
    AttrNumber column,Snapshot businessSnapshot)
{
    if(!LakeHasInlineIndexVersion() || !OidIsValid(binding->inline_oid) ||
       binding->identity.kind!=LAKE_TABLE_PERMANENT || column<=0 || column>binding->column_count)return false;
    Oid types[2]={UUIDOID,UUIDOID};Datum args[2]={DirectFunctionCall1(uuid_in,CStringGetDatum(uuid)),
        DirectFunctionCall1(uuid_in,CStringGetDatum(binding->binding_id))};
    Snapshot snapshot=RegisterSnapshot(GetLatestSnapshot());bool usable=false;
    PG_TRY();
    {
        LakeMetaRows* row=LakeMetadataQuery("SELECT inline_index_oid,column_id,type_oid,table_uuid::text,"
            "COALESCE(definition->>'kind',''),COALESCE(definition->>'method','') FROM "
            "og_lake_internal.index_definition WHERE index_uuid=$1 AND binding_id=$2 AND state='ACTIVE'",
            2,types,args,NULL,snapshot,true,SPI_OK_SELECT);
        if(row->count>1)ereport(ERROR,(errcode(ERRCODE_DATA_CORRUPTED),errmsg("duplicate lake index definitions")));
        if(row->count==1) {
            bool absent=false;Datum stored=LakeMetadataValue(row,0,1,OIDOID,true,&absent);
            if(!absent && LakeInlineScalarMethod(LakeMetadataText(row,0,5),LakeMetadataText(row,0,6))&&
               DatumGetObjectId(stored)==indexOid &&
               DatumGetInt64(LakeMetadataValue(row,0,2,INT8OID))==binding->columns[column-1].column_id &&
               DatumGetObjectId(LakeMetadataValue(row,0,3,OIDOID))==binding->columns[column-1].type &&
               strcmp(LakeMetadataText(row,0,4),binding->table_uuid)==0 && get_rel_name(indexOid)!=NULL) {
                Relation index=LakeOpenInlineIndex(indexOid,binding,column);
                usable=LakeInlineIndexSnapshotSafe(index,businessSnapshot);index_close(index,NoLock);
            }
        }
        UnregisterSnapshot(snapshot);
    }
    PG_CATCH();{UnregisterSnapshot(snapshot);PG_RE_THROW();}PG_END_TRY();
    return usable;
}

void LakeAddInlineIndexPaths(PlannerInfo* root,RelOptInfo* relation,Oid relationOid,double startup,double total,const LakeCostLedger* ledger)
{
    if(!LakeHasInlineIndexVersion() || root->parse->commandType!=CMD_SELECT ||
        !u_sess->attr.attr_sql.enable_indexscan || relation->baserestrictinfo==NIL)return;
    Snapshot snapshot=ActiveSnapshotSet()?GetActiveSnapshot():GetTransactionSnapshot();
    LakeInlineBinding* binding=LakeLoadInlineBinding(relationOid,snapshot);
    if((!LakeIxSmpExtendedVersionEnabled()&&(LakeBindingHasBinary(binding)||LakeBindingHasUuid(binding)||LakeBindingHasJson(binding)||LakeBindingHasNumeric(binding)))||binding->has_dropped_columns||binding->identity.kind!=LAKE_TABLE_PERMANENT||(!LakePartitionComboVersionEnabled()&&LakeIdentityPartitioned(binding)))return;
    double seqCost=0,randomCost=0;get_tablespace_page_costs(InvalidOid,&randomCost,&seqCost);
    double pages=0,inlineVersions=0;bool nativeCarrier=false;
    if(OidIsValid(binding->inline_oid)){
        Relation carrier=heap_open(binding->inline_oid,AccessShareLock);nativeCarrier=true;
        get_tablespace_page_costs(carrier->rd_rel->reltablespace,&randomCost,&seqCost);
        pages=(double)LakeInlinePhysicalPages(binding);
        /* This is the carrier's own stored-version work, never the lake's
         * visible reltuples. Ended lake versions are still ordinary carrier
         * tuples. With stale/missing carrier statistics, data+TOAST pages and
         * native descriptor width give a coarse, deliberately conservative
         * estimate. No subtraction of live lake rows/file rows is valid. */
        double width=Max(1,get_relation_data_width(binding->inline_oid,InvalidOid,NULL));
        inlineVersions=pages*BLCKSZ/(width+sizeof(HeapTupleHeaderData));
        if(carrier->rd_rel->relpages>0&&carrier->rd_rel->reltuples>=0)
            inlineVersions=Max(inlineVersions,(double)carrier->rd_rel->reltuples);
        heap_close(carrier,NoLock);
    }
    Oid type=UUIDOID;Datum arg=DirectFunctionCall1(uuid_in,CStringGetDatum(binding->binding_id));
    LakeMetaRows* definitions=LakeMetadataQuery("SELECT index_uuid::text,column_id,type_oid,inline_index_oid,"
        "COALESCE(definition->>'kind',''),COALESCE(definition->>'method','') FROM "
        "og_lake_internal.index_definition WHERE binding_id=$1 AND state='ACTIVE' "
        "ORDER BY index_uuid LIMIT 65",1,&type,&arg,NULL,snapshot,true,SPI_OK_SELECT);
    if(definitions->count>64)return; /* Bounded planning; full scan remains complete. */
    for(uint32 d=0;d<definitions->count;++d) {
        int64 id=DatumGetInt64(LakeMetadataValue(definitions,d,2,INT8OID));
        Oid keyType=DatumGetObjectId(LakeMetadataValue(definitions,d,3,OIDOID));int column=-1;
        for(int i=0;i<binding->column_count;++i)
            if(binding->columns[i].column_id==id && binding->columns[i].type==keyType)column=i;
        if(column<0 || !LakeScalarIndexedType(keyType)||
            !LakeInlineScalarMethod(LakeMetadataText(definitions,d,5),LakeMetadataText(definitions,d,6)))continue;
        bool bitmap=strcmp(LakeMetadataText(definitions,d,6),"bitmap")==0;
        const char* uuid=LakeMetadataText(definitions,d,1);
        bool absent=false;Datum stored=LakeMetadataValue(definitions,d,4,OIDOID,true,&absent);
        Oid indexOid=absent?InvalidOid:DatumGetObjectId(stored);
        bool inlineUsable=nativeCarrier&&OidIsValid(indexOid)&&LakeInlineIndexAvailable(binding,uuid,indexOid,column+1,snapshot);
        double coveredRows,coveredBytes,coveredGroups;int coveredFiles;
        LakeCostLedger* costLedger=ledger?LakeCostCopy(ledger):NULL;
        LakeScalarFileCoverageEstimate(binding,uuid,column+1,snapshot,&coveredRows,&coveredBytes,&coveredGroups,&coveredFiles,costLedger);
        if(!inlineUsable&&coveredFiles==0)continue;
        if(!inlineUsable)indexOid=InvalidOid;
        ListCell* cell=NULL;
        foreach(cell,relation->baserestrictinfo) {
            RestrictInfo* restriction=(RestrictInfo*)lfirst(cell);
            Node* left=NULL;Node* right=NULL;Oid op=InvalidOid;bool arrayKey=false;int nullMode=0;
            if(IsA(restriction->clause,OpExpr)) {
                OpExpr* expression=(OpExpr*)restriction->clause;
                if(expression->opretset || expression->opresulttype!=BOOLOID || list_length(expression->args)!=2)continue;
                left=(Node*)linitial(expression->args);right=(Node*)lsecond(expression->args);op=expression->opno;
                if(!IsA(left,Var) && IsA(right,Var)){Node* swap=left;left=right;right=swap;op=get_commutator(op);}
            } else if(IsA(restriction->clause,ScalarArrayOpExpr)) {
                ScalarArrayOpExpr* expression=(ScalarArrayOpExpr*)restriction->clause;
                if(!expression->useOr || list_length(expression->args)!=2)continue;
                left=(Node*)linitial(expression->args);right=(Node*)lsecond(expression->args);
                op=expression->opno;arrayKey=true;
            } else if(IsA(restriction->clause,NullTest)) {
                NullTest* expression=(NullTest*)restriction->clause;
                if(expression->argisrow || (expression->nulltesttype!=IS_NULL && expression->nulltesttype!=IS_NOT_NULL))continue;
                left=(Node*)expression->arg;right=(Node*)makeNullConst(keyType,-1,InvalidOid);
                nullMode=expression->nulltesttype==IS_NULL?1:2;
            } else continue;
            if(!IsA(left,Var) || (!IsA(right,Const) && !IsA(right,Param)))continue;
            Var* variable=(Var*)left;
            if(variable->varno!=relation->relid || variable->varlevelsup!=0 || variable->varattno!=column+1 ||
               variable->vartype!=keyType)continue;
            if(keyType==INT2OID&&arrayKey)continue;
            if(nullMode==0) {
                if(!OidIsValid(op))continue;
                Oid argumentType=arrayKey?get_element_type(exprType(right)):exprType(right);
                if(!LakeScalarArgumentType(keyType,argumentType))continue;
                int strategy=get_op_opfamily_strategy(op,LakeScalarFamily(keyType));
                if(strategy<1 || strategy>5 || (arrayKey && strategy!=BTEqualStrategyNumber) ||
                   get_opcode(op)>=FirstNormalObjectId)continue;
                Oid leftType,rightType;
                get_op_opfamily_properties(op,LakeScalarFamily(keyType),false,&strategy,&leftType,&rightType);
                if(leftType!=keyType || rightType!=argumentType)continue;
            }
            double indexPages=0;
            if(inlineUsable){
                Relation index=LakeOpenInlineIndex(indexOid,binding,column+1);
                if((arrayKey && !index->rd_am->amsearcharray) || (nullMode && !index->rd_am->amsearchnulls)) {
                    index_close(index,NoLock);continue;
                }
                indexPages=Max(1,index->rd_rel->relpages);index_close(index,NoLock);
            }
            double selectivity=clause_selectivity(root,(Node*)restriction,0,JOIN_INNER,NULL);
            selectivity=Max(0.0,Min(1.0,selectivity));
            /* Replace only inline sequential I/O. File work, original quals,
             * visibility and private after-image work remain in the baseline. */
            double probes=arrayKey?Max(1,estimate_array_length(right)):1;
            double indexIo=inlineUsable?randomCost*(probes*ceil(log(indexPages+1)/log(2.0))+selectivity*indexPages+
                Min(pages,Max(1.0,relation->tuples*selectivity))):pages*seqCost;
            double fileSaving=0;
            if(coveredFiles){
                /* Conservative random row-group fetch upper estimate. The
                 * catalog can affect cost, never execution coverage proof. */
                double matched=coveredRows*selectivity;
                double fetchFraction=Min(1.0,matched/Max(1.0,coveredGroups));
                double indexCost;
                if(bitmap){
                    /* Prototype v1 validates/loads the entire bounded value
                     * dictionary. Do not cost it as a sorted-page lookup.
                     * Each posting needs at most two portable bytes plus
                     * bounded per-key/container/Parquet metadata; this loose
                     * full-read bound also charges full coverage validation.
                     * Catalog estimates never authorize dropping a source. */
                    double dictionaryRows=Min(coveredRows,coveredFiles*65536.0);
                    double portableBytes=coveredRows*2.0+dictionaryRows*256.0+coveredFiles*4096.0;
                    indexCost=randomCost*coveredFiles+seqCost*ceil(portableBytes/BLCKSZ)+
                        coveredRows*(u_sess->attr.attr_sql.cpu_tuple_cost+3*u_sess->attr.attr_sql.cpu_operator_cost);
                }else{
                    double lookupPages=coveredFiles+ceil(ceil(coveredRows/4096.0)*128.0/BLCKSZ);
                    double leafRows=Min(coveredRows,Max(1.0,matched)+2*4096.0*probes);
                    indexCost=randomCost*(lookupPages+ceil(leafRows*24.0/BLCKSZ));
                }
                fileSaving=seqCost*ceil(coveredBytes/BLCKSZ)*(1-fetchFraction)+
                    coveredRows*(1-selectivity)*(u_sess->attr.attr_sql.cpu_tuple_cost+3*u_sess->attr.attr_sql.cpu_operator_cost)-indexCost;
            }
            double cost=Max(startup,total-pages*seqCost+indexIo-fileSaving);
            double pathStartup=startup;
            LakeCostLedger* clauseLedger=costLedger?LakeCostCopy(costLedger):NULL;
            if(clauseLedger){
                /* Each clause gets its own image: one source cannot be charged
                 * as both a full data scan and an indexed candidate scan. */
                LakeCostScalar(clauseLedger,bitmap,selectivity,probes);
                LakeCostResult priced=LakeCostFinish(clauseLedger,indexIo);
                cost=priced.total;pathStartup=priced.startup;
            }
            List* data=list_make3(makeString(pstrdup(uuid)),makeInteger((int)indexOid),makeInteger(column+1));
            data=lappend(data,makeInteger((int)op));data=lappend(data,makeInteger((int)keyType));
            data=lappend(data,makeInteger(nullMode));data=lappend(data,copyObject(right));
            add_path(root,relation,(Path*)create_foreignscan_path(root,relation,pathStartup,cost,NIL,NULL,NULL,data,1));
            int dop=LakeStreamPlanDop(root,relation,binding);
            if(dop>1&&coveredFiles>0){
                /* Same bounded one-base-table admission as the full SMP path.
                 * Inline is deliberately a complete native task in this slice.
                 * Costs remain host units and uncalibrated; source assignment
                 * is static and the largest input owner limits parallel work. */
                double baseline=Max(startup,total-fileSaving),run=(baseline-startup)/dop;
                int offset=OidIsValid(binding->inline_oid)?1:0;
                double work[4]={0,0,0,0},fileRows=0;
                for(int f=0;f<binding->file_count;++f)fileRows+=binding->files[f].record_count;
                double physical=Max(1.0,fileRows+inlineVersions);
                for(int f=0;f<binding->file_count;++f)
                    work[(f+offset)%dop]+=(baseline-startup)*binding->files[f].record_count/physical;
                if(offset)work[0]+=(baseline-startup)*inlineVersions/physical;
                for(int i=0;i<dop;++i)run=Max(run,work[i]);
                List* parallel=(List*)copyObject(data);
                lsecond(parallel)=makeInteger((int)InvalidOid);
                double parallelStartup=startup,parallelTotal=startup+run+u_sess->opt_cxt.smp_thread_cost*(dop-1);
                if(clauseLedger){
                    /* Inline remains a FULL task on owner0 even when the
                     * serial alternative has a native inline index. */
                    LakeCostResult priced=LakeCostFinish(clauseLedger,-1,dop,u_sess->opt_cxt.smp_thread_cost);
                    parallelStartup=priced.startup;parallelTotal=priced.total;
                }
                add_path(root,relation,(Path*)create_foreignscan_path(root,relation,parallelStartup,
                    parallelTotal,NIL,NULL,NULL,parallel,dop));
            }
        }
    }
}
