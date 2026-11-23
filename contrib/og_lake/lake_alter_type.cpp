/* Restricted widening ALTER FOREIGN TABLE ... ALTER COLUMN ... TYPE,
 * reusing the reviewed native definition claim/child ownership protocol.
 * Supports DuckLake 1.1 lossless widening promotions:
 *   int2 -> int4 -> int8, float4 -> float8, numeric(p1,s1) -> numeric(p2,s2)
 *   with s2 >= s1 and (p2 - s2) >= (p1 - s1).
 * Old inline carriers and Parquet files retain their birth schema and are
 * losslessly widened on read; new writes target a new schema version carrier.
 */
#include "postgres.h"
#include "lake_version.h"
#include "lake_column_layout.h"
#include "lake_column_default.h"
#include "lake_create_publish.h"
#include "lake_types.h"
#include "access/heapam.h"
#include "access/og_lake.h"
#include "catalog/heap.h"
#include "catalog/pg_inherits_fn.h"
#include "commands/extension.h"
#include "executor/spi.h"
#include "knl/knl_variable.h"
#include "lake_txn_private.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "optimizer/clauses.h"
#include "parser/parse_type.h"
#include "rewrite/rewriteHandler.h"
#include "storage/lmgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"

extern void LakeTxnForgetReadOnlyTableForRename(Oid relationOid);

struct LakeAlterTypePlan {
    MemoryContext context;
    LakeTxnState* state;
    LakeInlineBinding* original;
    int column;
    AttrNumber attnum;
    Oid old_type;
    int32 old_typmod;
    Oid target_type;
    int32 target_typmod;
    Oid collation;
    bool has_default;
    Const* widened_literal;
    char* new_default_canonical;
    bool noop;
};

static void LakeCheckAlterTypeDependencies(LakeInlineBinding* binding, int column, Relation host, Snapshot snapshot)
{
    int64 id = binding->columns[column].column_id;
    Oid types[3] = {INT8OID, INT8OID, UUIDOID};
    Datum args[3] = {
        Int64GetDatum(LakePermanentTableId(binding)),
        Int64GetDatum(id),
        DirectFunctionCall1(uuid_in, CStringGetDatum(binding->table_uuid))
    };
    LakeMetaRows* rows = LakeMetadataQuery(psprintf(
        "SELECT EXISTS(SELECT 1 FROM %s p JOIN %s c USING(partition_id) "
        "WHERE p.table_id=$1 AND c.table_id=$1 AND c.column_id=$2) "
        "OR EXISTS(SELECT 1 FROM %s WHERE table_id=$1)",
        LakeMetadataRelation(binding->metadata_schema_oid, "ducklake_partition_info"),
        LakeMetadataRelation(binding->metadata_schema_oid, "ducklake_partition_column"),
        LakeMetadataRelation(binding->metadata_schema_oid, "ducklake_sort_info")),
        2, types, args, NULL, snapshot, true, SPI_OK_SELECT, binding->metadata_schema_oid);
    LakeMetadataRequireCount(rows, 1, "alter type partition/sort dependency check");
    if (DatumGetBool(LakeMetadataValue(rows, 0, 1, BOOLOID)))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake ALTER COLUMN TYPE does not support columns with partition or sort history")));

    rows = LakeMetadataQuery(
        "SELECT EXISTS(SELECT 1 FROM og_lake_internal.index_definition "
        "WHERE table_uuid=$3 AND column_id=$2 AND state='ACTIVE')",
        3, types, args, NULL, snapshot, true, SPI_OK_SELECT);
    LakeMetadataRequireCount(rows, 1, "alter type active index check");
    if (DatumGetBool(LakeMetadataValue(rows, 0, 1, BOOLOID)))
        ereport(ERROR, (errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
            errmsg("active lake index depends on column \"%s\"", binding->columns[column].name)));

    List* indexes = RelationGetIndexList(host);
    ListCell* cell = NULL;
    foreach(cell, indexes) {
        Relation index = index_open(lfirst_oid(cell), AccessShareLock);
        bool used = false;
        for (int n = 0; n < index->rd_index->indnatts; ++n) {
            if (index->rd_index->indkey.values[n] == binding->columns[column].output_attnum)
                used = true;
        }
        used |= RelationGetIndexExpressions(index) != NIL || RelationGetIndexPredicate(index) != NIL;
        index_close(index, NoLock);
        if (used)
            ereport(ERROR, (errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                errmsg("host index depends on column \"%s\"", binding->columns[column].name)));
    }
    list_free(indexes);
}

void* LakeNativePrepareAlterColumnType(Relation relation, List* commands)
{
    if (!LakeIsNativeForeignRelation(relation) || !LakeAlterTypeOpsVersionEnabled())
        return NULL;
    LakeRejectNativePartitionDDL(relation, "ALTER COLUMN TYPE");
    if (list_length(commands) != 1)
        return NULL;
    AlterTableCmd* command = (AlterTableCmd*)linitial(commands);
    if (command->subtype != AT_AlterColumnType || !command->def || !IsA(command->def, ColumnDef))
        return NULL;
    ColumnDef* def = (ColumnDef*)command->def;
    if (!command->name || !*command->name || command->recursing ||
        def->raw_default != NULL || def->constraints != NIL || def->generatedCol ||
        def->update_default || def->fdwoptions != NIL || def->columnOptions != NIL ||
        def->collClause != NULL || OidIsValid(def->collOid) ||
        def->typname == NULL || def->typname->arrayBounds != NIL)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake ALTER COLUMN TYPE requires a plain widening scalar type change without USING or collation change")));

    if (!pg_class_ownercheck(RelationGetRelid(relation), GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CLASS, RelationGetRelationName(relation));
    if (u_sess->utils_cxt.XactIsoLevel != XACT_READ_COMMITTED)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake ALTER COLUMN TYPE requires READ COMMITTED")));
    if (u_sess->attr.attr_common.XactReadOnly)
        ereport(ERROR, (errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
            errmsg("cannot alter lake column type in a read-only transaction")));
    if (creating_extension || RelationIsPartitioned(relation) ||
        relation->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT ||
        find_inheritance_children(RelationGetRelid(relation), NoLock) != NIL ||
        LakeTxnFindCreatedTable(RelationGetRelid(relation)) != NULL ||
        !LakeNativeProtectedCreate(RelationGetRelid(relation)))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake ALTER COLUMN TYPE requires a committed ordinary protected CREATE table")));

    AttrNumber attnum = get_attnum(RelationGetRelid(relation), command->name);
    if (attnum <= 0)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
            errmsg("column \"%s\" of relation \"%s\" does not exist",
                command->name, RelationGetRelationName(relation))));
    Form_pg_attribute attr = &RelationGetDescr(relation)->attrs[attnum - 1];
    if (attr->attisdropped || attr->attinhcount ||
        !LakeNativeFlatTypmod(attr->atttypid, attr->atttypmod))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake ALTER COLUMN TYPE requires an ordinary flat scalar column")));

    Oid targetType = InvalidOid;
    int32 targetTypmod = -1;
    typenameTypeIdAndMod(NULL, def->typname, &targetType, &targetTypmod);
    const LakeNativeType* nt = LakeNativeTypeByOid(targetType);
    if (nt->fixed_width == -2 || !nt->writable || !LakeNativeFlatTypmod(targetType, targetTypmod))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake ALTER COLUMN TYPE target type or typmod is unsupported")));
    LakeNativeTypeRequireBusinessVersion(targetType);
    LakeNativeTypeRequireSdk(targetType, LakeFileProvider());

    if (targetType == attr->atttypid && targetTypmod == attr->atttypmod) {
        LakeAlterTypePlan* noop = (LakeAlterTypePlan*)palloc0(sizeof(LakeAlterTypePlan));
        noop->noop = true;
        return noop;
    }
    if (!LakeIsWideningPromotion(attr->atttypid, attr->atttypmod, targetType, targetTypmod))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake ALTER COLUMN TYPE only supports safe widening promotions")));

    LakeTryLockCatalogRegistration();
    if (!OidIsValid(LakeMetadataOwner()))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake ALTER COLUMN TYPE requires a private metadata owner")));
    LakeTxnState* state = LakeTxnBeginColumnDefinition();
    LakeTxnForgetReadOnlyTableForRename(RelationGetRelid(relation));

    MemoryContext caller = CurrentMemoryContext;
    MemoryContext context = AllocSetContextCreate(caller, "Native lake ALTER COLUMN TYPE", ALLOCSET_DEFAULT_SIZES);
    Snapshot volatile snapshot = NULL;
    LakeAlterTypePlan* result = NULL;
    PG_TRY();
    {
        MemoryContextSwitchTo(context);
        snapshot = RegisterSnapshot(GetLatestSnapshot());
        result = (LakeAlterTypePlan*)palloc0(sizeof(LakeAlterTypePlan));
        result->context = context;
        result->state = state;
        result->attnum = attnum;
        result->old_type = attr->atttypid;
        result->old_typmod = attr->atttypmod;
        result->target_type = targetType;
        result->target_typmod = targetTypmod;
        result->collation = attr->attcollation;
        result->original = LakeLoadCommittedInlineBinding(RelationGetRelid(relation), snapshot);
        if (result->original->read_only || !result->original->alter_type_evolution)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("native lake ALTER COLUMN TYPE target is not writable or lacks SQL0.70 capability")));
        result->column = LakeHostToLogical(result->original, attnum);
        if (result->column < 0 || result->column >= result->original->column_count)
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("native lake ALTER COLUMN TYPE column mapping missing")));
        LakeCheckAlterTypeDependencies(result->original, result->column, relation, snapshot);

        if (attr->atthasdef) {
            Node* rawDef = strip_implicit_coercions(
                eval_const_expressions(NULL, build_column_default(relation, attnum)));
            if (rawDef == NULL || !IsA(rawDef, Const))
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("native lake ALTER COLUMN TYPE supports only literal Const defaults")));
            Const* c = (Const*)rawDef;
            if (c->consttype != result->old_type || c->consttypmod != result->old_typmod)
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("native lake ALTER COLUMN TYPE default constant disagrees with column type")));
            if (c->constisnull) {
                result->has_default = true;
                result->widened_literal = makeNullConst(targetType, targetTypmod, result->collation);
                result->new_default_canonical = NULL;
            } else {
                if (!LakeLiteralDefaultType(targetType, targetTypmod))
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("native lake ALTER COLUMN TYPE target default type is unsupported")));
                Datum widened = LakeWidenDatum(c->constvalue, result->old_type, result->old_typmod,
                    targetType, targetTypmod);
                int16 typlen = 0;
                bool typbyval = true;
                get_typlenbyval(targetType, &typlen, &typbyval);
                widened = datumCopy(widened, typbyval, typlen);
                result->has_default = true;
                result->widened_literal = makeConst(targetType, targetTypmod, result->collation,
                    typlen, widened, false, typbyval);
                result->new_default_canonical = LakeEncodeLiteralDefault(targetType, targetTypmod, widened);
            }
        }
        LakeTxnForgetReadOnlyTableForRename(RelationGetRelid(relation));
        UnregisterSnapshot(snapshot);
        snapshot = NULL;
        MemoryContextSwitchTo(caller);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(caller);
        if (snapshot)
            UnregisterSnapshot(snapshot);
        MemoryContextDelete(context);
        PG_RE_THROW();
    }
    PG_END_TRY();
    return result;
}

static void LakeWriteAlteredColumnType(LakeAlterTypePlan* plan, int64 head, int64 commit, int64 schema, Snapshot snapshot)
{
    const LakeInlineBinding* old = plan->original;
    Oid ns = old->metadata_schema_oid;
    const LakeInlineColumn* col = &old->columns[plan->column];
    const char* columnsRel = LakeDefinitionRelation(ns, "ducklake_column");

    Oid endTypes[4] = {INT8OID, INT8OID, INT8OID, INT8OID};
    Datum endArgs[4] = {
        Int64GetDatum(LakePermanentTableId(old)),
        Int64GetDatum(col->column_id),
        Int64GetDatum(head),
        Int64GetDatum(commit)
    };
    LakeMetaRows* wrote = LakeMetadataQuery(psprintf(
        "UPDATE %s SET end_snapshot=$4 WHERE table_id=$1 AND column_id=$2 "
        "AND parent_column IS NULL AND begin_snapshot<=$3 AND (end_snapshot IS NULL OR end_snapshot>$3)",
        columnsRel), 4, endTypes, endArgs, NULL, snapshot, false, SPI_OK_UPDATE, ns);
    LakeMetadataRequireCount(wrote, 1, "ended old column type definition");

    const char* duckType = LakeNativeDuckType(plan->target_type, plan->target_typmod);
    if (plan->has_default && plan->new_default_canonical != NULL) {
        bool escaped = false;
        const char* cur = LakeCurrentDefaultMetadata(plan->target_type, plan->new_default_canonical, &escaped);
        Oid insTypes[7] = {INT8OID, INT8OID, INT8OID, INT8OID, TEXTOID, TEXTOID, TEXTOID};
        Datum insArgs[7] = {
            endArgs[0], endArgs[1], endArgs[2], endArgs[3],
            CStringGetTextDatum(duckType),
            CStringGetTextDatum(cur),
            CStringGetTextDatum(escaped ? "expression" : "literal")
        };
        wrote = LakeMetadataQuery(psprintf(
            "INSERT INTO %s(column_id,begin_snapshot,end_snapshot,table_id,column_order,"
            "column_name,column_type,initial_default,default_value,nulls_allowed,parent_column,"
            "default_value_type,default_value_dialect) "
            "SELECT column_id,$4,NULL,table_id,column_order,column_name,$5,initial_default,"
            "$6,nulls_allowed,parent_column,$7,'duckdb' "
            "FROM %s WHERE table_id=$1 AND column_id=$2 AND parent_column IS NULL "
            "AND begin_snapshot<=$3 AND end_snapshot=$4",
            columnsRel, columnsRel),
            7, insTypes, insArgs, NULL, snapshot, false, SPI_OK_INSERT, ns);
    } else {
        Oid insTypes[5] = {INT8OID, INT8OID, INT8OID, INT8OID, TEXTOID};
        Datum insArgs[5] = {
            endArgs[0], endArgs[1], endArgs[2], endArgs[3],
            CStringGetTextDatum(duckType)
        };
        wrote = LakeMetadataQuery(psprintf(
            "INSERT INTO %s(column_id,begin_snapshot,end_snapshot,table_id,column_order,"
            "column_name,column_type,initial_default,default_value,nulls_allowed,parent_column,"
            "default_value_type,default_value_dialect) "
            "SELECT column_id,$4,NULL,table_id,column_order,column_name,$5,initial_default,"
            "default_value,nulls_allowed,parent_column,default_value_type,default_value_dialect "
            "FROM %s WHERE table_id=$1 AND column_id=$2 AND parent_column IS NULL "
            "AND begin_snapshot<=$3 AND end_snapshot=$4",
            columnsRel, columnsRel),
            5, insTypes, insArgs, NULL, snapshot, false, SPI_OK_INSERT, ns);
    }
    LakeMetadataRequireCount(wrote, 1, "inserted widened column type definition");

    Oid vt[3] = {INT8OID, INT8OID, INT8OID};
    Datum va[3] = {endArgs[3], Int64GetDatum(schema), endArgs[0]};
    wrote = LakeMetadataQuery(psprintf(
        "INSERT INTO %s(begin_snapshot,schema_version,table_id) VALUES($1,$2,$3)",
        LakeDefinitionRelation(ns, "ducklake_schema_versions")),
        3, vt, va, NULL, snapshot, false, SPI_OK_INSERT, ns);
    LakeMetadataRequireCount(wrote, 1, "alter type schema version");

    Datum bindingUuid = DirectFunctionCall1(uuid_in, CStringGetDatum(old->binding_id));
    Oid mt[5] = {OIDOID, INT4OID, OIDOID, UUIDOID, INT8OID};
    Datum ma[5] = {
        ObjectIdGetDatum(plan->target_type),
        Int32GetDatum(plan->target_typmod),
        ObjectIdGetDatum(plan->collation),
        bindingUuid,
        Int64GetDatum(col->column_id)
    };
    wrote = LakeMetadataQuery(
        "UPDATE og_lake_internal.column_binding SET type_oid=$1, typmod=$2, collation_oid=$3 "
        "WHERE binding_id=$4 AND column_id=$5",
        5, mt, ma, NULL, snapshot, false, SPI_OK_UPDATE);
    LakeMetadataRequireCount(wrote, 1, "alter type column binding update");

    Oid ht[5] = {INT8OID, UUIDOID, INT8OID, UUIDOID, OIDOID};
    Datum ha[5] = {
        va[1], bindingUuid, endArgs[0],
        DirectFunctionCall1(uuid_in, CStringGetDatum(old->table_uuid)),
        ObjectIdGetDatum(old->server_oid)
    };
    wrote = LakeMetadataQuery(
        "UPDATE og_lake_internal.table_binding SET schema_version=$1 WHERE binding_id=$2 "
        "AND table_id=$3 AND table_uuid=$4 AND server_oid=$5 AND state='active' AND NOT read_only",
        5, ht, ha, NULL, snapshot, false, SPI_OK_UPDATE);
    LakeMetadataRequireCount(wrote, 1, "alter type table binding schema version");

    Oid ct[2] = {INT8OID, TEXTOID};
    Datum ca[2] = {
        endArgs[3],
        CStringGetTextDatum(psprintf("altered_table:" INT64_FORMAT, LakePermanentTableId(old)))
    };
    wrote = LakeMetadataQuery(psprintf(
        "INSERT INTO %s(snapshot_id,changes_made,author,commit_message,commit_extra_info) "
        "VALUES($1,$2,NULL,NULL,NULL)", LakeMetadataRelation(ns, "ducklake_snapshot_changes")),
        2, ct, ca, NULL, snapshot, false, SPI_OK_INSERT, ns);
    LakeMetadataRequireCount(wrote, 1, "alter type changeset");
}

void LakeNativeFinishAlterColumnType(void* opaque)
{
    LakeAlterTypePlan* plan = (LakeAlterTypePlan*)opaque;
    if (plan->noop) {
        pfree(plan);
        return;
    }
    MemoryContext caller = CurrentMemoryContext;
    ResourceOwner owner = t_thrd.utils_cxt.CurrentResourceOwner;
    void* hostSlot = LakeHostPrepareImmediatePublish(plan->original, GetCurrentSubTransactionId());
    int64 winningSnapshot = -1;
    bool complete = false;
    volatile bool inside = false;
    PG_TRY();
    {
        MemoryContextSwitchTo(plan->context);
        CommandCounterIncrement();
        Relation host = heap_open(plan->original->relation_oid, NoLock);
        TupleDesc desc = RelationGetDescr(host);
        if (desc->natts != LakeHostColumnCount(plan->original))
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("native ALTER COLUMN TYPE host attribute count changed")));
        for (int c = 0; c < plan->original->column_count; ++c) {
            Form_pg_attribute attr = &desc->attrs[LakeLogicalToHost(plan->original, c)];
            const LakeInlineColumn* field = &plan->original->columns[c];
            if (c == plan->column) {
                if (attr->attisdropped || attr->atttypid != plan->target_type ||
                    attr->atttypmod != plan->target_typmod || attr->attcollation != plan->collation ||
                    strcmp(NameStr(attr->attname), field->name) != 0 || attr->attnotnull != field->not_null)
                    ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("host ALTER COLUMN TYPE did not apply expected target type")));
            } else if (attr->attisdropped || attr->atttypid != field->type ||
                       attr->atttypmod != field->typmod ||
                       strcmp(NameStr(attr->attname), field->name) != 0 ||
                       attr->attnotnull != field->not_null) {
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("host ALTER COLUMN TYPE altered an unrelated column")));
            }
        }
        if (plan->has_default) {
            RemoveAttrDefault(RelationGetRelid(host), plan->attnum, DROP_RESTRICT, true, true);
            CommandCounterIncrement();
            StoreAttrDefault(host, plan->attnum, (Node*)plan->widened_literal, 0, NULL);
        }
        heap_close(host, NoLock);
        if (plan->has_default) {
            CacheInvalidateRelcacheByRelid(plan->original->relation_oid);
            CommandCounterIncrement();
        }

        for (int attempt = 0; attempt < 16 && !complete; ++attempt) {
            CHECK_FOR_INTERRUPTS();
            MemoryContext scratch = AllocSetContextCreate(plan->context, "Native ALTER TYPE claim", ALLOCSET_DEFAULT_SIZES);
            plan->state->publication_subtransaction = true;
            BeginInternalSubTransaction(NULL);
            inside = true;
            MemoryContextSwitchTo(scratch);
            Oid ns = plan->original->metadata_schema_oid;
            (void)LakeMetadataRelation(ns, "ducklake_metadata");
            Relation options = heap_open(get_relname_relid("ducklake_metadata", ns), ShareLock);
            heap_close(options, NoLock);
            Snapshot fresh = GetLatestSnapshot();
            LakeInlineBinding* current = LakeReloadAddColumnDefinition(plan->original, fresh);
            if (!current || current->column_count <= plan->column ||
                current->columns[plan->column].column_id != plan->original->columns[plan->column].column_id ||
                current->columns[plan->column].type != plan->old_type ||
                current->columns[plan->column].typmod != plan->old_typmod)
                ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                    errmsg("native lake ALTER COLUMN TYPE concurrent DDL conflict")));

            const char* snapshots = LakeMetadataRelation(ns, "ducklake_snapshot");
            LakeMetaRows* rows = LakeMetadataQuery(psprintf(
                "SELECT snapshot_id,schema_version,next_catalog_id,next_file_id "
                "FROM %s ORDER BY snapshot_id DESC LIMIT 1", snapshots),
                0, NULL, NULL, NULL, fresh, true, SPI_OK_SELECT, ns);
            LakeMetadataRequireCount(rows, 1, "alter type snapshot head");
            int64 head = DatumGetInt64(LakeMetadataValue(rows, 0, 1, INT8OID));
            int64 schema = DatumGetInt64(LakeMetadataValue(rows, 0, 2, INT8OID));
            int64 catalog = DatumGetInt64(LakeMetadataValue(rows, 0, 3, INT8OID));
            int64 file = DatumGetInt64(LakeMetadataValue(rows, 0, 4, INT8OID));
            if (head < 0 || head == PG_INT64_MAX || schema < 0 || schema == PG_INT64_MAX ||
                catalog < 0 || catalog >= LAKE_REFERENCE_LOCAL_ID_START || file < 0)
                ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg("native alter type snapshot counters exceed their bounds")));
            Oid types[4] = {INT8OID, INT8OID, INT8OID, INT8OID};
            Datum args[4] = {Int64GetDatum(head + 1), Int64GetDatum(schema + 1),
                Int64GetDatum(catalog), Int64GetDatum(file)};
            LakeMetaRows* claim = LakeMetadataQuery(psprintf(
                "INSERT INTO %s(snapshot_id,snapshot_time,schema_version,next_catalog_id,next_file_id) "
                "VALUES($1,pg_catalog.now(),$2,$3,$4) ON CONFLICT(snapshot_id) DO NOTHING RETURNING snapshot_id",
                snapshots), 4, types, args, NULL, fresh, false, SPI_OK_INSERT_RETURNING, ns);
            if (claim->count > 1)
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("duplicate alter type snapshot claim")));
            if (claim->count == 1) {
                Snapshot own = LakeCreatePushOwnWriteSnapshot(GetLatestSnapshot());
                LakeWriteAlteredColumnType(plan, head, head + 1, schema + 1, own);
                PopActiveSnapshot();
                ReleaseCurrentSubTransaction();
                inside = false;
                plan->state->publication_subtransaction = false;
                complete = true;
                winningSnapshot = head + 1;
            } else {
                RollbackAndReleaseCurrentSubTransaction();
                inside = false;
                plan->state->publication_subtransaction = false;
            }
            MemoryContextSwitchTo(plan->context);
            t_thrd.utils_cxt.CurrentResourceOwner = owner;
            MemoryContextDelete(scratch);
            if (!complete)
                pg_usleep(1000L * (attempt + 1));
        }
        if (!complete)
            ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                errmsg("native alter column type snapshot claim exceeded retry limit")));
        CacheInvalidateRelcacheByRelid(plan->original->relation_oid);
        CommandCounterIncrement();
        (void)LakeLoadCommittedInlineBinding(plan->original->relation_oid, GetLatestSnapshot());
        if (winningSnapshot >= 0)
            LakeHostActivateImmediatePublish(hostSlot, winningSnapshot);
        MemoryContextSwitchTo(caller);
        MemoryContextDelete(plan->context);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(caller);
        ErrorData* error = CopyErrorData();
        FlushErrorState();
        if (inside)
            RollbackAndReleaseCurrentSubTransaction();
        plan->state->publication_subtransaction = false;
        t_thrd.utils_cxt.CurrentResourceOwner = owner;
        MemoryContextDelete(plan->context);
        ReThrowError(error);
    }
    PG_END_TRY();
}
