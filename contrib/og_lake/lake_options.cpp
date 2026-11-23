/* DuckLake 1.1 persistent options. Values remain in the host SQL transaction. */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "lake_txn_private.h"
#include "lake_index_private.h"
#include "lake_types.h"
#include "lake_version.h"
#include "access/og_lake.h"
#include "access/heapam.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

extern "C" Datum og_lake_set_table_option(PG_FUNCTION_ARGS);
extern "C" Datum og_lake_set_catalog_option(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(og_lake_set_table_option);
PG_FUNCTION_INFO_V1(og_lake_set_catalog_option);

static void LakeParsePersistentOption(FunctionCallInfo fcinfo, const char** canonicalKeyOut, char** canonicalValueOut,
    bool* isInlineLimitOut)
{
    if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
            errmsg("native lake option target, key and value cannot be NULL")));
    const char* key = text_to_cstring(PG_GETARG_TEXT_PP(1));
    const char* value = text_to_cstring(PG_GETARG_TEXT_PP(2));
    if (pg_strcasecmp(key, "data_inlining_row_limit") == 0) {
        if (*value == '\0')
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("inline row limit cannot be empty")));
        for (const char* c = value; *c != '\0'; ++c)
            if (*c < '0' || *c > '9')
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("inline row limit must be a nonnegative decimal integer")));
        int64 limit = DatumGetInt64(DirectFunctionCall1(int8in, CStringGetDatum(value)));
        /* Only SQL0.19 has the pure-new committed-table file INSERT path.
         * Older installed SQL contracts retain their explicit zero-limit refusal;
         * accepting the option does not admit CREATE-local or origin file DML. */
        if (limit == 0 && !LakeDirectInsertVersionEnabled())
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("disabling native lake inlining requires the direct file-write path")));
        *canonicalKeyOut = "data_inlining_row_limit";
        *canonicalValueOut = psprintf(INT64_FORMAT, limit);
        *isInlineLimitOut = true;
        return;
    }
    if (pg_strcasecmp(key, "target_file_size") == 0) {
        if (!LakeMergeAdjacentVersionEnabled())
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("native lake target_file_size option requires installed SQL0.60")));
        if (*value == '\0')
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("target_file_size cannot be empty")));
        for (const char* c = value; *c != '\0'; ++c)
            if (*c < '0' || *c > '9')
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("target_file_size must be a positive decimal byte count")));
        int64 targetBytes = DatumGetInt64(DirectFunctionCall1(int8in, CStringGetDatum(value)));
        if (targetBytes <= 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("target_file_size must be greater than zero")));
        *canonicalKeyOut = "target_file_size";
        *canonicalValueOut = psprintf(INT64_FORMAT, targetBytes);
        *isInlineLimitOut = false;
        return;
    }
    if (pg_strcasecmp(key, "auto_compact") == 0) {
        if (!LakeMergeAdjacentVersionEnabled())
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("native lake auto_compact option requires installed SQL0.60")));
        if (pg_strcasecmp(value, "true") == 0 || strcmp(value, "1") == 0 || pg_strcasecmp(value, "on") == 0) {
            *canonicalKeyOut = "auto_compact";
            *canonicalValueOut = pstrdup("true");
            *isInlineLimitOut = false;
            return;
        }
        if (pg_strcasecmp(value, "false") == 0 || strcmp(value, "0") == 0 || pg_strcasecmp(value, "off") == 0) {
            *canonicalKeyOut = "auto_compact";
            *canonicalValueOut = pstrdup("false");
            *isInlineLimitOut = false;
            return;
        }
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("auto_compact must be a boolean value (true/false)")));
    }
    if (pg_strcasecmp(key, "delete_older_than") == 0) {
        if (!LakePhysicalCleanupVersionEnabled())
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("native lake delete_older_than option requires installed SQL0.69")));
        if (*value == '\0')
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("delete_older_than cannot be empty")));
        Datum ivDatum = DirectFunctionCall3(interval_in, CStringGetDatum(value),
            ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
        Interval* iv = DatumGetIntervalP(ivDatum);
        Interval zero = {0};
        if (interval_cmp_internal(iv, &zero) < 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("delete_older_than must be a nonnegative interval")));
        *canonicalKeyOut = "delete_older_than";
        *canonicalValueOut = DatumGetCString(DirectFunctionCall1(interval_out, ivDatum));
        *isInlineLimitOut = false;
        return;
    }
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
        errmsg("unsupported native lake persistent option \"%s\"", key)));
}

static void LakeCheckOptionTransaction(void)
{
    LakeRequireAccessPolicy();
    if (!LakeHasFlushVersion() || !OidIsValid(LakeMetadataOwner()))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake persistent options require a current installation and private metadata owner")));
    if (u_sess->attr.attr_common.XactReadOnly)
        ereport(ERROR, (errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
            errmsg("cannot change native lake options in a read-only transaction")));
    /* Reuse the savepoint-aware DDL exclusion. No callback or commit of our own.
     * This avoids holding the options writer lock while staging lake DML. */
    LakeTxnRejectBindingDdl();
    LakeLockCatalogRegistration();
}

static void LakeStorePersistentOption(LakeInlineBinding* binding, bool tableScope,
    const char* canonicalKey, const char* value, bool isInlineLimit)
{
    if (binding->read_only)
        ereport(ERROR, (errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION), errmsg("native lake catalog is read-only")));
    Oid ns = binding->metadata_schema_oid;
    const char* metadata = LakeMetadataRelation(ns, "ducklake_metadata");
    /* Standard metadata has no UNIQUE key for nullable scopes. Serialize native
     * option writers and exclude ordinary RowExclusive option writes. DML's
     * publication ShareLock pairs with this lock; lake reads do not wait. */
    Relation options = heap_open(get_relname_relid("ducklake_metadata", ns), ShareRowExclusiveLock);
    if (RelationGetDescr(options)->natts != 4 || options->trigdesc != NULL || options->rd_rules != NULL)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported native lake options layout")));
    heap_close(options, NoLock);

    /* A writer may have waited for the lock. Never use the statement's earlier
     * snapshot to decide INSERT versus UPDATE: that would permit duplicates. */
    Snapshot fresh = RegisterSnapshot(GetLatestSnapshot());
    PG_TRY();
    {
        LakeInlineBinding* current = tableScope ?
            LakeLoadCommittedInlineBinding(binding->relation_oid, fresh) : LakeLoadCatalogHeader(binding->server_oid, fresh);
        if (current->read_only || current->metadata_schema_oid != ns ||
            (tableScope && (LakePermanentTableId(current) != LakePermanentTableId(binding) ||
             strcmp(current->table_uuid, binding->table_uuid) != 0)))
            ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE), errmsg("native lake option target changed")));

        Oid types[5] = {TEXTOID, TEXTOID, TEXTOID, INT8OID, TEXTARRAYOID};
        Datum args[5] = {
            CStringGetTextDatum(canonicalKey),
            CStringGetTextDatum(value),
            tableScope ? CStringGetTextDatum("table") : (Datum)0,
            tableScope ? Int64GetDatum(LakePermanentTableId(current)) : (Datum)0,
            (Datum)0
        };
        const char* nulls = tableScope ? NULL : "  nn ";

        if (isInlineLimit) {
            /* Enabling inlining for unbound tables must also be safe. The initial
             * catalog-wide slice deliberately admits only the native primitive
             * column set; richer types and scoped exceptions are not enabled. */
            const char* tables = LakeMetadataRelation(ns, "ducklake_table");
            const char* columns = LakeMetadataRelation(ns, "ducklake_column");
            Datum admitted[lengthof(lake_native_types)+MaxHeapAttributeNumber];
            int admitted_count = 0;
            const bool smallint = LakeSmallintVersionEnabled();
            const bool temporal = LakeTemporalBusinessVersionEnabled();
            const bool binary = tableScope && current->column_evolution && LakeBinaryBusinessVersionEnabled();
            for (Size i = 0; i < lengthof(lake_native_types); ++i)
                if (lake_native_types[i].writable && lake_native_types[i].oid != NUMERICOID &&
                    (lake_native_types[i].oid != INT2OID || smallint) &&
                    (lake_native_types[i].oid != BYTEAOID || binary) &&
                    (!LakeNativeTypeIsTemporal(lake_native_types[i].oid) || temporal))
                    admitted[admitted_count++] = CStringGetTextDatum(lake_native_types[i].duck_type);
            if (tableScope && current->column_evolution && LakeNumericBusinessVersionEnabled())
                for (int c = 0; c < current->column_count; ++c)
                    if (current->columns[c].type == NUMERICOID)
                        admitted[admitted_count++] = CStringGetTextDatum(LakeNumericDuckType(current->columns[c].typmod));
            ArrayType* supported = construct_array(admitted, admitted_count, TEXTOID, -1, false, 'i');
            args[4] = PointerGetDatum(supported);
            LakeMetaRows* unsafe = LakeMetadataQuery(psprintf(
                "SELECT count(*)::bigint FROM %s c JOIN %s t ON t.table_id=c.table_id "
                "WHERE t.end_snapshot IS NULL AND c.end_snapshot IS NULL AND ($4 IS NULL OR t.table_id=$4) AND "
                "(c.column_type IS NULL OR NOT(c.column_type::text=ANY($5)) OR "
                "(c.parent_column IS NOT NULL AND (c.column_type<>'float32' OR NOT EXISTS(SELECT 1 FROM %s p "
                "WHERE p.table_id=c.table_id AND p.column_id=c.parent_column AND p.parent_column IS NULL "
                "AND p.column_type='list' AND p.end_snapshot IS NULL))) OR "
                "(c.column_type='list' AND (SELECT count(*) FROM %s e WHERE e.table_id=c.table_id "
                "AND e.parent_column=c.column_id AND e.end_snapshot IS NULL)<>1) OR "
                "pg_catalog.lower(c.column_name) IN ('_ducklake_row_id','_ducklake_begin_snapshot','_ducklake_end_snapshot'))",
                columns, tables, columns, columns), 5, types, args, nulls, fresh, true, SPI_OK_SELECT, ns);
            if (DatumGetInt64(LakeMetadataValue(unsafe, 0, 1, INT8OID)) != 0)
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("native lake inline options require supported columns and list elements without reserved names")));
        }

        const char* predicate = "key=$1 AND scope IS NOT DISTINCT FROM $3 AND scope_id IS NOT DISTINCT FROM $4";
        LakeMetaRows* rows = LakeMetadataQuery(psprintf("SELECT value::text FROM %s WHERE %s", metadata, predicate),
            4, types, args, nulls, fresh, true, SPI_OK_SELECT, ns);
        if (rows->count > 1)
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("duplicate native lake persistent option")));
        LakeMetaRows* changed = rows->count == 0 ?
            LakeMetadataQuery(psprintf("INSERT INTO %s(key,value,scope,scope_id) VALUES($1,$2,$3,$4)", metadata),
                4, types, args, nulls, fresh, false, SPI_OK_INSERT, ns) :
            LakeMetadataQuery(psprintf("UPDATE %s SET value=$2 WHERE %s", metadata, predicate),
                4, types, args, nulls, fresh, false, SPI_OK_UPDATE, ns);
        LakeMetadataRequireCount(changed, 1, "persistent option writes");
        UnregisterSnapshot(fresh);
    }
    PG_CATCH();
    {
        UnregisterSnapshot(fresh);
        PG_RE_THROW();
    }
    PG_END_TRY();
}

Datum og_lake_set_table_option(PG_FUNCTION_ARGS)
{
    const char* canonicalKey = NULL;
    char* value = NULL;
    bool isInlineLimit = false;
    LakeParsePersistentOption(fcinfo, &canonicalKey, &value, &isInlineLimit);
    if (strcmp(canonicalKey, "delete_older_than") == 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("native lake delete_older_than option requires catalog scope")));
    Relation relation = heap_open(PG_GETARG_OID(0), AccessShareLock);
    if (!LakeIsNativeForeignRelation(relation))
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("native lake option target is not a lake foreign table")));
    if (!pg_class_ownercheck(RelationGetRelid(relation), GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CLASS, RelationGetRelationName(relation));
    LakeCheckOptionTransaction();
    if (LakeTxnFindCreatedTable(RelationGetRelid(relation)) != NULL)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot set options for a transaction-local lake table")));
    LakeInlineBinding* binding = LakeLoadCommittedInlineBinding(RelationGetRelid(relation), GetLatestSnapshot());
    LakeTxnRejectBoundSnapshotWrite(binding->server_oid);
    LakeStorePersistentOption(binding, true, canonicalKey, value, isInlineLimit);
    LakeInvalidateRelationSafe(RelationGetRelid(relation));
    heap_close(relation, NoLock);
    PG_RETURN_VOID();
}

Datum og_lake_set_catalog_option(PG_FUNCTION_ARGS)
{
    if (!PG_ARGISNULL(0) && !PG_ARGISNULL(1) && !PG_ARGISNULL(2) &&
        pg_strcasecmp(text_to_cstring(PG_GETARG_TEXT_PP(1)), "index_root") == 0) {
        LakeSetCatalogIndexRoot(NameStr(*PG_GETARG_NAME(0)), text_to_cstring(PG_GETARG_TEXT_PP(2)));
        PG_RETURN_VOID();
    }
    const char* canonicalKey = NULL;
    char* value = NULL;
    bool isInlineLimit = false;
    LakeParsePersistentOption(fcinfo, &canonicalKey, &value, &isInlineLimit);
    LakeInlineRequireAdministrator();
    LakeCheckOptionTransaction();
    ForeignServer* server = GetForeignServerByName(NameStr(*PG_GETARG_NAME(0)), false);
    LakeTxnRejectBoundSnapshotWrite(server->serverid);
    LakeInlineBinding* binding = LakeLoadCatalogHeader(server->serverid, GetLatestSnapshot());
    LakeStorePersistentOption(binding, false, canonicalKey, value, isInlineLimit);
    LakeInvalidateCatalogRelations(server->serverid, GetLatestSnapshot());
    PG_RETURN_VOID();
}
