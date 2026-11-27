/* Native ForeignModify callbacks; only the user's outer transaction publishes. */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "lake_txn_private.h"
#include "lake_row_lock.h"
#include "lake_native_partition.h"
#include "access/og_lake.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "funcapi.h"
#include "nodes/makefuncs.h"
#include "nodes/plannodes.h"
#include "nodes/nodeFuncs.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

static const int LAKE_MODIFY_PRIVATE_VERSION = 2; /* Tagged object locator. */

struct LakeSeenVersion { uint64 source; uint64 row_id; uint64 begin_snapshot; uint64 token; uint64 file_id; uint64 position; };
struct LakeModifyState {
    LakeTxnTable* table;
    LakeTxnTable* prepared_table;
    LakeRowLocator prepared_locator;
    /* Original scan identity is distinct from the fresh locator after EPQ.
     * Only a successful mutation may consume it for command deduplication. */
    LakeRowLocator scanned_locator;
    bool have_scanned_locator;
    TupleTableSlot* latest_slot;
    TupleDesc epq_scan_descriptor;
    bool prepared;
    CmdType operation;
    bool recheck_failed;
    bool recheck_retry;
    TupleTableSlot* retry_slot;
    bool retry_prepared;
    CommandId command_id;
    uint64 statement_token;
    AttrNumber locator_attnum;
    AttrNumber oldrow_attnum;
    HTAB* seen;
    HTAB* seen_rows;
};

struct LakeTargetScan {
    Index rti;
    TupleDesc descriptor;
};

static bool LakeFindTargetScan(PlanState* node, void* argument)
{
    if (node == NULL) return false;
    LakeTargetScan* target = (LakeTargetScan*)argument;
    if (IsA(node->plan, ForeignScan) && ((ForeignScan*)node->plan)->scan.scanrelid == target->rti) {
        target->descriptor = ((ForeignScanState*)node)->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
        return true;
    }
    return planstate_tree_walker(node, LakeFindTargetScan, argument);
}

static void LakeAddForeignUpdateTargets(Query* query, RangeTblEntry* rte, Relation)
{
    LakeRequireWriteVersion(query->commandType);
    if (list_length(query->resultRelations) != 1 || query->hasModifyingCTE)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("native lake modification supports one target and no modifying CTE")));
    Index target = linitial_int(query->resultRelations);
    ListCell* cell = NULL;
    foreach(cell, query->targetList) {
        TargetEntry* entry = (TargetEntry*)lfirst(cell);
        if (entry->resjunk && entry->resname != NULL &&
            (strcmp(entry->resname, "og_lake_row_locator") == 0 || strcmp(entry->resname, "og_lake_old_row") == 0))
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("duplicate native lake modification target")));
    }
    Var* whole = makeWholeRowVar(rte, target, 0, false);
    FuncExpr* marker = makeFuncExpr(LakeLocatorMarkerOid(false), BYTEAOID, list_make1(copyObject(whole)),
        InvalidOid, InvalidOid, COERCE_EXPLICIT_CALL);
    TargetEntry* locator = makeTargetEntry((Expr*)marker, list_length(query->targetList) + 1,
        pstrdup("og_lake_row_locator"), true);
    locator->rtindex = target;
    query->targetList = lappend(query->targetList, locator);
    TargetEntry* oldrow = makeTargetEntry((Expr*)whole, list_length(query->targetList) + 1,
        pstrdup("og_lake_old_row"), true);
    oldrow->rtindex = target;
    query->targetList = lappend(query->targetList, oldrow);
}

static bool LakeReturningHasSystemColumn(Node* node, void*)
{
    if (node == NULL) return false;
    if (IsA(node, Var) && ((Var*)node)->varattno < 0) return true;
    return expression_tree_walker(node, (bool (*)())LakeReturningHasSystemColumn, NULL);
}

static List* LakePlanForeignModifyImpl(PlannerInfo* root, ModifyTable* plan, Index, int subplanIndex)
{
    LakeCheckQueryPermissions(root->parse);
    if ((plan->operation != CMD_INSERT && plan->operation != CMD_UPDATE && plan->operation != CMD_DELETE &&
         plan->operation != CMD_MERGE) ||
        plan->upsertAction != UPSERT_NONE || root->parse->hasModifyingCTE || list_length(root->parse->resultRelations) != 1)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake supports single-target INSERT, UPDATE, DELETE and MERGE without UPSERT or modifying CTE")));
    LakeRequireWriteVersion(plan->operation);
    if (plan->returningLists != NIL &&
        LakeReturningHasSystemColumn((Node*)list_nth(plan->returningLists, subplanIndex), NULL))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("native lake RETURNING does not expose heap system columns")));
    return list_make1(makeInteger(LAKE_MODIFY_PRIVATE_VERSION)); /* locator ABI, never an execution pointer */
}

static List* LakePlanForeignModify(PlannerInfo* root, ModifyTable* plan, Index rti, int subplanIndex)
{
    int fastScope = LakeEnterFastValidationScope();
    List* volatile result = NIL;
    PG_TRY();
    {
        result = LakePlanForeignModifyImpl(root, plan, rti, subplanIndex);
    }
    PG_CATCH();
    {
        LakeLeaveFastValidationScope(fastScope);
        PG_RE_THROW();
    }
    PG_END_TRY();
    LakeLeaveFastValidationScope(fastScope);
    return result;
}

static AttrNumber LakeFindJunk(List* targetlist, const char* name, Oid expectedType)
{
    AttrNumber number = ExecFindJunkAttributeInTlist(targetlist, name);
    if (!AttributeNumberIsValid(number))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("native lake junk target %s was lost", name)));
    ListCell* cell = NULL;
    foreach(cell, targetlist) {
        TargetEntry* entry = (TargetEntry*)lfirst(cell);
        if (entry->resno == number && entry->resjunk && exprType((Node*)entry->expr) == expectedType) return number;
    }
    ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("native lake junk target %s has the wrong type", name)));
    return InvalidAttrNumber;
}

static void LakeBeginForeignModifyImpl(ModifyTableState* node, ResultRelInfo* info, List* privateData, int subplanIndex, int eflags)
{
    if (node!=NULL) LakeCheckExecutorPermissions(node->ps.state);
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY) return;
    if (node == NULL || StreamThreadAmI() ||
        (node->operation != CMD_INSERT && node->operation != CMD_UPDATE && node->operation != CMD_DELETE &&
         node->operation != CMD_MERGE))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported native lake modification execution mode")));
    if (node->operation == CMD_MERGE && !LakeRowLocksEnabled())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("native lake MERGE requires row-lock support")));
    if (list_length(privateData) != 1 || intVal(linitial(privateData)) != LAKE_MODIFY_PRIVATE_VERSION)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported native lake modification plan version")));
    EState* estate = node->ps.state;
    LakeTxnTable* table = LakeTxnReadStatementTable(RelationGetRelid(info->ri_RelationDesc), estate->es_snapshot);
    LakeTxnCheckModify(table, info->ri_RelationDesc, node->operation);
    MemoryContext previous = MemoryContextSwitchTo(estate->es_query_cxt);
    LakeModifyState* state = (LakeModifyState*)palloc0(sizeof(LakeModifyState));
    state->table = table;
    state->operation = node->operation;
    state->command_id = estate->es_output_cid;
    state->statement_token = LakeTxnNextStatementToken();
    if (node->operation != CMD_INSERT) {
        List* targetlist = node->mt_plans[subplanIndex]->plan->targetlist;
        state->locator_attnum = LakeFindJunk(targetlist, "og_lake_row_locator", BYTEAOID);
        state->oldrow_attnum = LakeFindJunk(targetlist, "og_lake_old_row", RelationGetDescr(info->ri_RelationDesc)->tdtypeid);
        HASHCTL control;
        errno_t rc = memset_s(&control, sizeof(control), 0, sizeof(control));
        securec_check(rc, "", "");
        control.keysize = sizeof(LakeSeenVersion); control.entrysize = sizeof(LakeSeenVersion);
        control.hcxt = estate->es_query_cxt;
        state->seen = hash_create("Native lake statement old versions", 128, &control, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
        if (LakeRowLocksEnabled()) {
            control.keysize = sizeof(int64); control.entrysize = sizeof(int64);
            state->seen_rows = hash_create("Native lake statement logical rows", 128, &control,
                HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
            state->latest_slot = MakeSingleTupleTableSlot(table->descriptor);
            LakeTargetScan target = {info->ri_RangeTableIndex, NULL};
            (void)LakeFindTargetScan(node->mt_plans[subplanIndex], &target);
            /* A MERGE target scan is the inner side of the join and uses the
             * base business-row descriptor.  The whole-row value and row
             * locator are produced by the join targetlist.  UPDATE/DELETE
             * keep the traditional N+2 scan tuple. */
            int business_attrs = table->descriptor->natts;
            bool complete_scan = target.descriptor != NULL &&
                (target.descriptor->natts == business_attrs + 2 ||
                 (node->operation == CMD_MERGE && target.descriptor->natts == business_attrs));
            if (!complete_scan)
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("native lake RC update requires a complete target scan tuple (got %d attributes, expected %d)",
                        target.descriptor == NULL ? 0 : target.descriptor->natts,
                        business_attrs + 2)));
            state->epq_scan_descriptor = CreateTupleDescCopy(target.descriptor);
        }
    }
    info->ri_FdwState = state;
    MemoryContextSwitchTo(previous);
}

static void LakeBeginForeignModify(ModifyTableState* node, ResultRelInfo* info, List* privateData, int subplanIndex, int eflags)
{
    int fastScope = LakeEnterFastValidationScope();
    PG_TRY();
    {
        LakeBeginForeignModifyImpl(node, info, privateData, subplanIndex, eflags);
    }
    PG_CATCH();
    {
        LakeLeaveFastValidationScope(fastScope);
        PG_RE_THROW();
    }
    PG_END_TRY();
    LakeLeaveFastValidationScope(fastScope);
}

/* CopyFrom has no ModifyTableState/ForeignModify plan. Its parser, column
 * permissions and line callback remain authoritative. Initialize the same
 * INSERT state used by the ordinary native FDW callbacks, once per command. */
static void LakeBeginCopyFromImpl(EState* estate,ResultRelInfo* info,CommandId commandId)
{
    if(estate==NULL||info==NULL||info->ri_RelationDesc==NULL||
       !LakeIsNativeForeignRelation(info->ri_RelationDesc)||StreamThreadAmI()||
       commandId==InvalidCommandId||info->ri_FdwState!=NULL)
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg("unsupported native lake COPY execution context")));
    /* CopyFrom captured commandId before these SPI/policy checks. */
    LakeRequireCopyVersion();
    LakeCheckExecutorPermissions(estate);
    Snapshot snapshot=ActiveSnapshotSet()?GetActiveSnapshot():GetTransactionSnapshot();
    LakeTxnTable* table=LakeTxnReadStatementTable(RelationGetRelid(info->ri_RelationDesc),snapshot);
    LakeTxnCheckModify(table,info->ri_RelationDesc,CMD_INSERT);
    MemoryContext previous=MemoryContextSwitchTo(estate->es_query_cxt);
    LakeModifyState* state=(LakeModifyState*)palloc0(sizeof(LakeModifyState));
    state->table=table;state->command_id=commandId;state->statement_token=LakeTxnNextStatementToken();
    info->ri_FdwState=state;
    MemoryContextSwitchTo(previous);
    /* EndForeignModify releases the normal read view. Existing transaction/
     * subtransaction abort removes the view and journal rows on ERROR; there
     * is no independent commit, row-SQL expansion or exception swallowing. */
}

void LakeBeginCopyFrom(EState* estate,ResultRelInfo* info,CommandId commandId)
{
    int fastScope = LakeEnterFastValidationScope();
    PG_TRY();
    {
        LakeBeginCopyFromImpl(estate, info, commandId);
    }
    PG_CATCH();
    {
        LakeLeaveFastValidationScope(fastScope);
        PG_RE_THROW();
    }
    PG_END_TRY();
    LakeLeaveFastValidationScope(fastScope);
}

static LakeModifyState* LakeModifyStateFor(ResultRelInfo* info)
{
    LakeModifyState* state = (LakeModifyState*)info->ri_FdwState;
    if (state == NULL)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("native lake modification was not initialized")));
    return state;
}

static void LakeReleasePreparedTarget(LakeModifyState* state)
{
    if (state->prepared_table != NULL && state->prepared_table != state->table)
        LakeTxnReleaseReadView(state->prepared_table);
    state->prepared_table = NULL;
    state->prepared = false;
    state->have_scanned_locator = false;
}

static TupleTableSlot* LakeExecForeignInsert(EState*, ResultRelInfo* info, TupleTableSlot* slot, TupleTableSlot*)
{
    LakeModifyState* state = LakeModifyStateFor(info);
    LakeTxnAppend(state->table, slot, state->command_id, state->statement_token);
    return slot;
}

static bool LakeReadOldLocator(LakeModifyState* state, TupleTableSlot* planSlot, LakeRowLocator* locator)
{
    if (state->prepared) {
        *locator = state->prepared_locator;
        LakeValidateInlineLocator(locator, state->prepared_table, LakeTxnNonce(), state->command_id);
        return true;
    }
    bool isnull = false;
    Datum value = ExecGetJunkAttribute(planSlot, state->locator_attnum, &isnull);
    if (isnull)
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("native lake old row locator is NULL")));
    *locator = LakeDecodeRowLocator(value);
    LakeValidateInlineLocator(locator, state->table, LakeTxnNonce(), state->command_id);
    LakeSeenVersion key = {locator->source, (uint64)locator->row_id, locator->begin_snapshot,
        locator->local_version_token, locator->file_id, locator->file_row_position};
    bool found = false;
    (void)hash_search(state->seen, &key, HASH_FIND, &found);
    if (found) {
        if (state->operation == CMD_MERGE)
            ereport(ERROR, (errcode(ERRCODE_CARDINALITY_VIOLATION),
                errmsg("MERGE command cannot affect a native lake row a second time"),
                errhint("Ensure that not more than one source row matches any one target row.")));
        return false;
    }
    state->scanned_locator = *locator;
    state->have_scanned_locator = true;
    return true;
}

/* An EPQ-rejected join candidate did not modify its target. Keep it eligible
 * for another original join row which can pass the fresh SQL qualification.
 * This also serves legacy callbacks without the RC recheck entry. */
static void LakeRememberModifiedTarget(LakeModifyState* state)
{
    if (!state->have_scanned_locator)
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("native lake modified target lost its original scan identity")));
    const LakeRowLocator* locator = &state->scanned_locator;
    LakeSeenVersion key = {locator->source, (uint64)locator->row_id, locator->begin_snapshot,
        locator->local_version_token, locator->file_id, locator->file_row_position};
    (void)hash_search(state->seen, &key, HASH_ENTER, NULL);
    if (state->seen_rows != NULL && locator->source != LAKE_LOCATOR_TXN)
        (void)hash_search(state->seen_rows, &locator->row_id, HASH_ENTER, NULL);
}

/* The row head carries only synchronization. Its tuple and TM status never
 * substitute for fetching the latest lake business version and locator. */
bool LakeRecheckModifyTarget(EState* estate, ResultRelInfo* info,
    TupleTableSlot** valueSlot, TupleTableSlot** planSlot,
    EPQState* epqstate, ModifyTableState* node)
{
    if(info&&LakeHasNativePartitionMarker(info->ri_RelationDesc))
        return LakeNativePartitionRecheckMutation(estate,info,valueSlot,planSlot,epqstate,node);
    if (info == NULL || info->ri_FdwRoutine == NULL ||
        !LakeIsNativeForeignRelation(info->ri_RelationDesc) || !LakeRowLocksEnabled()) return true;
    LakeModifyState* state = LakeModifyStateFor(info);
    if (node != NULL && node->operation == CMD_MERGE && state->retry_prepared) {
        /* The previous EPQ supplied a fresh joined tuple and locator.  Let
         * the retried action consume that prepared target without validating
         * it against the statement's old read view a second time. */
        state->retry_prepared = false;
        return true;
    }
    state->recheck_failed = false;
    state->recheck_retry = false;
    state->retry_slot = NULL;
    LakeReleasePreparedTarget(state);
    if (node == NULL || (node->operation != CMD_UPDATE && node->operation != CMD_DELETE && node->operation != CMD_MERGE) ||
        planSlot == NULL || TupIsNull(*planSlot) || state->latest_slot == NULL)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("native lake RC modification target was not initialized")));
    LakeRowLocator scanned;
    if (!LakeReadOldLocator(state, *planSlot, &scanned)) {
        state->recheck_failed = true;
        return false;
    }
    if (scanned.source == LAKE_LOCATOR_TXN) {
        state->prepared_locator = scanned;
        state->prepared_table = state->table;
        state->prepared = true;
        return true;
    }
    bool alreadySeen = false;
    (void)hash_search(state->seen_rows, &scanned.row_id, HASH_FIND, &alreadySeen);
    if (alreadySeen) {
        if (node->operation == CMD_MERGE)
            ereport(ERROR, (errcode(ERRCODE_CARDINALITY_VIOLATION),
                errmsg("MERGE command cannot affect a native lake row a second time"),
                errhint("Ensure that not more than one source row matches any one target row.")));
        return false; /* Same command must not update its own output. */
    }
    LakeLockLogicalRow(state->table, scanned.row_id);
    ExecClearTuple(state->latest_slot);
    LakeRowLocator latest;
    LakeTxnTable* latestTable = NULL;
    if (!LakeTxnResolveLatestRow(state->table, scanned.row_id, state->command_id,
        state->latest_slot, &latest, &latestTable)) {
        state->recheck_failed = true;
        return false;
    }
    if (latestTable == NULL)
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("native lake latest row has no read binding")));
    LakeValidateInlineLocator(&latest, latestTable, LakeTxnNonce(), state->command_id);

    if (latest.source == LAKE_LOCATOR_TXN || latest.begin_snapshot != scanned.begin_snapshot) {
        /* EPQ consumes the target scan tuple layout captured at begin. MERGE
         * may use the base N-column scan, while UPDATE/DELETE include the
         * whole business row and refreshed locator as N+2 attributes. */
        MemoryContext caller = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
        int columns = state->table->descriptor->natts;
        int scan_attrs = state->epq_scan_descriptor->natts;
        if (scan_attrs != columns && scan_attrs != columns + 2)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("native lake RC update has an unsupported target scan tuple shape")));
        Datum* values = (Datum*)palloc0(sizeof(Datum) * scan_attrs);
        bool* nulls = (bool*)palloc0(sizeof(bool) * scan_attrs);
        tableam_tslot_getallattrs(state->latest_slot);
        for (int i = 0; i < columns; ++i) {
            values[i] = state->latest_slot->tts_values[i];
            nulls[i] = state->latest_slot->tts_isnull[i];
        }
        HeapTuple business = heap_form_tuple(state->table->descriptor, values, nulls);
        if (scan_attrs == columns + 2) {
            values[columns] = HeapTupleGetDatum(business);
            values[columns + 1] = PointerGetDatum(LakeEncodeRowLocator(&latest));
        }
        HeapTuple injected = heap_form_tuple(state->epq_scan_descriptor, values, nulls);
        MemoryContextSwitchTo(caller);
        volatile bool installed = false;
        TupleTableSlot* checked = NULL;
        PG_TRY();
        {
            EvalPlanQualBegin(epqstate, estate);
            EvalPlanQualSetTuple(epqstate, info->ri_RangeTableIndex, injected);
            installed = true;
            EvalPlanQualFetchRowMarks(epqstate);
            checked = EvalPlanQualNext(epqstate);
            if (!TupIsNull(checked)) (void)ExecMaterializeSlot(checked);
            EvalPlanQualSetTuple(epqstate, info->ri_RangeTableIndex, NULL);
            installed = false;
        }
        PG_CATCH();
        {
            MemoryContextSwitchTo(caller);
            if (installed) EvalPlanQualSetTuple(epqstate, info->ri_RangeTableIndex, NULL);
            PG_RE_THROW();
        }
        PG_END_TRY();
        if (TupIsNull(checked)) {
            if (latestTable != state->table) LakeTxnReleaseReadView(latestTable);
            state->recheck_failed = true;
            return false;
        }
        /* A MERGE EPQ plan is an outer join.  A non-NULL result can still
         * represent NOT MATCHED when the refreshed target side is NULL.
         * Treat the missing locator as a rejected MATCHED candidate so the
         * caller can run the NOT MATCHED action list. */
        if (node->operation == CMD_MERGE) {
            bool locator_is_null = false;
            (void)ExecGetJunkAttribute(checked, state->locator_attnum, &locator_is_null);
            if (locator_is_null) {
                if (latestTable != state->table) LakeTxnReleaseReadView(latestTable);
                state->recheck_failed = true;
                return false;
            }
            /* The matched action was projected before the row lock.  If EPQ
             * refreshed the target, restart action selection with the fresh
             * joined tuple so WHEN predicates and target references are
             * evaluated against the committed version. */
            state->retry_slot = checked;
            state->recheck_retry = true;
            state->prepared_locator = latest;
            state->prepared_table = latestTable;
            state->prepared = true;
            state->retry_prepared = true;
            return false;
        }
        *planSlot = checked;
        if (node->operation == CMD_UPDATE) {
            if (valueSlot == NULL || info->ri_junkFilter == NULL)
                ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("native lake EPQ update has no value slot")));
            *valueSlot = ExecFilterJunk(info->ri_junkFilter, checked);
        }
    }
    state->prepared_locator = latest;
    state->prepared_table = latestTable;
    state->prepared = true;
    return true;
}

bool LakeMergeRecheckFailed(ResultRelInfo* info)
{
    if (info == NULL || info->ri_FdwRoutine == NULL ||
        !LakeIsNativeForeignRelation(info->ri_RelationDesc))
        return false;
    LakeModifyState* state = LakeModifyStateFor(info);
    return state != NULL && state->operation == CMD_MERGE && state->recheck_failed;
}

bool LakeMergeRecheckRetry(ResultRelInfo* info, TupleTableSlot** slot)
{
    if (info == NULL || info->ri_FdwRoutine == NULL ||
        !LakeIsNativeForeignRelation(info->ri_RelationDesc))
        return false;
    LakeModifyState* state = LakeModifyStateFor(info);
    if (state == NULL || state->operation != CMD_MERGE || !state->recheck_retry)
        return false;
    if (slot != NULL)
        *slot = state->retry_slot;
    state->retry_slot = NULL;
    state->recheck_retry = false;
    return true;
}

void LakeMergeRecheckDiscard(ResultRelInfo* info)
{
    if (info == NULL || info->ri_FdwRoutine == NULL ||
        !LakeIsNativeForeignRelation(info->ri_RelationDesc))
        return;
    LakeModifyState* state = LakeModifyStateFor(info);
    if (state->operation == CMD_MERGE && state->retry_prepared) {
        LakeReleasePreparedTarget(state);
        state->retry_prepared = false;
    }
}

static TupleTableSlot* LakeExecForeignUpdate(EState*, ResultRelInfo* info, TupleTableSlot* slot, TupleTableSlot* planSlot)
{
    LakeModifyState* state = LakeModifyStateFor(info);
    LakeRowLocator locator;
    if (!LakeReadOldLocator(state, planSlot, &locator)) return NULL;
    LakeTxnModifyRow(state->prepared ? state->prepared_table : state->table,
        &locator, slot, state->command_id, state->statement_token);
    LakeRememberModifiedTarget(state);
    LakeReleasePreparedTarget(state);
    return slot;
}

static TupleTableSlot* LakeExecForeignDelete(EState* estate, ResultRelInfo* info, TupleTableSlot* slot, TupleTableSlot* planSlot)
{
    LakeModifyState* state = LakeModifyStateFor(info);
    LakeRowLocator locator;
    if (!LakeReadOldLocator(state, planSlot, &locator)) return NULL;
    bool isnull = false;
    Datum oldrow = ExecGetJunkAttribute(planSlot, state->oldrow_attnum, &isnull);
    if (isnull) ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("native lake old business row is NULL")));
    HeapTupleHeader header = DatumGetHeapTupleHeader(oldrow);
    if (HeapTupleHeaderGetTypeId(header) != state->table->descriptor->tdtypeid)
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("native lake old business row type changed")));
    HeapTupleData tuple;
    errno_t rc = memset_s(&tuple, sizeof(tuple), 0, sizeof(tuple));
    securec_check(rc, "", "");
    tuple.t_len = HeapTupleHeaderGetDatumLength(header); tuple.t_data = header;
    ItemPointerSetInvalid(&tuple.t_self);
    MemoryContext previous = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
    HeapTuple copy = heap_copytuple(&tuple);
    MemoryContextSwitchTo(previous);
    LakeTxnModifyRow(state->prepared ? state->prepared_table : state->table,
        &locator, NULL, state->command_id, state->statement_token);
    LakeRememberModifiedTarget(state);
    LakeReleasePreparedTarget(state);
    ExecStoreTuple(copy, slot, InvalidBuffer, false);
    return slot;
}

static void LakeEndForeignModify(EState*, ResultRelInfo* info)
{
    LakeModifyState* state = (LakeModifyState*)info->ri_FdwState;
    if (state != NULL) {
        LakeReleasePreparedTarget(state);
        if (state->latest_slot != NULL) ExecDropSingleTupleTableSlot(state->latest_slot);
        LakeTxnReleaseReadView(state->table);
    }
    info->ri_FdwState = NULL;
}
static int LakeIsForeignRelUpdatable(Relation)
{
    LakeRequireAccessPolicy();
    return (1 << CMD_INSERT) | (1 << CMD_UPDATE) | (1 << CMD_DELETE);
}

void LakeInstallModifyCallbacks(FdwRoutine* routine)
{
    routine->AddForeignUpdateTargets = LakeAddForeignUpdateTargets;
    routine->PlanForeignModify = LakePlanForeignModify;
    routine->BeginForeignModify = LakeBeginForeignModify;
    routine->ExecForeignInsert = LakeExecForeignInsert;
    routine->ExecForeignUpdate = LakeExecForeignUpdate;
    routine->ExecForeignDelete = LakeExecForeignDelete;
    routine->EndForeignModify = LakeEndForeignModify;
    routine->IsForeignRelUpdatable = LakeIsForeignRelUpdatable;
}
