/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * execMerge.cpp
 *	  routines to handle Merge nodes relating to the MERGE command
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/execMerge.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/tableam.h"
#include "access/og_lake.h"
#include "executor/executor.h"
#include "executor/node/nodeModifyTable.h"
#include "executor/exec/execMerge.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "access/heapam.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"

static void ExecMergeNotMatched(ModifyTableState* mtstate, EState* estate, TupleTableSlot* slot, char* partExprKeyStr = NULL);
static bool ExecMergeMatched(ModifyTableState* mtstate, EState* estate, TupleTableSlot* slot, JunkFilter* junkfilter,
    ItemPointer tupleid, HeapTupleHeader oldtuple, Oid oldPartitionOid, int2 bucketid, char* partExprKeyStr = NULL);
static LockTupleMode ExecUpdateLockMode(EState *estate, ResultRelInfo *relinfo);

#define GET_ALL_UPDATED_COLUMNS(relinfo, estate)                                     \
    (bms_union(exec_rt_fetch((relinfo)->ri_RangeTableIndex, estate)->updatedCols, \
        exec_rt_fetch((relinfo)->ri_RangeTableIndex, estate)->extraUpdatedCols))

/*
 * ExecUpdateLockMode -- find the appropriate UPDATE tuple lock mode for a
 * given ResultRelInfo
 */
static LockTupleMode
ExecUpdateLockMode(EState *estate, ResultRelInfo *relinfo)
{
    /**
     * Ustore only support LockTupleExclusive for UPDATE tuple lock
     */
    if (RelationIsUstoreFormat(relinfo->ri_RelationDesc)) {
        return LockTupleExclusive;
    }

    Bitmapset  *keyCols;
    Bitmapset  *updatedCols;

    /*
     * Compute lock mode to use.  If columns that are part of the key have not
     * been modified, then we can use a weaker lock, allowing for better
     * concurrency.
     */
    updatedCols = GET_ALL_UPDATED_COLUMNS(relinfo, estate);
    keyCols = RelationGetIndexAttrBitmap(relinfo->ri_RelationDesc,
                                         INDEX_ATTR_BITMAP_KEY);

    if (bms_overlap(keyCols, updatedCols))
        return LockTupleExclusive;

    return LockTupleNoKeyExclusive;
}

/*
 * Perform MERGE.
 */
void ExecMerge(ModifyTableState* mtstate, EState* estate, TupleTableSlot* slot, JunkFilter* junkfilter,
    ResultRelInfo* resultRelInfo, char* partExprKeyStr)
{
    ExprContext* econtext = mtstate->ps.ps_ExprContext;
    ItemPointer tupleid;
    ItemPointerData tuple_ctid;
    bool matched = false;
    Datum datum;
    bool isNull = false;
    HeapTupleHeader oldtuple = NULL;
    Oid oldPartitionOid = InvalidOid;
    AttrNumber partOidNum;
    AttrNumber bucketIdNum;
    int2 bucketid = InvalidBktId;
    bool foreign_target = resultRelInfo->ri_FdwRoutine != NULL &&
        (RelationIsForeignTable(resultRelInfo->ri_RelationDesc) ||
         RelationIsStream(resultRelInfo->ri_RelationDesc));

    Assert(resultRelInfo->ri_RelationDesc->rd_rel->relkind == RELKIND_RELATION ||
           resultRelInfo->ri_RelationDesc->rd_rel->relkind == PARTTYPE_PARTITIONED_RELATION ||
           junkfilter != NULL);

    /*
     * Reset per-tuple memory context to free any expression evaluation
     * storage allocated in the previous cycle.
     */
    ResetExprContext(econtext);

    /*
     * We run a JOIN between the target relation and the source relation to
     * find a set of candidate source rows that has matching row in the target
     * table and a set of candidate source rows that does not have matching
     * row in the target table. If the join returns us a tuple with target
     * relation's tid set, that implies that the join found a matching row for
     * the given source tuple. This case triggers the WHEN MATCHED clause of
     * the MERGE. Whereas a NULL in the target relation's ctid column
     * indicates a NOT MATCHED case.
     */
    datum = ExecGetJunkAttribute(slot, junkfilter->jf_junkAttNo, &isNull);

    if (foreign_target) {
        /* FDWs identify a matched row with a non-NULL identity junk value.
         * The actual locator is consumed by ExecForeignUpdate/Delete through
         * planSlot, so the heap tupleid remains NULL. */
        matched = !isNull;
        tupleid = NULL;
    } else if (!isNull) {
        matched = true;
        tupleid = (ItemPointer)DatumGetPointer(datum);
        tuple_ctid = *tupleid; /* be sure we don't free ctid!! */
        tupleid = &tuple_ctid;

        if (RELATION_IS_PARTITIONED(resultRelInfo->ri_RelationDesc) ||
            RelationIsCUFormat(resultRelInfo->ri_RelationDesc)) {
            Datum tableOiddatum;
            bool tableOidisnull = false;

            partOidNum = resultRelInfo->ri_partOidAttNum;
            tableOiddatum = ExecGetJunkAttribute(slot, partOidNum, &tableOidisnull);

            if (tableOidisnull) {
                ereport(ERROR,
                    (errcode(ERRCODE_NULL_JUNK_ATTRIBUTE), errmsg("tableoid is null when merge partitioned table")));
            }

            oldPartitionOid = DatumGetObjectId(tableOiddatum);
        }

        if (RELATION_HAS_BUCKET(resultRelInfo->ri_RelationDesc)) {
            Datum bucketIddatum;
            bool bucketIdisnull = false;

            bucketIdNum = resultRelInfo->ri_bucketIdAttNum;
            bucketIddatum = ExecGetJunkAttribute(slot, bucketIdNum, &bucketIdisnull);

            if (bucketIdisnull) {
                ereport(ERROR, (errcode(ERRCODE_NULL_JUNK_ATTRIBUTE), errmsg("bucketid is null when merge table")));
            }

            bucketid = DatumGetObjectId(bucketIddatum);
        }

    } else {
        matched = false;
        tupleid = NULL; /* we don't need it for INSERT actions */
    }

    /*
     * If we are dealing with a WHEN MATCHED case, we execute the first action
     * for which the additional WHEN MATCHED AND quals pass. If an action
     * without quals is found, that action is executed.
     *
     * Similarly, if we are dealing with WHEN NOT MATCHED case, we look at the
     * given WHEN NOT MATCHED actions in sequence until one passes.
     *
     * Things get interesting in case of concurrent update/delete of the
     * target tuple. Such concurrent update/delete is detected while we are
     * executing a WHEN MATCHED action.
     *
     * A concurrent update can:
     *
     * 1. modify the target tuple so that it no longer satisfies the
     * additional quals attached to the current WHEN MATCHED action OR
     *
     * In this case, we are still dealing with a WHEN MATCHED case, but
     * we should recheck the list of WHEN MATCHED actions and choose the first
     * one that satisfies the new target tuple.
     *
     * 2. modify the target tuple so that the join quals no longer pass and
     * hence the source tuple no longer has a match.
     *
     * In the second case, the source tuple no longer matches the target tuple,
     * so we now instead find a qualifying WHEN NOT MATCHED action to execute.
     *
     * A concurrent delete, changes a WHEN MATCHED case to WHEN NOT MATCHED.
     *
     * ExecMergeMatched takes care of following the update chain and
     * re-finding the qualifying WHEN MATCHED action, as long as the updated
     * target tuple still satisfies the join quals i.e. it still remains a
     * WHEN MATCHED case. If the tuple gets deleted or the join quals fail, it
     * returns and we try ExecMergeNotMatched. Given that ExecMergeMatched
     * always make progress by following the update chain and we never switch
     * from ExecMergeNotMatched to ExecMergeMatched, there is no risk of a
     * livelock.
     */
    if (matched)
        matched = ExecMergeMatched(mtstate, estate, slot, junkfilter, tupleid, oldtuple, oldPartitionOid, bucketid, partExprKeyStr);

    /*
     * Either we were dealing with a NOT MATCHED tuple or ExecMergeNotMatched()
     * returned "false", indicating the previously MATCHED tuple is no longer a
     * matching tuple.
     */
    if (!matched)
        ExecMergeNotMatched(mtstate, estate, slot, partExprKeyStr);
}

/*
 * Extract tuple for checking constraints from plan slot
 */
static TupleTableSlot* ExtractConstraintTuple(
    ModifyTableState* mtstate, CmdType commandType, TupleTableSlot* slot, TupleDesc tupDesc)
{
    ExprContext* econtext = mtstate->ps.ps_ExprContext;
    AutoContextSwitch memContext(econtext->ecxt_per_tuple_memory);
    HeapTuple tempTuple = NULL;
    TupleTableSlot* constrSlot = NULL;
    Datum* values = (Datum*)palloc0(sizeof(Datum) * tupDesc->natts);
    bool* isnull = (bool*)palloc0(sizeof(bool) * tupDesc->natts);
    TupleDesc originTupleDesc = slot->tts_tupleDescriptor;
    int index = 0;
    int i = 0;

    switch (commandType) {
        case CMD_UPDATE:
            constrSlot = mtstate->mt_update_constr_slot;
            for (i = 0; i < originTupleDesc->natts; i++) {
                if (strstr(originTupleDesc->attrs[i].attname.data, "action UPDATE target")) {
                    values[index] = slot->tts_values[i];
                    isnull[index] = slot->tts_isnull[i];
                    index++;
                }
            }
            break;
        case CMD_INSERT:
            constrSlot = mtstate->mt_insert_constr_slot;
            for (i = 0; i < originTupleDesc->natts; i++) {
                if (strstr(originTupleDesc->attrs[i].attname.data, "action INSERT target")) {
                    values[index] = slot->tts_values[i];
                    isnull[index] = slot->tts_isnull[i];
                    index++;
                }
            }
            break;
        default:
            Assert(0);
    }

    Assert(constrSlot->tts_tupleDescriptor->td_tam_ops == originTupleDesc->td_tam_ops);
    tempTuple = (HeapTuple)tableam_tops_form_tuple(tupDesc, values, isnull);
    (void)ExecStoreTuple(tempTuple, constrSlot, InvalidBuffer, false);

    return constrSlot;
}

/*
 * Extract scan tuple for target table from plan slot
 */
TupleTableSlot* ExtractScanTuple(ModifyTableState* mtstate, TupleTableSlot* slot, TupleDesc tupDesc)
{
    List* sourceTargetList = ((ModifyTable*)mtstate->ps.plan)->mergeSourceTargetList;
    ExprContext* econtext = mtstate->ps.ps_ExprContext;
    AutoContextSwitch memContext(econtext->ecxt_per_tuple_memory);
    HeapTuple tempTuple = NULL;
    TupleTableSlot* scanSlot = mtstate->mt_scan_slot;
    ListCell* lc = NULL;
    Datum* values = (Datum*)palloc0(sizeof(Datum) * tupDesc->natts);
    bool* isnull = (bool*)palloc0(sizeof(bool) * tupDesc->natts);
    int startIdx = 0;
    int index = 0;

    /*
     * Find the right start index for target table. We should skip the sourceTargetList.
     * First count the number of source targetlist. We add new columns to sourceTargetList
     * but the resno is not continuous, so find the max continuous number to be the original
     * length of sourceTargetList.
     */
    foreach (lc, sourceTargetList) {
        TargetEntry* tle = (TargetEntry*)lfirst(lc);
        if (tle->resno != startIdx + 1)
            break;
        startIdx++;
    }

    for (index = 0; index < tupDesc->natts; index++) {
        if (tupDesc->attrs[index].attisdropped == true) {
            isnull[index] = true;
            continue;
        }

        Assert(startIdx < slot->tts_tupleDescriptor->natts);
        values[index] = tableam_tslot_getattr(slot, startIdx + 1, &isnull[index]);
        startIdx++;
    }

    tempTuple = (HeapTuple)tableam_tops_form_tuple(tupDesc, values, isnull);
    (void)ExecStoreTuple(tempTuple, scanSlot, InvalidBuffer, false);

    return scanSlot;
}

/* Evaluate one MATCHED action and project its UPDATE tuple when needed. */
static bool ExecMergeActionQual(ModifyTableState* mtstate, MergeActionState* action,
    ExprContext* econtext, TupleTableSlot* originSlot, TupleTableSlot** result_slot, EState* estate)
{
    ResultRelInfo* resultRelInfo = estate->es_result_relation_info;
    Relation resultRelationDesc = resultRelInfo->ri_RelationDesc;

    /* DELETE and DO NOTHING have no action targetlist. Their WHEN
     * expressions still need the current target row as the scan tuple. */
    if (estate->es_result_update_remoterel == NULL) {
        econtext->ecxt_scantuple = ExtractScanTuple(mtstate, originSlot, action->tupDesc);
        econtext->ecxt_innertuple = originSlot;
        econtext->ecxt_outertuple = NULL;
    } else {
        econtext->ecxt_scantuple = originSlot;
        econtext->ecxt_innertuple = NULL;
        econtext->ecxt_outertuple = NULL;
    }

    if (!ExecQual((List*)action->whenqual, econtext))
        return false;

    if (action->commandType == CMD_UPDATE) {
        if (estate->es_result_update_remoterel == NULL) {
            *result_slot = ExecProject(action->proj);
        } else {
            *result_slot = originSlot;
            estate->es_result_remoterel = estate->es_result_update_remoterel;
            if (resultRelationDesc->rd_att->constr)
                mtstate->mt_update_constr_slot =
                    ExtractConstraintTuple(mtstate, CMD_UPDATE, *result_slot, action->tupDesc);
        }
    }

    return true;
}

/* Compatibility wrapper retained for callers outside the MERGE loop. */
TupleTableSlot* ExecMergeProjQual(ModifyTableState* mtstate, List* mergeMatchedActionStates, ExprContext* econtext,
    TupleTableSlot* originSlot, TupleTableSlot* result_slot, EState* estate)
{
    if (mergeMatchedActionStates == NIL)
        return NULL;

    MergeActionState* action = (MergeActionState*)linitial(mergeMatchedActionStates);
    if (!ExecMergeActionQual(mtstate, action, econtext, originSlot, &result_slot, estate))
        return NULL;
    return result_slot;
}

/*
 * Check and execute the first qualifying MATCHED action. The current target
 * tuple is identified by tupleid.
 *
 * We start from the first WHEN MATCHED action and check if the WHEN AND quals
 * pass, if any. If the WHEN AND quals for the first action do not pass, we
 * check the second, then the third and so on. If we reach to the end, no
 * action is taken and we return true, indicating that no further action is
 * required for this tuple.
 *
 * If we do find a qualifying action, then we attempt to execute the action.
 *
 * If the tuple is concurrently updated, EvalPlanQual is run with the updated
 * tuple to recheck the join quals. Note that the additional quals associated
 * with individual actions are evaluated separately by the MERGE code, while
 * EvalPlanQual checks for the join quals. If EvalPlanQual tells us that the
 * updated tuple still passes the join quals, then we restart from the first
 * action to look for a qualifying action. Otherwise, we return false meaning
 * that a NOT MATCHED action must now be executed for the current source tuple.
 */
static bool ExecMergeMatched(ModifyTableState* mtstate, EState* estate, TupleTableSlot* slot, JunkFilter* junkfilter,
    ItemPointer tupleid, HeapTupleHeader oldtuple, Oid oldPartitionOid, int2 bucketid, char* partExprKeyStr)
{
    ExprContext* econtext = mtstate->ps.ps_ExprContext;
    List* mergeMatchedActionStates = NIL;
    EPQState* epqstate = &mtstate->mt_epqstate;
    ResultRelInfo* saved_resultRelInfo = NULL;
    ResultRelInfo* resultRelInfo = estate->es_result_relation_info;
    TupleTableSlot* saved_slot = slot;
    bool partKeyUpdated = ((ModifyTable*)mtstate->ps.plan)->partKeyUpdated;

    saved_resultRelInfo = resultRelInfo;
    estate->es_result_relation_info = resultRelInfo;
    mergeMatchedActionStates = resultRelInfo->ri_mergeState->matchedActionStates;
    if (mergeMatchedActionStates == NIL) {
        estate->es_result_relation_info = saved_resultRelInfo;
        return true;
    }

lmerge_matched:
    {
        ListCell* lc = NULL;
        foreach (lc, mergeMatchedActionStates) {
            MergeActionState* action = (MergeActionState*)lfirst(lc);
            TupleTableSlot* action_slot = NULL;

            if (!ExecMergeActionQual(mtstate, action, econtext, slot, &action_slot, estate))
                continue;

            switch (action->commandType) {
                case CMD_NOTHING:
                    LakeMergeRecheckDiscard(resultRelInfo);
                    estate->es_result_relation_info = saved_resultRelInfo;
                    return true;

                case CMD_DELETE:
                    (void)ExecDelete(tupleid,
                        oldPartitionOid,
                        bucketid,
                        oldtuple,
                        slot,
                        epqstate,
                        mtstate,
                        mtstate->canSetTag);
                    if (LakeMergeRecheckFailed(resultRelInfo)) {
                        estate->es_result_relation_info = saved_resultRelInfo;
                        return false;
                    }
                    if (LakeMergeRecheckRetry(resultRelInfo, &slot)) {
                        saved_slot = slot;
                        goto lmerge_matched;
                    }
                    estate->es_result_relation_info = saved_resultRelInfo;
                    return true;

                case CMD_UPDATE: {
                    TM_Result result = TM_Ok;
                    TM_FailureData tmfd;
                    (void)ExecUpdate(tupleid,
                        oldPartitionOid,
                        bucketid,
                        oldtuple,
                        action_slot,
                        saved_slot,
                        epqstate,
                        mtstate,
                        mtstate->canSetTag,
                        partKeyUpdated,
                        &result,
                        partExprKeyStr,
                        &tmfd);

                    /* Native lake performs the row lock and EPQ inside its
                     * FDW callback. A rejected candidate becomes NOT MATCHED. */
                    if (LakeMergeRecheckFailed(resultRelInfo)) {
                        estate->es_result_relation_info = saved_resultRelInfo;
                        return false;
                    }
                    if (LakeMergeRecheckRetry(resultRelInfo, &slot)) {
                        saved_slot = slot;
                        goto lmerge_matched;
                    }

                    switch (result) {
                        case TM_Ok:
                            InstrCountFiltered2(&mtstate->ps, 1);
                            estate->es_result_relation_info = saved_resultRelInfo;
                            return true;

                        case TM_SelfUpdated:
                        case TM_SelfModified:
                            if (TransactionIdIsCurrentTransactionId(tmfd.xmax)) {
                                if (!MERGE_UPDATE_MULTI)
                                    ereport(ERROR,
                                        (errcode(ERRCODE_CARDINALITY_VIOLATION),
                                            errmsg("MERGE command cannot affect row a second time"),
                                            errhint("Ensure that not more than one source row matches any one target row.")));
                                estate->es_result_relation_info = saved_resultRelInfo;
                                return true;
                            }
                            elog(ERROR, "attempted to update or delete invisible tuple");
                            break;

                        case TM_Deleted:
                            if (IsolationUsesXactSnapshot())
                                ereport(ERROR,
                                    (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                                        errmsg("could not serialize access due to concurrent delete")));
                            if (resultRelInfo->ri_RelationDesc->rd_rel->relrowmovement) {
                                Assert(RELATION_IS_PARTITIONED(resultRelInfo->ri_RelationDesc));
                                ereport(ERROR,
                                    (errcode(ERRCODE_TRANSACTION_ROLLBACK),
                                        errmsg("partition table update conflict"),
                                        errdetail("disable row movement of table can avoid this conflict")));
                            }
                            estate->es_result_relation_info = saved_resultRelInfo;
                            return false;

                        case TM_Updated: {
                            Relation resultRelationDesc = resultRelInfo->ri_RelationDesc;
                            Relation partRelationDesc = NULL;
                            TupleTableSlot* epqslot = NULL;
                            LockTupleMode lockmode;
                            bool isNull = false;
                            bool isPartition = RELATION_IS_PARTITIONED(resultRelationDesc);

                            /* Heap MERGE supplies a ctid. Native foreign
                             * targets resolve concurrent versions in their
                             * FDW recheck callback. */
                            if (tupleid == NULL)
                                elog(ERROR, "MERGE update returned TM_Updated without a tuple identity");

                            if (isPartition) {
                                Partition partition = NULL;
                                searchFakeReationForPartitionOid(estate->esfRelations,
                                    estate->es_query_cxt,
                                    resultRelationDesc,
                                    oldPartitionOid,
                                    GetCurrentPartitionNo(oldPartitionOid),
                                    partRelationDesc,
                                    partition,
                                    RowExclusiveLock);
                            }
                            lockmode = ExecUpdateLockMode(estate, resultRelInfo);
                            epqslot = EvalPlanQual(estate,
                                epqstate,
                                isPartition ? partRelationDesc : resultRelationDesc,
                                resultRelInfo->ri_RangeTableIndex,
                                lockmode,
                                &tmfd.ctid,
                                tmfd.xmax,
                                resultRelationDesc->rd_rel->relrowmovement);
                            if (TupIsNull(epqslot)) {
                                estate->es_result_relation_info = saved_resultRelInfo;
                                return false;
                            }

                            (void)ExecGetJunkAttribute(epqslot,
                                resultRelInfo->ri_junkFilter->jf_junkAttNo,
                                &isNull);
                            if (isNull) {
                                estate->es_result_relation_info = saved_resultRelInfo;
                                return false;
                            }

                            if (isPartition) {
                                Datum partoid = ExecGetJunkAttribute(epqslot,
                                    resultRelInfo->ri_partOidAttNum,
                                    &isNull);
                                if (isNull)
                                    ereport(ERROR,
                                        (errcode(ERRCODE_NULL_JUNK_ATTRIBUTE),
                                            errmsg("tableoid is null when merge partitioned table")));
                                Assert(oldPartitionOid == DatumGetObjectId(partoid));
                            }

                            saved_slot = slot = epqslot;
                            *tupleid = tmfd.ctid;
                            goto lmerge_matched;
                        }

                        default:
                            elog(ERROR, "unexpected tuple operation result: %d", result);
                    }
                    break;
                }

                default:
                    elog(ERROR, "unknown action in MERGE WHEN MATCHED clause");
            }
        }
    }

    LakeMergeRecheckDiscard(resultRelInfo);
    estate->es_result_relation_info = saved_resultRelInfo;
    return true;
}

/* Execute the first qualifying NOT MATCHED action. */
static void ExecMergeNotMatched(ModifyTableState* mtstate, EState* estate, TupleTableSlot* slot, char* partExprKeyStr)
{
    ExprContext* econtext = mtstate->ps.ps_ExprContext;
    ResultRelInfo* resultRelInfo = mtstate->resultRelInfo;
    Relation resultRelationDesc = resultRelInfo->ri_RelationDesc;
    const int hi_options = 0;
    List* action_states = resultRelInfo->ri_mergeState->notMatchedActionStates;
    ListCell* lc = NULL;

    /* NOT MATCHED conditions and INSERT expressions can only use the source
     * tuple. Keep the same tuple in both slots for compatibility with the
     * existing projection setup. */
    if (estate->es_result_insert_remoterel == NULL) {
        econtext->ecxt_scantuple = slot;
        econtext->ecxt_innertuple = slot;
        econtext->ecxt_outertuple = NULL;
    } else {
        econtext->ecxt_scantuple = slot;
        econtext->ecxt_innertuple = NULL;
        econtext->ecxt_outertuple = NULL;
    }

    foreach (lc, action_states) {
        MergeActionState* action = (MergeActionState*)lfirst(lc);
        TupleTableSlot* myslot = NULL;

        if (!ExecQual((List*)action->whenqual, econtext))
            continue;

        switch (action->commandType) {
            case CMD_NOTHING:
                return;

            case CMD_INSERT:
                if (estate->es_result_insert_remoterel == NULL) {
                    (void)ExecProject(action->proj);
                    /* ExecPrepareTupleRouting may modify the passed slot, so
                     * use the dedicated MERGE projection slot. */
                    myslot = mtstate->mt_mergeproj;
                } else {
                    myslot = slot;
                    if (resultRelationDesc->rd_att->constr)
                        mtstate->mt_insert_constr_slot =
                            ExtractConstraintTuple(mtstate, CMD_INSERT, slot, action->tupDesc);
                }

                estate->es_result_remoterel = estate->es_result_insert_remoterel;
                (void)ExecInsertT<false>(mtstate,
                    myslot,
                    slot,
                    estate,
                    mtstate->canSetTag,
                    hi_options,
                    NULL,
                    partExprKeyStr);
                InstrCountFiltered1(&mtstate->ps, 1);
                return;

            default:
                elog(ERROR, "unknown action in MERGE WHEN NOT MATCHED clause");
        }
    }
}

/*
 * Creates the run-time state information for the Merge node
 */
void ExecInitMerge(ModifyTableState* mtstate, EState* estate, ResultRelInfo* resultRelInfo)
{
    ListCell* l = NULL;
    ExprContext* econtext = NULL;
    List* mergeMatchedActionStates = NIL;
    List* mergeNotMatchedActionStates = NIL;
    TupleDesc relationDesc = resultRelInfo->ri_RelationDesc->rd_att;
    ModifyTable* node = (ModifyTable*)mtstate->ps.plan;

    if (node->mergeActionList == NIL)
        return;

    mtstate->mt_merge_subcommands = 0;

    if (mtstate->ps.ps_ExprContext == NULL)
        ExecAssignExprContext(estate, &mtstate->ps);

    econtext = mtstate->ps.ps_ExprContext;

    /* initialize scan slot and constraint slot */
    mtstate->mt_scan_slot = NULL;
    mtstate->mt_update_constr_slot = NULL;
    mtstate->mt_insert_constr_slot = NULL;

    /* initialize slot for merge actions */
    Assert(mtstate->mt_mergeproj == NULL);
    mtstate->mt_mergeproj = ExecInitExtraTupleSlot(mtstate->ps.state);
    ExecSetSlotDescriptor(mtstate->mt_mergeproj, relationDesc);

    /*
     * Create a MergeActionState for each action on the mergeActionList
     * and add it to either a list of matched actions or not-matched
     * actions.
     */
    foreach (l, node->mergeActionList) {
        MergeAction* action = (MergeAction*)lfirst(l);
        MergeActionState* action_state = makeNode(MergeActionState);
        TupleDesc tupDesc;

        action_state->matched = action->matched;
        action_state->commandType = action->commandType;
        if (estate->es_is_flt_frame) {
            action_state->whenqual = ExecInitQualByFlatten((List*)action->qual, &mtstate->ps);
        } else {
            action_state->whenqual = ExecInitExprByRecursion((Expr*)action->qual, &mtstate->ps);
        }

        /* DELETE and DO NOTHING still evaluate conditions against the target
         * row, so use the relation descriptor when they have no projection. */
        if (action->commandType == CMD_DELETE || action->commandType == CMD_NOTHING)
            tupDesc = relationDesc;
        else
            tupDesc = ExecTypeFromTL((List*)action->targetList, false, true, relationDesc->td_tam_ops);
        action_state->tupDesc = tupDesc;

        if (action_state->matched && mtstate->mt_scan_slot == NULL) {
            mtstate->mt_scan_slot = MakeSingleTupleTableSlot(tupDesc);
        }

        if (IS_PGXC_COORDINATOR && CMD_UPDATE == action->commandType && relationDesc->constr != NULL) {
            mtstate->mt_update_constr_slot = MakeSingleTupleTableSlot(tupDesc);
        }

        if (IS_PGXC_COORDINATOR && CMD_INSERT == action->commandType && relationDesc->constr != NULL) {
            mtstate->mt_insert_constr_slot = MakeSingleTupleTableSlot(tupDesc);
        }

        /* Build projections only for actions that produce a tuple. */
        if (action->commandType == CMD_INSERT || action->commandType == CMD_UPDATE)
            action_state->proj = ExecBuildProjectionInfo(
                action->targetList, econtext, mtstate->mt_mergeproj, &mtstate->ps, relationDesc);
        else
            action_state->proj = NULL;

        /*
         * We create two lists - one for WHEN MATCHED actions and one
         * for WHEN NOT MATCHED actions - and stick the
         * MergeActionState into the appropriate list.
         */
        if (action_state->matched)
            mergeMatchedActionStates = lappend(mergeMatchedActionStates, action_state);
        else
            mergeNotMatchedActionStates = lappend(mergeNotMatchedActionStates, action_state);

        switch (action->commandType) {
            case CMD_INSERT:
                ExecCheckPlanOutput(resultRelInfo->ri_RelationDesc, action->targetList);
                mtstate->mt_merge_subcommands |= MERGE_INSERT;
                break;
            case CMD_UPDATE:
                ExecCheckPlanOutput(resultRelInfo->ri_RelationDesc, action->targetList);
                mtstate->mt_merge_subcommands |= MERGE_UPDATE;
                break;
            case CMD_DELETE:
                mtstate->mt_merge_subcommands |= MERGE_DELETE;
                break;
            case CMD_NOTHING:
                break;
            default:
                Assert(0);
                break;
        }

        resultRelInfo->ri_mergeState->matchedActionStates = mergeMatchedActionStates;
        resultRelInfo->ri_mergeState->notMatchedActionStates = mergeNotMatchedActionStates;
    }
}
