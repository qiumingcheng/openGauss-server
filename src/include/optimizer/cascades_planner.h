/* -------------------------------------------------------------------------
 *
 * cascades_planner.h
 *    Cascades optimizer entry point
 *
 * Portions Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 * src/include/optimizer/cascades_planner.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef CASCADES_PLANNER_H
#define CASCADES_PLANNER_H

#include "nodes/parsenodes.h"
#include "optimizer/planner.h"

/*
 * Cascades optimizer entry point
 * Can be used as a planner_hook for integration with standard planner
 */
extern PlannedStmt* cascades_planner(Query* parse, int cursorOptions, ParamListInfo boundParams);

/*
 * GUC parameter for enabling/disabling Cascades optimizer
 */
extern bool enable_cascades_optimizer;

#endif /* CASCADES_PLANNER_H */
