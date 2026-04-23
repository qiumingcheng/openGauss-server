#pragma once

#include "optimizer/planner.h"
#include "nodes/relation.h"

/*
 * Plan a subquery using Cascades optimizer
 * Called from SubqueryScanImplRule
 */
Plan* cascades_plan_subquery(PlannerGlobal* glob, Query* subquery, PlannerInfo* parent_root,
                             double tuple_fraction, PlannerInfo** subroot);