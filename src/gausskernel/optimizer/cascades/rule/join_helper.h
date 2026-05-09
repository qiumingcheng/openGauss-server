#pragma once
#include "nodes/relation.h"

namespace cascades {

// Initialize SpecialJoinInfo for join operations (refer to make_join_rel in joinrels.cpp)
inline void init_sjinfo_for_join(SpecialJoinInfo* sjinfo, RelOptInfo* left_rel,
    RelOptInfo* right_rel, JoinType jointype)
{
    sjinfo->type = T_SpecialJoinInfo;
    sjinfo->min_lefthand = left_rel->relids;
    sjinfo->min_righthand = right_rel->relids;
    sjinfo->syn_lefthand = left_rel->relids;
    sjinfo->syn_righthand = right_rel->relids;
    sjinfo->jointype = jointype;
    sjinfo->lhs_strict = false;
    sjinfo->delay_upper_joins = false;
    sjinfo->join_quals = NIL;
    sjinfo->varratio_cached = true;
}

// Initialize JoinPathExtraData for join path creation (refer to make_join_rel in joinrels.cpp)
inline void init_extra_for_join(JoinPathExtraData* extra, SpecialJoinInfo* sjinfo)
{
    extra->inner_unique = false;
    extra->sjinfo = sjinfo;
    extra->semifactors = {};
}

// Zero-initialize JoinCostWorkspace before calling initial_cost_xxx()
inline void init_cost_workspace(JoinCostWorkspace* workspace)
{
    errno_t rc = memset_s(workspace, sizeof(JoinCostWorkspace), 0, sizeof(JoinCostWorkspace));
    securec_check(rc, "\0", "\0");
}

}  // namespace cascades
