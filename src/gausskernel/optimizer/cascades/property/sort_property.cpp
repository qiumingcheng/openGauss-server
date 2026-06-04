#include "sort_property.h"
#include "../base/memory_utils.h"
#include "../memo/group.h"
#include "../memo/group_expression.h"
#include "../operator/operators.h"
#include "nodes/parsenodes.h"
#include "optimizer/tlist.h"

namespace cascades {

OrderSpec build_order_spec_from_sort_clause(::PlannerInfo* root, void* sort_clause)
{
    OrderSpec spec;
    if (sort_clause == nullptr) {
        return spec;
    }

    List* targetlist = root != nullptr ? root->parse->targetList : nullptr;
    List* clauses = static_cast<List*>(sort_clause);
    ListCell* lc = nullptr;
    foreach (lc, clauses) {
        SortGroupClause* sgcl = static_cast<SortGroupClause*>(lfirst(lc));
        int column_id = sgcl->tleSortGroupRef;
        bool ascending = (sgcl->sortop < 500);

        int table_id = -1;
        if (targetlist != nullptr) {
            TargetEntry* tle = get_sortgroupref_tle(column_id, targetlist, false);
            if (tle != nullptr && tle->expr != nullptr && IsA(tle->expr, Var)) {
                table_id = ((Var*)tle->expr)->varno;
            }
        }

        spec.add_ordering(column_id, ascending, table_id);
    }
    return spec;
}
bool OrderSpec::is_satisfy(const OrderSpec& required) const {
    // A output order satisfies required order if it starts with the required order
    // e.g., output [a, b, c] satisfies required [a, b] but not required [b, c]
    if (required.orderings_.empty()) {
        return true;  // Any order satisfies "no order required"
    }
    if (orderings_.size() < required.orderings_.size()) {
        return false;
    }
    for (size_t i = 0; i < required.orderings_.size(); ++i) {
        if (orderings_[i].column_id != required.orderings_[i].column_id ||
            orderings_[i].ascending != required.orderings_[i].ascending) {
            return false;
        }
        if (orderings_[i].table_id != required.orderings_[i].table_id) {
            return false;
        }
    }
    return true;
}

size_t OrderSpec::hash() const {
    size_t h = 0;
    for (const auto& ord : orderings_) {
        h = h * 31 + ord.column_id;
        h = h * 37 + (ord.ascending ? 1 : 0);
        h = h * 41 + ord.table_id;
    }
    return h;
}

SortProperty::SortProperty() : is_empty_(true), sort_clause_(nullptr) {}

SortProperty::SortProperty(const OrderSpec& spec) : is_empty_(false), spec_(spec), sort_clause_(nullptr) {}

SortProperty::SortProperty(const OrderSpec& spec, void* sort_clause)
    : is_empty_(false), spec_(spec), sort_clause_(sort_clause) {}

bool SortProperty::is_satisfy(const SortProperty& required) const {
    if (required.is_empty_) {
        return true;  // Any output satisfies "no sort required"
    }
    if (is_empty_) {
        return false;  // No output cannot satisfy "sort required"
    }
    return spec_.is_satisfy(required.spec_);
}

size_t SortProperty::hash() const {
    if (is_empty_) {
        return 0;
    }
    return spec_.hash();
}

bool SortProperty::operator==(const SortProperty& other) const {
    if (is_empty_ && other.is_empty_) return true;
    if (is_empty_ || other.is_empty_) return false;
    return spec_ == other.spec_;
}

std::string SortProperty::to_string() const {
    if (is_empty_) {
        return "SortProperty(empty)";
    }
    std::string result = "SortProperty(";
    const auto& orderings = spec_.get_orderings();
    for (size_t i = 0; i < orderings.size(); ++i) {
        if (i > 0) result += ", ";
        result += "col" + std::to_string(orderings[i].column_id);
        if (orderings[i].table_id >= 0) {
            result += "(table=" + std::to_string(orderings[i].table_id) + ")";
        }
        result += orderings[i].ascending ? " ASC" : " DESC";
    }
    result += ")";
    return result;
}

GroupExpression* SortProperty::append_enforcers(Group* child) const {
    // Create PhysicalSort enforcer for sort property
    PhysicalSort* sort = NEW_EX(PhysicalSort);
    sort->set_sort_clause(sort_clause_);
    std::vector<Group*> inputs = {child};
    GroupExpression* enforcer = NEW_EX(GroupExpression, sort, inputs);
    enforcer->set_group(child);
    return enforcer;
}


}  // namespace cascades