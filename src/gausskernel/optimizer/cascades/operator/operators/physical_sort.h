#pragma once
#include "../operator.h"
#include "nodes/relation.h"
#include "nodes/pg_list.h"

namespace cascades {

class PhysicalSort : public PhysicalOptExprOp {
public:
    PhysicalSort();
    PhysicalSort(Path* sort_path, Path* child_path);

    OptExprOp* clone() const override;
    size_t hash() const override;
    std::string to_string() const override;

    Path* get_child_path() const { return child_path_; }
    void* get_sort_clause() const { return sort_clause_; }
    void set_sort_clause(void* clause) { sort_clause_ = clause; }

private:
    Path* child_path_ = nullptr;
    void* sort_clause_ = nullptr;
};

}  // namespace cascades