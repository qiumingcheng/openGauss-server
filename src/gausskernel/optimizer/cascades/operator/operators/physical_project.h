#pragma once
#include "../operator.h"
#include "nodes/relation.h"
#include "nodes/pg_list.h"

namespace cascades {

class PhysicalProject : public PhysicalOptExprOp {
public:
    PhysicalProject();
    PhysicalProject(Path* project_path, Path* child_path);

    OptExprOp* clone() const override;
    size_t hash() const override;
    std::string to_string() const override;
    bool equals(const OptExprOp* other) const override;

    Path* get_child_path() const { return child_path_; }
    List* get_target_list() const { return target_list_; }
    void set_target_list(List* tl) { target_list_ = tl; }

private:
    Path* child_path_ = nullptr;
    List* target_list_ = nullptr;
};

}  // namespace cascades
