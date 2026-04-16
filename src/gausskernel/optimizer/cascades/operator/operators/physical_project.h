#pragma once
#include "../operator.h"
#include "nodes/relation.h"

namespace cascades {

class PhysicalProject : public PhysicalOptExprOp {
public:
    PhysicalProject();
    PhysicalProject(Path* project_path, Path* child_path);

    OptExprOp* clone() const override;
    size_t hash() const override;
    std::string to_string() const override;

    Path* get_child_path() const { return child_path_; }

private:
    Path* child_path_ = nullptr;
};

}  // namespace cascades
