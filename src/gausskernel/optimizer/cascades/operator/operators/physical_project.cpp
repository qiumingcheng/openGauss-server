#include "physical_project.h"

namespace cascades {

PhysicalProject::PhysicalProject()
    : PhysicalOptExprOp(OptExprOpType::PHYSICAL_PROJECT) {}

PhysicalProject::PhysicalProject(Path* project_path, Path* child_path)
    : PhysicalOptExprOp(OptExprOpType::PHYSICAL_PROJECT, project_path),
      child_path_(child_path) {}

OptExprOp* PhysicalProject::clone() const {
    return NEW_EX(PhysicalProject, path_, child_path_);
}

size_t PhysicalProject::hash() const {
    return static_cast<size_t>(get_type());
}

std::string PhysicalProject::to_string() const {
    return "PhysicalProject";
}

}  // namespace cascades
