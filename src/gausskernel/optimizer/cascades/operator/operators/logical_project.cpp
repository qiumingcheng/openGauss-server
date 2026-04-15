#include "logical_project.h"
#include "../../memo/opt_expression.h"

namespace cascades {

LogicalProject::LogicalProject()
    : LogicalOptExprOp(OptExprOpType::LOGICAL_PROJECT) {}

LogicalProject::LogicalProject(OptExpression* child)
    : LogicalOptExprOp(OptExprOpType::LOGICAL_PROJECT) {}

LogicalProject::LogicalProject(List* target_list)
    : LogicalOptExprOp(OptExprOpType::LOGICAL_PROJECT), target_list_(target_list) {}

OptExprOp* LogicalProject::clone() const {
    LogicalProject* cloned = NEW_EX(LogicalProject);
    cloned->set_projections(projections_);
    cloned->set_target_list(target_list_);
    return cloned;
}

size_t LogicalProject::hash() const {
    size_t h = static_cast<size_t>(get_type());
    for (int col : projections_) {
        h = h * 31 + col;
    }
    return h;
}

std::string LogicalProject::to_string() const {
    return "LogicalProject(cols=" + std::to_string(projections_.size()) + ")";
}

}  // namespace cascades
