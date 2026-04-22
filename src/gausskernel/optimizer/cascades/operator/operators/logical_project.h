#pragma once
#include "../operator.h"
#include <vector>
#include "nodes/nodes.h"
#include "nodes/pg_list.h"

namespace cascades {

class LogicalProject : public LogicalOptExprOp {
public:
    LogicalProject();
    explicit LogicalProject(class OptExpression* child);

    // Constructor that takes OpenGauss target list
    explicit LogicalProject(List* target_list);
    List* get_target_list() const { return target_list_; }
    void set_target_list(List* tl) { target_list_ = tl; }

    OptExprOp* clone() const override;
    size_t hash() const override;
    std::string to_string() const override;

private:
    List* target_list_ = nullptr;  // OpenGauss target list
};

}  // namespace cascades
