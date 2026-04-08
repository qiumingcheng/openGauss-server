#pragma once
#include "../operator.h"
#include "nodes/nodes.h"

namespace cascades {

class LogicalFilter : public LogicalOptExprOp {
public:
    LogicalFilter();
    explicit LogicalFilter(class OptExpression* child);

    // Constructor that takes OpenGauss qualifier Node
    explicit LogicalFilter(Node* qual);
    Node* get_qual() const { return qual_; }
    void set_qual(Node* q) { qual_ = q; }

    OptExprOp* clone() const override;
    size_t hash() const override;
    std::string to_string() const override;

    int get_predicate_id() const { return predicate_id_; }
    void set_predicate_id(int id) { predicate_id_ = id; }

private:
    int predicate_id_ = -1;
    Node* qual_ = nullptr;  // OpenGauss qualifier expression
};

}  // namespace cascades
