#pragma once
#include "../operator.h"
#include "nodes/nodes.h"
#include <vector>

namespace cascades {

/**
 * LogicalFilter - Logical filter operator
 *
 * Supports both predicate_id (simple) and qual (OpenGauss Node*) for complex expressions.
 * The qual can be used for selectivity estimation with openGauss clauselist_selectivity().
 */
class LogicalFilter : public LogicalOptExprOp {
public:
    LogicalFilter();
    explicit LogicalFilter(class OptExpression* child);

    // Constructor that takes OpenGauss qualifier Node
    explicit LogicalFilter(Node* qual);

    // Constructor with qual list for complex filter conditions
    LogicalFilter(Node* qual, class List* qual_list);

    // Qualifier access
    Node* get_qual() const { return qual_; }
    void set_qual(Node* q) { qual_ = q; }

    // Qual list for selectivity estimation
    void* get_qual_list() const { return qual_list_; }
    void set_qual_list(void* list) { qual_list_ = list; }

    OptExprOp* clone() const override;
    size_t hash() const override;
    std::string to_string() const override;

    int get_predicate_id() const { return predicate_id_; }
    void set_predicate_id(int id) { predicate_id_ = id; }

private:
    int predicate_id_ = -1;
    Node* qual_ = nullptr;         // OpenGauss qualifier expression (single)
    void* qual_list_ = nullptr;    // List* of qualifier clauses for selectivity
};

}  // namespace cascades
