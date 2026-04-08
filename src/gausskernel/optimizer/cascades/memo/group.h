#pragma once
#include "group_expression.h"
#include "../property/logical_property.h"
#include "../statistics/statistics.h"
#include "nodes/relation.h"
#include <vector>
#include <unordered_map>
#include <unordered_set>

namespace cascades {

class Group {
public:
    explicit Group(int id);
    ~Group();

    int id() const { return id_; }

    void add_expression(GroupExpression* expr);
    void remove_expression(GroupExpression* expr);
    const std::vector<GroupExpression*>& get_logical_expressions() const { return logical_expressions_; }
    const std::vector<GroupExpression*>& get_physical_expressions() const { return physical_expressions_; }
    GroupExpression* get_first_logical_expression() const;

    void set_logical_property(LogicalProperty* property);
    LogicalProperty* get_logical_property() const { return logical_property_; }

    void set_statistics(Statistics* stats);
    Statistics* get_statistics() const { return statistics_; }

    void set_best_expression(GroupExpression* expr, double cost,
                             const PhysicalPropertySet& property);
    GroupExpression* get_best_expression(const PhysicalPropertySet& property) const;
    bool has_best_expression(const PhysicalPropertySet& property) const;

    double get_cost_lower_bound(const PhysicalPropertySet& property) const;
    void set_cost_lower_bound(const PhysicalPropertySet& property, double cost);

    bool is_explored() const { return is_explored_; }
    void set_explored(bool explored) { is_explored_ = explored; }

    void set_rel(RelOptInfo* rel) { rel_ = rel; }
    RelOptInfo* get_rel() const { return rel_; }

    void merge(Group* other);

    std::string to_string() const;

private:
    int id_;
    std::vector<GroupExpression*> logical_expressions_;
    std::vector<GroupExpression*> physical_expressions_;
    LogicalProperty* logical_property_;
    Statistics* statistics_;
    bool is_explored_;
    RelOptInfo* rel_ = nullptr;
    std::unordered_map<PhysicalPropertySet,
                       std::pair<double, GroupExpression*>,
                       PhysicalPropertySetHash> best_expressions_;
    std::unordered_map<PhysicalPropertySet, double, PhysicalPropertySetHash> cost_lower_bounds_;
};

}