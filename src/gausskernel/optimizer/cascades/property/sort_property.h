#pragma once
#include <vector>
#include <string>

struct PlannerInfo;

namespace cascades {


class Group;
class GroupExpression;

/**
 * Ordering - Represents a single ordering column with direction
 */
struct Ordering {
    int column_id;
    bool ascending;  // true = ASC, false = DESC
    int table_id;    // cascades table_id, -1 = unknown

    Ordering(int col, bool asc, int tid = -1) : column_id(col), ascending(asc), table_id(tid) {}
};

/**
 * OrderSpec - Collection of orderings (for multi-column sort)
 */
class OrderSpec {
public:
    OrderSpec() = default;
    explicit OrderSpec(const std::vector<Ordering>& orderings) : orderings_(orderings) {}

    const std::vector<Ordering>& get_orderings() const { return orderings_; }
    void add_ordering(int column_id, bool ascending, int table_id = -1) {
        orderings_.emplace_back(column_id, ascending, table_id);
    }

    bool is_satisfy(const OrderSpec& required) const;

    size_t hash() const;

    bool operator==(const OrderSpec& other) const {
        if (orderings_.size() != other.orderings_.size()) return false;
        for (size_t i = 0; i < orderings_.size(); ++i) {
            if (orderings_[i].column_id != other.orderings_[i].column_id ||
                orderings_[i].ascending != other.orderings_[i].ascending) {
                return false;
            }
            if (orderings_[i].table_id != other.orderings_[i].table_id) {
                return false;
            }
        }
        return true;
    }

private:
    std::vector<Ordering> orderings_;
};

/**
 * SortProperty - Represents required sort order property
 */
class SortProperty {
public:
    SortProperty();
    explicit SortProperty(const OrderSpec& spec);
    explicit SortProperty(const OrderSpec& spec, void* sort_clause);
    ~SortProperty() = default;

    bool is_empty() const { return is_empty_; }
    const OrderSpec& get_spec() const { return spec_; }
    void* get_sort_clause() const { return sort_clause_; }

    bool is_satisfy(const SortProperty& required) const;
    size_t hash() const;
    bool operator==(const SortProperty& other) const;
    std::string to_string() const;

    /**
     * Append enforcer (Sort) operator to child group
     * Returns a new GroupExpression with Sort operator on top
     */
    GroupExpression* append_enforcers(Group* child) const;

private:
    bool is_empty_;
    OrderSpec spec_;
    void* sort_clause_;  // Original sortClause (List<SortGroupClause>) for PhysicalSort
};

/**
 * Build OrderSpec from SortGroupClause list, extracting table_id from targetlist Var expressions
 * Returns empty OrderSpec if sort_clause is null
 */
OrderSpec build_order_spec_from_sort_clause(::PlannerInfo* root, void* sort_clause);

}