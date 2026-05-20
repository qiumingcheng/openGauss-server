#pragma once
#include <vector>
#include <string>

namespace cascades {


class Group;
class GroupExpression;

/**
 * Ordering - Represents a single ordering column with direction
 */
struct Ordering {
    int column_id;
    bool ascending;  // true = ASC, false = DESC

    Ordering(int col, bool asc) : column_id(col), ascending(asc) {}
};

/**
 * OrderSpec - Collection of orderings (for multi-column sort)
 */
class OrderSpec {
public:
    OrderSpec() = default;
    explicit OrderSpec(const std::vector<Ordering>& orderings) : orderings_(orderings) {}

    const std::vector<Ordering>& get_orderings() const { return orderings_; }
    void add_ordering(int column_id, bool ascending) {
        orderings_.emplace_back(column_id, ascending);
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

}