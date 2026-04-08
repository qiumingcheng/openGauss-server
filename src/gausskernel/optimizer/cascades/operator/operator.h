#pragma once
#include <string>
#include <vector>
#include <memory>
#include "nodes/relation.h"

namespace cascades {

// Use OpenGauss JoinType from nodes/nodes.h directly
// typedef enum JoinType { JOIN_INNER, JOIN_LEFT, JOIN_FULL, JOIN_RIGHT, JOIN_SEMI, JOIN_ANTI, ... }

/**
 * OptExprOpType - Types of operators in the Cascades optimizer
 * These map to logical and physical operators in the optimizer
 */
enum class OptExprOpType {
    // Logical operators
    LOGICAL_AGGREGATE,
    LOGICAL_JOIN,
    LOGICAL_FILTER,
    LOGICAL_PROJECT,
    LOGICAL_SCAN,
    LOGICAL_SUBQUERY,

    // Physical operators
    PHYSICAL_HASH_JOIN,
    PHYSICAL_MERGE_JOIN,
    PHYSICAL_NESTLOOP_JOIN,
    PHYSICAL_HASH_AGG,
    PHYSICAL_SORT_AGG,
    PHYSICAL_SCAN,
    PHYSICAL_MATERIAL,
    PHYSICAL_SORT,
    PHYSICAL_LIMIT,
    PHYSICAL_EXCHANGE,

    // Special
    ANY  // wildcard for pattern matching
};

/**
 * OptExprOp - Abstract base class for all operators in the Cascades optimizer
 * This provides an abstraction layer over OpenGauss's Path/Plan nodes
 */
class OptExprOp {
public:
    explicit OptExprOp(OptExprOpType type) : type_(type) {}
    virtual ~OptExprOp() = default;

    OptExprOpType get_type() const { return type_; }

    virtual bool is_logical() const = 0;
    virtual bool is_physical() const = 0;

    virtual std::string to_string() const = 0;
    virtual size_t hash() const = 0;
    virtual OptExprOp* clone() const = 0;

protected:
    OptExprOpType type_;
};

/**
 * LogicalOptExprOp - Base for logical operators (transformation rules apply)
 */
class LogicalOptExprOp : public OptExprOp {
public:
    explicit LogicalOptExprOp(OptExprOpType type) : OptExprOp(type) {}

    bool is_logical() const override { return true; }
    bool is_physical() const override { return false; }
};

/**
 * PhysicalOptExprOp - Base for physical operators (cost can be computed)
 */
class PhysicalOptExprOp : public OptExprOp {
public:
    explicit PhysicalOptExprOp(OptExprOpType type) : OptExprOp(type) {}
    PhysicalOptExprOp(OptExprOpType type, Path* path) : OptExprOp(type), path_(path) {}

    bool is_logical() const override { return false; }
    bool is_physical() const override { return true; }

    Path* get_path() const { return path_; }
    void set_path(Path* path) { path_ = path; }

protected:
    Path* path_ = nullptr;
};

}