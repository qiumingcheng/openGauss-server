#pragma once
#include "../operator.h"

namespace cascades {

/**
 * PassThroughOperator - A physical operator that passes through output
 *
 * This operator is used as a placeholder in the SortImplRule to represent
 * the "eliminate Sort" path. It sets the child required property to SORTED,
 * forcing the child to produce sorted output (typically via index scan).
 *
 * The PassThrough operator itself does not modify the data - it only enforces
 * that the child must produce sorted output.
 */
class PassThroughOperator : public PhysicalOptExprOp {
public:
    PassThroughOperator();
    explicit PassThroughOperator(Path* path);

    OptExprOpType get_type() const;
    OptExprOp* clone() const override;
    size_t hash() const override;
    std::string to_string() const override;
    bool equals(const OptExprOp* other) const override;
};

}  // namespace cascades
