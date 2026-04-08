#pragma once
#include "../operator.h"
#include "nodes/relation.h"

namespace cascades {

class PhysicalNestLoop : public PhysicalOptExprOp {
public:
    PhysicalNestLoop();
    PhysicalNestLoop(Path* join_path, Path* left_path, Path* right_path);

    OptExprOp* clone() const override;
    size_t hash() const override;
    std::string to_string() const override;

    Path* get_left_path() const { return left_path_; }
    Path* get_right_path() const { return right_path_; }

private:
    Path* left_path_ = nullptr;
    Path* right_path_ = nullptr;
};

}  // namespace cascades
