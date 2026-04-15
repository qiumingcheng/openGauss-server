#include "physical_nest_loop.h"

namespace cascades {

PhysicalNestLoop::PhysicalNestLoop()
    : PhysicalOptExprOp(OptExprOpType::PHYSICAL_NESTLOOP_JOIN) {}

PhysicalNestLoop::PhysicalNestLoop(Path* join_path, Path* left_path, Path* right_path)
    : PhysicalOptExprOp(OptExprOpType::PHYSICAL_NESTLOOP_JOIN, join_path),
      left_path_(left_path), right_path_(right_path) {}

OptExprOp* PhysicalNestLoop::clone() const {
    return NEW_EX(PhysicalNestLoop, path_, left_path_, right_path_);
}

size_t PhysicalNestLoop::hash() const {
    return static_cast<size_t>(get_type());
}

std::string PhysicalNestLoop::to_string() const {
    return "PhysicalNestLoop";
}

}  // namespace cascades
