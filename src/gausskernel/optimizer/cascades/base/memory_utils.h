// src/gausskernel/optimizer/cascades/base/memory_utils.h
#pragma once
#include <memory>
#include <utility>

namespace cascades {

/**
 * Factory function to create shared_ptr in C++11 style.
 * Equivalent to std::make_unique from C++14.
 */
template<typename T, typename... Args>
std::shared_ptr<T> MakeTask(Args&&... args) {
    return std::shared_ptr<T>(new T(std::forward<Args>(args)...));
}

/**
 * Alias for MakeTask - used for operators and expressions.
 */
template<typename T, typename... Args>
std::shared_ptr<T> MakeOpt(Args&&... args) {
    return std::shared_ptr<T>(new T(std::forward<Args>(args)...));
}

}  // namespace cascades
