// src/gausskernel/optimizer/cascades/base/memory_utils.h
#pragma once
#include <memory>
#include <utility>

// Note: palloc0 is defined in utils/palloc.h as a macro
// Files using this header should include postgres.h which includes palloc.h

namespace cascades {

/**
 * NEW_EX - Macro for allocation using MemoryContext + placement new
 *
 * Usage: NEW_EX(Type, constructor_args...)
 *
 * This macro allocates memory using palloc0 (MemoryContext allocation)
 * and constructs the object using placement new.
 *
 * Memory will be automatically freed when the MemoryContext is reset.
 *
 * Example:
 *   LogicalFilter* filter = NEW_EX(LogicalFilter, qual_list);
 *   PhysicalScan* scan = NEW_EX(PhysicalScan, path, table_id);
 */
#define NEW_EX(Type, ...) \
    ([](auto&&... args) -> Type* { \
        void* mem = palloc0(sizeof(Type)); \
        return new (mem) Type(std::forward<decltype(args)>(args)...); \
    }(__VA_ARGS__))

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
