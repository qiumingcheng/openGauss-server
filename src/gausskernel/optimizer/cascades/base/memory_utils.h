// src/gausskernel/optimizer/cascades/base/memory_utils.h
#pragma once
#include <utility>
#include "postgres.h"
#include "utils/memutils.h"

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
 * MakeTask - Factory function to create Task objects using MemoryContext
 *
 * Usage: MakeTask<TaskType>(args...)
 *
 * This function allocates memory using NEW_EX macro (MemoryContext allocation)
 * and constructs the object using placement new.
 *
 * Memory will be automatically freed when the MemoryContext is reset.
 *
 * Note: Returns raw pointer - caller must ensure MemoryContext outlives the object.
 */
template<typename T, typename... Args>
T* MakeTask(Args&&... args) {
    return NEW_EX(T, std::forward<Args>(args)...);
}

}  // namespace cascades
