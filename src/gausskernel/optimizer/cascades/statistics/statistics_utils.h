#pragma once
#include "../statistics/statistics.h"
#include "postgres.h"
#include "access/attnum.h"

namespace cascades {

/**
 * StatisticsUtils - Utilities for integrating with openGauss statistics system
 *
 * This class provides unified access to openGauss statistics:
 * - pg_class.reltuples for table row counts
 * - pg_statistic for column-level statistics (NDV, null fraction, histograms)
 * - Selectivity estimation for predicates and joins
 *
 * All openGauss-specific statistics access is centralized here for maintainability.
 */
class StatisticsUtils {
public:
    StatisticsUtils() = default;
    ~StatisticsUtils() = default;

    // ========================================================================
    // Table statistics from pg_class
    // ========================================================================

    /**
     * Get table row count from pg_class.reltuples
     * @param relid Relation OID
     * @return Estimated row count, or default if not available
     */
    static double get_table_reltuples(Oid relid);

    /**
     * Get table pages count from pg_class.relpages
     * @param relid Relation OID
     * @return Estimated pages, or default if not available
     */
    static double get_table_relpages(Oid relid);

    // ========================================================================
    // Column statistics from pg_statistic
    // ========================================================================

    /**
     * Get column NDV (Number of Distinct Values) from pg_statistic
     * @param relid Relation OID
     * @param attnum Attribute number (1-based)
     * @return NDV estimate, or default if not available
     */
    static double get_column_ndv(Oid relid, AttrNumber attnum);

    /**
     * Get column null fraction from pg_statistic
     * @param relid Relation OID
     * @param attnum Attribute number (1-based)
     * @return Null fraction (0.0 - 1.0)
     */
    static double get_column_null_frac(Oid relid, AttrNumber attnum);

    /**
     * Get column average width from pg_statistic
     * @param relid Relation OID
     * @param attnum Attribute number (1-based)
     * @return Average width in bytes
     */
    static double get_column_avg_width(Oid relid, AttrNumber attnum);

    /**
     * Build Statistics object from a table's pg_class/pg_statistic entries
     * @param relid Relation OID
     * @param target_list Target column list (NIL for all columns)
     * @return Statistics object with row count and column stats
     */
    static Statistics* build_table_statistics(Oid relid, void* target_list = nullptr);

    // ========================================================================
    // Selectivity estimation
    // ========================================================================

    /**
     * Estimate filter selectivity using openGauss estimation
     * Extension Point 1: Integrates with clauselist_selectivity()
     *
     * @param clauses Filter clauses (List* in openGauss terms)
     * @param relid Relation OID
     * @return Selectivity factor (0.0 - 1.0)
     */
    static double estimate_filter_selectivity(void* clauses, Oid relid);

    /**
     * Estimate join selectivity using openGauss estimation
     * Extension Point 2: Supports multi-key join estimation
     *
     * @param clauses Join clauses (List* in openGauss terms)
     * @param left_relid Left relation OID
     * @param right_relid Right relation OID
     * @return Join selectivity factor (0.0 - 1.0)
     */
    static double estimate_join_selectivity(void* clauses, Oid left_relid, Oid right_relid);

    // ========================================================================
    // Column statistics copying and merging
    // Extension Point 3: Complete ColumnStats implementation
    // ========================================================================

    /**
     * Copy column statistics from one Statistics to another for matching columns
     */
    static void copy_column_statistics(Statistics* dest, const Statistics* src,
                                       const std::vector<int>& columns);

    /**
     * Merge column statistics from multiple children
     */
    static void merge_column_statistics(Statistics* result,
                                        const std::vector<Statistics*>& children);

    /**
     * Merge column statistics specifically for join operations
     * Handles join key NDV updates based on join type
     */
    static void merge_join_column_statistics(Statistics* result,
                                             Statistics* left,
                                             Statistics* right,
                                             const std::vector<int>& left_keys,
                                             const std::vector<int>& right_keys);
};

}  // namespace cascades
