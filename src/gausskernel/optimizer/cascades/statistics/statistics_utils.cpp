#include "statistics_utils.h"
#include "catalog/pg_class.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_type.h"
#include "utils/syscache.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/pathnode.h"
#include "parser/parsetree.h"
#include <algorithm>

namespace cascades {

// ============================================================================
// Table statistics from pg_class
// ============================================================================

double StatisticsUtils::get_table_reltuples(Oid relid) {
    if (!OidIsValid(relid)) {
        return 1000.0;  // Default estimate
    }

    Relation relation = heap_open(relid, AccessShareLock);
    if (relation == NULL) {
        return 1000.0;
    }

    double reltuples = relation->rd_rel->reltuples;
    heap_close(relation, AccessShareLock);

    if (reltuples < 0) {
        reltuples = 1000.0;
    }

    return reltuples;
}

double StatisticsUtils::get_table_relpages(Oid relid) {
    if (!OidIsValid(relid)) {
        return 100.0;
    }

    Relation relation = heap_open(relid, AccessShareLock);
    if (relation == NULL) {
        return 100.0;
    }

    int32 relpages = relation->rd_rel->relpages;
    heap_close(relation, AccessShareLock);

    if (relpages < 0) {
        relpages = 100;
    }

    return static_cast<double>(relpages);
}

// ============================================================================
// Column statistics from pg_statistic
// ============================================================================

double StatisticsUtils::get_column_ndv(Oid relid, AttrNumber attnum) {
    if (!OidIsValid(relid) || attnum <= 0) {
        return 100.0;
    }

    HeapTuple tuple = SearchSysCache3(STATRELATTINH,
                                      ObjectIdGetDatum(relid),
                                      Int16GetDatum(attnum),
                                      BoolGetDatum(false));

    if (!HeapTupleIsValid(tuple)) {
        return 100.0;
    }

    Form_pg_statistic stats = (Form_pg_statistic) GETSTRUCT(tuple);
    double stadistinct = stats->stadistinct;
    ReleaseSysCache(tuple);

    if (stadistinct > 0) {
        return stadistinct;
    } else if (stadistinct < 0) {
        return stadistinct;  // Negative multiplier
    } else {
        return 100.0;
    }
}

double StatisticsUtils::get_column_null_frac(Oid relid, AttrNumber attnum) {
    if (!OidIsValid(relid) || attnum <= 0) {
        return 0.1;
    }

    HeapTuple tuple = SearchSysCache3(STATRELATTINH,
                                      ObjectIdGetDatum(relid),
                                      Int16GetDatum(attnum),
                                      BoolGetDatum(false));

    if (!HeapTupleIsValid(tuple)) {
        return 0.1;
    }

    Form_pg_statistic stats = (Form_pg_statistic) GETSTRUCT(tuple);
    double stanullfrac = stats->stanullfrac;
    ReleaseSysCache(tuple);

    return stanullfrac;
}

double StatisticsUtils::get_column_avg_width(Oid relid, AttrNumber attnum) {
    if (!OidIsValid(relid) || attnum <= 0) {
        return 32.0;
    }

    HeapTuple tuple = SearchSysCache3(STATRELATTINH,
                                      ObjectIdGetDatum(relid),
                                      Int16GetDatum(attnum),
                                      BoolGetDatum(false));

    if (!HeapTupleIsValid(tuple)) {
        return 32.0;
    }

    Form_pg_statistic stats = (Form_pg_statistic) GETSTRUCT(tuple);
    int32 stawidth = stats->stawidth;
    ReleaseSysCache(tuple);

    if (stawidth <= 0) {
        stawidth = 32;
    }

    return static_cast<double>(stawidth);
}

// ============================================================================
// Build Statistics from openGauss metadata
// ============================================================================

Statistics* StatisticsUtils::build_table_statistics(Oid relid, void* target_list) {
    Statistics* stats = new Statistics();

    if (!OidIsValid(relid)) {
        stats->set_row_count(1000.0);
        return stats;
    }

    // Get table row count from pg_class
    double reltuples = get_table_reltuples(relid);
    stats->set_row_count(reltuples);

    // Get relation descriptor to fetch attribute list
    Relation relation = heap_open(relid, AccessShareLock);
    if (relation != NULL) {
        TupleDesc tupdesc = relation->rd_att;

        // Build column statistics for each attribute
        for (int i = 1; i <= tupdesc->natts; i++) {
            if (TupleDescAttr(tupdesc, i - 1)->attisdropped) {
                continue;  // Skip dropped columns
            }

            double ndv = get_column_ndv(relid, i);
            double null_frac = get_column_null_frac(relid, i);
            double avg_width = get_column_avg_width(relid, i);

            ColumnStatsPtr col_stats = std::make_shared<ColumnStats>(
                null_frac, ndv, avg_width);
            stats->add_column_stats(i, col_stats);
        }

        heap_close(relation, AccessShareLock);
    }

    return stats;
}

// ============================================================================
// Selectivity estimation - Extension Point 1: Filter Selectivity
// ============================================================================

double StatisticsUtils::estimate_filter_selectivity(void* clauses, Oid relid) {
    if (clauses == nullptr) {
        return 1.0;  // No clauses = no filtering
    }

    // Cast to openGauss List*
    List* qual_list = reinterpret_cast<List*>(clauses);

    if (qual_list == nullptr || list_length(qual_list) == 0) {
        return 1.0;
    }

    // TODO: This requires a PlannerInfo pointer which is not available here
    // In a full integration, you would pass the PlannerInfo from the planner
    // and call:
    //   return clauselist_selectivity(root, qual_list, varRelid, JOIN_INNER, nullptr);

    // For now, estimate selectivity based on number of clauses
    // Each clause is assumed to reduce rows by a factor
    double selectivity = 1.0;

    // Simple estimation: each clause reduces selectivity
    // This is a placeholder for actual clauselist_selectivity integration
    for (ListCell* cell = list_head(qual_list); cell != nullptr; cell = lnext(cell)) {
        Node* clause = (Node*) lfirst(cell);

        // Check clause type for better estimation
        if (clause != nullptr) {
            // OpExpr (equality): ~0.1 selectivity
            // RangeQuery: ~0.25 selectivity
            // Like/ILike: ~0.1 selectivity
            if (IsA(clause, OpExpr)) {
                selectivity *= 0.1;
            } else if (IsA(clause, NullTest)) {
                selectivity *= 0.5;
            } else {
                selectivity *= 0.2;
            }
        }
    }

    // Clamp to reasonable range
    return std::max(0.0001, std::min(1.0, selectivity));
}

// ============================================================================
// Join selectivity - Extension Point 2: Multi-key Join Estimation
// ============================================================================

double StatisticsUtils::estimate_join_selectivity(void* clauses, Oid left_relid, Oid right_relid) {
    if (clauses == nullptr) {
        return 1.0;  // No join conditions = cartesian product
    }

    List* qual_list = reinterpret_cast<List*>(clauses);

    if (qual_list == nullptr || list_length(qual_list) == 0) {
        return 1.0;
    }

    // Count equality clauses vs other clauses
    int equality_clauses = 0;
    int other_clauses = 0;

    for (ListCell* cell = list_head(qual_list); cell != nullptr; cell = lnext(cell)) {
        Node* clause = (Node*) lfirst(cell);

        if (clause != nullptr) {
            if (IsA(clause, OpExpr)) {
                equality_clauses++;
            } else {
                other_clauses++;
            }
        }
    }

    // Estimate selectivity based on clause types
    // Equality joins: 1/NDV estimation
    double selectivity = 1.0;

    // For equality clauses, use NDV-based estimation
    if (equality_clauses > 0) {
        // Assume average NDV of 100 for each join key
        double avg_ndv = 100.0;
        selectivity *= pow(1.0 / avg_ndv, equality_clauses);
    }

    // For other clauses, use fixed selectivity
    if (other_clauses > 0) {
        selectivity *= pow(0.1, other_clauses);
    }

    return std::max(0.0001, std::min(1.0, selectivity));
}

// ============================================================================
// Column statistics copying and merging - Extension Point 3
// ============================================================================

void StatisticsUtils::copy_column_statistics(Statistics* dest, const Statistics* src,
                                             const std::vector<int>& columns) {
    if (dest == nullptr || src == nullptr) {
        return;
    }

    for (int col : columns) {
        auto col_stats = src->get_column_stats(col);
        if (col_stats != nullptr) {
            dest->add_column_stats(col, col_stats);
        }
    }
}

void StatisticsUtils::merge_column_statistics(Statistics* result,
                                              const std::vector<Statistics*>& children) {
    if (result == nullptr || children.empty()) {
        return;
    }

    // Merge column statistics from all children
    // For joins: combine columns from both sides
    // For other operators: copy from primary child

    for (const auto* child : children) {
        if (child != nullptr) {
            for (int col : child->get_output_columns()) {
                auto col_stats = child->get_column_stats(col);
                if (col_stats != nullptr) {
                    // Only add if not already present
                    if (!result->has_column_stats(col)) {
                        result->add_column_stats(col, col_stats);
                    }
                }
            }
        }
    }
}

// ============================================================================
// Advanced column statistics merging for Join
// ============================================================================

void StatisticsUtils::merge_join_column_statistics(Statistics* result,
                                                   Statistics* left,
                                                   Statistics* right,
                                                   const std::vector<int>& left_keys,
                                                   const std::vector<int>& right_keys) {
    if (result == nullptr || left == nullptr || right == nullptr) {
        return;
    }

    // Copy all columns from left
    for (int col : left->get_output_columns()) {
        auto col_stats = left->get_column_stats(col);
        if (col_stats != nullptr) {
            result->add_column_stats(col, col_stats);
        }
    }

    // Copy all columns from right (with offset if needed)
    for (int col : right->get_output_columns()) {
        auto col_stats = right->get_column_stats(col);
        if (col_stats != nullptr) {
            // For join output, right columns are typically offset
            // The actual offset depends on the schema
            result->add_column_stats(col, col_stats);
        }
    }

    // Update join key statistics based on join type
    // After an inner join, join keys may have reduced NDV
    for (size_t i = 0; i < std::min(left_keys.size(), right_keys.size()); ++i) {
        int left_key = left_keys[i];
        int right_key = right_keys[i];

        auto left_key_stats = left->get_column_stats(left_key);
        auto right_key_stats = right->get_column_stats(right_key);

        if (left_key_stats != nullptr && right_key_stats != nullptr) {
            // For inner join on matching keys, use minimum NDV
            double min_ndv = std::min(left_key_stats->ndv(), right_key_stats->ndv());

            // Create merged stats
            ColumnStatsPtr merged_stats = std::make_shared<ColumnStats>(
                left_key_stats->null_fraction(),
                min_ndv,
                std::max(left_key_stats->avg_width(), right_key_stats->avg_width())
            );

            result->add_column_stats(left_key, merged_stats);
        }
    }
}

}  // namespace cascades
