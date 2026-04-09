/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 * IDENTIFICATION
 *        src/test/ut/cascades/logical_subquery_test.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "logical_subquery_test.h"

using namespace cascades;

GUNIT_TEST_REGISTRATION(LogicalSubqueryTest, TestCase_DefaultConstructor)
GUNIT_TEST_REGISTRATION(LogicalSubqueryTest, TestCase_SubqueryIdConstructor)
GUNIT_TEST_REGISTRATION(LogicalSubqueryTest, TestCase_SubqueryId)
GUNIT_TEST_REGISTRATION(LogicalSubqueryTest, TestCase_IsCorrelated)
GUNIT_TEST_REGISTRATION(LogicalSubqueryTest, TestCase_OutputColumns)
GUNIT_TEST_REGISTRATION(LogicalSubqueryTest, TestCase_Clone)
GUNIT_TEST_REGISTRATION(LogicalSubqueryTest, TestCase_HashConsistency)
GUNIT_TEST_REGISTRATION(LogicalSubqueryTest, TestCase_HashDifference)
GUNIT_TEST_REGISTRATION(LogicalSubqueryTest, TestCase_ToString)

void LogicalSubqueryTest::SetUp() {}

void LogicalSubqueryTest::TearDown() {}

void LogicalSubqueryTest::TestCase_DefaultConstructor() {
    LogicalSubquery op;

    ASSERT_EQ(op.get_type(), OptExprOpType::LOGICAL_SUBQUERY);
    ASSERT_TRUE(op.is_logical());
    ASSERT_FALSE(op.is_physical());
    ASSERT_EQ(op.get_subquery_id(), -1);
    ASSERT_FALSE(op.is_correlated());
    ASSERT_TRUE(op.get_output_columns().empty());
}

void LogicalSubqueryTest::TestCase_SubqueryIdConstructor() {
    LogicalSubquery op(42);

    ASSERT_EQ(op.get_subquery_id(), 42);
}

void LogicalSubqueryTest::TestCase_SubqueryId() {
    LogicalSubquery op;

    ASSERT_EQ(op.get_subquery_id(), -1);

    op.set_subquery_id(100);
    ASSERT_EQ(op.get_subquery_id(), 100);
}

void LogicalSubqueryTest::TestCase_IsCorrelated() {
    LogicalSubquery op;

    ASSERT_FALSE(op.is_correlated());

    op.set_correlated(true);
    ASSERT_TRUE(op.is_correlated());
}

void LogicalSubqueryTest::TestCase_OutputColumns() {
    LogicalSubquery op;
    std::vector<int> cols = {0, 1, 2};

    op.set_output_columns(cols);

    ASSERT_EQ(op.get_output_columns().size(), (size_t)3);
    ASSERT_EQ(op.get_output_columns()[0], 0);
    ASSERT_EQ(op.get_output_columns()[1], 1);
    ASSERT_EQ(op.get_output_columns()[2], 2);
}

void LogicalSubqueryTest::TestCase_Clone() {
    LogicalSubquery op(42);
    op.set_correlated(true);
    op.set_output_columns({0, 1});

    OptExprOp* cloned = op.clone();
    LogicalSubquery* cloned_subquery = dynamic_cast<LogicalSubquery*>(cloned);

    ASSERT_NE(cloned_subquery, nullptr);
    ASSERT_EQ(cloned_subquery->get_type(), OptExprOpType::LOGICAL_SUBQUERY);
    ASSERT_EQ(cloned_subquery->get_subquery_id(), 42);
    ASSERT_TRUE(cloned_subquery->is_correlated());
    ASSERT_EQ(cloned_subquery->get_output_columns().size(), (size_t)2);

    delete cloned;
}

void LogicalSubqueryTest::TestCase_HashConsistency() {
    LogicalSubquery op1(42);
    LogicalSubquery op2(42);

    ASSERT_EQ(op1.hash(), op2.hash());
}

void LogicalSubqueryTest::TestCase_HashDifference() {
    LogicalSubquery op1(42);
    LogicalSubquery op2(43);

    ASSERT_NE(op1.hash(), op2.hash());
}

void LogicalSubqueryTest::TestCase_ToString() {
    LogicalSubquery op;
    ASSERT_EQ(op.to_string(), "LogicalSubquery");
}
